"""
The Hybrid CSV Ingestor — Fast Engine with GPU Batching
========================================================
ARCHITECTURE:
  Phase 1: PREPARE  — Build all Caveman JSONs + collect context strings
  Phase 2: VECTORIZE — Batch encode ALL strings on GPU in one pass (batch_size=256)
  Phase 3: RESOLVE   — Entity Resolution Gauntlet with pre-computed vectors
                       + UNWIND batch edge injection

PAUSE/RESUME:
  Progress is tracked per-file in SQLite. If interrupted (Ctrl+C), re-running
  the same file will skip already-processed rows automatically.

Usage:
    python ingest_csv.py configs/nifty500.json
    python ingest_csv.py configs/politicians.json
    python ingest_csv.py configs/politicians.json --reset   (clear progress & re-ingest)
"""

import sys
import json
import time
import hashlib
import sqlite3
import pandas as pd

from src.config import NEO4J_URI, NEO4J_USER, NEO4J_PASS, DB_PATH
from src.db_manager import get_db_connection, init_db
from src.knowledge_graph import KnowledgeGraphBackend, get_embedder


# ──── Pause/Resume Progress Tracking ────

def _init_progress_table(conn):
    """Creates the progress tracking table if it doesn't exist."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS CSV_Ingestion_Progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_hash TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            entity_name TEXT,
            status TEXT DEFAULT 'DONE',
            ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(file_hash, row_index)
        )
    ''')
    conn.commit()


def _get_file_hash(filepath):
    """Generates a short hash to identify a specific CSV file."""
    return hashlib.md5(filepath.encode()).hexdigest()[:12]


def _get_completed_rows(conn, file_hash):
    """Returns a set of row indices already processed for this file."""
    cursor = conn.execute(
        "SELECT row_index FROM CSV_Ingestion_Progress WHERE file_hash = ?",
        (file_hash,)
    )
    return {row[0] for row in cursor.fetchall()}


def _mark_rows_done(conn, file_hash, rows):
    """Batch-mark multiple rows as completed."""
    conn.executemany(
        "INSERT OR IGNORE INTO CSV_Ingestion_Progress (file_hash, row_index, entity_name) VALUES (?, ?, ?)",
        [(file_hash, idx, name) for idx, name in rows]
    )
    conn.commit()


def _clear_progress(conn, file_hash):
    """Clears all progress for a specific file (for --reset flag)."""
    conn.execute("DELETE FROM CSV_Ingestion_Progress WHERE file_hash = ?", (file_hash,))
    conn.commit()


# ──── Caveman Schema Builder ────

def extract_universal_year(date_string):
    """Dynamically hunts for a valid year format to prevent hardcoding crashes."""
    import re
    if not date_string:
        return None
    # Searches for a standalone 4-digit number starting with 19 or 20
    match = re.search(r'\b(19\d{2}|20\d{2})\b', str(date_string))
    return int(match.group(1)) if match else None

def build_caveman_json(row, config):
    """
    Transforms a single CSV row into the Caveman Schema JSON that
    kg.ingest_osint_data() expects.
    """
    primary_cfg = config["primary_entity"]
    primary_name = str(row.get(primary_cfg["name_column"], "")).strip()

    if not primary_name:
        return None

    # Temporal Mapping (Century Shield)
    temporal_config = config.get("temporal_mapping", {})
    start_col = temporal_config.get("start_date_column")
    end_col = temporal_config.get("end_date_column")
    
    start_date = str(row.get(start_col, "")).strip() if start_col else None
    end_date = str(row.get(end_col, "")).strip() if end_col else None
    
    start_year = extract_universal_year(start_date)
    end_year = extract_universal_year(end_date)
    
    # Static Date Properties & Unique ID Row tracking
    static_props = {}
    for col in primary_cfg.get("static_date_properties", []):
        val = str(row.get(col, "")).strip()
        if val:
            extracted = extract_universal_year(val)
            if extracted:
                static_props[col] = extracted
                
    unique_col = primary_cfg.get("unique_id_column")
    unique_row_id = str(row.get(unique_col, "")).strip() if unique_col else ""

    # Build Disambiguation Keys
    dis_map = config.get("disambiguation_mapping", {})

    role = "None"
    if "role_or_title" in dis_map:
        role_col = dis_map["role_or_title"]
        role = str(row.get(role_col, "None")).strip() or "None"

    locations = []
    if "associated_locations" in dis_map:
        loc_cols = dis_map["associated_locations"]
        if isinstance(loc_cols, str):
            loc_cols = [loc_cols]
        for col in loc_cols:
            val = str(row.get(col, "")).strip()
            if val:
                locations.append(val)

    organizations = []
    if "affiliated_organizations" in dis_map:
        org_cols = dis_map["affiliated_organizations"]
        if isinstance(org_cols, str):
            org_cols = [org_cols]
        for col in org_cols:
            val = str(row.get(col, "")).strip()
            if val:
                organizations.append(val)

    # Fallback: build generic context from ALL columns
    if not dis_map:
        context_parts = []
        for col_name, value in row.items():
            val = str(value).strip()
            if val and col_name != primary_cfg["name_column"]:
                context_parts.append(val)
        role = ". ".join(context_parts) if context_parts else "None"

    entities = [{
        "name": primary_name,
        "type": primary_cfg.get("type", "UNKNOWN"),
        "static_date_properties": static_props,
        "disambiguation_keys": {
            "role_or_title": role,
            "associated_locations": locations if locations else [],
            "affiliated_organizations": organizations if organizations else []
        }
    }]

    edges = []
    # ──── THE TRASH FILTER ────
    # Prevents phantom supernodes from placeholder values like "N.A." or "Not found"
    # that would connect hundreds of unrelated entities to a single garbage node.
    default_trash = {"", "n.a.", "n/a", "na", "not found", "not available", 
                     "-", "--", "none", "nan", "null", "unknown", "nil", "tbd", "tbc"}
    custom_trash = {v.lower().strip() for v in config.get("trash_values", [])}
    trash_values = default_trash | custom_trash

    for rel in config.get("relationships", []):
        target_col = rel["target_column"]
        raw_value = str(row.get(target_col, "")).strip()

        # Drop trash values before they become nodes
        if raw_value.lower() in trash_values:
            continue

        if rel.get("multi_value", False):
            target_values = [v.strip() for v in raw_value.split(",") if v.strip() and v.strip().lower() not in trash_values]
        else:
            target_values = [raw_value]

        for target_name in target_values:
            target_entity = {
                "name": target_name,
                "type": rel.get("target_type", "UNKNOWN"),
                "disambiguation_keys": {
                    "role_or_title": rel.get("target_type", "None"),
                    "associated_locations": [],
                    "affiliated_organizations": []
                }
            }
            entities.append(target_entity)

            edges.append({
                "source": primary_name,
                "target": target_name,
                "relationship": rel["edge_name"],
                "context": f"Source: CSV import. Column: {target_col}.",
                "event_year": start_year,
                "source_url": f"csv://{config.get('file_path', 'upload')}",
                "article_date": time.strftime("%Y-%m-%d"),
                "start_date": start_date,
                "end_date": end_date,
                "start_year": start_year,
                "end_year": end_year,
                "allow_multiple": rel.get("allow_multiple", False),
                "unique_row_id": unique_row_id
            })

    return {"entities": entities, "edges": edges}


def _build_context_string(entity):
    """Builds the dense context string the same way the gauntlet does."""
    keys = entity.get("disambiguation_keys", {})
    role = keys.get("role_or_title", "None")
    locs = ", ".join(keys.get("associated_locations", [])) if isinstance(keys.get("associated_locations"), list) else "None"
    orgs = ", ".join(keys.get("affiliated_organizations", [])) if isinstance(keys.get("affiliated_organizations"), list) else "None"
    return f"Role: {role}. Locations: {locs}. Organizations: {orgs}."


# ──── SQLite Mirror ────

def mirror_to_sqlite(conn, graph_json, csv_source):
    """Legacy function: Mirrors entities and edges to SQLite for dashboard UI visibility."""
    cursor = conn.cursor()

    for entity in graph_json.get("entities", []):
        try:
            desc = _build_context_string(entity)
            cursor.execute(
                "INSERT OR IGNORE INTO Nodes (Node_ID, Type, Description) VALUES (?, ?, ?)",
                (entity["name"], entity.get("type"), desc)
            )
        except Exception as e:
            pass

    for edge in graph_json.get("edges", []):
        try:
            cursor.execute('''
                INSERT INTO Edges (Source_Node, Target_Node, Relationship, Context, Document_Source, Document_Date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                edge["source"], edge["target"], edge["relationship"],
                edge.get("context", ""), csv_source, edge.get("article_date", "")
            ))
        except Exception as e:
            pass

    conn.commit()


def batch_mirror_to_sqlite(conn, pending_rows, csv_source):
    """Batched mirroring to SQLite for massive speedups."""
    cursor = conn.cursor()
    nodes = []
    edges = []
    
    for row_idx, primary_name, graph_json, _ in pending_rows:
        for entity in graph_json.get("entities", []):
            desc = _build_context_string(entity)
            nodes.append((entity["name"], entity.get("type"), desc))
        for edge in graph_json.get("edges", []):
            edges.append((
                edge["source"], edge["target"], edge["relationship"],
                edge.get("context", ""), csv_source, edge.get("article_date", "")
            ))
            
    cursor.executemany("INSERT OR IGNORE INTO Nodes (Node_ID, Type, Description) VALUES (?, ?, ?)", nodes)
    cursor.executemany('''
        INSERT INTO Edges (Source_Node, Target_Node, Relationship, Context, Document_Source, Document_Date)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', edges)
    conn.commit()



# ══════════════════════════════════════════════════
#  THE FAST 3-PHASE INGESTION ENGINE
# ══════════════════════════════════════════════════

def ingest_csv_fast(config, df, progress_callback=None, skip_gauntlet=False, registry=None):
    """
    Fast CSV ingestion with GPU batching + UNWIND edges + pause/resume.

    Args:
        config: Dict with column mapping (from JSON file or dashboard UI).
        df: Pandas DataFrame of the CSV data.
        progress_callback: Optional callable(current, total, name) for UI updates.
        skip_gauntlet: If True, skip entity resolution and use direct UNWIND bulk
                       injection (MERGE on name+type). ~100x faster for structured CSVs.
                       Vectors are still stored for future news-pipeline resolution.

    Returns:
        Dict with success_count, skip_count, error_count, resumed_count.
    """
    csv_source = f"csv://{config.get('file_path', 'dashboard_upload')}"
    file_hash = _get_file_hash(csv_source)

    # Initialize systems
    init_db()
    conn = get_db_connection()
    _init_progress_table(conn)
    completed_rows = _get_completed_rows(conn, file_hash)

    stats = {"success": 0, "skip": 0, "error": 0, "resumed": len(completed_rows)}

    if completed_rows:
        print(f"⏩ Resuming: {len(completed_rows)} rows already processed, skipping them.")

    # ════════════════════════════════════════════
    # PHASE 1: PREPARE — Build all JSONs + context strings
    # ════════════════════════════════════════════
    print("\n[Phase 1/3] Preparing data...")
    
    primary_col = config["primary_entity"]["name_column"]
    pending_rows = []  # (row_index, primary_name, graph_json)

    for index, row in df.iterrows():
        if index in completed_rows:
            continue

        primary_name = str(row.get(primary_col, "")).strip()
        if not primary_name:
            stats["skip"] += 1
            continue

        graph_json = build_caveman_json(row, config)
        if not graph_json:
            stats["skip"] += 1
            continue

        pending_rows.append((index, primary_name, graph_json, row.to_dict()))

    if not pending_rows:
        print("Nothing to process — all rows already ingested or skipped.")
        conn.close()
        return stats

    print(f"  {len(pending_rows)} rows to process ({stats['resumed']} previously completed)")

    # ════════════════════════════════════════════
    # PHASE 2: GPU BATCH VECTORIZE — One GPU pass for ALL entities
    # ════════════════════════════════════════════
    print("\n[Phase 2/3] GPU batch vectorization...")

    # Collect ALL context strings across all rows
    all_entity_contexts = []  # (entity_name, context_string)
    entity_to_context_idx = {}  # entity_name -> index in all_entity_contexts

    for _, _, graph_json, _ in pending_rows:
        for entity in graph_json["entities"]:
            name = entity["name"]
            if name not in entity_to_context_idx:
                context = _build_context_string(entity)
                entity_to_context_idx[name] = len(all_entity_contexts)
                all_entity_contexts.append((name, context))

    context_strings = [ctx for _, ctx in all_entity_contexts]
    
    print(f"  Encoding {len(context_strings)} unique entities...")
    t0 = time.time()
    
    embedder = get_embedder()
    vectors_array = embedder.encode(
        context_strings,
        batch_size=256,
        show_progress_bar=True,
        normalize_embeddings=False
    )
    
    # Build name -> vector lookup
    precomputed_vectors = {}
    for i, (name, _) in enumerate(all_entity_contexts):
        precomputed_vectors[name] = vectors_array[i].tolist()

    encode_time = time.time() - t0
    print(f"  ✅ Encoded {len(context_strings)} entities in {encode_time:.1f}s")

    # ════════════════════════════════════════════
    # PHASE 3: INJECT INTO NEO4J + SQLITE
    # ════════════════════════════════════════════

    if skip_gauntlet:
        # ─────────────────────────────────────────────
        # FAST PATH: Direct UNWIND bulk injection
        # ─────────────────────────────────────────────
        # For structured CSVs where data is already clean.
        # ~100x faster than the gauntlet. Deduplicates by exact name+type match.
        # Vectors are still stored so the news pipeline can resolve against them.
        print(f"\n[Phase 3/3] UNWIND bulk injection (gauntlet SKIPPED)...")
        
        # Progress steps for UI: 10% collect, 30% nodes, 40-80% edges, 80-95% sqlite, 100% done
        def _fast_progress(pct, msg):
            if progress_callback:
                progress_callback(int(pct * 100), 100, msg)
        
        import uuid as _uuid
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        
        # A. Collect all unique entities into a flat list
        _fast_progress(0.05, "Collecting unique entities...")
        unique_entities = {}
        all_edges = []
        
        for row_idx, primary_name, graph_json, _ in pending_rows:
            for entity in graph_json["entities"]:
                name = entity["name"]
                if name not in unique_entities:
                    etype = entity.get("type", "UNKNOWN")
                    context = _build_context_string(entity)
                    unique_entities[name] = {
                        "id": str(_uuid.uuid4()),
                        "name": name,
                        "type": etype,
                        "name_lower": name.lower(),
                        "vector": precomputed_vectors.get(name, []),
                        "raw_context": context,
                        "static_props": entity.get("static_date_properties", {})
                    }
            for edge in graph_json.get("edges", []):
                edge["row_idx"] = str(row_idx)
            all_edges.extend(graph_json.get("edges", []))
        
        node_batch = list(unique_entities.values())
        _fast_progress(0.10, f"Pushing {len(node_batch)} nodes to Neo4j...")
        print(f"  Pushing {len(node_batch)} unique nodes via UNWIND...")
        
        with driver.session() as session:
            # B. Create index on name_lower for fast MATCH lookups during edge UNWIND
            # Without this, each edge MATCH scans all 15K+ nodes = ~200M comparisons
            session.run("CREATE INDEX entity_name_lower IF NOT EXISTS FOR (n:Entity) ON (n.name_lower)")
            
            # C. UNWIND all nodes in ONE Cypher call
            node_query = """
                UNWIND $batch AS row
                MERGE (n:Entity {name_lower: row.name_lower, type: row.type})
                ON CREATE SET n.id = row.id,
                              n.name = row.name,
                              n.context_vector = row.vector,
                              n.raw_context = row.raw_context,
                              n.name_lower = row.name_lower,
                              n += row.static_props
                ON MATCH SET n.context_vector = CASE 
                    WHEN n.context_vector IS NULL THEN row.vector 
                    ELSE n.context_vector END
            """
            session.run(node_query, batch=node_batch)
            print(f"  ✅ {len(node_batch)} nodes injected.")
            _fast_progress(0.30, "Nodes injected. Pushing edges...")
            
            # D. UNWIND all edges, partitioned by relationship type and allow_multiple
            EDGE_CHUNK_SIZE = 2000
            edges_by_key = {}
            for edge in all_edges:
                rel_type = edge["relationship"].replace(" ", "_").upper()
                allow_mult = edge.get("allow_multiple", False)
                key = (rel_type, allow_mult)
                
                if key not in edges_by_key:
                    edges_by_key[key] = []
                    
                edges_by_key[key].append({
                    "source_lower": edge["source"].lower(),
                    "target_lower": edge["target"].lower(),
                    "context": edge.get("context", ""),
                    "source_url": edge.get("source_url", "Unknown"),
                    "start_date": edge.get("start_date"),
                    "end_date": edge.get("end_date"),
                    "start_year": edge.get("start_year"),
                    "end_year": edge.get("end_year"),
                    "row_id": edge.get("unique_row_id") or edge.get("row_idx", "")
                })
            
            for (rel_type, allow_mult), full_batch in edges_by_key.items():
                for chunk_start in range(0, len(full_batch), EDGE_CHUNK_SIZE):
                    chunk = full_batch[chunk_start:chunk_start + EDGE_CHUNK_SIZE]
                    chunk_end = min(chunk_start + EDGE_CHUNK_SIZE, len(full_batch))
                    edge_pct = 0.30 + (0.50 * (chunk_start / max(len(all_edges), 1)))
                    _fast_progress(edge_pct, f"Pushing {rel_type} edges [{chunk_start+1}-{chunk_end}]...")
                    print(f"  Pushing '{rel_type}' edges [{chunk_start+1}-{chunk_end}/{len(full_batch)}]...")
                    
                    if allow_mult:
                        edge_query = (
                            "UNWIND $batch AS rel "
                            "MATCH (source:Entity) WHERE source.name_lower = rel.source_lower "
                            "MATCH (target:Entity) WHERE target.name_lower = rel.target_lower "
                            f"MERGE (source)-[r:{rel_type} {{csv_row_id: rel.row_id}}]->(target) "
                            "ON CREATE SET r.context = rel.context, r.extracted_at = datetime(), "
                            "  r.source_url = rel.source_url, "
                            "  r.start_date = rel.start_date, r.end_date = rel.end_date, "
                            "  r.start_year = rel.start_year, r.end_year = rel.end_year, "
                            "  r.year = rel.start_year"
                        )
                    else:
                        edge_query = (
                            "UNWIND $batch AS rel "
                            "MATCH (source:Entity) WHERE source.name_lower = rel.source_lower "
                            "MATCH (target:Entity) WHERE target.name_lower = rel.target_lower "
                            f"MERGE (source)-[r:{rel_type}]->(target) "
                            "ON CREATE SET r.context = rel.context, r.extracted_at = datetime(), "
                            "  r.source_url = rel.source_url, "
                            "  r.start_date = rel.start_date, r.end_date = rel.end_date, "
                            "  r.start_year = rel.start_year, r.end_year = rel.end_year, "
                            "  r.year = rel.start_year"
                        )
                    session.run(edge_query, batch=chunk)
            
            print(f"  ✅ {len(all_edges)} edges injected.")
        
        driver.close()
        
        # D. Mirror to SQLite + mark progress
        _fast_progress(0.80, "Mirroring to SQLite...")
        print("  Mirroring to SQLite...")
        batch_mirror_to_sqlite(conn, pending_rows, csv_source)
        stats["success"] += len(pending_rows)
        
        _mark_rows_done(conn, file_hash,
                        [(row_idx, primary_name) for row_idx, primary_name, _, _ in pending_rows])
        
        _fast_progress(1.0, "Ingestion complete!")

    else:
        # ─────────────────────────────────────────────
        # ─────────────────────────────────────────────
        # DETERMINISTIC RAM-GAUNTLET (Orbit Intersection)
        # ─────────────────────────────────────────────
        print(f"\n[Phase 3/3] RAM Gauntlet + UNWIND injection...")
        
        import jellyfish
        def evaluate_ram_candidates(new_name, new_type, target_values_to_verify, existing_candidates):
            new_name_raw = str(new_name).strip().lower()
            new_name_clean = new_name_raw.replace('.', '')
            
            trash_values = ["", "n.a.", "not found", "-", "nan", "none"]
            targets = [str(v).lower().strip() for v in target_values_to_verify if str(v).lower().strip() not in trash_values]

            for candidate in existing_candidates:
                cand_name_raw = str(candidate['name']).strip().lower()
                cand_name_clean = cand_name_raw.replace('.', '')
                
                if new_type == 'PERSON':
                    honorifics = ['shri', 'smt', 'ms', 'mr', 'dr', 'prof']
                    new_tokens = [t for t in new_name_clean.split() if t not in honorifics]
                    cand_tokens = [t for t in cand_name_clean.split() if t not in honorifics]
                    
                    if new_tokens and cand_tokens and new_tokens[0][0] != cand_tokens[0][0]: 
                        continue 

                name_sim = jellyfish.jaro_winkler_similarity(new_name_raw, cand_name_raw)
                
                cand_orbit = set([str(v).lower().strip() for v in candidate.get('props', {}).values()])
                cand_orbit.update([str(v).lower().strip() for v in candidate.get('connected_concepts', [])])

                validated_context_points = sum(1 for val in targets if val in cand_orbit)

                if validated_context_points > 0:
                    if name_sim >= 0.97: 
                        return "AUTO_MERGE", candidate['id']
                    elif name_sim >= 0.88:
                        return "REVIEW_QUEUE", candidate['id']
                else:
                    if name_sim >= 0.99:
                        return "REVIEW_QUEUE", candidate['id']

            return "NEW_ENTITY", None

        import uuid as _uuid
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        
        unique_entities = {}
        all_edges = []
        entity_uuid_map = {}
        
        if registry is None:
            # Fallback if somehow not provided explicitly
            registry = {}
            
        print("  Evaluating Entities in RAM...")
        for row_idx, primary_name, graph_json, row_dict in pending_rows:
            all_edges.extend(graph_json.get("edges", []))
            
            for entity in graph_json["entities"]:
                name = entity["name"]
                if name not in entity_uuid_map:
                    etype = entity.get("type", "UNKNOWN")
                    context = _build_context_string(entity)
                    vector = precomputed_vectors.get(name, [])
                    
                    honorifics = ['shri', 'smt', 'ms', 'mr', 'dr', 'prof']
                    clean_tokens = [t for t in name.lower().replace('.', '').split() if t not in honorifics]
                    blocking_key = clean_tokens[-1] if clean_tokens else "unknown"

                    target_values_to_verify = []
                    for k, v in entity.get("disambiguation_keys", {}).items():
                        if isinstance(v, list): target_values_to_verify.extend(v)
                        else: target_values_to_verify.append(v)
                        
                    for edge in graph_json.get("edges", []):
                        if edge["source"] == name: target_values_to_verify.append(edge["target"])
                        elif edge["target"] == name: target_values_to_verify.append(edge["source"])

                    candidates = registry.get(blocking_key, [])
                    
                    decision, target_id = evaluate_ram_candidates(
                        name, etype, target_values_to_verify, candidates
                    )
                    
                    if decision == "NEW_ENTITY" or not target_id:
                        final_id = str(_uuid.uuid4())
                        
                        # 1. The NEO4J PAYLOAD (Strict, Flat, Schema-Safe)
                        db_node = {
                            "id": final_id,
                            "name": name,
                            "type": etype,
                            "name_lower": name.lower(),
                            "vector": vector if len(vector) > 0 else [0.0] * 384,
                            "raw_context": context,
                            "static_props": entity.get("static_date_properties", {})
                        }
                        unique_entities[name] = db_node
                        
                        # 2. The RAM PAYLOAD (Rich, Nested, Orbit-Aware)
                        ram_node = {
                            "id": final_id,
                            "name": db_node["name"],
                            "type": db_node["type"],
                            "props": row_dict, 
                            "connected_concepts": target_values_to_verify
                        }
                        
                        if blocking_key not in registry:
                            registry[blocking_key] = []
                        registry[blocking_key].append(ram_node)

                        entity_uuid_map[name] = final_id
                    else:
                        entity_uuid_map[name] = target_id

        node_batch = list(unique_entities.values())
        print(f"  Pushing {len(node_batch)} new unique nodes via UNWIND...")
        
        with driver.session() as session:
            # Inject Nodes
            if node_batch:
                NODE_CHUNK_SIZE = 2000
                for chunk_start in range(0, len(node_batch), NODE_CHUNK_SIZE):
                    chunk = node_batch[chunk_start:chunk_start + NODE_CHUNK_SIZE]
                    node_query = """
                        UNWIND $batch AS row
                        MERGE (n:Entity {id: row.id})
                        ON CREATE SET n.name = row.name, n.type = row.type, 
                                      n.context_vector = row.vector, n.raw_context = row.raw_context,
                                      n.name_lower = row.name_lower, n += row.static_props
                    """
                    session.run(node_query, batch=chunk)
                print(f"  ✅ {len(node_batch)} new nodes injected.")
            
            # Inject Edges
            for row_idx, primary_name, graph_json, row_dict in pending_rows:
                for edge in graph_json.get("edges", []):
                    edge["row_idx"] = str(row_idx)
            
            EDGE_CHUNK_SIZE = 2000
            edges_by_key = {}
            for edge in all_edges:
                rel_type = edge["relationship"].replace(" ", "_").upper()
                allow_mult = edge.get("allow_multiple", False)
                key = (rel_type, allow_mult)
                
                if key not in edges_by_key:
                    edges_by_key[key] = []
                
                source_id = entity_uuid_map.get(edge["source"])
                target_id = entity_uuid_map.get(edge["target"])
                
                if source_id and target_id:
                    edges_by_key[key].append({
                        "source_id": source_id,
                        "target_id": target_id,
                        "context": edge.get("context", ""),
                        "source_url": edge.get("source_url", "Unknown"),
                        "start_date": edge.get("start_date"),
                        "end_date": edge.get("end_date"),
                        "start_year": edge.get("start_year"),
                        "end_year": edge.get("end_year"),
                        "row_id": edge.get("unique_row_id") or edge.get("row_idx", "")
                    })
            
            for (rel_type, allow_mult), full_batch in edges_by_key.items():
                for chunk_start in range(0, len(full_batch), EDGE_CHUNK_SIZE):
                    chunk = full_batch[chunk_start:chunk_start + EDGE_CHUNK_SIZE]
                    if allow_mult:
                        edge_query = (
                            "UNWIND $batch AS rel "
                            "MATCH (source:Entity {id: rel.source_id}) "
                            "MATCH (target:Entity {id: rel.target_id}) "
                            f"MERGE (source)-[r:{rel_type} {{csv_row_id: rel.row_id}}]->(target) "
                            "ON CREATE SET r.context = rel.context, r.extracted_at = datetime(), "
                            "  r.source_url = rel.source_url, "
                            "  r.start_date = rel.start_date, r.end_date = rel.end_date, "
                            "  r.start_year = rel.start_year, r.end_year = rel.end_year, "
                            "  r.year = rel.start_year"
                        )
                    else:
                        edge_query = (
                            "UNWIND $batch AS rel "
                            "MATCH (source:Entity {id: rel.source_id}) "
                            "MATCH (target:Entity {id: rel.target_id}) "
                            f"MERGE (source)-[r:{rel_type}]->(target) "
                            "ON CREATE SET r.context = rel.context, r.extracted_at = datetime(), "
                            "  r.source_url = rel.source_url, "
                            "  r.start_date = rel.start_date, r.end_date = rel.end_date, "
                            "  r.start_year = rel.start_year, r.end_year = rel.end_year, "
                            "  r.year = rel.start_year"
                        )
                    session.run(edge_query, batch=chunk)
                    
            print(f"  ✅ {len(all_edges)} edges injected.")
            
        driver.close()
        
        print("  Mirroring to SQLite...")
        batch_mirror_to_sqlite(conn, pending_rows, csv_source)
        stats["success"] += len(pending_rows)
        
        _mark_rows_done(conn, file_hash,
                        [(row_idx, primary_name) for row_idx, primary_name, _, _ in pending_rows])


    conn.close()

    # Summary
    mode = "UNWIND Bulk (fast)" if skip_gauntlet else "Entity Resolution Gauntlet"
    print(f"\n{'='*60}")
    print(f"  INGESTION COMPLETE")
    print(f"  ✅ Success: {stats['success']} rows")
    print(f"  ⏭️  Skipped: {stats['skip']} rows")
    print(f"  ❌ Errors:  {stats['error']} rows")
    print(f"  ⏩ Resumed: {stats['resumed']} rows (from previous run)")
    print(f"  ⚡ GPU encode: {encode_time:.1f}s")
    print(f"  🔧 Mode: {mode}")
    print(f"{'='*60}\n")

    return stats


# ──── CLI Entry Point ────

def ingest_csv(config_path, reset=False, fast=False):
    """CLI wrapper that loads config + CSV and calls the fast engine in chunks."""
    import gc
    with open(config_path) as f:
        config = json.load(f)

    csv_path = config["file_path"]
    print(f"\n{'='*60}")
    print(f"  FAST CSV INGESTOR (GPU Batch + UNWIND)")
    print(f"  Config: {config_path}")
    print(f"  CSV:    {csv_path}")
    print(f"  Mode:   {'UNWIND Bulk (--fast)' if fast else 'Entity Resolution Gauntlet'}")
    print(f"{'='*60}")

    if reset:
        conn = get_db_connection()
        _init_progress_table(conn)
        _clear_progress(conn, _get_file_hash(f"csv://{csv_path}"))
        conn.close()
        print("🔄 Progress cleared — starting fresh.")

    try:
        print(f"Loading {csv_path} in chunks...")
        chunk_iterator = pd.read_csv(csv_path, chunksize=10000)
    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_path}")
        return

    registry = None
    if not fast:
        from src.knowledge_graph import KnowledgeGraphBackend
        from src.config import NEO4J_URI, NEO4J_USER, NEO4J_PASS
        kg = KnowledgeGraphBackend(NEO4J_URI, NEO4J_USER, NEO4J_PASS)
        registry = kg.fetch_candidate_registry()
        kg.close()

    for chunk_number, chunk in enumerate(chunk_iterator):
        print(f"\n--- Processing Chunk {chunk_number + 1} ---")
        chunk = chunk.fillna("")
        ingest_csv_fast(config, chunk, skip_gauntlet=fast, registry=registry)
        
        # Critical: Flush the RAM before the next loop
        del chunk
        gc.collect()

    print("✅ Massive Dataset Ingested Successfully.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ingest_csv.py <config.json> [--fast] [--reset]")
        print("  --fast   Skip entity resolution, use UNWIND bulk injection (~100x faster)")
        print("  --reset  Clear progress and re-ingest from scratch")
        sys.exit(1)

    reset_flag = "--reset" in sys.argv
    fast_flag = "--fast" in sys.argv
    ingest_csv(sys.argv[1], reset=reset_flag, fast=fast_flag)

