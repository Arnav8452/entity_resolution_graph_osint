import uuid
import jellyfish
import numpy as np
import dateutil.parser
from datetime import datetime
from scipy.spatial.distance import cosine
from neo4j import GraphDatabase
from src.config import ENTITY_JW_THRESHOLD, ENTITY_VECTOR_THRESHOLD

# ──── Constants ────
VECTOR_DIMENSIONS = 384  # all-MiniLM-L6-v2 output size

# Lazy-loaded embedder (only initialized when entity resolution actually runs)
_embedder = None

def get_embedder():
    global _embedder
    if _embedder is None:
        from sentence_transformers import SentenceTransformer
        print("Loading Vector Embedding Model into VRAM...")
        _embedder = SentenceTransformer('all-MiniLM-L6-v2')
    return _embedder


# ──── Person Detection & Attribute Parsing ────

PERSON_PREFIXES = frozenset({
    "shri", "smt", "ms", "mr", "dr", "prof", "justice",
    "lt", "gen", "col", "maj", "capt", "sri", "kumari"
})

def _is_person_name(name):
    """Check if this entity name is a person (has a title prefix)."""
    tokens = name.strip().split()
    return bool(tokens) and tokens[0].lower().rstrip('.') in PERSON_PREFIXES


def _extract_surname(name):
    """Extract the last token (surname) from a person name.
    Returns None if surname is too short (single initial) to be useful for blocking.
    """
    tokens = name.strip().split()
    if len(tokens) >= 2:
        surname = tokens[-1].lower()
        if len(surname) >= 3:  # Skip single initials like "S" or "Bk"
            return surname
    return None


def _parse_context_attrs(context_str):
    """Parse 'Role: X. Locations: Y. Organizations: Z.' into attribute dict."""
    attrs = {}
    if not context_str:
        return attrs
    for part in context_str.split(". "):
        part = part.strip().rstrip(".")
        if ": " in part:
            key, val = part.split(": ", 1)
            attrs[key.strip()] = val.strip()
    return attrs


# ──── Layer 1: Smart Candidate Blocking ────

def _fetch_layer1_candidates(tx, entity_name):
    """
    Layer 1: The Nomination Net — executed server-side via Cypher.
    
    SMART BLOCKING (Palantir-style):
      - Person names (Shri/Ms./Dr.): Block by SURNAME (last token).
        "Shri A P Sarwan" only compares against other "...Sarwan" entities, 
        not all 5,000 "Shri..." officers.
      - Non-person entities (departments, locations): Block by first-4-chars prefix.
    """
    is_person = _is_person_name(entity_name)
    surname = _extract_surname(entity_name) if is_person else None
    
    if is_person and surname:
        query = """
            MATCH (n:Entity)
            WHERE n.name IS NOT NULL AND n.context_vector IS NOT NULL
              AND (
                toLower(n.name) CONTAINS toLower($name_fragment)
                OR toLower($name_fragment) CONTAINS toLower(n.name)
                OR n.name_lower CONTAINS $surname
              )
            RETURN n.id AS node_id, n.name AS name, n.context_vector AS vector,
                   n.raw_context AS raw_context
        """
        return tx.run(query, name_fragment=entity_name, surname=surname).data()
    else:
        query = """
            MATCH (n:Entity)
            WHERE n.name IS NOT NULL AND n.context_vector IS NOT NULL
              AND (
                toLower(n.name) CONTAINS toLower($name_fragment)
                OR toLower($name_fragment) CONTAINS toLower(n.name)
                OR n.name_lower STARTS WITH toLower(left($name_fragment, 4))
              )
            RETURN n.id AS node_id, n.name AS name, n.context_vector AS vector,
                   n.raw_context AS raw_context
        """
        return tx.run(query, name_fragment=entity_name).data()


def get_or_create_entity(tx, entity_name, entity_type, dense_context, precomputed_vector=None):
    """
    The Multi-Attribute Entity Resolution Engine (Palantir-style).
    
    ARCHITECTURE:
      Layer 0: Smart Blocking (Cypher-side) — surname for persons, prefix for orgs
      Layer 1: The Nomination Net (Python-side)
        - PERSON entities: JW > 0.92 AND cadre/location must match
        - NON-PERSON entities: JW > 0.85 OR substring match (with length ratio guard)
      Layer 2: The Vector Interrogation — cosine similarity final check
    """
    # 1. Vector: pre-computed or encode on-the-fly
    if precomputed_vector is not None:
        new_vector = precomputed_vector
    else:
        new_vector = get_embedder().encode(dense_context).tolist()
    
    # 2. Determine if this is a person entity
    is_person = _is_person_name(entity_name)
    new_attrs = _parse_context_attrs(dense_context) if is_person else {}
    
    # 3. Fetch candidates via smart blocking
    candidates = _fetch_layer1_candidates(tx, entity_name)
    
    best_match_id = None
    best_match_name = None
    highest_vector_sim = -1.0
    
    # 4. THE GAUNTLET
    for record in candidates:
        existing_name = record['name']
        existing_vector = record['vector']
        
        if not existing_vector or not existing_name:
            continue
        
        jw_score = jellyfish.jaro_winkler_similarity(entity_name.lower(), existing_name.lower())
        new_name_clean = entity_name.strip().lower()
        cand_name_clean = existing_name.strip().lower()
        
        # ---------------------------------------------------------
        # RULE 1: PERSON LOGIC (Multi-attribute + First-Token Lock)
        # ---------------------------------------------------------
        if is_person or entity_type == 'PERSON':
            honorifics = ['shri', 'smt', 'ms', 'mr', 'dr', 'prof']
            new_tokens = [t for t in new_name_clean.replace('.', '').split() if t not in honorifics]
            cand_tokens = [t for t in cand_name_clean.replace('.', '').split() if t not in honorifics]
            
            if new_tokens and cand_tokens and new_tokens[0][0] != cand_tokens[0][0]:
                continue # Abort merge

            # Require VERY high name similarity to prevent false positives
            if jw_score <= 0.92:
                continue
            
            # Require matching cadre/location
            existing_attrs = _parse_context_attrs(record.get('raw_context', ''))
            new_loc = new_attrs.get("Locations", "").lower().strip()
            existing_loc = existing_attrs.get("Locations", "").lower().strip()
            
            if (new_loc and existing_loc 
                and new_loc != "none" and existing_loc != "none"
                and new_loc != "" and existing_loc != ""):
                loc_match = (new_loc == existing_loc or 
                           new_loc in existing_loc or 
                           existing_loc in new_loc)
                if not loc_match:
                    continue  # Different cadre/state = not the same person
                    
        # ---------------------------------------------------------
        # RULE 2: CONCEPT & ORG LOGIC (The Anti-Hierarchy Lock)
        # ---------------------------------------------------------
        elif entity_type in ['CONCEPT', 'ORGANIZATION', 'LOCATION']:
            # 1. Exact matches are always merged
            if new_name_clean == cand_name_clean:
                pass 
            else:
                # 2. Prevent Vector-Merging of Hierarchical Modifiers
                hierarchy_keywords = ['sub ', 'deputy ', 'joint ', 'additional ', 'vice ', 'assistant ']
                
                new_has_modifier = any(mod in new_name_clean for mod in hierarchy_keywords)
                cand_has_modifier = any(mod in cand_name_clean for mod in hierarchy_keywords)
                
                if new_has_modifier != cand_has_modifier:
                    continue # Abort merge ("Sub Divisional" != "Divisional")

                # 3. Trust Spelling (Jaro-Winkler) over Semantic Vectors for categories
                if jw_score < 0.95: 
                    # If vectors are high but spelling is different, do not auto merge.
                    continue
        
        else:
            # ──── NON-PERSON (Other): Original JW + substring gate ────
            is_substring = False
            if len(entity_name) > 4 and len(existing_name) > 4:
                shorter = min(len(entity_name), len(existing_name))
                longer = max(len(entity_name), len(existing_name))
                if shorter / longer >= 0.6:
                    is_substring = (entity_name.lower() in existing_name.lower()) or \
                                   (existing_name.lower() in entity_name.lower())
            
            if not (jw_score > ENTITY_JW_THRESHOLD or is_substring):
                continue
        
        # ──── LAYER 2: The Vector Interrogation ────
        vector_sim = 1 - cosine(new_vector, existing_vector)
        
        if vector_sim > highest_vector_sim:
            highest_vector_sim = vector_sim
            best_match_id = record['node_id']
            best_match_name = existing_name
                    
    # 5. The Final Decision
    if highest_vector_sim >= ENTITY_VECTOR_THRESHOLD and best_match_id:
        print(f"🔗 MERGE: '{entity_name}' matched existing node '{best_match_name}' (Vector Confidence: {highest_vector_sim:.2f})")
        return best_match_id
    else:
        if best_match_id:
             print(f"🛑 REJECTED: '{entity_name}' spelling matched, but context failed (Score: {highest_vector_sim:.2f}). Splitting node.")
        else:
             print(f"🆕 NEW ENTITY: '{entity_name}' created.")
             
        new_id = str(uuid.uuid4())
        
        create_query = """
            MERGE (n:Entity {id: $id})
            ON CREATE SET n.name = $name, 
                          n.type = $type, 
                          n.context_vector = $vector,
                          n.name_lower = toLower($name),
                          n.raw_context = $context
        """
        tx.run(create_query, id=new_id, name=entity_name, type=entity_type, vector=new_vector, context=dense_context)
        return new_id



class KnowledgeGraphBackend:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._ensure_indexes()

    def _ensure_indexes(self):
        """
        Creates Neo4j indexes on startup if they don't already exist.
        - Text index on Entity.name_lower for fast prefix/contains queries
        - Uniqueness constraint on Entity.id
        """
        with self.driver.session() as session:
            try:
                # Index for fast text lookups in Layer 1 candidate filtering
                session.run("""
                    CREATE TEXT INDEX entity_name_text IF NOT EXISTS
                    FOR (n:Entity) ON (n.name_lower)
                """)
                print("✅ Neo4j text index on Entity.name_lower ensured.")
            except Exception as e:
                # Index may already exist or Neo4j version doesn't support this syntax
                print(f"⚠️ Text index creation note: {e}")
            
            try:
                # Uniqueness constraint for UUID-based entity deduplication
                session.run("""
                    CREATE CONSTRAINT entity_id_unique IF NOT EXISTS
                    FOR (n:Entity) REQUIRE n.id IS UNIQUE
                """)
                print("✅ Neo4j uniqueness constraint on Entity.id ensured.")
            except Exception as e:
                print(f"⚠️ Constraint creation note: {e}")
                
            try:
                # Backfill name_lower for any existing entities missing it
                session.run("""
                    MATCH (n:Entity) WHERE n.name_lower IS NULL AND n.name IS NOT NULL
                    SET n.name_lower = toLower(n.name)
                """)
            except Exception:
                pass

    def fetch_candidate_registry(self):
        """Pulls the entity graph into RAM, flattening 1-hop relationships into an 'Orbit' context pool."""
        print("Pre-fetching Neo4j Database Orbits into RAM...")
        
        # Restrict the OPTIONAL MATCH to high-value entities to prevent RAM exhaustion
        query = """
            MATCH (n:Entity)
            OPTIONAL MATCH (n)-[]-(neighbor:Entity) 
            WHERE neighbor.type IN ['ORGANIZATION', 'LOCATION', 'PERSON']
            WITH n, collect(DISTINCT toLower(neighbor.name)) AS connected_concepts
            RETURN n.id AS id, n.name AS name, n.type AS type, 
                   n.context_vector AS vector, 
                   properties(n) AS props,
                   connected_concepts
        """
        
        registry = {}
        with self.driver.session() as session:
            result = session.run(query)
            for record in result:
                name_str = str(record["name"]).lower().replace('.', '')
                
                # Strip honorifics to find the true blocking key
                honorifics = ['shri', 'smt', 'ms', 'mr', 'dr', 'prof']
                clean_tokens = [t for t in name_str.split() if t not in honorifics]
                
                # Group by surname (last token) to preserve Jaro-Winkler capability inside the canopy
                blocking_key = clean_tokens[-1] if clean_tokens else "unknown"
                
                if blocking_key not in registry:
                    registry[blocking_key] = []
                registry[blocking_key].append(record.data())
                
        print(f"✅ Pre-fetched {sum(len(v) for v in registry.values())} entities into {len(registry)} blocking canopies.")
        return registry

    def close(self):
        self.driver.close()

    def ingest_osint_data(self, graph_json, precomputed_vectors=None):
        """
        Ingests a Caveman Schema JSON into Neo4j.
        
        Args:
            graph_json: Standard caveman schema with entities and edges.
            precomputed_vectors: Optional dict mapping entity name -> vector list.
                                 When provided, skips GPU encoding for those entities.
        """
        if not graph_json:
            return
            
        entity_uuid_map = {}
        vectors = precomputed_vectors or {}
            
        with self.driver.session() as session:
            # 1. Ingest Nodes (Entities) via the Vector Condenser
            for entity in graph_json.get("entities", []):
                name = entity["name"]
                ent_type = entity.get("type", "UNKNOWN")
                keys = entity.get("disambiguation_keys", {})
                
                # 2. The Condenser: Turn the JSON keys into a dense semantic string
                role = keys.get("role_or_title", "None")
                locs = ", ".join(keys.get("associated_locations", [])) if isinstance(keys.get("associated_locations"), list) else "None"
                orgs = ", ".join(keys.get("affiliated_organizations", [])) if isinstance(keys.get("affiliated_organizations"), list) else "None"
                
                dense_context = f"Role: {role}. Locations: {locs}. Organizations: {orgs}."
                
                # Use pre-computed vector if available
                pre_vec = vectors.get(name)
                
                resolved_uuid = session.execute_write(
                    get_or_create_entity, 
                    name, 
                    ent_type, 
                    dense_context,
                    pre_vec
                )
                entity_uuid_map[name] = resolved_uuid
            
            # 2. Ingest Edges — batch by relationship type using UNWIND
            resolved_edges = []
            for edge in graph_json.get("edges", []):
                s_id = entity_uuid_map.get(edge["source"])
                t_id = entity_uuid_map.get(edge["target"])
                
                if s_id and t_id:
                    resolved_edges.append({
                        "source_uuid": s_id,
                        "target_uuid": t_id,
                        "relationship": edge["relationship"],
                        "context": edge.get("context", ""),
                        "source_url": edge.get("source_url", "Unknown"),
                        "article_date": edge.get("article_date", "Unknown"),
                        "event_year": edge.get("event_year")
                    })
            
            if resolved_edges:
                self.batch_merge_edges(session, resolved_edges)

    def batch_merge_edges(self, session, resolved_edges):
        """
        Batch edge injection using Cypher UNWIND.
        Groups edges by relationship type and fires one UNWIND query per type.
        This replaces N individual session.run() calls with ~5 total calls.
        """
        from datetime import datetime as _dt
        import dateutil.parser as _dp
        
        # Group edges by relationship type (Cypher can't parameterize rel types)
        edges_by_type = {}
        for edge in resolved_edges:
            rel_type = edge["relationship"].replace(" ", "_").upper()
            if rel_type not in edges_by_type:
                edges_by_type[rel_type] = []
            
            # Resolve temporal year
            extracted_year = edge.get("event_year")
            rss_year = _dt.now().year
            try:
                article_date_str = edge.get("article_date", "Unknown")
                if article_date_str != "Unknown":
                    rss_year = _dp.parse(article_date_str, fuzzy=True).year
            except Exception:
                pass
            
            final_year = extracted_year if (extracted_year and isinstance(extracted_year, int)) else rss_year
            
            edges_by_type[rel_type].append({
                "source_id": edge["source_uuid"],
                "target_id": edge["target_uuid"],
                "context": edge.get("context", ""),
                "source_url": edge.get("source_url", "Unknown"),
                "edge_year": final_year
            })
        
        # Fire one UNWIND per relationship type
        for rel_type, batch in edges_by_type.items():
            query = (
                "UNWIND $batch AS rel "
                "MATCH (source:Entity {id: rel.source_id}) "
                "MATCH (target:Entity {id: rel.target_id}) "
                f"MERGE (source)-[r:{rel_type}]->(target) "
                "ON CREATE SET r.context = rel.context, r.extracted_at = datetime(), "
                "  r.source_url = rel.source_url, r.year = rel.edge_year "
                "ON MATCH SET r.context = rel.context, r.source_url = rel.source_url, "
                "  r.year = coalesce(rel.edge_year, r.year)"
            )
            session.run(query, batch=batch)

    @staticmethod
    def _merge_edge(tx, edge):
        rel_type = edge["relationship"].replace(" ", "_").upper() 
        
        # --- THE TEMPORAL FUSION ENGINE ---
        extracted_year = edge.get("event_year")
        rss_year = datetime.now().year
        try:
            article_date_str = edge.get("article_date", "Unknown")
            if article_date_str != "Unknown":
                rss_datetime = dateutil.parser.parse(article_date_str, fuzzy=True)
                rss_year = rss_datetime.year
        except Exception:
            pass
            
        if extracted_year and isinstance(extracted_year, int):
            final_edge_year = extracted_year
        else:
            final_edge_year = rss_year
            
        query = (
            "MATCH (source:Entity {id: $source_id}) "
            "MATCH (target:Entity {id: $target_id}) "
            f"MERGE (source)-[r:{rel_type}]->(target) "
            "ON CREATE SET r.context = $context, r.extracted_at = datetime(), r.source_url = $source_url, r.year = $edge_year "
            "ON MATCH SET r.context = $context, r.source_url = $source_url, r.year = coalesce($edge_year, r.year)"
        )
        tx.run(query, 
               source_id=edge["source_uuid"], 
               target_id=edge["target_uuid"], 
               context=edge.get("context", ""),
               source_url=edge.get("source_url", "Unknown"),
               edge_year=final_edge_year)

    def find_multi_hop_connection(self, source_name, target_name, max_hops=3):
        """
        Uncovers hidden OSINT links executing a variable depth path query.
        Updated to match on 'name' instead of UUID 'id'.
        """
        query = (
            f"MATCH path = (p:Entity)-[*1..{max_hops}]-(c:Entity) "
            "WHERE p.name = $source_name AND c.name = $target_name "
            "RETURN path"
        )
        with self.driver.session() as session:
            result = session.run(query, source_name=source_name, target_name=target_name)
            
            paths = []
            for record in result:
                path = record["path"]
                
                parsed_path = []
                for node in path.nodes:
                    parsed_path.append({
                        "id": node.get("name", node["id"]),
                        "type": node.get("type"),
                        "description": node.get("description")
                    })
                    
                relationships = []
                for rel in path.relationships:
                    relationships.append({
                        "type": rel.type,
                        "context": rel.get("context"),
                        "start_node": rel.start_node.get("name", rel.start_node["id"]),
                        "end_node": rel.end_node.get("name", rel.end_node["id"])
                    })
                    
                paths.append({
                    "nodes": parsed_path,
                    "edges": relationships
                })
                
            return paths
