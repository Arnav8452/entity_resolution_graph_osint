import streamlit as st
import pandas as pd
import sqlite3
import os
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from src.config import DB_PATH, NEO4J_URI, NEO4J_USER, NEO4J_PASS

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# --- 1. Alert Center Component ---
def render_alert_center():
    conn = sqlite3.connect(DB_PATH)
    
    # Check if table exists first to prevent crashes on first run
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Intelligence_Alerts'")
    if not cursor.fetchone():
        st.success("System Active: Zero target anomalies detected in global network.")
        conn.close()
        st.markdown("---")
        return

    # Fetch Unread Alerts
    alerts_df = pd.read_sql_query("SELECT * FROM Intelligence_Alerts WHERE status = 'UNREAD' ORDER BY timestamp DESC", conn)
    
    if not alerts_df.empty:
        st.error(f"Intelligence Warning: {len(alerts_df)} New Tripwires Triggered")
        
        for index, row in alerts_df.iterrows():
            with st.expander(f"Alert: {row['trigger_type']} | Target: {row['target_entity']}"):
                st.caption(f"Detected at: {row['timestamp']}")
                st.markdown("### AI Analyst Briefing")
                st.write(row['briefing_text'])
                
                # Mark as Read Button
                if st.button("Mark as Reviewed", key=f"btn_read_{row['id']}"):
                    cursor.execute("UPDATE Intelligence_Alerts SET status = 'READ' WHERE id = ?", (row['id'],))
                    conn.commit()
                    st.rerun() # Refresh the UI instantly
    else:
        st.success("System Active: Zero unread anomalies detected.")
    
    conn.close()
    st.markdown("---")

# --- 2. Main Dashboard Layout ---
st.set_page_config(page_title="OSINT Fusion Engine", layout="wide", page_icon="📡")

# --- Control Panel Sidebar ---
st.sidebar.title("Control Panel")
st.sidebar.markdown("Manually trigger intelligence systems.")

if st.sidebar.button("Execute Tripwire Daemon", width="stretch"):
    with st.spinner("Scanning Neo4j graph for anomalies..."):
        subprocess.run(["python", "watchdog.py"], cwd=BASE_DIR)
        st.sidebar.success("Daemon sync complete!")
        st.rerun()

# Push button to the bottom
st.sidebar.markdown("<div style='height: 40vh;'></div>", unsafe_allow_html=True)
if st.sidebar.button("Wipe Database"):
    with st.spinner("Deleting SQLite Cache and Neo4j Graph..."):
        subprocess.run(["python", "reset_db.py"], cwd=BASE_DIR)
        st.sidebar.success("Database wiped.")
        st.rerun()


st.title("OSINT Fusion Engine")
st.markdown("Active Network Analysis Station. Review extracted Nodes, anomalous Tripwires, and Edges from the global pipeline.")

# Render the alerts at the top
render_alert_center()

from streamlit_agraph import agraph, Node, Edge, Config
from ingest_csv import build_caveman_json, mirror_to_sqlite, ingest_csv_fast
from ingest_csv import build_caveman_json, mirror_to_sqlite, ingest_csv_fast
import textwrap
import time as _time

# --- Ontology Color Mapping ---
COLOR_MAP = {
    "PERSON": "#3182bd",            # Blue
    "ORGANIZATION": "#e6550d",      # Orange
    "GOVERNMENT_BODY": "#31a354",   # Green
    "LOCATION": "#756bb1",          # Purple
    "POLITICAL_PARTY": "#de2d26",   # Red
    "LAW_STATUTE": "#8c6d31",       # Brown
    "LEGAL_CASE": "#843c39",        # Dark Red
    "EVENT": "#d6616b",             # Light Red
    "CONCEPT": "#bdbdbd",           # Light Gray
    "PRODUCT": "#636363",           # Dark Gray
    "FINANCIAL_INSTRUMENT": "#e7ba52" # Yellow
}

@st.cache_data(ttl=300)
def get_temporal_bounds():
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    query = """
    MATCH ()-[r]->() 
    WHERE r.start_year IS NOT NULL 
    RETURN min(r.start_year) as min_y, max(r.start_year) as max_y
    """
    with driver.session() as session:
        res = session.run(query).single()
        driver.close()
        if res and res["min_y"]:
            return (res["min_y"], res["max_y"])
        return (1950, 2026)

def fetch_visual_subgraph(entity_name, limit=50, start_year=1950, end_year=2026, rel_filter=None, hops=2):
    """Fetches a 1 or 2-hop neighborhood from Neo4j and formats it for Agraph.
    
    Args:
        rel_filter: Optional list of relationship type strings to include.
                    If None or empty, all relationship types are shown.
        hops: Number of hops to traverse (1 or 2).
    """
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    
    # Build dynamic relationship type filter for targeted queries
    if rel_filter:
        # Cypher syntax: [:TYPE1|TYPE2*1..hops]
        rel_types = "|".join(rel_filter)
        rel_pattern = f"[:{rel_types}*1..{hops}]"
        # For 1-hop targeted mode (more useful than hairball 2-hop with filters)
        query = f"""
            MATCH path = (target:Entity {{name: $entity_name}})-{rel_pattern}-(connected:Entity)
            WHERE ALL(rel IN relationships(path) WHERE rel.start_year IS NULL OR (rel.start_year >= $start_year AND rel.start_year <= $end_year))
            UNWIND relationships(path) AS r
            WITH DISTINCT r, startNode(r) AS n1, endNode(r) AS n2
            RETURN n1.name AS source_id, n1.type AS source_type, 
                   type(r) AS relationship, r.context AS context, 
                   n2.name AS target_id, n2.type AS target_type
            LIMIT $limit
        """
    else:
        query = f"""
            MATCH path = (target:Entity {{name: $entity_name}})-[*1..{hops}]-(connected:Entity)
            WHERE ALL(rel IN relationships(path) WHERE rel.start_year IS NULL OR (rel.start_year >= $start_year AND rel.start_year <= $end_year))
            UNWIND relationships(path) AS r
            WITH DISTINCT r, startNode(r) AS n1, endNode(r) AS n2
            RETURN n1.name AS source_id, n1.type AS source_type, 
                   type(r) AS relationship, r.context AS context, 
                   n2.name AS target_id, n2.type AS target_type
            LIMIT $limit
        """
    
    nodes_dict = {}
    edges_list = []
    
    with driver.session() as session:
        result = session.run(query, entity_name=entity_name, limit=limit, start_year=start_year, end_year=end_year)
        
        for record in result:
            s_id = record["source_id"]
            t_id = record["target_id"]
            
            if s_id not in nodes_dict:
                color = COLOR_MAP.get(record["source_type"], "#969696")
                size = 25 if s_id == entity_name else 15
                nodes_dict[s_id] = Node(
                    id=s_id, 
                    label=s_id, 
                    size=size, 
                    color=color,
                    title=f"[ENTITY] Type: {record['source_type']}"
                )
                
            if t_id not in nodes_dict:
                color = COLOR_MAP.get(record["target_type"], "#969696")
                size = 25 if t_id == entity_name else 15
                nodes_dict[t_id] = Node(
                    id=t_id, 
                    label=t_id, 
                    size=size, 
                    color=color,
                    title=f"[ENTITY] Type: {record['target_type']}"
                )
                
            edges_list.append(
                Edge(source=s_id, 
                     target=t_id, 
                     label=record['relationship'], 
                     title=record["context"],
                     font={
                         'color': '#a3a3a3', 
                         'strokeWidth': 0, 
                         'size': 8, 
                         'align': 'top'
                     }
                )
            )
            
    driver.close()
    return list(nodes_dict.values()), edges_list


def fetch_relationship_types(entity_name):
    """Fetches all relationship types connected to a given entity."""
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    query = """
        MATCH (target:Entity {name: $entity_name})-[r]-()
        RETURN DISTINCT type(r) AS rel_type, count(r) AS count
        ORDER BY count DESC
    """
    types = []
    with driver.session() as session:
        result = session.run(query, entity_name=entity_name)
        for record in result:
            types.append(f"{record['rel_type']} ({record['count']})")
    driver.close()
    return types

# Create the Tabs
tab1, tab2, tab3, tab4 = st.tabs(["Raw Intelligence Cache", "Live Intelligence Briefings", "Visual Link Analysis", "CSV Ingestor"])

# --- TAB 1: The Flat Data (Your existing SQLite tables) ---
with tab1:
    # Connect to SQLite for fast rendering
    conn = get_db_connection()
    try:
        raw_tab1, raw_tab2 = st.tabs(["Extracted Entities", "Extracted Relationships"])
        
        with raw_tab1:
            st.header("Latest Extracted Entities")
            nodes = conn.execute("SELECT Node_ID, Type, Description FROM Nodes ORDER BY id DESC").fetchall()
            if nodes:
                st.dataframe([dict(n) for n in nodes], width="stretch")
            else:
                st.info("No nodes found.")

        with raw_tab2:
            st.header("Latest Extracted Relationships")
            edges = conn.execute("SELECT Source_Node, Target_Node, Relationship, Context, Document_Source, Document_Date FROM Edges ORDER BY id DESC").fetchall()
            if edges:
                st.dataframe([dict(e) for e in edges], width="stretch")
            else:
                st.info("No edges found.")
            
    except Exception as e:
        st.warning("SQLite database is currently empty or still building.")
    finally:
        conn.close()

# --- TAB 2: The Deep Dive (Neo4j + Qwen) ---
with tab2:
    st.header("Generate Target Dossier")
    st.markdown("Query the Neo4j Knowledge Graph directly to generate live executive intelligence summaries.")
    
    # 1. Fetch available entities for autocomplete
    entity_list = []
    try:
        conn = get_db_connection()
        entity_list = pd.read_sql_query("SELECT Node_ID FROM Nodes", conn)['Node_ID'].tolist()
    except Exception:
        pass
    finally:
        conn.close()

    # 2. Analyst Search UI (Autocomplete enabled)
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        selected_entity = st.selectbox(
            "Enter Target Entity for Deep Dive:", 
            options=entity_list,
            index=None,
            placeholder="Type to search... (e.g. Google, Microsoft)"
        )
    with col2:
        dossier_hops = st.selectbox("Network Depth:", options=[1, 2], index=1, help="1-hop is faster and hyper-focused. 2-hops reveals extended associates.")
    with col3:
        st.write("") # Spacing
        st.write("")
        generate_button = st.button("Generate Live Dossier", type="primary", use_container_width=True)

    # 3. The Generation Trigger
    if generate_button and selected_entity:
        with st.spinner(f"Initiating Neo4j graph traversal for '{selected_entity}'... Spinning up Qwen..."):
            from src.ai_pipeline import generate_live_briefing
            briefing_result = generate_live_briefing(selected_entity, hops=dossier_hops)
            
            st.success("Graph Traversal & Analysis Complete.")
            
            # Display the Dossier in a nice formatted box
            with st.container(border=True):
                st.markdown(f"### Target Dossier: {selected_entity.upper()}")
                st.write(briefing_result)
    elif generate_button:
        st.warning("Please select a valid entity autocomplete target.")

# --- TAB 3: Interactive Graph Visualization ---
with tab3:
    st.header("Visual Link Analysis")
    st.markdown("Explore the topological neighborhood of any target. Use **Relationship Filter** for surgical queries.")
    
    if 'entity_list' in locals() and entity_list:
        # Analyst Tools panel
        with st.expander("Graph Configuration Pipeline", expanded=True):
            cfg_col1, cfg_col2, cfg_col3, cfg_col4, cfg_col5 = st.columns(5)
            with cfg_col1:
                query_limit = st.slider("Topology Mapping Edge Limit:", min_value=10, max_value=500, value=50, step=10)
            with cfg_col2:
                min_y, max_y = get_temporal_bounds()
                time_range = st.slider("Select Analysis Window:", min_value=min_y, max_value=max_y, value=(min_y, max_y), help="Shows entities linked within this explicit era.")
            with cfg_col3:
                num_hops = st.selectbox("Number of Hops:", options=[1, 2], index=1, help="Choose whether to explore 1 layer deep or 2 layers deep.")
            with cfg_col4:
                enable_physics = st.checkbox("Enable Physics Forces", value=True, help="Disable to freeze nodes so you can manually arrange them without rubber-banding.")
            with cfg_col5:
                enable_hierarchy = st.checkbox("Hierarchical Layout", value=False, help="Overrides physics cluster and enforces top-down structural trees.")

        col1, col2 = st.columns([3, 1])
        with col1:
            viz_target = st.selectbox(
                "Select Node to Visualize:", 
                options=entity_list, 
                index=None, 
                placeholder="Type to search... (e.g., M/o Commerce & Industry)", 
                key="viz_select"
            )
        with col2:
            st.write("")
            st.write("")
            render_button = st.button("Render Canvas", type="primary", use_container_width=True)
        
        # ──── Targeted Query: Relationship Filter ────
        rel_filter_selection = []
        if viz_target:
            available_rels = fetch_relationship_types(viz_target)
            if available_rels:
                st.caption("**Targeted Query Mode** — Filter by specific relationship types to cut through dense graphs. "
                           "Leave empty to show all connections (2-hop neighborhood).")
                rel_filter_selection = st.multiselect(
                    "Filter by Relationship Type:",
                    options=available_rels,
                    default=[],
                    key="rel_filter",
                    help="Select specific edge types to see only those connections. Shows count of connections per type."
                )
            
        if render_button and viz_target:
            with st.spinner("Compiling physics engine and fetching topology..."):
                # Parse selected relationship types (strip the count suffix like "(120)")
                rel_types_clean = None
                if rel_filter_selection:
                    rel_types_clean = [r.split(" (")[0] for r in rel_filter_selection]
                
                visual_nodes, visual_edges = fetch_visual_subgraph(
                    viz_target, limit=query_limit, start_year=time_range[0], end_year=time_range[1], rel_filter=rel_types_clean, hops=num_hops
                )
                
                if not visual_nodes:
                    st.warning("No connections found for this entity.")
                else:
                    actual_physics = {
                        "barnesHut": {
                            "gravitationalConstant": -30000,
                            "centralGravity": 0.1,
                            "springLength": 500,
                            "springConstant": 0.04,
                            "damping": 0.09
                        },
                        "minVelocity": 0.75
                    } if enable_physics else False
                    
                    config = Config(
                        width=1200,
                        height=1000,
                        directed=True, 
                        physics=actual_physics, 
                        hierarchical=enable_hierarchy,
                        nodeHighlightBehavior=True,
                        highlightColor="#00ff00",
                        collapsible=False,
                        # Inject raw Vis.js Gotham Overrides
                        **{
                            "nodes": {
                                "shape": "dot",
                                "borderWidth": 2,
                                "borderWidthSelected": 4,
                                "shadow": {
                                    "enabled": True,
                                    "color": "rgba(0, 255, 0, 0.4)",
                                    "size": 15,
                                    "x": 0,
                                    "y": 0
                                },
                                "font": {
                                    "color": "white",
                                    "size": 12,
                                    "face": "Courier New",
                                    "background": "rgba(0,0,0,0.6)"
                                }
                            },
                            "edges": {
                                "width": 1,
                                "color": {
                                    "color": "#4a4a4a",
                                    "highlight": "#00ff00",
                                    "hover": "#00ff00"
                                },
                                "smooth": {
                                    "type": "curvedCW",
                                    "roundness": 0.2
                                },
                                "shadow": {"enabled": True, "color": "rgba(0,0,0,0.8)"}
                            },
                            "interaction": {
                                "hover": True,
                                "tooltipDelay": 250
                            }
                        }
                    )
                    
                    st.success(f"Rendered {len(visual_nodes)} Nodes and {len(visual_edges)} Edges.")
                    with st.container(border=True):
                        return_value = agraph(nodes=visual_nodes, edges=visual_edges, config=config)
        elif render_button:
            st.warning("Please select a valid entity autocomplete target.")
    else:
        st.info("Awaiting entity extraction from the scraping pipeline...")

# --- TAB 4: CSV Ingestor (Cold Start Engine) ---
ENTITY_TYPES = [
    "PERSON", "ORGANIZATION", "LOCATION", "POLITICAL_PARTY", "GOVERNMENT_BODY",
    "CONCEPT", "EVENT", "PRODUCT", "LAW_STATUTE", "LEGAL_CASE", "FINANCIAL_INSTRUMENT"
]

with tab4:
    st.header("CSV Cold Start Ingestor")
    st.markdown("Upload a structured CSV to inject entities directly into the Knowledge Graph **without LLM compute**. "
                "Map your columns to entity types and relationships using the controls below.")
    
    uploaded_file = st.file_uploader("Upload CSV File", type=["csv"], key="csv_upload")
    
    if uploaded_file is not None:
        # ──── Data Preview ────
        try:
            df = pd.read_csv(uploaded_file).fillna("")
            uploaded_file.seek(0)  # Reset for potential re-read
        except Exception as e:
            st.error(f"Failed to read CSV: {e}")
            st.stop()
        
        columns = list(df.columns)
        
        st.success(f"Loaded **{len(df)} rows** and **{len(columns)} columns**")
        
        with st.expander("Preview Data (first 5 rows)", expanded=True):
            st.dataframe(df.head(), use_container_width=True)
        
        st.markdown("---")
        
        # ══════════════════════════════════════════════
        # STEP 1: Primary Entity Configuration
        # ══════════════════════════════════════════════
        st.subheader("Step 1: Primary Entity")
        st.caption("Select the column that contains the main entity name (e.g., company name, person name).")
        
        pe_col1, pe_col2 = st.columns(2)
        with pe_col1:
            primary_name_col = st.selectbox("Name Column", options=columns, key="primary_name_col")
        with pe_col2:
            primary_type = st.selectbox("Entity Type", options=ENTITY_TYPES, key="primary_type")
        
        st.markdown("---")
        
        # ══════════════════════════════════════════════
        # STEP 2: Disambiguation Key Mapping (Optional)
        # ══════════════════════════════════════════════
        st.subheader("Step 2: Disambiguation Keys")
        st.caption("Map columns to semantic context fields. These help the Entity Resolution Gauntlet "
                   "distinguish between entities with similar names (e.g., \"John Smith\" the CEO vs the politician). "
                   "Leave as \"— None —\" to skip.")
        
        col_options_with_none = ["— None —"] + columns
        
        dk_col1, dk_col2, dk_col3 = st.columns(3)
        with dk_col1:
            role_col = st.selectbox("Role / Title Column", options=col_options_with_none, key="role_col")
        with dk_col2:
            loc_cols = st.multiselect("Location Column(s)", options=columns, key="loc_cols")
        with dk_col3:
            org_cols = st.multiselect("Organization Column(s)", options=columns, key="org_cols")
        
        st.markdown("---")
        
        # ══════════════════════════════════════════════
        # STEP 3: Relationship Builder (Dynamic Rows)
        # ══════════════════════════════════════════════
        st.subheader("Step 3: Relationships")
        st.caption("Define which columns should become linked entities with edges. "
                   "Each relationship creates a separate node connected to the primary entity.")
        
        # Session state for dynamic relationship rows
        if "csv_rel_count" not in st.session_state:
            st.session_state.csv_rel_count = 1
        
        rel_add_col1, rel_add_col2 = st.columns([1, 5])
        with rel_add_col1:
            if st.button("＋ Add Relationship", key="add_rel"):
                st.session_state.csv_rel_count += 1
                st.rerun()
        with rel_add_col2:
            if st.session_state.csv_rel_count > 1:
                if st.button("－ Remove Last", key="remove_rel"):
                    st.session_state.csv_rel_count -= 1
                    st.rerun()
        
        relationships_config = []
        for i in range(st.session_state.csv_rel_count):
            with st.container(border=True):
                rc1, rc2, rc3 = st.columns(3)
                with rc1:
                    rel_target_col = st.selectbox(
                        f"Target Column", options=columns, key=f"rel_target_{i}"
                    )
                with rc2:
                    rel_target_type = st.selectbox(
                        f"Target Entity Type", options=ENTITY_TYPES, key=f"rel_type_{i}"
                    )
                with rc3:
                    rel_edge_name = st.text_input(
                        f"Edge Label", value="RELATED_TO", key=f"rel_edge_{i}",
                        help="e.g., MEMBER_OF, HEADQUARTERED_IN, BELONGS_TO_SECTOR"
                    ).strip().upper().replace(" ", "_")
                
                relationships_config.append({
                    "target_column": rel_target_col,
                    "target_type": rel_target_type,
                    "edge_name": rel_edge_name if rel_edge_name else "RELATED_TO"
                })
        
        st.markdown("---")
        
        # ══════════════════════════════════════════════
        # STEP 4: Ingestion Trigger
        # ══════════════════════════════════════════════
        # Build the config dict from UI state (same schema as JSON config files)
        ui_config = {
            "file_path": uploaded_file.name,
            "primary_entity": {
                "name_column": primary_name_col,
                "type": primary_type
            },
            "disambiguation_mapping": {},
            "relationships": relationships_config
        }
        
        # Pack disambiguation mapping
        if role_col != "— None —":
            ui_config["disambiguation_mapping"]["role_or_title"] = role_col
        if loc_cols:
            ui_config["disambiguation_mapping"]["associated_locations"] = loc_cols
        if org_cols:
            ui_config["disambiguation_mapping"]["affiliated_organizations"] = org_cols
        
        # Show the generated config — EDITABLE for power users
        import json as _json
        with st.expander("Generated Config (JSON) — Editable", expanded=False):
            st.caption("The dropdowns above auto-generate this config. You can also edit it directly here "
                       "to add advanced options like `\"multi_value\": true` on relationships, "
                       "tweak edge names, or modify disambiguation mappings. "
                       "**Edits here override the dropdowns.**")
            edited_json_str = st.text_area(
                "Config JSON",
                value=_json.dumps(ui_config, indent=2),
                height=300,
                key="config_editor",
                label_visibility="collapsed"
            )
        
        st.markdown("")
        fast_col1, fast_col2 = st.columns([1, 3])
        with fast_col1:
            fast_mode = st.checkbox(
                "⚡ Fast Mode", value=True, key="fast_mode",
                help="Skip entity resolution gauntlet and use direct UNWIND bulk injection. "
                     "~100x faster for structured CSVs. Uncheck for messy/mixed data that needs fuzzy matching."
            )
        with fast_col2:
            if fast_mode:
                st.caption("🟢 **UNWIND Bulk** — Exact name+type dedup. Best for clean structured CSVs (government databases, Nifty 500).")
            else:
                st.caption("🟡 **Gauntlet Mode** — Fuzzy Jaro-Winkler + vector resolution. Best for messy data with aliases. Much slower.")
        
        ingest_button = st.button(
            "🚀 Ingest CSV into Knowledge Graph", 
            type="primary", 
            use_container_width=True,
            key="ingest_btn"
        )
        
        if ingest_button:
            # ──── Parse the (potentially edited) config ────
            try:
                final_config = _json.loads(edited_json_str)
            except _json.JSONDecodeError as je:
                st.error(f"Invalid JSON in config editor: {je}")
                st.stop()
            
            # Validate required fields
            if "primary_entity" not in final_config or "name_column" not in final_config["primary_entity"]:
                st.error("Config must contain `primary_entity.name_column`.")
                st.stop()
            
            # ──── Execute Fast 3-Phase Engine ────
            progress_bar = st.progress(0, text="Phase 1/3: Preparing data...")
            phase_status = st.empty()
            
            try:
                def ui_progress(current, total, name):
                    pct = min(current / max(total, 1), 1.0)
                    progress_bar.progress(pct, text=f"{name[:60]}...")
                
                phase_status.info("🚀 Running fast 3-phase pipeline: Prepare → GPU Vectorize → Resolve + Inject")
                
                stats = ingest_csv_fast(
                    config=final_config,
                    df=df,
                    progress_callback=ui_progress,
                    skip_gauntlet=fast_mode
                )
                
                progress_bar.progress(1.0, text="Ingestion complete!")
                phase_status.empty()
                
                # ──── Summary ────
                col_s1, col_s2, col_s3, col_s4 = st.columns(4)
                col_s1.metric("✅ Ingested", f"{stats['success']} rows")
                col_s2.metric("⏭️ Skipped", f"{stats['skip']} rows")
                col_s3.metric("❌ Errors", f"{stats['error']} rows")
                col_s4.metric("⏩ Resumed", f"{stats['resumed']} rows")
                
                if stats['success'] > 0:
                    st.success(f"Successfully injected {stats['success']} rows through the fast GPU pipeline. "
                              f"Switch to **Raw Intelligence Cache** or **Visual Link Analysis** to explore.")
                    st.balloons()
                    
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                st.error(f"Ingestion failed: {e}\n\nTraceback:\n{error_trace}")
                print(error_trace)
    else:
        st.info("Upload a CSV file to begin. The ingestor will guide you through column mapping step by step.")
