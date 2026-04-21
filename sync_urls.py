import sqlite3
from neo4j import GraphDatabase
from src.config import DB_PATH, NEO4J_URI, NEO4J_USER, NEO4J_PASS

def sync_urls_to_neo4j():
    print("Initiating Database Sync: SQLite -> Neo4j...")
    
    # 1. Pull all legacy edges from the flat SQLite cache
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT Source_Node, Target_Node, Relationship, Context, Document_Source FROM Edges WHERE Document_Source IS NOT NULL")
    rows = cursor.fetchall()
    print(f"Found {len(rows)} edges with URLs in SQLite.")
    
    # 2. Connect to the advanced Graph Backend
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    
    updated_count = 0
    with driver.session() as session:
        for source, target, rel, context, doc_source in rows:
            # Reconstruct the Neo4j Relationship type formatting
            rel_type = rel.replace(" ", "_").upper()
            
            # Execute surgical SET query strictly updating edges missing URLs
            query = f"""
                MATCH (s:Entity {{id: $source_id}})-[r:{rel_type}]->(t:Entity {{id: $target_id}})
                WHERE r.context = $context AND (r.source_url IS NULL OR r.source_url = 'Unknown')
                SET r.source_url = $source_url
                RETURN r
            """
            result = session.run(query, source_id=source, target_id=target, context=context, source_url=doc_source)
            if result.peek() is not None:
                updated_count += len(list(result))
                
    driver.close()
    conn.close()
    print(f"✅ Successfully backported {updated_count} URLs across legacy Neo4j structures.")

if __name__ == "__main__":
    sync_urls_to_neo4j()
