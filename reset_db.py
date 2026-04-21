import sqlite3
import os
from neo4j import GraphDatabase
from src.config import DB_PATH, NEO4J_URI, NEO4J_USER, NEO4J_PASS

def reset_databases():
    print("Resetting SQLite Database...")
    if os.path.exists(DB_PATH):
        try:
            # Let's try to just drop the tables so we don't hit file lock issues with Streamlit
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            tables = ['Nodes', 'Edges', 'Processed_Articles', 'Intelligence_Alerts', 'Failed_Articles', 'CSV_Ingestion_Progress']
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
            conn.close()
            print("Dropped pristine SQLite tables.")
        except Exception as e:
            print(f"SQLite Reset Error: {e}")
            
    # Re-initialize fresh
    from src.db_manager import init_db
    init_db()

    print("Resetting Neo4j Graph Database...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        driver.close()
        print("Neo4j database wiped clean.")
    except Exception as e:
        print(f"Failed to connect to Neo4j: {e}")

if __name__ == "__main__":
    reset_databases()
