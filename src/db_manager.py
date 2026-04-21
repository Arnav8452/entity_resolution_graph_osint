import sqlite3
import os
from src.config import BASE_DIR, DB_PATH

def get_db_connection():
    """Returns a connection to the SQLite tracking database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the SQLite database and creates the necessary tables."""
    # Ensure standard directory context
    os.makedirs(BASE_DIR, exist_ok=True)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create Target_Sites table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Target_Sites (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            URL TEXT UNIQUE NOT NULL,
            Last_ETag TEXT,
            Last_Modified TEXT
        )
    ''')
    
    # Remove old Drafts table if it exists
    cursor.execute('DROP TABLE IF EXISTS Drafts')
    
    # Create Nodes table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Nodes (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Node_ID TEXT UNIQUE NOT NULL,
            Type TEXT,
            Description TEXT
        )
    ''')
    
    # Create Edges table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Edges (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Source_Node TEXT NOT NULL,
            Target_Node TEXT NOT NULL,
            Relationship TEXT NOT NULL,
            Context TEXT,
            Document_Source TEXT,
            Document_Date TEXT
        )
    ''')

    # Create Processed_Articles table to track Google News RSS items
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Processed_Articles (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            GUID TEXT UNIQUE NOT NULL,
            Processed_At DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create Failed_Articles table to prevent infinite retry loops
    # Articles here exhausted all Ollama retries and won't be re-attempted
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Failed_Articles (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            GUID TEXT UNIQUE NOT NULL,
            Title TEXT,
            Failure_Reason TEXT,
            Failed_At DATETIME DEFAULT CURRENT_TIMESTAMP,
            Retry_Count INTEGER DEFAULT 0
        )
    ''')
    
    # Create CSV_Ingestion_Progress table for pause/resume tracking
    cursor.execute('''
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
    conn.close()
    
if __name__ == "__main__":
    init_db()
    print(f"Database initialized successfully at {DB_PATH}")
