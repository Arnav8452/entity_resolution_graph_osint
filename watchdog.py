import sqlite3
import requests
from neo4j import GraphDatabase
from src.config import NEO4J_URI, NEO4J_USER, NEO4J_PASS, OLLAMA_URL, OLLAMA_MODEL, OLLAMA_NUM_CTX, DB_PATH

class OSINTWatchdog:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_all_tripwires(self):
        queries = {
            "Tripwire A: Smoke Alarm": """
                MATCH (target:Entity)-[r]->(destination:Entity)
                WHERE type(r) IN ['INVESTIGATED_BY', 'ACCUSED_OF', 'RAIDED_BY', 'ARRESTED_BY']
                AND r.extracted_at > datetime() - duration('PT2H')
                RETURN DISTINCT target.name AS suspicious_entity, 'High-Risk Relationship: ' + type(r) AS trigger_type
            """,
            "Tripwire D: Circular Ownership": """
                MATCH path = (target:Entity {type: 'ORGANIZATION'})-[*2..5]->(target)
                WHERE ANY(r IN relationships(path) WHERE r.extracted_at > datetime() - duration('P1D'))
                AND ALL(r IN relationships(path) WHERE type(r) IN ['OWNS', 'ACQUIRED', 'SUBSIDIARY_OF', 'FUNDED_BY', 'AFFILIATED_WITH'])
                RETURN DISTINCT target.name AS suspicious_entity, 'Circular Ownership Loop Detected' AS trigger_type
            """,
            "Tripwire E: VIP Bridge": """
                MATCH (vip1:Entity {type: 'PERSON'}), (vip2:Entity {type: 'ORGANIZATION'})
                WHERE apoc.node.degree(vip1) > 5 AND apoc.node.degree(vip2) > 5
                MATCH path = shortestPath((vip1)-[*2..3]-(vip2))
                WHERE ANY(r IN relationships(path) WHERE r.extracted_at > datetime() - duration('P1D'))
                WITH [n IN nodes(path) WHERE n.id <> vip1.id AND n.id <> vip2.id | n.name] AS middlemen
                WHERE size(middlemen) > 0
                RETURN middlemen[0] AS suspicious_entity, 'VIP Bridge (Middleman) Detected' AS trigger_type
            """
        }

        alerts = []
        with self.driver.session() as session:
            for name, query in queries.items():
                print(f"Executing {name}...")
                try:
                    result = session.run(query)
                    for record in result:
                        alerts.append({
                            "entity": record["suspicious_entity"], 
                            "type": record["trigger_type"]
                        })
                except Exception as e:
                    print(f"Error running {name}: {repr(e)}")
        return alerts

def generate_intelligence_briefing(target_entity):
    """Uses local Qwen model to write the alert, with retry/backoff via shared wrapper."""
    from src.ai_pipeline import ollama_request_with_retry
    
    prompt = f"You are an OSINT analyst. Write a highly concise, 3-sentence urgent intelligence briefing about a newly detected anomaly involving: {target_entity}. State that manual review is required."
    
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.2, "num_ctx": OLLAMA_NUM_CTX}
    }
    data = ollama_request_with_retry("/api/generate", payload, timeout=60)
    if data:
        return data.get('response', "Briefing generation failed.")
    return f"System Alert: Ollama API unreachable after retries. Target entity '{target_entity}' requires manual review."

def save_alert_to_sqlite(entity, trigger_type, briefing):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Intelligence_Alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            target_entity TEXT,
            trigger_type TEXT,
            briefing_text TEXT,
            status TEXT DEFAULT 'UNREAD'
        )
    ''')
    cursor.execute('''
        INSERT INTO Intelligence_Alerts (target_entity, trigger_type, briefing_text)
        VALUES (?, ?, ?)
    ''', (entity, trigger_type, briefing))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    print("[LOG] Waking up OSINT Watchdog...")
    watchdog = OSINTWatchdog(NEO4J_URI, NEO4J_USER, NEO4J_PASS)
    
    triggered_alerts = watchdog.run_all_tripwires()
    
    if not triggered_alerts:
        print("[SUCCESS] Graph is quiet. No anomalies detected.")
    else:
        for alert in triggered_alerts:
            target = alert["entity"]
            reason = alert["type"]
            print(f"[ALERT] TRIGGERED: {target} ({reason})")
            
            print(f"[AI] Generating intelligence briefing for {target}...")
            briefing = generate_intelligence_briefing(target) 
            
            save_alert_to_sqlite(target, reason, briefing)
            print(f"[DB] Alert saved to SQLite Dashboard.")
                
    watchdog.close()
    print("[LOG] Watchdog going back to sleep.")
