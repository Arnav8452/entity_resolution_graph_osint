import time
import schedule
import os
import sys
import xml.etree.ElementTree as ET
from curl_cffi import requests

from src.config import NEO4J_URI, NEO4J_USER, NEO4J_PASS, OLLAMA_URL, OLLAMA_MODEL, ZERO_SHOT_THRESHOLD
from src.db_manager import get_db_connection, init_db
from src.knowledge_graph import KnowledgeGraphBackend
from src.polite_scraper import scrape_article, unroll_google_link
from src.ai_pipeline import filter_article, extract_knowledge_graph
from src.bing_scraper import fetch_bing_news
from src.reddit_scraper import fetch_reddit_hot
from watchdog import OSINTWatchdog, generate_intelligence_briefing, save_alert_to_sqlite

# Neo4j Graph DB Client Initialization
kg = KnowledgeGraphBackend(NEO4J_URI, NEO4J_USER, NEO4J_PASS)

# Data Sources
GOOGLE_FEEDS = [
    "https://news.google.com/rss/search?q=India%20%28politics%20OR%20crime%20OR%20law%29&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=India%20%28relations%20OR%20international%20OR%20war%29&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=India%20%28companies%20OR%20ventures%29&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=India%20%28OSINT%20OR%20intelligence%20OR%20defense%20OR%20security%29&hl=en-IN&gl=IN&ceid=IN:en"
]
BING_QUERIES = ["News"]
REDDIT_SUBS = ["news"]

# ──── SQLite Helper Functions (Connection Pooling) ────
def _is_already_processed(conn, item_id):
    """Check if an article has already been processed OR has permanently failed."""
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM Processed_Articles WHERE GUID = ?", (item_id,))
    if cursor.fetchone():
        return True
    cursor.execute("SELECT 1 FROM Failed_Articles WHERE GUID = ?", (item_id,))
    if cursor.fetchone():
        return True
    return False

def _mark_as_failed(conn, item_id, title, reason):
    """Log a permanently failed article so it's never retried."""
    try:
        conn.execute(
            "INSERT OR IGNORE INTO Failed_Articles (GUID, Title, Failure_Reason) VALUES (?, ?, ?)",
            (item_id, title, reason)
        )
        conn.commit()
        print(f"📝 Article '{title}' logged as FAILED: {reason}")
    except Exception as e:
        print(f"Failed to log failure: {e}")


def process_item_through_pipeline(item_id, text_chunks: list[str], real_publisher_url, title, article_date="Unknown Date"):
    """
    Standardizes the pipeline execution for AI logic and Database storage
    so all data sources can share it.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Prevent Processing Duplicates (also checks Failed_Articles table)
        if _is_already_processed(conn, item_id):
            return

        print(f"\nProcessing new item: {title}")
        
        # 1. AI Filter
        # Merge chunks up to 2000 chars strictly for the classification score
        combined_text = " ".join(text_chunks)[:2000]
        classification = filter_article(combined_text)
        
        top_category = classification['top_label']
        confidence = classification['score']
        print(f"Category: {top_category} (Score: {confidence:.2f})")
        
        # 2. Check the Air-Lock
        if top_category == "routine news, sports, or entertainment":
            print("🗑️ Junk data detected. Dropping article to save VRAM.")
            # Mark as processed so we don't re-classify every cycle
            cursor.execute("INSERT OR IGNORE INTO Processed_Articles (GUID) VALUES (?)", (item_id,))
            conn.commit()
            return
            
        # 3. Enforce a Confidence Threshold
        if confidence < ZERO_SHOT_THRESHOLD:
            print(f"⚠️ Low confidence ({confidence:.2f}) for category '{top_category}'. Dropping to keep graph clean.")
            cursor.execute("INSERT OR IGNORE INTO Processed_Articles (GUID) VALUES (?)", (item_id,))
            conn.commit()
            return
        
        # 4. AI Generation
        print("Starting Knowledge Graph extraction...")
        captions = extract_knowledge_graph(text_chunks, source_url=real_publisher_url, article_date=article_date)
        
        if not captions or (not captions.get('entities') and not captions.get('edges')):
            _mark_as_failed(conn, item_id, title, "Ollama returned empty graph after retries")
            return

        entities = captions.get('entities', [])
        edges = captions.get('edges', [])
        
        print(f"Extracted {len(entities)} nodes and {len(edges)} edges.")

        # 5. Database Save
        for entity in entities:
            try:
                keys = entity.get("disambiguation_keys", {})
                role = keys.get("role_or_title", "None")
                locs = ", ".join(keys.get("associated_locations", [])) if isinstance(keys.get("associated_locations"), list) else "None"
                orgs = ", ".join(keys.get("affiliated_organizations", [])) if isinstance(keys.get("affiliated_organizations"), list) else "None"
                
                condensed_description = f"Role: {role}. Locations: {locs}. Organizations: {orgs}."
                
                cursor.execute("INSERT OR IGNORE INTO Nodes (Node_ID, Type, Description) VALUES (?, ?, ?)", (entity.get('name'), entity.get('type'), condensed_description))
            except Exception as e:
                print(f"Node insert error: {e}")
                
        for edge in edges:
            try:
                cursor.execute('''
                    INSERT INTO Edges (Source_Node, Target_Node, Relationship, Context, Document_Source, Document_Date)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    edge.get('source'), edge.get('target'), edge.get('relationship'),
                    edge.get('context'), real_publisher_url, article_date
                ))
            except Exception as e:
                print(f"Edge insert error: {e}")
                
        # 6. Neo4j Advanced Intelligence DB Save
        print("Ingesting JSON into Neo4j Knowledge Graph...")
        try:
            kg.ingest_osint_data(captions)
        except Exception as e:
            print(f"Neo4j ingestion failed for item {item_id}: {e}")
        
        # Mark as permanently processed
        cursor.execute("INSERT INTO Processed_Articles (GUID) VALUES (?)", (item_id,))
        conn.commit()
        print("✅ Knowledge Graph successfully saved to DB.")
        
    except Exception as e:
        print(f"Pipeline error for {title}: {e}")
    finally:
        conn.close()

def process_google_feed(feed_url):
    print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Check Google News: {feed_url}")
    try:
        response = requests.get(feed_url, impersonate="chrome120", timeout=15, verify=False)
        if response.status_code != 200: return
        
        root = ET.fromstring(response.text)
        
        # Single connection for all dedup checks in this feed
        conn = get_db_connection()
        try:
            for item in root.findall('.//item'):
                guid = item.find('guid')
                link = item.find('link')
                title = item.find('title')
                pubDate = item.find('pubDate')
                
                item_id = guid.text if guid is not None else (link.text if link is not None else None)
                google_url = link.text if link is not None else None
                title_text = title.text if title is not None else "Unknown"
                pubDate_text = pubDate.text if pubDate is not None else "Unknown Date"
                
                if not item_id or not google_url: continue
                
                if _is_already_processed(conn, item_id):
                    continue
                
                real_url = unroll_google_link(google_url)
                text = scrape_article(real_url)
                
                if not text:
                    conn.execute("INSERT OR IGNORE INTO Processed_Articles (GUID) VALUES (?)", (item_id,))
                    conn.commit()
                    continue
                    
                process_item_through_pipeline(item_id, text, real_url, title_text, pubDate_text)
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Google parsing exception: {e}")

def process_bing_query(query):
    print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Check Bing News: {query}")
    articles = fetch_bing_news(query)
    
    # Single connection for all dedup checks
    conn = get_db_connection()
    try:
        for art in articles:
            if _is_already_processed(conn, art["guid"]):
                continue
            
            text = scrape_article(art["link"])
            if not text:
                conn.execute("INSERT OR IGNORE INTO Processed_Articles (GUID) VALUES (?)", (art["guid"],))
                conn.commit()
                continue
                
            process_item_through_pipeline(art["guid"], text, art["link"], art["title"], art.get("date", "Unknown Date"))
            time.sleep(2) # Polite stagger
    finally:
        conn.close()

def process_reddit_sub(sub):
    print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Check Reddit: r/{sub}")
    articles = fetch_reddit_hot(sub)
    for art in articles:
        process_item_through_pipeline(art["guid"], [art["text"]], art["source_url"], art["title"], art.get("date", "Unknown Date"))
        time.sleep(2)

def job():
    print("\n--- Starting Master Orchestration Loop ---")
    
    for url in GOOGLE_FEEDS:
        process_google_feed(url)
        time.sleep(3)
        
    for query in BING_QUERIES:
        process_bing_query(query)
        time.sleep(3)
        
    for sub in REDDIT_SUBS:
        process_reddit_sub(sub)
        time.sleep(3)
    
    # --- Integrated Tripwire Sweep ---
    print("\n--- Running Tripwire Daemon Sweep ---")
    try:
        watchdog = OSINTWatchdog(NEO4J_URI, NEO4J_USER, NEO4J_PASS)
        alerts = watchdog.run_all_tripwires()
        if alerts:
            for alert in alerts:
                target = alert["entity"]
                reason = alert["type"]
                print(f"[ALERT] TRIGGERED: {target} ({reason})")
                briefing = generate_intelligence_briefing(target)
                save_alert_to_sqlite(target, reason, briefing)
                print(f"[DB] Alert saved to SQLite Dashboard.")
        else:
            print("[OK] Graph is quiet. No anomalies detected.")
        watchdog.close()
    except Exception as e:
        print(f"Tripwire sweep failed: {e}")
        
    print("--- Loop Complete. Waiting 15 minutes. ---")

def force_unload_model():
    print("\n🛑 Ctrl+C detected! Sending emergency kill signal to Ollama...")
    try:
        # Tell Ollama to immediately dump the model from VRAM
        requests.post(f'{OLLAMA_URL}/api/generate', json={
            "model": OLLAMA_MODEL, 
            "keep_alive": 0 
        }, timeout=3, impersonate=None)
        print("✅ Ollama VRAM successfully flushed. Safe to exit.")
    except Exception as e:
        print(f"⚠️ Could not reach Ollama: {e}")

def main():
    print("Initializing Database...")
    init_db()
    job()
    schedule.every(15).minutes.do(job)
    print("\n🕒 Master scheduler is fully active across Google, Bing, and Reddit. Stop via Ctrl+C.")
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        force_unload_model()
        sys.exit(0)
