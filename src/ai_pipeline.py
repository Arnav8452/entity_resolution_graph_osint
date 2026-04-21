import json
import time
import requests
from src.config import OLLAMA_URL, OLLAMA_MODEL, OLLAMA_NUM_CTX, OLLAMA_KEEP_ALIVE, NEO4J_URI, NEO4J_USER, NEO4J_PASS

# ──── Retry Configuration ────
MAX_RETRIES = 3
BASE_DELAY = 2  # seconds, doubles each retry


def ollama_request_with_retry(endpoint: str, payload: dict, timeout: int = 120) -> dict | None:
    """
    Wraps Ollama API calls with exponential backoff.
    Returns the parsed JSON response on success, None on exhausted retries.
    """
    url = f"{OLLAMA_URL}{endpoint}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, json=payload, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️ Ollama returned {response.status_code} (attempt {attempt}/{MAX_RETRIES})")
        except requests.exceptions.Timeout:
            print(f"⏱️ Ollama timeout (attempt {attempt}/{MAX_RETRIES})")
        except Exception as e:
            print(f"❌ Ollama error: {e} (attempt {attempt}/{MAX_RETRIES})")
        
        if attempt < MAX_RETRIES:
            delay = BASE_DELAY * (2 ** (attempt - 1))
            print(f"   Retrying in {delay}s...")
            time.sleep(delay)
    
    print(f"🛑 Ollama API exhausted all {MAX_RETRIES} retries.")
    return None

SYSTEM_PROMPT = """You are a strict OSINT data extraction engine. You do not write text. You only output valid JSON.

Your objective is to extract entities and their relationships from the provided text.
CRITICAL: For every entity, you MUST extract 'disambiguation_keys'. These are the specific roles, locations, and organizations associated with that entity in the text. If not mentioned, use "None".
CRITICAL: For every edge, you MUST extract the 'event_year'. If the text mentions a specific historical year (e.g., 1947, 1556), extract it as an integer. If the event is current or no year is mentioned, use null.

You MUST strictly follow this JSON schema:
{
  "entities": [
    {
      "name": "Narendra Modi",
      "type": "PERSON",
      "disambiguation_keys": {
        "role_or_title": "Prime Minister",
        "associated_locations": ["India", "Kerala"],
        "affiliated_organizations": ["BJP", "Union Government"]
      }
    }
  ],
  "edges": [
    {
      "source": "Akbar the Great",
      "relationship": "RULED_OVER",
      "target": "India",
      "context": "The reign of Akbar the Great was marked by expansion.",
      "event_year": 1556
    },
    {
      "source": "Narendra Modi",
      "relationship": "RELATED_TO",
      "target": "Kerala",
      "context": "He asserted that the NDA would form the next government in Kerala.",
      "event_year": null
    }
  ]
}
"""

# Lazy load classifier to save memory
_classifier = None

def get_classifier():
    global _classifier
    if _classifier is None:
        print("Loading Zero-Shot classification model...")
        from transformers import pipeline
        _classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
    return _classifier

def filter_article(text: str, candidate_labels: list = None) -> dict:
    """
    Zero-Shot classification using Transformers.
    Checks if an article is relevant to our target audience based on categories.
    """
    if candidate_labels is None:
        candidate_labels = [
            "geopolitics and international relations", 
            "national security and military",
            "domestic politics and government policy",
            "civil unrest and protests",
            "financial crime and corruption",
            "business mergers and corporate restructuring",
            "law enforcement and legal proceedings",
            "cybersecurity and digital threats",
            "law enforcement and criminal investigations",
            "terrorism and national security",
            "corporate fraud and financial crime",
            "routine news, sports, or entertainment"
        ]
        
    classifier = get_classifier()
    
    # Truncate text to a safe context length to avoid out-of-memory or input size limits
    truncated_text = text[:2000]
    
    result = classifier(truncated_text, candidate_labels, multi_label=True)
    
    return {
        "top_label": result["labels"][0],
        "score": result["scores"][0],
        "all_scores": dict(zip(result["labels"], result["scores"]))
    }

def extract_knowledge_graph(chunks: list[str], source_url: str = "Unknown", article_date: str = "Unknown") -> dict:
    full_graph = {"entities": [], "edges": []}
    
    for idx, chunk in enumerate(chunks):
        print(f"Processing chunk {idx + 1}/{len(chunks)}...")
        
        user_prompt = f"""Process the following article and extract the Knowledge Graph JSON. 

ARTICLE PUBLICATION DATE: {article_date}
ARTICLE SOURCE: {source_url}

ARTICLE TEXT: 
{chunk}"""
        
        payload = {
            "model": OLLAMA_MODEL, 
            "prompt": f"{SYSTEM_PROMPT}\n\n{user_prompt}",
            "format": "json",
            "stream": False,
            "keep_alive": OLLAMA_KEEP_ALIVE,  
            "options": {
                "temperature": 0.1,
                "num_ctx": OLLAMA_NUM_CTX
            }
        }
        
        try:
            data = ollama_request_with_retry("/api/generate", payload)
            if data:
                response_text = data.get("response", "")
                try:
                    graph_data = json.loads(response_text)
                    if "entities" in graph_data:
                        full_graph["entities"].extend(graph_data["entities"])
                    if "edges" in graph_data:
                        # [NEO4J INJECTION PATCH] Guarantee URLs and native Date-stamps are applied logically
                        for edge in graph_data["edges"]:
                            edge["source_url"] = source_url
                            edge["article_date"] = article_date
                        full_graph["edges"].extend(graph_data["edges"])
                except json.JSONDecodeError:
                    print("Model hallucinated formatting.")
            else:
                print(f"Ollama API failed for chunk {idx + 1} after retries.")
        except Exception as e:
            print(f"Error processing chunk {idx + 1}: {e}")
            
    # Deduplicate entities by name
    unique_entities = {}
    for entity in full_graph["entities"]:
        if "name" in entity and entity["name"] not in unique_entities:
            unique_entities[entity["name"]] = entity
            
    full_graph["entities"] = list(unique_entities.values())
    
    return full_graph

def generate_intelligence_briefing(target_entity_id: str, kg_backend) -> str:
    # 1. Query Neo4j for the target's network (2 hops away)
    query = """
    MATCH path = (target:Entity {id: $entity_id})-[*1..2]-(connected:Entity)
    RETURN path
    """
    
    with kg_backend.driver.session() as session:
        result = session.run(query, entity_id=target_entity_id)
        
        context_data = []
        for record in result:
            path = record["path"]
            # Extract the 'context' properties from the edges in the path
            for rel in path.relationships:
                context_data.append(f"- {rel['context']}")
                
        # Deduplicate the context lines
        context_data = list(set(context_data))

    if not context_data:
        print(f"No intelligence found in the graph for '{target_entity_id}'.")
        return None

    # 2. Format the retrieved context into a prompt for Qwen
    graph_context_string = "\n".join(context_data)
    
    analysis_prompt = f"""
    You are a high-level Intelligence Analyst. 
    Analyze the following verified network data regarding the target: {target_entity_id}.
    
    GRAPH DATABASE CONTEXT:
    {graph_context_string}
    
    TASK: Write a concise intelligence briefing. Highlight potential conflicts of interest, legal troubles, and key associates. Connect the dots.
    """

    # 3. Ask your local qwen2.5:3b to write the briefing
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": analysis_prompt,
        "stream": False,
        "keep_alive": OLLAMA_KEEP_ALIVE,  
        "options": {
            "temperature": 0.3, # Slightly higher temperature for natural language generation
            "num_ctx": OLLAMA_NUM_CTX
        }
    }

    try:
        data = ollama_request_with_retry("/api/generate", payload)
        if data:
            return data.get('response', 'Briefing generation failed.')
        return None
    except Exception as e:
        print(f"Intelligence Generation Failed: {e}")
        return None

def generate_live_briefing(entity_name, neo4j_uri=None, user=None, password=None, hops=2):
    """Pulls multi-hop context from Neo4j and streams it to Qwen for a strict, cited live briefing."""
    from neo4j import GraphDatabase
    neo4j_uri = neo4j_uri or NEO4J_URI
    user = user or NEO4J_USER
    password = password or NEO4J_PASS
    
    # 1. Dive directly into Neo4j for the Graph Context
    driver = GraphDatabase.driver(neo4j_uri, auth=(user, password))
    
    # Optimized query for temporal prioritization (Static first, then Chronological)
    query = f"""
        MATCH path = (target:Entity {{name: $entity_name}})-[*1..{hops}]-(connected:Entity)
        UNWIND relationships(path) AS r
        WITH DISTINCT r, startNode(r) AS n1, endNode(r) AS n2
        RETURN n1.name AS source, 
               type(r) AS relationship, 
               n2.name AS target_node, 
               r.context AS edge_context, 
               r.source_url AS source_url,
               r.start_date AS start_date,
               r.end_date AS end_date,
               r.start_year AS start_year
        ORDER BY 
            CASE WHEN r.start_year IS NULL THEN 0 ELSE 1 END, 
            r.start_year ASC, 
            r.start_date ASC
        LIMIT 60
    """
    
    raw_context = []
    url_map = {}
    
    with driver.session() as session:
        result = session.run(query, entity_name=entity_name)
        for idx, record in enumerate(result):
            # Bulletproof Extraction Logic
            rel = record["relationship"]
            source = record["source"]
            target = record["target_node"]
            s_date = record.get("start_date")
            e_date = record.get("end_date")
            
            # Dynamic Temporal Check (Zero Hardcoding)
            if s_date and e_date:
                time_str = f"(Timeframe: {s_date} to {e_date})"
            elif s_date:
                time_str = f"(Started: {s_date})"
            elif e_date:
                time_str = f"(Ended: {e_date})"
            else:
                time_str = ""
            
            # Citation Mapping
            url_text = record['source_url'] if record['source_url'] else "Unknown"
            ref_tag = f"[Ref {idx}]"
            url_map[ref_tag] = url_text
            
            # Cleanly joined string for LLM injection
            line = f"{ref_tag} {source} {rel} {target} {time_str}.".replace("  ", " ").strip()
            raw_context.append(line)
    driver.close()

    if not raw_context:
        return "⚠️ No deep graph connections found for this entity yet."

    context_string = "\n".join(raw_context)

    # 1. Separate the System Rules from the User Data
    system_prompt = """You are an automated OSINT evidence synthesizer. You have NO opinions and make NO predictions.
    
STRICT RULES:
1. OUTPUT FORMAT: You must output ONLY bullet points. Do not write introductory or concluding paragraphs.
2. ZERO EXTRAPOLATION: Do not predict future risks, legal outcomes, or public trust. State ONLY the exact facts provided in the data.
3. CONDITIONAL CHRONOLOGY: If the raw data provides a timeframe or date for a fact, you MUST include it in your bullet point. If a fact has no date, state it plainly without inventing a timeline.
4. MANDATORY CITATIONS: Every single bullet point MUST end with the exact citation ID provided in the raw data. Format: [Ref X].

EXAMPLE BULLET FORMAT:
- Target is domiciled in Kerala. [Ref 0]
- Target serves as the CEO of Global Tech. [Ref 1]
- Target served as Additional Secretary at the M/o Agriculture from 1976-08-01 to 1980-01-01. [Ref 2]"""

    user_prompt = f"TARGET ENTITY: {entity_name}\n\nRAW GRAPH DATA:\n{context_string}\n\nSynthesize the above data into factual bullet points with citations."

    # 2. Use the Chat Endpoint to enforce the System Prompt
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "stream": False,
        "keep_alive": OLLAMA_KEEP_ALIVE,  
        "options": {
            "temperature": 0.0, # Absolute zero creativity
            "top_p": 0.8,       # Restore vocabulary enough to print complex URL hashes
            "num_ctx": OLLAMA_NUM_CTX
        }
    }
    
    try:
        # 3. CRITICAL: Notice we changed the endpoint from /api/generate to /api/chat
        data = ollama_request_with_retry("/api/chat", payload)
        
        if not data:
            return "System Alert: Ollama API failed after retries."
        
        # Pull the mechanical output
        final_text = data.get('message', {}).get('content', "Briefing generation failed.")
        
        # [Python Execution Layer]: Hot-swap the Ref IDs for fully structured Markdown links safely
        for ref_tag, actual_url in url_map.items():
            final_text = final_text.replace(ref_tag, f"[[Source]({actual_url})]")
            
        return final_text
    except Exception as e:
        return f"System Alert: Could not reach local LLM. Error: {e}"
