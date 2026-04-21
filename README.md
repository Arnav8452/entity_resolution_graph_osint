# 🛰️ Entity Resolution Graph OSINT
### The Palantir-Grade OSINT Intelligence Watchdog

An autonomous, entirely localized, AI-driven Open Source Intelligence (OSINT) pipeline. This system operates as a continuous data-vacuum—scraping live news, evaluating relevance, extracting structured relationships, mitigating entity-resolution hallucinations, and surfacing geopolitical anomalies via a graph-theoretic dashboard.

---

## 🏗️ Core Architecture

### 1. The Autonomous Ingestion Engine
Controlled via a 15-minute Python master loop (`main.py` & `watchdog.py`), the system dynamically ingests unformatted HTML and API feeds from:
- **Google News RSS** (Targeting: Geopolitics, Crime, Financial Ventures, etc.)
- **Bing News API**
- **Reddit ('Hot' threads)**

### 2. The Semantic "Air-Lock" (Zero-Shot Filtering)
Before raw text touches the expensive generative LLMs, it must pass through a `facebook/bart-large-mnli` Zero-Shot classification layer.
- **Configuration**: `multi_label=True` (Sigmoid scoring) to prevent false rejections of multi-topic articles.
- **Taxonomy**: Expanded to catch Geopolitics, Financial Crime, National Security, and Cybersecurity.
- **Junk Filter**: Articles flagged as `routine news, sports, or entertainment` or falling below a strict (> 0.50) confidence threshold are dropped to prevent database bloat.

### 3. The Extraction Engine (Local Ollama)
Powered by a localized 3-Billion parameter instruction model (**Qwen 2.5 3B**), the engine transforms unstructured text into a structured Knowledge Graph using the **Caveman Schema**.
- **Disambiguation Keys**: Entities are broken down into associative arrays (Target Roles, Known Locations, Associated Organizations) to prevent vague descriptions.
- **The Temporal Shield**: Surgically extracts `event_year`. If historical (e.g., 1556), it provides the integer. If current, it falls back to the RSS `pubDate` using `dateutil`.

### 4. The 2-Layer Resolution "Gauntlet" (RAM-Optimized)
Solves the Knowledge Graph duplication problem by assigning cryptographic UUIDs and executing checks completely in RAM using **Lightning-C** dictionary subset scans.

#### Layer 1: The Nomination Net (Typo & Acronym Defense)
- **Fuzzy Matching**: Uses `jellyfish` (Jaro-Winkler > 0.85) to catch typos.
- **The First-Token Lock**: For `PERSON` entities, mandates root character matches (e.g., "Shri Dinesh" != "Shri Vinod").
- **The Anti-Hierarchy Lock**: Prevents merging hierarchical modifiers (e.g., "Assistant Director" != "Director") for `CONCEPT/ORG` nodes.

#### Layer 2: The Vector Scalpel (Semantic Defense)
- **Context Interrogation**: A `SentenceTransformer` (`all-MiniLM-L6-v2`) maps combined context (Roles + Locations + Orgs) into a 384-dimensional array.
- **Cosine-Similarity**: If the math proves `< 0.91` match (e.g., "John Smith the CEO" vs "John Smith the Smuggler"), the system triggers a **Hard Rejection** and assigns a separate UUID.

### 5. Automated Cypher Tripwires
A daemon (`watchdog.py`) executes cyclical Neo4j topology sweeps searching for geopolitical red flags:
- **Smoke Alarms**: High-risk edges like `ARRESTED_BY` or `RAIDED_BY`.
- **Circular Ownership**: Recursive `OWNS` and `ACQUIRED` loops.
- **VIP Bridges**: Centralized "Middleman" individuals linking separate networked organizations.

### 6. The Analyst Dashboard (Visual Link Analysis)
A dedicated Streamlit interface (`ui/dashboard.py`) for fluid topological intelligence.
- **Zero-Hallucination AI Briefings**: Prompts Qwen using `/api/chat` with zero temperature and `[Ref 0]` mapping for factual bullet points with verifiable Markdown hyperlinks to source URLs.
- **Kinetic Graph Canvas**: Powered by `streamlit-agraph`, utilizes `barnesHut` physics and a **Timeline Horizon Guard** (Century Shield) to prevent modern events from colliding with historical graph data.

---

## 🛠️ Stack & Hardware Optimization

### Tech Stack Summary
- **Database**: Neo4j (Deep Topology) + SQLite (Tabular Caching/Syncing)
- **Inference**: Ollama (Local Qwen 2.5 3B / memebot-qwen)
- **Embeddings**: `all-MiniLM-L6-v2` (384-dim vectors)
- **Filtering**: Transformers Zero-Shot Pipeline (`BART-MNLI`)
- **Scraping**: `curl_cffi` (Chrome Impersonation) + `Trafilatura`

### Hardware Profile (Tuned for 4GB VRAM limit)
- **VRAM Management**: Reduction of `num_ctx` to 2048 and `num_gpu` layers to fit within a 4GB ceiling (e.g., RTX 3050).
- **Rolling Memory Window**: 15-minute "Keep-Alive" window to flush the model from VRAM during downtime.
- **Deadlock Prevention**: `KeyboardInterrupt` triggers a `keep_alive: 0` signal to Ollama to prevent zombie processes.
- **Forced GPU Routing**: Windows graphics settings optimization to bypass iGPU.

---

## 🚀 The 5-Stage Ingestion Pipeline

1. **The Gatekeeper**: Lightweight Zero-Shot classification and junk routing.
2. **Extraction**: "Caveman" JSON extraction with temporal stamping.
3. **Vector Condenser**: Mechanical context string building and GPU batch vectorization.
4. **The Gauntlet**: Multi-layered entity resolution (Blocking -> Nomination -> Vector Scalpel).
5. **TKG Traversal**: Temporal Knowledge Graph path discovery with **Century Shield** filters.

---

## 📂 Bulk Data: CSV Ingestor

The **Hybrid CSV Ingestor** (`ingest_csv.py`) solves the "Cold Start" problem via a high-speed chunked **UNWIND** architecture.

- **Fast Path**: 100x faster injection for structured data, bypassing LLM compute costs.
- **Automatic Merging**: CSV-imported entities merge with news-scraped entities through the ER Gauntlet.
- **GPU Batching**: Encodes ALL entities in one GPU pass (batch_size=256).

### How to Use (Dashboard UI)
1. Go to the **"CSV Ingestor"** tab.
2. Upload CSV, configure **Name Column** and **Entity Type**.
3. Map **Disambiguation Keys** (Role, Locations, Orgs) for better resolution.
4. Define **Relationships** (Target Column, Edge Label).
5. Click **🚀 Ingest CSV into Knowledge Graph**.

---

## ⚙️ Installation & Setup

1. **Prerequisites**: Neo4j, Ollama, Python 3.10+.
2. **Clone & Install**:
   ```bash
   git clone https://github.com/Arnav8452/entity_resolution_graph_osint.git
   pip install -r requirements.txt
   ```
3. **Configure Environment**: Create a `.env` file with `NEO4J_URI`, `NEO4J_USER`, and `NEO4J_PASS`.
4. **Run**:
   ```bash
   python main.py             # Start ingestion loop
   streamlit run ui/dashboard.py  # Launch dashboard
   ```

---

## 🚀 Roadmap & Next Steps
- **API Integration**: Connecting to live government databases (MCA, OpenCorporates).
- **Dynamic Watchlists**: UI tab for registering high-priority targets with automated detection alerts.
- **Temporal Fidelity**: Enhancing multi-hop chronological path tracing.

---

## ⚖️ License & Contributing
Distributed under the MIT License. Contributions are welcome—feel free to fork and submit PRs!