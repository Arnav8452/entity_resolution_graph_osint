# CSV Ingestor — User Guide

## Overview

The CSV Ingestor lets you upload any structured CSV file directly into the Knowledge Graph **without using any LLM compute**. Each row is transformed into entities and relationships, then routed through the same Entity Resolution Gauntlet that the news pipeline uses.

This means CSV-imported entities will **automatically merge** with entities discovered from live news scraping (e.g., a CSV row for "BJP" will merge with a news-scraped "Bharatiya Janata Party" through the Jaro-Winkler + vector similarity match).

---

## How To Use (Dashboard UI)

### 1. Launch the Dashboard

```bash
streamlit run ui/dashboard.py
```

### 2. Go to the "CSV Ingestor" Tab

Click the 4th tab at the top of the dashboard.

### 3. Upload Your CSV

Drag and drop or browse to select your CSV file. You'll see:
- Row and column count
- A preview of the first 5 rows

### 4. Step 1 — Primary Entity

Configure the **main entity** in each row:

| Field | What to Pick | Example |
|-------|-------------|---------|
| **Name Column** | The column containing the entity name | `Company Name`, `Name`, `Politician` |
| **Entity Type** | What kind of entity this is | `PERSON`, `ORGANIZATION`, `LOCATION` |

### 5. Step 2 — Disambiguation Keys (Optional but Recommended)

These help the Entity Resolution Gauntlet tell apart entities with similar names:

| Field | Purpose | Example Column |
|-------|---------|---------------|
| **Role / Title** | What the entity does | `Designation`, `Industry`, `Job Title` |
| **Location(s)** | Where the entity is based | `State`, `Headquarters`, `Country` |
| **Organization(s)** | What the entity is affiliated with | `Party`, `Group`, `Employer` |

> **Tip:** If you skip this step, the system auto-builds context from ALL columns. But explicit mapping produces much better entity resolution.

### 6. Step 3 — Relationships

Define which columns should become **linked entities with edges**:

For each relationship row, configure:

| Field | What It Does | Example |
|-------|-------------|---------|
| **Target Column** | Which CSV column contains the linked entity | `Party`, `Sector`, `State` |
| **Target Entity Type** | What type of entity the target is | `POLITICAL_PARTY`, `CONCEPT`, `LOCATION` |
| **Edge Label** | The relationship name (auto-uppercased) | `MEMBER_OF`, `BELONGS_TO_SECTOR`, `HEADQUARTERED_IN` |

Click **＋ Add Relationship** to add more rows. Click **－ Remove Last** to delete one.

### 7. Ingest

Click **🚀 Ingest CSV into Knowledge Graph**. You'll see:
- A live progress bar showing each row being processed
- Summary metrics (✅ Ingested / ⏭️ Skipped / ❌ Errors)
- 🎈 Balloons on success

### 8. Explore Your Data

Switch to:
- **Raw Intelligence Cache** → See all entities and edges in table format
- **Visual Link Analysis** → Render interactive graph visualizations
- **Live Intelligence Briefings** → Generate AI dossiers on any entity

---

## How To Use (Command Line)

For automation or batch processing, use the CLI directly:

```bash
python ingest_csv.py configs/nifty500.json
python ingest_csv.py configs/politicians.json
```

### JSON Config Format

```json
{
    "file_path": "data/your_file.csv",
    
    "primary_entity": {
        "name_column": "Company Name",
        "type": "ORGANIZATION"
    },
    
    "disambiguation_mapping": {
        "role_or_title": "Industry",
        "associated_locations": ["Headquarters"],
        "affiliated_organizations": ["Group"]
    },
    
    "relationships": [
        {
            "target_column": "Sector",
            "target_type": "CONCEPT",
            "edge_name": "BELONGS_TO_SECTOR"
        },
        {
            "target_column": "Headquarters",
            "target_type": "LOCATION",
            "edge_name": "HEADQUARTERED_IN"
        }
    ]
}
```

### Config Fields Reference

| Field | Required | Description |
|-------|----------|-------------|
| `file_path` | ✅ | Path to the CSV file |
| `primary_entity.name_column` | ✅ | Column containing the main entity name |
| `primary_entity.type` | ✅ | Entity type (PERSON, ORGANIZATION, etc.) |
| `disambiguation_mapping` | Optional | Maps columns to role/location/org context fields |
| `relationships` | Optional | Array of column-to-edge mappings |
| `relationships[].multi_value` | Optional | Set `true` if column has comma-separated values |

---

## Example CSV Files Included

| File | Entities | Description |
|------|----------|-------------|
| `data/nifty500.csv` | 10 companies | Reliance, TCS, Infosys, HDFC Bank, etc. |
| `data/politicians.csv` | 10 politicians | Modi, Rahul Gandhi, Kejriwal, etc. with criminal case counts |

---

## Prerequisites

- **Neo4j** must be running (`docker compose up -d`)
- **python-dotenv** must be installed (`pip install python-dotenv`)
- The `.env` file must exist in the project root with Neo4j credentials


📖 The Temporal Knowledge Graph: CSV Configuration Guide
This configuration file is the translation layer between your flat CSVs and the Temporal Knowledge Graph. It dictates how the RAM-Gauntlet resolves identities, how the Century Shield stamps time onto edges, and how the system prevents temporal events from collapsing into one another.

1. The Master Template
Every JSON configuration requires these six operational blocks:

JSON
{
    "file_path": "data/your_dataset.csv",
    
    "primary_entity": { ... },
    "disambiguation_mapping": { ... },
    "trash_values": [ ... ],
    "temporal_mapping": { ... },
    "relationships": [ ... ]
}
2. Block Breakdown & Architecture
Block 1: File Path
The target dataset.

file_path: String path to your CSV.

Block 2: Primary Entity (The Hub & Static Properties)
This defines the main subject. Crucially, this block handles properties that belong to the entity permanently, rather than properties that change over time.

name_column: The CSV header containing the target's name.

type: The Neo4j label (e.g., "PERSON").

unique_id_column (Optional but Critical): If your CSV has multiple rows for the same person across different times (like a career history), map the unique row ID here (e.g., "Reference_Value"). This tells the engine to evaluate every row as a distinct event.

static_date_properties: An array of columns containing permanent temporal facts (like "Date_of_Birth" or "Allotment_Year"). These will be extracted and saved directly to the Node, immune to the Timeline Slider.

Block 3: Disambiguation Mapping (The Graph Orbit)
🚨 The RAM-Gauntlet's core defense.
Maps columns that contain highly identifying contextual information. The engine searches the existing graph for these specific values to prevent "Supernode" identity collisions (e.g., merging two officers who share a name).

Map categories like "associated_locations" or "affiliated_organizations" to their respective CSV columns.

Block 4: The Trash Filter
Prevents the system from building massive, useless nodes.

trash_values: An array of lowercase strings (["n.a.", "-", "nan", "on leave"]). If a cell matches exactly, the engine safely skips edge creation.

Block 5: Temporal Mapping (The Century Shield)
⏳ Powers the Streamlit Timeline Slider.
These dates are processed by the Universal Regex Extractor (hunting for 19XX or 20XX) and stamped as integers onto the relationship edges.

start_date_column: Maps to the event's start.

end_date_column: Maps to the event's end.

Block 6: Relationships (Edge Generation)
This array transforms flat columns into graph topology.

target_column: The CSV column containing the destination node's name.

target_type: The Neo4j label for the destination (e.g., "ORGANIZATION").

edge_name: The physical relationship label (e.g., "WORKED_AT").

allow_multiple (🚨 The Temporal Fix): * Set to true: Forces Neo4j to draw a new edge for this row, even if an edge to that organization already exists. Use this for career histories. (e.g., An officer is posted to "Centre" in 1980, and again in 1985 -> Creates 2 edges).

Set to false: Overwrites/merges the edge. Use this for static facts. (e.g., Birth state -> Creates 1 edge).

3. Reference Configurations
Example A: The "Event History" Config (e.g., Postings)
Uses unique_id_column and allow_multiple: true to map sequential events.

JSON
{
    "file_path": "data/2026-04-03T03-07_export.csv",
    "primary_entity": {
        "name_column": "Name",
        "type": "PERSON",
        "unique_id_column": "Reference_Value"
    },
    "temporal_mapping": {
        "start_date_column": "Start_Date",
        "end_date_column": "End_Date"
    },
    "relationships": [
        {
            "target_column": "Office",
            "target_type": "ORGANIZATION",
            "edge_name": "WORKED_AT",
            "allow_multiple": true
        }
    ]
}
Example B: The "Profile" Config (e.g., Demographics)
Uses static_date_properties and allow_multiple: false to map permanent traits.

JSON
{
    "file_path": "data/2026-04-03T03-03_export.csv",
    "primary_entity": {
        "name_column": "Name",
        "type": "PERSON",
        "static_date_properties": ["Date_of_Birth"]
    },
    "relationships": [
        {
            "target_column": "Place_of_Domicile",
            "target_type": "LOCATION",
            "edge_name": "DOMICILED_IN",
            "allow_multiple": false
        }
    ]
}