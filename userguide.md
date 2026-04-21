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