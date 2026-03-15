# NFT Pipeline (OpenSea)

A **4-stage NFT data extraction pipeline** that collects and processes OpenSea data for any NFT collection.

## Pipeline Overview

The pipeline runs four scripts in sequence:

| Stage | Script | Output |
|---|---|---|
| Level 1 | `Level1.py` | Collection metadata |
| Level 2 | `Level2.py` | Full NFT metadata (all tokens) |
| Level 3 | `Level3_activity_history.py` | Complete activity history |
| Level 4 | `Level4_financial_dataset.py` | Aggregated financial stats |

All outputs are written to `collections/<collection-slug>/`.

---

## Setup

1. **Install dependencies**
   ```powershell
   python -m pip install -r requirements.txt
   ```

2. **Create your config file** — copy the example and fill in your API key:
   ```powershell
   Copy-Item pipeline_config.example.json pipeline_config.json
   ```
   Then edit `pipeline_config.json`:
   ```json
   {
     "collection": "mutant-ape-yacht-club",
     "opensea_api_key": "YOUR_OPENSEA_API_KEY_HERE",
     "force_refresh": false
   }
   ```

---

## Running the Pipeline

```powershell
python .\run_all_levels.py
```

### Optional CLI flags

| Flag | Description |
|---|---|
| `--collection <slug>` | Override the collection slug from config |
| `--config <path>` | Use a different config file (default: `pipeline_config.json`) |
| `--force` | Force re-run all levels even if outputs already exist |

---

## Choosing the Level 3 Mode

Open `run_all_levels.py` and edit the two lines near the top:

```python
# ✏️ EDIT HERE — comment out one, keep the other active
LEVEL3_SCRIPT = "Level3_activity_history.py"           # full (all event types)
# LEVEL3_SCRIPT = "Level3_activity_history_filtered.py"  # filtered event types
```

- **`Level3_activity_history.py`** — collects every event type (mint, transfer, sale, listing, offer, cancel, etc.)
- **`Level3_activity_history_filtered.py`** — collects only the event types you specify

To change which event types the filtered script collects, open `Level3_activity_history_filtered.py` and edit:

```python
# ✏️ EDIT HERE — types to keep (comma-separated). Leave empty to keep all.
ACTIVITY_TYPES_RAW = os.getenv("ACTIVITY_TYPES", "sale,mint,transfer")
```

---

## Output Structure

```
collections/
└── mutant-ape-yacht-club/
    ├── Level1_collection_info.csv
    ├── level2_all_nfts_metadata.csv
    ├── Level3_complete_activity_history.csv   (or filtered variant)
    └── Level4_financial_dataset.csv
```

---

## Resuming

Level 3 supports checkpointing. If interrupted, re-running the pipeline will resume from where it left off automatically.

---

## Notes

- An [OpenSea API key](https://docs.opensea.io/reference/api-overview) is required for Levels 1, 2, and 3.
- Level 4 works entirely offline — it aggregates data from the Level 3 CSV.
- `pipeline_config.json` is excluded from git. Use `pipeline_config.example.json` as your template.
