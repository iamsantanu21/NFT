"""
Level 4: Financial Dataset Aggregation
Builds Level4_financial_dataset.csv from Level3 activity data (no API calls).
"""
import pandas as pd
import os
import re
from pathlib import Path

# =========================
# PIPELINE CONFIGURATION (Level 4)
# =========================
RAW_COLLECTION = os.getenv("COLLECTION_SLUG")
if not RAW_COLLECTION:
    raise RuntimeError("COLLECTION_SLUG environment variable not set. Please set it via pipeline_config.json or environment.")
COLLECTION_SLUG = re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", RAW_COLLECTION.lower())).strip("-")
COLLECTION_DIR = Path("collections") / COLLECTION_SLUG
COLLECTION_DIR.mkdir(parents=True, exist_ok=True)

LEVEL1_FILE = COLLECTION_DIR / "Level1_collection_info.csv"
LEVEL2_FILE = COLLECTION_DIR / "Level2_all_nfts_metadata.csv"
ACTIVITY_FILE = COLLECTION_DIR / "Level3_complete_activity_history.csv"
OUTPUT_FILE = COLLECTION_DIR / "Level4_financial_dataset.csv"

if not ACTIVITY_FILE.exists():
    raise RuntimeError(f"Level 3 activity file not found: {ACTIVITY_FILE}")

df = pd.read_csv(LEVEL2_FILE)
token_ids = df["identifier"].astype(str).tolist()
activity_df = pd.read_csv(ACTIVITY_FILE)

required = {"identifier", "event_type", "price_eth", "from", "to"}
missing = required.difference(set(activity_df.columns))
if missing:
    raise RuntimeError(f"Level 3 activity file missing columns: {sorted(missing)}")

activity_df["identifier"] = activity_df["identifier"].astype(str)
activity_df["event_type"] = activity_df["event_type"].astype(str).str.lower()
activity_df["price_eth"] = pd.to_numeric(activity_df["price_eth"], errors="coerce").fillna(0.0)
sales_df = activity_df[(activity_df["event_type"] == "sale") & (activity_df["price_eth"] > 0)].copy()

if sales_df.empty:
    base = pd.DataFrame({"identifier": token_ids})
    for col in [
        "transaction_count",
        "avg_price_eth",
        "max_price_eth",
        "min_price_eth",
        "total_volume_eth",
        "unique_buyers",
        "unique_sellers",
    ]:
        base[col] = 0
    base.to_csv(OUTPUT_FILE, index=False)
    print(f"Built Level 4 from Level 3 (no sales found): {OUTPUT_FILE}")
    raise SystemExit(0)

grouped = sales_df.groupby("identifier")
summary = grouped["price_eth"].agg(["count", "mean", "max", "min", "sum"]).reset_index()
summary = summary.rename(
    columns={
        "count": "transaction_count",
        "mean": "avg_price_eth",
        "max": "max_price_eth",
        "min": "min_price_eth",
        "sum": "total_volume_eth",
    }
)
buyers = grouped["to"].nunique().reset_index(name="unique_buyers")
sellers = grouped["from"].nunique().reset_index(name="unique_sellers")
summary = summary.merge(buyers, on="identifier", how="left")
summary = summary.merge(sellers, on="identifier", how="left")
base = pd.DataFrame({"identifier": token_ids})
final = base.merge(summary, on="identifier", how="left")
final = final.fillna(
    {
        "transaction_count": 0,
        "avg_price_eth": 0,
        "max_price_eth": 0,
        "min_price_eth": 0,
        "total_volume_eth": 0,
        "unique_buyers": 0,
        "unique_sellers": 0,
    }
)
final["transaction_count"] = final["transaction_count"].astype(int)
final["unique_buyers"] = final["unique_buyers"].astype(int)
final["unique_sellers"] = final["unique_sellers"].astype(int)
final.to_csv(OUTPUT_FILE, index=False)
print(f"Built Level 4 from Level 3: {OUTPUT_FILE}")
