import requests
import pandas as pd
import time
from pathlib import Path
import os
import re

# =========================
# PIPELINE CONFIGURATION (Level 2)
# =========================
API_KEY = os.getenv("OPENSEA_API_KEY")
if not API_KEY:
    raise RuntimeError("OPENSEA_API_KEY environment variable not set. Please set it via pipeline_config.json or environment.")
BASE_URL = "https://api.opensea.io/api/v2"
RAW_COLLECTION = os.getenv("COLLECTION_SLUG")
if not RAW_COLLECTION:
    raise RuntimeError("COLLECTION_SLUG environment variable not set. Please set it via pipeline_config.json or environment.")
SLUG = re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", RAW_COLLECTION.lower())).strip("-")
FORCE_REFRESH = os.getenv("FORCE_REFRESH", "0") == "1"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

print("Fetching ALL NFTs in collection...")

output_dir = Path("collections") / SLUG
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "Level2_all_nfts_metadata.csv"
legacy_output_file = output_dir / "level2_all_nfts_metadata.csv"

if not output_file.exists() and legacy_output_file.exists() and not FORCE_REFRESH:
    legacy_output_file.replace(output_file)
    print(f"Migrated legacy file to pipeline name: {output_file}")

if output_file.exists() and not FORCE_REFRESH:
    print(f"Skipped Level 2 (already exists): {output_file}")
    raise SystemExit(0)

all_nfts = []
next_cursor = None

while True:
    url = f"{BASE_URL}/collection/{SLUG}/nfts"

    params = {
        "limit": 200
    }

    if next_cursor:
        params["next"] = next_cursor

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()

    data = response.json()

    nfts = data.get("nfts", [])
    all_nfts.extend(nfts)

    print(f"Collected: {len(all_nfts)} NFTs")

    next_cursor = data.get("next")
    if not next_cursor:
        break

    time.sleep(0.2)

print("Total NFTs Collected:", len(all_nfts))

# Save raw NFT metadata
df_nfts = pd.json_normalize(all_nfts)
df_nfts.to_csv(output_file, index=False)

print(f"Saved: {output_file}")

print("LEVEL 2 COMPLETE ✅")