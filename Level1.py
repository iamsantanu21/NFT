import requests
import pandas as pd
from pathlib import Path
import os
import re


# =========================
# PIPELINE CONFIGURATION (Level 1)
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

# Create per-collection output folder
output_dir = Path("collections") / SLUG
output_dir.mkdir(parents=True, exist_ok=True)

# Name the output file after the NFT collection slug
output_file = output_dir / "Level1_collection_info.csv"
legacy_output_file = output_dir / f"{SLUG}_collection_info.csv"

if not output_file.exists() and legacy_output_file.exists() and not FORCE_REFRESH:
    legacy_output_file.replace(output_file)
    print(f"Migrated legacy file to pipeline name: {output_file}")

if output_file.exists() and not FORCE_REFRESH:
    print(f"Skipped Level 1 (already exists): {output_file}")
    raise SystemExit(0)

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

print("Fetching Collection Information...")

url = f"{BASE_URL}/collections/{SLUG}"
response = requests.get(url, headers=HEADERS)
response.raise_for_status()

collection_data = response.json()

# Save as structured dataset
df_collection = pd.json_normalize(collection_data)
df_collection.to_csv(output_file, index=False)

print(f"Saved: {output_file}")

print("LEVEL 1 COMPLETE ✅")