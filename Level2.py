import requests
import pandas as pd
import time
from pathlib import Path
import os
import re
import random

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
MAX_RETRIES = int(os.getenv("OPENSEA_MAX_RETRIES", "8"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("OPENSEA_REQUEST_TIMEOUT_SECONDS", "30"))

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


def fetch_page(url: str, params: dict) -> dict:
    """Fetch one page with retry/backoff for rate limits and transient failures."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT_SECONDS)

            if response.status_code == 429 or 500 <= response.status_code < 600:
                retry_after_raw = response.headers.get("Retry-After", "").strip()
                retry_after_seconds = 0.0
                if retry_after_raw.isdigit():
                    retry_after_seconds = float(retry_after_raw)

                # Exponential backoff with jitter, while honoring Retry-After when available.
                backoff_seconds = min(60.0, (2 ** (attempt - 1)) + random.uniform(0, 0.5))
                sleep_seconds = max(retry_after_seconds, backoff_seconds)

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                print(
                    f"Rate limit/transient error ({response.status_code}) on attempt {attempt}/{MAX_RETRIES}. "
                    f"Sleeping {sleep_seconds:.1f}s before retry..."
                )
                time.sleep(sleep_seconds)
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise

            backoff_seconds = min(60.0, (2 ** (attempt - 1)) + random.uniform(0, 0.5))
            print(
                f"Request failed on attempt {attempt}/{MAX_RETRIES}: {exc}. "
                f"Sleeping {backoff_seconds:.1f}s before retry..."
            )
            time.sleep(backoff_seconds)

    raise RuntimeError("Unreachable state while fetching OpenSea page.")

while True:
    url = f"{BASE_URL}/collection/{SLUG}/nfts"

    params = {
        "limit": 200
    }

    if next_cursor:
        params["next"] = next_cursor

    data = fetch_page(url, params)

    nfts = data.get("nfts", [])
    all_nfts.extend(nfts)

    print(f"Collected: {len(all_nfts)} NFTs")

    next_cursor = data.get("next")
    if not next_cursor:
        break

    time.sleep(0.4)

print("Total NFTs Collected:", len(all_nfts))

# Save raw NFT metadata
df_nfts = pd.json_normalize(all_nfts)
df_nfts.to_csv(output_file, index=False)

print(f"Saved: {output_file}")

print("LEVEL 2 COMPLETE ✅")