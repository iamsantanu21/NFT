import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


# =============================
# CONFIGURATION
# =============================

API_KEY = os.getenv("OPENSEA_API_KEY", "")

BASE_URL = "https://api.opensea.io/api/v2"
CHAIN = "ethereum"
SLUG = "cryptopunks"
CONTRACT = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

OUTPUT_FILE = "cryptopunks_all_nft_data.csv"
OUTPUT_IMAGES_DIR = "cryptopunks_images"

REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 0.25
SAVE_EVERY = 50

# Set True if you want to download image files locally
DOWNLOAD_IMAGES = False


# =============================
# HEADERS
# =============================

HEADERS = {
    "accept": "application/json",
}

if API_KEY:
    HEADERS["X-API-KEY"] = API_KEY


# =============================
# HELPERS
# =============================


def safe_get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response = requests.get(
        url,
        headers=HEADERS,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def fetch_all_token_ids() -> List[str]:
    print("Fetching token IDs from CryptoPunks collection...")

    token_ids: List[str] = []
    next_cursor: Optional[str] = None

    while True:
        url = f"{BASE_URL}/collection/{SLUG}/nfts"
        params: Dict[str, Any] = {"limit": 200}

        if next_cursor:
            params["next"] = next_cursor

        data = safe_get(url, params=params)
        nfts = data.get("nfts", [])

        for nft in nfts:
            identifier = nft.get("identifier")
            if identifier is not None:
                token_ids.append(str(identifier))

        print(f"Collected token IDs: {len(token_ids)}")

        next_cursor = data.get("next")
        if not next_cursor:
            break

        time.sleep(SLEEP_SECONDS)

    unique_ids = sorted(set(token_ids), key=lambda x: int(x))
    print(f"Total unique token IDs: {len(unique_ids)}")
    return unique_ids


def download_image(url: str, token_id: str) -> str:
    if not url:
        return ""

    images_dir = Path(OUTPUT_IMAGES_DIR)
    images_dir.mkdir(parents=True, exist_ok=True)

    file_path = images_dir / f"{token_id}.png"

    if file_path.exists():
        return str(file_path)

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        file_path.write_bytes(response.content)
        return str(file_path)
    except Exception as exc:
        print(f"Image download failed for token {token_id}: {exc}")
        return ""


def parse_nft_detail(detail: Dict[str, Any], token_id: str) -> Dict[str, Any]:
    nft = detail.get("nft", detail)

    traits = nft.get("traits") or []
    owners = nft.get("owners") or []
    rarity = nft.get("rarity") or {}

    creator_data = nft.get("creator")
    creator_address = ""

    if isinstance(creator_data, dict):
        creator_address = creator_data.get("address", "") or ""
    elif isinstance(creator_data, str):
        creator_address = creator_data

    image_url = nft.get("image_url") or ""

    image_file = ""
    if DOWNLOAD_IMAGES and image_url:
        image_file = download_image(image_url, token_id)

    return {
        "identifier": token_id,
        "collection": nft.get("collection"),
        "contract": nft.get("contract"),
        "token_standard": nft.get("token_standard"),
        "name": nft.get("name"),
        "description": nft.get("description"),
        "image_url": image_url,
        "display_image_url": nft.get("display_image_url"),
        "original_image_url": nft.get("original_image_url"),
        "metadata_url": nft.get("metadata_url"),
        "opensea_url": nft.get("opensea_url"),
        "updated_at": nft.get("updated_at"),
        "is_disabled": nft.get("is_disabled"),
        "is_nsfw": nft.get("is_nsfw"),
        "is_suspicious": nft.get("is_suspicious"),
        "creator_address": creator_address,
        "owners_count": len(owners),
        "owners_json": json.dumps(owners, ensure_ascii=False),
        "traits_count": len(traits),
        "traits_json": json.dumps(traits, ensure_ascii=False),
        "rarity_strategy_id": rarity.get("strategy_id"),
        "rarity_strategy_version": rarity.get("strategy_version"),
        "rarity_rank": rarity.get("rank"),
        "image_file": image_file,
    }


def load_resume_state() -> List[Dict[str, Any]]:
    if not os.path.exists(OUTPUT_FILE):
        return []

    old_df = pd.read_csv(OUTPUT_FILE)
    print(f"Resuming from existing file: {OUTPUT_FILE} ({len(old_df)} rows)")
    return old_df.to_dict("records")


# =============================
# MAIN
# =============================


def main() -> None:
    if not API_KEY:
        print("Warning: OPENSEA_API_KEY not set. Requests may be limited or blocked.")

    token_ids = fetch_all_token_ids()

    results = load_resume_state()
    done_ids = {str(row.get("identifier")) for row in results if row.get("identifier") is not None}

    processed_since_save = 0

    for token_id in token_ids:
        if token_id in done_ids:
            continue

        url = f"{BASE_URL}/chain/{CHAIN}/contract/{CONTRACT}/nfts/{token_id}"

        try:
            detail = safe_get(url)
            row = parse_nft_detail(detail, token_id)
            results.append(row)
            done_ids.add(token_id)
            processed_since_save += 1

            print(f"Processed token {token_id} | total rows: {len(results)}")

            if processed_since_save >= SAVE_EVERY:
                pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
                print(f"Saved progress to {OUTPUT_FILE}")
                processed_since_save = 0

            time.sleep(SLEEP_SECONDS)

        except Exception as exc:
            print(f"Failed token {token_id}: {exc}")
            time.sleep(SLEEP_SECONDS)
            continue

    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print(f"Done. Final dataset saved to {OUTPUT_FILE} ({len(results)} rows)")


if __name__ == "__main__":
    main()
