"""
Level 3 (Filtered): Activity History Extraction
Extracts activity history for all NFTs in the collection and keeps only selected activity types.
"""

import ast
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Set

import pandas as pd
import requests

# =========================
# PIPELINE CONFIGURATION (Level 3 Filtered)
# =========================
API_KEY = os.getenv("OPENSEA_API_KEY")
if not API_KEY:
    raise RuntimeError("OPENSEA_API_KEY environment variable not set. Please set it via pipeline_config.json or environment.")

BASE_URL = "https://api.opensea.io/api/v2"
HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY,
}

CHAIN = "ethereum"
RAW_COLLECTION = os.getenv("COLLECTION_SLUG")
if not RAW_COLLECTION:
    raise RuntimeError("COLLECTION_SLUG environment variable not set. Please set it via pipeline_config.json or environment.")

COLLECTION_SLUG = re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", RAW_COLLECTION.lower())).strip("-")
COLLECTION_NAME = COLLECTION_SLUG.replace("-", " ").title()

COLLECTION_DIR = os.path.join("collections", COLLECTION_SLUG)
os.makedirs(COLLECTION_DIR, exist_ok=True)

# Files
LEVEL1_FILE = os.path.join(COLLECTION_DIR, "Level1_collection_info.csv")
METADATA_INPUT_FILE = os.path.join(COLLECTION_DIR, "Level2_all_nfts_metadata.csv")
OUTPUT_FILE = os.path.join(COLLECTION_DIR, "Level3_filtered_activity_history.csv")
CHECKPOINT_FILE = os.path.join(COLLECTION_DIR, "Level3_filtered_extraction_checkpoint.json")
SUMMARY_FILE = os.path.join(COLLECTION_DIR, "Level3_filtered_extraction_summary.json")
ERROR_LOG_FILE = os.path.join(COLLECTION_DIR, "Level3_filtered_extraction_errors.log")
TEMP_FILE = os.path.join(COLLECTION_DIR, "TEMP_Level3_filtered_activity_history.csv")

LEGACY_METADATA_INPUT_FILE = os.path.join(COLLECTION_DIR, "level2_all_nfts_metadata.csv")
if not os.path.exists(METADATA_INPUT_FILE) and os.path.exists(LEGACY_METADATA_INPUT_FILE):
    METADATA_INPUT_FILE = LEGACY_METADATA_INPUT_FILE

# Rate limiting
REQUESTS_PER_MINUTE = 100
REQUEST_DELAY = 60 / REQUESTS_PER_MINUTE
CHECKPOINT_INTERVAL = 10
COOLDOWN_AFTER_BATCH = 5
MAX_PAGES = None
FORCE_REFRESH = os.getenv("FORCE_REFRESH", "0") == "1"

# Filtering
# Example: ACTIVITY_TYPES=sale,mint,transfer
# ✏️ EDIT HERE — types to keep (comma-separated). Leave empty to keep all.
ACTIVITY_TYPES_RAW = os.getenv("ACTIVITY_TYPES", "sale,mint,transfer")


def parse_activity_types(value: str) -> Set[str]:
    items = [v.strip().lower() for v in value.split(",") if v.strip()]
    return set(items)


FILTERED_TYPES = parse_activity_types(ACTIVITY_TYPES_RAW)

# Maps user-specified type names → OpenSea API event_type values
# "mint" is derived from transfer-from-0x0, so it maps to "transfer"
_OPENSEA_API_TYPE_MAP = {
    "sale": "sale",
    "transfer": "transfer",
    "mint": "transfer",   # mints are reclassified from transfer
    "listing": "listing",
    "offer": "order",
    "cancel": "cancel",
    "order": "order",
}

def _get_api_types_to_fetch() -> Set[str]:
    """Returns OpenSea API event_type values needed to satisfy FILTERED_TYPES."""
    if not FILTERED_TYPES:
        return set()  # empty = fetch all
    return {_OPENSEA_API_TYPE_MAP[t] for t in FILTERED_TYPES if t in _OPENSEA_API_TYPE_MAP}

API_TYPES_TO_FETCH = _get_api_types_to_fetch()


def resolve_collection_details() -> tuple[str, str]:
    env_contract = os.getenv("COLLECTION_CONTRACT")
    env_name = os.getenv("COLLECTION_NAME")
    if env_contract and env_name:
        return env_contract, env_name

    if os.path.exists(LEVEL1_FILE):
        try:
            df_level1 = pd.read_csv(LEVEL1_FILE)
            if not df_level1.empty:
                collection_name = env_name or str(df_level1.iloc[0].get("name") or COLLECTION_NAME)
                contracts_raw = df_level1.iloc[0].get("contracts")
                contracts = ast.literal_eval(str(contracts_raw))
                if isinstance(contracts, list) and contracts:
                    address = contracts[0].get("address")
                    if address:
                        return address, collection_name
        except Exception:
            pass

    url = f"{BASE_URL}/collections/{COLLECTION_SLUG}"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    collection = response.json()
    contracts = collection.get("contracts", [])
    if not contracts:
        raise ValueError(f"Could not resolve contract for collection '{COLLECTION_SLUG}'")

    address = contracts[0].get("address")
    if not address:
        raise ValueError(f"Could not resolve contract address for collection '{COLLECTION_SLUG}'")

    resolved_name = env_name or str(collection.get("name") or COLLECTION_NAME)
    return address, resolved_name


COLLECTION_CONTRACT, COLLECTION_NAME = resolve_collection_details()


def load_checkpoint() -> Dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed_ids": [], "last_index": 0, "total_activities": 0}


def save_checkpoint(checkpoint: Dict):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2)


def log_error(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def load_identifiers() -> tuple[List[str], str]:
    if not os.path.exists(METADATA_INPUT_FILE):
        raise FileNotFoundError(f"No metadata file found. Expected: {METADATA_INPUT_FILE}")

    df_input = pd.read_csv(METADATA_INPUT_FILE)
    if "identifier" not in df_input.columns:
        raise ValueError(f"'identifier' column not found in {METADATA_INPUT_FILE}")

    return df_input["identifier"].astype(str).tolist(), METADATA_INPUT_FILE


def format_event_timestamp(value) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        pass

    try:
        ts = float(raw)
        if ts > 1e12:
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, TypeError, OSError, OverflowError):
        return raw


def _fetch_events_for_type(token_id: str, event_type: str | None, max_retries: int = 3) -> List[Dict]:
    """Fetch all pages of events for a single event_type (or all types if event_type is None)."""
    all_events: List[Dict] = []
    next_cursor = None
    page = 0
    retries = 0

    while True:
        if MAX_PAGES is not None and page >= MAX_PAGES:
            break

        url = f"{BASE_URL}/events/chain/{CHAIN}/contract/{COLLECTION_CONTRACT}/nfts/{token_id}"
        params = {"limit": 50}
        if event_type:
            params["event_type"] = event_type
        if next_cursor:
            params["next"] = next_cursor

        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()

            events = data.get("asset_events", [])
            all_events.extend(events)
            next_cursor = data.get("next")

            if not next_cursor:
                break

            page += 1
            retries = 0
            time.sleep(REQUEST_DELAY)

        except requests.exceptions.Timeout:
            retries += 1
            if retries < max_retries:
                wait_time = 2 ** retries
                print(f"      Timeout (attempt {retries}/{max_retries}), waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            log_error(f"Timeout fetching events for token {token_id} after {max_retries} retries")
            break

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                print("      Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                continue
            status = e.response.status_code if e.response is not None else "unknown"
            log_error(f"HTTP {status} for token {token_id}: {str(e)}")
            break

        except Exception as e:
            log_error(f"Error fetching events for token {token_id}: {str(e)}")
            break

    return all_events


def get_nft_events(token_id: str, max_retries: int = 3) -> List[Dict]:
    """Fetch only the event types needed. Uses API-level filtering for speed."""
    if not API_TYPES_TO_FETCH:
        # No filter — fetch everything
        return _fetch_events_for_type(token_id, None, max_retries)

    seen_txns: Set[str] = set()
    combined: List[Dict] = []
    for api_type in API_TYPES_TO_FETCH:
        for event in _fetch_events_for_type(token_id, api_type, max_retries):
            # Deduplicate by transaction hash; fall back to object id
            key = str(event.get("transaction") or id(event))
            if key not in seen_txns:
                seen_txns.add(key)
                combined.append(event)
    return combined


def parse_event_to_activity(event: Dict, identifier: str) -> Dict:
    raw_event_type = str(event.get("event_type", "unknown") or "unknown")
    order_type = str(event.get("order_type", "") or "")
    event_type = order_type if raw_event_type == "order" and order_type else raw_event_type

    payment = event.get("payment")
    price_eth = 0.0
    if payment:
        try:
            quantity = float(payment.get("quantity", 0))
            decimals = int(payment.get("decimals", 18))
            if quantity > 0 and decimals > 0:
                price_eth = quantity / (10 ** decimals)
        except (ValueError, TypeError):
            price_eth = 0.0

    from_address = event.get("from_address") or "0x0"
    to_address = event.get("to_address") or "0x0"

    normalized_from = str(from_address).strip().lower()
    if event_type == "transfer" and normalized_from in {"0x0", "0x0000000000000000000000000000000000000000"}:
        event_type = "mint"

    return {
        "identifier": str(identifier),
        "name": f"{COLLECTION_NAME} #{identifier}",
        "raw_event_type": raw_event_type,
        "event_type": event_type,
        "order_type": order_type,
        "price_eth": round(price_eth, 6),
        "from": from_address,
        "to": to_address,
        "timestamp": format_event_timestamp(event.get("event_timestamp")),
        "tx_hash": str(event.get("transaction", "")),
    }


def activity_matches_filter(activity: Dict) -> bool:
    if not FILTERED_TYPES:
        return True

    candidates = {
        str(activity.get("event_type", "")).lower(),
        str(activity.get("raw_event_type", "")).lower(),
        str(activity.get("order_type", "")).lower(),
    }
    candidates.discard("")
    return any(c in FILTERED_TYPES for c in candidates)


def main():
    print("\n" + "=" * 80)
    print(f"{COLLECTION_NAME.upper()} FILTERED ACTIVITY HISTORY EXTRACTION")
    print("=" * 80)
    if FILTERED_TYPES:
        print(f"API-level filter: requesting only {', '.join(sorted(API_TYPES_TO_FETCH))} from OpenSea")
        print(f"Post-fetch filter: keeping {', '.join(sorted(FILTERED_TYPES))}")
    else:
        print("Filtering disabled. Fetching all event types.")
    print("=" * 80 + "\n")

    identifiers, input_file = load_identifiers()
    print(f"Loaded {len(identifiers)} NFTs from {input_file}")

    if os.path.exists(OUTPUT_FILE) and not FORCE_REFRESH and not os.path.exists(CHECKPOINT_FILE):
        print(f"Skipped Level 3 Filtered (already exists): {OUTPUT_FILE}")
        return

    checkpoint = load_checkpoint()
    processed_ids = set(checkpoint.get("processed_ids", []))
    start_index = int(checkpoint.get("last_index", 0))
    kept_activities: List[Dict] = []

    for idx in range(start_index, len(identifiers)):
        token_id = identifiers[idx]
        if token_id in processed_ids:
            continue

        print(f"[{idx + 1}/{len(identifiers)}] {COLLECTION_NAME} #{token_id}")
        events = get_nft_events(token_id)

        kept_for_token = 0
        for event in events:
            activity = parse_event_to_activity(event, token_id)
            if activity_matches_filter(activity):
                kept_activities.append(activity)
                kept_for_token += 1

        print(f"    Raw events: {len(events)} | Kept after filter: {kept_for_token}")

        processed_ids.add(token_id)
        save_checkpoint({
            "processed_ids": list(processed_ids),
            "last_index": idx + 1,
            "total_activities": len(kept_activities),
        })

        if (idx + 1) % CHECKPOINT_INTERVAL == 0 and kept_activities:
            pd.DataFrame(kept_activities).sort_values(["identifier", "timestamp"], ascending=[True, True]).to_csv(TEMP_FILE, index=False)
            time.sleep(COOLDOWN_AFTER_BATCH)
        else:
            time.sleep(REQUEST_DELAY)

    if not kept_activities:
        print("No activities matched the selected filter.")
        return

    df = pd.DataFrame(kept_activities).sort_values(["identifier", "timestamp"], ascending=[True, True])
    df.to_csv(OUTPUT_FILE, index=False)

    summary = {
        "extraction_date": datetime.now().isoformat(),
        "filtered_event_types": sorted(list(FILTERED_TYPES)),
        "total_activities": len(df),
        "unique_nfts_with_activity": int(df["identifier"].nunique()),
        "event_type_distribution": df["event_type"].value_counts().to_dict(),
    }
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
    if os.path.exists(TEMP_FILE):
        os.remove(TEMP_FILE)

    print("\nFiltered extraction complete.")
    print(f"Saved dataset: {OUTPUT_FILE}")
    print(f"Saved summary: {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
