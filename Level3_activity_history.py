"""
Level 3: Activity History Extraction
Extracts full activity history (mint, sale, transfer, listing, offer, etc.) for all NFTs in the collection.
"""
import requests
import pandas as pd
import time
from datetime import datetime, timezone
import json
import os
import ast
import re
from typing import List, Dict

# =========================
# PIPELINE CONFIGURATION (Level 3)
# =========================
API_KEY = os.getenv("OPENSEA_API_KEY")
if not API_KEY:
    raise RuntimeError("OPENSEA_API_KEY environment variable not set. Please set it via pipeline_config.json or environment.")
BASE_URL = "https://api.opensea.io/api/v2"
HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
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
OUTPUT_FILE = os.path.join(COLLECTION_DIR, "Level3_complete_activity_history.csv")
CHECKPOINT_FILE = os.path.join(COLLECTION_DIR, "Level3_extraction_checkpoint.json")
SUMMARY_FILE = os.path.join(COLLECTION_DIR, "Level3_extraction_summary.json")
ERROR_LOG_FILE = os.path.join(COLLECTION_DIR, "Level3_extraction_errors.log")
TEMP_FILE = os.path.join(COLLECTION_DIR, "TEMP_Level3_complete_activity_history.csv")

# Backward-compatible fallbacks for older filenames.
LEGACY_METADATA_INPUT_FILE = os.path.join(COLLECTION_DIR, "level2_all_nfts_metadata.csv")
LEGACY_OUTPUT_FILE = os.path.join(COLLECTION_DIR, f"{COLLECTION_SLUG}_complete_activity_history.csv")
if not os.path.exists(METADATA_INPUT_FILE) and os.path.exists(LEGACY_METADATA_INPUT_FILE):
    METADATA_INPUT_FILE = LEGACY_METADATA_INPUT_FILE
if not os.path.exists(OUTPUT_FILE) and os.path.exists(LEGACY_OUTPUT_FILE):
    OUTPUT_FILE = LEGACY_OUTPUT_FILE

# Rate limiting
REQUESTS_PER_MINUTE = 100
REQUEST_DELAY = 60 / REQUESTS_PER_MINUTE  # Seconds between requests
CHECKPOINT_INTERVAL = 10  # Save progress every N NFTs
COOLDOWN_AFTER_BATCH = 5  # Seconds to wait after checkpoint
MAX_PAGES = None  # Set to an int to cap pages per NFT, or None for full pagination.
FORCE_REFRESH = os.getenv("FORCE_REFRESH", "0") == "1"


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


# ============================================================================
# CHECKPOINT MANAGEMENT
# ============================================================================

def load_checkpoint() -> Dict:
    """Load extraction progress checkpoint"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"processed_ids": [], "last_index": 0, "total_activities": 0}


def save_checkpoint(checkpoint: Dict):
    """Save extraction progress checkpoint"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)


def log_error(message: str):
    """Log errors to file"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(ERROR_LOG_FILE, 'a') as f:
        f.write(f"[{timestamp}] {message}\n")


def load_identifiers() -> tuple[list[str], str]:
    """Load NFT identifiers from metadata."""
    if not os.path.exists(METADATA_INPUT_FILE):
        raise FileNotFoundError(
            f"No metadata file found. Expected: {METADATA_INPUT_FILE}"
        )

    df_input = pd.read_csv(METADATA_INPUT_FILE)
    if "identifier" not in df_input.columns:
        raise ValueError(f"'identifier' column not found in {METADATA_INPUT_FILE}")

    identifiers = df_input["identifier"].astype(str).tolist()
    return identifiers, METADATA_INPUT_FILE


def format_event_timestamp(value) -> str:
    """Return a normalized UTC datetime string: YYYY-MM-DD HH:MM:SS UTC."""
    if value is None:
        return ""

    raw = str(value).strip()
    if not raw:
        return ""

    # Try ISO-8601 timestamps first.
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        pass

    # Fallback for Unix timestamps (seconds or milliseconds).
    try:
        ts = float(raw)
        if ts > 1e12:
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, TypeError, OSError, OverflowError):
        return raw


# ============================================================================
# DATA EXTRACTION
# ============================================================================

def get_nft_events(token_id: str, max_retries: int = 3) -> List[Dict]:
    """
    Fetch ALL events for a specific collection token
    Includes: mint, transfer, sale, listing, bid, cancel, etc.
    """
    all_events = []
    next_cursor = None
    page = 0
    retries = 0

    while True:
        if MAX_PAGES is not None and page >= MAX_PAGES:
            break

        url = f"{BASE_URL}/events/chain/{CHAIN}/contract/{COLLECTION_CONTRACT}/nfts/{token_id}"
        params = {"limit": 50}

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
            retries = 0  # Reset retry counter on success
            time.sleep(REQUEST_DELAY)

        except requests.exceptions.Timeout:
            retries += 1
            if retries < max_retries:
                wait_time = 2 ** retries  # Exponential backoff
                print(f"      Timeout (attempt {retries}/{max_retries}), waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                log_error(f"Timeout fetching events for token {token_id} after {max_retries} retries")
                break

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"      Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                continue
            else:
                log_error(f"HTTP {e.response.status_code} for token {token_id}: {str(e)}")
                break

        except Exception as e:
            log_error(f"Error fetching events for token {token_id}: {str(e)}")
            break

    return all_events


def parse_event_to_activity(event: Dict, identifier: str) -> Dict:
    """
    Parse OpenSea event into standardized activity format
    Captures all event types with full detail
    """
    # OpenSea v2 often returns listing/offer as event_type='order' with details in order_type
    raw_event_type = event.get("event_type", "unknown")
    order_type = event.get("order_type", "")
    if raw_event_type == "order" and order_type:
        event_type = order_type
    else:
        event_type = raw_event_type

    # Price extraction
    price_eth = 0.0
    payment = event.get("payment")
    if payment:
        try:
            quantity = float(payment.get("quantity", 0))
            decimals = int(payment.get("decimals", 18))
            if quantity > 0 and decimals > 0:
                price_eth = quantity / (10 ** decimals)
        except (ValueError, TypeError):
            price_eth = 0.0

    # Address extraction
    from_address = event.get("from_address") or "0x0"
    to_address = event.get("to_address") or "0x0"

    def is_zero_address(addr: str) -> bool:
        if not addr:
            return True
        normalized = str(addr).strip().lower()
        return normalized in {"0x0", "0x0000000000000000000000000000000000000000"}

    # Derive mint activity from transfer events originating at the zero address.
    if event_type == "transfer" and is_zero_address(from_address):
        event_type = "mint"

    # Timestamp parsing
    timestamp = format_event_timestamp(event.get("event_timestamp"))

    # Preserve full transaction hash from multiple OpenSea response shapes.
    transaction = event.get("transaction")
    tx_hash = ""
    if isinstance(transaction, dict):
        tx_hash = str(
            transaction.get("transaction_hash")
            or transaction.get("hash")
            or transaction.get("id")
            or ""
        )
    elif transaction is not None:
        tx_hash = str(transaction)

    return {
        "identifier": str(identifier),
        "name": f"{COLLECTION_NAME} #{identifier}",
        "raw_event_type": raw_event_type,
        "event_type": event_type,
        "order_type": order_type,
        "price_eth": round(price_eth, 6),
        "from": str(from_address),
        "to": str(to_address),
        "timestamp": timestamp,
        "tx_hash": tx_hash
    }


# ============================================================================
# MAIN EXTRACTION LOGIC
# ============================================================================

def main():
    print("\n" + "="*80)
    print(f"{COLLECTION_NAME.upper()} COMPLETE ACTIVITY HISTORY EXTRACTION")
    print("="*80)
    print("Extracting FULL history: mint -> transfers -> sales -> listings -> all events")
    print("="*80 + "\n")

    # Load input
    try:
        identifiers, input_file = load_identifiers()
        print(f"Loaded {len(identifiers)} NFTs from {input_file}")
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")
        print("Please ensure the metadata exists with an 'identifier' column.")
        return

    if os.path.exists(OUTPUT_FILE) and not FORCE_REFRESH and not os.path.exists(CHECKPOINT_FILE):
        print(f"Skipped Level 3 (already exists): {OUTPUT_FILE}")
        return

    # Load checkpoint
    checkpoint = load_checkpoint()
    processed_ids = set(checkpoint.get("processed_ids", []))
    start_index = checkpoint.get("last_index", 0)

    if processed_ids:
        print(f"Resuming from checkpoint:")
        print(f"   - Already processed: {len(processed_ids)} NFTs")
        print(f"   - Starting from index: {start_index}")

    # Initialize error log
    if not os.path.exists(ERROR_LOG_FILE):
        with open(ERROR_LOG_FILE, 'w') as f:
            f.write(f"Extraction started at {datetime.now()}\n")

    print(f"\n{'='*80}")
    print(f"EXTRACTION START")
    print(f"{'='*80}\n")

    all_activities = []
    total = len(identifiers)
    start_time = time.time()
    failed_ids = []

    # Process each NFT
    for idx in range(start_index, total):
        token_id = identifiers[idx]

        if token_id in processed_ids:
            continue

        # Progress metrics
        progress_pct = ((idx + 1) / total) * 100
        elapsed = time.time() - start_time
        processed_count = idx - start_index + 1
        rate = processed_count / elapsed if elapsed > 0 else 0
        remaining = total - idx - 1
        eta = remaining / rate if rate > 0 else 0

        print(f"[{idx+1}/{total}] ({progress_pct:.1f}%) {COLLECTION_NAME} #{token_id}")
        print(f"    Processed: {processed_count} | Rate: {rate:.2f}/min | ETA: {eta/60:.1f}min")

        # Fetch events
        print(f"    Fetching complete event history...")
        events = get_nft_events(token_id)

        if len(events) == 0:
            print(f"    No events found")
            failed_ids.append(token_id)
        else:
            print(f"    Found {len(events)} events")

            # Parse events
            for event in events:
                activity = parse_event_to_activity(event, token_id)
                all_activities.append(activity)

        # Update checkpoint
        processed_ids.add(token_id)
        checkpoint = {
            "processed_ids": list(processed_ids),
            "last_index": idx + 1,
            "total_activities": len(all_activities)
        }
        save_checkpoint(checkpoint)

        # Periodic save
        if (idx + 1) % CHECKPOINT_INTERVAL == 0:
            if all_activities:
                df_temp = pd.DataFrame(all_activities)
                df_temp = df_temp.sort_values(["identifier", "timestamp"], ascending=[True, True])
                df_temp.to_csv(TEMP_FILE, index=False)
                print(f"    Progress saved: {len(all_activities)} activities")

            print(f"    Cooling down for {COOLDOWN_AFTER_BATCH}s...")
            time.sleep(COOLDOWN_AFTER_BATCH)
        else:
            time.sleep(REQUEST_DELAY)

        print()

    # Save final results
    if all_activities:
        print("\n" + "="*80)
        print("SAVING FINAL DATASET")
        print("="*80 + "\n")

        df = pd.DataFrame(all_activities)
        df = df.sort_values(["identifier", "timestamp"], ascending=[True, True])
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"Saved to: {OUTPUT_FILE}")

        # Generate summary statistics
        print("\nGenerating summary statistics...")

        summary = {
            "extraction_date": datetime.now().isoformat(),
            "total_activities": len(all_activities),
            "unique_nfts_with_activity": df['identifier'].nunique(),
            "total_nfts_processed": len(processed_ids),
            "nfts_with_no_events": len(failed_ids),
            "extraction_time_minutes": (time.time() - start_time) / 60,
            "event_type_distribution": df['event_type'].value_counts().to_dict()
        }

        # Price statistics
        price_df = df[df['price_eth'] > 0]
        if len(price_df) > 0:
            summary["price_statistics"] = {
                "total_volume_eth": float(price_df['price_eth'].sum()),
                "transactions_with_price": len(price_df),
                "avg_price_eth": float(price_df['price_eth'].mean()),
                "median_price_eth": float(price_df['price_eth'].median()),
                "min_price_eth": float(price_df['price_eth'].min()),
                "max_price_eth": float(price_df['price_eth'].max())
            }

        # Activity statistics
        activity_stats = df.groupby('identifier').size()
        summary["activity_statistics"] = {
            "avg_events_per_nft": float(activity_stats.mean()),
            "median_events_per_nft": float(activity_stats.median()),
            "min_events": int(activity_stats.min()),
            "max_events": int(activity_stats.max())
        }

        with open(SUMMARY_FILE, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"Summary saved to: {SUMMARY_FILE}")

        # Clean up
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
        if os.path.exists(TEMP_FILE):
            os.remove(TEMP_FILE)

        # Final report
        print("\n" + "="*80)
        print("EXTRACTION COMPLETE!")
        print("="*80)
        print(f"Total activities: {len(all_activities)}")
        print(f"NFTs with activity: {df['identifier'].nunique()}")
        print(f"Total time: {(time.time() - start_time)/60:.1f} minutes")
        print(f"Average rate: {len(processed_ids)/(elapsed/60):.2f} NFTs/minute")

        if failed_ids:
            print(f"\n{len(failed_ids)} NFTs had no events")

        print(f"\nEvent Type Breakdown:")
        for event_type, count in df['event_type'].value_counts().items():
            print(f"   {event_type}: {count:,}")

        print("\n" + "="*80)
        print("Your complete activity history dataset is ready!")
        print(f"File: {OUTPUT_FILE}")
        print("="*80 + "\n")

    else:
        print("\nNo activities extracted!")


if __name__ == "__main__":
    main()
