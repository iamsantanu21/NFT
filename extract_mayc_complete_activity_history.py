"""
FINAL PRODUCTION SCRIPT: Mutant Ape Yacht Club Complete Activity History Extraction
Extracts full activity history from mint to present for all MAYC NFTs.
Includes: mint, transfers, sales, listings, bids, and other events.
"""

import argparse
import ast
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

# ============================================================================
# CONFIGURATION
# ============================================================================
API_KEY = os.getenv("OPENSEA_API_KEY", "5575c781fdb2424f8e5aa693c8f68a35")
BASE_URL = "https://api.opensea.io/api/v2"
HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY,
}

CHAIN = "ethereum"

COLLECTION_DIR = Path(r"D:\NFT\collections\mutant-ape-yacht-club")
LEVEL1_FILE = COLLECTION_DIR / "Level1_collection_info.csv"
LEVEL2_FILE = COLLECTION_DIR / "Level2_all_nfts_metadata.csv"
OUTPUT_FILE = COLLECTION_DIR / "Level3_complete_activity_history.csv"
CHECKPOINT_FILE = COLLECTION_DIR / "Level3_extraction_checkpoint.json"
SUMMARY_FILE = COLLECTION_DIR / "extraction_summary.json"
ERROR_LOG_FILE = COLLECTION_DIR / "Level3_extraction_errors.log"

# Rate limiting
REQUESTS_PER_MINUTE = 100
REQUEST_DELAY = 60 / REQUESTS_PER_MINUTE
CHECKPOINT_INTERVAL = 10
COOLDOWN_AFTER_BATCH = 5
ALLOWED_ACTIVITY_TYPES = {"sale", "transfer", "mint"}


def parse_contract_from_level1(df_level1: pd.DataFrame) -> str:
    """Extract contract address from Level1 contracts field."""
    contracts_raw = str(df_level1.loc[0, "contracts"])
    contracts = ast.literal_eval(contracts_raw)
    if not contracts or not isinstance(contracts, list):
        raise ValueError("Level1 contracts field is empty or invalid")

    contract = str(contracts[0].get("address", "")).strip().lower()
    if not contract.startswith("0x"):
        raise ValueError("Contract address in Level1 is invalid")
    return contract


def load_collection_context() -> Dict[str, str | int | List[str]]:
    """Load and validate collection metadata from Level1 and Level2 CSV files."""
    if not LEVEL1_FILE.exists():
        raise FileNotFoundError(f"Level1 file not found: {LEVEL1_FILE}")
    if not LEVEL2_FILE.exists():
        raise FileNotFoundError(f"Level2 file not found: {LEVEL2_FILE}")

    df_level1 = pd.read_csv(LEVEL1_FILE)
    df_level2 = pd.read_csv(LEVEL2_FILE)

    if df_level1.empty:
        raise ValueError("Level1 CSV is empty")
    if df_level2.empty:
        raise ValueError("Level2 CSV is empty")

    required_l1 = {"collection", "name", "contracts", "total_supply"}
    required_l2 = {"identifier", "collection", "contract"}
    missing_l1 = required_l1 - set(df_level1.columns)
    missing_l2 = required_l2 - set(df_level2.columns)
    if missing_l1:
        raise ValueError(f"Missing required Level1 columns: {sorted(missing_l1)}")
    if missing_l2:
        raise ValueError(f"Missing required Level2 columns: {sorted(missing_l2)}")

    collection_slug = str(df_level1.loc[0, "collection"])
    collection_name = str(df_level1.loc[0, "name"])
    contract_from_level1 = parse_contract_from_level1(df_level1)
    expected_supply = int(df_level1.loc[0, "total_supply"])

    identifiers = df_level2["identifier"].astype(str).tolist()
    level2_contracts = (
        df_level2["contract"].dropna().astype(str).str.strip().str.lower().unique().tolist()
    )
    level2_collections = df_level2["collection"].dropna().astype(str).str.strip().unique().tolist()

    if len(level2_contracts) != 1:
        raise ValueError(f"Expected 1 unique contract in Level2, found {len(level2_contracts)}")

    if level2_contracts[0] != contract_from_level1:
        raise ValueError(
            "Contract mismatch between Level1 and Level2: "
            f"{contract_from_level1} != {level2_contracts[0]}"
        )

    if collection_slug not in level2_collections:
        raise ValueError(
            f"Collection mismatch: Level1='{collection_slug}' not present in Level2 collections"
        )

    if len(identifiers) != expected_supply:
        print(
            "Warning: Level2 row count does not match Level1 total_supply "
            f"({len(identifiers)} != {expected_supply})"
        )

    return {
        "collection_slug": collection_slug,
        "collection_name": collection_name,
        "contract": contract_from_level1,
        "expected_supply": expected_supply,
        "identifiers": identifiers,
    }


def load_checkpoint() -> Dict:
    """Load extraction progress checkpoint."""
    if CHECKPOINT_FILE.exists():
        with CHECKPOINT_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed_ids": [], "last_index": 0, "total_activities": 0}


def save_checkpoint(checkpoint: Dict) -> None:
    """Save extraction progress checkpoint."""
    with CHECKPOINT_FILE.open("w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2)


def log_error(message: str) -> None:
    """Log errors to file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with ERROR_LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def get_nft_events(contract: str, token_id: str, max_retries: int = 3) -> List[Dict]:
    """Fetch all events for a specific MAYC token."""
    all_events: List[Dict] = []
    next_cursor = None
    max_pages = 50
    page = 0
    retries = 0

    while page < max_pages:
        url = f"{BASE_URL}/events/chain/{CHAIN}/contract/{contract}/nfts/{token_id}"
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

        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                print("      Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                continue
            code = exc.response.status_code if exc.response is not None else "unknown"
            log_error(f"HTTP {code} for token {token_id}: {str(exc)}")
            break

        except Exception as exc:  # pylint: disable=broad-except
            log_error(f"Error fetching events for token {token_id}: {str(exc)}")
            break

    return all_events


def classify_activity_type(raw_event_type: str, from_address: str, has_payment: bool) -> Optional[str]:
    """Map raw OpenSea event data into one of sale/transfer/mint, else return None."""
    raw = (raw_event_type or "").strip().lower()
    from_addr = (from_address or "").strip().lower()

    sale_aliases = {"sale", "item_sold", "successful", "successful_transfer"}
    transfer_aliases = {"transfer", "item_transferred", "transfer_single", "transfer_batch"}
    mint_aliases = {"mint", "item_minted"}

    if raw in sale_aliases:
        return "sale"

    if raw in mint_aliases:
        return "mint"

    if raw in transfer_aliases:
        if from_addr in {"", "0x0", "0x0000000000000000000000000000000000000000"}:
            return "mint"
        if has_payment:
            return "sale"
        return "transfer"

    # Some APIs encode mint as transfer from zero address without explicit transfer type.
    if from_addr in {"0x0", "0x0000000000000000000000000000000000000000"} and not has_payment:
        return "mint"

    return None


def parse_event_to_activity(event: Dict, identifier: str, nft_label: str) -> Optional[Dict]:
    """Parse OpenSea event into a standardized activity row."""
    raw_event_type = event.get("event_type", "unknown")

    price_eth = 0.0
    payment = event.get("payment")
    has_payment = False
    if payment:
        try:
            quantity = float(payment.get("quantity", 0))
            decimals = int(payment.get("decimals", 18))
            if quantity > 0 and decimals > 0:
                price_eth = quantity / (10 ** decimals)
                has_payment = True
        except (ValueError, TypeError):
            price_eth = 0.0

    from_address = event.get("from_address") or "0x0"
    to_address = event.get("to_address") or "0x0"

    activity_type = classify_activity_type(raw_event_type, from_address, has_payment)
    if activity_type is None:
        return None

    def format_address(addr: str) -> str:
        if not addr or addr == "0x0":
            return "0x0"
        if len(addr) > 12:
            return f"{addr[:6]}...{addr[-4:]}"
        return addr

    timestamp_str = event.get("event_timestamp", "")
    if timestamp_str:
        try:
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, AttributeError):
            try:
                timestamp = str(int(float(timestamp_str)))
            except (ValueError, TypeError):
                timestamp = str(timestamp_str)
    else:
        timestamp = ""

    tx_hash = event.get("transaction", "")
    if isinstance(tx_hash, dict):
        tx_hash = tx_hash.get("transaction_hash", "")
    if isinstance(tx_hash, str) and len(tx_hash) > 16:
        tx_hash = f"{tx_hash[:10]}..."

    return {
        "identifier": str(identifier),
        "name": f"{nft_label} #{identifier}",
        "event_type": activity_type,
        "raw_event_type": raw_event_type,
        "price_eth": round(price_eth, 6),
        "from": format_address(from_address),
        "to": format_address(to_address),
        "timestamp": timestamp,
        "tx_hash": tx_hash,
    }


def main(max_nfts: int | None = None, ignore_checkpoint: bool = False) -> None:
    if not API_KEY:
        print("Error: OpenSea API key is missing. Set OPENSEA_API_KEY.")
        return

    try:
        context = load_collection_context()
    except (FileNotFoundError, ValueError, SyntaxError) as exc:
        print(f"Error while loading collection context: {exc}")
        return

    collection_name = str(context["collection_name"])
    contract = str(context["contract"])
    expected_supply = int(context["expected_supply"])
    identifiers = list(context["identifiers"])
    nft_label = collection_name.replace(" Yacht Club", "").replace(" Collection", "").strip()

    print("\n" + "=" * 80)
    print(f"{collection_name.upper()} COMPLETE ACTIVITY HISTORY EXTRACTION")
    print("=" * 80)
    print("Extracting full history: mint -> transfers -> sales -> listings -> all events")
    print("=" * 80 + "\n")
    print(f"Collection slug: {context['collection_slug']}")
    print(f"Contract: {contract}")
    print(f"Level1 expected supply: {expected_supply}")

    if max_nfts is not None:
        identifiers = identifiers[:max_nfts]

    print(f"Loaded {len(identifiers)} NFTs from {LEVEL2_FILE}")

    if ignore_checkpoint:
        checkpoint = {"processed_ids": [], "last_index": 0, "total_activities": 0}
    else:
        checkpoint = load_checkpoint()
    processed_ids = set(checkpoint.get("processed_ids", []))
    start_index = checkpoint.get("last_index", 0)

    if processed_ids:
        print("Resuming from checkpoint:")
        print(f"   - Already processed: {len(processed_ids)} NFTs")
        print(f"   - Starting from index: {start_index}")

    if not ERROR_LOG_FILE.exists():
        with ERROR_LOG_FILE.open("w", encoding="utf-8") as f:
            f.write(f"Extraction started at {datetime.now()}\n")

    print(f"\n{'=' * 80}")
    print("EXTRACTION START")
    print(f"{'=' * 80}\n")

    all_activities: List[Dict] = []
    skipped_events = 0
    temp_file = OUTPUT_FILE.parent / f"TEMP_{OUTPUT_FILE.name}"

    # Resume-safe behavior: restore prior rows if a temp snapshot exists.
    if not ignore_checkpoint and temp_file.exists():
        try:
            existing_temp_df = pd.read_csv(temp_file)
            if "event_type" in existing_temp_df.columns:
                existing_temp_df = existing_temp_df[
                    existing_temp_df["event_type"].isin(ALLOWED_ACTIVITY_TYPES)
                ]
            all_activities = existing_temp_df.to_dict(orient="records")
            print(f"Recovered {len(all_activities)} existing activities from {temp_file}")
        except Exception as exc:  # pylint: disable=broad-except
            log_error(f"Failed to restore temporary activities from {temp_file}: {exc}")
    total = len(identifiers)
    start_time = time.time()
    failed_ids: List[str] = []

    for idx in range(start_index, total):
        token_id = identifiers[idx]

        if token_id in processed_ids:
            continue

        progress_pct = ((idx + 1) / total) * 100 if total else 100
        elapsed = time.time() - start_time
        processed_count = idx - start_index + 1
        rate_per_sec = processed_count / elapsed if elapsed > 0 else 0
        remaining = total - idx - 1
        eta_seconds = remaining / rate_per_sec if rate_per_sec > 0 else 0

        print(f"[{idx + 1}/{total}] ({progress_pct:.1f}%) {collection_name} NFT #{token_id}")
        print(
            f"    Processed: {processed_count} | "
            f"Rate: {rate_per_sec * 60:.2f}/min | ETA: {eta_seconds / 60:.1f}min"
        )

        print("    Fetching complete event history...")
        events = get_nft_events(contract, token_id)

        if len(events) == 0:
            print("    No events found")
            failed_ids.append(token_id)
        else:
            print(f"    Found {len(events)} events")
            for event in events:
                activity = parse_event_to_activity(event, token_id, nft_label)
                if activity is None:
                    skipped_events += 1
                    continue
                all_activities.append(activity)

        processed_ids.add(token_id)
        checkpoint = {
            "processed_ids": list(processed_ids),
            "last_index": idx + 1,
            "total_activities": len(all_activities),
        }
        save_checkpoint(checkpoint)

        if (idx + 1) % CHECKPOINT_INTERVAL == 0:
            if all_activities:
                df_temp = pd.DataFrame(all_activities)
                df_temp = df_temp.sort_values(["identifier", "timestamp"], ascending=[True, True])
                df_temp.to_csv(temp_file, index=False)
                print(f"    Progress saved: {len(all_activities)} activities")

            print(f"    Cooling down for {COOLDOWN_AFTER_BATCH}s...")
            time.sleep(COOLDOWN_AFTER_BATCH)
        else:
            time.sleep(REQUEST_DELAY)

        print()

    if not all_activities:
        empty_columns = [
            "identifier",
            "name",
            "event_type",
            "raw_event_type",
            "price_eth",
            "from",
            "to",
            "timestamp",
            "tx_hash",
        ]
        pd.DataFrame(columns=empty_columns).to_csv(OUTPUT_FILE, index=False)

        summary = {
            "extraction_date": datetime.now().isoformat(),
            "total_activities": 0,
            "unique_nfts_with_activity": 0,
            "total_nfts_processed": len(processed_ids),
            "nfts_with_no_events": len(failed_ids),
            "extraction_time_minutes": (time.time() - start_time) / 60,
            "event_type_distribution": {},
            "skipped_events": skipped_events,
            "allowed_event_types": sorted(ALLOWED_ACTIVITY_TYPES),
        }
        with SUMMARY_FILE.open("w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        print("\nNo mint/transfer/sale activities extracted.")
        print(f"Saved empty filtered dataset to: {OUTPUT_FILE}")
        print(f"Summary saved to: {SUMMARY_FILE}")
        return

    print("\n" + "=" * 80)
    print("SAVING FINAL DATASET")
    print("=" * 80 + "\n")

    df = pd.DataFrame(all_activities)
    df = df.sort_values(["identifier", "timestamp"], ascending=[True, True])
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved to: {OUTPUT_FILE}")

    print("\nGenerating summary statistics...")
    summary = {
        "extraction_date": datetime.now().isoformat(),
        "total_activities": len(all_activities),
        "unique_nfts_with_activity": int(df["identifier"].nunique()),
        "total_nfts_processed": len(processed_ids),
        "nfts_with_no_events": len(failed_ids),
        "extraction_time_minutes": (time.time() - start_time) / 60,
        "event_type_distribution": df["event_type"].value_counts().to_dict(),
        "skipped_events": skipped_events,
        "allowed_event_types": sorted(ALLOWED_ACTIVITY_TYPES),
    }

    price_df = df[df["price_eth"] > 0]
    if len(price_df) > 0:
        summary["price_statistics"] = {
            "total_volume_eth": float(price_df["price_eth"].sum()),
            "transactions_with_price": int(len(price_df)),
            "avg_price_eth": float(price_df["price_eth"].mean()),
            "median_price_eth": float(price_df["price_eth"].median()),
            "min_price_eth": float(price_df["price_eth"].min()),
            "max_price_eth": float(price_df["price_eth"].max()),
        }

    activity_stats = df.groupby("identifier").size()
    summary["activity_statistics"] = {
        "avg_events_per_nft": float(activity_stats.mean()),
        "median_events_per_nft": float(activity_stats.median()),
        "min_events": int(activity_stats.min()),
        "max_events": int(activity_stats.max()),
    }

    with SUMMARY_FILE.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"Summary saved to: {SUMMARY_FILE}")

    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
    temp_file = OUTPUT_FILE.parent / f"TEMP_{OUTPUT_FILE.name}"
    if temp_file.exists():
        temp_file.unlink()

    total_minutes = (time.time() - start_time) / 60
    print("\n" + "=" * 80)
    print("EXTRACTION COMPLETE!")
    print("=" * 80)
    print(f"Total activities: {len(all_activities)}")
    print(f"Filtered out events: {skipped_events}")
    print(f"NFTs with activity: {df['identifier'].nunique()}")
    print(f"Total time: {total_minutes:.1f} minutes")
    if total_minutes > 0:
        print(f"Average rate: {len(processed_ids) / total_minutes:.2f} NFTs/minute")

    if failed_ids:
        print(f"\n{len(failed_ids)} NFTs had no events")

    print("\nEvent Type Breakdown:")
    for event_type, count in df["event_type"].value_counts().items():
        print(f"   {event_type}: {count:,}")

    print("\n" + "=" * 80)
    print("Your complete activity history dataset is ready!")
    print(f"File: {OUTPUT_FILE}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract full MAYC activity history from OpenSea")
    parser.add_argument(
        "--max-nfts",
        type=int,
        default=None,
        help="Optional limit for test runs (e.g. --max-nfts 5)",
    )
    parser.add_argument(
        "--ignore-checkpoint",
        action="store_true",
        help="Start from the beginning and ignore any existing checkpoint file",
    )
    args = parser.parse_args()
    main(max_nfts=args.max_nfts, ignore_checkpoint=args.ignore_checkpoint)
