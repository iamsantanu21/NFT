import json
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


API_KEY = os.getenv("OPENSEA_API_KEY", "4a233d22f9224b19831636cbfb9ab9b1")
CONTRACT = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
INPUT_FILE = "level2_all_nfts_metadata.csv"
OUTPUT_FILE = "cryptopunks_activity_full.csv"
CHECKPOINT_FILE = "cryptopunks_activity_checkpoint.json"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY,
}

SLEEP = 0.30
REQUEST_TIMEOUT = 30
MAX_RETRIES = 4
SAVE_EVERY = 20


def load_token_ids() -> List[str]:
    metadata = pd.read_csv(INPUT_FILE)
    token_ids = metadata["identifier"].astype(str).tolist()
    print("Total NFTs:", len(token_ids))
    return token_ids


def load_existing_results() -> List[Dict[str, Any]]:
    if not os.path.exists(OUTPUT_FILE):
        return []

    try:
        old = pd.read_csv(OUTPUT_FILE)
        print("Resuming existing events rows:", len(old))
        return old.to_dict("records")
    except Exception:
        print("Existing output file is empty/corrupted. Starting from fresh results.")
        return []


def load_checkpoint() -> set[str]:
    if not os.path.exists(CHECKPOINT_FILE):
        return set()

    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    done_ids = set(str(x) for x in data.get("processed_token_ids", []))
    print("Resuming processed token IDs:", len(done_ids))
    return done_ids


def save_checkpoint(done_ids: set[str]) -> None:
    payload = {
        "processed_token_ids": sorted(done_ids, key=lambda x: int(x))
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def safe_request(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                url,
                headers=HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if r.status_code == 429:
                wait_time = attempt * 2
                print(f"Rate limit hit (429). Sleeping {wait_time}s")
                time.sleep(wait_time)
                continue

            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_error = e
            time.sleep(attempt)

    raise RuntimeError(f"Request failed after retries: {last_error}")


def normalize_event(token_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(event, dict):
        return {
            "identifier": token_id,
            "event_type": None,
            "event_timestamp": None,
            "from_address": None,
            "to_address": None,
            "price_native": None,
            "payment_symbol": None,
            "payment_quantity_raw": None,
            "transaction_hash": None,
            "block_number": None,
            "chain": None,
            "marketplace": None,
            "event_id": None,
        }

    transaction = event.get("transaction") or {}
    payment = event.get("payment") or {}
    seller = event.get("seller") or {}
    buyer = event.get("buyer") or {}
    marketplace = event.get("marketplace") or {}

    if not isinstance(transaction, dict):
        transaction = {}
    if not isinstance(payment, dict):
        payment = {}
    if not isinstance(seller, dict):
        seller = {}
    if not isinstance(buyer, dict):
        buyer = {}

    quantity_raw = payment.get("quantity") or event.get("total_price")
    decimals = payment.get("decimals")
    symbol = payment.get("symbol")

    price_native = None
    if quantity_raw is not None:
        try:
            quantity_int = int(quantity_raw)
            if decimals is None:
                decimals = 18
            price_native = quantity_int / (10 ** int(decimals))
        except Exception:
            price_native = None

    marketplace_name = marketplace.get("name") if isinstance(marketplace, dict) else marketplace

    return {
        "identifier": token_id,
        "event_type": event.get("event_type"),
        "event_timestamp": event.get("event_timestamp"),
        "from_address": event.get("from_address") or seller.get("address"),
        "to_address": event.get("to_address") or buyer.get("address"),
        "price_native": price_native,
        "payment_symbol": symbol,
        "payment_quantity_raw": quantity_raw,
        "transaction_hash": transaction.get("transaction_hash"),
        "block_number": transaction.get("block_number"),
        "chain": event.get("chain"),
        "marketplace": marketplace_name,
        "event_id": event.get("id") or event.get("event_id"),
    }


def get_activity(token_id: str) -> List[Dict[str, Any]]:
    all_events: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        url = f"https://api.opensea.io/api/v2/events/chain/ethereum/contract/{CONTRACT}/nfts/{token_id}"
        params: Dict[str, Any] = {"limit": 50}

        if cursor:
            params["cursor"] = cursor

        data = safe_request(url, params=params)
        events = data.get("asset_events") or data.get("events") or []

        if isinstance(events, dict):
            events = [events]
        elif not isinstance(events, list):
            events = []

        if not events:
            break

        for event in events:
            all_events.append(normalize_event(token_id, event))

        cursor = data.get("next")
        if not cursor:
            break

        time.sleep(SLEEP)

    return all_events


def main() -> None:
    token_ids = load_token_ids()
    results = load_existing_results()
    done = load_checkpoint()

    processed_since_save = 0

    for token_id in token_ids:
        if token_id in done:
            continue

        print("Processing token:", token_id)

        try:
            rows = get_activity(token_id)
            results.extend(rows)
            done.add(token_id)
            processed_since_save += 1
        except Exception as e:
            print(f"Failed token {token_id}: {e}")
            done.add(token_id)
            processed_since_save += 1

        if processed_since_save >= SAVE_EVERY:
            pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
            save_checkpoint(done)
            print(
                f"Saved progress | processed tokens: {len(done)} | event rows: {len(results)}"
            )
            processed_since_save = 0

        time.sleep(SLEEP)

    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    save_checkpoint(done)
    print("Finished extraction")
    print("Total processed tokens:", len(done))
    print("Total event rows:", len(results))


if __name__ == "__main__":
    main()