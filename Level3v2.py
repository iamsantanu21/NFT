"""
FAST Level 3: Parallel NFT Activity History Extraction
"""

import requests
import pandas as pd
import time
import os
import json
import re
import ast
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ================= CONFIG =================

API_KEY = os.getenv("OPENSEA_API_KEY")
COLLECTION_SLUG = os.getenv("COLLECTION_SLUG")

if not API_KEY or not COLLECTION_SLUG:
    raise RuntimeError("Set OPENSEA_API_KEY and COLLECTION_SLUG environment variables")

BASE_URL = "https://api.opensea.io/api/v2"
CHAIN = "ethereum"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

MAX_WORKERS = 25
REQUEST_DELAY = 0.05
MAX_RETRIES = 5

COLLECTION_DIR = f"collections/{COLLECTION_SLUG}"
METADATA_FILE = f"{COLLECTION_DIR}/Level2_all_nfts_metadata.csv"
OUTPUT_FILE = f"{COLLECTION_DIR}/Level3_complete_activity_history.csv"
CHECKPOINT_FILE = f"{COLLECTION_DIR}/Level3_checkpoint.json"

os.makedirs(COLLECTION_DIR, exist_ok=True)

file_lock = Lock()

# ================= CONTRACT RESOLUTION =================

def resolve_contract():

    level1 = f"{COLLECTION_DIR}/Level1_collection_info.csv"

    if os.path.exists(level1):
        df = pd.read_csv(level1)
        contracts = ast.literal_eval(df.iloc[0]["contracts"])
        return contracts[0]["address"], df.iloc[0]["name"]

    url = f"{BASE_URL}/collections/{COLLECTION_SLUG}"

    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()

    data = r.json()

    return data["contracts"][0]["address"], data["name"]

CONTRACT_ADDRESS, COLLECTION_NAME = resolve_contract()

# ================= TIMESTAMP FORMAT =================

def format_timestamp(ts):

    if not ts:
        return ""

    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except:
        return ts

# ================= EVENT PARSER =================

def parse_event(event, token_id):

    raw_type = event.get("event_type", "unknown")
    order_type = event.get("order_type")

    event_type = order_type if raw_type == "order" else raw_type

    price = 0

    payment = event.get("payment")

    if payment:
        try:
            price = float(payment["quantity"]) / (10 ** int(payment["decimals"]))
        except:
            pass

    tx = ""

    if isinstance(event.get("transaction"), dict):
        tx = event["transaction"].get("transaction_hash", "")

    return {
        "identifier": token_id,
        "event_type": event_type,
        "price_eth": round(price,6),
        "from": event.get("from_address"),
        "to": event.get("to_address"),
        "timestamp": format_timestamp(event.get("event_timestamp")),
        "tx_hash": tx
    }

# ================= EVENT FETCH =================

def fetch_events(token_id):

    url = f"{BASE_URL}/events/chain/{CHAIN}/contract/{CONTRACT_ADDRESS}/nfts/{token_id}"

    cursor = None
    events_all = []

    while True:

        params = {"limit": 50}

        if cursor:
            params["next"] = cursor

        for retry in range(MAX_RETRIES):

            try:

                r = requests.get(url, headers=HEADERS, params=params, timeout=20)

                if r.status_code == 429:
                    time.sleep(5)
                    continue

                r.raise_for_status()

                data = r.json()

                events = data.get("asset_events", [])

                events_all.extend(events)

                cursor = data.get("next")

                break

            except Exception:
                time.sleep(2 ** retry)

        if not cursor:
            break

        time.sleep(REQUEST_DELAY)

    return token_id, events_all

# ================= CSV WRITER =================

def write_events(rows):

    with file_lock:

        df = pd.DataFrame(rows)

        if not os.path.exists(OUTPUT_FILE):
            df.to_csv(OUTPUT_FILE, index=False)
        else:
            df.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)

# ================= CHECKPOINT =================

def load_checkpoint():

    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return set(json.load(f))

    return set()

def save_checkpoint(done):

    with open(CHECKPOINT_FILE,"w") as f:
        json.dump(list(done),f)

# ================= MAIN =================

def main():

    print("\nFAST NFT ACTIVITY EXTRACTION\n")

    df = pd.read_csv(METADATA_FILE)

    identifiers = df["identifier"].astype(str).tolist()

    done = load_checkpoint()

    identifiers = [i for i in identifiers if i not in done]

    total = len(identifiers)

    print("NFTs remaining:", total)

    start = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {executor.submit(fetch_events, token): token for token in identifiers}

        for i,f in enumerate(as_completed(futures)):

            token_id, events = f.result()

            rows = []

            for e in events:
                rows.append(parse_event(e, token_id))

            write_events(rows)

            done.add(token_id)

            if i % 20 == 0:
                save_checkpoint(done)

            elapsed = (time.time()-start)/60

            print(f"[{i+1}/{total}] Token {token_id} | events={len(events)} | {elapsed:.1f} min")

    save_checkpoint(done)

    print("\nExtraction finished")

if __name__ == "__main__":
    main()