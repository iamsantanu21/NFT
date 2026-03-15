import requests
import pandas as pd
import time
import os
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ===============================
# CONFIG
# ===============================

API_KEY = os.getenv("OPENSEA_API_KEY")
COLLECTION = os.getenv("COLLECTION_SLUG")

BASE_URL = "https://api.opensea.io/api/v2"
CHAIN = "ethereum"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

MAX_WORKERS = 20
REQUEST_DELAY = 0.05
MAX_RETRIES = 5

COLLECTION_DIR = f"collections/{COLLECTION}"
METADATA_FILE = f"{COLLECTION_DIR}/Level2_all_nfts_metadata.csv"
OUTPUT_FILE = f"{COLLECTION_DIR}/Level3_core_activity.csv"
CHECKPOINT_FILE = f"{COLLECTION_DIR}/Level3_checkpoint.json"

os.makedirs(COLLECTION_DIR, exist_ok=True)

file_lock = Lock()

# ===============================
# IMPORTANT EVENTS
# ===============================

IMPORTANT_EVENTS = {
    "transfer",
    "sale"
}

# mint is detected from transfer from zero address

ZERO_ADDR = "0x0000000000000000000000000000000000000000"

# ===============================
# LOAD CONTRACT
# ===============================

def get_contract():

    level1 = f"{COLLECTION_DIR}/Level1_collection_info.csv"

    df = pd.read_csv(level1)

    contracts = eval(df.iloc[0]["contracts"])

    return contracts[0]["address"], df.iloc[0]["name"]

CONTRACT, COLLECTION_NAME = get_contract()

# ===============================
# CHECKPOINT
# ===============================

def load_checkpoint():

    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return set(json.load(f))

    return set()

def save_checkpoint(done):

    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(done), f)

# ===============================
# EVENT PARSER
# ===============================

def parse_event(event, token_id):

    event_type = event.get("event_type")

    from_addr = event.get("from_address")
    to_addr = event.get("to_address")

    # Detect mint
    if event_type == "transfer" and from_addr == ZERO_ADDR:
        event_type = "mint"

    # Price extraction
    price = 0

    payment = event.get("payment")

    if payment:
        try:
            price = float(payment["quantity"]) / (10 ** int(payment["decimals"]))
        except:
            pass

    tx = ""

    tx_data = event.get("transaction")

    if isinstance(tx_data, dict):
        tx = tx_data.get("transaction_hash", "")

    return {
        "identifier": token_id,
        "event_type": event_type,
        "price_eth": round(price,6),
        "from": from_addr,
        "to": to_addr,
        "timestamp": event.get("event_timestamp"),
        "tx_hash": tx
    }

# ===============================
# FETCH EVENTS
# ===============================

def fetch_events(token_id):

    url = f"{BASE_URL}/events/chain/{CHAIN}/contract/{CONTRACT}/nfts/{token_id}"

    cursor = None

    rows = []

    while True:

        params = {"limit": 50}

        if cursor:
            params["next"] = cursor

        for retry in range(MAX_RETRIES):

            try:

                r = requests.get(url, headers=HEADERS, params=params, timeout=20)

                if r.status_code == 429:
                    time.sleep(3)
                    continue

                r.raise_for_status()

                data = r.json()

                events = data.get("asset_events", [])

                for e in events:

                    raw_type = e.get("event_type")

                    from_addr = e.get("from_address")

                    if raw_type == "transfer":

                        if from_addr == ZERO_ADDR:
                            rows.append(parse_event(e, token_id))

                        else:
                            rows.append(parse_event(e, token_id))

                    elif raw_type == "sale":
                        rows.append(parse_event(e, token_id))

                cursor = data.get("next")

                break

            except Exception:
                time.sleep(2 ** retry)

        if not cursor:
            break

        time.sleep(REQUEST_DELAY)

    return rows

# ===============================
# WRITE CSV
# ===============================

def write_rows(rows):

    with file_lock:

        df = pd.DataFrame(rows)

        if not os.path.exists(OUTPUT_FILE):
            df.to_csv(OUTPUT_FILE, index=False)

        else:
            df.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)

# ===============================
# MAIN
# ===============================

def main():

    df = pd.read_csv(METADATA_FILE)

    identifiers = df["identifier"].astype(str).tolist()

    done = load_checkpoint()

    identifiers = [i for i in identifiers if i not in done]

    total = len(identifiers)

    print("\nNFTs to process:", total)

    start = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {executor.submit(fetch_events, token): token for token in identifiers}

        for i, future in enumerate(as_completed(futures)):

            token_id = futures[future]

            rows = future.result()

            write_rows(rows)

            done.add(token_id)

            if i % 25 == 0:
                save_checkpoint(done)

            elapsed = (time.time() - start) / 60

            print(f"[{i+1}/{total}] token={token_id} events={len(rows)} time={elapsed:.1f}m")

    save_checkpoint(done)

    print("\nExtraction finished")

if __name__ == "__main__":
    main()