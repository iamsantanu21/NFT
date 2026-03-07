import requests
import pandas as pd
import time
import os
from datetime import datetime

# =============================
# CONFIGURATION
# =============================

API_KEY = "4a233d22f9224b19831636cbfb9ab9b1"

CONTRACT = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

INPUT_FILE = "level2_all_nfts_metadata.csv"

OUTPUT_FILE = "cryptopunks_full_activity.csv"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

SLEEP = 0.35


# =============================
# LOAD NFT IDS
# =============================

df = pd.read_csv(INPUT_FILE)

token_ids = df["identifier"].astype(str).tolist()

print("Total NFTs:", len(token_ids))


# =============================
# RESUME SUPPORT
# =============================

if os.path.exists(OUTPUT_FILE):

    old = pd.read_csv(OUTPUT_FILE)

    results = old.to_dict("records")

    done = set(old["identifier"].astype(str))

else:

    results = []

    done = set()


# =============================
# API FUNCTION
# =============================

def fetch_activity(token_id):

    all_rows = []

    cursor = None

    while True:

        url = f"https://api.opensea.io/api/v2/events/chain/ethereum/contract/{CONTRACT}/nfts/{token_id}"

        params = {"limit": 50}

        if cursor:
            params["cursor"] = cursor

        try:

            r = requests.get(url, headers=HEADERS, params=params)

            if r.status_code != 200:

                print("API error:", r.status_code)

                break

            data = r.json()

            events = data.get("asset_events") or data.get("events") or []

            if len(events) == 0:
                break

            for e in events:

                try:

                    event_type = e.get("event_type")

                    timestamp = e.get("event_timestamp")

                    tx_hash = None
                    block = None

                    if e.get("transaction"):
                        tx_hash = e["transaction"].get("transaction_hash")
                        block = e["transaction"].get("block_number")

                    price = 0
                    payment_symbol = None

                    if e.get("payment"):

                        price = int(e["payment"]["quantity"]) / 1e18

                        payment_symbol = e["payment"].get("symbol")

                    elif e.get("total_price"):

                        price = int(e["total_price"]) / 1e18

                        payment_symbol = "ETH"

                    from_addr = (
                        e.get("seller", {}).get("address")
                        or e.get("from_address")
                    )

                    to_addr = (
                        e.get("buyer", {}).get("address")
                        or e.get("to_address")
                    )

                    marketplace = None

                    if e.get("marketplace"):
                        marketplace = e["marketplace"].get("name")

                    row = {

                        "identifier": token_id,

                        "event_type": event_type,

                        "price_eth": price,

                        "payment_token": payment_symbol,

                        "from_address": from_addr,

                        "to_address": to_addr,

                        "marketplace": marketplace,

                        "timestamp": timestamp,

                        "transaction_hash": tx_hash,

                        "block_number": block
                    }

                    all_rows.append(row)

                except:
                    continue

            cursor = data.get("next")

            if not cursor:
                break

            time.sleep(SLEEP)

        except Exception as e:

            print("Error:", token_id, e)

            break

    return all_rows


# =============================
# MAIN LOOP
# =============================

count = 0

for token_id in token_ids:

    if token_id in done:
        continue

    print("Processing NFT:", token_id)

    rows = fetch_activity(token_id)

    for r in rows:
        results.append(r)

    done.add(token_id)

    count += 1

    if count % 20 == 0:

        pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

        print("Progress saved", datetime.now())

    time.sleep(SLEEP)


# =============================
# FINAL SAVE
# =============================

pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

print("Finished dataset extraction")