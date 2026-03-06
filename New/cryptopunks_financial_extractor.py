import requests
import pandas as pd
import time
import os
from datetime import datetime

# =========================
# CONFIGURATION
# =========================

API_KEY = "4a233d22f9224b19831636cbfb9ab9b1"

CONTRACT = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

INPUT_FILE = "level2_all_nfts_metadata.csv"
OUTPUT_FILE = "cryptopunks_financial_dataset.csv"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

SLEEP_TIME = 0.25   # avoid rate limits


# =========================
# LOAD DATASET
# =========================

df = pd.read_csv(INPUT_FILE)

token_ids = df["identifier"].tolist()

print("Total NFTs:", len(token_ids))


# =========================
# LOAD PREVIOUS PROGRESS
# =========================

if os.path.exists(OUTPUT_FILE):
    existing = pd.read_csv(OUTPUT_FILE)
    done_ids = set(existing["identifier"])
    results = existing.to_dict("records")
else:
    done_ids = set()
    results = []


# =========================
# FUNCTION TO FETCH SALES
# =========================

def get_sales_data(token_id):

    url = f"https://api.opensea.io/api/v2/events/chain/ethereum/contract/{CONTRACT}/nfts/{token_id}"

    params = {
        "event_type": "sale",
        "limit": 50
    }

    try:

        r = requests.get(url, headers=HEADERS, params=params)

        if r.status_code != 200:
            return None

        data = r.json()

        events = data.get("asset_events", [])

        prices = []
        buyers = set()
        sellers = set()

        for e in events:

            try:
                price = int(e["payment"]["quantity"]) / 1e18
                prices.append(price)

                buyers.add(e["buyer"]["address"])
                sellers.add(e["seller"]["address"])

            except:
                continue

        if len(prices) == 0:

            return {
                "identifier": token_id,
                "transaction_count": 0,
                "avg_price_eth": 0,
                "max_price_eth": 0,
                "min_price_eth": 0,
                "total_volume_eth": 0,
                "unique_buyers": 0,
                "unique_sellers": 0
            }

        return {

            "identifier": token_id,

            "transaction_count": len(prices),

            "avg_price_eth": sum(prices) / len(prices),

            "max_price_eth": max(prices),

            "min_price_eth": min(prices),

            "total_volume_eth": sum(prices),

            "unique_buyers": len(buyers),

            "unique_sellers": len(sellers)

        }

    except Exception as e:

        print("Error:", token_id, e)

        return None


# =========================
# MAIN LOOP
# =========================

count = 0

for token_id in token_ids:

    if token_id in done_ids:
        continue

    data = get_sales_data(token_id)

    if data:

        results.append(data)

        done_ids.add(token_id)

        count += 1

        print("Processed:", token_id)

    # SAVE EVERY 20 NFTs
    if count % 20 == 0:

        pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

        print("Saved progress", datetime.now())

    time.sleep(SLEEP_TIME)


# =========================
# FINAL SAVE
# =========================

pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

print("Completed extraction")