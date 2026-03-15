"""
Ultra Fast NFT Event Extraction
Downloads ALL events for a collection
"""

import requests
import pandas as pd
import time
import os

API_KEY = os.getenv("OPENSEA_API_KEY")
COLLECTION = os.getenv("COLLECTION_SLUG")

BASE_URL = "https://api.opensea.io/api/v2"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

OUTPUT = f"collections/{COLLECTION}/Level3_collection_events.csv"

os.makedirs(f"collections/{COLLECTION}", exist_ok=True)

cursor = None
page = 0
rows = []

print("\nDownloading collection events...\n")

while True:

    url = f"{BASE_URL}/events/collection/{COLLECTION}"

    params = {
        "limit": 50
    }

    if cursor:
        params["next"] = cursor

    r = requests.get(url, headers=HEADERS, params=params)

    if r.status_code == 429:
        print("Rate limited, sleeping...")
        time.sleep(10)
        continue

    r.raise_for_status()

    data = r.json()

    events = data.get("asset_events", [])

    for e in events:

        token = None

        nft = e.get("nft")

        if nft:
            token = nft.get("identifier")

        rows.append({
            "identifier": token,
            "event_type": e.get("event_type"),
            "from": e.get("from_address"),
            "to": e.get("to_address"),
            "timestamp": e.get("event_timestamp"),
        })

    cursor = data.get("next")

    page += 1

    print(f"Page {page} | events {len(rows)}")

    if not cursor:
        break

    if len(rows) > 50000:

        df = pd.DataFrame(rows)

        if not os.path.exists(OUTPUT):
            df.to_csv(OUTPUT, index=False)
        else:
            df.to_csv(OUTPUT, mode="a", header=False, index=False)

        rows = []

    time.sleep(0.2)

if rows:

    df = pd.DataFrame(rows)

    if not os.path.exists(OUTPUT):
        df.to_csv(OUTPUT, index=False)
    else:
        df.to_csv(OUTPUT, mode="a", header=False, index=False)

print("\nDownload complete")