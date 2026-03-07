import requests
import pandas as pd
import time
from tqdm import tqdm

API_KEY = "5575c781fdb2424f8e5aa693c8f68a35"
BASE_URL = "https://api.opensea.io/api/v2"
SLUG = "cryptopunks"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

print("Fetching ALL NFTs in collection...")

all_nfts = []
next_cursor = None

while True:
    url = f"{BASE_URL}/collection/{SLUG}/nfts"

    params = {
        "limit": 200
    }

    if next_cursor:
        params["next"] = next_cursor

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()

    data = response.json()

    nfts = data.get("nfts", [])
    all_nfts.extend(nfts)

    print(f"Collected: {len(all_nfts)} NFTs")

    next_cursor = data.get("next")
    if not next_cursor:
        break

    time.sleep(0.2)

print("Total NFTs Collected:", len(all_nfts))

# Save raw NFT metadata
df_nfts = pd.json_normalize(all_nfts)
df_nfts.to_csv("level2_all_nfts_metadata.csv", index=False)

print("LEVEL 2 COMPLETE ✅")