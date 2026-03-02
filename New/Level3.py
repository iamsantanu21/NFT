import requests
import pandas as pd
import time
from tqdm import tqdm

API_KEY = "5575c781fdb2424f8e5aa693c8f68a35"
BASE_URL = "https://api.opensea.io/api/v2"
CHAIN = "ethereum"
CONTRACT = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

# Load Level 2 file
nfts_df = pd.read_csv("level2_all_nfts_metadata.csv")

detailed_nfts = []

print("Fetching detailed info for each NFT...")

for token_id in tqdm(nfts_df["identifier"]):

    url = f"{BASE_URL}/chain/{CHAIN}/contract/{CONTRACT}/nfts/{token_id}"

    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()

        nft_data = response.json()
        detailed_nfts.append(nft_data)

        time.sleep(0.2)  # rate limit protection

    except Exception as e:
        print(f"Error on token {token_id}: {e}")
        continue

# Save detailed NFT data
df_detailed = pd.json_normalize(detailed_nfts)
df_detailed.to_csv("level3_detailed_nfts.csv", index=False)

print("LEVEL 3 COMPLETE ✅")