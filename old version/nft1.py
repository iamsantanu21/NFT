import requests
import pandas as pd
import os
import time
from tqdm import tqdm
from collections import defaultdict

# ============================
# CONFIG
# ============================
API_KEY = "4a233d22f9224b19831636cbfb9ab9b1"
COLLECTION_SLUG = "cryptopunks"
BASE_URL = "https://api.opensea.io/api/v2"

HEADERS = {
    "accept": "application/json",
    "x-api-key": API_KEY
}

# ============================
# CREATE FOLDERS
# ============================
os.makedirs("raw/nfts", exist_ok=True)

# ============================
# 1. FETCH COLLECTION METADATA
# ============================
print("Fetching collection metadata...")

collection_url = f"{BASE_URL}/collections/{COLLECTION_SLUG}"
collection_data = requests.get(collection_url, headers=HEADERS).json()

with open("collection.json", "w") as f:
    f.write(str(collection_data))

# ============================
# 2. FETCH ALL NFTS
# ============================
print("Fetching all NFTs...")

all_nfts = []
cursor = None

while True:
    url = f"{BASE_URL}/collection/{COLLECTION_SLUG}/nfts?limit=50"
    if cursor:
        url += f"&next={cursor}"

    response = requests.get(url, headers=HEADERS)
    data = response.json()

    nfts = data.get("nfts", [])
    all_nfts.extend(nfts)

    cursor = data.get("next")
    if not cursor:
        break

    time.sleep(0.4)

print(f"NFTs collected: {len(all_nfts)}")

# ============================
# 3. PROCESS + RARITY
# ============================
print("Processing NFTs & computing rarity...")

trait_counter = defaultdict(int)

for nft in all_nfts:
    for t in nft.get("traits", []):
        trait_counter[(t["trait_type"], t["value"])] += 1

rows = []
total = len(all_nfts)

for nft in tqdm(all_nfts):
    token_id = nft.get("identifier")
    traits = nft.get("traits", [])

    rarity = 0
    for t in traits:
        freq = trait_counter[(t["trait_type"], t["value"])]
        rarity += 1 / (freq / total)

    rows.append({
        "token_id": token_id,
        "name": nft.get("name"),
        "owner": nft.get("owner"),
        "image_url": nft.get("image_url"),
        "contract": nft.get("contract"),
        "chain": nft.get("chain"),
        "metadata_url": nft.get("metadata_url"),
        "traits": traits,
        "rarity_score": rarity
    })

    with open(f"raw/nfts/{token_id}.json", "w") as f:
        f.write(str(nft))

df = pd.DataFrame(rows)
df["rarity_rank"] = df["rarity_score"].rank(ascending=False)

# ============================
# 4. SAVE DATASET
# ============================
df.to_csv("cryptopunks_nfts_dataset.csv", index=False)

print("===================================")
print(" DATASET CREATED SUCCESSFULLY ✅")
print(" cryptopunks_nfts_dataset.csv")
print("===================================")