import requests
import pandas as pd
import os
import time
from tqdm import tqdm
from collections import defaultdict

API_KEY = "4a233d22f9224b19831636cbfb9ab9b1"
SLUG = "cryptopunks"
CHAIN = "ethereum"

HEADERS = {
    "accept": "application/json",
    "x-api-key": API_KEY
}

BASE = "https://api.opensea.io/api/v2"

os.makedirs("raw/nft_json", exist_ok=True)
os.makedirs("raw/sale_json", exist_ok=True)

# --------------------------
# 1. Collection Metadata
# --------------------------
collection = requests.get(
    f"{BASE}/collections/{SLUG}",
    headers=HEADERS
).json()

with open("collection.json", "w") as f:
    f.write(str(collection))

# --------------------------
# 2. Get All NFTs
# --------------------------
all_nfts = []
cursor = None

while True:
    url = f"{BASE}/collection/{SLUG}/nfts?limit=50"
    if cursor:
        url += f"&next={cursor}"

    r = requests.get(url, headers=HEADERS).json()
    nfts = r.get("nfts", [])
    all_nfts.extend(nfts)

    cursor = r.get("next")
    if not cursor:
        break

    time.sleep(1)

print("NFTs collected:", len(all_nfts))

# --------------------------
# 3. Extract NFT Data
# --------------------------
rows = []
trait_freq = defaultdict(int)

for nft in tqdm(all_nfts):
    token_id = nft.get("identifier")
    traits = nft.get("traits", [])

    for t in traits:
        trait_freq[(t["trait_type"], t["value"])] += 1

    rows.append({
        "token_id": token_id,
        "name": nft.get("name"),
        "owner": nft.get("owner"),
        "image_url": nft.get("image_url"),
        "contract": nft.get("contract"),
        "traits": traits
    })

    with open(f"raw/nft_json/{token_id}.json","w") as f:
        f.write(str(nft))

# --------------------------
# 4. Calculate Rarity
# --------------------------
total_supply = len(rows)

for row in rows:
    rarity = 0
    for t in row["traits"]:
        count = trait_freq[(t["trait_type"], t["value"])]
        rarity += 1 / (count / total_supply)
    row["rarity_score"] = rarity

df = pd.DataFrame(rows)
df["rarity_rank"] = df["rarity_score"].rank(ascending=False)

# --------------------------
# 5. Sales History
# --------------------------
sales_rows = []
cursor = None

while True:
    url = f"{BASE}/events?collection_slug={SLUG}&event_type=sale&limit=50"
    if cursor:
        url += f"&next={cursor}"

    r = requests.get(url, headers=HEADERS).json()
    events = r.get("asset_events", [])

    for e in events:
        sales_rows.append({
            "token_id": e.get("nft", {}).get("identifier"),
            "tx_hash": e.get("transaction"),
            "buyer": e.get("buyer"),
            "seller": e.get("seller"),
            "price": e.get("payment"),
            "timestamp": e.get("event_timestamp")
        })

    cursor = r.get("next")
    if not cursor:
        break

    time.sleep(1)

sales_df = pd.DataFrame(sales_rows)

# --------------------------
# SAVE FILES
# --------------------------
df.to_csv("nfts_master.csv", index=False)
sales_df.to_csv("sales_history.csv", index=False)

print("FULL DATASET CREATED ✅")