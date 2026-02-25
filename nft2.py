import requests
import pandas as pd
import os
import time
from tqdm import tqdm
from collections import defaultdict

# =================================
# CONFIG
# =================================
API_KEY = "4a233d22f9224b19831636cbfb9ab9b1"
COLLECTION_SLUG = "cryptopunks"
CHAIN = "ethereum"
BASE_URL = "https://api.opensea.io/api/v2"

HEADERS = {
    "accept": "application/json",
    "x-api-key": API_KEY
}

os.makedirs("raw/nfts", exist_ok=True)

# =================================
# 1. COLLECTION METADATA
# =================================
print("Fetching collection metadata...")

collection = requests.get(
    f"{BASE_URL}/collections/{COLLECTION_SLUG}",
    headers=HEADERS
).json()

with open("collection.json", "w") as f:
    f.write(str(collection))

# =================================
# 2. FETCH ALL NFTS
# =================================
print("Fetching NFTs...")

all_nfts = []
cursor = None

while True:
    url = f"{BASE_URL}/collection/{COLLECTION_SLUG}/nfts?limit=50"
    if cursor:
        url += f"&next={cursor}"

    response = requests.get(url, headers=HEADERS)
    data = response.json()

    all_nfts.extend(data.get("nfts", []))
    cursor = data.get("next")

    if not cursor:
        break

    time.sleep(0.4)

print(f"NFTs collected: {len(all_nfts)}")

# =================================
# 3. TRAIT FREQUENCY
# =================================
trait_counter = defaultdict(int)

for nft in all_nfts:
    for t in nft.get("traits", []):
        trait_counter[(t["trait_type"], t["value"])] += 1

total_nfts = len(all_nfts)

# =================================
# 4. BUILD DATASET + PRICING
# =================================
rows = []

for nft in tqdm(all_nfts):

    token_id = nft.get("identifier")
    traits = nft.get("traits", [])

    # ---- RARITY ----
    rarity_score = 0
    for t in traits:
        freq = trait_counter[(t["trait_type"], t["value"])]
        rarity_score += 1 / (freq / total_nfts)

    # ---- SAFE PRICING FETCH ----
    last_sale_price = None
    last_sale_date = None
    current_price = None
    currency = None
    is_listed = False

    try:
        detail_url = f"{BASE_URL}/nft/{CHAIN}/{nft.get('contract')}/{token_id}"
        detail = requests.get(detail_url, headers=HEADERS).json()

        last_sale = detail.get("last_sale")
        if last_sale:
            last_sale_price = last_sale.get("total_price")
            last_sale_date = last_sale.get("event_timestamp")

        orders = detail.get("orders")
        if orders:
            current_price = orders[0].get("current_price")
            currency = orders[0].get("payment_token_contract", {}).get("symbol")
            is_listed = True

        time.sleep(0.2)

    except:
        pass

    rows.append({
        "token_id": token_id,
        "name": nft.get("name"),
        "description": nft.get("description"),
        "owner": nft.get("owner"),
        "image_url": nft.get("image_url"),
        "animation_url": nft.get("animation_url"),
        "contract_address": nft.get("contract"),
        "chain": nft.get("chain"),
        "token_standard": nft.get("token_standard"),
        "traits": traits,
        "trait_count": len(traits),
        "rarity_score": rarity_score,
        "last_sale_price": last_sale_price,
        "last_sale_date": last_sale_date,
        "current_listing_price": current_price,
        "listing_currency": currency,
        "is_listed": is_listed
    })

    with open(f"raw/nfts/{token_id}.json", "w") as f:
        f.write(str(nft))

df = pd.DataFrame(rows)
df["rarity_rank"] = df["rarity_score"].rank(ascending=False)

# =================================
# SAVE DATASET
# =================================
df.to_csv("cryptopunks_complete_dataset.csv", index=False)

print("====================================")
print(" COMPLETE DATASET CREATED ✅")
print(" cryptopunks_complete_dataset.csv")
print("====================================")