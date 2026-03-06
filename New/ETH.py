import requests
import pandas as pd
from datetime import datetime, timedelta

API_KEY = "u2b7JQUNLzkgObMtQi8n1"
BASE_URL = f"https://eth-mainnet.g.alchemy.com/nft/v2/{API_KEY}"

CONTRACT = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

# Calculate timestamp 1 year ago
one_year_ago = int((datetime.utcnow() - timedelta(days=365)).timestamp())

url = f"{BASE_URL}/getNFTSales"

params = {
    "contractAddress": CONTRACT,
    "fromBlock": "0x0",
    "toBlock": "latest"
}

print("Fetching sales...")

response = requests.get(url, params=params)

data = response.json()

sales = []

for sale in data.get("nftSales", []):
    if sale.get("blockTimestamp"):
        sale_time = int(datetime.fromisoformat(
            sale["blockTimestamp"].replace("Z", "")
        ).timestamp())

        if sale_time >= one_year_ago:
            sales.append({
                "token_id": sale.get("tokenId"),
                "buyer": sale.get("buyerAddress"),
                "seller": sale.get("sellerAddress"),
                "price": sale.get("sellerFee", {}).get("amount"),
                "transaction_hash": sale.get("transactionHash"),
                "timestamp": sale.get("blockTimestamp")
            })

df = pd.DataFrame(sales)
df.to_csv("punk_last_year_sales.csv", index=False)

print("Done")
print("Total sales (last year):", len(df))