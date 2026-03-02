import requests
import pandas as pd

API_KEY = "5575c781fdb2424f8e5aa693c8f68a35"
BASE_URL = "https://api.opensea.io/api/v2"
SLUG = "cryptopunks"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

print("Fetching Collection Information...")

url = f"{BASE_URL}/collections/{SLUG}"
response = requests.get(url, headers=HEADERS)
response.raise_for_status()

collection_data = response.json()

# Save as structured dataset
df_collection = pd.json_normalize(collection_data)
df_collection.to_csv("level1_collection_info.csv", index=False)

print("LEVEL 1 COMPLETE ✅")