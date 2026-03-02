import requests
import pandas as pd
import time
from tqdm import tqdm

API_KEY = "YOUR_NEW_API_KEY"
BASE_URL = "https://api.opensea.io/api/v2"
SLUG = "cryptopunks"

HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

print("Fetching ALL collection events...")

all_events = []
next_cursor = None

while True:
    url = f"{BASE_URL}/events/collection/{SLUG}"

    params = {
        "limit": 200
    }

    if next_cursor:
        params["next"] = next_cursor

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()

    data = response.json()

    events = data.get("events", [])
    all_events.extend(events)

    print(f"Collected {len(all_events)} events")

    next_cursor = data.get("next")
    if not next_cursor:
        break

    time.sleep(0.25)

events_df = pd.json_normalize(all_events)
events_df.to_csv("level4_all_collection_events.csv", index=False)

print("LEVEL 4 COMPLETE ✅")