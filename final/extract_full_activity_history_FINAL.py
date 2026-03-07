"""
FINAL PRODUCTION SCRIPT: CryptoPunks Complete Activity History Extraction
Extracts full activity history from MINT to present for all CryptoPunks
Includes: mint, transfers, sales, listings, bids, and all other events
"""
import requests
import pandas as pd
import time
from datetime import datetime
import json
import os
from typing import List, Dict

# ============================================================================
# CONFIGURATION
# ============================================================================
API_KEY = "5575c781fdb2424f8e5aa693c8f68a35"
BASE_URL = "https://api.opensea.io/api/v2"
HEADERS = {
    "accept": "application/json",
    "X-API-KEY": API_KEY
}

CRYPTOPUNKS_CONTRACT = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
CHAIN = "ethereum"

# Files
INPUT_FILE = "cryptopunks_financial_dataset.csv"
OUTPUT_FILE = "cryptopunks_complete_activity_history.csv"
CHECKPOINT_FILE = "extraction_checkpoint.json"
SUMMARY_FILE = "extraction_summary.json"
ERROR_LOG_FILE = "extraction_errors.log"

# Rate limiting
REQUESTS_PER_MINUTE = 100
REQUEST_DELAY = 60 / REQUESTS_PER_MINUTE  # Seconds between requests
CHECKPOINT_INTERVAL = 10  # Save progress every N NFTs
COOLDOWN_AFTER_BATCH = 5  # Seconds to wait after checkpoint

# ============================================================================
# CHECKPOINT MANAGEMENT
# ============================================================================
def load_checkpoint() -> Dict:
    """Load extraction progress checkpoint"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"processed_ids": [], "last_index": 0, "total_activities": 0}

def save_checkpoint(checkpoint: Dict):
    """Save extraction progress checkpoint"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

def log_error(message: str):
    """Log errors to file"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(ERROR_LOG_FILE, 'a') as f:
        f.write(f"[{timestamp}] {message}\n")

# ============================================================================
# DATA EXTRACTION
# ============================================================================
def get_nft_events(token_id: str, max_retries: int = 3) -> List[Dict]:
    """
    Fetch ALL events for a specific CryptoPunk token
    Includes: mint, transfer, sale, listing, bid, cancel, etc.
    """
    all_events = []
    next_cursor = None
    max_pages = 50  # Increased to capture full history
    page = 0
    retries = 0
    
    while page < max_pages:
        url = f"{BASE_URL}/events/chain/{CHAIN}/contract/{CRYPTOPUNKS_CONTRACT}/nfts/{token_id}"
        params = {"limit": 50}
        
        if next_cursor:
            params["next"] = next_cursor
        
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()
            
            events = data.get("asset_events", [])
            all_events.extend(events)
            
            next_cursor = data.get("next")
            
            if not next_cursor:
                break
            
            page += 1
            retries = 0  # Reset retry counter on success
            time.sleep(REQUEST_DELAY)
            
        except requests.exceptions.Timeout:
            retries += 1
            if retries < max_retries:
                wait_time = 2 ** retries  # Exponential backoff
                print(f"      Timeout (attempt {retries}/{max_retries}), waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                log_error(f"Timeout fetching events for token {token_id} after {max_retries} retries")
                break
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"      Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                continue
            else:
                log_error(f"HTTP {e.response.status_code} for token {token_id}: {str(e)}")
                break
                
        except Exception as e:
            log_error(f"Error fetching events for token {token_id}: {str(e)}")
            break
    
    return all_events

def parse_event_to_activity(event: Dict, identifier: str) -> Dict:
    """
    Parse OpenSea event into standardized activity format
    Captures all event types with full detail
    """
    # Event type
    event_type = event.get("event_type", "unknown")
    
    # Price extraction
    price_eth = 0.0
    payment = event.get("payment")
    if payment:
        try:
            quantity = float(payment.get("quantity", 0))
            decimals = int(payment.get("decimals", 18))
            if quantity > 0 and decimals > 0:
                price_eth = quantity / (10 ** decimals)
        except (ValueError, TypeError):
            price_eth = 0.0
    
    # Address extraction
    from_address = event.get("from_address") or "0x0"
    to_address = event.get("to_address") or "0x0"
    
    def format_address(addr: str) -> str:
        """Format address for readability"""
        if not addr or addr == "0x0":
            return "0x0"
        if len(addr) > 12:
            return f"{addr[:6]}...{addr[-4:]}"
        return addr
    
    # Timestamp parsing
    timestamp_str = event.get("event_timestamp", "")
    if timestamp_str:
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, AttributeError):
            # Fallback to raw timestamp if it's already a Unix timestamp
            try:
                timestamp = str(int(float(timestamp_str)))
            except:
                timestamp = timestamp_str
    else:
        timestamp = ""
    
    # Transaction hash
    tx_hash = event.get("transaction", "")
    if len(tx_hash) > 16:
        tx_hash = f"{tx_hash[:10]}..."
    
    return {
        "identifier": str(identifier),
        "name": f"CryptoPunk #{identifier}",
        "event_type": event_type,
        "price_eth": round(price_eth, 6),
        "from": format_address(from_address),
        "to": format_address(to_address),
        "timestamp": timestamp,
        "tx_hash": tx_hash
    }

# ============================================================================
# MAIN EXTRACTION LOGIC
# ============================================================================
def main():
    print("\n" + "="*80)
    print("CRYPTOPUNKS COMPLETE ACTIVITY HISTORY EXTRACTION")
    print("="*80)
    print("Extracting FULL history: mint -> transfers -> sales -> listings -> all events")
    print("="*80 + "\n")
    
    # Load input
    try:
        df_financial = pd.read_csv(INPUT_FILE)
        identifiers = df_financial['identifier'].astype(str).tolist()
        print(f"Loaded {len(identifiers)} CryptoPunks from {INPUT_FILE}")
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found!")
        print("Please ensure the financial dataset exists.")
        return
    
    # Load checkpoint
    checkpoint = load_checkpoint()
    processed_ids = set(checkpoint.get("processed_ids", []))
    start_index = checkpoint.get("last_index", 0)
    
    if processed_ids:
        print(f"Resuming from checkpoint:")
        print(f"   - Already processed: {len(processed_ids)} NFTs")
        print(f"   - Starting from index: {start_index}")
    
    # Initialize error log
    if not os.path.exists(ERROR_LOG_FILE):
        with open(ERROR_LOG_FILE, 'w') as f:
            f.write(f"Extraction started at {datetime.now()}\n")
    
    print(f"\n{'='*80}")
    print(f"EXTRACTION START")
    print(f"{'='*80}\n")
    
    all_activities = []
    total = len(identifiers)
    start_time = time.time()
    failed_ids = []
    
    # Process each NFT
    for idx in range(start_index, total):
        token_id = identifiers[idx]
        
        if token_id in processed_ids:
            continue
        
        # Progress metrics
        progress_pct = ((idx + 1) / total) * 100
        elapsed = time.time() - start_time
        processed_count = idx - start_index + 1
        rate = processed_count / elapsed if elapsed > 0 else 0
        remaining = total - idx - 1
        eta = remaining / rate if rate > 0 else 0
        
        print(f"[{idx+1}/{total}] ({progress_pct:.1f}%) CryptoPunk #{token_id}")
        print(f"    Processed: {processed_count} | Rate: {rate:.2f}/min | ETA: {eta/60:.1f}min")
        
        # Fetch events
        print(f"    Fetching complete event history...")
        events = get_nft_events(token_id)
        
        if len(events) == 0:
            print(f"    No events found")
            failed_ids.append(token_id)
        else:
            print(f"    Found {len(events)} events")
            
            # Parse events
            for event in events:
                activity = parse_event_to_activity(event, token_id)
                all_activities.append(activity)
        
        # Update checkpoint
        processed_ids.add(token_id)
        checkpoint = {
            "processed_ids": list(processed_ids),
            "last_index": idx + 1,
            "total_activities": len(all_activities)
        }
        save_checkpoint(checkpoint)
        
        # Periodic save
        if (idx + 1) % CHECKPOINT_INTERVAL == 0:
            if all_activities:
                df_temp = pd.DataFrame(all_activities)
                df_temp = df_temp.sort_values(["identifier", "timestamp"], ascending=[True, True])
                df_temp.to_csv(f"TEMP_{OUTPUT_FILE}", index=False)
                print(f"    Progress saved: {len(all_activities)} activities")
            
            print(f"    Cooling down for {COOLDOWN_AFTER_BATCH}s...")
            time.sleep(COOLDOWN_AFTER_BATCH)
        else:
            time.sleep(REQUEST_DELAY)
        
        print()
    
    # Save final results
    if all_activities:
        print("\n" + "="*80)
        print("SAVING FINAL DATASET")
        print("="*80 + "\n")
        
        df = pd.DataFrame(all_activities)
        df = df.sort_values(["identifier", "timestamp"], ascending=[True, True])
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"Saved to: {OUTPUT_FILE}")
        
        # Generate summary
        print("\nGenerating summary statistics...")
        
        summary = {
            "extraction_date": datetime.now().isoformat(),
            "total_activities": len(all_activities),
            "unique_nfts_with_activity": df['identifier'].nunique(),
            "total_nfts_processed": len(processed_ids),
            "nfts_with_no_events": len(failed_ids),
            "extraction_time_minutes": (time.time() - start_time) / 60,
            "event_type_distribution": df['event_type'].value_counts().to_dict()
        }
        
        # Price statistics
        price_df = df[df['price_eth'] > 0]
        if len(price_df) > 0:
            summary["price_statistics"] = {
                "total_volume_eth": float(price_df['price_eth'].sum()),
                "transactions_with_price": len(price_df),
                "avg_price_eth": float(price_df['price_eth'].mean()),
                "median_price_eth": float(price_df['price_eth'].median()),
                "min_price_eth": float(price_df['price_eth'].min()),
                "max_price_eth": float(price_df['price_eth'].max())
            }
        
        # Activity statistics
        activity_stats = df.groupby('identifier').size()
        summary["activity_statistics"] = {
            "avg_events_per_nft": float(activity_stats.mean()),
            "median_events_per_nft": float(activity_stats.median()),
            "min_events": int(activity_stats.min()),
            "max_events": int(activity_stats.max())
        }
        
        with open(SUMMARY_FILE, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"Summary saved to: {SUMMARY_FILE}")
        
        # Clean up
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
        if os.path.exists(f"TEMP_{OUTPUT_FILE}"):
            os.remove(f"TEMP_{OUTPUT_FILE}")
        
        # Final report
        print("\n" + "="*80)
        print("EXTRACTION COMPLETE!")
        print("="*80)
        print(f"Total activities: {len(all_activities)}")
        print(f"NFTs with activity: {df['identifier'].nunique()}")
        print(f"Total time: {(time.time() - start_time)/60:.1f} minutes")
        print(f"Average rate: {len(processed_ids)/(elapsed/60):.2f} NFTs/minute")
        
        if failed_ids:
            print(f"\n{len(failed_ids)} NFTs had no events")
        
        print(f"\nEvent Type Breakdown:")
        for event_type, count in df['event_type'].value_counts().items():
            print(f"   {event_type}: {count:,}")
        
        print("\n" + "="*80)
        print("Your complete activity history dataset is ready!")
        print(f"File: {OUTPUT_FILE}")
        print("="*80 + "\n")
        
    else:
        print("\nNo activities extracted!")


if __name__ == "__main__":
    main()
