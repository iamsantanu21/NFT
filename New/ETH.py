from web3 import Web3
import pandas as pd
from tqdm import tqdm
import time

# ==============================
# CONFIG
# ==============================

ALCHEMY_URL = "https://eth-mainnet.g.alchemy.com/v2/u2b7JQUNLzkgObMtQi8n1"
CONTRACT_ADDRESS = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))
if not w3.isConnected():
    print("❌ ERROR: Cannot connect to Ethereum RPC")
    exit()

print("Ethereum RPC connected:", w3.clientVersion)

contract_address = Web3.to_checksum_address(CONTRACT_ADDRESS)

# Topic hash for PunkTransfer event
transfer_topic = w3.keccak(text="PunkTransfer(address,address,uint256)").hex()

# ==============================
# BLOCK RANGE
# ==============================

# Punk genesis block ~ 3.918M
start_block = 3918000
end_block = w3.eth.block_number
step = 5000  # blocks per batch

all_transfers = []

print(f"Fetching PunkTransfer events from block {start_block} to {end_block}...")

for from_block in tqdm(range(start_block, end_block + 1, step)):
    to_block = min(from_block + step - 1, end_block)

    try:
        logs = w3.eth.get_logs({
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": contract_address,
            "topics": [transfer_topic]
        })

        for log in logs:
            from_addr = "0x" + log["topics"][1].hex()[-40:]
            to_addr = "0x" + log["topics"][2].hex()[-40:]
            token_id = int(log["topics"][3].hex(), 16)

            tx_hash = log["transactionHash"].hex()
            block_number = log["blockNumber"]
            timestamp = w3.eth.get_block(block_number)["timestamp"]

            all_transfers.append({
                "token_id": token_id,
                "from": from_addr,
                "to": to_addr,
                "block_number": block_number,
                "timestamp": timestamp,
                "transaction_hash": tx_hash
            })

    except Exception as e:
        print(f"⚠ Error retrieving blocks {from_block}→{to_block}: {e}")
        time.sleep(2)

print("Total transfers collected:", len(all_transfers))

df = pd.DataFrame(all_transfers)
df.sort_values(by=["block_number", "token_id"], inplace=True)
df.to_csv("blockchain_punk_transfers.csv", index=False)

print("✔ EXPORT COMPLETE: blockchain_punk_transfers.csv")