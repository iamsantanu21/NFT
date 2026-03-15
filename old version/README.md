# CryptoPunks NFT Dataset Collector

A Python script to collect CryptoPunks NFT data from OpenSea API, including metadata, traits, rarity scores, and sales history.

## 📦 Collection Info

| Property | Value |
|----------|-------|
| Collection | CryptoPunks |
| API Provider | OpenSea |
| Blockchain | Ethereum |

---

## 🖥️ Windows Installation

### ✅ 1️⃣ Install Python (Required)

#### 🔹 Recommended Version
- **Python 3.10 – 3.12**
- ⚠️ Do NOT use Python 3.14 (very new, can cause library issues)

#### 🔹 Download From
👉 https://www.python.org/downloads/

#### ⚠️ Important During Installation
✔ **Check: "Add Python to PATH"**

After installation, verify:
```cmd
python --version
```

### ✅ 2️⃣ Install Required Libraries

Open **Command Prompt (cmd)** or **PowerShell** and run:

```cmd
pip install requests pandas tqdm
```

That's it. Only 3 packages needed.

### ✅ 3️⃣ Optional (Recommended) — Create Virtual Environment

Inside your project folder:

```cmd
python -m venv venv
```

Activate it:

**For CMD:**
```cmd
venv\Scripts\activate
```

**For PowerShell:**
```powershell
venv\Scripts\Activate.ps1
```

Then install packages inside it:
```cmd
pip install requests pandas tqdm
```

### ✅ 4️⃣ Run the Script

```cmd
python nft.py
```

---

## 🍎 macOS Installation

### ✅ 1️⃣ Install Python (Required)

#### 🔹 Recommended Version
- **Python 3.10 – 3.12**
- ⚠️ Do NOT use Python 3.14 (very new, can cause library issues)

#### 🔹 Installation Options

**Option A: Download from Python.org**
👉 https://www.python.org/downloads/

**Option B: Install via Homebrew (Recommended)**
```bash
brew install python@3.12
```

After installation, verify:
```bash
python3 --version
```

### ✅ 2️⃣ Install Required Libraries

Open **Terminal** and run:

```bash
pip3 install requests pandas tqdm
```

That's it. Only 3 packages needed.

### ✅ 3️⃣ Optional (Recommended) — Create Virtual Environment

Inside your project folder:

```bash
python3 -m venv venv
```

Activate it:
```bash
source venv/bin/activate
```

Then install packages inside it:
```bash
pip install requests pandas tqdm
```

### ✅ 4️⃣ Run the Script

```bash
python3 nft.py
```

---

## 📁 Folder Structure

```
NFT/
├── nft.py                          # Main script
├── collection.json                 # Collection metadata (generated)
├── nfts_master.csv                 # NFT data with rarity scores (generated)
├── sales_history.csv               # Sales history (generated)
├── cryptopunks_nfts_dataset.csv    # Dataset file
├── README.md                       # This file
└── raw/
    ├── nft_json/                   # Individual NFT JSON files
    ├── nfts/                       # NFT data files
    └── sale_json/                  # Sale event JSON files
```

---

## 🌐 Internet Requirements

Make sure:
- ✔ Stable internet connection
- ✔ No VPN blocking OpenSea
- ✔ Firewall not blocking Python
- ✔ DNS working properly

💡 **Tip:** If corporate WiFi blocks it, try using a mobile hotspot.

---

## ✅ Expected Output

After running the script successfully:

| File | Description |
|------|-------------|
| `collection.json` | Collection metadata |
| `nfts_master.csv` | All NFTs with traits and rarity scores |
| `sales_history.csv` | Historical sales data |
| `raw/nft_json/*.json` | Individual NFT JSON files |

---

## ❌ You DO NOT Need

- ❌ Node.js
- ❌ npm
- ❌ nvm
- ❌ Docker
- ❌ Anaconda
- ❌ Database
- ❌ Web3.py

**Just Python + 3 libraries.**

---

## 🚀 Minimum Requirements Summary

| Component | Required |
|-----------|----------|
| Python 3.10–3.12 | ✅ |
| requests | ✅ |
| pandas | ✅ |
| tqdm | ✅ |
| OpenSea API key | ✅ |
| Internet connection | ✅ |

---

## 🔑 API Key Setup

The script uses an OpenSea API key. To get your own:

1. Go to https://opensea.io/
2. Create an account
3. Request API access at https://docs.opensea.io/reference/api-keys
4. Replace the `API_KEY` value in `nft.py` with your key

---

## 📝 Script Features

- ✅ Fetches all CryptoPunks NFT metadata
- ✅ Extracts traits and attributes
- ✅ Calculates rarity scores
- ✅ Ranks NFTs by rarity
- ✅ Collects sales history
- ✅ Saves data in CSV and JSON formats

---

## ⚠️ Troubleshooting

### Rate Limiting
The script includes `time.sleep(1)` between API calls to avoid rate limits.

### API Errors
If you get 401/403 errors, check:
- Your API key is valid
- You haven't exceeded rate limits
- Your IP isn't blocked

### Import Errors
If you get `ModuleNotFoundError`, reinstall packages:
```bash
pip install --upgrade requests pandas tqdm
```

---

## 📜 License

This project is for educational purposes. Please respect OpenSea's Terms of Service and API usage guidelines.
