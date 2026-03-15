def format_ethereum_address(address):
    if address.startswith("0x") and len(address) == 42:
        return address.lower()
    raise ValueError("Invalid Ethereum address format")

def convert_price_format(price):
    try:
        return float(price.replace('$', '').replace(',', ''))
    except ValueError:
        raise ValueError("Invalid price format")

def handle_timestamp(timestamp):
    from datetime import datetime
    try:
        return datetime.fromtimestamp(timestamp).isoformat()
    except Exception as e:
        raise ValueError(f"Error handling timestamp: {e}")