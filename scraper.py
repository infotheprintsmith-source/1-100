import ccxt
import requests
import os

# Secrets se values uthayega
MASTER_IP = os.getenv('MASTER_IP')
START = int(os.getenv('START_INDEX', 0))
END = int(os.getenv('END_INDEX', 23))

# Gate.io use karenge kyunke ye block nahi hota
exchange = ccxt.gateio()

def run_scraper():
    try:
        # 1. Master Server se database wale symbols ki list mangwana
        print(f"Connecting to Master IP: {MASTER_IP}...")
        response = requests.get(f"http://{MASTER_IP}:8000/get_symbols", timeout=10)
        all_db_symbols = response.json()
        
        # 2. Sirf Account #1 ke hisse ke coins (0 se 23)
        my_symbols = all_db_symbols[START:END]
        print(f"Total coins in DB: {len(all_db_symbols)}. I am scanning: {len(my_symbols)}")

        for sym in my_symbols:
            try:
                # Symbol format fix (e.g. BTCUSDT -> BTC/USDT)
                gate_sym = sym.replace('USDT', '/USDT')
                ticker = exchange.fetch_ticker(gate_sym)
                
                payload = {
                    "symbol": sym,
                    "price": float(ticker['last'])
                }
                # Master ko price bhejna
                requests.post(f"http://{MASTER_IP}:8000/receive_data", json=payload, timeout=5)
                print(f"✅ Synced: {sym} @ {ticker['last']}")
            except Exception as e:
                print(f"Skipping {sym}: {e}")
                continue
    except Exception as e:
        print(f"Error connecting to Master: {e}")

if __name__ == "__main__":
    run_scraper()
