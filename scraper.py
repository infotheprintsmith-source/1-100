import ccxt
import requests
import os
import time

MASTER_IP = os.getenv('MASTER_IP')
START = int(os.getenv('START_INDEX', 0))
END = int(os.getenv('END_INDEX', 22)) # Pehle account ke liye 0-22

exchange = ccxt.gateio()

def run_scraper():
    try:
        response = requests.get(f"http://{MASTER_IP}:8000/get_symbols", timeout=15)
        all_db_symbols = response.json()
        my_symbols = all_db_symbols[START:END]
        
        print(f"Scanning: {len(my_symbols)} coins")

        for sym in my_symbols:
            try:
                gate_sym = sym.replace('USDT', '/USDT')
                ticker = exchange.fetch_ticker(gate_sym)
                payload = {"symbol": sym, "price": float(ticker['last'])}
                requests.post(f"http://{MASTER_IP}:8000/receive_data", json=payload, timeout=10)
                print(f"✅ Sent: {sym}")
                time.sleep(1)
            except: continue
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_scraper()
