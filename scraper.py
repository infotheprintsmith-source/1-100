import ccxt
import requests
import os
import time

# Environmental variables
MASTER_IP = os.getenv('MASTER_IP')
START = int(os.getenv('START_INDEX', 0))
END = int(os.getenv('END_INDEX', 22))

exchange = ccxt.gateio()

def run_scraper():
    try:
        # 1. Master Server se symbols ki list mangwao
        response = requests.get(f"http://{MASTER_IP}:8000/get_symbols", timeout=15)
        all_db_symbols = response.json()
        my_symbols = all_db_symbols[START:END]
        
        print(f"Scanning: {len(my_symbols)} coins")

        for sym in my_symbols:
            try:
                gate_sym = sym.replace('USDT', '/USDT')
                
                # --- A. Price Data (Ticker) ---
                ticker = exchange.fetch_ticker(gate_sym)
                price_payload = {"symbol": sym, "price": float(ticker['last'])}
                requests.post(f"http://{MASTER_IP}:8000/receive_data", json=price_payload, timeout=10)
                
                # --- B. Order Book Data (Bids/Asks) ---
                # Hum top 5 levels uthayenge lekin sirf top level bhejenge optimization ke liye
                book = exchange.fetch_order_book(gate_sym, limit=5)
                book_payload = {
                    "symbol": sym,
                    "bid_price": float(book['bids'][0][0]), # Best Buy Price
                    "bid_size": float(book['bids'][0][1]),  # Volume at Best Buy
                    "ask_price": float(book['asks'][0][0]), # Best Sell Price
                    "ask_size": float(book['asks'][0][1])   # Volume at Best Sell
                }
                requests.post(f"http://{MASTER_IP}:8000/receive_books", json=book_payload, timeout=10)
                
                print(f"✅ Sent Full Data: {sym}")
                time.sleep(1) # IP block se bachne ke liye gap
                
            except Exception as e:
                print(f"❌ Error on {sym}: {e}")
                continue
                
    except Exception as e:
        print(f"Master Server Connection Error: {e}")

if __name__ == "__main__":
    run_scraper()
