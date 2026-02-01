import ccxt
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# GitHub Secrets se details uthana
MASTER_IP = os.getenv('MASTER_IP')
MASTER_PORT = os.getenv('MASTER_PORT', '8000')
START = int(os.getenv('START_INDEX', 0))
END = int(os.getenv('END_INDEX', 100))

# Gate.io use kar rahe hain kyunke ye GitHub par block nahi hai
exchange = ccxt.gateio({'enableRateLimit': True})

def fetch_data():
    try:
        markets = exchange.load_markets()
        # Sirf USDT pairs filter karna
        all_symbols = [s for s in markets if '/USDT' in s]
        symbols = all_symbols[START:END]
        
        print(f"Total USDT pairs found on Gate.io: {len(all_symbols)}")
        print(f"Account Task: Fetching index {START} to {END}")

        for symbol in symbols:
            try:
                print(f"Fetching 3Y history for {symbol}...")
                # 3 Saal pichay ka waqt
                since = int((datetime.now() - timedelta(days=3*365)).timestamp() * 1000)
                
                all_ohlcv = []
                while since < exchange.milliseconds():
                    # Gate.io se 1-hour candles uthana
                    ohlcv = exchange.fetch_ohlcv(symbol, '1h', since, 1000)
                    if not ohlcv or len(ohlcv) == 0: break
                    since = ohlcv[-1][0] + 1
                    all_ohlcv += ohlcv
                    time.sleep(0.1) # Rate limit ka khayal
                
                if not all_ohlcv: continue

                # Format for Master
                formatted_data = []
                for x in all_ohlcv:
                    formatted_data.append({
                        "ts": datetime.fromtimestamp(x[0]/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        "o": x[1], "h": x[2], "l": x[3], "c": x[4], "v": x[5]
                    })
                
                # Master VPS ko data bhejna (Coin by Coin taake payload bara na ho)
                response = requests.post(
                    f"http://{MASTER_IP}:{MASTER_PORT}/receive_history", 
                    json={"symbol": symbol.replace('/', ''), "data": formatted_data}, 
                    timeout=60
                )
                print(f"✅ Sent {symbol} to Master. Status: {response.status_code}")
                
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                continue

    except Exception as e:
        print(f"Exchange Connection Error: {e}")

if __name__ == "__main__":
    fetch_data()
