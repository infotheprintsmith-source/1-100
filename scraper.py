import ccxt
import requests
import os
import pandas as pd
import time

# Secrets se Details uthana
MASTER_IP = os.getenv('MASTER_IP')
MASTER_PORT = os.getenv('MASTER_PORT')
# Naya: Har account ka apna range hoga
START_INDEX = int(os.getenv('START_INDEX', 0)) 
END_INDEX = int(os.getenv('END_INDEX', 100))

MASTER_URL = f"http://{MASTER_IP}:{MASTER_PORT}/receive_data"

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return 50
    df = pd.DataFrame(prices, columns=['close'])
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])

def run_scanner():
    exchange = ccxt.gateio() 
    try:
        markets = exchange.load_markets()
        all_symbols = [s for s in markets if '/USDT' in s]
        
        # Sirf is account ke hisse ke coins uthana
        symbols = all_symbols[START_INDEX:END_INDEX]
        
        print(f"Account Task: Scanning coins from {START_INDEX} to {END_INDEX}")
        print(f"Total coins in this batch: {len(symbols)}")

        for symbol in symbols:
            try:
                bars = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=30)
                if not bars: continue
                
                closes = [x[4] for x in bars]
                current_price = closes[-1]
                rsi_val = calculate_rsi(closes)

                payload = {
                    "symbol": symbol,
                    "price": float(current_price),
                    "rsi": rsi_val
                }

                response = requests.post(MASTER_URL, json=payload, timeout=10)
                print(f"Sent: {symbol} | Price: {current_price} | Status: {response.status_code}")
                
                time.sleep(0.1) 
            except:
                continue
    except Exception as e:
        print(f"Exchange error: {e}")

if __name__ == "__main__":
    run_scanner()
