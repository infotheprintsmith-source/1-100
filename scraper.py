import ccxt
import requests
import os
import pandas as pd
import time

# Secrets se Master VPS ki details uthana
MASTER_IP = os.getenv('MASTER_IP')
MASTER_PORT = os.getenv('MASTER_PORT')
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
    exchange = ccxt.binance()
    try:
        markets = exchange.load_markets()
        # Pehle 100 USDT pairs ko filter karna
        symbols = [s for s in markets if '/USDT' in s][:100] 
        print(f"Scanning {len(symbols)} coins...")

        for symbol in symbols:
            try:
                # 1 Hour candles fetch karna
                bars = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=30)
                closes = [x[4] for x in bars]
                current_price = closes[-1]
                rsi_val = calculate_rsi(closes)

                payload = {
                    "symbol": symbol,
                    "price": float(current_price),
                    "rsi": rsi_val
                }

                # Master VPS ko data bhejna
                response = requests.post(MASTER_URL, json=payload, timeout=10)
                print(f"Sent: {symbol} | Status: {response.status_code}")
                
                time.sleep(0.2) # Binance rate limit se bachne ke liye
            except Exception as e:
                continue
    except Exception as e:
        print(f"Exchange error: {e}")

if __name__ == "__main__":
    run_scanner()
