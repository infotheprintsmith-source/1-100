import ccxt
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# GitHub Secrets se uthana
MASTER_IP = os.getenv('MASTER_IP')
MASTER_PORT = os.getenv('MASTER_PORT', '8000')
START = int(os.getenv('START_INDEX', 0))
END = int(os.getenv('END_INDEX', 100))

exchange = ccxt.binance()

def fetch_data():
    markets = exchange.load_markets()
    symbols = [s for s in markets if '/USDT' in s][START:END]
    
    for symbol in symbols:
        print(f"Fetching 3Y history for {symbol}...")
        # 3 Saal pichay ka waqt
        since = int((datetime.now() - timedelta(days=3*365)).timestamp() * 1000)
        
        all_ohlcv = []
        while since < exchange.milliseconds():
            ohlcv = exchange.fetch_ohlcv(symbol, '1h', since, 1000)
            if not ohlcv: break
            since = ohlcv[-1][0] + 1
            all_ohlcv += ohlcv
        
        # Format for Master
        formatted_data = []
        for x in all_ohlcv:
            formatted_data.append({
                "ts": datetime.fromtimestamp(x[0]/1000).strftime('%Y-%m-%d %H:%M:%S'),
                "o": x[1], "h": x[2], "l": x[3], "c": x[4], "v": x[5]
            })
            
        # Send to Master
        try:
            requests.post(f"http://{MASTER_IP}:{MASTER_PORT}/receive_history", 
                          json={"symbol": symbol.replace('/', ''), "data": formatted_data}, 
                          timeout=30)
        except:
            print(f"Failed to send {symbol}")

if __name__ == "__main__":
    fetch_data()
