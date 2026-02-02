import ccxt, requests, os, time

MASTER_IP = os.getenv('MASTER_IP')
START = int(os.getenv('START_INDEX', 0))
END = int(os.getenv('END_INDEX', 22))
exchange = ccxt.gateio()

def run_scraper():
    try:
        response = requests.get(f"http://{MASTER_IP}:8000/get_symbols", timeout=15)
        my_symbols = response.json()[START:END]
        
        for sym in my_symbols:
            try:
                gate_sym = sym.replace('USDT', '/USDT')
                
                # 1. Price
                ticker = exchange.fetch_ticker(gate_sym)
                requests.post(f"http://{MASTER_IP}:8000/receive_data", json={"symbol": sym, "price": float(ticker['last'])})
                
                # 2. Order Book
                book = exchange.fetch_order_book(gate_sym, limit=5)
                book_payload = {
                    "symbol": sym, "bid_price": float(book['bids'][0][0]), "bid_size": float(book['bids'][0][1]),
                    "ask_price": float(book['asks'][0][0]), "ask_size": float(book['asks'][0][1])
                }
                requests.post(f"http://{MASTER_IP}:8000/receive_books", json=book_payload)
                
                # 3. Market Trades (Recent 10 trades)
                trades = exchange.fetch_trades(gate_sym, limit=10)
                trades_list = [{"side": t['side'], "price": t['price'], "amount": t['amount'], "cost": t['cost']} for t in trades]
                requests.post(f"http://{MASTER_IP}:8000/receive_trades", json={"symbol": sym, "trades": trades_list})
                
                print(f"✅ Full Data Sent: {sym}")
                time.sleep(1)
            except: continue
    except Exception as e: print(f"Error: {e}")

run_scraper()
