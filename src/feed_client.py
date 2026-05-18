import asyncio
import json
import time
import websockets

class HFTMarketDataFeed:
    """
    High-performance real-time WebSocket client for HFT order book/trade ingestion.
    Aims for sub-millisecond parsing latency.
    """
    def __init__(self, symbol="btcusdt"):
        self.symbol = symbol.lower()
        # Public Binance WebSocket stream URL for raw aggregate trades
        self.uri = f"wss://stream.binance.com:9443/ws/{self.symbol}@aggTrade"
        self.running = False
        self.trade_count = 0
        self.start_time = None

    async def connect_and_stream(self):
        print(f"[FEED] Connecting to live HFT feed: {self.uri}")
        self.running = True
        self.start_time = time.time()
        
        async with websockets.connect(self.uri) as websocket:
            print("[FEED] Connection established. Ingesting high-speed trade packets...")
            while self.running:
                try:
                    # Capture raw WebSocket packet
                    packet = await websocket.recv()
                    recv_time = time.time()
                    
                    # High-speed JSON parsing
                    data = json.loads(packet)
                    self.trade_count += 1
                    
                    # Calculate transit latency (packet timestamp vs local time)
                    event_time = data.get("E", 0) / 1000.0
                    latency_ms = (recv_time - event_time) * 1000.0
                    
                    # Print statistics every 50 trades to avoid console print overhead
                    if self.trade_count % 50 == 0:
                        elapsed = time.time() - self.start_time
                        rate = self.trade_count / elapsed
                        price = data.get("p", "0.0")
                        quantity = data.get("q", "0.0")
                        print(f"[FEED] Trades Ingested: {self.trade_count} | Speed: {rate:.2f} trades/sec | Last Price: {price} | Latency: {latency_ms:.2f}ms")
                        
                except websockets.exceptions.ConnectionClosed:
                    print("[FEED] Warning: WebSocket connection closed. Reconnecting...")
                    break
                except Exception as e:
                    print(f"[FEED] Error parsing packet: {e}")
                    break

if __name__ == "__main__":
    feed = HFTMarketDataFeed()
    try:
        asyncio.run(feed.connect_and_stream())
    except KeyboardInterrupt:
        print("\n[FEED] Shutting down high-speed market data client.")
