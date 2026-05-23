import asyncio
import json
import time
import websockets
import orjson

class MultiSymbolFeed:
    """
    WebSocket client for aggregating multiple symbols from Binance combined streams.
    """
    def __init__(self, symbols: list, use_mainnet: bool = False):
        self.symbols = [s.lower() for s in symbols]
        self.use_mainnet = use_mainnet
        self.callbacks = []
        self.running = False

        streams = "/".join(f"{s}@depth20@100ms/{s}@aggTrade" for s in self.symbols)
        if self.use_mainnet:
            self.ws_uri = f"wss://stream.binance.com:9443/stream?streams={streams}"
        else:
            self.ws_uri = f"wss://testnet.binance.vision/stream?streams={streams}"

    def add_callback(self, fn):
        self.callbacks.append(fn)

    async def connect_and_stream(self):
        print(f"[MULTI-FEED] Connecting to stream: {self.ws_uri}")
        self.running = True

        retry_count = 0
        while self.running and retry_count < 3:
            try:
                async with websockets.connect(self.ws_uri) as websocket:
                    print("[MULTI-FEED] Connection established. Ingesting packets...")
                    retry_count = 0  # reset on successful connection
                    while self.running:
                        packet = await websocket.recv()
                        data = orjson.loads(packet)

                        if "data" in data and "stream" in data:
                            stream_name = data["stream"]
                            payload = data["data"]

                            for cb in self.callbacks:
                                await cb(stream_name, payload)

            except websockets.exceptions.ConnectionClosed:
                print(f"[MULTI-FEED] Warning: WebSocket connection closed. Reconnecting... (Attempt {retry_count + 1}/3)")
                retry_count += 1
                if retry_count < 3 and self.running:
                    await asyncio.sleep(2)
            except Exception as e:
                print(f"[MULTI-FEED] Error: {e}")
                break

    def stop(self):
        self.running = False


class HFTMarketDataFeed:
    """
    High-performance real-time WebSocket client for HFT order book/trade ingestion.
    Aims for sub-millisecond parsing latency.
    """
    def __init__(self, symbol="btcusdt", use_mainnet: bool = False):
        self.symbol = symbol.lower()
        self.use_mainnet = use_mainnet

        if self.use_mainnet:
            ws_base = "wss://stream.binance.com:9443/ws"
            rest_base = "https://api.binance.com/api/v3"
        else:
            ws_base = "wss://testnet.binance.vision/ws"
            rest_base = "https://testnet.binance.vision/api/v3"

        self.ws_uri = ws_base
        self.rest_uri = rest_base

        # Public Binance WebSocket stream URL for raw aggregate trades
        self.uri = f"{ws_base}/{self.symbol}@aggTrade"
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

async def main():
    # Show old single-symbol usage
    old_feed = HFTMarketDataFeed()

    # Show new multi-symbol usage
    multi_feed = MultiSymbolFeed(["btcusdt", "ethusdt"], use_mainnet=False)

    async def demo_callback(stream, data):
        print(f"[{stream}] event_type={data.get('e')}")

    multi_feed.add_callback(demo_callback)

    print("[MAIN] Starting feeds for 10 seconds...")

    task_old = asyncio.create_task(old_feed.connect_and_stream())
    task_multi = asyncio.create_task(multi_feed.connect_and_stream())

    await asyncio.sleep(10)

    print("\n[MAIN] Stopping feeds...")
    old_feed.running = False
    multi_feed.stop()

    # Wait for tasks to finish cleanly (they will exit their while loops)
    # Using gather with return_exceptions=True to avoid unhandled exceptions
    # crashing the script if connection fails (e.g., HTTP 451 from Binance)
    await asyncio.gather(task_old, task_multi, return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] Shutting down high-speed market data client.")
