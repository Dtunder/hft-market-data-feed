import asyncio
import json
import time
import websockets
import orjson
from src.ring_buffer import LockFreeRingBuffer

class MultiSymbolFeed:
    # (Kept for compatibility if anything else uses it)
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
    def __init__(self, symbol="btcusdt", use_mainnet: bool = False, ring_buffer_name: str = "binance_feed"):
        self.symbol = symbol.lower()
        self.use_mainnet = use_mainnet

        self.running = False
        self.buffer = LockFreeRingBuffer(name=ring_buffer_name, size=10000, element_size=2048, create=True)

        streams = f"{self.symbol}@depth5@100ms/{self.symbol}@aggTrade"

        if self.use_mainnet:
            self.ws_uri = f"wss://stream.binance.com:9443/stream?streams={streams}"
            self.rest_uri = "https://api.binance.com/api/v3"
        else:
            self.ws_uri = f"wss://testnet.binance.vision/stream?streams={streams}"
            self.rest_uri = "https://testnet.binance.vision/api/v3"

        # State tracking
        self.orderbooks = {self.symbol: {"bids": [], "asks": []}}
        self.latest_trades = {self.symbol: {}}
        self.last_update_ids = {self.symbol: None}
        self.trade_count = 0
        self.start_time = None

    def get_latest_orderbook(self, symbol: str) -> tuple:
        sym = symbol.lower()
        if sym in self.orderbooks:
            return self.orderbooks[sym]["bids"], self.orderbooks[sym]["asks"]
        return [], []

    def get_latest_trade(self, symbol: str) -> dict:
        sym = symbol.lower()
        return self.latest_trades.get(sym, {})

    async def connect_and_stream(self):
        print(f"[FEED] Connecting to live HFT feed: {self.ws_uri}")
        self.running = True
        self.start_time = time.time()
        
        retry_count = 0
        max_retries = 5
        base_delay = 1.0

        while self.running and retry_count <= max_retries:
            try:
                async with websockets.connect(self.ws_uri) as websocket:
                    print("[FEED] Connection established. Ingesting high-speed trade packets...")
                    retry_count = 0  # reset on successful connection
                    self.last_update_ids = {self.symbol: None} # Reset sequence tracking on reconnect

                    while self.running:
                        # Capture raw WebSocket packet
                        packet = await websocket.recv()

                        # High-speed JSON parsing < 1ms using orjson
                        t1 = time.perf_counter()
                        data = orjson.loads(packet)

                        # Push raw message to lock-free ring buffer
                        self.buffer.push(packet if isinstance(packet, bytes) else packet.encode('utf-8'))
                        
                        # Process state
                        if "stream" in data and "data" in data:
                            stream_name = data["stream"]
                            payload = data["data"]

                            if "@depth" in stream_name:
                                # Sequence validation for @depth5 which uses lastUpdateId
                                if "lastUpdateId" in payload:
                                    last_u = self.last_update_ids.get(self.symbol)
                                    if last_u is not None and payload["lastUpdateId"] <= last_u:
                                        print(f"[FEED] Sequence mismatch for {self.symbol}. Expected >{last_u}, got {payload['lastUpdateId']}")
                                        raise ConnectionError("Sequence mismatch")
                                    self.last_update_ids[self.symbol] = payload["lastUpdateId"]

                                self.orderbooks[self.symbol]["bids"] = payload.get("bids", []) or payload.get("b", [])
                                self.orderbooks[self.symbol]["asks"] = payload.get("asks", []) or payload.get("a", [])

                            elif "@aggTrade" in stream_name:
                                self.latest_trades[self.symbol] = payload
                                self.trade_count += 1

                                t2 = time.perf_counter()
                                latency_ms = (t2 - t1) * 1000.0

                                # Print statistics every 50 trades to avoid console print overhead
                                if self.trade_count % 50 == 0:
                                    elapsed = time.time() - self.start_time
                                    rate = self.trade_count / elapsed
                                    price = payload.get("p", "0.0")
                                    print(f"[FEED] Trades Ingested: {self.trade_count} | Speed: {rate:.2f} trades/sec | Last Price: {price} | Latency: {latency_ms:.2f}ms")

            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.InvalidStatus, ConnectionError) as e:
                if not self.running:
                    break
                retry_count += 1
                if retry_count > max_retries:
                    print(f"[FEED] Max retries reached. Stopping.")
                    break

                delay = base_delay * (2 ** (retry_count - 1))
                print(f"[FEED] Disconnected ({e}). Reconnecting in {delay}s... (Attempt {retry_count}/{max_retries})")
                await asyncio.sleep(delay)
            except Exception as e:
                print(f"[FEED] Unexpected error: {e}")
                if not self.running:
                    break
                retry_count += 1
                if retry_count > max_retries:
                    print(f"[FEED] Max retries reached. Stopping.")
                    break

                delay = base_delay * (2 ** (retry_count - 1))
                print(f"[FEED] Unexpected Disconnected ({e}). Reconnecting in {delay}s... (Attempt {retry_count}/{max_retries})")
                await asyncio.sleep(delay)

    def stop(self):
        self.running = False
        if hasattr(self, 'buffer'):
            self.buffer.close()
            self.buffer.unlink()

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
    old_feed.stop()
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
