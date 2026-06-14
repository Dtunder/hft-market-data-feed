import asyncio
import time
import orjson
import websockets
from src.ring_buffer import LockFreeRingBuffer

class BinanceTestnetFeedClient:
    def __init__(self, symbols: list, ring_buffer_name: str = "binance_feed"):
        self.symbols = [s.lower() for s in symbols]
        self.running = False
        self.buffer = LockFreeRingBuffer(name=ring_buffer_name, size=10000, element_size=2048, create=True)

        streams = "/".join(f"{s}@depth5@100ms/{s}@aggTrade" for s in self.symbols)
        self.ws_uri = f"wss://testnet.binance.vision/stream?streams={streams}"

        # State tracking
        self.orderbooks = {s: {"bids": [], "asks": []} for s in self.symbols}
        self.latest_trades = {s: {} for s in self.symbols}
        self.last_update_ids = {s: None for s in self.symbols}

    def get_latest_orderbook(self, symbol: str) -> tuple:
        sym = symbol.lower()
        if sym in self.orderbooks:
            return self.orderbooks[sym]["bids"], self.orderbooks[sym]["asks"]
        return [], []

    def get_latest_trade(self, symbol: str) -> dict:
        sym = symbol.lower()
        return self.latest_trades.get(sym, {})

    async def connect_and_stream(self):
        self.running = True
        retry_count = 0
        max_retries = 5
        base_delay = 1.0

        while self.running and retry_count <= max_retries:
            try:
                print(f"[FEED] Connecting to {self.ws_uri}")
                async with websockets.connect(self.ws_uri) as websocket:
                    print("[FEED] Connection established.")
                    retry_count = 0  # reset on successful connection
                    self.last_update_ids = {s: None for s in self.symbols} # Reset sequence tracking on reconnect

                    while self.running:
                        packet = await websocket.recv()

                        # Parse < 1ms using orjson
                        t1 = time.perf_counter()
                        data = orjson.loads(packet)

                        # Push raw message to lock-free ring buffer
                        self.buffer.push(packet if isinstance(packet, bytes) else packet.encode('utf-8'))

                        # Process state
                        if "stream" in data and "data" in data:
                            stream_name = data["stream"]
                            payload = data["data"]

                            symbol = stream_name.split("@")[0]
                            if "@depth" in stream_name:
                                # Sequence validation for @depth5 which uses lastUpdateId
                                if "U" in payload and "u" in payload and "pu" in payload:
                                    last_u = self.last_update_ids.get(symbol)
                                    if last_u is None:
                                        self.last_update_ids[symbol] = payload["u"]
                                    else:
                                        if payload["pu"] != last_u:
                                            print(f"[FEED] Sequence mismatch for {symbol}. Expected pu={last_u}, got {payload['pu']}")
                                            raise ConnectionError("Sequence mismatch")
                                        self.last_update_ids[symbol] = payload["u"]

                                self.orderbooks[symbol]["bids"] = payload.get("bids", []) or payload.get("b", [])
                                self.orderbooks[symbol]["asks"] = payload.get("asks", []) or payload.get("a", [])
                            elif "@aggTrade" in stream_name:
                                self.latest_trades[symbol] = payload

                        t2 = time.perf_counter()
                        # Ensure latency is measured with exact float timestamps
                        latency_ms = (t2 - t1) * 1000.0
                        if latency_ms > 1.0:
                            pass # Can log slow parsing if needed

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
        self.buffer.close()
        self.buffer.unlink()
