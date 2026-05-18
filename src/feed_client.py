import asyncio
import orjson
import time
import websockets
import aiohttp
from src.ring_buffer import LockFreeRingBuffer

class HFTMarketDataFeed:
    """
    High-performance real-time WebSocket client for HFT order book/trade ingestion.
    Aims for sub-millisecond parsing latency.
    """
    def __init__(self, symbol="btcusdt", buffer_name="hft_feed_buffer"):
        self.symbol = symbol.lower()
        self.upper_symbol = symbol.upper()
        # Binance testnet streams (using both depth and raw trades)
        self.ws_uri = f"wss://testnet.binance.vision/ws/{self.symbol}@depth@100ms/{self.symbol}@aggTrade"
        self.rest_uri = f"https://testnet.binance.vision/api/v3/depth?symbol={self.upper_symbol}&limit=1000"
        self.running = False

        self.trade_count = 0
        self.start_time = None

        self.buffer = LockFreeRingBuffer(name=buffer_name, create=True)
        self.last_update_id = None
        self.awaiting_first_event = False

        self.buffered_events = [] # For out-of-order sequencing

    async def fetch_snapshot(self):
        """Fetches the REST snapshot for sequence recovery."""
        print(f"[FEED] Fetching REST snapshot from {self.rest_uri}")
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.rest_uri) as resp:
                    if resp.status == 451:
                        print("[FEED] Error 451: Unavailable for legal reasons (IP restricted). Using testnet might prevent this, but throwing if it happens.")
                        raise ConnectionError("HTTP 451: IP Restricted")
                    elif resp.status == 200:
                        data = await resp.json()
                        print(f"[FEED] Snapshot fetched successfully. Last Update ID: {data.get('lastUpdateId')}")
                        return data
                    else:
                        print(f"[FEED] Snapshot fetch failed with status: {resp.status}")
                        return None
            except Exception as e:
                if isinstance(e, ConnectionError):
                    raise
                print(f"[FEED] Exception during snapshot fetch: {e}")
                return None

    def process_depth_event(self, data):
        """Processes depth update and checks sequence."""
        u = data.get("u") # Final update ID
        U = data.get("U") # First update ID
        pu = data.get("pu") # Previous update ID

        if self.last_update_id is None:
            # Need snapshot first. We buffer the event.
            self.buffered_events.append(data)
            return False

        if self.awaiting_first_event:
            if U <= self.last_update_id + 1 and u >= self.last_update_id + 1:
                self.awaiting_first_event = False
                self.last_update_id = u
                return True
            elif u <= self.last_update_id:
                # Event is older than snapshot, ignore
                return True # not technically false/gap, just ignore
            else:
                print(f"[FEED] First event after snapshot doesn't overlap properly. U: {U}, u: {u}, snapshot lastUpdateId: {self.last_update_id}")
                self.last_update_id = None
                self.buffered_events.append(data)
                return False

        if pu != self.last_update_id:
            print(f"[FEED] SEQUENCE GAP DETECTED: Expected {self.last_update_id}, got {pu}. Dropping state.")
            self.last_update_id = None
            self.buffered_events.append(data)
            return False

        self.last_update_id = u
        return True

    async def connect_and_stream(self):
        print(f"[FEED] Connecting to live HFT feed: {self.ws_uri}")
        self.running = True
        self.start_time = time.time()
        
        async with websockets.connect(self.ws_uri) as websocket:
            print("[FEED] Connection established. Ingesting high-speed trade/depth packets...")
            while self.running:
                try:
                    # Capture raw WebSocket packet
                    packet = await websocket.recv()
                    recv_time = time.time()
                    
                    if isinstance(packet, str):
                        packet_bytes = packet.encode('utf-8')
                    else:
                        packet_bytes = packet

                    # High-speed JSON parsing with orjson
                    data = orjson.loads(packet_bytes)
                    
                    # Process based on event type
                    event_type = data.get("e")
                    
                    if event_type == "depthUpdate":
                        valid_sequence = self.process_depth_event(data)
                        if not valid_sequence:
                            # State recovery logic
                            snapshot = await self.fetch_snapshot()
                            if snapshot:
                                self.last_update_id = snapshot.get("lastUpdateId")
                                self.awaiting_first_event = True
                                print("[FEED] Recovered state. Attempting to apply buffered events...")
                                # Try applying buffered events that fall after the snapshot
                                new_buffer = []
                                for ev in self.buffered_events:
                                    # Process through process_depth_event logic
                                    # But since they are buffered, we manually apply the logic here
                                    U = ev.get("U")
                                    u = ev.get("u")
                                    pu = ev.get("pu")

                                    if self.awaiting_first_event:
                                        if U <= self.last_update_id + 1 and u >= self.last_update_id + 1:
                                            self.awaiting_first_event = False
                                            self.last_update_id = u
                                            self.buffer.write(orjson.dumps(ev))
                                    else:
                                        if pu == self.last_update_id:
                                            self.last_update_id = u
                                            self.buffer.write(orjson.dumps(ev))

                                self.buffered_events = new_buffer
                            continue

                    elif event_type == "aggTrade":
                        # Raw trades pass through
                        self.trade_count += 1

                        # Calculate transit latency for trades
                        event_time = data.get("E", 0) / 1000.0
                        latency_ms = (recv_time - event_time) * 1000.0
                        
                        if self.trade_count % 50 == 0:
                            elapsed = time.time() - self.start_time
                            rate = self.trade_count / elapsed
                            print(f"[FEED] Trades Ingested: {self.trade_count} | Speed: {rate:.2f} trades/sec | Latency: {latency_ms:.2f}ms")

                    # Push directly to LockFreeRingBuffer
                    self.buffer.write(packet_bytes)

                except websockets.exceptions.ConnectionClosed:
                    print("[FEED] Warning: WebSocket connection closed. Reconnecting...")
                    break
                except ConnectionError as ce:
                    # e.g., HTTP 451 error from snapshot
                    print(f"[FEED] Encountered ConnectionError: {ce}")
                    self.running = False
                    raise
                except Exception as e:
                    print(f"[FEED] Error parsing packet: {e}")
                    break

    def stop(self):
        self.running = False
        self.buffer.close()

if __name__ == "__main__":
    feed = HFTMarketDataFeed()
    try:
        asyncio.run(feed.connect_and_stream())
    except KeyboardInterrupt:
        print("\n[FEED] Shutting down high-speed market data client.")
        feed.stop()
    except Exception:
        feed.stop()
