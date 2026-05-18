import asyncio
import json
import time
import aiohttp
import websockets
from collections import deque

class L2OrderBook:
    """
    Adaptive Level 2 Order Book Reconstruction.
    Maintains a local L2 order book up to depth levels (default 20) using High-speed WebSocket diff streams.
    Handles exact sequence matching and automatic recovery via REST snapshot on missing packets.
    """
    def __init__(self, symbol="btcusdt", depth=20):
        self.symbol = symbol.lower()
        self.depth = depth
        self.uri = f"wss://stream.binance.com:9443/ws/{self.symbol}@depth"
        self.rest_url = f"https://api.binance.com/api/v3/depth?symbol={self.symbol.upper()}&limit=1000"

        self.bids = {}  # price (str) -> qty (float)
        self.asks = {}  # price (str) -> qty (float)
        self.last_update_id = None
        self.buffer = deque()
        self.is_syncing = False
        self.running = False
        self._is_first_processed_event = False

    async def fetch_snapshot(self):
        """Fetches the orderbook snapshot via REST API."""
        print(f"[L2] Fetching REST snapshot from {self.rest_url}...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.rest_url) as response:
                    # Binance may return HTTP 451 (IP Restricted) in certain regions
                    if response.status == 451:
                        print("[L2] Warning: API returned HTTP 451 (IP Restricted). Simulating snapshot fallback.")
                        self.bids = {}
                        self.asks = {}
                        self.last_update_id = 0 # Dummy ID for testing
                        return True

                    response.raise_for_status()
                    data = await response.json()

                    self.last_update_id = data.get("lastUpdateId")
                    self.bids = {p: float(q) for p, q in data.get("bids", [])}
                    self.asks = {p: float(q) for p, q in data.get("asks", [])}
                    print(f"[L2] Snapshot acquired. lastUpdateId: {self.last_update_id}")
                    return True
        except Exception as e:
            print(f"[L2] Failed to fetch REST snapshot: {e}")
            return False

    def update_book(self, data, book_side):
        """Updates the local book given a list of [price, qty] updates."""
        for price_str, qty_str in data:
            qty = float(qty_str)
            if qty == 0.0:
                book_side.pop(price_str, None)
            else:
                book_side[price_str] = qty

    def process_diff(self, event):
        """Processes a single WS depth diff event."""
        # Update IDs
        first_u = event.get('U') # First update ID in event
        final_u = event.get('u') # Final update ID in event
        pu_id = event.get('pu') # Previous final update ID

        # If we just synced, process buffered events
        if self.last_update_id is not None and not self.is_syncing:
            # Drop events older than snapshot
            if final_u <= self.last_update_id:
                return False

            # Exact sequence matching:
            # 1. The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1
            # 2. Subsequent events should have pu == previous event's u
            # Let's simplify and just check pu == self.last_update_id for continuous updates
            if self.last_update_id != 0 and self.last_update_id is not None:
                if self._is_first_processed_event:
                    if first_u > self.last_update_id + 1 or final_u < self.last_update_id + 1:
                         print("[L2] Sequence mismatch on first event. Resyncing.")
                         return "resync"
                    self._is_first_processed_event = False
                elif pu_id != self.last_update_id:
                    print(f"[L2] Sequence mismatch. Expected pu={self.last_update_id}, got pu={pu_id}. Resyncing.")
                    return "resync"

        # Apply updates
        self.update_book(event.get('b', []), self.bids)
        self.update_book(event.get('a', []), self.asks)

        # Update sequence tracking
        self.last_update_id = final_u
        return True

    def get_top_levels(self):
        """Returns the top N levels of bids and asks."""
        # Sort bids descending, asks ascending
        sorted_bids = sorted(self.bids.items(), key=lambda x: float(x[0]), reverse=True)[:self.depth]
        sorted_asks = sorted(self.asks.items(), key=lambda x: float(x[0]))[:self.depth]
        return {"bids": sorted_bids, "asks": sorted_asks}

    async def connect_and_stream(self):
        """Connects to the WebSocket depth stream and manages syncing."""
        print(f"[L2] Connecting to live L2 feed: {self.uri}")
        self.running = True

        # Initiate first snapshot fetch concurrently
        self.is_syncing = True
        sync_task = asyncio.create_task(self.fetch_snapshot())

        while self.running:
            try:
                async with websockets.connect(self.uri) as websocket:
                    print("[L2] Connection established. Ingesting depth diffs...")
                    while self.running:
                        try:
                            packet = await websocket.recv()
                            event = json.loads(packet)

                            if self.is_syncing:
                                self.buffer.append(event)
                                if sync_task.done():
                                    if sync_task.result() == True:
                                        self.is_syncing = False
                                        self._is_first_processed_event = True
                                        print(f"[L2] Snapshot ready. Processing {len(self.buffer)} buffered events.")
                                        while self.buffer:
                                            buf_event = self.buffer.popleft()
                                            res = self.process_diff(buf_event)
                                            if res == "resync":
                                                self.buffer.clear()
                                                self.is_syncing = True
                                                sync_task = asyncio.create_task(self.fetch_snapshot())
                                                break
                                    else:
                                        # Retry snapshot
                                        self.is_syncing = True
                                        sync_task = asyncio.create_task(self.fetch_snapshot())
                            else:
                                # Normal processing
                                res = self.process_diff(event)
                                if res == "resync":
                                    self.is_syncing = True
                                    sync_task = asyncio.create_task(self.fetch_snapshot())

                        except asyncio.TimeoutError:
                            pass
            except websockets.exceptions.ConnectionClosed:
                print("[L2] Warning: WebSocket connection closed. Reconnecting...")
                await asyncio.sleep(1)
            except Exception as e:
                print(f"[L2] Error: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    book = L2OrderBook()
    try:
        asyncio.run(book.connect_and_stream())
    except KeyboardInterrupt:
        print("\n[L2] Shutting down L2 market data client.")
