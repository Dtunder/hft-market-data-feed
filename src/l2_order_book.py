import asyncio
import json
import time
import websockets
import aiohttp

class L2OrderBook:
    """
    Adaptive Level 2 Order Book Reconstruction
    Reconstructs local L2 order books up to 20 levels deep from high-speed WebSocket diff streams.
    Maintains exact sequence numbers, handles missing packets by automatically triggering REST snapshots.
    """
    def __init__(self, symbol="btcusdt", depth=20):
        self.symbol = symbol.lower()
        self.depth = depth
        self.ws_uri = f"wss://stream.binance.com:9443/ws/{self.symbol}@depth@100ms"
        self.rest_uri = f"https://api.binance.com/api/v3/depth?symbol={self.symbol.upper()}&limit=1000"
        self.running = False

        self.bids = {} # price (float) -> quantity (float)
        self.asks = {} # price (float) -> quantity (float)

        self.last_update_id = None
        self.buffer = []
        self.syncing = False
        self.synced = False

        # Benchmarking stats
        self.update_count = 0
        self.start_time = None
        self.total_latency_ms = 0.0

    def _process_updates(self, bids, asks):
        """Process bid and ask updates and maintain sorted book up to self.depth levels."""
        for price_str, qty_str in bids:
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0.0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for price_str, qty_str in asks:
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0.0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

    def get_top_levels(self):
        """Returns the top `self.depth` bids and asks from the maintained full book."""
        top_bids = dict(sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:self.depth])
        top_asks = dict(sorted(self.asks.items(), key=lambda x: x[0])[:self.depth])
        return top_bids, top_asks

    async def _fetch_snapshot(self):
        """Fetch REST snapshot from Binance."""
        print("[L2] Fetching REST snapshot...")
        async with aiohttp.ClientSession() as session:
            async with session.get(self.rest_uri) as response:
                if response.status == 200:
                    data = await response.json()
                    self.last_update_id = data['lastUpdateId']
                    self.bids = {}
                    self.asks = {}
                    self._process_updates(data['bids'], data['asks'])
                    print(f"[L2] Snapshot received. lastUpdateId: {self.last_update_id}")
                    return True
                else:
                    print(f"[L2] Failed to fetch snapshot: {response.status}")
                    return False

    async def _fetch_snapshot_and_sync(self):
        """Fetch REST snapshot and replay buffered events to sync."""
        success = await self._fetch_snapshot()
        if not success:
            self.syncing = False
            return

        # Replay buffered events
        valid_buffer = []
        for event in self.buffer:
            U = event['U']
            u = event['u']
            if u <= self.last_update_id:
                # Drop older events
                continue
            if U <= self.last_update_id + 1 and u >= self.last_update_id + 1:
                # First valid event
                self._process_updates(event['b'], event['a'])
                self.last_update_id = u
                self.synced = True
            elif self.synced:
                if U == self.last_update_id + 1 or event.get('pu', None) == self.last_update_id:
                    self._process_updates(event['b'], event['a'])
                    self.last_update_id = u
                else:
                    # Missing packet detected in buffer, restart sync
                    print("[L2] Gap detected during buffer replay. Restarting sync.")
                    self.buffer = [] # Clear buffer and try again later
                    self.syncing = False
                    asyncio.create_task(self._fetch_snapshot_and_sync())
                    return

        self.buffer = []
        self.syncing = False
        if not self.synced:
            # If no valid events were in the buffer to catch up to snapshot
            # It will eventually get a future event and sync if condition is met in loop
            self.synced = True # Consider synced so that the websocket loop can start checking events

    async def connect_and_stream(self):
        """Connect to WebSocket and maintain L2 OrderBook."""
        print(f"[L2] Connecting to {self.ws_uri}")
        self.running = True
        self.start_time = time.time()

        try:
            async with websockets.connect(self.ws_uri) as websocket:
                print("[L2] Connected. Receiving diff streams...")
                # Start fetching the snapshot in the background
                self.syncing = True
                self.synced = False
                asyncio.create_task(self._fetch_snapshot_and_sync())

                while self.running:
                    try:
                        packet = await websocket.recv()
                        recv_time = time.time()

                        data = json.loads(packet)
                        event_time = data.get("E", 0) / 1000.0
                        latency_ms = (recv_time - event_time) * 1000.0

                        U = data['U']
                        u = data['u']
                        pu = data.get('pu', None) # Previous 'u', missing in older API but present in depth

                        if not self.synced:
                            self.buffer.append(data)
                            # Once snapshot is fetched and synced, self.synced will be true
                        else:
                            if U <= self.last_update_id + 1 and u >= self.last_update_id + 1:
                                 # First event after snapshot
                                 self._process_updates(data['b'], data['a'])
                                 self.last_update_id = u
                            elif pu is not None and pu == self.last_update_id: # Better sequence check if pu is available
                                self._process_updates(data['b'], data['a'])
                                self.last_update_id = u
                            elif U == self.last_update_id + 1: # Strict sequence check
                                self._process_updates(data['b'], data['a'])
                                self.last_update_id = u
                            else:
                                print(f"[L2] Missing packet detected (Expected {self.last_update_id + 1}, Got {U}). Resyncing...")
                                self.buffer.append(data)
                                self.synced = False
                                self.syncing = True
                                asyncio.create_task(self._fetch_snapshot_and_sync())
                                continue

                            self.update_count += 1
                            self.total_latency_ms += latency_ms

                            if self.update_count % 100 == 0:
                                elapsed = time.time() - self.start_time
                                rate = self.update_count / elapsed
                                avg_latency = self.total_latency_ms / self.update_count
                                top_bids, top_asks = self.get_top_levels()
                                best_bid = list(top_bids.keys())[0] if top_bids else 0.0
                                best_ask = list(top_asks.keys())[0] if top_asks else 0.0
                                print(f"[L2] Updates: {self.update_count} | Speed: {rate:.2f}/sec | Avg Latency: {avg_latency:.2f}ms | Bbo: {best_bid} / {best_ask}")

                    except websockets.exceptions.ConnectionClosed:
                        print("[L2] Warning: WebSocket closed. Reconnecting...")
                        break
                    except Exception as e:
                        print(f"[L2] Error: {e}")
                        break
        except websockets.exceptions.InvalidStatus as e:
            print(f"[L2] Failed to connect to WebSocket: {e}")
            self.running = False

if __name__ == "__main__":
    book = L2OrderBook()
    try:
        asyncio.run(book.connect_and_stream())
    except KeyboardInterrupt:
        print("\n[L2] Shutting down.")
