import asyncio
import aiohttp
import json
import logging

class OrderBook:
    def __init__(self, symbol="btcusdt", depth=20):
        self.symbol = symbol.upper()
        self.depth = depth
        self.bids = {}
        self.asks = {}
        self.last_update_id = 0
        self.last_u = 0
        self.event_buffer = []
        self.is_synced = False
        self.syncing = False
        self.logger = logging.getLogger(__name__)

    async def fetch_snapshot(self):
        url = f"https://api.binance.com/api/v3/depth?symbol={self.symbol}&limit=1000"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 451:
                        self.logger.warning("HTTP 451: IP blocked by Binance. Returning None.")
                        return None
                    elif response.status != 200:
                        self.logger.error(f"Failed to fetch snapshot, status: {response.status}")
                        return None
                    return await response.json()
            except Exception as e:
                self.logger.error(f"Error fetching snapshot: {e}")
                return None

    def process_snapshot(self, snapshot):
        if not snapshot:
            return
        self.last_update_id = snapshot['lastUpdateId']
        self.bids = {float(price): float(qty) for price, qty in snapshot['bids']}
        self.asks = {float(price): float(qty) for price, qty in snapshot['asks']}
        self.is_synced = False
        self.last_u = 0

    def get_l2_book(self):
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:self.depth]
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])[:self.depth]
        return {
            "bids": sorted_bids,
            "asks": sorted_asks
        }

    def apply_diff(self, event):
        u = event['u']
        U = event['U']

        if not self.is_synced:
            # 4. Drop any event where u is <= lastUpdateId
            if u <= self.last_update_id:
                return True # Dropped, but no error
            # 5. The first processed event should have U <= lastUpdateId + 1 AND u >= lastUpdateId + 1
            if U <= self.last_update_id + 1 and u >= self.last_update_id + 1:
                self.is_synced = True
            else:
                self.logger.warning("Initial sequence mismatch. Need resync.")
                return False # Trigger resync

        else:
            # 6. While listening to the stream, each new event's U should be equal to the previous event's u + 1
            if U != self.last_u + 1:
                self.logger.warning(f"Sequence mismatch: U={U}, last_u={self.last_u}. Need resync.")
                return False # Trigger resync

        # Apply updates
        for price_str, qty_str in event.get('b', []):
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for price_str, qty_str in event.get('a', []):
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.last_u = u
        return True

    async def sync_book(self):
        if self.syncing:
            return
        self.syncing = True

        # Implement a basic retry logic
        retry_delay = 1
        snapshot = None
        while not snapshot:
            snapshot = await self.fetch_snapshot()
            if not snapshot:
                self.logger.warning(f"Failed to get snapshot, retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                # Cap the retry delay to 30s
                retry_delay = min(retry_delay * 2, 30)

        self.process_snapshot(snapshot)
        # Apply buffered events
        for event in self.event_buffer:
            self.apply_diff(event)
        self.event_buffer.clear()
        self.syncing = False

    async def on_diff_event(self, event):
        if not self.is_synced and self.last_update_id == 0:
            self.event_buffer.append(event)
            if not self.syncing:
                 # Trigger sync on first event if we haven't already
                 await self.sync_book()
        else:
            if self.syncing:
                self.event_buffer.append(event)
                return

            success = self.apply_diff(event)
            if not success:
                # Re-sync
                self.last_update_id = 0
                self.is_synced = False
                self.event_buffer = [event]
                await self.sync_book()
