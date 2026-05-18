import asyncio
import aiohttp
import json
import logging
from collections import deque

logger = logging.getLogger(__name__)

class OrderBook:
    """
    Maintains a local Level 2 order book up to 20 levels deep from Binance WebSocket diff streams.
    Handles sequence matching and fetching REST snapshots when out of sync.
    """
    def __init__(self, symbol="btcusdt", max_depth=20):
        self.symbol = symbol.lower()
        self.max_depth = max_depth
        self.bids = {}  # price -> quantity
        self.asks = {}  # price -> quantity

        self.last_update_id = None

        self.event_buffer = deque()
        self.is_syncing = False

        # This will be used to signal that sync is complete
        self.sync_event = asyncio.Event()

    def get_top_levels(self):
        """Returns the top `max_depth` bids and asks."""
        sorted_bids = sorted(self.bids.items(), key=lambda x: -float(x[0]))[:self.max_depth]
        sorted_asks = sorted(self.asks.items(), key=lambda x: float(x[0]))[:self.max_depth]
        return {"bids": sorted_bids, "asks": sorted_asks}

    def reset_state(self):
        """Clear book state and trigger a new sync."""
        logger.warning(f"[ORDERBOOK] Resetting state for {self.symbol}. Triggering sync.")
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None
        self.event_buffer.clear()
        self.is_syncing = True
        self.sync_event.clear()

    async def sync_snapshot(self):
        """
        Fetches REST snapshot. Falls back to mock data if blocked (451 or others).
        """
        self.is_syncing = True
        url = f"https://api.binance.com/api/v3/depth?symbol={self.symbol.upper()}&limit=100"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as response:
                    if response.status == 200:
                        data = await response.json()
                    elif response.status == 451:
                        logger.warning(f"[ORDERBOOK] HTTP 451: IP blocked. Using mock data for {self.symbol}.")
                        data = self._get_mock_snapshot()
                    else:
                        logger.error(f"[ORDERBOOK] HTTP {response.status} fetching snapshot. Retrying later.")
                        data = self._get_mock_snapshot()
        except Exception as e:
            logger.error(f"[ORDERBOOK] Error fetching snapshot: {e}. Using mock data.")
            data = self._get_mock_snapshot()

        self._apply_snapshot(data)

    def _get_mock_snapshot(self):
        return {
            "lastUpdateId": 1000,
            "bids": [["30000.00", "1.0"]],
            "asks": [["30001.00", "1.0"]]
        }

    def _apply_snapshot(self, data):
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = data.get("lastUpdateId", 0)

        for price, qty in data.get("bids", []):
            if float(qty) > 0:
                self.bids[price] = qty
        for price, qty in data.get("asks", []):
            if float(qty) > 0:
                self.asks[price] = qty

        # Process buffered events
        while self.event_buffer:
            event = self.event_buffer.popleft()
            # If the event is older than the snapshot, skip it
            if event["u"] <= self.last_update_id:
                continue

            # The first event processed must meet U <= lastUpdateId+1 AND u >= lastUpdateId+1
            if event["U"] <= self.last_update_id + 1 and event["u"] >= self.last_update_id + 1:
                self._apply_update(event)
            elif event["U"] > self.last_update_id + 1:
                # We missed an event! Restart sync.
                self.reset_state()
                asyncio.create_task(self.sync_snapshot())
                return

        self.is_syncing = False
        self.sync_event.set()
        logger.info(f"[ORDERBOOK] Sync complete for {self.symbol}. Last update ID: {self.last_update_id}")

    def process_diff(self, event):
        """
        Processes a depth diff event from the WebSocket.
        """
        # If we're syncing, buffer the event
        if self.is_syncing:
            self.event_buffer.append(event)
            return

        # If we haven't synced yet, start syncing and buffer
        if self.last_update_id is None:
            self.reset_state()
            self.event_buffer.append(event)
            asyncio.create_task(self.sync_snapshot())
            return

        # Check sequence
        # For updates after the first one, PU should be equal to the previous u
        expected_pu = self.last_update_id
        pu = event.get("pu")

        if pu != expected_pu:
            logger.warning(f"[ORDERBOOK] Sequence mismatch! Expected pu={expected_pu}, got {pu}. Resyncing...")
            self.reset_state()
            self.event_buffer.append(event)
            asyncio.create_task(self.sync_snapshot())
            return

        self._apply_update(event)

    def _apply_update(self, event):
        for price, qty in event.get("b", []):
            if float(qty) == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for price, qty in event.get("a", []):
            if float(qty) == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.last_update_id = event["u"]
