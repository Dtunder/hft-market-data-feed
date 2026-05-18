import pytest
import asyncio
import orjson
from aioresponses import aioresponses
from unittest.mock import patch

from src.feed_client import HFTMarketDataFeed
from src.ring_buffer import LockFreeRingBuffer

@pytest.fixture
def feed():
    feed = HFTMarketDataFeed(buffer_name="test_fixture_buffer")
    yield feed
    feed.stop()

@pytest.mark.asyncio
async def test_snapshot_http_451(feed):
    with aioresponses() as m:
        m.get('https://testnet.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=1000', status=451)
        with pytest.raises(ConnectionError, match="HTTP 451: IP Restricted"):
            await feed.fetch_snapshot()

@pytest.mark.asyncio
async def test_snapshot_success(feed):
    with aioresponses() as m:
        m.get('https://testnet.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=1000', payload={"lastUpdateId": 12345}, status=200)
        res = await feed.fetch_snapshot()
        assert res["lastUpdateId"] == 12345

def test_process_depth_event_initial_buffer(feed):
    # Initial state, no snapshot
    ev = {"e": "depthUpdate", "U": 10, "u": 15, "pu": 9}
    assert feed.process_depth_event(ev) == False
    assert len(feed.buffered_events) == 1

def test_process_depth_event_sequence_gap(feed):
    feed.last_update_id = 15
    ev = {"e": "depthUpdate", "U": 20, "u": 25, "pu": 19}
    assert feed.process_depth_event(ev) == False
    assert feed.last_update_id is None
    assert len(feed.buffered_events) == 1

def test_process_depth_event_valid_sequence(feed):
    feed.last_update_id = 15
    ev = {"e": "depthUpdate", "U": 16, "u": 20, "pu": 15}
    assert feed.process_depth_event(ev) == True
    assert feed.last_update_id == 20

def test_process_depth_event_after_snapshot(feed):
    feed.last_update_id = 15
    feed.awaiting_first_event = True
    # U <= 16 and u >= 16
    ev = {"e": "depthUpdate", "U": 14, "u": 20, "pu": 13}
    assert feed.process_depth_event(ev) == True
    assert feed.awaiting_first_event == False
    assert feed.last_update_id == 20

def test_process_depth_event_after_snapshot_gap(feed):
    feed.last_update_id = 15
    feed.awaiting_first_event = True
    # U > 16
    ev = {"e": "depthUpdate", "U": 17, "u": 20, "pu": 16}
    assert feed.process_depth_event(ev) == False
    assert feed.last_update_id is None
    assert len(feed.buffered_events) == 1

@pytest.mark.asyncio
async def test_connect_and_stream_flow(feed):
    # Mock websockets
    class MockWebsocket:
        def __init__(self, messages):
            self.messages = messages
            self.idx = 0

        async def recv(self):
            if self.idx < len(self.messages):
                msg = self.messages[self.idx]
                self.idx += 1
                return msg
            else:
                feed.running = False
                raise asyncio.CancelledError()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    # Sequence with gap, snapshot recovery, and trades
    # Use string to trigger the previous TypeError logic
    messages = [
        orjson.dumps({"e": "depthUpdate", "U": 10, "u": 15, "pu": 9, "E": 1000}).decode('utf-8'), # depth (buffered)
        orjson.dumps({"e": "aggTrade", "p": "100.0", "q": "1.0", "E": 1000}).decode('utf-8'), # trade
        orjson.dumps({"e": "depthUpdate", "U": 14, "u": 20, "pu": 13, "E": 1000}).decode('utf-8') # valid depth overlapping snapshot
    ]

    with aioresponses() as m:
        m.get('https://testnet.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=1000', payload={"lastUpdateId": 15}, status=200)

        with patch('websockets.connect', return_value=MockWebsocket(messages)):
            try:
                await feed.connect_and_stream()
            except asyncio.CancelledError:
                pass

    assert feed.last_update_id == 20
    assert feed.trade_count == 1

    # Read the aggTrade
    reader_buf = LockFreeRingBuffer("test_fixture_buffer", create=False)
    msg = reader_buf.read()
    assert msg is not None
    decoded = orjson.loads(msg)
    assert decoded["e"] == "aggTrade"

    # Read the depth event applied after recovery
    msg = reader_buf.read()
    assert msg is not None
    decoded = orjson.loads(msg)
    assert decoded["e"] == "depthUpdate"
    assert decoded["u"] == 20
