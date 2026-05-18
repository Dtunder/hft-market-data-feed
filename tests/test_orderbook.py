import pytest
import asyncio
from aioresponses import aioresponses
from src.orderbook import OrderBook

@pytest.mark.asyncio
async def test_orderbook_sync_and_update():
    ob = OrderBook("btcusdt")

    # Send a diff before synced
    diff1 = {
        "e": "depthUpdate",
        "E": 123456789,
        "s": "BTCUSDT",
        "U": 1001,
        "u": 1005,
        "pu": 1000,
        "b": [["30000.0", "2.0"]],
        "a": []
    }

    # Mock the HTTP call
    with aioresponses() as m:
        m.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=100', status=451)

        ob.process_diff(diff1)

        assert ob.is_syncing
        assert len(ob.event_buffer) == 1

        # Wait for the mocked sync to complete
        await ob.sync_event.wait()

    assert not ob.is_syncing
    # Mock data sets lastUpdateId to 1000,
    # Event U=1001 u=1005 fits U <= 1000+1 and u >= 1000+1, so it should be applied.
    assert ob.last_update_id == 1005
    assert ob.bids.get("30000.0") == "2.0"

@pytest.mark.asyncio
async def test_orderbook_sequence_mismatch():
    ob = OrderBook("btcusdt")
    ob._apply_snapshot({
        "lastUpdateId": 1000,
        "bids": [],
        "asks": []
    })

    # Valid update
    diff1 = {
        "U": 1001,
        "u": 1005,
        "pu": 1000,
        "b": [],
        "a": []
    }
    ob.process_diff(diff1)
    assert ob.last_update_id == 1005

    # Mismatch update (pu != 1005)
    diff2 = {
        "U": 1010,
        "u": 1015,
        "pu": 1009, # Expected 1005
        "b": [],
        "a": []
    }
    ob.process_diff(diff2)
    assert ob.is_syncing # Should trigger resync
