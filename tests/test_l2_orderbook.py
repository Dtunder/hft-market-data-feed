import pytest
import asyncio
from unittest.mock import patch, MagicMock
from aioresponses import aioresponses
from src.l2_orderbook import L2OrderBook

@pytest.fixture
def orderbook():
    return L2OrderBook()

@pytest.mark.asyncio
async def test_fetch_snapshot_success(orderbook):
    with aioresponses() as m:
        m.get(orderbook.rest_url, payload={
            "lastUpdateId": 100,
            "bids": [["40000.0", "1.0"]],
            "asks": [["40100.0", "2.0"]]
        })

        orderbook.is_syncing = True # Setup state before snapshot (as done in main stream)
        success = await orderbook.fetch_snapshot()
        assert success is True
        assert orderbook.last_update_id == 100
        assert orderbook.bids == {"40000.0": 1.0}
        assert orderbook.asks == {"40100.0": 2.0}
        assert orderbook.is_syncing is True

@pytest.mark.asyncio
async def test_fetch_snapshot_ip_restricted(orderbook):
    with aioresponses() as m:
        m.get(orderbook.rest_url, status=451)

        success = await orderbook.fetch_snapshot()
        assert success is True
        assert orderbook.last_update_id == 0
        assert orderbook.bids == {}
        assert orderbook.asks == {}

def test_process_diff_continuous(orderbook):
    orderbook.last_update_id = 100
    orderbook.is_syncing = False

    event = {
        "U": 101,
        "u": 105,
        "pu": 100,
        "b": [["40000.0", "1.5"]],
        "a": [["40100.0", "0.0"]] # Delete ask
    }

    orderbook.asks = {"40100.0": 2.0, "40200.0": 3.0}

    res = orderbook.process_diff(event)
    assert res is True
    assert orderbook.last_update_id == 105
    assert orderbook.bids == {"40000.0": 1.5}
    assert orderbook.asks == {"40200.0": 3.0}

def test_process_diff_missing_packet(orderbook):
    orderbook.last_update_id = 100
    orderbook.is_syncing = False

    event = {
        "U": 105,
        "u": 110,
        "pu": 104, # Expected 100
        "b": [],
        "a": []
    }

    res = orderbook.process_diff(event)
    assert res == "resync"

def test_process_diff_first_event_after_sync(orderbook):
    orderbook.last_update_id = 100
    orderbook.is_syncing = False

    # Valid first event: U <= 101 AND u >= 101
    orderbook._is_first_processed_event = True
    event_valid = {
        "U": 99,
        "u": 105,
        "pu": 98,
        "b": [],
        "a": []
    }
    res = orderbook.process_diff(event_valid)
    assert res is True
    assert orderbook.last_update_id == 105
    assert orderbook._is_first_processed_event is False

    # Reset
    orderbook.last_update_id = 100
    orderbook._is_first_processed_event = True

    # Invalid first event: U > 101
    event_invalid = {
        "U": 102,
        "u": 105,
        "pu": 101,
        "b": [],
        "a": []
    }
    res = orderbook.process_diff(event_invalid)
    assert res == "resync"
    # Ensure flag remains untouched on failure or is re-set up in sync loop later

def test_get_top_levels(orderbook):
    orderbook.bids = {"40000.0": 1.0, "39900.0": 2.0, "40100.0": 0.5}
    orderbook.asks = {"40200.0": 1.0, "40300.0": 2.0, "40150.0": 0.5}
    orderbook.depth = 2

    top = orderbook.get_top_levels()

    assert top["bids"] == [("40100.0", 0.5), ("40000.0", 1.0)]
    assert top["asks"] == [("40150.0", 0.5), ("40200.0", 1.0)]
