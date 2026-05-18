import pytest
import asyncio
from unittest.mock import patch, MagicMock
from src.orderbook import OrderBook

@pytest.fixture
def order_book():
    return OrderBook(symbol="BTCUSDT")

@pytest.mark.asyncio
async def test_orderbook_snapshot_processing(order_book):
    snapshot = {
        "lastUpdateId": 100,
        "bids": [["50000.0", "1.0"]],
        "asks": [["50010.0", "1.0"]]
    }
    order_book.process_snapshot(snapshot)

    assert order_book.last_update_id == 100
    assert 50000.0 in order_book.bids
    assert 50010.0 in order_book.asks
    assert not order_book.is_synced

@pytest.mark.asyncio
async def test_apply_diff_sequence_logic(order_book):
    snapshot = {
        "lastUpdateId": 100,
        "bids": [["50000.0", "1.0"]],
        "asks": [["50010.0", "1.0"]]
    }
    order_book.process_snapshot(snapshot)

    # 1. Event where u <= lastUpdateId (should be dropped, return True)
    event_old = {"U": 90, "u": 95, "b": [], "a": []}
    assert order_book.apply_diff(event_old) is True
    assert not order_book.is_synced

    # 2. First event U <= lastUpdateId + 1 and u >= lastUpdateId + 1
    event_initial = {"U": 101, "u": 105, "b": [["50001.0", "2.0"]], "a": []}
    assert order_book.apply_diff(event_initial) is True
    assert order_book.is_synced
    assert 50001.0 in order_book.bids
    assert order_book.last_u == 105

    # 3. Subsequent event U = previous u + 1
    event_next = {"U": 106, "u": 110, "b": [], "a": [["50009.0", "2.0"]]}
    assert order_book.apply_diff(event_next) is True
    assert 50009.0 in order_book.asks

    # 4. Out of sequence event (U != previous u + 1)
    event_out_of_seq = {"U": 112, "u": 115, "b": [], "a": []}
    assert order_book.apply_diff(event_out_of_seq) is False

@pytest.mark.asyncio
async def test_on_diff_event_resync(order_book):
    async def mock_fetch_snapshot1():
        return {
            "lastUpdateId": 100,
            "bids": [["50000.0", "1.0"]],
            "asks": [["50010.0", "1.0"]]
        }
    order_book.fetch_snapshot = mock_fetch_snapshot1

    # First event triggers sync
    event1 = {"U": 101, "u": 105, "b": [["50005.0", "1.0"]], "a": []}
    await order_book.on_diff_event(event1)

    assert order_book.is_synced
    assert 50005.0 in order_book.bids
    assert order_book.last_u == 105

    # Next event misses sequence
    event_miss = {"U": 107, "u": 110, "b": [], "a": []}

    # Mock snapshot again for resync
    async def mock_fetch_snapshot2():
        return {
            "lastUpdateId": 150,
            "bids": [["51000.0", "1.0"]],
            "asks": []
        }
    order_book.fetch_snapshot = mock_fetch_snapshot2

    await order_book.on_diff_event(event_miss)

    # Because U=107 < 150+1, it drops the event and remains unsynced waiting for valid U/u
    assert order_book.last_update_id == 150
    assert not order_book.is_synced

@pytest.mark.asyncio
@patch('aiohttp.ClientSession.get')
async def test_fetch_snapshot_http_451(mock_get, order_book):
    mock_response = MagicMock()
    mock_response.status = 451
    mock_response.__aenter__.return_value = mock_response
    mock_get.return_value = mock_response

    snapshot = await order_book.fetch_snapshot()
    assert snapshot is None

@pytest.mark.asyncio
async def test_delete_level(order_book):
    snapshot = {
        "lastUpdateId": 100,
        "bids": [["50000.0", "1.0"]],
        "asks": []
    }
    order_book.process_snapshot(snapshot)

    event = {"U": 101, "u": 101, "b": [["50000.0", "0.0"]], "a": []}
    order_book.apply_diff(event)

    assert 50000.0 not in order_book.bids
