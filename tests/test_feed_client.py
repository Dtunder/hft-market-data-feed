import asyncio
import json
import pytest
import websockets
from websockets.asyncio.server import serve
from src.binance_feed import BinanceTestnetFeedClient

async def mock_binance_server(websocket):
    # Send a depth5 message
    depth_msg = {
        "stream": "btcusdt@depth5@100ms",
        "data": {
            "lastUpdateId": 160,
            "bids": [
                [
                    "0.0024",
                    "10"
                ]
            ],
            "asks": [
                [
                    "0.0026",
                    "100"
                ]
            ]
        }
    }
    await websocket.send(json.dumps(depth_msg))

    # Send an aggTrade message
    agg_msg = {
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": 123456789,
            "s": "BTCUSDT",
            "a": 5933014,
            "p": "0.001",
            "q": "100",
            "f": 100,
            "l": 105,
            "T": 123456785,
            "m": True,
            "M": True
        }
    }
    await websocket.send(json.dumps(agg_msg))

    # Keep connection open for a bit
    await asyncio.sleep(0.5)

@pytest.mark.asyncio
async def test_feed_client_integration():
    # Start mock server
    server = await serve(mock_binance_server, "localhost", 8765)

    client = BinanceTestnetFeedClient(["btcusdt"], ring_buffer_name="test_feed")
    client.ws_uri = "ws://localhost:8765"  # Override URI to hit our mock

    # Run the client connect in background
    task = asyncio.create_task(client.connect_and_stream())

    # Wait for messages to be processed
    await asyncio.sleep(0.2)

    # Assert Orderbook state
    bids, asks = client.get_latest_orderbook("btcusdt")
    assert len(bids) > 0
    assert bids[0][0] == "0.0024"
    assert asks[0][0] == "0.0026"

    # Assert Trade state
    trade = client.get_latest_trade("btcusdt")
    assert trade["p"] == "0.001"

    # Assert RingBuffer (2 messages)
    msg1 = client.buffer.pop()
    msg2 = client.buffer.pop()
    assert msg1 is not None
    assert msg2 is not None

    data1 = json.loads(msg1)
    data2 = json.loads(msg2)
    assert data1["stream"] == "btcusdt@depth5@100ms"
    assert data2["stream"] == "btcusdt@aggTrade"

    # Cleanup
    client.stop()
    await task
    server.close()
    await server.wait_closed()

@pytest.mark.asyncio
async def test_feed_client_reconnection():
    # Only serve 1 message then close to force reconnect
    async def dropping_server(websocket):
        depth_msg = {
            "stream": "ethusdt@depth5@100ms",
            "data": {"bids": [], "asks": []}
        }
        await websocket.send(json.dumps(depth_msg))
        # Keep open very briefly to ensure the client receives the msg before the socket closes
        await asyncio.sleep(0.2)
        # Connection ends here, forcing client to reconnect

    client = BinanceTestnetFeedClient(["ethusdt"], ring_buffer_name="test_reconnect")
    client.ws_uri = "ws://localhost:8766"  # override

    server_task = None
    async def manage_server():
        nonlocal server_task
        server = await serve(dropping_server, "localhost", 8766)
        await asyncio.sleep(0.5)
        server.close()
        await server.wait_closed()

        # Start server again to allow reconnect
        server2 = await serve(dropping_server, "localhost", 8766)
        await asyncio.sleep(1.5) # Wait past the 1s reconnect delay
        server2.close()
        await server2.wait_closed()

    server_task = asyncio.create_task(manage_server())

    # Add a tiny delay to ensure server starts first
    await asyncio.sleep(0.1)
    client_task = asyncio.create_task(client.connect_and_stream())

    await asyncio.sleep(2.5)

    # Read before stopping to avoid closed shared memory
    msg1 = client.buffer.pop()
    msg2 = client.buffer.pop()

    client.stop()
    await client_task
    await server_task

    assert msg1 is not None, "First message not received"
    assert msg2 is not None, "Second message not received, reconnect failed?"
# End of file
