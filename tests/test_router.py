import pytest
import asyncio
import time
import orjson
from src.feed_client import LatencyAwareRouter

class MockWebSocket:
    def __init__(self, region, initial_latency):
        self.region = region
        self.latency = initial_latency
        self.count = 0
        self.closed = False

    async def recv(self):
        if self.closed:
            raise Exception("Closed")

        await asyncio.sleep(self.latency)

        # Create packet matching Binance combined stream format
        # E is event time in ms
        event_time_ms = (time.time() - self.latency) * 1000

        payload = {
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": event_time_ms,
                "s": "BTCUSDT",
                "p": "50000.00",
                "q": "1.0",
                "region": self.region
            }
        }

        self.count += 1
        return orjson.dumps(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.closed = True

@pytest.mark.asyncio
async def test_router_failover_and_latency(mocker):
    # Mock endpoints
    endpoints = {
        "Tokyo": "wss://tokyo",
        "Frankfurt": "wss://frankfurt"
    }

    router = LatencyAwareRouter(endpoints, ["btcusdt"])

    # Track received messages and routing latencies
    received_messages = []
    routing_latencies = []

    async def mock_callback(stream, data):
        # Measure callback execution time vs event creation to get routing overhead
        # routing_overhead = current_time - (event_time + simulated_network_latency)
        # Because our mock calculates event_time = current_time - simulated_latency,
        # current_time - event_time = simulated_network_latency + routing_overhead.
        # So routing_overhead = current_time - event_time - simulated_network_latency.

        current_time_ms = time.time() * 1000
        event_time_ms = data["E"]

        # determine which region this came from to subtract the simulated network latency
        region = data.get("region")
        if region == "Tokyo":
            simulated_latency_ms = tokyo_ws.latency * 1000
        else:
            simulated_latency_ms = frankfurt_ws.latency * 1000

        routing_overhead_ms = (current_time_ms - event_time_ms) - simulated_latency_ms
        routing_latencies.append(routing_overhead_ms)
        received_messages.append((region, data))

    router.add_callback(mock_callback)

    tokyo_ws = MockWebSocket("Tokyo", 0.050)      # 50ms latency
    frankfurt_ws = MockWebSocket("Frankfurt", 0.010) # 10ms latency (best)

    # Mock websockets.connect to return our MockWebSockets
    def mock_connect(uri):
        if "tokyo" in uri:
            return tokyo_ws
        elif "frankfurt" in uri:
            return frankfurt_ws

    mocker.patch('websockets.connect', side_effect=mock_connect)

    # Run the router
    router_task = asyncio.create_task(router.connect_and_stream())

    # Let it run for a bit
    await asyncio.sleep(0.5)

    # Degrade Frankfurt latency
    frankfurt_ws.latency = 0.100 # 100ms
    await asyncio.sleep(0.5)

    router.stop()
    tokyo_ws.closed = True
    frankfurt_ws.closed = True

    await asyncio.gather(router_task, return_exceptions=True)

    # Verify failover happened (should have received messages from both regions)
    regions_received = set(region for region, _ in received_messages)
    assert "Frankfurt" in regions_received, "Failed to receive from initial best region"
    assert "Tokyo" in regions_received, "Failed to failover to Tokyo"

    # Verify routing latency is under 50us (0.05ms)
    avg_routing_latency_ms = sum(routing_latencies) / len(routing_latencies)
    print(f"Average routing latency: {avg_routing_latency_ms * 1000:.2f} us")
    assert avg_routing_latency_ms < 0.050, f"Routing latency {avg_routing_latency_ms*1000:.2f}us exceeds 50us threshold"

@pytest.mark.asyncio
async def test_router_451_exception(mocker):
    import websockets
    endpoints = {"Tokyo": "wss://tokyo"}
    router = LatencyAwareRouter(endpoints, ["btcusdt"])

    class Mock451Response:
        status_code = 451

    class Mock451Exception(websockets.exceptions.InvalidStatus):
        def __init__(self):
            self.response = Mock451Response()

    # Manually patch exception as __init__ requires response object
    websockets.exceptions.InvalidStatus.status_code = property(lambda self: self.response.status_code)

    def mock_connect_error(uri):
        raise Mock451Exception()

    mocker.patch('websockets.connect', side_effect=mock_connect_error)

    with pytest.raises(ConnectionError, match="HTTP 451: IP Restricted for Tokyo"):
        await router.connect_and_stream()
