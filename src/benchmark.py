import asyncio
import time
import json
import sys
from l2_orderbook import L2OrderBook

class DummyWebsocket:
    def __init__(self, events):
        self.events = events
        self.idx = 0

    async def recv(self):
        if self.idx < len(self.events):
            event = self.events[self.idx]
            self.idx += 1
            # Simulate slight delay
            await asyncio.sleep(0.00001)
            return json.dumps(event)
        # Block forever when done
        await asyncio.sleep(3600)

class BenchmarkBook(L2OrderBook):
    def __init__(self, num_events, symbol="btcusdt", depth=20):
        super().__init__(symbol, depth)
        self.num_events = num_events
        self.latencies = []

    async def fetch_snapshot(self):
        # Mock instantaneous snapshot
        self.bids = {}
        self.asks = {}
        self.last_update_id = 0
        self.is_syncing = False
        return True

    async def connect_and_stream(self):
        # Generate dummy payload
        print(f"[BENCHMARK] Generating {self.num_events} dummy depth events...")
        events = []
        for i in range(self.num_events):
            events.append({
                "e": "depthUpdate",
                "E": int(time.time()*1000),
                "s": self.symbol.upper(),
                "U": i+1,
                "u": i+1,
                "pu": i,
                "b": [[str(40000.0 + i % 100), "1.0"]],
                "a": [[str(40100.0 + i % 100), "1.0"]]
            })

        websocket = DummyWebsocket(events)

        await self.fetch_snapshot()

        print("[BENCHMARK] Starting ingestion benchmark...")
        start_time = time.time()

        for _ in range(self.num_events):
            packet = await websocket.recv()
            recv_time = time.perf_counter()

            event = json.loads(packet)
            parse_time = time.perf_counter()

            res = self.process_diff(event)
            proc_time = time.perf_counter()

            # End-to-end latency per event processing (parse + update)
            self.latencies.append((proc_time - recv_time) * 1000) # in ms

        total_time = time.time() - start_time

        print(f"\n--- Benchmark Results ---")
        print(f"Total Events Processed: {self.num_events}")
        print(f"Total Time: {total_time:.4f} seconds")
        print(f"Throughput: {self.num_events/total_time:.2f} events/sec")

        if self.latencies:
            avg_latency = sum(self.latencies) / len(self.latencies)
            max_latency = max(self.latencies)
            min_latency = min(self.latencies)
            print(f"Average Latency: {avg_latency:.4f} ms")
            print(f"Min Latency:     {min_latency:.4f} ms")
            print(f"Max Latency:     {max_latency:.4f} ms")


if __name__ == "__main__":
    num_events = 10000
    if len(sys.argv) > 1:
        try:
            num_events = int(sys.argv[1])
        except ValueError:
            pass

    bench = BenchmarkBook(num_events)
    asyncio.run(bench.connect_and_stream())
