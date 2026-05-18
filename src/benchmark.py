import sys
import time
import asyncio
from src.orderbook import OrderBook

def generate_mock_diffs(count, start_u):
    diffs = []
    u = start_u
    for i in range(count):
        diff = {
            "e": "depthUpdate",
            "E": 123456789,
            "s": "BTCUSDT",
            "U": u + 1,
            "u": u + 10,
            "pu": u,
            "b": [[str(30000.0 - (i % 100)), "1.0"]],
            "a": [[str(30001.0 + (i % 100)), "1.0"]]
        }
        diffs.append(diff)
        u += 10
    return diffs

async def main():
    duration = 5 # default duration
    if len(sys.argv) > 1:
        duration = float(sys.argv[1])

    print(f"[BENCHMARK] Running order book benchmark for {duration} seconds...")

    orderbook = OrderBook()

    # Pre-sync the orderbook to bypass async sleep and network calls during bench
    orderbook._apply_snapshot({
        "lastUpdateId": 1000,
        "bids": [["30000.0", "1.0"]],
        "asks": [["30001.0", "1.0"]]
    })

    start_time = time.time()
    events_processed = 0
    u = 1000

    # Process as many events as we can in `duration` seconds
    while time.time() - start_time < duration:
        batch = generate_mock_diffs(10000, u)
        for diff in batch:
            orderbook.process_diff(diff)
            events_processed += 1
            u = diff["u"]

    elapsed = time.time() - start_time
    rate = events_processed / elapsed

    print(f"[BENCHMARK] Events Processed: {events_processed}")
    print(f"[BENCHMARK] Elapsed Time: {elapsed:.2f} seconds")
    print(f"[BENCHMARK] Processing Rate: {rate:.2f} events/second")

if __name__ == "__main__":
    asyncio.run(main())
