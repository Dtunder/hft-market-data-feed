import asyncio
import time
import sys
from orderbook import OrderBook

async def run_benchmark(duration_seconds):
    ob = OrderBook(symbol="BTCUSDT")

    # Mock a snapshot
    ob.process_snapshot({
        "lastUpdateId": 1000,
        "bids": [[f"{50000.00 - i}", "1.0"] for i in range(100)],
        "asks": [[f"{50010.00 + i}", "1.0"] for i in range(100)]
    })

    # Pre-generate a list of events to apply
    events = []
    base_price = 50000.00
    for i in range(100000):
        # We simulate U = previous_u + 1
        event = {
            "U": 1001 + i,
            "u": 1001 + i,
            "b": [[str(base_price - (i % 100)), str(i % 5)]],
            "a": [[str(base_price + 10 + (i % 100)), str(i % 5)]]
        }
        events.append(event)

    print(f"Running latency benchmark under heavy market depth updates for {duration_seconds} seconds...")
    start_time = time.time()
    events_processed = 0

    while time.time() - start_time < duration_seconds:
        for event in events:
            # Maintain sequence synchronization
            event['U'] = ob.last_u + 1 if ob.is_synced else 1001
            event['u'] = event['U']

            await ob.on_diff_event(event)
            events_processed += 1

            if time.time() - start_time >= duration_seconds:
                break

    end_time = time.time()
    elapsed = end_time - start_time
    rate = events_processed / elapsed

    print(f"Processed {events_processed} events in {elapsed:.2f} seconds.")
    print(f"Throughput: {rate:.2f} updates/sec")
    print(f"Average latency per update: {(elapsed / events_processed) * 1e6:.2f} microseconds")

if __name__ == "__main__":
    duration = 5
    if len(sys.argv) > 1:
        duration = float(sys.argv[1])
    asyncio.run(run_benchmark(duration))
