import asyncio
import time
import sys
from l2_order_book import L2OrderBook

async def benchmark_orderbook(duration=10):
    print(f"--- Starting L2 OrderBook Benchmark for {duration} seconds ---")
    book = L2OrderBook("btcusdt")

    # Run the streaming task in the background
    task = asyncio.create_task(book.connect_and_stream())

    start_time = time.time()
    while time.time() - start_time < duration:
        await asyncio.sleep(1)
        if not book.running:
            print("Benchmark stopped early because the orderbook stopped running.")
            break

    # Stop the book
    book.running = False
    await task

    print("\n--- Benchmark Results ---")
    print(f"Total Updates Processed: {book.update_count}")

    if book.update_count > 0:
        elapsed = time.time() - book.start_time
        rate = book.update_count / elapsed
        avg_latency = book.total_latency_ms / book.update_count
        print(f"Update Rate: {rate:.2f} updates/second")
        print(f"Average Latency: {avg_latency:.2f} ms")
    else:
        print("No updates processed. Check connectivity or errors.")

if __name__ == "__main__":
    duration = 10
    if len(sys.argv) > 1:
        duration = int(sys.argv[1])
    asyncio.run(benchmark_orderbook(duration))
