import asyncio
import sys
from src.binance_feed import BinanceTestnetFeedClient

async def main():
    symbols = ["btcusdt", "ethusdt"]
    print(f"Starting Binance Testnet Feed Client for: {symbols}")

    # Initialize our production-grade client
    client = BinanceTestnetFeedClient(symbols=symbols, ring_buffer_name="prod_feed")

    # Start the background task
    stream_task = asyncio.create_task(client.connect_and_stream())

    try:
        # Give it a moment to connect
        await asyncio.sleep(2)

        for _ in range(5):
            print("\n--- Current State ---")
            for symbol in symbols:
                bids, asks = client.get_latest_orderbook(symbol)
                trade = client.get_latest_trade(symbol)

                best_bid = bids[0][0] if bids else "N/A"
                best_ask = asks[0][0] if asks else "N/A"
                last_price = trade.get('p', "N/A")

                print(f"[{symbol.upper()}] Best Bid: {best_bid} | Best Ask: {best_ask} | Last Trade: {last_price}")

            # Pop a message from ring buffer just to show it works
            msg = client.buffer.pop()
            if msg:
                print(f"Popped from RingBuffer (len: {len(msg)})")

            await asyncio.sleep(1)

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        client.stop()
        await stream_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
