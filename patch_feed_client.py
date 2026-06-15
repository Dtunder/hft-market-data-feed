with open("src/feed_client.py", "r") as f:
    code = f.read()

import re
old_main = re.search(r"async def main\(\):.*?(?=if __name__ ==)", code, re.DOTALL).group(0)

new_main = """from src.binance_feed import BinanceTestnetFeedClient

async def main():
    print("[MAIN] Starting BinanceTestnetFeedClient for 10 seconds...")
    feed = BinanceTestnetFeedClient(["btcusdt"])

    # Run the client connect in background
    task = asyncio.create_task(feed.connect_and_stream())

    # Monitor output for a few seconds
    for _ in range(10):
        await asyncio.sleep(1.0)

        # Check orderbook
        bids, asks = feed.get_latest_orderbook("btcusdt")
        if bids and asks:
            print(f"[MAIN] Best bid: {bids[0][0]}, Best ask: {asks[0][0]}")

        # Check latest trade
        trade = feed.get_latest_trade("btcusdt")
        if trade:
            print(f"[MAIN] Latest trade price: {trade.get('p')}")

    print("\\n[MAIN] Stopping feed...")
    feed.stop()
    await asyncio.gather(task, return_exceptions=True)

"""

code = code.replace(old_main, new_main)

with open("src/feed_client.py", "w") as f:
    f.write(code)
