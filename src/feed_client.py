import asyncio
import json
import time
import websockets
import os
import struct
from multiprocessing import shared_memory
from collections import deque

RING_BUFFER_SIZE = 10000
# timestamp (8 bytes), symbol (10 bytes), price (8 bytes), quantity (8 bytes)
ENTRY_FORMAT = "d10sdd"
ENTRY_SIZE = struct.calcsize(ENTRY_FORMAT)
HEAD_FORMAT = "Q"
HEAD_SIZE = struct.calcsize(HEAD_FORMAT)

BUFFER_SIZE = HEAD_SIZE + RING_BUFFER_SIZE * ENTRY_SIZE

class HFTMarketDataFeed:
    """
    High-performance real-time WebSocket client for HFT order book/trade ingestion.
    Aims for sub-millisecond parsing latency.
    """
    def __init__(self, symbols=None):
        if symbols is None:
            symbols = ["btcusdt"]
        self.symbols = [s.lower() for s in symbols]
        self.running = False
        self.trade_count = 0
        self.start_time = None
        self.latencies = deque(maxlen=1000)

        # Initialize Shared Memory Ring Buffer
        try:
            self.shm = shared_memory.SharedMemory(create=True, size=BUFFER_SIZE, name="hft_ring_buffer")
        except FileExistsError:
            self.shm = shared_memory.SharedMemory(create=False, name="hft_ring_buffer")

        # Initialize head pointer to 0
        struct.pack_into(HEAD_FORMAT, self.shm.buf, 0, 0)

        os.makedirs("logs", exist_ok=True)

    def write_to_ring_buffer(self, timestamp, symbol, price, quantity):
        # Read current head
        head = struct.unpack_from(HEAD_FORMAT, self.shm.buf, 0)[0]

        # Calculate offset
        index = head % RING_BUFFER_SIZE
        offset = HEAD_SIZE + index * ENTRY_SIZE

        # Write data
        symbol_bytes = symbol.encode('utf-8')[:10].ljust(10, b'\x00')
        struct.pack_into(ENTRY_FORMAT, self.shm.buf, offset, timestamp, symbol_bytes, price, quantity)
        
        # Increment head
        struct.pack_into(HEAD_FORMAT, self.shm.buf, 0, head + 1)

    async def stream_symbol(self, symbol):
        uri = f"wss://stream.binance.com:9443/ws/{symbol}@aggTrade"
        print(f"[FEED] Connecting to live HFT feed: {uri}")

        async with websockets.connect(uri) as websocket:
            print(f"[FEED] Connection established for {symbol}. Ingesting high-speed trade packets...")
            while self.running:
                try:
                    packet = await websocket.recv()
                    recv_time = time.time()
                    
                    data = json.loads(packet)
                    self.trade_count += 1
                    
                    event_time = data.get("E", 0) / 1000.0
                    latency_ms = (recv_time - event_time) * 1000.0
                    
                    self.latencies.append(latency_ms)

                    price = float(data.get("p", "0.0"))
                    quantity = float(data.get("q", "0.0"))

                    self.write_to_ring_buffer(recv_time, symbol, price, quantity)

                except websockets.exceptions.ConnectionClosed:
                    print(f"[FEED] Warning: WebSocket connection closed for {symbol}. Reconnecting...")
                    break
                except Exception as e:
                    print(f"[FEED] Error parsing packet for {symbol}: {e}")
                    break

    async def stat_logger(self):
        while self.running:
            await asyncio.sleep(5)
            if self.start_time is None:
                continue

            elapsed = time.time() - self.start_time
            rate = self.trade_count / elapsed if elapsed > 0 else 0
            avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0

            log_line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Throughput: {rate:.2f} trades/sec | Avg Latency: {avg_latency:.2f} ms | Total Trades: {self.trade_count}\n"

            with open("logs/ingestion_pool_stats.txt", "a") as f:
                f.write(log_line)

            print(log_line.strip())

    async def connect_and_stream(self):
        self.running = True
        self.start_time = time.time()

        tasks = [self.stream_symbol(symbol) for symbol in self.symbols]
        tasks.append(self.stat_logger())

        await asyncio.gather(*tasks)

    def cleanup(self):
        self.running = False
        try:
            self.shm.close()
            self.shm.unlink()
        except Exception:
            pass

if __name__ == "__main__":
    symbols = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt"]
    feed = HFTMarketDataFeed(symbols)
    try:
        asyncio.run(feed.connect_and_stream())
    except KeyboardInterrupt:
        print("\n[FEED] Shutting down high-speed market data client.")
    finally:
        feed.cleanup()
