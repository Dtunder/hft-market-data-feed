# 📡 HFT Market Data Feed Client
*High-Frequency WebSocket Ingestion Engine*

> [!NOTE]
> This module manages real-time, low-latency market data ingestion using highly optimized asynchronous WebSockets to parse exchange order books and trade streams with $<1$ms latency overhead.

## 🛠️ Performance Architecture
- **Asynchronous Loop:** Built on Python's `asyncio` to prevent thread-blocking latency spikes.
- **Message Parsing:** Microsecond JSON deserialization directly into shared memory.
- **Buffer Queue:** Thread-safe lockless queue to feed the execution and signal models.

---

## ⚡ Execution Instructions
To launch the market feed and monitor incoming real-time trade packets:
```bash
python src/feed_client.py
```
