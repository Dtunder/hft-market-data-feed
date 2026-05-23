import collections
import time
import random

class Candle:
    def __init__(self, timestamp, open, high, low, close, volume, timeframe):
        self.timestamp = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.timeframe = timeframe

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "timeframe": self.timeframe
        }

    def is_bullish(self) -> bool:
        return self.close > self.open

    def body_pct(self) -> float:
        if self.open == 0:
            return 0.0
        return abs(self.close - self.open) / self.open * 100.0


class CandleAggregator:
    def __init__(self, timeframes: list = None):
        if timeframes is None:
            timeframes = [60, 300, 900]
        self.timeframes = timeframes
        self.candles = {tf: collections.deque(maxlen=500) for tf in timeframes}
        self.current_candle = {tf: None for tf in timeframes}
        self.tick_count = 0

    def process_tick(self, price: float, volume: float, timestamp: float = None):
        timestamp = timestamp or time.time()
        for tf in self.timeframes:
            bucket = int(timestamp // tf) * tf
            curr = self.current_candle[tf]
            if not curr or curr.timestamp != bucket:
                if curr is not None:
                    self.candles[tf].append(curr)
                self.current_candle[tf] = Candle(
                    timestamp=bucket,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=volume,
                    timeframe=tf
                )
            else:
                curr.high = max(curr.high, price)
                curr.low = min(curr.low, price)
                curr.close = price
                curr.volume += volume
        self.tick_count += 1

    def get_latest_candle(self, timeframe: int) -> Candle:
        if timeframe in self.candles and self.candles[timeframe]:
            return self.candles[timeframe][-1]
        return None

    def get_candles(self, timeframe: int, n: int = 20) -> list:
        if timeframe in self.candles:
            c = self.candles[timeframe]
            return list(c)[-n:]
        return []

    def detect_support_resistance(self, timeframe: int, n: int = 50) -> dict:
        candles = self.get_candles(timeframe, n)
        if not candles:
            return {"support": 0.0, "resistance": 0.0, "current_price": 0.0,
                    "distance_to_support_pct": 0.0, "distance_to_resistance_pct": 0.0}

        lows = sorted([c.low for c in candles])
        highs = sorted([c.high for c in candles], reverse=True)

        k = max(1, int(len(candles) * 0.1))

        support = round(sum(lows[:k]) / k)
        resistance = round(sum(highs[:k]) / k)

        current_price = candles[-1].close

        dist_supp = 0.0
        dist_res = 0.0
        if current_price > 0:
            dist_supp = abs(current_price - support) / current_price * 100.0
            dist_res = abs(resistance - current_price) / current_price * 100.0

        return {
            "support": float(support),
            "resistance": float(resistance),
            "current_price": float(current_price),
            "distance_to_support_pct": dist_supp,
            "distance_to_resistance_pct": dist_res
        }

    def detect_trend(self, timeframe: int, n: int = 20) -> str:
        candles = self.get_candles(timeframe, n)
        m = len(candles)
        if m < 2:
            return "SIDEWAYS"

        # x is 0, 1, 2, ... m-1
        # y is close price
        sum_x = sum(range(m))
        sum_y = sum(c.close for c in candles)
        sum_xy = sum(i * c.close for i, c in enumerate(candles))
        sum_x2 = sum(i * i for i in range(m))

        denominator = (m * sum_x2 - sum_x ** 2)
        if denominator == 0:
            return "SIDEWAYS"

        slope = (m * sum_xy - sum_x * sum_y) / denominator

        if slope > 0.001:
            return "UPTREND"
        elif slope < -0.001:
            return "DOWNTREND"
        else:
            return "SIDEWAYS"


if __name__ == '__main__':
    agg = CandleAggregator(timeframes=[60, 300])

    # Simulate 400 ticks over 2 hours
    base_time = time.time() - 7200
    price = 58000.0
    for i in range(400):
        price += random.normalvariate(0, 50)
        vol = random.uniform(0.1, 2.0)
        ts = base_time + i * 18  # one tick every 18 seconds
        agg.process_tick(price, vol, ts)

    for tf in [60, 300]:
        print(f"\n--- {tf}s candles ---")
        print(f"Completed candles: {len(agg.candles[tf])}")
        print(f"Trend: {agg.detect_trend(tf)}")
        sr = agg.detect_support_resistance(tf)
        print(f"Support: {sr['support']:.0f} | Resistance: {sr['resistance']:.0f}")
