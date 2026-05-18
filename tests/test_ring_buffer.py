import pytest
import time
import multiprocessing
import multiprocessing.shared_memory
from src.ring_buffer import LockFreeRingBuffer

def consumer_process(name, capacity, num_items, result_queue):
    """
    Consumer process that pops items from the ring buffer.
    """
    try:
        ring_buffer = LockFreeRingBuffer(capacity=capacity, name=name, create=False)
        received = 0

        while received < num_items:
            tick = ring_buffer.pop()
            if tick is not None:
                received += 1
            else:
                # Tight loop, wait for items
                pass

        result_queue.put(received)
    except Exception as e:
        result_queue.put(e)
    finally:
        # Need to clean up data_view before close
        try:
            ring_buffer.close()
        except:
            pass

def test_ring_buffer_basic():
    buffer = LockFreeRingBuffer(capacity=16, name="test_basic", create=True)
    try:
        # Buffer is empty initially
        assert buffer.pop() is None

        # Push items
        assert buffer.push(1.0, 100.0, 10.0) == True
        assert buffer.push(2.0, 101.0, 11.0) == True

        # Pop items
        t1 = buffer.pop()
        assert t1 == (1.0, 100.0, 10.0)

        t2 = buffer.pop()
        assert t2 == (2.0, 101.0, 11.0)

        # Buffer is empty again
        assert buffer.pop() is None
    finally:
        buffer.close()

def test_ring_buffer_overflow():
    buffer = LockFreeRingBuffer(capacity=4, name="test_overflow", create=True)
    try:
        # Push to capacity
        for i in range(4):
            assert buffer.push(float(i), 100.0, 10.0) == True

        # Buffer is full
        assert buffer.push(4.0, 100.0, 10.0) == False

        # Pop one
        assert buffer.pop() == (0.0, 100.0, 10.0)

        # Now we can push one
        assert buffer.push(4.0, 100.0, 10.0) == True
    finally:
        buffer.close()

def test_ring_buffer_stress_throughput():
    capacity = 16384
    num_items = 100_000
    name = "test_stress"

    buffer = LockFreeRingBuffer(capacity=capacity, name=name, create=True)
    try:
        result_queue = multiprocessing.Queue()
        consumer = multiprocessing.Process(target=consumer_process, args=(name, capacity, num_items, result_queue))

        start_time = time.time()
        consumer.start()

        produced = 0
        while produced < num_items:
            success = buffer.push(float(produced), 50000.0, 1.5)
            if success:
                produced += 1
            else:
                # Buffer full, wait a bit
                pass

        consumer.join()
        end_time = time.time()

        # Check consumer result
        result = result_queue.get()
        assert result == num_items, f"Consumer did not receive all items. Got: {result}"

        # Calculate throughput
        duration = end_time - start_time
        throughput = num_items / duration
        print(f"\nThroughput: {throughput:,.2f} ticks/sec")

        # Ensure throughput exceeds requirement (10,000+ ticks per second)
        assert throughput > 10000, f"Throughput {throughput} is below required 10,000 ticks/sec"

    finally:
        buffer.close()
