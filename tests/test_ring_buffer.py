import pytest
from src.ring_buffer import LockFreeRingBuffer

def test_ring_buffer_basic():
    buf = LockFreeRingBuffer("test_basic_buffer", size=1024)
    buf.write(b'hello')
    assert buf.read() == b'hello'
    assert buf.read() is None
    buf.close()

def test_ring_buffer_wrap_around():
    buf = LockFreeRingBuffer("test_wrap_buffer", size=64)
    # The header is 16 bytes. Capacity is 48 bytes.
    # Write 20 bytes -> payload is 4 + 20 = 24 bytes
    msg1 = b'A' * 20
    buf.write(msg1)

    # Write another 20 bytes -> would take 24 bytes. Total 48 bytes.
    # We require 1 byte to stay empty, so 48 bytes on 48 byte capacity is full.
    # Let's read first so we wrap around correctly without buffer error.
    assert buf.read() == msg1

    msg2 = b'B' * 20
    buf.write(msg2)
    assert buf.read() == msg2

    # Write another msg that wraps around
    msg3 = b'C' * 20
    buf.write(msg3)
    assert buf.read() == msg3
    buf.close()
