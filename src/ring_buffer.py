import multiprocessing.shared_memory as shared_memory
import struct
import math
import ctypes

class LockFreeRingBuffer:
    """
    A lock-free circular ring buffer (Single Producer, Single Consumer)
    using Python's shared memory, inspired by the LMAX Disruptor pattern.
    Designed for high-frequency market data tick ingestion.
    """

    # Tick data structure: timestamp (double 8 bytes), price (double 8 bytes), quantity (double 8 bytes)
    # Total tick size: 24 bytes
    TICK_FMT = "ddd"
    TICK_SIZE = struct.calcsize(TICK_FMT)

    # Header format: producer_sequence (long long 8 bytes), consumer_sequence (long long 8 bytes)
    HEADER_FMT = "qq"
    HEADER_SIZE = struct.calcsize(HEADER_FMT)

    def __init__(self, capacity: int, name: str = None, create: bool = True):
        # Capacity must be a power of 2 for fast modulo operations using bitwise AND
        self.capacity = 1 << (capacity - 1).bit_length() if capacity > 0 else 1
        self.mask = self.capacity - 1
        self.buffer_size = self.HEADER_SIZE + (self.capacity * self.TICK_SIZE)

        self.create = create
        self.name = name

        if self.create:
            self.shm = shared_memory.SharedMemory(create=True, size=self.buffer_size, name=self.name)
            self.name = self.shm.name
            # Initialize sequences to 0
            self._write_seq(0, 0)
            self._write_seq(1, 0)
        else:
            self.shm = shared_memory.SharedMemory(create=False, name=self.name)

        # Buffer memory view points to the data area
        self.data_view = self.shm.buf[self.HEADER_SIZE:]

    def _read_seq(self, index: int) -> int:
        """Reads sequence number. index 0 = producer, 1 = consumer."""
        offset = index * 8
        return struct.unpack_from("q", self.shm.buf, offset)[0]

    def _write_seq(self, index: int, value: int):
        """Writes sequence number. index 0 = producer, 1 = consumer."""
        offset = index * 8
        struct.pack_into("q", self.shm.buf, offset, value)

    @property
    def producer_seq(self) -> int:
        return self._read_seq(0)

    @property
    def consumer_seq(self) -> int:
        return self._read_seq(1)

    def push(self, timestamp: float, price: float, quantity: float) -> bool:
        """
        Pushes a new tick into the buffer.
        Returns True if successful, False if the buffer is full.
        """
        prod_seq = self.producer_seq
        cons_seq = self.consumer_seq

        # Check if the buffer is full
        if prod_seq - cons_seq >= self.capacity:
            return False

        # Calculate position using bitwise AND (faster than modulo)
        idx = prod_seq & self.mask
        offset = idx * self.TICK_SIZE

        # Write tick data
        struct.pack_into(self.TICK_FMT, self.data_view, offset, timestamp, price, quantity)

        # Update producer sequence (acts as a memory barrier / publish)
        self._write_seq(0, prod_seq + 1)
        return True

    def pop(self) -> tuple:
        """
        Pops a tick from the buffer.
        Returns a tuple (timestamp, price, quantity) if successful, None if empty.
        """
        prod_seq = self.producer_seq
        cons_seq = self.consumer_seq

        # Check if buffer is empty
        if cons_seq >= prod_seq:
            return None

        # Calculate position
        idx = cons_seq & self.mask
        offset = idx * self.TICK_SIZE

        # Read tick data
        tick = struct.unpack_from(self.TICK_FMT, self.data_view, offset)

        # Update consumer sequence
        self._write_seq(1, cons_seq + 1)
        return tick

    def close(self):
        """Closes the shared memory."""
        # Need to release memoryview before closing to prevent BufferError
        del self.data_view
        self.shm.close()
        if self.create:
            try:
                self.shm.unlink()
            except FileNotFoundError:
                pass
