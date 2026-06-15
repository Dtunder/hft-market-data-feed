import multiprocessing.shared_memory
import struct
import json

class LockFreeRingBuffer:
    """
    A lock-free ring buffer using shared memory for inter-process communication.
    Stores messages of a fixed size.
    Size: maximum number of elements.
    Element size: bytes per element.
    """
    def __init__(self, name: str, size: int = 10000, element_size: int = 2048, create: bool = True):
        self.name = name
        self.size = size
        self.element_size = element_size
        self.header_size = 16  # head (8 bytes), tail (8 bytes)
        self.total_size = self.header_size + (self.size * self.element_size)

        self.shm = None
        if create:
            try:
                self.shm = multiprocessing.shared_memory.SharedMemory(name=self.name, create=True, size=self.total_size)
                # Initialize head and tail to 0
                self._write_head(0)
                self._write_tail(0)
            except FileExistsError:
                self.shm = multiprocessing.shared_memory.SharedMemory(name=self.name, create=False)
        else:
            self.shm = multiprocessing.shared_memory.SharedMemory(name=self.name, create=False)

    def _read_head(self):
        if self.shm is None:
            return 0
        view = self.shm.buf[0:8]
        val = struct.unpack('Q', view)[0]
        del view
        return val

    def _write_head(self, val):
        if self.shm is None:
            return
        view = self.shm.buf[0:8]
        struct.pack_into('Q', view, 0, val)
        del view

    def _read_tail(self):
        if self.shm is None:
            return 0
        view = self.shm.buf[8:16]
        val = struct.unpack('Q', view)[0]
        del view
        return val

    def _write_tail(self, val):
        if self.shm is None:
            return
        view = self.shm.buf[8:16]
        struct.pack_into('Q', view, 0, val)
        del view

    def push(self, data: bytes):
        if self.shm is None:
            return

        head = self._read_head()
        tail = self._read_tail()

        # Calculate next head position
        next_head = (head + 1) % self.size

        # Write data to current head slot
        offset = self.header_size + (head * self.element_size)
        data_len = min(len(data), self.element_size - 4)  # Leave 4 bytes for length prefix

        view = self.shm.buf[offset:offset + self.element_size]
        struct.pack_into('I', view, 0, data_len)
        view[4:4+data_len] = data[:data_len]
        del view

        # Update head
        self._write_head(next_head)

        # If buffer is full, advance tail to drop oldest message
        if next_head == tail:
            self._write_tail((tail + 1) % self.size)

    def pop(self):
        if self.shm is None:
            return None

        head = self._read_head()
        tail = self._read_tail()

        if head == tail:
            return None  # Empty

        offset = self.header_size + (tail * self.element_size)
        view = self.shm.buf[offset:offset + self.element_size]

        data_len = struct.unpack_from('I', view, 0)[0]
        if data_len > self.element_size - 4:
            data_len = 0 # Corrupted or unitialized

        data = bytes(view[4:4+data_len])
        del view

        self._write_tail((tail + 1) % self.size)
        return data

    def close(self):
        if self.shm is not None:
            self.shm.close()

    def unlink(self):
        if self.shm is not None:
            try:
                self.shm.unlink()
            except FileNotFoundError:
                pass
# End of file
