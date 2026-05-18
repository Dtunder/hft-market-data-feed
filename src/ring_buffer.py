import multiprocessing.shared_memory as shm
import struct

class LockFreeRingBuffer:
    def __init__(self, name, size=1024*1024, create=True):
        self.size = size
        self.name = name
        self.create = create
        self.shm = shm.SharedMemory(create=create, size=size, name=name)
        self.header_format = 'Q'
        self.header_size = struct.calcsize('QQ') # 16 bytes: W and R
        self.capacity = size - self.header_size

        if create:
            self._set_w(0)
            self._set_r(0)

    def _get_w(self):
        view = self.shm.buf[0:8]
        w = struct.unpack('Q', view)[0]
        view.release()
        return w

    def _get_r(self):
        view = self.shm.buf[8:16]
        r = struct.unpack('Q', view)[0]
        view.release()
        return r

    def _set_w(self, w):
        view = self.shm.buf[0:8]
        struct.pack_into('Q', view, 0, w)
        view.release()

    def _set_r(self, r):
        view = self.shm.buf[8:16]
        struct.pack_into('Q', view, 0, r)
        view.release()

    def write(self, data: bytes):
        w = self._get_w()
        r = self._get_r()

        msg_len = len(data)

        payload = struct.pack('I', msg_len) + data
        total_len = len(payload)

        used = w - r if w >= r else self.capacity - r + w
        # Keep 1 byte free so w==r always means empty
        if used + total_len >= self.capacity:
            raise BufferError("Buffer full")

        if w + total_len <= self.capacity:
            view = self.shm.buf[self.header_size + w : self.header_size + w + total_len]
            view[:] = payload
            view.release()
            w = (w + total_len) % self.capacity
        else:
            first_part = self.capacity - w
            if first_part > 0:
                view1 = self.shm.buf[self.header_size + w : self.header_size + self.capacity]
                view1[:] = payload[:first_part]
                view1.release()
            second_part = total_len - first_part
            view2 = self.shm.buf[self.header_size : self.header_size + second_part]
            view2[:] = payload[first_part:]
            view2.release()
            w = second_part

        self._set_w(w)

    def read(self):
        w = self._get_w()
        r = self._get_r()

        if w == r:
            return None # empty

        # read length
        len_bytes = bytearray(4)
        if r + 4 <= self.capacity:
            view = self.shm.buf[self.header_size + r : self.header_size + r + 4]
            len_bytes[:] = view
            view.release()
        else:
            first_part = self.capacity - r
            if first_part > 0:
                view1 = self.shm.buf[self.header_size + r : self.header_size + self.capacity]
                len_bytes[:first_part] = view1
                view1.release()
            second_part = 4 - first_part
            view2 = self.shm.buf[self.header_size : self.header_size + second_part]
            len_bytes[first_part:] = view2
            view2.release()

        msg_len = struct.unpack('I', len_bytes)[0]
        data = bytearray(msg_len)
        r = (r + 4) % self.capacity

        if r + msg_len <= self.capacity:
            view = self.shm.buf[self.header_size + r : self.header_size + r + msg_len]
            data[:] = view
            view.release()
            r = (r + msg_len) % self.capacity
        else:
            first_part = self.capacity - r
            if first_part > 0:
                view1 = self.shm.buf[self.header_size + r : self.header_size + self.capacity]
                data[:first_part] = view1
                view1.release()
            second_part = msg_len - first_part
            view2 = self.shm.buf[self.header_size : self.header_size + second_part]
            data[first_part:] = view2
            view2.release()
            r = second_part

        self._set_r(r)
        return bytes(data)

    def close(self):
        self.shm.close()
        if self.create:
            try:
                self.shm.unlink()
            except FileNotFoundError:
                pass
