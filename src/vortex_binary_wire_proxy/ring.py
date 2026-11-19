"""Power-of-two ring buffer for streaming wire bytes (bytearray-backed)."""

from __future__ import annotations

from typing import Optional, Tuple


def _next_pow2(n: int) -> int:
    if n < 1:
        raise ValueError("capacity must be >= 1")
    p = 1
    while p < n:
        p <<= 1
    return p


class RingBuffer:
    __slots__ = ("_buf", "_mask", "_r", "_w", "_size")

    def __init__(self, capacity: int = 65536) -> None:
        cap = _next_pow2(capacity)
        self._buf = bytearray(cap)
        self._mask = cap - 1
        self._r = 0
        self._w = 0
        self._size = 0

    @property
    def capacity(self) -> int:
        return self._mask + 1

    @property
    def size(self) -> int:
        return self._size

    @property
    def free(self) -> int:
        return self.capacity - self._size

    def clear(self) -> None:
        self._r = self._w = self._size = 0

    def write(self, data: bytes | bytearray | memoryview) -> int:
        mv = memoryview(data).cast("B")
        n = min(len(mv), self.free)
        if n == 0:
            return 0
        cap = self.capacity
        w = self._w
        first = min(n, cap - w)
        self._buf[w : w + first] = mv[:first]
        if n > first:
            self._buf[0 : n - first] = mv[first:n]
        self._w = (w + n) & self._mask
        self._size += n
        return n

    def write_all(self, data: bytes | bytearray | memoryview) -> None:
        mv = memoryview(data).cast("B")
        if len(mv) > self.free:
            raise BufferError(f"ring full: need {len(mv)}, free {self.free}")
        written = self.write(mv)
        if written != len(mv):
            raise BufferError("short write")

    def read(self, n: int) -> bytes:
        take = min(n, self._size)
        if take == 0:
            return b""
        cap = self.capacity
        r = self._r
        first = min(take, cap - r)
        out = bytes(self._buf[r : r + first])
        if take > first:
            out += bytes(self._buf[0 : take - first])
        self._r = (r + take) & self._mask
        self._size -= take
        return out

    def peek(self, n: int) -> bytes:
        take = min(n, self._size)
        if take == 0:
            return b""
        cap = self.capacity
        r = self._r
        first = min(take, cap - r)
        out = bytes(self._buf[r : r + first])
        if take > first:
            out += bytes(self._buf[0 : take - first])
        return out

    def discard(self, n: int) -> int:
        take = min(n, self._size)
        self._r = (self._r + take) & self._mask
        self._size -= take
        return take

    def contiguous_read_view(self) -> memoryview:
        """View of the longest contiguous readable slice (may be < size)."""
        if self._size == 0:
            return memoryview(self._buf)[0:0]
        r = self._r
        first = min(self._size, self.capacity - r)
        return memoryview(self._buf)[r : r + first]

    def snapshot(self) -> bytes:
        return self.peek(self._size)
