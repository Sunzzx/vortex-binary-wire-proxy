"""Stream multiplexer over VX frames with per-stream sequencing."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional

from .frame import Frame, FrameCodec, FrameFlags
from .ring import RingBuffer


class StreamState(str, Enum):
    IDLE = "idle"
    OPEN = "open"
    HALF_CLOSED = "half_closed"
    CLOSED = "closed"


@dataclass
class _Stream:
    stream_id: int
    state: StreamState = StreamState.IDLE
    next_tx_seq: int = 0
    next_rx_seq: int = 0
    inbox: RingBuffer = field(default_factory=lambda: RingBuffer(8192))


class StreamMux:
    """Demux inbound frames into stream inboxes; encode outbound payloads."""

    __slots__ = ("_streams", "_inbound", "_max_streams")

    def __init__(self, *, max_streams: int = 256, inbound_capacity: int = 131072) -> None:
        self._streams: Dict[int, _Stream] = {}
        self._inbound = RingBuffer(inbound_capacity)
        self._max_streams = max_streams

    def open_stream(self, stream_id: int) -> None:
        if stream_id in self._streams and self._streams[stream_id].state not in (
            StreamState.CLOSED,
            StreamState.IDLE,
        ):
            raise ValueError(f"stream {stream_id} already open")
        if len([s for s in self._streams.values() if s.state == StreamState.OPEN]) >= self._max_streams:
            raise RuntimeError("max streams reached")
        self._streams[stream_id] = _Stream(stream_id=stream_id, state=StreamState.OPEN)

    def close_stream(self, stream_id: int, *, reset: bool = False) -> bytes:
        st = self._streams.get(stream_id)
        if st is None:
            raise KeyError(stream_id)
        flags = FrameFlags.RST if reset else FrameFlags.FIN
        frame = FrameCodec.encode(stream_id, b"", flags=flags, sequence=st.next_tx_seq)
        st.next_tx_seq += 1
        st.state = StreamState.CLOSED
        return frame

    def send(self, stream_id: int, payload: bytes, *, fin: bool = False) -> bytes:
        st = self._require_open(stream_id)
        flags = FrameFlags.FIN if fin else FrameFlags.NONE
        if st.next_tx_seq == 0 and st.state == StreamState.OPEN:
            flags |= FrameFlags.SYN
        frame = FrameCodec.encode(stream_id, payload, flags=flags, sequence=st.next_tx_seq)
        st.next_tx_seq += 1
        if fin:
            st.state = StreamState.HALF_CLOSED
        return frame

    def feed(self, data: bytes) -> List[Frame]:
        """Push wire bytes; return fully decoded frames in arrival order."""
        self._inbound.write_all(data) if len(data) <= self._inbound.free else self._write_chunked(data)
        out: List[Frame] = []
        while True:
            snap = self._inbound.snapshot()
            frame, n = FrameCodec.try_decode(snap)
            if frame is None:
                break
            self._inbound.discard(n)
            self._accept(frame)
            out.append(frame)
        return out

    def recv(self, stream_id: int, max_bytes: int = 65536) -> bytes:
        st = self._streams.get(stream_id)
        if st is None:
            raise KeyError(stream_id)
        return st.inbox.read(max_bytes)

    def stream_state(self, stream_id: int) -> StreamState:
        st = self._streams.get(stream_id)
        return st.state if st else StreamState.IDLE

    def active_streams(self) -> Iterable[int]:
        return (sid for sid, st in self._streams.items() if st.state == StreamState.OPEN)

    def _write_chunked(self, data: bytes) -> None:
        offset = 0
        while offset < len(data):
            n = self._inbound.write(data[offset:])
            if n == 0:
                raise BufferError("inbound ring full")
            offset += n

    def _require_open(self, stream_id: int) -> _Stream:
        st = self._streams.get(stream_id)
        if st is None or st.state not in (StreamState.OPEN, StreamState.HALF_CLOSED):
            raise ValueError(f"stream {stream_id} not open")
        return st

    def _accept(self, frame: Frame) -> None:
        sid = frame.stream_id
        st = self._streams.get(sid)
        if st is None:
            st = _Stream(stream_id=sid, state=StreamState.OPEN)
            self._streams[sid] = st
        if frame.flags & FrameFlags.RST:
            st.state = StreamState.CLOSED
            st.inbox.clear()
            return
        if frame.sequence != st.next_rx_seq:
            raise ValueError(
                f"sequence gap on stream {sid}: got {frame.sequence}, expected {st.next_rx_seq}"
            )
        st.next_rx_seq += 1
        if len(frame.payload):
            written = st.inbox.write(frame.payload)
            if written != len(frame.payload):
                raise BufferError(f"stream {sid} inbox full")
        if frame.flags & FrameFlags.FIN:
            st.state = StreamState.CLOSED if st.state == StreamState.HALF_CLOSED else StreamState.HALF_CLOSED
