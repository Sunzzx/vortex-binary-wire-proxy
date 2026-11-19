"""Header filter + lightweight wire proxy with optional TLS ClientHello SNI peek."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from .frame import Frame, FrameCodec, FrameFlags
from .mux import StreamMux
from .pool import ConnectionPool


def peek_tls_sni(data: bytes) -> Optional[str]:
    """Extract SNI from a TLS 1.2/1.3 ClientHello if fully present; else None."""
    if len(data) < 5:
        return None
    if data[0] != 0x16:  # handshake
        return None
    # record length
    rec_len = (data[3] << 8) | data[4]
    if len(data) < 5 + rec_len:
        return None
    hs = data[5:]
    if not hs or hs[0] != 0x01:  # client_hello
        return None
    if len(hs) < 4:
        return None
    hs_len = (hs[1] << 16) | (hs[2] << 8) | hs[3]
    body = hs[4 : 4 + hs_len]
    # version(2) random(32) session_id
    if len(body) < 34:
        return None
    pos = 34
    if pos >= len(body):
        return None
    sid_len = body[pos]
    pos += 1 + sid_len
    if pos + 2 > len(body):
        return None
    cs_len = (body[pos] << 8) | body[pos + 1]
    pos += 2 + cs_len
    if pos + 1 > len(body):
        return None
    comp_len = body[pos]
    pos += 1 + comp_len
    if pos + 2 > len(body):
        return None
    ext_len = (body[pos] << 8) | body[pos + 1]
    pos += 2
    end = pos + ext_len
    if end > len(body):
        return None
    while pos + 4 <= end:
        etype = (body[pos] << 8) | body[pos + 1]
        elen = (body[pos + 2] << 8) | body[pos + 3]
        pos += 4
        if pos + elen > end:
            return None
        if etype == 0x0000:  # server_name
            ext = body[pos : pos + elen]
            if len(ext) < 2:
                return None
            list_len = (ext[0] << 8) | ext[1]
            p = 2
            list_end = 2 + list_len
            while p + 3 <= list_end and p + 3 <= len(ext):
                name_type = ext[p]
                name_len = (ext[p + 1] << 8) | ext[p + 2]
                p += 3
                if p + name_len > len(ext):
                    return None
                if name_type == 0:
                    try:
                        return ext[p : p + name_len].decode("ascii")
                    except UnicodeDecodeError:
                        return None
                p += name_len
            return None
        pos += elen
    return None


@dataclass
class HeaderFilter:
    """Allow/deny frames by stream id prefix and flag mask (SIMD-shaped batch API)."""

    allow_stream_ids: Optional[Sequence[int]] = None
    deny_flags: int = 0
    max_payload: int = 1024 * 1024

    def accept(self, frame: Frame) -> bool:
        if frame.header.length > self.max_payload:
            return False
        if frame.flags & self.deny_flags:
            return False
        if self.allow_stream_ids is not None and frame.stream_id not in self.allow_stream_ids:
            return False
        return True

    def filter_batch(self, frames: Sequence[Frame]) -> List[Frame]:
        # Explicit tight loop — CPython-friendly; vector-shaped API for future SIMD.
        return [f for f in frames if self.accept(f)]


RouteFn = Callable[[Frame], str]


@dataclass
class WireProxy:
    """Multiplex inbound VX frames, filter headers, route to pooled peer backends."""

    filter: HeaderFilter = field(default_factory=HeaderFilter)
    pool: ConnectionPool = field(default_factory=lambda: ConnectionPool(64))
    mux: StreamMux = field(default_factory=StreamMux)
    routes: Dict[int, str] = field(default_factory=dict)
    default_peer: str = "upstream:443"
    forwarded: List[Tuple[str, bytes]] = field(default_factory=list)

    def route_stream(self, stream_id: int, peer: str) -> None:
        self.routes[stream_id] = peer

    def handle_wire(self, data: bytes) -> List[Frame]:
        frames = self.mux.feed(data)
        accepted = self.filter.filter_batch(frames)
        for frame in accepted:
            peer = self.routes.get(frame.stream_id, self.default_peer)
            slot = self.pool.acquire(peer)
            try:
                wire = frame.to_bytes()
                self.pool.record_io(slot.conn_id, bytes_out=len(wire))
                self.forwarded.append((peer, wire))
            finally:
                self.pool.release(slot.conn_id)
        return accepted

    def inject_upstream(self, stream_id: int, payload: bytes, *, fin: bool = False) -> bytes:
        if stream_id not in list(self.mux.active_streams()) and self.mux.stream_state(stream_id).value == "idle":
            self.mux.open_stream(stream_id)
        return self.mux.send(stream_id, payload, fin=fin)
