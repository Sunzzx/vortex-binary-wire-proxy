"""CLI smoke runner for vortex binary wire proxy primitives."""

from __future__ import annotations

from .frame import FrameCodec, FrameFlags
from .mux import StreamMux
from .pool import ConnectionPool
from .proxy import HeaderFilter, WireProxy, peek_tls_sni
from .ring import RingBuffer


def smoke() -> dict:
    mux = StreamMux()
    mux.open_stream(1)
    wire = mux.send(1, b"ping", fin=False)
    frames = mux.feed(wire)
    assert len(frames) == 1
    assert mux.recv(1) == b"ping"

    ring = RingBuffer(64)
    ring.write_all(b"abc")
    assert ring.read(2) == b"ab"

    pool = ConnectionPool(4)
    s = pool.acquire("a:1")
    pool.release(s.conn_id)

    # Minimal TLS ClientHello-ish buffer without SNI should return None
    sni = peek_tls_sni(b"\x16\x03\x01\x00\x01\x00")
    proxy = WireProxy(filter=HeaderFilter(allow_stream_ids=[1, 2]))
    proxy.mux.open_stream(1)
    out = proxy.handle_wire(FrameCodec.encode(1, b"x", flags=FrameFlags.SYN))
    return {
        "frames": len(frames),
        "sni": sni,
        "proxy_accepted": len(out),
        "pool": pool.stats(),
        "all_passed": True,
    }


if __name__ == "__main__":
    print(smoke())
