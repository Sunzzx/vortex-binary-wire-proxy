"""Smoke harness for vortex binary wire proxy end-to-end path."""

from __future__ import annotations

from typing import Any, Dict

from .frame import FrameCodec, FrameFlags
from .mux import StreamMux
from .pool import ConnectionPool
from .proxy import HeaderFilter, WireProxy


def smoke() -> Dict[str, Any]:
    """Exercise encode → mux → filter → pool → proxy round-trip."""
    mux_a = StreamMux()
    mux_a.open_stream(1)
    wire = mux_a.send(1, b"vortex-smoke", fin=False)

    proxy = WireProxy(filter=HeaderFilter(allow_stream_ids=[1]))
    proxy.route_stream(1, "backend-smoke:9000")
    frames = proxy.handle_wire(wire)

    pool = ConnectionPool(2)
    slot = pool.acquire("peer-a")
    pool.record_io(slot.conn_id, bytes_in=len(wire), bytes_out=8)
    pool.release(slot.conn_id)
    reused = pool.acquire("peer-a")
    pool_ok = reused.conn_id == slot.conn_id
    pool.release(reused.conn_id)

    decoded = FrameCodec.decode(wire)
    frame_ok = (
        decoded.stream_id == 1
        and bytes(decoded.payload) == b"vortex-smoke"
        and bool(decoded.flags & FrameFlags.SYN)
    )

    result = {
        "frames": len(frames),
        "proxy_accepted": len(frames),
        "frame_ok": frame_ok,
        "pool_reuse": pool_ok,
        "forwarded": len(proxy.forwarded),
        "all_passed": bool(frames) and frame_ok and pool_ok and len(proxy.forwarded) == 1,
    }
    return result


if __name__ == "__main__":
    out = smoke()
    raise SystemExit(0 if out["all_passed"] else 1)
