# vortex-binary-wire-proxy

> **Clean-Room Open-Source Implementation**  
> *Target Architecture Archetype:* Envoy / HAProxy binary framing & transparent wire proxy  
> *Author:* Ayush Kandwal ([@Sunzzx](https://github.com/Sunzzx)) | License: MIT

---

## Overview

`vortex-binary-wire-proxy` is a dependency-free Python library for:

- **VX binary frames** — fixed 16-byte big-endian header (`magic|ver|flags|stream|len|seq`) + payload views
- **Power-of-two ring buffers** — wraparound write/read/peek for streaming ingress
- **Stream multiplexer** — per-stream sequencing, SYN/FIN/RST lifecycle, fragmented reassembly
- **Connection pool** — bounded LIFO reuse keyed by peer
- **Header filter + wire proxy** — batch allow/deny by stream id and flag mask; route to backends
- **TLS ClientHello SNI peek** — extract hostname without full handshake crypto

Zero external runtime dependencies. Sub-millisecond pure-Python paths with unit coverage.

## Quickstart

```bash
uv run pytest -q
python -c "from vortex_binary_wire_proxy.runner import smoke; print(smoke())"
```

```python
from vortex_binary_wire_proxy import FrameCodec, StreamMux, WireProxy, HeaderFilter

mux = StreamMux()
mux.open_stream(1)
wire = mux.send(1, b"hello")
peer = StreamMux()
peer.feed(wire)
assert peer.recv(1) == b"hello"

proxy = WireProxy(filter=HeaderFilter(allow_stream_ids=[1]))
proxy.route_stream(1, "backend:9000")
proxy.mux.open_stream(1)
proxy.handle_wire(FrameCodec.encode(1, b"payload"))
```

## Frame layout

```
 0               1               2               3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      magic 0x5658 'VX'        |  ver  | flags |   stream_id   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                            stream_id (cont)                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                             length                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                            sequence                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                            payload...                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

## Architecture

```
ingress bytes → RingBuffer → FrameCodec.try_decode
                              ↓
                         StreamMux (seq check, inbox)
                              ↓
                      HeaderFilter.batch
                              ↓
                   ConnectionPool.acquire(peer)
                              ↓
                      forward framed bytes
```
