"""High-throughput binary wire protocol multiplexer and TLS-aware proxy."""

from .frame import Frame, FrameCodec, FrameFlags, FrameHeader, HEADER_SIZE, MAGIC, VERSION
from .mux import StreamMux, StreamState
from .pool import ConnSlot, ConnectionPool
from .proxy import HeaderFilter, WireProxy, peek_tls_sni
from .ring import RingBuffer
from .runner import smoke

__version__ = "0.1.1"

__all__ = [
    "HEADER_SIZE",
    "MAGIC",
    "VERSION",
    "ConnSlot",
    "ConnectionPool",
    "Frame",
    "FrameCodec",
    "FrameFlags",
    "FrameHeader",
    "HeaderFilter",
    "RingBuffer",
    "StreamMux",
    "StreamState",
    "WireProxy",
    "peek_tls_sni",
    "smoke",
    "__version__",
]
