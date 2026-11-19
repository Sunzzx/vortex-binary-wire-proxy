"""Vortex binary wire protocol multiplexer and transparent proxy primitives."""

from .frame import Frame, FrameCodec, FrameFlags, FrameHeader
from .ring import RingBuffer
from .mux import StreamMux, StreamState
from .proxy import HeaderFilter, WireProxy, peek_tls_sni
from .pool import ConnectionPool

__version__ = "0.2.0"
__all__ = [
    "Frame",
    "FrameCodec",
    "FrameFlags",
    "FrameHeader",
    "RingBuffer",
    "StreamMux",
    "StreamState",
    "HeaderFilter",
    "WireProxy",
    "peek_tls_sni",
    "ConnectionPool",
]
