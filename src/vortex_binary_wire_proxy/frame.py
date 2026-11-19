"""Binary frame codec: fixed 16-byte header + payload (zero-copy views).

Layout (big-endian):
  magic(2)=0x5658 'VX' | version(1) | flags(1) | stream_id(4)
  length(4) | sequence(4) | payload[length]
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntFlag
from typing import Final, Optional, Tuple, Union

MAGIC: Final[int] = 0x5658
VERSION: Final[int] = 1
HEADER_SIZE: Final[int] = 16
_HEADER_STRUCT: Final[struct.Struct] = struct.Struct("!HBBIII")
MAX_PAYLOAD: Final[int] = 16 * 1024 * 1024


class FrameFlags(IntFlag):
    NONE = 0
    FIN = 1 << 0
    SYN = 1 << 1
    RST = 1 << 2
    ACK = 1 << 3
    PRIORITY = 1 << 4
    COMPRESSED = 1 << 5


@dataclass(frozen=True, slots=True)
class FrameHeader:
    magic: int
    version: int
    flags: int
    stream_id: int
    length: int
    sequence: int

    def pack(self) -> bytes:
        return _HEADER_STRUCT.pack(
            self.magic,
            self.version,
            self.flags & 0xFF,
            self.stream_id & 0xFFFFFFFF,
            self.length & 0xFFFFFFFF,
            self.sequence & 0xFFFFFFFF,
        )


@dataclass(frozen=True, slots=True)
class Frame:
    header: FrameHeader
    payload: memoryview

    @property
    def stream_id(self) -> int:
        return self.header.stream_id

    @property
    def flags(self) -> int:
        return self.header.flags

    @property
    def sequence(self) -> int:
        return self.header.sequence

    def to_bytes(self) -> bytes:
        return self.header.pack() + self.payload.tobytes()


class FrameCodec:
    """Encode/decode VX frames; decode returns views into the source buffer."""

    __slots__ = ()

    @staticmethod
    def encode(
        stream_id: int,
        payload: Union[bytes, bytearray, memoryview],
        *,
        flags: int = FrameFlags.NONE,
        sequence: int = 0,
        version: int = VERSION,
    ) -> bytes:
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise TypeError("payload must be bytes-like")
        mv = memoryview(payload).cast("B")
        length = len(mv)
        if length > MAX_PAYLOAD:
            raise ValueError(f"payload too large: {length} > {MAX_PAYLOAD}")
        if stream_id < 0 or stream_id > 0xFFFFFFFF:
            raise ValueError("stream_id out of range")
        hdr = FrameHeader(MAGIC, version, int(flags) & 0xFF, stream_id, length, sequence & 0xFFFFFFFF)
        return hdr.pack() + mv.tobytes()

    @staticmethod
    def decode_header(buf: Union[bytes, bytearray, memoryview]) -> FrameHeader:
        mv = memoryview(buf).cast("B")
        if len(mv) < HEADER_SIZE:
            raise ValueError("incomplete header")
        magic, version, flags, stream_id, length, sequence = _HEADER_STRUCT.unpack_from(mv)
        if magic != MAGIC:
            raise ValueError(f"bad magic: 0x{magic:04x}")
        if version != VERSION:
            raise ValueError(f"unsupported version: {version}")
        if length > MAX_PAYLOAD:
            raise ValueError(f"declared length too large: {length}")
        return FrameHeader(magic, version, flags, stream_id, length, sequence)

    @staticmethod
    def try_decode(buf: Union[bytes, bytearray, memoryview]) -> Tuple[Optional[Frame], int]:
        """Return (frame, bytes_consumed). frame is None if more data needed."""
        mv = memoryview(buf).cast("B")
        if len(mv) < HEADER_SIZE:
            return None, 0
        header = FrameCodec.decode_header(mv)
        total = HEADER_SIZE + header.length
        if len(mv) < total:
            return None, 0
        payload = mv[HEADER_SIZE:total]
        return Frame(header, payload), total

    @staticmethod
    def decode(buf: Union[bytes, bytearray, memoryview]) -> Frame:
        frame, n = FrameCodec.try_decode(buf)
        if frame is None:
            raise ValueError("incomplete frame")
        expected = HEADER_SIZE + frame.header.length
        if n != expected:
            raise ValueError("frame length mismatch")
        return frame
