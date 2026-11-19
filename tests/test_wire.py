"""Unit tests for vortex binary wire proxy."""

from __future__ import annotations

import struct

import pytest

from vortex_binary_wire_proxy import (
    ConnectionPool,
    FrameCodec,
    FrameFlags,
    HeaderFilter,
    RingBuffer,
    StreamMux,
    StreamState,
    WireProxy,
    peek_tls_sni,
)
from vortex_binary_wire_proxy.frame import HEADER_SIZE, MAGIC, VERSION
from vortex_binary_wire_proxy.runner import smoke


class TestFrameCodec:
    def test_roundtrip(self):
        raw = FrameCodec.encode(7, b"hello-wire", flags=FrameFlags.SYN | FrameFlags.ACK, sequence=3)
        frame = FrameCodec.decode(raw)
        assert frame.stream_id == 7
        assert bytes(frame.payload) == b"hello-wire"
        assert frame.flags & FrameFlags.SYN
        assert frame.flags & FrameFlags.ACK
        assert frame.sequence == 3
        assert frame.header.magic == MAGIC
        assert frame.header.version == VERSION
        assert len(raw) == HEADER_SIZE + 10

    def test_try_decode_partial(self):
        raw = FrameCodec.encode(1, b"abcdef")
        frame, n = FrameCodec.try_decode(raw[:HEADER_SIZE])
        assert frame is None and n == 0
        frame, n = FrameCodec.try_decode(raw[: HEADER_SIZE + 3])
        assert frame is None and n == 0
        frame, n = FrameCodec.try_decode(raw)
        assert frame is not None and n == len(raw)

    def test_bad_magic(self):
        raw = bytearray(FrameCodec.encode(1, b"x"))
        raw[0] = 0x00
        with pytest.raises(ValueError, match="bad magic"):
            FrameCodec.decode(raw)

    def test_empty_payload(self):
        raw = FrameCodec.encode(2, b"", flags=FrameFlags.FIN, sequence=0)
        frame = FrameCodec.decode(raw)
        assert len(frame.payload) == 0
        assert frame.flags & FrameFlags.FIN


class TestRingBuffer:
    def test_wraparound(self):
        rb = RingBuffer(8)  # capacity becomes 8
        assert rb.capacity == 8
        assert rb.write(b"123456") == 6
        assert rb.read(4) == b"1234"
        assert rb.write(b"abcd") == 4  # wrap
        assert rb.size == 6
        assert rb.read(6) == b"56abcd"
        assert rb.size == 0

    def test_write_all_overflow(self):
        rb = RingBuffer(4)
        rb.write_all(b"12")
        with pytest.raises(BufferError):
            rb.write_all(b"xyzzy")

    def test_peek_discard(self):
        rb = RingBuffer(16)
        rb.write_all(b"hello")
        assert rb.peek(2) == b"he"
        assert rb.size == 5
        assert rb.discard(2) == 2
        assert rb.read(10) == b"llo"


class TestStreamMux:
    def test_send_recv_ordered(self):
        a = StreamMux()
        b = StreamMux()
        a.open_stream(1)
        wire = a.send(1, b"alpha") + a.send(1, b"beta")
        frames = b.feed(wire)
        assert len(frames) == 2
        assert b.recv(1) == b"alphabeta"
        assert b.stream_state(1) == StreamState.OPEN

    def test_sequence_gap_raises(self):
        mux = StreamMux()
        mux.open_stream(9)
        good = FrameCodec.encode(9, b"a", sequence=0)
        bad = FrameCodec.encode(9, b"b", sequence=2)
        mux.feed(good)
        with pytest.raises(ValueError, match="sequence gap"):
            mux.feed(bad)

    def test_fin_and_rst(self):
        a = StreamMux()
        b = StreamMux()
        a.open_stream(3)
        wire = a.send(3, b"x", fin=True)
        b.feed(wire)
        assert b.stream_state(3) == StreamState.HALF_CLOSED
        rst = a.close_stream(3, reset=True)
        b.feed(rst)
        assert b.stream_state(3) == StreamState.CLOSED

    def test_fragmented_feed(self):
        a = StreamMux()
        b = StreamMux()
        a.open_stream(1)
        wire = a.send(1, b"chunked-payload-data")
        mid = len(wire) // 2
        assert b.feed(wire[:mid]) == []
        frames = b.feed(wire[mid:])
        assert len(frames) == 1
        assert b.recv(1) == b"chunked-payload-data"


class TestConnectionPool:
    def test_reuse_same_peer(self):
        pool = ConnectionPool(2)
        s1 = pool.acquire("p1")
        cid = s1.conn_id
        pool.release(cid)
        s2 = pool.acquire("p1")
        assert s2.conn_id == cid
        pool.release(s2.conn_id)

    def test_exhausted(self):
        pool = ConnectionPool(1)
        pool.acquire("a")
        with pytest.raises(RuntimeError, match="exhausted"):
            pool.acquire("b")

    def test_stats(self):
        pool = ConnectionPool(4)
        s = pool.acquire("x")
        pool.record_io(s.conn_id, bytes_in=10, bytes_out=20)
        assert pool.stats()["in_use"] == 1
        pool.release(s.conn_id)
        assert pool.stats()["free"] == 1


class TestProxyAndSNI:
    def test_header_filter(self):
        f = HeaderFilter(allow_stream_ids=[1], deny_flags=FrameFlags.RST)
        ok = FrameCodec.decode(FrameCodec.encode(1, b"a"))
        deny_id = FrameCodec.decode(FrameCodec.encode(2, b"a"))
        deny_flag = FrameCodec.decode(FrameCodec.encode(1, b"", flags=FrameFlags.RST))
        assert f.accept(ok)
        assert not f.accept(deny_id)
        assert not f.accept(deny_flag)
        assert f.filter_batch([ok, deny_id]) == [ok]

    def test_wire_proxy_forwards(self):
        proxy = WireProxy(filter=HeaderFilter(allow_stream_ids=[1, 5]))
        proxy.route_stream(1, "backend-a:9000")
        proxy.mux.open_stream(1)
        frames = proxy.handle_wire(FrameCodec.encode(1, b"payload", flags=FrameFlags.SYN))
        assert len(frames) == 1
        assert proxy.forwarded
        peer, blob = proxy.forwarded[0]
        assert peer == "backend-a:9000"
        assert FrameCodec.decode(blob).payload.tobytes() == b"payload"

    def test_peek_tls_sni_extracts_hostname(self):
        # Build a minimal ClientHello with SNI extension = example.com
        host = b"example.com"
        # server_name extension body: list_len + name_type + name_len + host
        sni_entry = b"\x00" + struct.pack("!H", len(host)) + host
        sni_list = struct.pack("!H", len(sni_entry)) + sni_entry
        sni_ext = struct.pack("!HH", 0x0000, len(sni_list)) + sni_list

        # client hello body
        body = b"\x03\x03" + (b"\x11" * 32)  # version + random
        body += b"\x00"  # session id len 0
        body += b"\x00\x02\x00\x2f"  # cipher suites
        body += b"\x01\x00"  # compression
        body += struct.pack("!H", len(sni_ext)) + sni_ext

        hs = b"\x01" + struct.pack("!I", len(body))[1:] + body
        record = b"\x16\x03\x01" + struct.pack("!H", len(hs)) + hs
        assert peek_tls_sni(record) == "example.com"

    def test_peek_tls_sni_incomplete(self):
        assert peek_tls_sni(b"\x16\x03\x01") is None
        assert peek_tls_sni(b"\x17\x03\x01\x00\x01\x00") is None


class TestSmoke:
    def test_smoke(self):
        result = smoke()
        assert result["all_passed"] is True
        assert result["frames"] == 1
        assert result["proxy_accepted"] == 1
