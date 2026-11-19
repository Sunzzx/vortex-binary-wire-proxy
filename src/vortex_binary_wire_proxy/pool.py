"""Bounded connection slot pool with LIFO reuse (ring of free ids)."""

from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic
from typing import Dict, List, Optional


@dataclass
class ConnSlot:
    conn_id: int
    peer: str
    created_at: float = field(default_factory=monotonic)
    last_used: float = field(default_factory=monotonic)
    in_use: bool = False
    bytes_in: int = 0
    bytes_out: int = 0


class ConnectionPool:
    __slots__ = ("_max", "_slots", "_free", "_next_id", "_by_peer")

    def __init__(self, max_connections: int = 128) -> None:
        if max_connections < 1:
            raise ValueError("max_connections must be >= 1")
        self._max = max_connections
        self._slots: Dict[int, ConnSlot] = {}
        self._free: List[int] = []
        self._next_id = 1
        self._by_peer: Dict[str, List[int]] = {}

    @property
    def size(self) -> int:
        return len(self._slots)

    @property
    def in_use_count(self) -> int:
        return sum(1 for s in self._slots.values() if s.in_use)

    def acquire(self, peer: str) -> ConnSlot:
        # Prefer free slot already bound to peer
        for cid in list(self._by_peer.get(peer, [])):
            slot = self._slots.get(cid)
            if slot and not slot.in_use:
                slot.in_use = True
                slot.last_used = monotonic()
                if cid in self._free:
                    self._free.remove(cid)
                return slot
        # Any free slot
        while self._free:
            cid = self._free.pop()
            slot = self._slots.get(cid)
            if slot and not slot.in_use:
                # rebind peer
                self._unbind_peer(slot)
                slot.peer = peer
                slot.in_use = True
                slot.last_used = monotonic()
                self._by_peer.setdefault(peer, []).append(cid)
                return slot
        if len(self._slots) >= self._max:
            raise RuntimeError("connection pool exhausted")
        cid = self._next_id
        self._next_id += 1
        slot = ConnSlot(conn_id=cid, peer=peer, in_use=True)
        self._slots[cid] = slot
        self._by_peer.setdefault(peer, []).append(cid)
        return slot

    def release(self, conn_id: int) -> None:
        slot = self._slots.get(conn_id)
        if slot is None:
            raise KeyError(conn_id)
        slot.in_use = False
        slot.last_used = monotonic()
        if conn_id not in self._free:
            self._free.append(conn_id)

    def record_io(self, conn_id: int, *, bytes_in: int = 0, bytes_out: int = 0) -> None:
        slot = self._slots[conn_id]
        slot.bytes_in += bytes_in
        slot.bytes_out += bytes_out
        slot.last_used = monotonic()

    def drop(self, conn_id: int) -> None:
        slot = self._slots.pop(conn_id, None)
        if slot is None:
            return
        self._unbind_peer(slot)
        if conn_id in self._free:
            self._free.remove(conn_id)

    def stats(self) -> dict:
        return {
            "size": self.size,
            "in_use": self.in_use_count,
            "free": len(self._free),
            "max": self._max,
        }

    def _unbind_peer(self, slot: ConnSlot) -> None:
        peers = self._by_peer.get(slot.peer)
        if not peers:
            return
        if slot.conn_id in peers:
            peers.remove(slot.conn_id)
        if not peers:
            del self._by_peer[slot.peer]
