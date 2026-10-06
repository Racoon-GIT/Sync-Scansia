"""Test fixtures for the register: one API over the in-memory fake (always) and,
from §4.3, ``racoon_staging`` (never ``racoon``).

A *fixture backend* exposes:

* ``store()``    — a fresh app-side store handle (what production builds);
* ``register()`` — a :class:`~backend.persistence.register.Register` on it;
* ``advance(ms)`` — move the SERVER clock forward (memory: the fake clock;
  MySQL: a real sleep — the server clock cannot be moved);
* DML standing for the register's OTHER writers (STOCK_Manager, the SPs):
  ``add_pair``, ``add_event``, ``add_shelf_pair``, ``consume``,
  ``leave_shelf``, ``seed_audit``.
"""
from __future__ import annotations

from typing import Any, Optional

from backend.persistence.memory_store import FakeClock, MemoryDB
from backend.persistence.records import (
    DONE,
    KIND_BASELINE,
    SCOPE_REGISTER,
    InsertAudit,
)
from backend.persistence.register import Register

PROMO = "gid://shopify/Location/PROMO"


class MemoryBackend:
    """The in-memory fake behind the fixture API."""

    name = "memory"

    def __init__(self) -> None:
        self.clock = FakeClock()
        self.db = MemoryDB(clock=self.clock)

    def store(self) -> Any:
        return self.db.connect()

    def register(self, actor: str = "test") -> Register:
        return Register(self.store(), actor=actor)

    def advance(self, ms: int) -> None:
        self.clock.advance(milliseconds=ms)

    # DML of the other writers
    def add_pair(self, sku: str, size: str, **kw: Any) -> int:
        return self.db.add_pair(sku, size, **kw)

    def add_event(self, pair_id: Optional[int], delta: int, **kw: Any) -> int:
        return self.db.add_event(pair_id, delta, **kw)

    def add_shelf_pair(self, sku: str, size: str, **kw: Any):
        return self.db.add_shelf_pair(sku, size, **kw)

    def consume(self, sku: str, size: str, *, outlet_line: bool = False) -> int:
        return self.db.consume(sku, size, outlet_line=outlet_line)

    def leave_shelf(self, pair_id: int, to_state: str, **kw: Any) -> int:
        return self.db.leave_shelf(pair_id, to_state, **kw)

    def seed_audit(self, row: InsertAudit, *, generation: Optional[int] = None) -> int:
        return self.db.seed_audit(row, generation=generation)

    def seed_baseline(self, *, held=(), stamped=()) -> int:
        """A whole-register BASELINE row as 'Inizializza' leaves it."""
        return self.seed_audit(InsertAudit(
            kind=KIND_BASELINE, actor="test", action="baseline",
            payload={"scope": SCOPE_REGISTER, "held": list(held), "stamped": list(stamped)},
        ))


class StubLease:
    """A held lease without the client rules: fence + renew hook (for store /
    register tests that are not about the lease client)."""

    def __init__(self, register: Register, holder: str = "test-holder", ttl_ms: int = 300_000) -> None:
        from backend.persistence.records import Fence

        gen = register.lease_take(holder, ttl_ms)
        assert gen is not None, "lease busy in fixture"
        self._fence = Fence(holder, gen, ttl_ms)
        self.renewals = 0

    @property
    def fence(self):
        return self._fence

    def monotonic(self) -> float:
        import time

        return time.monotonic()

    def mark_renewed(self, started: float) -> None:
        self.renewals += 1


def stamp_carrier(event_ids, keys=(), action="stamp-only") -> InsertAudit:
    """A RECONCILE row closed DONE that carries a stamp with no Shopify write."""
    from backend.persistence.records import KIND_RECONCILE

    return InsertAudit(
        kind=KIND_RECONCILE, actor="test", action=action, status=DONE,
        payload={"event_ids": sorted(event_ids), "keys": [list(k) for k in keys]},
    )


__all__ = ["MemoryBackend", "StubLease", "stamp_carrier", "PROMO"]
