"""In-memory :class:`~backend.persistence.ports.RegisterStore` — the test fake.

Never wired in production (``backend.app`` builds the MySQL store). It models the
MySQL semantics the plan relies on, so a service test against it proves what the
same code does against ``racoon``; the store contract tests run the same cases
on both (``tests/test_register_store.py``, MySQL on ``racoon_staging`` only):

* one :class:`MemoryDB` = one server, shared by every handle (connection);
* a fenced write is ONE atomic transaction: the guarded renew, then the ops in
  order — any failure rolls the whole of it back (nothing of it is visible);
* every guarded statement counts rows MATCHED (``CLIENT_FOUND_ROWS``), and a
  short count rolls back (:class:`~backend.persistence.records.GuardMismatch`);
* time is the server's clock (``UTC_TIMESTAMP(3)``): an injectable, naive-UTC
  :class:`FakeClock`, one instant per statement batch;
* a JSON column comes back with its object keys re-ordered the way MySQL
  normalises them (by key length, then bytes) — which is why a request is
  re-sent from ``REQUEST``, never rebuilt from ``PAYLOAD`` (plan §3.3 rule 2).

The fixture helpers (``add_pair``, ``add_event``, ``consume``, …) stand for the
register's OTHER writers — STOCK_Manager and the two SPs — and are deliberately
outside the store surface the app uses.
"""
from __future__ import annotations

import copy
import json
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from backend.persistence.records import (
    EVENT_CONSUME_MISS,
    KIND_RECONCILE,
    SHELF,
    TERMINAL,
    AuditRow,
    DuplicateKey,
    EventRow,
    Fence,
    GuardMismatch,
    InsertAudit,
    LeaseLost,
    LeaseRow,
    PairRow,
    Snapshot,
    StampEvents,
    StoreClosed,
    UpdateAudit,
    UpdatePairs,
    WriteOp,
    check_op_order,
    resolve_inserted,
)


class SimulatedCrash(RuntimeError):
    """Raised by a test's crash hook inside a transaction (the process dies there)."""


class FakeClock:
    """The server clock: naive UTC, advanced by the test."""

    def __init__(self, start: Optional[datetime] = None) -> None:
        self.now = start or datetime(2026, 10, 6, 12, 0, 0)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **delta: float) -> datetime:
        self.now = self.now + timedelta(**delta)
        return self.now


def _mysql_json(value: Any) -> Any:
    """Round-trip through JSON as a MySQL JSON column does: strings for what JSON
    cannot hold (Decimal), object keys re-ordered by (length, bytes)."""
    raw = json.loads(json.dumps(value, default=str))

    def norm(v: Any) -> Any:
        if isinstance(v, dict):
            return {k: norm(v[k]) for k in sorted(v, key=lambda s: (len(s), s.encode("utf-8")))}
        if isinstance(v, list):
            return [norm(x) for x in v]
        return v

    return norm(raw)


def _ms(ms: int) -> timedelta:
    return timedelta(milliseconds=ms)


@dataclass
class _State:
    pairs: Dict[int, Dict[str, Any]]
    events: Dict[int, Dict[str, Any]]
    audit: Dict[int, Dict[str, Any]]
    lease: Dict[str, Any]
    next_ids: Dict[str, int]


class MemoryDB:
    """One in-memory 'server': the register tables, the audit table, the lease."""

    def __init__(self, clock: Optional[Callable[[], datetime]] = None) -> None:
        self.clock: Callable[[], datetime] = clock or FakeClock()
        self.lock = threading.RLock()
        self.state = _State(
            pairs={},
            events={},
            audit={},
            lease={
                "HOLDER": None, "GENERATION": 0, "EXPIRES_AT": None,
                "WAITER": None, "WAITER_EXPIRES_AT": None, "UPDATED_AT": self.clock(),
            },
            next_ids={"pair": 1, "event": 1, "audit": 1},
        )
        # Every statement the app ran, in order (tests assert shapes and order).
        self.log: List[Tuple[Any, ...]] = []
        # Called before each op of a fenced write: (op_index, op) -> may raise.
        self.crash_hook: Optional[Callable[[int, WriteOp], None]] = None

    def connect(self) -> "MemoryStore":
        return MemoryStore(self)

    # ------------------------------------------------------------------
    # Fixture DML — the register's OTHER writers (STOCK_Manager, the SPs)
    # ------------------------------------------------------------------
    def _next(self, name: str) -> int:
        n = self.state.next_ids[name]
        self.state.next_ids[name] = n + 1
        return n

    def add_pair(
        self,
        sku: str,
        size: str,
        *,
        state: str = SHELF,
        price_high: Optional[Any] = None,
        price: Optional[Any] = None,
        price_outlet: Optional[Any] = None,
        discount: Optional[Any] = None,
        outlet_product_id: Optional[int] = None,
        online: bool = False,
        product_title: Optional[str] = None,
        brand: Optional[str] = None,
        pair_uuid: Optional[str] = None,
    ) -> int:
        with self.lock:
            pid = self._next("pair")
            now = self.clock()
            self.state.pairs[pid] = {
                "ID": pid,
                "PAIR_UUID": pair_uuid or str(uuid.UUID(int=pid)),
                "SKU": sku,
                "SIZE": size,
                "STATE": state,
                "PRICE_HIGH": _dec(price_high),
                "PRICE": _dec(price),
                "PRICE_OUTLET": _dec(price_outlet),
                "DISCOUNT": _dec(discount),
                "OUTLET_PRODUCT_ID": outlet_product_id,
                "ONLINE": 1 if online else 0,
                "PRODUCT_TITLE": product_title,
                "BRAND": brand,
                "SELLABLE_AT": now if state == SHELF else None,
                "CREATED_AT": now,
                "UPDATED_AT": now,
            }
            return pid

    def add_event(
        self,
        pair_id: Optional[int],
        delta: int,
        *,
        event_type: str = "IMPORTED",
        actor: str = "IMPORT",
        reconciled: bool = False,
        payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        with self.lock:
            eid = self._next("event")
            now = self.clock()
            self.state.events[eid] = {
                "ID": eid,
                "PAIR_ID": pair_id,
                "EVENT_TYPE": event_type,
                "ACTOR": actor,
                "OUTLET_DELTA": int(delta),
                "OUTLET_RECONCILED_AT": now if reconciled else None,
                "CREATED_AT": now,
                "PAYLOAD": payload,
            }
            return eid

    def add_shelf_pair(
        self,
        sku: str,
        size: str,
        *,
        event_type: str = "ARRIVE",
        actor: str = "GATE",
        reconciled: bool = False,
        **pair_kw: Any,
    ) -> Tuple[int, int]:
        """A pair entering ``IN_SCANSIA`` with its +1 event (design §3.5). Returns
        ``(pair_id, event_id)``."""
        pid = self.add_pair(sku, size, state=SHELF, **pair_kw)
        eid = self.add_event(pid, +1, event_type=event_type, actor=actor, reconciled=reconciled)
        return pid, eid

    def leave_shelf(
        self, pair_id: int, to_state: str, *, event_type: str, actor: str = "OPERATOR", delta: int = -1
    ) -> int:
        """A transition out of ``IN_SCANSIA`` (X4/X5/X8) with its event."""
        with self.lock:
            p = self.state.pairs[pair_id]
            if p["STATE"] != SHELF:
                raise ValueError(f"pair {pair_id} is not on the shelf")
            p["STATE"] = to_state
            p["UPDATED_AT"] = self.clock()
            return self.add_event(pair_id, delta, event_type=event_type, actor=actor)

    def consume(self, sku: str, size: str, *, outlet_line: bool = False) -> int:
        """``SP_SCANSIA_CONSUME``'s effect: the oldest shelf pair of the key goes
        ``VENDUTO`` (``ONLINE`` 0) with a ``CONSUME`` event, Δ −1 — 0 on an outlet
        line; no pair → a keyless ``CONSUME_MISS`` (Δ 0). Returns the event id."""
        with self.lock:
            cands = sorted(
                (p for p in self.state.pairs.values()
                 if p["STATE"] == SHELF and p["SKU"] == sku and p["SIZE"] == size),
                key=lambda p: (p["SELLABLE_AT"] or datetime.min, p["ID"]),
            )
            if not cands:
                return self.add_event(None, 0, event_type=EVENT_CONSUME_MISS, actor="SP")
            p = cands[0]
            now = self.clock()
            p["STATE"] = "VENDUTO"
            p["ONLINE"] = 0
            p["UPDATED_AT"] = now
            return self.add_event(p["ID"], 0 if outlet_line else -1, event_type="CONSUME", actor="SP")

    def seed_audit(self, row: InsertAudit, *, generation: Optional[int] = None) -> int:
        """Insert an audit row as if an earlier run had written it."""
        with self.lock:
            return self._insert_audit(row, self.clock(), generation)

    # ------------------------------------------------------------------
    # Internals shared by the handles
    # ------------------------------------------------------------------
    def _snapshot(self) -> _State:
        return copy.deepcopy(self.state)

    def _restore(self, snap: _State) -> None:
        self.state = snap

    def _insert_audit(self, row: InsertAudit, now: datetime, generation: Optional[int]) -> int:
        if row.intent_id is not None and any(
            a["INTENT_ID"] == row.intent_id for a in self.state.audit.values()
        ):
            raise DuplicateKey("uk_sma_intent")
        aid = self._next("audit")
        self.state.audit[aid] = {
            "ID": aid,
            "KIND": row.kind,
            "ACTOR": row.actor,
            "ACTION": row.action,
            "PLAN_HASH": row.plan_hash,
            "INTENT_ID": row.intent_id,
            "LEASE_GEN": generation if row.kind == KIND_RECONCILE else None,
            "SENT_AT": None,
            "SENT_AVAILABLE": None,
            "IDEMPOTENCY_KEY": row.idempotency_key,
            "PRODUCT_ID": row.product_id,
            "STEP": row.step,
            "GROUP_ID": row.group_id,
            "REQUEST": None,
            "STATUS": row.status,
            "OUTCOME": _mysql_json(dict(row.outcome)) if row.outcome is not None else None,
            "PAYLOAD": _mysql_json(dict(row.payload)) if row.payload is not None else None,
            "CREATED_AT": now,
            "CLOSED_AT": now if row.status in TERMINAL else None,
        }
        return aid


def _dec(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    return Decimal(str(v)).quantize(Decimal("0.01"))


class MemoryStore:
    """One handle (connection) onto a :class:`MemoryDB`."""

    def __init__(self, db: MemoryDB) -> None:
        self.db = db
        self._closed = False

    # -- lifecycle --------------------------------------------------------
    def close(self) -> None:
        self._closed = True

    def _open(self) -> MemoryDB:
        if self._closed:
            raise StoreClosed("store handle used after close()")
        return self.db

    # -- reads --------------------------------------------------------------
    def read_pairs(
        self,
        *,
        states: Optional[Sequence[str]] = (SHELF,),
        sku: Optional[str] = None,
        pair_ids: Optional[Sequence[int]] = None,
        outlet_product_id: Optional[int] = None,
    ) -> List[PairRow]:
        db = self._open()
        with db.lock:
            db.log.append(("read_pairs",))
            ids = set(pair_ids) if pair_ids is not None else None
            out = []
            for p in sorted(db.state.pairs.values(), key=lambda r: r["ID"]):
                if states is not None and p["STATE"] not in states:
                    continue
                if sku is not None and p["SKU"] != sku:
                    continue
                if ids is not None and p["ID"] not in ids:
                    continue
                if outlet_product_id is not None and p["OUTLET_PRODUCT_ID"] != outlet_product_id:
                    continue
                out.append(_pair_row(p))
            return out

    def read_unstamped_events(self) -> List[EventRow]:
        db = self._open()
        with db.lock:
            db.log.append(("read_unstamped_events",))
            out = []
            for e in sorted(db.state.events.values(), key=lambda r: r["ID"]):
                if e["OUTLET_RECONCILED_AT"] is not None:
                    continue
                p = db.state.pairs.get(e["PAIR_ID"]) if e["PAIR_ID"] is not None else None
                out.append(_event_row(e, p))
            return out

    def read_audit(
        self,
        kind: str,
        *,
        statuses: Optional[Sequence[str]] = None,
        row_ids: Optional[Sequence[int]] = None,
        group_id: Optional[int] = None,
        product_id: Optional[int] = None,
        intent_id: Optional[str] = None,
        last: Optional[int] = None,
    ) -> List[AuditRow]:
        db = self._open()
        with db.lock:
            db.log.append(("read_audit", kind))
            ids = set(row_ids) if row_ids is not None else None
            out = []
            for a in sorted(db.state.audit.values(), key=lambda r: r["ID"]):
                if a["KIND"] != kind:
                    continue
                if statuses is not None and a["STATUS"] not in statuses:
                    continue
                if ids is not None and a["ID"] not in ids:
                    continue
                if group_id is not None and a["GROUP_ID"] != group_id:
                    continue
                if product_id is not None and a["PRODUCT_ID"] != product_id:
                    continue
                if intent_id is not None and a["INTENT_ID"] != intent_id:
                    continue
                out.append(_audit_row(a))
            if last is not None:
                out = out[-last:] if last > 0 else []
            return out

    def read_lease(self) -> LeaseRow:
        db = self._open()
        with db.lock:
            L = db.state.lease
            return LeaseRow(
                holder=L["HOLDER"], generation=L["GENERATION"], expires_at=L["EXPIRES_AT"],
                waiter=L["WAITER"], waiter_expires_at=L["WAITER_EXPIRES_AT"],
                updated_at=L["UPDATED_AT"],
            )

    def read_snapshot(self, key: Optional[Tuple[str, str]] = None) -> "Snapshot":
        db = self._open()
        with db.lock:
            db.log.append(("snapshot",))
            pairs = tuple(_pair_row(p) for p in sorted(db.state.pairs.values(), key=lambda r: r["ID"])
                          if p["STATE"] == SHELF and (key is None or (p["SKU"], p["SIZE"]) == tuple(key)))
            unstamped = []
            maxima: Dict[Tuple[str, str], int] = {}
            for e in sorted(db.state.events.values(), key=lambda r: r["ID"]):
                p = db.state.pairs.get(e["PAIR_ID"]) if e["PAIR_ID"] is not None else None
                k = (p["SKU"], p["SIZE"]) if p else None
                if key is not None and k != tuple(key):
                    continue
                if k is not None:
                    maxima[k] = max(maxima.get(k, 0), e["ID"])
                if e["OUTLET_RECONCILED_AT"] is None:
                    unstamped.append(_event_row(e, p))
            return Snapshot(pairs=pairs, unstamped=tuple(unstamped), max_event_id=maxima)

    def read_import_payloads(self, pair_ids: Sequence[int]) -> List[Tuple[int, Any]]:
        db = self._open()
        with db.lock:
            ids = set(pair_ids)
            return [(e["PAIR_ID"], _mysql_json(e.get("PAYLOAD")) if e.get("PAYLOAD") is not None else None)
                    for e in sorted(db.state.events.values(), key=lambda r: r["ID"])
                    if e["PAIR_ID"] in ids and e["EVENT_TYPE"] == "IMPORTED"]

    def server_now(self) -> datetime:
        return self._open().clock()

    # -- the lease (one guarded statement each, autocommit) -------------------
    def lease_take(self, holder: str, ttl_ms: int) -> Optional[int]:
        db = self._open()
        with db.lock:
            now = db.clock()
            L = db.state.lease
            db.log.append(("lease_take", holder))
            free = L["HOLDER"] is None or (L["EXPIRES_AT"] is not None and L["EXPIRES_AT"] < now)
            not_jumped = (
                L["WAITER"] is None
                or (L["WAITER_EXPIRES_AT"] is not None and L["WAITER_EXPIRES_AT"] < now)
                or L["WAITER"] == holder
            )
            if not (free and not_jumped):
                return None
            L["HOLDER"] = holder
            L["GENERATION"] = L["GENERATION"] + 1
            L["EXPIRES_AT"] = now + _ms(ttl_ms)
            if L["WAITER"] == holder:
                L["WAITER"] = None
            L["UPDATED_AT"] = now
            return L["GENERATION"]

    def lease_release(self, fence: Fence) -> bool:
        db = self._open()
        with db.lock:
            L = db.state.lease
            db.log.append(("lease_release", fence.holder, fence.generation))
            if L["HOLDER"] == fence.holder and L["GENERATION"] == fence.generation:
                L["HOLDER"] = None
                L["EXPIRES_AT"] = None
                L["UPDATED_AT"] = db.clock()
                return True
            return False

    def lease_wait(self, holder: str, wait_ms: int) -> bool:
        db = self._open()
        with db.lock:
            now = db.clock()
            L = db.state.lease
            db.log.append(("lease_wait", holder))
            if (
                L["WAITER"] is None
                or (L["WAITER_EXPIRES_AT"] is not None and L["WAITER_EXPIRES_AT"] < now)
                or L["WAITER"] == holder
            ):
                L["WAITER"] = holder
                L["WAITER_EXPIRES_AT"] = now + _ms(wait_ms)
                L["UPDATED_AT"] = now
                return True
            return False

    # -- the one write primitive --------------------------------------------
    def fenced_write(self, fence: Fence, ops: Sequence[WriteOp]) -> List[int]:
        db = self._open()
        check_op_order(ops)
        with db.lock:
            snap = db._snapshot()
            try:
                now = db.clock()
                L = db.state.lease
                if not (
                    L["HOLDER"] == fence.holder
                    and L["GENERATION"] == fence.generation
                    and L["EXPIRES_AT"] is not None
                    and L["EXPIRES_AT"] > now
                ):
                    raise LeaseLost("renew matched no row")
                L["EXPIRES_AT"] = now + _ms(fence.ttl_ms)
                L["UPDATED_AT"] = now
                db.log.append(("renew", fence.holder, fence.generation))
                inserted: List[int] = []
                for i, op in enumerate(ops):
                    if db.crash_hook is not None:
                        db.crash_hook(i, op)
                    new_id = self._apply(db, i, op, now, fence, inserted)
                    if new_id is not None:
                        inserted.append(new_id)
                db.log.append(("commit",))
                return inserted
            except BaseException:
                db._restore(snap)
                db.log.append(("rollback",))
                raise

    def _apply(
        self, db: MemoryDB, i: int, op: WriteOp, now: datetime, fence: Fence, inserted: Sequence[int] = ()
    ) -> Optional[int]:
        st = db.state
        if isinstance(op, InsertAudit):
            db.log.append(("insert_audit", op.kind, op.status))
            return db._insert_audit(op, now, fence.generation)
        if isinstance(op, UpdateAudit):
            db.log.append(("update_audit", op.row_id, op.status))
            a = st.audit.get(op.row_id)
            ok = (
                a is not None
                and a["KIND"] == KIND_RECONCILE
                and a["STATUS"] == op.expect_status
                and (not op.first_send or a["SENT_AT"] is None)
            )
            if not ok:
                raise GuardMismatch(i, 1, 0)
            if op.mark_sent:
                a["SENT_AT"] = now
            if op.first_send:
                a["SENT_AVAILABLE"] = op.sent_available
                a["REQUEST"] = op.request
            if op.product_id is not None:
                a["PRODUCT_ID"] = op.product_id
            if op.step is not None:
                a["STEP"] = op.step
            if op.outcome is not None:
                a["OUTCOME"] = _mysql_json(resolve_inserted(op.outcome, inserted))
            if op.status is not None:
                a["STATUS"] = op.status
                if op.status in TERMINAL:
                    a["CLOSED_AT"] = now
            return None
        if isinstance(op, StampEvents):
            ids = sorted(set(op.event_ids))
            db.log.append(("stamp", tuple(ids)))
            matched = [st.events[e] for e in ids if e in st.events]
            if len(matched) != len(ids):
                raise GuardMismatch(i, len(ids), len(matched))
            for e in matched:
                if e["OUTLET_RECONCILED_AT"] is None:
                    e["OUTLET_RECONCILED_AT"] = now
            return None
        if isinstance(op, UpdatePairs):
            ids = sorted(set(op.pair_ids))
            db.log.append(("update_pairs", tuple(ids), tuple(sorted(op.values))))
            matched = []
            for pid in ids:
                p = st.pairs.get(pid)
                if p is None:
                    continue
                if op.require_state is not None and p["STATE"] != op.require_state:
                    continue
                if (
                    op.require_outlet_product_id is not None
                    and p["OUTLET_PRODUCT_ID"] != op.require_outlet_product_id
                ):
                    continue
                matched.append(p)
            if len(matched) != len(ids):
                raise GuardMismatch(i, len(ids), len(matched))
            for p in matched:
                for col, val in op.values.items():
                    if col == "ONLINE":
                        p[col] = 1 if val else 0
                    elif col in ("PRICE_OUTLET", "DISCOUNT"):
                        p[col] = _dec(val)
                    else:
                        p[col] = val
                p["UPDATED_AT"] = now
            return None
        raise TypeError(f"unknown op {type(op).__name__}")


def _pair_row(p: Dict[str, Any]) -> PairRow:
    return PairRow(
        pair_id=p["ID"], pair_uuid=p["PAIR_UUID"], sku=p["SKU"], size=p["SIZE"],
        state=p["STATE"], price_high=p["PRICE_HIGH"], price=p["PRICE"],
        price_outlet=p["PRICE_OUTLET"], discount=p["DISCOUNT"],
        outlet_product_id=p["OUTLET_PRODUCT_ID"], online=bool(p["ONLINE"]),
        updated_at=p["UPDATED_AT"], product_title=p["PRODUCT_TITLE"], brand=p.get("BRAND"),
    )


def _event_row(e: Dict[str, Any], p: Optional[Dict[str, Any]]) -> EventRow:
    return EventRow(
        event_id=e["ID"], pair_id=e["PAIR_ID"], event_type=e["EVENT_TYPE"], actor=e["ACTOR"],
        delta=e["OUTLET_DELTA"], created_at=e["CREATED_AT"], reconciled_at=e["OUTLET_RECONCILED_AT"],
        sku=p["SKU"] if p else None, size=p["SIZE"] if p else None,
        pair_state=p["STATE"] if p else None,
    )


def _audit_row(a: Dict[str, Any]) -> AuditRow:
    return AuditRow(
        row_id=a["ID"], kind=a["KIND"], actor=a["ACTOR"], action=a["ACTION"],
        created_at=a["CREATED_AT"], plan_hash=a["PLAN_HASH"], intent_id=a["INTENT_ID"],
        lease_gen=a["LEASE_GEN"], sent_at=a["SENT_AT"], sent_available=a["SENT_AVAILABLE"],
        idempotency_key=a["IDEMPOTENCY_KEY"], product_id=a["PRODUCT_ID"], step=a["STEP"],
        group_id=a["GROUP_ID"], request=a["REQUEST"], status=a["STATUS"],
        outcome=copy.deepcopy(a["OUTCOME"]), payload=copy.deepcopy(a["PAYLOAD"]),
        closed_at=a["CLOSED_AT"],
    )


__all__ = ["MemoryDB", "MemoryStore", "FakeClock", "DuplicateKey", "StoreClosed", "SimulatedCrash"]
