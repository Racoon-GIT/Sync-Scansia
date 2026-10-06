"""Register and audit records — the read models and the fenced-write algebra.

Rows as the register tables hold them (STOCK_Manager's DDL at ``92f6f90``:
``sc_returned_pair``, ``sc_returned_pair_event``) and as SERVER's F7.1 DDL holds
F7's own two tables (``sm_outlet_audit``, ``sm_outlet_lease`` —
``IT/SERVER/_deliverables/scansia-f7-2026-10.sql`` A08/A09), plus the small
algebra of writes one fenced transaction may carry (plan §3.3 rule 6, §3.7
rule 1). Pure data: no I/O, no driver import.

A fenced transaction is: the guarded renew of the writer's own lease, then the
ops in order, then ``COMMIT`` — at most ONE register op (``StampEvents`` /
``UpdatePairs``), and it is the last (§3.7 rule 1: "the register ``UPDATE`` is the
last statement before ``COMMIT``"). Every guarded statement must match exactly
the rows it names (``CLIENT_FOUND_ROWS``: matched, not changed — §3.7 rule 6),
else the whole transaction rolls back (:class:`GuardMismatch`).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, FrozenSet, Mapping, Optional, Sequence, Tuple, Union

# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------
SHELF = "IN_SCANSIA"  # the only sellable state (design §3.1)

# ``sc_returned_pair_event.ACTOR`` of the import births / re-bases (design §3.2, §3.3 X10).
ACTOR_IMPORT = "IMPORT"
# The only event with ``PAIR_ID`` NULL (design §2.2 at 92f6f90; SP spec step 4: Δ always 0).
EVENT_CONSUME_MISS = "CONSUME_MISS"

# ``sm_outlet_audit.KIND`` (SERVER A08 column comment).
KIND_AUDIT = "AUDIT"
KIND_DELETE = "DELETE"
KIND_PRICE = "PRICE"
KIND_REGISTER = "REGISTER"
KIND_INIT_BEFORE = "INIT_BEFORE"
KIND_BASELINE = "BASELINE"
KIND_RECONCILE = "RECONCILE"
KIND_HOLD = "HOLD"
KINDS = frozenset({
    KIND_AUDIT, KIND_DELETE, KIND_PRICE, KIND_REGISTER, KIND_INIT_BEFORE,
    KIND_BASELINE, KIND_RECONCILE, KIND_HOLD,
})

# ``sm_outlet_audit.STATUS`` of a RECONCILE row (plan §3.6); NULL for every other kind.
OPEN = "OPEN"
APPLIED = "APPLIED"
DONE = "DONE"
CARRIED = "CARRIED"
FAILED = "FAILED"
HELD = "HELD"
SUPERSEDED = "SUPERSEDED"
STATUSES = frozenset({OPEN, APPLIED, DONE, CARRIED, FAILED, HELD, SUPERSEDED})
NOT_CLOSED = frozenset({OPEN, APPLIED})          # the in-flight hold (§3.1 held_keys())
TERMINAL = frozenset({DONE, CARRIED, FAILED, HELD, SUPERSEDED})  # CLOSED_AT is set on these

# BASELINE scopes (lane's PAYLOAD convention).
SCOPE_REGISTER = "register"   # the whole-register baseline (§3.4 Part A) — one, ever
SCOPE_KEY = "key"             # a per-key re-level or the N-0 release: releases the key

# Hold kinds (§3.1 held_keys(): "its kind — baseline, recovery HELD, HOLD, in flight").
HOLD_BASELINE = "baseline"
HOLD_RECOVERY = "recovery"
HOLD_ROW = "hold"
HOLD_IN_FLIGHT = "in_flight"

# The register columns F7 may write (the grant of §3.6), besides UPDATED_AT.
PAIR_WRITABLE = frozenset({"PRICE_OUTLET", "DISCOUNT", "OUTLET_PRODUCT_ID", "ONLINE"})

# One stamp statement carries at most this many ids (the baseline's ~700 fit; §3.1 "batched").
MAX_STAMP_IDS = 5000

Key = Tuple[str, str]  # (SKU, SIZE), exact (utf8mb4_bin) — plan §3.2


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PairRow:
    """The ``sc_returned_pair`` columns F7 reads (one row = one physical pair)."""

    pair_id: int
    pair_uuid: str
    sku: str
    size: str
    state: str
    price_high: Optional[Decimal]
    price: Optional[Decimal]
    price_outlet: Optional[Decimal]
    discount: Optional[Decimal]
    outlet_product_id: Optional[int]
    online: bool
    updated_at: datetime
    product_title: Optional[str] = None

    @property
    def key(self) -> Key:
        return (self.sku, self.size)


@dataclass(frozen=True)
class EventRow:
    """One ``sc_returned_pair_event`` row joined (LEFT) to its pair's key and state.

    ``sku``/``size``/``pair_state`` are ``None`` only when ``pair_id`` is ``None``
    (``CONSUME_MISS``, the one keyless event).
    """

    event_id: int
    pair_id: Optional[int]
    event_type: str
    actor: str
    delta: int
    created_at: datetime
    reconciled_at: Optional[datetime]
    sku: Optional[str]
    size: Optional[str]
    pair_state: Optional[str]

    @property
    def key(self) -> Optional[Key]:
        if self.sku is None or self.size is None:
            return None
        return (self.sku, self.size)


@dataclass(frozen=True)
class AuditRow:
    """One ``sm_outlet_audit`` row (SERVER A08)."""

    row_id: int
    kind: str
    actor: str
    action: str
    created_at: datetime
    plan_hash: Optional[str] = None
    intent_id: Optional[str] = None
    lease_gen: Optional[int] = None
    sent_at: Optional[datetime] = None
    sent_available: Optional[int] = None
    idempotency_key: Optional[str] = None
    product_id: Optional[int] = None
    step: Optional[str] = None
    group_id: Optional[int] = None
    request: Optional[str] = None
    status: Optional[str] = None
    outcome: Optional[Dict[str, Any]] = None
    payload: Optional[Dict[str, Any]] = None
    closed_at: Optional[datetime] = None

    # -- PAYLOAD conventions (the lane's; plan §3.6 leaves the JSON shape open) --
    @property
    def keys(self) -> Tuple[Key, ...]:
        """The (SKU, SIZE) keys the row serves (``PAYLOAD.keys``; a per-key
        BASELINE row's ``PAYLOAD.key``)."""
        p = self.payload or {}
        raw = p.get("keys")
        if raw is None and p.get("key") is not None:
            raw = [p["key"]]
        return tuple((str(k[0]), str(k[1])) for k in (raw or ()))

    @property
    def event_ids(self) -> FrozenSet[int]:
        return frozenset(int(i) for i in ((self.payload or {}).get("event_ids") or ()))

    @property
    def is_closed(self) -> bool:
        return self.status in TERMINAL


@dataclass(frozen=True)
class LeaseRow:
    """The one ``sm_outlet_lease`` row (``ID = 1``; SERVER A09)."""

    holder: Optional[str]
    generation: int
    expires_at: Optional[datetime]
    waiter: Optional[str]
    waiter_expires_at: Optional[datetime]
    updated_at: datetime


@dataclass(frozen=True)
class Fence:
    """What a fenced transaction renews first: holder + generation, for ``ttl_ms``."""

    holder: str
    generation: int
    ttl_ms: int


# ---------------------------------------------------------------------------
# The fenced-write algebra (one transaction = renew + these, in order)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class InsertAudit:
    """``INSERT INTO sm_outlet_audit`` — ``CREATED_AT`` is the server's
    ``UTC_TIMESTAMP(3)``; ``LEASE_GEN`` is the fence's generation for a
    ``RECONCILE`` row (rule 6: every row carries its writer's generation)."""

    kind: str
    actor: str
    action: str
    plan_hash: Optional[str] = None
    intent_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    product_id: Optional[int] = None
    step: Optional[str] = None
    group_id: Optional[int] = None
    status: Optional[str] = None
    payload: Optional[Mapping[str, Any]] = None
    outcome: Optional[Mapping[str, Any]] = None

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"unknown audit kind {self.kind!r}")
        if (self.kind == KIND_RECONCILE) != (self.status is not None):
            raise ValueError("STATUS is set on RECONCILE rows only, and always on them")
        if self.status is not None and self.status not in STATUSES:
            raise ValueError(f"unknown status {self.status!r}")


@dataclass(frozen=True)
class UpdateAudit:
    """Guarded ``UPDATE sm_outlet_audit … WHERE ID = ? AND STATUS = ?`` of a
    ``RECONCILE`` row — only the granted columns (SENT_AT, SENT_AVAILABLE,
    REQUEST, PRODUCT_ID, STEP, STATUS, OUTCOME, CLOSED_AT). ``None`` = unchanged.

    ``mark_sent`` sets ``SENT_AT = UTC_TIMESTAMP(3)``; with ``first_send`` the
    guard adds ``SENT_AT IS NULL`` and ``sent_available``/``request`` are written
    with it — once, never by a re-send (rule 2). ``CLOSED_AT`` is set when the
    new status is terminal.
    """

    row_id: int
    expect_status: str
    status: Optional[str] = None
    mark_sent: bool = False
    first_send: bool = False
    sent_available: Optional[int] = None
    request: Optional[str] = None
    product_id: Optional[int] = None
    step: Optional[str] = None
    outcome: Optional[Mapping[str, Any]] = None

    def __post_init__(self) -> None:
        if self.expect_status not in STATUSES:
            raise ValueError(f"unknown expected status {self.expect_status!r}")
        if self.status is not None and self.status not in STATUSES:
            raise ValueError(f"unknown status {self.status!r}")
        if self.first_send and not self.mark_sent:
            raise ValueError("first_send requires mark_sent")
        if (self.sent_available is not None or self.request is not None) and not self.first_send:
            raise ValueError("SENT_AVAILABLE / REQUEST are written by the first send only")
        if not (self.mark_sent or self.status is not None or self.product_id is not None
                or self.step is not None or self.outcome is not None):
            raise ValueError("an audit row update changes at least one column")


@dataclass(frozen=True)
class StampEvents:
    """``UPDATE sc_returned_pair_event SET OUTLET_RECONCILED_AT =
    COALESCE(OUTLET_RECONCILED_AT, UTC_TIMESTAMP(3)) WHERE ID IN (…)`` — by
    PRIMARY KEY only (§3.1 ``stamp``); matched must equal the distinct ids."""

    event_ids: Tuple[int, ...]

    def __post_init__(self) -> None:
        if not self.event_ids:
            raise ValueError("a stamp names at least one event id")
        if len(set(self.event_ids)) > MAX_STAMP_IDS:
            raise ValueError(f"a stamp carries at most {MAX_STAMP_IDS} ids")


@dataclass(frozen=True)
class UpdatePairs:
    """``UPDATE sc_returned_pair SET <cols>, UPDATED_AT = UTC_TIMESTAMP(3)
    WHERE ID IN (…) [AND STATE = ?] [AND OUTLET_PRODUCT_ID = ?]`` — by PRIMARY
    KEY after a plain SELECT (§3.7 rule 3); matched must equal the ids."""

    pair_ids: Tuple[int, ...]
    values: Mapping[str, Any]
    require_state: Optional[str] = None
    require_outlet_product_id: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.pair_ids:
            raise ValueError("a pair update names at least one pair id")
        bad = set(self.values) - PAIR_WRITABLE
        if bad or not self.values:
            raise ValueError(f"F7 writes only {sorted(PAIR_WRITABLE)} (got {sorted(self.values)})")


WriteOp = Union[InsertAudit, UpdateAudit, StampEvents, UpdatePairs]
REGISTER_OPS = (StampEvents, UpdatePairs)


def check_op_order(ops: Sequence[WriteOp]) -> None:
    """At most one register op per fenced transaction, and it is the last (§3.7 rule 1)."""
    idx = [i for i, op in enumerate(ops) if isinstance(op, REGISTER_OPS)]
    if len(idx) > 1:
        raise ValueError("a fenced transaction carries at most one register UPDATE")
    if idx and idx[0] != len(ops) - 1:
        raise ValueError("the register UPDATE is the last statement before COMMIT")


# ---------------------------------------------------------------------------
# Derived views
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Hold:
    """One held key (§3.1 ``held_keys()``) and the audit row that holds it."""

    key: Key
    kind: str
    reason: str
    event_ids: FrozenSet[int]
    row_id: int
    created_at: datetime
    status: Optional[str] = None
    action: Optional[str] = None
    product_id: Optional[int] = None
    sent_at: Optional[datetime] = None


@dataclass(frozen=True)
class PendingDeltas:
    """``pending_deltas()``: the events F7 may read now.

    ``events`` — unstamped, keyed, minus every held event (§3.1); ``keyless`` —
    unstamped events with no pair (``CONSUME_MISS``, Δ 0): surfaced, never
    grouped, never stamped; ``holds`` — the holds subtracted; ``carried`` —
    ``CARRIED`` rows over still-unstamped events (their moved share nets the
    remainder, §3.3 rule 2's Groups).
    """

    events: Tuple[EventRow, ...]
    keyless: Tuple[EventRow, ...] = ()
    holds: Tuple[Hold, ...] = ()
    carried: Tuple[AuditRow, ...] = ()

    @property
    def held_keys(self) -> FrozenSet[Key]:
        return frozenset(h.key for h in self.holds)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class StoreError(RuntimeError):
    """Base for register/audit store failures (bounded at the API boundary)."""


class LeaseLost(StoreError):
    """The guarded renew opening a fenced transaction matched no row: another
    holder took the lease, or it expired. The transaction rolled back; the
    holder stops (rule 6)."""


class GuardMismatch(StoreError):
    """A guarded statement matched fewer rows than it names — the transaction
    rolled back, nothing of it was written."""

    def __init__(self, op_index: int, expected: int, matched: int) -> None:
        self.op_index = op_index
        self.expected = expected
        self.matched = matched
        super().__init__(f"guarded op #{op_index} matched {matched} of {expected}")


class BaselineMissing(StoreError):
    """The reader refuses publish and decrement before the whole-register
    ``BASELINE`` row exists — never read as an empty cut (§3.4)."""


class DuplicateKey(StoreError):
    """A UNIQUE index refused an INSERT (MySQL 1062) — e.g. a repeated ``INTENT_ID``."""


class StoreClosed(StoreError):
    """A call on a store handle after ``close()`` (no connection outlives a job)."""


class DatabaseUnavailable(StoreError):
    """The connection failed or was lost; nothing of the statement in flight
    committed (a lost ``COMMIT`` is :class:`CommitUnknown` instead)."""


class CommitUnknown(StoreError):
    """The connection failed during ``COMMIT``: the transaction may or may not
    have committed. Never retried blind — the caller re-reads (recovery)."""


class LockWaitTimeout(StoreError):
    """``innodb_lock_wait_timeout`` (MySQL 1205): the app's own give-up — the
    transaction was rolled back."""


class Deadlock(StoreError):
    """MySQL 1213: InnoDB chose this transaction as the victim and rolled it back."""


__all__ = [
    "SHELF", "ACTOR_IMPORT", "EVENT_CONSUME_MISS",
    "KIND_AUDIT", "KIND_DELETE", "KIND_PRICE", "KIND_REGISTER", "KIND_INIT_BEFORE",
    "KIND_BASELINE", "KIND_RECONCILE", "KIND_HOLD", "KINDS",
    "OPEN", "APPLIED", "DONE", "CARRIED", "FAILED", "HELD", "SUPERSEDED",
    "STATUSES", "NOT_CLOSED", "TERMINAL",
    "SCOPE_REGISTER", "SCOPE_KEY",
    "HOLD_BASELINE", "HOLD_RECOVERY", "HOLD_ROW", "HOLD_IN_FLIGHT",
    "PAIR_WRITABLE", "MAX_STAMP_IDS", "Key",
    "PairRow", "EventRow", "AuditRow", "LeaseRow", "Fence",
    "InsertAudit", "UpdateAudit", "StampEvents", "UpdatePairs", "WriteOp", "REGISTER_OPS",
    "check_op_order", "Hold", "PendingDeltas",
    "StoreError", "LeaseLost", "GuardMismatch", "BaselineMissing", "DuplicateKey",
    "StoreClosed", "DatabaseUnavailable", "CommitUnknown", "LockWaitTimeout", "Deadlock",
]
