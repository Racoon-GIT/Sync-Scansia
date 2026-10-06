"""The register as the services use it — :class:`Register` implements
:class:`~backend.persistence.ports.RegisterPort` over a
:class:`~backend.persistence.ports.RegisterStore` (MySQL in production, the
in-memory fake in tests), so the semantics below are written — and tested — once.

Plan ``docs/f7-mysql-adapter-plan.md``: §3.1 (the operations), §3.3 rules 2 and 6
(the ``RECONCILE`` rows, the fenced lease), §3.4 (``BASELINE`` rows, holds),
§3.6 (the audit table), §3.7 (connection discipline: plain reads, every write a
renew-first transaction, ``UPDATE`` by PRIMARY KEY after a plain SELECT, the
register ``UPDATE`` last).

Holds (§3.1 ``held_keys()``) are DERIVED from the audit rows, never stored as
state of their own:

* ``baseline`` — the whole-register ``BASELINE`` row's ``PAYLOAD.held`` list;
* ``recovery`` — a ``RECONCILE`` row closed ``HELD``, on every key it serves;
* ``hold``     — a ``HOLD`` row (the delete's "outlet deleted", a key moved at a
  re-level's confirm, any hold made outside a baseline or recovery);
* ``in_flight`` — every ``RECONCILE`` row not closed (``OPEN``/``APPLIED``),
  on every key it serves; it ends when its row closes.

A per-key ``BASELINE`` row (a re-level, or the N-0 release) releases the
baseline / recovery / hold holds of its key written BEFORE it (lower row id);
it never ends an in-flight hold — only the row's close does.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from backend.persistence.records import (
    APPLIED,
    CARRIED,
    DONE,
    HELD,
    HOLD_BASELINE,
    HOLD_IN_FLIGHT,
    HOLD_RECOVERY,
    HOLD_ROW,
    KIND_BASELINE,
    KIND_HOLD,
    KIND_PRICE,
    KIND_RECONCILE,
    KIND_REGISTER,
    NOT_CLOSED,
    OPEN,
    SCOPE_KEY,
    SCOPE_REGISTER,
    SHELF,
    AuditRow,
    BaselineMissing,
    GuardMismatch,
    Hold,
    InsertAudit,
    Key,
    LeaseRow,
    PairRow,
    PendingDeltas,
    StampEvents,
    UpdateAudit,
    UpdatePairs,
    WriteOp,
)

DEFAULT_ACTOR = "scansia-manager"


def _key(raw: Any) -> Key:
    return (str(raw[0]), str(raw[1]))


def compute_holds(
    baselines: Iterable[AuditRow],
    hold_rows: Iterable[AuditRow],
    reconcile_rows: Iterable[AuditRow],
) -> List[Hold]:
    """Derive the active holds from the audit rows (see the module docstring).

    ``reconcile_rows`` may carry any status; only ``HELD`` and not-closed rows
    hold. Pure: the fake and MySQL share it.
    """
    releases: Dict[Key, int] = {}
    holds: List[Hold] = []
    for b in baselines:
        p = b.payload or {}
        scope = p.get("scope")
        if scope == SCOPE_KEY and p.get("key") is not None:
            k = _key(p["key"])
            releases[k] = max(releases.get(k, 0), b.row_id)
        elif scope == SCOPE_REGISTER:
            for h in p.get("held") or ():
                holds.append(Hold(
                    key=_key(h["key"]), kind=HOLD_BASELINE, reason=str(h.get("reason") or ""),
                    event_ids=frozenset(int(i) for i in (h.get("event_ids") or ())),
                    row_id=b.row_id, created_at=b.created_at,
                ))
    for r in hold_rows:
        p = r.payload or {}
        for k in r.keys:
            holds.append(Hold(
                key=k, kind=HOLD_ROW, reason=str(p.get("reason") or ""), event_ids=r.event_ids,
                row_id=r.row_id, created_at=r.created_at, product_id=r.product_id,
            ))
    for r in reconcile_rows:
        if r.status == HELD:
            kind = HOLD_RECOVERY
        elif r.status in NOT_CLOSED:
            kind = HOLD_IN_FLIGHT
        else:
            continue
        reason = str((r.outcome or {}).get("reason") or r.action) if r.status == HELD else r.action
        for k in r.keys:
            holds.append(Hold(
                key=k, kind=kind, reason=reason, event_ids=r.event_ids, row_id=r.row_id,
                created_at=r.created_at, status=r.status, action=r.action,
                product_id=r.product_id, sent_at=r.sent_at,
            ))
    return [
        h for h in holds
        if h.kind == HOLD_IN_FLIGHT or releases.get(h.key, 0) < h.row_id
    ]


class Register:
    """:class:`~backend.persistence.ports.RegisterPort` (+ the ``RECONCILE``
    ledger and lease primitives the reconciliation composes) over one store
    handle. One instance per job; :meth:`close` ends its connection."""

    def __init__(self, store: Any, *, actor: str = DEFAULT_ACTOR) -> None:
        self.store = store
        self.actor = (actor or "").strip() or DEFAULT_ACTOR

    def close(self) -> None:
        self.store.close()

    # =====================================================================
    # Reads (plain, non-locking)
    # =====================================================================
    def shelf(self, sku: Optional[str] = None) -> List[PairRow]:
        """Pairs ``STATE = 'IN_SCANSIA'`` (optionally one SKU), by id."""
        return self.store.read_pairs(states=(SHELF,), sku=sku)

    def pairs(
        self,
        *,
        states: Optional[Sequence[str]] = None,
        sku: Optional[str] = None,
        pair_ids: Optional[Sequence[int]] = None,
        outlet_product_id: Optional[int] = None,
    ) -> List[PairRow]:
        """Pairs in any state (``states=None``) — history included."""
        return self.store.read_pairs(
            states=states, sku=sku, pair_ids=pair_ids, outlet_product_id=outlet_product_id
        )

    def baseline(self) -> Optional[AuditRow]:
        """The whole-register ``BASELINE`` row, or ``None`` (never an empty cut)."""
        for b in self.store.read_audit(KIND_BASELINE):
            if (b.payload or {}).get("scope") == SCOPE_REGISTER:
                return b
        return None

    def held_keys(self) -> List[Hold]:
        return compute_holds(
            self.store.read_audit(KIND_BASELINE),
            self.store.read_audit(KIND_HOLD),
            self.store.read_audit(KIND_RECONCILE, statuses=tuple(NOT_CLOSED | {HELD})),
        )

    def pending_deltas(self) -> PendingDeltas:
        """Unstamped events joined to their pair's key, minus every held event.

        Refuses before the whole-register ``BASELINE`` row (:class:`BaselineMissing`,
        §3.4). Events are read FIRST, the holds after: an event landing in between
        is either read now or left for the next pass (rule 5)."""
        if self.baseline() is None:
            raise BaselineMissing("no whole-register BASELINE row: run 'Inizializza' first")
        events = self.store.read_unstamped_events()
        holds = self.held_keys()
        held_ids = frozenset().union(*(h.event_ids for h in holds)) if holds else frozenset()
        unstamped = {e.event_id for e in events}
        carried = tuple(
            r for r in self.store.read_audit(KIND_RECONCILE, statuses=(CARRIED,))
            if r.event_ids & unstamped
        )
        return PendingDeltas(
            events=tuple(e for e in events if e.key is not None and e.event_id not in held_ids),
            keyless=tuple(e for e in events if e.key is None),
            holds=tuple(holds),
            carried=carried,
        )

    def unstamped_events(self) -> List[Any]:
        """Every unstamped event, held ones included (a re-level's and the N-0
        release's read, §3.3/§3.4) — no baseline gate."""
        return self.store.read_unstamped_events()

    def reconcile_rows(
        self,
        *,
        statuses: Optional[Sequence[str]] = None,
        row_ids: Optional[Sequence[int]] = None,
        group_id: Optional[int] = None,
        product_id: Optional[int] = None,
    ) -> List[AuditRow]:
        return self.store.read_audit(
            KIND_RECONCILE, statuses=statuses, row_ids=row_ids, group_id=group_id,
            product_id=product_id,
        )

    def audit_rows(self, kind: str, **filters: Any) -> List[AuditRow]:
        return self.store.read_audit(kind, **filters)

    def server_now(self) -> datetime:
        return self.store.server_now()

    # =====================================================================
    # The lease statements (each one guarded statement, autocommit)
    # =====================================================================
    def lease_take(self, holder: str, ttl_ms: int) -> Optional[int]:
        return self.store.lease_take(holder, ttl_ms)

    def lease_release(self, lease: Any) -> bool:
        return self.store.lease_release(lease.fence)

    def lease_wait(self, holder: str, wait_ms: int) -> bool:
        return self.store.lease_wait(holder, wait_ms)

    def lease_state(self) -> LeaseRow:
        return self.store.read_lease()

    def renew(self, lease: Any) -> None:
        """A renew alone (a re-send's fence, §3.3 rule 6) — no other statement."""
        self._write(lease, [])

    # =====================================================================
    # Fenced writes
    # =====================================================================
    def _write(self, lease: Any, ops: Sequence[WriteOp]) -> List[int]:
        started = lease.monotonic()
        ids = self.store.fenced_write(lease.fence, list(ops))
        lease.mark_renewed(started)
        return ids

    # -- the stamp (§3.1) ---------------------------------------------------
    def stamp(
        self,
        lease: Any,
        event_ids: Sequence[int],
        *,
        close_rows: Sequence[int] = (),
        carrier: Optional[InsertAudit] = None,
    ) -> List[int]:
        """Stamp ``event_ids`` (by PRIMARY KEY, ``COALESCE`` keeps an earlier
        stamp) in one fenced transaction with the audit row(s) that carry it:
        the renew, then ``carrier`` (a new ``RECONCILE``/``BASELINE`` row) and/or
        the close of ``close_rows`` (``APPLIED`` → ``DONE``), then the stamp —
        the register ``UPDATE`` last. Returns the carrier's id, if any.

        A matched count short of the distinct ids rolls everything back and
        raises (:class:`~backend.persistence.records.GuardMismatch`)."""
        ids = tuple(sorted({int(i) for i in event_ids}))
        if not ids:
            raise ValueError("stamp() needs at least one event id")
        if carrier is None and not close_rows:
            raise ValueError("a stamp is carried by an audit row (§3.1): carrier or close_rows")
        ops: List[WriteOp] = []
        if carrier is not None:
            ops.append(carrier)
        ops.extend(UpdateAudit(row_id=r, expect_status=APPLIED, status=DONE) for r in close_rows)
        ops.append(StampEvents(ids))
        return self._write(lease, ops)

    # -- register columns (each with one audit row, §3.1 last paragraph) ----------
    def _pairs_write(
        self,
        lease: Any,
        build: Callable[[], Optional[List[WriteOp]]],
    ) -> bool:
        """Build (plain SELECT) then write; a pair that left the shelf between
        the two (the guard matched short) → re-read and rebuild once."""
        for attempt in (1, 2):
            ops = build()
            if not ops:
                return False
            try:
                self._write(lease, ops)
                return True
            except GuardMismatch as e:
                if attempt == 2 or e.op_index != len(ops) - 1:
                    raise
        return False  # pragma: no cover - loop always returns or raises

    def set_outlet_price(
        self,
        lease: Any,
        sku: str,
        price: Optional[Decimal],
        discount: Optional[Decimal],
        *,
        only_missing: bool = False,
    ) -> List[int]:
        """One price per SKU onto its ``IN_SCANSIA`` pairs (§3.5) — all of them,
        or only those with no ``PRICE_OUTLET`` (the publish's fill). A ``PRICE``
        audit row records each pair's priors. Returns the pair ids written."""
        written: List[int] = []

        def build() -> Optional[List[WriteOp]]:
            pairs = [
                p for p in self.store.read_pairs(states=(SHELF,), sku=sku)
                if not only_missing or p.price_outlet is None
            ]
            written[:] = [p.pair_id for p in pairs]
            if not pairs:
                return None
            payload = {
                "sku": sku,
                "price_outlet": None if price is None else str(price),
                "discount": None if discount is None else str(discount),
                "pairs": [
                    {"pair_id": p.pair_id,
                     "old_price_outlet": None if p.price_outlet is None else str(p.price_outlet),
                     "old_discount": None if p.discount is None else str(p.discount)}
                    for p in pairs
                ],
            }
            return [
                InsertAudit(kind=KIND_PRICE, actor=self.actor, action="set_outlet_price", payload=payload),
                UpdatePairs(
                    pair_ids=tuple(written), values={"PRICE_OUTLET": price, "DISCOUNT": discount},
                    require_state=SHELF,
                ),
            ]

        return list(written) if self._pairs_write(lease, build) else []

    def _register_row(self, column: str, pairs: Sequence[PairRow], new: Any) -> InsertAudit:
        attr = {"OUTLET_PRODUCT_ID": "outlet_product_id", "ONLINE": "online"}[column]
        return InsertAudit(
            kind=KIND_REGISTER, actor=self.actor, action=f"set_{column.lower()}",
            payload={
                "column": column,
                "pairs": [
                    {"pair_id": p.pair_id, "old": getattr(p, attr), "new": new} for p in pairs
                ],
            },
        )

    def set_outlet_product(self, lease: Any, pair_ids: Sequence[int], product_id: int) -> None:
        """Write ``OUTLET_PRODUCT_ID`` on shelf pairs (by PRIMARY KEY, guarded
        ``IN_SCANSIA``); the caller has checked the product is an outlet by title
        AND collection, never a resolver ``review`` match (§3.1)."""
        wanted = sorted({int(i) for i in pair_ids})

        def build() -> Optional[List[WriteOp]]:
            pairs = self.store.read_pairs(states=(SHELF,), pair_ids=wanted)
            if not pairs:
                return None
            ids = tuple(p.pair_id for p in pairs)
            return [
                self._register_row("OUTLET_PRODUCT_ID", pairs, int(product_id)),
                UpdatePairs(pair_ids=ids, values={"OUTLET_PRODUCT_ID": int(product_id)},
                            require_state=SHELF),
            ]

        self._pairs_write(lease, build)

    def clear_outlet_product(self, lease: Any, product_id: int) -> List[int]:
        """Null ``OUTLET_PRODUCT_ID`` on every pair carrying ``product_id`` (any
        state: a restored pair must not bring a deleted id back) — the delete's
        step (1), §3.5. Returns the pair ids cleared."""
        cleared: List[int] = []

        def build() -> Optional[List[WriteOp]]:
            pairs = self.store.read_pairs(states=None, outlet_product_id=int(product_id))
            cleared[:] = [p.pair_id for p in pairs]
            if not pairs:
                return None
            return [
                self._register_row("OUTLET_PRODUCT_ID", pairs, None),
                UpdatePairs(pair_ids=tuple(cleared), values={"OUTLET_PRODUCT_ID": None},
                            require_outlet_product_id=int(product_id)),
            ]

        return list(cleared) if self._pairs_write(lease, build) else []

    def set_online(self, lease: Any, pair_ids: Sequence[int], value: bool) -> None:
        """``ONLINE`` — informational only, never read by reconciliation (§3.2);
        setting 1 is guarded ``STATE = 'IN_SCANSIA'`` (a pair that left the shelf
        meanwhile is dropped on the re-read)."""
        wanted = sorted({int(i) for i in pair_ids})
        if not wanted:
            return

        def build() -> Optional[List[WriteOp]]:
            pairs = self.store.read_pairs(states=(SHELF,) if value else None, pair_ids=wanted)
            if not pairs:
                return None
            return [
                self._register_row("ONLINE", pairs, bool(value)),
                UpdatePairs(pair_ids=tuple(p.pair_id for p in pairs), values={"ONLINE": bool(value)},
                            require_state=SHELF if value else None),
            ]

        self._pairs_write(lease, build)

    # -- BASELINE / HOLD rows --------------------------------------------------
    def record_baseline(
        self,
        lease: Any,
        *,
        scope: str,
        action: str,
        payload: Dict[str, Any],
        stamp_ids: Sequence[int],
        plan_hash: Optional[str] = None,
    ) -> int:
        """Write a ``BASELINE`` row (+ its explicit stamp list) in one fenced
        transaction. ``scope="register"`` is the whole-register row (one, ever);
        ``scope="key"`` releases its key (a re-level, or the N-0 release)."""
        if scope not in (SCOPE_REGISTER, SCOPE_KEY):
            raise ValueError(f"unknown baseline scope {scope!r}")
        if scope == SCOPE_KEY and not payload.get("key"):
            raise ValueError("a per-key BASELINE row names its key")
        if scope == SCOPE_REGISTER and self.baseline() is not None:
            raise ValueError("the whole-register BASELINE row already exists")
        row = InsertAudit(
            kind=KIND_BASELINE, actor=self.actor, action=action, plan_hash=plan_hash,
            payload={**payload, "scope": scope},
        )
        ops: List[WriteOp] = [row]
        ids = sorted({int(i) for i in stamp_ids})
        if ids:
            ops.append(StampEvents(tuple(ids)))
        return self._write(lease, ops)[0]

    def record_hold(
        self,
        lease: Any,
        *,
        keys: Sequence[Key],
        reason: str,
        event_ids: Sequence[int] = (),
        action: str = "hold",
        product_id: Optional[int] = None,
    ) -> int:
        """A ``HOLD`` row: a hold made outside a baseline or recovery (§3.6)."""
        if not keys:
            raise ValueError("a HOLD row names at least one key")
        row = InsertAudit(
            kind=KIND_HOLD, actor=self.actor, action=action, product_id=product_id,
            payload={"keys": [list(k) for k in keys], "reason": reason,
                     "event_ids": sorted({int(i) for i in event_ids})},
        )
        return self._write(lease, [row])[0]

    # -- the RECONCILE ledger (§3.3 rule 2) ---------------------------------------
    def write_ahead(self, lease: Any, rows: Sequence[InsertAudit]) -> List[int]:
        """Every row of a group at once, ``OPEN``, in one renew transaction —
        the group's row set is complete before anything is sent."""
        if not rows:
            raise ValueError("write_ahead() needs at least one row")
        for r in rows:
            if r.kind != KIND_RECONCILE or r.status != OPEN:
                raise ValueError("write-ahead rows are RECONCILE rows, OPEN")
        return self._write(lease, list(rows))

    def mark_sent(
        self,
        lease: Any,
        row_id: int,
        *,
        first: bool,
        sent_available: Optional[int] = None,
        request: Optional[str] = None,
        step: Optional[str] = None,
    ) -> None:
        """``SENT_AT`` right before a send (fenced). An inventory row's FIRST send
        writes ``SENT_AVAILABLE`` and ``REQUEST`` with it, once (guard
        ``SENT_AT IS NULL``); a CREATE / tail / normalise row marks every call,
        with its ``STEP``."""
        self._write(lease, [UpdateAudit(
            row_id=row_id, expect_status=OPEN, mark_sent=True, first_send=first,
            sent_available=sent_available if first else None,
            request=request if first else None, step=step,
        )])

    def record_outcome(
        self,
        lease: Any,
        row_id: int,
        *,
        expect: str,
        status: Optional[str] = None,
        outcome: Optional[Dict[str, Any]] = None,
        product_id: Optional[int] = None,
        step: Optional[str] = None,
    ) -> None:
        """One guarded update of a ``RECONCILE`` row (outcome / status / product
        id / step) — 0 rows matched → rolled back, raised (re-read)."""
        self._write(lease, [UpdateAudit(
            row_id=row_id, expect_status=expect, status=status, outcome=outcome,
            product_id=product_id, step=step,
        )])

    def close_rows(self, lease: Any, closes: Sequence[UpdateAudit]) -> None:
        """Several guarded row updates in one transaction (e.g. a group's
        ``APPLIED`` siblings closed ``CARRIED`` beside a ``FAILED`` one)."""
        if closes:
            self._write(lease, list(closes))

    def insert_audit(self, lease: Any, row: InsertAudit) -> int:
        """A non-register audit row (``AUDIT``, ``DELETE``, ``PRICE`` intent,
        ``INIT_BEFORE``, ``HOLD``), fenced like every F7 write."""
        return self._write(lease, [row])[0]


__all__ = ["Register", "compute_holds", "DEFAULT_ACTOR"]
