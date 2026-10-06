"""The ``RECONCILE`` ledger — write-ahead, send, outcome, settle (plan
``docs/f7-mysql-adapter-plan.md`` §3.3 rule 2; ``docs/f7-build-notes.md``).

**The requirement**: a +Δ reaches Shopify at most once, a −Δ at least once, and
every outcome the app cannot know resolves toward FEWER outlet units and is
surfaced — never more. Every Shopify write is one ``RECONCILE`` row:

1. **write-ahead** — every row of a group at once, ``OPEN``, in the renew
   transaction, before the group's first call (:meth:`Ledger.write_group`);
2. **rule 8's live read** right before the send (``includeInactive: true``);
   a row whose premise that read contradicts is never sent — it closes
   ``FAILED`` "never sent" (the next pass plans the remainder);
3. **SENT_AT** (+ ``SENT_AVAILABLE`` = the ``changeFromQuantity`` read, +
   ``REQUEST`` = the exact body) fenced right before the first send — a re-send
   rewrites none of the three;
4. **the outcome** right after the call, one fenced transaction, guarded
   ``STATUS = 'OPEN'``: ``APPLIED`` / ``FAILED`` / still ``OPEN`` (unknown);
5. **settle** — the group's events are stamped, its rows closed ``DONE``, only
   when every row not ``SUPERSEDED`` is ``APPLIED``; beside a ``FAILED`` or
   ``HELD`` sibling each ``APPLIED`` one closes ``CARRIED`` with the quantity it
   moved (the remainder of the next pass nets it).

Row kinds (``ACTION``): ``adjust`` (a relative ``available`` write; its
``PAYLOAD.purpose`` says which: ``add`` / ``lower`` carry a signed ``share`` of
their group's Δ; ``zero`` / ``excess`` / ``custom`` / ``orphan`` / ``nonpromo``
/ ``relevel`` carry an absolute ``target`` — the delta is computed at the send,
from the live read: "a level re-read, not a delta"), ``activate``,
``deactivate`` (no key: not ``@idempotent``), the state rows ``create``,
``revive-tail``, ``normalise`` (``SENT_AT`` + ``STEP`` before EVERY call), and
``stamp-only`` (closed ``DONE`` at its insert: a stamp with no Shopify write).

The outcome maps (first send, recovery re-send) are the plan's, verbatim in
effect; the answer codes are the 2026-10 ``InventoryAdjustQuantitiesUserErrorCode``
values (``ops.py``).
"""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from backend.persistence.records import (
    APPLIED,
    CARRIED,
    DONE,
    FAILED,
    HELD,
    KIND_RECONCILE,
    OPEN,
    SUPERSEDED,
    AuditRow,
    InsertAudit,
    Key,
    UpdateAudit,
)
from backend.shopify import ops
from backend.shopify.transport import ShopifyOutcomeUnknown, ShopifyRefused

logger = logging.getLogger("backend.services.ledger")

# --- vocabulary ---------------------------------------------------------------
A_ADJUST = "adjust"
A_ACTIVATE = "activate"
A_DEACTIVATE = "deactivate"
A_CREATE = "create"
A_TAIL = "revive-tail"
A_NORMALISE = "normalise"
A_STAMP = "stamp-only"
INVENTORY_ACTIONS = frozenset({A_ADJUST, A_ACTIVATE, A_DEACTIVATE})
STATE_ACTIONS = frozenset({A_CREATE, A_TAIL, A_NORMALISE})

P_ADD = "add"
P_LOWER = "lower"
P_ZERO = "zero"
P_EXCESS = "excess"
P_CUSTOM = "custom"
P_ORPHAN = "orphan"
P_NONPROMO = "nonpromo"
P_RELEVEL = "relevel"
SHARE_PURPOSES = frozenset({P_ADD, P_LOWER})
TARGET_PURPOSES = frozenset({P_ZERO, P_EXCESS, P_CUSTOM, P_ORPHAN, P_NONPROMO, P_RELEVEL})
EVENTLESS_LOWERINGS = frozenset({P_ZERO, P_EXCESS, P_CUSTOM, P_ORPHAN, P_NONPROMO})

# The re-send window: the first send + 24 h − the 1 h margin (build notes).
RESEND_WINDOW = timedelta(hours=23)
# Shopify keeps a key 24 h: the N-0 release waits a HELD row's first send past it.
KEY_RETENTION = timedelta(hours=24)

CONCURRENT_BACKOFF_S = (2.0, 4.0, 8.0)

C_CONCURRENT = "IDEMPOTENCY_CONCURRENT_REQUEST"
C_MISMATCH = "IDEMPOTENCY_KEY_PARAMETER_MISMATCH"
C_PREVIOUS_FAILED = "IDEMPOTENCY_PREVIOUS_ATTEMPT_FAILED"
C_STALE = "CHANGE_FROM_QUANTITY_STALE"
C_GONE = frozenset({"INVALID_INVENTORY_ITEM", "INVALID_LOCATION"})

# Answer classes (one vocabulary for both maps).
ANS_OK = "ok"
ANS_CONCURRENT = "concurrent"
ANS_MISMATCH = "mismatch"
ANS_PREVIOUS_FAILED = "previous_failed"
ANS_STALE = "stale"
ANS_GONE = "gone"
ANS_INVALID = "invalid"
ANS_OTHER = "other"      # any other userErrors code (ADJUST_QUANTITIES_FAILED, SERVICE_UNAVAILABLE, …)
ANS_UNKNOWN = "unknown"  # the transport could not tell (timeout, 5xx, …)
ANS_REFUSED = "refused"  # refused before execution (4xx, throttled out, validation)

APP_NAMESPACE = "scansia-manager"


def group_id_for(generation: int, n: int) -> int:
    """``GROUP_ID`` = ``LEASE_GEN × 10^6 + n`` (build notes): unique, no sequence table."""
    return int(generation) * 1_000_000 + int(n)


def reference(row_id: int) -> str:
    """The ``referenceDocumentUri`` of a row's write: Shopify's inventory history
    names the row that caused it (GID form, all components present)."""
    return f"gid://{APP_NAMESPACE}/OutletReconcile/{int(row_id)}"


def payload(row: AuditRow) -> Dict[str, Any]:
    return row.payload or {}


def purpose(row: AuditRow) -> Optional[str]:
    return payload(row).get("purpose")


def share(row: AuditRow) -> int:
    return int(payload(row).get("share") or 0)


def moved(row: AuditRow) -> int:
    """What an applied row moved (its actual delta), from its outcome."""
    return int((row.outcome or {}).get("moved") or 0)


def is_plus(row: AuditRow) -> bool:
    """A +Δ row: never re-added, held on any unknown (rule 2)."""
    return row.action == A_ADJUST and purpose(row) == P_ADD


def is_minus(row: AuditRow) -> bool:
    """A −Δ row of an event group: re-applied at least once."""
    return row.action == A_ADJUST and purpose(row) == P_LOWER


def is_eventless_lowering(row: AuditRow) -> bool:
    return row.action == A_ADJUST and purpose(row) in EVENTLESS_LOWERINGS


def classify(ans: Optional["ops.InventoryAnswer"], transport_class: Optional[str] = None) -> str:
    """One answer class for an inventory mutation's answer (or its transport error)."""
    if transport_class is not None:
        return transport_class
    assert ans is not None
    if ans.ok:
        return ANS_OK
    codes = set(ans.codes)
    if C_CONCURRENT in codes:
        return ANS_CONCURRENT
    if C_MISMATCH in codes:
        return ANS_MISMATCH
    if C_PREVIOUS_FAILED in codes:
        return ANS_PREVIOUS_FAILED
    if C_STALE in codes:
        return ANS_STALE
    if codes & C_GONE:
        return ANS_GONE
    if any(c.startswith("INVALID_") for c in codes):
        return ANS_INVALID
    return ANS_OTHER


# --- write-ahead specs --------------------------------------------------------
@dataclass(frozen=True)
class RowSpec:
    """One row to write ahead (``PAYLOAD`` conventions in ``f7-build-notes.md``)."""

    action: str
    keys: Tuple[Key, ...] = ()
    event_ids: Tuple[int, ...] = ()
    purpose: Optional[str] = None
    share: int = 0
    target: Optional[int] = None
    inventory_item_id: Optional[str] = None
    location_id: Optional[str] = None
    variant_id: Optional[str] = None
    product_id: Optional[int] = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @property
    def keyed(self) -> bool:
        return self.action in (A_ADJUST, A_ACTIVATE)

    def to_insert(self, *, actor: str, group_id: int) -> InsertAudit:
        p: Dict[str, Any] = {
            "keys": [list(k) for k in self.keys],
            "event_ids": sorted({int(i) for i in self.event_ids}),
        }
        if self.purpose is not None:
            p["purpose"] = self.purpose
        if self.action == A_ADJUST:
            p["share"] = int(self.share)
        if self.target is not None:
            p["target"] = int(self.target)
        for name in ("inventory_item_id", "location_id", "variant_id"):
            if getattr(self, name) is not None:
                p[name] = getattr(self, name)
        p.update(dict(self.extra))
        return InsertAudit(
            kind=KIND_RECONCILE, actor=actor, action=self.action, status=OPEN,
            idempotency_key=str(uuid.uuid4()) if self.keyed else None,
            product_id=self.product_id, group_id=group_id, payload=p,
        )


@dataclass
class Report:
    """What a pass surfaces (never silently dropped): one dict per item."""

    items: List[Dict[str, Any]] = field(default_factory=list)

    def surface(self, kind: str, **fields: Any) -> None:
        self.items.append({"kind": kind, **fields})

    def kinds(self) -> List[str]:
        return [i["kind"] for i in self.items]


# --- the ledger -----------------------------------------------------------------
class Ledger:
    """The send protocol of one lease holder's pass."""

    def __init__(
        self,
        register: Any,
        transport: Any,
        lease: Any,
        promo_id: str,
        *,
        report: Optional[Report] = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.register = register
        self.transport = transport
        self.lease = lease
        self.promo_id = promo_id
        self.report = report or Report()
        self.sleep = sleep
        self._n = 0
        self._gen: Optional[int] = None

    # -- groups --------------------------------------------------------------------
    def new_group(self) -> int:
        gen = self.lease.generation
        if gen != self._gen:  # a re-take: a new generation numbers its groups afresh
            self._gen, self._n = gen, 0
        self._n += 1
        return group_id_for(gen, self._n)

    def write_group(self, specs: Sequence[RowSpec], group_id: Optional[int] = None) -> List[AuditRow]:
        """Write every row of a group ahead, in one renew transaction; return them."""
        gid = group_id if group_id is not None else self.new_group()
        inserts = [s.to_insert(actor=self.register.actor, group_id=gid) for s in specs]
        ids = self.register.write_ahead(self.lease, inserts)
        rows = {r.row_id: r for r in self.register.reconcile_rows(row_ids=ids)}
        return [rows[i] for i in ids]

    def reload(self, row: AuditRow) -> AuditRow:
        return self.register.reconcile_rows(row_ids=[row.row_id])[0]

    def group_rows(self, group_id: int) -> List[AuditRow]:
        return self.register.reconcile_rows(group_id=group_id)

    # -- outcome writes ----------------------------------------------------------------
    def close(self, row: AuditRow, status: str, **outcome: Any) -> str:
        merged = {**(row.outcome or {}), **outcome}
        if status == FAILED and payload(row).get("reapply_of"):
            # its superseded original may have landed (rule 2)
            merged.setdefault("possibly_applied", True)
        merged = {k: v for k, v in merged.items() if v is not None}
        self.register.record_outcome(self.lease, row.row_id, expect=row.status or OPEN,
                                     status=status, outcome=merged)
        return status

    def never_sent(self, row: AuditRow, why: str) -> str:
        """A row not sent: ``FAILED`` "never sent" — nothing of it can have landed."""
        return self.close(row, FAILED, never_sent=True, reason=why)

    def _fit_to_send(self) -> None:
        """Before a send: the local deadline. Past it, renew first (the caller re-reads)."""
        self.lease.keepalive()

    # -- adjust rows -----------------------------------------------------------------------
    def _planned_delta(self, row: AuditRow, lvl: Optional[Dict[str, Any]]) -> Tuple[Optional[int], str]:
        """The delta this row may send now, from rule 8's live read — or why not."""
        if lvl is None:
            return None, "level_absent"
        live = lvl.get("available")
        live = int(live) if live is not None else 0
        pur = purpose(row)
        if pur == P_ADD:
            if not lvl.get("is_active"):
                return None, "level_inactive"           # a +Δ never lands on an inactive level
            return share(row), ""
        if pur == P_LOWER:
            s = share(row)
            if live <= 0:
                return None, "nothing_to_lower"         # rule 4 decides at the next pass
            if -s > live:
                return None, "share_above_available"    # the next pass plans the remainder
            return s, ""
        target = int(payload(row).get("target") or 0)
        if pur == P_RELEVEL:
            d = target - live
            if d > 0 and (lvl.get("committed") or 0) > 0:
                return None, "committed_on_upward_adjust"
            if d > 0 and not lvl.get("is_active"):
                return None, "level_inactive"
            return d, ""
        if live <= target:
            return None, "nothing_to_lower"
        return target - live, ""

    def send_adjust(self, row: AuditRow) -> str:
        """The FIRST send of an ``adjust`` row. Returns the row's status after it."""
        p = payload(row)
        item, loc = p["inventory_item_id"], p["location_id"]
        while True:
            self._fit_to_send()
            read = ops.read_level(self.transport, item, loc)
            if not read["item_found"]:
                return self.never_sent(row, "item_gone")
            lvl = read["level"]
            delta, why = self._planned_delta(row, lvl)
            if delta is None:
                return self.never_sent(row, why)
            live = int(lvl.get("available") or 0)
            if delta == 0:  # a re-level already at its target: nothing to write
                return self.close(row, APPLIED, moved=0, no_write=True, change_from=live)
            body = ops.adjust_request(item, loc, delta, live, row.idempotency_key, reference=reference(row.row_id))
            if not self.lease.can_send():
                self.register.renew(self.lease)
                continue  # renewed: re-read before sending
            self.register.mark_sent(self.lease, row.row_id, first=True, sent_available=live, request=body)
            row = self.reload(row)
            return self._first_answer(row, body, delta)

    def _send(self, body: str) -> Tuple[str, Optional["ops.InventoryAnswer"]]:
        try:
            ans = ops.send_inventory(self.transport, body)
        except ShopifyOutcomeUnknown:
            return ANS_UNKNOWN, None
        except ShopifyRefused:
            return ANS_REFUSED, None
        return classify(ans), ans

    def _applied(self, row: AuditRow, ans: "ops.InventoryAnswer", delta: int, **extra: Any) -> str:
        return self.close(row, APPLIED, moved=int(delta), changes=list(ans.changes),
                          change_from=row.sent_available, **extra)

    def _first_answer(self, row: AuditRow, body: str, delta: int) -> str:
        """The first send's outcome map (rule 2)."""
        for attempt in range(len(CONCURRENT_BACKOFF_S) + 1):
            cls, ans = self._send(body)
            if cls == ANS_OK:
                return self._applied(row, ans, delta)
            if cls == ANS_CONCURRENT and attempt < len(CONCURRENT_BACKOFF_S):
                self.sleep(CONCURRENT_BACKOFF_S[attempt])
                self.register.renew(self.lease)   # a re-send: a renew alone, nothing rewritten
                continue
            break
        if cls in (ANS_UNKNOWN, ANS_CONCURRENT):
            self.report.surface("outcome_unknown", row_id=row.row_id, answer=cls)
            return OPEN
        if cls == ANS_MISMATCH:
            self.report.surface("idempotency_mismatch", row_id=row.row_id)  # a client defect
            return OPEN
        codes = list(ans.codes) if ans else []
        return self.close(row, FAILED, answer=cls, codes=codes)

    # -- the re-send window (recovery) ---------------------------------------------------------
    def in_window(self, row: AuditRow, now: Any) -> bool:
        return row.sent_at is not None and now < row.sent_at + RESEND_WINDOW

    def resend_adjust(self, row: AuditRow) -> Tuple[str, str]:
        """One in-window re-send of the STORED request. Returns ``(answer class,
        status after it)``; the caller decides a ``REAPPLY`` / ``RECOMPUTE``."""
        self.register.renew(self.lease)  # a re-send's fence: a renew alone, nothing rewritten
        cls, ans = self._send(row.request)
        if cls == ANS_OK:
            delta = share(row) if purpose(row) in SHARE_PURPOSES else _sent_delta(row)
            return cls, self._applied(row, ans, delta, cached=True)
        reapplied = bool(payload(row).get("reapply_of"))
        if cls in (ANS_PREVIOUS_FAILED, ANS_INVALID):
            return cls, self.close(row, FAILED, answer=cls, possibly_applied=reapplied or None)
        if is_plus(row) or purpose(row) == P_RELEVEL:
            if cls == ANS_CONCURRENT:
                return cls, OPEN                       # re-sent again until the window's end
            return cls, self.close(row, HELD, answer=cls, reason="unknown_plus")
        if cls in (ANS_GONE, ANS_STALE):
            if is_minus(row):
                return cls, self.close(row, FAILED, answer=cls, possibly_applied=True)
            return cls, self.close(row, FAILED, answer=cls)
        return cls, OPEN                               # −Δ / lowering: the caller re-applies

    # -- activation ----------------------------------------------------------------------------------
    def send_activate(self, row: AuditRow) -> str:
        """First send of an explicit activation — only of an absent or inactive level."""
        p = payload(row)
        item, loc = p["inventory_item_id"], p["location_id"]
        self._fit_to_send()
        read = ops.read_level(self.transport, item, loc)
        if not read["item_found"]:
            return self.never_sent(row, "item_gone")
        lvl = read["level"]
        if lvl is not None and lvl.get("is_active"):
            return self.close(row, APPLIED, moved=0, already_active=True)
        body = ops.activate_request(item, loc, row.idempotency_key)
        if not self.lease.can_send():
            self.register.renew(self.lease)
        self.register.mark_sent(self.lease, row.row_id, first=True,
                                sent_available=(lvl or {}).get("available"), request=body)
        row = self.reload(row)
        cls, _ans = self._send(body)
        if cls == ANS_OK:
            return self.close(row, APPLIED, moved=0)
        return self.activation_by_read(row, answer=cls)

    def activation_by_read(self, row: AuditRow, **why: Any) -> str:
        """An activation's unknown outcome read off the level's ``isActive``."""
        p = payload(row)
        read = ops.read_level(self.transport, p["inventory_item_id"], p["location_id"])
        if not read["item_found"]:
            return self.close(row, HELD, reason="activation_level_gone", **why)
        lvl = read["level"]
        if lvl is not None and lvl.get("is_active"):
            return self.close(row, APPLIED, moved=0, read_active=True, **why)
        return self.close(row, FAILED, read_active=False, **why)

    def resend_activate(self, row: AuditRow) -> str:
        """Recovery: re-read first (a level since made active is never re-sent:
        its explicit quantities would be an absolute set — E15)."""
        p = payload(row)
        read = ops.read_level(self.transport, p["inventory_item_id"], p["location_id"])
        if not read["item_found"]:
            return self.close(row, HELD, reason="activation_level_gone")
        if read["level"] is not None and read["level"].get("is_active"):
            return self.close(row, APPLIED, moved=0, read_active=True)
        self.register.renew(self.lease)
        cls, _ans = self._send(row.request)
        if cls == ANS_OK:
            return self.close(row, APPLIED, moved=0, cached=True)
        return self.activation_by_read(row, answer=cls)

    # -- deactivation ----------------------------------------------------------------------------------
    def send_deactivate(self, row: AuditRow) -> str:
        """``inventoryDeactivate`` (no key): an inactive or absent level is
        already the outcome; an unknown answer is read off ``isActive``."""
        p = payload(row)
        item, loc = p["inventory_item_id"], p["location_id"]
        self._fit_to_send()
        read = ops.read_level(self.transport, item, loc)
        if not read["item_found"]:
            return self.never_sent(row, "item_gone")
        lvl = read["level"]
        if lvl is None or not lvl.get("is_active"):
            return self.close(row, APPLIED, moved=0, already_inactive=True)
        body = ops.deactivate_request(lvl["id"])
        if not self.lease.can_send():
            self.register.renew(self.lease)
        self.register.mark_sent(self.lease, row.row_id, first=True, request=body)
        row = self.reload(row)
        cls, _ans = self._send(body)
        if cls == ANS_OK:
            return self.close(row, APPLIED, moved=0)
        return self.deactivation_by_read(row, answer=cls)

    def deactivation_by_read(self, row: AuditRow, **why: Any) -> str:
        p = payload(row)
        read = ops.read_level(self.transport, p["inventory_item_id"], p["location_id"])
        lvl = read["level"] if read["item_found"] else None
        if lvl is None or not lvl.get("is_active"):
            return self.close(row, APPLIED, moved=0, read_inactive=True, **why)
        return self.close(row, FAILED, read_inactive=False, **why)

    # -- state rows ------------------------------------------------------------------------------------------
    def step(self, row: AuditRow, step: str, call: Callable[[], Any]) -> Any:
        """One call of a CREATE / tail / normalise row: ``SENT_AT`` + ``STEP``
        fenced right before it, the step reached recorded right after."""
        self._fit_to_send()
        if not self.lease.can_send():
            self.register.renew(self.lease)
        self.register.mark_sent(self.lease, row.row_id, first=False, step=step)
        result = call()
        self.register.record_outcome(self.lease, row.row_id, expect=OPEN, step=f"{step}:ok")
        return result

    # -- settle --------------------------------------------------------------------------------------------------
    def settle(self, group_id: int) -> str:
        """Stamp + close a finished group, or carry its applied rows beside a
        failed / held one. Returns ``waiting`` / ``stamped`` / ``closed`` /
        ``carried`` / ``settled``."""
        rows = [r for r in self.group_rows(group_id) if r.status != SUPERSEDED]
        if any(r.status == OPEN for r in rows):
            return "waiting"
        applied = [r for r in rows if r.status == APPLIED]
        if not applied:
            return "settled"
        if all(r.status in (APPLIED, DONE) for r in rows):
            event_ids = sorted(set().union(*(r.event_ids for r in rows)))
            if event_ids:
                self.register.stamp(self.lease, event_ids, close_rows=[r.row_id for r in applied])
                return "stamped"
            self.register.close_rows(self.lease, [
                UpdateAudit(row_id=r.row_id, expect_status=APPLIED, status=DONE) for r in applied
            ])
            return "closed"
        self.register.close_rows(self.lease, [
            UpdateAudit(row_id=r.row_id, expect_status=APPLIED, status=CARRIED,
                        outcome={**(r.outcome or {}), "carried": True})
            for r in applied
        ])
        return "carried"


def _sent_delta(row: AuditRow) -> int:
    """The delta inside a stored adjust request (an event-less / re-level row)."""
    import json

    try:
        change = json.loads(row.request)["variables"]["input"]["changes"][0]
        return int(change["delta"])
    except Exception:  # noqa: BLE001 - a malformed stored request: count nothing moved
        return 0


def carried_share(carried_rows: Iterable[AuditRow], key: Key, event_ids: Iterable[int]) -> int:
    """Σ of what ``CARRIED`` rows already moved over these events of this key
    (matched on event ids; activation and zeroing rows moved 0 for the group)."""
    ids = set(event_ids)
    total = 0
    for r in carried_rows:
        if r.action == A_ADJUST and purpose(r) in SHARE_PURPOSES and key in r.keys and (r.event_ids & ids):
            total += moved(r)
    return total


__all__ = [
    "A_ADJUST", "A_ACTIVATE", "A_DEACTIVATE", "A_CREATE", "A_TAIL", "A_NORMALISE", "A_STAMP",
    "INVENTORY_ACTIONS", "STATE_ACTIONS",
    "P_ADD", "P_LOWER", "P_ZERO", "P_EXCESS", "P_CUSTOM", "P_ORPHAN", "P_NONPROMO", "P_RELEVEL",
    "SHARE_PURPOSES", "TARGET_PURPOSES", "EVENTLESS_LOWERINGS",
    "RESEND_WINDOW", "KEY_RETENTION", "CONCURRENT_BACKOFF_S",
    "ANS_OK", "ANS_CONCURRENT", "ANS_MISMATCH", "ANS_PREVIOUS_FAILED", "ANS_STALE", "ANS_GONE",
    "ANS_INVALID", "ANS_OTHER", "ANS_UNKNOWN", "ANS_REFUSED",
    "group_id_for", "reference", "payload", "purpose", "share", "moved", "is_plus", "is_minus",
    "is_eventless_lowering", "classify", "RowSpec", "Report", "Ledger", "carried_share",
]
