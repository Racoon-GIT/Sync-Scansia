"""A person's close of a ``RECONCILE`` row — plan ``docs/f7-mysql-adapter-plan.md``
§3.3 rule 2's terminal path (iv).

Any row not closed (``OPEN``, or ``APPLIED`` awaiting its stamp) may be closed by
a person: an audit-recorded close (who, when, why) under the pass lease — a
decrement-class holder, whose recovery runs first and may close the row itself —
guarded on the row's ``STATUS`` still the one the person read (0 rows matched →
nothing closed: re-read). It sets ``HELD`` — never ``FAILED`` or ``APPLIED`` — so
the row's keys stay held (§3.4) until a per-key re-level (which stamps the row's
events) or the N-0 release: a person's close never re-adds and never stamps. A
revive-tail or normalise row (no event ids: nothing to re-add or stamp) closes
``FAILED`` instead, with terminal path (i)'s consequences: a normalise row's ACTIVE
product still carrying a variant not DENY + tracked is drafted; a tail's product is
left as the tail left it, surfaced (S13 alarms a DRAFT or unpublished outlet
holding Promo). The row's group is then settled: beside the ``HELD`` row its
``APPLIED`` siblings close ``CARRIED`` (AB1), the events held by the row.

Its designed case is the "orphan unknown" CREATE (``PRODUCT_ID`` NULL) whose orphan
nobody can find any more (AB15) — and any row a person judges stuck.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.persistence.records import (
    FAILED,
    HELD,
    NOT_CLOSED,
    AuditRow,
    GuardMismatch,
    LeaseLost,
    StoreError,
)
from backend.services import flows, passes
from backend.services import ledger as L
from backend.services.lease import HOLDER_CLOSE, LeaseUnavailable, PassLease
from backend.shopify.ops import ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.close_service")

ST_CLOSED = "CLOSED"
ST_CHANGED = "CHANGED_MEANWHILE"     # the row's status is no longer the one read: re-read
ST_NOT_FOUND = "NOT_FOUND"
ST_REFUSED = "REFUSED"
ST_LEASE_UNAVAILABLE = "LEASE_UNAVAILABLE"
ST_INCOMPLETE = "INCOMPLETE"

CLOSE_TAKE_BOUND_S = passes.HOLDER_TAKE_BOUND_S
WHY_MIN_CHARS = 3


@dataclass(frozen=True)
class OpenRow:
    row_id: int
    action: str
    purpose: Optional[str]
    status: str
    keys: Tuple[Tuple[str, str], ...]
    product_id: Optional[int]
    group_id: Optional[int]
    events: int
    sent_at: Optional[str]
    created_at: str
    step: Optional[str]
    closes_as: str                     # what a person's close would set


@dataclass(frozen=True)
class CloseReport:
    status: str
    row_id: int
    closed_as: Optional[str] = None
    reason: Optional[str] = None
    audit_row: Optional[int] = None
    settled: Optional[str] = None
    surfaced: Tuple[Dict[str, Any], ...] = ()


def _iso(ts: Optional[datetime]) -> Optional[str]:
    if ts is None:
        return None
    return (ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts).isoformat()


def closes_as(row: AuditRow) -> str:
    """``FAILED`` for a revive-tail or normalise row, ``HELD`` for every other."""
    return FAILED if row.action in (L.A_TAIL, L.A_NORMALISE) else HELD


def open_rows(register: Any) -> List[OpenRow]:
    """Every ``RECONCILE`` row not closed, oldest first (a read: no lease)."""
    return [
        OpenRow(row_id=r.row_id, action=r.action, purpose=L.purpose(r), status=r.status or "",
                keys=r.keys, product_id=r.product_id, group_id=r.group_id, events=len(r.event_ids),
                sent_at=_iso(r.sent_at), created_at=_iso(r.created_at) or "", step=r.step, closes_as=closes_as(r))
        for r in register.reconcile_rows(statuses=tuple(sorted(NOT_CLOSED)))
    ]


def _consequences(ctx: "flows.Context", row: AuditRow) -> None:
    """Terminal path (i)'s consequences of a tail or normalise row closed FAILED."""
    gid = flows.product_gid(row.product_id)
    if gid is None:
        return
    if row.action == L.A_NORMALISE:
        flows.draft_if_unsafe(ctx, gid)
    else:
        ctx.report.surface("tail_closed_by_person", row_id=row.row_id, product_id=row.product_id)


def close_row(
    register: Any,
    transport: Any,
    row_id: int,
    *,
    expect_status: str,
    why: str,
    promo_location_id: str,
    lease: Optional[PassLease] = None,
    take_bound_s: float = CLOSE_TAKE_BOUND_S,
    sleep: Callable[[float], None] = time.sleep,
) -> CloseReport:
    """Close one row as a person, under the pass lease (see the module docstring)."""
    if len((why or "").strip()) < WHY_MIN_CHARS:
        return CloseReport(ST_REFUSED, row_id, reason="why_required")
    if expect_status not in NOT_CLOSED:
        return CloseReport(ST_REFUSED, row_id, reason="only_a_row_not_closed")
    run = passes.Holder(register, transport, promo_location_id, HOLDER_CLOSE, lease=lease,
                        take_bound_s=take_bound_s, sleep=sleep)
    try:
        run.start()                                   # its recovery may close the row first
        rows = register.reconcile_rows(row_ids=[int(row_id)])
        if not rows:
            return CloseReport(ST_NOT_FOUND, row_id, surfaced=tuple(run.report.items))
        row = rows[0]
        if row.status != expect_status:
            return CloseReport(ST_CHANGED, row_id, reason=f"status_now:{row.status}", surfaced=tuple(run.report.items))
        status = closes_as(row)
        try:
            audit = register.person_close(run.lease, row, status, why)
        except GuardMismatch:
            return CloseReport(ST_CHANGED, row_id, reason="closed_meanwhile", surfaced=tuple(run.report.items))
        closed = run.ctx.ledger.reload(row)
        if status == FAILED:
            try:
                _consequences(run.ctx, closed)
            except (ShopifyUserError, ShopifyTransportError, ProductNotFound) as e:
                run.report.surface("close_consequence_failed", row_id=row_id, error=type(e).__name__)
        settled = run.ctx.ledger.settle(closed.group_id) if closed.group_id is not None else None
        return CloseReport(ST_CLOSED, row_id, closed_as=status, audit_row=audit, settled=settled,
                           surfaced=tuple(run.report.items))
    except LeaseUnavailable:
        status = ST_INCOMPLETE if run.lease.takes else ST_LEASE_UNAVAILABLE
        return CloseReport(status, row_id, reason="lease", surfaced=tuple(run.report.items))
    except LeaseLost:
        return CloseReport(ST_INCOMPLETE, row_id, reason="lease_lost", surfaced=tuple(run.report.items))
    except StoreError as e:
        return CloseReport(ST_INCOMPLETE, row_id, reason=f"register_error:{type(e).__name__}",
                           surfaced=tuple(run.report.items))
    finally:
        run.release()


__all__ = [
    "ST_CLOSED", "ST_CHANGED", "ST_NOT_FOUND", "ST_REFUSED", "ST_LEASE_UNAVAILABLE", "ST_INCOMPLETE",
    "WHY_MIN_CHARS", "OpenRow", "CloseReport", "closes_as", "open_rows", "close_row",
]
