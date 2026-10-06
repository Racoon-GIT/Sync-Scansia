"""Recovery — plan ``docs/f7-mysql-adapter-plan.md`` §3.3 rule 2's per-row
bullets, run by whoever takes the lease (first start, take-over, or a re-take
after another holder) BEFORE anything else, and after any stamp failure.

It never infers from live stock whether its own write landed: a sale between
the crash and the recovery moves the level too. Per row not closed, terminal
path (ii) is read first (its product / inventory item reads gone), then the
row's own bullet; finally every touched group is settled (stamp + ``DONE``, or
``CARRIED`` beside a ``FAILED`` / ``HELD`` sibling).

**Recovery classes.** A publish-class holder (publish, baseline, re-level, the
normalisation at a re-level's confirm) runs every bullet. A decrement-class
holder (the decrement — it never adds, activates or creates — pricing, delete,
the bulk cleanup, the multi-outlet exit, deny-normalize, a person's close)
re-sends and closes only −Δ rows, lowerings and stamp retries, reads
activation / deactivation rows off ``isActive`` (never re-sending one), closes
never-sent inventory and CREATE rows ``FAILED``, and closes rows by terminal
path (ii); every other sent +Δ, re-level and CREATE row, and every revive-tail
and normalise row, sent or not, stays open — surfaced, its keys held — for the
next publish-class holder.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from backend.persistence.records import (
    APPLIED,
    CARRIED,
    FAILED,
    HELD,
    OPEN,
    AuditRow,
    LeaseLost,
    StoreError,
)
from backend.services import flows
from backend.services import ledger as L
from backend.shopify import ops
from backend.shopify.ops import ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.recovery")

CLASS_PUBLISH = "publish"
CLASS_DECREMENT = "decrement"


@dataclass
class RecoveryResult:
    statuses: Dict[int, str] = field(default_factory=dict)  # row id → status after recovery
    settled: Dict[int, str] = field(default_factory=dict)   # group id → settle result
    left_open: List[int] = field(default_factory=list)


def recover(ctx: "flows.Context", klass: str) -> RecoveryResult:
    """Run the recovery of ``klass`` over every ``RECONCILE`` row not closed."""
    if klass not in (CLASS_PUBLISH, CLASS_DECREMENT):
        raise ValueError(f"unknown recovery class {klass!r}")
    reg = ctx.register
    out = RecoveryResult()
    rows = reg.reconcile_rows(statuses=(OPEN, APPLIED))
    now = reg.server_now()
    groups: List[int] = []
    for row in rows:  # id order: a revive's zeroing rows precede its tail
        if row.group_id is not None and row.group_id not in groups:
            groups.append(row.group_id)
        row = ctx.ledger.reload(row)  # an earlier bullet may have superseded it
        if row.status == APPLIED:
            _applied_gone(ctx, row, out)
            continue
        if row.status != OPEN:
            continue
        try:
            status = _recover_row(ctx, row, klass, now)
        except LeaseLost:
            raise
        except (ShopifyUserError, ShopifyTransportError, ProductNotFound, StoreError, RuntimeError) as e:
            ctx.report.surface("recovery_error", row_id=row.row_id, error=type(e).__name__)
            status = OPEN
        out.statuses[row.row_id] = status
        if status == OPEN:
            out.left_open.append(row.row_id)
    for gid in groups:
        try:
            out.settled[gid] = ctx.ledger.settle(gid)
        except LeaseLost:
            raise
        except StoreError as e:
            ctx.report.surface("stamp_failed", group_id=gid, error=type(e).__name__)
    return out


def _applied_gone(ctx: "flows.Context", row: AuditRow, out: RecoveryResult) -> None:
    """Terminal path (ii) binds every row kind and is read first (§3.3 rule 2):
    an ``APPLIED`` row whose target reads gone closes ``CARRIED`` — what it moved
    stays netted (Groups) — instead of waiting for, or taking, its group's stamp.
    Otherwise the ``APPLIED`` bullet: nothing on Shopify, the settle below."""
    try:
        what = _gone(ctx, row)
        if what:
            ctx.report.surface("target_gone", row_id=row.row_id, what=what)
            out.statuses[row.row_id] = ctx.ledger.close(row, CARRIED, carried=True, reason=f"{what}_gone")
    except LeaseLost:
        raise
    except (ShopifyUserError, ShopifyTransportError, StoreError, RuntimeError) as e:
        ctx.report.surface("recovery_error", row_id=row.row_id, error=type(e).__name__)


# --- terminal path (ii) --------------------------------------------------------
def _gone(ctx: "flows.Context", row: AuditRow) -> Optional[str]:
    """What of the row's targets reads gone (a definitive null), if anything."""
    p = L.payload(row)
    if row.action in L.INVENTORY_ACTIONS and p.get("inventory_item_id"):
        read = ops.read_level(ctx.transport, p["inventory_item_id"], p["location_id"])
        return None if read["item_found"] else "inventory_item"
    if row.action in L.STATE_ACTIONS and row.product_id is not None:
        try:
            ops.get_product_core(ctx.transport, flows.product_gid(row.product_id))
        except ProductNotFound:
            return "product"
    return None


def _close_gone(ctx: "flows.Context", row: AuditRow, what: str) -> str:
    led = ctx.ledger
    ctx.report.surface("target_gone", row_id=row.row_id, what=what)
    if row.sent_at is None:
        return led.never_sent(row, f"{what}_gone")
    if L.is_minus(row):
        return led.close(row, FAILED, possibly_applied=True, reason=f"{what}_gone")
    if L.is_plus(row) or L.purpose(row) == L.P_RELEVEL or row.action in (L.A_ACTIVATE, L.A_CREATE):
        return led.close(row, HELD, reason=f"{what}_gone")   # a null that is no deletion must not re-add
    return led.close(row, FAILED, reason=f"{what}_gone")


def _leave(ctx: "flows.Context", row: AuditRow, why: str) -> str:
    ctx.report.surface("left_for_publish", row_id=row.row_id, action=row.action, why=why,
                       keys=[list(k) for k in row.keys])
    return OPEN


# --- the bullets ------------------------------------------------------------------
def _recover_row(ctx: "flows.Context", row: AuditRow, klass: str, now: Any) -> str:
    led = ctx.ledger
    pub = klass == CLASS_PUBLISH
    act = row.action
    what = _gone(ctx, row)
    if what:
        return _close_gone(ctx, row, what)

    if row.sent_at is None:
        if act in L.INVENTORY_ACTIONS or act == L.A_CREATE:
            return led.never_sent(row, "never_sent_recovered")
        if act in (L.A_TAIL, L.A_NORMALISE):
            if not pub:
                return _leave(ctx, row, "state_row_owed_to_publish")
            return flows.run_tail(ctx, row) if act == L.A_TAIL else flows.run_normalise(ctx, row)
        return OPEN

    if act == L.A_ADJUST:
        pur = L.purpose(row)
        if pur == L.P_LOWER:
            return _minus(ctx, row, now)
        if pur in L.EVENTLESS_LOWERINGS:
            return _lowering(ctx, row, now)
        if not pub:
            return _leave(ctx, row, "plus_owed_to_publish")
        if led.in_window(row, now):
            _cls, status = led.resend_adjust(row)
            return status
        return led.close(row, HELD, reason="past_window_unknown_plus")
    if act == L.A_ACTIVATE:
        if pub and led.in_window(row, now):
            return led.resend_activate(row)
        return led.activation_by_read(row, recovered=True)
    if act == L.A_DEACTIVATE:
        return led.deactivation_by_read(row, recovered=True)
    if act == L.A_CREATE:
        if not pub:
            return _leave(ctx, row, "create_owed_to_publish")
        return _create(ctx, row)
    if act in (L.A_TAIL, L.A_NORMALISE):
        if not pub:
            return _leave(ctx, row, "state_row_owed_to_publish")
        return flows.run_tail(ctx, row) if act == L.A_TAIL else flows.run_normalise(ctx, row)
    return OPEN


def _minus(ctx: "flows.Context", row: AuditRow, now: Any) -> str:
    """An ``OPEN`` sent −Δ: one identical re-send inside the window; past it, or
    on that re-send's unknown / CONCURRENT answer, re-applied AT ONCE, relative
    to live ``available``, bounded by rule 4, as a new row."""
    led = ctx.ledger
    if led.in_window(row, now):
        _cls, status = led.resend_adjust(row)
        if status != OPEN:
            return status
        row = led.reload(row)
    return reapply(ctx, row, "unknown_minus")


def reapply(ctx: "flows.Context", row: AuditRow, why: str) -> str:
    led = ctx.ledger
    p = L.payload(row)
    read = ops.read_level(ctx.transport, p["inventory_item_id"], p["location_id"])
    if not read["item_found"]:
        return led.close(row, FAILED, possibly_applied=True, reason="inventory_item_gone")
    lvl = read["level"] or {}
    a = int(lvl.get("available") or 0)
    if a <= 0:
        # nothing left to lower now: the events stay pending, the next pass applies rule 4
        return led.close(row, FAILED, possibly_applied=True, reason="nothing_to_lower_at_reapply")
    s = -min(-L.share(row), a)
    spec = L.RowSpec(
        action=L.A_ADJUST, keys=row.keys, event_ids=tuple(sorted(row.event_ids)), purpose=L.P_LOWER, share=s,
        inventory_item_id=p["inventory_item_id"], location_id=p["location_id"], variant_id=p.get("variant_id"),
        product_id=row.product_id, extra={"reapply_of": row.row_id},
    )
    new_id = ctx.register.supersede(led.lease, row, spec.to_insert(actor=ctx.register.actor, group_id=row.group_id),
                                    reason=why)
    ctx.report.surface("possible_extra_minus_one", row_id=new_id, predecessor=row.row_id,
                       keys=[list(k) for k in row.keys])
    return led.send_adjust(led.register.reconcile_rows(row_ids=[new_id])[0])


def _lowering(ctx: "flows.Context", row: AuditRow, now: Any) -> str:
    """An event-less lowering: re-sent identical inside the window; past it, or
    on an unknown answer, re-computed from live as a new row (its target is a
    level, so a landed first write leaves nothing more to lower). An excess row
    is left to the held-key sweep that follows recovery, which re-derives it."""
    led = ctx.ledger
    if led.in_window(row, now):
        _cls, status = led.resend_adjust(row)
        if status != OPEN:
            return status
        row = led.reload(row)
    if L.purpose(row) == L.P_EXCESS:
        return led.close(row, FAILED, reason="re_derived_by_the_held_key_sweep")
    p = L.payload(row)
    read = ops.read_level(ctx.transport, p["inventory_item_id"], p["location_id"])
    lvl = read["level"] if read["item_found"] else None
    target = int(p.get("target") or 0)
    if lvl is None or int(lvl.get("available") or 0) <= target:
        return led.close(row, FAILED, reason="nothing_left_to_lower")
    spec = L.RowSpec(
        action=L.A_ADJUST, keys=row.keys, purpose=L.purpose(row), target=target,
        inventory_item_id=p["inventory_item_id"], location_id=p["location_id"], variant_id=p.get("variant_id"),
        product_id=row.product_id, extra={"reapply_of": row.row_id},
    )
    new_id = ctx.register.supersede(led.lease, row, spec.to_insert(actor=ctx.register.actor, group_id=row.group_id),
                                    reason="recomputed_from_live")
    return led.send_adjust(led.register.reconcile_rows(row_ids=[new_id])[0])


def _create(ctx: "flows.Context", row: AuditRow) -> str:
    """A CREATE failed after step 1 or ``OPEN``: with its product id, the
    orphan is drafted and zeroed and the row closes ``HELD`` (every key of its
    Δ-per-size list held); with no id, the orphan is searched by its recorded
    title among the SKU's outlet matches, collection membership regardless —
    not found: the row stays ``OPEN`` ("orphan unknown"), keys held, surfaced."""
    led = ctx.ledger
    num = row.product_id
    if num is None:
        num = _find_orphan(ctx, row)
        if num is None:
            ctx.report.surface("orphan_unknown", row_id=row.row_id, title=L.payload(row).get("title"),
                               keys=[list(k) for k in row.keys])
            return OPEN
        ctx.register.record_outcome(led.lease, row.row_id, expect=OPEN, product_id=num, step="orphan_found")
        row = led.reload(row)
    flows.orphan(ctx, row, flows.product_gid(num))
    return led.close(row, HELD, reason="create_unfinished")


def _find_orphan(ctx: "flows.Context", row: AuditRow) -> Optional[int]:
    title = L.payload(row).get("title")
    if not title:
        return None
    found = set()
    for sku in sorted({k[0] for k in row.keys}):
        res = ctx.resolve(sku)
        for m in res.matches:
            if (m.get("title") or "") == title:
                num = flows.product_number(m.get("product_gid"))
                if num is not None:
                    found.add(num)
    if len(found) == 1:
        return found.pop()
    if len(found) > 1:
        ctx.report.surface("orphan_ambiguous", row_id=row.row_id, candidates=sorted(found))
    return None


__all__ = ["CLASS_PUBLISH", "CLASS_DECREMENT", "RecoveryResult", "recover", "reapply"]
