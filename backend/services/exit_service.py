"""The multi-outlet exit — plan ``docs/f7-mysql-adapter-plan.md`` §3.5 (§8's risk:
a SKU with two or more outlet products; its keys held by the baseline, §3.4).

A person chooses the outlet to keep. On each OTHER outlet the app lowers,
relatively, every Promo variant with ``available`` > 0 to 0 — one ``RECONCILE``
row each: every real-size variant (``exit``), not only the held key's, since the
delete predicate is product-wide; and the customisation-only ones as rule 7 lowers
them (``custom``) — then deletes that outlet by the deletion order's steps (1)–(3)
(``delete_service.delete_product``). Step (0)'s hold is the keys' existing hold (no
new row); step (4) is skipped: ``ONLINE`` stays untouched, the shelf pairs stay on
the kept outlet. A variant the predicate still refuses after the lowering (a
negative, a ``committed`` unit, a CONTINUE variant, stock at another location)
stops the exit for that outlet BEFORE its delete, listed. Each held key is then
released by a per-key re-level on the kept outlet (§3.4) — listed, not done here.

It runs under the pass lease as a decrement-class holder
(:class:`~backend.services.passes.Holder`), yielding between products.
"""
from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.persistence.records import APPLIED, DONE, FAILED, Key, LeaseLost, StoreError
from backend.services import delete_service as D
from backend.services import flows, passes, resolvers
from backend.services import ledger as L
from backend.services import variants as V
from backend.services.lease import HOLDER_EXIT, LeaseUnavailable, PassLease
from backend.shopify import ops
from backend.shopify.ops import ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.exit_service")

# Plan-level refusals (nothing to exit).
R_BLANK_SKU = "blank_sku"
R_UNRESOLVED = "unresolved"
R_NOT_MULTI = "not_multi_outlet"
R_KEEP = "keep_not_an_outlet_of_the_sku"

# Report / outcome statuses.
ST_APPLIED = "APPLIED"
ST_REFUSED = "REFUSED"
ST_VERIFY_FAILED = "VERIFY_FAILED"
ST_LEASE_UNAVAILABLE = "LEASE_UNAVAILABLE"
ST_INCOMPLETE = "INCOMPLETE"
O_STOPPED = "STOPPED"            # the predicate still refuses after the lowering: not deleted, listed
O_PRODUCT_GONE = "PRODUCT_GONE"

EXIT_TAKE_BOUND_S = passes.HOLDER_TAKE_BOUND_S
_SHOPIFY_ERRORS = (ShopifyUserError, ShopifyTransportError, ProductNotFound)


@dataclass(frozen=True)
class ExitLowering:
    variant_id: str
    inventory_item_id: str
    title: str
    available: int
    customisation_only: bool


@dataclass(frozen=True)
class ExitOutlet:
    product_gid: str
    title: Optional[str]
    status: str
    lowerings: Tuple[ExitLowering, ...]
    refusals: Tuple[str, ...]            # what the predicate would still refuse once lowered


@dataclass(frozen=True)
class ExitPlan:
    dry_run: bool
    sku: str
    keep_gid: str
    keep_title: Optional[str]
    outlets: Tuple[ExitOutlet, ...]       # the outlets to lower and delete
    keys: Tuple[Key, ...]                 # the SKU's shelf keys: a re-level each, on the kept outlet
    refused: Optional[str]
    plan_hash: str


@dataclass(frozen=True)
class ExitOutcome:
    product_gid: str
    status: str
    warnings: Tuple[str, ...] = ()
    lowered_rows: Tuple[int, ...] = ()
    closed_rows: Tuple[int, ...] = ()


@dataclass(frozen=True)
class ExitReport:
    status: str
    sku: str
    keep_gid: str
    outcomes: Tuple[ExitOutcome, ...] = ()
    keys_to_relevel: Tuple[Key, ...] = ()
    reason: Optional[str] = None
    surfaced: Tuple[Dict[str, Any], ...] = ()


def _hash(sku: str, keep_gid: str, others: List[str]) -> str:
    raw = f"{sku}\x1f{keep_gid}\x1f" + "|".join(sorted(others))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _lowerings(inv: List[Dict[str, Any]], promo: str) -> Tuple[ExitLowering, ...]:
    out = []
    for v in inv:
        a = V.promo_available(v, promo)
        if a is not None and a > 0:
            out.append(ExitLowering(v.get("id") or "", V.item_id(v) or "", V.variant_title(v), int(a),
                                    V.is_customisation_only(v)))
    return tuple(out)


def _refusals_once_lowered(inv: List[Dict[str, Any]], promo: str) -> Tuple[str, ...]:
    """The delete predicate on the product as the lowering would leave it (every
    positive Promo ``available`` at 0): what still refuses it."""
    lowered = []
    for v in inv:
        levels = []
        for lvl in v.get("levels") or []:
            if lvl.get("location_id") == promo and (lvl.get("available") or 0) > 0:
                lvl = {**lvl, "available": 0}
            levels.append(lvl)
        lowered.append({**v, "levels": levels})
    bucket, reasons = D.classify_outlet(lowered, promo)
    return () if bucket == D.BUCKET_CANDIDATE else (reasons or (bucket.lower(),))


def exit_preview(register: Any, transport: Any, sku: str, keep_gid: str, *, promo_location_id: str) -> ExitPlan:
    """READ-ONLY. The SKU's outlets (a clean resolution with two or more), the one
    to keep, and for every other: its lowerings and what the predicate would still
    refuse; the SKU's shelf keys (each released by a re-level on the kept outlet)."""
    sku = (sku or "").strip()
    keys = tuple(sorted({p.key for p in register.shelf(sku)})) if sku else ()

    def refused(why: str, keep_title: Optional[str] = None) -> ExitPlan:
        return ExitPlan(True, sku, keep_gid, keep_title, (), keys, why, _hash(sku, keep_gid, [why]))

    if not sku:
        return refused(R_BLANK_SKU)
    res = resolvers.outlet_resolver(transport, sku)
    if res.get("truncated"):
        return refused(R_UNRESOLVED)
    matches = {m["product_gid"]: m for m in res.get("matches") or ()}
    if len(matches) < 2:
        return refused(R_NOT_MULTI)
    if keep_gid not in matches:
        return refused(R_KEEP)
    others: List[ExitOutlet] = []
    for gid in sorted(g for g in matches if g != keep_gid):
        inv = ops.read_variant_inventory(transport, gid)
        m = matches[gid]
        others.append(ExitOutlet(gid, m.get("title"), (m.get("status") or "").upper(),
                                 _lowerings(inv, promo_location_id), _refusals_once_lowered(inv, promo_location_id)))
    return ExitPlan(True, sku, keep_gid, matches[keep_gid].get("title"), tuple(others), keys, None,
                    _hash(sku, keep_gid, [o.product_gid for o in others]))


def _at_target(row: Any) -> bool:
    """A lowering row applied — or never sent because the level already read 0."""
    if row.status in (APPLIED, DONE):
        return True
    return row.status == FAILED and (row.outcome or {}).get("reason") == "nothing_to_lower"


def _exit_one(ctx: "flows.Context", audit_sink: Any, gid: str) -> ExitOutcome:
    """One non-kept outlet: lower every positive Promo to 0, re-read, delete."""
    try:
        inv = ctx.inventory(gid)
    except ProductNotFound:
        return ExitOutcome(gid, O_PRODUCT_GONE, ("product_gone",))
    num = flows.product_number(gid)
    specs: List[L.RowSpec] = []
    for low in _lowerings(inv, ctx.promo):
        v = next(x for x in inv if x.get("id") == low.variant_id)
        vsku = (v.get("sku") or "").strip()
        keys = () if low.customisation_only or not vsku else ((vsku, low.title),)
        specs.append(L.RowSpec(action=L.A_ADJUST, keys=keys, purpose=L.P_CUSTOM if low.customisation_only else L.P_EXIT,
                               target=0, inventory_item_id=low.inventory_item_id, location_id=ctx.promo,
                               variant_id=low.variant_id, product_id=num))
    lowered: Tuple[int, ...] = ()
    if specs:
        rows = ctx.ledger.write_group(specs)
        for r in rows:
            ctx.ledger.send_adjust(r)
        ctx.ledger.settle(rows[0].group_id)
        lowered = tuple(r.row_id for r in rows)
        left = [r.row_id for r in (ctx.ledger.reload(r) for r in rows) if not _at_target(r)]
        if left:
            return ExitOutcome(gid, O_STOPPED, (f"lowering_not_applied:{','.join(str(i) for i in left)}",), lowered)
    try:
        inv = ctx.inventory(gid)
    except ProductNotFound:
        return ExitOutcome(gid, O_PRODUCT_GONE, ("product_gone",), lowered)
    bucket, reasons = D.classify_outlet(inv, ctx.promo)
    if bucket != D.BUCKET_CANDIDATE:
        ctx.report.surface("exit_stopped", product_id=num, reasons=list(reasons) or [bucket.lower()])
        return ExitOutcome(gid, O_STOPPED, tuple(reasons) or (bucket.lower(),), lowered)
    out = D.delete_product(ctx, audit_sink, gid)
    return ExitOutcome(gid, out.status, out.warnings, lowered, out.closed_rows)


def exit_apply(
    register: Any,
    transport: Any,
    audit_sink: Any,
    approved: ExitPlan,
    *,
    promo_location_id: str,
    lease: Optional[PassLease] = None,
    take_bound_s: float = EXIT_TAKE_BOUND_S,
    sleep: Callable[[float], None] = time.sleep,
) -> ExitReport:
    """Execute the approved exit under the pass lease, one outlet at a time."""
    if approved.refused:
        return ExitReport(ST_REFUSED, approved.sku, approved.keep_gid, reason=approved.refused)
    run = passes.Holder(register, transport, promo_location_id, HOLDER_EXIT, lease=lease,
                        take_bound_s=take_bound_s, sleep=sleep)
    outcomes: List[ExitOutcome] = []
    try:
        run.start()
        bind = getattr(audit_sink, "bind", None)
        if callable(bind):
            bind(run.lease)
        fresh = exit_preview(register, transport, approved.sku, approved.keep_gid,
                             promo_location_id=promo_location_id)
        if fresh.refused or fresh.plan_hash != approved.plan_hash:
            return ExitReport(ST_VERIFY_FAILED, approved.sku, approved.keep_gid,
                              reason=fresh.refused or "plan_hash_mismatch:live_state_changed",
                              surfaced=tuple(run.report.items))
        for o in fresh.outlets:
            run.next_item()
            try:
                outcomes.append(_exit_one(run.ctx, audit_sink, o.product_gid))
            except _SHOPIFY_ERRORS as e:
                outcomes.append(ExitOutcome(o.product_gid, D.STATUS_ERROR, (f"shopify_error:{type(e).__name__}",)))
        return ExitReport(ST_APPLIED, approved.sku, approved.keep_gid, tuple(outcomes), fresh.keys,
                          surfaced=tuple(run.report.items))
    except LeaseUnavailable:
        status = ST_INCOMPLETE if run.lease.takes else ST_LEASE_UNAVAILABLE
        return ExitReport(status, approved.sku, approved.keep_gid, tuple(outcomes), reason="lease",
                          surfaced=tuple(run.report.items))
    except LeaseLost:
        return ExitReport(ST_INCOMPLETE, approved.sku, approved.keep_gid, tuple(outcomes), reason="lease_lost",
                          surfaced=tuple(run.report.items))
    except StoreError as e:
        return ExitReport(ST_INCOMPLETE, approved.sku, approved.keep_gid, tuple(outcomes),
                          reason=f"register_error:{type(e).__name__}", surfaced=tuple(run.report.items))
    finally:
        run.release()


__all__ = [
    "R_BLANK_SKU", "R_UNRESOLVED", "R_NOT_MULTI", "R_KEEP",
    "ST_APPLIED", "ST_REFUSED", "ST_VERIFY_FAILED", "ST_LEASE_UNAVAILABLE", "ST_INCOMPLETE",
    "O_STOPPED", "O_PRODUCT_GONE",
    "ExitLowering", "ExitOutlet", "ExitPlan", "ExitOutcome", "ExitReport",
    "exit_preview", "exit_apply",
]
