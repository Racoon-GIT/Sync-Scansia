"""PUBLISH — the Δ > 0 flows of plan ``docs/f7-mysql-adapter-plan.md`` §3.3's
table (add / revive / create), on the register and the ``RECONCILE`` ledger.

* :func:`publish_preview` — READ-ONLY. ``pending_deltas()`` (refused before the
  whole-register ``BASELINE`` row) grouped by exact (SKU, SIZE), Δ net of what
  ``CARRIED`` rows already moved; a key held, a key with N 0 (its +Δ is never
  added — the N-0 release's), a blank SKU (nothing resolves ``''``, rule 7) are
  surfaced, never planned. One price and one compare-at per SKU (§3.5, B-2 /
  D20 + D25): the one non-NULL ``PRICE_OUTLET`` among its shelf pairs (two
  values → the SKU listed and blocked); compare-at ``PRICE_HIGH`` where > 0,
  else ``PRICE`` where ``PRICE_HIGH`` is 0.00 or NULL (two values → blocked; a
  pair with neither keeps ITS +1 out, never lent another pair's). FIX4 on both.
* :func:`publish_apply` — MUTATES, confirm-gated by the caller. Under the pass
  lease (taken; released and re-taken between SKUs, so a waiting decrement gets
  in — the hand-off, rule 6), it re-derives each SKU's plan as it is now and
  writes only what the approved plan bounds: a SKU absent from it gets no add; a
  re-derived +Δ larger than the confirmed one is deferred to the next preview; a
  SKU whose outlet gid or status changed → ``VERIFY_FAILED``.

The branches (rule 7: every variant a flow exposes is DENY + tracked first):

* **ACTIVE** — a ``normalise`` row (DENY + tracked + the SKU's prices on every
  variant), an explicit activation where the level is absent or inactive, then
  each ``add`` (its activation ``APPLIED`` first) — relative, fenced by
  ``changeFromQuantity``, keyed; no level precondition (``frozen_pre`` retired).
* **DRAFT revive** — deferred while any ``RECONCILE`` row not closed records the
  product or one of its keys; first lowers to 0, relatively, every real-size
  Promo variant with no shelf pair, every held key's real-size variant and every
  customisation-only variant with Promo > 0; activation, adds and the tail
  (``revive-tail``: DENY + tracked, ACTIVE, publish) only once every zeroing row
  has applied — else nothing is activated, added or exposed.
* **CREATE** — the ``create`` row written before ``productDuplicate`` (DRAFT
  atomically), the duplicate's id recorded as soon as it answers; zero +
  disconnect every non-Promo level, zero every Promo variant, activate and add
  the returned sizes, DENY + tracked + prices, then ACTIVE and publish LAST. Any
  failure after step 1 → the orphan drafted and zeroed, the row ``HELD``.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

from backend.persistence.records import (
    APPLIED,
    FAILED,
    HELD,
    NOT_CLOSED,
    OPEN,
    LeaseLost,
    PairRow,
    StoreError,
)
from backend.services import flows
from backend.services import ledger as L
from backend.services import passes, resolvers, variants
from backend.services.lease import HOLDER_PUBLISH, LeaseUnavailable, PassLease
from backend.services.recovery import CLASS_PUBLISH
from backend.shopify import ops
from backend.shopify.ops import ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyOutcomeUnknown, ShopifyTransportError

logger = logging.getLogger("backend.services.outlet_service")

BRANCH_CREATE = "CREATE"
BRANCH_ACTIVE = "ACTIVE"
BRANCH_DRAFT = "DRAFT"

# How long a publish waits for the lease (a decrement in flight).
PUBLISH_TAKE_BOUND_S = 120.0

_STATUS_BY_REASON = {
    "no_source": "NO_SOURCE",
    "multi_outlet": "MULTI_OUTLET",
    "multi_source": "MULTI_SOURCE",
    "price_invalid": "PRICE_INVALID",
    "price_conflict": "PRICE_CONFLICT",
    "quarantine": "QUARANTINED",
    "unexpected_status": "SKIPPED",
    "unresolved": "UNRESOLVED",
}

product_gid = flows.product_gid
product_number = flows.product_number


# =============================================================================
# Plan data model
# =============================================================================
@dataclass(frozen=True)
class SizeTarget:
    """One (SKU, SIZE) group with Δ > 0: the events it stamps once written."""

    size: str
    event_ids: Tuple[int, ...]
    delta: int
    matched: bool
    variant_id: Optional[str]
    inventory_item_id: Optional[str]
    twins: Tuple[str, ...] = ()


@dataclass(frozen=True)
class PlanAction:
    sku: str
    branch: Optional[str]
    publishable: bool
    reason: Optional[str]
    target_gid: Optional[str]
    source_gid: Optional[str]
    outlet_title: Optional[str]
    live_status: Optional[str]
    price: Optional[str]
    compare_at: Optional[str]
    price_ok: bool
    size_targets: Tuple[SizeTarget, ...]
    unmatched_sizes: Tuple[str, ...]
    warnings: Tuple[str, ...]
    plan_hash: str


@dataclass(frozen=True)
class Plan:
    dry_run: bool
    actions: Tuple[PlanAction, ...]
    anomalies: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ApplyOutcome:
    sku: str
    branch: Optional[str]
    status: str
    target_gid: Optional[str]
    warnings: Tuple[str, ...] = ()
    stamped_event_ids: Tuple[int, ...] = ()


@dataclass(frozen=True)
class ApplyReport:
    outcomes: Tuple[ApplyOutcome, ...] = field(default_factory=tuple)
    surfaced: Tuple[Dict[str, Any], ...] = ()


# =============================================================================
# Pure helpers
# =============================================================================
def _money(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else f"{Decimal(value):.2f}"


def _plan_hash(anchor: Optional[str], branch: Optional[str], live_status: Optional[str]) -> str:
    raw = f"{anchor or ''}|{branch or ''}|{(live_status or '').upper()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def validate_price(price: Optional[str], compare_at: Optional[str]) -> Tuple[bool, Optional[str]]:
    """FIX4/D3: reject missing/zero price, missing/zero compare-at, price ≥ compare-at."""
    if not price:
        return False, "price_missing"
    try:
        p = float(price)
    except (TypeError, ValueError):
        return False, "price_unparseable"
    if p <= 0:
        return False, "price_zero"
    if not compare_at:
        return False, "compare_at_missing"
    try:
        c = float(compare_at)
    except (TypeError, ValueError):
        return False, "compare_at_unparseable"
    if c <= 0:
        return False, "compare_at_zero"
    if p >= c:
        return False, "price_ge_compare_at"
    return True, None


def compare_at_of(pair: PairRow) -> Optional[Decimal]:
    """B-2 (D20 + D25): ``PRICE_HIGH`` where > 0, else ``PRICE`` where
    ``PRICE_HIGH`` is 0.00 or NULL; neither → None (the pair stays out)."""
    if pair.price_high is not None and pair.price_high > 0:
        return pair.price_high
    return pair.price


@dataclass(frozen=True)
class SkuPrices:
    price: Optional[str]
    compare_at: Optional[str]
    blocked: Optional[str]                 # price_conflict / compare_at_conflict
    no_compare_at_pairs: Tuple[int, ...]   # pairs kept out (D25)


def sku_prices(shelf: List[PairRow]) -> SkuPrices:
    prices = {p.price_outlet for p in shelf if p.price_outlet is not None}
    cats = {compare_at_of(p) for p in shelf if compare_at_of(p) is not None}
    missing = tuple(p.pair_id for p in shelf if compare_at_of(p) is None)
    blocked = None
    if len(prices) > 1:
        blocked = "price_conflict"
    elif len(cats) > 1:
        blocked = "compare_at_conflict"
    return SkuPrices(
        price=_money(next(iter(prices))) if len(prices) == 1 else None,
        compare_at=_money(next(iter(cats))) if len(cats) == 1 else None,
        blocked=blocked, no_compare_at_pairs=missing,
    )


def outlet_title(title: str) -> str:
    t = (title or "").strip()
    return t if t.casefold().endswith("outlet") else f"{t} - Outlet"


def _reason_status(reason: Optional[str]) -> str:
    return _STATUS_BY_REASON.get(reason or "", "SKIPPED")


def _nonpub(sku: str, branch: Optional[str], reason: str, *, warnings: List[str], plan_hash: str,
            target_gid: Optional[str] = None, source_gid: Optional[str] = None,
            outlet_title: Optional[str] = None, live_status: Optional[str] = None,
            price: Optional[str] = None, compare_at: Optional[str] = None) -> PlanAction:
    return PlanAction(sku=sku, branch=branch, publishable=False, reason=reason, target_gid=target_gid,
                      source_gid=source_gid, outlet_title=outlet_title, live_status=live_status, price=price,
                      compare_at=compare_at, price_ok=False, size_targets=(), unmatched_sizes=(),
                      warnings=tuple(warnings), plan_hash=plan_hash)


# =============================================================================
# Planning — shared by the preview and the apply's per-SKU re-derivation
# =============================================================================
@dataclass(frozen=True)
class _SkuGroups:
    sku: str
    groups: Tuple[passes.KeyGroup, ...]   # Δ > 0, not held, N > 0, the D25 pairs' events out


def _positive_groups(register: Any, anomalies: List[str]) -> Dict[str, _SkuGroups]:
    pend = register.pending_deltas()
    held = pend.held_keys
    shelf_by_key: Dict[Tuple[str, str], int] = {}
    for p in register.shelf():
        shelf_by_key[p.key] = shelf_by_key.get(p.key, 0) + 1
    for e in pend.keyless:
        anomalies.append(f"consume_miss:{e.event_id}")
    out: Dict[str, List[passes.KeyGroup]] = {}
    for g in passes.key_groups(pend):
        sku, size = g.key
        if g.delta <= 0:
            continue
        if not sku:
            anomalies.append(f"blank_sku:{size}:{g.delta}")
            continue
        if g.key in held:
            anomalies.append(f"held_positive:{sku}:{size}:{g.delta}")
            continue
        if shelf_by_key.get(g.key, 0) == 0:
            anomalies.append(f"n0_positive:{sku}:{size}:{g.delta}")   # never added (§3.3)
            continue
        out.setdefault(sku, []).append(g)
    return {s: _SkuGroups(s, tuple(gs)) for s, gs in out.items()}


def _targets(sku: str, groups: Tuple[passes.KeyGroup, ...], live: List[Dict[str, Any]],
             out_pairs: Set[int]) -> Tuple[List[SizeTarget], List[str], List[str]]:
    targets: List[SizeTarget] = []
    unmatched: List[str] = []
    warns: List[str] = []
    for g in groups:
        events = tuple(e for e in g.events if e.pair_id not in out_pairs)
        if len(events) != len(g.events):
            warns.append(f"no_compare_at_pairs_kept_out:{g.key[1]}")
        delta = g.delta - sum(e.delta for e in g.events if e.pair_id in out_pairs)
        if delta <= 0:
            continue
        v = variants.match_exact(g.key[1], live)
        twin_titles = tuple(variants.variant_title(t) for t in variants.twins(g.key[1], live))
        if v is None:
            unmatched.append(g.key[1])
            if twin_titles:
                warns.append(f"size_twin:{g.key[1]}:{'|'.join(twin_titles)}")
        elif v.get("sku") and str(v["sku"]).strip() and str(v["sku"]).strip() != sku:
            warns.append(f"sku_mismatch_on_size:{g.key[1]}:{str(v['sku']).strip()}")
        targets.append(SizeTarget(size=g.key[1], event_ids=tuple(e.event_id for e in events), delta=delta,
                                  matched=v is not None, variant_id=v.get("id") if v else None,
                                  inventory_item_id=variants.item_id(v), twins=twin_titles))
    return targets, unmatched, warns


def _plan_sku(register: Any, transport: Any, promo: str, sg: _SkuGroups) -> PlanAction:
    sku = sg.sku
    warnings: List[str] = []
    shelf = register.shelf(sku)
    pr = sku_prices(shelf)
    ok, why = validate_price(pr.price, pr.compare_at)
    out_pairs = set(pr.no_compare_at_pairs)
    q_gid = product_gid(next((p.outlet_product_id for p in shelf if p.outlet_product_id), None))
    try:
        outlet_res = resolvers.outlet_resolver(transport, sku)
    except (ShopifyTransportError, ShopifyUserError, RuntimeError) as e:
        return _nonpub(sku, None, "unresolved", warnings=[f"resolution_failed:{type(e).__name__}"],
                       plan_hash=_plan_hash(sku, "unresolved", ""))
    if outlet_res.get("truncated"):
        return _nonpub(sku, None, "unresolved", warnings=[outlet_res.get("warning") or "truncated"],
                       plan_hash=_plan_hash(sku, "unresolved", ""))
    if outlet_res.get("warning"):
        warnings.append(outlet_res["warning"])
    matches = outlet_res.get("matches") or []
    chosen: Optional[Dict[str, Any]] = None
    if q_gid:
        chosen = next((m for m in matches if m["product_gid"] == q_gid), None)
    if chosen is None and len(matches) == 1:
        chosen = matches[0]
    if chosen is None and len(matches) > 1:
        return _nonpub(sku, None, "multi_outlet", warnings=warnings, price=pr.price, compare_at=pr.compare_at,
                       plan_hash=_plan_hash(sku, "multi_outlet", ""))
    if pr.blocked:
        return _nonpub(sku, None, "price_conflict", warnings=warnings + [pr.blocked], price=pr.price,
                       compare_at=pr.compare_at, plan_hash=_plan_hash(sku, "price_conflict", ""))
    if chosen is not None:
        gid = chosen["product_gid"]
        status = (chosen.get("status") or "").upper()
        ph = _plan_hash(gid, status if status in (BRANCH_ACTIVE, BRANCH_DRAFT) else "OTHER", status)
        if status not in (BRANCH_ACTIVE, BRANCH_DRAFT):
            return _nonpub(sku, None, "unexpected_status", warnings=warnings + [f"unexpected_status:{status}"],
                           target_gid=gid, live_status=status, price=pr.price, compare_at=pr.compare_at, plan_hash=ph)
        if not ok:
            return _nonpub(sku, status, "price_invalid", warnings=warnings + [f"price:{why}"], target_gid=gid,
                           live_status=status, price=pr.price, compare_at=pr.compare_at, plan_hash=ph)
        inv = ops.read_variant_inventory(transport, gid)
        if variants.has_non_promo_stock(inv, promo):
            return _nonpub(sku, status, "quarantine", warnings=warnings + ["quarantine:non_promo_stock_inherited"],
                           target_gid=gid, live_status=status, price=pr.price, compare_at=pr.compare_at, plan_hash=ph)
        targets, unmatched, tw = _targets(sku, sg.groups, inv, out_pairs)
        extra = ["draft_revive:fresh_return"] if status == BRANCH_DRAFT else []
        return PlanAction(sku=sku, branch=status, publishable=True, reason=None, target_gid=gid, source_gid=None,
                          outlet_title=None, live_status=status, price=pr.price, compare_at=pr.compare_at,
                          price_ok=True, size_targets=tuple(targets), unmatched_sizes=tuple(unmatched),
                          warnings=tuple(warnings + extra + tw), plan_hash=ph)
    source_res = resolvers.source_resolver(transport, sku)
    if source_res.get("warning"):
        warnings.append(source_res["warning"])
    srcs = source_res.get("matches") or []
    if not srcs:
        return _nonpub(sku, None, "no_source", warnings=warnings, price=pr.price, compare_at=pr.compare_at,
                       plan_hash=_plan_hash(sku, "no_source", ""))
    if len(srcs) > 1:
        return _nonpub(sku, None, "multi_source", warnings=warnings, price=pr.price, compare_at=pr.compare_at,
                       plan_hash=_plan_hash(sku, "multi_source", ""))
    source_gid = srcs[0]["product_gid"]
    title = outlet_title(srcs[0].get("title") or "")
    ph = _plan_hash(source_gid, BRANCH_CREATE, "")
    if not ok:
        return _nonpub(sku, BRANCH_CREATE, "price_invalid", warnings=warnings + [f"price:{why}"],
                       source_gid=source_gid, outlet_title=title, price=pr.price, compare_at=pr.compare_at, plan_hash=ph)
    src_variants = ops.get_product_variants(transport, source_gid)
    targets, unmatched, tw = _targets(sku, sg.groups, src_variants, out_pairs)
    return PlanAction(sku=sku, branch=BRANCH_CREATE, publishable=True, reason=None, target_gid=None,
                      source_gid=source_gid, outlet_title=title, live_status=None, price=pr.price,
                      compare_at=pr.compare_at, price_ok=True, size_targets=tuple(targets),
                      unmatched_sizes=tuple(unmatched), warnings=tuple(warnings + tw), plan_hash=ph)


def publish_preview(register: Any, transport: Any, *, promo_location_id: str) -> Plan:
    """Compute the publish PLAN without mutating Shopify or the register."""
    anomalies: List[str] = []
    groups = _positive_groups(register, anomalies)
    actions = [_plan_sku(register, transport, promo_location_id, sg) for sg in groups.values()]
    return Plan(dry_run=True, actions=tuple(actions), anomalies=tuple(anomalies))


# =============================================================================
# Apply — the branches on the ledger
# =============================================================================
def _approved_targets(fresh: PlanAction, approved: PlanAction, ctx: flows.Context) -> List[SizeTarget]:
    """The approved plan bounds what is written: a re-derived +Δ larger than the
    confirmed one is deferred to the next preview (not written, no hold)."""
    bound = {st.size: st.delta for st in approved.size_targets}
    out = []
    for st in fresh.size_targets:
        if not st.matched or st.delta <= 0:
            continue
        if st.delta > bound.get(st.size, 0):
            ctx.report.surface("deferred_larger_than_confirmed", sku=fresh.sku, size=st.size,
                               confirmed=bound.get(st.size, 0), now=st.delta)
            continue
        out.append(st)
    return out


def _add_specs(ctx: flows.Context, sku: str, targets: List[SizeTarget], inv: List[Dict[str, Any]],
               product: Optional[int]) -> List[L.RowSpec]:
    specs: List[L.RowSpec] = []
    for st in targets:
        v = variants.match_exact(st.size, inv)
        if v is None:
            continue
        key = (sku, st.size)
        lvl = flows.promo_of(v, ctx.promo)
        if lvl is None or not lvl.get("is_active", True):
            specs.append(L.RowSpec(action=L.A_ACTIVATE, keys=(key,), event_ids=st.event_ids,
                                   inventory_item_id=variants.item_id(v), location_id=ctx.promo,
                                   variant_id=v.get("id"), product_id=product))
        specs.append(L.RowSpec(action=L.A_ADJUST, keys=(key,), event_ids=st.event_ids, purpose=L.P_ADD,
                               share=st.delta, inventory_item_id=variants.item_id(v), location_id=ctx.promo,
                               variant_id=v.get("id"), product_id=product))
    return specs


def _run_adds(ctx: flows.Context, rows: List[Any]) -> None:
    """Activations, then each add only once its key's activation is APPLIED."""
    act_status: Dict[Tuple[str, str], str] = {}
    for r in rows:
        if r.action == L.A_ACTIVATE:
            act_status[r.keys[0]] = ctx.ledger.send_activate(r)
    for r in rows:
        if r.action == L.A_ADJUST and L.purpose(r) == L.P_ADD:
            st = act_status.get(r.keys[0])
            if st is not None and st != APPLIED:
                ctx.ledger.never_sent(r, "activation_not_applied")
            else:
                ctx.ledger.send_adjust(r)


def _active(ctx: flows.Context, fresh: PlanAction, approved: PlanAction) -> ApplyOutcome:
    gid = fresh.target_gid
    num = product_number(gid)
    inv = ctx.inventory(gid)
    if variants.has_non_promo_stock(inv, ctx.promo):
        return ApplyOutcome(fresh.sku, BRANCH_ACTIVE, "QUARANTINED", gid, fresh.warnings + ("quarantine:non_promo_stock",))
    targets = _approved_targets(fresh, approved, ctx)
    if not targets:
        return ApplyOutcome(fresh.sku, BRANCH_ACTIVE, "NOTHING_TO_ADD", gid, fresh.warnings)
    keys = tuple((fresh.sku, st.size) for st in targets)
    norm = L.RowSpec(action=L.A_NORMALISE, keys=keys, product_id=num,
                     extra={"price": fresh.price, "compare_at": fresh.compare_at})
    rows = ctx.ledger.write_group([norm] + _add_specs(ctx, fresh.sku, targets, inv, num))
    gid_group = rows[0].group_id
    if flows.run_normalise(ctx, rows[0]) != APPLIED:
        for r in rows[1:]:
            ctx.ledger.never_sent(r, "normalise_not_applied")
        ctx.ledger.settle(gid_group)
        return ApplyOutcome(fresh.sku, BRANCH_ACTIVE, "ERROR", gid, fresh.warnings + ("normalise_failed",))
    _run_adds(ctx, rows[1:])
    return _settled(ctx, fresh, BRANCH_ACTIVE, gid, gid_group, "APPLIED")


def _settled(ctx: flows.Context, fresh: PlanAction, branch: str, gid: Optional[str], group_id: int,
             ok_status: str) -> ApplyOutcome:
    pending_before = {e.event_id: e for e in ctx.register.unstamped_events()}
    result = ctx.ledger.settle(group_id)
    if result not in ("stamped", "closed"):     # closed: a re-level of a key with nothing unstamped
        return ApplyOutcome(fresh.sku, branch, "INCOMPLETE" if result == "waiting" else "PARTIAL", gid,
                            fresh.warnings + (f"group:{result}",))
    rows = ctx.ledger.group_rows(group_id)
    stamped = sorted(set().union(*(r.event_ids for r in rows)))
    shelf = {p.pair_id for p in ctx.register.shelf(fresh.sku)}
    to_online = sorted({pending_before[e].pair_id for e in stamped
                        if e in pending_before and pending_before[e].pair_id in shelf})
    if to_online:
        ctx.register.set_online(ctx.ledger.lease, to_online, True)
    if fresh.price is not None:
        ctx.register.set_outlet_price(ctx.ledger.lease, fresh.sku, Decimal(fresh.price), None, only_missing=True)
    flows.maybe_set_outlet_product(ctx, fresh.sku, ctx.resolve(fresh.sku))   # §3.1: any pass (a fresh CREATE is `review`)
    return ApplyOutcome(fresh.sku, branch, ok_status, gid, fresh.warnings, tuple(stamped))


def revive_outlet(ctx: flows.Context, fresh: PlanAction, approved: PlanAction, *,
            zero_titles: frozenset = frozenset()) -> ApplyOutcome:
    """``zero_titles``: real-size variants zeroed whatever their key — a
    re-level's zero-then-revive (§3.4) zeroes its own key's variant first."""
    gid = fresh.target_gid
    num = product_number(gid)
    sku = fresh.sku
    for r in ctx.register.reconcile_rows(statuses=tuple(NOT_CLOSED)):
        if r.product_id == num or any(k[0] == sku for k in r.keys):
            ctx.report.surface("revive_deferred", sku=sku, product_id=num, row_id=r.row_id)
            return ApplyOutcome(sku, BRANCH_DRAFT, "DEFERRED", gid, fresh.warnings + ("revive_deferred:open_row",))
    inv = ctx.inventory(gid)
    if variants.has_non_promo_stock(inv, ctx.promo):
        return ApplyOutcome(sku, BRANCH_DRAFT, "QUARANTINED", gid, fresh.warnings + ("quarantine:non_promo_stock",))
    targets = _approved_targets(fresh, approved, ctx)
    if not targets:
        return ApplyOutcome(sku, BRANCH_DRAFT, "NOTHING_TO_ADD", gid, fresh.warnings)
    keys = tuple((sku, st.size) for st in targets)
    shelf_sizes = {p.size for p in ctx.register.shelf(sku)}
    held = {h.key for h in ctx.register.held_keys()}

    def must_zero(v: Dict[str, Any]) -> bool:
        if variants.is_customisation_only(v):
            return True
        title = variants.variant_title(v)
        return title in zero_titles or title not in shelf_sizes or (sku, title) in held

    zero = [s for s in flows.zero_rows_for(ctx, inv, keys=keys, product=num, purpose=L.P_ZERO, which=must_zero)]
    tail = L.RowSpec(action=L.A_TAIL, keys=keys, product_id=num,
                     extra={"price": fresh.price, "compare_at": fresh.compare_at})
    rows = ctx.ledger.write_group(zero + _add_specs(ctx, sku, targets, inv, num) + [tail])
    group = rows[0].group_id
    zero_rows = [r for r in rows if r.action == L.A_ADJUST and L.purpose(r) == L.P_ZERO]
    for r in zero_rows:
        ctx.ledger.send_adjust(r)
    if flows.zeroing_applied(ctx, group) is not True:
        for r in rows:
            r = ctx.ledger.reload(r)
            if r.status == OPEN and r.sent_at is None and r.action != L.A_TAIL:
                ctx.ledger.never_sent(r, "revive_stopped_at_zeroing")
        flows.run_tail(ctx, ctx.ledger.reload(rows[-1]))   # closes it FAILED "never run"
        ctx.ledger.settle(group)
        return ApplyOutcome(sku, BRANCH_DRAFT, "REVIVE_STOPPED", gid, fresh.warnings + ("zeroing_not_applied",))
    _run_adds(ctx, [r for r in rows if r.action in (L.A_ACTIVATE, L.A_ADJUST) and L.purpose(r) != L.P_ZERO])
    flows.run_tail(ctx, ctx.ledger.reload(rows[-1]))
    return _settled(ctx, fresh, BRANCH_DRAFT, gid, group, "REVIVED")


class _CreateAbort(RuntimeError):
    pass


def create_outlet(ctx: flows.Context, fresh: PlanAction, approved: PlanAction) -> ApplyOutcome:
    sku = fresh.sku
    targets = _approved_targets(fresh, approved, ctx)
    if not targets:
        return ApplyOutcome(sku, BRANCH_CREATE, "NOTHING_TO_ADD", None, fresh.warnings)
    keys = tuple((sku, st.size) for st in targets)
    ids = sorted({i for st in targets for i in st.event_ids})
    spec = L.RowSpec(action=L.A_CREATE, keys=keys, event_ids=tuple(ids),
                     extra={"source_gid": fresh.source_gid, "title": fresh.outlet_title,
                            "deltas": {st.size: st.delta for st in targets},
                            "price": fresh.price, "compare_at": fresh.compare_at})
    (row,) = ctx.ledger.write_group([spec])
    group = row.group_id
    try:
        new_gid = ctx.ledger.step(row, "1:duplicate",
                                  lambda: ops.product_duplicate(ctx.transport, fresh.source_gid, fresh.outlet_title))
    except ShopifyUserError as e:
        ctx.ledger.close(ctx.ledger.reload(row), FAILED, refused=e.mutation, step="1:duplicate")
        return ApplyOutcome(sku, BRANCH_CREATE, "ERROR", None, fresh.warnings + ("duplicate_refused",))
    except ShopifyOutcomeUnknown:
        ctx.report.surface("orphan_unknown", row_id=row.row_id, title=fresh.outlet_title)
        return ApplyOutcome(sku, BRANCH_CREATE, "ERROR", None, fresh.warnings + ("duplicate_outcome_unknown",))
    except ShopifyTransportError:
        ctx.ledger.close(ctx.ledger.reload(row), FAILED, refused="transport", step="1:duplicate")
        return ApplyOutcome(sku, BRANCH_CREATE, "ERROR", None, fresh.warnings + ("duplicate_refused",))
    num = product_number(new_gid)
    ctx.register.record_outcome(ctx.ledger.lease, row.row_id, expect=OPEN, product_id=num, step="1:duplicate:id")
    row = ctx.ledger.reload(row)
    try:
        inv = ctx.inventory(new_gid)
        if any(v.get("levels_truncated") for v in inv):
            raise _CreateAbort("levels_truncated_after_duplicate")
        # FIX1's order: every non-Promo level zeroed and disconnected first, then
        # every Promo variant zeroed, then the returned sizes' activation + add.
        zero_all = flows.zero_rows_for(ctx, inv, keys=keys, product=num, purpose=L.P_ZERO, include_nonpromo=True)
        nonpromo = [s for s in zero_all if s.location_id != ctx.promo]
        promo_zero = [s for s in zero_all if s.location_id == ctx.promo]
        subs = nonpromo + promo_zero + _add_specs(ctx, sku, targets, inv, num)
        sub_rows = ctx.ledger.write_group(subs, group_id=group) if subs else []
        zeroing = [r for r in sub_rows if r.action == L.A_DEACTIVATE
                   or (r.action == L.A_ADJUST and L.purpose(r) in (L.P_ZERO, L.P_NONPROMO))]
        if any(st != APPLIED for st in flows.run_rows_in_order(ctx, zeroing).values()):
            raise _CreateAbort("zeroing_not_applied")
        _run_adds(ctx, [r for r in sub_rows if r not in zeroing])
        if any(ctx.ledger.reload(r).status != APPLIED for r in sub_rows if r not in zeroing):
            raise _CreateAbort("add_not_applied")
        price_extra = {"price": fresh.price, "compareAtPrice": fresh.compare_at}
        ctx.ledger.step(row, "5:bulk", lambda: ops.product_variants_bulk_update(
            ctx.transport, new_gid, ops.deny_tracked_inputs(inv, **price_extra)))
        ctx.ledger.step(row, "6:active", lambda: ops.product_update_status(ctx.transport, new_gid, "ACTIVE"))
        pub = ctx.publication_id()
        ctx.ledger.step(row, "6:publish", lambda: ops.product_publish(ctx.transport, new_gid, pub))
    except (_CreateAbort, ShopifyUserError, ShopifyTransportError, ProductNotFound) as e:
        why = str(e) if isinstance(e, _CreateAbort) else type(e).__name__
        flows.orphan(ctx, row, new_gid)
        ctx.ledger.close(ctx.ledger.reload(row), HELD, reason=f"create_unfinished:{why}")
        ctx.ledger.settle(group)
        status = "QUARANTINED" if why.startswith("levels_truncated") else "CREATE_ABORTED"
        return ApplyOutcome(sku, BRANCH_CREATE, status, new_gid, fresh.warnings + (f"create_aborted:{why}",))
    ctx.ledger.close(ctx.ledger.reload(row), APPLIED, moved=0)
    return _settled(ctx, fresh, BRANCH_CREATE, new_gid, group, "APPLIED")


def _execute(ctx: flows.Context, fresh: PlanAction, approved: PlanAction) -> ApplyOutcome:
    if not fresh.publishable:
        return ApplyOutcome(fresh.sku, fresh.branch, _reason_status(fresh.reason), fresh.target_gid,
                            ((fresh.reason,) if fresh.reason else ()) + fresh.warnings)
    if fresh.branch == BRANCH_CREATE:
        return create_outlet(ctx, fresh, approved)
    if fresh.branch == BRANCH_ACTIVE:
        return _active(ctx, fresh, approved)
    if fresh.branch == BRANCH_DRAFT:
        return revive_outlet(ctx, fresh, approved)
    return ApplyOutcome(fresh.sku, fresh.branch, "SKIPPED", fresh.target_gid, ("unknown_branch",))


def publish_apply(
    register: Any,
    transport: Any,
    approved_plan: Plan,
    *,
    promo_location_id: str,
    publication_id: Optional[str] = None,
    lease: Optional[Any] = None,
    take_bound_s: float = PUBLISH_TAKE_BOUND_S,
) -> ApplyReport:
    """Execute the approved plan under the pass lease, SKU by SKU (re-derived)."""
    pass_lease = lease or PassLease(register, HOLDER_PUBLISH)
    try:
        pass_lease.take(bound_s=take_bound_s)
    except LeaseUnavailable:
        return ApplyReport(tuple(ApplyOutcome(a.sku, a.branch, "ERROR", a.target_gid, ("lease_unavailable",))
                                 for a in approved_plan.actions))
    led = L.Ledger(register, transport, pass_lease, promo_location_id)
    ctx = flows.Context(led, publication_id=publication_id)
    outcomes: List[ApplyOutcome] = []
    try:
        passes.prologue(ctx, CLASS_PUBLISH)
        flows.n0_release(ctx)
        approved_by_sku = {a.sku: a for a in approved_plan.actions}
        if any(a.publishable and a.branch in (BRANCH_CREATE, BRANCH_DRAFT) for a in approved_plan.actions):
            try:
                ctx.publication_id()                       # EAGER: before any mutation
            except Exception as e:  # noqa: BLE001 - fail-fast, nothing mutated
                return ApplyReport(tuple(
                    ApplyOutcome(a.sku, a.branch, "ERROR", a.target_gid, (f"publication_resolve_failed:{type(e).__name__}",))
                    for a in approved_plan.actions))
        first = True
        for sku, approved in approved_by_sku.items():
            if not first:
                pass_lease.retake()                        # the hand-off: a waiting decrement gets in
                if pass_lease.others_between:
                    passes.prologue(ctx, CLASS_PUBLISH)
            first = False
            fresh = _fresh_action(ctx, sku)
            if fresh is None:
                outcomes.append(ApplyOutcome(sku, approved.branch, "NOTHING_PENDING", approved.target_gid))
                continue
            if fresh.plan_hash != approved.plan_hash:
                outcomes.append(ApplyOutcome(sku, fresh.branch, "VERIFY_FAILED", fresh.target_gid,
                                             ("plan_hash_mismatch:live_state_changed", "re_preview_required")))
                continue
            try:
                outcomes.append(_execute(ctx, fresh, approved))
            except LeaseLost:
                outcomes.append(ApplyOutcome(sku, fresh.branch, "ERROR", fresh.target_gid, ("lease_lost",)))
                done = {o.sku for o in outcomes}
                outcomes.extend(ApplyOutcome(a.sku, a.branch, "ERROR", a.target_gid, ("lease_lost",))
                                for a in approved_plan.actions if a.sku not in done)
                break
            except ShopifyUserError as e:
                outcomes.append(ApplyOutcome(sku, fresh.branch, "ERROR", fresh.target_gid, (f"shopify_user_error:{e.mutation}",)))
            except ShopifyTransportError:
                outcomes.append(ApplyOutcome(sku, fresh.branch, "ERROR", fresh.target_gid, ("shopify_transport_error",)))
            except StoreError as e:
                outcomes.append(ApplyOutcome(sku, fresh.branch, "ERROR", fresh.target_gid, (f"register_error:{type(e).__name__}",)))
            except Exception as e:  # noqa: BLE001 - per-SKU isolation (fix3)
                outcomes.append(ApplyOutcome(sku, fresh.branch, "ERROR", fresh.target_gid, (f"unexpected_error:{type(e).__name__}",)))
    except LeaseLost:
        done = {o.sku for o in outcomes}
        outcomes.extend(ApplyOutcome(a.sku, a.branch, "ERROR", a.target_gid, ("lease_lost",))
                        for a in approved_plan.actions if a.sku not in done)
    finally:
        pass_lease.release()
    return ApplyReport(tuple(outcomes), tuple(led.report.items))


def _fresh_action(ctx: flows.Context, sku: str) -> Optional[PlanAction]:
    anomalies: List[str] = []
    groups = _positive_groups(ctx.register, anomalies)
    sg = groups.get(sku)
    if sg is None:
        return None
    return _plan_sku(ctx.register, ctx.transport, ctx.promo, sg)


def run(dry_run: bool) -> None:
    """``RUN_MODE=SYNC`` read the sheet; the register has no cron path. Publish
    runs from the web app (preview → confirm). Removed with §3.8 (§4 step 5)."""
    raise RuntimeError("RUN_MODE=SYNC is retired: the register is MySQL (F7); publish from the web app")
