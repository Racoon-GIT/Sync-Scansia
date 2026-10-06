"""PUBLISH orchestration for the Scansia outlet lifecycle — on the register.

Composes the leaf layers — :class:`~backend.persistence.ports.RegisterPort` (the
register ``racoon.sc_returned_pair`` + ``sc_returned_pair_event`` and F7's audit
table), ``backend.services.resolvers`` (SKU -> outlet/source) and
``backend.shopify.ops`` (Admin-GraphQL op-wrappers) — into the publish flow.

Two entry points, split preview/apply so DRY_RUN is fail-closed by construction:

* :func:`publish_preview` — READ-ONLY. Reads ``pending_deltas()`` (refused before
  the whole-register ``BASELINE`` row), groups the events by exact (SKU, SIZE),
  re-resolves each SKU live and returns a :class:`Plan` of :class:`PlanAction`
  (branch, target, validated prices, per-size Δ and event ids, warnings). Nothing
  on Shopify or the register is touched.
* :func:`publish_apply` — MUTATES (confirm-gated by the caller). Takes the pass
  lease, re-derives the plan under it (TOCTOU), and for every action whose
  ``plan_hash`` still matches the approved plan executes the branch; each size's
  write is followed by the stamp of that key's events, carried by its
  ``RECONCILE`` row, in one fenced transaction.

Plan ``docs/f7-mysql-adapter-plan.md`` — this module is at its §4 step 2 (the
port swap: the positive-delta flows on the register, the sheet-era tests as their
parity oracle; the ``frozen_pre`` branches retired, §3.3). §4 step 4 rewrites the
reconciliation (signed deltas, write-ahead ``RECONCILE`` rows and recovery,
relative writes with ``changeFromQuantity``, the decrement, the held-key
lowering) on top of it.

Design invariants kept from the sheet era:

* **CREATE anti-phantom-stock ORDER** — ``ops.product_duplicate`` passes
  ``newStatus: DRAFT`` (the duplicate is never ACTIVE for an instant); zero +
  disconnect every non-Promo location (FIX1); zero every Promo variant; set the
  Δ only on the returned sizes; DENY; ONLY THEN ACTIVE + publish (the last two).
* **Quarantine** — an outlet (ACTIVE or a DRAFT being revived) whose variants
  hold stock at a location other than Promo, or whose level set is truncated, is
  never written (``variants.has_non_promo_stock``), at plan and again at apply.
* **FIX4** price validation (missing / zero / ≥ compare-at) blocks publish.
* **Exact size** (§3.2) — a group publishes onto the variant whose TITLE equals
  its ``SIZE`` exactly; a normalised-only match (``42L`` / ``42 L``) is surfaced
  for respelling, never written (``variants.twins``). The SKU on the variant is a
  secondary cross-check, surfaced only.
* **Publication resolved EAGER/fail-fast** before any action executes, when a
  CREATE/DRAFT branch will publish.
* **Per-action isolation** — any exception in one SKU's execution is bounded to
  that SKU's ``ERROR`` outcome; a lost lease stops the whole apply (nothing more
  can be written by this holder, rule 6).
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.persistence.records import (
    DONE,
    KIND_RECONCILE,
    EventRow,
    InsertAudit,
    LeaseLost,
    PairRow,
    PendingDeltas,
    StoreError,
)
from backend.services import resolvers, variants
from backend.services.lease import HOLDER_PUBLISH, PassLease
from backend.shopify import ops
from backend.shopify.ops import ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.outlet_service")

# Branches (mirror the SPEC's three publish branches).
BRANCH_CREATE = "CREATE"
BRANCH_ACTIVE = "ACTIVE"
BRANCH_DRAFT = "DRAFT"

_PRODUCT_GID_PREFIX = "gid://shopify/Product/"

# Non-publishable skip reasons -> outcome status codes surfaced to the caller.
_STATUS_BY_REASON = {
    "no_source": "NO_SOURCE",
    "multi_outlet": "MULTI_OUTLET",
    "multi_source": "MULTI_SOURCE",
    "price_invalid": "PRICE_INVALID",
    "quarantine": "QUARANTINED",
    "unexpected_status": "SKIPPED",
}


# =============================================================================
# Plan data model (preview output)
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
    """The planned action for one SKU group (a product)."""

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


# =============================================================================
# Pure helpers
# =============================================================================


def product_gid(numeric_id: Optional[int]) -> Optional[str]:
    """``OUTLET_PRODUCT_ID`` (BIGINT) -> the GID the Admin API takes (§3.2)."""
    return f"{_PRODUCT_GID_PREFIX}{int(numeric_id)}" if numeric_id is not None else None


def product_number(gid: Optional[str]) -> Optional[int]:
    """A product GID -> its numeric id (``OUTLET_PRODUCT_ID``), else ``None``."""
    if not gid or not gid.startswith(_PRODUCT_GID_PREFIX):
        return None
    tail = gid[len(_PRODUCT_GID_PREFIX):]
    return int(tail) if tail.isdigit() else None


def _money(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else f"{Decimal(value):.2f}"


def _plan_hash(anchor: Optional[str], branch: Optional[str], live_status: Optional[str]) -> str:
    """Bind {gid target/source, branch, live status} — the TOCTOU verify key."""
    raw = f"{anchor or ''}|{branch or ''}|{(live_status or '').upper()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _validate_price(
    prezzo_outlet: Optional[str], prezzo_high: Optional[str]
) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """FIX4/D3: reject missing/zero/negative price, missing/zero compareAt, or
    price >= compareAt. Returns ``(ok, price, compare_at, reason)``."""
    price, compare_at = prezzo_outlet, prezzo_high
    if not price:
        return False, price, compare_at, "price_missing"
    try:
        p = float(price)
    except (TypeError, ValueError):
        return False, price, compare_at, "price_unparseable"
    if p <= 0:
        return False, price, compare_at, "price_zero"
    if not compare_at:
        return False, price, compare_at, "compare_at_missing"
    try:
        c = float(compare_at)
    except (TypeError, ValueError):
        return False, price, compare_at, "compare_at_unparseable"
    if c <= 0:
        return False, price, compare_at, "compare_at_zero"
    if p >= c:
        return False, price, compare_at, "price_ge_compare_at"
    return True, price, compare_at, None


def _outlet_title(title: str) -> str:
    t = (title or "").strip()
    return t if t.casefold().endswith("outlet") else f"{t} - Outlet"


def _reason_status(reason: Optional[str]) -> str:
    return _STATUS_BY_REASON.get(reason or "", "SKIPPED")


@dataclass(frozen=True)
class _Group:
    """The pending events of one (SKU, SIZE) key."""

    size: str
    events: Tuple[EventRow, ...]

    @property
    def delta(self) -> int:
        return sum(e.delta for e in self.events)

    @property
    def event_ids(self) -> Tuple[int, ...]:
        return tuple(e.event_id for e in self.events)


def _groups_by_sku(pend: PendingDeltas) -> Tuple[List[str], Dict[str, List[_Group]]]:
    """Group the readable pending events by SKU then exact SIZE, in first-event order."""
    order: List[str] = []
    by_key: Dict[Tuple[str, str], List[EventRow]] = {}
    for e in pend.events:
        sku, size = e.key  # pending_deltas() returns keyed events only
        if sku not in order:
            order.append(sku)
        by_key.setdefault((sku, size), []).append(e)
    out: Dict[str, List[_Group]] = {s: [] for s in order}
    for (sku, size), evs in by_key.items():
        out[sku].append(_Group(size, tuple(evs)))
    return order, out


# =============================================================================
# Planning (used identically by preview and by apply's TOCTOU re-derivation)
# =============================================================================


def _build_size_targets(
    sku: str, groups: List[_Group], live_variants: List[Dict[str, Any]]
) -> Tuple[List[SizeTarget], List[str], List[str]]:
    """Match each Δ > 0 group to the variant titled exactly its SIZE.

    Returns ``(size_targets, unmatched_sizes, warnings)``. A size with no exact
    match is surfaced (never written); a normalised-only match is surfaced for
    respelling at the source (§3.2), never written.
    """
    targets: List[SizeTarget] = []
    unmatched: List[str] = []
    warns: List[str] = []
    for g in groups:
        v = variants.match_exact(g.size, live_variants)
        twin_titles = tuple(variants.variant_title(t) for t in variants.twins(g.size, live_variants))
        if v is None:
            unmatched.append(g.size)
            if twin_titles:
                warns.append(f"size_twin:{g.size}:{'|'.join(twin_titles)}")
        elif v.get("sku") and str(v["sku"]).strip() and str(v["sku"]).strip() != sku:
            warns.append(f"sku_mismatch_on_size:{g.size}:{str(v['sku']).strip()}")
        targets.append(SizeTarget(
            size=g.size, event_ids=g.event_ids, delta=g.delta, matched=v is not None,
            variant_id=(v.get("id") if v else None), inventory_item_id=variants.item_id(v),
            twins=twin_titles,
        ))
    return targets, unmatched, warns


def _nonpub(
    sku: str, branch: Optional[str], reason: str, *, warnings: List[str], plan_hash: str,
    target_gid: Optional[str] = None, source_gid: Optional[str] = None,
    outlet_title: Optional[str] = None, live_status: Optional[str] = None,
    price: Optional[str] = None, compare_at: Optional[str] = None,
) -> PlanAction:
    return PlanAction(
        sku=sku, branch=branch, publishable=False, reason=reason, target_gid=target_gid,
        source_gid=source_gid, outlet_title=outlet_title, live_status=live_status, price=price,
        compare_at=compare_at, price_ok=False, size_targets=(), unmatched_sizes=(),
        warnings=tuple(warnings), plan_hash=plan_hash,
    )


def _plan_action_for_sku(
    sku: str, groups: List[_Group], shelf: List[PairRow], transport: Any, promo_id: str
) -> PlanAction:
    warnings: List[str] = []
    first = shelf[0] if shelf else None
    price_ok, price, compare_at, price_reason = _validate_price(
        _money(first.price_outlet) if first else None, _money(first.price_high) if first else None
    )
    q_gid = product_gid(next((p.outlet_product_id for p in shelf if p.outlet_product_id), None))

    outlet_res = resolvers.outlet_resolver(transport, sku)
    if outlet_res.get("warning"):
        warnings.append(outlet_res["warning"])
    outlet_matches = outlet_res.get("matches") or []

    # MATCH step 1: the register's OUTLET_PRODUCT_ID if it resolves live; else the sole outlet.
    chosen: Optional[Dict[str, Any]] = None
    if q_gid:
        chosen = next((m for m in outlet_matches if m["product_gid"] == q_gid), None)
    if chosen is None and len(outlet_matches) == 1:
        chosen = outlet_matches[0]
    if chosen is None and len(outlet_matches) > 1:
        return _nonpub(sku, None, "multi_outlet", warnings=warnings, price=price,
                       compare_at=compare_at, plan_hash=_plan_hash(sku, "multi_outlet", ""))

    if chosen is not None:
        gid = chosen["product_gid"]
        status = (chosen.get("status") or "").upper()
        ph = _plan_hash(gid, status if status in (BRANCH_ACTIVE, BRANCH_DRAFT) else "OTHER", status)
        if status not in (BRANCH_ACTIVE, BRANCH_DRAFT):
            return _nonpub(sku, None, "unexpected_status",
                           warnings=warnings + [f"unexpected_status:{status}"], target_gid=gid,
                           live_status=status, price=price, compare_at=compare_at, plan_hash=ph)
        if not price_ok:
            return _nonpub(sku, status, "price_invalid", warnings=warnings + [f"price:{price_reason}"],
                           target_gid=gid, live_status=status, price=price, compare_at=compare_at,
                           plan_hash=ph)
        inv = ops.read_variant_inventory(transport, gid)
        if variants.has_non_promo_stock(inv, promo_id):
            return _nonpub(sku, status, "quarantine",
                           warnings=warnings + ["quarantine:non_promo_stock_inherited"],
                           target_gid=gid, live_status=status, price=price, compare_at=compare_at,
                           plan_hash=ph)
        targets, unmatched, size_warns = _build_size_targets(sku, groups, inv)
        extra = ["draft_revive:fresh_return"] if status == BRANCH_DRAFT else []
        return PlanAction(
            sku=sku, branch=status, publishable=True, reason=None, target_gid=gid, source_gid=None,
            outlet_title=None, live_status=status, price=price, compare_at=compare_at, price_ok=True,
            size_targets=tuple(targets), unmatched_sizes=tuple(unmatched),
            warnings=tuple(warnings + extra + size_warns), plan_hash=ph,
        )

    # MATCH step 3: no outlet -> source -> CREATE (or surface no-source).
    source_res = resolvers.source_resolver(transport, sku)
    if source_res.get("warning"):
        warnings.append(source_res["warning"])
    src_matches = source_res.get("matches") or []
    if not src_matches:
        return _nonpub(sku, None, "no_source", warnings=warnings, price=price,
                       compare_at=compare_at, plan_hash=_plan_hash(sku, "no_source", ""))
    if len(src_matches) > 1:
        return _nonpub(sku, None, "multi_source", warnings=warnings, price=price,
                       compare_at=compare_at, plan_hash=_plan_hash(sku, "multi_source", ""))
    source = src_matches[0]
    source_gid = source["product_gid"]
    outlet_title = _outlet_title(source.get("title") or "")
    ph = _plan_hash(source_gid, BRANCH_CREATE, "")
    if not price_ok:
        return _nonpub(sku, BRANCH_CREATE, "price_invalid", warnings=warnings + [f"price:{price_reason}"],
                       source_gid=source_gid, outlet_title=outlet_title, price=price,
                       compare_at=compare_at, plan_hash=ph)
    src_variants = ops.get_product_variants(transport, source_gid)
    targets, unmatched, size_warns = _build_size_targets(sku, groups, src_variants)
    return PlanAction(
        sku=sku, branch=BRANCH_CREATE, publishable=True, reason=None, target_gid=None,
        source_gid=source_gid, outlet_title=outlet_title, live_status=None, price=price,
        compare_at=compare_at, price_ok=True, size_targets=tuple(targets),
        unmatched_sizes=tuple(unmatched), warnings=tuple(warnings + size_warns), plan_hash=ph,
    )


def _build_actions(register: Any, transport: Any, promo_id: str) -> Tuple[List[PlanAction], List[str]]:
    """Plan every SKU carrying ≥ 1 Δ > 0 (SKU, SIZE) group that is not held.

    A held key's Δ > 0 stays pending and surfaced, never added (§3.3); Δ ≤ 0
    groups are the decrement's (§3.3's table, §4 step 4) and stay pending here.
    """
    pend = register.pending_deltas()
    held = pend.held_keys
    order, by_sku = _groups_by_sku(pend)
    anomalies = [f"consume_miss:{e.event_id}" for e in pend.keyless]
    actions: List[PlanAction] = []
    for sku in order:
        positive = [g for g in by_sku[sku] if g.delta > 0 and (sku, g.size) not in held]
        for g in by_sku[sku]:
            if g.delta > 0 and (sku, g.size) in held:
                anomalies.append(f"held_positive:{sku}:{g.size}:{g.delta}")
        if not positive:
            continue
        actions.append(_plan_action_for_sku(sku, positive, register.shelf(sku), transport, promo_id))
    return actions, anomalies


# =============================================================================
# Preview (READ-ONLY, DRY_RUN-safe)
# =============================================================================


def publish_preview(register: Any, transport: Any, *, promo_location_id: str) -> Plan:
    """Compute the publish PLAN without mutating Shopify or the register."""
    actions, anomalies = _build_actions(register, transport, promo_location_id)
    return Plan(dry_run=True, actions=tuple(actions), anomalies=tuple(anomalies))


# =============================================================================
# Apply (MUTATES — confirm-gated upstream)
# =============================================================================


class _Writer:
    """The apply's writes to the register: each size's stamp, carried by its
    ``RECONCILE`` row, under the pass lease."""

    def __init__(self, register: Any, lease: Any, promo_id: str) -> None:
        self.register = register
        self.lease = lease
        self.promo_id = promo_id

    def stamp_size(
        self, *, action: str, sku: str, st: SizeTarget, product: Optional[str], variant: Dict[str, Any],
        level_before: int, level_after: int,
    ) -> Tuple[int, ...]:
        carrier = InsertAudit(
            kind=KIND_RECONCILE, actor=self.register.actor, action=action, status=DONE,
            product_id=product_number(product),
            payload={
                "keys": [[sku, st.size]], "event_ids": list(st.event_ids), "delta": st.delta,
                "variant_id": variant.get("id"), "inventory_item_id": variants.item_id(variant),
                "location_id": self.promo_id, "level_before": level_before, "level_after": level_after,
            },
        )
        self.register.stamp(self.lease, st.event_ids, carrier=carrier)
        return st.event_ids


def _set_level(transport: Any, variant: Dict[str, Any], promo_id: str, target: int) -> None:
    ops.inventory_set_quantities(transport, variants.item_id(variant), promo_id, target)


def _execute_create(fresh: PlanAction, w: _Writer, transport: Any, get_pub: Callable[[], str]) -> ApplyOutcome:
    promo_id = w.promo_id
    warns: List[str] = list(fresh.warnings)
    stamped: List[int] = []

    # (1) duplicate — STAYS DRAFT for the whole finalization.
    new_gid = ops.product_duplicate(transport, fresh.source_gid, fresh.outlet_title)
    inv = ops.read_variant_inventory(transport, new_gid)
    if any(v.get("levels_truncated") for v in inv):
        warns.append("quarantine:levels_truncated_after_duplicate")
        return ApplyOutcome(fresh.sku, BRANCH_CREATE, "QUARANTINED", new_gid, tuple(warns), ())

    # (2) FIX1: zero + disconnect EVERY non-Promo location the duplicate inherited.
    for v in inv:
        item = variants.item_id(v)
        for lvl in v.get("levels") or []:
            loc = lvl.get("location_id")
            if loc and loc != promo_id:
                ops.inventory_set_quantities(transport, item, loc, 0)
                ops.inventory_deactivate(transport, item, loc)

    # (3) zero ALL variants at Promo (kills inherited stock on non-returned sizes).
    for v in inv:
        ops.inventory_set_quantities(transport, variants.item_id(v), promo_id, 0)

    # (4) the Δ ONLY on the returned sizes, matched exactly on the duplicate.
    for st in fresh.size_targets:
        if st.delta <= 0:
            continue
        v = variants.match_exact(st.size, inv)
        if v is None:
            warns.append(f"unmatched_size:{st.size}")
            continue
        _set_level(transport, v, promo_id, st.delta)
        stamped.extend(w.stamp_size(action="create", sku=fresh.sku, st=st, product=new_gid, variant=v,
                                    level_before=0, level_after=st.delta))

    # (5) prices + inventoryPolicy=DENY in one bulk update.
    variants_input = [
        {"id": v["id"], "price": fresh.price, "compareAtPrice": fresh.compare_at, "inventoryPolicy": "DENY"}
        for v in inv
    ]
    ops.product_variants_bulk_update(transport, new_gid, variants_input)

    # (6) LAST: flip ACTIVE, then publish to the Online Store channel.
    ops.product_update_status(transport, new_gid, "ACTIVE")
    ops.product_publish(transport, new_gid, get_pub())
    return ApplyOutcome(fresh.sku, BRANCH_CREATE, "APPLIED", new_gid, tuple(warns), tuple(stamped))


def _add_sizes(fresh: PlanAction, w: _Writer, transport: Any, inv: List[Dict[str, Any]],
               action: str, warns: List[str]) -> List[int]:
    """Each Δ > 0 size: the live level + Δ (§4 step 4 makes it a relative write
    with ``changeFromQuantity``), then its stamp."""
    stamped: List[int] = []
    for st in fresh.size_targets:
        if st.delta <= 0:
            continue
        v = variants.match_exact(st.size, inv)
        if v is None:
            warns.append(f"unmatched_size:{st.size}")
            continue
        live = variants.promo_available(v, w.promo_id) or 0
        _set_level(transport, v, w.promo_id, live + st.delta)
        stamped.extend(w.stamp_size(action=action, sku=fresh.sku, st=st, product=fresh.target_gid,
                                    variant=v, level_before=live, level_after=live + st.delta))
    return stamped


def _execute_active(fresh: PlanAction, w: _Writer, transport: Any) -> ApplyOutcome:
    warns: List[str] = list(fresh.warnings)
    gid = fresh.target_gid
    inv = ops.read_variant_inventory(transport, gid)
    if variants.has_non_promo_stock(inv, w.promo_id):
        return ApplyOutcome(fresh.sku, BRANCH_ACTIVE, "QUARANTINED", gid,
                            tuple(warns + ["quarantine:non_promo_stock"]), ())
    # refresh price/compareAt on every variant (never re-inflate stock here).
    ops.product_variants_bulk_update(
        transport, gid, [{"id": v["id"], "price": fresh.price, "compareAtPrice": fresh.compare_at} for v in inv]
    )
    stamped = _add_sizes(fresh, w, transport, inv, "add", warns)
    return ApplyOutcome(fresh.sku, BRANCH_ACTIVE, "APPLIED", gid, tuple(warns), tuple(stamped))


def _execute_draft(fresh: PlanAction, w: _Writer, transport: Any, get_pub: Callable[[], str]) -> ApplyOutcome:
    """DRAFT with a fresh return -> revive: the Δ, then DENY, ACTIVE, publish."""
    warns: List[str] = list(fresh.warnings)
    gid = fresh.target_gid
    inv = ops.read_variant_inventory(transport, gid)
    if variants.has_non_promo_stock(inv, w.promo_id):
        return ApplyOutcome(fresh.sku, BRANCH_DRAFT, "QUARANTINED", gid,
                            tuple(warns + ["quarantine:non_promo_stock"]), ())
    stamped = _add_sizes(fresh, w, transport, inv, "revive", warns)
    ops.product_variants_bulk_update(
        transport, gid,
        [{"id": v["id"], "price": fresh.price, "compareAtPrice": fresh.compare_at, "inventoryPolicy": "DENY"}
         for v in inv],
    )
    ops.product_update_status(transport, gid, "ACTIVE")
    ops.product_publish(transport, gid, get_pub())
    return ApplyOutcome(fresh.sku, BRANCH_DRAFT, "REVIVED", gid, tuple(warns), tuple(stamped))


def _execute_action(fresh: PlanAction, w: _Writer, transport: Any, get_pub: Callable[[], str]) -> ApplyOutcome:
    if not fresh.publishable:
        return ApplyOutcome(
            fresh.sku, fresh.branch, _reason_status(fresh.reason), fresh.target_gid,
            ((fresh.reason,) if fresh.reason else ()) + fresh.warnings, (),
        )
    if fresh.branch == BRANCH_CREATE:
        return _execute_create(fresh, w, transport, get_pub)
    if fresh.branch == BRANCH_ACTIVE:
        return _execute_active(fresh, w, transport)
    if fresh.branch == BRANCH_DRAFT:
        return _execute_draft(fresh, w, transport, get_pub)
    return ApplyOutcome(fresh.sku, fresh.branch, "SKIPPED", fresh.target_gid, ("unknown_branch",), ())


def publish_apply(
    register: Any,
    transport: Any,
    approved_plan: Plan,
    *,
    promo_location_id: str,
    publication_id: Optional[str] = None,
    lease: Optional[Any] = None,
) -> ApplyReport:
    """Execute the approved plan under the pass lease, with a live re-derivation.

    Takes the lease (refused → every approved action ``ERROR``
    ``lease_unavailable``, nothing written), re-derives the plan from the
    register and Shopify as they are now, and executes each action whose
    ``plan_hash`` still equals the approved one; a drifted one → ``VERIFY_FAILED``.

    The "Online Store" publication is resolved EAGER/fail-fast, before any
    action, when a CREATE/DRAFT will publish; its failure aborts the batch
    cleanly. Per-action isolation: a Shopify / store / unexpected error is
    bounded to that SKU's ``ERROR``; a lost lease stops the apply — every
    remaining action ``ERROR`` ``lease_lost``.
    """
    pass_lease = lease or PassLease(register, HOLDER_PUBLISH)
    if not pass_lease.try_take():
        return ApplyReport(tuple(
            ApplyOutcome(a.sku, a.branch, "ERROR", a.target_gid, ("lease_unavailable",), ())
            for a in approved_plan.actions
        ))
    try:
        fresh_actions, _anoms = _build_actions(register, transport, promo_location_id)
        approved_by_sku = {a.sku: a for a in approved_plan.actions}

        resolved_publication_id = publication_id
        needs_publish = any(a.publishable and a.branch in (BRANCH_CREATE, BRANCH_DRAFT) for a in fresh_actions)
        if resolved_publication_id is None and needs_publish:
            try:
                resolved_publication_id = ops.get_online_store_publication_id(transport)
            except Exception as e:  # noqa: BLE001 - fail-fast, nothing mutated yet
                return ApplyReport(tuple(
                    ApplyOutcome(a.sku, a.branch, "ERROR", a.target_gid,
                                 (f"publication_resolve_failed:{type(e).__name__}",), ())
                    for a in fresh_actions
                ))

        def get_pub() -> str:
            return resolved_publication_id

        writer = _Writer(register, pass_lease, promo_location_id)
        outcomes: List[ApplyOutcome] = []
        for i, fresh in enumerate(fresh_actions):
            approved = approved_by_sku.get(fresh.sku)
            if approved is None:
                outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "NOT_IN_PLAN", fresh.target_gid,
                                             ("not_in_approved_plan",), ()))
                continue
            if fresh.plan_hash != approved.plan_hash:
                outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "VERIFY_FAILED", fresh.target_gid,
                                             ("plan_hash_mismatch:live_state_changed", "re_preview_required"), ()))
                continue
            try:
                outcomes.append(_execute_action(fresh, writer, transport, get_pub))
            except LeaseLost:
                outcomes.extend(
                    ApplyOutcome(a.sku, a.branch, "ERROR", a.target_gid, ("lease_lost",), ())
                    for a in fresh_actions[i:]
                )
                break
            except ShopifyUserError as e:
                outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "ERROR", fresh.target_gid,
                                             (f"shopify_user_error:{e.mutation}",), ()))
            except ShopifyTransportError:
                outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "ERROR", fresh.target_gid,
                                             ("shopify_transport_error",), ()))
            except StoreError as e:
                outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "ERROR", fresh.target_gid,
                                             (f"register_error:{type(e).__name__}",), ()))
            except Exception as e:  # noqa: BLE001 - per-action isolation (fix3)
                outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "ERROR", fresh.target_gid,
                                             (f"unexpected_error:{type(e).__name__}",), ()))
        return ApplyReport(tuple(outcomes))
    finally:
        pass_lease.release()


# =============================================================================
# Legacy RUN_MODE=SYNC entry point — retired with the sheet (§3.8, §4 step 5)
# =============================================================================


def run(dry_run: bool) -> None:
    """``RUN_MODE=SYNC`` read the sheet; the register has no cron path. Publish
    runs from the web app (preview → confirm). Removed with §3.8 (§4 step 5)."""
    raise RuntimeError(
        "RUN_MODE=SYNC is retired: the register is MySQL (F7); publish from the web app"
    )
