"""PRICES — one outlet price per SKU, on the register (plan
``docs/f7-mysql-adapter-plan.md`` §3.5, §3.1 ``set_outlet_price``, §6 B-2 / D20 + D25;
§3.3 rule 6's lease).

The register is where an outlet price lives: ``PRICE_OUTLET`` / ``DISCOUNT`` on the
SKU's ``IN_SCANSIA`` pairs, by PRIMARY KEY, a ``PRICE`` audit row per write (a pair
already sold keeps the price it was sold at). Shopify follows the register: the push
refreshes the SKU's outlet — price and compare-at on every variant — when it is
ACTIVE; a DRAFT outlet takes the register's price at its revive (its tail carries
it) unless ``status_override`` pushes it now; a SKU with no outlet takes it at its
CREATE. The publish then reads that one price (``outlet_service.sku_prices``).

* :func:`prices_preview` — READ-ONLY: per SKU on the shelf, the mode's target price,
  B-2's compare-at (one per SKU), FIX4, and the register ↔ target ↔ live diff.
* :func:`prices_apply` — under the pass lease (a decrement-class holder: its
  recovery and the held-key lowering at the take; released and re-taken between
  SKUs so a waiting decrement gets in, rule 6). It captures the priors first — one
  ``PRICE`` intent row keyed by a unique ``INTENT_ID``, the abort gate: nothing is
  written unless it lands — then, per SKU re-derived live, writes the register and
  pushes. Only what the approved plan bounds is written (same per-SKU plan hash).
* :func:`revert_prices` — an intent undone, per SKU, under the lease: the register
  pairs it wrote go back to the values it replaced (read off its own ``PRICE``
  rows; a pair whose price moved since leaves the SKU untouched, listed), then the
  captured Shopify priors are re-pushed. A revert that left the register at the
  new price would be undone by the next publish, which refreshes every variant from
  the register.
* :func:`discharge_debt_count` — READ-ONLY recon of live outlets failing FIX4.

Modes: **percent** (price = compare-at × (1 − pct), to the cent, half up),
**direct** (a typed price, or — none typed — the register's own price as it is: a
repair push; fill-missing stays REFUSED: no price → ``SKIP_MISSING_PRICE``, never a
default), **bulk** (ordered rules scoped by ``sku`` / ``brand`` / ``product_title``
— the last matching rule wins; a per-SKU override wins over every rule).
``DISCOUNT`` is always the discount the written price makes on the compare-at, in
percent to the cent (what the sheet's K held: J 149 on a compare-at of 189 →
21.16, §6 B-2's example) — one rule for every mode.

The push target is the SKU's ``OUTLET_PRODUCT_ID`` — written only for an outlet by
title AND collection, never a resolver ``review`` match (§3.1) — and it must still
be a member of the outlet collection: the sheet era's cardinal gate (column Q empty
→ no push; never a SKU search, which also finds the full-price source sharing the
SKU, LD21) holds on the register. Two different ids on one SKU's shelf pairs →
the SKU is listed and blocked.
"""
from __future__ import annotations

import hashlib
import logging
import math
import time
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

from backend.persistence.records import KIND_PRICE, AuditRow, LeaseLost, PairRow, StoreError
from backend.services import flows, passes
from backend.services.lease import HOLDER_PRICING, LeaseUnavailable, PassLease
from backend.services.outlet_service import compare_at_of, validate_price
from backend.shopify import ops
from backend.shopify.ops import OUTLET_COLLECTION_GID, ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.pricing_service")

# --- modes -----------------------------------------------------------------
MODE_PERCENT = "percent"
MODE_DIRECT = "direct"
MODE_BULK = "bulk"
_MODES = frozenset({MODE_PERCENT, MODE_DIRECT, MODE_BULK})

_STATUS_ACTIVE = "ACTIVE"
# The bulk rules' scope keys: the register columns that describe a model.
SCOPE_KEYS = frozenset({"sku", "brand", "product_title"})
PRICING_TAKE_BOUND_S = passes.HOLDER_TAKE_BOUND_S
# The PRICE rows a pricing apply / revert writes (``Register``'s actions).
ACTION_SET = "set_outlet_price"

# --- status / reason codes (PriceDiff.status / PriceOutcome.status) ----------
STATUS_OK = "OK"                                  # preview: actionable
STATUS_APPLIED = "APPLIED"                        # apply: register written (if it differed) + pushed
STATUS_REGISTER_UPDATED = "REGISTER_UPDATED"      # apply: register written, nothing to push
STATUS_ALREADY_CORRECT = "SKIP_ALREADY_CORRECT"   # register and live already on target
STATUS_MISSING_PRICE = "SKIP_MISSING_PRICE"       # no price, or no compare-at (fill-missing REFUSED)
STATUS_PRICE_INVALID = "PRICE_INVALID"            # FIX4: price ≤ 0, ≥ compare-at, …
STATUS_PRICE_CONFLICT = "SKIP_PRICE_CONFLICT"     # a repair push with two register prices
STATUS_COMPARE_AT_CONFLICT = "SKIP_COMPARE_AT_CONFLICT"  # the SKU's pairs yield two compare-ats
STATUS_MULTI_PRODUCT_ID = "SKIP_MULTI_PRODUCT_ID"  # two OUTLET_PRODUCT_IDs on one SKU's pairs
STATUS_VERIFY_FAILED = "VERIFY_FAILED"            # apply: the SKU moved since the preview
STATUS_NOT_IN_PLAN = "NOT_IN_PLAN"                # apply: SKU absent from the approved plan
STATUS_ERROR = "ERROR"
STATUS_REVERTED = "REVERTED"
STATUS_REVERT_SKIPPED = "SKIP_CHANGED_SINCE"      # revert: a pair's price moved after the intent


# =============================================================================
# Params (per-mode inputs the GUI passes down)
# =============================================================================
@dataclass(frozen=True)
class BulkRule:
    """One bulk-pricing rule (mode 3). ``percent`` is a fraction (0.30 == 30 % off).

    ``scope`` maps an attribute (``sku`` / ``brand`` / ``product_title``) to the
    accepted values; ALL entries must match (AND); an empty scope matches every SKU.
    """

    percent: float
    scope: Mapping[str, Tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class PriceParams:
    """Unified per-mode parameters. Only the fields of the active mode are read."""

    percent_by_sku: Mapping[str, float] = field(default_factory=dict)
    price_by_sku: Mapping[str, str] = field(default_factory=dict)
    rules: Tuple[BulkRule, ...] = ()
    override_percent_by_sku: Mapping[str, float] = field(default_factory=dict)


# =============================================================================
# The intent (the abort gate's record) and the audit sink
# =============================================================================
@dataclass(frozen=True)
class VariantPrior:
    variant_id: str
    price: Optional[str]
    compare_at: Optional[str]


@dataclass(frozen=True)
class ProductPrior:
    product_gid: str
    sku: str
    variants: Tuple[VariantPrior, ...]


@dataclass(frozen=True)
class PriceIntent:
    """What :meth:`AuditSink.capture_before` persists before any write: the live
    priors of every product the apply may push. The register's priors are kept by
    each write's own ``PRICE`` row, which names this intent."""

    mode: str
    plan_hash: str
    priors: Tuple[ProductPrior, ...]


class AuditSink(Protocol):
    def capture_before(self, intent: PriceIntent) -> str: ...

    def load(self, intent_id: str) -> PriceIntent: ...


# =============================================================================
# Plan / report data model
# =============================================================================
@dataclass(frozen=True)
class PriceDiff:
    sku: str
    product_gid: Optional[str]      # the push target; None → the register only
    status: str
    actionable: bool
    price: Optional[str]            # target price → PRICE_OUTLET and Shopify price
    compare_at: Optional[str]       # B-2's compare-at → Shopify compareAtPrice
    discount: Optional[str]         # → DISCOUNT (percent, to the cent)
    percent: Optional[float]        # modes 1 / 3: the fraction that made the price
    register_price: Optional[str]   # the SKU's one PRICE_OUTLET now (None: none, or two)
    register_changed: bool          # a shelf pair's PRICE_OUTLET / DISCOUNT ≠ target
    live_price: Optional[str]
    live_compare_at: Optional[str]
    live_changed: bool              # a variant of the push target ≠ target
    live_status: Optional[str]
    pair_ids: Tuple[int, ...]
    warnings: Tuple[str, ...]
    plan_hash: str


@dataclass(frozen=True)
class PricePlan:
    dry_run: bool
    mode: str
    diffs: Tuple[PriceDiff, ...]
    anomalies: Tuple[str, ...] = ()


@dataclass(frozen=True)
class PriceOutcome:
    sku: str
    product_gid: Optional[str]
    status: str
    warnings: Tuple[str, ...] = ()


@dataclass(frozen=True)
class PriceApplyReport:
    intent_id: Optional[str]
    outcomes: Tuple[PriceOutcome, ...]
    surfaced: Tuple[Dict[str, Any], ...] = ()


@dataclass(frozen=True)
class RevertReport:
    intent_id: str
    reverted_products: int
    reverted_variants: int
    outcomes: Tuple[PriceOutcome, ...]
    reverted_pairs: int = 0
    surfaced: Tuple[Dict[str, Any], ...] = ()


@dataclass(frozen=True)
class DebtReport:
    scanned_products: int
    broken_products: int
    broken_variants: int
    broken_gids: Tuple[str, ...]


@dataclass(frozen=True)
class _PlanRow:
    """The public diff plus the push target's live variants (apply's priors and push)."""

    diff: PriceDiff
    live_variants: Tuple[Dict[str, Any], ...]


# =============================================================================
# Pure helpers
# =============================================================================
_CENT = Decimal("0.01")
_SHOPIFY_ERRORS = (ShopifyUserError, ShopifyTransportError, ProductNotFound)


def _check_mode(mode: str) -> None:
    if mode not in _MODES:
        raise ValueError(f"unknown price mode {mode!r} (expected one of {sorted(_MODES)})")


def money(value: Any) -> Optional[str]:
    """A price as a 2-decimal string (``'129'``, ``'129,9'``, ``'€ 129.90'`` →
    ``'129.90'``), or ``None`` when empty or not a finite number. The sign is kept:
    FIX4 refuses a non-positive price, never a silently flipped one."""
    if value is None:
        return None
    s = str(value).strip().replace("€", "").replace(" ", "")
    if not s:
        return None
    if s.count(",") == 1 and "." not in s:
        s = s.replace(",", ".")
    try:
        d = Decimal(s)
    except InvalidOperation:
        return None
    if not d.is_finite():
        return None
    return str(d.quantize(_CENT, rounding=ROUND_HALF_UP))


def _dec(value: Optional[str]) -> Optional[Decimal]:
    return None if value is None else Decimal(value)


def discount_of(price: str, compare_at: str) -> str:
    """The discount ``price`` makes on ``compare_at``, in percent to the cent."""
    d = (Decimal(1) - Decimal(price) / Decimal(compare_at)) * 100
    return str(d.quantize(_CENT, rounding=ROUND_HALF_UP))


def _percent_price(compare_at: Optional[str], percent: float) -> Optional[str]:
    if compare_at is None or not isinstance(percent, (int, float)) or not math.isfinite(percent):
        return None
    d = Decimal(compare_at) * (Decimal(1) - Decimal(str(percent)))
    return str(d.quantize(_CENT, rounding=ROUND_HALF_UP))


def _plan_hash(gid: Optional[str], price: Optional[str], compare_at: Optional[str],
               discount: Optional[str], status: Optional[str]) -> str:
    """TOCTOU key: {push target, price, compare-at, discount, its status}."""
    raw = f"{gid or ''}|{price or ''}|{compare_at or ''}|{discount or ''}|{(status or '').upper()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _first(shelf: Sequence[PairRow], attr: str) -> str:
    for p in shelf:
        v = getattr(p, attr, None)
        if v not in (None, ""):
            return str(v).strip()
    return ""


def _scope_matches(scope: Mapping[str, Tuple[str, ...]], attrs: Mapping[str, str]) -> bool:
    return all(str(attrs.get(k, "")).strip() in accepted for k, accepted in scope.items())


def _bulk_percent(params: PriceParams, sku: str, attrs: Mapping[str, str]) -> Optional[float]:
    """Mode 3 precedence: per-SKU override > the last matching rule > None."""
    if sku in params.override_percent_by_sku:
        return float(params.override_percent_by_sku[sku])
    chosen: Optional[float] = None
    for rule in params.rules:
        if _scope_matches(rule.scope, attrs):
            chosen = float(rule.percent)
    return chosen


def _target(mode: str, params: PriceParams, sku: str, shelf: Sequence[PairRow],
            compare_at: Optional[str], register_price: Optional[str]) -> Tuple[bool, Optional[str], Optional[float]]:
    """``(in_scope, target price, percent)``; out of scope → the SKU is not in the plan."""
    if mode == MODE_PERCENT:
        pct = params.percent_by_sku.get(sku)
        if pct is None:
            return False, None, None
        return True, _percent_price(compare_at, pct), float(pct)
    if mode == MODE_BULK:
        attrs = {"sku": sku, "brand": _first(shelf, "brand"), "product_title": _first(shelf, "product_title")}
        pct = _bulk_percent(params, sku, attrs)
        if pct is None:
            return False, None, None
        return True, _percent_price(compare_at, pct), pct
    override = params.price_by_sku.get(sku)
    if override not in (None, ""):
        return True, money(override), None
    return True, register_price, None


def _live(variants: Sequence[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    if not variants:
        return None, None
    return money(variants[0].get("price")), money(variants[0].get("compareAtPrice"))


# =============================================================================
# Planning — shared by the preview, the apply and its per-SKU re-derivation
# =============================================================================
StatusOf = Callable[[str], Optional[str]]


def collection_status_map(transport: Any) -> StatusOf:
    """Product status by gid over the outlet collection, read once (lazily);
    ``None`` = not a member."""
    cache: Dict[str, Dict[str, str]] = {}

    def status_of(gid: str) -> Optional[str]:
        if "m" not in cache:
            members = ops.enumerate_outlet_products(transport)
            cache["m"] = {n["id"]: (n.get("status") or "").upper() for n in members}
        return cache["m"].get(gid)

    return status_of


def core_status(transport: Any) -> StatusOf:
    """The same answer from the product itself (one read, right before a write)."""

    def status_of(gid: str) -> Optional[str]:
        try:
            core = ops.get_product_core(transport, gid)
        except ProductNotFound:
            return None
        if not any(c.get("id") == OUTLET_COLLECTION_GID for c in core.get("collections") or ()):
            return None
        return (core.get("status") or "").upper()

    return status_of


def _diff(sku: str, status: str, *, gid: Optional[str] = None, price: Optional[str] = None,
          compare_at: Optional[str] = None, discount: Optional[str] = None, percent: Optional[float] = None,
          register_price: Optional[str] = None, register_changed: bool = False,
          live: Tuple[Optional[str], Optional[str]] = (None, None), live_changed: bool = False,
          live_status: Optional[str] = None, pair_ids: Tuple[int, ...] = (),
          warnings: Sequence[str] = ()) -> PriceDiff:
    return PriceDiff(
        sku=sku, product_gid=gid, status=status, actionable=status == STATUS_OK, price=price,
        compare_at=compare_at, discount=discount, percent=percent, register_price=register_price,
        register_changed=register_changed, live_price=live[0], live_compare_at=live[1],
        live_changed=live_changed, live_status=live_status, pair_ids=pair_ids, warnings=tuple(warnings),
        plan_hash=_plan_hash(gid, price, compare_at, discount, live_status if gid else ""),
    )


def plan_sku(mode: str, params: PriceParams, sku: str, shelf: Sequence[PairRow], transport: Any,
             status_of: StatusOf, status_override: bool) -> Optional[_PlanRow]:
    """One SKU's diff (``None`` when the mode does not address it)."""
    warnings: List[str] = []
    pair_ids = tuple(p.pair_id for p in shelf)
    prices = {p.price_outlet for p in shelf if p.price_outlet is not None}
    cats = {compare_at_of(p) for p in shelf if compare_at_of(p) is not None}
    register_price = money(next(iter(prices))) if len(prices) == 1 else None
    compare_at = money(next(iter(cats))) if len(cats) == 1 else None
    in_scope, price, percent = _target(mode, params, sku, shelf, compare_at, register_price)
    if not in_scope:
        return None
    base = dict(price=price, compare_at=compare_at, percent=percent, register_price=register_price,
                pair_ids=pair_ids)
    if len(prices) > 1:
        warnings.append(f"price_conflict:{'|'.join(sorted(money(p) or '' for p in prices))}")
    kept_out = [p.pair_id for p in shelf if compare_at_of(p) is None]
    if kept_out:   # D25: kept out of the publish, never its SKU; no compare-at lent
        warnings.append(f"no_compare_at_pairs:{','.join(str(i) for i in kept_out)}")
    if len(cats) > 1:
        warnings.append(f"compare_at_conflict:{'|'.join(sorted(money(c) or '' for c in cats))}")
        return _PlanRow(_diff(sku, STATUS_COMPARE_AT_CONFLICT, warnings=warnings, **base), ())
    if mode == MODE_DIRECT and price is None and len(prices) > 1:
        return _PlanRow(_diff(sku, STATUS_PRICE_CONFLICT, warnings=warnings, **base), ())
    ok, why = validate_price(price, compare_at) if compare_at is not None else (False, "compare_at_missing")
    if not ok:
        warnings.append(f"price:{why}")
        status = STATUS_MISSING_PRICE if why in ("price_missing", "compare_at_missing") else STATUS_PRICE_INVALID
        return _PlanRow(_diff(sku, status, warnings=warnings, **base), ())
    assert price is not None and compare_at is not None
    discount = discount_of(price, compare_at)
    register_changed = any(p.price_outlet != Decimal(price) or p.discount != Decimal(discount) for p in shelf)
    ids = sorted({p.outlet_product_id for p in shelf if p.outlet_product_id is not None})
    if len(ids) > 1:
        warnings.append(f"multi_product_id:{','.join(str(i) for i in ids)}")
        return _PlanRow(_diff(sku, STATUS_MULTI_PRODUCT_ID, discount=discount, warnings=warnings, **base), ())
    gid = flows.product_gid(ids[0]) if ids else None
    push: Optional[str] = None
    live_status: Optional[str] = None
    if gid is None:
        warnings.append("no_outlet_product_id")        # the publish's CREATE takes the price
    else:
        live_status = status_of(gid)
        if live_status is None:
            warnings.append("outlet_not_in_collection")
        elif live_status != _STATUS_ACTIVE and not status_override:
            warnings.append(f"status:{live_status}")    # the revive's tail takes the price
        else:
            push = gid
    variants: List[Dict[str, Any]] = []
    if push is not None:
        variants = ops.get_product_variants(transport, push)
        if not variants:
            warnings.append("no_variants")
            push = None
    live = _live(variants)
    live_changed = any(money(v.get("price")) != price or money(v.get("compareAtPrice")) != compare_at
                       for v in variants)
    status = STATUS_OK if (register_changed or live_changed) else STATUS_ALREADY_CORRECT
    return _PlanRow(_diff(sku, status, gid=push, discount=discount, register_changed=register_changed,
                          live=live, live_changed=live_changed, live_status=live_status if push else None,
                          warnings=warnings, **base), tuple(variants))


def _by_sku(register: Any) -> List[Tuple[str, List[PairRow]]]:
    """The shelf grouped by SKU in first-pair order (a blank SKU resolves nothing, rule 7)."""
    groups: Dict[str, List[PairRow]] = {}
    for p in register.shelf():
        if (p.sku or "").strip():
            groups.setdefault(p.sku, []).append(p)
    return list(groups.items())


def _error_row(sku: str, err: str) -> _PlanRow:
    return _PlanRow(_diff(sku, STATUS_ERROR, warnings=(f"error:{err}",)), ())


def _plan_all(register: Any, transport: Any, mode: str, params: PriceParams, status_override: bool,
              status_of: StatusOf) -> Tuple[List[_PlanRow], Tuple[str, ...]]:
    rows: List[_PlanRow] = []
    anomalies: List[str] = []
    for p in register.shelf():
        if not (p.sku or "").strip():
            anomalies.append(f"blank_sku:{p.pair_id}")
    for sku, shelf in _by_sku(register):
        try:
            row = plan_sku(mode, params, sku, shelf, transport, status_of, status_override)
        except _SHOPIFY_ERRORS + (RuntimeError,) as e:
            rows.append(_error_row(sku, type(e).__name__))
            continue
        if row is not None:
            rows.append(row)
    return rows, tuple(anomalies)


# =============================================================================
# Preview (READ-ONLY)
# =============================================================================
def prices_preview(register: Any, transport: Any, mode: str, params: PriceParams, *,
                   status_override: bool = False) -> PricePlan:
    """Compute the price PLAN without writing the register or Shopify."""
    _check_mode(mode)
    rows, anomalies = _plan_all(register, transport, mode, params, status_override,
                                collection_status_map(transport))
    return PricePlan(dry_run=True, mode=mode, diffs=tuple(r.diff for r in rows), anomalies=anomalies)


# =============================================================================
# Apply (MUTATES — confirm-gated upstream)
# =============================================================================
def _prior_of(row: _PlanRow) -> ProductPrior:
    return ProductPrior(row.diff.product_gid or "", row.diff.sku, tuple(
        VariantPrior(v["id"], money(v.get("price")), money(v.get("compareAtPrice"))) for v in row.live_variants))


def _bind(audit_sink: Any, lease: PassLease) -> None:
    bind = getattr(audit_sink, "bind", None)
    if callable(bind):
        bind(lease)


def _push(transport: Any, diff: PriceDiff, variants: Sequence[Dict[str, Any]]) -> None:
    ops.product_variants_bulk_update(transport, diff.product_gid, [
        {"id": v["id"], "price": diff.price, "compareAtPrice": diff.compare_at} for v in variants])


def _moved_since_capture(row: _PlanRow, prior: Optional[ProductPrior]) -> bool:
    """A push whose live prices are no longer the captured priors (or with none
    captured) cannot be reverted faithfully: it is not sent."""
    if prior is None:
        return True
    now = {v["id"]: (money(v.get("price")), money(v.get("compareAtPrice"))) for v in row.live_variants}
    then = {v.variant_id: (v.price, v.compare_at) for v in prior.variants}
    return now != then


def prices_apply(
    register: Any,
    transport: Any,
    mode: str,
    params: PriceParams,
    approved_plan: PricePlan,
    audit_sink: AuditSink,
    *,
    promo_location_id: str,
    status_override: bool = False,
    lease: Optional[PassLease] = None,
    take_bound_s: float = PRICING_TAKE_BOUND_S,
    sleep: Callable[[float], None] = time.sleep,
) -> PriceApplyReport:
    """Execute the approved plan under the pass lease: priors first, then per SKU
    (re-derived live after each re-take) the register write, then the push."""
    _check_mode(mode)
    run = passes.Holder(register, transport, promo_location_id, HOLDER_PRICING, lease=lease,
                        take_bound_s=take_bound_s, sleep=sleep)
    approved = {d.sku: d for d in approved_plan.diffs}
    outcomes: List[PriceOutcome] = []
    intent_id: Optional[str] = None
    todo: List[_PlanRow] = []
    try:
        run.start()
        _bind(audit_sink, run.lease)
        fresh, _anomalies = _plan_all(register, transport, mode, params, status_override,
                                      collection_status_map(transport))
        for row in fresh:
            d = row.diff
            a = approved.get(d.sku)
            if not d.actionable:
                outcomes.append(PriceOutcome(d.sku, d.product_gid, d.status, d.warnings))
            elif a is None:
                outcomes.append(PriceOutcome(d.sku, d.product_gid, STATUS_NOT_IN_PLAN, ("not_in_approved_plan",)))
            elif d.plan_hash != a.plan_hash:
                outcomes.append(PriceOutcome(d.sku, d.product_gid, STATUS_VERIFY_FAILED,
                                             ("plan_hash_mismatch:live_state_changed",)))
            else:
                todo.append(row)
        if not todo:
            return PriceApplyReport(None, tuple(outcomes), tuple(run.report.items))
        priors = {r.diff.sku: _prior_of(r) for r in todo if r.diff.product_gid and r.diff.live_changed}
        intent = PriceIntent(
            mode=mode,
            plan_hash=hashlib.sha256("|".join(sorted(f"{r.diff.sku}:{r.diff.plan_hash}" for r in todo))
                                     .encode("utf-8")).hexdigest()[:16],
            priors=tuple(priors[s] for s in sorted(priors)),
        )
        try:
            intent_id = audit_sink.capture_before(intent)
        except LeaseLost:
            raise
        except Exception as e:  # noqa: BLE001 - the abort gate: nothing written
            outcomes.extend(PriceOutcome(r.diff.sku, r.diff.product_gid, STATUS_ERROR,
                                         (f"capture_before_failed:{type(e).__name__}",)) for r in todo)
            return PriceApplyReport(None, tuple(outcomes), tuple(run.report.items))
        live_status = core_status(transport)
        for row in todo:
            run.next_item()
            outcomes.append(_apply_sku(register, transport, run.lease, mode, params, row, priors,
                                       intent_id, live_status, status_override))
    except LeaseUnavailable:
        if not run.lease.takes:
            return PriceApplyReport(None, tuple(PriceOutcome(d.sku, d.product_gid, STATUS_ERROR, ("lease_unavailable",))
                                                for d in approved_plan.diffs if d.actionable))
        _lease_lost(outcomes, [(r.diff.sku, r.diff.product_gid) for r in todo])
    except LeaseLost:
        _lease_lost(outcomes, [(r.diff.sku, r.diff.product_gid) for r in todo])
    finally:
        run.release()
    return PriceApplyReport(intent_id, tuple(outcomes), tuple(run.report.items))


def _lease_lost(outcomes: List[PriceOutcome], items: Sequence[Tuple[str, Optional[str]]]) -> None:
    done = {o.sku for o in outcomes}
    outcomes.extend(PriceOutcome(sku, gid, STATUS_ERROR, ("lease_lost",)) for sku, gid in items if sku not in done)


def _apply_sku(register: Any, transport: Any, lease: PassLease, mode: str, params: PriceParams,
               approved_row: _PlanRow, priors: Dict[str, ProductPrior], intent_id: str,
               status_of: StatusOf, status_override: bool) -> PriceOutcome:
    """One SKU, re-derived live under the lease: same plan hash → register, then push."""
    sku = approved_row.diff.sku
    try:
        row = plan_sku(mode, params, sku, register.shelf(sku), transport, status_of, status_override)
    except _SHOPIFY_ERRORS as e:
        return PriceOutcome(sku, approved_row.diff.product_gid, STATUS_ERROR, (f"error:{type(e).__name__}",))
    if row is None or not row.diff.actionable:
        d = row.diff if row is not None else approved_row.diff
        return PriceOutcome(sku, d.product_gid, d.status if row is not None else STATUS_VERIFY_FAILED, d.warnings)
    d = row.diff
    if d.plan_hash != approved_row.diff.plan_hash:
        return PriceOutcome(sku, d.product_gid, STATUS_VERIFY_FAILED, ("plan_hash_mismatch:live_state_changed",))
    push = d.product_gid is not None and d.live_changed
    if push and _moved_since_capture(row, priors.get(sku)):
        return PriceOutcome(sku, d.product_gid, STATUS_VERIFY_FAILED, ("live_prices_moved_since_capture",))
    written = False
    try:
        if d.register_changed:
            assert d.price is not None and d.discount is not None
            written = bool(register.set_outlet_price(lease, sku, Decimal(d.price), Decimal(d.discount),
                                                     ref=intent_id))
        if push:
            lease.keepalive()
            _push(transport, d, row.live_variants)
            return PriceOutcome(sku, d.product_gid, STATUS_APPLIED, d.warnings)
        return PriceOutcome(sku, d.product_gid, STATUS_REGISTER_UPDATED, d.warnings)
    except LeaseLost:
        raise
    except _SHOPIFY_ERRORS as e:
        # the register is the price's home: a refused or unknown push leaves it
        # written, surfaced; a re-run pushes it (live ≠ target), or the publish does
        return PriceOutcome(sku, d.product_gid, STATUS_ERROR,
                            d.warnings + (f"push_failed:{type(e).__name__}",) + (("register_written",) if written else ()))
    except StoreError as e:
        return PriceOutcome(sku, d.product_gid, STATUS_ERROR, d.warnings + (f"register_error:{type(e).__name__}",))


# =============================================================================
# Revert (MUTATES — confirm-gated upstream)
# =============================================================================
def _written_by(register: Any, intent_id: str) -> Dict[str, List[AuditRow]]:
    """The intent's own register writes — its ``set_outlet_price`` rows — by SKU."""
    out: Dict[str, List[AuditRow]] = {}
    for r in register.audit_rows(KIND_PRICE):
        p = r.payload or {}
        if r.action == ACTION_SET and p.get("intent_id") == intent_id and p.get("sku"):
            out.setdefault(str(p["sku"]), []).append(r)
    return out


def _revert_register(register: Any, lease: PassLease, sku: str, rows: Sequence[AuditRow],
                     intent_id: str) -> Tuple[Optional[int], Tuple[str, ...]]:
    """Restore the pairs the intent wrote, still on the shelf, to their priors.
    ``(pairs restored, ())`` — or ``(None, why)`` when a pair moved since (the SKU
    is left untouched)."""
    restore: Dict[int, Tuple[Optional[Decimal], Optional[Decimal]]] = {}
    wrote: Dict[int, Tuple[Optional[Decimal], Optional[Decimal]]] = {}
    for r in sorted(rows, key=lambda x: x.row_id):
        p = r.payload or {}
        value = (_dec(p.get("price_outlet")), _dec(p.get("discount")))
        for pp in p.get("pairs") or ():
            pid = int(pp["pair_id"])
            restore.setdefault(pid, (_dec(pp.get("old_price_outlet")), _dec(pp.get("old_discount"))))
            wrote[pid] = value
    current = {p.pair_id: p for p in register.shelf(sku)}
    moved = sorted(pid for pid in restore
                   if pid in current and (current[pid].price_outlet, current[pid].discount) != wrote[pid])
    if moved:
        return None, (f"price_changed_since:{','.join(str(i) for i in moved)}",)
    groups: Dict[Tuple[Optional[Decimal], Optional[Decimal]], List[int]] = {}
    for pid, value in restore.items():
        if pid in current:
            groups.setdefault(value, []).append(pid)
    n = 0
    for (price, discount), ids in sorted(groups.items(), key=lambda kv: min(kv[1])):
        n += len(register.restore_outlet_price(lease, ids, price, discount, ref=intent_id))
    return n, ()


def revert_prices(
    register: Any,
    transport: Any,
    audit_sink: AuditSink,
    intent_id: str,
    *,
    promo_location_id: str,
    lease: Optional[PassLease] = None,
    take_bound_s: float = PRICING_TAKE_BOUND_S,
    sleep: Callable[[float], None] = time.sleep,
) -> RevertReport:
    """Undo one pricing intent, SKU by SKU, under the pass lease: the register
    first (its pairs back to their priors), then the captured Shopify priors."""
    intent = audit_sink.load(intent_id)
    written = _written_by(register, intent_id)
    priors: Dict[str, List[ProductPrior]] = {}
    for prior in intent.priors:
        priors.setdefault(prior.sku, []).append(prior)
    skus = sorted(set(written) | set(priors))
    run = passes.Holder(register, transport, promo_location_id, HOLDER_PRICING, lease=lease,
                        take_bound_s=take_bound_s, sleep=sleep)
    outcomes: List[PriceOutcome] = []
    products = variants_n = pairs_n = 0
    try:
        run.start()
        for sku in skus:
            run.next_item()
            gid = priors[sku][0].product_gid if sku in priors else None
            try:
                n, why = _revert_register(register, run.lease, sku, written.get(sku, ()), intent_id)
                if n is None:
                    outcomes.append(PriceOutcome(sku, gid, STATUS_REVERT_SKIPPED, why))
                    continue
                pairs_n += n
                for prior in priors.get(sku, ()):
                    run.lease.keepalive()
                    ops.product_variants_bulk_update(transport, prior.product_gid, [
                        {"id": v.variant_id, "price": v.price, "compareAtPrice": v.compare_at}
                        for v in prior.variants])
                    products += 1
                    variants_n += len(prior.variants)
                outcomes.append(PriceOutcome(sku, gid, STATUS_REVERTED, ()))
            except LeaseLost:
                raise
            except _SHOPIFY_ERRORS as e:
                outcomes.append(PriceOutcome(sku, gid, STATUS_ERROR, (f"push_failed:{type(e).__name__}",)))
            except StoreError as e:
                outcomes.append(PriceOutcome(sku, gid, STATUS_ERROR, (f"register_error:{type(e).__name__}",)))
    except LeaseUnavailable:
        why = "lease_lost" if run.lease.takes else "lease_unavailable"
        _lease_lost_as(outcomes, skus, why)
    except LeaseLost:
        _lease_lost_as(outcomes, skus, "lease_lost")
    finally:
        run.release()
    return RevertReport(intent_id, products, variants_n, tuple(outcomes), pairs_n, tuple(run.report.items))


def _lease_lost_as(outcomes: List[PriceOutcome], skus: Sequence[str], why: str) -> None:
    done = {o.sku for o in outcomes}
    outcomes.extend(PriceOutcome(s, None, STATUS_ERROR, (why,)) for s in skus if s not in done)


# =============================================================================
# Discharge-debt recon (READ-ONLY count of broken live outlets)
# =============================================================================
def discharge_debt_count(transport: Any, *, collection_gid: str = OUTLET_COLLECTION_GID) -> DebtReport:
    """Count live ACTIVE outlets whose variants fail FIX4 (READ-ONLY): a product is
    'broken' if any variant has ``price <= 0``, ``price >= compareAt`` or a
    missing / invalid compare-at — the debt the pricing screen repairs."""
    members = ops.enumerate_outlet_products(transport, collection_gid)
    scanned = broken_products = broken_variants = 0
    broken_gids: List[str] = []
    for m in members:
        if (m.get("status") or "").upper() != _STATUS_ACTIVE:
            continue
        scanned += 1
        try:
            vs = ops.get_product_variants(transport, m["id"])
        except _SHOPIFY_ERRORS + (RuntimeError,):
            continue  # best-effort recon: an unreadable product is not counted
        bad = sum(1 for v in vs if not validate_price(money(v.get("price")), money(v.get("compareAtPrice")))[0])
        if bad:
            broken_products += 1
            broken_variants += bad
            broken_gids.append(m["id"])
    return DebtReport(scanned, broken_products, broken_variants, tuple(broken_gids))


__all__ = [
    "MODE_PERCENT", "MODE_DIRECT", "MODE_BULK", "SCOPE_KEYS",
    "BulkRule", "PriceParams",
    "AuditSink", "VariantPrior", "ProductPrior", "PriceIntent",
    "PriceDiff", "PricePlan", "PriceOutcome", "PriceApplyReport", "RevertReport", "DebtReport",
    "money", "discount_of", "plan_sku", "collection_status_map", "core_status",
    "prices_preview", "prices_apply", "revert_prices", "discharge_debt_count",
]
