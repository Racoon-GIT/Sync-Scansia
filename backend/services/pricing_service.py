"""FIX_PRICES successor — safe price/discount maintenance over the Scansia sheet.

Replaces the legacy ``fix_prices.py`` monolith with a preview/apply split so
DRY_RUN is fail-closed by construction, composing the same three leaf layers as
:mod:`backend.services.outlet_service`:

* :func:`prices_preview` — READ-ONLY. Reads the sheet with ``assign_uuids=False``
  (never mutates the Sheet), computes the target ``(price, compareAtPrice)`` per
  SKU group for the chosen mode, and returns a :class:`PricePlan` of
  :class:`PriceDiff` (sheet<->live diff, validation, status, warnings). Nothing on
  Shopify or the Sheet is touched.
* :func:`prices_apply` — MUTATES (confirm-gated upstream by the APPLY token /
  the web confirm-nonce). Re-reads with ``assign_uuids=True``, re-plans live
  (TOCTOU: a drifted target/status -> ``VERIFY_FAILED``), writes the decided
  price back to the sheet, captures the PRIOR ``price``/``compareAtPrice`` of every
  impacted variant via the injected :class:`AuditSink` BEFORE any Shopify push,
  then pushes.
* :func:`revert_prices` — consumes an ``intent_id`` from the audit sink and
  re-pushes the captured prior values (the concrete rollback affordance required
  by the plan, not a snapshot-that-can't-be-invoked).
* :func:`discharge_debt_count` — READ-ONLY recon: counts live outlets whose
  variants would fail price validation (``price<=0`` / ``price>=compareAt`` /
  compareAt missing) — the FIX_PRICES debt the successor must actually close.

Mapping (authoritative, legacy fix_prices.py §"Prezzi/Sconti" M4):
``compareAtPrice <- col H (prezzo_high)``; ``price <- col J (prezzo_outlet)``;
Shopify target ``product_gid <- col Q (product_id)``.

Three modes (plan §Prezzi 1/2/3):

1. **percent** — ``price = round(prezzo_high * (1 - pct), 2)`` per product; the
   pct and computed price are written to the sheet (``sconto`` + ``prezzo_outlet``).
2. **direct / repair** — operator-typed ``prezzo_outlet`` (or the sheet value
   as-is); pushed to Shopify. **fill-missing is REFUSED** (owner decision
   Q-fixprices): an empty required price -> ``SKIP_MISSING_PRICE``, never
   cross-filled from the twin column, never defaulted to ``0.00`` (that legacy
   behaviour re-introduced the zero-price bug it was meant to fix).
3. **bulk rules** — ordered rules scoped by product attributes; precedence is
   **per-product override > bulk rule**, and among matching bulk rules the LAST
   defined wins (overlap tie-break).

Invariants preserved from legacy (SPEC A1/A3/A4/A5):

* **Row-eligibility** — only ``online=SI AND qta>0`` rows (via the canonical
  ``ScansiaSheet.eligible_rows``), unless an explicit operator ``row_override``.
* **Q gate (security)** — an empty col Q -> ``SKIP_NO_PRODUCT_ID`` with NO
  SKU-search fallback (the cardinal fix_prices safety filter).
* **Status ACTIVE-only** — non-ACTIVE outlets are skipped by default
  (``SKIP_DRAFT``); ``status_override`` documents the intentional extension.
* **skip-if-correct** (SPEC B2) — a product whose live variants already carry the
  target ``(price, compareAt)`` is NOT re-pushed (``SKIP_ALREADY_CORRECT``).
* **Uniform price** — the same ``(price, compareAtPrice)`` is broadcast to every
  variant of the product (:func:`ops.product_variants_bulk_update`).

Boundary hygiene mirrors ``outlet_service``: raw ``ShopifyUserError`` /
``ShopifyTransportError`` / ``GSheetError`` / any unexpected ``RuntimeError`` from
a read op are caught per-group and translated to a bounded, code-shaped outcome —
one SKU's failure never aborts the batch.

Status source note: the ACTIVE-only filter reads product status from ONE
``ops.enumerate_outlet_products`` call (gid->status over the ground-truth OUTLET
collection) rather than a per-SKU search — cheap, and it applies the status gate
uniformly to a col-Q GID (closing the legacy D2 gap where the GID branch
synthesised ``ACTIVE`` and bypassed the filter). Caveat: a col-Q GID that is not
(yet) a member of the OUTLET collection surfaces as ``SKIP_NOT_FOUND``.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Protocol, Tuple

from backend.gsheet import GSheetError, ScansiaSheet
from backend.gsheet.reader import _clean_price
from backend.shopify import ops
from backend.shopify.ops import OUTLET_COLLECTION_GID, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.pricing_service")

# --- modes -----------------------------------------------------------------
MODE_PERCENT = "percent"
MODE_DIRECT = "direct"
MODE_BULK = "bulk"
_MODES = frozenset({MODE_PERCENT, MODE_DIRECT, MODE_BULK})

_GID_PREFIX = "gid://shopify/Product/"
_STATUS_ACTIVE = "ACTIVE"

# --- status / reason codes (surfaced on PriceDiff.status / PriceOutcome.status)
STATUS_OK = "OK"                                # preview: actionable, will apply
STATUS_APPLIED = "APPLIED"                       # apply: sheet written + pushed
STATUS_SHEET_UPDATED = "SHEET_UPDATED"           # apply: sheet written, live already correct
STATUS_ALREADY_CORRECT = "SKIP_ALREADY_CORRECT"  # sheet + live already on target
STATUS_NO_PRODUCT_ID = "SKIP_NO_PRODUCT_ID"      # col Q empty (security gate)
STATUS_Q_NOT_GID = "SKIP_Q_NOT_GID"              # col Q present but not a GID (handle-shape)
STATUS_NOT_FOUND = "SKIP_NOT_FOUND"              # gid not an outlet-collection member / no variants
STATUS_DRAFT = "SKIP_DRAFT"                       # status != ACTIVE (ACTIVE-only)
STATUS_MISSING_PRICE = "SKIP_MISSING_PRICE"       # fill-missing REFUSED (B1)
STATUS_PRICE_INVALID = "PRICE_INVALID"           # price<=0 / >=compareAt / compareAt<=0
STATUS_VERIFY_FAILED = "VERIFY_FAILED"           # apply TOCTOU: plan_hash drifted
STATUS_NOT_IN_PLAN = "NOT_IN_PLAN"               # apply: sku absent from approved plan
STATUS_ERROR = "ERROR"
STATUS_REVERTED = "REVERTED"


# =============================================================================
# Params (per-mode inputs the GUI passes down)
# =============================================================================


@dataclass(frozen=True)
class BulkRule:
    """One bulk-pricing rule (mode 3). ``percent`` is a fraction (0.30 == 30% off).

    ``scope`` maps a product-attribute name -> the accepted values for it; ALL
    entries must match (AND). An empty scope matches every product. Attributes are
    the group's normalized sheet columns plus ``sku`` (e.g. ``{"brand": ("Nike",)}``
    matches rows whose ``brand`` cell is ``Nike``).
    """

    percent: float
    scope: Mapping[str, Tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class PriceParams:
    """Unified per-mode parameters. Only the fields for the active mode are read."""

    # mode PERCENT: sku -> discount fraction.
    percent_by_sku: Mapping[str, float] = field(default_factory=dict)
    # mode DIRECT: sku -> explicit prezzo_outlet override (else the sheet col J as-is).
    price_by_sku: Mapping[str, str] = field(default_factory=dict)
    # mode BULK: ordered rules (last matching wins) + per-product overrides (win over rules).
    rules: Tuple[BulkRule, ...] = ()
    override_percent_by_sku: Mapping[str, float] = field(default_factory=dict)


# =============================================================================
# Audit sink (injected — mocked in tests; the SQLite impl lives in backend.audit)
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
    """The before-snapshot persisted by :meth:`AuditSink.capture_before`."""

    mode: str
    plan_hash: str
    priors: Tuple[ProductPrior, ...]


class AuditSink(Protocol):
    """Durable append-only audit boundary (dependency-injected).

    ``capture_before`` persists the prior state and returns the intent id that
    :func:`revert_prices` later consumes; ``load`` retrieves it.
    """

    def capture_before(self, intent: PriceIntent) -> str: ...

    def load(self, intent_id: str) -> PriceIntent: ...


# =============================================================================
# Plan / report data model
# =============================================================================


@dataclass(frozen=True)
class PriceDiff:
    sku: str
    product_gid: Optional[str]
    status: str
    actionable: bool
    price: Optional[str]            # target price (col J) -> Shopify price
    compare_at: Optional[str]       # target compareAt (col H) -> Shopify compareAtPrice
    percent: Optional[float]        # mode 1/3: fraction written to `sconto`
    sheet_price: Optional[str]      # current col J value in the sheet
    sheet_changed: bool             # target price != sheet current
    live_price: Optional[str]       # live variant price (first variant)
    live_compare_at: Optional[str]
    live_changed: bool              # target != live (a push is needed)
    live_status: Optional[str]
    row_uuids: Tuple[str, ...]
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


@dataclass(frozen=True)
class RevertReport:
    intent_id: str
    reverted_products: int
    reverted_variants: int
    outcomes: Tuple[PriceOutcome, ...]


@dataclass(frozen=True)
class DebtReport:
    scanned_products: int
    broken_products: int
    broken_variants: int
    broken_gids: Tuple[str, ...]


# Internal planning row: the public diff plus the live variants (needed by apply
# for the prior snapshot + the broadcast push; dropped from the public plan).
@dataclass(frozen=True)
class _PlanRow:
    diff: PriceDiff
    live_variants: Tuple[Dict[str, Any], ...]
    expected_sku: str


# =============================================================================
# Pure helpers
# =============================================================================

_CAUGHT = (ShopifyUserError, ShopifyTransportError, GSheetError, RuntimeError)


def _check_mode(mode: str) -> None:
    if mode not in _MODES:
        raise ValueError(f"unknown price mode {mode!r} (expected one of {sorted(_MODES)})")


def _is_gid(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(_GID_PREFIX)


def _fmt_percent(percent: float) -> str:
    """Sheet-cell form of a discount fraction: ``0.30 -> '30'``, ``0.125 -> '12.5'``."""
    return f"{percent * 100:g}"


def _percent_price(high: Optional[str], percent: float) -> Optional[str]:
    """``round(prezzo_high * (1 - pct), 2)`` as a 2-decimal string, or None."""
    if not high:
        return None
    try:
        return f"{round(float(high) * (1.0 - percent), 2):.2f}"
    except (TypeError, ValueError):
        return None


def _validate_price(
    price: Optional[str], compare_at: Optional[str]
) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """Reject missing/zero/negative price, missing/zero compareAt, or price>=compareAt.

    Mirrors ``outlet_service._validate_price`` (FIX4) so publish and prices reject
    identically. Returns ``(ok, price, compare_at, reason)``.
    """
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


def _plan_hash(
    gid: Optional[str], price: Optional[str], compare_at: Optional[str], status: Optional[str]
) -> str:
    """TOCTOU verify key: bind {target gid, target price, target compareAt, status}."""
    raw = f"{gid or ''}|{price or ''}|{compare_at or ''}|{(status or '').upper()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _first_nonempty(group: List[Any], attr: str) -> Optional[Any]:
    for r in group:
        v = getattr(r, attr, None)
        if v not in (None, ""):
            return v
    return None


def _group_by_sku(rows: List[Any]) -> List[Tuple[str, List[Any]]]:
    """Group rows by trimmed SKU, preserving first-seen order; blank SKU dropped."""
    groups: Dict[str, List[Any]] = {}
    order: List[str] = []
    for r in rows:
        sku = (getattr(r, "sku", "") or "").strip()
        if not sku:
            continue
        if sku not in groups:
            groups[sku] = []
            order.append(sku)
        groups[sku].append(r)
    return [(sku, groups[sku]) for sku in order]


def _scope_matches(scope: Mapping[str, Tuple[str, ...]], attrs: Mapping[str, Any]) -> bool:
    for key, accepted in scope.items():
        if str(attrs.get(key, "")).strip() not in accepted:
            return False
    return True


def _bulk_percent(params: PriceParams, sku: str, attrs: Mapping[str, Any]) -> Optional[float]:
    """Mode-3 precedence: per-product override > last matching bulk rule > None."""
    if sku in params.override_percent_by_sku:
        return float(params.override_percent_by_sku[sku])
    chosen: Optional[float] = None
    for rule in params.rules:
        if _scope_matches(rule.scope, attrs):
            chosen = float(rule.percent)  # last match wins (overlap tie-break)
    return chosen


def _is_broken(price: Optional[str], compare_at: Optional[str]) -> bool:
    """A live variant is 'debt' iff it would fail price validation."""
    ok, *_ = _validate_price(price, compare_at)
    return not ok


# =============================================================================
# Planning (shared by preview and apply's TOCTOU re-plan)
# =============================================================================


def _skip(
    sku: str,
    gid: Optional[str],
    status: str,
    *,
    price: Optional[str] = None,
    compare_at: Optional[str] = None,
    percent: Optional[float] = None,
    sheet_price: Optional[str] = None,
    row_uuids: Tuple[str, ...] = (),
    warnings: Tuple[str, ...] = (),
    live_status: Optional[str] = None,
) -> _PlanRow:
    diff = PriceDiff(
        sku=sku,
        product_gid=gid,
        status=status,
        actionable=False,
        price=price,
        compare_at=compare_at,
        percent=percent,
        sheet_price=sheet_price,
        sheet_changed=False,
        live_price=None,
        live_compare_at=None,
        live_changed=False,
        live_status=live_status,
        row_uuids=tuple(row_uuids),
        warnings=tuple(warnings),
        plan_hash=_plan_hash(gid, price, compare_at, live_status or ""),
    )
    return _PlanRow(diff, (), sku)


def _error_row(sku: str, gid: Optional[str], err: str) -> _PlanRow:
    diff = PriceDiff(
        sku=sku, product_gid=gid, status=STATUS_ERROR, actionable=False, price=None,
        compare_at=None, percent=None, sheet_price=None, sheet_changed=False, live_price=None,
        live_compare_at=None, live_changed=False, live_status=None, row_uuids=(),
        warnings=(f"error:{err}",), plan_hash=_plan_hash(gid, None, None, ""),
    )
    return _PlanRow(diff, (), sku)


def _target_price(mode: str, params: PriceParams, sku: str, group: List[Any], high: Optional[str],
                  sheet_outlet: Optional[str]) -> Tuple[bool, Optional[str], Optional[float]]:
    """Compute (in_scope, target_price, percent) for a group in the given mode.

    ``in_scope=False`` means the group is not addressed by this operation (mode 1/3
    with no matching pct) and is dropped from the plan entirely.
    """
    if mode == MODE_PERCENT:
        pct = params.percent_by_sku.get(sku)
        if pct is None:
            return False, None, None
        return True, _percent_price(high, float(pct)), float(pct)
    if mode == MODE_BULK:
        attrs = {"sku": sku, **(getattr(group[0], "raw", None) or {})}
        pct = _bulk_percent(params, sku, attrs)
        if pct is None:
            return False, None, None
        return True, _percent_price(high, pct), pct
    # MODE_DIRECT: operator override, else the sheet col J value as-is (repair).
    override = params.price_by_sku.get(sku)
    price = _clean_price(override) if override not in (None, "") else sheet_outlet
    return True, price, None


def _plan_group(
    mode: str, params: PriceParams, sku: str, group: List[Any], transport: Any,
    status_map, status_override: bool,
) -> Optional[_PlanRow]:
    warnings: List[str] = []
    high = _first_nonempty(group, "prezzo_high")
    sheet_outlet = _first_nonempty(group, "prezzo_outlet")
    gid = _first_nonempty(group, "product_id")
    row_uuids = tuple(r.row_uuid for r in group)

    distinct_q = {r.product_id for r in group if getattr(r, "product_id", "")}
    if len(distinct_q) > 1:
        warnings.append(f"multi_product_id:{sorted(distinct_q)}")

    in_scope, price, percent = _target_price(mode, params, sku, group, high, sheet_outlet)
    if not in_scope:
        return None
    compare_at = high

    # --- Q gate (security invariant A3): empty Q -> skip, NO SKU-search fallback ---
    if not gid:
        return _skip(sku, None, STATUS_NO_PRODUCT_ID, price=price, compare_at=compare_at,
                     percent=percent, sheet_price=sheet_outlet, row_uuids=row_uuids,
                     warnings=tuple(warnings))
    if not _is_gid(gid):
        warnings.append("q_not_gid:handle_shape_unsupported")
        return _skip(sku, gid, STATUS_Q_NOT_GID, price=price, compare_at=compare_at,
                     percent=percent, sheet_price=sheet_outlet, row_uuids=row_uuids,
                     warnings=tuple(warnings))

    # --- validation / fill-missing REFUSE (B1) ---
    ok, price, compare_at, reason = _validate_price(price, compare_at)
    if not ok:
        status = (STATUS_MISSING_PRICE if reason in ("price_missing", "compare_at_missing")
                  else STATUS_PRICE_INVALID)
        warnings.append(f"price:{reason}")
        return _skip(sku, gid, status, price=price, compare_at=compare_at, percent=percent,
                     sheet_price=sheet_outlet, row_uuids=row_uuids, warnings=tuple(warnings))

    # --- status ACTIVE-only (A4) ---
    live_status = status_map().get(gid)
    if live_status is None:
        warnings.append("not_in_outlet_collection")
        return _skip(sku, gid, STATUS_NOT_FOUND, price=price, compare_at=compare_at,
                     percent=percent, sheet_price=sheet_outlet, row_uuids=row_uuids,
                     warnings=tuple(warnings))
    if live_status != _STATUS_ACTIVE and not status_override:
        warnings.append(f"status:{live_status}")
        return _skip(sku, gid, STATUS_DRAFT, price=price, compare_at=compare_at, percent=percent,
                     sheet_price=sheet_outlet, row_uuids=row_uuids, warnings=tuple(warnings),
                     live_status=live_status)

    # --- live diff (sheet<->live) ---
    variants = ops.get_product_variants(transport, gid)
    if not variants:
        warnings.append("no_variants")
        return _skip(sku, gid, STATUS_NOT_FOUND, price=price, compare_at=compare_at,
                     percent=percent, sheet_price=sheet_outlet, row_uuids=row_uuids,
                     warnings=tuple(warnings), live_status=live_status)

    live_price = _clean_price(variants[0].get("price"))
    live_compare = _clean_price(variants[0].get("compareAtPrice"))
    # skip-if-correct (B2): normalize both sides before comparing (None vs "129.00").
    live_changed = any(
        _clean_price(v.get("price")) != price or _clean_price(v.get("compareAtPrice")) != compare_at
        for v in variants
    )
    sheet_changed = _clean_price(sheet_outlet) != price
    actionable = live_changed or sheet_changed
    status = STATUS_OK if actionable else STATUS_ALREADY_CORRECT
    diff = PriceDiff(
        sku=sku, product_gid=gid, status=status, actionable=actionable, price=price,
        compare_at=compare_at, percent=percent, sheet_price=sheet_outlet,
        sheet_changed=sheet_changed, live_price=live_price, live_compare_at=live_compare,
        live_changed=live_changed, live_status=live_status, row_uuids=row_uuids,
        warnings=tuple(warnings), plan_hash=_plan_hash(gid, price, compare_at, live_status),
    )
    return _PlanRow(diff, tuple(variants), sku)


def _plan_all(
    mode: str, params: PriceParams, groups: List[Tuple[str, List[Any]]], transport: Any,
    status_override: bool,
) -> List[_PlanRow]:
    """Plan every SKU group, isolating per-group Shopify/read errors to an ERROR row.

    ``status_map`` is a memoizing closure so the single
    ``ops.enumerate_outlet_products`` call is made lazily (only when a group
    actually reaches the status gate) and at most once.
    """
    cache: Dict[str, Dict[str, str]] = {}

    def status_map() -> Dict[str, str]:
        if "m" not in cache:
            members = ops.enumerate_outlet_products(transport)
            cache["m"] = {n["id"]: (n.get("status") or "").upper() for n in members}
        return cache["m"]

    out: List[_PlanRow] = []
    for sku, group in groups:
        try:
            row = _plan_group(mode, params, sku, group, transport, status_map, status_override)
        except _CAUGHT as e:
            out.append(_error_row(sku, _first_nonempty(group, "product_id"), type(e).__name__))
            continue
        if row is not None:
            out.append(row)
    return out


def _eligible_groups(
    sheet: Any, mode: str, params: PriceParams, transport: Any, *,
    assign_uuids: bool, row_override: bool, status_override: bool,
) -> Tuple[List[_PlanRow], Tuple[str, ...]]:
    read = sheet.read_canonical(assign_uuids=assign_uuids)
    eligible = ScansiaSheet.eligible_rows(read.rows, override=row_override)
    groups = _group_by_sku(eligible)
    rows = _plan_all(mode, params, groups, transport, status_override)
    anomalies = tuple(
        f"{a.kind}:{a.sku}:{a.row_uuid}" for a in getattr(read, "anomalies", ()) or ()
    )
    return rows, anomalies


# =============================================================================
# Preview (READ-ONLY, DRY_RUN-safe)
# =============================================================================


def prices_preview(
    sheet: Any, transport: Any, mode: str, params: PriceParams, *,
    row_override: bool = False, status_override: bool = False,
) -> PricePlan:
    """Compute the price PLAN without mutating Shopify or the Sheet.

    Reads canonically with ``assign_uuids=False`` (no row_uuid minted, no cell
    written). Returns a :class:`PricePlan` whose diffs carry the sheet<->live
    comparison, validation status, and TOCTOU ``plan_hash``.
    """
    _check_mode(mode)
    rows, anomalies = _eligible_groups(
        sheet, mode, params, transport,
        assign_uuids=False, row_override=row_override, status_override=status_override,
    )
    return PricePlan(dry_run=True, mode=mode, diffs=tuple(r.diff for r in rows), anomalies=anomalies)


# =============================================================================
# Apply (MUTATES — confirm-gated upstream)
# =============================================================================


def _write_sheet(sheet: Any, row: _PlanRow) -> None:
    d = row.diff
    fields: Dict[str, Any] = {"prezzo_outlet": d.price}
    if d.percent is not None:
        fields["sconto"] = _fmt_percent(d.percent)
    for u in d.row_uuids:
        sheet.write_back(u, fields, expected_sku=row.expected_sku)


def _prior_of(row: _PlanRow) -> ProductPrior:
    variants = tuple(
        VariantPrior(v["id"], v.get("price"), v.get("compareAtPrice")) for v in row.live_variants
    )
    return ProductPrior(row.diff.product_gid, row.diff.sku, variants)


def _push(transport: Any, row: _PlanRow) -> None:
    d = row.diff
    updates = [
        {"id": v["id"], "price": d.price, "compareAtPrice": d.compare_at} for v in row.live_variants
    ]
    ops.product_variants_bulk_update(transport, d.product_gid, updates)


def prices_apply(
    sheet: Any, transport: Any, mode: str, params: PriceParams, approved_plan: PricePlan,
    audit_sink: AuditSink, *, row_override: bool = False, status_override: bool = False,
) -> PriceApplyReport:
    """Execute the approved plan: capture PRIORs, THEN write the sheet and push.

    Re-reads the sheet (``assign_uuids=True``) and re-plans LIVE (TOCTOU). For each
    fresh, actionable group whose ``plan_hash`` still equals the approved plan's:
    a group whose live variants are already on target only needs its sheet cell
    updated (``SHEET_UPDATED``). A group whose live variants differ from target is
    queued into ``push_set`` — its sheet write is DEFERRED: only once
    ``audit_sink.capture_before`` has durably captured the PRIOR
    ``price``/``compareAtPrice`` of every queued group does its sheet cell get
    written and its ``product_variants_bulk_update`` get pushed (in that order,
    per row). This is deliberate: it prevents a sheet-updated-but-not-pushed
    drift if the sink itself fails — ANY exception from ``capture_before`` (not
    just the ops-boundary set) is caught, propagates NO raw traceback, touches NO
    sheet cell in ``push_set``, pushes nothing, and surfaces a bounded ``ERROR``
    outcome per queued sku. On success the intent id is returned for
    :func:`revert_prices`. A drifted ``plan_hash`` -> ``VERIFY_FAILED``.

    Per-group isolation: any Shopify/Sheet/read error is caught and reported as an
    ``ERROR`` outcome for that sku only — the batch always completes.
    """
    _check_mode(mode)
    fresh_rows, _anoms = _eligible_groups(
        sheet, mode, params, transport,
        assign_uuids=True, row_override=row_override, status_override=status_override,
    )
    approved_by_sku = {d.sku: d for d in approved_plan.diffs}

    outcomes: List[PriceOutcome] = []
    push_set: List[_PlanRow] = []

    for row in fresh_rows:
        d = row.diff
        if not d.actionable:
            outcomes.append(PriceOutcome(d.sku, d.product_gid, d.status, d.warnings))
            continue
        approved = approved_by_sku.get(d.sku)
        if approved is None:
            outcomes.append(PriceOutcome(d.sku, d.product_gid, STATUS_NOT_IN_PLAN,
                                         ("not_in_approved_plan",)))
            continue
        if d.plan_hash != approved.plan_hash:
            outcomes.append(PriceOutcome(d.sku, d.product_gid, STATUS_VERIFY_FAILED,
                                         ("plan_hash_mismatch:live_state_changed",)))
            continue
        if d.live_changed:
            # Sheet write DEFERRED until after a successful capture_before (below)
            # — never ahead of an un-pushed live price.
            push_set.append(row)
            continue
        try:
            if d.sheet_changed:
                _write_sheet(sheet, row)
            outcomes.append(PriceOutcome(d.sku, d.product_gid, STATUS_SHEET_UPDATED, d.warnings))
        except _CAUGHT as e:
            outcomes.append(PriceOutcome(d.sku, d.product_gid, STATUS_ERROR,
                                         (f"unexpected_error:{type(e).__name__}",)))

    # Capture BEFORE any sheet write / push for the push_set: all priors first,
    # one intent -> one revert.
    intent_id: Optional[str] = None
    if push_set:
        intent = PriceIntent(
            mode=mode,
            plan_hash=_plan_hash(
                "|".join(r.diff.product_gid or "" for r in push_set), None, None, mode
            ),
            priors=tuple(_prior_of(r) for r in push_set),
        )
        try:
            intent_id = audit_sink.capture_before(intent)
        except Exception as e:  # noqa: BLE001 - the sink's own failure must not
            # abort the process raw NOR leave the sheet ahead of an un-pushed
            # live price: nothing in push_set has touched the sheet yet.
            for row in push_set:
                d = row.diff
                outcomes.append(PriceOutcome(d.sku, d.product_gid, STATUS_ERROR,
                                             (f"capture_before_failed:{type(e).__name__}",)))
            return PriceApplyReport(intent_id=None, outcomes=tuple(outcomes))

        for row in push_set:
            d = row.diff
            try:
                if d.sheet_changed:
                    _write_sheet(sheet, row)
                _push(transport, row)
                outcomes.append(PriceOutcome(d.sku, d.product_gid, STATUS_APPLIED, d.warnings))
            except _CAUGHT as e:
                outcomes.append(PriceOutcome(d.sku, d.product_gid, STATUS_ERROR,
                                             (f"unexpected_error:{type(e).__name__}",)))

    return PriceApplyReport(intent_id=intent_id, outcomes=tuple(outcomes))


# =============================================================================
# Revert (re-push captured priors — the concrete rollback affordance)
# =============================================================================


def revert_prices(transport: Any, audit_sink: AuditSink, intent_id: str) -> RevertReport:
    """Consume an audit ``intent_id`` and re-push each variant's captured prior.

    Confirm-gated upstream. Per-product isolation: a failed re-push is an
    ``ERROR`` outcome for that product only.
    """
    intent = audit_sink.load(intent_id)
    outcomes: List[PriceOutcome] = []
    reverted_products = 0
    reverted_variants = 0
    for prior in intent.priors:
        updates = [
            {"id": v.variant_id, "price": v.price, "compareAtPrice": v.compare_at}
            for v in prior.variants
        ]
        try:
            ops.product_variants_bulk_update(transport, prior.product_gid, updates)
            reverted_products += 1
            reverted_variants += len(updates)
            outcomes.append(PriceOutcome(prior.sku, prior.product_gid, STATUS_REVERTED, ()))
        except _CAUGHT as e:
            outcomes.append(PriceOutcome(prior.sku, prior.product_gid, STATUS_ERROR,
                                         (f"unexpected_error:{type(e).__name__}",)))
    return RevertReport(intent_id, reverted_products, reverted_variants, tuple(outcomes))


# =============================================================================
# Discharge-debt recon (READ-ONLY count of broken live outlets)
# =============================================================================


def discharge_debt_count(
    transport: Any, *, collection_gid: str = OUTLET_COLLECTION_GID,
) -> DebtReport:
    """Count live ACTIVE outlets whose variants fail price validation (READ-ONLY).

    A product counts as 'broken' if any variant has ``price<=0``, ``price>=compareAt``,
    or a missing/invalid compareAt — the exact debt the successor repairs. No
    mutation, no Sheet access.
    """
    members = ops.enumerate_outlet_products(transport, collection_gid)
    scanned = 0
    broken_products = 0
    broken_variants = 0
    broken_gids: List[str] = []
    for m in members:
        if (m.get("status") or "").upper() != _STATUS_ACTIVE:
            continue
        scanned += 1
        try:
            variants = ops.get_product_variants(transport, m["id"])
        except _CAUGHT:
            continue  # best-effort recon: an unreadable product is not counted
        prod_broken = sum(
            1 for v in variants
            if _is_broken(_clean_price(v.get("price")), _clean_price(v.get("compareAtPrice")))
        )
        if prod_broken:
            broken_products += 1
            broken_variants += prod_broken
            broken_gids.append(m["id"])
    return DebtReport(scanned, broken_products, broken_variants, tuple(broken_gids))


__all__ = [
    "MODE_PERCENT", "MODE_DIRECT", "MODE_BULK",
    "BulkRule", "PriceParams",
    "AuditSink", "VariantPrior", "ProductPrior", "PriceIntent",
    "PriceDiff", "PricePlan", "PriceOutcome", "PriceApplyReport", "RevertReport", "DebtReport",
    "prices_preview", "prices_apply", "revert_prices", "discharge_debt_count",
]
