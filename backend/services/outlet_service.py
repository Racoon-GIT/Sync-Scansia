"""Stateless PUBLISH orchestration for the Scansia outlet lifecycle.

Composes the three leaf layers — ``backend.gsheet`` (canonical sheet + row_uuid/
reconciled model), ``backend.services.resolvers`` (SKU -> outlet/source), and
``backend.shopify.ops`` (Admin-GraphQL op-wrappers) — into the publish flow that
replaces the legacy ``src/sync.py`` ``process_sku_group`` monolith.

Two entry points, split preview/apply so DRY_RUN is fail-closed by construction:

* :func:`publish_preview` — READ-ONLY. Reads the sheet with ``assign_uuids=False``
  (never mutates the Sheet), re-resolves each SKU live, and returns a :class:`Plan`
  of :class:`PlanAction` (branch, target, validated prices, per-size deltas,
  structured warnings). Nothing on Shopify or the Sheet is touched.
* :func:`publish_apply` — MUTATES (confirm-gated by the caller / the APPLY token in
  ``main.py``). Re-reads with ``assign_uuids=True``, re-resolves live (TOCTOU), and
  for every action whose ``plan_hash`` still matches the approved plan, executes
  the branch. A branch whose live state drifted since preview -> ``VERIFY_FAILED``.

Design invariants (from the approved plan, §"POST /outlet/publish"):

* **CREATE anti-phantom-stock ORDER** — ``ops.product_duplicate`` passes
  ``newStatus: DRAFT`` so the duplicate is NEVER ACTIVE for even one instant
  (2025-07 ``productDuplicate`` with no ``newStatus`` inherits the SOURCE's
  status — post-review fix); zero+disconnect ALL non-Promo locations (FIX1);
  zero every Promo variant; set the return delta only on returned sizes;
  normalize inventoryPolicy=DENY; and ONLY THEN flip ACTIVE + publish (the two
  LAST mutations). ACTIVE != published.
* **DRAFT-revive quarantine gate** — a DRAFT outlet being revived by a fresh
  return is gated by the SAME ``_has_non_promo_stock`` check as ACTIVE, in both
  ``_draft_action`` (planning) and ``_execute_draft`` (execution, belt-and-braces
  against the plan/apply TOCTOU window) — mirrors ``_active_action``/
  ``_execute_active``. Without this gate, reviving a DRAFT that inherited
  non-Promo stock (e.g. a CREATE that crashed mid-cleanup, now landing DRAFT
  post-fix) would publish with phantom stock.
* **DELTA exactly-once** — the return delta is the sum of the NON-reconciled sheet
  rows for a (sku,size). We compare the live Promo ``available`` against the
  ``frozen_pre`` captured at preview: match -> apply once + mark_reconciled;
  live already >= target -> treat as already applied (mark, surface anomaly, NO
  re-add); otherwise -> compare-mismatch, do NOT mark, request re-preview. This
  prefers UNDER-count to DOUBLE-count and never oversells.
* **FIX4** price validation (reject 0 / >= compareAt / missing) blocks publish.
* **FIX5** variant match keyed by normalized SIZE (primary), SKU only a secondary
  best-effort cross-check; unmatched sizes are surfaced, never silently zeroed.
  The cross-check reads ``sku`` off ``ops.read_variant_inventory`` output too
  (added post-review), so it fires on ACTIVE/DRAFT, not just CREATE.
* **Publication resolved EAGER/fail-fast** — ``publish_apply`` resolves the
  "Online Store" publication ONCE, up front, before any action executes, if the
  fresh batch contains at least one CREATE/DRAFT branch (the only branches that
  publish). A resolution failure aborts the WHOLE batch cleanly (every action
  reported ``ERROR``, nothing mutated) instead of failing lazily after a
  CREATE/DRAFT has already flipped ACTIVE and marked rows reconciled.

Boundary hygiene: raw ``ShopifyUserError`` / ``ShopifyTransportError`` / gspread
errors — and any other unexpected exception (e.g. a ``RuntimeError`` from a read
op) — are caught per-action and translated to a bounded, code-shaped outcome —
never leaked verbatim — so one SKU's failure never aborts the batch.

NOTE on the compareQuantity CAS: the leaf ``inventory_set_quantities`` op issues
an unconditional ``on_hand`` set (``ignoreCompareQuantity: true``). The SPEC's
optimistic compareQuantity is therefore realized at THIS layer as a
read-compare-against-frozen_pre (single-writer cron; the deploy is
single-instance), which delivers the same exactly-once decision tree for the
"already applied by Make / a crashed previous run" case.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from backend.config import load_shopify_config
from backend.gsheet import GSheetError, ScansiaSheet
from backend.services import resolvers
from backend.shopify import ops
from backend.shopify.ops import ShopifyUserError
from backend.shopify.transport import ShopifyTransport, ShopifyTransportError

logger = logging.getLogger("backend.services.outlet_service")

# Branches (mirror the SPEC's three publish branches).
BRANCH_CREATE = "CREATE"
BRANCH_ACTIVE = "ACTIVE"
BRANCH_DRAFT = "DRAFT"

# selectedOptions names that carry the shoe size (FIX5: not just size/taglia).
_SIZE_OPTION_NAMES = frozenset({"size", "taglia", "numero"})

# Non-publishable skip reasons -> outcome status codes surfaced to the caller.
_STATUS_BY_REASON = {
    "no_source": "NO_SOURCE",
    "multi_outlet": "MULTI_OUTLET",
    "multi_source": "MULTI_SOURCE",
    "price_invalid": "PRICE_INVALID",
    "sold_out": "SOLD_OUT",
    "quarantine": "QUARANTINED",
    "unexpected_status": "SKIPPED",
}


# =============================================================================
# Plan data model (preview output)
# =============================================================================


@dataclass(frozen=True)
class SizeTarget:
    """One (normalized-size) return bucket for a product."""

    raw_size: str
    norm_size: str
    row_uuids: Tuple[str, ...]
    delta: int
    frozen_pre: Optional[int]  # Promo `available` captured at preview (CREATE -> 0)
    matched: bool
    variant_id: Optional[str]
    inventory_item_id: Optional[str]


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
    reconciled_uuids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ApplyReport:
    outcomes: Tuple[ApplyOutcome, ...] = field(default_factory=tuple)


# =============================================================================
# Pure helpers
# =============================================================================


def _plan_hash(anchor: Optional[str], branch: Optional[str], live_status: Optional[str]) -> str:
    """Bind {gid target/source, branch, live status} — the TOCTOU verify key."""
    raw = f"{anchor or ''}|{branch or ''}|{(live_status or '').upper()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _norm_size(value: Any) -> str:
    """Normalize a size token (FIX5): case/whitespace/comma + numeric format.

    ``"42.0" -> "42"``, ``"42,5" -> "42.5"``, ``"M" -> "m"``. Non-numeric tokens
    fall through as their casefolded, whitespace-collapsed form.
    """
    s = str(value or "").strip().casefold().replace(",", ".")
    s = " ".join(s.split())
    try:
        f = float(s)
    except (TypeError, ValueError):
        return s
    return str(int(f)) if f == int(f) else str(f)


def _extract_norm_size(selected_options: List[Dict[str, Any]]) -> Optional[str]:
    for opt in selected_options or []:
        if (opt.get("name") or "").strip().casefold() in _SIZE_OPTION_NAMES:
            return _norm_size(opt.get("value"))
    return None


def _variant_size_index(variants: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """norm_size -> variant dict (first occurrence wins)."""
    idx: Dict[str, Dict[str, Any]] = {}
    for v in variants:
        ns = _extract_norm_size(v.get("selectedOptions") or [])
        if ns is not None and ns not in idx:
            idx[ns] = v
    return idx


def _match_variant(
    norm_size: str, size_index: Dict[str, Dict[str, Any]], variants: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """FIX5: match by normalized size (primary). Empty size on a mono-variant
    product -> that single variant (legacy mono-variant path)."""
    if norm_size in size_index:
        return size_index[norm_size]
    if norm_size == "" and len(variants) == 1:
        return variants[0]
    return None


def _item_id(variant: Optional[Dict[str, Any]]) -> Optional[str]:
    if not variant:
        return None
    if variant.get("inventoryItemId"):
        return variant["inventoryItemId"]
    return (variant.get("inventoryItem") or {}).get("id")


def _promo_available(variant: Optional[Dict[str, Any]], promo_id: str) -> Optional[int]:
    """Live Promo ``available`` for a variant, or ``None`` if the Promo level is
    ABSENT (never stocked). ``read_variant_inventory`` never fabricates a 0."""
    if not variant:
        return None
    for lvl in variant.get("levels") or []:
        if lvl.get("location_id") == promo_id:
            a = lvl.get("available")
            return a if a is not None else 0
    return None


def _has_non_promo_stock(variants: List[Dict[str, Any]], promo_id: str) -> bool:
    """True if any variant carries stock at a location != Promo (or the level set
    is truncated == UNKNOWN). Drives the restart-reconciliation quarantine."""
    for v in variants:
        if v.get("levels_truncated"):
            return True
        for lvl in v.get("levels") or []:
            if lvl.get("location_id") != promo_id:
                if (lvl.get("available") or 0) > 0 or (lvl.get("on_hand") or 0) > 0:
                    return True
    return False


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


# =============================================================================
# Planning (used identically by preview and by apply's TOCTOU re-resolution)
# =============================================================================


def _build_size_targets(
    return_rows: List[Any],
    size_index: Dict[str, Dict[str, Any]],
    variants: List[Dict[str, Any]],
    promo_id: str,
    *,
    is_create: bool,
) -> Tuple[List[SizeTarget], List[str], List[str]]:
    """Aggregate NON-reconciled return rows into per-size deltas + match variants.

    Returns ``(size_targets, unmatched_raw_sizes, sku_warnings)``. Rows sharing a
    normalized size are summed (no double-count); a size with no variant match is
    surfaced (never silently dropped).
    """
    groups: Dict[str, Dict[str, Any]] = {}
    for r in return_rows:
        ns = _norm_size(r.size)
        g = groups.setdefault(ns, {"raw": r.size, "uuids": [], "delta": 0})
        g["uuids"].append(r.row_uuid)
        g["delta"] += r.qta

    targets: List[SizeTarget] = []
    unmatched: List[str] = []
    sku_warnings: List[str] = []
    for ns, g in groups.items():
        v = _match_variant(ns, size_index, variants)
        matched = v is not None
        if not matched:
            unmatched.append(g["raw"])
        # FIX5: SKU is a secondary cross-check only (where the variant carries one).
        if v is not None and v.get("sku") and str(v["sku"]).strip() != "":
            sk = str(v["sku"]).strip()
            if getattr(return_rows[0], "sku", "") and sk != return_rows[0].sku:
                sku_warnings.append(f"sku_mismatch_on_size:{g['raw']}:{sk}")
        frozen_pre = 0 if is_create else _promo_available(v, promo_id)
        if frozen_pre is None:
            frozen_pre = 0
        targets.append(
            SizeTarget(
                raw_size=g["raw"],
                norm_size=ns,
                row_uuids=tuple(g["uuids"]),
                delta=int(g["delta"]),
                frozen_pre=frozen_pre,
                matched=matched,
                variant_id=(v.get("id") if v else None),
                inventory_item_id=_item_id(v),
            )
        )
    return targets, unmatched, sku_warnings


def _nonpub(
    sku: str,
    branch: Optional[str],
    reason: str,
    *,
    warnings: List[str],
    target_gid: Optional[str] = None,
    source_gid: Optional[str] = None,
    outlet_title: Optional[str] = None,
    live_status: Optional[str] = None,
    price: Optional[str] = None,
    compare_at: Optional[str] = None,
    plan_hash: str,
) -> PlanAction:
    return PlanAction(
        sku=sku,
        branch=branch,
        publishable=False,
        reason=reason,
        target_gid=target_gid,
        source_gid=source_gid,
        outlet_title=outlet_title,
        live_status=live_status,
        price=price,
        compare_at=compare_at,
        price_ok=False,
        size_targets=(),
        unmatched_sizes=(),
        warnings=tuple(warnings),
        plan_hash=plan_hash,
    )


def _plan_action_for_sku(
    sku: str, group_rows: List[Any], transport: Any, promo_id: str
) -> PlanAction:
    warnings: List[str] = []
    first = group_rows[0]
    return_rows = [r for r in group_rows if (not r.reconciled) and r.qta > 0]
    price_ok, price, compare_at, price_reason = _validate_price(
        first.prezzo_outlet, first.prezzo_high
    )
    q_gid = next((r.product_id for r in group_rows if r.product_id), "")

    outlet_res = resolvers.outlet_resolver(transport, sku)
    if outlet_res.get("warning"):
        warnings.append(outlet_res["warning"])
    outlet_matches = outlet_res.get("matches") or []

    # MATCH step 1: Q gid that resolves live to an outlet; else the sole outlet.
    chosen: Optional[Dict[str, Any]] = None
    if q_gid:
        chosen = next((m for m in outlet_matches if m["product_gid"] == q_gid), None)
    if chosen is None and len(outlet_matches) == 1:
        chosen = outlet_matches[0]
    if chosen is None and len(outlet_matches) > 1:
        return _nonpub(
            sku, None, "multi_outlet", warnings=warnings, price=price, compare_at=compare_at,
            plan_hash=_plan_hash(sku, "multi_outlet", ""),
        )

    if chosen is not None:
        gid = chosen["product_gid"]
        status = (chosen.get("status") or "").upper()
        ph = _plan_hash(gid, status if status in (BRANCH_ACTIVE, BRANCH_DRAFT) else "OTHER", status)
        if status == BRANCH_ACTIVE:
            return _active_action(
                sku, gid, status, transport, promo_id, return_rows, price, compare_at,
                price_ok, price_reason, warnings, ph,
            )
        if status == BRANCH_DRAFT:
            return _draft_action(
                sku, gid, status, transport, promo_id, return_rows, price, compare_at,
                price_ok, price_reason, warnings, ph,
            )
        return _nonpub(
            sku, None, "unexpected_status", warnings=warnings + [f"unexpected_status:{status}"],
            target_gid=gid, live_status=status, price=price, compare_at=compare_at, plan_hash=ph,
        )

    # MATCH step 3: no outlet -> source -> CREATE (or surface no-source).
    source_res = resolvers.source_resolver(transport, sku)
    if source_res.get("warning"):
        warnings.append(source_res["warning"])
    src_matches = source_res.get("matches") or []
    if not src_matches:
        return _nonpub(
            sku, None, "no_source", warnings=warnings, price=price, compare_at=compare_at,
            plan_hash=_plan_hash(sku, "no_source", ""),
        )
    if len(src_matches) > 1:
        return _nonpub(
            sku, None, "multi_source", warnings=warnings, price=price, compare_at=compare_at,
            plan_hash=_plan_hash(sku, "multi_source", ""),
        )
    return _create_action(
        sku, src_matches[0], transport, promo_id, return_rows, price, compare_at,
        price_ok, price_reason, warnings,
    )


def _active_action(
    sku, gid, status, transport, promo_id, return_rows, price, compare_at,
    price_ok, price_reason, warnings, plan_hash,
) -> PlanAction:
    if not price_ok:
        return _nonpub(
            sku, BRANCH_ACTIVE, "price_invalid", warnings=warnings + [f"price:{price_reason}"],
            target_gid=gid, live_status=status, price=price, compare_at=compare_at, plan_hash=plan_hash,
        )
    inv = ops.read_variant_inventory(transport, gid)
    if _has_non_promo_stock(inv, promo_id):
        return _nonpub(
            sku, BRANCH_ACTIVE, "quarantine",
            warnings=warnings + ["quarantine:non_promo_stock_inherited"],
            target_gid=gid, live_status=status, price=price, compare_at=compare_at, plan_hash=plan_hash,
        )
    idx = _variant_size_index(inv)
    targets, unmatched, sku_warns = _build_size_targets(return_rows, idx, inv, promo_id, is_create=False)
    return PlanAction(
        sku=sku, branch=BRANCH_ACTIVE, publishable=True, reason=None, target_gid=gid,
        source_gid=None, outlet_title=None, live_status=status, price=price, compare_at=compare_at,
        price_ok=True, size_targets=tuple(targets), unmatched_sizes=tuple(unmatched),
        warnings=tuple(warnings + sku_warns), plan_hash=plan_hash,
    )


def _draft_action(
    sku, gid, status, transport, promo_id, return_rows, price, compare_at,
    price_ok, price_reason, warnings, plan_hash,
) -> PlanAction:
    # Make drafted it == sold-out signal: no fresh return -> skip + surface.
    if not return_rows:
        return _nonpub(
            sku, BRANCH_DRAFT, "sold_out", warnings=warnings + ["sold_out:drafted_no_fresh_return"],
            target_gid=gid, live_status=status, price=price, compare_at=compare_at, plan_hash=plan_hash,
        )
    if not price_ok:
        return _nonpub(
            sku, BRANCH_DRAFT, "price_invalid", warnings=warnings + [f"price:{price_reason}"],
            target_gid=gid, live_status=status, price=price, compare_at=compare_at, plan_hash=plan_hash,
        )
    inv = ops.read_variant_inventory(transport, gid)
    if _has_non_promo_stock(inv, promo_id):
        return _nonpub(
            sku, BRANCH_DRAFT, "quarantine",
            warnings=warnings + ["quarantine:non_promo_stock_inherited"],
            target_gid=gid, live_status=status, price=price, compare_at=compare_at, plan_hash=plan_hash,
        )
    idx = _variant_size_index(inv)
    targets, unmatched, sku_warns = _build_size_targets(return_rows, idx, inv, promo_id, is_create=False)
    return PlanAction(
        sku=sku, branch=BRANCH_DRAFT, publishable=True, reason=None, target_gid=gid,
        source_gid=None, outlet_title=None, live_status=status, price=price, compare_at=compare_at,
        price_ok=True, size_targets=tuple(targets), unmatched_sizes=tuple(unmatched),
        warnings=tuple(warnings + ["draft_revive:fresh_return"] + sku_warns), plan_hash=plan_hash,
    )


def _create_action(
    sku, source, transport, promo_id, return_rows, price, compare_at,
    price_ok, price_reason, warnings,
) -> PlanAction:
    source_gid = source["product_gid"]
    outlet_title = _outlet_title(source.get("title") or "")
    plan_hash = _plan_hash(source_gid, BRANCH_CREATE, "")
    if not price_ok:
        return _nonpub(
            sku, BRANCH_CREATE, "price_invalid", warnings=warnings + [f"price:{price_reason}"],
            source_gid=source_gid, outlet_title=outlet_title, price=price, compare_at=compare_at,
            plan_hash=plan_hash,
        )
    src_variants = ops.get_product_variants(transport, source_gid)
    idx = _variant_size_index(src_variants)
    targets, unmatched, sku_warns = _build_size_targets(
        return_rows, idx, src_variants, promo_id, is_create=True
    )
    return PlanAction(
        sku=sku, branch=BRANCH_CREATE, publishable=True, reason=None, target_gid=None,
        source_gid=source_gid, outlet_title=outlet_title, live_status=None, price=price,
        compare_at=compare_at, price_ok=True, size_targets=tuple(targets),
        unmatched_sizes=tuple(unmatched), warnings=tuple(warnings + sku_warns), plan_hash=plan_hash,
    )


def _build_actions(rows: List[Any], transport: Any, promo_id: str) -> List[PlanAction]:
    """Group canonical rows by SKU; plan every group carrying >=1 non-reconciled
    row (the return signal). Fully-reconciled groups produce no action."""
    groups: Dict[str, List[Any]] = {}
    order: List[str] = []
    for r in rows:
        sku = (r.sku or "").strip()
        if not sku:
            continue
        if sku not in groups:
            groups[sku] = []
            order.append(sku)
        groups[sku].append(r)
    actions: List[PlanAction] = []
    for sku in order:
        grp = groups[sku]
        if not any(not r.reconciled for r in grp):
            continue
        actions.append(_plan_action_for_sku(sku, grp, transport, promo_id))
    return actions


# =============================================================================
# Preview (READ-ONLY, DRY_RUN-safe)
# =============================================================================


def publish_preview(sheet: Any, transport: Any, *, promo_location_id: str) -> Plan:
    """Compute the publish PLAN without mutating Shopify or the Sheet.

    Reads canonically with ``assign_uuids=False`` (fail-closed DRY_RUN: no
    row_uuid is minted, no cell is written). Returns a :class:`Plan`.
    """
    read = sheet.read_canonical(assign_uuids=False)
    actions = _build_actions(read.rows, transport, promo_location_id)
    anomalies = tuple(f"{a.kind}:{a.sku}:{a.row_uuid}" for a in getattr(read, "anomalies", ()))
    return Plan(dry_run=True, actions=tuple(actions), anomalies=anomalies)


# =============================================================================
# Apply (MUTATES — confirm-gated upstream)
# =============================================================================


def _mark(sheet: Any, row_uuid: str, sku: str, reconciled: List[str], warns: List[str]) -> None:
    res = sheet.mark_reconciled(row_uuid, expected_sku=sku)
    if getattr(res, "ok", False):
        reconciled.append(row_uuid)
    else:
        warns.append(f"mark_failed:{getattr(res, 'reason', None)}")


def _execute_create(fresh: PlanAction, sheet, transport, promo_id: str, get_pub) -> ApplyOutcome:
    warns: List[str] = list(fresh.warnings)
    reconciled: List[str] = []

    # (1) duplicate — STAYS DRAFT for the whole finalization.
    new_gid = ops.product_duplicate(transport, fresh.source_gid, fresh.outlet_title)
    inv = ops.read_variant_inventory(transport, new_gid)
    if any(v.get("levels_truncated") for v in inv):
        # UNKNOWN level set -> cannot guarantee non-Promo cleanup -> quarantine.
        warns.append("quarantine:levels_truncated_after_duplicate")
        return ApplyOutcome(fresh.sku, BRANCH_CREATE, "QUARANTINED", new_gid, tuple(warns), ())

    # (2) FIX1: zero + disconnect EVERY non-Promo location the duplicate inherited.
    for v in inv:
        item = _item_id(v)
        for lvl in v.get("levels") or []:
            loc = lvl.get("location_id")
            if loc and loc != promo_id:
                ops.inventory_set_quantities(transport, item, loc, 0)
                ops.inventory_deactivate(transport, item, loc)

    # (3) zero ALL variants at Promo (kills inherited stock on non-returned sizes).
    for v in inv:
        ops.inventory_set_quantities(transport, _item_id(v), promo_id, 0)

    # (4) set the return delta ONLY on returned sizes (pre == 0 -> target == delta).
    idx = _variant_size_index(inv)
    for st in fresh.size_targets:
        if st.delta <= 0:
            continue
        v = _match_variant(st.norm_size, idx, inv)
        if v is None:
            warns.append(f"unmatched_size:{st.raw_size}")
            continue
        ops.inventory_set_quantities(transport, _item_id(v), promo_id, st.delta)
        for u in st.row_uuids:
            _mark(sheet, u, fresh.sku, reconciled, warns)

    # (5) prices + inventoryPolicy=DENY in one bulk update (gated Q-oversell default).
    variants_input = [
        {"id": v["id"], "price": fresh.price, "compareAtPrice": fresh.compare_at, "inventoryPolicy": "DENY"}
        for v in inv
    ]
    ops.product_variants_bulk_update(transport, new_gid, variants_input)

    # (6) LAST: flip ACTIVE, then publish to the Online Store channel.
    ops.product_update_status(transport, new_gid, "ACTIVE")
    ops.product_publish(transport, new_gid, get_pub())
    return ApplyOutcome(fresh.sku, BRANCH_CREATE, "APPLIED", new_gid, tuple(warns), tuple(reconciled))


def _apply_delta(
    st: SizeTarget, variant, sheet, transport, promo_id, sku, approved_pre: Optional[int],
    reconciled: List[str], warns: List[str],
) -> None:
    """DELTA exactly-once against ``approved_pre`` (frozen at preview)."""
    item = _item_id(variant)
    live = _promo_available(variant, promo_id)
    live_v = live if live is not None else 0
    base_v = approved_pre if approved_pre is not None else live_v
    target = base_v + st.delta
    if live_v == base_v:
        ops.inventory_set_quantities(transport, item, promo_id, target)
        for u in st.row_uuids:
            _mark(sheet, u, sku, reconciled, warns)
    elif live_v >= target:
        # already applied (Make / a crashed prior run) -> close idempotently, NO re-add.
        warns.append(f"delta_already_applied:{st.raw_size}")
        for u in st.row_uuids:
            _mark(sheet, u, sku, reconciled, warns)
    else:
        # compare-mismatch, value below target -> do NOT mark; needs re-preview.
        warns.append(f"delta_compare_mismatch:{st.raw_size}")


def _execute_active(fresh: PlanAction, approved: PlanAction, sheet, transport, promo_id: str) -> ApplyOutcome:
    warns: List[str] = list(fresh.warnings)
    reconciled: List[str] = []
    gid = fresh.target_gid
    inv = ops.read_variant_inventory(transport, gid)
    if _has_non_promo_stock(inv, promo_id):
        return ApplyOutcome(fresh.sku, BRANCH_ACTIVE, "QUARANTINED", gid, tuple(warns + ["quarantine:non_promo_stock"]), ())

    # refresh price/compareAt on every variant (never re-inflate stock here).
    variants_input = [{"id": v["id"], "price": fresh.price, "compareAtPrice": fresh.compare_at} for v in inv]
    ops.product_variants_bulk_update(transport, gid, variants_input)

    approved_pre = {st.norm_size: st.frozen_pre for st in approved.size_targets}
    idx = _variant_size_index(inv)
    for st in fresh.size_targets:
        if st.delta <= 0:
            continue
        v = _match_variant(st.norm_size, idx, inv)
        if v is None:
            warns.append(f"unmatched_size:{st.raw_size}")
            continue
        _apply_delta(st, v, sheet, transport, promo_id, fresh.sku, approved_pre.get(st.norm_size), reconciled, warns)
    return ApplyOutcome(fresh.sku, BRANCH_ACTIVE, "APPLIED", gid, tuple(warns), tuple(reconciled))


def _execute_draft(fresh: PlanAction, sheet, transport, promo_id: str, get_pub) -> ApplyOutcome:
    """DRAFT with a fresh return -> revive: apply delta, normalize, activate+publish."""
    warns: List[str] = list(fresh.warnings)
    reconciled: List[str] = []
    gid = fresh.target_gid
    inv = ops.read_variant_inventory(transport, gid)
    if _has_non_promo_stock(inv, promo_id):
        return ApplyOutcome(
            fresh.sku, BRANCH_DRAFT, "QUARANTINED", gid,
            tuple(warns + ["quarantine:non_promo_stock"]), (),
        )
    idx = _variant_size_index(inv)
    for st in fresh.size_targets:
        if st.delta <= 0:
            continue
        v = _match_variant(st.norm_size, idx, inv)
        if v is None:
            warns.append(f"unmatched_size:{st.raw_size}")
            continue
        live = _promo_available(v, promo_id) or 0
        ops.inventory_set_quantities(transport, _item_id(v), promo_id, live + st.delta)
        for u in st.row_uuids:
            _mark(sheet, u, fresh.sku, reconciled, warns)
    variants_input = [
        {"id": v["id"], "price": fresh.price, "compareAtPrice": fresh.compare_at, "inventoryPolicy": "DENY"}
        for v in inv
    ]
    ops.product_variants_bulk_update(transport, gid, variants_input)
    ops.product_update_status(transport, gid, "ACTIVE")
    ops.product_publish(transport, gid, get_pub())
    return ApplyOutcome(fresh.sku, BRANCH_DRAFT, "REVIVED", gid, tuple(warns), tuple(reconciled))


def _execute_action(fresh: PlanAction, approved: PlanAction, sheet, transport, promo_id, get_pub) -> ApplyOutcome:
    if not fresh.publishable:
        return ApplyOutcome(
            fresh.sku, fresh.branch, _reason_status(fresh.reason), fresh.target_gid,
            ((fresh.reason,) if fresh.reason else ()) + fresh.warnings, (),
        )
    if fresh.branch == BRANCH_CREATE:
        return _execute_create(fresh, sheet, transport, promo_id, get_pub)
    if fresh.branch == BRANCH_ACTIVE:
        return _execute_active(fresh, approved, sheet, transport, promo_id)
    if fresh.branch == BRANCH_DRAFT:
        return _execute_draft(fresh, sheet, transport, promo_id, get_pub)
    return ApplyOutcome(fresh.sku, fresh.branch, "SKIPPED", fresh.target_gid, ("unknown_branch",), ())


def publish_apply(
    sheet: Any,
    transport: Any,
    approved_plan: Plan,
    *,
    promo_location_id: str,
    publication_id: Optional[str] = None,
) -> ApplyReport:
    """Execute the approved plan with a live TOCTOU re-resolution.

    Re-reads the sheet (``assign_uuids=True``), re-resolves every SKU, and for
    each action whose freshly-computed ``plan_hash`` still equals the approved
    plan's, executes the branch. A drifted branch/status -> ``VERIFY_FAILED``
    (the caller re-previews).

    ``publication_id``: if not passed explicitly, and the fresh batch contains
    at least one publishable CREATE/DRAFT action (the only branches that call
    ``product_publish``), the "Online Store" publication is resolved EAGER/
    fail-fast HERE, before any action executes (post-review fix: resolving it
    lazily, inside CREATE/DRAFT, used to happen AFTER the ACTIVE flip and
    ``mark_reconciled``, so a resolve failure left an ACTIVE-but-unpublished
    product with rows already marked reconciled — no future run would retry the
    publish). A resolution failure here aborts the WHOLE batch cleanly (every
    action reported ``ERROR``, nothing mutated).

    Per-action isolation: any exception from an action's execution (a Shopify
    userError, a transport error, a Sheet error, or any other unexpected
    exception e.g. a ``RuntimeError`` from a read op) is caught and translated
    to a bounded ``ERROR`` outcome for THAT sku only — the loop always continues
    to the next action, and an ``ApplyReport`` is always returned.
    """
    read = sheet.read_canonical(assign_uuids=True)
    fresh_actions = _build_actions(read.rows, transport, promo_location_id)
    approved_by_sku = {a.sku: a for a in approved_plan.actions}

    resolved_publication_id = publication_id
    needs_publish = any(
        a.publishable and a.branch in (BRANCH_CREATE, BRANCH_DRAFT) for a in fresh_actions
    )
    if resolved_publication_id is None and needs_publish:
        try:
            resolved_publication_id = ops.get_online_store_publication_id(transport)
        except Exception as e:
            return ApplyReport(tuple(
                ApplyOutcome(
                    a.sku, a.branch, "ERROR", a.target_gid,
                    (f"publication_resolve_failed:{type(e).__name__}",), (),
                )
                for a in fresh_actions
            ))

    def get_pub() -> str:
        return resolved_publication_id

    outcomes: List[ApplyOutcome] = []
    for fresh in fresh_actions:
        approved = approved_by_sku.get(fresh.sku)
        if approved is None:
            outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "NOT_IN_PLAN", fresh.target_gid, ("not_in_approved_plan",), ()))
            continue
        if fresh.plan_hash != approved.plan_hash:
            outcomes.append(ApplyOutcome(
                fresh.sku, fresh.branch, "VERIFY_FAILED", fresh.target_gid,
                ("plan_hash_mismatch:live_state_changed", "re_preview_required"), (),
            ))
            continue
        try:
            outcomes.append(_execute_action(fresh, approved, sheet, transport, promo_location_id, get_pub))
        except ShopifyUserError as e:
            outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "ERROR", fresh.target_gid, (f"shopify_user_error:{e.mutation}",), ()))
        except ShopifyTransportError:
            outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "ERROR", fresh.target_gid, ("shopify_transport_error",), ()))
        except GSheetError:
            outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "ERROR", fresh.target_gid, ("sheet_error",), ()))
        except Exception as e:
            # fix3: any OTHER unexpected exception (e.g. RuntimeError from
            # ops.read_variant_inventory / get_online_store_publication_id) must
            # not abort the whole batch — isolate it to this sku's outcome.
            outcomes.append(ApplyOutcome(fresh.sku, fresh.branch, "ERROR", fresh.target_gid, (f"unexpected_error:{type(e).__name__}",), ()))
    return ApplyReport(tuple(outcomes))


# =============================================================================
# Production entry point (wires live config/transport/sheet) — used by main.py
# =============================================================================


def _log_plan(plan: Plan) -> None:
    by_branch: Dict[str, int] = {}
    for a in plan.actions:
        key = a.branch if a.publishable else f"skip:{a.reason}"
        by_branch[key or "skip"] = by_branch.get(key or "skip", 0) + 1
    logger.info("PLAN: %d azioni %s", len(plan.actions), by_branch)


def _log_report(report: ApplyReport) -> None:
    by_status: Dict[str, int] = {}
    for o in report.outcomes:
        by_status[o.status] = by_status.get(o.status, 0) + 1
    logger.info("APPLY: %d esiti %s", len(report.outcomes), by_status)


def run(dry_run: bool) -> None:
    """RUN_MODE=SYNC entry point. Preview always; apply only when NOT dry_run."""
    config = load_shopify_config()
    transport = ShopifyTransport(config)
    sheet = ScansiaSheet.open()
    promo = config.promo_location_id

    plan = publish_preview(sheet, transport, promo_location_id=promo)
    _log_plan(plan)
    if dry_run:
        logger.info("DRY-RUN: nessuna mutazione (Shopify/foglio) applicata.")
        return
    report = publish_apply(sheet, transport, plan, promo_location_id=promo)
    _log_report(report)
