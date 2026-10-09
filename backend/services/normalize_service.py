"""Deny-normalize — plan ``docs/f7-mysql-adapter-plan.md`` §3.5: the app's
``/outlet/deny-normalize``, ported as §3.3 rule 7's normalisation, with its own
preview → confirm (``CONFERMO``).

Rule 7: DENY + tracked is a precondition for every variant of an outlet F7 exposes
or keeps ACTIVE, customisation-only variants included (one with a real SKU and
CONTINUE on an ACTIVE outlet sells at any level, unrecorded); a customisation-only
variant's positive Promo is lowered to 0, relatively, a negative left. Here it runs
on the outlets a person names — or, none named, on every member of the outlet
collection — whatever their status: a DRAFT outlet with a CONTINUE variant can
never pass the delete predicate (the bulk cleanup's CONTINUE coupling), which is
what the sheet-era helper was for.

Per product, one group: a ``normalise`` ``RECONCILE`` row (no prices: rule 7's own
normalisation carries none — the product id and its real-size variants' keys on
it, §3.6) when a variant is not DENY + tracked, and one ``custom`` lowering row per
customisation-only variant with Promo > 0. A refused normalisation closes its row
``FAILED`` and drafts the product if it is ACTIVE and still carries an unsafe
variant (terminal path (i)); an unknown answer leaves the row ``OPEN`` for the
next publish-class holder (a decrement-class holder never re-runs it).

It runs under the pass lease as a decrement-class holder
(:class:`~backend.services.passes.Holder`), yielding between products. The
approved plan bounds the products; each is re-read right before its writes and
normalised as it is then (normalising only ever moves a product toward rule 7).
"""
from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from backend.persistence.records import APPLIED, FAILED, OPEN, Key, LeaseLost, StoreError
from backend.services import delete_service as D
from backend.services import flows, passes
from backend.services import ledger as L
from backend.services import variants as V
from backend.services.lease import HOLDER_NORMALIZE, LeaseUnavailable, PassLease
from backend.shopify import ops
from backend.shopify.ops import ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.normalize_service")

ST_APPLIED = "APPLIED"
ST_LEASE_UNAVAILABLE = "LEASE_UNAVAILABLE"
ST_INCOMPLETE = "INCOMPLETE"
O_NORMALISED = "NORMALISED"
O_ALREADY = "ALREADY_NORMAL"
O_FAILED = "NORMALISE_FAILED"        # refused (the product drafted if unsafe on ACTIVE) or not applied
O_UNKNOWN = "OUTCOME_UNKNOWN"        # left OPEN for a publish-class holder
O_GONE = "PRODUCT_GONE"
O_ERROR = "ERROR"

NORMALIZE_TAKE_BOUND_S = passes.HOLDER_TAKE_BOUND_S
_SHOPIFY_ERRORS = (ShopifyUserError, ShopifyTransportError, ProductNotFound)


@dataclass(frozen=True)
class NormalizeProduct:
    product_gid: str
    title: Optional[str]
    status: str
    keys: Tuple[Key, ...]
    unsafe_variant_ids: Tuple[str, ...]                 # CONTINUE or untracked
    custom_lowerings: Tuple[Tuple[str, str, int], ...]  # (variant id, inventory item, available > 0)


@dataclass(frozen=True)
class NormalizePlan:
    dry_run: bool
    products: Tuple[NormalizeProduct, ...]
    scanned: int
    review: Tuple[Tuple[str, str], ...]                 # (product gid, why it was not planned)
    plan_hash: str


@dataclass(frozen=True)
class NormalizeOutcome:
    product_gid: str
    status: str
    warnings: Tuple[str, ...] = ()
    rows: Tuple[int, ...] = ()


@dataclass(frozen=True)
class NormalizeReport:
    status: str
    outcomes: Tuple[NormalizeOutcome, ...] = ()
    reason: Optional[str] = None
    surfaced: Tuple[Dict[str, Any], ...] = ()


def _unsafe(v: Dict[str, Any]) -> bool:
    return (v.get("inventoryPolicy") or "").upper() != "DENY" or v.get("tracked") is False


def plan_product(gid: str, title: Optional[str], status: str, inv: Sequence[Dict[str, Any]],
                 promo: str) -> Optional[NormalizeProduct]:
    """What rule 7 asks of one product now (``None``: nothing)."""
    unsafe = tuple(sorted(v.get("id") for v in inv if _unsafe(v) and v.get("id")))
    custom = []
    for v in inv:
        if V.is_customisation_only(v):
            a = V.promo_available(v, promo)
            if a is not None and a > 0:
                custom.append((v.get("id") or "", V.item_id(v) or "", int(a)))
    if not unsafe and not custom:
        return None
    keys = tuple(sorted({((v.get("sku") or "").strip(), V.variant_title(v)) for v in V.real_size_variants(inv)
                         if (v.get("sku") or "").strip()}))
    return NormalizeProduct(gid, title, (status or "").upper(), keys, unsafe, tuple(sorted(custom)))


def _product_hash(p: NormalizeProduct) -> str:
    return f"{p.product_gid}#{','.join(p.unsafe_variant_ids)}#{','.join(c[0] for c in p.custom_lowerings)}"


def normalize_preview(register: Any, transport: Any, *, promo_location_id: str,
                      product_gids: Sequence[str] = ()) -> NormalizePlan:
    """READ-ONLY. The named outlets (each must resolve to an outlet) or, none named,
    every outlet-collection member: those with a variant not DENY + tracked or a
    customisation-only variant with Promo > 0."""
    D.require_promo_anchor(promo_location_id)
    review: List[Tuple[str, str]] = []
    targets: List[Tuple[str, Optional[str], str]] = []
    if product_gids:
        for gid in dict.fromkeys(g.strip() for g in product_gids if g and g.strip()):
            try:
                if not D.resolve_is_outlet(transport, gid):
                    review.append((gid, "not_an_outlet"))
                    continue
                core = ops.get_product_core(transport, gid)
            except ProductNotFound:
                review.append((gid, "product_gone"))
                continue
            targets.append((gid, core.get("title"), core.get("status") or ""))
    else:
        targets = [(m["id"], m.get("title"), m.get("status") or "") for m in ops.enumerate_outlet_products(transport)]
    products: List[NormalizeProduct] = []
    for gid, title, status in targets:
        try:
            inv = ops.read_variant_inventory(transport, gid)
        except _SHOPIFY_ERRORS + (RuntimeError,) as e:
            review.append((gid, f"read_error:{type(e).__name__}"))
            continue
        if any(v.get("levels_truncated") for v in inv):
            review.append((gid, "levels_truncated_unknown"))
            continue
        p = plan_product(gid, title, status, inv, promo_location_id)
        if p is not None:
            products.append(p)
    raw = "|".join(sorted(_product_hash(p) for p in products))
    return NormalizePlan(True, tuple(products), len(targets), tuple(review),
                         hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16])


def _normalise_one(ctx: "flows.Context", approved: NormalizeProduct) -> NormalizeOutcome:
    gid = approved.product_gid
    try:
        core = ops.get_product_core(ctx.transport, gid)
        inv = ctx.inventory(gid)
    except ProductNotFound:
        return NormalizeOutcome(gid, O_GONE, ("product_gone",))
    p = plan_product(gid, core.get("title"), core.get("status") or "", inv, ctx.promo)
    if p is None:
        return NormalizeOutcome(gid, O_ALREADY)
    num = flows.product_number(gid)
    specs: List[L.RowSpec] = []
    if p.unsafe_variant_ids:
        specs.append(L.RowSpec(action=L.A_NORMALISE, keys=p.keys, product_id=num))
    for vid, item, _a in p.custom_lowerings:
        specs.append(L.RowSpec(action=L.A_ADJUST, purpose=L.P_CUSTOM, target=0, inventory_item_id=item,
                               location_id=ctx.promo, variant_id=vid, product_id=num))
    rows = ctx.ledger.write_group(specs)
    warnings: List[str] = []
    status = O_NORMALISED
    for r in rows:
        if r.action == L.A_NORMALISE:
            st = flows.run_normalise(ctx, r)          # a refusal drafts an unsafe ACTIVE product (i)
            if st == OPEN:
                status, warnings = O_UNKNOWN, warnings + ["normalise_outcome_unknown"]
            elif st != APPLIED:
                status, warnings = O_FAILED, warnings + ["normalise_not_applied"]
        else:
            st = ctx.ledger.send_adjust(r)
            if st != APPLIED and not (st == FAILED and (ctx.ledger.reload(r).outcome or {}).get("reason")
                                      == "nothing_to_lower"):
                warnings.append(f"custom_lowering_not_applied:{r.row_id}")
                if status == O_NORMALISED:
                    status = O_FAILED
    ctx.ledger.settle(rows[0].group_id)
    return NormalizeOutcome(gid, status, tuple(warnings), tuple(r.row_id for r in rows))


def normalize_apply(
    register: Any,
    transport: Any,
    approved: NormalizePlan,
    *,
    promo_location_id: str,
    lease: Optional[PassLease] = None,
    take_bound_s: float = NORMALIZE_TAKE_BOUND_S,
    sleep: Callable[[float], None] = time.sleep,
) -> NormalizeReport:
    """Normalise the approved products under the pass lease, one at a time."""
    run = passes.Holder(register, transport, promo_location_id, HOLDER_NORMALIZE, lease=lease,
                        take_bound_s=take_bound_s, sleep=sleep)
    outcomes: List[NormalizeOutcome] = []
    try:
        run.start()
        for p in approved.products:
            run.next_item()
            try:
                outcomes.append(_normalise_one(run.ctx, p))
            except _SHOPIFY_ERRORS as e:
                outcomes.append(NormalizeOutcome(p.product_gid, O_ERROR, (f"shopify_error:{type(e).__name__}",)))
        return NormalizeReport(ST_APPLIED, tuple(outcomes), surfaced=tuple(run.report.items))
    except LeaseUnavailable:
        status = ST_INCOMPLETE if run.lease.takes else ST_LEASE_UNAVAILABLE
        return NormalizeReport(status, tuple(outcomes), "lease", tuple(run.report.items))
    except LeaseLost:
        return NormalizeReport(ST_INCOMPLETE, tuple(outcomes), "lease_lost", tuple(run.report.items))
    except StoreError as e:
        return NormalizeReport(ST_INCOMPLETE, tuple(outcomes), f"register_error:{type(e).__name__}",
                               tuple(run.report.items))
    finally:
        run.release()


__all__ = [
    "ST_APPLIED", "ST_LEASE_UNAVAILABLE", "ST_INCOMPLETE",
    "O_NORMALISED", "O_ALREADY", "O_FAILED", "O_UNKNOWN", "O_GONE", "O_ERROR",
    "NormalizeProduct", "NormalizePlan", "NormalizeOutcome", "NormalizeReport",
    "plan_product", "normalize_preview", "normalize_apply",
]
