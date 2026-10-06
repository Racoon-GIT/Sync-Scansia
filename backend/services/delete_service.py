"""IRREVERSIBLE outlet deletion on the register — plan ``docs/f7-mysql-adapter-plan.md``
§3.5 (the deletion order, the bulk cleanup) and §3.3 rule 2's terminal path (iii);
safety-critical, confirm-gated.

Four public entry points, preview/apply-split so DRY_RUN is fail-closed by
construction:

* :func:`zero_stock_candidates` — READ-ONLY, no lease. Every OUTLET member (any
  status) through the EXACT per-variant delete predicate: ``candidates``,
  ``review`` (never auto-deleted, with reasons) or in-stock.
* :func:`cleanup_preview` / :func:`cleanup_apply` — the bulk cleanup of
  DRAFT-at-zero outlets. **An outlet whose SKU has an ``IN_SCANSIA`` pair is never
  deleted**: it is listed apart (``protected``), at preview and again at apply,
  right before its delete — the predicate also matches the DRAFT outlets B-1's
  Level (b) zeroed for their revive, and deleting one would turn a revive into a
  re-level that recreates (§3.5, AA3). So the deletion order's steps (0) and (4)
  find no shelf pair here.
* :func:`single_delete_preview` / :func:`delete_single_apply` — one product (a
  creation error, a botched duplicate). The predicate is NOT applied (by design: a
  crashed CREATE may carry inherited stock — that is what is removed); step (0)
  is: the preview lists the product SKUs' ``IN_SCANSIA`` pairs and the confirm
  holds their keys (a ``HOLD`` row, reason "outlet deleted") until a per-key
  re-level recreates; step (4) writes ``ONLINE = 0`` on those pairs.
* :func:`delete_product` — the deletion order's steps (1)–(3), shared by both and
  by the multi-outlet exit (``exit_service``): (1) ``OUTLET_PRODUCT_ID`` cleared —
  safe even if the delete then fails, the SPs fall back to the title lookup (E5);
  (2) the durable ``DELETE`` snapshot, whose failure aborts (the gate); (3)
  ``productDelete`` — once it answers success, or the product reads gone, the
  product's ``RECONCILE`` rows not closed close by terminal path (iii) ("product
  deleted", the ``DELETE`` row's id in ``OUTCOME``): an ``APPLIED`` row
  ``CARRIED`` (its moved quantity netted), a sent −Δ row ``FAILED`` "possibly
  applied", every other row ``FAILED``; their groups are settled.

Every apply runs under the pass lease as a decrement-class holder
(:class:`~backend.services.passes.Holder`: its class's recovery and the held-key
lowering at the take; released and re-taken between products, so a waiting
decrement gets in — rule 6).

### The per-variant delete PREDICATE (exact — GET /outlet/zero-stock)

An outlet is a **candidate** iff, over ALL its variants:

1. every variant resolves to a Promo inventory level that is PRESENT with
   ``available == 0`` (a MISSING Promo level → the whole outlet is UNKNOWN →
   REVIEW, never summed as ``0``), AND
2. ``SUM(committed @ Promo) == 0``, AND
3. ``∀`` variant ``inventoryPolicy == "DENY"`` (NO ``CONTINUE`` — for-all).

Routed to **review** (never a candidate): ``available < 0`` (oversell) · stock
``!= 0`` at any location ``!= Promo`` · a Promo level absent on any variant · a
truncated level set (UNKNOWN) · any ``CONTINUE`` variant · zero-available-but-
committed > 0 · no variants. A CONTINUE outlet is unblocked by deny-normalize
(``normalize_service``) first.

### SAFETY (every apply path)

1. **Mandatory reconstructive snapshot** (title/handle/status/tags, variants'
   sku/size/price/compareAt/inventoryItem, image srcs, metafields, collections)
   handed to :meth:`DeleteAuditSink.write_durable`, which RAISES on any failure —
   the single abort gate: ``productDelete`` is never reached without it.
2. **Live re-verification** under the lease: the cleanup's fresh candidate set
   must hash as approved (else the whole batch aborts, ``verify_failed``), and
   each product is re-read right before its own delete; the single delete's plan
   (product + keys to hold) must hash as approved.
3. **Human gesture**: the exact candidate count or ``CONFERMO`` (cleanup, with a
   second confirm over the hard-capped threshold); ``CONFERMO`` + count 1 (single).

``productDelete`` has NO undo. The snapshot is a reconstruction AID, not a restore;
the primary recovery is a per-key re-level that recreates from the source product.
"""
from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, List, Optional, Protocol, Sequence, Tuple

from backend.persistence.records import (
    APPLIED,
    CARRIED,
    FAILED,
    OPEN,
    Key,
    LeaseLost,
    PairRow,
    StoreError,
)
from backend.services import flows, passes
from backend.services import ledger as L
from backend.services import variants as V
from backend.services.lease import HOLDER_CLEANUP, HOLDER_DELETE, LeaseUnavailable, PassLease
from backend.services.variants import (
    SIZE_OPTION_NAMES as _SIZE_OPTION_NAMES,
    has_non_promo_stock as _has_non_promo_stock,
    promo_available as _promo_available,
)
from backend.shopify import ops
from backend.shopify.ops import ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.delete_service")

_DENY = "DENY"
CONFIRM_WORD = "CONFERMO"
# Above this many bulk candidates, cleanup_apply demands a second confirmation.
DEFAULT_CLEANUP_THRESHOLD = 25
# Hard ceiling on the operator-supplied threshold: an operator-set threshold can
# never silently disable the second-confirm gate above this count (fail-closed).
CLEANUP_HARD_CAP = 50
# Cleanup targets the one-time DRAFT-at-zero backlog.
CLEANUP_STATUS_FILTER = "DRAFT"

# Buckets returned by classify_outlet.
BUCKET_CANDIDATE = "CANDIDATE"
BUCKET_REVIEW = "REVIEW"
BUCKET_IN_STOCK = "IN_STOCK"

# Per-outcome status codes.
STATUS_DELETED = "DELETED"
STATUS_ARCHIVED = "ARCHIVED"                    # archive_first staging (hold, not deleted)
STATUS_SNAPSHOT_ABORTED = "SNAPSHOT_ABORTED"    # write_durable raised -> NO delete (the gate)
STATUS_SNAPSHOT_BUILD_FAILED = "SNAPSHOT_BUILD_FAILED"  # a read op for the snapshot failed
STATUS_DELETE_FAILED = "DELETE_FAILED"          # productDelete refused, or its outcome unknown
STATUS_SKIPPED = "SKIPPED"                      # re-read right before its delete: no longer deletable
STATUS_VERIFY_FAILED = "VERIFY_FAILED"          # the approved plan moved since the preview
STATUS_ERROR = "ERROR"

# Step (0)'s hold (§3.5): released by a per-key re-level that recreates.
REASON_OUTLET_DELETED = "outlet deleted"
# How long a delete / cleanup waits for the lease.
DELETE_TAKE_BOUND_S = passes.HOLDER_TAKE_BOUND_S

_CAUGHT = (ShopifyUserError, ShopifyTransportError, RuntimeError)


# =============================================================================
# Errors
# =============================================================================


class PromoAnchorError(RuntimeError):
    """The Promo anchor is missing — fail-closed refusal to enumerate."""


class DeleteConfirmationError(RuntimeError):
    """A required human gesture / second confirmation was missing or wrong."""


class SingleDeleteNotOutletError(RuntimeError):
    """HARDENING (post-review): the single-delete target does not resolve to an
    OUTLET (collection membership OR title match) — refuse, fail-closed. The
    single-delete escape hatch exists for a botched CREATE (which IS an outlet
    duplicate); a mistyped/wrong GID must never delete an arbitrary product."""


# =============================================================================
# Injected audit sink (``RegisterAuditSink`` on ``sm_outlet_audit``)
# =============================================================================


@dataclass(frozen=True)
class SnapshotVariant:
    variant_id: str
    sku: Optional[str]
    size: Optional[str]
    price: Optional[str]
    compare_at: Optional[str]
    inventory_item_id: Optional[str]


@dataclass(frozen=True)
class SnapshotCollection:
    id: str
    title: Optional[str]
    handle: Optional[str]
    smart: bool


@dataclass(frozen=True)
class BeforeSnapshot:
    """Reconstruction AID persisted durably BEFORE any productDelete (not a restore)."""

    product_gid: str
    title: Optional[str]
    handle: Optional[str]
    status: Optional[str]
    tags: Tuple[str, ...]
    variants: Tuple[SnapshotVariant, ...]
    image_srcs: Tuple[str, ...]
    metafields: Tuple[Dict[str, Any], ...]
    collections: Tuple[SnapshotCollection, ...]


@dataclass(frozen=True)
class DeleteOutcomeEvent:
    product_gid: str
    deleted_id: Optional[str]
    status: str


class DeleteAuditSink(Protocol):
    """Durable append-only audit boundary (dependency-injected).

    ``write_durable`` MUST persist the snapshot durably and RAISE on any failure —
    that raise is the abort gate; it returns the ``DELETE`` row's id (terminal path
    (iii) names it). ``write_outcome`` records the post-delete event and is
    best-effort (never blocks a completed delete).
    """

    def write_durable(self, snapshot: BeforeSnapshot) -> Optional[int]: ...

    def write_outcome(self, event: DeleteOutcomeEvent) -> None: ...



# =============================================================================
# Predicate / report data model
# =============================================================================


@dataclass(frozen=True)
class CandidateOutlet:
    product_gid: str
    title: Optional[str]
    status: str
    variants: Tuple[Dict[str, Any], ...]  # raw read_variant_inventory rows


@dataclass(frozen=True)
class ReviewOutlet:
    product_gid: str
    title: Optional[str]
    status: str
    reasons: Tuple[str, ...]


@dataclass(frozen=True)
class ZeroStockReport:
    scanned: int
    candidates: Tuple[CandidateOutlet, ...]
    review: Tuple[ReviewOutlet, ...]
    in_stock: int


@dataclass(frozen=True)
class CleanupCandidate:
    product_gid: str
    title: Optional[str]
    status: str


@dataclass(frozen=True)
class CleanupPlan:
    dry_run: bool
    candidates: Tuple[CleanupCandidate, ...]
    review: Tuple[ReviewOutlet, ...]
    count: int
    threshold: int
    archive_first: bool
    requires_second_confirm: bool
    plan_hash: str
    # DRAFT-at-zero outlets whose SKU has an IN_SCANSIA pair: listed apart, never deleted
    protected: Tuple[ReviewOutlet, ...] = ()


@dataclass(frozen=True)
class DeleteOutcome:
    product_gid: str
    status: str
    deleted_id: Optional[str] = None
    warnings: Tuple[str, ...] = ()
    closed_rows: Tuple[int, ...] = ()           # RECONCILE rows closed by terminal path (iii)
    held_keys: Tuple[Key, ...] = ()             # step (0)'s hold (the single delete)
    surfaced: Tuple[Dict[str, Any], ...] = ()


@dataclass(frozen=True)
class CleanupReport:
    dry_run: bool
    verify_failed: bool
    plan_hash: str
    deleted: int
    outcomes: Tuple[DeleteOutcome, ...] = field(default_factory=tuple)
    surfaced: Tuple[Dict[str, Any], ...] = ()


@dataclass(frozen=True)
class SingleDeletePlan:
    """The single delete's preview (step (0)): the product, the SKUs of its
    real-size variants, their ``IN_SCANSIA`` pairs and the keys the confirm holds,
    and the product's ``RECONCILE`` rows not closed (terminal path (iii) closes
    them once the product is gone)."""

    dry_run: bool
    product_gid: str
    title: Optional[str]
    status: Optional[str]
    skus: Tuple[str, ...]
    shelf_pairs: Tuple[int, ...]
    keys: Tuple[Key, ...]
    open_rows: Tuple[int, ...]
    plan_hash: str


# =============================================================================
# Predicate
# =============================================================================


def classify_outlet(
    variants: List[Dict[str, Any]], promo_id: str
) -> Tuple[str, Tuple[str, ...]]:
    """Bucket one outlet from its per-variant inventory. Returns (bucket, reasons).

    Order is safety-first: any UNKNOWN/unsafe signal short-circuits to REVIEW
    BEFORE the zero-assessment, so an outlet is never auto-candidated on partial
    knowledge.
    """
    if not variants:
        return BUCKET_REVIEW, ("no_variants",)

    # UNKNOWN: a truncated level set means ANY per-location conclusion is unsafe.
    if any(v.get("levels_truncated") for v in variants):
        return BUCKET_REVIEW, ("levels_truncated_unknown",)

    # Stock (or unknown) at a location != Promo -> the exact publish-quarantine
    # predicate (reused). With truncation already excluded above, this is pure
    # non-Promo stock.
    if _has_non_promo_stock(variants, promo_id):
        return BUCKET_REVIEW, ("non_promo_stock",)

    # HARDENING (IRREVERSIBLE-path only, not shared with outlet_service): a
    # PRESENT Promo level whose ``available`` is None/UNKNOWN must never be
    # collapsed to 0 here (unlike ``_promo_available``, used by publish
    # quarantine, which does collapse it) — that collapse could turn an outlet
    # into a delete candidate on partial knowledge. Route to REVIEW instead.
    for v in variants:
        for lvl in v.get("levels") or []:
            if lvl.get("location_id") == promo_id and lvl.get("available") is None:
                return BUCKET_REVIEW, ("promo_available_unknown",)

    promo_absent = False
    oversell = False
    total_committed = 0
    has_stock = False
    has_continue = False
    for v in variants:
        if (v.get("inventoryPolicy") or "").upper() != _DENY:
            has_continue = True
        avail = _promo_available(v, promo_id)  # None => Promo level ABSENT (UNKNOWN)
        if avail is None:
            promo_absent = True
            continue
        if avail < 0:
            oversell = True
        elif avail > 0:
            has_stock = True
        for lvl in v.get("levels") or []:
            if lvl.get("location_id") == promo_id:
                total_committed += lvl.get("committed") or 0
                break

    unsafe: List[str] = []
    if promo_absent:
        unsafe.append("promo_level_absent")
    if oversell:
        unsafe.append("promo_oversell")
    if unsafe:
        return BUCKET_REVIEW, tuple(unsafe)

    if has_stock:  # genuine Promo stock -> not a delete target (and not "review")
        return BUCKET_IN_STOCK, ()

    # Fully zero-available at Promo, level sets complete, no non-Promo stock.
    if total_committed > 0:
        return BUCKET_REVIEW, ("promo_committed",)
    if has_continue:  # CONTINUE-DENY coupling: needs deny_normalize first
        return BUCKET_REVIEW, ("continue_policy",)
    return BUCKET_CANDIDATE, ()


def require_promo_anchor(promo_location_id: Optional[str]) -> None:
    if not promo_location_id or not str(promo_location_id).strip():
        raise PromoAnchorError(
            "PROMO_LOCATION_ID missing — refusing to enumerate outlets "
            "(fail-closed anchor gate)"
        )


def zero_stock_candidates(
    transport: Any, *, promo_location_id: str
) -> ZeroStockReport:
    """READ-ONLY. Enumerate every OUTLET member and apply the delete predicate.

    Buckets each outlet into candidates / review (with reasons) / in-stock. A
    per-outlet read error isolates to a ``read_error`` review row — one unreadable
    outlet never aborts the scan. Nothing on Shopify or the Sheet is mutated.
    """
    require_promo_anchor(promo_location_id)
    members = ops.enumerate_outlet_products(transport)
    candidates: List[CandidateOutlet] = []
    review: List[ReviewOutlet] = []
    in_stock = 0
    for m in members:
        gid = m["id"]
        title = m.get("title")
        status = (m.get("status") or "").upper()
        try:
            variants = ops.read_variant_inventory(transport, gid)
        except _CAUGHT as e:
            review.append(ReviewOutlet(gid, title, status, (f"read_error:{type(e).__name__}",)))
            continue
        bucket, reasons = classify_outlet(variants, promo_location_id)
        if bucket == BUCKET_CANDIDATE:
            candidates.append(CandidateOutlet(gid, title, status, tuple(variants)))
        elif bucket == BUCKET_REVIEW:
            review.append(ReviewOutlet(gid, title, status, reasons))
        else:
            in_stock += 1
    return ZeroStockReport(len(members), tuple(candidates), tuple(review), in_stock)


# =============================================================================
# Snapshot (reconstruction aid, built BEFORE every delete)
# =============================================================================


def _variant_size(selected_options: List[Dict[str, Any]]) -> Optional[str]:
    for opt in selected_options or []:
        if (opt.get("name") or "").strip().casefold() in _SIZE_OPTION_NAMES:
            return opt.get("value")
    return None


def build_snapshot(transport: Any, product_gid: str) -> BeforeSnapshot:
    """Assemble the reconstructive before-snapshot from four READ ops."""
    core = ops.get_product_core(transport, product_gid)
    variants = ops.get_product_variants(transport, product_gid)
    metafields = ops.get_product_metafields(transport, product_gid)
    images = ops.get_product_media(transport, product_gid)

    snap_variants = tuple(
        SnapshotVariant(
            variant_id=v["id"],
            sku=v.get("sku"),
            size=_variant_size(v.get("selectedOptions") or []),
            price=v.get("price"),
            compare_at=v.get("compareAtPrice"),
            inventory_item_id=(v.get("inventoryItem") or {}).get("id"),
        )
        for v in variants
    )
    cols = tuple(
        SnapshotCollection(c["id"], c.get("title"), c.get("handle"), bool(c.get("smart")))
        for c in core.get("collections", [])
    )
    return BeforeSnapshot(
        product_gid=product_gid,
        title=core.get("title"),
        handle=core.get("handle"),
        status=core.get("status"),
        tags=tuple(core.get("tags") or []),
        variants=snap_variants,
        image_srcs=tuple(images),
        metafields=tuple(dict(m) for m in metafields),
        collections=cols,
    )


# =============================================================================
# Confirmation gates
# =============================================================================


def resolve_is_outlet(transport: Any, product_gid: str) -> bool:
    """READ-ONLY. True iff ``product_gid`` is a member of the OUTLET collection
    or its title matches the ``outlet`` substring — the SAME two-signal
    predicate :func:`resolvers.outlet_resolver` applies by SKU
    (``is_outlet_member OR title_is_outlet``), reused here directly on a GID:
    the single-delete escape hatch never has a SKU, only an operator-typed
    ``product_gid``. One :func:`ops.get_product_core` read; no mutation.
    """
    core = ops.get_product_core(transport, product_gid)
    title = core.get("title") or ""
    is_member = any(
        c.get("id") == ops.OUTLET_COLLECTION_GID for c in (core.get("collections") or [])
    )
    title_is_outlet = "outlet" in title.casefold()
    return is_member or title_is_outlet


def require_single_delete_target_is_outlet(transport: Any, product_gid: str) -> None:
    """Fail-closed HARDENING gate (post-review) for the single-delete escape
    hatch: raises :class:`SingleDeleteNotOutletError` if ``product_gid`` does
    NOT resolve as an outlet (see :func:`resolve_is_outlet`). Called BOTH
    synchronously by the API endpoint (before any job/``product_delete`` is
    reachable) AND, as belt-and-braces, from :func:`delete_single_apply` itself.
    """
    if not resolve_is_outlet(transport, product_gid):
        raise SingleDeleteNotOutletError(
            f"single-delete target does not resolve to an outlet: {product_gid}"
        )


def _require_gesture(human_gesture: Optional[str], count: int) -> None:
    """Human-gesture gate: the operator types the exact count OR ``CONFERMO``."""
    if human_gesture is None:
        raise DeleteConfirmationError(
            f"human gesture required: type {count} or {CONFIRM_WORD!r}"
        )
    g = str(human_gesture).strip()
    if g == CONFIRM_WORD:
        return
    if g.isdigit() and int(g) == count:
        return
    raise DeleteConfirmationError(
        f"human gesture mismatch: expected {count} or {CONFIRM_WORD!r}, got {g!r}"
    )



# =============================================================================
# Register reads shared by the delete paths
# =============================================================================


def product_skus(inventory: Sequence[Dict[str, Any]]) -> Tuple[str, ...]:
    """The SKUs of a product's real-size variants (rule 7: customisation-only
    variants are excluded from every match; a blank SKU resolves nothing)."""
    return tuple(sorted({(v.get("sku") or "").strip() for v in V.real_size_variants(inventory)} - {""}))


def shelf_of(register: Any, skus: Sequence[str]) -> List[PairRow]:
    """The ``IN_SCANSIA`` pairs of these SKUs, by pair id."""
    out: List[PairRow] = []
    for sku in skus:
        out.extend(register.shelf(sku))
    return sorted(out, key=lambda p: p.pair_id)


def _bind(audit_sink: Any, lease: PassLease) -> None:
    bind = getattr(audit_sink, "bind", None)
    if callable(bind):
        bind(lease)


def _safe_outcome(audit_sink: DeleteAuditSink, event: DeleteOutcomeEvent) -> None:
    try:
        audit_sink.write_outcome(event)
    except LeaseLost:
        raise
    except Exception:  # best-effort: never undo/block a completed delete
        logger.warning("write_outcome failed for %s (delete already committed)", event.product_gid)


# =============================================================================
# Terminal path (iii) and the deletion order's steps (1)–(3)
# =============================================================================


def close_product_rows(ctx: "flows.Context", product_id: int, delete_row: Optional[int]) -> Tuple[int, ...]:
    """Rule 2's terminal path (iii): once the product is gone, every ``RECONCILE``
    row of it not closed closes ("product deleted", the ``DELETE`` row's id): an
    ``APPLIED`` row ``CARRIED`` (what it moved stays netted), a sent −Δ row
    ``FAILED`` "possibly applied", every other row ``FAILED`` (a +Δ's, an
    activation's or a CREATE's units left with the product). Their groups are
    settled: an ``APPLIED`` sibling on another product closes ``CARRIED``."""
    closed: List[int] = []
    groups: List[int] = []
    for row in ctx.register.reconcile_rows(statuses=(OPEN, APPLIED), product_id=int(product_id)):
        row = ctx.ledger.reload(row)
        if row.status not in (OPEN, APPLIED):
            continue
        why = {"reason": "product_deleted", "delete_row": delete_row}
        if row.status == APPLIED:
            ctx.ledger.close(row, CARRIED, carried=True, **why)
        elif L.is_minus(row) and row.sent_at is not None:
            ctx.ledger.close(row, FAILED, possibly_applied=True, **why)
        else:
            ctx.ledger.close(row, FAILED, **why)
        closed.append(row.row_id)
        if row.group_id is not None and row.group_id not in groups:
            groups.append(row.group_id)
    for g in groups:
        ctx.ledger.settle(g)
    if closed:
        ctx.report.surface("rows_closed_product_deleted", product_id=int(product_id), rows=closed)
    return tuple(closed)


def _gone(transport: Any, product_gid: str) -> Optional[bool]:
    """True: the product reads gone (a definitive null); False: it exists; None:
    the read itself failed (outcome unknown)."""
    try:
        ops.get_product_core(transport, product_gid)
    except ProductNotFound:
        return True
    except (ShopifyUserError, ShopifyTransportError):
        return None
    return False


def delete_product(ctx: "flows.Context", audit_sink: DeleteAuditSink, product_gid: str, *,
                   archive_first: bool = False, snapshot: Optional[BeforeSnapshot] = None) -> DeleteOutcome:
    """The deletion order's steps (1)–(3) (§3.5) and terminal path (iii). The
    snapshot is built first (reads only: a read failure writes nothing); then (1)
    ``OUTLET_PRODUCT_ID`` cleared, (2) the durable snapshot — its failure aborts —
    and (3) ``productDelete``: success, or the product reading gone after a refused
    or unknown answer, closes the product's open rows; otherwise ``DELETE_FAILED``
    (the product exists; its rows stay as they are). ``snapshot``: one the caller
    built already (the single delete reads it before its step (0))."""
    reg, transport, lease = ctx.register, ctx.transport, ctx.ledger.lease
    num = flows.product_number(product_gid)
    if num is None:
        return DeleteOutcome(product_gid, STATUS_DELETE_FAILED, warnings=("not_a_product_gid",))
    if snapshot is None:
        try:
            snapshot = build_snapshot(transport, product_gid)
        except LeaseLost:
            raise
        except Exception as e:  # noqa: BLE001 - reads only, nothing written: skip this product
            return DeleteOutcome(product_gid, STATUS_SNAPSHOT_BUILD_FAILED,
                                 warnings=(f"snapshot_build:{type(e).__name__}",))
    warnings: List[str] = []
    cleared = reg.clear_outlet_product(lease, num)                                    # (1)
    if cleared:
        warnings.append(f"outlet_product_id_cleared:{len(cleared)}")
    try:
        delete_row = audit_sink.write_durable(snapshot)                               # (2) the gate
    except LeaseLost:
        raise
    except Exception as e:  # noqa: BLE001 - the sink's raise IS the abort gate
        return DeleteOutcome(product_gid, STATUS_SNAPSHOT_ABORTED,
                             warnings=tuple(warnings) + (f"durable_write_failed:{type(e).__name__}",))
    if archive_first:
        lease.keepalive()
        try:
            ops.product_update_status(transport, product_gid, "ARCHIVED")
        except _CAUGHT as e:
            return DeleteOutcome(product_gid, STATUS_DELETE_FAILED,
                                 warnings=tuple(warnings) + (f"archive_failed:{type(e).__name__}",))
        return DeleteOutcome(product_gid, STATUS_ARCHIVED, warnings=tuple(warnings) + ("archive_first:hold_window",))
    lease.keepalive()                                                                 # (3)
    deleted_id: Optional[str] = None
    try:
        deleted_id = ops.product_delete(transport, product_gid)
    except (ShopifyUserError, ShopifyTransportError) as e:
        gone = _gone(transport, product_gid)
        if gone is not True:
            why = "outcome_unknown" if gone is None else "refused"
            return DeleteOutcome(product_gid, STATUS_DELETE_FAILED,
                                 warnings=tuple(warnings) + (f"product_delete:{type(e).__name__}:{why}",))
        warnings.append(f"product_reads_gone_after:{type(e).__name__}")
    closed = close_product_rows(ctx, num, delete_row)                                 # (iii)
    _safe_outcome(audit_sink, DeleteOutcomeEvent(product_gid, deleted_id or product_gid, STATUS_DELETED))
    return DeleteOutcome(product_gid, STATUS_DELETED, deleted_id=deleted_id or product_gid,
                         warnings=tuple(warnings), closed_rows=closed)


# =============================================================================
# Bulk cleanup (DRAFT-at-zero) — preview / apply
# =============================================================================


def _effective_cap(threshold: int) -> int:
    """The threshold an operator can actually reach — hard-capped (fail-closed)."""
    return min(threshold, CLEANUP_HARD_CAP)


def _cleanup_hash(sorted_gids: List[str], threshold: int, archive_first: bool) -> str:
    """Bind {exact candidate set, threshold, effective cap, archive_first} — the
    count-gesture key. Binding the cap too means a hard-cap change (or an
    operator threshold that would have exceeded it) is itself covered by the
    TOCTOU re-verify, not just the raw threshold value."""
    cap = _effective_cap(threshold)
    raw = "|".join(sorted_gids) + f"#{threshold}#{cap}#{int(bool(archive_first))}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def cleanup_preview(
    register: Any, transport: Any, *, promo_location_id: str,
    threshold: int = DEFAULT_CLEANUP_THRESHOLD, archive_first: bool = False,
) -> CleanupPlan:
    """READ-ONLY. The DRAFT-at-zero candidates, minus every outlet whose SKU has an
    ``IN_SCANSIA`` pair (listed apart in ``protected``). ``requires_second_confirm``
    when the deletable count exceeds ``min(threshold, CLEANUP_HARD_CAP)``; the set,
    threshold, effective cap and archive_first are bound in ``plan_hash``."""
    z = zero_stock_candidates(transport, promo_location_id=promo_location_id)
    shelf_skus = {p.sku for p in register.shelf()}
    deletable: List[CandidateOutlet] = []
    protected: List[ReviewOutlet] = []
    for c in z.candidates:
        if c.status != CLEANUP_STATUS_FILTER:
            continue
        on_shelf = [s for s in product_skus(c.variants) if s in shelf_skus]
        if on_shelf:
            protected.append(ReviewOutlet(c.product_gid, c.title, c.status,
                                          tuple(f"shelf_pairs:{s}" for s in on_shelf)))
        else:
            deletable.append(c)
    gids = sorted(c.product_gid for c in deletable)
    return CleanupPlan(
        dry_run=True,
        candidates=tuple(CleanupCandidate(c.product_gid, c.title, c.status) for c in deletable),
        review=z.review,
        count=len(deletable),
        threshold=threshold,
        archive_first=archive_first,
        requires_second_confirm=len(deletable) > _effective_cap(threshold),
        plan_hash=_cleanup_hash(gids, threshold, archive_first),
        protected=tuple(protected),
    )


def _still_deletable(register: Any, transport: Any, product_gid: str, promo: str) -> Optional[str]:
    """Re-read right before the delete (the lease was released in between): still
    DRAFT, still a predicate candidate, still no shelf pair for its SKUs."""
    try:
        core = ops.get_product_core(transport, product_gid)
        inv = ops.read_variant_inventory(transport, product_gid)
    except ProductNotFound:
        return "product_gone"
    if (core.get("status") or "").upper() != CLEANUP_STATUS_FILTER:
        return f"status:{(core.get('status') or '').upper()}"
    bucket, reasons = classify_outlet(inv, promo)
    if bucket != BUCKET_CANDIDATE:
        return f"no_longer_candidate:{','.join(reasons) or bucket.lower()}"
    if shelf_of(register, product_skus(inv)):
        return "shelf_pairs"
    return None


def cleanup_apply(
    register: Any,
    transport: Any,
    audit_sink: DeleteAuditSink,
    approved_plan: CleanupPlan,
    *,
    human_gesture: Optional[str],
    promo_location_id: str,
    second_confirm: bool = False,
    lease: Optional[PassLease] = None,
    take_bound_s: float = DELETE_TAKE_BOUND_S,
    sleep: Callable[[float], None] = time.sleep,
) -> CleanupReport:
    """Execute the approved bulk cleanup (MUTATES — IRREVERSIBLE, confirm-gated).

    Gates, in order, all BEFORE any write: (1) the human gesture == count or
    CONFERMO; (2) the over-threshold second confirm; then, under the pass lease,
    (3) the live re-verify — the fresh deletable set must hash as approved, else
    the whole batch aborts (``verify_failed``). Each product is then re-read right
    before its own delete (yielding to a waiting decrement in between) and deleted
    by the deletion order's steps (1)–(3)."""
    _require_gesture(human_gesture, approved_plan.count)
    cap = _effective_cap(approved_plan.threshold)
    if approved_plan.count > cap and not second_confirm:
        raise DeleteConfirmationError(
            f"{approved_plan.count} candidates exceed cap {cap} "
            f"(threshold {approved_plan.threshold}, hard cap {CLEANUP_HARD_CAP}): second_confirm required"
        )
    run = passes.Holder(register, transport, promo_location_id, HOLDER_CLEANUP, lease=lease,
                        take_bound_s=take_bound_s, sleep=sleep)
    outcomes: List[DeleteOutcome] = []
    fresh_hash = approved_plan.plan_hash
    try:
        run.start()
        _bind(audit_sink, run.lease)
        fresh = cleanup_preview(register, transport, promo_location_id=promo_location_id,
                                threshold=approved_plan.threshold, archive_first=approved_plan.archive_first)
        fresh_hash = fresh.plan_hash
        if fresh_hash != approved_plan.plan_hash:
            return CleanupReport(dry_run=False, verify_failed=True, plan_hash=fresh_hash, deleted=0,
                                 surfaced=tuple(run.report.items))
        for cand in fresh.candidates:
            run.next_item()
            try:
                why = _still_deletable(register, transport, cand.product_gid, promo_location_id)
                if why is not None:
                    outcomes.append(DeleteOutcome(cand.product_gid, STATUS_SKIPPED, warnings=(why,)))
                    continue
                outcomes.append(delete_product(run.ctx, audit_sink, cand.product_gid,
                                               archive_first=approved_plan.archive_first))
            except (ShopifyUserError, ShopifyTransportError) as e:   # one product's failure never aborts the batch
                outcomes.append(DeleteOutcome(cand.product_gid, STATUS_ERROR,
                                              warnings=(f"shopify_error:{type(e).__name__}",)))
    except (LeaseLost, LeaseUnavailable) as e:
        why = "lease_unavailable" if isinstance(e, LeaseUnavailable) and not run.lease.takes else "lease_lost"
        done = {o.product_gid for o in outcomes}
        outcomes.extend(DeleteOutcome(c.product_gid, STATUS_ERROR, warnings=(why,))
                        for c in approved_plan.candidates if c.product_gid not in done)
    except StoreError as e:
        done = {o.product_gid for o in outcomes}
        outcomes.extend(DeleteOutcome(c.product_gid, STATUS_ERROR, warnings=(f"register_error:{type(e).__name__}",))
                        for c in approved_plan.candidates if c.product_gid not in done)
    finally:
        run.release()
    return CleanupReport(
        dry_run=False, verify_failed=False, plan_hash=fresh_hash,
        deleted=sum(1 for o in outcomes if o.status == STATUS_DELETED), outcomes=tuple(outcomes),
        surfaced=tuple(run.report.items),
    )


# =============================================================================
# Single delete (creation errors) — preview / apply
# =============================================================================


def _single_hash(product_gid: str, keys: Sequence[Key]) -> str:
    raw = product_gid + "#" + "|".join(f"{k[0]}\x1f{k[1]}" for k in sorted(keys))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def single_delete_preview(register: Any, transport: Any, product_gid: str) -> SingleDeletePlan:
    """READ-ONLY. Step (0)'s listing: the product SKUs' ``IN_SCANSIA`` pairs and
    the keys the confirm holds; the rows terminal path (iii) will close. Refuses a
    target that does not resolve to an outlet (:class:`SingleDeleteNotOutletError`)."""
    require_single_delete_target_is_outlet(transport, product_gid)
    core = ops.get_product_core(transport, product_gid)
    inv = ops.read_variant_inventory(transport, product_gid)
    skus = product_skus(inv)
    shelf = shelf_of(register, skus)
    keys = tuple(sorted({p.key for p in shelf}))
    num = flows.product_number(product_gid)
    rows = register.reconcile_rows(statuses=(OPEN, APPLIED), product_id=num) if num is not None else []
    return SingleDeletePlan(
        dry_run=True, product_gid=product_gid, title=core.get("title"),
        status=(core.get("status") or "").upper() or None, skus=skus,
        shelf_pairs=tuple(p.pair_id for p in shelf), keys=keys,
        open_rows=tuple(r.row_id for r in rows), plan_hash=_single_hash(product_gid, keys),
    )


def delete_single_apply(
    register: Any,
    transport: Any,
    audit_sink: DeleteAuditSink,
    approved: SingleDeletePlan,
    *,
    human_gesture: Optional[str],
    promo_location_id: str,
    lease: Optional[PassLease] = None,
    take_bound_s: float = DELETE_TAKE_BOUND_S,
    sleep: Callable[[float], None] = time.sleep,
) -> DeleteOutcome:
    """Hard-delete ONE product (a botched duplicate) by the deletion order: (0) the
    previewed keys held ("outlet deleted"), (1)–(3) :func:`delete_product`, (4)
    ``ONLINE = 0`` on the SKUs' shelf pairs once it is gone. The predicate is not
    applied (by design). Gesture: ``1`` or ``CONFERMO``; the target must resolve to
    an outlet; the plan re-derived under the lease must hash as approved."""
    _require_gesture(human_gesture, 1)
    gid = approved.product_gid
    require_single_delete_target_is_outlet(transport, gid)
    run = passes.Holder(register, transport, promo_location_id, HOLDER_DELETE, lease=lease,
                        take_bound_s=take_bound_s, sleep=sleep)
    try:
        run.start()
        _bind(audit_sink, run.lease)
        run.next_item()
        fresh = single_delete_preview(register, transport, gid)
        if fresh.plan_hash != approved.plan_hash:
            return DeleteOutcome(gid, STATUS_VERIFY_FAILED, warnings=("plan_hash_mismatch:live_state_changed",),
                                 surfaced=tuple(run.report.items))
        num = flows.product_number(gid)
        try:
            snapshot = build_snapshot(transport, gid)       # reads first: a failed read writes nothing
        except LeaseLost:
            raise
        except Exception as e:  # noqa: BLE001 - nothing written yet
            return DeleteOutcome(gid, STATUS_SNAPSHOT_BUILD_FAILED, warnings=(f"snapshot_build:{type(e).__name__}",),
                                 surfaced=tuple(run.report.items))
        if fresh.keys:                                                                # (0)
            register.record_hold(run.lease, keys=fresh.keys, reason=REASON_OUTLET_DELETED,
                                 action="delete-hold", product_id=num)
        out = delete_product(run.ctx, audit_sink, gid, snapshot=snapshot)             # (1)–(3), (iii)
        if out.status == STATUS_DELETED:                                              # (4)
            online = [p.pair_id for p in shelf_of(register, fresh.skus) if p.online]
            if online:
                register.set_online(run.lease, online, False)
        return replace(out, held_keys=fresh.keys, surfaced=tuple(run.report.items))
    except LeaseUnavailable:
        why = "lease_lost" if run.lease.takes else "lease_unavailable"
        return DeleteOutcome(gid, STATUS_ERROR, warnings=(why,), surfaced=tuple(run.report.items))
    except LeaseLost:
        return DeleteOutcome(gid, STATUS_ERROR, warnings=("lease_lost",), surfaced=tuple(run.report.items))
    finally:
        run.release()


__all__ = [
    "PromoAnchorError", "DeleteConfirmationError", "SingleDeleteNotOutletError",
    "CONFIRM_WORD", "DEFAULT_CLEANUP_THRESHOLD", "CLEANUP_HARD_CAP", "REASON_OUTLET_DELETED",
    "BUCKET_CANDIDATE", "BUCKET_REVIEW", "BUCKET_IN_STOCK",
    "STATUS_DELETED", "STATUS_ARCHIVED", "STATUS_SNAPSHOT_ABORTED", "STATUS_SNAPSHOT_BUILD_FAILED",
    "STATUS_DELETE_FAILED", "STATUS_SKIPPED", "STATUS_VERIFY_FAILED", "STATUS_ERROR",
    "SnapshotVariant", "SnapshotCollection", "BeforeSnapshot",
    "DeleteOutcomeEvent", "DeleteAuditSink",
    "CandidateOutlet", "ReviewOutlet", "ZeroStockReport",
    "CleanupCandidate", "CleanupPlan", "DeleteOutcome", "CleanupReport", "SingleDeletePlan",
    "classify_outlet", "zero_stock_candidates", "cleanup_preview", "cleanup_apply",
    "resolve_is_outlet", "require_single_delete_target_is_outlet", "require_promo_anchor",
    "product_skus", "shelf_of", "build_snapshot", "close_product_rows", "delete_product",
    "single_delete_preview", "delete_single_apply",
]
