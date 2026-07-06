"""IRREVERSIBLE outlet delete/cleanup (M5) — safety-critical, confirm-gated.

Composes the same leaf layers as :mod:`backend.services.outlet_service` /
:mod:`backend.services.pricing_service` (``backend.gsheet`` + ``backend.shopify.ops``)
into the hard-delete lifecycle for zero-stock / mis-created outlets. Four public
entry points, all preview/apply-split so DRY_RUN is fail-closed by construction:

* :func:`zero_stock_candidates` — READ-ONLY. Enumerates every OUTLET member
  (any status) and applies the EXACT per-variant delete predicate, bucketing each
  outlet into ``candidates`` (safely deletable), ``review`` (ambiguous/unsafe —
  never auto-deleted, carries reasons), or in-stock (skipped). Nothing mutated.
* :func:`cleanup_preview` / :func:`cleanup_apply` — the one-time bulk cleanup of
  DRAFT-at-zero outlets. Preview builds a :class:`CleanupPlan` whose ``plan_hash``
  binds the exact candidate set + threshold + archive_first. Apply is triple-gated
  (see SAFETY below) and only ever deletes outlets that STILL pass the predicate
  live and whose set is unchanged since preview.
* :func:`delete_single_apply` — single hard-delete for a creation-error product
  (a botched duplicate). Same mandatory-snapshot gate + human gesture; the
  zero-stock predicate is deliberately NOT re-applied here (a crashed CREATE may
  carry inherited non-Promo stock — that is exactly what you are deleting).
* :func:`deny_normalize` — the gated DENY-normalization helper (below).

### The per-variant delete PREDICATE (exact — plan §Delete / GET /outlet/zero-stock)

An outlet is a **candidate** iff, over ALL its variants:

1. every variant resolves to a Promo inventory level that is PRESENT with
   ``available == 0`` (a MISSING Promo level -> the whole outlet is UNKNOWN ->
   REVIEW, never summed as ``0``, never auto-candidate), AND
2. ``SUM(committed @ Promo) == 0``, AND
3. ``∀`` variant ``inventoryPolicy == "DENY"`` (NO ``CONTINUE`` — for-all, not
   aggregated).

Routed to **review** (never a candidate, never counted toward the delete gate):
``available < 0`` (oversell) · stock ``!= 0`` at any location ``!= Promo`` · a
Promo level absent on any variant · a truncated level set (UNKNOWN) · any
``CONTINUE`` variant · zero-available-but-committed>0 · no variants.

The truncation / non-Promo / Promo-present distinctions reuse the SAME predicate
helpers that drive the publish quarantine (``outlet_service._has_non_promo_stock``
/ ``_promo_available``) — one source of truth for "phantom / unknown stock".

### SAFETY (common to every apply path — confirm-gated upstream)

1. **Mandatory reconstructive ``before_snapshot``** (title/handle/status/tags, all
   variants' sku/size/price/compareAt/inventoryItem, image srcs, metafields,
   collection memberships) is built and handed to the injected
   :class:`DeleteAuditSink.write_durable` — which persists it to TWO durable sinks
   and RAISES on any failure. That durable write is the SINGLE gate: if it raises,
   ``productDelete`` is NEVER reached for that outlet (:data:`STATUS_SNAPSHOT_ABORTED`).
2. **Live predicate re-verification** at apply (cleanup): the fresh candidate set
   is recomputed and its ``plan_hash`` compared to the approved plan's — ANY drift
   (a formerly-candidate outlet that gained stock, a changed count) aborts the
   WHOLE batch (:data:`verify_failed`), because the human's count-gesture is bound
   to the exact set.
3. **Human gesture** (:func:`_require_gesture`) — the operator must type the exact
   candidate count OR the word ``CONFERMO``, IN ADDITION to the upstream confirm
   token. Over :data:`threshold` candidates additionally demands a ``second_confirm``.

### IRREVERSIBILITY (explicit)

``productDelete`` has NO undo on Shopify. The snapshot is a best-effort
reconstruction AID, NOT a restore: it deliberately omits bodyHtml/description,
option structure, SEO, published channels, variant barcode/weight/tracking/cost —
a rebuild yields fresh GIDs/handles and loses order/SEO linkage. **Primary
recovery = re-publish from the still-existing SOURCE product** (the outlet is a
duplicate of it). ARCHIVE->DRAFT staging is offered (``archive_first``) but is NOT
the default (owner: outlets are disposable, history lives in MySQL).

### CONTINUE-DENY coupling

The 38 legacy ``CONTINUE`` outlets are IRREMOVABLE while the predicate demands
for-all ``DENY``: they land in review (``continue_policy``) forever until a gated
DENY-normalization runs first. :func:`deny_normalize` is that mutation
(``productVariantsBulkUpdate`` inventoryPolicy=DENY on every variant); it is a
confirm-gated MUTATION, not part of any preview.

### Promo anchor gate (fail-closed)

:func:`zero_stock_candidates` refuses to enumerate when ``promo_location_id`` is
absent — a wrong/empty anchor would make ``available@Promo`` unresolvable and risk
flagging every outlet. Verifying that the id LIVE-resolves to a location named
"Promo" is the M1b startup gate (see ``backend.config`` note), out of this pure
service layer.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, Tuple

from backend.gsheet import GSheetError
from backend.services.outlet_service import (
    _SIZE_OPTION_NAMES,
    _has_non_promo_stock,
    _promo_available,
)
from backend.shopify import ops
from backend.shopify.ops import ShopifyUserError
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

# Buckets returned by _classify_outlet.
BUCKET_CANDIDATE = "CANDIDATE"
BUCKET_REVIEW = "REVIEW"
BUCKET_IN_STOCK = "IN_STOCK"

# Per-outcome status codes.
STATUS_DELETED = "DELETED"
STATUS_ARCHIVED = "ARCHIVED"                    # archive_first staging (hold, not deleted)
STATUS_SNAPSHOT_ABORTED = "SNAPSHOT_ABORTED"    # write_durable raised -> NO delete (the gate)
STATUS_SNAPSHOT_BUILD_FAILED = "SNAPSHOT_BUILD_FAILED"  # a read op for the snapshot failed
STATUS_DELETE_FAILED = "DELETE_FAILED"          # productDelete raised

# Row write-back defaults (DoD: online=NO). Both are PARAMETERS on the apply calls
# so an operator can point them at a tool-private column instead (writer CI-6),
# but the plan DoD asserts online=NO, which is the default here.
DEFAULT_WRITEBACK_FIELD = "online"
DEFAULT_WRITEBACK_VALUE = "NO"

_CAUGHT = (ShopifyUserError, ShopifyTransportError, GSheetError, RuntimeError)


# =============================================================================
# Errors
# =============================================================================


class PromoAnchorError(RuntimeError):
    """The Promo anchor is missing — fail-closed refusal to enumerate."""


class DeleteConfirmationError(RuntimeError):
    """A required human gesture / second confirmation was missing or wrong."""


# =============================================================================
# Injected audit sink (mocked in tests; the SQLite+GSheet impl arrives with M2)
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

    ``write_durable`` MUST persist the snapshot to TWO durable sinks and RAISE on
    any failure — that raise is the abort gate. ``write_outcome`` records the
    post-delete event and is best-effort (never blocks a completed delete).
    """

    def write_durable(self, snapshot: BeforeSnapshot) -> None: ...

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


@dataclass(frozen=True)
class DeleteOutcome:
    product_gid: str
    status: str
    deleted_id: Optional[str] = None
    warnings: Tuple[str, ...] = ()


@dataclass(frozen=True)
class CleanupReport:
    dry_run: bool
    verify_failed: bool
    plan_hash: str
    deleted: int
    outcomes: Tuple[DeleteOutcome, ...] = field(default_factory=tuple)


# =============================================================================
# Predicate
# =============================================================================


def _classify_outlet(
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


def _require_promo_anchor(promo_location_id: Optional[str]) -> None:
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
    _require_promo_anchor(promo_location_id)
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
        bucket, reasons = _classify_outlet(variants, promo_location_id)
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


def _build_snapshot(transport: Any, product_gid: str) -> BeforeSnapshot:
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
# Single-outlet delete core (shared by cleanup_apply and delete_single_apply)
# =============================================================================


def _safe_outcome(audit_sink: DeleteAuditSink, event: DeleteOutcomeEvent) -> None:
    try:
        audit_sink.write_outcome(event)
    except Exception:  # best-effort: never undo/block a completed delete
        logger.warning("write_outcome failed for %s (delete already committed)", event.product_gid)


def _delete_one(
    transport: Any,
    sheet: Any,
    audit_sink: DeleteAuditSink,
    product_gid: str,
    gid_rows: Dict[str, List[Tuple[str, str]]],
    *,
    archive_first: bool,
    writeback_field: str,
    writeback_value: Any,
) -> DeleteOutcome:
    """Snapshot -> durable write (ABORT gate) -> [archive?] -> delete -> write-back.

    The durable write is the ONLY gate: if it raises, ``product_delete`` is NEVER
    reached (:data:`STATUS_SNAPSHOT_ABORTED`).
    """
    warnings: List[str] = []

    # (1) Build the reconstructive snapshot. This is PRE-delete (nothing mutated
    #     yet), so ANY failure — not just the known ops-boundary set — must skip
    #     this outlet cleanly rather than propagate and abort the whole batch
    #     (e.g. a malformed non-null Shopify response -> KeyError/TypeError).
    try:
        snapshot = _build_snapshot(transport, product_gid)
    except Exception as e:  # noqa: BLE001 - pre-delete, graceful per-outlet skip
        return DeleteOutcome(
            product_gid, STATUS_SNAPSHOT_BUILD_FAILED,
            warnings=(f"snapshot_build:{type(e).__name__}",),
        )

    # (2) Durable write to TWO sinks — the SINGLE abort gate. On ANY failure we
    #     do NOT delete (product_delete below is unreachable for this outlet).
    try:
        audit_sink.write_durable(snapshot)
    except Exception as e:  # noqa: BLE001 - the injected sink's raise IS the gate
        return DeleteOutcome(
            product_gid, STATUS_SNAPSHOT_ABORTED,
            warnings=(f"durable_write_failed:{type(e).__name__}",),
        )

    # (3) Optional ARCHIVE-first staging (surfaced, not default): hold, don't delete.
    if archive_first:
        try:
            ops.product_update_status(transport, product_gid, "ARCHIVED")
        except _CAUGHT as e:
            return DeleteOutcome(
                product_gid, STATUS_DELETE_FAILED,
                warnings=(f"archive_failed:{type(e).__name__}",),
            )
        return DeleteOutcome(product_gid, STATUS_ARCHIVED, warnings=("archive_first:hold_window",))

    # (4) Hard delete. Optional urlRedirectCreate (ACTIVE -> 404/SEO) is a documented
    #     manual/runbook follow-up: no op exists and it is out of the delete gate.
    try:
        deleted_id = ops.product_delete(transport, product_gid)
    except _CAUGHT as e:
        return DeleteOutcome(
            product_gid, STATUS_DELETE_FAILED,
            warnings=(f"product_delete:{type(e).__name__}",),
        )

    # (5) Row write-back keyed by col Q (gid), CAS-guarded — best-effort.
    for row_uuid, sku in gid_rows.get(product_gid, []):
        try:
            res = sheet.write_delete_state(
                row_uuid, product_gid, expected_sku=sku,
                field=writeback_field, value=writeback_value,
            )
            if not getattr(res, "ok", False):
                warnings.append(f"writeback_failed:{getattr(res, 'reason', None)}")
        except _CAUGHT as e:
            warnings.append(f"writeback_error:{type(e).__name__}")

    # (6) Outcome event (best-effort — the delete is already committed).
    _safe_outcome(audit_sink, DeleteOutcomeEvent(product_gid, deleted_id, STATUS_DELETED))
    return DeleteOutcome(product_gid, STATUS_DELETED, deleted_id=deleted_id, warnings=tuple(warnings))


def _sheet_gid_index(sheet: Any) -> Dict[str, List[Tuple[str, str]]]:
    """Map col-Q gid -> [(row_uuid, sku), ...] for delete write-back."""
    if sheet is None:
        return {}
    read = sheet.read_canonical(assign_uuids=True)
    idx: Dict[str, List[Tuple[str, str]]] = {}
    for r in read.rows:
        gid = (getattr(r, "product_id", "") or "").strip()
        if gid:
            idx.setdefault(gid, []).append((r.row_uuid, r.sku))
    return idx


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
    transport: Any, *, promo_location_id: str,
    threshold: int = DEFAULT_CLEANUP_THRESHOLD, archive_first: bool = False,
) -> CleanupPlan:
    """READ-ONLY. Plan the one-time bulk cleanup of DRAFT-at-zero candidates.

    ``requires_second_confirm`` is set when the candidate count exceeds
    ``min(threshold, CLEANUP_HARD_CAP)`` — the operator-supplied ``threshold`` can
    never silently disable the second-confirm gate above the hard cap. The count +
    threshold + effective cap + archive_first are bound in ``plan_hash``.
    """
    z = zero_stock_candidates(transport, promo_location_id=promo_location_id)
    cands = [c for c in z.candidates if c.status == CLEANUP_STATUS_FILTER]
    gids = sorted(c.product_gid for c in cands)
    ph = _cleanup_hash(gids, threshold, archive_first)
    return CleanupPlan(
        dry_run=True,
        candidates=tuple(CleanupCandidate(c.product_gid, c.title, c.status) for c in cands),
        review=z.review,
        count=len(cands),
        threshold=threshold,
        archive_first=archive_first,
        requires_second_confirm=len(cands) > _effective_cap(threshold),
        plan_hash=ph,
    )


def cleanup_apply(
    transport: Any,
    sheet: Any,
    audit_sink: DeleteAuditSink,
    approved_plan: CleanupPlan,
    *,
    human_gesture: Optional[str],
    promo_location_id: str,
    second_confirm: bool = False,
    writeback_field: str = DEFAULT_WRITEBACK_FIELD,
    writeback_value: Any = DEFAULT_WRITEBACK_VALUE,
) -> CleanupReport:
    """Execute the approved bulk cleanup (MUTATES — IRREVERSIBLE, confirm-gated).

    Gates, in order (all BEFORE any mutation): (1) human gesture == count/CONFERMO;
    (2) over-threshold second confirm; (3) live predicate re-verify — the fresh
    DRAFT-candidate set's ``plan_hash`` must equal the approved plan's, else the
    whole batch aborts (``verify_failed``). Then each surviving candidate goes
    through :func:`_delete_one` (snapshot -> durable-write gate -> delete).
    """
    # (1) human gesture — bound to the APPROVED count.
    _require_gesture(human_gesture, approved_plan.count)
    # (2) over-threshold second confirmation — gated on the HARD-CAPPED threshold,
    #     so an operator-supplied threshold (e.g. 10000) can never silently
    #     disable this gate.
    cap = _effective_cap(approved_plan.threshold)
    if approved_plan.count > cap and not second_confirm:
        raise DeleteConfirmationError(
            f"{approved_plan.count} candidates exceed cap {cap} "
            f"(threshold {approved_plan.threshold}, hard cap {CLEANUP_HARD_CAP}): "
            f"second_confirm required"
        )

    # (3) live re-verify: recompute the DRAFT-candidate set + hash.
    fresh = zero_stock_candidates(transport, promo_location_id=promo_location_id)
    fresh_cands = [c for c in fresh.candidates if c.status == CLEANUP_STATUS_FILTER]
    fresh_gids = sorted(c.product_gid for c in fresh_cands)
    fresh_hash = _cleanup_hash(fresh_gids, approved_plan.threshold, approved_plan.archive_first)
    if fresh_hash != approved_plan.plan_hash:
        # Set drifted since preview -> the count-gesture no longer applies. Abort.
        return CleanupReport(dry_run=False, verify_failed=True, plan_hash=fresh_hash, deleted=0)

    gid_rows = _sheet_gid_index(sheet)
    outcomes: List[DeleteOutcome] = []
    for cand in fresh_cands:
        outcomes.append(
            _delete_one(
                transport, sheet, audit_sink, cand.product_gid, gid_rows,
                archive_first=approved_plan.archive_first,
                writeback_field=writeback_field, writeback_value=writeback_value,
            )
        )
    deleted = sum(1 for o in outcomes if o.status == STATUS_DELETED)
    return CleanupReport(
        dry_run=False, verify_failed=False, plan_hash=fresh_hash,
        deleted=deleted, outcomes=tuple(outcomes),
    )


# =============================================================================
# Single delete (creation errors)
# =============================================================================


def delete_single_apply(
    transport: Any,
    sheet: Any,
    audit_sink: DeleteAuditSink,
    product_gid: str,
    *,
    human_gesture: Optional[str],
    writeback_field: str = DEFAULT_WRITEBACK_FIELD,
    writeback_value: Any = DEFAULT_WRITEBACK_VALUE,
) -> DeleteOutcome:
    """Hard-delete ONE product (a creation-error / botched duplicate).

    Same mandatory-snapshot abort gate as the bulk path. The zero-stock predicate
    is deliberately NOT applied: a crashed CREATE may carry inherited non-Promo
    stock — that is exactly what this escape hatch removes. Human gesture: type
    ``1`` or ``CONFERMO``.
    """
    _require_gesture(human_gesture, 1)
    gid_rows = _sheet_gid_index(sheet)
    return _delete_one(
        transport, sheet, audit_sink, product_gid, gid_rows,
        archive_first=False, writeback_field=writeback_field, writeback_value=writeback_value,
    )


# =============================================================================
# CONTINUE-DENY coupling: gated DENY-normalization
# =============================================================================


def deny_normalize(transport: Any, product_gid: str) -> int:
    """Force ``inventoryPolicy=DENY`` on EVERY variant (gated MUTATION).

    Unblocks the 38 legacy ``CONTINUE`` outlets so they can pass the for-all-DENY
    delete predicate. Returns the number of variants normalized. Confirm-gated
    upstream (it is a live mutation, never called from a preview).
    """
    variants = ops.get_product_variants(transport, product_gid)
    updates = [{"id": v["id"], "inventoryPolicy": _DENY} for v in variants]
    ops.product_variants_bulk_update(transport, product_gid, updates)
    return len(updates)


__all__ = [
    "PromoAnchorError", "DeleteConfirmationError",
    "CONFIRM_WORD", "DEFAULT_CLEANUP_THRESHOLD", "CLEANUP_HARD_CAP",
    "BUCKET_CANDIDATE", "BUCKET_REVIEW", "BUCKET_IN_STOCK",
    "STATUS_DELETED", "STATUS_ARCHIVED", "STATUS_SNAPSHOT_ABORTED",
    "STATUS_SNAPSHOT_BUILD_FAILED", "STATUS_DELETE_FAILED",
    "SnapshotVariant", "SnapshotCollection", "BeforeSnapshot",
    "DeleteOutcomeEvent", "DeleteAuditSink",
    "CandidateOutlet", "ReviewOutlet", "ZeroStockReport",
    "CleanupCandidate", "CleanupPlan", "DeleteOutcome", "CleanupReport",
    "zero_stock_candidates", "cleanup_preview", "cleanup_apply",
    "delete_single_apply", "deny_normalize",
]
