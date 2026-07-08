""""Inizializza" (init / cutover reconciliation) — locked spec
``docs/init-reconcile-design.md``.

The one-time operation the operator runs ONCE, from the GUI first-run banner,
combining two concerns:

* **Part A — baseline** (``writer.backfill_cutover``, already built): stamps
  ``row_uuid``/``reconciled=true`` on every pre-existing row and writes the
  ``_scansia_cutover`` sentinel LAST. Sheet-only, zero Shopify impact, idempotent.
* **Part B — online-flag reconciliation against Shopify reality** (THIS module,
  NEW, Shopify-MUTATING at apply): for every sheet row with ``online=si``,
  resolve the outlet product for (SKU, Size) and decide whether it is genuinely
  still sellable. Rows with ``online=no`` are never touched — no claim to verify.

Two entry points, preview/apply split so DRY_RUN is fail-closed by construction
(mirrors :mod:`backend.services.outlet_service` / :mod:`backend.services.delete_service`):

* :func:`init_preview` — READ-ONLY. Reads the sheet with ``assign_uuids=False,
  require_cutover=False`` (the ONLY caller in the codebase that reads before
  cutover — see ``backend.gsheet.reader.read_canonical``), classifies every
  online row LIVE, and returns an :class:`InitPlan` split by bucket. Nothing on
  Shopify or the Sheet is touched.
* :func:`init_apply` — MUTATES (confirm-gated by the caller, ``api/init.py``).
  Runs Part A (``sheet.backfill_cutover()``) FIRST, then re-reads canonically
  (``assign_uuids=True`` — now every row carries a REAL, persisted ``row_uuid``),
  recomputes each row's classification LIVE (TOCTOU) and skips (VERIFY_FAILED)
  any row whose fresh decision no longer matches the approved one, then executes:
  one ``productUpdate status=DRAFT`` per distinct target product (deduped across
  sizes) and one sheet write-back (``online=NO``, ``Vendute il=<now, UTC ISO 8601>``)
  per demoted row.

### The "truly online" predicate (spec §B.2)

An outlet product row is **truly online** iff: the outlet product for its SKU
resolves to EXACTLY ONE match (>1 -> ``review:multi-match``, ambiguous, never
auto-demoted) AND that product's live status is ``ACTIVE`` (not DRAFT/ARCHIVED)
AND the variant for THIS ROW'S SIZE has ``available > 0`` on the Promo location.
Every other outcome demotes:

* 0 outlet matches -> ``demote:missing`` (nothing to draft — the product doesn't
  exist).
* 1 match, status != ACTIVE -> ``demote:draft`` (product exists -> DRAFT it).
* 1 match, ACTIVE, but this row's size is unmatched OR ``available`` is 0/absent
  on Promo -> ``demote:sold-out-size`` (product exists -> DRAFT it).

### plan_hash / row_uuid (TOCTOU key design)

``_row_plan_hash`` binds ``(sku, NORMALIZED size, bucket, target_gid,
live_status)`` — DELIBERATELY EXCLUDING ``row_uuid``: at preview time
(pre-cutover) a row's ``row_uuid`` may be an EPHEMERAL, in-memory-only value
(never persisted), so it cannot be compared against the REAL ``row_uuid``
minted by Part A's backfill inside ``init_apply``. The apply-time TOCTOU verify
therefore matches an approved decision to a freshly-reclassified row by
``(sku, normalized size)``, not by ``row_uuid`` — safe because classification
is a pure function of live Shopify state for that (sku, size) pair, so two
sheet rows sharing a (sku, size) always classify identically (CI-5
duplicate-return rows are indistinguishable here on purpose).

HIGH-1 fix (post-review): the hash is built from ``_norm_size(size)``, NOT the
raw sheet cell — two rows whose raw size differs only in formatting ('42' vs
'42.0') but which are equal after normalization MUST produce the identical
plan_hash, otherwise ``approved_by_key`` (keyed on ``(sku, _norm_size(size))``)
silently collapses the two approved decisions into one and the other row's
fresh recompute never matches -> false ``VERIFY_FAILED``.

### Dedup + ordering (apply)

Multiple demoted rows can share the same ``target_gid`` (e.g. two sizes of the
same outlet, both sold out) — ``productUpdate status=DRAFT`` is issued ONCE per
distinct ``target_gid`` (already-DRAFT is a Shopify no-op), BEFORE the sheet
write-backs for rows pointing at that gid. If the DRAFT call fails for a gid, ALL
rows tied to it are reported ``DRAFT_FAILED`` and their sheet cells are left
UNTOUCHED (never demote the sheet when the Shopify side didn't actually change).
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from backend.gsheet.reader import CanonRow, _norm_key, _truthy_si
from backend.services import resolvers
from backend.services.outlet_service import (
    _match_variant,
    _norm_size,
    _promo_available,
    _variant_size_index,
)
from backend.shopify import ops
from backend.shopify.ops import ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.init_service")

_ROME = ZoneInfo("Europe/Rome")
_UTC = ZoneInfo("UTC")

# Buckets (mirror the locked spec verbatim).
BUCKET_KEPT_ONLINE = "kept-online"
BUCKET_DEMOTE_MISSING = "demote:missing"
BUCKET_DEMOTE_DRAFT = "demote:draft"
BUCKET_DEMOTE_SOLD_OUT_SIZE = "demote:sold-out-size"
BUCKET_REVIEW_MULTI_MATCH = "review:multi-match"

_ALL_BUCKETS = (
    BUCKET_KEPT_ONLINE,
    BUCKET_DEMOTE_MISSING,
    BUCKET_DEMOTE_DRAFT,
    BUCKET_DEMOTE_SOLD_OUT_SIZE,
    BUCKET_REVIEW_MULTI_MATCH,
)
_DEMOTE_BUCKETS = (BUCKET_DEMOTE_MISSING, BUCKET_DEMOTE_DRAFT, BUCKET_DEMOTE_SOLD_OUT_SIZE)

_WRITEBACK_ONLINE_FIELD = "online"
_WRITEBACK_ONLINE_VALUE = "NO"
_WRITEBACK_DATE_FIELD = "Vendute il"
# Normalized-header key matching CanonRow.raw's key for the SAME column (reader
# builds ``raw`` via ``_norm_key(header_cell)`) — used to read the PRIOR
# 'Vendute il' value for the before-snapshot (HIGH-2), never to write it.
_VENDUTE_IL_RAW_KEY = _norm_key(_WRITEBACK_DATE_FIELD)

_CAUGHT_MUTATION_ERRORS = (ShopifyUserError, ShopifyTransportError)


# =============================================================================
# Plan / report data model
# =============================================================================
@dataclass(frozen=True)
class InitRowDecision:
    sku: str
    size: str
    row_uuid: str
    bucket: str
    target_gid: Optional[str]
    live_status: Optional[str]
    plan_hash: str


@dataclass(frozen=True)
class InitPlan:
    dry_run: bool
    cutover_already_done: bool
    backfill_pending_rows: int
    kept_online: Tuple[InitRowDecision, ...] = field(default_factory=tuple)
    demote_missing: Tuple[InitRowDecision, ...] = field(default_factory=tuple)
    demote_draft: Tuple[InitRowDecision, ...] = field(default_factory=tuple)
    demote_sold_out_size: Tuple[InitRowDecision, ...] = field(default_factory=tuple)
    review_multi_match: Tuple[InitRowDecision, ...] = field(default_factory=tuple)
    anomalies: Tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class InitOutcome:
    sku: str
    size: str
    row_uuid: str
    bucket: str
    status: str
    target_gid: Optional[str]
    warnings: Tuple[str, ...] = ()


@dataclass(frozen=True)
class InitReport:
    backfill_stamped: int
    backfill_already_done: bool
    demoted_rows: int
    drafted_products: int
    verify_failed_rows: int
    outcomes: Tuple[InitOutcome, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class InitDemotedRowSnapshot:
    """One demoted row's PRIOR state — captured BEFORE any Shopify mutation
    (HIGH-2, post-review)."""

    row_uuid: str
    sku: str
    size: str
    prior_online: str
    prior_vendute_il: str


@dataclass(frozen=True)
class InitTargetSnapshot:
    """One target product's PRIOR live status — captured BEFORE its
    ``productUpdate`` (HIGH-2, post-review)."""

    gid: str
    prior_status: Optional[str]


@dataclass(frozen=True)
class InitBeforeSnapshot:
    """Durable BEFORE-snapshot persisted BEFORE the first live Shopify mutation
    of an init apply (HIGH-2, post-review) — mirrors
    :class:`backend.services.delete_service.BeforeSnapshot` / its
    ``write_durable`` hard abort-gate contract, adapted to init's reconstruction
    need: prior online flag + prior 'Vendute il' per demoted row, prior
    ``live_status`` per distinct ``target_gid``."""

    plan_hash: str
    rows: Tuple[InitDemotedRowSnapshot, ...]
    targets: Tuple[InitTargetSnapshot, ...]


# =============================================================================
# Pure helpers
# =============================================================================
def _row_plan_hash(
    sku: str, size: str, bucket: str, target_gid: Optional[str], live_status: Optional[str]
) -> str:
    """TOCTOU key for one row. Deliberately EXCLUDES ``row_uuid`` (see module
    docstring): stable across the pre-cutover preview / post-backfill apply.

    HIGH-1 fix (post-review): hashes ``_norm_size(size)``, NOT the raw sheet
    cell — two rows whose raw size differs only in formatting ('42' vs '42.0')
    but are equal after normalization always classify identically and MUST
    produce the identical plan_hash (see module docstring)."""
    ns = _norm_size(size)
    raw = f"{sku}|{ns}|{bucket}|{target_gid or ''}|{(live_status or '').upper()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _sold_at_utc(now: Callable[[], datetime]) -> str:
    """Current instant as a UTC ISO-8601 timestamp matching the format Make
    already writes into ``Vendute il`` (the order ``created_at``) —
    ``YYYY-MM-DDTHH:MM:SS.000Z`` (owner-confirmed 2026-07-09, example
    ``2025-04-25T06:54:44.000Z``).

    A full UTC timestamp — NOT date-only, NOT Europe/Rome — so init's writes to
    this column never leave it with two incompatible date formats alongside
    Make's. Whole-second precision (``.000`` ms) matches the observed sample.
    """
    return now().astimezone(_UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _classify(
    sku: str,
    size: str,
    outlet_matches: List[Dict[str, Any]],
    size_index: Dict[str, Dict[str, Any]],
    inv: List[Dict[str, Any]],
    promo_location_id: str,
) -> Tuple[str, Optional[str], Optional[str]]:
    """Pure classifier — the "truly online" predicate (spec §B.2).

    Returns ``(bucket, target_gid, live_status)``. ``size_index``/``inv`` are
    ONLY consulted when there is exactly one ACTIVE outlet match (the caller
    decides whether it was worth fetching live inventory at all).
    """
    if len(outlet_matches) > 1:
        return BUCKET_REVIEW_MULTI_MATCH, None, None
    if len(outlet_matches) == 0:
        return BUCKET_DEMOTE_MISSING, None, None

    match = outlet_matches[0]
    gid = match.get("product_gid")
    status = (match.get("status") or "").upper()
    if status != "ACTIVE":
        return BUCKET_DEMOTE_DRAFT, gid, status

    ns = _norm_size(size)
    variant = _match_variant(ns, size_index, inv)
    if variant is None:
        return BUCKET_DEMOTE_SOLD_OUT_SIZE, gid, status

    avail = _promo_available(variant, promo_location_id)
    if avail is not None and avail > 0:
        return BUCKET_KEPT_ONLINE, gid, status
    return BUCKET_DEMOTE_SOLD_OUT_SIZE, gid, status


def _group_by_sku(rows: List[CanonRow]) -> Tuple[Dict[str, List[CanonRow]], List[str]]:
    groups: Dict[str, List[CanonRow]] = {}
    order: List[str] = []
    for r in rows:
        sku = (r.sku or "").strip()
        if not sku:
            continue
        if sku not in groups:
            groups[sku] = []
            order.append(sku)
        groups[sku].append(r)
    return groups, order


def _decide_all(
    online_rows: List[CanonRow], transport: Any, promo_location_id: str
) -> Tuple[Dict[str, List[InitRowDecision]], List[str]]:
    """Group ``online_rows`` by SKU, resolve the outlet ONCE per SKU + read live
    inventory ONCE per candidate gid (cached), and classify every row.

    Returns ``(buckets, anomalies)``; ``buckets`` always carries all 5 keys
    (possibly empty). Resolver warnings (MULTI_OUTLET/TRUNCATED/NO_EXACT/
    MIXED_SKU/REVIEW) are surfaced verbatim into ``anomalies`` — never silently
    dropped, never used to auto-promote a row to ``kept-online``.

    LOW-a fix (post-review): a per-SKU ``try/except`` isolates any resolver/
    inventory-read failure (known ``ShopifyUserError``/``ShopifyTransportError``
    OR an unexpected exception) to THAT SKU alone — its rows are skipped
    entirely (absent from every bucket) and the SKU is surfaced into
    ``anomalies``; every other SKU is still classified. Mirrors the per-outlet/
    per-action isolation in ``delete_service`` (``zero_stock_candidates``,
    ``_build_snapshot``) and ``publish_apply``.
    """
    groups, order = _group_by_sku(online_rows)
    buckets: Dict[str, List[InitRowDecision]] = {b: [] for b in _ALL_BUCKETS}
    anomalies: List[str] = []
    inv_cache: Dict[str, List[Dict[str, Any]]] = {}

    for sku in order:
        try:
            outlet_res = resolvers.outlet_resolver(transport, sku)
            if outlet_res.get("warning"):
                anomalies.append(f"{sku}:{outlet_res['warning']}")
            matches = outlet_res.get("matches") or []

            inv: List[Dict[str, Any]] = []
            idx: Dict[str, Dict[str, Any]] = {}
            if len(matches) == 1 and (matches[0].get("status") or "").upper() == "ACTIVE":
                gid = matches[0]["product_gid"]
                if gid not in inv_cache:
                    inv_cache[gid] = ops.read_variant_inventory(transport, gid)
                inv = inv_cache[gid]
                idx = _variant_size_index(inv)

            sku_decisions: List[InitRowDecision] = []
            for row in groups[sku]:
                bucket, target_gid, live_status = _classify(
                    sku, row.size, matches, idx, inv, promo_location_id
                )
                ph = _row_plan_hash(sku, row.size, bucket, target_gid, live_status)
                sku_decisions.append(
                    InitRowDecision(
                        sku=sku, size=row.size, row_uuid=row.row_uuid, bucket=bucket,
                        target_gid=target_gid, live_status=live_status, plan_hash=ph,
                    )
                )
        except Exception as e:  # noqa: BLE001 - LOW-a per-SKU isolation (see docstring)
            anomalies.append(f"{sku}:sku_error:{type(e).__name__}")
            logger.warning(
                "init _decide_all: SKU %s raised %s while resolving/classifying — "
                "skipping its rows (never aborts the whole reconcile)",
                sku, type(e).__name__,
            )
            continue

        for d in sku_decisions:
            buckets[d.bucket].append(d)
    return buckets, anomalies


def _flatten(plan: InitPlan) -> List[InitRowDecision]:
    return (
        list(plan.kept_online)
        + list(plan.demote_missing)
        + list(plan.demote_draft)
        + list(plan.demote_sold_out_size)
        + list(plan.review_multi_match)
    )


# =============================================================================
# Preview (READ-ONLY, DRY_RUN-safe — the ONLY caller of require_cutover=False)
# =============================================================================
def init_preview(sheet: Any, transport: Any, *, promo_location_id: str) -> InitPlan:
    """Compute the init PLAN without mutating Shopify or the Sheet.

    Reads canonically with ``assign_uuids=False, require_cutover=False`` — the
    ONLY read path in the codebase that tolerates the pre-cutover sheet (the
    sentinel is exactly what has not been written yet). Rows with ``online=no``
    are ignored entirely (no resolve, no bucket, per spec — no claim to verify).
    """
    cutover_already_done = sheet.cutover_done()
    read = sheet.read_canonical(assign_uuids=False, require_cutover=False)
    backfill_pending_rows = 0 if cutover_already_done else len(read.rows)

    online_rows = [r for r in read.rows if _truthy_si(r.online)]
    buckets, anomalies = _decide_all(online_rows, transport, promo_location_id)

    return InitPlan(
        dry_run=True,
        cutover_already_done=cutover_already_done,
        backfill_pending_rows=backfill_pending_rows,
        kept_online=tuple(buckets[BUCKET_KEPT_ONLINE]),
        demote_missing=tuple(buckets[BUCKET_DEMOTE_MISSING]),
        demote_draft=tuple(buckets[BUCKET_DEMOTE_DRAFT]),
        demote_sold_out_size=tuple(buckets[BUCKET_DEMOTE_SOLD_OUT_SIZE]),
        review_multi_match=tuple(buckets[BUCKET_REVIEW_MULTI_MATCH]),
        anomalies=tuple(anomalies),
    )


# =============================================================================
# Apply (MUTATES — confirm-gated upstream)
# =============================================================================
def init_apply(
    sheet: Any,
    transport: Any,
    approved_plan: InitPlan,
    audit_sink: Optional[Any],
    *,
    promo_location_id: str,
    now: Optional[Callable[[], datetime]] = None,
    approved_plan_hash: Optional[str] = None,
) -> InitReport:
    """Execute the approved plan.

    Ordering (per spec, load-bearing): (A) ``sheet.backfill_cutover()`` FIRST —
    sheet-only baseline, idempotent via the sentinel. (B) re-read canonically
    (``assign_uuids=True``) — every row now carries a REAL, persisted
    ``row_uuid``. (C) recompute every online row's classification LIVE and
    verify it against ``approved_plan`` by ``(sku, normalized size)`` — a row
    whose fresh decision no longer matches is skipped (``VERIFY_FAILED``,
    counted, no mutation for it). (D-pre) HIGH-2 (post-review): persist a
    durable BEFORE-snapshot (prior online flag/'Vendute il' per demoted row,
    prior live_status per target_gid) via ``audit_sink.write_init_before`` —
    the hard abort gate: if it raises, NOTHING further is mutated (no DRAFT, no
    write-back). (D) one ``productUpdate status=DRAFT`` per distinct
    ``target_gid`` among the surviving demoted rows. (E) one sheet write-back
    (``online=NO``, ``Vendute il=<now, UTC ISO 8601>``) per surviving demoted row (skipped,
    ``DRAFT_FAILED``, if that row's product DRAFT call failed). ``kept-online``
    and ``review:multi-match`` rows are left completely untouched.

    ``approved_plan_hash``: the caller's (api layer's) aggregate plan_hash for
    ``approved_plan`` — threaded through ONLY to be recorded verbatim into the
    before-snapshot and the AFTER audit event (an opaque string as far as this
    module is concerned; the aggregate-hash FORMULA itself lives in
    ``backend.api.init.init_plan_hash``, kept out of this module to stay
    FastAPI-free/stdlib-testable).
    """
    clock = now or (lambda: datetime.now(_UTC))
    sold_at = _sold_at_utc(clock)

    # (A) baseline backfill FIRST — sheet-only, idempotent, zero Shopify impact.
    backfill_report = sheet.backfill_cutover()

    # (B) re-read post-backfill: every row now carries a REAL, persisted row_uuid.
    read = sheet.read_canonical(assign_uuids=True)
    online_rows = [r for r in read.rows if _truthy_si(r.online)]
    row_by_uuid: Dict[str, CanonRow] = {r.row_uuid: r for r in online_rows}

    # (C) TOCTOU: recompute live, verify per row against the approved plan by
    # (sku, normalized size) — row_uuid is NOT a stable key across the
    # pre-cutover preview / post-backfill apply reads (see module docstring).
    approved_by_key: Dict[Tuple[str, str], InitRowDecision] = {}
    for d in _flatten(approved_plan):
        approved_by_key[(d.sku, _norm_size(d.size))] = d

    fresh_buckets, _fresh_anomalies = _decide_all(online_rows, transport, promo_location_id)

    verified: List[Tuple[CanonRow, InitRowDecision]] = []
    outcomes: List[InitOutcome] = []
    verify_failed_rows = 0

    for bucket_name in _ALL_BUCKETS:
        for d in fresh_buckets[bucket_name]:
            approved = approved_by_key.get((d.sku, _norm_size(d.size)))
            if approved is None or approved.plan_hash != d.plan_hash:
                verify_failed_rows += 1
                outcomes.append(InitOutcome(
                    sku=d.sku, size=d.size, row_uuid=d.row_uuid, bucket=bucket_name,
                    status="VERIFY_FAILED", target_gid=d.target_gid,
                    warnings=("plan_hash_mismatch:live_state_changed",),
                ))
                continue
            row = row_by_uuid.get(d.row_uuid)
            if row is None:  # pragma: no cover - defensive; d.row_uuid comes from online_rows itself
                continue
            verified.append((row, d))

    # (D) dedupe product->DRAFT: ONE call per distinct target_gid among demoted rows.
    demote_gids: Dict[str, List[Tuple[CanonRow, InitRowDecision]]] = {}
    for row, d in verified:
        if d.bucket in (BUCKET_DEMOTE_DRAFT, BUCKET_DEMOTE_SOLD_OUT_SIZE) and d.target_gid:
            demote_gids.setdefault(d.target_gid, []).append((row, d))

    # (D-pre) HIGH-2 (post-review): durable BEFORE-snapshot — prior online flag
    # + prior 'Vendute il' per demoted row, prior live_status per target_gid —
    # persisted BEFORE the first productUpdate/write_back below (mirrors
    # delete_service's write_durable abort gate). If the sink raises, ABORT:
    # nothing is drafted, nothing is written back (step A's backfill already
    # ran, but that's sheet-only/idempotent and safe).
    demote_row_snapshots = tuple(
        InitDemotedRowSnapshot(
            row_uuid=row.row_uuid, sku=d.sku, size=d.size,
            prior_online=row.online,
            prior_vendute_il=row.raw.get(_VENDUTE_IL_RAW_KEY, ""),
        )
        for row, d in verified if d.bucket in _DEMOTE_BUCKETS
    )
    target_snapshots = tuple(
        InitTargetSnapshot(gid=gid, prior_status=gid_rows[0][1].live_status)
        for gid, gid_rows in demote_gids.items()
    )
    snapshot_aborted = False
    if audit_sink is not None and demote_row_snapshots:
        try:
            audit_sink.write_init_before(InitBeforeSnapshot(
                plan_hash=approved_plan_hash or "",
                rows=demote_row_snapshots,
                targets=target_snapshots,
            ))
        except Exception as e:  # noqa: BLE001 - the injected sink's raise IS the gate
            snapshot_aborted = True
            logger.error(
                "init_apply: durable before-snapshot write failed -> aborting "
                "before any Shopify mutation: %s", type(e).__name__,
            )

    drafted_products = 0
    failed_gids: Dict[str, str] = {}
    if not snapshot_aborted:
        for gid in demote_gids:
            try:
                ops.product_update_status(transport, gid, "DRAFT")
                drafted_products += 1
            except Exception as e:  # noqa: BLE001 - LOW-b (post-review):
                # broadened beyond _CAUGHT_MUTATION_ERRORS — ANY failure here
                # (known Shopify error or unexpected) must leave this gid's rows
                # DRAFT_FAILED and their sheet cells untouched, never propagate
                # after some gids are already drafted and before any write-back.
                failed_gids[gid] = type(e).__name__
                logger.warning(
                    "init_apply: product_update_status(DRAFT) failed for a target gid: %s",
                    type(e).__name__,
                )

    # (E) sheet write-back — demote:* rows only; kept-online / review untouched.
    demoted_rows = 0
    for row, d in verified:
        if d.bucket == BUCKET_KEPT_ONLINE:
            outcomes.append(InitOutcome(d.sku, d.size, d.row_uuid, d.bucket, "KEPT", d.target_gid, ()))
            continue
        if d.bucket == BUCKET_REVIEW_MULTI_MATCH:
            outcomes.append(InitOutcome(d.sku, d.size, d.row_uuid, d.bucket, "REVIEW", d.target_gid, ()))
            continue
        # d.bucket in _DEMOTE_BUCKETS
        if snapshot_aborted:
            outcomes.append(InitOutcome(
                d.sku, d.size, d.row_uuid, d.bucket, "SNAPSHOT_ABORTED", d.target_gid,
                ("before_snapshot_write_failed",),
            ))
            continue
        if d.target_gid and d.target_gid in failed_gids:
            outcomes.append(InitOutcome(
                d.sku, d.size, d.row_uuid, d.bucket, "DRAFT_FAILED", d.target_gid,
                (f"product_update_status_failed:{failed_gids[d.target_gid]}",),
            ))
            continue
        res = sheet.write_back(
            row.row_uuid,
            {_WRITEBACK_ONLINE_FIELD: _WRITEBACK_ONLINE_VALUE, _WRITEBACK_DATE_FIELD: sold_at},
            expected_sku=d.sku,
        )
        if getattr(res, "ok", False):
            demoted_rows += 1
            outcomes.append(InitOutcome(d.sku, d.size, d.row_uuid, d.bucket, "DEMOTED", d.target_gid, ()))
        else:
            outcomes.append(InitOutcome(
                d.sku, d.size, d.row_uuid, d.bucket, "WRITEBACK_FAILED", d.target_gid,
                (f"writeback_failed:{getattr(res, 'reason', None)}",),
            ))

    if audit_sink is not None:
        try:
            audit_sink.write_event(
                action="init_reconcile",
                target_gids=",".join(sorted(demote_gids.keys())),
                plan_hash=approved_plan_hash or "",
                result=json.dumps({
                    "backfill_stamped": backfill_report.rows_stamped,
                    "backfill_already_done": backfill_report.already_done,
                    "demoted_rows": demoted_rows,
                    "drafted_products": drafted_products,
                    "verify_failed_rows": verify_failed_rows,
                    "snapshot_aborted": snapshot_aborted,
                }, separators=(",", ":")),
            )
        except Exception:  # noqa: BLE001 - best-effort: never undo a completed apply
            logger.warning("init_apply: audit write_event failed (apply already committed)")

    return InitReport(
        backfill_stamped=backfill_report.rows_stamped,
        backfill_already_done=backfill_report.already_done,
        demoted_rows=demoted_rows,
        drafted_products=drafted_products,
        verify_failed_rows=verify_failed_rows,
        outcomes=tuple(outcomes),
    )


__all__ = [
    "BUCKET_KEPT_ONLINE",
    "BUCKET_DEMOTE_MISSING",
    "BUCKET_DEMOTE_DRAFT",
    "BUCKET_DEMOTE_SOLD_OUT_SIZE",
    "BUCKET_REVIEW_MULTI_MATCH",
    "InitRowDecision",
    "InitPlan",
    "InitOutcome",
    "InitReport",
    "InitDemotedRowSnapshot",
    "InitTargetSnapshot",
    "InitBeforeSnapshot",
    "init_preview",
    "init_apply",
]
