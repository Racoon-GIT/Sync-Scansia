"""INIT vertical — the one-time "Inizializza" cutover reconciliation behind the
shared preview-job / apply-job / confirm-token machinery (:mod:`backend.api.mutations`).

Wires :mod:`backend.services.init_service` to auth-gated routes:

* ``POST /init/preview`` + ``GET .../{job_id}`` — READ-ONLY plan job
  (:func:`~init_service.init_preview`): an :class:`InitPlan` split by bucket
  (``kept_online`` / ``demote_missing`` / ``demote_draft`` / ``demote_sold_out_size``
  / ``review_multi_match``) + an aggregate ``plan_hash`` + a confirm-token bound
  to it. Reads the sheet BEFORE cutover (``require_cutover=False`` — see
  ``backend.gsheet.reader.read_canonical``), so this is the one preview allowed
  to run on a never-cut sheet.
* ``POST /init/apply`` — confirm-gated: a bad/expired token -> 409 (no job, no
  service call). Otherwise a job re-plans LIVE, drift-gates on the aggregate hash
  (``VERIFY_FAILED`` on mismatch), enforces the human gesture (``CONFERMO``
  ALWAYS, PLUS ``second_confirm`` when the total demotion count exceeds
  :data:`INIT_DEMOTE_THRESHOLD`), and only then calls
  :func:`~init_service.init_apply` — which owns Part A (``backfill_cutover``,
  run FIRST) + Part B (per-row TOCTOU re-verify + dedup DRAFT + write-back) and
  its own audit event.
* ``GET /init/status`` — READ-ONLY, NO token, Basic-Auth only: ``{"cutover_done": bool}``
  — the GUI first-run-banner gating surface (the sentinel-presence probe).

The aggregate ``plan_hash`` is the order-independent sha256 over EVERY row's
``(sku, normalized size, row plan_hash)`` across all 5 buckets — the coarse
batch drift gate on top of ``init_apply``'s own per-row TOCTOU verify (by
``(sku, normalized size)`` — see ``init_service`` module docstring for why not
``row_uuid``).
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Request

from backend.api.mutations import (
    ApplyRequest,
    CODE_GESTURE_REQUIRED,
    MutationVertical,
    build_mutation_router,
)
from backend.auth.basic_auth import require_basic_auth
from backend.persistence.gsheet_audit import GSheetAuditSink
from backend.services import init_service
from backend.services.delete_service import CONFIRM_WORD
from backend.services.outlet_service import _norm_size

PREFIX = "/init"
KIND_PREVIEW = "init_preview"
KIND_APPLY = "init_apply"

# Vertical id SIGNED into the confirm-token (post-review HARDENING — see
# backend.persistence.tokens): binds the init preview/apply token pair to
# THIS vertical only.
TOKEN_KIND_INIT = "init"

# Above this many TOTAL demotions (missing + draft + sold-out-size combined),
# init_apply demands a second_confirm — mirrors delete_service's cleanup threshold.
INIT_DEMOTE_THRESHOLD = 25


# =============================================================================
# Aggregate plan_hash — the batch-level TOCTOU key the confirm-token binds.
# =============================================================================
def _all_decisions(plan: Any) -> List[Any]:
    return (
        list(plan.kept_online)
        + list(plan.demote_missing)
        + list(plan.demote_draft)
        + list(plan.demote_sold_out_size)
        + list(plan.review_multi_match)
    )


def init_plan_hash(plan: Any) -> str:
    """Order-independent sha256 (16 hex) over every row's ``(sku, normalized
    size, plan_hash)`` across ALL buckets. Changes iff any row's live
    classification (or the row set) moved between preview and apply — the
    coarse batch drift gate on top of ``init_apply``'s own per-row TOCTOU
    verify.

    HIGH-1 fix (post-review): the SIZE fed into this aggregate is normalized
    (``_norm_size``), matching ``d.plan_hash`` itself (see
    ``init_service._row_plan_hash``) — two rows equal after normalization but
    differing only in raw formatting ('42' vs '42.0') must never make this
    aggregate depend on which raw spelling happened to be re-read.
    """
    parts = sorted(f"{d.sku}:{_norm_size(d.size)}:{d.plan_hash}" for d in _all_decisions(plan))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


# =============================================================================
# JSON-safe serializers
# =============================================================================
def _decision_to_dict(d: Any) -> Dict[str, Any]:
    return {
        "sku": d.sku,
        "size": d.size,
        "row_uuid": d.row_uuid,
        "bucket": d.bucket,
        "target_gid": d.target_gid,
        "live_status": d.live_status,
        "plan_hash": d.plan_hash,
    }


def serialize_plan(plan: Any) -> Dict[str, Any]:
    return {
        "dry_run": plan.dry_run,
        "cutover_already_done": plan.cutover_already_done,
        "backfill_pending_rows": plan.backfill_pending_rows,
        "kept_online": [_decision_to_dict(d) for d in plan.kept_online],
        "demote_missing": [_decision_to_dict(d) for d in plan.demote_missing],
        "demote_draft": [_decision_to_dict(d) for d in plan.demote_draft],
        "demote_sold_out_size": [_decision_to_dict(d) for d in plan.demote_sold_out_size],
        "review_multi_match": [_decision_to_dict(d) for d in plan.review_multi_match],
        "anomalies": list(plan.anomalies),
    }


def _outcome_to_dict(o: Any) -> Dict[str, Any]:
    return {
        "sku": o.sku,
        "size": o.size,
        "row_uuid": o.row_uuid,
        "bucket": o.bucket,
        "status": o.status,
        "target_gid": o.target_gid,
        "warnings": list(o.warnings),
    }


def serialize_report(report: Any) -> Dict[str, Any]:
    return {
        "backfill_stamped": report.backfill_stamped,
        "backfill_already_done": report.backfill_already_done,
        "demoted_rows": report.demoted_rows,
        "drafted_products": report.drafted_products,
        "verify_failed_rows": report.verify_failed_rows,
        "outcomes": [_outcome_to_dict(o) for o in report.outcomes],
    }


# =============================================================================
# Service closures over app.state (reached via the module so tests can
# monkeypatch init_service.init_preview / init_apply).
# =============================================================================
def _make_preview(state: Any):
    promo = state.promo_location_id

    def _preview(sheet: Any, transport: Any) -> Any:
        return init_service.init_preview(sheet, transport, promo_location_id=promo)

    return _preview


def _make_apply(state: Any):
    promo = state.promo_location_id

    def _apply(sheet: Any, transport: Any, approved_plan: Any, audit_sink: Any) -> Any:
        # HIGH-2 (post-review): thread the REAL aggregate plan_hash through, so
        # init_apply's before-snapshot + AFTER audit event record it (instead of
        # ""). Recomputed here (not passed as a fresh field on approved_plan)
        # because ``approved_plan`` IS the fresh, drift-verified plan the caller
        # (mutations._run_apply_job) already re-previewed and hashed — recomputing
        # via the SAME pure function is cheap and keeps init_service FastAPI-free.
        return init_service.init_apply(
            sheet, transport, approved_plan, audit_sink,
            promo_location_id=promo,
            approved_plan_hash=init_plan_hash(approved_plan),
        )

    return _apply


def _make_audit(state: Any, actor: str):
    def _audit(sheet: Any) -> Any:
        return GSheetAuditSink.from_scansia_sheet(sheet, actor=actor)

    return _audit


def _make_gesture(body: ApplyRequest):
    """Human-gesture gate checked against the FRESH (re-previewed) plan.

    ``confirm`` must equal ``CONFERMO`` ALWAYS (init drafts live products — never
    silently auto-run). A ``second_confirm`` is additionally required when the
    total demotion count (missing + draft + sold-out-size) exceeds
    :data:`INIT_DEMOTE_THRESHOLD` — mirrors the delete/cleanup speed-bump.
    """

    def _gesture(fresh_plan: Any) -> Optional[str]:
        if (body.confirm or "").strip() != CONFIRM_WORD:
            return CODE_GESTURE_REQUIRED
        demote_count = (
            len(fresh_plan.demote_missing)
            + len(fresh_plan.demote_draft)
            + len(fresh_plan.demote_sold_out_size)
        )
        if demote_count > INIT_DEMOTE_THRESHOLD and not body.second_confirm:
            return CODE_GESTURE_REQUIRED
        return None

    return _gesture


INIT_VERTICAL = MutationVertical(
    prefix=PREFIX,
    preview_kind=KIND_PREVIEW,
    apply_kind=KIND_APPLY,
    make_preview=_make_preview,
    make_apply=_make_apply,
    plan_hash_fn=init_plan_hash,
    serialize_plan=serialize_plan,
    serialize_report=serialize_report,
    token_kind=TOKEN_KIND_INIT,
    make_audit=_make_audit,
    make_gesture=_make_gesture,
)


def build_init_router() -> APIRouter:
    """Construct the INIT router (mount under the app's auth gate).

    The 4 preview/apply routes come from :func:`build_mutation_router`; the
    banner-gating ``GET /status`` (no token, no job — a cheap sentinel probe) is
    added onto the SAME prefixed router.
    """
    router = build_mutation_router(INIT_VERTICAL)

    @router.get("/status")
    async def status(request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        sheet = state.sheet_factory()
        return {"cutover_done": sheet.cutover_done()}

    return router


__all__ = [
    "PREFIX",
    "INIT_DEMOTE_THRESHOLD",
    "TOKEN_KIND_INIT",
    "INIT_VERTICAL",
    "init_plan_hash",
    "serialize_plan",
    "serialize_report",
    "build_init_router",
]
