"""PRICES vertical — price/discount maintenance behind the shared preview-job /
apply-job / confirm-token machinery (:mod:`backend.api.mutations`).

Wires :mod:`backend.services.pricing_service` to auth-gated routes:

* ``POST /prices/preview`` (body: ``mode`` + ``params``) + ``GET .../{job_id}`` —
  READ-ONLY plan job (:func:`~pricing_service.prices_preview`): a ``PricePlan`` +
  an aggregate ``plan_hash`` + a confirm-token bound to it.
* ``POST /prices/apply`` (body: ``plan_hash`` + ``confirm_token`` + the same
  ``mode``/``params``) — confirm-gated: a bad/expired token -> 409 (no job, no
  service call). Otherwise a job re-plans LIVE, drift-gates (``VERIFY_FAILED``),
  and calls :func:`~pricing_service.prices_apply`, which captures priors via the
  actor-bound audit sink BEFORE any push. Option A: ``mode``/``params`` are
  re-submitted (plans are not persisted server-side).
* ``POST /prices/revert`` (body: ``intent_id`` + ``confirm``) — confirm-gated
  MUTATION: ``confirm`` must equal ``CONFERMO`` (-> 409). A job re-pushes the
  captured priors via :func:`~pricing_service.revert_prices`.
* ``POST /prices/discharge-debt`` + ``GET .../{job_id}`` — READ-ONLY recon job
  (:func:`~pricing_service.discharge_debt_count`): live outlets failing price
  validation. (Exposed as an off-loop job, not a synchronous GET: it is a slow
  Shopify fan-out and must not block the event loop — same rationale as
  ``/scansia/inventory`` and ``/outlet/zero-stock``.)

The aggregate ``plan_hash`` is the order-independent sha256 (16 hex) over the
per-SKU ``PriceDiff.plan_hash`` values — the coarse batch drift gate on top of
``prices_apply``'s own per-diff TOCTOU re-verify. Secrets/GIDs are never logged.
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Mapping, Optional, Tuple

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from backend.api.jobs import JobBusyError
from backend.api.mutations import (
    CODE_CONFIRM_INVALID,
    CODE_GESTURE_REQUIRED,
    CODE_INVALID_MODE,
    CODE_JOB_BUSY,
    DEFAULT_PREVIEW_TTL_S,
    _error,
    _run_apply_job,
    _run_mutation_job,
    _run_preview_job,
    _run_readonly_job,
    poll_job,
)
from backend.auth.basic_auth import require_basic_auth
from backend.persistence.gsheet_audit import GSheetAuditSink
from backend.services import pricing_service
from backend.services.delete_service import CONFIRM_WORD
from backend.services.pricing_service import _MODES, BulkRule, PriceParams

KIND_PREVIEW = "prices_preview"
KIND_APPLY = "prices_apply"
KIND_REVERT = "prices_revert"
KIND_DEBT = "prices_discharge_debt"

# Vertical id SIGNED into the confirm-token (post-review HARDENING — see
# backend.persistence.tokens): binds the prices preview/apply token pair to
# THIS vertical only.
TOKEN_KIND_PRICES = "prices"


# =============================================================================
# Aggregate plan_hash — the batch-level TOCTOU key the confirm-token binds.
# =============================================================================
def prices_plan_hash(plan: Any) -> str:
    """Order-independent sha256 (16 hex) over the per-SKU ``PriceDiff.plan_hash``.

    Each diff already carries ``_plan_hash(gid, price, compare_at, status)`` binding
    its live target; aggregating the sorted ``sku:plan_hash`` pairs yields one batch
    key that changes iff any diff's target/live state (or the diff set) moved. The
    per-diff verify inside ``prices_apply`` is the fine gate on top of this.
    """
    parts = sorted(f"{d.sku}:{d.plan_hash}" for d in plan.diffs)
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


# =============================================================================
# Request bodies (params mirror pricing_service.PriceParams / BulkRule)
# =============================================================================
class BulkRuleModel(BaseModel):
    percent: float
    scope: Dict[str, List[str]] = Field(default_factory=dict)


class PriceParamsModel(BaseModel):
    percent_by_sku: Dict[str, float] = Field(default_factory=dict)
    price_by_sku: Dict[str, str] = Field(default_factory=dict)
    rules: List[BulkRuleModel] = Field(default_factory=list)
    override_percent_by_sku: Dict[str, float] = Field(default_factory=dict)


class PricePreviewRequest(BaseModel):
    mode: str
    params: PriceParamsModel = Field(default_factory=PriceParamsModel)
    row_override: bool = False
    status_override: bool = False


class PriceApplyRequest(BaseModel):
    plan_hash: str
    confirm_token: str
    # Echoed from the preview (Option A: plans are not persisted server-side); a
    # mismatch drifts the recomputed plan_hash -> VERIFY_FAILED.
    mode: str
    params: PriceParamsModel = Field(default_factory=PriceParamsModel)
    row_override: bool = False
    status_override: bool = False


class PriceRevertRequest(BaseModel):
    intent_id: str
    confirm: Optional[str] = None            # must equal CONFERMO


def _to_params(m: PriceParamsModel) -> PriceParams:
    """Convert the JSON body model into the frozen service dataclass (lists->tuples)."""
    rules: Tuple[BulkRule, ...] = tuple(
        BulkRule(percent=r.percent, scope={k: tuple(v) for k, v in r.scope.items()})
        for r in m.rules
    )
    return PriceParams(
        percent_by_sku=dict(m.percent_by_sku),
        price_by_sku=dict(m.price_by_sku),
        rules=rules,
        override_percent_by_sku=dict(m.override_percent_by_sku),
    )


# =============================================================================
# JSON-safe serializers
# =============================================================================
def _price_diff_to_dict(d: Any) -> Dict[str, Any]:
    return {
        "sku": d.sku,
        "product_gid": d.product_gid,
        "status": d.status,
        "actionable": d.actionable,
        "price": d.price,
        "compare_at": d.compare_at,
        "percent": d.percent,
        "sheet_price": d.sheet_price,
        "sheet_changed": d.sheet_changed,
        "live_price": d.live_price,
        "live_compare_at": d.live_compare_at,
        "live_changed": d.live_changed,
        "live_status": d.live_status,
        "row_uuids": list(d.row_uuids),
        "warnings": list(d.warnings),
        "plan_hash": d.plan_hash,
    }


def serialize_price_plan(plan: Any) -> Dict[str, Any]:
    return {
        "dry_run": plan.dry_run,
        "mode": plan.mode,
        "diffs": [_price_diff_to_dict(d) for d in plan.diffs],
        "anomalies": list(plan.anomalies),
    }


def _price_outcome_to_dict(o: Any) -> Dict[str, Any]:
    return {"sku": o.sku, "product_gid": o.product_gid, "status": o.status,
            "warnings": list(o.warnings)}


def serialize_price_report(report: Any) -> Dict[str, Any]:
    return {"intent_id": report.intent_id,
            "outcomes": [_price_outcome_to_dict(o) for o in report.outcomes]}


def serialize_revert_report(report: Any) -> Dict[str, Any]:
    return {
        "intent_id": report.intent_id,
        "reverted_products": report.reverted_products,
        "reverted_variants": report.reverted_variants,
        "outcomes": [_price_outcome_to_dict(o) for o in report.outcomes],
    }


def serialize_debt_report(report: Any) -> Dict[str, Any]:
    return {
        "scanned_products": report.scanned_products,
        "broken_products": report.broken_products,
        "broken_variants": report.broken_variants,
        "broken_gids": list(report.broken_gids),
    }


# =============================================================================
# Router
# =============================================================================
def build_prices_router() -> APIRouter:
    """Construct the PRICES router (mount under the app's auth gate)."""
    router = APIRouter(tags=["prices"])

    # -- preview (READ-ONLY plan job) ---------------------------------------
    @router.post("/prices/preview", status_code=202)
    async def preview(request: Request, body: PricePreviewRequest,
                      actor: str = Depends(require_basic_auth)) -> Any:
        if body.mode not in _MODES:
            return _error(422, CODE_INVALID_MODE, "Unknown price mode.")
        state = request.app.state
        try:
            rec = state.job_store.create(KIND_PREVIEW)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        mode, params = body.mode, _to_params(body.params)
        ro, so = body.row_override, body.status_override

        def preview_fn(sheet: Any, transport: Any) -> Any:
            return pricing_service.prices_preview(
                sheet, transport, mode, params, row_override=ro, status_override=so)

        state.executor.submit(
            _run_preview_job, state.job_store, rec.job_id,
            state.sheet_factory, state.transport_factory,
            preview_fn, prices_plan_hash, serialize_price_plan,
            state.token_service, TOKEN_KIND_PRICES, DEFAULT_PREVIEW_TTL_S, None,
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/prices/preview/{job_id}")
    async def get_preview(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    # -- apply (confirm-gated) ----------------------------------------------
    @router.post("/prices/apply", status_code=202)
    async def apply(request: Request, body: PriceApplyRequest,
                    actor: str = Depends(require_basic_auth)) -> Any:
        if body.mode not in _MODES:
            return _error(422, CODE_INVALID_MODE, "Unknown price mode.")
        state = request.app.state
        # Synchronous confirm gate — bad/expired token -> 409, no job, no service call.
        if not state.token_service.verify(body.confirm_token, body.plan_hash, kind=TOKEN_KIND_PRICES):
            return _error(409, CODE_CONFIRM_INVALID, "Confirm token invalid or expired.")
        try:
            rec = state.job_store.create(KIND_APPLY)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        mode, params = body.mode, _to_params(body.params)
        ro, so = body.row_override, body.status_override

        def preview_fn(sheet: Any, transport: Any) -> Any:
            return pricing_service.prices_preview(
                sheet, transport, mode, params, row_override=ro, status_override=so)

        def apply_fn(sheet: Any, transport: Any, fresh_plan: Any, audit_sink: Any) -> Any:
            return pricing_service.prices_apply(
                sheet, transport, mode, params, fresh_plan, audit_sink,
                row_override=ro, status_override=so)

        audit_builder = lambda sheet: GSheetAuditSink.from_scansia_sheet(sheet, actor=actor)
        state.executor.submit(
            _run_apply_job, state.job_store, rec.job_id,
            body.plan_hash, state.sheet_factory, state.transport_factory,
            audit_builder, preview_fn, prices_plan_hash, apply_fn,
            serialize_price_report, None,               # prices has no human-gesture gate
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/prices/apply/{job_id}")
    async def get_apply(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    # -- revert (confirm-gated MUTATION) ------------------------------------
    @router.post("/prices/revert", status_code=202)
    async def revert(request: Request, body: PriceRevertRequest,
                     actor: str = Depends(require_basic_auth)) -> Any:
        if (body.confirm or "").strip() != CONFIRM_WORD:
            return _error(409, CODE_GESTURE_REQUIRED, f"Human gesture required: type {CONFIRM_WORD}.")
        state = request.app.state
        try:
            rec = state.job_store.create(KIND_REVERT)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        intent_id = body.intent_id

        def mutate_fn(sheet: Any, transport: Any, audit_sink: Any) -> Any:
            return pricing_service.revert_prices(transport, audit_sink, intent_id)

        audit_builder = lambda sheet: GSheetAuditSink.from_scansia_sheet(sheet, actor=actor)
        state.executor.submit(
            _run_mutation_job, state.job_store, rec.job_id,
            state.sheet_factory, state.transport_factory, audit_builder,
            mutate_fn, serialize_revert_report,
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/prices/revert/{job_id}")
    async def get_revert(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    # -- discharge-debt recon (READ-ONLY job) -------------------------------
    @router.post("/prices/discharge-debt", status_code=202)
    async def discharge_debt(request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        try:
            rec = state.job_store.create(KIND_DEBT)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        state.executor.submit(
            _run_readonly_job, state.job_store, rec.job_id, state.transport_factory,
            lambda transport: pricing_service.discharge_debt_count(transport),
            serialize_debt_report,
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/prices/discharge-debt/{job_id}")
    async def get_discharge_debt(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    return router


__all__ = [
    "build_prices_router",
    "prices_plan_hash",
    "serialize_price_plan",
    "serialize_price_report",
    "serialize_revert_report",
    "serialize_debt_report",
    "PricePreviewRequest",
    "PriceApplyRequest",
    "PriceRevertRequest",
]
