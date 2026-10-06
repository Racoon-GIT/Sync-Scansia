"""DELETE vertical — IRREVERSIBLE outlet deletion, the multi-outlet exit and
deny-normalize on the register (plan ``docs/f7-mysql-adapter-plan.md`` §3.5),
behind the shared preview-job / apply-job / confirm-token machinery
(:mod:`backend.api.mutations`). Every route is auth-gated (``require_basic_auth``);
every job opens a register handle bound to the acting user (the name every audit
row carries) and closes it; every apply works under the pass lease in its service.

* ``POST /outlet/zero-stock`` + ``GET .../{job_id}`` — READ-ONLY recon
  (:func:`delete_service.zero_stock_candidates`): candidates + review buckets.
* ``POST /outlet/cleanup/preview`` + ``GET`` — the bulk cleanup's plan: the
  DRAFT-at-zero candidates, those whose SKU has an ``IN_SCANSIA`` pair listed apart
  (``protected``, never deleted), ``count`` / ``requires_second_confirm`` /
  ``plan_hash``, and a token bound to ``plan_hash#count#requires_second_confirm``.
* ``POST /outlet/cleanup/apply`` — the gestures enforced SYNCHRONOUSLY before any
  job: the token over ``{plan_hash}#{count}#{0|1}`` (a wrong count or a bad token →
  409), ``CONFERMO`` (→ 409), the SIGNED second confirm (→ 409); then a job re-plans
  live, drift-gates (``VERIFY_FAILED``) and calls
  :func:`~delete_service.cleanup_apply`.
* ``POST /outlet/delete/preview`` + ``GET`` — the single delete's step (0): the
  product SKUs' ``IN_SCANSIA`` pairs and the keys the confirm will hold; refused at
  once (409 ``single_delete_not_outlet``) for a target that is not an outlet.
* ``POST /outlet/delete/apply`` — ``CONFERMO`` + count 1 + the token (kind
  ``delete``) + the outlet gate, synchronously; then a job re-plans, drift-gates and
  calls :func:`~delete_service.delete_single_apply`.
* ``POST /outlet/exit/preview`` + ``GET`` / ``POST /outlet/exit/apply`` — the
  multi-outlet exit (:mod:`backend.services.exit_service`): the SKU and the outlet
  to keep; ``CONFERMO`` + the token (kind ``exit``).
* ``POST /outlet/deny-normalize/preview`` + ``GET`` / ``POST
  /outlet/deny-normalize/apply`` — rule 7's normalisation
  (:mod:`backend.services.normalize_service`) on the named outlets, or every
  outlet; ``CONFERMO`` + the token (kind ``normalize``).

Plans are not persisted server-side (Option A): the apply body re-submits the
preview's inputs and the live re-plan must hash as the confirmed one.
"""
from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from backend.api.errors import CODE_SINGLE_DELETE_NOT_OUTLET
from backend.api.jobs import JobBusyError
from backend.api.mutations import (
    CODE_CONFIRM_INVALID,
    CODE_GESTURE_REQUIRED,
    CODE_JOB_BUSY,
    DEFAULT_PREVIEW_TTL_S,
    _error,
    _run_apply_job,
    _run_preview_job,
    _run_readonly_job,
    poll_job,
    register_for,
)
from backend.auth.basic_auth import require_basic_auth
from backend.persistence.audit_sink import RegisterAuditSink
from backend.services import delete_service, exit_service, normalize_service
from backend.services.delete_service import (
    CONFIRM_WORD,
    DEFAULT_CLEANUP_THRESHOLD,
    SingleDeleteNotOutletError,
)

KIND_ZERO_STOCK = "outlet_zero_stock"
KIND_CLEANUP_PREVIEW = "outlet_cleanup_preview"
KIND_CLEANUP_APPLY = "outlet_cleanup_apply"
KIND_DELETE_PREVIEW = "outlet_delete_preview"
KIND_DELETE_SINGLE = "outlet_delete_single"
KIND_EXIT_PREVIEW = "outlet_exit_preview"
KIND_EXIT_APPLY = "outlet_exit_apply"
KIND_NORMALIZE_PREVIEW = "outlet_normalize_preview"
KIND_NORMALIZE_APPLY = "outlet_normalize_apply"

# Vertical ids SIGNED into the confirm-tokens: a token minted for one never
# verifies for another.
TOKEN_KIND_CLEANUP = "cleanup"
TOKEN_KIND_DELETE = "delete"
TOKEN_KIND_EXIT = "exit"
TOKEN_KIND_NORMALIZE = "normalize"


# =============================================================================
# Request bodies
# =============================================================================
class CleanupPreviewRequest(BaseModel):
    threshold: int = DEFAULT_CLEANUP_THRESHOLD
    archive_first: bool = False


class CleanupApplyRequest(BaseModel):
    plan_hash: str
    confirm_token: str
    confirm: Optional[str] = None            # human gesture word (must be CONFERMO)
    count: Optional[int] = None              # human gesture count (typed candidate count)
    threshold: int = DEFAULT_CLEANUP_THRESHOLD
    archive_first: bool = False
    second_confirm: bool = False


class DeletePreviewRequest(BaseModel):
    product_gid: str


class DeleteSingleRequest(BaseModel):
    product_gid: str
    plan_hash: str
    confirm_token: str
    confirm: Optional[str] = None
    count: Optional[int] = None              # must be 1 for a single delete


class ExitPreviewRequest(BaseModel):
    sku: str
    keep_product_gid: str


class ExitApplyRequest(ExitPreviewRequest):
    plan_hash: str
    confirm_token: str
    confirm: Optional[str] = None


class NormalizePreviewRequest(BaseModel):
    product_gids: List[str] = Field(default_factory=list)


class NormalizeApplyRequest(NormalizePreviewRequest):
    plan_hash: str
    confirm_token: str
    confirm: Optional[str] = None


# =============================================================================
# JSON-safe serializers (explicit; never raw inventory payloads)
# =============================================================================
def _review_to_dict(r: Any) -> Dict[str, Any]:
    return {"product_gid": r.product_gid, "title": r.title, "status": r.status, "reasons": list(r.reasons)}


def serialize_zero_stock(report: Any) -> Dict[str, Any]:
    return {
        "scanned": report.scanned,
        "in_stock": report.in_stock,
        "candidate_count": len(report.candidates),
        "candidates": [
            {"product_gid": c.product_gid, "title": c.title, "status": c.status, "variant_count": len(c.variants)}
            for c in report.candidates
        ],
        "review": [_review_to_dict(r) for r in report.review],
    }


def serialize_cleanup_plan(plan: Any) -> Dict[str, Any]:
    return {
        "dry_run": plan.dry_run,
        "count": plan.count,
        "threshold": plan.threshold,
        "archive_first": plan.archive_first,
        "requires_second_confirm": plan.requires_second_confirm,
        "plan_hash": plan.plan_hash,
        "candidates": [{"product_gid": c.product_gid, "title": c.title, "status": c.status} for c in plan.candidates],
        "review": [_review_to_dict(r) for r in plan.review],
        "protected": [_review_to_dict(r) for r in plan.protected],
    }


def serialize_delete_outcome(o: Any) -> Dict[str, Any]:
    return {
        "product_gid": o.product_gid, "status": o.status, "deleted_id": o.deleted_id,
        "warnings": list(o.warnings), "closed_rows": list(o.closed_rows),
        "held_keys": [list(k) for k in o.held_keys], "surfaced": list(o.surfaced),
    }


def serialize_cleanup_report(report: Any) -> Dict[str, Any]:
    return {
        "dry_run": report.dry_run,
        "verify_failed": report.verify_failed,
        "plan_hash": report.plan_hash,
        "deleted": report.deleted,
        "outcomes": [serialize_delete_outcome(o) for o in report.outcomes],
        "surfaced": list(report.surfaced),
    }


def serialize_single_plan(plan: Any) -> Dict[str, Any]:
    return {
        "dry_run": plan.dry_run, "product_gid": plan.product_gid, "title": plan.title, "status": plan.status,
        "skus": list(plan.skus), "shelf_pairs": list(plan.shelf_pairs), "keys": [list(k) for k in plan.keys],
        "open_rows": list(plan.open_rows), "plan_hash": plan.plan_hash,
    }


def serialize_exit_plan(plan: Any) -> Dict[str, Any]:
    return {
        "dry_run": plan.dry_run, "sku": plan.sku, "keep_gid": plan.keep_gid, "keep_title": plan.keep_title,
        "refused": plan.refused, "keys": [list(k) for k in plan.keys], "plan_hash": plan.plan_hash,
        "outlets": [{"product_gid": o.product_gid, "title": o.title, "status": o.status,
                     "lowerings": [asdict(x) for x in o.lowerings], "refusals": list(o.refusals)}
                    for o in plan.outlets],
    }


def serialize_exit_report(report: Any) -> Dict[str, Any]:
    out = asdict(report)
    out["keys_to_relevel"] = [list(k) for k in report.keys_to_relevel]
    return out


def serialize_normalize_plan(plan: Any) -> Dict[str, Any]:
    return {
        "dry_run": plan.dry_run, "scanned": plan.scanned, "plan_hash": plan.plan_hash,
        "products": [{"product_gid": p.product_gid, "title": p.title, "status": p.status,
                      "keys": [list(k) for k in p.keys], "unsafe_variant_ids": list(p.unsafe_variant_ids),
                      "custom_lowerings": [list(c) for c in p.custom_lowerings]} for p in plan.products],
        "review": [list(r) for r in plan.review],
    }


def serialize_normalize_report(report: Any) -> Dict[str, Any]:
    return asdict(report)


def _plan_hash(plan: Any) -> str:
    return plan.plan_hash


# =============================================================================
# Shared drivers
# =============================================================================
def _gesture_word(confirm: Optional[str]) -> Optional[Any]:
    if (confirm or "").strip() != CONFIRM_WORD:
        return _error(409, CODE_GESTURE_REQUIRED, f"Human gesture required: type {CONFIRM_WORD}.")
    return None


def _submit_preview(request: Request, kind: str, preview_fn: Any, serialize: Any, token_kind: str,
                    extra_fn: Any = None, token_binding_fn: Any = None) -> Any:
    state = request.app.state
    try:
        rec = state.job_store.create(kind)
    except JobBusyError:
        return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
    state.executor.submit(
        _run_preview_job, state.job_store, rec.job_id, register_for(state), state.transport_factory,
        preview_fn, _plan_hash, serialize, state.token_service, token_kind, DEFAULT_PREVIEW_TTL_S,
        extra_fn, token_binding_fn,
    )
    return {"job_id": rec.job_id, "status": rec.status}


def _submit_apply(request: Request, kind: str, actor: str, plan_hash: str, preview_fn: Any, apply_fn: Any,
                  serialize: Any, *, audit: bool = True) -> Any:
    state = request.app.state
    try:
        rec = state.job_store.create(kind)
    except JobBusyError:
        return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
    audit_builder = (lambda register: RegisterAuditSink(register, actor=actor)) if audit else None
    state.executor.submit(
        _run_apply_job, state.job_store, rec.job_id, plan_hash, register_for(state, actor),
        state.transport_factory, audit_builder, preview_fn, _plan_hash, apply_fn, serialize, None,
    )
    return {"job_id": rec.job_id, "status": rec.status}


# =============================================================================
# Router
# =============================================================================
def build_delete_router() -> APIRouter:
    """Construct the DELETE router (mount under the app's auth gate)."""
    router = APIRouter(tags=["delete"])

    # -- zero-stock recon (READ-ONLY job, no register) -------------------------
    @router.post("/outlet/zero-stock", status_code=202)
    async def zero_stock(request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        try:
            rec = state.job_store.create(KIND_ZERO_STOCK)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        promo = state.promo_location_id
        state.executor.submit(
            _run_readonly_job, state.job_store, rec.job_id, state.transport_factory,
            lambda transport: delete_service.zero_stock_candidates(transport, promo_location_id=promo),
            serialize_zero_stock,
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/outlet/zero-stock/{job_id}")
    async def get_zero_stock(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    # -- bulk cleanup ------------------------------------------------------------------
    @router.post("/outlet/cleanup/preview", status_code=202)
    async def cleanup_preview(request: Request, body: CleanupPreviewRequest,
                              actor: str = Depends(require_basic_auth)) -> Any:
        promo = request.app.state.promo_location_id
        th, af = body.threshold, body.archive_first

        def preview_fn(register: Any, transport: Any) -> Any:
            return delete_service.cleanup_preview(register, transport, promo_location_id=promo,
                                                  threshold=th, archive_first=af)

        return _submit_preview(
            request, KIND_CLEANUP_PREVIEW, preview_fn, serialize_cleanup_plan, TOKEN_KIND_CLEANUP,
            lambda p: {"count": p.count, "requires_second_confirm": p.requires_second_confirm},
            # the second-confirm decision is SIGNED at preview: a client-echoed
            # threshold can never skip it at apply
            lambda p: f"{p.plan_hash}#{p.count}#{int(p.requires_second_confirm)}",
        )

    @router.get("/outlet/cleanup/preview/{job_id}")
    async def get_cleanup_preview(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    @router.post("/outlet/cleanup/apply", status_code=202)
    async def cleanup_apply(request: Request, body: CleanupApplyRequest,
                            actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        if body.count is None:
            return _error(409, CODE_GESTURE_REQUIRED, "Human gesture required: type the candidate count.")
        base = f"{body.plan_hash}#{body.count}"
        if state.token_service.verify(body.confirm_token, f"{base}#1", kind=TOKEN_KIND_CLEANUP):
            gate_requires_second_confirm = True
        elif state.token_service.verify(body.confirm_token, f"{base}#0", kind=TOKEN_KIND_CLEANUP):
            gate_requires_second_confirm = False
        else:
            return _error(409, CODE_CONFIRM_INVALID, "Confirm token invalid or expired.")
        refused = _gesture_word(body.confirm)
        if refused is not None:
            return refused
        if gate_requires_second_confirm and not body.second_confirm:
            return _error(409, CODE_GESTURE_REQUIRED,
                          f"{body.count} candidates exceed the approved threshold: second_confirm required.")
        promo = state.promo_location_id
        th, af, sc = body.threshold, body.archive_first, body.second_confirm

        def preview_fn(register: Any, transport: Any) -> Any:
            return delete_service.cleanup_preview(register, transport, promo_location_id=promo,
                                                  threshold=th, archive_first=af)

        def apply_fn(register: Any, transport: Any, fresh_plan: Any, audit_sink: Any) -> Any:
            return delete_service.cleanup_apply(register, transport, audit_sink, fresh_plan,
                                                human_gesture=CONFIRM_WORD, promo_location_id=promo,
                                                second_confirm=sc)

        return _submit_apply(request, KIND_CLEANUP_APPLY, actor, body.plan_hash, preview_fn, apply_fn,
                             serialize_cleanup_report)

    @router.get("/outlet/cleanup/apply/{job_id}")
    async def get_cleanup_apply(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    # -- single delete -----------------------------------------------------------------
    def _not_outlet(state: Any, gid: str) -> Optional[Any]:
        # the escape hatch is for a botched CREATE (an outlet duplicate): a GID that
        # does not resolve to an outlet is refused before any job is reachable
        try:
            delete_service.require_single_delete_target_is_outlet(state.transport_factory(), gid)
        except SingleDeleteNotOutletError:
            return _error(409, CODE_SINGLE_DELETE_NOT_OUTLET, "Target does not resolve to an outlet product.")
        return None

    # plain defs: the outlet gate is a Shopify read, run in FastAPI's worker threads,
    # never on the event loop
    @router.post("/outlet/delete/preview", status_code=202)
    def delete_preview(request: Request, body: DeletePreviewRequest,
                       actor: str = Depends(require_basic_auth)) -> Any:
        gid = body.product_gid.strip()
        refused = _not_outlet(request.app.state, gid)
        if refused is not None:
            return refused

        def preview_fn(register: Any, transport: Any) -> Any:
            return delete_service.single_delete_preview(register, transport, gid)

        return _submit_preview(request, KIND_DELETE_PREVIEW, preview_fn, serialize_single_plan, TOKEN_KIND_DELETE)

    @router.get("/outlet/delete/preview/{job_id}")
    async def get_delete_preview(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    @router.post("/outlet/delete/apply", status_code=202)
    def delete_single(request: Request, body: DeleteSingleRequest,
                      actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        refused = _gesture_word(body.confirm)
        if refused is not None:
            return refused
        if body.count != 1:
            return _error(409, CODE_GESTURE_REQUIRED, "Human gesture required: count must be 1.")
        if not state.token_service.verify(body.confirm_token, body.plan_hash, kind=TOKEN_KIND_DELETE):
            return _error(409, CODE_CONFIRM_INVALID, "Confirm token invalid or expired.")
        gid = body.product_gid.strip()
        refused = _not_outlet(state, gid)
        if refused is not None:
            return refused
        promo = state.promo_location_id

        def preview_fn(register: Any, transport: Any) -> Any:
            return delete_service.single_delete_preview(register, transport, gid)

        def apply_fn(register: Any, transport: Any, fresh_plan: Any, audit_sink: Any) -> Any:
            return delete_service.delete_single_apply(register, transport, audit_sink, fresh_plan,
                                                      human_gesture=CONFIRM_WORD, promo_location_id=promo)

        return _submit_apply(request, KIND_DELETE_SINGLE, actor, body.plan_hash, preview_fn, apply_fn,
                             serialize_delete_outcome)

    @router.get("/outlet/delete/apply/{job_id}")
    async def get_delete_single(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    # -- the multi-outlet exit ---------------------------------------------------------
    @router.post("/outlet/exit/preview", status_code=202)
    async def exit_preview(request: Request, body: ExitPreviewRequest,
                           actor: str = Depends(require_basic_auth)) -> Any:
        promo = request.app.state.promo_location_id
        sku, keep = body.sku.strip(), body.keep_product_gid.strip()

        def preview_fn(register: Any, transport: Any) -> Any:
            return exit_service.exit_preview(register, transport, sku, keep, promo_location_id=promo)

        return _submit_preview(request, KIND_EXIT_PREVIEW, preview_fn, serialize_exit_plan, TOKEN_KIND_EXIT)

    @router.get("/outlet/exit/preview/{job_id}")
    async def get_exit_preview(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    @router.post("/outlet/exit/apply", status_code=202)
    async def exit_apply(request: Request, body: ExitApplyRequest, actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        refused = _gesture_word(body.confirm)
        if refused is not None:
            return refused
        if not state.token_service.verify(body.confirm_token, body.plan_hash, kind=TOKEN_KIND_EXIT):
            return _error(409, CODE_CONFIRM_INVALID, "Confirm token invalid or expired.")
        promo = state.promo_location_id
        sku, keep = body.sku.strip(), body.keep_product_gid.strip()

        def preview_fn(register: Any, transport: Any) -> Any:
            return exit_service.exit_preview(register, transport, sku, keep, promo_location_id=promo)

        def apply_fn(register: Any, transport: Any, fresh_plan: Any, audit_sink: Any) -> Any:
            return exit_service.exit_apply(register, transport, audit_sink, fresh_plan, promo_location_id=promo)

        return _submit_apply(request, KIND_EXIT_APPLY, actor, body.plan_hash, preview_fn, apply_fn,
                             serialize_exit_report)

    @router.get("/outlet/exit/apply/{job_id}")
    async def get_exit_apply(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    # -- deny-normalize (rule 7's normalisation) ---------------------------------------
    @router.post("/outlet/deny-normalize/preview", status_code=202)
    async def normalize_preview(request: Request, body: NormalizePreviewRequest,
                                actor: str = Depends(require_basic_auth)) -> Any:
        promo = request.app.state.promo_location_id
        gids = [g.strip() for g in body.product_gids if g and g.strip()]

        def preview_fn(register: Any, transport: Any) -> Any:
            return normalize_service.normalize_preview(register, transport, promo_location_id=promo,
                                                       product_gids=gids)

        return _submit_preview(request, KIND_NORMALIZE_PREVIEW, preview_fn, serialize_normalize_plan,
                               TOKEN_KIND_NORMALIZE)

    @router.get("/outlet/deny-normalize/preview/{job_id}")
    async def get_normalize_preview(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    @router.post("/outlet/deny-normalize/apply", status_code=202)
    async def normalize_apply(request: Request, body: NormalizeApplyRequest,
                              actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        refused = _gesture_word(body.confirm)
        if refused is not None:
            return refused
        if not state.token_service.verify(body.confirm_token, body.plan_hash, kind=TOKEN_KIND_NORMALIZE):
            return _error(409, CODE_CONFIRM_INVALID, "Confirm token invalid or expired.")
        promo = state.promo_location_id
        gids = [g.strip() for g in body.product_gids if g and g.strip()]

        def preview_fn(register: Any, transport: Any) -> Any:
            return normalize_service.normalize_preview(register, transport, promo_location_id=promo,
                                                       product_gids=gids)

        def apply_fn(register: Any, transport: Any, fresh_plan: Any, audit_sink: Any) -> Any:
            return normalize_service.normalize_apply(register, transport, fresh_plan, promo_location_id=promo)

        return _submit_apply(request, KIND_NORMALIZE_APPLY, actor, body.plan_hash, preview_fn, apply_fn,
                             serialize_normalize_report, audit=False)

    @router.get("/outlet/deny-normalize/apply/{job_id}")
    async def get_normalize_apply(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    return router


__all__ = [
    "build_delete_router",
    "serialize_zero_stock", "serialize_cleanup_plan", "serialize_cleanup_report", "serialize_delete_outcome",
    "serialize_single_plan", "serialize_exit_plan", "serialize_exit_report", "serialize_normalize_plan",
    "serialize_normalize_report",
    "CleanupPreviewRequest", "CleanupApplyRequest", "DeletePreviewRequest", "DeleteSingleRequest",
    "ExitPreviewRequest", "ExitApplyRequest", "NormalizePreviewRequest", "NormalizeApplyRequest",
    "TOKEN_KIND_CLEANUP", "TOKEN_KIND_DELETE", "TOKEN_KIND_EXIT", "TOKEN_KIND_NORMALIZE",
]
