"""DELETE vertical — IRREVERSIBLE outlet cleanup / single-delete behind the shared
preview-job / apply-job / confirm-token machinery (:mod:`backend.api.mutations`).

Five surfaces, all auth-gated (``require_basic_auth``); the authenticated username
is the audit actor, captured into the per-request :class:`GSheetAuditSink`:

* ``POST /outlet/zero-stock`` + ``GET .../{job_id}`` — READ-ONLY recon job
  (:func:`delete_service.zero_stock_candidates`): candidates + review buckets. No
  token, no mutation.
* ``POST /outlet/cleanup/preview`` + ``GET .../{job_id}`` — READ-ONLY plan job
  (:func:`~delete_service.cleanup_preview`): a :class:`CleanupPlan` with
  ``count`` / ``requires_second_confirm`` / ``plan_hash`` + a confirm-token bound
  to the composite ``plan_hash#count#requires_second_confirm`` (so a mistyped
  count fails the apply token check synchronously, AND the second_confirm
  requirement itself is SIGNED — post-review HARDENING, see ``cleanup_apply``).
* ``POST /outlet/cleanup/apply`` — confirm-gated + human-gesture-gated. The gesture
  is enforced SYNCHRONOUSLY before any job/mutation: the token must verify over
  ``{plan_hash}#{count}#{0|1}`` (a wrong count OR a bad/expired token -> 409), the
  typed ``confirm`` must equal ``CONFERMO`` (-> 409), and a SIGNED
  ``requires_second_confirm=1`` needs ``second_confirm`` (-> 409) — an
  operator-resubmitted ``threshold`` can no longer silently skip this speed-bump,
  since the decision was signed at preview time, not re-derived from the
  client-echoed value. Only then is a job enqueued that re-plans LIVE,
  drift-gates (``VERIFY_FAILED``), and calls :func:`~delete_service.cleanup_apply`
  — which owns the mandatory snapshot->write_durable abort gate BEFORE any
  ``productDelete``. The endpoint NEVER deletes; the service does, behind its gate.
* ``POST /outlet/delete/apply`` — single hard-delete (a botched CREATE). Same
  ``CONFERMO`` + ``count == 1`` synchronous gesture, PLUS a HARDENING gate
  (post-review): the target GID must resolve to an OUTLET (collection membership
  or ``*Outlet*`` title — :func:`delete_service.resolve_is_outlet`) BEFORE any
  job/``productDelete`` is reachable, else 409 ``single_delete_not_outlet`` — a
  mistyped GID can no longer delete an arbitrary non-outlet product. The
  service's snapshot gate still applies. No live predicate re-check beyond the
  outlet-membership gate (by design — a crashed CREATE may hold inherited stock,
  which is exactly what is being removed).
* ``POST /outlet/deny-normalize`` — the gated DENY-normalization mutation
  (``CONFERMO`` gesture), unblocking legacy CONTINUE outlets.

Secrets (the signing secret behind the token, the Basic-Auth actor) come from the
environment / the auth dependency and are NEVER logged; snapshots/GIDs never leave
the service+sink boundary.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from backend.api.errors import CODE_SINGLE_DELETE_NOT_OUTLET
from backend.api.jobs import JobBusyError
from backend.api.mutations import (
    CODE_CONFIRM_INVALID,
    CODE_GESTURE_REQUIRED,
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
from backend.services import delete_service
from backend.services.delete_service import (
    CONFIRM_WORD,
    DEFAULT_CLEANUP_THRESHOLD,
    SingleDeleteNotOutletError,
)

KIND_ZERO_STOCK = "outlet_zero_stock"
KIND_CLEANUP_PREVIEW = "outlet_cleanup_preview"
KIND_CLEANUP_APPLY = "outlet_cleanup_apply"
KIND_DELETE_SINGLE = "outlet_delete_single"
KIND_DENY = "outlet_deny_normalize"

# Vertical id SIGNED into the confirm-token (post-review HARDENING — see
# backend.persistence.tokens): binds the cleanup preview/apply token pair to
# THIS vertical only (a token minted here can never verify for publish/prices).
TOKEN_KIND_CLEANUP = "cleanup"


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
    # Echoed from the preview (Option A: plans are not persisted server-side); a
    # mismatch vs the preview is caught by the live drift gate (plan_hash binds them).
    threshold: int = DEFAULT_CLEANUP_THRESHOLD
    archive_first: bool = False
    second_confirm: bool = False


class DeleteSingleRequest(BaseModel):
    product_gid: str
    confirm: Optional[str] = None
    count: Optional[int] = None              # must be 1 for a single delete


class DenyNormalizeRequest(BaseModel):
    product_gid: str
    confirm: Optional[str] = None


# =============================================================================
# JSON-safe serializers (explicit; never dump raw inventory payloads / GIDs blindly)
# =============================================================================
def _review_to_dict(r: Any) -> Dict[str, Any]:
    return {"product_gid": r.product_gid, "title": r.title, "status": r.status,
            "reasons": list(r.reasons)}


def serialize_zero_stock(report: Any) -> Dict[str, Any]:
    return {
        "scanned": report.scanned,
        "in_stock": report.in_stock,
        "candidate_count": len(report.candidates),
        "candidates": [
            {"product_gid": c.product_gid, "title": c.title, "status": c.status,
             "variant_count": len(c.variants)}
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
        "candidates": [
            {"product_gid": c.product_gid, "title": c.title, "status": c.status}
            for c in plan.candidates
        ],
        "review": [_review_to_dict(r) for r in plan.review],
    }


def _delete_outcome_to_dict(o: Any) -> Dict[str, Any]:
    return {"product_gid": o.product_gid, "status": o.status,
            "deleted_id": o.deleted_id, "warnings": list(o.warnings)}


def serialize_cleanup_report(report: Any) -> Dict[str, Any]:
    return {
        "dry_run": report.dry_run,
        "verify_failed": report.verify_failed,
        "plan_hash": report.plan_hash,
        "deleted": report.deleted,
        "outcomes": [_delete_outcome_to_dict(o) for o in report.outcomes],
    }


# =============================================================================
# Router
# =============================================================================
def build_delete_router() -> APIRouter:
    """Construct the DELETE router (mount under the app's auth gate)."""
    router = APIRouter(tags=["delete"])

    # -- zero-stock recon (READ-ONLY job) -----------------------------------
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

    # -- cleanup preview (READ-ONLY plan job, token bound to plan_hash#count) -
    @router.post("/outlet/cleanup/preview", status_code=202)
    async def cleanup_preview(request: Request, body: CleanupPreviewRequest,
                              actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        try:
            rec = state.job_store.create(KIND_CLEANUP_PREVIEW)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        promo = state.promo_location_id
        th, af = body.threshold, body.archive_first

        def preview_fn(sheet: Any, transport: Any) -> Any:
            return delete_service.cleanup_preview(
                transport, promo_location_id=promo, threshold=th, archive_first=af)

        state.executor.submit(
            _run_preview_job, state.job_store, rec.job_id,
            state.sheet_factory, state.transport_factory,
            preview_fn,
            lambda p: p.plan_hash,                      # stored (raw) TOCTOU key
            serialize_cleanup_plan,
            state.token_service, TOKEN_KIND_CLEANUP, DEFAULT_PREVIEW_TTL_S,
            lambda p: {"count": p.count, "requires_second_confirm": p.requires_second_confirm},
            # HARDENING (post-review): SIGN requires_second_confirm into the
            # token too — an operator cannot inflate `threshold` at apply time to
            # silently skip the speed-bump; the decision is bound to what was
            # ACTUALLY reviewed at preview, not re-derived from a client-echoed
            # threshold (see cleanup_apply below).
            lambda p: f"{p.plan_hash}#{p.count}#{int(p.requires_second_confirm)}",
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/outlet/cleanup/preview/{job_id}")
    async def get_cleanup_preview(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    # -- cleanup apply (confirm + human-gesture gated) ----------------------
    @router.post("/outlet/cleanup/apply", status_code=202)
    async def cleanup_apply(request: Request, body: CleanupApplyRequest,
                            actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        # (1) count is part of the token binding -> its absence is a gesture failure.
        if body.count is None:
            return _error(409, CODE_GESTURE_REQUIRED,
                          "Human gesture required: type the candidate count.")
        # (2) token gate over plan_hash#count#requires_second_confirm: a wrong
        #     count OR a bad/expired token both fail here -> 409, NO job,
        #     cleanup_apply NEVER called. HARDENING (post-review): the
        #     second_confirm REQUIREMENT is SIGNED at preview time (try both
        #     signed outcomes) — an operator-resubmitted `threshold` can no
        #     longer silently skip the speed-bump by claiming a looser cap than
        #     what was actually reviewed at preview (the backstop
        #     CLEANUP_HARD_CAP is already folded into the signed decision via
        #     cleanup_preview's own _effective_cap).
        base = f"{body.plan_hash}#{body.count}"
        if state.token_service.verify(body.confirm_token, f"{base}#1", kind=TOKEN_KIND_CLEANUP):
            gate_requires_second_confirm = True
        elif state.token_service.verify(body.confirm_token, f"{base}#0", kind=TOKEN_KIND_CLEANUP):
            gate_requires_second_confirm = False
        else:
            return _error(409, CODE_CONFIRM_INVALID, "Confirm token invalid or expired.")
        # (3) typed confirmation word.
        if (body.confirm or "").strip() != CONFIRM_WORD:
            return _error(409, CODE_GESTURE_REQUIRED, f"Human gesture required: type {CONFIRM_WORD}.")
        # (4) over-cap count (per the SIGNED preview decision) demands a second
        #     confirmation.
        if gate_requires_second_confirm and not body.second_confirm:
            return _error(409, CODE_GESTURE_REQUIRED,
                          f"{body.count} candidates exceed the approved threshold: "
                          f"second_confirm required.")
        # (5) all gestures satisfied -> enqueue the apply job.
        try:
            rec = state.job_store.create(KIND_CLEANUP_APPLY)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        promo = state.promo_location_id
        th, af, sc = body.threshold, body.archive_first, body.second_confirm

        def preview_fn(sheet: Any, transport: Any) -> Any:
            return delete_service.cleanup_preview(
                transport, promo_location_id=promo, threshold=th, archive_first=af)

        def apply_fn(sheet: Any, transport: Any, fresh_plan: Any, audit_sink: Any) -> Any:
            # The service owns the snapshot->write_durable abort gate + its own live
            # re-verify; the endpoint never issues a productDelete itself.
            return delete_service.cleanup_apply(
                transport, sheet, audit_sink, fresh_plan,
                human_gesture=CONFIRM_WORD, promo_location_id=promo, second_confirm=sc)

        audit_builder = lambda sheet: GSheetAuditSink.from_scansia_sheet(sheet, actor=actor)
        state.executor.submit(
            _run_apply_job, state.job_store, rec.job_id,
            body.plan_hash, state.sheet_factory, state.transport_factory,
            audit_builder, preview_fn, (lambda p: p.plan_hash), apply_fn,
            serialize_cleanup_report, None,             # gesture already gated synchronously
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/outlet/cleanup/apply/{job_id}")
    async def get_cleanup_apply(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    # -- single delete (confirm + count==1 gated) ---------------------------
    @router.post("/outlet/delete/apply", status_code=202)
    async def delete_single(request: Request, body: DeleteSingleRequest,
                            actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        if (body.confirm or "").strip() != CONFIRM_WORD:
            return _error(409, CODE_GESTURE_REQUIRED, f"Human gesture required: type {CONFIRM_WORD}.")
        if body.count != 1:
            return _error(409, CODE_GESTURE_REQUIRED, "Human gesture required: count must be 1.")
        # HARDENING (post-review): a mistyped GID must resolve to an OUTLET
        # BEFORE any job/product_delete is reachable — the escape hatch is for a
        # botched CREATE (which IS an outlet duplicate), never an arbitrary
        # product. Belt-and-braces: the SAME gate also runs inside
        # delete_single_apply itself.
        transport = state.transport_factory()
        try:
            delete_service.require_single_delete_target_is_outlet(transport, body.product_gid)
        except SingleDeleteNotOutletError:
            return _error(409, CODE_SINGLE_DELETE_NOT_OUTLET,
                          "Target does not resolve to an outlet product.")
        try:
            rec = state.job_store.create(KIND_DELETE_SINGLE)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        gid = body.product_gid

        def mutate_fn(sheet: Any, transport: Any, audit_sink: Any) -> Any:
            return delete_service.delete_single_apply(
                transport, sheet, audit_sink, gid, human_gesture=CONFIRM_WORD)

        audit_builder = lambda sheet: GSheetAuditSink.from_scansia_sheet(sheet, actor=actor)
        state.executor.submit(
            _run_mutation_job, state.job_store, rec.job_id,
            state.sheet_factory, state.transport_factory, audit_builder,
            mutate_fn, _delete_outcome_to_dict,
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/outlet/delete/apply/{job_id}")
    async def get_delete_single(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    # -- deny-normalize (confirm-gated mutation, no audit sink) --------------
    @router.post("/outlet/deny-normalize", status_code=202)
    async def deny_normalize(request: Request, body: DenyNormalizeRequest,
                             actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        if (body.confirm or "").strip() != CONFIRM_WORD:
            return _error(409, CODE_GESTURE_REQUIRED, f"Human gesture required: type {CONFIRM_WORD}.")
        try:
            rec = state.job_store.create(KIND_DENY)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        gid = body.product_gid

        def mutate_fn(sheet: Any, transport: Any, audit_sink: Any) -> Any:
            return delete_service.deny_normalize(transport, gid)

        state.executor.submit(
            _run_mutation_job, state.job_store, rec.job_id,
            state.sheet_factory, state.transport_factory, None,   # no audit sink
            mutate_fn, lambda n: {"normalized": n},
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/outlet/deny-normalize/{job_id}")
    async def get_deny_normalize(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    return router


__all__ = [
    "build_delete_router",
    "serialize_zero_stock",
    "serialize_cleanup_plan",
    "serialize_cleanup_report",
    "CleanupPreviewRequest",
    "CleanupApplyRequest",
    "DeleteSingleRequest",
    "DenyNormalizeRequest",
]
