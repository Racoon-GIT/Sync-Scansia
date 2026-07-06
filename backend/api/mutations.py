"""Shared PREVIEW-job / APPLY-job / confirm-token pattern for the mutating verticals.

This is the ONE place the three confirm-gated verticals (publish / delete / prices)
share their HTTP mechanics; each vertical supplies only its service closures +
serializers via a :class:`MutationVertical` spec and reuses everything here. It
imports FastAPI, so — like ``backend.api.read`` — it loads ONLY where the web deps
are installed (Render / CI); the compute core it drives has none.

The contract (per the approved M3-M5 design):

* **PREVIEW** is a READ-ONLY background job (single-slot ``JobStore`` + off-loop
  ``executor.submit``, exactly like the inventory join): it runs ``*_preview``
  (NO mutation), derives an aggregate ``plan_hash`` from the returned plan, mints a
  short-TTL HMAC confirm-token bound to that hash, and stores
  ``{plan, plan_hash, confirm_token, ...}`` as the job result. The token doubles as
  the CSRF token for the matching apply.

* **APPLY** is confirm-gated + off-loop. The POST handler FIRST verifies the
  submitted ``confirm_token`` against the submitted ``plan_hash`` (a pure, fast
  HMAC check): a bad/expired token -> HTTP 409 ``confirm_invalid`` with NO job
  created and NO service call. Only then is a job enqueued that (1) RE-RUNS
  ``*_preview`` against LIVE state -> ``fresh_plan``; (2) if
  ``plan_hash(fresh_plan) != submitted_plan_hash`` the world moved between preview
  and apply -> the job resolves ``VERIFY_FAILED`` and the service ``*_apply`` is
  NEVER called; (3) for verticals that require it, a human-gesture gate runs
  against ``fresh_plan``; (4) only then ``*_apply(fresh_plan, audit_sink=...)`` runs
  and its report is serialized into the job result. Recompute-at-apply is coherent
  with the sheet-centric persistence (plans are not durably stored).

Single-slot: preview and apply share the same ``JobStore`` slot (one operation at a
time) — correct for the single-operator tool. Secrets (the signing secret behind
the token, the Basic-Auth actor) come from the environment / the auth dependency
and are NEVER logged: the only log line on failure carries the job id, the
exception TYPE name, and the stable ``error_code`` — never a token, a plan_hash, a
GID, or a raw message.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.api.jobs import JobBusyError, JobStore, job_record_to_dict
from backend.auth.basic_auth import require_basic_auth

logger = logging.getLogger("backend.api.mutations")

# Confirm-token TTL: short by design (replay is further defused by the apply-time
# TOCTOU re-resolution — see ``backend.persistence.tokens``). 15 minutes covers a
# human preview -> review -> approve -> apply round-trip without being replay-generous.
DEFAULT_PREVIEW_TTL_S = 900

# Stable, machine-parseable control-flow codes (part of the API contract). These
# are DELIBERATE responses, not exceptions, so they never pass through the error
# boundary; each carries a FIXED, secret-free message.
CODE_CONFIRM_INVALID = "confirm_invalid"
CODE_JOB_BUSY = "job_busy"
# Human-gesture gate failures (delete vertical; prices revert). Synchronous 409.
CODE_GESTURE_REQUIRED = "gesture_required"
# Prices: an unknown price mode is a client input error, rejected synchronously.
CODE_INVALID_MODE = "invalid_mode"

# Apply-job result statuses (stored in the job result, surfaced on poll).
APPLY_STATUS_APPLIED = "APPLIED"
APPLY_STATUS_VERIFY_FAILED = "VERIFY_FAILED"


# =============================================================================
# Request body (shared) — publish uses only plan_hash + confirm_token; the
# gesture fields serve the delete vertical (ignored where unused).
# =============================================================================
class ApplyRequest(BaseModel):
    plan_hash: str
    confirm_token: str
    # Human-gesture fields (delete vertical). Optional so publish/prices ignore them.
    confirm: Optional[str] = None
    count: Optional[int] = None
    second_confirm: bool = False


# =============================================================================
# Vertical spec — the ONLY per-vertical surface. Everything is a plain callable so
# the spec is pure data + closures (no FastAPI knowledge leaks into the services).
# =============================================================================
@dataclass(frozen=True)
class MutationVertical:
    """Per-vertical wiring for :func:`build_mutation_router`.

    ``make_preview`` / ``make_apply`` are FACTORIES over ``app.state`` so the
    service kwargs (promo location, mode, params, ...) are closed over per-request
    without this module knowing any service signature:

    * ``make_preview(state) -> (sheet, transport) -> plan``
    * ``make_apply(state)   -> (sheet, transport, approved_plan, audit_sink) -> report``

    ``plan_hash_fn(plan) -> str`` derives the aggregate TOCTOU key (the token is
    bound to it and the drift gate compares it). ``serialize_plan`` /
    ``serialize_report`` return JSON-safe dicts for the job result.

    Optional hooks (unused by publish):

    * ``make_audit(state, actor) -> (sheet) -> audit_sink`` — per-request sink for
      verticals that write audit (delete/prices); the actor is captured here.
    * ``make_gesture(body) -> (fresh_plan) -> Optional[str]`` — human-gesture gate;
      returns a stable failure code (job result status) or ``None`` when satisfied.
    * ``extra_fn(plan) -> dict`` — extra preview-result fields (e.g.
      ``requires_second_confirm`` for delete).
    """

    prefix: str
    preview_kind: str
    apply_kind: str
    make_preview: Callable[[Any], Callable[[Any, Any], Any]]
    make_apply: Callable[[Any], Callable[[Any, Any, Any, Any], Any]]
    plan_hash_fn: Callable[[Any], str]
    serialize_plan: Callable[[Any], Any]
    serialize_report: Callable[[Any], Any]
    # The vertical's id, SIGNED into the confirm-token (post-review HARDENING —
    # see backend.persistence.tokens): required, no default, so every vertical
    # must state its own (never inherit an accidental empty/shared value).
    token_kind: str
    ttl_s: int = DEFAULT_PREVIEW_TTL_S
    extra_fn: Optional[Callable[[Any], Dict[str, Any]]] = None
    make_audit: Optional[Callable[[Any, str], Callable[[Any], Any]]] = None
    make_gesture: Optional[Callable[[ApplyRequest], Callable[[Any], Optional[str]]]] = None


# =============================================================================
# Off-loop workers (PURE — no FastAPI; run inside app.state.executor). Each has a
# top-level guard that records JOB_FAILED with a STABLE error_code via the boundary
# mapper — never a raw message, never a propagated crash.
# =============================================================================
def _run_preview_job(
    store: JobStore,
    job_id: str,
    sheet_factory: Callable[[], Any],
    transport_factory: Callable[[], Any],
    preview_fn: Callable[[Any, Any], Any],
    plan_hash_fn: Callable[[Any], str],
    serialize_plan: Callable[[Any], Any],
    token_service: Any,
    token_kind: str,
    ttl_s: int,
    extra_fn: Optional[Callable[[Any], Dict[str, Any]]],
    token_binding_fn: Optional[Callable[[Any], str]] = None,
) -> None:
    """Worker: build collaborators, run the READ-ONLY preview, mint the token.

    ``token_binding_fn`` (optional) decouples what the confirm-token BINDS from the
    ``plan_hash`` stored in the result: by default the token binds the stored
    ``plan_hash`` (publish/prices), but the delete vertical binds the composite
    ``plan_hash#count`` so a mistyped candidate count fails the apply token check
    synchronously (409) while the stored/echoed ``plan_hash`` stays the raw
    TOCTOU key the drift gate compares. The stored ``plan_hash`` is ALWAYS
    ``plan_hash_fn(plan)`` — never the (possibly composite) binding.
    """
    from backend.api.errors import map_exception  # local import: keep module light

    store.mark_running(job_id)
    try:
        sheet = sheet_factory()
        transport = transport_factory()
        plan = preview_fn(sheet, transport)  # READ-ONLY: no mutation
        plan_hash = plan_hash_fn(plan)
        token_hash = token_binding_fn(plan) if token_binding_fn is not None else plan_hash
        # mint fail-closed on a missing signing secret -> ConfigError -> stable code.
        # kind (post-review HARDENING) binds the token to THIS vertical only.
        confirm_token = token_service.mint(token_hash, ttl_s, kind=token_kind)
        result: Dict[str, Any] = {
            "plan": serialize_plan(plan),
            "plan_hash": plan_hash,
            "confirm_token": confirm_token,
        }
        if extra_fn is not None:
            result.update(extra_fn(plan))
        store.mark_done(job_id, result)
    except Exception as e:  # noqa: BLE001 - top-level job guard; bounded code only
        err = map_exception(e)
        logger.error("preview job %s failed: %s -> %s", job_id, type(e).__name__, err.error_code)
        store.mark_failed(job_id, err.error_code)


def _run_apply_job(
    store: JobStore,
    job_id: str,
    submitted_plan_hash: str,
    sheet_factory: Callable[[], Any],
    transport_factory: Callable[[], Any],
    audit_builder: Optional[Callable[[Any], Any]],
    preview_fn: Callable[[Any, Any], Any],
    plan_hash_fn: Callable[[Any], str],
    apply_fn: Callable[[Any, Any, Any, Any], Any],
    serialize_report: Callable[[Any], Any],
    gesture_check: Optional[Callable[[Any], Optional[str]]],
) -> None:
    """Worker: re-preview LIVE, drift-gate, gesture-gate, then apply.

    The service ``*_apply`` is reached ONLY when the fresh plan_hash still equals
    the submitted one (no drift) AND any gesture gate is satisfied — so a stale or
    unconfirmed apply mutates nothing.
    """
    from backend.api.errors import map_exception  # local import: keep module light

    store.mark_running(job_id)
    try:
        sheet = sheet_factory()
        transport = transport_factory()
        # (1) recompute the plan against LIVE state (read-only).
        fresh_plan = preview_fn(sheet, transport)
        fresh_hash = plan_hash_fn(fresh_plan)
        # (2) drift gate: the world changed between preview and apply -> abort clean.
        if fresh_hash != submitted_plan_hash:
            store.mark_done(job_id, {
                "status": APPLY_STATUS_VERIFY_FAILED,
                "applied": False,
                "reason": "plan_hash_mismatch:live_state_changed",
                "plan_hash": fresh_hash,
            })
            return
        # (3) human-gesture gate (delete vertical) — checked against the FRESH plan.
        if gesture_check is not None:
            code = gesture_check(fresh_plan)
            if code is not None:
                store.mark_done(job_id, {"status": code, "applied": False, "plan_hash": fresh_hash})
                return
        # (4) apply. Build the per-request audit sink (actor-bound) only where the
        # vertical writes audit; publish passes no builder (audit_sink=None).
        audit_sink = audit_builder(sheet) if audit_builder is not None else None
        report = apply_fn(sheet, transport, fresh_plan, audit_sink)
        store.mark_done(job_id, {
            "status": APPLY_STATUS_APPLIED,
            "applied": True,
            "plan_hash": fresh_hash,
            "report": serialize_report(report),
        })
    except Exception as e:  # noqa: BLE001 - top-level job guard; bounded code only
        err = map_exception(e)
        logger.error("apply job %s failed: %s -> %s", job_id, type(e).__name__, err.error_code)
        store.mark_failed(job_id, err.error_code)


def _run_readonly_job(
    store: JobStore,
    job_id: str,
    transport_factory: Callable[[], Any],
    work_fn: Callable[[Any], Any],
    serialize_fn: Callable[[Any], Any],
) -> None:
    """Worker: a slow READ-ONLY Shopify recon (zero-stock / discharge-debt).

    Mints NO token (nothing to apply), so it runs even when the signing secret is
    unset. Builds only the transport (these recons never touch the sheet). Same
    top-level guard: a whole-job failure -> ``JOB_FAILED`` with a STABLE code (a
    missing Promo anchor -> ``promo_anchor_missing``, etc.), never a raw message.
    """
    from backend.api.errors import map_exception  # local import: keep module light

    store.mark_running(job_id)
    try:
        transport = transport_factory()
        result = work_fn(transport)  # READ-ONLY: no mutation
        store.mark_done(job_id, serialize_fn(result))
    except Exception as e:  # noqa: BLE001 - top-level job guard; bounded code only
        err = map_exception(e)
        logger.error("readonly job %s failed: %s -> %s", job_id, type(e).__name__, err.error_code)
        store.mark_failed(job_id, err.error_code)


def _run_mutation_job(
    store: JobStore,
    job_id: str,
    sheet_factory: Callable[[], Any],
    transport_factory: Callable[[], Any],
    audit_builder: Optional[Callable[[Any], Any]],
    mutate_fn: Callable[[Any, Any, Any], Any],
    serialize_fn: Callable[[Any], Any],
) -> None:
    """Worker: a single-shot, confirm-gated MUTATION with no preview/drift split.

    For the verticals whose safety gate lives INSIDE the service (delete-single's
    mandatory snapshot->write_durable abort gate; deny-normalize; prices revert):
    the endpoint has already enforced the human gesture synchronously, so this
    worker only builds the (actor-bound) audit sink and calls the service, which
    owns the irreversibility gate — the endpoint never mutates Shopify itself. The
    per-request audit sink is built only where the vertical writes audit
    (delete/revert); deny-normalize passes no builder.
    """
    from backend.api.errors import map_exception  # local import: keep module light

    store.mark_running(job_id)
    try:
        sheet = sheet_factory()
        transport = transport_factory()
        audit_sink = audit_builder(sheet) if audit_builder is not None else None
        result = mutate_fn(sheet, transport, audit_sink)
        store.mark_done(job_id, serialize_fn(result))
    except Exception as e:  # noqa: BLE001 - top-level job guard; bounded code only
        err = map_exception(e)
        logger.error("mutation job %s failed: %s -> %s", job_id, type(e).__name__, err.error_code)
        store.mark_failed(job_id, err.error_code)


# =============================================================================
# Endpoint drivers (reused by every vertical router)
# =============================================================================
def _error(status: int, code: str, message: str) -> JSONResponse:
    """A stable, secret-free error envelope (matches ``ApiError.as_body`` shape)."""
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


def submit_preview(request: Request, spec: MutationVertical) -> Any:
    """Reserve the single slot and enqueue the READ-ONLY preview job."""
    state = request.app.state
    try:
        rec = state.job_store.create(spec.preview_kind)
    except JobBusyError:
        return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
    preview_fn = spec.make_preview(state)
    state.executor.submit(
        _run_preview_job,
        state.job_store,
        rec.job_id,
        state.sheet_factory,
        state.transport_factory,
        preview_fn,
        spec.plan_hash_fn,
        spec.serialize_plan,
        state.token_service,
        spec.token_kind,
        spec.ttl_s,
        spec.extra_fn,
    )
    return {"job_id": rec.job_id, "status": rec.status}


def submit_apply(request: Request, spec: MutationVertical, body: ApplyRequest, actor: str) -> Any:
    """Confirm-gate then enqueue the apply job.

    A bad/expired token -> 409 ``confirm_invalid`` with NO job and NO service call.
    A missing signing secret makes ``verify`` raise ``ConfigError`` — caught by the
    app error boundary as a 503, never surfaced as an invalid token.
    """
    state = request.app.state
    # (1) synchronous confirm gate — pure HMAC, no I/O. Bad/expired -> 409.
    # kind (post-review HARDENING) must match the vertical that minted the token.
    if not state.token_service.verify(body.confirm_token, body.plan_hash, kind=spec.token_kind):
        return _error(409, CODE_CONFIRM_INVALID, "Confirm token invalid or expired.")
    # (2) reserve the slot only after the token check passes.
    try:
        rec = state.job_store.create(spec.apply_kind)
    except JobBusyError:
        return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
    preview_fn = spec.make_preview(state)
    apply_fn = spec.make_apply(state)
    audit_builder = spec.make_audit(state, actor) if spec.make_audit is not None else None
    gesture_check = spec.make_gesture(body) if spec.make_gesture is not None else None
    state.executor.submit(
        _run_apply_job,
        state.job_store,
        rec.job_id,
        body.plan_hash,
        state.sheet_factory,
        state.transport_factory,
        audit_builder,
        preview_fn,
        spec.plan_hash_fn,
        apply_fn,
        spec.serialize_report,
        gesture_check,
    )
    return {"job_id": rec.job_id, "status": rec.status}


def poll_job(request: Request, job_id: str) -> Any:
    """Poll a preview/apply job by id (record only; the result is already JSON-safe)."""
    rec = request.app.state.job_store.get(job_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job_record_to_dict(rec)


# =============================================================================
# Router factory — one call builds the 4 routes for a vertical.
# =============================================================================
def build_mutation_router(spec: MutationVertical) -> APIRouter:
    """Build ``POST/GET <prefix>/preview`` and ``POST/GET <prefix>/apply``.

    Every route is auth-gated by ``require_basic_auth``; the authenticated actor is
    threaded into the apply job (used by verticals that write audit).
    """
    router = APIRouter(prefix=spec.prefix, tags=[spec.prefix.strip("/").replace("/", "-")])

    @router.post("/preview", status_code=202)
    async def preview(request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return submit_preview(request, spec)

    @router.get("/preview/{job_id}")
    async def get_preview(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    @router.post("/apply", status_code=202)
    async def apply(request: Request, body: ApplyRequest, actor: str = Depends(require_basic_auth)) -> Any:
        return submit_apply(request, spec, body, actor)

    @router.get("/apply/{job_id}")
    async def get_apply(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    return router


__all__ = [
    "DEFAULT_PREVIEW_TTL_S",
    "CODE_CONFIRM_INVALID",
    "CODE_JOB_BUSY",
    "CODE_GESTURE_REQUIRED",
    "CODE_INVALID_MODE",
    "APPLY_STATUS_APPLIED",
    "APPLY_STATUS_VERIFY_FAILED",
    "ApplyRequest",
    "MutationVertical",
    "build_mutation_router",
    "submit_preview",
    "submit_apply",
    "poll_job",
    "_error",
    "_run_preview_job",
    "_run_apply_job",
    "_run_readonly_job",
    "_run_mutation_job",
]
