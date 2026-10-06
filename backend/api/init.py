"""INIT vertical — "Inizializza" on MySQL (plan ``docs/f7-mysql-adapter-plan.md``
§3.4; B-1 *Level once, after D6*): the one-time baseline, on the register
(``app.state.register_factory``), behind the confirm-token machinery of
:mod:`backend.api.mutations`.

* ``POST /init/preview`` + ``GET .../{job_id}`` — READ-ONLY plan job
  (:func:`~backend.services.baseline_service.baseline_preview`): per key the
  Level decision, the normalisations, the J price seeds, Part B, the catalogue
  notes, + ``plan_hash`` + a confirm token bound to it. The plan is kept in this
  process's memory under its hash: the confirm applies THAT plan, and the service
  re-reads each key under the lease (§3.4: a key that moved since the preview is
  held, nothing written — never the whole confirm refused for one sale).
* ``POST /init/apply`` — confirm-gated, synchronously: a bad/expired token → 409
  ``confirm_invalid``; a plan no longer kept (a restart, another process) → 409
  ``preview_expired``; ``confirm`` ≠ ``CONFERMO`` or §3.4's precondition failing
  (the ``make-operator`` GET's instant and ``dlqCount``, typed) → 409
  ``gesture_required`` with the reason. Only then a job runs
  :func:`~backend.services.baseline_service.baseline_apply`, which checks the
  precondition again on the server clock and keeps it in the ``BASELINE`` row.
* ``GET /init/status`` — ``{"baseline_done": bool}`` (the GUI's first-run banner).
* ``/relevel`` — the per-key re-level (:mod:`backend.services.relevel_service`):
  ``GET /relevel/held`` lists the held keys; ``POST /relevel/preview`` ({keys})
  and ``POST /relevel/apply`` follow the same custody and the same confirm gate.
"""
from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.api.jobs import JobBusyError
from backend.api.mutations import (
    CODE_CONFIRM_INVALID,
    CODE_GESTURE_REQUIRED,
    CODE_JOB_BUSY,
    ApplyRequest,
    MutationVertical,
    _close,
    _error,
    _run_preview_job,
    _store_factory,
    poll_job,
    submit_preview,
)
from backend.auth.basic_auth import require_basic_auth
from backend.persistence.audit_sink import RegisterAuditSink
from backend.services import baseline_service, relevel_service
from backend.services.delete_service import CONFIRM_WORD

logger = logging.getLogger("backend.api.init")

PREFIX = "/init"
KIND_PREVIEW = "init_preview"
KIND_APPLY = "init_apply"

# Vertical id SIGNED into the confirm-token: an init token never verifies elsewhere.
TOKEN_KIND_INIT = "init"
# The confirm names a plan this process no longer keeps.
CODE_PREVIEW_EXPIRED = "preview_expired"
# Previews kept per process (the confirm-token TTL bounds their use anyway).
PLANS_KEPT = 4

_PLANS_LOCK = threading.Lock()


# =============================================================================
# The previewed plans, kept by hash until confirmed
# =============================================================================
def _plans(state: Any, kind: str) -> "OrderedDict[str, Any]":
    kept = getattr(state, "kept_plans", None)
    if kept is None:
        kept = {}
        state.kept_plans = kept
    return kept.setdefault(kind, OrderedDict())


def remember_plan(state: Any, plan: Any, kind: str = TOKEN_KIND_INIT) -> None:
    with _PLANS_LOCK:
        plans = _plans(state, kind)
        plans[plan.plan_hash] = plan
        plans.move_to_end(plan.plan_hash)
        while len(plans) > PLANS_KEPT:
            plans.popitem(last=False)


def recall_plan(state: Any, plan_hash: str, kind: str = TOKEN_KIND_INIT) -> Optional[Any]:
    with _PLANS_LOCK:
        return _plans(state, kind).get(plan_hash)


def forget_plan(state: Any, plan_hash: str, kind: str = TOKEN_KIND_INIT) -> None:
    with _PLANS_LOCK:
        _plans(state, kind).pop(plan_hash, None)


# =============================================================================
# JSON-safe serializers
# =============================================================================
def _iso(ts: Optional[datetime]) -> Optional[str]:
    if ts is None:
        return None
    return (ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts).isoformat()


def init_plan_hash(plan: Any) -> str:
    return plan.plan_hash


def serialize_plan(plan: Any) -> Dict[str, Any]:
    return {
        "dry_run": plan.dry_run,
        "already_done": plan.already_done,
        "previewed_at": _iso(plan.previewed_at),
        "counts": plan.counts(),
        "keys": [asdict(kp) for kp in plan.keys],
        "products": [asdict(p) for p in plan.products],
        "prices": [asdict(s) for s in plan.prices],
        "part_b": [asdict(r) for r in plan.part_b],
        "catalogue": [asdict(c) for c in plan.catalogue],
        "keyless_event_ids": list(plan.keyless_event_ids),
        "anomalies": list(plan.anomalies),
    }


def serialize_report(report: Any) -> Dict[str, Any]:
    return asdict(report)


# =============================================================================
# The vertical (the preview half of the shared machinery)
# =============================================================================
def _make_preview(state: Any) -> Callable[[Any, Any], Any]:
    promo = state.promo_location_id

    def _preview(register: Any, transport: Any) -> Any:
        plan = baseline_service.baseline_preview(register, transport, promo_location_id=promo)
        remember_plan(state, plan)
        return plan

    return _preview


def _no_generic_apply(state: Any) -> Callable[..., Any]:  # pragma: no cover - the route below applies
    raise RuntimeError("init applies through its own route")


INIT_VERTICAL = MutationVertical(
    prefix=PREFIX,
    preview_kind=KIND_PREVIEW,
    apply_kind=KIND_APPLY,
    make_preview=_make_preview,
    make_apply=_no_generic_apply,
    plan_hash_fn=init_plan_hash,
    serialize_plan=serialize_plan,
    serialize_report=serialize_report,
    token_kind=TOKEN_KIND_INIT,
    collaborator="register_factory",
)


class InitApplyRequest(ApplyRequest):
    """The confirm: ``plan_hash`` + ``confirm_token`` + ``CONFERMO`` + §3.4's
    precondition as typed — the ``/dlqs`` GET's instant and ``dlqCount``."""

    dlq_checked_at: Optional[str] = None
    dlq_count: Optional[int] = None


def _run_baseline_job(
    store: Any,
    job_id: str,
    state: Any,
    register_factory: Callable[[], Any],
    transport_factory: Callable[[], Any],
    plan: Any,
    dlq: Any,
    actor: str,
) -> None:
    """Worker: the confirmed plan, applied (the service re-reads each key)."""
    from backend.api.errors import map_exception  # local import: keep module light

    store.mark_running(job_id)
    register = None
    try:
        register = register_factory()
        transport = transport_factory()
        report = baseline_service.baseline_apply(
            register, transport, plan, RegisterAuditSink(register, actor=actor),
            promo_location_id=state.promo_location_id, dlq=dlq,
        )
        if report.status == baseline_service.ST_APPLIED:
            forget_plan(state, plan.plan_hash)
        store.mark_done(job_id, {
            "status": report.status,
            "applied": report.status == baseline_service.ST_APPLIED,
            "plan_hash": plan.plan_hash,
            "report": serialize_report(report),
        })
    except Exception as e:  # noqa: BLE001 - top-level job guard; bounded code only
        err = map_exception(e)
        logger.error("init apply job %s failed: %s -> %s", job_id, type(e).__name__, err.error_code)
        store.mark_failed(job_id, err.error_code)
    finally:
        _close(register)


def _gesture(state: Any, body: InitApplyRequest, kind: str) -> Tuple[Optional[Any], Any, Optional[Any]]:
    """The synchronous confirm gate shared by the baseline and the re-level:
    ``(plan, dlq, None)``, or ``(None, None, the 409 response)``."""
    if not state.token_service.verify(body.confirm_token, body.plan_hash, kind=kind):
        return None, None, _error(409, CODE_CONFIRM_INVALID, "Confirm token invalid or expired.")
    plan = recall_plan(state, body.plan_hash, kind)
    if plan is None:
        return None, None, _error(409, CODE_PREVIEW_EXPIRED, "Preview no longer available: run it again.")
    if (body.confirm or "").strip() != CONFIRM_WORD:
        return None, None, _error(409, CODE_GESTURE_REQUIRED, "Type CONFERMO to apply.")
    dlq, why = baseline_service.parse_dlq(body.dlq_checked_at, body.dlq_count)
    if why is None:
        why = baseline_service.check_dlq(dlq, previewed_at=plan.previewed_at, now=datetime.now(timezone.utc))
    if why is not None:
        return None, None, JSONResponse(status_code=409, content={"error": {
            "code": CODE_GESTURE_REQUIRED, "message": "The /dlqs precondition is not met.", "reason": why}})
    return plan, dlq, None


def submit_init_apply(request: Request, body: InitApplyRequest, actor: str) -> Any:
    state = request.app.state
    plan, dlq, refused = _gesture(state, body, TOKEN_KIND_INIT)
    if refused is not None:
        return refused
    try:
        rec = state.job_store.create(KIND_APPLY)
    except JobBusyError:
        return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
    state.executor.submit(
        _run_baseline_job, state.job_store, rec.job_id, state,
        _store_factory(state, INIT_VERTICAL, actor), state.transport_factory, plan, dlq, actor,
    )
    return {"job_id": rec.job_id, "status": rec.status}


# =============================================================================
# The per-key re-level (§3.4): the gesture that releases a held key
# =============================================================================
RELEVEL_PREFIX = "/relevel"
KIND_RELEVEL_PREVIEW = "relevel_preview"
KIND_RELEVEL_APPLY = "relevel_apply"
TOKEN_KIND_RELEVEL = "relevel"
# Keys one re-level may carry (each costs a re-read and its writes under the lease).
RELEVEL_MAX_KEYS = 50


class RelevelPreviewRequest(BaseModel):
    keys: List[List[str]]


def serialize_relevel_plan(plan: Any) -> Dict[str, Any]:
    return {"dry_run": plan.dry_run, "previewed_at": _iso(plan.previewed_at),
            "keys": [asdict(k) for k in plan.keys]}


def _run_relevel_job(store: Any, job_id: str, state: Any, register_factory: Callable[[], Any],
                     transport_factory: Callable[[], Any], plan: Any, dlq: Any) -> None:
    from backend.api.errors import map_exception  # local import: keep module light

    store.mark_running(job_id)
    register = None
    try:
        register = register_factory()
        report = relevel_service.relevel_apply(register, transport_factory(), plan,
                                               promo_location_id=state.promo_location_id, dlq=dlq)
        if report.status == relevel_service.ST_APPLIED:
            forget_plan(state, plan.plan_hash, TOKEN_KIND_RELEVEL)
        store.mark_done(job_id, {"status": report.status, "applied": report.status == relevel_service.ST_APPLIED,
                                 "plan_hash": plan.plan_hash, "report": asdict(report)})
    except Exception as e:  # noqa: BLE001 - top-level job guard; bounded code only
        err = map_exception(e)
        logger.error("relevel apply job %s failed: %s -> %s", job_id, type(e).__name__, err.error_code)
        store.mark_failed(job_id, err.error_code)
    finally:
        _close(register)


def build_relevel_router() -> APIRouter:
    """``GET /relevel/held`` (the held keys, a read), ``POST /relevel/preview``
    ({keys}), ``POST /relevel/apply`` (the same confirm gate as the baseline)."""
    router = APIRouter(prefix=RELEVEL_PREFIX, tags=["relevel"])

    @router.get("/held")
    def held(request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        register = request.app.state.register_factory(actor=actor)
        try:
            out: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for h in register.held_keys():
                cur = out.setdefault(h.key, {"key": list(h.key), "kinds": [], "reasons": [], "events": 0})
                cur["kinds"] = sorted(set(cur["kinds"]) | {h.kind})
                cur["reasons"] = sorted(set(cur["reasons"]) | {h.reason})
                cur["events"] += len(h.event_ids)
        finally:
            _close(register)
        return {"held": [out[k] for k in sorted(out)]}

    @router.post("/preview", status_code=202)
    async def preview(request: Request, body: RelevelPreviewRequest, actor: str = Depends(require_basic_auth)) -> Any:
        keys = [k for k in body.keys if len(k) == 2]
        if not keys or len(keys) != len(body.keys) or len(keys) > RELEVEL_MAX_KEYS:
            return _error(422, "invalid_keys", f"Give 1 to {RELEVEL_MAX_KEYS} keys, each [SKU, SIZE].")
        state = request.app.state
        try:
            rec = state.job_store.create(KIND_RELEVEL_PREVIEW)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        promo = state.promo_location_id

        def _preview(register: Any, transport: Any) -> Any:
            plan = relevel_service.relevel_preview(register, transport, keys, promo_location_id=promo)
            remember_plan(state, plan, TOKEN_KIND_RELEVEL)
            return plan

        state.executor.submit(
            _run_preview_job, state.job_store, rec.job_id, _store_factory(state, INIT_VERTICAL),
            state.transport_factory, _preview, init_plan_hash, serialize_relevel_plan, state.token_service,
            TOKEN_KIND_RELEVEL, INIT_VERTICAL.ttl_s, None,
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/preview/{job_id}")
    async def get_preview(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    @router.post("/apply", status_code=202)
    async def apply(request: Request, body: InitApplyRequest, actor: str = Depends(require_basic_auth)) -> Any:
        state = request.app.state
        plan, dlq, refused = _gesture(state, body, TOKEN_KIND_RELEVEL)
        if refused is not None:
            return refused
        try:
            rec = state.job_store.create(KIND_RELEVEL_APPLY)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        state.executor.submit(_run_relevel_job, state.job_store, rec.job_id, state,
                              _store_factory(state, INIT_VERTICAL, actor), state.transport_factory, plan, dlq)
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/apply/{job_id}")
    async def get_apply(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    return router


def build_init_router() -> APIRouter:
    """The INIT router (mounted under the app's auth gate)."""
    router = APIRouter(prefix=PREFIX, tags=["init"])

    @router.post("/preview", status_code=202)
    async def preview(request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return submit_preview(request, INIT_VERTICAL)

    @router.get("/preview/{job_id}")
    async def get_preview(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    @router.post("/apply", status_code=202)
    async def apply(request: Request, body: InitApplyRequest, actor: str = Depends(require_basic_auth)) -> Any:
        return submit_init_apply(request, body, actor)

    @router.get("/apply/{job_id}")
    async def get_apply(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    @router.get("/status")
    def status(request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        # a plain def: FastAPI runs it in a worker thread (a database read)
        register = request.app.state.register_factory(actor=actor)
        try:
            done = register.baseline() is not None
        finally:
            _close(register)
        return {"baseline_done": done}

    return router


__all__ = [
    "PREFIX",
    "TOKEN_KIND_INIT",
    "CODE_PREVIEW_EXPIRED",
    "PLANS_KEPT",
    "INIT_VERTICAL",
    "InitApplyRequest",
    "init_plan_hash",
    "serialize_plan",
    "serialize_report",
    "remember_plan",
    "recall_plan",
    "forget_plan",
    "submit_init_apply",
    "build_init_router",
    "RELEVEL_PREFIX",
    "TOKEN_KIND_RELEVEL",
    "RELEVEL_MAX_KEYS",
    "build_relevel_router",
]
