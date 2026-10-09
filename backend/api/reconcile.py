"""RECONCILE rows — the open rows, and a person's close (plan
``docs/f7-mysql-adapter-plan.md`` §3.3 rule 2's terminal path (iv)), on the register.

* ``GET /outlet/reconcile/open`` — every ``RECONCILE`` row not closed (a read): its
  kind, keys, product, when it was sent, and what a person's close would set.
* ``POST /outlet/reconcile/close`` (``row_id``, ``expect_status`` — the status the
  person read —, ``why``, ``confirm``) — ``CONFERMO`` and a reason are enforced
  synchronously (409 ``gesture_required``); then a job runs
  :func:`~backend.services.close_service.close_row` under the pass lease, on a
  register handle bound to the acting user (the "who" of the close).
"""
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from backend.api.jobs import JobBusyError
from backend.api.mutations import (
    CODE_GESTURE_REQUIRED,
    CODE_JOB_BUSY,
    _close,
    _error,
    _run_mutation_job,
    poll_job,
    register_for,
)
from backend.auth.basic_auth import require_basic_auth
from backend.persistence.records import NOT_CLOSED
from backend.services import close_service
from backend.services.delete_service import CONFIRM_WORD

KIND_CLOSE = "reconcile_close"


class CloseRequest(BaseModel):
    row_id: int
    expect_status: str
    why: str = ""
    confirm: str = ""


def build_reconcile_router() -> APIRouter:
    router = APIRouter(prefix="/outlet/reconcile", tags=["reconcile"])

    @router.get("/open")
    def open_rows(request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        # a plain def: FastAPI runs it in a worker thread (a database read)
        register = request.app.state.register_factory(actor=actor)
        try:
            rows = close_service.open_rows(register)
        finally:
            _close(register)
        return {"rows": [asdict(r) for r in rows]}

    @router.post("/close", status_code=202)
    async def close(request: Request, body: CloseRequest, actor: str = Depends(require_basic_auth)) -> Any:
        if body.confirm.strip() != CONFIRM_WORD:
            return _error(409, CODE_GESTURE_REQUIRED, f"Human gesture required: type {CONFIRM_WORD}.")
        if len(body.why.strip()) < close_service.WHY_MIN_CHARS:
            return _error(409, CODE_GESTURE_REQUIRED, "Say why the row is closed by hand.")
        if body.expect_status not in NOT_CLOSED:
            return _error(409, CODE_GESTURE_REQUIRED, "Only a row not closed (OPEN / APPLIED) can be closed.")
        state = request.app.state
        try:
            rec = state.job_store.create(KIND_CLOSE)
        except JobBusyError:
            return _error(409, CODE_JOB_BUSY, "An operation is already in flight.")
        promo = state.promo_location_id
        row_id, expect, why = body.row_id, body.expect_status, body.why.strip()

        def mutate_fn(register: Any, transport: Any, _audit: Any) -> Any:
            return close_service.close_row(register, transport, row_id, expect_status=expect, why=why,
                                           promo_location_id=promo)

        state.executor.submit(_run_mutation_job, state.job_store, rec.job_id, register_for(state, actor),
                              state.transport_factory, None, mutate_fn, asdict)
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/close/{job_id}")
    async def get_close(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        return poll_job(request, job_id)

    return router


__all__ = ["build_reconcile_router", "CloseRequest", "KIND_CLOSE"]
