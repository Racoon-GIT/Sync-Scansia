"""FastAPI READ router for Scansia Manager, on the register.

Wires the pure compute core (``backend.api.inventory`` / ``backend.api.jobs``) to
HTTP. Every route is auth-gated by ``require_basic_auth`` (``/health`` lives on the
app). Collaborators come from ``request.app.state`` FACTORIES, so tests inject
in-memory fakes; every register handle is closed when its request or job ends
(plan §3.7 rule 4).

* ``GET  /scansia``                    — fast: the shelf (every ``IN_SCANSIA`` pair).
* ``POST /scansia/inventory``          — enqueue the live-join background job.
* ``GET  /scansia/inventory/{job_id}`` — poll the job (record only).
* ``GET  /audit``                      — the most recent ``AUDIT`` rows of
  ``sm_outlet_audit`` (timestamps Europe/Rome).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from backend.api.inventory import (
    join_results_to_payload,
    pair_to_dict,
    read_recent_audit,
    read_shelf,
    run_inventory_join,
)
from backend.api.jobs import JOB_KIND_INVENTORY, JobBusyError, JobStore, job_record_to_dict
from backend.api.mutations import _close
from backend.auth.basic_auth import require_basic_auth
from backend.persistence.audit_sink import RegisterAuditSink

logger = logging.getLogger("backend.api.read")


def _run_inventory_job(store: JobStore, job_id: str, register_factory, transport_factory, promo_id: str) -> None:
    """Worker-thread entry point: build collaborators, run the join, record the
    result. A whole-job failure becomes ``JOB_FAILED`` with a STABLE error code —
    never a raw message, never a crash."""
    from backend.api.errors import map_exception  # local import: keep module import light

    store.mark_running(job_id)
    register = None
    try:
        register = register_factory()
        transport = transport_factory()
        store.mark_done(job_id, run_inventory_join(register, transport, promo_id))
    except Exception as e:  # noqa: BLE001 - top-level job guard; bounded code only
        err = map_exception(e)
        logger.error("inventory job %s failed: %s -> %s", job_id, type(e).__name__, err.error_code)
        store.mark_failed(job_id, err.error_code)
    finally:
        _close(register)


def build_read_router() -> APIRouter:
    router = APIRouter(tags=["read"])

    @router.get("/scansia")
    def list_scansia(request: Request, actor: str = Depends(require_basic_auth)) -> dict:
        """The shelf, no live Shopify join (a plain def: run in a worker thread)."""
        register = request.app.state.register_factory(actor=actor)
        try:
            pairs = read_shelf(register)
        finally:
            _close(register)
        return {"count": len(pairs), "pairs": [pair_to_dict(p) for p in pairs]}

    @router.post("/scansia/inventory", status_code=202)
    async def start_inventory(request: Request, actor: str = Depends(require_basic_auth)) -> dict:
        state = request.app.state
        try:
            rec = state.job_store.create(JOB_KIND_INVENTORY)
        except JobBusyError:
            raise HTTPException(status_code=409, detail="a join job is already running")
        state.executor.submit(_run_inventory_job, state.job_store, rec.job_id, state.register_factory,
                              state.transport_factory, state.promo_location_id)
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/scansia/inventory/{job_id}")
    async def get_inventory(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> dict:
        rec = request.app.state.job_store.get(job_id)
        if rec is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job_record_to_dict(rec, serialize_result=join_results_to_payload)

    @router.get("/audit")
    def get_audit(request: Request, limit: int = 50, actor: str = Depends(require_basic_auth)) -> dict:
        register = request.app.state.register_factory(actor=actor)
        try:
            events = read_recent_audit(RegisterAuditSink(register, actor=actor), limit)
        finally:
            _close(register)
        return {"count": len(events), "events": events}

    return router


__all__ = ["build_read_router", "_run_inventory_job"]
