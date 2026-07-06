"""FastAPI READ router for Scansia Manager (M2).

Wires the pure compute core (``backend.api.inventory`` / ``backend.api.jobs``) to
HTTP. This module imports FastAPI, so it is loaded ONLY where the web deps are
installed (the Render deploy target / CI); the compute core it drives has no such
dependency and is unit-tested on a bare interpreter.

Endpoints (all auth-gated by ``require_basic_auth`` EXCEPT ``/health``, which lives
on the app, not here):

* ``GET  /scansia``                      — fast: eligible sheet rows, no live join.
* ``POST /scansia/inventory``            — enqueue the live-join background job.
* ``GET  /scansia/inventory/{job_id}``   — poll job status/result (record only).
* ``GET  /audit``                        — recent AUDIT tab events.

Collaborators are resolved from ``request.app.state`` FACTORIES so tests inject
in-memory fakes with zero network. The blocking join runs via
``app.state.executor`` (a single-worker thread pool in prod), so the async
endpoints never block the event loop; the poll endpoint only reads a job record.

The mutating verticals (publish / delete / prices) are M3-M5: intentionally NOT
mounted here yet. Add them as sibling routers behind the same auth + confirm-token
gate — this file stays READ-only.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from backend.api.inventory import (
    canonrow_to_dict,
    join_results_to_payload,
    read_eligible_rows,
    read_recent_audit,
    run_inventory_join,
)
from backend.api.jobs import (
    JOB_KIND_INVENTORY,
    JobBusyError,
    JobStore,
    job_record_to_dict,
)
from backend.auth.basic_auth import require_basic_auth

logger = logging.getLogger("backend.api.read")


def _run_inventory_job(
    store: JobStore,
    job_id: str,
    sheet_factory,
    transport_factory,
    promo_id: str,
) -> None:
    """Worker-thread entry point: build collaborators, run the join, record result.

    Runs OFF the event loop (in ``app.state.executor``). Has a top-level guard: a
    whole-job failure is recorded as ``JOB_FAILED`` with a STABLE error_code (via
    the boundary mapper) — never a raw message, never a propagated crash. The
    sheet/transport are built INSIDE the thread so their blocking construction and
    I/O stay off the loop.
    """
    from backend.api.errors import map_exception  # local import: keep module import light

    store.mark_running(job_id)
    try:
        sheet = sheet_factory()
        transport = transport_factory()
        results = run_inventory_join(sheet, transport, promo_id)
        store.mark_done(job_id, results)
    except Exception as e:  # noqa: BLE001 - top-level job guard; bounded code only
        err = map_exception(e)
        logger.error("inventory job %s failed: %s -> %s", job_id, type(e).__name__, err.error_code)
        store.mark_failed(job_id, err.error_code)


def build_read_router() -> APIRouter:
    """Construct the READ router. State/collaborators come from ``request.app.state``."""
    router = APIRouter(tags=["read"])

    @router.get("/scansia")
    def list_scansia(request: Request, actor: str = Depends(require_basic_auth)) -> dict:
        """Fast projection: the eligible canonical rows (no live Shopify join)."""
        sheet = request.app.state.sheet_factory()
        rows = read_eligible_rows(sheet)
        return {"count": len(rows), "rows": [canonrow_to_dict(r) for r in rows]}

    @router.post("/scansia/inventory", status_code=202)
    async def start_inventory(request: Request, actor: str = Depends(require_basic_auth)) -> dict:
        """Enqueue the single-slot live-inventory join. Returns immediately."""
        state = request.app.state
        try:
            rec = state.job_store.create(JOB_KIND_INVENTORY)
        except JobBusyError:
            raise HTTPException(status_code=409, detail="a join job is already running")
        state.executor.submit(
            _run_inventory_job,
            state.job_store,
            rec.job_id,
            state.sheet_factory,
            state.transport_factory,
            state.promo_location_id,
        )
        return {"job_id": rec.job_id, "status": rec.status}

    @router.get("/scansia/inventory/{job_id}")
    async def get_inventory(job_id: str, request: Request, actor: str = Depends(require_basic_auth)) -> dict:
        """Poll the join: status + (when done) results with freshness/stale/failed."""
        rec = request.app.state.job_store.get(job_id)
        if rec is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job_record_to_dict(rec, serialize_result=join_results_to_payload)

    @router.get("/audit")
    def get_audit(request: Request, limit: int = 50, actor: str = Depends(require_basic_auth)) -> dict:
        """Recent AUDIT tab events (any authenticated user)."""
        sink = request.app.state.audit_factory()
        events = read_recent_audit(sink, limit)
        return {"count": len(events), "events": events}

    return router


__all__ = ["build_read_router", "_run_inventory_job"]
