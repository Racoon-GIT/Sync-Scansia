"""B-3's trigger (plan ``docs/f7-mysql-adapter-plan.md`` §6 B-3, D23): the
"decrement only" endpoint 678434 calls after a scansia sale and the
working-window net (a Scheduler job) calls every 15 min.

* ``POST /outlet/decrement/trigger`` — OUTSIDE the app's Basic Auth, guarded by
  its own bearer, the workspace's Render trigger shape
  (``IT/SVILUPPO/docs/shared-secrets.md`` §TRIGGER_SECRET): env
  ``TRIGGER_SECRET``, sent as ``Authorization: Bearer`` (primary) or
  ``X-Trigger-Secret`` — never in the URL. Unset → 503, nothing runs
  (fail-closed). It trusts no payload: it answers 202 at once and the pass runs on
  the decrement's own worker (:mod:`backend.services.decrement_worker`), which
  reads the register itself.
* ``GET /outlet/decrement/status`` — Basic Auth: the worker's last pass.
"""
from __future__ import annotations

import hmac
import os
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, Request

from backend.api.mutations import _error
from backend.auth.basic_auth import require_basic_auth

PREFIX = "/outlet/decrement"
TRIGGER_SECRET_ENV = "TRIGGER_SECRET"
CODE_TRIGGER_NOT_CONFIGURED = "trigger_not_configured"
CODE_TRIGGER_UNAUTHORIZED = "trigger_unauthorized"


def _presented(authorization: Optional[str], x_trigger_secret: Optional[str]) -> str:
    auth = (authorization or "").strip()
    if auth[:7].lower() == "bearer ":
        return auth[7:].strip()
    return (x_trigger_secret or "").strip()


def build_decrement_router() -> APIRouter:
    router = APIRouter(prefix=PREFIX, tags=["decrement"])

    @router.post("/trigger", status_code=202)
    async def trigger(
        request: Request,
        authorization: Optional[str] = Header(default=None),
        x_trigger_secret: Optional[str] = Header(default=None),
    ) -> Any:
        secret = (os.environ.get(TRIGGER_SECRET_ENV) or "").strip()
        if not secret:
            return _error(503, CODE_TRIGGER_NOT_CONFIGURED, "The trigger is not configured.")
        given = _presented(authorization, x_trigger_secret)
        if not given or not hmac.compare_digest(given.encode("utf-8"), secret.encode("utf-8")):
            return _error(401, CODE_TRIGGER_UNAUTHORIZED, "Trigger refused.")
        return request.app.state.decrement_worker.trigger("http")

    @router.get("/status")
    async def status(request: Request, actor: str = Depends(require_basic_auth)) -> Any:
        worker = request.app.state.decrement_worker
        return {"running": worker.running, "runs": worker.runs, "last": worker.last}

    return router


__all__ = ["PREFIX", "TRIGGER_SECRET_ENV", "CODE_TRIGGER_NOT_CONFIGURED", "CODE_TRIGGER_UNAUTHORIZED",
           "build_decrement_router"]
