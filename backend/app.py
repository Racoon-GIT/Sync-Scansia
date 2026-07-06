"""Scansia Manager — FastAPI ASGI application (single-origin web utility).

Replaces the Sync-Scansia cron entry points with a small, auth-gated web app. This
module imports FastAPI, so it is loaded ONLY where the web deps are installed (the
Render deploy target / CI). The compute core (``backend.api.*``) has no such
dependency and is unit-tested on a bare interpreter.

Wiring:

* **Auth** — every route is gated by app-level HTTP Basic Auth
  (``backend.auth.require_basic_auth``) EXCEPT ``GET /health``. Fail-closed: a
  missing ``APP_PASSWORD`` denies every gated request with 503 (per request, so
  the app still BOOTS for diagnostics and the 503 surfaces where it matters).
* **Startup (fail-closed)** — loads the Shopify config from the environment; a
  missing required var raises ``ConfigError`` and the app fails to boot rather than
  starting misconfigured. Collaborators are built lazily from that config into
  ``app.state`` factories; tests inject in-memory fakes via ``create_app`` kwargs.
* **Error boundary** — a shared exception handler translates every known internal
  exception (Shopify user/transport, GSheet/SheetIO, Config, cutover) into a
  STABLE ``error_code`` + a fixed safe message via ``backend.api.errors``. NO raw
  message, stack, GraphQL payload, or gspread text ever reaches the client; the
  only internal log line carries the exception TYPE name + code.
* **Concurrency** — the slow live-inventory join runs OFF the event loop in a
  single-worker thread pool (``app.state.executor``); a single-slot ``JobStore``
  (in-memory) tracks it. A restart LOSES in-flight jobs — acceptable: a join is
  idempotent and recomputable, nothing durable depends on it.

Secrets (``SHOPIFY_ADMIN_TOKEN``, ``APP_PASSWORD``, ``TOKEN_SIGNING_SECRET``) come
ONLY from the environment and are NEVER logged.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, Callable, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from backend.api.errors import log_boundary, map_exception
from backend.api.jobs import JobStore
from backend.api.read import build_read_router
from backend.auth.basic_auth import AuthError, AuthNotConfigured
from backend.config import ConfigError, ShopifyConfig, load_shopify_config
from backend.gsheet import ScansiaSheet
from backend.gsheet.reader import GSheetError
from backend.persistence.gsheet_audit import GSheetAuditSink
from backend.shopify.ops import ShopifyUserError
from backend.shopify.transport import ShopifyTransport, ShopifyTransportError

logger = logging.getLogger("backend.app")

# Exception types translated by the boundary. Registering the bases is enough —
# ``map_exception`` refines subclasses (SheetIOError/CutoverNotDoneError -> distinct
# codes) via ordered isinstance checks. ``Exception`` is the last-resort 500 (a
# genuine bug still yields a safe body before the server re-logs it).
_BOUNDARY_TYPES = (
    ConfigError,
    GSheetError,
    ShopifyUserError,
    ShopifyTransportError,
    AuthError,
    AuthNotConfigured,
)


def _boundary_handler(request: Request, exc: Exception) -> JSONResponse:
    """Translate any exception into a bounded, secret-free JSON error."""
    err = map_exception(exc)
    log_boundary(exc, err)
    return JSONResponse(status_code=err.status_code, content=err.as_body())


def _register_error_handlers(app: FastAPI) -> None:
    for exc_type in _BOUNDARY_TYPES:
        app.add_exception_handler(exc_type, _boundary_handler)
    app.add_exception_handler(Exception, _boundary_handler)


def create_app(
    *,
    config: Optional[ShopifyConfig] = None,
    sheet_factory: Optional[Callable[[], Any]] = None,
    transport_factory: Optional[Callable[[], Any]] = None,
    audit_factory: Optional[Callable[[], Any]] = None,
    executor: Optional[Any] = None,
    job_store: Optional[JobStore] = None,
    promo_location_id: Optional[str] = None,
) -> FastAPI:
    """Build the app. All collaborators are injectable (tests pass fakes).

    Un-injected collaborators are filled with production defaults on startup
    (``load_shopify_config`` fail-closed, real transport/sheet/audit/thread-pool),
    so ``create_app()`` with no args is the production entry point and
    ``create_app(config=..., sheet_factory=..., ...)`` is the test entry point.
    """
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup (fail-closed): fill any un-injected collaborator with a
        # production default. ``load_shopify_config`` raises ``ConfigError`` on a
        # missing env var, so a misconfigured deploy fails to boot.
        state = app.state
        if state.config is None:
            state.config = load_shopify_config()
        if state.promo_location_id is None:
            state.promo_location_id = state.config.promo_location_id
        if state.transport_factory is None:
            cfg = state.config
            state.transport_factory = lambda: ShopifyTransport(cfg)
        if state.sheet_factory is None:
            state.sheet_factory = ScansiaSheet.open
        if state.audit_factory is None:
            state.audit_factory = lambda: GSheetAuditSink.from_scansia_sheet(state.sheet_factory())
        if state.executor is None:
            state.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="scansia-job")
        yield
        # Shutdown: release the worker pool (only if we own a real one).
        ex = app.state.executor
        if isinstance(ex, ThreadPoolExecutor):
            ex.shutdown(wait=False)

    app = FastAPI(
        title="Scansia Manager",
        docs_url=None,          # internal single-operator tool: no ungated schema surface
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    app.state.config = config
    app.state.sheet_factory = sheet_factory
    app.state.transport_factory = transport_factory
    app.state.audit_factory = audit_factory
    app.state.executor = executor
    app.state.job_store = job_store or JobStore()
    app.state.promo_location_id = promo_location_id

    _register_error_handlers(app)

    @app.get("/health")
    def health() -> dict:
        """Liveness probe — NOT auth-gated. Minimal, no store/counts/version."""
        return {"status": "ok"}

    app.include_router(build_read_router())
    return app


app = create_app()

__all__ = ["app", "create_app"]
