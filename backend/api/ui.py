"""UI router — serves the static Scansia Manager dashboard (thin presentation
client over the already-tested JSON endpoints; NO new business logic here).

Wiring:

* ``GET /``        — the single-page dashboard (``static/index.html``).
* ``GET /app.js``  — vanilla JS client (no build step, no CDN, no framework).
* ``GET /app.css`` — stylesheet.

All three are gated by ``require_basic_auth`` — the SAME dependency every other
route uses (the browser's native Basic Auth prompt then covers the whole app,
including subsequent asset/API fetches from the same origin). ``/health``
remains the only UNGATED route (declared on the app itself, not here).

This module only reads static files off disk via ``FileResponse`` (part of
``starlette``, already a transitive dependency of the installed ``fastapi``
package — NO new dependency added). It introduces zero new data endpoints: the
dashboard is a pure client of the existing read/publish/prices/delete routers.
"""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse

from backend.auth.basic_auth import require_basic_auth

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


def build_ui_router() -> APIRouter:
    """Construct the UI router (mount under the app's auth gate)."""
    router = APIRouter(tags=["ui"])

    @router.get("/")
    def index(actor: str = Depends(require_basic_auth)) -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html", media_type="text/html")

    @router.get("/app.js")
    def app_js(actor: str = Depends(require_basic_auth)) -> FileResponse:
        return FileResponse(STATIC_DIR / "app.js", media_type="application/javascript")

    @router.get("/app.css")
    def app_css(actor: str = Depends(require_basic_auth)) -> FileResponse:
        return FileResponse(STATIC_DIR / "app.css", media_type="text/css")

    return router


__all__ = ["build_ui_router", "STATIC_DIR"]
