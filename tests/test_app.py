"""backend.app — FastAPI wiring: health gate, auth gate, error boundary.

PURE tier (always): ``map_exception`` translates every known internal exception
to a stable code + a FIXED safe message, and unknowns collapse to a generic 500 —
no branch echoes the raw exception text.

TestClient tier (``importorskip``): ``/health`` open, gated routes 401 without
auth, and the error boundary — a service exception raised inside a handler yields
a stable ``error_code`` with NO raw message/stack/GraphQL/gspread text in the body.
Skipped where FastAPI/httpx are absent (this bare interpreter); runs on CI.
"""
from __future__ import annotations

import base64

import pytest

from backend.api.errors import (
    CODE_CONFIG_ERROR,
    CODE_CUTOVER_NOT_DONE,
    CODE_INTERNAL_ERROR,
    CODE_SHEET_ERROR,
    CODE_SHEET_IO_ERROR,
    CODE_SHOPIFY_TRANSPORT_ERROR,
    CODE_SHOPIFY_USER_ERROR,
    CODE_UNAUTHORIZED,
    map_exception,
)
from backend.auth.basic_auth import AuthError, AuthNotConfigured
from backend.config import ConfigError
from backend.gsheet.reader import CutoverNotDoneError, GSheetError, SheetIOError
from backend.shopify.ops import ShopifyUserError
from backend.shopify.transport import ShopifyTransportError


# ===========================================================================
# Pure: map_exception
# ===========================================================================
def test_map_shopify_user_error():
    err = map_exception(ShopifyUserError("productDelete", [{"field": "id", "message": "boom"}]))
    assert err.status_code == 422 and err.error_code == CODE_SHOPIFY_USER_ERROR
    # the raw userError payload never appears in the safe message
    assert "boom" not in err.message and "productDelete" not in err.message


def test_map_shopify_transport_error():
    err = map_exception(ShopifyTransportError("GraphQL errors: [secret internals]"))
    assert err.status_code == 502 and err.error_code == CODE_SHOPIFY_TRANSPORT_ERROR
    assert "secret" not in err.message


def test_map_sheet_io_error_specific_over_base():
    err = map_exception(SheetIOError("gspread APIError 500 quota exceeded xyz"))
    assert err.status_code == 502 and err.error_code == CODE_SHEET_IO_ERROR
    assert "quota" not in err.message


def test_map_cutover_specific_over_base():
    err = map_exception(CutoverNotDoneError("sentinel absent"))
    assert err.status_code == 409 and err.error_code == CODE_CUTOVER_NOT_DONE


def test_map_gsheet_base_error():
    err = map_exception(GSheetError("some sheet failure"))
    assert err.status_code == 502 and err.error_code == CODE_SHEET_ERROR


def test_map_config_error():
    err = map_exception(ConfigError("Missing required environment variable: SHOPIFY_STORE"))
    assert err.status_code == 503 and err.error_code == CODE_CONFIG_ERROR
    assert "SHOPIFY_STORE" not in err.message  # never echo the missing var name


def test_map_auth_errors():
    assert map_exception(AuthError("x")).status_code == 401
    assert map_exception(AuthError("x")).error_code == CODE_UNAUTHORIZED
    assert map_exception(AuthNotConfigured("x")).status_code == 503


def test_map_unknown_exception_is_generic_500():
    err = map_exception(KeyError("node"))
    assert err.status_code == 500 and err.error_code == CODE_INTERNAL_ERROR
    assert "node" not in err.message


def test_api_error_as_body_shape():
    body = map_exception(ShopifyUserError("m", [])).as_body()
    assert set(body["error"].keys()) == {"code", "message"}


# ===========================================================================
# TestClient tier — RUNS ONLY when `fastapi`/`httpx` are installed in the
# interpreter running pytest (``pytest.importorskip`` inside `_app`). On a bare
# local interpreter without those deps, every test below is reported as
# SKIPPED, not failed/passed — this is an intentional, documented local skip,
# not masked. They run for real on the deploy target / CI where the web deps
# are installed.
# ===========================================================================
def _basic(user: str, pw: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pw}".encode("utf-8")).decode("ascii")


def _app(monkeypatch, *, boom=None):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi import Depends

    from backend.app import create_app
    from backend.auth.basic_auth import require_basic_auth
    from backend.config import ShopifyConfig

    monkeypatch.delenv("APP_USERNAME", raising=False)
    monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
    cfg = ShopifyConfig("t.myshopify.com", "shpat_x", "2025-07", "gid://shopify/Location/PROMO")
    app = create_app(
        config=cfg,
        sheet_factory=lambda: None,
        transport_factory=lambda: None,
        audit_factory=lambda: None,
        promo_location_id="gid://shopify/Location/PROMO",
    )

    if boom is not None:
        # A gated route that raises the given exception — exercises the boundary.
        @app.get("/boom")
        def _boom(actor: str = Depends(require_basic_auth)):
            raise boom

    return app


def _client(app):
    from fastapi.testclient import TestClient

    return TestClient(app)


def test_health_open_200(monkeypatch):
    client = _client(_app(monkeypatch))
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_health_body_has_no_sensitive_data(monkeypatch):
    client = _client(_app(monkeypatch))
    body = client.get("/health").json()
    assert set(body.keys()) == {"status"}  # no store, counts, or version leaked


@pytest.mark.parametrize("method,path", [
    ("get", "/scansia"),
    ("post", "/scansia/inventory"),
    ("get", "/scansia/inventory/whatever"),
    ("get", "/audit"),
])
def test_all_non_health_routes_require_auth(monkeypatch, method, path):
    client = _client(_app(monkeypatch))
    r = getattr(client, method)(path)
    assert r.status_code == 401
    assert "www-authenticate" in {k.lower() for k in r.headers}


def test_error_boundary_shopify_user_error_is_stable_and_safe(monkeypatch):
    boom = ShopifyUserError("productDelete", [{"field": "id", "message": "RAW-SECRET-DETAIL"}])
    client = _client(_app(monkeypatch, boom=boom))
    r = client.get("/boom", headers={"Authorization": _basic("racoon", "s3cret-pw")})
    assert r.status_code == 422
    body = r.json()
    assert body["error"]["code"] == CODE_SHOPIFY_USER_ERROR
    # NEVER leak the raw userError payload / mutation name
    import json
    dumped = json.dumps(body)
    assert "RAW-SECRET-DETAIL" not in dumped
    assert "productDelete" not in dumped


def test_error_boundary_sheet_io_error_maps_502(monkeypatch):
    client = _client(_app(monkeypatch, boom=SheetIOError("gspread quota secret")))
    r = client.get("/boom", headers={"Authorization": _basic("racoon", "s3cret-pw")})
    assert r.status_code == 502
    body = r.json()
    assert body["error"]["code"] == CODE_SHEET_IO_ERROR
    assert "quota" not in body["error"]["message"]


def test_error_boundary_cutover_maps_409(monkeypatch):
    client = _client(_app(monkeypatch, boom=CutoverNotDoneError("sentinel absent")))
    r = client.get("/boom", headers={"Authorization": _basic("racoon", "s3cret-pw")})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == CODE_CUTOVER_NOT_DONE


def test_error_boundary_unmapped_exception_is_generic_500_via_http(monkeypatch):
    """An exception NOT in ``_BOUNDARY_TYPES`` (e.g. a raw ``ValueError`` from a
    genuine bug) is still caught by the catch-all ``Exception`` handler and must
    surface as a safe, generic 500 — never a raw crash/traceback to the client.

    Starlette's ``ServerErrorMiddleware`` re-raises the exception AFTER invoking
    the registered handler (so the server can log it) — the TestClient must be
    built with ``raise_server_exceptions=False`` to observe the actual HTTP
    response instead of the exception propagating into the test itself.
    """
    boom = ValueError("raw internal detail: never to leak to the client")
    app = _app(monkeypatch, boom=boom)

    from fastapi.testclient import TestClient

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/boom", headers={"Authorization": _basic("racoon", "s3cret-pw")})
    assert r.status_code == 500
    body = r.json()
    assert body["error"]["code"] == CODE_INTERNAL_ERROR
    assert "raw internal detail" not in r.text


def test_error_boundary_auth_not_configured_503(monkeypatch):
    """APP_PASSWORD unset at request time -> gated route 503 (app still boots)."""
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from backend.app import create_app
    from backend.config import ShopifyConfig

    monkeypatch.delenv("APP_PASSWORD", raising=False)
    cfg = ShopifyConfig("t.myshopify.com", "shpat_x", "2025-07", "gid://shopify/Location/PROMO")
    app = create_app(config=cfg, sheet_factory=lambda: None, transport_factory=lambda: None,
                     audit_factory=lambda: None, promo_location_id="gid://shopify/Location/PROMO")
    r = _client(app).get("/scansia", headers={"Authorization": _basic("racoon", "x")})
    assert r.status_code == 503
