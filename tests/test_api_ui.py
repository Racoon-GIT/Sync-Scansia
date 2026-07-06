"""backend.api.ui — static dashboard router.

TestClient tier only (``importorskip`` — skipped where FastAPI/httpx are
absent, e.g. this bare interpreter; runs on the deploy target / CI, and in a
throwaway venv with the web deps installed).

The dashboard is a thin PRESENTATION client over already-tested JSON
endpoints: no new business logic lives here, so these tests only verify the
server-side wiring (auth gate + static file serving). The JS itself is
rendering logic — its plan_hash/confirm_token echo path is documented in
``backend/static/app.js`` and is verifiable by manual QA against a live
server; it has no server-observable behavior to unit-test here.
"""
from __future__ import annotations

import base64

import pytest


def _basic(user: str, pw: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pw}".encode("utf-8")).decode("ascii")


def _app(monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from backend.app import create_app
    from backend.config import ShopifyConfig

    monkeypatch.delenv("APP_USERNAME", raising=False)
    monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
    cfg = ShopifyConfig("t.myshopify.com", "shpat_x", "2025-07", "gid://shopify/Location/PROMO")
    return create_app(
        config=cfg,
        sheet_factory=lambda: None,
        transport_factory=lambda: None,
        audit_factory=lambda: None,
        promo_location_id="gid://shopify/Location/PROMO",
    )


def _client(app):
    from fastapi.testclient import TestClient

    return TestClient(app)


# ===========================================================================
# GET / — the dashboard shell
# ===========================================================================
def test_index_without_auth_is_401(monkeypatch):
    client = _client(_app(monkeypatch))
    r = client.get("/")
    assert r.status_code == 401
    assert "www-authenticate" in {k.lower() for k in r.headers}


def test_index_with_auth_serves_dashboard_html(monkeypatch):
    client = _client(_app(monkeypatch))
    r = client.get("/", headers={"Authorization": _basic("racoon", "s3cret-pw")})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert 'data-app="scansia-manager"' in r.text
    assert "<title>Scansia Manager</title>" in r.text


# ===========================================================================
# Static assets — gated the same way
# ===========================================================================
def test_app_js_without_auth_is_401(monkeypatch):
    client = _client(_app(monkeypatch))
    assert client.get("/app.js").status_code == 401


def test_app_js_with_auth_is_served(monkeypatch):
    client = _client(_app(monkeypatch))
    r = client.get("/app.js", headers={"Authorization": _basic("racoon", "s3cret-pw")})
    assert r.status_code == 200
    assert "javascript" in r.headers["content-type"]
    # load-bearing preview->apply plan_hash/confirm_token echo, sanity-checked
    # server-side (the JS itself is exercised manually against a live server).
    assert "plan_hash" in r.text
    assert "confirm_token" in r.text


def test_app_css_without_auth_is_401(monkeypatch):
    client = _client(_app(monkeypatch))
    assert client.get("/app.css").status_code == 401


def test_app_css_with_auth_is_served(monkeypatch):
    client = _client(_app(monkeypatch))
    r = client.get("/app.css", headers={"Authorization": _basic("racoon", "s3cret-pw")})
    assert r.status_code == 200
    assert "css" in r.headers["content-type"]


# ===========================================================================
# No new DATA endpoint sneaked in — the ui router serves static content only.
# ===========================================================================
def test_ui_router_only_exposes_static_routes():
    pytest.importorskip("fastapi")
    from backend.api.ui import build_ui_router

    router = build_ui_router()
    paths = {r.path for r in router.routes}
    assert paths == {"/", "/app.js", "/app.css"}
