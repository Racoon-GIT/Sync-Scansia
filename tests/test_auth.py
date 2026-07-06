"""backend.auth: app-level HTTP Basic Auth (fail-closed).

The credential CORE (``verify_credentials``) is pure stdlib and is unit-tested
directly here — no FastAPI required. The thin FastAPI dependency wrapper is
additionally exercised end-to-end via ``TestClient`` in a block guarded by
``importorskip`` (skipped where FastAPI/httpx are not installed, e.g. this bare
interpreter; runs on the deploy target / CI).
"""
from __future__ import annotations

import base64

import pytest

from backend.auth.basic_auth import (
    AuthError,
    AuthNotConfigured,
    verify_credentials,
)

_PW_ENV = "APP_PASSWORD"
_USER_ENV = "APP_USERNAME"


def _basic(user: str, pw: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pw}".encode("utf-8")).decode("ascii")


@pytest.fixture
def app_password(monkeypatch):
    monkeypatch.delenv(_USER_ENV, raising=False)  # default username "racoon"
    monkeypatch.setenv(_PW_ENV, "s3cret-pw")


# ===========================================================================
# Pure core: verify_credentials
# ===========================================================================
def test_correct_credentials_returns_actor(app_password):
    assert verify_credentials(_basic("racoon", "s3cret-pw")) == "racoon"


def test_missing_password_env_is_fail_closed_503(monkeypatch):
    monkeypatch.delenv(_PW_ENV, raising=False)
    with pytest.raises(AuthNotConfigured) as ei:
        verify_credentials(_basic("racoon", "whatever"))
    assert ei.value.status_code == 503


def test_blank_password_env_is_fail_closed_503(monkeypatch):
    monkeypatch.setenv(_PW_ENV, "   ")
    with pytest.raises(AuthNotConfigured) as ei:
        verify_credentials(_basic("racoon", "   "))
    assert ei.value.status_code == 503


def test_no_authorization_header_401(app_password):
    with pytest.raises(AuthError) as ei:
        verify_credentials(None)
    assert ei.value.status_code == 401
    assert "WWW-Authenticate" in ei.value.headers


def test_non_basic_scheme_401(app_password):
    with pytest.raises(AuthError):
        verify_credentials("Bearer sometoken")


def test_wrong_password_401(app_password):
    with pytest.raises(AuthError) as ei:
        verify_credentials(_basic("racoon", "WRONG"))
    assert ei.value.status_code == 401


def test_wrong_username_401(app_password):
    with pytest.raises(AuthError):
        verify_credentials(_basic("intruder", "s3cret-pw"))


def test_custom_username_from_env(monkeypatch):
    monkeypatch.setenv(_PW_ENV, "s3cret-pw")
    monkeypatch.setenv(_USER_ENV, "ale")
    assert verify_credentials(_basic("ale", "s3cret-pw")) == "ale"
    with pytest.raises(AuthError):
        verify_credentials(_basic("racoon", "s3cret-pw"))  # default no longer valid


def test_undecodable_base64_401(app_password):
    with pytest.raises(AuthError):
        verify_credentials("Basic !!!not-base64!!!")


def test_missing_colon_in_credentials_401(app_password):
    raw = base64.b64encode(b"nocolonhere").decode("ascii")
    with pytest.raises(AuthError):
        verify_credentials(f"Basic {raw}")


def test_password_with_colon_is_supported(monkeypatch):
    monkeypatch.delenv(_USER_ENV, raising=False)
    monkeypatch.setenv(_PW_ENV, "pa:ss:word")
    assert verify_credentials(_basic("racoon", "pa:ss:word")) == "racoon"


# ===========================================================================
# FastAPI dependency wrapper (end-to-end) — skipped without FastAPI/httpx
# ===========================================================================
def _client(monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient

    from backend.auth.basic_auth import require_basic_auth

    app = FastAPI()

    @app.get("/health")
    def health():  # open route
        return {"ok": True}

    @app.get("/protected")
    def protected(actor: str = Depends(require_basic_auth)):
        return {"actor": actor}

    return TestClient(app)


def test_fastapi_health_open(monkeypatch):
    client = _client(monkeypatch)
    assert client.get("/health").status_code == 200


def test_fastapi_protected_no_auth_401(monkeypatch, app_password):
    client = _client(monkeypatch)
    r = client.get("/protected")
    assert r.status_code == 401
    assert "www-authenticate" in {k.lower() for k in r.headers}


def test_fastapi_protected_wrong_password_401(monkeypatch, app_password):
    client = _client(monkeypatch)
    r = client.get("/protected", headers={"Authorization": _basic("racoon", "WRONG")})
    assert r.status_code == 401


def test_fastapi_protected_not_configured_503(monkeypatch):
    monkeypatch.delenv(_PW_ENV, raising=False)
    client = _client(monkeypatch)
    r = client.get("/protected", headers={"Authorization": _basic("racoon", "x")})
    assert r.status_code == 503


def test_fastapi_protected_correct_returns_actor(monkeypatch, app_password):
    client = _client(monkeypatch)
    r = client.get("/protected", headers={"Authorization": _basic("racoon", "s3cret-pw")})
    assert r.status_code == 200
    assert r.json() == {"actor": "racoon"}
