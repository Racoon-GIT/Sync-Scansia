"""App-level HTTP Basic Auth for Scansia Manager (no network perimeter).

Render FREE, single origin: the app itself authenticates every request except
``/health``. The credential check is PURE STDLIB
(:func:`verify_credentials`) so it is fully unit-testable without FastAPI
installed; :func:`require_basic_auth` is a THIN FastAPI dependency wrapper that
maps the typed auth errors to ``HTTPException``. FastAPI is imported through a
guarded optional import, so this module loads cleanly whether or not FastAPI is
present (tests run on a bare interpreter; the deploy target ships FastAPI).

FAIL-CLOSED: if ``APP_PASSWORD`` is unset/blank the check denies EVERYTHING with a
503 (auth not configured) — never an open door. The password is compared with
:func:`hmac.compare_digest` (constant-time, on bytes) and is NEVER logged. The
username is a configurable fixed value (``APP_USERNAME``, default ``racoon``); the
authenticated username is returned as the audit ACTOR.
"""
from __future__ import annotations

import base64
import binascii
import hmac
import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger("backend.auth")

_APP_PASSWORD_ENV = "APP_PASSWORD"
_APP_USERNAME_ENV = "APP_USERNAME"
DEFAULT_USERNAME = "racoon"

_REALM = 'Basic realm="Scansia Manager", charset="UTF-8"'
WWW_AUTHENTICATE: Dict[str, str] = {"WWW-Authenticate": _REALM}


class AuthError(RuntimeError):
    """Missing/malformed header or wrong credentials. Maps to HTTP 401."""

    status_code = 401
    headers: Dict[str, str] = WWW_AUTHENTICATE


class AuthNotConfigured(RuntimeError):
    """``APP_PASSWORD`` not set — fail-closed. Maps to HTTP 503."""

    status_code = 503
    headers: Dict[str, str] = {}


def _expected_username() -> str:
    raw = os.environ.get(_APP_USERNAME_ENV)
    return raw.strip() if raw and raw.strip() else DEFAULT_USERNAME


def verify_credentials(authorization: Optional[str]) -> str:
    """Validate a Basic ``Authorization`` header value; return the actor username.

    Args:
        authorization: the raw header value (e.g. ``"Basic cmFjb29uOnB3"``) or None.

    Returns:
        The authenticated username (the audit actor).

    Raises:
        AuthNotConfigured: ``APP_PASSWORD`` unset/blank (fail-closed, 503).
        AuthError: header missing/malformed or credentials wrong (401).
    """
    password = os.environ.get(_APP_PASSWORD_ENV)
    if not password or not password.strip():
        # Fail-closed. Never log the (absent) secret value — only the fact it is unset.
        logger.error("auth non configurata: %s non impostata", _APP_PASSWORD_ENV)
        raise AuthNotConfigured("authentication not configured")

    if not authorization or not authorization.startswith("Basic "):
        raise AuthError("missing or malformed Authorization header")
    try:
        decoded = base64.b64decode(authorization[len("Basic "):], validate=True).decode("utf-8")
    except (binascii.Error, ValueError, UnicodeDecodeError):
        raise AuthError("undecodable Basic credentials")
    username, sep, presented = decoded.partition(":")
    if not sep:
        raise AuthError("malformed Basic credentials")

    # Compare on bytes (compare_digest rejects non-ASCII str); both branches
    # always evaluated so the check does not short-circuit on username alone.
    user_ok = hmac.compare_digest(username.encode("utf-8"), _expected_username().encode("utf-8"))
    pass_ok = hmac.compare_digest(presented.encode("utf-8"), password.encode("utf-8"))
    if not (user_ok and pass_ok):
        raise AuthError("invalid credentials")
    return username


# --- FastAPI dependency wrapper (optional import) --------------------------
try:  # FastAPI is optional: the pure core above works without it (tests, tooling).
    from fastapi import HTTPException as _HTTPException
    from fastapi import Request as _Request

    _FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only where FastAPI is absent
    _FASTAPI_AVAILABLE = False


if _FASTAPI_AVAILABLE:

    def require_basic_auth(request: "_Request") -> str:
        """FastAPI dependency: authenticate via Basic Auth, return the actor username.

        Apply to every route EXCEPT ``/health``. Maps :class:`AuthNotConfigured`
        -> 503 and :class:`AuthError` -> 401 (+ ``WWW-Authenticate``); never leaks
        the underlying reason to the client.
        """
        authorization = request.headers.get("Authorization")
        try:
            return verify_credentials(authorization)
        except AuthNotConfigured as e:
            raise _HTTPException(status_code=e.status_code, detail="Authentication not configured")
        except AuthError as e:
            raise _HTTPException(status_code=e.status_code, detail="Unauthorized", headers=e.headers)

else:

    def require_basic_auth(request: Any) -> str:  # pragma: no cover - FastAPI absent
        raise RuntimeError(
            "FastAPI is not installed; require_basic_auth requires the web dependencies"
        )


__all__ = [
    "AuthError",
    "AuthNotConfigured",
    "DEFAULT_USERNAME",
    "WWW_AUTHENTICATE",
    "verify_credentials",
    "require_basic_auth",
]
