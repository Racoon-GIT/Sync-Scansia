"""App-level HTTP Basic Auth for Scansia Manager.

No network perimeter (Render FREE, single origin): the app authenticates every
request except ``/health`` against ``APP_PASSWORD`` (fail-closed if unset). The
credential check is pure stdlib
(:func:`~backend.auth.basic_auth.verify_credentials`), testable without FastAPI;
:func:`~backend.auth.basic_auth.require_basic_auth` is the FastAPI dependency
wrapper (FastAPI imported optionally, so this package loads on a bare interpreter).
"""
from backend.auth.basic_auth import (
    AuthError,
    AuthNotConfigured,
    require_basic_auth,
    verify_credentials,
)

__all__ = ["AuthError", "AuthNotConfigured", "verify_credentials", "require_basic_auth"]
