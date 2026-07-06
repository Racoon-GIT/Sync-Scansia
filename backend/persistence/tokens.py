"""Stateless HMAC confirm-token service (stdlib only — NO PyJWT).

Token layout: ``base64url(payload).base64url(sig)`` where ``payload`` is the
compact JSON ``{"exp": <unix-seconds>, "plan_hash": <str>, "kind": <str>}`` and
``sig = HMAC-SHA256(secret, payload_bytes)``. ``kind`` is the vertical's id
(``"publish"``/``"cleanup"``/``"prices"``/...) SIGNED into the payload
(post-review HARDENING): without it, a token minted for one vertical would
verify successfully against another vertical's ``plan_hash`` if the two hashes
ever coincided (e.g. two SHA256-truncated 16-hex digests colliding, or a
vertical reusing the same opaque string by accident) — ``verify`` now requires
BOTH the plan_hash AND the kind to match. The signing secret comes ONLY from
``TOKEN_SIGNING_SECRET`` in the environment (fail-closed: absent/blank ->
``ConfigError``); it is never logged, never embedded in the token, never returned.

SINGLE-USE IS NOT GUARANTEED at the token layer. The service is stateless (no
server-side store of spent tokens), so a token can technically be replayed within
its TTL. Replay is instead defused by two mechanisms already in the services:

1. a SHORT TTL on the token, and
2. the apply-time TOCTOU re-resolution — the approved ``plan_hash`` is recomputed
   against LIVE state at apply and any drift yields ``VERIFY_FAILED``. A replayed
   token cannot re-drive a mutation once the world has moved on.

This is an accepted trade-off for a single-operator tool behind HTTP Basic Auth.
The same token doubles as the CSRF token for the apply POST (possession of a
freshly minted, plan-bound token proves the request originated from the preview
this operator just approved).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Callable, Optional

from backend.config import ConfigError

_SECRET_ENV = "TOKEN_SIGNING_SECRET"


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


class HmacTokenService:
    """Stateless signed confirm-token (:class:`~backend.persistence.ports.TokenService`)."""

    def __init__(self, *, now: Optional[Callable[[], float]] = None) -> None:
        # ``now`` is an injectable clock for deterministic expiry tests. It is NOT
        # a secret and never affects the signature — only the ``exp`` claim.
        self._now = now or time.time

    def _secret(self) -> bytes:
        secret = os.environ.get(_SECRET_ENV)
        if not secret or not secret.strip():
            # Fail-closed: never mint/verify with a missing secret. No value logged.
            raise ConfigError(f"Missing required environment variable: {_SECRET_ENV}")
        return secret.encode("utf-8")

    def mint(self, plan_hash: str, ttl_s: int, *, kind: str) -> str:
        """Return a signed token binding ``plan_hash`` + ``kind``, valid for
        ``ttl_s`` seconds. ``kind`` (required, keyword-only) is the id of the
        vertical minting the token (post-review HARDENING — see module
        docstring); every call site must pass its own."""
        exp = int(self._now()) + int(ttl_s)
        payload = json.dumps(
            {"exp": exp, "plan_hash": plan_hash, "kind": kind},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        sig = hmac.new(self._secret(), payload, hashlib.sha256).digest()
        return f"{_b64url_encode(payload)}.{_b64url_encode(sig)}"

    def verify(self, token: str, plan_hash: str, *, kind: str) -> bool:
        """True iff ``token`` is a valid, unexpired signature over ``plan_hash``
        AND ``kind`` matches the SIGNED claim (post-review HARDENING: a token
        minted for one vertical no longer verifies against another, even if the
        opaque ``plan_hash`` string happened to coincide).

        Fail-closed on CONFIG: a missing/blank ``TOKEN_SIGNING_SECRET`` raises
        ``ConfigError`` (a deployment error must surface, not masquerade as an
        invalid token). Every TOKEN failure mode (bad shape, undecodable, forged
        signature, wrong plan_hash, wrong kind, expired) returns ``False`` —
        never raises and never leaks why.
        """
        secret = self._secret()  # fail-closed BEFORE any token parsing
        if not isinstance(token, str) or token.count(".") != 1:
            return False
        payload_b64, sig_b64 = token.split(".")
        try:
            payload = _b64url_decode(payload_b64)
            sig = _b64url_decode(sig_b64)
        except (ValueError, TypeError):  # binascii.Error is a ValueError subclass
            return False

        expected = hmac.new(secret, payload, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expected):  # constant-time
            return False

        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            return False
        if not isinstance(data, dict):
            return False
        if data.get("plan_hash") != plan_hash:
            return False
        if data.get("kind") != kind:
            return False
        try:
            exp = int(data["exp"])
        except (KeyError, TypeError, ValueError):
            return False
        return int(self._now()) < exp


__all__ = ["HmacTokenService"]
