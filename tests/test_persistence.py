"""backend.persistence.tokens: HMAC confirm-tokens.

Secrets come only from ``monkeypatch``-ed env vars and are never asserted-on as
plaintext beyond the round-trip they enable. The audit sink on the register is
``tests/test_audit_sink.py``'s.
"""
from __future__ import annotations

import json

import pytest

from backend.config import ConfigError
from backend.persistence.tokens import HmacTokenService

_SECRET_ENV = "TOKEN_SIGNING_SECRET"


# ===========================================================================
# HmacTokenService
# ===========================================================================
@pytest.fixture
def signing_secret(monkeypatch):
    monkeypatch.setenv(_SECRET_ENV, "unit-test-signing-secret")


def test_token_mint_verify_happy(signing_secret):
    svc = HmacTokenService()
    tok = svc.mint("planhash-abc", ttl_s=300, kind="publish")
    assert isinstance(tok, str) and tok.count(".") == 1
    assert svc.verify(tok, "planhash-abc", kind="publish") is True


def test_token_verify_altered_signature_false(signing_secret):
    svc = HmacTokenService()
    tok = svc.mint("planhash-abc", ttl_s=300, kind="publish")
    payload_b64, sig_b64 = tok.split(".")
    # The FIRST char: the last one of an unpadded 32-byte MAC carries 2 padding bits,
    # so 'A' → 'B' there can decode to the same bytes (a 1-in-16 flaky pass of verify).
    flipped = ("A" if sig_b64[0] != "A" else "B") + sig_b64[1:]
    assert svc.verify(f"{payload_b64}.{flipped}", "planhash-abc", kind="publish") is False


def test_token_verify_tampered_payload_false(signing_secret):
    """Forging a longer TTL (rewriting exp) invalidates the signature."""
    import base64

    svc = HmacTokenService()
    tok = svc.mint("planhash-abc", ttl_s=1, kind="publish")
    payload_b64, sig_b64 = tok.split(".")
    pad = "=" * (-len(payload_b64) % 4)
    data = json.loads(base64.urlsafe_b64decode(payload_b64 + pad))
    data["exp"] = data["exp"] + 10_000_000  # attacker extends validity
    forged = base64.urlsafe_b64encode(
        json.dumps(data, separators=(",", ":"), sort_keys=True).encode()
    ).rstrip(b"=").decode()
    assert svc.verify(f"{forged}.{sig_b64}", "planhash-abc", kind="publish") is False


def test_token_verify_expired_false(signing_secret):
    minted = HmacTokenService(now=lambda: 1_000.0)
    tok = minted.mint("planhash-abc", ttl_s=10, kind="publish")  # exp == 1010
    later = HmacTokenService(now=lambda: 5_000.0)
    assert later.verify(tok, "planhash-abc", kind="publish") is False


def test_token_verify_not_yet_expired_true(signing_secret):
    minted = HmacTokenService(now=lambda: 1_000.0)
    tok = minted.mint("planhash-abc", ttl_s=100, kind="publish")  # exp == 1100
    still = HmacTokenService(now=lambda: 1_050.0)
    assert still.verify(tok, "planhash-abc", kind="publish") is True


def test_token_verify_wrong_plan_hash_false(signing_secret):
    svc = HmacTokenService()
    tok = svc.mint("planhash-abc", ttl_s=300, kind="publish")
    assert svc.verify(tok, "planhash-DIFFERENT", kind="publish") is False


def test_token_verify_wrong_kind_false(signing_secret):
    """HARDENING (post-review): a token minted for one vertical must NOT verify
    for another, even when the (opaque) plan_hash string matches."""
    svc = HmacTokenService()
    tok = svc.mint("planhash-abc", ttl_s=300, kind="publish")
    assert svc.verify(tok, "planhash-abc", kind="cleanup") is False
    assert svc.verify(tok, "planhash-abc", kind="prices") is False
    assert svc.verify(tok, "planhash-abc", kind="publish") is True  # sanity: correct kind still works


def test_token_verify_malformed_false(signing_secret):
    svc = HmacTokenService()
    assert svc.verify("no-dot-here", "planhash-abc", kind="publish") is False
    assert svc.verify("!!!.$$$", "planhash-abc", kind="publish") is False
    assert svc.verify("", "planhash-abc", kind="publish") is False


def test_token_mint_missing_secret_raises(monkeypatch):
    monkeypatch.delenv(_SECRET_ENV, raising=False)
    with pytest.raises(ConfigError, match=_SECRET_ENV):
        HmacTokenService().mint("planhash-abc", ttl_s=300, kind="publish")


def test_token_verify_missing_secret_raises(monkeypatch):
    """Fail-closed on verify too: no secret -> ConfigError, not a silent False."""
    monkeypatch.delenv(_SECRET_ENV, raising=False)
    with pytest.raises(ConfigError, match=_SECRET_ENV):
        HmacTokenService().verify("a.b", "planhash-abc", kind="publish")


def test_token_blank_secret_is_missing(monkeypatch):
    monkeypatch.setenv(_SECRET_ENV, "   ")
    with pytest.raises(ConfigError, match=_SECRET_ENV):
        HmacTokenService().mint("planhash-abc", ttl_s=300, kind="publish")
