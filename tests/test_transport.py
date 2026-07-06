"""backend.shopify.transport.ShopifyTransport.

All HTTP is mocked at the requests.Session method level (``transport.sess.post``)
— no real network calls, per project rule (never touch the live store).
"""
from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest
import requests

from backend.shopify.transport import ShopifyTransport, ShopifyTransportError


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        json_body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        text: Optional[str] = None,
    ):
        self.status_code = status_code
        self._json_body = json_body if json_body is not None else {}
        self.headers = headers or {}
        self.text = text if text is not None else ("{}" if json_body is not None else "")

    def json(self):
        return self._json_body


@pytest.fixture
def transport(shopify_config) -> ShopifyTransport:
    t = ShopifyTransport(config=shopify_config)
    t.min_interval = 0.0  # don't slow tests down with the throttle wait
    return t


def test_happy_path_returns_data(transport):
    ok = FakeResponse(200, json_body={"data": {"shop": {"name": "Racoon"}}})
    transport.sess.post = MagicMock(return_value=ok)

    data = transport.graphql("query { shop { name } }", {})

    assert data == {"shop": {"name": "Racoon"}}
    transport.sess.post.assert_called_once()


def test_every_call_passes_timeout_kwarg(transport):
    """Regression: sync.py's legacy transport.graphql() had NO timeout at all."""
    ok = FakeResponse(200, json_body={"data": {"ok": True}})
    transport.sess.post = MagicMock(return_value=ok)

    transport.graphql("query {}", {})

    _, kwargs = transport.sess.post.call_args
    assert kwargs["timeout"] == 30


def test_retries_on_429_then_succeeds_respecting_retry_after(transport, monkeypatch):
    sleeps = []
    monkeypatch.setattr("backend.shopify.transport.time.sleep", lambda s: sleeps.append(s))

    throttled = FakeResponse(429, headers={"Retry-After": "3"})
    ok = FakeResponse(200, json_body={"data": {"ok": True}})
    transport.sess.post = MagicMock(side_effect=[throttled, ok])

    data = transport.graphql("query {}", {})

    assert data == {"ok": True}
    assert transport.sess.post.call_count == 2
    assert 3.0 in sleeps


def test_retries_then_aborts_on_persistent_5xx(transport, monkeypatch):
    monkeypatch.setattr("backend.shopify.transport.time.sleep", lambda s: None)
    transport.max_retries = 3
    server_error = FakeResponse(503, text="upstream down")
    transport.sess.post = MagicMock(return_value=server_error)

    with pytest.raises(ShopifyTransportError):
        transport.graphql("query {}", {})

    assert transport.sess.post.call_count == 3


def test_timeout_is_retried_then_raises_clean_error(transport, monkeypatch):
    monkeypatch.setattr("backend.shopify.transport.time.sleep", lambda s: None)
    transport.max_retries = 2
    transport.sess.post = MagicMock(side_effect=requests.exceptions.Timeout("slow upstream"))

    with pytest.raises(ShopifyTransportError, match="attempts"):
        transport.graphql("query {}", {})

    assert transport.sess.post.call_count == 2


def test_request_exception_is_retried_then_raises_clean_error(transport, monkeypatch):
    monkeypatch.setattr("backend.shopify.transport.time.sleep", lambda s: None)
    transport.max_retries = 2
    transport.sess.post = MagicMock(
        side_effect=requests.exceptions.ConnectionError("dns broke")
    )

    with pytest.raises(ShopifyTransportError):
        transport.graphql("query {}", {})

    assert transport.sess.post.call_count == 2


def test_top_level_graphql_errors_raise(transport):
    error_body = FakeResponse(200, json_body={"errors": [{"message": "boom"}]})
    transport.sess.post = MagicMock(return_value=error_body)

    with pytest.raises(ShopifyTransportError, match="boom"):
        transport.graphql("query {}", {})


def test_4xx_other_than_429_raises_without_retry(transport):
    unauthorized = FakeResponse(401, text="invalid token")
    transport.sess.post = MagicMock(return_value=unauthorized)

    with pytest.raises(ShopifyTransportError, match="401"):
        transport.graphql("query {}", {})

    transport.sess.post.assert_called_once()
