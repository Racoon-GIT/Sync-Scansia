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


# ---------------------------------------------------------------------------
# Rule 9 (F7 §3.3): a mutation is never re-posted blind
# ---------------------------------------------------------------------------
from backend.shopify.transport import (  # noqa: E402
    ShopifyOutcomeUnknown,
    ShopifyRefused,
    request_body,
)

MUTATION = "mutation($x: Int!) { thing(x: $x) { id userErrors { field message } } }"


@pytest.mark.parametrize("exc", [requests.exceptions.Timeout("slow"), requests.exceptions.ConnectionError("rst")])
def test_a_mutation_whose_answer_was_lost_is_unknown_and_sent_once(transport, exc):
    transport.sess.post = MagicMock(side_effect=exc)
    with pytest.raises(ShopifyOutcomeUnknown):
        transport.graphql(MUTATION, {"x": 1})
    transport.sess.post.assert_called_once()


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_a_mutation_answered_5xx_is_unknown_and_sent_once(transport, status):
    transport.sess.post = MagicMock(return_value=FakeResponse(status, text="upstream"))
    with pytest.raises(ShopifyOutcomeUnknown):
        transport.graphql(MUTATION, {"x": 1})
    transport.sess.post.assert_called_once()


def test_a_mutation_answered_internal_server_error_is_unknown(transport):
    body = {"errors": [{"message": "Internal error", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]}
    transport.sess.post = MagicMock(return_value=FakeResponse(200, json_body=body))
    with pytest.raises(ShopifyOutcomeUnknown):
        transport.graphql(MUTATION, {"x": 1})
    transport.sess.post.assert_called_once()


def test_a_mutation_with_partial_data_beside_errors_is_unknown(transport):
    body = {"data": {"thing": {"id": "1", "userErrors": []}}, "errors": [{"message": "late field"}]}
    transport.sess.post = MagicMock(return_value=FakeResponse(200, json_body=body))
    with pytest.raises(ShopifyOutcomeUnknown):
        transport.graphql(MUTATION, {"x": 1})


def test_a_mutation_answered_429_did_not_run_and_is_re_sent(transport, monkeypatch):
    sleeps = []
    monkeypatch.setattr("backend.shopify.transport.time.sleep", lambda s: sleeps.append(s))
    ok = FakeResponse(200, json_body={"data": {"thing": {"id": "1", "userErrors": []}}})
    transport.sess.post = MagicMock(side_effect=[FakeResponse(429, headers={"Retry-After": "2"}), ok])
    assert transport.graphql(MUTATION, {"x": 1}) == {"thing": {"id": "1", "userErrors": []}}
    assert transport.sess.post.call_count == 2 and 2.0 in sleeps


def test_a_throttled_mutation_did_not_run_and_is_re_sent_after_the_restore_wait(transport, monkeypatch):
    sleeps = []
    monkeypatch.setattr("backend.shopify.transport.time.sleep", lambda s: sleeps.append(s))
    throttled = FakeResponse(200, json_body={
        "errors": [{"message": "Throttled", "extensions": {"code": "THROTTLED"}}],
        "extensions": {"cost": {"requestedQueryCost": 110, "throttleStatus": {
            "maximumAvailable": 2000, "currentlyAvailable": 50, "restoreRate": 100}}},
    })
    ok = FakeResponse(200, json_body={"data": {"thing": {"id": "1", "userErrors": []}}})
    transport.sess.post = MagicMock(side_effect=[throttled, ok])
    transport.graphql(MUTATION, {"x": 1})
    assert transport.sess.post.call_count == 2 and sleeps[-1] == 1.0   # ceil((110-50)/100)


def test_a_mutation_throttled_every_time_is_refused_never_unknown(transport, monkeypatch):
    monkeypatch.setattr("backend.shopify.transport.time.sleep", lambda s: None)
    transport.max_retries = 3
    transport.sess.post = MagicMock(return_value=FakeResponse(429))
    with pytest.raises(ShopifyRefused):
        transport.graphql(MUTATION, {"x": 1})
    assert transport.sess.post.call_count == 3


@pytest.mark.parametrize("status", [400, 401, 402, 403, 404, 423])
def test_a_mutation_refused_4xx_is_refused_and_sent_once(transport, status):
    transport.sess.post = MagicMock(return_value=FakeResponse(status, text="no"))
    with pytest.raises(ShopifyRefused):
        transport.graphql(MUTATION, {"x": 1})
    transport.sess.post.assert_called_once()


def test_a_mutation_failing_validation_is_refused(transport):
    body = {"errors": [{"message": "Field 'nope' doesn't exist", "extensions": {"code": "undefinedField"}}]}
    transport.sess.post = MagicMock(return_value=FakeResponse(200, json_body=body))
    with pytest.raises(ShopifyRefused):
        transport.graphql(MUTATION, {"x": 1})


def test_send_body_posts_the_stored_bytes_unchanged(transport):
    stored = '{"query":"mutation { a }","variables":{"z":1,"a":2}}'
    transport.sess.post = MagicMock(return_value=FakeResponse(200, json_body={"data": {"a": 1}}))
    transport.send_body(stored)
    _, kwargs = transport.sess.post.call_args
    assert kwargs["data"] == stored.encode("utf-8") and kwargs["timeout"] == 30


def test_request_body_is_compact_and_keeps_key_order():
    assert request_body("q", {"z": 1, "a": {"y": 2, "b": 3}}) == '{"query":"q","variables":{"z":1,"a":{"y":2,"b":3}}}'


def test_a_query_throttled_is_retried(transport, monkeypatch):
    monkeypatch.setattr("backend.shopify.transport.time.sleep", lambda s: None)
    throttled = FakeResponse(200, json_body={"errors": [{"message": "Throttled", "extensions": {"code": "THROTTLED"}}]})
    ok = FakeResponse(200, json_body={"data": {"shop": {"id": "1"}}})
    transport.sess.post = MagicMock(side_effect=[throttled, ok])
    assert transport.graphql("query { shop { id } }", {}) == {"shop": {"id": "1"}}


@pytest.mark.parametrize("doc", ["query", "mutation"])
@pytest.mark.parametrize("answer", ["429", "throttled"])
def test_a_throttle_wait_is_capped_at_10_s_the_lease_budget(transport, monkeypatch, doc, answer):
    """Gate-2 set V (V49): a Retry-After or a restore wait of 30 s sleeps 10 s — the
    cap ``lease.py``'s 177 s budget counts on (build notes §The reconciliation engine)."""
    sleeps = []
    monkeypatch.setattr("backend.shopify.transport.time.sleep", lambda s: sleeps.append(s))
    if answer == "429":
        first = FakeResponse(429, headers={"Retry-After": "30"})
    else:                                                        # ceil((3050 - 50) / 100) = 30 s to restore
        first = FakeResponse(200, json_body={
            "errors": [{"message": "Throttled", "extensions": {"code": "THROTTLED"}}],
            "extensions": {"cost": {"requestedQueryCost": 3050, "throttleStatus": {
                "maximumAvailable": 4000, "currentlyAvailable": 50, "restoreRate": 100}}},
        })
    ok = FakeResponse(200, json_body={"data": {"thing": {"id": "1", "userErrors": []}}})
    transport.sess.post = MagicMock(side_effect=[first, ok])
    transport.graphql(MUTATION if doc == "mutation" else "query { thing { id } }", {"x": 1})
    assert transport.sess.post.call_count == 2 and sleeps == [10.0]


def test_the_served_api_version_is_recorded_and_a_mismatch_logged_once(transport, caplog):
    served = FakeResponse(200, json_body={"data": {"ok": 1}}, headers={"X-Shopify-API-Version": "2027-01"})
    transport.sess.post = MagicMock(return_value=served)
    with caplog.at_level("WARNING", logger="backend.shopify.transport"):
        transport.graphql("query { ok }", {})
        transport.graphql("query { ok }", {})
    assert transport.last_api_version == "2027-01"
    assert sum("pinned 2026-10" in r.message for r in caplog.records) == 1
