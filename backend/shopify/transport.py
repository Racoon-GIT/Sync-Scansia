"""Unified Shopify GraphQL transport.

One HTTP session, a timeout on every call, the throttle, and two retry policies
(plan ``docs/f7-mysql-adapter-plan.md`` §3.3 rule 9):

* **Queries** (reads) are retried on what may be transient: timeout /
  ``RequestException``, HTTP 5xx, HTTP 429 (``Retry-After``) and a throttled
  answer — a read repeated is harmless.
* **Mutations are never re-posted blind.** A mutation is re-sent only on an
  answer that says it did NOT run: HTTP 429, or HTTP 200 with a top-level error
  whose ``extensions.code`` is ``THROTTLED`` (the bucket must hold the requested
  cost *before execution begins* — shopify.dev GraphQL Admin rate limits,
  2026-10). After a timeout, a ``RequestException``, an HTTP 5xx, an
  unparseable body or an ``INTERNAL_SERVER_ERROR`` the outcome is unknown:
  :class:`ShopifyOutcomeUnknown`, never retried here (the ``RECONCILE`` row stays
  ``OPEN`` for rule 2's recovery). Any other HTTP 4xx or top-level error is a
  refusal before execution: :class:`ShopifyRefused`.

A mutation's request body is built once (:func:`request_body`) so that a
recovery re-send can post the STORED text byte for byte (:meth:`send_body`) —
an idempotency key re-sent with other bytes is a ``MISMATCH`` (rule 2).

Per-mutation ``userErrors`` are NOT handled here (each mutation has its own
shape — an op-wrapper concern). Every response's ``X-Shopify-API-Version`` is
recorded; a value other than the pinned version is logged once (S14, §5: an
expired pin falls forward silently).
"""
from __future__ import annotations

import json
import logging
import math
import time
from typing import Any, Dict, Optional

import requests

from backend.config import ShopifyConfig, load_shopify_config

logger = logging.getLogger("backend.shopify.transport")

DEFAULT_TIMEOUT_SEC = 30
DEFAULT_MIN_INTERVAL_SEC = 0.7
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_AFTER_SEC = 2.0
MAX_THROTTLE_WAIT_SEC = 10.0  # a throttle wait is capped: the lease's TTL budget (lease.py)


class ShopifyTransportError(RuntimeError):
    """Unrecoverable transport-level error (HTTP, network, or GraphQL top-level errors)."""


class ShopifyOutcomeUnknown(ShopifyTransportError):
    """A mutation may or may not have run (timeout, connection error, 5xx,
    unparseable body, ``INTERNAL_SERVER_ERROR``). Never re-posted blind."""


class ShopifyRefused(ShopifyTransportError):
    """The request was refused before execution (HTTP 4xx other than 429, or a
    top-level error other than ``THROTTLED`` / ``INTERNAL_SERVER_ERROR``) —
    nothing ran."""


def request_body(query: str, variables: Dict[str, Any]) -> str:
    """The exact request text: compact JSON, keys in the order given. A
    ``RECONCILE`` row stores it (``REQUEST``) at its first send."""
    return json.dumps({"query": query, "variables": variables}, separators=(",", ":"), ensure_ascii=False)


def is_mutation(query: str) -> bool:
    return query.lstrip().startswith("mutation")


def _codes(errors: Any) -> list:
    out = []
    for e in errors if isinstance(errors, list) else []:
        code = ((e or {}).get("extensions") or {}).get("code")
        if code:
            out.append(str(code))
    return out


class ShopifyTransport:
    """Single HTTP session GraphQL transport with throttle + the two retry policies.

    Store/token/api_version are read from ``backend.config`` (env-only,
    fail-closed, the version pinned) — pass an explicit ``config`` for tests.
    """

    def __init__(self, config: Optional[ShopifyConfig] = None) -> None:
        self.config = config or load_shopify_config()
        self.graphql_url = (
            f"https://{self.config.store}/admin/api/{self.config.api_version}/graphql.json"
        )
        self.sess = requests.Session()
        self.sess.headers.update(
            {
                "X-Shopify-Access-Token": self.config.token,
                "Content-Type": "application/json",
            }
        )
        self.timeout = DEFAULT_TIMEOUT_SEC
        self.min_interval = DEFAULT_MIN_INTERVAL_SEC
        self.max_retries = DEFAULT_MAX_RETRIES
        self._last_call_ts = 0.0
        self.last_api_version: Optional[str] = None
        self._version_warned = False

    def _throttle(self) -> None:
        now = time.time()
        elapsed = now - self._last_call_ts
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

    def _note_version(self, r: Any) -> None:
        served = (getattr(r, "headers", None) or {}).get("X-Shopify-API-Version")
        if not served:
            return
        self.last_api_version = served
        if served != self.config.api_version and not self._version_warned:
            self._version_warned = True
            logger.warning("Shopify served API version %s, pinned %s", served, self.config.api_version)

    @staticmethod
    def _throttle_wait(data: Dict[str, Any]) -> float:
        cost = ((data.get("extensions") or {}).get("cost") or {})
        status = cost.get("throttleStatus") or {}
        try:
            need = float(cost.get("requestedQueryCost")) - float(status.get("currentlyAvailable"))
            rate = float(status.get("restoreRate"))
            wait = math.ceil(need / rate) if rate > 0 else DEFAULT_RETRY_AFTER_SEC
        except (TypeError, ValueError):
            wait = DEFAULT_RETRY_AFTER_SEC
        return min(max(float(wait), 1.0), MAX_THROTTLE_WAIT_SEC)

    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Run a GraphQL document; mutations take the never-blind path."""
        body = request_body(query, variables)
        if is_mutation(query):
            return self.send_body(body)
        return self._query(body)

    # -- reads -----------------------------------------------------------------
    def _query(self, body: str) -> Dict[str, Any]:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            try:
                r = self.sess.post(self.graphql_url, data=body.encode("utf-8"), timeout=self.timeout)
                self._last_call_ts = time.time()
            except requests.exceptions.Timeout as e:
                last_exc = e
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning("Timeout (attempt %d/%d). Retry in %ds", attempt, self.max_retries, backoff)
                time.sleep(backoff)
                continue
            except requests.exceptions.RequestException as e:
                last_exc = e
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning("Request error (attempt %d/%d): %s. Retry in %ds",
                               attempt, self.max_retries, type(e).__name__, backoff)
                time.sleep(backoff)
                continue
            self._note_version(r)
            if r.status_code == 429:
                retry_after = min(float(r.headers.get("Retry-After", DEFAULT_RETRY_AFTER_SEC)), MAX_THROTTLE_WAIT_SEC)
                logger.warning("429 rate limit (attempt %d/%d). Retry in %.2fs", attempt, self.max_retries, retry_after)
                time.sleep(retry_after)
                continue
            if 500 <= r.status_code < 600:
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning("Server error %d (attempt %d/%d). Retry in %ds",
                               r.status_code, attempt, self.max_retries, backoff)
                time.sleep(backoff)
                continue
            if r.status_code >= 400:
                body_text = r.text[:200] if r.text else "no response body"
                raise ShopifyTransportError(f"GraphQL HTTP {r.status_code}: {body_text}")
            data = r.json()
            if "errors" in data:
                if "THROTTLED" in _codes(data["errors"]):
                    time.sleep(self._throttle_wait(data))
                    continue
                raise ShopifyTransportError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        suffix = f": {last_exc}" if last_exc else ""
        raise ShopifyTransportError(f"GraphQL failed after {self.max_retries} attempts{suffix}")

    # -- mutations (rule 9) ----------------------------------------------------------
    def send_body(self, body: str) -> Dict[str, Any]:
        """POST a mutation body as given (a first send or the stored ``REQUEST``).

        Re-sent ONLY after an answer that says it did not run (429 /
        ``THROTTLED``); every other failure is classified, never retried."""
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            try:
                r = self.sess.post(self.graphql_url, data=body.encode("utf-8"), timeout=self.timeout)
                self._last_call_ts = time.time()
            except requests.exceptions.RequestException as e:  # Timeout included
                raise ShopifyOutcomeUnknown(f"mutation outcome unknown: {type(e).__name__}") from None
            self._note_version(r)
            if r.status_code == 429:
                retry_after = min(float(r.headers.get("Retry-After", DEFAULT_RETRY_AFTER_SEC)), MAX_THROTTLE_WAIT_SEC)
                logger.warning("429 on a mutation (did not run; attempt %d/%d). Retry in %.2fs",
                               attempt, self.max_retries, retry_after)
                time.sleep(retry_after)
                continue
            if 500 <= r.status_code < 600:
                raise ShopifyOutcomeUnknown(f"mutation outcome unknown: HTTP {r.status_code}")
            if r.status_code >= 400:
                raise ShopifyRefused(f"mutation refused: HTTP {r.status_code}")
            try:
                data = r.json()
            except ValueError:
                raise ShopifyOutcomeUnknown("mutation outcome unknown: unparseable body") from None
            if "errors" in data:
                codes = _codes(data["errors"])
                if "THROTTLED" in codes:
                    wait = self._throttle_wait(data)
                    logger.warning("THROTTLED mutation (did not run; attempt %d/%d). Retry in %.0fs",
                                   attempt, self.max_retries, wait)
                    time.sleep(wait)
                    continue
                payload = data.get("data") or {}
                if "INTERNAL_SERVER_ERROR" in codes or any(v is not None for v in payload.values()):
                    raise ShopifyOutcomeUnknown(f"mutation outcome unknown: {codes or 'errors'}")
                raise ShopifyRefused(f"mutation refused: {codes or 'errors'}")
            return data["data"]
        raise ShopifyRefused(f"mutation throttled {self.max_retries} times (it never ran)")


__all__ = [
    "ShopifyTransport", "ShopifyTransportError", "ShopifyOutcomeUnknown", "ShopifyRefused",
    "request_body", "is_mutation", "DEFAULT_TIMEOUT_SEC", "DEFAULT_MAX_RETRIES",
]
