"""Unified Shopify GraphQL transport.

Merges the two legacy clients into one:
  - ``src/sync.py`` ``Shopify.graphql``/``_request`` (no timeout, REST helpers
    alongside GraphQL)
  - ``src/reorder_collection.py`` ``ShopifyCollectionReorder.graphql``
    (``timeout=30`` + clean ``requests.exceptions.Timeout``/``RequestException``
    handling)

This class adopts the best of both: the reorder client's timeout + exception
handling (the fix — ``sync.py``'s transport has no timeout today) plus the
shared throttle/retry policy (429 respecting ``Retry-After``, 5xx backoff).

GraphQL-only by design: no generic REST helper (``_get``/``_post``/``_put``/
``_delete``) is ported here. Legacy op methods that call REST endpoints today
(inventory connect/set/delete-level, ``get_location_by_name``, ``delete_collects``,
...) are out of scope for this step; they migrate to GraphQL op-by-op in
``backend/shopify/ops.py`` per the M1a method-migration map. There is no REST
code path inside this transport to flag with a TODO.

Per-mutation ``userErrors`` are NOT handled here — that's an op-wrapper
concern (each mutation has its own ``userErrors`` shape). This transport
raises only on HTTP errors, network errors/timeouts after retries exhausted,
and top-level GraphQL ``errors``.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

import requests

from backend.config import ShopifyConfig, load_shopify_config

logger = logging.getLogger("backend.shopify.transport")

DEFAULT_TIMEOUT_SEC = 30
DEFAULT_MIN_INTERVAL_SEC = 0.7
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_AFTER_SEC = 2.0  # legacy default: sync.py used 1.0, reorder used 2.0 — see report


class ShopifyTransportError(RuntimeError):
    """Unrecoverable transport-level error (HTTP, network, or GraphQL top-level errors)."""


class ShopifyTransport:
    """Single HTTP session GraphQL transport with throttle + retry.

    Store/token/api_version are read from ``backend.config`` (env-only,
    fail-closed) — pass an explicit ``config`` for tests, otherwise one is
    loaded from the environment.
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

    def _throttle(self) -> None:
        now = time.time()
        elapsed = now - self._last_call_ts
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Run a GraphQL query/mutation with throttle + retry.

        Retries (up to ``max_retries`` attempts) on:
          - HTTP 429 — sleeps ``Retry-After`` seconds (default
            ``DEFAULT_RETRY_AFTER_SEC`` if the header is absent).
          - HTTP 5xx — exponential backoff capped at 8s.
          - ``requests.exceptions.Timeout`` / ``RequestException`` — same
            exponential backoff.

        Raises ``ShopifyTransportError`` on:
          - HTTP 4xx other than 429.
          - top-level GraphQL ``errors``.
          - retries exhausted (429/5xx/timeout/request-error persisting).
        """
        last_exc: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            self._throttle()

            try:
                r = self.sess.post(
                    self.graphql_url,
                    json={"query": query, "variables": variables},
                    timeout=self.timeout,
                )
                self._last_call_ts = time.time()
            except requests.exceptions.Timeout as e:
                last_exc = e
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning(
                    "Timeout (attempt %d/%d). Retry in %ds", attempt, self.max_retries, backoff
                )
                time.sleep(backoff)
                continue
            except requests.exceptions.RequestException as e:
                last_exc = e
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning(
                    "Request error (attempt %d/%d): %s. Retry in %ds",
                    attempt,
                    self.max_retries,
                    e,
                    backoff,
                )
                time.sleep(backoff)
                continue

            if r.status_code == 429:
                retry_after = float(r.headers.get("Retry-After", DEFAULT_RETRY_AFTER_SEC))
                logger.warning(
                    "429 rate limit (attempt %d/%d). Retry in %.2fs",
                    attempt,
                    self.max_retries,
                    retry_after,
                )
                time.sleep(retry_after)
                continue

            if 500 <= r.status_code < 600:
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning(
                    "Server error %d (attempt %d/%d). Retry in %ds",
                    r.status_code,
                    attempt,
                    self.max_retries,
                    backoff,
                )
                time.sleep(backoff)
                continue

            if r.status_code >= 400:
                body = r.text[:200] if r.text else "no response body"
                raise ShopifyTransportError(f"GraphQL HTTP {r.status_code}: {body}")

            data = r.json()
            if "errors" in data:
                raise ShopifyTransportError(f"GraphQL errors: {data['errors']}")
            return data["data"]

        suffix = f": {last_exc}" if last_exc else ""
        raise ShopifyTransportError(f"GraphQL failed after {self.max_retries} attempts{suffix}")
