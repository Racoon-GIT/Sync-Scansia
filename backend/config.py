"""Env-only configuration for the Scansia Manager backend.

Fail-closed: every required setting must come from the environment. There is
no hardcoded fallback for the Shopify store domain (or anything else) — a
missing required var raises ``ConfigError`` immediately instead of silently
defaulting to a value that could point at the wrong store.

**The Admin API version is pinned** (plan ``docs/f7-mysql-adapter-plan.md`` §7
F7.2, E15): the app is written against ``PINNED_API_VERSION`` —
``@idempotent`` and ``changeFromQuantity`` on every inventory write, explicit
activation, ``includeInactive`` reads, ``productUpdate(product:)``,
``Collection.sources``. ``SHOPIFY_API_VERSION`` may only restate it: any other
value refuses to boot (a version the code was not written against would fail
at the first write, or worse, succeed with other semantics). Re-pinning is a
code change whose precondition is every surface deprecated at the old pin
migrated (S14, §5). Pinned 2026-10-06: ``2026-10`` is "Latest stable", accessible
until 2027-10-16 15:00 UTC (shopify.dev/docs/api/usage/versioning, read that day).

``PROMO_LOCATION_ID`` is only checked for *presence* here. Verifying that it
actually resolves (live, via the Shopify API) to a location named "Promo" is
a startup gate that belongs to M1b, not this module.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

PINNED_API_VERSION = "2026-10"
DEFAULT_API_VERSION = PINNED_API_VERSION


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


def _require(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class ShopifyConfig:
    store: str
    token: str
    api_version: str
    promo_location_id: str


def load_shopify_config() -> ShopifyConfig:
    """Build a ``ShopifyConfig`` from environment variables.

    Raises:
        ConfigError: if ``SHOPIFY_STORE``, ``SHOPIFY_ADMIN_TOKEN``, or
            ``PROMO_LOCATION_ID`` is missing/empty, or if ``SHOPIFY_API_VERSION``
            names a version other than the pinned one.
    """
    requested = (os.environ.get("SHOPIFY_API_VERSION") or "").strip()
    if requested and requested != PINNED_API_VERSION:
        raise ConfigError(
            f"SHOPIFY_API_VERSION must be {PINNED_API_VERSION} (the version this build is written against)"
        )
    return ShopifyConfig(
        store=_require("SHOPIFY_STORE"),
        token=_require("SHOPIFY_ADMIN_TOKEN"),
        api_version=PINNED_API_VERSION,
        promo_location_id=_require("PROMO_LOCATION_ID"),
    )
