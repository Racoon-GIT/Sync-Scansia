"""Env-only configuration for the Scansia Manager backend.

Fail-closed: every required setting must come from the environment. There is
no hardcoded fallback for the Shopify store domain (or anything else) — a
missing required var raises ``ConfigError`` immediately instead of silently
defaulting to a value that could point at the wrong store.

``PROMO_LOCATION_ID`` is only checked for *presence* here. Verifying that it
actually resolves (live, via the Shopify API) to a location named "Promo" is
a startup gate that belongs to M1b, not this module.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_API_VERSION = "2025-07"


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
            ``PROMO_LOCATION_ID`` is missing/empty. ``SHOPIFY_API_VERSION``
            is optional and defaults to ``DEFAULT_API_VERSION`` (not a
            secret, safe to default).
    """
    return ShopifyConfig(
        store=_require("SHOPIFY_STORE"),
        token=_require("SHOPIFY_ADMIN_TOKEN"),
        api_version=(os.environ.get("SHOPIFY_API_VERSION") or DEFAULT_API_VERSION).strip(),
        promo_location_id=_require("PROMO_LOCATION_ID"),
    )
