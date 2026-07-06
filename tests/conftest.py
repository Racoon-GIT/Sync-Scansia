"""Shared pytest fixtures for the Scansia Manager backend test suite."""
from __future__ import annotations

import pytest

from backend.config import ShopifyConfig


@pytest.fixture
def shopify_env(monkeypatch):
    """Set the minimal required Shopify env vars (config-level, fail-closed check)."""
    monkeypatch.setenv("SHOPIFY_STORE", "test-store.myshopify.com")
    monkeypatch.setenv("SHOPIFY_ADMIN_TOKEN", "shpat_test_token")
    monkeypatch.setenv("PROMO_LOCATION_ID", "gid://shopify/Location/123")
    monkeypatch.delenv("SHOPIFY_API_VERSION", raising=False)


@pytest.fixture
def shopify_config() -> ShopifyConfig:
    """A ready-made config object for transport tests — bypasses env vars entirely."""
    return ShopifyConfig(
        store="test-store.myshopify.com",
        token="shpat_test_token",
        api_version="2025-07",
        promo_location_id="gid://shopify/Location/123",
    )
