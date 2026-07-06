"""backend.config: env-only, fail-closed configuration."""
from __future__ import annotations

import pytest

from backend.config import DEFAULT_API_VERSION, ConfigError, load_shopify_config


def test_missing_shopify_store_raises(monkeypatch, shopify_env):
    monkeypatch.delenv("SHOPIFY_STORE", raising=False)
    with pytest.raises(ConfigError, match="SHOPIFY_STORE"):
        load_shopify_config()


def test_missing_admin_token_raises(monkeypatch, shopify_env):
    monkeypatch.delenv("SHOPIFY_ADMIN_TOKEN", raising=False)
    with pytest.raises(ConfigError, match="SHOPIFY_ADMIN_TOKEN"):
        load_shopify_config()


def test_missing_promo_location_id_raises(monkeypatch, shopify_env):
    monkeypatch.delenv("PROMO_LOCATION_ID", raising=False)
    with pytest.raises(ConfigError, match="PROMO_LOCATION_ID"):
        load_shopify_config()


def test_blank_shopify_store_raises(monkeypatch, shopify_env):
    """Whitespace-only value must be treated as missing, not as a valid store."""
    monkeypatch.setenv("SHOPIFY_STORE", "   ")
    with pytest.raises(ConfigError, match="SHOPIFY_STORE"):
        load_shopify_config()


def test_api_version_defaults_when_absent(shopify_env):
    cfg = load_shopify_config()
    assert cfg.api_version == DEFAULT_API_VERSION == "2025-07"


def test_api_version_from_env_overrides_default(monkeypatch, shopify_env):
    monkeypatch.setenv("SHOPIFY_API_VERSION", "2024-10")
    cfg = load_shopify_config()
    assert cfg.api_version == "2024-10"


def test_no_hardcoded_store_fallback(monkeypatch, shopify_env):
    """Regression: legacy sync.py/reorder_collection.py fell back to
    'racoon-lab.myshopify.com' when SHOPIFY_STORE was unset. That fallback
    must be gone — a missing SHOPIFY_STORE must always raise, never resolve
    to a hardcoded store domain."""
    monkeypatch.delenv("SHOPIFY_STORE", raising=False)
    with pytest.raises(ConfigError):
        cfg = load_shopify_config()
        assert cfg.store != "racoon-lab.myshopify.com"
