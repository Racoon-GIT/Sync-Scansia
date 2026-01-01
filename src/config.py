"""
Configurazione centralizzata per Sync-Scansia.
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ShopifyConfig:
    """Configurazione Shopify."""
    store: str
    token: str
    api_version: str = "2025-01"
    min_interval: float = 0.7
    max_retries: int = 5
    rest_min_interval: float = 0.12
    gql_min_interval: float = 0.12


@dataclass
class LocationConfig:
    """Configurazione location Shopify."""
    promo_name: str
    magazzino_name: str


@dataclass
class GoogleConfig:
    """Configurazione Google Sheets."""
    sheet_id: str
    worksheet_title: str
    credentials_json: Optional[str] = None
    credentials_file: Optional[str] = None


@dataclass
class FeatureFlags:
    """Feature flags per funzionalità opzionali."""
    enable_variant_reset: bool = True
    enable_channel_restriction: bool = True
    enable_batch_image_upload: bool = True
    enable_location_cache: bool = True


@dataclass
class PerformanceConfig:
    """Configurazione performance."""
    batch_size_images: int = 10
    batch_size_metafields: int = 20
    inventory_propagation_delay: float = 1.5
    image_upload_delay: float = 0.15
    location_cache_file: str = "/tmp/shopify_locations_cache.json"


@dataclass
class Config:
    """Configurazione completa applicazione."""
    shopify: ShopifyConfig
    locations: LocationConfig
    google: GoogleConfig
    features: FeatureFlags
    performance: PerformanceConfig
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        """Carica configurazione da variabili d'ambiente."""
        shopify = ShopifyConfig(
            store=os.getenv("SHOPIFY_STORE", ""),
            token=os.getenv("SHOPIFY_ADMIN_TOKEN", ""),
            api_version=os.getenv("SHOPIFY_API_VERSION", "2025-01"),
            min_interval=float(os.getenv("SHOPIFY_MIN_INTERVAL_SEC", "0.7")),
            max_retries=int(os.getenv("SHOPIFY_MAX_RETRIES", "5")),
        )

        locations = LocationConfig(
            promo_name=os.getenv("PROMO_LOCATION_NAME", ""),
            magazzino_name=os.getenv("MAGAZZINO_LOCATION_NAME", ""),
        )

        google = GoogleConfig(
            sheet_id=os.getenv("GSPREAD_SHEET_ID", ""),
            worksheet_title=os.getenv("GSPREAD_WORKSHEET_TITLE", ""),
            credentials_json=os.getenv("GOOGLE_CREDENTIALS_JSON"),
            credentials_file=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        )

        features = FeatureFlags(
            enable_variant_reset=os.getenv("ENABLE_VARIANT_RESET", "true").lower() in ("true", "1", "yes"),
            enable_channel_restriction=os.getenv("ENABLE_CHANNEL_RESTRICTION", "true").lower() in ("true", "1", "yes"),
            enable_batch_image_upload=os.getenv("ENABLE_BATCH_IMAGE_UPLOAD", "true").lower() in ("true", "1", "yes"),
            enable_location_cache=os.getenv("ENABLE_LOCATION_CACHE", "true").lower() in ("true", "1", "yes"),
        )

        performance = PerformanceConfig(
            batch_size_images=int(os.getenv("BATCH_SIZE_IMAGES", "10")),
            batch_size_metafields=int(os.getenv("BATCH_SIZE_METAFIELDS", "20")),
            inventory_propagation_delay=float(os.getenv("INVENTORY_PROPAGATION_DELAY", "1.5")),
            image_upload_delay=float(os.getenv("IMAGE_UPLOAD_DELAY", "0.15")),
            location_cache_file=os.getenv("LOCATION_CACHE_FILE", "/tmp/shopify_locations_cache.json"),
        )

        return cls(
            shopify=shopify,
            locations=locations,
            google=google,
            features=features,
            performance=performance,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


# Istanza globale configurazione
config = Config.from_env()
