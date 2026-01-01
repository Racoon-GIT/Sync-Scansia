"""
Custom exceptions per Sync-Scansia.
"""


class SyncScansiaError(Exception):
    """Base exception per tutti gli errori Sync-Scansia."""
    pass


class ShopifyAPIError(SyncScansiaError):
    """Errore generico API Shopify."""
    pass


class ShopifyRateLimitError(ShopifyAPIError):
    """Rate limit Shopify raggiunto."""
    pass


class ShopifyServerError(ShopifyAPIError):
    """Errore server Shopify (5xx)."""
    pass


class ProductNotFoundError(SyncScansiaError):
    """Prodotto non trovato."""
    pass


class VariantNotFoundError(SyncScansiaError):
    """Variante non trovata."""
    pass


class InvalidSKUError(SyncScansiaError):
    """SKU non valido."""
    pass


class InvalidQuantityError(SyncScansiaError):
    """Quantità non valida."""
    pass


class LocationNotFoundError(SyncScansiaError):
    """Location Shopify non trovata."""
    pass


class GoogleSheetsError(SyncScansiaError):
    """Errore Google Sheets."""
    pass


class InventoryError(SyncScansiaError):
    """Errore gestione inventory."""
    pass
