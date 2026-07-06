"""Stateless service-layer resolvers over backend.shopify transport/ops.

Currently the SKU->product resolvers (outlet vs source) that replace the fragile
legacy ``find_outlet_by_sku`` / ``find_product_by_sku_non_outlet`` heuristics.
"""
from backend.services.resolvers import (
    OUTLET_COLLECTION_GID,
    outlet_resolver,
    source_resolver,
)

__all__ = ["outlet_resolver", "source_resolver", "OUTLET_COLLECTION_GID"]

# NOTE: outlet_service is intentionally NOT imported here — it pulls in the
# transport/config chain. Import it directly (`from backend.services import
# outlet_service`) at the call site to keep this package import side-effect free.
