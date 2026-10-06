"""The Promo anchor gate (plan ``docs/f7-mysql-adapter-plan.md`` E18): before a pass
writes anything, ``PROMO_LOCATION_ID`` must resolve, live, to the location Shopify
names "Promo". The README's env examples set the id of "Magazzino": every relative
write would land on the wrong location, every Promo read would find nothing.

One ``locations`` read (paginated) per pass that writes — publish, decrement,
baseline, re-level and every §3.5 holder — before its lease is taken; a refusal
writes nothing. Read-only recons (zero-stock, the inventory join) are not gated.
"""
from __future__ import annotations

from typing import Any

from backend.shopify import ops

PROMO_LOCATION_NAME = "Promo"


class PromoAnchorInvalid(RuntimeError):
    """``PROMO_LOCATION_ID`` does not resolve to the location named "Promo"."""


def require_promo_location(transport: Any, promo_location_id: str) -> None:
    """Raise :class:`PromoAnchorInvalid` unless the id is an active location named
    "Promo" (exact). A failed read propagates (the pass does not start)."""
    for node in ops.get_locations(transport):
        if node.get("id") == promo_location_id:
            if node.get("name") == PROMO_LOCATION_NAME and node.get("isActive", True):
                return
            raise PromoAnchorInvalid("PROMO_LOCATION_ID names another location")
    raise PromoAnchorInvalid("PROMO_LOCATION_ID names no location of the store")


__all__ = ["PROMO_LOCATION_NAME", "PromoAnchorInvalid", "require_promo_location"]
