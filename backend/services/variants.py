"""Variant helpers shared by the outlet flows — pure functions over
``ops.read_variant_inventory`` / ``ops.get_product_variants`` dicts.

* **Exact size match** (plan ``docs/f7-mysql-adapter-plan.md`` §3.2): a pair
  publishes onto a variant only when its ``SIZE`` equals the variant TITLE
  exactly — the SPs' rule (the order sends ``variant_title``, matched
  spelling-exact). A match found only after normalisation (case, comma / dot,
  inner spaces: ``42L`` vs ``42 L``) is a *twin*: surfaced for respelling at the
  source, never published — a ``42L`` pair published on ``42 L`` would sell
  unrecorded.
* **Customisation-only variants** (§3.3 rule 7) are identified by option value,
  never by a blank SKU (seven carry a real SKU): any option value or title
  containing "personalizzazione", case-folded — the measured spellings are
  "Solo Personalizzazione", "Sola Personalizzazione", "Solo personalizzazione"
  (``measures/2026-09-30-f7/c7_join_sku_size.csv:2272-2274``). They are
  excluded from every match, count and total — not from normalisation.
* Promo / non-Promo level readers (the quarantine predicate shared with the
  delete flow).

The ``legacy_*`` helpers are the sheet era's normalised match (``_norm_size``,
FIX5); only the flows not yet rewritten for F7 (init, the inventory join) use
them, and they go with those rewrites.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

# selectedOptions names that carry the shoe size (FIX5: not just size/taglia).
SIZE_OPTION_NAMES = frozenset({"size", "taglia", "numero"})

_CUSTOMISATION_MARK = "personalizzazione"


def variant_title(variant: Dict[str, Any]) -> str:
    """The variant's title as Shopify returns it (an order's ``variant_title``)."""
    return str(variant.get("title") or "")


def is_customisation_only(variant: Dict[str, Any]) -> bool:
    values = [variant_title(variant)] + [
        str(o.get("value") or "") for o in (variant.get("selectedOptions") or [])
    ]
    return any(_CUSTOMISATION_MARK in " ".join(v.casefold().split()) for v in values)


def real_size_variants(variants: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Every variant that is not customisation-only (rule 7)."""
    return [v for v in variants if not is_customisation_only(v)]


def twin_form(size: str) -> str:
    """The normalised form two spellings share when they are twins: case-folded,
    comma → dot, inner whitespace removed (§3.2's list — nothing more)."""
    return re.sub(r"\s+", "", str(size or "").casefold().replace(",", "."))


def match_exact(size: str, variants: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The real-size variant whose title equals ``size`` exactly, or ``None``."""
    for v in real_size_variants(variants):
        if variant_title(v) == size:
            return v
    return None


def twins(size: str, variants: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Real-size variants that match ``size`` ONLY after normalisation."""
    form = twin_form(size)
    return [
        v for v in real_size_variants(variants)
        if variant_title(v) != size and twin_form(variant_title(v)) == form
    ]


def item_id(variant: Optional[Dict[str, Any]]) -> Optional[str]:
    if not variant:
        return None
    if variant.get("inventoryItemId"):
        return variant["inventoryItemId"]
    return (variant.get("inventoryItem") or {}).get("id")


def promo_level(variant: Optional[Dict[str, Any]], promo_id: str) -> Optional[Dict[str, Any]]:
    """The variant's Promo level dict, or ``None`` when the level is ABSENT."""
    if not variant:
        return None
    for lvl in variant.get("levels") or []:
        if lvl.get("location_id") == promo_id:
            return lvl
    return None


def promo_available(variant: Optional[Dict[str, Any]], promo_id: str) -> Optional[int]:
    """Live Promo ``available``, or ``None`` if the Promo level is ABSENT (never
    stocked). ``read_variant_inventory`` never fabricates a 0."""
    lvl = promo_level(variant, promo_id)
    if lvl is None:
        return None
    a = lvl.get("available")
    return a if a is not None else 0


def has_non_promo_stock(variants: Iterable[Dict[str, Any]], promo_id: str) -> bool:
    """True if any variant carries stock at a location != Promo (or its level set
    is truncated == UNKNOWN). Drives the quarantine."""
    for v in variants:
        if v.get("levels_truncated"):
            return True
        for lvl in v.get("levels") or []:
            if lvl.get("location_id") != promo_id:
                if (lvl.get("available") or 0) > 0 or (lvl.get("on_hand") or 0) > 0:
                    return True
    return False


# ---------------------------------------------------------------------------
# legacy — the sheet era's normalised match (init / inventory join only)
# ---------------------------------------------------------------------------
def legacy_norm_size(value: Any) -> str:
    """FIX5 normalisation: case/whitespace/comma + numeric format
    (``"42.0" -> "42"``, ``"42,5" -> "42.5"``)."""
    s = str(value or "").strip().casefold().replace(",", ".")
    s = " ".join(s.split())
    try:
        f = float(s)
    except (TypeError, ValueError):
        return s
    return str(int(f)) if f == int(f) else str(f)


def legacy_extract_norm_size(selected_options: List[Dict[str, Any]]) -> Optional[str]:
    for opt in selected_options or []:
        if (opt.get("name") or "").strip().casefold() in SIZE_OPTION_NAMES:
            return legacy_norm_size(opt.get("value"))
    return None


def legacy_variant_size_index(variants: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for v in variants:
        ns = legacy_extract_norm_size(v.get("selectedOptions") or [])
        if ns is not None and ns not in idx:
            idx[ns] = v
    return idx


def legacy_match_variant(
    norm_size: str, size_index: Dict[str, Dict[str, Any]], variants: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    if norm_size in size_index:
        return size_index[norm_size]
    if norm_size == "" and len(variants) == 1:
        return variants[0]
    return None


__all__ = [
    "SIZE_OPTION_NAMES", "variant_title", "is_customisation_only", "real_size_variants",
    "twin_form", "match_exact", "twins", "item_id", "promo_level", "promo_available",
    "has_non_promo_stock",
    "legacy_norm_size", "legacy_extract_norm_size", "legacy_variant_size_index",
    "legacy_match_variant",
]
