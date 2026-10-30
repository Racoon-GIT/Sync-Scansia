"""Variant helpers shared by the outlet flows — pure functions over
``ops.read_variant_inventory`` / ``ops.get_product_variants`` dicts.

* **Exact size match** (plan ``docs/f7-mysql-adapter-plan.md`` §3.2): a pair
  publishes onto a variant only when its ``SIZE`` equals the variant TITLE
  exactly — the SPs' rule (the order sends ``variant_title``, matched
  spelling-exact). A match found only after normalisation (case, comma / dot,
  inner spaces: ``42L`` vs ``42 L``) is a *twin*: surfaced for respelling at the
  source, never published — a ``42L`` pair published on ``42 L`` would sell
  unrecorded. A title match whose variant carries another SKU, or none (D59),
  is not the key's variant either (``foreign_sku``, F1) — same reason: it sells
  unrecorded.
* **Customisation-only variants** (§3.3 rule 7) are identified by option value,
  never by a blank SKU (seven carry a real SKU): any option value or title
  containing "personalizzazione", case-folded — the measured spellings are
  "Solo Personalizzazione", "Sola Personalizzazione", "Solo personalizzazione"
  (``measures/2026-09-30-f7/c7_join_sku_size.csv:2272-2274``). They are
  excluded from every match, count and total — not from normalisation.
* Promo / non-Promo level readers (the quarantine predicate shared with the
  delete flow).
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

# selectedOptions names that carry the shoe size (FIX5: not just size/taglia).
SIZE_OPTION_NAMES = frozenset({"size", "taglia", "numero"})

_CUSTOMISATION_MARK = "personalizzazione"

# How a variant with no SKU is named where ``foreign_sku`` is shown (D59: a blank
# SKU is a mismatch too — its sale CONSUMEs no shelf pair).
BLANK_SKU = "(blank)"


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


def foreign_sku(sku: str, variant: Optional[Dict[str, Any]]) -> Optional[str]:
    """``None`` when the variant's SKU is the pair's ``sku`` as CONSUME compares
    them (``SKU = p_sku``, ``utf8mb4_bin`` PAD SPACE: a trailing space still
    matches, a leading one does not — STOCK_Manager ``docs/scansia-sp-spec.md:40-46``);
    else the variant's SKU, or ``BLANK_SKU`` when it has none (D59). Such a title
    match is not the key's variant (F7.4 review, F1): its order line carries that
    SKU, CONSUME finds no shelf pair and the register never decrements — a phantom
    unit. ``None`` for no variant at all (a size the outlet lacks is not this).
    A blank pair SKU is never a match: CONSUME matches no blank ``p_sku``."""
    if variant is None:
        return None
    raw = str(variant.get("sku") or "")
    vs = raw.rstrip(" ")
    if vs == sku and raw.strip():
        return None
    return vs if raw.strip() else BLANK_SKU


def match_to_raise(sku: str, size: str, variants: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """``match_exact`` for a flow that raises a Promo level: ``None`` on a
    foreign-SKU match too — each flow's "size the outlet lacks" path."""
    v = match_exact(size, variants)
    return None if v is None or foreign_sku(sku, v) else v


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


__all__ = [
    "SIZE_OPTION_NAMES", "BLANK_SKU", "variant_title", "is_customisation_only", "real_size_variants",
    "twin_form", "match_exact", "foreign_sku", "match_to_raise", "twins", "item_id", "promo_level", "promo_available",
    "has_non_promo_stock",
]
