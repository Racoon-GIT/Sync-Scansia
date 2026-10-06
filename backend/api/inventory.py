"""Live-inventory JOIN + read projections on the register (PURE — no FastAPI, no
network of its own; plan ``docs/f7-mysql-adapter-plan.md`` §3.1, §3.2).

The compute core behind the READ endpoints: plain functions over injected
collaborators (a register handle, a transport, an audit sink), unit-testable with
in-memory fakes, run OFF the event loop inside the job worker thread.

* :func:`read_shelf` / :func:`pair_to_dict` — the FAST path for ``GET /scansia``:
  the register's shelf (every ``IN_SCANSIA`` pair), projected to JSON-safe dicts
  with the SKU's B-2 compare-at. No live Shopify call.
* :func:`run_inventory_join` / :func:`join_group` — the SLOW path, a background job
  (``POST /scansia/inventory``): per SKU on the shelf, the outlet (the pairs'
  ``OUTLET_PRODUCT_ID`` when it resolves live, else the sole match) and its live
  per-variant inventory, as status CHIPS. A size matches a variant only when the
  titles are EXACTLY equal (§3.2 — the SPs' rule); a match after normalisation only
  is a twin, surfaced for respelling, never counted as a match. Every result
  carries ``fetched_at`` and ``stale`` / ``failed``: a group whose live read raised
  is ``failed`` with NO chips — never read as "sold out" or "in scansia".

Chips (per SKU): ``in_scansia`` (≥ 1 shelf pair) · ``pubblicato`` (the outlet is
ACTIVE) · ``sold_out`` (every real-size Promo ``available`` a known 0) ·
``mismatch`` (no single outlet, or a shelf size with no exact variant) ·
``oversell`` (a CONTINUE variant, or a Promo ``available`` < 0).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from backend.persistence.records import PairRow
from backend.services import flows, resolvers
from backend.services import variants as V
from backend.services.outlet_service import compare_at_of
from backend.shopify import ops
from backend.shopify.ops import ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

_ROME = ZoneInfo("Europe/Rome")

CHIP_IN_SCANSIA = "in_scansia"
CHIP_PUBLISHED = "pubblicato"
CHIP_SOLD_OUT = "sold_out"
CHIP_MISMATCH = "mismatch"
CHIP_OVERSELL = "oversell"

_STATUS_ACTIVE = "ACTIVE"
_DENY = "DENY"

# One SKU's read error never aborts the join: it becomes a non-authoritative result.
_CAUGHT = (ShopifyUserError, ShopifyTransportError, RuntimeError)


def _money(value: Any) -> Optional[str]:
    return None if value is None else f"{value:.2f}"


# =============================================================================
# GET /scansia projection (fast, no live join)
# =============================================================================
def read_shelf(register: Any) -> List[PairRow]:
    """Every ``IN_SCANSIA`` pair, by id (a plain read)."""
    return register.shelf()


def pair_to_dict(p: PairRow) -> Dict[str, Any]:
    """A shelf pair for the client: ids, key, prices (B-2's compare-at), outlet."""
    return {
        "pair_id": p.pair_id,
        "pair_uuid": p.pair_uuid,
        "sku": p.sku,
        "size": p.size,
        "brand": p.brand,
        "product_title": p.product_title,
        "price_high": _money(p.price_high),
        "price": _money(p.price),
        "compare_at": _money(compare_at_of(p)),
        "price_outlet": _money(p.price_outlet),
        "discount": _money(p.discount),
        "outlet_product_id": p.outlet_product_id,
        "online": p.online,
    }


# =============================================================================
# Live inventory JOIN (slow, background job)
# =============================================================================
@dataclass(frozen=True)
class SizeStatus:
    size: str
    n: int                           # shelf pairs of the key
    matched: bool                    # a variant titled exactly SIZE
    promo_available: Optional[int]   # None: no exact variant, or its Promo level absent
    twins: Tuple[str, ...] = ()      # titles equal only after normalisation (respell)


@dataclass(frozen=True)
class JoinResult:
    """One SKU's live-join outcome. ``failed=True`` → NON-authoritative."""

    sku: str
    product_gid: Optional[str]
    live_status: Optional[str]
    chips: Tuple[str, ...]
    sizes: Tuple[SizeStatus, ...]
    pair_ids: Tuple[int, ...]
    warnings: Tuple[str, ...]
    fetched_at: str   # ISO 8601 Europe/Rome
    stale: bool       # data obtained but incomplete (truncation / absent Promo level)
    failed: bool      # the live resolve / read raised


def _by_sku(pairs: List[PairRow]) -> List[Tuple[str, List[PairRow]]]:
    groups: Dict[str, List[PairRow]] = {}
    for p in pairs:
        sku = (p.sku or "").strip()
        if sku:
            groups.setdefault(p.sku, []).append(p)
    return list(groups.items())


def _choose_outlet(matches: List[Dict[str, Any]], shelf: List[PairRow]) -> Optional[Dict[str, Any]]:
    """The pairs' ``OUTLET_PRODUCT_ID`` when it resolved live, else the sole match."""
    ids = {flows.product_gid(p.outlet_product_id) for p in shelf if p.outlet_product_id is not None}
    for m in matches:
        if m.get("product_gid") in ids:
            return m
    return matches[0] if len(matches) == 1 else None


def join_group(sku: str, shelf: List[PairRow], transport: Any, promo_id: str, *,
               now: Callable[[], datetime]) -> JoinResult:
    """Resolve + live-read one SKU and derive its chips (errors isolated to it)."""
    pair_ids = tuple(p.pair_id for p in shelf)
    ts = now().isoformat()
    warnings: List[str] = []
    try:
        res = resolvers.outlet_resolver(transport, sku)
        matches = res.get("matches") or []
        if res.get("warning"):
            warnings.append(res["warning"])
        truncated = bool(res.get("truncated"))
        chosen = _choose_outlet(matches, shelf)
        if chosen is None:
            warnings.append("multi_outlet" if len(matches) > 1 else "no_outlet")
            return JoinResult(sku, None, None, (CHIP_IN_SCANSIA, CHIP_MISMATCH), (), pair_ids, tuple(warnings),
                              ts, truncated, False)
        gid = chosen["product_gid"]
        status = (chosen.get("status") or "").upper()
        chips = {CHIP_IN_SCANSIA}
        if status == _STATUS_ACTIVE:
            chips.add(CHIP_PUBLISHED)
        inv = ops.read_variant_inventory(transport, gid)
        stale = any(v.get("levels_truncated") for v in inv)
        promo_absent = False
        total = 0
        for v in inv:
            if (v.get("inventoryPolicy") or "").upper() != _DENY:
                chips.add(CHIP_OVERSELL)
            if V.is_customisation_only(v):
                continue                                   # rule 7: never in a count
            a = V.promo_available(v, promo_id)
            if a is None:
                promo_absent = True
                continue
            if a < 0:
                chips.add(CHIP_OVERSELL)
            total += max(a, 0)
        counts: Dict[str, int] = {}
        for p in shelf:
            counts[p.size] = counts.get(p.size, 0) + 1
        sizes: List[SizeStatus] = []
        for size, n in counts.items():
            v = V.match_exact(size, inv)
            twin_titles = tuple(V.variant_title(t) for t in V.twins(size, inv))
            if v is None:
                chips.add(CHIP_MISMATCH)
                warnings.append(f"unmatched_size:{size}")
                if twin_titles:
                    warnings.append(f"size_twin:{size}:{'|'.join(twin_titles)}")
            sizes.append(SizeStatus(size, n, v is not None,
                                    V.promo_available(v, promo_id) if v is not None else None, twin_titles))
        if promo_absent:
            stale = True
            warnings.append("promo_level_absent")
        elif not stale and total == 0 and CHIP_OVERSELL not in chips:
            chips.add(CHIP_SOLD_OUT)
        return JoinResult(sku, gid, status, tuple(sorted(chips)), tuple(sizes), pair_ids, tuple(warnings), ts,
                          stale or truncated, False)
    except _CAUGHT as e:
        return JoinResult(sku, None, None, (), (), pair_ids, (f"error:{type(e).__name__}",), ts, True, True)


def run_inventory_join(register: Any, transport: Any, promo_id: str, *,
                       now: Optional[Callable[[], datetime]] = None) -> List[JoinResult]:
    """Read the shelf, group it by SKU, live-join each SKU (errors isolated per SKU)."""
    clock = now or (lambda: datetime.now(_ROME))
    return [join_group(sku, shelf, transport, promo_id, now=clock) for sku, shelf in _by_sku(read_shelf(register))]


def join_result_to_dict(r: JoinResult) -> Dict[str, Any]:
    return {
        "sku": r.sku,
        "product_gid": r.product_gid,
        "live_status": r.live_status,
        "chips": list(r.chips),
        "sizes": [{"size": s.size, "n": s.n, "matched": s.matched, "promo_available": s.promo_available,
                   "twins": list(s.twins)} for s in r.sizes],
        "pair_ids": list(r.pair_ids),
        "warnings": list(r.warnings),
        "fetched_at": r.fetched_at,
        "stale": r.stale,
        "failed": r.failed,
    }


def join_results_to_payload(results: List[JoinResult]) -> Dict[str, Any]:
    return {
        "count": len(results),
        "failed_count": sum(1 for r in results if r.failed),
        "results": [join_result_to_dict(r) for r in results],
    }


# =============================================================================
# GET /audit projection
# =============================================================================
def read_recent_audit(sink: Any, limit: int = 50) -> List[Dict[str, Any]]:
    """The most recent ``AUDIT`` events (``limit`` clamped to 1..500)."""
    safe_limit = max(1, min(int(limit), 500))
    return sink.read_recent(safe_limit)


__all__ = [
    "CHIP_IN_SCANSIA", "CHIP_PUBLISHED", "CHIP_SOLD_OUT", "CHIP_MISMATCH", "CHIP_OVERSELL",
    "SizeStatus", "JoinResult", "read_shelf", "pair_to_dict", "join_group", "run_inventory_join",
    "join_result_to_dict", "join_results_to_payload", "read_recent_audit",
]
