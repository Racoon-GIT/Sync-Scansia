"""Live-inventory JOIN + read projections (PURE — no FastAPI, no network of its own).

This is the compute core behind the READ endpoints. Everything here is a plain
function over injected collaborators (a ``ScansiaSheet``-shaped sheet, a
``ShopifyTransport``-shaped transport, an audit sink) so it is fully unit-testable
with in-memory fakes and runs OFF the event loop inside the job worker thread.

Two surfaces:

* :func:`read_eligible_rows` / :func:`canonrow_to_dict` — the FAST path for
  ``GET /scansia``: one canonical sheet read (``assign_uuids=False`` — DRY, never
  mutates the Sheet) filtered to the eligible rows, projected to JSON-safe dicts.
  No live Shopify call.

* :func:`run_inventory_join` / :func:`join_group` — the SLOW path driven as a
  background job by ``POST /scansia/inventory``. For each eligible SKU group it
  resolves the outlet (``resolvers.outlet_resolver``), reads live per-variant
  inventory (``ops.read_variant_inventory``) and derives the status CHIPS. Every
  result carries a freshness ``fetched_at`` plus ``stale`` / ``failed`` flags: a
  group whose live resolve/read raised is returned ``failed=True`` with NO chips —
  it is explicitly NON-authoritative, never silently treated as "sold out" or
  "in scansia". Throttling is delegated to ``ShopifyTransport`` (its built-in
  ``min_interval`` rate limiter), so the fan-out already respects Shopify limits
  without a second sleep layer.

Chip vocabulary (per SKU group):

* ``in_scansia``  — baseline: the group has >=1 eligible sheet row.
* ``pubblicato``  — the resolved outlet is live ``ACTIVE`` (published-eligible).
* ``sold_out``    — outlet found, every variant's Promo ``available`` is a known 0.
* ``mismatch``    — no single outlet resolves, or a returned size has no variant.
* ``oversell``    — a variant is ``inventoryPolicy=CONTINUE`` or Promo ``available<0``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from backend.gsheet import ScansiaSheet
from backend.gsheet.reader import GSheetError
from backend.services import resolvers
from backend.services.outlet_service import (
    _match_variant,
    _norm_size,
    _promo_available,
    _variant_size_index,
)
from backend.shopify import ops
from backend.shopify.ops import ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

_ROME = ZoneInfo("Europe/Rome")

# Chip identifiers (stable — part of the API contract).
CHIP_IN_SCANSIA = "in_scansia"
CHIP_PUBLISHED = "pubblicato"
CHIP_SOLD_OUT = "sold_out"
CHIP_MISMATCH = "mismatch"
CHIP_OVERSELL = "oversell"

_STATUS_ACTIVE = "ACTIVE"
_DENY = "DENY"

# The same per-group read/resolve error set the services isolate on: one bad
# outlet never aborts the join — it becomes a non-authoritative failed result.
_CAUGHT = (ShopifyUserError, ShopifyTransportError, GSheetError, RuntimeError)


# =============================================================================
# GET /scansia projection (fast, no live join)
# =============================================================================


def read_eligible_rows(sheet: Any) -> List[Any]:
    """Canonically read the sheet (DRY: ``assign_uuids=False``) -> eligible rows.

    ``assign_uuids=False`` guarantees NO row_uuid is minted and NO cell is written
    (fail-closed read). ``eligible_rows`` applies the legacy online=SI AND qta>0
    filter. Raises the typed ``GSheetError`` family upward for the API boundary.
    """
    read = sheet.read_canonical(assign_uuids=False)
    return ScansiaSheet.eligible_rows(read.rows)


def canonrow_to_dict(row: Any) -> Dict[str, Any]:
    """Project a ``CanonRow`` to a JSON-safe dict for the client.

    Deliberately omits ``row_index`` (an internal 1-based write-back address) and
    ``raw`` (the full echo of every sheet column) — the client gets the canonical
    business fields only.
    """
    return {
        "row_uuid": row.row_uuid,
        "sku": row.sku,
        "size": row.size,
        "product_id": row.product_id,
        "prezzo_high": row.prezzo_high,
        "prezzo_outlet": row.prezzo_outlet,
        "qta": row.qta,
        "online": row.online,
        "sconto": row.sconto,
        "reconciled": row.reconciled,
        "anomalies": list(row.anomalies or []),
    }


# =============================================================================
# Live inventory JOIN (slow, background job)
# =============================================================================


@dataclass(frozen=True)
class SizeStatus:
    raw_size: str
    norm_size: str
    matched: bool
    promo_available: Optional[int]  # None => Promo level absent (UNKNOWN)


@dataclass(frozen=True)
class JoinResult:
    """One SKU group's live-join outcome. ``failed=True`` => NON-authoritative."""

    sku: str
    product_gid: Optional[str]
    live_status: Optional[str]
    chips: Tuple[str, ...]
    sizes: Tuple[SizeStatus, ...]
    row_uuids: Tuple[str, ...]
    warnings: Tuple[str, ...]
    fetched_at: str   # ISO8601 Europe/Rome — freshness stamp
    stale: bool       # data obtained but incomplete/UNKNOWN (truncation / absent Promo level)
    failed: bool      # live resolve/read raised — result carries NO authoritative chips


def _group_eligible_by_sku(rows: List[Any]) -> List[Tuple[str, List[Any]]]:
    """Group eligible rows by trimmed SKU, first-seen order; blank SKU dropped."""
    groups: Dict[str, List[Any]] = {}
    order: List[str] = []
    for r in rows:
        sku = (getattr(r, "sku", "") or "").strip()
        if not sku:
            continue
        if sku not in groups:
            groups[sku] = []
            order.append(sku)
        groups[sku].append(r)
    return [(sku, groups[sku]) for sku in order]


def _choose_outlet(
    matches: List[Dict[str, Any]], group_rows: List[Any]
) -> Optional[Dict[str, Any]]:
    """Pick the outlet: a col-Q gid that resolved live, else the sole match."""
    q_gid = next((r.product_id for r in group_rows if getattr(r, "product_id", "")), "")
    if q_gid:
        hit = next((m for m in matches if m.get("product_gid") == q_gid), None)
        if hit is not None:
            return hit
    if len(matches) == 1:
        return matches[0]
    return None


def join_group(
    sku: str,
    group_rows: List[Any],
    transport: Any,
    promo_id: str,
    *,
    now: Callable[[], datetime],
) -> JoinResult:
    """Resolve + live-read one SKU group and derive its chips.

    Any resolve/read error is isolated to THIS group: it returns ``failed=True``
    with empty chips (explicitly non-authoritative) rather than propagating.
    """
    row_uuids = tuple(getattr(r, "row_uuid", "") for r in group_rows)
    ts = now().isoformat()
    warnings: List[str] = []
    try:
        res = resolvers.outlet_resolver(transport, sku)
        matches = res.get("matches") or []
        if res.get("warning"):
            warnings.append(res["warning"])
        # A truncated resolver candidate set (>100 matches, hasNextPage=true) means
        # a second/further outlet page was silently ignored: whatever conclusion
        # follows (mismatch OR a chosen outlet's chips) is NOT fully authoritative.
        truncated = bool(res.get("truncated"))

        chosen = _choose_outlet(matches, group_rows)
        if chosen is None:
            # No single authoritative outlet — a real finding (the resolve
            # succeeded), surfaced as a mismatch, not a failure — UNLESS the
            # candidate set was truncated, in which case it's not authoritative.
            warnings.append("multi_outlet" if len(matches) > 1 else "no_outlet")
            return JoinResult(
                sku=sku, product_gid=None, live_status=None,
                chips=(CHIP_IN_SCANSIA, CHIP_MISMATCH), sizes=(), row_uuids=row_uuids,
                warnings=tuple(warnings), fetched_at=ts, stale=truncated, failed=False,
            )

        gid = chosen["product_gid"]
        status = (chosen.get("status") or "").upper()
        chips = {CHIP_IN_SCANSIA}
        if status == _STATUS_ACTIVE:
            chips.add(CHIP_PUBLISHED)

        inv = ops.read_variant_inventory(transport, gid)
        stale = any(v.get("levels_truncated") for v in inv)

        promo_absent = False
        total_promo = 0
        for v in inv:
            if (v.get("inventoryPolicy") or "").upper() != _DENY:
                chips.add(CHIP_OVERSELL)
            avail = _promo_available(v, promo_id)  # None => Promo level ABSENT
            if avail is None:
                promo_absent = True
                continue
            if avail < 0:
                chips.add(CHIP_OVERSELL)
            total_promo += avail

        # Per returned size (non-reconciled, qta>0): unmatched -> mismatch.
        idx = _variant_size_index(inv)
        sizes: List[SizeStatus] = []
        for r in group_rows:
            if getattr(r, "reconciled", False) or getattr(r, "qta", 0) <= 0:
                continue
            ns = _norm_size(r.size)
            v = _match_variant(ns, idx, inv)
            if v is None:
                chips.add(CHIP_MISMATCH)
                warnings.append(f"unmatched_size:{r.size}")
            sizes.append(
                SizeStatus(
                    raw_size=r.size, norm_size=ns, matched=v is not None,
                    promo_available=_promo_available(v, promo_id) if v is not None else None,
                )
            )

        if promo_absent:
            # A missing Promo level makes any zero/sold-out conclusion unreliable.
            stale = True
            warnings.append("promo_level_absent")
        elif not stale and total_promo == 0 and CHIP_OVERSELL not in chips:
            chips.add(CHIP_SOLD_OUT)

        return JoinResult(
            sku=sku, product_gid=gid, live_status=status,
            chips=tuple(sorted(chips)), sizes=tuple(sizes), row_uuids=row_uuids,
            warnings=tuple(warnings), fetched_at=ts, stale=stale or truncated, failed=False,
        )
    except _CAUGHT as e:
        # Non-authoritative: the live state could not be resolved/read for this
        # outlet. NO chips (never assert sold_out/in_scansia on unknown state).
        return JoinResult(
            sku=sku, product_gid=None, live_status=None, chips=(), sizes=(),
            row_uuids=row_uuids, warnings=(f"error:{type(e).__name__}",),
            fetched_at=ts, stale=True, failed=True,
        )


def run_inventory_join(
    sheet: Any,
    transport: Any,
    promo_id: str,
    *,
    now: Optional[Callable[[], datetime]] = None,
) -> List[JoinResult]:
    """Read eligible rows, group by SKU, and live-join each group.

    Runs inside the background worker thread (all blocking I/O off the event
    loop). A whole-job failure (e.g. the sheet read raises ``CutoverNotDoneError``)
    propagates to the job runner's top-level guard; per-group errors are isolated
    inside :func:`join_group`. Throttling rides on ``ShopifyTransport``'s built-in
    ``min_interval`` limiter — no extra sleep layer here.
    """
    clock = now or (lambda: datetime.now(_ROME))
    rows = read_eligible_rows(sheet)
    return [
        join_group(sku, group_rows, transport, promo_id, now=clock)
        for sku, group_rows in _group_eligible_by_sku(rows)
    ]


def _size_status_to_dict(s: SizeStatus) -> Dict[str, Any]:
    return {
        "raw_size": s.raw_size,
        "norm_size": s.norm_size,
        "matched": s.matched,
        "promo_available": s.promo_available,
    }


def join_result_to_dict(r: JoinResult) -> Dict[str, Any]:
    return {
        "sku": r.sku,
        "product_gid": r.product_gid,
        "live_status": r.live_status,
        "chips": list(r.chips),
        "sizes": [_size_status_to_dict(s) for s in r.sizes],
        "row_uuids": list(r.row_uuids),
        "warnings": list(r.warnings),
        "fetched_at": r.fetched_at,
        "stale": r.stale,
        "failed": r.failed,
    }


def join_results_to_payload(results: List[JoinResult]) -> Dict[str, Any]:
    """Serialize a completed join into the poll-endpoint ``result`` payload."""
    return {
        "count": len(results),
        "failed_count": sum(1 for r in results if r.failed),
        "results": [join_result_to_dict(r) for r in results],
    }


# =============================================================================
# GET /audit projection
# =============================================================================


def read_recent_audit(sink: Any, limit: int = 50) -> List[Dict[str, Any]]:
    """Return the most recent AUDIT events via the injected sink's ``read_recent``.

    ``limit`` is clamped to a sane bound so a caller can never request an
    unbounded read. The sink raises the typed ``GSheetError`` family on I/O
    failure — surfaced to the API boundary, never swallowed.
    """
    safe_limit = max(1, min(int(limit), 500))
    return sink.read_recent(safe_limit)


__all__ = [
    "CHIP_IN_SCANSIA",
    "CHIP_PUBLISHED",
    "CHIP_SOLD_OUT",
    "CHIP_MISMATCH",
    "CHIP_OVERSELL",
    "SizeStatus",
    "JoinResult",
    "read_eligible_rows",
    "canonrow_to_dict",
    "join_group",
    "run_inventory_join",
    "join_result_to_dict",
    "join_results_to_payload",
    "read_recent_audit",
]
