"""The outlet flows on the ledger — plan ``docs/f7-mysql-adapter-plan.md``
§3.3's table, rules 1, 4, 7, 8, and the held-key lowering / N-0 release.

Every function here runs under the pass lease, through a :class:`Context`
(the pass's ledger, transport, Promo location, report). Shopify writes go
through ``RECONCILE`` rows (``ledger.py``); every read before a write is live.
Nothing here decides WHICH groups run — the passes do (``passes.py``).

* :func:`minus_group` — Δ < 0: lower the matched real-size variant's Promo by
  ``min(|Δ|, available)``, relatively, split over the outlets holding the size
  (ACTIVE first); ``available`` ≤ 0 → stamp only, unless Promo ``committed`` > 0
  (then pending, rule 4); no outlet (a CLEAN answer) → stamp; a truncated or
  failed resolution → pending, surfaced; no variant titled exactly ``SIZE`` →
  pending, surfaced, never stamped. Then the product-total rule.
* :func:`zero_group` — Δ = 0: no stock written; the product-total rule; stamp.
* :func:`total_rule` — "product total" = Σ ``available`` of the real-size Promo
  variants, each clamped at 0 (customisation-only never counts); 0 on an ACTIVE
  outlet → DRAFT (F7's own drafting rule), ``ONLINE`` 0 on the SKU's shelf pairs.
* :func:`held_key_lowering` — every held key (not one held only by an open tail
  or normalise row) lowered by clamp(P) + Σ pending + Σ open sent −Δ − N, when
  positive; never raised.
* :func:`n0_release` — a key with N 0, real-size Promo ≤ 0 everywhere, no row
  open, no ``HELD`` row inside the key retention: its events stamped, a per-key
  ``BASELINE`` row (reason "N 0") — nothing written to Shopify.
* :func:`run_tail`, :func:`run_normalise`, :func:`orphan` — the state flows
  recovery and the passes share.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from backend.persistence.records import (
    APPLIED,
    CARRIED,
    DONE,
    FAILED,
    HELD,
    HOLD_IN_FLIGHT,
    KIND_RECONCILE,
    NOT_CLOSED,
    OPEN,
    SCOPE_KEY,
    SUPERSEDED,
    AuditRow,
    EventRow,
    Hold,
    InsertAudit,
    Key,
)
from backend.services import ledger as L
from backend.services import resolvers, variants
from backend.shopify import ops
from backend.shopify.ops import ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.flows")

_GID = "gid://shopify/Product/"


def product_gid(numeric: Optional[int]) -> Optional[str]:
    return f"{_GID}{int(numeric)}" if numeric is not None else None


def product_number(gid: Optional[str]) -> Optional[int]:
    if gid and gid.startswith(_GID) and gid[len(_GID):].isdigit():
        return int(gid[len(_GID):])
    return None


# =============================================================================
# The pass context
# =============================================================================
@dataclass(frozen=True)
class Resolution:
    """An outlet resolution: ``clean`` False = truncated or failed — never "none"."""

    matches: Tuple[Dict[str, Any], ...]
    clean: bool
    warning: Optional[str] = None


class Context:
    """One pass's shared collaborators and per-pass caches."""

    def __init__(self, ledger: L.Ledger, *, publication_id: Optional[str] = None) -> None:
        self.ledger = ledger
        self.register = ledger.register
        self.transport = ledger.transport
        self.promo = ledger.promo_id
        self.report = ledger.report
        self._publication = publication_id

    def publication_id(self) -> str:
        if self._publication is None:
            self._publication = ops.get_online_store_publication_id(self.transport)
        return self._publication

    def resolve(self, sku: str) -> Resolution:
        """The SKU's outlets (``outlet_resolver``); any failure → not clean."""
        if not sku:
            return Resolution((), False, "blank_sku")  # nothing resolves a SKU '' (rule 7)
        try:
            res = resolvers.outlet_resolver(self.transport, sku)
        except (ShopifyTransportError, ShopifyUserError, RuntimeError) as e:
            self.report.surface("resolution_failed", sku=sku, error=type(e).__name__)
            return Resolution((), False, type(e).__name__)
        clean = not res.get("truncated")
        return Resolution(tuple(res.get("matches") or ()), clean, res.get("warning"))

    def inventory(self, gid: str) -> List[Dict[str, Any]]:
        self.ledger.lease.keepalive()
        return ops.read_variant_inventory(self.transport, gid)


def _status(match: Dict[str, Any]) -> str:
    return (match.get("status") or "").upper()


def _active_first(matches: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(matches, key=lambda m: 0 if _status(m) == "ACTIVE" else 1)


def promo_of(variant: Optional[Dict[str, Any]], promo: str) -> Optional[Dict[str, Any]]:
    return variants.promo_level(variant, promo)


def product_total(inv: Sequence[Dict[str, Any]], promo: str) -> int:
    """Σ real-size Promo ``available``, each clamped at 0 (rule 7: customisation-only never counts)."""
    total = 0
    for v in variants.real_size_variants(inv):
        a = variants.promo_available(v, promo)
        total += max(a or 0, 0)
    return total


def _stamp_only(ctx: Context, key: Key, events: Sequence[EventRow], reason: str) -> None:
    ids = sorted({e.event_id for e in events})
    if not ids:
        return
    carrier = InsertAudit(
        kind=KIND_RECONCILE, actor=ctx.register.actor, action=L.A_STAMP, status=DONE,
        group_id=ctx.ledger.new_group(),
        payload={"keys": [list(key)], "event_ids": ids, "reason": reason},
    )
    ctx.register.stamp(ctx.ledger.lease, ids, carrier=carrier)


def maybe_set_outlet_product(ctx: Context, sku: str, res: Resolution) -> None:
    """§3.1: a pass resolving the SKU to exactly one non-``review`` outlet (by
    title AND collection) writes its id on the SKU's shelf pairs that lack it."""
    if not res.clean or len(res.matches) != 1:
        return
    m = res.matches[0]
    if m.get("review") or not (m.get("is_outlet_member") and m.get("title_is_outlet")):
        return
    num = product_number(m.get("product_gid"))
    if num is None:
        return
    lacking = [p.pair_id for p in ctx.register.shelf(sku) if p.outlet_product_id is None]
    if lacking:
        ctx.register.set_outlet_product(ctx.ledger.lease, lacking, num)


# =============================================================================
# The product-total rule
# =============================================================================
def total_rule(ctx: Context, gid: str, status: str, sku: str) -> bool:
    """Draft an ACTIVE outlet whose product total reads 0; ``ONLINE`` 0 on the
    SKU's shelf pairs. Returns whether it drafted."""
    if status != "ACTIVE":
        return False
    try:
        inv = ctx.inventory(gid)
    except ProductNotFound:
        return False
    if product_total(inv, ctx.promo) > 0:
        return False
    ctx.ledger.lease.keepalive()
    try:
        ops.product_update_status(ctx.transport, gid, "DRAFT")
    except (ShopifyUserError, ShopifyTransportError) as e:
        ctx.report.surface("draft_failed", product_id=product_number(gid), error=type(e).__name__)
        return False
    ctx.report.surface("drafted", product_id=product_number(gid), sku=sku)
    online = [p.pair_id for p in ctx.register.shelf(sku) if p.online]
    if online:
        ctx.register.set_online(ctx.ledger.lease, online, False)
    return True


# =============================================================================
# Δ < 0 and Δ = 0
# =============================================================================
def _possibly_applied_before(ctx: Context, event_ids: Set[int]) -> List[int]:
    """FAILED rows marked "possibly applied" over any of these events (rule 2:
    the next row that lowers them records it and is surfaced)."""
    out = []
    for r in ctx.register.reconcile_rows(statuses=(FAILED,)):
        if (r.outcome or {}).get("possibly_applied") and (r.event_ids & event_ids):
            out.append(r.row_id)
    return out


def minus_group(ctx: Context, key: Key, events: Sequence[EventRow], delta: int) -> str:
    """Apply one Δ < 0 group (``delta`` net of ``CARRIED`` shares). Returns the
    outcome: ``stamped`` / ``carried`` / ``waiting`` / ``pending`` / ``stamp_only``."""
    sku, size = key
    ids = {e.event_id for e in events}
    res = ctx.resolve(sku)
    if not res.clean:
        ctx.report.surface("pending_resolution", key=list(key), delta=delta)
        return "pending"
    maybe_set_outlet_product(ctx, sku, res)
    if not res.matches:
        _stamp_only(ctx, key, events, "no_outlet")
        return "stamp_only"
    holders: List[Tuple[Dict[str, Any], Dict[str, Any], int]] = []
    sized: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    committed = False
    for m in _active_first(res.matches):
        inv = ctx.inventory(m["product_gid"])
        v = variants.match_exact(size, inv)
        if v is None:
            continue
        sized.append((m, v))
        lvl = promo_of(v, ctx.promo) or {}
        a = lvl.get("available") or 0
        if a > 0:
            holders.append((m, v, int(a)))
        elif (lvl.get("committed") or 0) > 0:
            committed = True
    if not sized:
        ctx.report.surface("pending_size_absent", key=list(key), delta=delta)
        return "pending"
    if not holders:
        if committed:
            ctx.report.surface("pending_committed", key=list(key), delta=delta)  # rule 4: a held unit
            return "pending"
        _stamp_only(ctx, key, events, "available_le_0")
        for m, _v in sized:
            total_rule(ctx, m["product_gid"], _status(m), sku)
        return "stamp_only"
    remaining = -int(delta)
    specs: List[L.RowSpec] = []
    before = _possibly_applied_before(ctx, ids)
    for m, v, a in holders:
        if remaining <= 0:
            break
        s = min(remaining, a)
        remaining -= s
        specs.append(L.RowSpec(
            action=L.A_ADJUST, keys=(key,), event_ids=tuple(sorted(ids)), purpose=L.P_LOWER, share=-s,
            inventory_item_id=variants.item_id(v), location_id=ctx.promo, variant_id=v.get("id"),
            product_id=product_number(m["product_gid"]),
            extra={"possibly_applied_before": before} if before else {},
        ))
    if before:
        ctx.report.surface("possible_extra_minus_one", key=list(key), predecessors=before)
    rows = ctx.ledger.write_group(specs)
    for r in rows:
        ctx.ledger.send_adjust(r)
    result = ctx.ledger.settle(rows[0].group_id)
    for m, _v, _a in holders:
        total_rule(ctx, m["product_gid"], _status(m), sku)
    return result


def zero_group(ctx: Context, key: Key, events: Sequence[EventRow]) -> str:
    """Δ = 0 (e.g. an outlet-line sale: Shopify moved itself): the product-total
    rule on the outlets having the size, then the stamp."""
    sku, size = key
    res = ctx.resolve(sku)
    if not res.clean:
        ctx.report.surface("pending_resolution", key=list(key), delta=0)
        return "pending"
    maybe_set_outlet_product(ctx, sku, res)
    for m in res.matches:
        inv = ctx.inventory(m["product_gid"])
        if variants.match_exact(size, inv) is not None:
            total_rule(ctx, m["product_gid"], _status(m), sku)
    _stamp_only(ctx, key, events, "delta_zero")
    return "stamp_only"


# =============================================================================
# Holds: the held-key lowering, the N-0 release
# =============================================================================
def _lowering_exempt(holds: Sequence[Hold], key: Key) -> bool:
    """A key held ONLY by an open revive-tail or normalise row moves no quantity:
    it is neither read nor lowered (AB8; S13 still alarms it above N)."""
    mine = [h for h in holds if h.key == key]
    return bool(mine) and all(
        h.kind == HOLD_IN_FLIGHT and h.action in (L.A_TAIL, L.A_NORMALISE) for h in mine
    )


def held_key_lowering(ctx: Context, *, holds: Sequence[Hold], pending: Sequence[EventRow],
                      carried: Sequence[AuditRow]) -> Dict[Key, int]:
    """Lower every held key by its excess (never raise). Returns key → units lowered."""
    keys = sorted({h.key for h in holds if not _lowering_exempt(holds, h.key)})
    if not keys:
        return {}
    shelf_n: Dict[Key, int] = {}
    for p in ctx.register.shelf():
        shelf_n[p.key] = shelf_n.get(p.key, 0) + 1
    open_minus: Dict[Key, int] = {}
    for r in ctx.register.reconcile_rows(statuses=(OPEN,)):
        if L.is_minus(r) and r.sent_at is not None:
            for k in r.keys:
                open_minus[k] = open_minus.get(k, 0) + L.share(r)
    lowered: Dict[Key, int] = {}
    by_sku: Dict[str, List[Key]] = {}
    for k in keys:
        by_sku.setdefault(k[0], []).append(k)
    for sku, sku_keys in by_sku.items():
        res = ctx.resolve(sku)
        if not res.clean:
            ctx.report.surface("held_lowering_unresolved", sku=sku)
            continue
        invs = [(m, ctx.inventory(m["product_gid"])) for m in _active_first(res.matches)]
        for k in sku_keys:
            evs = [e for e in pending if e.key == k]
            pend = sum(e.delta for e in evs) - L.carried_share(carried, k, [e.event_id for e in evs])
            holders = []
            p_total = 0
            for m, inv in invs:
                v = variants.match_exact(k[1], inv)
                a = variants.promo_available(v, ctx.promo) if v else None
                if a is not None and a > 0:
                    holders.append((m, v, a))
                    p_total += a
            excess = p_total + pend + open_minus.get(k, 0) - shelf_n.get(k, 0)
            if excess <= 0 or not holders:
                continue
            specs = []
            left = excess
            for m, v, a in holders:
                if left <= 0:
                    break
                take = min(left, a)
                left -= take
                specs.append(L.RowSpec(
                    action=L.A_ADJUST, keys=(k,), purpose=L.P_EXCESS, target=a - take,
                    inventory_item_id=variants.item_id(v), location_id=ctx.promo, variant_id=v.get("id"),
                    product_id=product_number(m["product_gid"]),
                ))
            rows = ctx.ledger.write_group(specs)
            for r in rows:
                ctx.ledger.send_adjust(r)
            ctx.ledger.settle(rows[0].group_id)
            lowered[k] = excess - left
            ctx.report.surface("held_key_lowered", key=list(k), units=excess - left)
    return lowered


def n0_release(ctx: Context) -> List[Key]:
    """The N-0 release (§3.3), before grouping. Reads the events FIRST, N after:
    a pair arriving in between is not among the events read, and N counts it."""
    events = ctx.register.unstamped_events()
    by_key: Dict[Key, List[EventRow]] = {}
    for e in events:
        if e.key is not None:
            by_key.setdefault(e.key, []).append(e)
    if not by_key:
        return []
    shelf_keys = {p.key for p in ctx.register.shelf()}
    candidates = sorted(k for k in by_key if k not in shelf_keys)
    if not candidates:
        return []
    now = ctx.register.server_now()
    blocked: Set[Key] = set()
    for r in ctx.register.reconcile_rows(statuses=tuple(NOT_CLOSED)):
        blocked.update(r.keys)
    for r in ctx.register.reconcile_rows(statuses=(HELD,)):
        if r.sent_at is not None and now < r.sent_at + L.KEY_RETENTION:
            blocked.update(r.keys)  # a stalled +Δ could still land
    holds = {h.key for h in ctx.register.held_keys()}
    released: List[Key] = []
    resolutions: Dict[str, Resolution] = {}
    for k in candidates:
        if k in blocked:
            continue
        sku, size = k
        res = resolutions.get(sku) or ctx.resolve(sku)
        resolutions[sku] = res
        if not res.clean:
            continue
        positive = False
        for m in res.matches:
            v = variants.match_exact(size, ctx.inventory(m["product_gid"]))
            lvl = promo_of(v, ctx.promo) if v else None
            # A unit only held (committed: a draft order, a transfer) reads 0 and
            # returns to sale when the hold ends — rule 4 keeps its −1 pending, so
            # the release waits too (the lane's reading; reported to the orchestrator).
            if lvl is not None and ((lvl.get("available") or 0) > 0 or (lvl.get("committed") or 0) > 0):
                positive = True
                break
        if positive:
            continue
        ids = sorted(e.event_id for e in by_key[k])
        ctx.register.record_baseline(
            ctx.ledger.lease, scope=SCOPE_KEY, action="n0-release",
            payload={"key": list(k), "reason": "N 0", "released": k in holds, "stamped": ids},
            stamp_ids=ids,
        )
        released.append(k)
        ctx.report.surface("n0_released", key=list(k), events=len(ids))
    return released


# =============================================================================
# State flows shared by the passes and recovery
# =============================================================================
def zeroing_applied(ctx: Context, group_id: int) -> Optional[bool]:
    """A revive's gate: True once every zeroing row of the group has applied
    (``APPLIED``/``DONE``/``CARRIED``, or ``SUPERSEDED`` by a successor that
    has); False once one ended ``FAILED`` (or ``HELD``); None while one is open."""
    rows = {r.row_id: r for r in ctx.register.reconcile_rows(group_id=group_id)}
    state: Optional[bool] = True
    for r in rows.values():
        if not (r.action == L.A_ADJUST and L.purpose(r) == L.P_ZERO):
            continue
        cur = r
        seen = set()
        while cur.status == SUPERSEDED and (cur.outcome or {}).get("successor") in rows and cur.row_id not in seen:
            seen.add(cur.row_id)
            cur = rows[(cur.outcome or {})["successor"]]
        if cur.status in (APPLIED, DONE, CARRIED):
            continue
        if cur.status in (FAILED, HELD, SUPERSEDED):
            return False
        state = None
    return state


def run_tail(ctx: Context, row: AuditRow) -> str:
    """A revive's tail — DENY + tracked bulk update (with the prices), ACTIVE,
    publish — idempotent by state; ``userErrors`` → ``FAILED`` (terminal path (i))."""
    gate = zeroing_applied(ctx, row.group_id)
    if gate is False or (gate is None and row.sent_at is None):
        ctx.report.surface("tail_never_run", row_id=row.row_id, product_id=row.product_id)
        return ctx.ledger.close(row, FAILED, never_run=True)
    if gate is None:
        return OPEN
    gid = product_gid(row.product_id)
    p = L.payload(row)
    try:
        inv = ctx.inventory(gid)
        extra = {}
        if p.get("price") is not None:
            extra = {"price": p.get("price"), "compareAtPrice": p.get("compare_at")}
        ctx.ledger.step(row, "1:bulk", lambda: ops.product_variants_bulk_update(
            ctx.transport, gid, ops.deny_tracked_inputs(inv, **extra)))
        ctx.ledger.step(row, "2:active", lambda: ops.product_update_status(ctx.transport, gid, "ACTIVE"))
        pub = ctx.publication_id()
        ctx.ledger.step(row, "3:publish", lambda: ops.product_publish(ctx.transport, gid, pub))
    except ShopifyUserError as e:
        ctx.report.surface("tail_refused", row_id=row.row_id, product_id=row.product_id, mutation=e.mutation)
        return ctx.ledger.close(ctx.ledger.reload(row), FAILED, refused=e.mutation)
    except ProductNotFound:
        return ctx.ledger.close(ctx.ledger.reload(row), FAILED, reason="product_gone")
    except ShopifyTransportError as e:
        # unknown or refused before execution: the row stays OPEN (sent), re-run
        # by the next publish-class recovery — idempotent by state
        ctx.report.surface("tail_outcome_unknown", row_id=row.row_id, error=type(e).__name__)
        return OPEN
    return ctx.ledger.close(ctx.ledger.reload(row), APPLIED, moved=0)


def _not_deny_tracked(inv: Sequence[Dict[str, Any]]) -> bool:
    return any((v.get("inventoryPolicy") or "").upper() != "DENY" or v.get("tracked") is False for v in inv)


def run_normalise(ctx: Context, row: AuditRow) -> str:
    """DENY + tracked on every variant of the product (rule 7) — with the SKU's
    price and compare-at when the row carries them (the publish's ACTIVE branch:
    the sheet era refreshed them on every variant). A refusal closes ``FAILED``
    and, on an ACTIVE product still carrying a variant not DENY + tracked, drafts
    it (terminal path (i))."""
    gid = product_gid(row.product_id)
    p = L.payload(row)
    prices = {"price": p["price"], "compareAtPrice": p.get("compare_at")} if p.get("price") is not None else {}
    try:
        inv = ctx.inventory(gid)
        if prices or _not_deny_tracked(inv):
            ctx.ledger.step(row, "1:normalise", lambda: ops.product_variants_bulk_update(
                ctx.transport, gid, ops.deny_tracked_inputs(inv, **prices)))
    except ShopifyUserError as e:
        status = ctx.ledger.close(ctx.ledger.reload(row), FAILED, refused=e.mutation)
        draft_if_unsafe(ctx, gid)
        return status
    except ProductNotFound:
        return ctx.ledger.close(ctx.ledger.reload(row), FAILED, reason="product_gone")
    except ShopifyTransportError as e:
        ctx.report.surface("normalise_outcome_unknown", row_id=row.row_id, error=type(e).__name__)
        return OPEN
    return ctx.ledger.close(ctx.ledger.reload(row), APPLIED, moved=0)


def draft_if_unsafe(ctx: Context, gid: str) -> None:
    """Rule 7's forbidden case: an ACTIVE outlet with a variant not DENY + tracked → DRAFT."""
    try:
        core = ops.get_product_core(ctx.transport, gid)
        if (core.get("status") or "").upper() != "ACTIVE":
            return
        if _not_deny_tracked(ctx.inventory(gid)):
            ops.product_update_status(ctx.transport, gid, "DRAFT")
            ctx.report.surface("drafted_unsafe_variants", product_id=product_number(gid))
    except (ShopifyUserError, ShopifyTransportError, ProductNotFound) as e:
        ctx.report.surface("draft_unsafe_failed", product_id=product_number(gid), error=type(e).__name__)


def zero_rows_for(ctx: Context, inv: Sequence[Dict[str, Any]], *, keys: Sequence[Key], product: Optional[int],
                  purpose: str, include_nonpromo: bool = False,
                  which: Optional[Any] = None) -> List[L.RowSpec]:
    """Lowering-to-0 rows (event-less, relative) for the variants ``which``
    selects (default every variant) with Promo ``available`` > 0; with
    ``include_nonpromo``, every other location's ``available`` > 0 too, plus its
    deactivation (sent after that level's zero row applied)."""
    specs: List[L.RowSpec] = []
    for v in inv:
        if which is not None and not which(v):
            continue
        item = variants.item_id(v)
        for lvl in v.get("levels") or []:
            loc = lvl.get("location_id")
            a = lvl.get("available") or 0
            if loc == ctx.promo:
                if a > 0:
                    specs.append(L.RowSpec(action=L.A_ADJUST, keys=tuple(keys), purpose=purpose, target=0,
                                           inventory_item_id=item, location_id=loc, variant_id=v.get("id"),
                                           product_id=product))
            elif include_nonpromo:
                if a > 0:
                    specs.append(L.RowSpec(action=L.A_ADJUST, keys=tuple(keys), purpose=L.P_NONPROMO, target=0,
                                           inventory_item_id=item, location_id=loc, variant_id=v.get("id"),
                                           product_id=product))
                specs.append(L.RowSpec(action=L.A_DEACTIVATE, keys=tuple(keys), inventory_item_id=item,
                                       location_id=loc, variant_id=v.get("id"), product_id=product))
    return specs


def run_rows_in_order(ctx: Context, rows: Sequence[AuditRow]) -> Dict[int, str]:
    """Send a group's inventory rows in written order; a deactivation only once
    its level's zero row (same item + location) has applied."""
    done: Dict[int, str] = {}
    zeroed: Dict[Tuple[str, str], str] = {}
    for r in rows:
        p = L.payload(r)
        if r.action == L.A_ADJUST:
            st = ctx.ledger.send_adjust(r)
            zeroed[(p.get("inventory_item_id"), p.get("location_id"))] = st
        elif r.action == L.A_DEACTIVATE:
            prior = zeroed.get((p.get("inventory_item_id"), p.get("location_id")))
            if prior not in (None, APPLIED):
                st = ctx.ledger.never_sent(r, "its_zero_row_did_not_apply")
            else:
                st = ctx.ledger.send_deactivate(r)
        elif r.action == L.A_ACTIVATE:
            st = ctx.ledger.send_activate(r)
        else:
            continue
        done[r.row_id] = st
    return done


def orphan(ctx: Context, create_row: AuditRow, gid: str) -> None:
    """A CREATE failed after step 1: the orphan is drafted, every Promo variant
    lowered to 0 relatively, its non-Promo stock zeroed and disconnected."""
    try:
        ops.product_update_status(ctx.transport, gid, "DRAFT")
    except (ShopifyUserError, ShopifyTransportError) as e:
        ctx.report.surface("orphan_draft_failed", product_id=product_number(gid), error=type(e).__name__)
    try:
        inv = ctx.inventory(gid)
    except ProductNotFound:
        return  # gone: nothing to zero; its step rows close by terminal path (ii)
    specs = zero_rows_for(ctx, inv, keys=create_row.keys, product=product_number(gid),
                          purpose=L.P_ORPHAN, include_nonpromo=True)
    if specs:
        rows = ctx.ledger.write_group(specs)
        run_rows_in_order(ctx, rows)
        ctx.ledger.settle(rows[0].group_id)
    _close_create_rows(ctx, create_row)
    ctx.report.surface("orphan_drafted_and_zeroed", product_id=product_number(gid), keys=[list(k) for k in create_row.keys])


def _close_create_rows(ctx: Context, create_row: AuditRow) -> None:
    """"A CREATE's rows follow the CREATE path" (§3.3 rule 2's Groups): once its
    orphan is drafted and zeroed from live, the CREATE's own step rows still open
    are closed, never sent again — a re-sent add would land on the drafted orphan.
    Never sent → ``FAILED`` "never sent"; a sent +Δ → ``HELD`` (a stalled one may
    still land: its key stays held, the N-0 release waits out its retention);
    any other sent row → ``FAILED`` (the orphan path re-read live and acted)."""
    for r in ctx.register.reconcile_rows(group_id=create_row.group_id, statuses=(OPEN,)):
        if r.row_id == create_row.row_id:
            continue
        if r.sent_at is None:
            ctx.ledger.never_sent(r, "create_unfinished")
        elif L.is_plus(r):
            ctx.ledger.close(r, HELD, reason="create_unfinished")
        else:
            ctx.ledger.close(r, FAILED, reason="create_unfinished")


__all__ = [
    "Context", "Resolution", "product_gid", "product_number", "product_total", "total_rule",
    "minus_group", "zero_group", "held_key_lowering", "n0_release", "zeroing_applied",
    "run_tail", "run_normalise", "draft_if_unsafe", "zero_rows_for", "run_rows_in_order", "orphan",
    "maybe_set_outlet_product",
]
