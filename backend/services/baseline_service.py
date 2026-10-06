""""Inizializza" on MySQL — the baseline (plan ``docs/f7-mysql-adapter-plan.md``
§3.4; B-1 *Level once, after D6* (D20); B-2's price seed (D20 + D25); the
mechanisms in ``docs/f7-build-notes.md`` §Baseline).

The one-time operation that declares which register events the outlet already
reflects and levels Promo once, key by key, so that afterwards on EVERY key

    live Promo (real-size, exact title, clamped at 0) + Σ pending Δ = N

— or the key is HELD: the whole-register ``BASELINE`` row records it (key,
reason, its snapshot event ids), its held events are excluded from every pass,
and only a per-key re-level releases it (or the N-0 release; an in-flight hold
ends with its row). Nothing here writes a register STATE: F7 is not an actor of
the state machine (§1); the baseline writes Promo levels, its own audit rows,
``ONLINE``, ``PRICE_OUTLET`` and the explicit stamp.

* :func:`baseline_preview` — READ-ONLY. One consistent snapshot (shelf pairs,
  every unstamped event, each key's max event id), then the live Promo of every
  real-size variant of the outlets of the register's SKUs (the resolver, as
  every pass resolves) and of the outlet collection (stale units on outlets of
  no register SKU); per key the Level decision; the normalisations (rule 7),
  the J price seeds (B-2), Part B's list, the catalogue notes of the keys with
  no outlet.
* :func:`baseline_apply` — MUTATES, confirm-gated by the caller with §3.4's
  precondition (the ``/dlqs`` GET's instant and ``dlqCount``, typed into the
  confirm and kept in the ``BASELINE`` row). Under the pass lease, held
  throughout — a baseline never yields: the reader refuses the decrement until
  its ``BASELINE`` row exists. Recovery (publish class) first, then
  ``INIT_BEFORE`` (the abort gate: it fails → nothing written), the
  normalisations, then per key the confirm's re-read (N, the max event id and the
  events, each live level, the SKU's outlets): a key where any moved is HELD,
  nothing written — the confirm still lowers every key it holds with live P > N
  (lowering errs to fewer) — else the key's writes as ``RECONCILE`` rows; then
  the J seeds, ``ONLINE`` from the result and, LAST, the ``BASELINE`` row with
  its explicit stamp list in one transaction. A crash before it leaves no
  ``BASELINE``: the reader keeps refusing; re-run from a fresh preview (every
  write is a target re-read live, so a re-run writes nothing twice).

Level, per key (B-1, decided D20):

* (a) one ACTIVE outlet with a variant titled exactly ``SIZE`` (§3.2) → that
  variant's Promo set to N relatively (N − live ``available``), from any level;
  an upward write to an absent or inactive level is preceded by its explicit
  activation (E15), one meeting ``committed`` > 0 holds the key; every snapshot
  event of the key is stamped, ``ONLINE`` 1 on its shelf pairs.
* (b) its only outlet a DRAFT → that variant lowered to 0, relatively; the events
  of pairs no longer on the shelf stamped, the shelf pairs' events left pending
  (+1 each, E17) — the revive restores exactly N (E9).
* (c) no outlet → nothing written; the departed pairs' events stamped; the
  publish creates the outlet with N.
* Held: a blank SKU (rule 7), a key already held, an unclean resolution or an
  unreadable outlet, a multi-outlet key (its exit, §3.5), a key whose outlet
  lacks the size as spelled (its twins listed: respelled at the source), a key
  whose upward write meets ``committed`` > 0, an unexpected outlet status; at
  confirm a key that moved, one whose normalisation or level write did not apply.
* A key with no shelf pair: its events are stamped where nothing is left to
  expose (no variant titled ``SIZE``: stamp only); a key with no shelf pair and
  no event but Promo units on an outlet (a stale unit, the outlet collection's
  included) is lowered to 0 — never held: there is nothing for a hold to keep.

Customisation-only variants (rule 7) never count; at confirm every ACTIVE outlet
carrying a CONTINUE or untracked variant gets a ``normalise`` row (DENY +
tracked) before any level write, and a customisation-only positive Promo is
lowered to 0 on any outlet (a negative is left: DENY makes it unsellable).
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

from backend.persistence.records import (
    APPLIED,
    DONE,
    FAILED,
    HOLD_BASELINE,
    SCOPE_REGISTER,
    CommitUnknown,
    EventRow,
    Hold,
    Key,
    LeaseLost,
    PairRow,
    Snapshot,
    StoreError,
)
from backend.services import flows
from backend.services import ledger as L
from backend.services import resolvers, variants
from backend.services.lease import HOLDER_BASELINE, LeaseUnavailable, PassLease
from backend.services.promo_gate import require_promo_location
from backend.services.recovery import CLASS_PUBLISH, recover
from backend.shopify import ops
from backend.shopify.ops import ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.baseline_service")

_ROME = ZoneInfo("Europe/Rome")

RULE_LEVEL = "level"

# The per-key decision of the preview (the apply re-checks each one).
ACT_LEVEL = "level"     # (a)
ACT_ZERO = "zero"       # (b)
ACT_PARKED = "parked"   # (b), DRAFT-parked at N: nothing written, listed for a re-level (§3.4's preview list)
ACT_NONE = "none"       # (c)
ACT_STAMP = "stamp"     # no shelf pair, no variant titled SIZE: the events stamped, nothing written
ACT_LOWER = "lower"     # no shelf pair, no event: a stale unit lowered to 0, never held
ACT_HOLD = "hold"

# Hold reasons (the BASELINE row's ``held[].reason``; listed by the preview).
R_BLANK_SKU = "blank_sku"
R_HELD_BEFORE = "held_before_baseline"
R_UNRESOLVED = "unresolved"
R_UNREADABLE = "outlet_unreadable"
R_MULTI_OUTLET = "multi_outlet"
R_SIZE_ABSENT = "size_absent"
R_STATUS = "unexpected_status"
R_COMMITTED = "committed_on_upward_adjust"
R_MOVED = "moved_at_confirm"
R_NORMALISE = "normalise_not_applied"
R_WRITE = "level_write_not_applied"
R_PENDING = "shelf_events_not_n"
R_UNIT_HELD = "n0_committed_unit"

# J seed statuses (B-2: one price per SKU; different values listed, never picked).
SEED = "seed"
SEED_CONFLICT = "conflict"
SEED_UNPARSEABLE = "unparseable"
SEED_INVALID = "not_positive"
SEED_PRICED = "already_priced"

# Part B buckets (the sheet era's classification, exact size — listed only under Level).
B_KEPT = "kept-online"
B_MISSING = "demote:missing"
B_DRAFT = "demote:draft"
B_SOLD_OUT = "demote:sold-out-size"
B_MULTI = "review:multi-match"
B_UNRESOLVED = "unresolved"
B_BLANK = "blank_sku"

# Apply statuses.
ST_APPLIED = "APPLIED"
ST_ALREADY_DONE = "ALREADY_DONE"
ST_PRECONDITION_REFUSED = "PRECONDITION_REFUSED"
ST_LEASE_UNAVAILABLE = "LEASE_UNAVAILABLE"
ST_ABORTED = "ABORTED"
ST_INCOMPLETE = "INCOMPLETE"

# How long the baseline waits for the lease (a decrement or another holder in flight).
BASELINE_TAKE_BOUND_S = 120.0
# §3.4's precondition: the /dlqs GET is "right before the preview" — at most one
# confirm-token TTL (15 min, api/mutations.py) before the snapshot — and never
# after the confirm (a clock skew of 2 min tolerated).
DLQ_CHECK_WINDOW = timedelta(minutes=15)
DLQ_CHECK_SKEW = timedelta(minutes=2)

_READ_ERRORS = (ShopifyTransportError, ShopifyUserError, RuntimeError)
# A Shopify failure inside one key's (one product's) writes: that key is held, the run goes on.
_SHOPIFY_ERRORS = (ShopifyTransportError, ShopifyUserError, ProductNotFound)


# =============================================================================
# Plan data model
# =============================================================================
@dataclass(frozen=True)
class VariantFacts:
    """The key's variant on one outlet of its SKU, as read — what the confirm
    re-reads and compares (§3.4: "each live available")."""

    product_id: int
    status: str
    variant_id: Optional[str]          # None: no real-size variant titled exactly SIZE
    inventory_item_id: Optional[str]
    level: bool                        # a Promo level exists, active or not
    active: bool
    available: Optional[int]           # None when the level is absent
    committed: Optional[int]

    @property
    def p(self) -> int:
        """Its Promo units as the invariant counts them: clamped at 0."""
        return max(self.available or 0, 0)


@dataclass(frozen=True)
class KeyPlan:
    """One (SKU, SIZE) key: the snapshot's facts and the Level decision."""

    sku: str
    size: str
    n: int
    shelf_pair_ids: Tuple[int, ...]
    event_ids: Tuple[int, ...]            # every unstamped keyed event of the key
    departed_event_ids: Tuple[int, ...]   # those of pairs no longer on the shelf
    max_event_id: int                     # any Δ, stamped or not
    action: str
    reason: Optional[str] = None
    outlets: Tuple[VariantFacts, ...] = ()
    target: Optional[int] = None
    delta: int = 0                        # the planned Promo change (a hold: the confirm's lowering)
    activate: bool = False
    twins: Tuple[str, ...] = ()

    @property
    def key(self) -> Key:
        return (self.sku, self.size)

    @property
    def stamp_ids(self) -> Tuple[int, ...]:
        """What the key stamps when it ends as planned (§3.4: an explicit list)."""
        if self.action in (ACT_LEVEL, ACT_STAMP):
            return self.event_ids
        if self.action in (ACT_ZERO, ACT_PARKED, ACT_NONE):
            return self.departed_event_ids
        return ()


@dataclass(frozen=True)
class ProductFix:
    """Rule 7 on one outlet: its CONTINUE / untracked variants (normalised when
    the outlet is ACTIVE) and its customisation-only variants holding Promo > 0
    (lowered to 0, relatively)."""

    product_id: int
    status: str
    title: str
    keys: Tuple[Key, ...]                          # its real-size keys (the normalise row's)
    unsafe_variant_ids: Tuple[str, ...]
    custom_lowerings: Tuple[Tuple[str, str, int], ...]   # (variant id, inventory item, available)


@dataclass(frozen=True)
class PriceSeed:
    """B-2: the SKU's outlet price seeded from the sheet's J ("Prezzo Outlet")."""

    sku: str
    status: str
    price: Optional[str]
    values: Tuple[str, ...]
    pair_ids: Tuple[int, ...]


@dataclass(frozen=True)
class PartBRow:
    """Part B under Level: a shelf pair with ``ONLINE`` 1 and what Shopify shows — listed, never written."""

    pair_id: int
    sku: str
    size: str
    bucket: str
    product_id: Optional[int] = None


@dataclass(frozen=True)
class CatalogueNote:
    """A key with no outlet the publish cannot create as it is (§3.3: a person)."""

    sku: str
    size: Optional[str]
    note: str
    twins: Tuple[str, ...] = ()


@dataclass(frozen=True)
class BaselinePlan:
    dry_run: bool
    already_done: bool
    previewed_at: Optional[datetime] = None          # the server clock (UTC) at the snapshot
    keys: Tuple[KeyPlan, ...] = ()
    products: Tuple[ProductFix, ...] = ()
    prices: Tuple[PriceSeed, ...] = ()
    part_b: Tuple[PartBRow, ...] = ()
    catalogue: Tuple[CatalogueNote, ...] = ()
    keyless_event_ids: Tuple[int, ...] = ()
    anomalies: Tuple[str, ...] = ()
    plan_hash: str = ""

    def counts(self) -> Dict[str, int]:
        out: Dict[str, int] = {"keys": len(self.keys)}
        for kp in self.keys:
            out[kp.action] = out.get(kp.action, 0) + 1
        out["units_up"] = sum(kp.delta for kp in self.keys if kp.delta > 0)
        out["units_down"] = -sum(kp.delta for kp in self.keys if kp.delta < 0)
        out["events_to_stamp"] = len({i for kp in self.keys for i in kp.stamp_ids})
        out["events_held"] = len({i for kp in self.keys if kp.action == ACT_HOLD for i in kp.event_ids})
        out["normalise"] = sum(1 for p in self.products if p.unsafe_variant_ids and p.status == "ACTIVE")
        out["custom_lowerings"] = sum(len(p.custom_lowerings) for p in self.products)
        out["price_seeds"] = sum(1 for s in self.prices if s.status == SEED)
        return out


@dataclass(frozen=True)
class InitBeforeSnapshot:
    """``INIT_BEFORE`` (§3.4 Audit): the before-snapshot, written before any
    Shopify write of the run — the approved per-key facts, every shelf pair's
    ``ONLINE`` and the price seeds as they stood, the precondition as typed."""

    plan_hash: str
    rule: str
    dlq_checked_at: str
    dlq_count: int
    keys: Tuple[KeyPlan, ...]
    products: Tuple[ProductFix, ...]
    online: Tuple[Tuple[int, bool], ...]
    prices: Tuple[PriceSeed, ...]


@dataclass(frozen=True)
class DlqCheck:
    """§3.4's precondition as the person typed it: the ``make-operator`` GET's
    instant and the unresolved ``/dlqs`` count it showed."""

    checked_at: datetime     # aware, UTC
    count: int
    typed: str


@dataclass(frozen=True)
class KeyOutcome:
    sku: str
    size: str
    action: str
    result: str
    reason: Optional[str] = None


@dataclass(frozen=True)
class BaselineReport:
    status: str
    reason: Optional[str] = None
    baseline_row_id: Optional[int] = None
    outcomes: Tuple[KeyOutcome, ...] = ()
    held: Tuple[Dict[str, Any], ...] = ()
    stamped: int = 0
    online_on: int = 0
    online_off: int = 0
    prices_seeded: Tuple[str, ...] = ()
    surfaced: Tuple[Dict[str, Any], ...] = ()


# =============================================================================
# Pure helpers
# =============================================================================
_PRICE_TEXT = re.compile(r"^(-)?(\d+)(?:[.,](\d{1,2}))?$")


def parse_sheet_price(cell: Any) -> Tuple[Optional[Decimal], bool]:
    """A sheet cell (``IMPORTED.PAYLOAD.raw.<col>`` = ``{t, v, w}``) → DECIMAL(10,2),
    by the register's own rule for H / I (STOCK_Manager ``parsePrice``,
    ``src/lib/returned-pair-legacy.ts`` at ``92f6f90``): a number cell rounded
    half-up to 2 decimals; a text such as ``"89,90"``, ``"89.90"``, ``"€ 89,90"``
    parsed (at most two decimals); an empty cell → ``(None, False)``; anything
    else → ``(None, True)`` (flagged, never guessed)."""
    if not isinstance(cell, dict):
        return None, cell is not None
    v, t = cell.get("v"), cell.get("t")
    if v is None or (isinstance(v, str) and not v.strip()):
        return None, False
    if t == "n" and isinstance(v, (int, float)) and not isinstance(v, bool):
        value = Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return (value, False) if abs(value) < Decimal("1e8") else (None, True)
    if t != "e" and isinstance(v, str):
        m = _PRICE_TEXT.match(re.sub(r"\s+", "", v.replace("€", "")))
        if m:
            text = f"{m.group(1) or ''}{m.group(2)}" + (f".{m.group(3)}" if m.group(3) is not None else "")
            value = Decimal(text).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            if abs(value) < Decimal("1e8"):
                return value, False
    return None, True


def parse_dlq(checked_at: Optional[str], count: Optional[int]) -> Tuple[Optional[DlqCheck], Optional[str]]:
    """The typed precondition → :class:`DlqCheck`, or why not. An instant with no
    offset is read as Europe/Rome (the person's clock)."""
    text = (checked_at or "").strip()
    if not text or count is None:
        return None, "dlq_check_missing"
    try:
        ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None, "dlq_check_unparseable"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=_ROME)
    return DlqCheck(ts.astimezone(timezone.utc), int(count), text), None


def _utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def check_dlq(dlq: Optional[DlqCheck], *, previewed_at: Optional[datetime], now: datetime) -> Optional[str]:
    """§3.4's precondition: no unresolved ``/dlqs`` entry, read right before the
    preview. Returns why it fails, or ``None``."""
    if dlq is None:
        return "dlq_check_missing"
    if dlq.count != 0:
        return "dlq_unresolved_entries"
    if previewed_at is None:
        return "preview_instant_missing"
    if dlq.checked_at > _utc(now) + DLQ_CHECK_SKEW:
        return "dlq_check_in_the_future"
    if dlq.checked_at < _utc(previewed_at) - DLQ_CHECK_WINDOW:
        return "dlq_check_too_old"
    return None


def _status(raw: Any) -> str:
    return str(raw or "").upper()


def _variant_facts(product_id: int, status: str, v: Dict[str, Any], promo: str) -> VariantFacts:
    lvl = variants.promo_level(v, promo)
    if lvl is None:
        return VariantFacts(product_id, status, v.get("id"), variants.item_id(v), False, False, None, None)
    return VariantFacts(product_id, status, v.get("id"), variants.item_id(v), True,
                        bool(lvl.get("is_active")), lvl.get("available"), lvl.get("committed"))


def plan_digest(doc: Dict[str, Any]) -> str:
    raw = json.dumps(doc, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# =============================================================================
# Reading Shopify (the preview's, and the confirm's re-read)
# =============================================================================
@dataclass
class _Outlet:
    gid: str
    product_id: int
    status: str
    title: str
    inventory: Optional[List[Dict[str, Any]]]
    unreadable: Optional[str] = None

    def facts(self, size: str, promo: str) -> VariantFacts:
        v = variants.match_exact(size, self.inventory or ())
        if v is None:
            return VariantFacts(self.product_id, self.status, None, None, False, False, None, None)
        return _variant_facts(self.product_id, self.status, v, promo)


@dataclass
class SkuView:
    sku: str
    clean: bool
    outlets: List[_Outlet]
    warning: Optional[str] = None

    @property
    def readable(self) -> bool:
        return self.clean and all(o.inventory is not None for o in self.outlets)

    def facts(self, size: str, promo: str) -> Tuple[VariantFacts, ...]:
        return tuple(sorted((o.facts(size, promo) for o in self.outlets), key=lambda f: f.product_id))


class OutletReader:
    """The Shopify reads of one preview or one confirm: each SKU resolved once,
    each product's inventory read once — or afresh (``fresh``: the confirm's
    per-key re-read). ``keepalive`` renews the lease before a call (the confirm's)."""

    def __init__(self, transport: Any, keepalive: Callable[[], None] = lambda: None) -> None:
        self.transport = transport
        self.keepalive = keepalive
        self._inv: Dict[str, Tuple[Optional[List[Dict[str, Any]]], Optional[str]]] = {}
        self._res: Dict[str, Tuple[bool, List[Dict[str, Any]], Optional[str]]] = {}

    def inventory(self, gid: str, *, fresh: bool = False) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        if fresh or gid not in self._inv:
            self.keepalive()
            try:
                inv = ops.read_variant_inventory(self.transport, gid)
            except ProductNotFound:
                self._inv[gid] = (None, "product_gone")
            except _READ_ERRORS as e:
                self._inv[gid] = (None, type(e).__name__)
            else:
                truncated = any(v.get("levels_truncated") for v in inv)
                self._inv[gid] = (None, "levels_truncated") if truncated else (inv, None)
        return self._inv[gid]

    def outlet(self, gid: str, status: Any, title: Any, *, fresh: bool = False) -> _Outlet:
        inv, why = self.inventory(gid, fresh=fresh)
        return _Outlet(gid, flows.product_number(gid) or 0, _status(status), str(title or ""), inv, why)

    def sku_view(self, sku: str, *, fresh: bool = False) -> SkuView:
        if sku not in self._res:
            self.keepalive()
            try:
                res = resolvers.outlet_resolver(self.transport, sku)
            except _READ_ERRORS as e:
                self._res[sku] = (False, [], type(e).__name__)
            else:
                self._res[sku] = (not res.get("truncated"), list(res.get("matches") or ()), res.get("warning"))
        clean, matches, warning = self._res[sku]
        outlets = [self.outlet(m["product_gid"], m.get("status"), m.get("title"), fresh=fresh) for m in matches]
        return SkuView(sku, clean, outlets, warning)

    def resolution(self, sku: str) -> Optional[flows.Resolution]:
        """The SKU's resolution as already read (``None`` if never read)."""
        if sku not in self._res:
            return None
        clean, matches, warning = self._res[sku]
        return flows.Resolution(tuple(matches), clean, warning)


# =============================================================================
# The Level decision (pure over what was read)
# =============================================================================
def _decide(k: Key, shelf: Sequence[PairRow], events: Sequence[EventRow], on_shelf: Set[int],
            max_event_id: int, view: Optional[SkuView], held_kinds: Set[str], promo: str) -> KeyPlan:
    sku, size = k
    n = len(shelf)
    base = dict(
        sku=sku, size=size, n=n, shelf_pair_ids=tuple(sorted(p.pair_id for p in shelf)),
        event_ids=tuple(sorted(e.event_id for e in events)),
        departed_event_ids=tuple(sorted(e.event_id for e in events if e.pair_id not in on_shelf)),
        max_event_id=int(max_event_id),
    )
    facts: Tuple[VariantFacts, ...] = ()
    readable = bool(sku) and view is not None and view.readable
    if readable:
        facts = view.facts(size, promo)
        # a level whose quantities did not read is not a level F7 can establish
        readable = not any(f.level and (f.available is None or f.committed is None) for f in facts)
    excess = max(sum(f.p for f in facts) - n, 0)

    def hold(reason: str, **kw: Any) -> KeyPlan:
        return KeyPlan(**base, action=ACT_HOLD, reason=reason, outlets=facts, delta=-excess, **kw)

    if not sku:
        return hold(R_BLANK_SKU)                      # nothing resolves a SKU '' (rule 7)
    if held_kinds:
        return hold(f"{R_HELD_BEFORE}:{'+'.join(sorted(held_kinds))}")
    if view is None or not view.clean:
        return hold(R_UNRESOLVED)                     # a failed / truncated resolution is not "none"
    if not readable:
        return hold(R_UNREADABLE)
    if n == 0 and any((f.committed or 0) > 0 for f in facts):
        return hold(R_UNIT_HELD)                      # its −1 waits for a unit only held (rule 4, D33)
    # (b) and (c) leave the shelf pairs' events pending (+1 each, E17): §3.4's
    # invariant holds only where they sum to N — a key where they do not is held
    pending_on_shelf = sum(e.delta for e in events if e.pair_id in on_shelf)
    if not view.outlets:
        if pending_on_shelf != n:
            return hold(R_PENDING)
        return KeyPlan(**base, action=ACT_NONE)       # (c)
    if len(view.outlets) > 1:
        return hold(R_MULTI_OUTLET)                   # its exit: §3.5
    o, f = view.outlets[0], facts[0]
    if f.variant_id is None:
        twins = tuple(variants.variant_title(t) for t in variants.twins(size, o.inventory or ()))
        if n > 0:
            return hold(R_SIZE_ABSENT, twins=twins)   # a person adds the variant or respells the pair
        return KeyPlan(**base, action=ACT_STAMP, outlets=facts, twins=twins)
    if o.status == "ACTIVE":                          # (a)
        activate = n > 0 and not (f.level and f.active)
        delta = n - (0 if activate else (f.available or 0))
        if (activate or delta > 0) and (f.committed or 0) > 0:
            return hold(R_COMMITTED)
        return KeyPlan(**base, action=ACT_LEVEL, outlets=facts, target=n, delta=delta, activate=activate)
    if o.status == "DRAFT":                           # (b)
        if pending_on_shelf == n:
            return KeyPlan(**base, action=ACT_ZERO, outlets=facts, target=0, delta=-f.p)
        if f.p + pending_on_shelf == n:               # DRAFT-parked: the invariant holds as it is (a re-level exposes it)
            return KeyPlan(**base, action=ACT_PARKED, outlets=facts)
        return hold(R_PENDING)
    return hold(f"{R_STATUS}:{o.status or 'NONE'}")


# =============================================================================
# Preview
# =============================================================================
def _foreign_outlets(reader: OutletReader, views: Dict[str, SkuView], skus: Set[str],
                     anomalies: List[str]) -> List[_Outlet]:
    """The outlet collection's products no register SKU resolves to (stale units,
    rule 7's normalisations). A collection product carrying a register SKU the
    resolver did not return makes that SKU's resolution unclean: never guessed."""
    resolved = {o.gid for v in views.values() for o in v.outlets}
    reader.keepalive()
    nodes = ops.enumerate_outlet_products(reader.transport)   # a failure fails the preview: fail closed
    out: List[_Outlet] = []
    for node in nodes:
        gid = node.get("id")
        if not gid or gid in resolved:
            continue
        o = reader.outlet(gid, node.get("status"), node.get("title"))
        if o.inventory is None:
            anomalies.append(f"outlet_unreadable:{o.product_id}:{o.unreadable}")
            continue
        mine = sorted({(v.get("sku") or "").strip() for v in o.inventory} & skus)
        if mine:
            anomalies.append(f"collection_product_not_resolved:{o.product_id}:{'|'.join(mine)}")
            for s in mine:
                views[s].clean = False
            continue
        out.append(o)
    return out


def _stale_keys(views: Dict[str, SkuView], foreign: Sequence[_Outlet], register_keys: Set[Key],
                promo: str) -> List[KeyPlan]:
    """Keys with no shelf pair and no event whose real-size Promo is > 0 on an outlet."""
    stale: Dict[Key, List[VariantFacts]] = {}
    for sku, view in views.items():
        if not view.readable:
            continue
        for o in view.outlets:
            for v in variants.real_size_variants(o.inventory or ()):
                k = (sku, variants.variant_title(v))
                f = _variant_facts(o.product_id, o.status, v, promo)
                if k not in register_keys and f.p > 0:
                    stale.setdefault(k, []).append(f)
    for o in foreign:
        for v in variants.real_size_variants(o.inventory or ()):
            k = ((v.get("sku") or "").strip(), variants.variant_title(v))
            f = _variant_facts(o.product_id, o.status, v, promo)
            if f.p > 0:
                stale.setdefault(k, []).append(f)
    return [
        KeyPlan(sku=k[0], size=k[1], n=0, shelf_pair_ids=(), event_ids=(), departed_event_ids=(),
                max_event_id=0, action=ACT_LOWER,
                outlets=tuple(sorted(fs, key=lambda f: f.product_id)), target=0, delta=-sum(f.p for f in fs))
        for k, fs in sorted(stale.items())
    ]


def _product_fixes(views: Dict[str, SkuView], foreign: Sequence[_Outlet], promo: str) -> List[ProductFix]:
    seen: Dict[str, Dict[str, Any]] = {}

    def add(o: _Outlet, sku: Optional[str]) -> None:
        if o.inventory is None:
            return
        cur = seen.setdefault(o.gid, {"o": o, "keys": set(), "unsafe": [], "custom": []})
        for v in o.inventory:
            if variants.is_customisation_only(v):
                a = variants.promo_available(v, promo)
                if a is not None and a > 0 and (v.get("id"), variants.item_id(v), a) not in cur["custom"]:
                    cur["custom"].append((v.get("id"), variants.item_id(v), int(a)))
            else:
                cur["keys"].add((sku if sku is not None else (v.get("sku") or "").strip(), variants.variant_title(v)))
            unsafe = (v.get("inventoryPolicy") or "").upper() != "DENY" or v.get("tracked") is False
            if o.status == "ACTIVE" and unsafe and v.get("id") not in cur["unsafe"]:
                cur["unsafe"].append(v.get("id"))

    for sku, view in views.items():
        if view.clean:
            for o in view.outlets:
                add(o, sku)
    for o in foreign:
        add(o, None)
    return [
        ProductFix(product_id=c["o"].product_id, status=c["o"].status, title=c["o"].title,
                   keys=tuple(sorted(c["keys"])), unsafe_variant_ids=tuple(c["unsafe"]),
                   custom_lowerings=tuple(c["custom"]))
        for _gid, c in sorted(seen.items(), key=lambda kv: kv[1]["o"].product_id)
        if c["unsafe"] or c["custom"]
    ]


def _price_seeds(register: Any, snap: Snapshot) -> List[PriceSeed]:
    """B-2 (D20): one price per SKU from J; a SKU whose pairs carry different J
    values — or a value that does not parse — is listed, never picked; a SKU
    already priced is left to the app's pricing screen."""
    shelf = [p for p in snap.pairs if p.sku]
    if not shelf:
        return []
    j: Dict[int, List[Tuple[Optional[Decimal], bool]]] = {}
    for pair_id, payload in register.import_payloads([p.pair_id for p in shelf]):
        cell = ((payload or {}).get("raw") or {}).get("J") if isinstance(payload, dict) else None
        j.setdefault(int(pair_id), []).append(parse_sheet_price(cell))
    by_sku: Dict[str, List[PairRow]] = {}
    for p in shelf:
        by_sku.setdefault(p.sku, []).append(p)
    out: List[PriceSeed] = []
    for sku, pairs in sorted(by_sku.items()):
        ids = tuple(sorted(p.pair_id for p in pairs))
        priced = sorted({str(p.price_outlet) for p in pairs if p.price_outlet is not None})
        if priced:
            out.append(PriceSeed(sku, SEED_PRICED, None, tuple(priced), ids))
            continue
        values: Set[Decimal] = set()
        flagged = False
        for p in pairs:
            for value, flag in j.get(p.pair_id, ()):
                flagged = flagged or flag
                if value is not None:
                    values.add(value)
        shown = tuple(sorted(f"{v:.2f}" for v in values))
        if flagged:
            out.append(PriceSeed(sku, SEED_UNPARSEABLE, None, shown, ids))
        elif len(values) > 1:
            out.append(PriceSeed(sku, SEED_CONFLICT, None, shown, ids))
        elif values:
            value = next(iter(values))
            out.append(PriceSeed(sku, SEED if value > 0 else SEED_INVALID, f"{value:.2f}" if value > 0 else None,
                                 shown, ids))
    return out


def _part_b(snap: Snapshot, views: Dict[str, SkuView], promo: str) -> List[PartBRow]:
    out: List[PartBRow] = []
    for p in snap.pairs:
        if not p.online:
            continue
        view = views.get(p.sku) if p.sku else None
        pid: Optional[int] = None
        if not p.sku:
            bucket = B_BLANK
        elif view is None or not view.readable:
            bucket = B_UNRESOLVED
        elif len(view.outlets) > 1:
            bucket = B_MULTI
        elif not view.outlets:
            bucket = B_MISSING
        else:
            o = view.outlets[0]
            pid = o.product_id
            f = o.facts(p.size, promo)
            if o.status != "ACTIVE":
                bucket = B_DRAFT
            elif f.variant_id is None or (f.available or 0) <= 0:
                bucket = B_SOLD_OUT
            else:
                bucket = B_KEPT
        out.append(PartBRow(p.pair_id, p.sku, p.size, bucket, pid))
    return out


def _catalogue(reader: OutletReader, keys: Sequence[KeyPlan]) -> List[CatalogueNote]:
    """§3.3's listing for the keys the publish would create: a SKU not in the
    catalogue, several sources, or a size its source lacks (a person)."""
    by_sku: Dict[str, List[KeyPlan]] = {}
    for kp in keys:
        if kp.action == ACT_NONE and kp.n > 0:
            by_sku.setdefault(kp.sku, []).append(kp)
    out: List[CatalogueNote] = []
    for sku, kps in sorted(by_sku.items()):
        reader.keepalive()
        try:
            srcs = resolvers.source_resolver(reader.transport, sku).get("matches") or []
            src_variants = ops.get_product_variants(reader.transport, srcs[0]["product_gid"]) if len(srcs) == 1 else []
        except _READ_ERRORS as e:
            out.append(CatalogueNote(sku, None, f"source_unreadable:{type(e).__name__}"))
            continue
        if not srcs:
            out.append(CatalogueNote(sku, None, "not_in_catalogue"))
        elif len(srcs) > 1:
            out.append(CatalogueNote(sku, None, "multi_source"))
        else:
            for kp in kps:
                if variants.match_exact(kp.size, src_variants) is None:
                    twins = tuple(variants.variant_title(t) for t in variants.twins(kp.size, src_variants))
                    out.append(CatalogueNote(sku, kp.size, "size_absent_on_source", twins))
    return out


def baseline_preview(register: Any, transport: Any, *, promo_location_id: str) -> BaselinePlan:
    """Compute the baseline PLAN without mutating Shopify or the register."""
    now = register.server_now()
    if register.baseline() is not None:
        return BaselinePlan(dry_run=True, already_done=True, previewed_at=now,
                            plan_hash=plan_digest({"already_done": True}))
    promo = promo_location_id
    snap = register.snapshot()
    holds = register.held_keys()
    reader = OutletReader(transport)

    shelf_by_key: Dict[Key, List[PairRow]] = {}
    for p in snap.pairs:
        shelf_by_key.setdefault(p.key, []).append(p)
    on_shelf = {p.pair_id for p in snap.pairs}
    events_by_key: Dict[Key, List[EventRow]] = {}
    for e in snap.unstamped:
        if e.key is not None:
            events_by_key.setdefault(e.key, []).append(e)
    register_keys = set(shelf_by_key) | set(events_by_key)
    skus = {k[0] for k in register_keys if k[0]}
    anomalies: List[str] = []
    views = {sku: reader.sku_view(sku) for sku in sorted(skus)}
    for sku, view in views.items():
        if view.warning:
            anomalies.append(f"{sku}:{view.warning}")
    foreign = _foreign_outlets(reader, views, skus, anomalies)
    held_kinds: Dict[Key, Set[str]] = {}
    for h in holds:
        held_kinds.setdefault(h.key, set()).add(h.kind)

    keys = [
        _decide(k, shelf_by_key.get(k, ()), events_by_key.get(k, ()), on_shelf, snap.max_event_id.get(k, 0),
                views.get(k[0]), held_kinds.get(k, set()), promo)
        for k in sorted(register_keys)
    ]
    keys += _stale_keys(views, foreign, register_keys, promo)
    products = _product_fixes(views, foreign, promo)
    prices = _price_seeds(register, snap)
    part_b = _part_b(snap, views, promo)
    catalogue = _catalogue(reader, keys)
    keyless = tuple(e.event_id for e in snap.unstamped if e.key is None)
    doc = {
        "at": now.isoformat(), "keys": [asdict(k) for k in keys], "products": [asdict(p) for p in products],
        "prices": [asdict(s) for s in prices], "keyless": list(keyless),
    }
    return BaselinePlan(
        dry_run=True, already_done=False, previewed_at=now, keys=tuple(keys), products=tuple(products),
        prices=tuple(prices), part_b=tuple(part_b), catalogue=tuple(catalogue), keyless_event_ids=keyless,
        anomalies=tuple(anomalies), plan_hash=plan_digest(doc),
    )


# =============================================================================
# Apply
# =============================================================================
class _Run:
    """One confirmed baseline under the pass lease (never released mid-run)."""

    def __init__(self, ctx: flows.Context, approved: BaselinePlan) -> None:
        self.ctx = ctx
        self.reg = ctx.register
        self.led = ctx.ledger
        self.promo = ctx.promo
        self.approved = approved
        self.reader = OutletReader(ctx.transport, keepalive=ctx.ledger.lease.keepalive)
        self.outcomes: List[KeyOutcome] = []
        self.held: List[Dict[str, Any]] = []
        self.held_plans: Dict[Key, KeyPlan] = {}
        self.stamp: Set[int] = set()
        self.on_pairs: Set[int] = set()
        self.bad_products: Set[int] = set()
        self.seeded: List[str] = []
        self.online_on = 0
        self.online_off = 0

    # -- bookkeeping -----------------------------------------------------------------
    def _done(self, kp: KeyPlan, result: str) -> None:
        self.stamp.update(kp.stamp_ids)
        if kp.action == ACT_LEVEL and kp.n > 0:
            self.on_pairs.update(kp.shelf_pair_ids)
        self.outcomes.append(KeyOutcome(kp.sku, kp.size, kp.action, result))

    def _hold(self, kp: KeyPlan, reason: str) -> None:
        self.held.append({"key": [kp.sku, kp.size], "reason": reason, "event_ids": list(kp.event_ids)})
        self.held_plans[kp.key] = kp
        self.outcomes.append(KeyOutcome(kp.sku, kp.size, kp.action, "held", reason))

    # -- rule 7 -------------------------------------------------------------------------
    def products(self) -> None:
        """Every normalisation before any level write (DENY + tracked before a
        variant is exposed); a product whose normalisation did not apply has
        every key held."""
        for fix in self.approved.products:
            try:
                self._product(fix)
            except _SHOPIFY_ERRORS as e:
                self.bad_products.add(fix.product_id)
                self.ctx.report.surface("normalise_error", product_id=fix.product_id, error=type(e).__name__)

    def _product(self, fix: ProductFix) -> None:
        specs: List[L.RowSpec] = []
        if fix.unsafe_variant_ids and fix.status == "ACTIVE":
            specs.append(L.RowSpec(action=L.A_NORMALISE, keys=fix.keys, product_id=fix.product_id))
        for vid, item, _a in fix.custom_lowerings:
            specs.append(L.RowSpec(action=L.A_ADJUST, purpose=L.P_CUSTOM, target=0, inventory_item_id=item,
                                   location_id=self.promo, variant_id=vid, product_id=fix.product_id))
        if not specs:
            return
        rows = self.led.write_group(specs)
        for r in rows:
            if r.action == L.A_NORMALISE:
                if flows.run_normalise(self.ctx, r) != APPLIED:
                    self.bad_products.add(fix.product_id)
                    self.ctx.report.surface("normalise_not_applied", product_id=fix.product_id)
            else:
                self.led.send_adjust(r)
        self.led.settle(rows[0].group_id)

    # -- the keys -----------------------------------------------------------------------
    def keys(self) -> None:
        for kp in self.approved.keys:
            try:
                self._key(kp)
            except _SHOPIFY_ERRORS as e:
                self.ctx.report.surface("baseline_key_error", key=list(kp.key), error=type(e).__name__)
                if kp.action == ACT_LOWER:
                    self.outcomes.append(KeyOutcome(kp.sku, kp.size, kp.action, "lower_failed"))
                else:
                    self._hold(kp, f"shopify_error:{type(e).__name__}")

    def _key(self, kp: KeyPlan) -> None:
        if kp.action == ACT_LOWER:
            self._lower(kp)
            return
        if kp.action == ACT_HOLD:
            self._hold(kp, kp.reason or "held")
            return
        if any(f.product_id in self.bad_products for f in kp.outlets):
            self._hold(kp, R_NORMALISE)     # before the re-read: its refusal may have drafted the outlet
            return
        if self._moved(kp, self.reader.sku_view(kp.sku, fresh=True)):
            self._hold(kp, R_MOVED)
        elif kp.action == ACT_LEVEL:
            self._level(kp)
        elif kp.action == ACT_ZERO:
            self._zero(kp)
        elif kp.action == ACT_PARKED:
            self._done(kp, "parked")
        elif kp.action == ACT_NONE:
            self._done(kp, "none")
        elif kp.action == ACT_STAMP:
            self._done(kp, "stamped")
        else:  # pragma: no cover - the preview builds only the actions above
            self._hold(kp, f"unknown_action:{kp.action}")

    def _moved(self, kp: KeyPlan, view: Optional[SkuView]) -> bool:
        """§3.4: the confirm re-reads N, the max event id (any Δ) and the events,
        and every live level of the key on its SKU's outlets."""
        snap = self.reg.snapshot(kp.key)
        if snap.shelf_count(kp.key) != kp.n or snap.max_event_id.get(kp.key, 0) != kp.max_event_id:
            return True
        if tuple(sorted(e.event_id for e in snap.unstamped if e.key == kp.key)) != kp.event_ids:
            return True
        if view is None or not view.readable:
            return True
        return view.facts(kp.size, self.promo) != kp.outlets

    def _level(self, kp: KeyPlan) -> None:
        f = kp.outlets[0]
        common = dict(keys=(kp.key,), inventory_item_id=f.inventory_item_id, location_id=self.promo,
                      variant_id=f.variant_id, product_id=f.product_id)
        specs: List[L.RowSpec] = []
        if kp.activate:
            specs.append(L.RowSpec(action=L.A_ACTIVATE, **common))
        if kp.delta != 0 or kp.activate:
            specs.append(L.RowSpec(action=L.A_ADJUST, purpose=L.P_RELEVEL, target=kp.n,
                                   extra={"n": kp.n, "shelf_pair_ids": list(kp.shelf_pair_ids)}, **common))
        if not specs:
            self._done(kp, "levelled")      # live Promo already N
            return
        rows = self.led.write_group(specs)
        activation: Optional[str] = None
        for r in rows:
            if r.action == L.A_ACTIVATE:
                activation = self.led.send_activate(r)
        for r in rows:
            if r.action == L.A_ADJUST:
                if activation is not None and activation != APPLIED:
                    self.led.never_sent(r, "activation_not_applied")
                else:
                    self.led.send_adjust(r)
        self.led.settle(rows[0].group_id)
        final = [self.led.reload(r) for r in rows]
        if all(r.status == DONE for r in final):
            self._done(kp, "levelled")
        else:
            self._hold(kp, f"{R_WRITE}:{','.join(r.status or '' for r in final)}")

    def _zero(self, kp: KeyPlan) -> None:
        f = kp.outlets[0]
        if kp.delta == 0:
            self._done(kp, "zeroed")        # nothing to lower (a negative is never raised)
            return
        (row,) = self.led.write_group([L.RowSpec(
            action=L.A_ADJUST, keys=(kp.key,), purpose=L.P_ZERO, target=0, inventory_item_id=f.inventory_item_id,
            location_id=self.promo, variant_id=f.variant_id, product_id=f.product_id)])
        self.led.send_adjust(row)
        self.led.settle(row.group_id)
        row = self.led.reload(row)
        if row.status == DONE or (row.status == FAILED and (row.outcome or {}).get("reason") == "nothing_to_lower"):
            self._done(kp, "zeroed")
        else:
            self._hold(kp, f"{R_WRITE}:{row.status}")

    def _lower(self, kp: KeyPlan) -> None:
        specs = [
            L.RowSpec(action=L.A_ADJUST, keys=(kp.key,), purpose=L.P_ZERO, target=0,
                      inventory_item_id=f.inventory_item_id, location_id=self.promo, variant_id=f.variant_id,
                      product_id=f.product_id)
            for f in kp.outlets if f.p > 0 and f.inventory_item_id
        ]
        if not specs:
            self.outcomes.append(KeyOutcome(kp.sku, kp.size, kp.action, "lowered"))
            return
        rows = self.led.write_group(specs)
        for r in rows:
            self.led.send_adjust(r)
        self.led.settle(rows[0].group_id)
        final = [self.led.reload(r) for r in rows]
        ok = all(r.status == DONE or (r.status == FAILED and (r.outcome or {}).get("reason") == "nothing_to_lower")
                 for r in final)
        if not ok:
            self.ctx.report.surface("stale_unit_not_lowered", key=list(kp.key))
        self.outcomes.append(KeyOutcome(kp.sku, kp.size, kp.action, "lowered" if ok else "lower_failed"))

    # -- the confirm's lowering of the keys it holds -------------------------------------
    def held_lowering(self) -> None:
        now = self.reg.server_now()
        holds = [
            Hold(key=k, kind=HOLD_BASELINE, reason="baseline", event_ids=frozenset(kp.event_ids), row_id=0,
                 created_at=now)
            for k, kp in sorted(self.held_plans.items()) if k[0]
        ]
        if not holds:
            return
        held_ids = {h.key: h.event_ids for h in holds}
        post_hold = [e for e in self.reg.unstamped_events()
                     if e.key in held_ids and e.event_id not in held_ids[e.key]]
        try:
            flows.held_key_lowering(self.ctx, holds=holds, pending=post_hold, carried=[])
        except _SHOPIFY_ERRORS as e:   # every later pass's take sweeps the held keys again
            self.ctx.report.surface("held_lowering_error", error=type(e).__name__)

    # -- the outlet ids, B-2's seed, ONLINE, the BASELINE row -------------------------------------
    def outlet_ids(self) -> None:
        """§3.1: a pass resolving a SKU to exactly one non-``review`` outlet (by
        title AND collection) writes its id on the SKU's shelf pairs that lack it."""
        for sku in sorted({kp.sku for kp in self.approved.keys if kp.sku and kp.action != ACT_LOWER}):
            res = self.reader.resolution(sku)
            if res is not None:
                flows.maybe_set_outlet_product(self.ctx, sku, res)

    def prices(self) -> None:
        for seed in self.approved.prices:
            if seed.status != SEED or seed.price is None:
                continue
            if any(p.price_outlet is not None for p in self.reg.shelf(seed.sku)):
                self.ctx.report.surface("priced_meanwhile", sku=seed.sku)
                continue
            if self.reg.set_outlet_price(self.led.lease, seed.sku, Decimal(seed.price), None):
                self.seeded.append(seed.sku)

    def online(self) -> None:
        shelf = self.reg.shelf()
        on = sorted(p.pair_id for p in shelf if p.pair_id in self.on_pairs and not p.online)
        off = sorted(p.pair_id for p in shelf if p.online and p.pair_id not in self.on_pairs)
        if on:
            self.reg.set_online(self.led.lease, on, True)
        if off:
            self.reg.set_online(self.led.lease, off, False)
        self.online_on, self.online_off = len(on), len(off)

    def baseline_row(self, dlq: DlqCheck) -> int:
        stamp = sorted(self.stamp)
        results: Dict[str, int] = {}
        for o in self.outcomes:
            results[o.result] = results.get(o.result, 0) + 1
        payload = {
            "rule": RULE_LEVEL,
            "dlq": {"checked_at": dlq.checked_at.isoformat(), "typed": dlq.typed, "count": dlq.count},
            "held": self.held,
            "stamped": stamp,
            "keyless": list(self.approved.keyless_event_ids),
            "counts": {**results, "events_stamped": len(stamp), "online_on": self.online_on,
                       "online_off": self.online_off, "prices_seeded": len(self.seeded)},
        }
        return self.reg.record_baseline(self.led.lease, scope=SCOPE_REGISTER, action="baseline",
                                        payload=payload, stamp_ids=stamp, plan_hash=self.approved.plan_hash)

    def report(self, status: str, *, reason: Optional[str] = None, row_id: Optional[int] = None) -> BaselineReport:
        return BaselineReport(
            status=status, reason=reason, baseline_row_id=row_id, outcomes=tuple(self.outcomes),
            held=tuple(self.held), stamped=len(self.stamp) if row_id else 0, online_on=self.online_on,
            online_off=self.online_off, prices_seeded=tuple(self.seeded), surfaced=tuple(self.ctx.report.items),
        )


def _before(approved: BaselinePlan, dlq: DlqCheck, shelf: Iterable[PairRow]) -> InitBeforeSnapshot:
    return InitBeforeSnapshot(
        plan_hash=approved.plan_hash, rule=RULE_LEVEL, dlq_checked_at=dlq.checked_at.isoformat(),
        dlq_count=dlq.count, keys=approved.keys, products=approved.products,
        online=tuple((p.pair_id, bool(p.online)) for p in shelf), prices=approved.prices,
    )


def baseline_apply(
    register: Any,
    transport: Any,
    approved: BaselinePlan,
    audit_sink: Any,
    *,
    promo_location_id: str,
    dlq: Optional[DlqCheck],
    lease: Optional[PassLease] = None,
    take_bound_s: float = BASELINE_TAKE_BOUND_S,
    sleep: Callable[[float], None] = time.sleep,
) -> BaselineReport:
    """Execute the approved baseline (see the module docstring for the order)."""
    if approved.already_done:
        return BaselineReport(status=ST_ALREADY_DONE)
    why = check_dlq(dlq, previewed_at=approved.previewed_at, now=register.server_now())
    if why is not None:
        return BaselineReport(status=ST_PRECONDITION_REFUSED, reason=why)
    assert dlq is not None
    require_promo_location(transport, promo_location_id)          # E18: before the lease, nothing written
    pass_lease = lease or PassLease(register, HOLDER_BASELINE, sleep=sleep)
    try:
        pass_lease.take(bound_s=take_bound_s)
    except LeaseUnavailable:
        return BaselineReport(status=ST_LEASE_UNAVAILABLE, reason="lease_held_by_another_pass")
    ctx = flows.Context(L.Ledger(register, transport, pass_lease, promo_location_id, sleep=sleep))
    run = _Run(ctx, approved)
    try:
        if register.baseline() is not None:
            return run.report(ST_ALREADY_DONE)
        recover(ctx, CLASS_PUBLISH)
        try:
            audit_sink.bind(pass_lease)
            audit_sink.write_init_before(_before(approved, dlq, register.shelf()))
        except LeaseLost:
            raise
        except Exception as e:  # noqa: BLE001 - the abort gate: no snapshot, no Shopify write
            logger.error("baseline: INIT_BEFORE not written -> aborting: %s", type(e).__name__)
            return run.report(ST_ABORTED, reason=f"init_before_failed:{type(e).__name__}")
        run.products()
        run.keys()
        run.held_lowering()
        run.outlet_ids()
        run.prices()
        run.online()
        try:
            row_id = run.baseline_row(dlq)
        except CommitUnknown:
            done = register.baseline()
            if done is None or done.plan_hash != approved.plan_hash:
                return run.report(ST_INCOMPLETE, reason="baseline_commit_unknown")
            row_id = done.row_id
        return run.report(ST_APPLIED, row_id=row_id)
    except LeaseLost:
        return run.report(ST_INCOMPLETE, reason="lease_lost")
    except StoreError as e:
        return run.report(ST_INCOMPLETE, reason=f"register_error:{type(e).__name__}")
    finally:
        pass_lease.release()


__all__ = [
    "RULE_LEVEL", "ACT_LEVEL", "ACT_ZERO", "ACT_PARKED", "ACT_NONE", "ACT_STAMP", "ACT_LOWER", "ACT_HOLD",
    "R_BLANK_SKU", "R_HELD_BEFORE", "R_UNRESOLVED", "R_UNREADABLE", "R_MULTI_OUTLET", "R_SIZE_ABSENT",
    "R_STATUS", "R_COMMITTED", "R_MOVED", "R_NORMALISE", "R_WRITE", "R_PENDING", "R_UNIT_HELD",
    "SEED", "SEED_CONFLICT", "SEED_UNPARSEABLE", "SEED_INVALID", "SEED_PRICED",
    "B_KEPT", "B_MISSING", "B_DRAFT", "B_SOLD_OUT", "B_MULTI", "B_UNRESOLVED", "B_BLANK",
    "ST_APPLIED", "ST_ALREADY_DONE", "ST_PRECONDITION_REFUSED", "ST_LEASE_UNAVAILABLE", "ST_ABORTED", "ST_INCOMPLETE",
    "BASELINE_TAKE_BOUND_S", "DLQ_CHECK_WINDOW", "DLQ_CHECK_SKEW",
    "VariantFacts", "KeyPlan", "ProductFix", "PriceSeed", "PartBRow", "CatalogueNote", "BaselinePlan",
    "InitBeforeSnapshot", "DlqCheck", "KeyOutcome", "BaselineReport",
    "OutletReader", "SkuView", "plan_digest",
    "parse_sheet_price", "parse_dlq", "check_dlq", "baseline_preview", "baseline_apply",
]
