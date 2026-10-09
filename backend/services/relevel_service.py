"""The per-key re-level (plan ``docs/f7-mysql-adapter-plan.md`` §3.4; the
mechanisms in ``docs/f7-build-notes.md`` §Re-level).

The gesture that releases a held key — besides the N-0 release and, for an
in-flight hold, its row's close — and the one that levels a key not held (a
DRAFT-parked one). Preview → confirm, under §3.4's precondition (the ``/dlqs``
GET, typed) and the pass lease: a publish-class holder (recovery and the
held-key lowering at its take), which yields to a waiting decrement between
SKUs (§3.3 rule 6).

A re-level's target is N whatever the key's pending mix; its ``RECONCILE`` row
carries every unstamped event of the key up to its snapshot — pending and held,
a ``HELD`` row's included, so a landed +Δ it releases is never re-added:

* one ACTIVE outlet with a variant titled exactly ``SIZE`` → DENY + tracked
  first, the explicit activation of an absent or inactive level before a raise,
  the variant set to N (a ``relevel`` row, relative from the live read), then the
  publish re-run in a group of its own (idempotent by state: an ACTIVE outlet left
  unpublished — a tail refused at its publish step — is healed, surfaced);
* its only outlet a DRAFT → zero-then-revive: the revive zeroes the key's own
  variant with its other zeroings, adds N, and its tail exposes;
* no outlet → CREATE with N;
* N 0 → every outlet's real-size Promo of the size lowered to 0, the events
  stamped (nothing to expose).

FIX4 on the SKU's one price and one compare-at (§3.5, B-2) before any exposure.
On success a per-key ``BASELINE`` row (reason "relevel") releases the key's older
holds. Refused, the key left as it is and listed: a blank SKU, a row of the key
not closed, an unclean resolution or an unreadable outlet, a multi-outlet key with
N > 0 (its exit first, §3.5), no variant titled ``SIZE`` with N > 0, an upward
write meeting ``committed`` > 0, FIX4, no single source holding the size. At the
confirm a key whose N, max event id, events or live level moved since the preview
is held — a ``HOLD`` row when it was not — and nothing is written for it; the
held keys are then lowered by their excess, never raised.
"""
from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from backend.persistence.records import (
    APPLIED,
    DONE,
    HOLD_IN_FLIGHT,
    HOLD_ROW,
    SCOPE_KEY,
    BaselineMissing,
    Hold,
    Key,
    LeaseLost,
    StoreError,
)
from backend.services import baseline_service as BS
from backend.services import flows
from backend.services import ledger as L
from backend.services import outlet_service as OS
from backend.services import passes, resolvers, variants
from backend.services.lease import HOLDER_RELEVEL, LeaseUnavailable, PassLease
from backend.services.promo_gate import require_promo_location
from backend.services.recovery import CLASS_PUBLISH
from backend.shopify import ops
from backend.shopify.ops import ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.relevel_service")

BR_ACTIVE = "active"
BR_REVIVE = "revive"
BR_CREATE = "create"
BR_LOWER = "lower"
BR_REFUSE = "refuse"

F_BLANK = "blank_sku"
F_IN_FLIGHT = "row_not_closed"
F_UNRESOLVED = "unresolved"
F_UNREADABLE = "outlet_unreadable"
F_MULTI = "multi_outlet"
F_SIZE = "size_absent"
F_STATUS = "unexpected_status"
F_COMMITTED = "committed_on_upward_adjust"
F_PRICE = "fix4"
F_SOURCE = "source"

R_MOVED = "moved_at_relevel_confirm"

ST_APPLIED = "APPLIED"
ST_PRECONDITION_REFUSED = "PRECONDITION_REFUSED"
ST_LEASE_UNAVAILABLE = "LEASE_UNAVAILABLE"
ST_BASELINE_MISSING = "BASELINE_MISSING"
ST_INCOMPLETE = "INCOMPLETE"

RELEVEL_TAKE_BOUND_S = 120.0

_SHOPIFY_ERRORS = (ShopifyTransportError, ShopifyUserError, ProductNotFound)


@dataclass(frozen=True)
class RelevelKey:
    """One key to re-level: the snapshot's facts and the branch."""

    sku: str
    size: str
    n: int
    shelf_pair_ids: Tuple[int, ...]
    event_ids: Tuple[int, ...]             # every unstamped event of the key, pending and held
    max_event_id: int
    held: Tuple[str, ...]                  # the kinds of its holds now
    branch: str
    reason: Optional[str] = None
    outlets: Tuple[BS.VariantFacts, ...] = ()
    delta: int = 0                         # the planned Promo change
    activate: bool = False
    price: Optional[str] = None
    compare_at: Optional[str] = None
    source_gid: Optional[str] = None
    source_variant_id: Optional[str] = None
    source_item_id: Optional[str] = None
    outlet_title: Optional[str] = None

    @property
    def key(self) -> Key:
        return (self.sku, self.size)


@dataclass(frozen=True)
class RelevelPlan:
    dry_run: bool
    previewed_at: Optional[datetime]
    keys: Tuple[RelevelKey, ...]
    plan_hash: str


@dataclass(frozen=True)
class RelevelOutcome:
    sku: str
    size: str
    branch: str
    result: str                            # released | refused | held | failed
    reason: Optional[str] = None
    baseline_row_id: Optional[int] = None


@dataclass(frozen=True)
class RelevelReport:
    status: str
    reason: Optional[str] = None
    outcomes: Tuple[RelevelOutcome, ...] = ()
    surfaced: Tuple[Dict[str, Any], ...] = ()


# =============================================================================
# Preview
# =============================================================================
def _source(transport: Any, sku: str, size: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[str]]:
    """The SKU's one source product and its variant titled ``size``, or why not."""
    try:
        srcs = resolvers.source_resolver(transport, sku).get("matches") or []
        if len(srcs) != 1:
            return None, None, "none" if not srcs else "several"
        v = variants.match_exact(size, ops.get_product_variants(transport, srcs[0]["product_gid"]))
    except (ShopifyTransportError, ShopifyUserError, RuntimeError) as e:
        return None, None, f"unreadable:{type(e).__name__}"
    return (srcs[0], v, None) if v is not None else (None, None, "size_absent")


def _plan_key(register: Any, transport: Any, reader: BS.OutletReader, k: Key, holds: Sequence[Hold],
              promo: str) -> RelevelKey:
    sku, size = k
    snap = register.snapshot(k)
    events = [e for e in snap.unstamped if e.key == k]
    kinds = tuple(sorted({h.kind for h in holds if h.key == k}))
    n = snap.shelf_count(k)
    base = dict(sku=sku, size=size, n=n, shelf_pair_ids=tuple(sorted(p.pair_id for p in snap.pairs)),
                event_ids=tuple(sorted(e.event_id for e in events)), max_event_id=snap.max_event_id.get(k, 0),
                held=kinds)

    def refuse(reason: str, **kw: Any) -> RelevelKey:
        return RelevelKey(**base, branch=BR_REFUSE, reason=reason, **kw)

    if not sku:
        return refuse(F_BLANK)
    if HOLD_IN_FLIGHT in kinds:
        return refuse(F_IN_FLIGHT)                 # an in-flight hold ends with its row's close
    view = reader.sku_view(sku)
    if not view.clean:
        return refuse(F_UNRESOLVED)
    if not view.readable:
        return refuse(F_UNREADABLE)
    facts = view.facts(size, promo)
    if any(f.level and (f.available is None or f.committed is None) for f in facts):
        return refuse(F_UNREADABLE, outlets=facts)
    if n == 0:                                     # nothing to expose: every outlet lowered to 0
        if any((f.committed or 0) > 0 for f in facts):
            return refuse(BS.R_UNIT_HELD, outlets=facts)   # a unit only held returns to sale (rule 4, D33)
        return RelevelKey(**base, branch=BR_LOWER, outlets=facts, delta=-sum(f.p for f in facts))
    if len(view.outlets) > 1:
        return refuse(F_MULTI, outlets=facts)
    shelf = register.shelf(sku)
    pr = OS.sku_prices(shelf)
    ok, why = OS.validate_price(pr.price, pr.compare_at)
    lacking = set(base["shelf_pair_ids"]) & set(pr.no_compare_at_pairs)
    problem = pr.blocked or (None if ok else why) or (
        "no_compare_at:" + ",".join(str(i) for i in sorted(lacking)) if lacking else None)   # D25: the pairs named
    if problem:
        return refuse(f"{F_PRICE}:{problem}", outlets=facts)
    prices = dict(price=pr.price, compare_at=pr.compare_at)
    if not view.outlets:
        src, v, why = _source(transport, sku, size)
        if src is None:
            return refuse(f"{F_SOURCE}:{why}")
        return RelevelKey(**base, branch=BR_CREATE, delta=n, source_gid=src["product_gid"],
                          source_variant_id=v.get("id"), source_item_id=variants.item_id(v),
                          outlet_title=OS.outlet_title(src.get("title") or ""), **prices)
    o, f = view.outlets[0], facts[0]
    if f.variant_id is None:
        return refuse(F_SIZE, outlets=facts)
    if o.status == "ACTIVE":
        activate = not (f.level and f.active)
        delta = n - (0 if activate else (f.available or 0))
        if (activate or delta > 0) and (f.committed or 0) > 0:
            return refuse(F_COMMITTED, outlets=facts)
        return RelevelKey(**base, branch=BR_ACTIVE, outlets=facts, delta=delta, activate=activate, **prices)
    if o.status == "DRAFT":
        return RelevelKey(**base, branch=BR_REVIVE, outlets=facts, delta=n - f.p, **prices)
    return refuse(f"{F_STATUS}:{o.status or 'NONE'}", outlets=facts)


def relevel_preview(register: Any, transport: Any, keys: Sequence[Sequence[str]], *,
                    promo_location_id: str) -> RelevelPlan:
    """The re-level PLAN of ``keys`` (READ-ONLY)."""
    now = register.server_now()
    holds = register.held_keys()
    reader = BS.OutletReader(transport)
    wanted = sorted({(str(k[0]), str(k[1])) for k in keys})
    plan_keys = tuple(_plan_key(register, transport, reader, k, holds, promo_location_id) for k in wanted)
    doc = {"at": now.isoformat(), "keys": [asdict(k) for k in plan_keys]}
    return RelevelPlan(dry_run=True, previewed_at=now, keys=plan_keys, plan_hash=BS.plan_digest(doc))


# =============================================================================
# Apply
# =============================================================================
class _Run:
    def __init__(self, ctx: flows.Context, approved: RelevelPlan, dlq: BS.DlqCheck) -> None:
        self.ctx = ctx
        self.reg = ctx.register
        self.led = ctx.ledger
        self.promo = ctx.promo
        self.approved = approved
        self.dlq = dlq
        self.reader = BS.OutletReader(ctx.transport, keepalive=ctx.ledger.lease.keepalive)
        self.outcomes: List[RelevelOutcome] = []
        self.newly_held: List[Hold] = []

    def _out(self, kp: RelevelKey, result: str, reason: Optional[str] = None, row: Optional[int] = None) -> None:
        self.outcomes.append(RelevelOutcome(kp.sku, kp.size, kp.branch, result, reason, row))

    def key(self, kp: RelevelKey) -> None:
        if kp.branch == BR_REFUSE:
            self._out(kp, "refused", kp.reason)
            return
        holds = [h for h in self.reg.held_keys() if h.key == kp.key]
        if any(h.kind == HOLD_IN_FLIGHT for h in holds):
            self._out(kp, "refused", F_IN_FLIGHT)
            return
        if self._moved(kp):
            if not holds:
                row = self.reg.record_hold(self.led.lease, keys=[kp.key], reason=R_MOVED,
                                           event_ids=kp.event_ids, action="relevel-moved")
                self.newly_held.append(Hold(key=kp.key, kind=HOLD_ROW, reason=R_MOVED,
                                            event_ids=frozenset(kp.event_ids), row_id=row,
                                            created_at=self.reg.server_now()))
            self._out(kp, "held", R_MOVED)
            return
        if kp.branch == BR_ACTIVE:
            stamped = self._active(kp)
        elif kp.branch == BR_LOWER:
            stamped = self._lower(kp)
        else:
            stamped = self._expose(kp)
        if stamped is None:
            return
        row = self.reg.record_baseline(
            self.led.lease, scope=SCOPE_KEY, action="relevel", plan_hash=self.approved.plan_hash,
            payload={"key": [kp.sku, kp.size], "reason": "relevel", "branch": kp.branch, "n": kp.n,
                     "shelf_pair_ids": list(kp.shelf_pair_ids), "event_ids": list(kp.event_ids),
                     "released": list(kp.held),
                     "dlq": {"checked_at": self.dlq.checked_at.isoformat(), "count": self.dlq.count}},
            stamp_ids=stamped,
        )
        self._out(kp, "released", None, row)

    def _moved(self, kp: RelevelKey) -> bool:
        """§3.3 rule 6: right before its write, the key's N, max event id (any Δ),
        events and live level, as the confirm approved them."""
        snap = self.reg.snapshot(kp.key)
        if snap.shelf_count(kp.key) != kp.n or snap.max_event_id.get(kp.key, 0) != kp.max_event_id:
            return True
        if tuple(sorted(e.event_id for e in snap.unstamped if e.key == kp.key)) != kp.event_ids:
            return True
        view = self.reader.sku_view(kp.sku, fresh=True)
        return not view.readable or view.facts(kp.size, self.promo) != kp.outlets

    # -- the branches: each returns the ids its BASELINE row still stamps, or None (not released)
    def _active(self, kp: RelevelKey) -> Optional[Tuple[int, ...]]:
        f = kp.outlets[0]
        common = dict(keys=(kp.key,), inventory_item_id=f.inventory_item_id, location_id=self.promo,
                      variant_id=f.variant_id, product_id=f.product_id)
        inv = self.ctx.inventory(flows.product_gid(f.product_id))
        keys = tuple(sorted({(kp.sku, variants.variant_title(v)) for v in variants.real_size_variants(inv)}))
        # DENY + tracked and the SKU's FIX4-checked prices on every variant, before the level (as the publish)
        specs: List[L.RowSpec] = [L.RowSpec(action=L.A_NORMALISE, keys=keys, product_id=f.product_id,
                                            extra={"price": kp.price, "compare_at": kp.compare_at})]
        if kp.activate:
            specs.append(L.RowSpec(action=L.A_ACTIVATE, event_ids=kp.event_ids, **common))
        specs.append(L.RowSpec(action=L.A_ADJUST, purpose=L.P_RELEVEL, target=kp.n, event_ids=kp.event_ids,
                               extra={"n": kp.n, "shelf_pair_ids": list(kp.shelf_pair_ids)}, **common))
        rows = self.led.write_group(specs)
        gate: Optional[str] = None
        for r in rows:
            if r.action == L.A_NORMALISE:
                gate = flows.run_normalise(self.ctx, r)
            elif r.action == L.A_ACTIVATE:
                gate = self.led.send_activate(r) if gate in (None, APPLIED) else self.led.never_sent(r, "normalise_not_applied")
            elif gate not in (None, APPLIED):
                self.led.never_sent(r, "previous_step_not_applied")
            else:
                self.led.send_adjust(r)
        settled = self.led.settle(rows[0].group_id)
        if settled not in ("stamped", "closed"):
            self._out(kp, "failed", f"group:{settled}")
            return None
        # the publish re-run, a group of its own: idempotent by state, its refusal surfaced (terminal path (i))
        (tail,) = self.led.write_group([L.RowSpec(action=L.A_TAIL, keys=(kp.key,), product_id=f.product_id)])
        if flows.run_tail(self.ctx, tail) != APPLIED:
            self.ctx.report.surface("relevel_publish_not_applied", key=list(kp.key), product_id=f.product_id)
        self.led.settle(tail.group_id)
        self._online(kp)
        return ()

    def _online(self, kp: RelevelKey) -> None:
        """``ONLINE`` 1 on the key's shelf pairs it exposed (§3.1), those not at 1 yet."""
        ids = set(kp.shelf_pair_ids)
        off = [p.pair_id for p in self.reg.shelf(kp.sku) if p.pair_id in ids and not p.online]
        if off:
            self.reg.set_online(self.led.lease, off, True)

    def _expose(self, kp: RelevelKey) -> Optional[Tuple[int, ...]]:
        """DRAFT → zero-then-revive; no outlet → CREATE — the publish's own flows
        with the key's target N (they stamp its events and set ``ONLINE``)."""
        f = kp.outlets[0] if kp.outlets else None
        target = OS.SizeTarget(size=kp.size, event_ids=kp.event_ids, delta=kp.n, matched=True,
                               variant_id=f.variant_id if f else kp.source_variant_id,
                               inventory_item_id=f.inventory_item_id if f else kp.source_item_id)
        branch = OS.BRANCH_DRAFT if kp.branch == BR_REVIVE else OS.BRANCH_CREATE
        gid = flows.product_gid(f.product_id) if f else None
        action = OS.PlanAction(
            sku=kp.sku, branch=branch, publishable=True, reason=None, target_gid=gid, source_gid=kp.source_gid,
            outlet_title=kp.outlet_title, live_status=branch if f else None, price=kp.price,
            compare_at=kp.compare_at, price_ok=True, size_targets=(target,), unmatched_sizes=(), warnings=(),
            plan_hash=self.approved.plan_hash,
        )
        if kp.branch == BR_REVIVE:
            out = OS.revive_outlet(self.ctx, action, action, zero_titles=frozenset({kp.size}))
            ok = out.status == "REVIVED"
        else:
            out = OS.create_outlet(self.ctx, action, action)
            ok = out.status == "APPLIED"
        if not ok:
            self._out(kp, "failed", f"{out.status}:{'|'.join(out.warnings)}")
            return None
        self._online(kp)
        return ()

    def _lower(self, kp: RelevelKey) -> Optional[Tuple[int, ...]]:
        specs = [
            L.RowSpec(action=L.A_ADJUST, keys=(kp.key,), purpose=L.P_ZERO, target=0,
                      inventory_item_id=f.inventory_item_id, location_id=self.promo, variant_id=f.variant_id,
                      product_id=f.product_id)
            for f in kp.outlets if f.p > 0 and f.inventory_item_id
        ]
        if specs:
            rows = self.led.write_group(specs)
            for r in rows:
                self.led.send_adjust(r)
            self.led.settle(rows[0].group_id)
            final = [self.led.reload(r) for r in rows]
            if not all(r.status == DONE or (r.outcome or {}).get("reason") == "nothing_to_lower" for r in final):
                self._out(kp, "failed", "lowering_not_applied")
                return None
        return kp.event_ids

    def lower_newly_held(self) -> None:
        if not self.newly_held:
            return
        ids = {h.key: h.event_ids for h in self.newly_held}
        post = [e for e in self.reg.unstamped_events() if e.key in ids and e.event_id not in ids[e.key]]
        flows.held_key_lowering(self.ctx, holds=self.newly_held, pending=post, carried=[])

    def report(self, status: str, reason: Optional[str] = None) -> RelevelReport:
        return RelevelReport(status, reason, tuple(self.outcomes), tuple(self.ctx.report.items))


def relevel_apply(
    register: Any,
    transport: Any,
    approved: RelevelPlan,
    *,
    promo_location_id: str,
    dlq: Optional[BS.DlqCheck],
    publication_id: Optional[str] = None,
    lease: Optional[PassLease] = None,
    take_bound_s: float = RELEVEL_TAKE_BOUND_S,
    sleep: Callable[[float], None] = time.sleep,
) -> RelevelReport:
    """Execute the approved re-level, key by key, yielding between SKUs."""
    why = BS.check_dlq(dlq, previewed_at=approved.previewed_at, now=register.server_now())
    if why is not None:
        return RelevelReport(ST_PRECONDITION_REFUSED, why)
    assert dlq is not None
    require_promo_location(transport, promo_location_id)          # E18: before the lease, nothing written
    pass_lease = lease or PassLease(register, HOLDER_RELEVEL, sleep=sleep)
    try:
        pass_lease.take(bound_s=take_bound_s)
    except LeaseUnavailable:
        return RelevelReport(ST_LEASE_UNAVAILABLE, "lease_held_by_another_pass")
    ctx = flows.Context(L.Ledger(register, transport, pass_lease, promo_location_id, sleep=sleep),
                        publication_id=publication_id)
    run = _Run(ctx, approved, dlq)
    by_sku: Dict[str, List[RelevelKey]] = {}
    for kp in approved.keys:
        by_sku.setdefault(kp.sku, []).append(kp)
    try:
        passes.prologue(ctx, CLASS_PUBLISH)
        for i, (sku, kps) in enumerate(sorted(by_sku.items())):
            if i:
                pass_lease.retake()                # the hand-off: a waiting decrement gets in
                if pass_lease.others_between:
                    passes.prologue(ctx, CLASS_PUBLISH)
            for kp in kps:
                try:
                    run.key(kp)
                except _SHOPIFY_ERRORS as e:
                    ctx.report.surface("relevel_key_error", key=list(kp.key), error=type(e).__name__)
                    run.outcomes.append(RelevelOutcome(kp.sku, kp.size, kp.branch, "failed",
                                                       f"shopify_error:{type(e).__name__}"))
        run.lower_newly_held()
        return run.report(ST_APPLIED)
    except BaselineMissing:
        return run.report(ST_BASELINE_MISSING, "no_whole_register_baseline")
    except (LeaseLost, LeaseUnavailable):
        return run.report(ST_INCOMPLETE, "lease_lost")
    except StoreError as e:
        return run.report(ST_INCOMPLETE, f"register_error:{type(e).__name__}")
    finally:
        pass_lease.release()


__all__ = [
    "BR_ACTIVE", "BR_REVIVE", "BR_CREATE", "BR_LOWER", "BR_REFUSE",
    "F_BLANK", "F_IN_FLIGHT", "F_UNRESOLVED", "F_UNREADABLE", "F_MULTI", "F_SIZE", "F_STATUS", "F_COMMITTED",
    "F_PRICE", "F_SOURCE", "R_MOVED",
    "ST_APPLIED", "ST_PRECONDITION_REFUSED", "ST_LEASE_UNAVAILABLE", "ST_BASELINE_MISSING", "ST_INCOMPLETE",
    "RELEVEL_TAKE_BOUND_S", "RelevelKey", "RelevelPlan", "RelevelOutcome", "RelevelReport",
    "relevel_preview", "relevel_apply",
]
