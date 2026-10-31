"""The passes — plan ``docs/f7-mysql-adapter-plan.md`` §3.3 rule 6, §6 B-3.

Every pass holds the pass lease. Whoever takes it — first start, take-over, or a
re-take after ANOTHER holder held it in between — runs its class's recovery and
then the held-key lowering BEFORE anything else (:func:`prologue`); a holder
re-taking after its own release, with nobody in between, skips both (its own
open rows wait). The N-0 release runs before grouping, every pass.

* :func:`decrement_pass` — B-3's "decrement only" pass (D23): the trigger route,
  the boot pass and the working-window net all run it. It waits for the lease
  (the hand-off: no other holder re-takes while it waits), then applies every
  Δ < 0 and Δ = 0 group — held keys included, on their post-hold events — and
  drafts at zero. It never adds, activates or creates: Δ > 0 groups stay pending
  for the publish, surfaced.
* :class:`Holder` — the run of a decrement-class holder of plan §3.5 (pricing, the
  delete, the bulk cleanup, the multi-outlet exit, deny-normalize) and of a
  person's close (rule 2's (iv)): the take, its class's recovery and the held-key
  lowering, and the yield between SKUs / products (rule 6).

Groups (§3.3): ``pending_deltas()`` grouped by exact (SKU, SIZE); Δ = the sum,
net of what ``CARRIED`` rows already moved over those events (the remainder).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.persistence.records import BaselineMissing, EventRow, Key, LeaseLost, PendingDeltas, StoreError
from backend.services import flows
from backend.services import ledger as L
from backend.services.lease import HOLDER_DECREMENT, LeaseUnavailable, PassLease
from backend.services.promo_gate import require_promo_location
from backend.services.recovery import CLASS_DECREMENT, CLASS_PUBLISH, recover
from backend.shopify.ops import ProductNotFound, ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.services.passes")


@dataclass(frozen=True)
class KeyGroup:
    key: Key
    events: Tuple[EventRow, ...]
    delta: int  # net of CARRIED shares

    @property
    def event_ids(self) -> Tuple[int, ...]:
        return tuple(e.event_id for e in self.events)


def key_groups(pend: PendingDeltas) -> List[KeyGroup]:
    """Group the readable pending events by exact key, in first-event order."""
    order: List[Key] = []
    by_key: Dict[Key, List[EventRow]] = {}
    for e in pend.events:
        if e.key not in by_key:
            order.append(e.key)
            by_key[e.key] = []
        by_key[e.key].append(e)
    out = []
    for k in order:
        evs = tuple(by_key[k])
        net = sum(e.delta for e in evs) - L.carried_share(pend.carried, k, [e.event_id for e in evs])
        out.append(KeyGroup(k, evs, net))
    return out


def prologue(ctx: "flows.Context", klass: str, *, require_baseline: bool = True) -> None:
    """Recovery + the held-key lowering when another holder held the lease
    since this holder's previous generation (or at a first take).

    Before the whole-register ``BASELINE`` row the lowering has no pending set to
    count (every event is unstamped until the baseline declares what the outlet
    reflects — the formula would lower units the outlet rightly shows), so it
    cannot run: a holder that refuses before the baseline (publish, re-level)
    raises :class:`BaselineMissing` here; one that may run before it (the §3.5
    verticals, a person's close — ``require_baseline=False``) runs its recovery
    and skips the lowering, surfaced."""
    if ctx.ledger.lease.others_between:
        recover(ctx, klass)
        try:
            pend = ctx.register.pending_deltas()
        except BaselineMissing:
            if require_baseline:
                raise
            ctx.report.surface("held_key_lowering_skipped", reason="baseline_missing")
            return
        flows.held_key_lowering(ctx, holds=pend.holds, pending=pend.events, carried=pend.carried)


@dataclass
class PassReport:
    applied: Dict[str, str] = field(default_factory=dict)  # "SKU|SIZE" → outcome
    report: L.Report = field(default_factory=L.Report)
    lease_generation: Optional[int] = None

    def as_dict(self) -> Dict[str, Any]:
        return {"applied": dict(self.applied), "surfaced": list(self.report.items),
                "lease_generation": self.lease_generation}


# How long a §3.5 holder waits for the lease (a decrement or a publish in flight).
HOLDER_TAKE_BOUND_S = 120.0


class Holder:
    """A decrement-class run that may start before the whole-register baseline
    (``prologue(require_baseline=False)``): :meth:`start` takes the lease
    (:class:`LeaseUnavailable` past ``take_bound_s``) and runs the prologue;
    :meth:`next_item` yields between items — release, re-take (a waiting decrement
    gets in first), the prologue again only when another holder came in between;
    :meth:`release` ends it. ``ctx`` is the run's flow context (its ledger, its
    report)."""

    def __init__(self, register: Any, transport: Any, promo_location_id: str, kind: str, *,
                 lease: Optional[PassLease] = None, take_bound_s: float = HOLDER_TAKE_BOUND_S,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.register = register
        self.lease = lease or PassLease(register, kind, sleep=sleep)
        self.take_bound_s = take_bound_s
        self.ctx = flows.Context(L.Ledger(register, transport, self.lease, promo_location_id, sleep=sleep))
        self._items = 0

    @property
    def report(self) -> L.Report:
        return self.ctx.report

    def start(self) -> None:
        require_promo_location(self.ctx.transport, self.ctx.promo)   # E18: before the lease, nothing written
        self.lease.take(bound_s=self.take_bound_s)
        prologue(self.ctx, CLASS_DECREMENT, require_baseline=False)

    def next_item(self) -> None:
        if self._items:
            self.lease.retake()                       # the hand-off: a waiting decrement gets in
            if self.lease.others_between:
                prologue(self.ctx, CLASS_DECREMENT, require_baseline=False)
        self._items += 1

    def release(self) -> None:
        self.lease.release()


def _backfill_outlet_products(ctx: flows.Context, out: PassReport) -> None:
    """§3.1 on every pass: a published shelf pair (``ONLINE`` 1) still lacking
    ``OUTLET_PRODUCT_ID`` — the publish resolved it before the collection
    re-indexed, a ``review`` match — gets it once the SKU resolves to one
    non-``review`` outlet. One resolver read per such SKU; none once filled."""
    skus = sorted({p.sku for p in ctx.register.shelf() if p.online and p.outlet_product_id is None})
    for sku in skus:
        try:
            ctx.ledger.lease.keepalive()                  # pure Shopify reads: nothing else renews across the loop
            flows.maybe_set_outlet_product(ctx, sku, ctx.resolve(sku))
        except LeaseLost:
            raise
        except (ShopifyUserError, ShopifyTransportError, ProductNotFound, StoreError) as e:
            out.report.surface("outlet_product_backfill_error", sku=sku, error=type(e).__name__)


def decrement_pass(
    register: Any,
    transport: Any,
    promo_location_id: str,
    *,
    lease: Optional[PassLease] = None,
    sleep: Callable[[float], None] = time.sleep,
    wait_bound_s: Optional[float] = None,
) -> PassReport:
    """B-3's pass. Raises :class:`LeaseUnavailable` when the lease is not handed
    over within its bound (the caller surfaces it; nothing was written)."""
    require_promo_location(transport, promo_location_id)          # E18: before the lease, nothing written
    lease = lease or PassLease(register, HOLDER_DECREMENT, sleep=sleep)
    if wait_bound_s is None:
        lease.take_waiting()
    else:
        lease.take_waiting(bound_s=wait_bound_s)
    out = PassReport(lease_generation=lease.generation)
    led = L.Ledger(register, transport, lease, promo_location_id, report=out.report, sleep=sleep)
    ctx = flows.Context(led)
    try:
        prologue(ctx, CLASS_DECREMENT)
        flows.n0_release(ctx)
        _backfill_outlet_products(ctx, out)
        pend = register.pending_deltas()
        for e in pend.keyless:
            out.report.surface("consume_miss", event_id=e.event_id)
        for g in key_groups(pend):
            label = f"{g.key[0]}|{g.key[1]}"
            if g.delta > 0:
                out.report.surface("positive_left_for_publish", key=list(g.key), delta=g.delta)
                continue
            try:
                if g.delta < 0:
                    out.applied[label] = flows.minus_group(ctx, g.key, g.events, g.delta)
                else:
                    out.applied[label] = flows.zero_group(ctx, g.key, g.events)
            except LeaseLost:
                raise
            except (ShopifyUserError, ShopifyTransportError, ProductNotFound, StoreError) as e:
                # one key's failure never aborts the pass; a stamp failure triggers recovery next time
                out.applied[label] = f"error:{type(e).__name__}"
                out.report.surface("group_error", key=list(g.key), error=type(e).__name__)
    finally:
        lease.release()
    return out


__all__ = ["KeyGroup", "key_groups", "prologue", "PassReport", "decrement_pass", "Holder", "HOLDER_TAKE_BOUND_S",
           "CLASS_PUBLISH", "CLASS_DECREMENT", "LeaseUnavailable"]
