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

Groups (§3.3): ``pending_deltas()`` grouped by exact (SKU, SIZE); Δ = the sum,
net of what ``CARRIED`` rows already moved over those events (the remainder).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.persistence.records import EventRow, Key, LeaseLost, PendingDeltas, StoreError
from backend.services import flows
from backend.services import ledger as L
from backend.services.lease import HOLDER_DECREMENT, LeaseUnavailable, PassLease
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


def prologue(ctx: "flows.Context", klass: str) -> None:
    """Recovery + the held-key lowering when another holder held the lease
    since this holder's previous generation (or at a first take)."""
    if ctx.ledger.lease.others_between:
        recover(ctx, klass)
        pend = ctx.register.pending_deltas()
        flows.held_key_lowering(ctx, holds=pend.holds, pending=pend.events, carried=pend.carried)


@dataclass
class PassReport:
    applied: Dict[str, str] = field(default_factory=dict)  # "SKU|SIZE" → outcome
    report: L.Report = field(default_factory=L.Report)
    lease_generation: Optional[int] = None

    def as_dict(self) -> Dict[str, Any]:
        return {"applied": dict(self.applied), "surfaced": list(self.report.items),
                "lease_generation": self.lease_generation}


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


__all__ = ["KeyGroup", "key_groups", "prologue", "PassReport", "decrement_pass",
           "CLASS_PUBLISH", "CLASS_DECREMENT", "LeaseUnavailable"]
