"""The pass lease, client side (plan ``docs/f7-mysql-adapter-plan.md`` §3.3 rule 6).

One pass at a time, across processes: every F7 flow that writes the database or
Shopify holds the one ``sm_outlet_lease`` row. The statements are the store's
(``lease_take`` / renew-first ``fenced_write`` / ``lease_release`` /
``lease_wait`` — see ``docs/f7-build-notes.md`` §Storage); this module owns the
CLIENT rules around them:

* **generation** — every take increments it (the fencing token every
  ``RECONCILE`` row carries); a re-take whose generation is not the holder's
  previous + 1 means another holder held the lease in between
  (:attr:`PassLease.others_between`: recovery and the held-key sweep run again);
* **local deadline** — a Shopify call is sent only before ``renewed_at + TTL −
  SEND_MARGIN`` on the app's monotonic clock, counted from the START of the
  renew that preceded it (conservative: the server stamps its expiry later);
* **keepalive** — before any call, a holder whose last renew is older than
  ``RENEW_AFTER`` renews first, so every call starts with at least
  ``TTL − RENEW_AFTER`` of lease left;
* **hand-off** — only the decrement waits: it marks itself waiting (a short,
  refreshed waiter expiry), and every other holder's take refuses while a live
  waiter that is not itself is marked, so a holder releasing between SKUs
  (products) lets the waiting decrement in before it re-takes.

Parameters (``docs/f7-build-notes.md`` §Parameters): the longest single Shopify
call with its retries, on ``backend/shopify/transport.py``'s constants, is a read
— 5 attempts × 30 s timeout + backoffs 1+2+4+8+8 s + 5 × 0.7 s throttle ≈ 177 s
(a mutation is never re-posted blind, rule 9: one attempt ≤ 30 s plus at most four
throttled re-sends). TTL 300 s exceeds it with ``RENEW_AFTER`` 60 s of slack
(300 − 60 = 240 s > 177 s); a send must start within 240 s of its renew.
"""
from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from backend.persistence.records import Fence, LeaseLost

LEASE_TTL_MS = 300_000
SEND_MARGIN_MS = 60_000
RENEW_AFTER_MS = 60_000
# The decrement's waiter flag: short, refreshed on every poll (a dead waiter
# stops blocking takes within this), and the poll interval.
WAITER_TTL_MS = 60_000
WAIT_POLL_S = 2.0
# How long the decrement waits for the lease before ending its run, surfaced:
# one TTL (a crashed holder's lease expires) + one minute.
DECREMENT_WAIT_BOUND_S = (LEASE_TTL_MS + 60_000) / 1000.0
# How long a releasing holder waits to re-take (a decrement runs in between).
RETAKE_BOUND_S = 600.0

HOLDER_PUBLISH = "publish"
HOLDER_DECREMENT = "decrement"
HOLDER_BASELINE = "baseline"
HOLDER_RELEVEL = "relevel"
HOLDER_PRICING = "pricing"
HOLDER_DELETE = "delete"
HOLDER_CLEANUP = "cleanup"
HOLDER_EXIT = "exit"
HOLDER_NORMALIZE = "normalize"
HOLDER_CLOSE = "close"


class LeaseUnavailable(RuntimeError):
    """The lease could not be taken within the holder's bound (another pass
    holds it); nothing was written. Surfaced, never retried blind."""


class PassLease:
    """One holder's view of the pass lease (implements
    :class:`~backend.persistence.ports.FenceHolder`)."""

    def __init__(
        self,
        register: Any,
        kind: str,
        *,
        ttl_ms: int = LEASE_TTL_MS,
        send_margin_ms: int = SEND_MARGIN_MS,
        renew_after_ms: int = RENEW_AFTER_MS,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        holder_id: Optional[str] = None,
    ) -> None:
        if not 0 < send_margin_ms < ttl_ms or not 0 < renew_after_ms <= ttl_ms - send_margin_ms:
            raise ValueError("need 0 < send_margin < ttl and 0 < renew_after <= ttl - send_margin")
        self.register = register
        self.kind = kind
        self.holder = holder_id or f"{kind}:{uuid.uuid4().hex[:12]}"
        self.ttl_ms = int(ttl_ms)
        self.send_margin_ms = int(send_margin_ms)
        self.renew_after_ms = int(renew_after_ms)
        self._clock = clock
        self._sleep = sleep
        self._generation: Optional[int] = None
        self._previous: Optional[int] = None
        self._renewed_at: Optional[float] = None
        self.others_between = True
        self.takes = 0

    # -- state -------------------------------------------------------------
    @property
    def held(self) -> bool:
        return self._generation is not None

    @property
    def generation(self) -> Optional[int]:
        return self._generation

    @property
    def fence(self) -> Fence:
        if self._generation is None:
            raise LeaseLost("lease not held")
        return Fence(self.holder, self._generation, self.ttl_ms)

    def monotonic(self) -> float:
        return self._clock()

    def mark_renewed(self, started_monotonic: float) -> None:
        self._renewed_at = started_monotonic

    # -- take / release ---------------------------------------------------------
    def try_take(self) -> bool:
        """One guarded take. On success records whether another holder held the
        lease since this holder's previous generation (``others_between``)."""
        if self.held:
            raise RuntimeError("lease already held by this holder")
        started = self._clock()
        gen = self.register.lease_take(self.holder, self.ttl_ms)
        if gen is None:
            return False
        self.others_between = self._previous is None or gen != self._previous + 1
        self._generation = gen
        self._renewed_at = started
        self.takes += 1
        return True

    def take(self, *, bound_s: float = 0.0, poll_s: float = WAIT_POLL_S) -> None:
        """Take now, or poll until ``bound_s``; :class:`LeaseUnavailable` past it."""
        deadline = self._clock() + bound_s
        while not self.try_take():
            if self._clock() >= deadline:
                raise LeaseUnavailable(f"{self.kind}: lease held by another pass")
            self._sleep(poll_s)

    def take_waiting(
        self, *, bound_s: float = DECREMENT_WAIT_BOUND_S, poll_s: float = WAIT_POLL_S,
        waiter_ttl_ms: int = WAITER_TTL_MS,
    ) -> None:
        """The decrement's take: when the lease is held, mark itself waiting
        (refreshed every poll) so no other holder re-takes before it."""
        deadline = self._clock() + bound_s
        while not self.try_take():
            self.register.lease_wait(self.holder, waiter_ttl_ms)
            if self._clock() >= deadline:
                raise LeaseUnavailable(f"{self.kind}: lease not handed over within {bound_s:.0f}s")
            self._sleep(poll_s)

    def release(self) -> None:
        """Release (guarded on holder + generation). Never raises: a lost lease
        is simply not ours any more."""
        if self._generation is None:
            return
        try:
            self.register.lease_release(self)
        finally:
            self._previous = self._generation
            self._generation = None
            self._renewed_at = None

    def retake(self, *, bound_s: float = RETAKE_BOUND_S, poll_s: float = WAIT_POLL_S) -> None:
        """Release, then re-take — the between-SKUs (products) yield of every
        holder but the baseline. A waiting decrement takes the lease in between."""
        self.release()
        self.take(bound_s=bound_s, poll_s=poll_s)

    # -- the local deadline -------------------------------------------------------
    def send_deadline(self) -> float:
        if self._renewed_at is None:
            return float("-inf")
        return self._renewed_at + (self.ttl_ms - self.send_margin_ms) / 1000.0

    def can_send(self) -> bool:
        """True while a Shopify call may still be sent under the last renew."""
        return self.held and self._clock() < self.send_deadline()

    def keepalive(self) -> None:
        """Renew if the last renew is older than ``renew_after`` (before a call)."""
        if not self.held:
            raise LeaseLost("lease not held")
        if self._renewed_at is None or self._clock() - self._renewed_at >= self.renew_after_ms / 1000.0:
            self.register.renew(self)

    def __enter__(self) -> "PassLease":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()


__all__ = [
    "LEASE_TTL_MS", "SEND_MARGIN_MS", "RENEW_AFTER_MS", "WAITER_TTL_MS", "WAIT_POLL_S",
    "DECREMENT_WAIT_BOUND_S", "RETAKE_BOUND_S",
    "HOLDER_PUBLISH", "HOLDER_DECREMENT", "HOLDER_BASELINE", "HOLDER_RELEVEL",
    "HOLDER_PRICING", "HOLDER_DELETE", "HOLDER_CLEANUP", "HOLDER_EXIT",
    "HOLDER_NORMALIZE", "HOLDER_CLOSE",
    "LeaseUnavailable", "PassLease",
]
