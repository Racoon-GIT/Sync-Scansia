"""The pass lease, client side (plan §3.3 rule 6): generation and the
``others_between`` read, the local deadline, keepalive, the decrement's
hand-off, a crashed holder taken over and a stalled one refused."""
from __future__ import annotations

import pytest

from backend.persistence.records import KIND_RECONCILE, OPEN, InsertAudit, LeaseLost
from backend.services.lease import LeaseUnavailable, PassLease

from tests.register_fixtures import MemoryBackend


class Mono:
    """The app's monotonic clock, driven by the test."""

    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def be():
    return MemoryBackend()


def _lease(be, kind, mono=None, sleep=None, **kw):
    mono = mono or Mono()
    return PassLease(be.register(), kind, clock=mono, sleep=sleep or (lambda s: None), **kw)


def _row():
    return InsertAudit(kind=KIND_RECONCILE, actor="t", action="adjust", status=OPEN,
                       payload={"keys": [], "event_ids": []})


def test_first_take_and_retakes_report_whether_another_holder_came_between(be):
    a = _lease(be, "publish")
    b = _lease(be, "decrement")
    a.take()
    assert a.others_between is True and a.generation == 1     # first take: recovery runs
    a.release()
    a.take()
    assert a.others_between is False and a.generation == 2    # nobody between
    a.release()
    b.take(); b.release()
    a.take()
    assert a.others_between is True and a.generation == 4     # b held it in between


def test_take_with_a_bound_gives_up_unavailable(be):
    a = _lease(be, "publish")
    a.take()
    mono = Mono()
    slept = []

    def sleep(s):
        slept.append(s)
        mono.t += s

    b = _lease(be, "pricing", mono=mono, sleep=sleep)
    with pytest.raises(LeaseUnavailable):
        b.take(bound_s=5, poll_s=2)
    assert slept == [2, 2, 2]


def test_a_crashed_holder_is_taken_over_once_expired(be):
    dead = _lease(be, "publish", ttl_ms=10_000, send_margin_ms=2_000, renew_after_ms=1_000)
    dead.take()
    live = _lease(be, "publish")
    assert live.try_take() is False                            # a live holder is refused
    be.advance(10_001)
    assert live.try_take() is True and live.generation == 2
    with pytest.raises(LeaseLost):                             # the stalled one resumes
        be.register().write_ahead(dead, [_row()])
    assert be.register().reconcile_rows() == []


def test_a_stalled_holder_right_after_its_renew_cannot_write(be):
    mono = Mono()
    stalled = _lease(be, "publish", mono=mono, ttl_ms=10_000, send_margin_ms=2_000,
                     renew_after_ms=1_000)
    stalled.take()
    reg = be.register()
    reg.renew(stalled)                                          # renewed, then the stall
    be.advance(10_001)
    mono.t += 10.001
    taker = _lease(be, "decrement")
    taker.take()
    assert stalled.can_send() is False                          # past its local deadline
    with pytest.raises(LeaseLost):
        reg.write_ahead(stalled, [_row()])


def test_send_deadline_counts_from_the_renew_start(be):
    mono = Mono()
    lease = _lease(be, "publish", mono=mono, ttl_ms=10_000, send_margin_ms=2_000,
                   renew_after_ms=1_000)
    lease.take()
    mono.t += 7.999
    assert lease.can_send() is True
    mono.t += 0.002
    assert lease.can_send() is False
    be.register().renew(lease)                                  # renew restarts it
    assert lease.can_send() is True


def test_keepalive_renews_only_when_due(be):
    mono = Mono()
    lease = _lease(be, "publish", mono=mono, ttl_ms=10_000, send_margin_ms=2_000,
                   renew_after_ms=3_000)
    lease.take()
    be.db.log.clear()
    mono.t += 2.9
    lease.keepalive()
    assert [x for x in be.db.log if x[0] == "renew"] == []
    mono.t += 0.2
    lease.keepalive()
    assert [x[0] for x in be.db.log if x[0] == "renew"] == ["renew"]


def test_the_waiting_decrement_is_handed_the_lease_between_skus(be):
    pub = _lease(be, "publish")
    pub.take()
    order = []
    mono = Mono()

    def dec_sleep(s):
        # while the decrement waits, the publish ends its SKU and yields
        mono.t += s
        if pub.held:
            pub.release()
            order.append("publish released")
            assert pub.try_take() is False        # it cannot jump the waiting decrement
            order.append("publish re-take refused")

    dec = _lease(be, "decrement", mono=mono, sleep=dec_sleep)
    dec.take_waiting(bound_s=30, poll_s=1)
    order.append("decrement took")
    assert be.register().lease_state().waiter is None
    dec.release()
    pub.take()
    assert order == ["publish released", "publish re-take refused", "decrement took"]
    assert pub.others_between is True                            # recovery runs again


def test_the_decrement_ends_its_run_past_its_bound(be):
    pub = _lease(be, "publish")
    pub.take()
    mono = Mono()

    def sleep(s):
        mono.t += s

    dec = _lease(be, "decrement", mono=mono, sleep=sleep)
    with pytest.raises(LeaseUnavailable):
        dec.take_waiting(bound_s=6, poll_s=2)
    assert be.register().lease_state().waiter == dec.holder     # flagged, expiring on its own


def test_release_never_raises_and_forgets_the_generation(be):
    lease = _lease(be, "publish", ttl_ms=10_000, send_margin_ms=2_000, renew_after_ms=1_000)
    lease.take()
    be.advance(10_001)
    _lease(be, "decrement").take()                               # taken over meanwhile
    lease.release()                                              # guarded: matches nothing
    assert lease.held is False
    with pytest.raises(LeaseLost):
        _ = lease.fence


def test_parameters_are_validated():
    with pytest.raises(ValueError):
        PassLease(object(), "x", ttl_ms=1000, send_margin_ms=1000)
    with pytest.raises(ValueError):
        PassLease(object(), "x", ttl_ms=1000, send_margin_ms=100, renew_after_ms=901)
