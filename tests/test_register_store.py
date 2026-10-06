"""Store contract — the SQL surface a ``Register`` composes (plan §3.3 rule 6, §3.7).

Every case runs on each fixture backend: the in-memory fake always, and (from
§4.3) the MySQL adapter on ``racoon_staging``. A fake that passes these and a
MySQL store that passes these behave alike for every service test written on
the fake.
"""
from __future__ import annotations

import pytest

from backend.persistence.memory_store import DuplicateKey
from backend.persistence.records import (
    APPLIED,
    DONE,
    EVENT_CONSUME_MISS,
    FAILED,
    KIND_AUDIT,
    KIND_RECONCILE,
    OPEN,
    SHELF,
    Fence,
    GuardMismatch,
    InsertAudit,
    LeaseLost,
    StampEvents,
    UpdateAudit,
    UpdatePairs,
)

from tests.register_fixtures import MemoryBackend


@pytest.fixture(params=["memory"])
def backend(request):
    if request.param == "memory":
        return MemoryBackend()
    raise AssertionError(request.param)  # pragma: no cover


def _recon(**kw):
    base = dict(kind=KIND_RECONCILE, actor="t", action="adjust", status=OPEN,
                payload={"event_ids": [], "keys": []})
    base.update(kw)
    return InsertAudit(**base)


def _take(store, holder="h", ttl=300_000):
    gen = store.lease_take(holder, ttl)
    assert gen is not None
    return Fence(holder, gen, ttl)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def test_read_pairs_filters_and_orders_by_id(backend):
    a = backend.add_pair("SKU1", "42")
    b = backend.add_pair("SKU1", "43", state="VENDUTO", outlet_product_id=77)
    c = backend.add_pair("SKU2", "42", outlet_product_id=77)
    s = backend.store()
    assert [p.pair_id for p in s.read_pairs()] == [a, c]                      # shelf only by default
    assert [p.pair_id for p in s.read_pairs(states=None)] == [a, b, c]
    assert [p.pair_id for p in s.read_pairs(sku="SKU1", states=None)] == [a, b]
    assert [p.pair_id for p in s.read_pairs(states=None, outlet_product_id=77)] == [b, c]
    assert [p.pair_id for p in s.read_pairs(pair_ids=[c, a])] == [a, c]


def test_unstamped_events_join_key_and_state_and_keep_the_keyless_miss(backend):
    p, e1 = backend.add_shelf_pair("SKU1", "42")
    e0 = backend.add_event(p, 1, reconciled=True)  # stamped: never returned
    e2 = backend.consume("SKU1", "42")               # the pair leaves: −1
    e3 = backend.consume("SKU1", "42")               # no pair left: CONSUME_MISS
    rows = {r.event_id: r for r in backend.store().read_unstamped_events()}
    assert set(rows) == {e1, e2, e3}
    assert e0 not in rows
    assert rows[e1].key == ("SKU1", "42") and rows[e1].pair_state == "VENDUTO"
    assert rows[e2].delta == -1
    assert rows[e3].event_type == EVENT_CONSUME_MISS and rows[e3].key is None and rows[e3].delta == 0


def test_sizes_are_exact_not_normalised(backend):
    backend.add_shelf_pair("AR-K-33PC", "42L")
    backend.add_shelf_pair("AR-K-33PC", "42 L")
    keys = sorted(r.key for r in backend.store().read_unstamped_events())
    assert keys == [("AR-K-33PC", "42 L"), ("AR-K-33PC", "42L")]


# ---------------------------------------------------------------------------
# The lease statements
# ---------------------------------------------------------------------------

def test_take_increments_generation_and_refuses_a_live_holder(backend):
    s = backend.store()
    assert s.lease_take("a", 1000) == 1
    assert s.lease_take("b", 1000) is None          # live holder refused
    assert s.read_lease().holder == "a"
    backend.advance(1001)
    assert s.lease_take("b", 1000) == 2             # expired: taken over, generation +1
    assert s.read_lease().holder == "b"


def test_release_is_guarded_on_holder_and_generation(backend):
    s = backend.store()
    g = s.lease_take("a", 1000)
    assert s.lease_release(Fence("a", g + 1, 1000)) is False
    assert s.lease_release(Fence("x", g, 1000)) is False
    assert s.lease_release(Fence("a", g, 1000)) is True
    lease = s.read_lease()
    assert lease.holder is None and lease.expires_at is None and lease.generation == g


def test_waiter_blocks_every_other_take_until_it_takes_or_expires(backend):
    s = backend.store()
    ga = s.lease_take("pub", 10_000)
    assert s.lease_wait("dec", 5_000) is True
    assert s.lease_wait("other", 5_000) is False          # one waiter at a time
    s.lease_release(Fence("pub", ga, 10_000))
    assert s.lease_take("pub", 10_000) is None            # the waiting decrement goes first
    gd = s.lease_take("dec", 10_000)
    assert gd == ga + 1 and s.read_lease().waiter is None  # its take clears its own flag
    s.lease_release(Fence("dec", gd, 10_000))
    assert s.lease_take("pub", 10_000) == gd + 1


def test_an_expired_waiter_blocks_nobody(backend):
    s = backend.store()
    assert s.lease_wait("dec", 1_000) is True
    backend.advance(1_001)
    assert s.lease_take("pub", 1_000) == 1


# ---------------------------------------------------------------------------
# The fenced write
# ---------------------------------------------------------------------------

def test_fenced_write_renews_first_and_refuses_a_lost_lease(backend):
    s = backend.store()
    f = _take(s, "a", ttl=1_000)
    backend.advance(600)
    s.fenced_write(f, [])                                   # a renew alone
    backend.advance(600)                                    # 1200 since take, 600 since renew
    assert s.fenced_write(f, [_recon()])                    # still ours
    backend.advance(1_001)                                  # expired
    with pytest.raises(LeaseLost):
        s.fenced_write(f, [_recon(action="late")])
    assert [r.action for r in s.read_audit(KIND_RECONCILE)] == ["adjust"]


def test_a_stale_generation_is_refused_after_a_take_over(backend):
    s = backend.store()
    old = _take(s, "a", ttl=1_000)
    backend.advance(1_001)
    _take(s, "b", ttl=1_000)
    with pytest.raises(LeaseLost):
        s.fenced_write(old, [_recon()])
    assert s.read_audit(KIND_RECONCILE) == []


def test_fenced_write_is_atomic(backend):
    s = backend.store()
    f = _take(s)
    p, e = backend.add_shelf_pair("SKU1", "42")
    with pytest.raises(GuardMismatch):
        s.fenced_write(f, [_recon(), StampEvents((e, 999_999))])   # unknown id: short match
    assert s.read_audit(KIND_RECONCILE) == []                       # the insert rolled back
    assert [r.event_id for r in s.read_unstamped_events()] == [e]   # nothing stamped


def test_reconcile_rows_carry_the_writers_generation(backend):
    s = backend.store()
    f = _take(s)
    rid, aid = s.fenced_write(f, [_recon(), InsertAudit(kind=KIND_AUDIT, actor="t", action="x")])
    assert s.read_audit(KIND_RECONCILE, row_ids=[rid])[0].lease_gen == f.generation
    assert s.read_audit(KIND_AUDIT, row_ids=[aid])[0].lease_gen is None


def test_register_update_must_be_last_and_alone(backend):
    s = backend.store()
    f = _take(s)
    p, e = backend.add_shelf_pair("SKU1", "42")
    with pytest.raises(ValueError):
        s.fenced_write(f, [StampEvents((e,)), _recon()])
    with pytest.raises(ValueError):
        s.fenced_write(f, [StampEvents((e,)), UpdatePairs((p,), {"ONLINE": True})])
    assert s.read_audit(KIND_RECONCILE) == []


def test_stamp_coalesces_and_counts_an_already_stamped_id_as_matched(backend):
    s = backend.store()
    f = _take(s)
    p, e1 = backend.add_shelf_pair("SKU1", "42")
    e2 = backend.add_event(p, 0)
    s.fenced_write(f, [StampEvents((e1,))])
    first = {r.event_id for r in s.read_unstamped_events()}
    assert first == {e2}
    backend.advance(5_000)
    s.fenced_write(f, [StampEvents((e1, e2))])              # e1 again: matched, kept
    assert s.read_unstamped_events() == []


def test_pair_update_guards_and_short_match_rolls_back(backend):
    s = backend.store()
    f = _take(s)
    a = backend.add_pair("SKU1", "42")
    b = backend.add_pair("SKU1", "43", state="VENDUTO")
    with pytest.raises(GuardMismatch):
        s.fenced_write(f, [InsertAudit(kind=KIND_AUDIT, actor="t", action="x"),
                           UpdatePairs((a, b), {"ONLINE": True}, require_state=SHELF)])
    assert s.read_audit(KIND_AUDIT) == []
    assert all(p.online is False for p in s.read_pairs(states=None))
    s.fenced_write(f, [UpdatePairs((a,), {"ONLINE": True}, require_state=SHELF)])
    assert s.read_pairs(pair_ids=[a])[0].online is True


def test_pair_update_refuses_columns_outside_the_grant():
    with pytest.raises(ValueError):
        UpdatePairs((1,), {"STATE": "VENDUTO"})
    with pytest.raises(ValueError):
        UpdatePairs((1,), {"CONSUME_KEY": "x"})


def test_audit_update_guards_status_and_first_send(backend):
    s = backend.store()
    f = _take(s)
    (rid,) = s.fenced_write(f, [_recon()])
    s.fenced_write(f, [UpdateAudit(rid, OPEN, mark_sent=True, first_send=True,
                                   sent_available=3, request='{"q":1}')])
    with pytest.raises(GuardMismatch):     # a second "first send" never rewrites the three
        s.fenced_write(f, [UpdateAudit(rid, OPEN, mark_sent=True, first_send=True,
                                       sent_available=9, request='{"q":2}')])
    row = s.read_audit(KIND_RECONCILE, row_ids=[rid])[0]
    assert (row.sent_available, row.request) == (3, '{"q":1}')
    with pytest.raises(GuardMismatch):     # guarded on the status read
        s.fenced_write(f, [UpdateAudit(rid, APPLIED, status=DONE)])
    s.fenced_write(f, [UpdateAudit(rid, OPEN, status=FAILED, outcome={"reason": "x"})])
    row = s.read_audit(KIND_RECONCILE, row_ids=[rid])[0]
    assert row.status == FAILED and row.closed_at is not None and row.outcome == {"reason": "x"}


def test_intent_id_is_unique(backend):
    s = backend.store()
    f = _take(s)
    s.fenced_write(f, [InsertAudit(kind=KIND_AUDIT, actor="t", action="x", intent_id="a" * 32)])
    with pytest.raises(DuplicateKey):
        s.fenced_write(f, [InsertAudit(kind=KIND_AUDIT, actor="t", action="y", intent_id="a" * 32)])


def test_request_text_comes_back_byte_identical_while_json_keys_may_not(backend):
    s = backend.store()
    f = _take(s)
    body = '{"variables":{"input":{"z":1,"a":2}},"query":"mutation"}'
    (rid,) = s.fenced_write(f, [_recon(payload={"zeta": 1, "a": 2, "event_ids": [], "keys": []})])
    s.fenced_write(f, [UpdateAudit(rid, OPEN, mark_sent=True, first_send=True, request=body)])
    row = s.read_audit(KIND_RECONCILE, row_ids=[rid])[0]
    assert row.request == body
    assert row.payload == {"zeta": 1, "a": 2, "event_ids": [], "keys": []}


def test_a_closed_handle_refuses_every_call(backend):
    s = backend.store()
    s.close()
    with pytest.raises(Exception):
        s.read_pairs()
