"""Store contract — the SQL surface a ``Register`` composes (plan §3.3 rule 6, §3.7).

Every case runs on each fixture backend: the in-memory fake always, and the
MySQL adapter on ``racoon_staging`` when ``SCANSIA_STAGING=1`` (never ``racoon``;
``tests/register_fixtures.py`` ``MySQLBackend``). A fake that passes these and a
MySQL store that passes these behave alike for every service test written on the
fake. Assertions are scoped to the rows each test writes (staging holds others),
and lease generations are read relative to the start.
"""
from __future__ import annotations

import pytest

from backend.persistence.records import (
    APPLIED,
    DONE,
    EVENT_CONSUME_MISS,
    FAILED,
    KIND_AUDIT,
    KIND_RECONCILE,
    OPEN,
    SHELF,
    DuplicateKey,
    Fence,
    GuardMismatch,
    InsertAudit,
    LeaseLost,
    StampEvents,
    StoreError,
    UpdateAudit,
    UpdatePairs,
)

from tests.register_fixtures import MemoryBackend, MySQLBackend

ACTOR = "f7test"


@pytest.fixture(params=["memory", "mysql"])
def backend(request):
    if request.param == "memory":
        yield MemoryBackend()
        return
    why = MySQLBackend.available()
    if why:
        pytest.skip(why)
    be = MySQLBackend()
    yield be
    be.reset()
    be.close()


def _recon(**kw):
    base = dict(kind=KIND_RECONCILE, actor=ACTOR, action="adjust", status=OPEN,
                payload={"event_ids": [], "keys": []})
    base.update(kw)
    return InsertAudit(**base)


def _audit(action="x", **kw):
    return InsertAudit(kind=KIND_AUDIT, actor=ACTOR, action=action, **kw)


def _take(store, holder="f7test-h", ttl=300_000):
    gen = store.lease_take(holder, ttl)
    assert gen is not None
    return Fence(holder, gen, ttl)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def test_read_pairs_filters_and_orders_by_id(backend):
    a = backend.add_pair("F7T-SKU1", "42")
    b = backend.add_pair("F7T-SKU1", "43", state="VENDUTO", outlet_product_id=987654321)
    c = backend.add_pair("F7T-SKU2", "42", outlet_product_id=987654321)
    s = backend.store()
    mine = {a, b, c}
    assert [p.pair_id for p in s.read_pairs() if p.pair_id in mine] == [a, c]
    assert [p.pair_id for p in s.read_pairs(states=None) if p.pair_id in mine] == [a, b, c]
    assert [p.pair_id for p in s.read_pairs(sku="F7T-SKU1", states=None)] == [a, b]
    assert [p.pair_id for p in s.read_pairs(states=None, outlet_product_id=987654321)] == [b, c]
    assert [p.pair_id for p in s.read_pairs(pair_ids=[c, a])] == [a, c]
    assert s.read_pairs(pair_ids=[]) == []


def test_unstamped_events_join_key_and_state_and_keep_the_keyless_miss(backend):
    p, e1 = backend.add_shelf_pair("F7T-SKU1", "42")
    e0 = backend.add_event(p, 1, reconciled=True)  # stamped: never returned
    e2 = backend.consume("F7T-SKU1", "42")          # the pair leaves: −1
    e3 = backend.consume("F7T-SKU1", "42")          # no pair left: CONSUME_MISS
    rows = {r.event_id: r for r in backend.store().read_unstamped_events()}
    assert {e1, e2, e3} <= set(rows) and e0 not in rows
    assert rows[e1].key == ("F7T-SKU1", "42") and rows[e1].pair_state == "VENDUTO"
    assert rows[e2].delta == -1 and rows[e2].event_type == "CONSUME"
    assert rows[e3].event_type == EVENT_CONSUME_MISS and rows[e3].key is None and rows[e3].delta == 0


def test_an_outlet_line_sale_records_delta_zero(backend):
    backend.add_shelf_pair("F7T-SKU1", "42")
    e = backend.consume("F7T-SKU1", "42", outlet_line=True)
    (row,) = [r for r in backend.store().read_unstamped_events() if r.event_id == e]
    assert row.delta == 0 and row.pair_state == "VENDUTO"


def test_a_snapshot_reads_the_shelf_every_unstamped_event_and_each_keys_max_event(backend):
    a, ea = backend.add_shelf_pair("F7T-SNAP", "42")                       # unstamped +1
    b, eb = backend.add_shelf_pair("F7T-SNAP", "42", reconciled=True)
    c = backend.add_pair("F7T-SNAP", "43", state="SOLD")
    ec = backend.add_event(c, -1, event_type="CONSUME", actor="SHOPIFY", reconciled=True)
    s = backend.store()
    snap = s.read_snapshot()
    assert [p.pair_id for p in snap.pairs if p.sku == "F7T-SNAP"] == [a, b]
    assert [e.event_id for e in snap.unstamped if e.event_id in {ea, eb, ec}] == [ea]
    assert snap.max_event_id[("F7T-SNAP", "42")] == eb                     # stamped or not
    assert snap.max_event_id[("F7T-SNAP", "43")] == ec                     # off the shelf too
    assert snap.shelf_count(("F7T-SNAP", "42")) == 2
    one = s.read_snapshot(("F7T-SNAP", "43"))                              # one key
    assert one.pairs == () and one.unstamped == () and dict(one.max_event_id) == {("F7T-SNAP", "43"): ec}
    s.close()


def test_a_snapshot_transaction_is_closed_before_it_returns(backend):
    s = backend.store()
    s.read_snapshot()
    p, e = backend.add_shelf_pair("F7T-SNAP2", "40")                        # a writer right after
    assert [x.pair_id for x in s.read_snapshot(("F7T-SNAP2", "40")).pairs] == [p]   # a new snapshot
    gen = s.lease_take("f7test-snap", 1000)      # autocommit again: inside READ ONLY it fails (1792)
    assert gen is not None
    s.close()


def test_import_payloads_come_back_per_pair_for_imported_events_only(backend):
    a = backend.add_pair("F7T-PAY", "42")
    backend.add_event(a, +1, event_type="IMPORTED", payload={"raw": {"J": {"v": "89,00"}, "K": {"v": 30}}})
    backend.add_event(a, -1, event_type="CONSUME", actor="SHOPIFY", payload={"raw": {"J": {"v": "1"}}})
    b = backend.add_pair("F7T-PAY", "43")
    backend.add_event(b, +1, event_type="IMPORTED")                        # no payload
    s = backend.store()
    got = s.read_import_payloads([a, b])
    assert [pid for pid, _ in got] == [a, b]
    assert got[0][1]["raw"]["J"]["v"] == "89,00" and got[1][1] is None
    assert s.read_import_payloads([]) == []
    s.close()


def test_sizes_are_exact_not_normalised(backend):
    backend.add_shelf_pair("F7T-AR-K-33PC", "42L")
    backend.add_shelf_pair("F7T-AR-K-33PC", "42 L")
    keys = sorted(r.key for r in backend.store().read_unstamped_events()
                  if r.key and r.key[0] == "F7T-AR-K-33PC")
    assert keys == [("F7T-AR-K-33PC", "42 L"), ("F7T-AR-K-33PC", "42L")]
    assert [p.size for p in backend.store().read_pairs(sku="F7T-AR-K-33PC")] == ["42L", "42 L"]


# ---------------------------------------------------------------------------
# The lease statements
# ---------------------------------------------------------------------------

def test_take_increments_generation_and_refuses_a_live_holder(backend):
    s = backend.store()
    g0 = s.read_lease().generation
    assert s.lease_take("f7test-a", 1000) == g0 + 1
    assert s.lease_take("f7test-b", 1000) is None          # live holder refused
    assert s.read_lease().holder == "f7test-a"
    backend.advance(1001)
    assert s.lease_take("f7test-b", 1000) == g0 + 2        # expired: taken over, +1
    assert s.read_lease().holder == "f7test-b"


def test_release_is_guarded_on_holder_and_generation(backend):
    s = backend.store()
    g = s.lease_take("f7test-a", 1000)
    assert s.lease_release(Fence("f7test-a", g + 1, 1000)) is False
    assert s.lease_release(Fence("f7test-x", g, 1000)) is False
    assert s.lease_release(Fence("f7test-a", g, 1000)) is True
    lease = s.read_lease()
    assert lease.holder is None and lease.expires_at is None and lease.generation == g


def test_waiter_blocks_every_other_take_until_it_takes_or_expires(backend):
    s = backend.store()
    ga = s.lease_take("f7test-pub", 10_000)
    assert s.lease_wait("f7test-dec", 5_000) is True
    assert s.lease_wait("f7test-other", 5_000) is False        # one waiter at a time
    s.lease_release(Fence("f7test-pub", ga, 10_000))
    assert s.lease_take("f7test-pub", 10_000) is None          # the waiting decrement goes first
    gd = s.lease_take("f7test-dec", 10_000)
    assert gd == ga + 1 and s.read_lease().waiter is None      # its take clears its own flag
    s.lease_release(Fence("f7test-dec", gd, 10_000))
    assert s.lease_take("f7test-pub", 10_000) == gd + 1


def test_an_expired_waiter_blocks_nobody(backend):
    s = backend.store()
    g0 = s.read_lease().generation
    assert s.lease_wait("f7test-dec", 1_000) is True
    backend.advance(1_001)
    assert s.lease_take("f7test-pub", 1_000) == g0 + 1


# ---------------------------------------------------------------------------
# The fenced write
# ---------------------------------------------------------------------------

def test_fenced_write_renews_first_and_refuses_a_lost_lease(backend):
    s = backend.store()
    k = backend.scale
    f = _take(s, ttl=1_000 * k)
    backend.advance(600 * k)
    s.fenced_write(f, [])                                   # a renew alone
    backend.advance(600 * k)                                # 1200 since take, 600 since renew
    (rid,) = s.fenced_write(f, [_recon()])                  # still ours
    backend.advance(1_001 * k)                              # expired
    with pytest.raises(LeaseLost):
        s.fenced_write(f, [_recon(action="late")])
    assert [r.action for r in s.read_audit(KIND_RECONCILE) if r.actor == ACTOR] == ["adjust"]


def test_a_stale_generation_is_refused_after_a_take_over(backend):
    s = backend.store()
    old = _take(s, "f7test-a", ttl=1_000)
    backend.advance(1_001)
    _take(s, "f7test-b", ttl=1_000)
    with pytest.raises(LeaseLost):
        s.fenced_write(old, [_recon()])
    assert [r for r in s.read_audit(KIND_RECONCILE) if r.actor == ACTOR] == []


def test_fenced_write_is_atomic(backend):
    s = backend.store()
    f = _take(s)
    p, e = backend.add_shelf_pair("F7T-SKU1", "42")
    with pytest.raises(GuardMismatch):
        s.fenced_write(f, [_recon(), StampEvents((e, 2_000_000_000))])   # unknown id: short match
    assert [r for r in s.read_audit(KIND_RECONCILE) if r.actor == ACTOR] == []   # insert rolled back
    assert [r.event_id for r in s.read_unstamped_events() if r.event_id == e] == [e]


def test_reconcile_rows_carry_the_writers_generation(backend):
    s = backend.store()
    f = _take(s)
    rid, aid = s.fenced_write(f, [_recon(), _audit()])
    assert s.read_audit(KIND_RECONCILE, row_ids=[rid])[0].lease_gen == f.generation
    assert s.read_audit(KIND_AUDIT, row_ids=[aid])[0].lease_gen is None


def test_register_update_must_be_last_and_alone(backend):
    s = backend.store()
    f = _take(s)
    p, e = backend.add_shelf_pair("F7T-SKU1", "42")
    with pytest.raises(ValueError):
        s.fenced_write(f, [StampEvents((e,)), _recon()])
    with pytest.raises(ValueError):
        s.fenced_write(f, [StampEvents((e,)), UpdatePairs((p,), {"ONLINE": True})])
    assert [r for r in s.read_audit(KIND_RECONCILE) if r.actor == ACTOR] == []


def test_stamp_coalesces_and_counts_an_already_stamped_id_as_matched(backend):
    s = backend.store()
    f = _take(s)
    p, e1 = backend.add_shelf_pair("F7T-SKU1", "42")
    e2 = backend.add_event(p, 0)
    s.fenced_write(f, [StampEvents((e1,))])
    assert [r.event_id for r in s.read_unstamped_events() if r.event_id in (e1, e2)] == [e2]
    s.fenced_write(f, [StampEvents((e1, e2))])              # e1 again: matched, kept
    assert [r for r in s.read_unstamped_events() if r.event_id in (e1, e2)] == []


def test_pair_update_guards_and_short_match_rolls_back(backend):
    s = backend.store()
    f = _take(s)
    a = backend.add_pair("F7T-SKU1", "42")
    b = backend.add_pair("F7T-SKU1", "43", state="VENDUTO")
    with pytest.raises(GuardMismatch):
        s.fenced_write(f, [_audit(), UpdatePairs((a, b), {"ONLINE": True}, require_state=SHELF)])
    assert [r for r in s.read_audit(KIND_AUDIT) if r.actor == ACTOR] == []
    assert all(p.online is False for p in s.read_pairs(states=None, pair_ids=[a, b]))
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


def test_an_audit_update_only_ever_touches_reconcile_rows(backend):
    s = backend.store()
    f = _take(s)
    (aid,) = s.fenced_write(f, [_audit()])
    with pytest.raises(GuardMismatch):
        s.fenced_write(f, [UpdateAudit(aid, OPEN, status=FAILED)])


def test_intent_id_is_unique(backend):
    s = backend.store()
    f = _take(s)
    s.fenced_write(f, [_audit(intent_id="f" * 32)])
    with pytest.raises(DuplicateKey):
        s.fenced_write(f, [_audit("y", intent_id="f" * 32)])
    assert [r.action for r in s.read_audit(KIND_AUDIT, intent_id="f" * 32)] == ["x"]


def test_request_text_comes_back_byte_identical(backend):
    s = backend.store()
    f = _take(s)
    body = '{"variables":{"input":{"z":1,"a":2}},"query":"mutation"}'
    (rid,) = s.fenced_write(f, [_recon(payload={"zeta": 1, "a": 2, "event_ids": [], "keys": []})])
    s.fenced_write(f, [UpdateAudit(rid, OPEN, mark_sent=True, first_send=True, request=body)])
    row = s.read_audit(KIND_RECONCILE, row_ids=[rid])[0]
    assert row.request == body
    assert row.payload == {"zeta": 1, "a": 2, "event_ids": [], "keys": []}


def test_last_reads_the_newest_rows_oldest_first(backend):
    s = backend.store()
    f = _take(s)
    ids = [s.fenced_write(f, [_audit(f"a{i}")])[0] for i in range(4)]
    assert [r.row_id for r in s.read_audit(KIND_AUDIT, last=2) if r.actor == ACTOR] == ids[2:]


def test_a_closed_handle_refuses_every_call(backend):
    s = backend.store()
    s.close()
    with pytest.raises(StoreError):
        s.read_pairs()


def test_an_outcome_can_name_an_id_inserted_in_the_same_transaction(backend):
    from backend.persistence.records import InsertedId, SUPERSEDED

    s = backend.store()
    f = _take(s)
    (old,) = s.fenced_write(f, [_recon(group_id=5)])
    new, = s.fenced_write(f, [_recon(group_id=5), UpdateAudit(old, OPEN, status=SUPERSEDED,
                                                               outcome={"successor": InsertedId(0)})])
    assert s.read_audit(KIND_RECONCILE, row_ids=[old])[0].outcome == {"successor": new}
