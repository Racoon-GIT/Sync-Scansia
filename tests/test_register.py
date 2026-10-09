"""``Register`` — the §3.1 operations over a store: holds, pending deltas, the
stamp and its carrier, the register columns with their audit rows, BASELINE /
HOLD rows and the RECONCILE ledger primitives. Logic shared by every store, so
it is tested on the fake (the store contract proves MySQL equal)."""
from __future__ import annotations

from decimal import Decimal

import pytest

from backend.persistence.records import (
    APPLIED,
    CARRIED,
    DONE,
    FAILED,
    HELD,
    HOLD_BASELINE,
    HOLD_IN_FLIGHT,
    HOLD_RECOVERY,
    HOLD_ROW,
    KIND_BASELINE,
    KIND_PRICE,
    KIND_RECONCILE,
    KIND_REGISTER,
    OPEN,
    SCOPE_KEY,
    SCOPE_REGISTER,
    BaselineMissing,
    GuardMismatch,
    InsertAudit,
    LeaseLost,
)
from backend.persistence.memory_store import SimulatedCrash

from tests.register_fixtures import MemoryBackend, StubLease, stamp_carrier

K = ("SKU1", "42")


@pytest.fixture
def be():
    return MemoryBackend()


def _row(status=OPEN, keys=(K,), event_ids=(), action="adjust", **kw):
    return InsertAudit(kind=KIND_RECONCILE, actor="t", action=action, status=status,
                       payload={"keys": [list(k) for k in keys], "event_ids": list(event_ids)}, **kw)


# ---------------------------------------------------------------------------
# The baseline gate (§3.4)
# ---------------------------------------------------------------------------

def test_pending_refuses_before_the_whole_register_baseline(be):
    be.add_shelf_pair(*K)
    reg = be.register()
    with pytest.raises(BaselineMissing):
        reg.pending_deltas()
    # a per-key BASELINE row is not the whole-register one
    be.seed_audit(InsertAudit(kind=KIND_BASELINE, actor="t", action="relevel",
                              payload={"scope": SCOPE_KEY, "key": list(K)}))
    with pytest.raises(BaselineMissing):
        reg.pending_deltas()
    be.seed_baseline()
    assert len(reg.pending_deltas().events) == 1


def test_only_one_whole_register_baseline(be):
    reg = be.register()
    lease = StubLease(reg)
    reg.record_baseline(lease, scope=SCOPE_REGISTER, action="baseline", payload={"held": []}, stamp_ids=[])
    with pytest.raises(ValueError):
        reg.record_baseline(lease, scope=SCOPE_REGISTER, action="baseline", payload={"held": []}, stamp_ids=[])


# ---------------------------------------------------------------------------
# Holds
# ---------------------------------------------------------------------------

def test_every_hold_kind_is_derived_and_its_events_subtracted(be):
    _, e_base = be.add_shelf_pair("A", "40")
    _, e_rec = be.add_shelf_pair("B", "41")
    _, e_hold = be.add_shelf_pair("C", "42")
    _, e_fly = be.add_shelf_pair("D", "43")
    _, e_free = be.add_shelf_pair("E", "44")
    be.seed_baseline(held=[{"key": ["A", "40"], "reason": "moved", "event_ids": [e_base]}])
    be.seed_audit(_row(status=HELD, keys=[("B", "41")], event_ids=[e_rec]))
    be.seed_audit(InsertAudit(kind="HOLD", actor="t", action="hold",
                              payload={"keys": [["C", "42"]], "reason": "outlet deleted",
                                       "event_ids": [e_hold]}))
    be.seed_audit(_row(status=OPEN, keys=[("D", "43")], event_ids=[e_fly]))
    reg = be.register()
    kinds = {h.key: h.kind for h in reg.held_keys()}
    assert kinds == {("A", "40"): HOLD_BASELINE, ("B", "41"): HOLD_RECOVERY,
                     ("C", "42"): HOLD_ROW, ("D", "43"): HOLD_IN_FLIGHT}
    pend = reg.pending_deltas()
    assert [e.event_id for e in pend.events] == [e_free]
    assert pend.held_keys == {("A", "40"), ("B", "41"), ("C", "42"), ("D", "43")}


def test_post_hold_events_of_a_held_key_stay_readable(be):
    p, e1 = be.add_shelf_pair(*K)
    be.seed_baseline(held=[{"key": list(K), "reason": "moved", "event_ids": [e1]}])
    e2 = be.consume(*K)                                   # after the hold
    pend = be.register().pending_deltas()
    assert [e.event_id for e in pend.events] == [e2]
    assert K in pend.held_keys


def test_a_per_key_baseline_releases_older_holds_only(be):
    _, e1 = be.add_shelf_pair(*K)
    be.seed_baseline(held=[{"key": list(K), "reason": "moved", "event_ids": [e1]}])
    be.seed_audit(InsertAudit(kind=KIND_BASELINE, actor="t", action="relevel",
                              payload={"scope": SCOPE_KEY, "key": list(K)}))
    assert be.register().held_keys() == []
    be.seed_audit(_row(status=HELD, keys=[K], event_ids=[e1]))     # a newer recovery hold
    assert [h.kind for h in be.register().held_keys()] == [HOLD_RECOVERY]


def test_an_in_flight_hold_ends_with_its_row_never_with_a_baseline(be):
    _, e1 = be.add_shelf_pair(*K)
    be.seed_baseline()
    rid = be.seed_audit(_row(status=OPEN, keys=[K], event_ids=[e1]))
    be.seed_audit(InsertAudit(kind=KIND_BASELINE, actor="t", action="n0",
                              payload={"scope": SCOPE_KEY, "key": list(K)}))
    reg = be.register()
    assert [h.kind for h in reg.held_keys()] == [HOLD_IN_FLIGHT]
    lease = StubLease(reg)
    reg.record_outcome(lease, rid, expect=OPEN, status=FAILED)
    assert reg.held_keys() == []
    assert [e.event_id for e in reg.pending_deltas().events] == [e1]


def test_applied_rows_hold_and_carried_rows_are_reported(be):
    _, e1 = be.add_shelf_pair(*K)
    be.seed_baseline()
    be.seed_audit(_row(status=APPLIED, keys=[K], event_ids=[e1]))
    be.seed_audit(_row(status=CARRIED, keys=[K], event_ids=[e1], outcome={"moved": -1}))
    pend = be.register().pending_deltas()
    assert pend.events == () and K in pend.held_keys
    assert [r.status for r in pend.carried] == [CARRIED]


def test_keyless_miss_is_surfaced_apart(be):
    be.seed_baseline()
    e = be.consume("NOPE", "40")
    pend = be.register().pending_deltas()
    assert pend.events == () and [m.event_id for m in pend.keyless] == [e]


# ---------------------------------------------------------------------------
# The stamp (§3.1)
# ---------------------------------------------------------------------------

def test_stamp_needs_an_audit_row_to_carry_it(be):
    _, e = be.add_shelf_pair(*K)
    reg = be.register()
    with pytest.raises(ValueError):
        reg.stamp(StubLease(reg), [e])


def test_stamp_closes_its_rows_then_stamps_in_one_transaction(be):
    _, e1 = be.add_shelf_pair(*K)
    _, e2 = be.add_shelf_pair(*K)
    be.seed_baseline()
    r1 = be.seed_audit(_row(status=APPLIED, event_ids=[e1, e2]))
    r2 = be.seed_audit(_row(status=APPLIED, event_ids=[e1, e2], action="activate"))
    reg = be.register()
    lease = StubLease(reg)
    be.db.log.clear()
    reg.stamp(lease, [e1, e2, e1], close_rows=[r1, r2])     # the group's ids, repeated
    shapes = [x[0] for x in be.db.log]
    assert shapes == ["renew", "update_audit", "update_audit", "stamp", "commit"]
    assert {r.status for r in reg.reconcile_rows(row_ids=[r1, r2])} == {DONE}
    assert reg.pending_deltas().events == ()


def test_stamp_rolls_back_when_a_row_is_no_longer_applied(be):
    _, e1 = be.add_shelf_pair(*K)
    be.seed_baseline()
    r1 = be.seed_audit(_row(status=FAILED, event_ids=[e1]))
    reg = be.register()
    with pytest.raises(GuardMismatch):
        reg.stamp(StubLease(reg), [e1], close_rows=[r1])
    assert reg.reconcile_rows(row_ids=[r1])[0].status == FAILED
    assert [e.event_id for e in reg.pending_deltas().events] == [e1]


def test_stamp_with_a_carrier_row(be):
    _, e1 = be.add_shelf_pair(*K)
    be.seed_baseline()
    reg = be.register()
    (rid,) = reg.stamp(StubLease(reg), [e1], carrier=stamp_carrier([e1], [K]))
    row = reg.reconcile_rows(row_ids=[rid])[0]
    assert row.status == DONE and row.closed_at is not None
    assert reg.pending_deltas().events == ()


def test_a_lost_lease_stamps_nothing(be):
    _, e1 = be.add_shelf_pair(*K)
    be.seed_baseline()
    reg = be.register()
    lease = StubLease(reg, ttl_ms=1_000)
    be.advance(1_001)
    with pytest.raises(LeaseLost):
        reg.stamp(lease, [e1], carrier=stamp_carrier([e1], [K]))
    assert [e.event_id for e in reg.pending_deltas().events] == [e1]


# ---------------------------------------------------------------------------
# Register columns, each with one audit row
# ---------------------------------------------------------------------------

def test_set_outlet_price_writes_shelf_pairs_only_with_priors(be):
    a = be.add_pair("SKU1", "42", price_outlet="80.00")
    b = be.add_pair("SKU1", "43")
    be.add_pair("SKU1", "44", state="VENDUTO")
    be.add_pair("SKU2", "42")
    reg = be.register()
    lease = StubLease(reg)
    assert reg.set_outlet_price(lease, "SKU1", Decimal("59.00"), Decimal("21.16")) == [a, b]
    prices = {p.pair_id: p.price_outlet for p in reg.pairs(sku="SKU1")}
    assert prices[a] == prices[b] == Decimal("59.00")
    (row,) = reg.audit_rows(KIND_PRICE)
    priors = {x["pair_id"]: x["old_price_outlet"] for x in row.payload["pairs"]}
    assert priors == {a: "80.00", b: None}


def test_set_outlet_price_only_missing(be):
    a = be.add_pair("SKU1", "42", price_outlet="80.00")
    b = be.add_pair("SKU1", "43")
    reg = be.register()
    assert reg.set_outlet_price(StubLease(reg), "SKU1", Decimal("80.00"), None, only_missing=True) == [b]
    assert reg.pairs(pair_ids=[a])[0].price_outlet == Decimal("80.00")


def test_a_pair_sold_between_read_and_write_is_dropped_on_the_retry(be, monkeypatch):
    be.add_pair("SKU1", "42")
    b = be.add_pair("SKU1", "42")
    reg = be.register()
    lease = StubLease(reg)
    real = reg.store.read_pairs
    calls = {"n": 0}

    def racing_read(**kw):
        rows = real(**kw)
        calls["n"] += 1
        if calls["n"] == 1:
            be.consume("SKU1", "42")          # CONSUME lands between SELECT and UPDATE
        return rows

    monkeypatch.setattr(reg.store, "read_pairs", racing_read)
    written = reg.set_outlet_price(lease, "SKU1", Decimal("50.00"), None)
    assert written == [b] and calls["n"] == 2
    assert len(reg.audit_rows(KIND_PRICE)) == 1      # the first attempt rolled back whole


def test_outlet_product_set_on_shelf_and_cleared_in_any_state(be):
    a = be.add_pair("SKU1", "42")
    b = be.add_pair("SKU1", "43", state="VENDUTO", outlet_product_id=9)
    reg = be.register()
    lease = StubLease(reg)
    reg.set_outlet_product(lease, [a], 9)
    assert reg.clear_outlet_product(lease, 9) == [a, b]
    assert all(p.outlet_product_id is None for p in reg.pairs())
    rows = reg.audit_rows(KIND_REGISTER)
    assert [r.payload["column"] for r in rows] == ["OUTLET_PRODUCT_ID", "OUTLET_PRODUCT_ID"]
    assert rows[1].payload["pairs"] == [{"pair_id": a, "old": 9, "new": None},
                                        {"pair_id": b, "old": 9, "new": None}]


def test_set_online_one_is_guarded_on_the_shelf_zero_is_not(be):
    a = be.add_pair("SKU1", "42")
    b = be.add_pair("SKU1", "43", state="VENDUTO")
    reg = be.register()
    lease = StubLease(reg)
    reg.set_online(lease, [a, b], True)               # b is not on the shelf: left out
    on = {p.pair_id: p.online for p in reg.pairs()}
    assert on == {a: True, b: False}
    reg.set_online(lease, [a, b], False)
    assert not any(p.online for p in reg.pairs())
    assert [len(r.payload["pairs"]) for r in reg.audit_rows(KIND_REGISTER)] == [1, 2]


def test_every_register_write_is_one_transaction_with_its_audit_row(be):
    a = be.add_pair("SKU1", "42")
    reg = be.register()
    lease = StubLease(reg)
    be.db.log.clear()
    reg.set_online(lease, [a], True)
    writes = [x[0] for x in be.db.log if x[0] not in ("read_pairs",)]
    assert writes == ["renew", "insert_audit", "update_pairs", "commit"]


# ---------------------------------------------------------------------------
# BASELINE / HOLD rows, the RECONCILE ledger
# ---------------------------------------------------------------------------

def test_record_baseline_stamps_its_explicit_list(be):
    _, e1 = be.add_shelf_pair(*K)
    _, e2 = be.add_shelf_pair("SKU2", "40")
    reg = be.register()
    lease = StubLease(reg)
    bid = reg.record_baseline(lease, scope=SCOPE_REGISTER, action="baseline",
                              payload={"held": [], "stamped": [e1]}, stamp_ids=[e1])
    assert reg.baseline().row_id == bid
    assert [e.event_id for e in reg.pending_deltas().events] == [e2]
    with pytest.raises(ValueError):
        reg.record_baseline(lease, scope=SCOPE_KEY, action="relevel", payload={}, stamp_ids=[])


def test_record_hold_holds_its_keys(be):
    _, e1 = be.add_shelf_pair(*K)
    be.seed_baseline()
    reg = be.register()
    reg.record_hold(StubLease(reg), keys=[K], reason="outlet deleted", event_ids=[e1], product_id=5)
    (h,) = reg.held_keys()
    assert (h.kind, h.reason, h.product_id) == (HOLD_ROW, "outlet deleted", 5)


def test_write_ahead_is_all_or_nothing(be):
    reg = be.register()
    lease = StubLease(reg)

    def crash(i, op):
        if i == 1:
            raise SimulatedCrash("process died inside the write-ahead")

    be.db.crash_hook = crash
    with pytest.raises(SimulatedCrash):
        reg.write_ahead(lease, [_row(), _row(action="activate")])
    be.db.crash_hook = None
    assert reg.reconcile_rows() == []
    ids = reg.write_ahead(lease, [_row(group_id=7), _row(action="activate", group_id=7)])
    assert [r.row_id for r in reg.reconcile_rows(group_id=7)] == ids


def test_write_ahead_takes_only_open_reconcile_rows(be):
    reg = be.register()
    with pytest.raises(ValueError):
        reg.write_ahead(StubLease(reg), [_row(status=DONE)])


def test_mark_sent_first_send_then_outcome(be):
    reg = be.register()
    lease = StubLease(reg)
    (rid,) = reg.write_ahead(lease, [_row()])
    reg.mark_sent(lease, rid, first=True, sent_available=4, request='{"x":1}')
    reg.record_outcome(lease, rid, expect=OPEN, status=APPLIED, outcome={"quantityAfterChange": 3})
    row = reg.reconcile_rows(row_ids=[rid])[0]
    assert (row.status, row.sent_available, row.request) == (APPLIED, 4, '{"x":1}')
    assert row.sent_at is not None and row.closed_at is None
    with pytest.raises(GuardMismatch):
        reg.record_outcome(lease, rid, expect=OPEN, status=FAILED)


def test_supersede_writes_the_successor_and_closes_the_old_row_atomically(be):
    from backend.persistence.records import SUPERSEDED

    reg = be.register()
    lease = StubLease(reg)
    (old_id,) = reg.write_ahead(lease, [_row(group_id=9, event_ids=[1])])
    old = reg.reconcile_rows(row_ids=[old_id])[0]
    succ = _row(group_id=9, event_ids=[1], action="adjust")
    new_id = reg.supersede(lease, old, succ, reason="past_window")
    old, new = reg.reconcile_rows(row_ids=[old_id, new_id])
    assert old.status == SUPERSEDED and old.outcome == {"reason": "past_window", "successor": new_id}
    assert new.status == OPEN and new.group_id == 9

    def crash(i, op):
        if i == 1:
            raise SimulatedCrash("between the two statements")

    be.db.crash_hook = crash
    with pytest.raises(SimulatedCrash):
        reg.supersede(lease, new, _row(group_id=9, event_ids=[1]))
    be.db.crash_hook = None
    assert len(reg.reconcile_rows(group_id=9)) == 2          # no third row: rolled back whole
    with pytest.raises(ValueError):
        reg.supersede(lease, new, _row(group_id=10))         # the successor keeps the GROUP_ID
