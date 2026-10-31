"""B-3's decrement pass (plan §3.3's table, rules 1, 2, 4, 8, 9; §6 B-3) — end to
end: the real ``ops`` documents against the stateful fake Shopify
(``tests/fake_shop.py``), the register on the in-memory store."""
from __future__ import annotations

from typing import List

import pytest

from backend.persistence.records import (
    DONE,
    FAILED,
    HELD,
    KIND_BASELINE,
    OPEN,
    SCOPE_KEY,
    SUPERSEDED,
)
from backend.services import ledger as L
from backend.services import passes
from backend.services.lease import PassLease

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend

K = ("SKU1", "42")


class Env:
    def __init__(self, *, baseline: bool = True) -> None:
        self.be = MemoryBackend()
        if baseline:
            self.be.seed_baseline()
        self.shop = FakeShop()

    def register(self):
        return self.be.register(actor="tester")

    def decrement(self, **kw):
        reg = self.register()
        lease = PassLease(reg, "decrement", sleep=lambda s: None)
        return passes.decrement_pass(reg, self.shop, PROMO, lease=lease, sleep=lambda s: None, **kw)

    def unstamped(self) -> List[int]:
        return [e.event_id for e in self.be.store().read_unstamped_events()]

    def rows(self, **kw):
        return self.register().reconcile_rows(**kw)

    def sold(self, sku="SKU1", size="42", outlet_line=False, pairs=1):
        """``pairs`` shelf pairs, their +1 already reflected (stamped), then one sale."""
        for _ in range(pairs):
            self.be.add_shelf_pair(sku, size, reconciled=True)
        return self.be.consume(sku, size, outlet_line=outlet_line)


# ---------------------------------------------------------------------------
# §3.3's table, Δ < 0
# ---------------------------------------------------------------------------

def test_a_full_price_sale_lowers_relatively_once_and_drafts_at_zero():
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 1, "43": 0})
    e = env.sold()
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "stamped"}
    assert env.shop.available(out_p.gid, "42") == 0
    (adj,) = [c for c in env.shop.calls if c[0] == "adjust"]
    change = adj[1]["input"]["changes"][0]
    assert change["delta"] == -1 and change["changeFromQuantity"] == 1           # rule 9's fence
    assert adj[1]["idempotencyKey"] and adj[1]["input"]["referenceDocumentUri"].startswith("gid://scansia-manager/")
    assert env.shop.products[out_p.gid].status == "DRAFT"                        # rule 1: decrement drafts too
    assert env.unstamped() == []
    (row,) = [r for r in env.rows() if r.action == L.A_ADJUST]
    assert row.status == DONE and row.sent_available == 1 and row.request and row.event_ids == {e}


def test_available_at_or_below_zero_stamps_without_touching_shopify():
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 0, "43": 2})
    env.sold(pairs=2)                                           # N 1: the group, not the N-0 release
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "stamp_only"}
    assert env.shop.count("adjust") == 0 and env.unstamped() == []
    assert env.shop.products[out_p.gid].status == "ACTIVE"          # 43 still sells: total 2


def test_a_negative_available_is_never_raised():
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": -1, "43": 1})
    env.sold()
    env.decrement()
    assert env.shop.available(out_p.gid, "42") == -1 and env.shop.count("adjust") == 0


def test_a_committed_unit_keeps_the_group_pending():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0}, committed={"42": 1})
    e = env.sold(pairs=2)
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "pending"} and env.unstamped() == [e]
    assert "pending_committed" in rep.report.kinds()


def test_no_outlet_on_a_clean_answer_stamps():
    env = Env()
    env.sold(pairs=2)
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "stamp_only"} and env.unstamped() == []


def test_a_failed_resolution_is_not_none_the_group_stays_pending(monkeypatch):
    from backend.services import resolvers

    env = Env()
    e = env.sold(pairs=2)
    monkeypatch.setattr(resolvers, "outlet_resolver",
                        lambda t, sku: {"matches": [], "warning": "TRUNCATED", "truncated": True})
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "pending"} and env.unstamped() == [e]


def test_a_size_absent_group_stays_pending_never_stamped():
    env = Env()
    env.shop.product("Nike - Outlet", sizes=("43",), promo={"43": 1})
    env.be.add_shelf_pair("SKU1", "42", reconciled=True)      # N 1: not the N-0 release
    e = env.be.consume("SKU1", "42")
    env.be.add_shelf_pair("SKU1", "42", reconciled=True)
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "pending"} and e in env.unstamped()
    assert "pending_size_absent" in rep.report.kinds()


def test_a_multi_outlet_lowering_takes_the_active_outlet_first():
    env = Env()
    draft = env.shop.product("Nike - Outlet B", status="DRAFT", promo={"42": 1})
    active = env.shop.product("Nike - Outlet A", promo={"42": 1, "43": 1})
    env.sold()
    env.decrement()
    assert env.shop.available(active.gid, "42") == 0 and env.shop.available(draft.gid, "42") == 1
    assert env.unstamped() == []


def test_a_minus_two_split_over_two_outlets_is_one_group():
    env = Env()
    a = env.shop.product("Nike - Outlet A", promo={"42": 1})
    b = env.shop.product("Nike - Outlet B", status="DRAFT", promo={"42": 1})
    for _ in range(2):
        env.be.add_shelf_pair("SKU1", "42", reconciled=True)
    env.be.consume("SKU1", "42")
    env.be.consume("SKU1", "42")
    env.decrement()
    assert (env.shop.available(a.gid, "42"), env.shop.available(b.gid, "42")) == (0, 0)
    adjs = [r for r in env.rows() if r.action == L.A_ADJUST]
    assert len(adjs) == 2 and len({r.group_id for r in adjs}) == 1 and {r.status for r in adjs} == {DONE}


def test_an_outlet_line_sale_runs_the_product_total_rule_and_stamps():
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 0, "43": 0})   # Shopify already moved
    env.sold(outlet_line=True, pairs=2)
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "stamp_only"} and env.shop.count("adjust") == 0
    assert env.shop.products[out_p.gid].status == "DRAFT" and env.unstamped() == []


def test_customisation_only_units_do_not_keep_an_outlet_active():
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 1, "Solo Personalizzazione": 3},
                             extra_variants=(("Solo Personalizzazione", "SKU1"),))
    env.sold()
    env.decrement()
    assert env.shop.products[out_p.gid].status == "DRAFT"


def test_a_positive_group_is_left_for_the_publish():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    _, e = env.be.add_shelf_pair("SKU1", "42")
    rep = env.decrement()
    assert rep.applied == {} and env.unstamped() == [e]
    assert "positive_left_for_publish" in rep.report.kinds() and env.shop.count("adjust") == 0


def test_the_keyless_miss_is_surfaced_never_stamped():
    env = Env()
    e = env.be.consume("NOPE", "40")
    rep = env.decrement()
    assert "consume_miss" in rep.report.kinds() and env.unstamped() == [e]


def test_the_reader_refuses_the_decrement_before_the_baseline():
    from backend.persistence.records import BaselineMissing

    env = Env(baseline=False)
    env.sold()
    with pytest.raises(BaselineMissing):
        env.decrement()
    assert env.register().lease_state().holder is None      # released on the way out


# ---------------------------------------------------------------------------
# Rule 2: outcomes the app cannot know
# ---------------------------------------------------------------------------

def test_a_lost_answer_leaves_the_row_open_and_the_re_send_answers_from_the_key():
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 2})
    for _ in range(2):
        env.be.add_shelf_pair("SKU1", "42", reconciled=True)
    e = env.be.consume("SKU1", "42")
    env.shop.fault("adjust", "lose_after")
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "waiting"} and env.unstamped() == [e]
    (row,) = [r for r in env.rows() if r.action == L.A_ADJUST]
    assert row.status == OPEN and row.sent_at is not None
    assert env.shop.available(out_p.gid, "42") == 1                    # it DID land
    env.decrement()                                                    # recovery: the identical re-send
    assert env.shop.available(out_p.gid, "42") == 1                    # applied once
    assert env.shop.count("adjust") == 2 and env.unstamped() == []
    adj = [c for c in env.shop.calls if c[0] == "adjust"]
    assert adj[0][1] == adj[1][1]                                       # byte-identical request


def test_a_send_that_never_ran_is_applied_by_the_re_send():
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 2})
    for _ in range(2):
        env.be.add_shelf_pair("SKU1", "42", reconciled=True)
    env.be.consume("SKU1", "42")
    env.shop.fault("adjust", "timeout_before")
    env.decrement()
    assert env.shop.available(out_p.gid, "42") == 2
    env.decrement()
    assert env.shop.available(out_p.gid, "42") == 1 and env.unstamped() == []


def test_a_stale_change_from_quantity_fails_the_row_and_the_next_pass_re_plans():
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 2})
    for _ in range(2):
        env.be.add_shelf_pair("SKU1", "42", reconciled=True)
    e = env.be.consume("SKU1", "42")
    env.shop.on("adjust", lambda shop: shop.sell(out_p.gid, "42") if shop.count("adjust") == 0 else None)
    env.decrement()                                   # an outlet sale lands between the read and the send
    (row,) = [r for r in env.rows() if r.action == L.A_ADJUST]
    assert row.status == FAILED and row.outcome["answer"] == L.ANS_STALE
    assert env.unstamped() == [e] and env.shop.available(out_p.gid, "42") == 1
    env.decrement()
    assert env.shop.available(out_p.gid, "42") == 0 and env.unstamped() == []


def test_past_the_window_an_unknown_minus_is_re_applied_as_a_new_row():
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 2})
    for _ in range(2):
        env.be.add_shelf_pair("SKU1", "42", reconciled=True)
    env.be.consume("SKU1", "42")
    env.shop.fault("adjust", "timeout_before")
    env.decrement()
    env.be.advance(23 * 3600 * 1000 + 1000)          # past SENT_AT + 24 h − 1 h
    rep = env.decrement()
    old, new = sorted([r for r in env.rows() if r.action == L.A_ADJUST], key=lambda r: r.row_id)
    assert old.status == SUPERSEDED and old.outcome["successor"] == new.row_id
    assert new.status == DONE and new.group_id == old.group_id and new.idempotency_key != old.idempotency_key
    assert env.shop.available(out_p.gid, "42") == 1 and env.unstamped() == []
    assert "possible_extra_minus_one" in rep.report.kinds()


def test_concurrent_on_the_one_in_window_re_send_re_applies_at_once():
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 2})
    for _ in range(2):
        env.be.add_shelf_pair("SKU1", "42", reconciled=True)
    env.be.consume("SKU1", "42")
    env.shop.fault("adjust", "lose_after")                     # landed, answer lost
    env.decrement()
    env.shop.fault("adjust", "concurrent")                     # the one re-send: CONCURRENT
    env.decrement()
    statuses = sorted((r.row_id, r.status) for r in env.rows() if r.action == L.A_ADJUST)
    assert [s for _, s in statuses] == [SUPERSEDED, DONE]
    assert env.shop.available(out_p.gid, "42") == 0            # the double −1 errs to fewer, surfaced


def test_a_dead_generations_never_sent_row_closes_failed_without_holding():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 1})
    e = env.sold()
    reg = env.register()
    dead = PassLease(reg, "publish")
    dead.take()
    reg.write_ahead(dead, [L.RowSpec(action=L.A_ADJUST, keys=(K,), event_ids=(e,), purpose=L.P_LOWER, share=-1,
                                     inventory_item_id="gid://shopify/InventoryItem/x", location_id=PROMO)
                           .to_insert(actor="t", group_id=7)])
    env.be.advance(301_000)                                    # the holder died; its lease expired
    env.decrement()
    (dead_row,) = env.rows(group_id=7)
    assert dead_row.status == FAILED and dead_row.outcome["never_sent"] is True
    assert env.unstamped() == []                               # the group itself ran this pass


# ---------------------------------------------------------------------------
# Holds: the held-key lowering, the N-0 release
# ---------------------------------------------------------------------------

def test_a_quiet_held_key_above_n_is_lowered_by_its_excess_never_raised():
    env = Env(baseline=False)
    out_p = env.shop.product("Nike - Outlet", promo={"42": 3})
    p, e = env.be.add_shelf_pair("SKU1", "42")
    env.be.seed_baseline(held=[{"key": list(K), "reason": "moved", "event_ids": [e]}])
    env.decrement()
    assert env.shop.available(out_p.gid, "42") == 1             # N = 1: excess 2 lowered
    (row,) = [r for r in env.rows() if L.purpose(r) == L.P_EXCESS]
    assert row.status == DONE and row.event_ids == frozenset()
    env.shop.levels[(env.shop.variant(out_p.gid, "42").item, PROMO)].available = 0
    env.decrement()
    assert env.shop.available(out_p.gid, "42") == 0             # below N: never raised


def test_the_held_key_lowering_counts_a_pending_minus_as_leaving():
    env = Env(baseline=False)
    out_p = env.shop.product("Nike - Outlet", promo={"42": 2})
    for _ in range(2):
        env.be.add_shelf_pair("SKU1", "42", reconciled=True)
    env.be.seed_baseline(held=[{"key": list(K), "reason": "moved", "event_ids": []}])
    env.be.consume("SKU1", "42")                                # post-hold −1: N 1
    env.decrement()
    assert env.shop.available(out_p.gid, "42") == 1             # lowered once, by the group — not twice


def test_the_n0_release_stamps_and_releases_a_held_key_with_no_unit():
    env = Env(baseline=False)
    env.shop.product("Nike - Outlet", promo={"42": 0})
    p, e1 = env.be.add_shelf_pair("SKU1", "42")
    env.be.seed_baseline(held=[{"key": list(K), "reason": "moved", "event_ids": [e1]}])
    e2 = env.be.consume("SKU1", "42")                          # the last pair sells: N 0
    rep = env.decrement()
    assert env.unstamped() == [] and env.shop.count("adjust") == 0
    (rel,) = [b for b in env.register().audit_rows(KIND_BASELINE) if (b.payload or {}).get("scope") == SCOPE_KEY]
    assert rel.payload["reason"] == "N 0" and rel.payload["released"] is True
    assert sorted(rel.payload["stamped"]) == sorted([e1, e2]) and env.register().held_keys() == []
    assert "n0_released" in rep.report.kinds()


def test_the_n0_release_waits_while_a_unit_is_on_sale():
    env = Env(baseline=False)
    out_p = env.shop.product("Nike - Outlet", promo={"42": 1})
    p, e1 = env.be.add_shelf_pair("SKU1", "42")
    env.be.seed_baseline(held=[{"key": list(K), "reason": "moved", "event_ids": [e1]}])
    env.be.consume("SKU1", "42")
    env.decrement()                                            # the held-key lowering takes the unit first
    assert env.shop.available(out_p.gid, "42") == 0
    env.decrement()                                            # then the next pass releases
    assert env.unstamped() == [] and env.register().held_keys() == []


def test_the_n0_release_is_refused_inside_a_held_rows_retention():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    p, e1 = env.be.add_shelf_pair("SKU1", "42")
    reg = env.register()
    lease = PassLease(reg, "publish")
    lease.take()
    (rid,) = reg.write_ahead(lease, [L.RowSpec(action=L.A_ADJUST, keys=(K,), event_ids=(e1,), purpose=L.P_ADD,
                                               share=1, inventory_item_id="i", location_id=PROMO)
                                     .to_insert(actor="t", group_id=1)])
    reg.mark_sent(lease, rid, first=True, sent_available=0, request="{}")
    reg.record_outcome(lease, rid, expect=OPEN, status=HELD, outcome={"reason": "unknown_plus"})
    lease.release()
    env.be.consume("SKU1", "42")
    env.decrement()
    assert env.unstamped() != []                                # a stalled +Δ could still land
    env.be.advance(24 * 3600 * 1000 + 1000)
    env.decrement()
    assert env.unstamped() == []


@pytest.mark.parametrize("hold_ends", ["unit_back", "shipped"])
def test_d33_n0_held_unit_no_stamp_then_unit_back_minus_applied_or_shipped_release_stamps(hold_ends):
    """D33 (rule 4 + the N-0 release): "N 0, held unit (committed > 0) → no stamp;
    hold ends with unit back → −1 applied; hold ends shipped (committed 0,
    available ≤ 0) → release stamps"."""
    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 0}, committed={"42": 1})
    e = env.sold()                                             # N 0
    env.decrement()
    assert env.unstamped() == [e] and env.shop.count("adjust") == 0
    lvl = env.shop.promo(out_p.gid, "42")
    if hold_ends == "unit_back":
        lvl.committed, lvl.available = 0, 1                    # the draft order is cancelled
        env.decrement()
        assert env.shop.available(out_p.gid, "42") == 0 and env.unstamped() == []
        assert env.shop.count("adjust") == 1
    else:
        lvl.committed = 0                                      # the draft order ships: the unit leaves
        rep = env.decrement()
        assert env.unstamped() == [] and env.shop.count("adjust") == 0
        (rel,) = [b for b in env.register().audit_rows(KIND_BASELINE) if (b.payload or {}).get("scope") == SCOPE_KEY]
        assert rel.payload["reason"] == "N 0" and rel.payload["stamped"] == [e] and rel.payload["released"] is False
        assert "n0_released" in rep.report.kinds()


# ---------------------------------------------------------------------------
# F1 guard: a lowering still reads a title match of another SKU (or of none) —
# only raising flows refuse it; the unit there is lowered, never left online
# ---------------------------------------------------------------------------
F1_FOREIGN = "SKU9"


def _f1_outlet(env: Env, foreign, **kw):
    """The outlet resolves by SKU1 (its «43»); its «42» carries ``foreign``."""
    return env.shop.product("Nike - Outlet", sizes=("43",), extra_variants=(("42", foreign),), **kw)


@pytest.mark.parametrize("foreign", [F1_FOREIGN, None])
def test_f1_a_minus_lowers_a_unit_on_a_title_match_of_another_sku(foreign):
    env = Env()
    out_p = _f1_outlet(env, foreign, promo={"42": 1, "43": 0})
    env.sold(pairs=2)                                           # N 1: the group, not the N-0 release
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "stamped"} and env.unstamped() == []
    assert env.shop.available(out_p.gid, "42") == 0
    assert env.shop.products[out_p.gid].status == "DRAFT"          # then the product-total rule


@pytest.mark.parametrize("foreign", [F1_FOREIGN, None])
def test_f1_the_n0_release_waits_on_a_held_unit_on_a_title_match_of_another_sku(foreign):
    env = Env()
    _f1_outlet(env, foreign, promo={"42": 0, "43": 0}, committed={"42": 1})
    e = env.sold()                                             # N 0
    env.decrement()
    assert env.unstamped() == [e] and env.shop.count("adjust") == 0
    assert [b for b in env.register().audit_rows(KIND_BASELINE)
            if (b.payload or {}).get("scope") == SCOPE_KEY] == []


@pytest.mark.parametrize("foreign", [F1_FOREIGN, None])
def test_f1_an_outlet_line_sale_on_a_title_match_of_another_sku_runs_the_product_total_rule(foreign):
    env = Env()
    out_p = _f1_outlet(env, foreign, promo={"42": 0, "43": 0})
    env.sold(outlet_line=True, pairs=2)
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "stamp_only"} and env.unstamped() == []
    assert env.shop.products[out_p.gid].status == "DRAFT"


# ---------------------------------------------------------------------------
# §3.1 backfill: a published pair whose publish saw a `review` match gets its id on a later pass
# ---------------------------------------------------------------------------
def _backfill_calls(monkeypatch):
    from backend.services import resolvers

    real, calls = resolvers.outlet_resolver, []

    def spy(transport, sku):
        calls.append(sku)
        return real(transport, sku)

    monkeypatch.setattr(resolvers, "outlet_resolver", spy)
    return calls


def test_backfill_writes_the_outlet_product_id_on_an_online_shelf_pair_with_one_register_row():
    from backend.persistence.records import KIND_REGISTER
    from backend.services import flows

    env = Env()
    out_p = env.shop.product("Nike - Outlet")
    pid, _ = env.be.add_shelf_pair("SKU1", "42", reconciled=True, online=True)
    assert env.unstamped() == []
    env.decrement()
    assert [p.outlet_product_id for p in env.register().shelf("SKU1")] == [flows.product_number(out_p.gid)]
    rows = [r for r in env.register().audit_rows(KIND_REGISTER) if r.action == "set_outlet_product_id"]
    assert len(rows) == 1 and [p["pair_id"] for p in rows[0].payload["pairs"]] == [pid]


def test_backfill_never_writes_a_review_match():
    from backend.persistence.records import KIND_REGISTER

    env = Env()
    env.shop.product("Nike - Outlet", outlet=False)           # title says outlet, the collection has not caught up
    env.be.add_shelf_pair("SKU1", "42", reconciled=True, online=True)
    env.decrement()
    assert [p.outlet_product_id for p in env.register().shelf("SKU1")] == [None]
    assert [r for r in env.register().audit_rows(KIND_REGISTER) if r.action == "set_outlet_product_id"] == []


def test_backfill_a_second_pass_makes_no_resolver_call_and_an_offline_pair_none_at_all(monkeypatch):
    env = Env()
    env.shop.product("Nike - Outlet")
    env.shop.product("Adidas - Outlet", sku="SKU2")
    env.be.add_shelf_pair("SKU1", "42", reconciled=True, online=True)
    env.be.add_shelf_pair("SKU2", "42", reconciled=True)            # ONLINE 0: not published, not resolved
    calls = _backfill_calls(monkeypatch)
    env.decrement()
    assert calls == ["SKU1"]
    env.decrement()
    assert calls == ["SKU1"]


def test_backfill_one_skus_failure_is_surfaced_and_never_aborts_the_pass(monkeypatch):
    from backend.services import flows
    from backend.shopify.ops import ShopifyUserError

    env = Env()
    env.shop.product("Nike - Outlet")
    env.shop.product("Adidas - Outlet", sku="SKU2")
    env.be.add_shelf_pair("SKU1", "42", reconciled=True, online=True)
    env.be.add_shelf_pair("SKU2", "42", reconciled=True, online=True)
    real = flows.maybe_set_outlet_product

    def flaky(ctx, sku, res):
        if sku == "SKU1":
            raise ShopifyUserError("productUpdate", [{"message": "boom"}])
        return real(ctx, sku, res)

    monkeypatch.setattr(flows, "maybe_set_outlet_product", flaky)
    rep = env.decrement()
    assert "outlet_product_backfill_error" in rep.report.kinds()
    assert [p.outlet_product_id is None for p in env.register().shelf()] == [True, False]   # SKU1 retried next pass


def test_backfill_renews_the_lease_before_every_resolve(monkeypatch):
    from backend.services import resolvers
    from tests.test_lease import Mono

    env = Env()
    for sku in ("SKU1", "SKU2"):
        env.shop.product("Nike - Outlet", sku=sku, outlet=False)    # review: nothing is written, nothing else renews
        env.be.add_shelf_pair(sku, "42", reconciled=True, online=True)
    mono = Mono()
    reg = env.register()
    lease = PassLease(reg, "decrement", clock=mono, sleep=lambda s: None)
    window = (lease.ttl_ms - lease.send_margin_ms) / 1000.0
    left, real = [], resolvers.outlet_resolver

    def slow(transport, sku):
        left.append(lease.send_deadline() - mono())
        mono.t += lease.renew_after_ms / 1000.0                     # a resolve takes as long as a renew is due
        return real(transport, sku)

    monkeypatch.setattr(resolvers, "outlet_resolver", slow)
    passes.decrement_pass(reg, env.shop, PROMO, lease=lease, sleep=lambda s: None)
    assert left == [window, window]


def test_backfill_a_lost_lease_aborts_the_pass_and_releases(monkeypatch):
    from backend.persistence.records import LeaseLost
    from backend.services import flows

    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 1, "43": 0})
    env.shop.product("Adidas - Outlet", sku="SKU2")
    e = env.sold()                                                   # a pending group that must NOT be applied
    env.be.add_shelf_pair("SKU2", "42", reconciled=True, online=True)
    seen = []

    def lost(ctx, sku, res):
        seen.append(sku)
        raise LeaseLost("taken over")

    monkeypatch.setattr(flows, "maybe_set_outlet_product", lost)
    reg = env.register()
    lease = PassLease(reg, "decrement", sleep=lambda s: None)
    with pytest.raises(LeaseLost):
        passes.decrement_pass(reg, env.shop, PROMO, lease=lease, sleep=lambda s: None)
    assert seen == ["SKU2"] and not lease.held
    assert env.unstamped() == [e] and env.shop.count("adjust") == 0


def test_backfill_a_store_error_on_one_write_is_surfaced_and_the_groups_still_apply(monkeypatch):
    from backend.persistence.records import GuardMismatch
    from backend.services import flows

    env = Env()
    out_p = env.shop.product("Nike - Outlet", promo={"42": 1, "43": 0})
    env.shop.product("Adidas - Outlet", sku="SKU2")
    env.shop.product("Puma - Outlet", sku="SKU3")
    env.sold()                                                       # SKU1|42: a group the pass must still apply
    env.be.add_shelf_pair("SKU2", "42", reconciled=True, online=True)
    env.be.add_shelf_pair("SKU3", "42", reconciled=True, online=True)
    real = flows.maybe_set_outlet_product

    def flaky(ctx, sku, res):
        if sku == "SKU2":
            raise GuardMismatch(1, 2, 1)
        return real(ctx, sku, res)

    monkeypatch.setattr(flows, "maybe_set_outlet_product", flaky)
    rep = env.decrement()
    assert [i for i in rep.report.items if i["kind"] == "outlet_product_backfill_error"] == [
        {"kind": "outlet_product_backfill_error", "sku": "SKU2", "error": "GuardMismatch"}]
    ids = {p.sku: p.outlet_product_id for p in env.register().shelf()}
    assert ids["SKU2"] is None and ids["SKU3"] is not None          # the next SKU was filled
    assert rep.applied == {"SKU1|42": "stamped"} and env.unstamped() == []
    assert env.shop.available(out_p.gid, "42") == 0
