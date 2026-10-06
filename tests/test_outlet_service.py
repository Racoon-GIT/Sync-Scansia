"""backend.services.outlet_service — PUBLISH (the Δ > 0 flows) on the register
and the RECONCILE ledger, end to end: the real ``ops`` documents against the
stateful fake Shopify (``tests/fake_shop.py``), the register on the in-memory
store.

Part 1 is the parity oracle (plan §4 step 2): the sheet-era publish tests
re-expressed on the register — CREATE's anti-phantom-stock order, phantom-stock
isolation, no re-inflate, TOCTOU, FIX4, size-primary matching (exact, §3.2),
DRY_RUN read-only, the post-review fixes. Retired with the ``frozen_pre``
branches (§3.3, not ported): the "delta exactly-once" compare-with-frozen pair.
Part 2 is F7's own (§3.3's Δ > 0 row, rules 2, 6, 7, 9; §3.5's one price per SKU).
"""
from __future__ import annotations

from typing import List

import pytest

from backend.persistence.records import (
    DONE,
    FAILED,
    HELD,
    OPEN,
    BaselineMissing,
    Fence,
    InsertAudit,
)
from backend.services import ledger as L
from backend.services import outlet_service, passes
from backend.services.lease import PassLease
from backend.shopify import ops

from tests.fake_shop import MAG, PROMO, FakeShop
from tests.register_fixtures import MemoryBackend

MUTATIONS = {"duplicate", "adjust", "activate", "deactivate", "bulk", "status", "publish", "delete"}


class World:
    def __init__(self, *, baseline: bool = True) -> None:
        self.be = MemoryBackend()
        if baseline:
            self.be.seed_baseline()
        self.shop = FakeShop()

    def returned(self, sku="SKU1", size="42", n=1, *, price_outlet="50.00", price_high="100.00", **kw) -> List[int]:
        out = []
        for _ in range(n):
            _, eid = self.be.add_shelf_pair(sku, size, price_outlet=price_outlet, price_high=price_high, **kw)
            out.append(eid)
        return out

    def register(self):
        return self.be.register(actor="tester")

    def preview(self):
        return outlet_service.publish_preview(self.register(), self.shop, promo_location_id=PROMO)

    def apply(self, plan, **kw):
        reg = self.register()
        lease = kw.pop("lease", None) or PassLease(reg, "publish", sleep=lambda s: None)
        return outlet_service.publish_apply(reg, self.shop, plan, promo_location_id=PROMO, lease=lease,
                                            take_bound_s=0, **kw)

    def run(self):
        plan = self.preview()
        return plan, self.apply(plan)

    def unstamped(self) -> List[int]:
        return [e.event_id for e in self.be.store().read_unstamped_events()]

    def rows(self, **kw):
        return self.register().reconcile_rows(**kw)

    def mutated(self) -> List[str]:
        return [n for n in self.shop.names() if n in MUTATIONS]


def _source(w: World, sizes=("42", "43"), **kw):
    return w.shop.product("Nike Air", outlet=False, sizes=sizes, **kw)


# =============================================================================
# Part 1 — the parity oracle
# =============================================================================

def test_create_branch_exact_order():
    w = World()
    events = w.returned(n=2)
    _source(w, promo={"42": 5, "43": 5}, mag={"42": 3, "43": 3})
    plan, rep = w.run()
    assert plan.actions[0].branch == "CREATE"
    (o,) = rep.outcomes
    assert o.status == "APPLIED" and sorted(o.stamped_event_ids) == sorted(events)
    names = w.mutated()
    assert names[0] == "duplicate"                                     # (1) before every other mutation
    adj = [(i, c) for i, c in enumerate(w.shop.calls) if c[0] == "adjust"]
    deact = [i for i, c in enumerate(w.shop.calls) if c[0] == "deactivate"]
    mag_zero = [i for i, c in adj if c[1]["input"]["changes"][0]["locationId"] == MAG]
    promo_zero = [i for i, c in adj if c[1]["input"]["changes"][0]["locationId"] == PROMO
                  and c[1]["input"]["changes"][0]["delta"] < 0]
    add = [i for i, c in adj if c[1]["input"]["changes"][0]["delta"] > 0]
    for i in deact:                                                    # (2) each level zeroed, then disconnected
        assert any(j < i for j in mag_zero)
    assert max(deact) < min(promo_zero)                                # every non-Promo before the Promo zeros
    assert max(promo_zero) < min(add)                                  # (3)/(4) the Δ after the zeros
    bulk = [i for i, c in enumerate(w.shop.calls) if c[0] == "bulk"]
    assert max(add) < min(bulk)                                        # (5) DENY after the Δ
    assert names[-2:] == ["status", "publish"]                         # (6) ACTIVE, publish LAST
    dup = next(g for g, p in w.shop.products.items() if p.title == "Nike Air - Outlet")
    assert w.shop.products[dup].status == "ACTIVE" and w.shop.products[dup].published
    assert all(v.policy == "DENY" and v.tracked and v.price == "50.00" and v.compare_at == "100.00"
               for v in w.shop.products[dup].variants)
    assert w.unstamped() == []


def test_create_isolates_returned_size_only():
    w = World()
    w.returned(n=2)                                                    # size 43 NOT returned
    _source(w, promo={"42": 5, "43": 5}, mag={"42": 3, "43": 3})
    w.run()
    dup = next(g for g, p in w.shop.products.items() if p.title == "Nike Air - Outlet")
    assert w.shop.available(dup, "42") == 2 and w.shop.available(dup, "43") == 0
    for size in ("42", "43"):
        mag = w.shop.levels[(w.shop.variant(dup, size).item, MAG)]
        assert mag.available == 0 and mag.active is False


def test_active_does_not_reapply_a_stamped_event():
    w = World()
    w.be.add_shelf_pair("SKU1", "42", reconciled=True, price_outlet="50.00", price_high="100.00")
    (e_new,) = w.returned(size="43")
    out = w.shop.product("Nike - Outlet", promo={"42": 1, "43": 0})
    plan, rep = w.run()
    assert plan.actions[0].branch == "ACTIVE" and rep.outcomes[0].status == "APPLIED"
    adds = [c for c in w.shop.calls if c[0] == "adjust"]
    assert len(adds) == 1 and adds[0][1]["input"]["changes"][0]["delta"] == 1
    assert (w.shop.available(out.gid, "42"), w.shop.available(out.gid, "43")) == (1, 1)
    assert rep.outcomes[0].stamped_event_ids == (e_new,)


def test_a_key_group_sums_its_events_and_stamps_all_of_them():
    w = World()
    e1, e2 = w.returned(n=2)
    e3 = w.be.consume("SKU1", "42")                                    # −1: Δ = +1
    out = w.shop.product("Nike - Outlet", promo={"42": 0})
    plan, rep = w.run()
    (st,) = plan.actions[0].size_targets
    assert st.delta == 1 and sorted(st.event_ids) == sorted([e1, e2, e3])
    assert w.shop.available(out.gid, "42") == 1
    assert sorted(rep.outcomes[0].stamped_event_ids) == sorted([e1, e2, e3])


def test_no_positive_group_plans_nothing_and_writes_nothing():
    w = World()
    w.be.add_shelf_pair("SKU1", "42", reconciled=True)
    w.be.add_shelf_pair("SKU1", "42", reconciled=True)
    e0 = w.be.consume("SKU1", "42", outlet_line=True)                  # Δ 0: the decrement's
    w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0})
    plan, rep = w.run()
    assert plan.actions == () and rep.outcomes == ()
    assert w.mutated() == [] and w.unstamped() == [e0]


def test_toctou_branch_drift_aborts_verify_failed():
    w = World()
    w.returned(n=2)
    out = w.shop.product("Nike - Outlet", promo={"42": 0})
    plan = w.preview()
    w.shop.products[out.gid].status = "DRAFT"                          # drift ACTIVE -> DRAFT
    rep = w.apply(plan)
    assert rep.outcomes[0].status == "VERIFY_FAILED"
    assert w.mutated() == [] and len(w.unstamped()) == 2


@pytest.mark.parametrize("outlet_price,high", [("0.00", "100.00"), ("120.00", "100.00"), (None, "100.00")])
def test_fix4_invalid_price_blocks_publish(outlet_price, high):
    w = World()
    w.returned(n=2, price_outlet=outlet_price, price_high=high)
    _source(w)
    plan, rep = w.run()
    act = plan.actions[0]
    assert act.publishable is False and act.reason == "price_invalid"
    assert rep.outcomes[0].status == "PRICE_INVALID" and "duplicate" not in w.shop.names()
    assert len(w.unstamped()) == 2


def test_size_primary_exact_match_ignores_sku():
    w = World()
    w.returned(n=2)
    src = _source(w)
    w.shop.variant(src.gid, "42").sku = "ZZZ"                          # SKU mismatch on the 42 variant
    plan, rep = w.run()
    st = plan.actions[0].size_targets[0]
    assert st.size == "42" and st.matched and st.variant_id == w.shop.variant(src.gid, "42").id
    assert any(x.startswith("sku_mismatch_on_size:42:ZZZ") for x in plan.actions[0].warnings)


def test_dry_run_preview_is_read_only():
    w = World()
    w.returned(n=2)
    _source(w)
    w.be.db.log.clear()
    plan = w.preview()
    assert plan.dry_run is True and plan.actions[0].branch == "CREATE"
    assert not any(x[0] in ("renew", "commit", "insert_audit", "stamp") or x[0].startswith("lease")
                   for x in w.be.db.log)
    assert w.mutated() == []


def test_main_resolve_dry_run_fail_closed(monkeypatch):
    import main
    monkeypatch.delenv("DRY_RUN", raising=False)
    assert main._resolve_dry_run() is True
    for tok in ["", "true", "1", "yes", "garbage", "dry", "TRUE "]:
        monkeypatch.setenv("DRY_RUN", tok)
        assert main._resolve_dry_run() is True, tok
    for tok in ["false", "0", "no", "apply", "APPLY", "Off"]:
        monkeypatch.setenv("DRY_RUN", tok)
        assert main._resolve_dry_run() is False, tok


def test_main_run_sync_dry_run_unset_does_not_mutate(monkeypatch):
    import main
    recorded = {}
    monkeypatch.setattr(outlet_service, "run", lambda dry_run: recorded.update(dry_run=dry_run))
    monkeypatch.delenv("DRY_RUN", raising=False)
    main.run_sync()
    assert recorded == {"dry_run": True}


def test_legacy_run_mode_sync_is_retired():
    with pytest.raises(RuntimeError):
        outlet_service.run(dry_run=True)


def test_create_duplicate_is_draft_atomically_fix1():
    w = World()
    w.returned(n=2)
    _source(w)
    w.run()
    (dup_call,) = [c for c in w.shop.calls if c[0] == "duplicate"]
    assert dup_call[1]["newStatus"] == "DRAFT"
    status_calls = [c[1]["product"]["status"] for c in w.shop.calls if c[0] == "status"]
    assert status_calls == ["ACTIVE"]                                  # the only flip, the last but one


def test_draft_revive_quarantines_non_promo_stock_fix2():
    w = World()
    w.returned(n=2)
    w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0}, mag={"42": 3})
    plan, rep = w.run()
    assert plan.actions[0].branch == "DRAFT" and plan.actions[0].reason == "quarantine"
    assert rep.outcomes[0].status == "QUARANTINED" and w.mutated() == [] and len(w.unstamped()) == 2


def test_draft_revive_adds_on_the_live_level_then_activates_last():
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 1, "43": 2})   # 43: no shelf pair
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "REVIVED"
    assert w.shop.available(out.gid, "42") == 2                        # E9: live + Δ
    assert w.shop.available(out.gid, "43") == 0                        # zeroed: no shelf pair (R4)
    assert w.mutated()[-2:] == ["status", "publish"] and w.shop.products[out.gid].status == "ACTIVE"
    assert rep.outcomes[0].stamped_event_ids == (e,)


def test_error_isolation_runtime_error_does_not_abort_batch_fix3():
    w = World()
    w.returned(sku="SKU1", n=1)
    (e2,) = w.returned(sku="SKU2", n=1)
    _source(w, sku="SKU1")
    w.shop.product("Nike2 - Outlet", sku="SKU2", promo={"42": 0})
    w.shop.fault("duplicate", "refuse")                                # SKU1's CREATE is refused
    plan, rep = w.run()
    by = {o.sku: o for o in rep.outcomes}
    assert by["SKU1"].status == "ERROR" and by["SKU2"].status == "APPLIED"
    assert by["SKU2"].stamped_event_ids == (e2,)


def test_publication_resolved_eager_before_any_mutation_fix4(monkeypatch):
    w = World()
    w.returned(n=2)
    _source(w)

    def boom(_t):
        raise RuntimeError("Online Store publication not found")

    monkeypatch.setattr(ops, "get_online_store_publication_id", boom)
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "ERROR"
    assert any(x.startswith("publication_resolve_failed") for x in rep.outcomes[0].warnings)
    assert w.mutated() == [] and len(w.unstamped()) == 2


def test_fix5_sku_cross_check_fires_on_active_branch():
    w = World()
    w.returned(n=2)
    out = w.shop.product("Nike - Outlet", promo={"42": 0})
    w.shop.variant(out.gid, "42").sku = "OTHER-SKU"
    w.shop.variant(out.gid, "43").sku = "SKU1"                         # still resolves by SKU
    plan = w.preview()
    assert any(x.startswith("sku_mismatch_on_size:42:OTHER-SKU") for x in plan.actions[0].warnings)


def test_create_quarantines_when_levels_truncated_after_duplicate_fix6b():
    w = World()
    w.returned(n=2)
    _source(w, promo={"42": 4})
    w.shop.duplicate_truncated = True
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "QUARANTINED"
    (create,) = [r for r in w.rows() if r.action == L.A_CREATE]
    assert create.status == HELD                                       # every key of its list held
    dup = next(g for g, p in w.shop.products.items() if p.title == "Nike Air - Outlet")
    assert w.shop.products[dup].status == "DRAFT" and w.shop.available(dup, "42") == 0   # the orphan zeroed
    assert len(w.unstamped()) == 2


# =============================================================================
# Part 2 — F7
# =============================================================================

def test_preview_refuses_before_the_whole_register_baseline():
    w = World(baseline=False)
    w.returned()
    with pytest.raises(BaselineMissing):
        w.preview()


def test_an_add_is_relative_fenced_and_keyed_with_no_level_precondition():
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 3})
    w.run()
    (adj,) = [c[1] for c in w.shop.calls if c[0] == "adjust"]
    ch = adj["input"]["changes"][0]
    assert (ch["delta"], ch["changeFromQuantity"]) == (1, 3) and adj["idempotencyKey"]
    assert w.shop.available(out.gid, "42") == 4
    add = next(r for r in w.rows() if L.purpose(r) == L.P_ADD)
    assert add.status == DONE and add.sent_available == 3 and add.request


def test_every_exposed_variant_is_deny_and_tracked_before_the_add():
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0}, policy="CONTINUE", tracked=False)
    w.run()
    names = w.mutated()
    assert names.index("bulk") < names.index("adjust")
    assert all(v.policy == "DENY" and v.tracked for v in w.shop.products[out.gid].variants)


def test_activation_is_explicit_and_only_for_an_absent_or_inactive_level():
    w = World()
    w.returned(size="42")
    w.returned(size="43")
    w.returned(size="44")
    out = w.shop.product("Nike - Outlet", sizes=("42", "43", "44"), promo={"42": 0, "43": 0},
                         inactive=("43",), absent=("44",))
    w.run()
    acts = [c[1] for c in w.shop.calls if c[0] == "activate"]
    items = {c["inventoryItemId"] for c in acts}
    assert items == {w.shop.variant(out.gid, "43").item, w.shop.variant(out.gid, "44").item}
    assert all((c["available"], c["onHand"]) == (0, 0) and c["idempotencyKey"] for c in acts)
    assert [w.shop.available(out.gid, s) for s in ("42", "43", "44")] == [1, 1, 1]


def test_an_add_is_never_sent_beside_an_activation_that_failed():
    w = World()
    (e,) = w.returned()
    w.shop.product("Nike - Outlet", promo={"42": 0}, inactive=("42",))
    w.shop.fault("activate", "user_error:X")                           # refused; the level stays inactive
    _plan, rep = w.run()
    add = next(r for r in w.rows() if L.purpose(r) == L.P_ADD)
    act = next(r for r in w.rows() if r.action == L.A_ACTIVATE)
    assert act.status == FAILED and add.status == FAILED and add.outcome["never_sent"] is True
    assert w.shop.count("adjust") == 0 and w.unstamped() == [e]
    assert rep.outcomes[0].status == "PARTIAL"


def test_a_lost_add_answer_is_never_re_added_and_holds_the_key_past_the_window():
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0})
    w.shop.fault("adjust", "lose_after")                               # landed; answer lost
    w.run()
    assert w.shop.available(out.gid, "42") == 1
    add = next(r for r in w.rows() if L.purpose(r) == L.P_ADD)
    assert add.status == OPEN
    w.be.advance(23 * 3600 * 1000 + 1000)
    w.run()                                                            # recovery: past the window
    add = w.register().reconcile_rows(row_ids=[add.row_id])[0]
    assert add.status == HELD and w.shop.available(out.gid, "42") == 1    # never re-added
    assert ("SKU1", "42") in {h.key for h in w.register().held_keys()} and w.unstamped() == [e]


def test_an_unknown_add_inside_the_window_is_re_sent_and_answered_by_its_key():
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0})
    w.shop.fault("adjust", "lose_after")
    w.run()
    w.run()                                                            # in window: the identical re-send
    assert w.shop.available(out.gid, "42") == 1 and w.unstamped() == []


def test_the_decrements_recovery_leaves_a_sent_plus_for_the_publish():
    w = World()
    (e,) = w.returned()
    w.shop.product("Nike - Outlet", promo={"42": 0})
    w.shop.fault("adjust", "lose_after")
    w.run()
    reg = w.register()
    rep = passes.decrement_pass(reg, w.shop, PROMO, lease=PassLease(reg, "decrement", sleep=lambda s: None),
                                sleep=lambda s: None)
    add = next(r for r in w.rows() if L.purpose(r) == L.P_ADD)
    assert add.status == OPEN and "left_for_publish" in rep.report.kinds()
    assert w.unstamped() == [e]


def test_one_price_per_sku_conflicting_prices_block_it():
    w = World()
    w.returned(size="42", price_outlet="50.00")
    w.returned(size="43", price_outlet="55.00")
    w.shop.product("Nike - Outlet", promo={"42": 0, "43": 0})
    plan, rep = w.run()
    assert plan.actions[0].reason == "price_conflict" and rep.outcomes[0].status == "PRICE_CONFLICT"
    assert w.mutated() == []


def test_a_new_return_with_no_price_takes_the_skus_and_is_written():
    w = World()
    w.returned(size="42", price_outlet="50.00")
    w.be.add_shelf_pair("SKU1", "42", reconciled=True, price_outlet="50.00", price_high="100.00")
    (e_new,) = w.returned(size="43", price_outlet=None)               # PRICE_OUTLET NULL
    w.shop.product("Nike - Outlet", promo={"42": 0, "43": 0})
    plan, _rep = w.run()
    assert plan.actions[0].price == "50.00"
    assert all(p.price_outlet is not None for p in w.register().shelf("SKU1"))


def test_compare_at_is_price_high_else_price_b2():
    w = World()
    w.returned(size="42", price_high="0.00", price="159.00")         # 0.00 → PRICE
    w.returned(size="43", price_high=None, price="159.00")           # NULL → PRICE
    w.shop.product("Nike - Outlet", promo={"42": 0, "43": 0})
    plan, _rep = w.run()
    assert plan.actions[0].compare_at == "159.00" and plan.actions[0].publishable


def test_a_pair_with_no_compare_at_is_kept_out_and_never_lent_one_d25():
    """D25 / D16: pair 221-like (PRICE_HIGH NULL, PRICE NULL) beside pair 118-like
    (compare-at 159.00) on the same SKU — 221's +1 stays out, 118's publishes."""
    w = World()
    (e118,) = w.returned(size="37", price_high="159.00")
    (e221,) = w.returned(size="38", price_high=None, price=None)
    out = w.shop.product("Nike - Outlet", sizes=("37", "38"), promo={"37": 0, "38": 0})
    plan, rep = w.run()
    assert [st.size for st in plan.actions[0].size_targets] == ["37"]
    assert w.shop.available(out.gid, "37") == 1 and w.shop.available(out.gid, "38") == 0
    assert w.unstamped() == [e221] and e118 not in w.unstamped()


def test_a_spelling_twin_is_surfaced_never_published():
    w = World()
    (e_twin,) = w.returned(sku="AR-K-33PC", size="42L")
    (e_ok,) = w.returned(sku="AR-K-33PC", size="43")
    out = w.shop.product("AR - Outlet", sku="AR-K-33PC", sizes=("42 L", "43"), promo={"42 L": 0, "43": 0})
    plan, rep = w.run()
    act = plan.actions[0]
    assert act.unmatched_sizes == ("42L",) and "size_twin:42L:42 L" in act.warnings
    assert w.shop.available(out.gid, "42 L") == 0 and w.shop.available(out.gid, "43") == 1
    assert w.unstamped() == [e_twin]


def test_customisation_only_variant_never_matches():
    w = World()
    (e,) = w.returned(size="Solo Personalizzazione")
    w.shop.product("Nike - Outlet", sizes=("42",), extra_variants=(("Solo Personalizzazione", "SKU1"),),
                   promo={"42": 0, "Solo Personalizzazione": 0})
    plan, _rep = w.run()
    assert plan.actions[0].unmatched_sizes == ("Solo Personalizzazione",)
    assert w.shop.count("adjust") == 0 and w.unstamped() == [e]


def test_a_revive_lowers_customisation_only_and_held_keys_to_zero():
    w = World(baseline=False)
    _, e_held = w.be.add_shelf_pair("SKU1", "43", price_outlet="50.00", price_high="100.00")
    w.be.seed_baseline(held=[{"key": ["SKU1", "43"], "reason": "moved", "event_ids": [e_held]}])
    w.returned(size="42")
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 2, "Solo Personalizzazione": 1},
                         extra_variants=(("Solo Personalizzazione", "SKU1"),))
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "REVIVED"
    assert [w.shop.available(out.gid, s) for s in ("42", "43", "Solo Personalizzazione")] == [1, 0, 0]


def test_a_revive_stopped_at_a_zeroing_row_activates_adds_and_exposes_nothing():
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 2})
    w.shop.fault("adjust", "user_error:ADJUST_QUANTITIES_FAILED")      # the zeroing refused
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "REVIVE_STOPPED"
    tail = next(r for r in w.rows() if r.action == L.A_TAIL)
    assert tail.status == FAILED and tail.outcome["never_run"] is True
    assert w.shop.products[out.gid].status == "DRAFT" and w.shop.available(out.gid, "42") == 0
    assert w.unstamped() == [e]
    plan2, rep2 = w.run()                                              # the retry is a new revive, not deferred
    assert rep2.outcomes[0].status == "REVIVED"


def test_a_revive_is_deferred_while_a_row_of_its_product_is_open():
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0}, policy="CONTINUE")
    reg = w.register()
    lease = PassLease(reg, "x")
    lease.take()
    reg.write_ahead(lease, [L.RowSpec(action=L.A_NORMALISE, keys=(("SKU9", "99"),),   # not the revive's key:
                                      product_id=outlet_service.product_number(out.gid)).to_insert(actor="t", group_id=1)])
    reg.mark_sent(lease, reg.reconcile_rows()[0].row_id, first=False, step="x")
    lease.release()
    w.shop.fault("bulk", "timeout_before", times=5)                   # the open row's re-runs stay unknown
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "DEFERRED"


def test_a_create_failing_after_step_1_leaves_an_orphan_drafted_zeroed_and_held():
    w = World()
    w.returned(n=2)
    _source(w, promo={"42": 3})
    w.shop.fault("status", "user_error:X")                            # step 6 refused
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "CREATE_ABORTED"
    (create,) = [r for r in w.rows() if r.action == L.A_CREATE]
    assert create.status == HELD and create.product_id is not None and create.step
    dup = outlet_service.product_gid(create.product_id)
    assert w.shop.available(dup, "42") == 0
    assert ("SKU1", "42") in {h.key for h in w.register().held_keys()}


def test_a_lost_duplicate_answer_leaves_an_orphan_unknown_found_later_by_title():
    w = World()
    w.returned(n=2)
    _source(w)
    w.shop.fault("duplicate", "lose_after")
    _plan, rep = w.run()
    (create,) = [r for r in w.rows() if r.action == L.A_CREATE]
    assert create.status == OPEN and create.product_id is None and create.sent_at is not None
    dup = next(g for g, p in w.shop.products.items() if p.title == "Nike Air - Outlet")
    w.shop.products[dup].outlet_member = True                         # re-indexed: an outlet match by title
    w.run()                                                            # recovery searches by title
    create = w.register().reconcile_rows(row_ids=[create.row_id])[0]
    assert create.status == HELD and create.product_id == outlet_service.product_number(dup)
    assert w.shop.products[dup].status == "DRAFT"


def test_a_re_derived_plus_larger_than_confirmed_is_deferred():
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0})
    plan = w.preview()
    w.returned()                                                       # a second return after the preview
    rep = w.apply(plan)
    assert rep.outcomes[0].status == "NOTHING_TO_ADD" and w.shop.available(out.gid, "42") == 0
    assert any(i["kind"] == "deferred_larger_than_confirmed" for i in rep.surfaced)


def test_a_held_positive_key_is_never_added():
    w = World()
    (e_held,) = w.returned(size="42")
    w.returned(size="43")
    (e_post,) = w.returned(size="42")                                  # a post-hold return: surfaced
    w.be.seed_audit(InsertAudit(kind="HOLD", actor="t", action="hold",
                                payload={"keys": [["SKU1", "42"]], "reason": "moved", "event_ids": [e_held]}))
    out = w.shop.product("Nike - Outlet", promo={"42": 0, "43": 0})
    plan, _rep = w.run()
    assert [st.size for st in plan.actions[0].size_targets] == ["43"]
    assert w.shop.available(out.gid, "42") == 0 and sorted(w.unstamped()) == sorted([e_held, e_post])
    assert "held_positive:SKU1:42:1" in plan.anomalies


def test_a_positive_group_of_a_key_with_n0_is_never_added():
    w = World()
    p, e = w.be.add_shelf_pair("SKU1", "42", price_outlet="50.00", price_high="100.00")
    w.be.leave_shelf(p, "ANNULLATO", event_type="MANUAL_CANCEL", delta=0)   # N 0, Σ +1 pending
    plan = w.preview()
    assert plan.actions == () and "n0_positive:SKU1:42:1" in plan.anomalies


def test_the_publish_yields_the_lease_between_skus_to_a_waiting_decrement():
    w = World()
    w.returned(sku="SKU1")
    w.returned(sku="SKU2")
    w.shop.product("A - Outlet", sku="SKU1", promo={"42": 0})
    b = w.shop.product("B - Outlet", sku="SKU2", promo={"42": 0})
    plan = w.preview()
    reg = w.register()
    order = []

    def decrement_between(shop):
        if shop.count("adjust") == 0 and not order:               # SKU1's add in flight: the decrement waits
            order.append("waiter")
            reg.lease_wait("decrement:x", 60_000)

    w.shop.on("adjust", decrement_between)
    lease = PassLease(reg, "publish", sleep=lambda s: None)
    real_try = lease.try_take

    def try_take():
        if order == ["waiter"]:                                    # the waiting decrement goes first
            order.append("decrement ran")
            g = reg.lease_take("decrement:x", 60_000)
            assert g is not None
            reg.store.lease_release(Fence("decrement:x", g, 60_000))
        return real_try()

    lease.try_take = try_take
    rep = outlet_service.publish_apply(reg, w.shop, plan, promo_location_id=PROMO, lease=lease, take_bound_s=5)
    assert [o.status for o in rep.outcomes] == ["APPLIED", "APPLIED"] and order == ["waiter", "decrement ran"]
    assert w.shop.available(b.gid, "42") == 1


def test_a_lost_lease_stops_the_apply():
    w = World()
    w.returned(sku="SKU1")
    w.returned(sku="SKU2")
    w.shop.product("A - Outlet", sku="SKU1", promo={"42": 0})
    b = w.shop.product("B - Outlet", sku="SKU2", promo={"42": 0})
    plan = w.preview()

    def take_over(shop):
        if shop.count("adjust") == 0:
            w.be.advance(400_000)
            PassLease(w.register(), "decrement").take()

    w.shop.on("adjust", take_over)
    rep = w.apply(plan)
    assert [o.warnings[-1] for o in rep.outcomes] == ["lease_lost", "lease_lost"]
    assert w.shop.available(b.gid, "42") == 0                          # SKU2 never written


def test_lease_unavailable_writes_nothing():
    w = World()
    w.returned()
    w.shop.product("Nike - Outlet", promo={"42": 0})
    plan = w.preview()
    PassLease(w.register(), "decrement").take()
    rep = w.apply(plan)
    assert [o.warnings for o in rep.outcomes] == [("lease_unavailable",)] and w.mutated() == []


def test_online_is_set_on_the_pairs_whose_plus_one_applied():
    w = World()
    w.returned(n=2)
    w.shop.product("Nike - Outlet", promo={"42": 0})
    w.run()
    assert all(p.online for p in w.register().shelf("SKU1"))


def test_the_keyless_miss_is_surfaced_never_stamped():
    w = World()
    e = w.be.consume("NOPE", "40")
    plan, _rep = w.run()
    assert plan.anomalies == (f"consume_miss:{e}",) and w.unstamped() == [e]
