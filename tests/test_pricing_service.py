"""PRICES on the register (plan §3.5, §3.1 ``set_outlet_price``, §6 B-2 / D20 + D25,
§3.3 rule 6) — end to end: the real ``ops`` documents against the stateful fake
Shopify (``tests/fake_shop.py``), the register and its audit rows on the in-memory
store (``tests/register_fixtures.py``)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List

import pytest

from backend.persistence.audit_sink import RegisterAuditSink
from backend.persistence.records import KIND_PRICE, OPEN
from backend.services import flows
from backend.services import ledger as L
from backend.services import pricing_service as ps
from backend.services.lease import PassLease

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend, StubLease


class Env:
    def __init__(self, *, baseline: bool = True) -> None:
        self.be = MemoryBackend()
        if baseline:
            self.be.seed_baseline()
        self.shop = FakeShop()
        self.leases: List[PassLease] = []

    def register(self):
        return self.be.register(actor="tester")

    def outlet(self, *, status: str = "ACTIVE", sku: str = "SKU1", outlet: bool = True, price: str = "90.00",
               compare_at: str = "100.00", sizes=("42", "43")):
        p = self.shop.product(f"{sku} - Outlet", status=status, outlet=outlet, sku=sku, sizes=sizes)
        for v in p.variants:
            v.price, v.compare_at = price, compare_at
        return p

    def pair(self, sku: str = "SKU1", size: str = "42", *, product=None, **kw) -> int:
        kw.setdefault("price_high", Decimal("100.00"))
        if product is not None:
            kw["outlet_product_id"] = flows.product_number(product.gid)
        pid, _e = self.be.add_shelf_pair(sku, size, reconciled=True, **kw)
        return pid

    def preview(self, mode: str, params: ps.PriceParams, **kw) -> ps.PricePlan:
        return ps.prices_preview(self.register(), self.shop, mode, params, **kw)

    def apply(self, mode: str, params: ps.PriceParams, plan: ps.PricePlan = None, *, audit=None, **kw):
        reg = self.register()
        plan = plan or ps.prices_preview(reg, self.shop, mode, params, **{k: v for k, v in kw.items()
                                                                         if k == "status_override"})
        lease = PassLease(reg, "pricing", sleep=lambda s: None)
        self.leases.append(lease)
        sink = audit or RegisterAuditSink(reg, actor="tester")
        return ps.prices_apply(reg, self.shop, mode, params, plan, sink, promo_location_id=PROMO,
                               lease=lease, sleep=lambda s: None, **kw)

    def revert(self, intent_id: str):
        reg = self.register()
        lease = PassLease(reg, "pricing", sleep=lambda s: None)
        return ps.revert_prices(reg, self.shop, RegisterAuditSink(reg, actor="tester"), intent_id,
                                promo_location_id=PROMO, lease=lease, sleep=lambda s: None)

    def pairs(self, sku: str = "SKU1") -> Dict[int, Any]:
        return {p.pair_id: p for p in self.register().pairs(sku=sku)}

    def prices(self, product) -> List[tuple]:
        return [(v.price, v.compare_at) for v in self.shop.products[product.gid].variants]

    def price_rows(self, action: str = None):
        rows = self.register().audit_rows(KIND_PRICE)
        return [r for r in rows if action is None or r.action == action]


def pct(sku: str = "SKU1", p: float = 0.30) -> ps.PriceParams:
    return ps.PriceParams(percent_by_sku={sku: p})


def by_sku(report) -> Dict[str, Any]:
    return {o.sku: o for o in report.outcomes}


# ---------------------------------------------------------------------------
# The target price, per mode; DISCOUNT; B-2's compare-at
# ---------------------------------------------------------------------------

def test_percent_mode_prices_on_the_compare_at_to_the_cent_with_its_discount():
    env = Env()
    env.pair(price_high=Decimal("189.00"))
    (d,) = env.preview(ps.MODE_PERCENT, pct(p=0.2116)).diffs
    # 189 × 0.7884 = 149.0076 → 149.01 (half up); the discount it makes: 21.16 %
    assert (d.price, d.compare_at, d.discount, d.percent) == ("149.01", "189.00", "21.16", 0.2116)
    assert d.status == ps.STATUS_OK and d.register_changed and d.product_gid is None


def test_discount_is_what_the_price_makes_on_the_compare_at_the_sheets_k():
    assert ps.discount_of("149.00", "189.00") == "21.16"     # §6 B-2: pair 21, J 149 on H 189, K 21.16 %
    assert ps.discount_of("70.00", "100.00") == "30.00"


def test_money_parses_typed_prices_and_never_flips_a_sign():
    assert ps.money("49,9") == "49.90" and ps.money("€ 129") == "129.00" and ps.money("129.905") == "129.91"
    assert ps.money("-5") == "-5.00" and ps.money("abc") is None and ps.money("") is None
    assert ps.money("NaN") is None and ps.money(None) is None


def test_compare_at_follows_b2_price_high_else_price_and_a_pair_with_neither_never_blocks_its_sku():
    env = Env()
    env.pair(price_high=None, price=Decimal("159.00"))                    # NULL → PRICE (D25)
    env.pair(size="43", price_high=Decimal("0.00"), price=Decimal("159.00"))   # 0.00 → PRICE (D20)
    gone = env.pair(size="44", price_high=None, price=None)                # neither: kept out of publish only
    (d,) = env.preview(ps.MODE_PERCENT, pct(p=0.5)).diffs
    assert d.compare_at == "159.00" and d.price == "79.50" and d.status == ps.STATUS_OK
    assert f"no_compare_at_pairs:{gone}" in d.warnings


def test_two_compare_ats_on_one_sku_list_and_block_it():
    env = Env()
    env.pair(price_high=None, price=Decimal("239.00"))
    env.pair(size="43", price_high=Decimal("279.00"))
    (d,) = env.preview(ps.MODE_PERCENT, pct()).diffs
    assert d.status == ps.STATUS_COMPARE_AT_CONFLICT and not d.actionable
    assert "compare_at_conflict:239.00|279.00" in d.warnings


def test_direct_mode_typed_price_resolves_a_register_price_conflict_on_every_pair():
    env = Env()
    a = env.pair(price_outlet=Decimal("60.00"))
    b = env.pair(size="43", price_outlet=Decimal("65.00"))
    plan = env.preview(ps.MODE_DIRECT, ps.PriceParams(price_by_sku={"SKU1": "70"}))
    (d,) = plan.diffs
    assert d.price == "70.00" and d.discount == "30.00" and "price_conflict:60.00|65.00" in d.warnings
    rep = env.apply(ps.MODE_DIRECT, ps.PriceParams(price_by_sku={"SKU1": "70"}), plan)
    assert by_sku(rep)["SKU1"].status == ps.STATUS_REGISTER_UPDATED
    assert {(p.price_outlet, p.discount) for p in env.pairs().values()} == {(Decimal("70.00"), Decimal("30.00"))}
    assert set(env.pairs()) == {a, b}


def test_direct_repair_pushes_the_register_price_and_refuses_to_invent_one():
    env = Env()
    out = env.outlet(price="99.00", compare_at="100.00")
    env.pair(product=out, price_outlet=Decimal("80.00"), discount=Decimal("20.00"))
    (d,) = env.preview(ps.MODE_DIRECT, ps.PriceParams()).diffs
    assert d.price == "80.00" and not d.register_changed and d.live_changed and d.product_gid == out.gid
    rep = env.apply(ps.MODE_DIRECT, ps.PriceParams())
    assert by_sku(rep)["SKU1"].status == ps.STATUS_APPLIED
    assert env.prices(out) == [("80.00", "100.00")] * 2
    other = Env()
    other.pair()                                                          # no PRICE_OUTLET: fill-missing REFUSED
    (d2,) = other.preview(ps.MODE_DIRECT, ps.PriceParams()).diffs
    assert d2.status == ps.STATUS_MISSING_PRICE and "price:price_missing" in d2.warnings


def test_direct_repair_with_two_register_prices_is_listed_and_blocked():
    env = Env()
    env.pair(price_outlet=Decimal("60.00"))
    env.pair(size="43", price_outlet=Decimal("65.00"))
    (d,) = env.preview(ps.MODE_DIRECT, ps.PriceParams()).diffs
    assert d.status == ps.STATUS_PRICE_CONFLICT and not d.actionable


def test_bulk_rules_scope_by_brand_last_match_wins_and_an_override_wins_over_rules():
    env = Env()
    env.pair("SKU_R", brand="Nike")
    env.pair("SKU_O", brand="Nike")
    env.pair("SKU_X", brand="Adidas")
    params = ps.PriceParams(rules=(ps.BulkRule(0.20, {"brand": ("Nike",)}), ps.BulkRule(0.50, {"brand": ("Nike",)})),
                            override_percent_by_sku={"SKU_O": 0.10})
    diffs = {d.sku: d for d in env.preview(ps.MODE_BULK, params).diffs}
    assert set(diffs) == {"SKU_R", "SKU_O"}                               # SKU_X out of scope: not in the plan
    assert diffs["SKU_R"].price == "50.00" and diffs["SKU_O"].price == "90.00"


def test_fix4_refuses_a_price_at_or_above_the_compare_at_and_a_zero_price():
    env = Env()
    env.pair()
    (d,) = env.preview(ps.MODE_DIRECT, ps.PriceParams(price_by_sku={"SKU1": "100"})).diffs
    assert d.status == ps.STATUS_PRICE_INVALID and "price:price_ge_compare_at" in d.warnings
    (z,) = env.preview(ps.MODE_PERCENT, pct(p=1.0)).diffs
    assert z.status == ps.STATUS_PRICE_INVALID and "price:price_zero" in z.warnings


def test_a_sku_with_no_compare_at_at_all_is_missing_price_never_written():
    env = Env()
    env.pair(price_high=None, price=None)
    rep_plan = env.preview(ps.MODE_DIRECT, ps.PriceParams(price_by_sku={"SKU1": "50"}))
    (d,) = rep_plan.diffs
    assert d.status == ps.STATUS_MISSING_PRICE and "price:compare_at_missing" in d.warnings


# ---------------------------------------------------------------------------
# The push target: OUTLET_PRODUCT_ID, a collection member, ACTIVE
# ---------------------------------------------------------------------------

def test_active_outlet_gets_price_and_compare_at_on_every_variant_after_the_register():
    env = Env()
    out = env.outlet()
    a = env.pair(product=out)
    rep = env.apply(ps.MODE_PERCENT, pct())
    assert by_sku(rep)["SKU1"].status == ps.STATUS_APPLIED and rep.intent_id
    assert env.prices(out) == [("70.00", "100.00")] * 2
    assert (env.pairs()[a].price_outlet, env.pairs()[a].discount) == (Decimal("70.00"), Decimal("30.00"))
    names = env.shop.names()
    assert names.index("bulk") > names.index("variants")


def test_no_outlet_product_id_writes_the_register_only_never_a_sku_search():
    env = Env()
    env.outlet()                                   # an outlet exists, but no pair names it
    env.pair()
    rep = env.apply(ps.MODE_PERCENT, pct())
    o = by_sku(rep)["SKU1"]
    assert o.status == ps.STATUS_REGISTER_UPDATED and "no_outlet_product_id" in o.warnings
    assert env.shop.count("bulk") == 0 and env.shop.count("resolve") == 0


def test_a_draft_outlet_takes_the_price_at_its_revive_unless_status_override():
    env = Env()
    out = env.outlet(status="DRAFT")
    env.pair(product=out)
    rep = env.apply(ps.MODE_PERCENT, pct())
    assert by_sku(rep)["SKU1"].status == ps.STATUS_REGISTER_UPDATED and env.shop.count("bulk") == 0
    assert "status:DRAFT" in by_sku(rep)["SKU1"].warnings
    env2 = Env()
    out2 = env2.outlet(status="DRAFT")
    env2.pair(product=out2)
    rep2 = env2.apply(ps.MODE_PERCENT, pct(), status_override=True)
    assert by_sku(rep2)["SKU1"].status == ps.STATUS_APPLIED and env2.prices(out2) == [("70.00", "100.00")] * 2


def test_an_outlet_no_longer_in_the_collection_is_never_pushed():
    env = Env()
    out = env.outlet(outlet=False)
    env.pair(product=out)
    (d,) = env.preview(ps.MODE_PERCENT, pct()).diffs
    assert d.product_gid is None and "outlet_not_in_collection" in d.warnings


def test_two_outlet_ids_on_one_skus_pairs_are_listed_and_block_it():
    env = Env()
    a, b = env.outlet(), env.outlet()
    env.pair(product=a)
    env.pair(size="43", product=b)
    (d,) = env.preview(ps.MODE_PERCENT, pct()).diffs
    assert d.status == ps.STATUS_MULTI_PRODUCT_ID and not d.actionable


def test_register_and_live_on_target_is_already_correct_and_writes_nothing():
    env = Env()
    out = env.outlet(price="70.00", compare_at="100.00")
    env.pair(product=out, price_outlet=Decimal("70.00"), discount=Decimal("30.00"))
    plan = env.preview(ps.MODE_PERCENT, pct())
    assert plan.diffs[0].status == ps.STATUS_ALREADY_CORRECT
    rep = env.apply(ps.MODE_PERCENT, pct(), plan)
    assert rep.intent_id is None and env.price_rows() == [] and env.shop.count("bulk") == 0


def test_live_already_correct_writes_the_register_only():
    env = Env()
    out = env.outlet(price="70.00", compare_at="100.00")
    env.pair(product=out)
    rep = env.apply(ps.MODE_PERCENT, pct())
    assert by_sku(rep)["SKU1"].status == ps.STATUS_REGISTER_UPDATED and env.shop.count("bulk") == 0


def test_a_sold_pair_keeps_the_price_it_was_sold_at():
    env = Env()
    keep = env.pair(price_outlet=Decimal("60.00"))
    env.be.consume("SKU1", "42")                    # pair `keep` leaves the shelf (VENDUTO)
    on_shelf = env.pair(size="43")
    env.apply(ps.MODE_PERCENT, pct())
    pairs = env.pairs()
    assert pairs[keep].price_outlet == Decimal("60.00") and pairs[on_shelf].price_outlet == Decimal("70.00")


# ---------------------------------------------------------------------------
# The abort gate, the audit rows, the TOCTOU re-derive
# ---------------------------------------------------------------------------

class _BoomAudit(RegisterAuditSink):
    def capture_before(self, intent):  # noqa: ANN001
        raise RuntimeError("audit down")


def test_a_failed_capture_writes_nothing_register_or_shopify():
    env = Env()
    out = env.outlet()
    a = env.pair(product=out)
    reg = env.register()
    rep = env.apply(ps.MODE_PERCENT, pct(), audit=_BoomAudit(reg, actor="tester"))
    o = by_sku(rep)["SKU1"]
    assert o.status == ps.STATUS_ERROR and o.warnings == ("capture_before_failed:RuntimeError",)
    assert env.pairs()[a].price_outlet is None and env.shop.count("bulk") == 0 and rep.intent_id is None


def test_the_intent_row_is_unique_and_every_register_write_names_it():
    env = Env()
    out = env.outlet()
    env.pair(product=out)
    env.pair("SKU2")
    rep = env.apply(ps.MODE_PERCENT, ps.PriceParams(percent_by_sku={"SKU1": 0.3, "SKU2": 0.3}))
    (cap,) = env.price_rows("capture_before")
    assert cap.intent_id == rep.intent_id and len(rep.intent_id) == 32
    sets = env.price_rows(ps.ACTION_SET)
    assert sorted(r.payload["sku"] for r in sets) == ["SKU1", "SKU2"]
    assert {r.payload["intent_id"] for r in sets} == {rep.intent_id}
    (prior,) = cap.payload["priors"]                   # only the pushed product's live priors
    assert prior["product_gid"] == out.gid and len(prior["variants"]) == 2


def test_a_status_change_between_preview_and_apply_is_verify_failed():
    env = Env()
    out = env.outlet()
    env.pair(product=out)
    plan = env.preview(ps.MODE_PERCENT, pct())
    env.shop.products[out.gid].status = "DRAFT"
    rep = env.apply(ps.MODE_PERCENT, pct(), plan)
    assert by_sku(rep)["SKU1"].status == ps.STATUS_VERIFY_FAILED and env.price_rows() == []


def test_a_sku_absent_from_the_approved_plan_is_not_written():
    env = Env()
    env.pair()
    env.pair("SKU2")
    params = ps.PriceParams(percent_by_sku={"SKU1": 0.3, "SKU2": 0.3})
    plan = env.preview(ps.MODE_PERCENT, ps.PriceParams(percent_by_sku={"SKU1": 0.3}))
    rep = env.apply(ps.MODE_PERCENT, params, plan)
    assert by_sku(rep)["SKU2"].status == ps.STATUS_NOT_IN_PLAN
    assert env.pairs("SKU2")[next(iter(env.pairs("SKU2")))].price_outlet is None


def test_live_prices_moved_between_capture_and_push_are_not_pushed():
    env = Env()
    out = env.outlet()
    env.pair(product=out)
    plan = env.preview(ps.MODE_PERCENT, pct())
    calls = {"n": 0}

    def edit(shop):
        calls["n"] += 1
        if calls["n"] == 2:                         # apply's fresh plan 1 (the capture's), the re-derive 2
            shop.products[out.gid].variants[0].price = "95.00"

    env.shop.on("variants", edit)
    rep = env.apply(ps.MODE_PERCENT, pct(), plan)
    o = by_sku(rep)["SKU1"]
    assert o.status == ps.STATUS_VERIFY_FAILED and o.warnings == ("live_prices_moved_since_capture",)
    assert env.shop.count("bulk") == 0


def test_a_refused_push_leaves_the_register_written_and_surfaced():
    env = Env()
    out = env.outlet()
    a = env.pair(product=out)
    env.shop.fault("bulk", "user_error:INVALID")
    rep = env.apply(ps.MODE_PERCENT, pct())
    o = by_sku(rep)["SKU1"]
    assert o.status == ps.STATUS_ERROR and "push_failed:ShopifyUserError" in o.warnings
    assert "register_written" in o.warnings and env.pairs()[a].price_outlet == Decimal("70.00")


# ---------------------------------------------------------------------------
# The lease (rule 6): a decrement-class holder, yielding between SKUs
# ---------------------------------------------------------------------------

def test_pricing_holds_the_lease_yields_between_skus_and_releases_it():
    env = Env()
    env.pair()
    env.pair("SKU2")
    env.apply(ps.MODE_PERCENT, ps.PriceParams(percent_by_sku={"SKU1": 0.3, "SKU2": 0.3}))
    (lease,) = env.leases
    assert lease.takes == 2 and not lease.held
    assert env.register().lease_state().holder is None


def _sent_plus_row(env: Env) -> int:
    """An add whose answer was lost: OPEN, sent (a crash between the send and its outcome)."""
    other = env.shop.product("SKU9 - Outlet", sku="SKU9", sizes=("40",))
    item = other.variants[0].item
    reg = env.register()
    stub = StubLease(reg)
    spec = L.RowSpec(action=L.A_ADJUST, keys=(("SKU9", "40"),), purpose=L.P_ADD, share=1,
                     inventory_item_id=item, location_id=PROMO, product_id=flows.product_number(other.gid))
    (rid,) = reg.write_ahead(stub, [spec.to_insert(actor="t", group_id=7)])
    reg.mark_sent(stub, rid, first=True, sent_available=0, request='{"query": "x", "variables": {}}')
    reg.lease_release(stub)
    return rid


def test_pricing_recovery_is_decrement_class_a_sent_plus_row_is_left_for_the_publish():
    env = Env()
    env.pair()
    row = _sent_plus_row(env)
    rep = env.apply(ps.MODE_PERCENT, pct())
    assert by_sku(rep)["SKU1"].status == ps.STATUS_REGISTER_UPDATED
    (r,) = env.register().reconcile_rows(row_ids=[row])
    assert r.status == OPEN                                   # a publish-class holder's to settle
    assert any(i["kind"] == "left_for_publish" and i["row_id"] == row for i in rep.surfaced)


def test_pricing_runs_before_the_baseline_with_the_held_key_lowering_skipped():
    env = Env(baseline=False)
    env.pair()
    rep = env.apply(ps.MODE_PERCENT, pct())
    assert by_sku(rep)["SKU1"].status == ps.STATUS_REGISTER_UPDATED
    assert any(i["kind"] == "held_key_lowering_skipped" for i in rep.surfaced)


# ---------------------------------------------------------------------------
# Revert: the register back to the values it replaced, then the Shopify priors
# ---------------------------------------------------------------------------

def test_revert_restores_every_pairs_prior_and_the_variants_priors():
    env = Env()
    out = env.outlet(price="90.00", compare_at="100.00")
    a = env.pair(product=out, price_outlet=Decimal("90.00"), discount=Decimal("10.00"))
    b = env.pair(size="43", product=out)                                  # no price yet: NULL is a prior too
    rep = env.apply(ps.MODE_PERCENT, pct())
    assert env.prices(out) == [("70.00", "100.00")] * 2
    rv = env.revert(rep.intent_id)
    assert by_sku(rv)["SKU1"].status == ps.STATUS_REVERTED
    assert (rv.reverted_pairs, rv.reverted_products, rv.reverted_variants) == (2, 1, 2)
    pairs = env.pairs()
    assert (pairs[a].price_outlet, pairs[a].discount) == (Decimal("90.00"), Decimal("10.00"))
    assert (pairs[b].price_outlet, pairs[b].discount) == (None, None)
    assert env.prices(out) == [("90.00", "100.00")] * 2
    restores = env.price_rows("revert_outlet_price")
    assert {r.payload["intent_id"] for r in restores} == {rep.intent_id} and len(restores) == 2


def test_revert_leaves_a_sku_repriced_since_untouched_and_listed():
    env = Env()
    out = env.outlet()
    a = env.pair(product=out)
    first = env.apply(ps.MODE_PERCENT, pct(p=0.3))
    env.apply(ps.MODE_PERCENT, pct(p=0.4))                                # a later pricing
    rv = env.revert(first.intent_id)
    o = by_sku(rv)["SKU1"]
    assert o.status == ps.STATUS_REVERT_SKIPPED and o.warnings == (f"price_changed_since:{a}",)
    assert env.pairs()[a].price_outlet == Decimal("60.00") and env.prices(out) == [("60.00", "100.00")] * 2


def test_revert_twice_is_skipped_the_second_time():
    env = Env()
    env.pair()
    rep = env.apply(ps.MODE_PERCENT, pct())
    assert by_sku(env.revert(rep.intent_id))["SKU1"].status == ps.STATUS_REVERTED
    assert by_sku(env.revert(rep.intent_id))["SKU1"].status == ps.STATUS_REVERT_SKIPPED


def test_revert_never_touches_a_pair_sold_since():
    env = Env()
    a = env.pair()
    rep = env.apply(ps.MODE_PERCENT, pct())
    env.be.consume("SKU1", "42")
    rv = env.revert(rep.intent_id)
    assert by_sku(rv)["SKU1"].status == ps.STATUS_REVERTED and rv.reverted_pairs == 0
    assert env.pairs()[a].price_outlet == Decimal("70.00")


def test_revert_of_a_repair_push_restores_the_captured_live_priors_and_leaves_the_register():
    """Gate-2 set V (V70): a SKU the intent only pushed (a repair push writes no
    register row) is still reverted on Shopify — its captured priors."""
    env = Env()
    out = env.outlet(price="99.00", compare_at="100.00")
    a = env.pair(product=out, price_outlet=Decimal("80.00"), discount=Decimal("20.00"))
    rep = env.apply(ps.MODE_DIRECT, ps.PriceParams())
    assert env.prices(out) == [("80.00", "100.00")] * 2 and env.price_rows("set_outlet_price") == []
    rv = env.revert(rep.intent_id)
    assert by_sku(rv)["SKU1"].status == ps.STATUS_REVERTED
    assert (rv.reverted_pairs, rv.reverted_products, rv.reverted_variants) == (0, 1, 2)
    assert env.prices(out) == [("99.00", "100.00")] * 2 and env.pairs()[a].price_outlet == Decimal("80.00")


def test_revert_of_an_unknown_intent_raises():
    from backend.persistence.audit_sink import IntentNotFound

    with pytest.raises(IntentNotFound):
        Env().revert("0" * 32)


# ---------------------------------------------------------------------------
# Discharge-debt recon, the mode guard
# ---------------------------------------------------------------------------

def test_discharge_debt_counts_active_outlets_failing_fix4():
    env = Env()
    env.outlet(price="90.00", compare_at="100.00")                     # fine
    bad = env.outlet(sku="SKU2", price="100.00", compare_at="100.00")   # price ≥ compare-at
    env.outlet(sku="SKU3", status="DRAFT", price="0.00", compare_at=None)  # DRAFT: not scanned
    rep = ps.discharge_debt_count(env.shop)
    assert (rep.scanned_products, rep.broken_products, rep.broken_variants) == (2, 1, 2)
    assert rep.broken_gids == (bad.gid,)


def test_an_unknown_mode_raises():
    with pytest.raises(ValueError):
        Env().preview("nope", ps.PriceParams())


def test_percent_mode_with_no_compare_at_says_so():
    env = Env()
    env.pair(price_high=None, price=None)
    (d,) = env.preview(ps.MODE_PERCENT, pct()).diffs
    assert d.status == ps.STATUS_MISSING_PRICE and d.warnings[-1] == "price:compare_at_missing"
