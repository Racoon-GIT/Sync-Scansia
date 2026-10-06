"""Deny-normalize = §3.3 rule 7's normalisation (plan §3.5) — end to end on the fake
Shopify and the in-memory register."""
from __future__ import annotations

from typing import List

from backend.persistence.records import DONE, FAILED, OPEN
from backend.services import flows
from backend.services import ledger as L
from backend.services import normalize_service as ns
from backend.services.lease import PassLease

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend

CUSTOM = (("Solo Personalizzazione", "SKU1"),)


class Env:
    def __init__(self) -> None:
        self.be = MemoryBackend()
        self.be.seed_baseline()
        self.shop = FakeShop()
        self.leases: List[PassLease] = []

    def register(self):
        return self.be.register(actor="tester")

    def preview(self, gids=()):
        return ns.normalize_preview(self.register(), self.shop, promo_location_id=PROMO, product_gids=list(gids))

    def apply(self, plan):
        reg = self.register()
        lease = PassLease(reg, "normalize", sleep=lambda s: None)
        self.leases.append(lease)
        return ns.normalize_apply(reg, self.shop, plan, promo_location_id=PROMO, lease=lease, sleep=lambda s: None)

    def rows(self, product):
        return [r for r in self.register().reconcile_rows() if r.product_id == flows.product_number(product.gid)]


def test_preview_finds_continue_untracked_and_customisation_promo_on_any_status():
    env = Env()
    cont = env.shop.product("A - Outlet", sku="SKU1", status="DRAFT", policy="CONTINUE")
    untracked = env.shop.product("B - Outlet", sku="SKU2", tracked=False)
    custom = env.shop.product("C - Outlet", sku="SKU3", extra_variants=CUSTOM)
    env.shop.promo(custom.gid, "Solo Personalizzazione").available = 2
    env.shop.product("D - Outlet", sku="SKU4")                            # already normal
    plan = env.preview()
    got = {p.product_gid: p for p in plan.products}
    assert set(got) == {cont.gid, untracked.gid, custom.gid} and plan.scanned == 4
    assert got[cont.gid].status == "DRAFT" and len(got[cont.gid].unsafe_variant_ids) == 2
    assert got[custom.gid].unsafe_variant_ids == () and got[custom.gid].custom_lowerings[0][2] == 2
    assert got[cont.gid].keys == (("SKU1", "42"), ("SKU1", "43"))


def test_apply_makes_every_variant_deny_and_tracked_and_lowers_a_custom_positive_to_zero():
    env = Env()
    p = env.shop.product("A - Outlet", sku="SKU1", policy="CONTINUE", tracked=False, extra_variants=CUSTOM)
    env.shop.promo(p.gid, "Solo Personalizzazione").available = 1
    rep = env.apply(env.preview())
    (o,) = rep.outcomes
    assert rep.status == ns.ST_APPLIED and o.status == ns.O_NORMALISED
    assert all(v.policy == "DENY" and v.tracked for v in env.shop.products[p.gid].variants)
    assert env.shop.available(p.gid, "Solo Personalizzazione") == 0
    rows = env.rows(p)
    assert sorted(r.action for r in rows) == [L.A_ADJUST, L.A_NORMALISE] and all(r.status == DONE for r in rows)
    (norm,) = [r for r in rows if r.action == L.A_NORMALISE]
    assert norm.keys == (("SKU1", "42"), ("SKU1", "43")) and "price" not in (norm.payload or {})


def test_a_negative_custom_level_is_left_and_only_the_policy_fixed():
    env = Env()
    p = env.shop.product("A - Outlet", sku="SKU1", policy="CONTINUE", extra_variants=CUSTOM)
    env.shop.promo(p.gid, "Solo Personalizzazione").available = -1
    env.apply(env.preview())
    assert env.shop.available(p.gid, "Solo Personalizzazione") == -1 and env.shop.count("adjust") == 0


def test_a_refused_normalisation_on_an_active_outlet_closes_failed_and_drafts_it():
    env = Env()
    p = env.shop.product("A - Outlet", sku="SKU1", policy="CONTINUE")
    env.shop.fault("bulk", "user_error:X")
    rep = env.apply(env.preview())
    assert rep.outcomes[0].status == ns.O_FAILED
    (row,) = env.rows(p)
    assert row.status == FAILED and env.shop.products[p.gid].status == "DRAFT"     # terminal path (i), AB2


def test_an_unknown_answer_leaves_the_normalise_row_open_for_a_publish_class_holder():
    env = Env()
    p = env.shop.product("A - Outlet", sku="SKU1", policy="CONTINUE")
    env.shop.fault("bulk", "timeout_before")
    rep = env.apply(env.preview())
    assert rep.outcomes[0].status == ns.O_UNKNOWN
    (row,) = env.rows(p)
    assert row.status == OPEN and row.sent_at is not None


def test_named_products_only_and_a_non_outlet_is_listed():
    env = Env()
    a = env.shop.product("A - Outlet", sku="SKU1", policy="CONTINUE")
    b = env.shop.product("B - Outlet", sku="SKU2", policy="CONTINUE")
    src = env.shop.product("Full price", sku="SKU3", outlet=False, policy="CONTINUE")
    plan = env.preview([a.gid, src.gid])
    assert [x.product_gid for x in plan.products] == [a.gid] and plan.review == ((src.gid, "not_an_outlet"),)
    env.apply(plan)
    assert env.shop.products[b.gid].variants[0].policy == "CONTINUE"                # not named: untouched


def test_a_product_fixed_by_hand_since_the_preview_is_already_normal():
    env = Env()
    p = env.shop.product("A - Outlet", sku="SKU1", policy="CONTINUE")
    plan = env.preview()
    for v in env.shop.products[p.gid].variants:
        v.policy = "DENY"
    rep = env.apply(plan)
    assert rep.outcomes[0].status == ns.O_ALREADY and env.rows(p) == []


def test_normalize_yields_between_products():
    env = Env()
    for i in range(3):
        env.shop.product(f"{i} - Outlet", sku=f"SKU{i}", policy="CONTINUE")
    env.apply(env.preview())
    assert env.leases[0].takes == 3 and env.register().lease_state().holder is None
