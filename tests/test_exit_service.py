"""The multi-outlet exit (plan §3.5, §8) — end to end on the fake Shopify and the
in-memory register."""
from __future__ import annotations

from typing import List

from backend.persistence.audit_sink import RegisterAuditSink
from backend.persistence.records import DONE, KIND_DELETE, KIND_HOLD
from backend.services import delete_service as ds
from backend.services import exit_service as xs
from backend.services import flows
from backend.services import ledger as L
from backend.services.lease import PassLease

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend


class Env:
    def __init__(self) -> None:
        self.be = MemoryBackend()
        self.be.seed_baseline()
        self.shop = FakeShop()
        self.leases: List[PassLease] = []

    def register(self):
        return self.be.register(actor="tester")

    def preview(self, sku, keep):
        return xs.exit_preview(self.register(), self.shop, sku, keep.gid if hasattr(keep, "gid") else keep,
                               promo_location_id=PROMO)

    def apply(self, plan):
        reg = self.register()
        lease = PassLease(reg, "exit", sleep=lambda s: None)
        self.leases.append(lease)
        return xs.exit_apply(reg, self.shop, RegisterAuditSink(reg, actor="tester"), plan, promo_location_id=PROMO,
                             lease=lease, sleep=lambda s: None)


def two_outlets(env: Env, **other_kw):
    keep = env.shop.product("SKU1 - Outlet", sku="SKU1", promo={"42": 1, "43": 0})
    other_kw.setdefault("promo", {"42": 2, "43": 1})
    other = env.shop.product("SKU1 - Outlet bis", sku="SKU1",
                             extra_variants=(("Solo Personalizzazione", "SKU1"),), **other_kw)
    return keep, other


def test_preview_lists_the_other_outlets_lowerings_and_what_the_predicate_would_refuse():
    env = Env()
    keep, other = two_outlets(env)
    env.shop.promo(other.gid, "Solo Personalizzazione").available = 1
    pid, _ = env.be.add_shelf_pair("SKU1", "42", reconciled=True, online=True)
    plan = env.preview("SKU1", keep)
    assert plan.refused is None and plan.keys == (("SKU1", "42"),) and plan.keep_gid == keep.gid
    (o,) = plan.outlets
    assert o.product_gid == other.gid and o.refusals == ()
    assert sorted((x.title, x.available, x.customisation_only) for x in o.lowerings) == [
        ("42", 2, False), ("43", 1, False), ("Solo Personalizzazione", 1, True)]


def test_preview_refuses_a_single_outlet_sku_and_a_keep_that_is_not_its_outlet():
    env = Env()
    keep = env.shop.product("SKU1 - Outlet", sku="SKU1")
    assert env.preview("SKU1", keep).refused == xs.R_NOT_MULTI
    env.shop.product("SKU1 - Outlet bis", sku="SKU1")
    assert env.preview("SKU1", "gid://shopify/Product/999").refused == xs.R_KEEP
    assert env.preview(" ", keep).refused == xs.R_BLANK_SKU


def test_apply_lowers_every_positive_on_the_other_outlet_then_deletes_it_keep_untouched():
    env = Env()
    keep, other = two_outlets(env)
    env.shop.promo(other.gid, "Solo Personalizzazione").available = 1
    pid, _ = env.be.add_shelf_pair("SKU1", "42", reconciled=True, online=True)
    plan = env.preview("SKU1", keep)
    seen = {}
    env.shop.on("delete", lambda shop: seen.update(levels={t: shop.available(other.gid, t)
                                                          for t in ("42", "43", "Solo Personalizzazione")}))
    rep = env.apply(plan)
    assert rep.status == xs.ST_APPLIED and [o.status for o in rep.outcomes] == [ds.STATUS_DELETED]
    assert seen["levels"] == {"42": 0, "43": 0, "Solo Personalizzazione": 0}      # lowered before the delete
    assert other.gid not in env.shop.products and keep.gid in env.shop.products
    assert env.shop.available(keep.gid, "42") == 1                               # the kept outlet untouched
    rows = [r for r in env.register().reconcile_rows() if r.product_id == flows.product_number(other.gid)]
    assert sorted(L.purpose(r) for r in rows) == [L.P_CUSTOM, L.P_EXIT, L.P_EXIT]
    assert all(r.status == DONE for r in rows)
    assert [r.keys for r in rows if L.purpose(r) == L.P_CUSTOM] == [()]
    assert env.register().audit_rows(KIND_HOLD) == []                            # step (0): the existing hold
    (p,) = env.register().pairs(pair_ids=[pid])
    assert p.online is True                                                      # step (4) skipped
    assert rep.keys_to_relevel == (("SKU1", "42"),)
    assert [r.product_id for r in env.register().audit_rows(KIND_DELETE)] == [flows.product_number(other.gid)]


def test_a_committed_unit_stops_the_exit_before_the_delete_listed():
    env = Env()
    keep, other = two_outlets(env, committed={"43": 1})
    plan = env.preview("SKU1", keep)
    assert plan.outlets[0].refusals == ("promo_committed",)
    rep = env.apply(plan)
    (o,) = rep.outcomes
    assert o.status == xs.O_STOPPED and o.warnings == ("promo_committed",)
    assert other.gid in env.shop.products and env.shop.count("delete") == 0
    assert env.shop.available(other.gid, "42") == 0                             # its positives were lowered


def test_stock_at_another_location_or_a_continue_variant_stops_it_too():
    env = Env()
    keep, other = two_outlets(env, mag={"42": 3})
    rep = env.apply(env.preview("SKU1", keep))
    assert rep.outcomes[0].status == xs.O_STOPPED and rep.outcomes[0].warnings == ("non_promo_stock",)
    env2 = Env()
    keep2, other2 = two_outlets(env2, policy="CONTINUE")
    rep2 = env2.apply(env2.preview("SKU1", keep2))
    assert rep2.outcomes[0].warnings == ("continue_policy",) and other2.gid in env2.shop.products


def test_a_third_outlet_appearing_after_the_preview_is_verify_failed():
    env = Env()
    keep, other = two_outlets(env)
    plan = env.preview("SKU1", keep)
    env.shop.product("SKU1 - Outlet ter", sku="SKU1")
    rep = env.apply(plan)
    assert rep.status == xs.ST_VERIFY_FAILED and env.shop.count("adjust") == 0 and env.shop.count("delete") == 0


def test_a_refused_plan_writes_nothing():
    env = Env()
    keep = env.shop.product("SKU1 - Outlet", sku="SKU1")
    rep = env.apply(env.preview("SKU1", keep))
    assert rep.status == xs.ST_REFUSED and rep.reason == xs.R_NOT_MULTI and env.leases[0].takes == 0
    assert env.register().reconcile_rows() == [] and env.shop.count("adjust") == 0


def test_the_exit_yields_between_outlets():
    env = Env()
    keep, other = two_outlets(env)
    env.shop.product("SKU1 - Outlet ter", sku="SKU1", promo={"42": 1})
    rep = env.apply(env.preview("SKU1", keep))
    assert [o.status for o in rep.outcomes] == [ds.STATUS_DELETED, ds.STATUS_DELETED]
    assert env.leases[0].takes == 2 and env.register().lease_state().holder is None
