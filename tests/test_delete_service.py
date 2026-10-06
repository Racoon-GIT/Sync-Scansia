"""Deletion on the register (plan §3.5's deletion order, the bulk cleanup; §3.3 rule
2's terminal path (iii)) — end to end: the real ``ops`` documents against the
stateful fake Shopify, the register and its audit rows on the in-memory store.
The predicate and the snapshot's fields: ``tests/test_delete_predicate.py``."""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List

import pytest

from backend.persistence.audit_sink import RegisterAuditSink
from backend.persistence.records import (
    APPLIED,
    CARRIED,
    FAILED,
    HOLD_ROW,
    KIND_AUDIT,
    KIND_DELETE,
    KIND_HOLD,
    OPEN,
)
from backend.services import delete_service as ds
from backend.services import flows
from backend.services import ledger as L
from backend.services.lease import PassLease

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend, seed_reconcile


class Env:
    def __init__(self, *, baseline: bool = True) -> None:
        self.be = MemoryBackend()
        if baseline:
            self.be.seed_baseline()
        self.shop = FakeShop()
        self.leases: List[PassLease] = []

    def register(self):
        return self.be.register(actor="tester")

    def lease(self, reg, kind="delete"):
        lease = PassLease(reg, kind, sleep=lambda s: None)
        self.leases.append(lease)
        return lease

    def outlet(self, sku="SKU1", status="DRAFT", **kw):
        kw.setdefault("sizes", ("42", "43"))
        return self.shop.product(f"{sku} - Outlet", status=status, sku=sku, **kw)

    def pair(self, sku="SKU1", size="42", *, product=None, **kw) -> int:
        if product is not None:
            kw["outlet_product_id"] = flows.product_number(product.gid)
        pid, _e = self.be.add_shelf_pair(sku, size, reconciled=True, **kw)
        return pid

    def single(self, product, *, plan=None, gesture="CONFERMO", sink=None):
        reg = self.register()
        plan = plan or ds.single_delete_preview(reg, self.shop, product.gid)
        return ds.delete_single_apply(reg, self.shop, sink or RegisterAuditSink(reg, actor="tester"), plan,
                                      human_gesture=gesture, promo_location_id=PROMO, lease=self.lease(reg),
                                      sleep=lambda s: None)

    def cleanup(self, *, plan=None, gesture=None, second_confirm=False, sink=None, threshold=25, archive_first=False):
        reg = self.register()
        plan = plan or ds.cleanup_preview(reg, self.shop, promo_location_id=PROMO, threshold=threshold,
                                          archive_first=archive_first)
        return ds.cleanup_apply(reg, self.shop, sink or RegisterAuditSink(reg, actor="tester"), plan,
                                human_gesture=gesture if gesture is not None else "CONFERMO",
                                promo_location_id=PROMO, second_confirm=second_confirm,
                                lease=self.lease(reg, "cleanup"), sleep=lambda s: None)

    def audit(self, kind):
        return self.register().audit_rows(kind)

    def pairs(self) -> Dict[int, Any]:
        return {p.pair_id: p for p in self.register().pairs()}


def num(product) -> int:
    return flows.product_number(product.gid)


# ---------------------------------------------------------------------------
# The single delete: step (0)'s preview and hold, steps (1)–(4)
# ---------------------------------------------------------------------------

def test_single_delete_preview_lists_the_skus_shelf_pairs_and_the_keys_it_will_hold():
    env = Env()
    out = env.outlet(status="ACTIVE", extra_variants=(("Solo Personalizzazione", "SKU1"),))
    a = env.pair("SKU1", "42", product=out)
    b = env.pair("SKU1", "44")
    env.pair("SKU2", "42")
    plan = ds.single_delete_preview(env.register(), env.shop, out.gid)
    assert plan.skus == ("SKU1",) and plan.shelf_pairs == (a, b)
    assert plan.keys == (("SKU1", "42"), ("SKU1", "44")) and plan.status == "ACTIVE"
    assert env.shop.count("delete") == 0 and env.audit(KIND_HOLD) == []


def test_single_delete_refuses_a_target_that_is_not_an_outlet():
    env = Env()
    source = env.shop.product("Full price", outlet=False)
    with pytest.raises(ds.SingleDeleteNotOutletError):
        ds.single_delete_preview(env.register(), env.shop, source.gid)


def test_single_delete_holds_clears_snapshots_deletes_then_offlines_in_that_order():
    env = Env()
    out = env.outlet(status="ACTIVE", promo={"42": 1})
    sold = env.be.add_pair("SKU1", "42", state="VENDUTO", outlet_product_id=num(out))
    on_shelf = env.pair("SKU1", "42", product=out, online=True)
    seen: Dict[str, Any] = {}

    def at_delete(shop):
        reg = env.register()
        seen["hold"] = [h for h in reg.held_keys() if h.kind == HOLD_ROW]
        seen["delete_rows"] = reg.audit_rows(KIND_DELETE)
        seen["ids"] = {p.pair_id: p.outlet_product_id for p in reg.pairs()}

    env.shop.on("delete", at_delete)
    rep = env.single(out)
    assert rep.status == ds.STATUS_DELETED and out.gid not in env.shop.products
    (hold,) = seen["hold"]                                      # (0) before the delete
    assert hold.key == ("SKU1", "42") and hold.reason == ds.REASON_OUTLET_DELETED and hold.event_ids == frozenset()
    assert seen["ids"] == {sold: None, on_shelf: None}          # (1): any state, before the delete
    (snap,) = seen["delete_rows"]                               # (2): the gate, before the delete
    assert snap.product_id == num(out) and snap.payload["title"] == "SKU1 - Outlet"
    assert env.pairs()[on_shelf].online is False                # (4)
    assert rep.held_keys == (("SKU1", "42"),)
    assert any(r.action == "product_delete" for r in env.audit(KIND_AUDIT))


def test_a_deleted_outlets_key_takes_no_add_until_a_relevel():
    from backend.services import outlet_service

    env = Env()
    out = env.outlet(status="ACTIVE", promo={"42": 1})
    env.pair("SKU1", "42", product=out)
    env.single(out)
    env.be.add_shelf_pair("SKU1", "42")                          # a new return: +1 pending
    plan = outlet_service.publish_preview(env.register(), env.shop, promo_location_id=PROMO)
    assert plan.actions == () and "held_positive:SKU1:42:1" in plan.anomalies


def test_single_delete_aborts_at_a_failed_durable_snapshot_and_never_deletes():
    class Boom(RegisterAuditSink):
        def write_durable(self, snapshot):  # noqa: ANN001
            raise RuntimeError("audit down")

    env = Env()
    out = env.outlet(status="ACTIVE")
    env.pair("SKU1", "42", product=out)
    reg = env.register()
    rep = env.single(out, sink=Boom(reg, actor="tester"))
    assert rep.status == ds.STATUS_SNAPSHOT_ABORTED and out.gid in env.shop.products
    assert env.shop.count("delete") == 0


def test_a_snapshot_that_cannot_be_read_writes_nothing_not_even_the_hold():
    env = Env()
    out = env.outlet(status="ACTIVE")
    env.pair("SKU1", "42", product=out)
    plan = ds.single_delete_preview(env.register(), env.shop, out.gid)
    env.shop.fault("metafields", "refuse")
    rep = env.single(out, plan=plan)
    assert rep.status == ds.STATUS_SNAPSHOT_BUILD_FAILED
    assert env.audit(KIND_HOLD) == [] and env.pairs()[next(iter(env.pairs()))].outlet_product_id == num(out)


def test_single_delete_is_verify_failed_when_a_pair_arrived_since_the_preview():
    env = Env()
    out = env.outlet(status="ACTIVE")
    env.pair("SKU1", "42", product=out)
    plan = ds.single_delete_preview(env.register(), env.shop, out.gid)
    env.pair("SKU1", "43")
    rep = env.single(out, plan=plan)
    assert rep.status == ds.STATUS_VERIFY_FAILED and out.gid in env.shop.products and env.audit(KIND_HOLD) == []


def test_single_delete_needs_its_gesture():
    env = Env()
    out = env.outlet()
    with pytest.raises(ds.DeleteConfirmationError):
        env.single(out, gesture="2")


def test_a_refused_delete_of_a_product_still_there_closes_no_row():
    env = Env()
    out = env.outlet(status="ACTIVE", promo={"42": 1})
    reg = env.register()
    item = out.variants[0].item
    row = seed_reconcile(reg, action=L.A_ADJUST, keys=(("SKU1", "42"),), purpose=L.P_ADD, share=1,
                         inventory_item_id=item, location_id=PROMO, product_id=num(out), sent=True)
    env.shop.fault("delete", "user_error:X")
    rep = env.single(out)
    assert rep.status == ds.STATUS_DELETE_FAILED and out.gid in env.shop.products
    assert env.register().reconcile_rows(row_ids=[row])[0].status == OPEN


def test_an_unknown_delete_answer_with_the_product_gone_is_a_delete():
    env = Env()
    out = env.outlet(status="ACTIVE")
    env.shop.fault("delete", "lose_after")
    rep = env.single(out)
    assert rep.status == ds.STATUS_DELETED and out.gid not in env.shop.products
    assert any(w.startswith("product_reads_gone_after:") for w in rep.warnings)


# ---------------------------------------------------------------------------
# Terminal path (iii): the product's rows not closed, closed by its delete
# ---------------------------------------------------------------------------

def test_terminal_path_iii_closes_a_products_open_rows_by_kind_and_settles_their_groups():
    """``close_product_rows`` itself (a delete runs its holder's recovery first,
    which would act on these rows before the delete: the kinds are tested here)."""
    from backend.services import passes

    env = Env()
    out = env.outlet(status="ACTIVE", promo={"42": 1, "43": 1})
    other = env.outlet(sku="SKU1", status="ACTIVE", sizes=("44",), promo={"44": 1})
    reg = env.register()
    i42, i43 = out.variants[0].item, out.variants[1].item
    applied = seed_reconcile(reg, action=L.A_ADJUST, keys=(("SKU1", "42"),), event_ids=(5,), purpose=L.P_LOWER,
                             share=-1, inventory_item_id=i42, location_id=PROMO, product_id=num(out), sent=True,
                             status=APPLIED, outcome={"moved": -1}, group_id=11)
    sent_minus = seed_reconcile(reg, action=L.A_ADJUST, keys=(("SKU1", "43"),), event_ids=(6,), purpose=L.P_LOWER,
                                share=-1, inventory_item_id=i43, location_id=PROMO, product_id=num(out), sent=True,
                                group_id=12)
    never_plus = seed_reconcile(reg, action=L.A_ADJUST, keys=(("SKU1", "42"),), purpose=L.P_ADD, share=1,
                                inventory_item_id=i42, location_id=PROMO, product_id=num(out), group_id=13)
    sibling = seed_reconcile(reg, action=L.A_ADJUST, keys=(("SKU1", "44"),), event_ids=(6,), purpose=L.P_LOWER,
                             share=-1, inventory_item_id=other.variants[0].item, location_id=PROMO,
                             product_id=num(other), sent=True, status=APPLIED, outcome={"moved": -1}, group_id=12)
    run = passes.Holder(reg, env.shop, PROMO, "delete", lease=env.lease(reg), sleep=lambda s: None)
    run.lease.take()
    try:
        closed = ds.close_product_rows(run.ctx, num(out), 99)
    finally:
        run.release()
    assert set(closed) == {applied, sent_minus, never_plus}
    rows = {r.row_id: r for r in env.register().reconcile_rows()}
    assert rows[applied].status == CARRIED and rows[applied].outcome["moved"] == -1      # netted (Groups)
    assert rows[sent_minus].status == FAILED and rows[sent_minus].outcome["possibly_applied"] is True
    assert rows[never_plus].status == FAILED and "possibly_applied" not in rows[never_plus].outcome
    for r in (applied, sent_minus, never_plus):
        assert rows[r].outcome["reason"] == "product_deleted" and rows[r].outcome["delete_row"] == 99
    assert rows[sibling].status == CARRIED                       # its group settled beside a FAILED row
    assert any(i["kind"] == "rows_closed_product_deleted" for i in run.report.items)


def test_a_delete_closes_a_sent_add_its_recovery_left_open_and_names_its_delete_row():
    env = Env()
    out = env.outlet(status="ACTIVE", promo={"42": 1})
    reg = env.register()
    row = seed_reconcile(reg, action=L.A_ADJUST, keys=(("SKU1", "42"),), purpose=L.P_ADD, share=1,
                         inventory_item_id=out.variants[0].item, location_id=PROMO, product_id=num(out), sent=True)
    rep = env.single(out)
    assert rep.status == ds.STATUS_DELETED and rep.closed_rows == (row,)
    (r,) = env.register().reconcile_rows(row_ids=[row])
    (delete_row,) = env.audit(KIND_DELETE)
    assert r.status == FAILED and r.outcome["delete_row"] == delete_row.row_id    # its units left with the product
    assert any(i["kind"] == "left_for_publish" for i in rep.surfaced)            # recovery left it first


# ---------------------------------------------------------------------------
# The bulk cleanup
# ---------------------------------------------------------------------------

def test_cleanup_lists_an_outlet_whose_sku_has_a_shelf_pair_apart_and_never_deletes_it():
    env = Env()
    keep = env.outlet(sku="SKU1")                                # DRAFT at 0, but a shelf pair: protected
    gone = env.outlet(sku="SKU2")
    env.pair("SKU1", "42")
    plan = ds.cleanup_preview(env.register(), env.shop, promo_location_id=PROMO)
    assert [c.product_gid for c in plan.candidates] == [gone.gid] and plan.count == 1
    assert [(p.product_gid, p.reasons) for p in plan.protected] == [(keep.gid, ("shelf_pairs:SKU1",))]
    rep = env.cleanup(plan=plan, gesture="1")
    assert rep.deleted == 1 and keep.gid in env.shop.products and gone.gid not in env.shop.products


def test_cleanup_rechecks_each_product_right_before_its_delete():
    env = Env()
    a = env.outlet(sku="SKU1")
    b = env.outlet(sku="SKU2")
    plan = ds.cleanup_preview(env.register(), env.shop, promo_location_id=PROMO)
    calls = {"n": 0}

    def a_return_lands(shop):
        calls["n"] += 1
        if calls["n"] == 1:                                      # the first product's own re-read
            env.be.add_shelf_pair("SKU1" if plan.candidates[0].product_gid == a.gid else "SKU2", "42")

    env.shop.on("core", a_return_lands)
    rep = env.cleanup(plan=plan, gesture="2")
    statuses = {o.product_gid: o.status for o in rep.outcomes}
    first, second = plan.candidates[0].product_gid, plan.candidates[1].product_gid
    assert statuses == {first: ds.STATUS_SKIPPED, second: ds.STATUS_DELETED}
    assert {o.product_gid: o.warnings for o in rep.outcomes}[first] == ("shelf_pairs",)
    assert first in env.shop.products and b.gid in (first, second)


def test_cleanup_clears_the_id_on_sold_pairs_and_writes_a_delete_row_per_product():
    env = Env()
    out = env.outlet(sku="SKU1")
    sold = env.be.add_pair("SKU1", "42", state="VENDUTO", outlet_product_id=num(out))
    rep = env.cleanup(gesture="1")
    assert rep.deleted == 1 and env.pairs()[sold].outlet_product_id is None
    assert [r.product_id for r in env.audit(KIND_DELETE)] == [num(out)]


def test_cleanup_aborts_the_whole_batch_on_drift():
    env = Env()
    out = env.outlet(sku="SKU1")
    plan = ds.cleanup_preview(env.register(), env.shop, promo_location_id=PROMO)
    env.shop.promo(out.gid, "42").available = 2                  # gained stock: no longer a candidate
    rep = env.cleanup(plan=plan, gesture="1")
    assert rep.verify_failed and rep.deleted == 0 and env.shop.count("delete") == 0


def test_cleanup_gestures_count_word_and_the_capped_second_confirm():
    env = Env()
    for i in range(3):
        env.outlet(sku=f"SKU{i}")
    plan = ds.cleanup_preview(env.register(), env.shop, promo_location_id=PROMO, threshold=2)
    assert plan.requires_second_confirm
    with pytest.raises(ds.DeleteConfirmationError):
        env.cleanup(plan=plan, gesture="2")                      # wrong count
    with pytest.raises(ds.DeleteConfirmationError):
        env.cleanup(plan=plan, gesture="CONFERMO")               # over the threshold, no second confirm
    capped = ds.cleanup_preview(env.register(), env.shop, promo_location_id=PROMO, threshold=10_000)
    assert capped.requires_second_confirm is False               # 3 ≤ min(10 000, hard cap 50)
    rep = env.cleanup(plan=plan, gesture="3", second_confirm=True)
    assert rep.deleted == 3 and env.shop.count("delete") == 3


def test_cleanup_archive_first_stages_and_closes_no_row():
    env = Env()
    out = env.outlet(sku="SKU1")
    rep = env.cleanup(gesture="1", archive_first=True)
    assert [o.status for o in rep.outcomes] == [ds.STATUS_ARCHIVED]
    assert env.shop.products[out.gid].status == "ARCHIVED" and env.shop.count("delete") == 0


def test_cleanup_yields_between_products_and_releases_the_lease():
    env = Env()
    for i in range(3):
        env.outlet(sku=f"SKU{i}")
    env.cleanup(gesture="3")
    (lease,) = env.leases
    assert lease.takes == 3 and not lease.held and env.register().lease_state().holder is None


def test_cleanup_runs_before_the_baseline():
    env = Env(baseline=False)
    env.outlet(sku="SKU1")
    rep = env.cleanup(gesture="1")
    assert rep.deleted == 1 and any(i["kind"] == "held_key_lowering_skipped" for i in rep.surfaced)


def test_product_skus_skip_customisation_only_and_blank_skus():
    inv = [{"sku": "A", "title": "42", "selectedOptions": [{"name": "Size", "value": "42"}]},
           {"sku": " ", "title": "43", "selectedOptions": [{"name": "Size", "value": "43"}]},
           {"sku": "B", "title": "Solo Personalizzazione",
            "selectedOptions": [{"name": "Size", "value": "Solo Personalizzazione"}]}]
    assert ds.product_skus(inv) == ("A",)


def test_sold_pairs_and_prices_are_never_touched_by_a_delete():
    env = Env()
    out = env.outlet(status="ACTIVE")
    sold = env.be.add_pair("SKU1", "42", state="VENDUTO", price_outlet=Decimal("50.00"))
    env.single(out)
    p = env.pairs()[sold]
    assert p.state == "VENDUTO" and p.price_outlet == Decimal("50.00")


def test_one_products_shopify_failure_never_aborts_the_cleanup_batch():
    env = Env()
    a = env.outlet(sku="SKU1")
    b = env.outlet(sku="SKU2")
    plan = ds.cleanup_preview(env.register(), env.shop, promo_location_id=PROMO)
    first = plan.candidates[0].product_gid
    env.shop.fault("core", "refuse", when=lambda v: v["id"] == first)
    rep = env.cleanup(plan=plan, gesture="2")
    statuses = {o.product_gid: o.status for o in rep.outcomes}
    assert statuses[first] == ds.STATUS_ERROR and rep.deleted == 1
    assert {a.gid, b.gid} - {first} <= set(statuses)
