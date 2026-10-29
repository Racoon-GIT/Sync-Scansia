"""The per-key re-level (plan §3.4) — end to end on the in-memory register and the
stateful fake Shopify, after a real baseline: what releases a held key (or levels
one not held), what it refuses, and §3.4's invariant on every key afterwards."""
from __future__ import annotations

from typing import List, Sequence, Tuple

import pytest

from backend.persistence.records import (
    HELD,
    KIND_BASELINE,
    KIND_HOLD,
    KIND_RECONCILE,
    OPEN,
    SCOPE_KEY,
    InsertAudit,
)
from backend.services import baseline_service as B
from backend.services import ledger as L
from backend.services import relevel_service as R
from backend.services import variants
from backend.services.lease import PassLease

from tests.fake_shop import PROMO
from tests.test_baseline import MUTATIONS, Env


def relevel(env: Env, keys: Sequence[Tuple[str, str]], *, dlq="default", **kw):
    plan = R.relevel_preview(env.register(), env.shop, keys, promo_location_id=PROMO)
    reg = env.register()
    lease = kw.pop("lease", None) or PassLease(reg, "relevel", sleep=lambda s: None)
    rep = R.relevel_apply(reg, env.shop, plan, promo_location_id=PROMO,
                          dlq=env.dlq(plan) if dlq == "default" else dlq, lease=lease, sleep=lambda s: None, **kw)
    return plan, rep


def result(rep: R.RelevelReport, sku: str, size: str) -> R.RelevelOutcome:
    return next(o for o in rep.outcomes if (o.sku, o.size) == (sku, size))


def held_keys(env: Env) -> List[Tuple[str, str]]:
    return sorted({h.key for h in env.register().held_keys()})


# ---------------------------------------------------------------------------
# What releases: ACTIVE to N, zero-then-revive, CREATE, N 0
# ---------------------------------------------------------------------------

def test_a_size_absent_hold_is_released_once_the_variant_exists_active_to_n_stamping_every_event():
    env = Env()
    out = env.shop.product("Nike - Outlet", sizes=("43",), promo={"43": 0})
    _, e1 = env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    assert env.held() == {("SKU1", "42"): B.R_SIZE_ABSENT}
    _, e2 = env.be.add_shelf_pair("SKU1", "42", price_outlet="50.00", price_high="100.00")   # post-hold
    env.shop.products[out.gid].variants.append(type(env.shop.products[out.gid].variants[0])(
        "gid://shopify/ProductVariant/9001", "SKU1", "42", "gid://shopify/InventoryItem/9001"))  # a person adds 42
    plan, rep = relevel(env, [("SKU1", "42")])
    kp = plan.keys[0]
    assert (kp.branch, kp.n, kp.event_ids, kp.activate) == (R.BR_ACTIVE, 2, (e1, e2), True)
    assert result(rep, "SKU1", "42").result == "released"
    assert env.shop.available(out.gid, "42") == 2 and env.unstamped() == []
    assert held_keys(env) == [] and env.shop.products[out.gid].published
    base = env.register().audit_rows(KIND_BASELINE)[-1]
    assert base.payload["scope"] == SCOPE_KEY and base.payload["reason"] == "relevel"
    assert {p.online for p in env.register().shelf("SKU1")} == {True}
    assert env.invariant() == {}


def test_a_draft_parked_key_not_held_is_levelled_to_n_never_n_plus_pending():
    env = Env()
    out = env.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 1})
    env.be.add_shelf_pair("SKU1", "42", reconciled=True, price_outlet="50.00", price_high="100.00")  # nothing pending
    env.run()                                             # parked at N: the invariant holds, nothing written
    assert env.shop.available(out.gid, "42") == 1 and held_keys(env) == []
    plan, rep = relevel(env, [("SKU1", "42")])
    assert plan.keys[0].branch == R.BR_REVIVE and result(rep, "SKU1", "42").result == "released"
    assert env.shop.available(out.gid, "42") == 1                        # zero-then-revive: N, not N + 1
    assert env.shop.products[out.gid].status == "ACTIVE"
    assert env.invariant() == {}


def test_a_key_with_no_outlet_is_created_with_n():
    env = Env()
    src = env.shop.product("Nike Air", outlet=False, sizes=("42", "43"), mag={"42": 3})
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    _, rep = relevel(env, [("SKU1", "42")])
    assert rep.status == R.ST_BASELINE_MISSING
    env.run()
    plan, rep = relevel(env, [("SKU1", "42")])
    assert plan.keys[0].branch == R.BR_CREATE and result(rep, "SKU1", "42").result == "released"
    outlets = [p for p in env.shop.products.values() if p.gid != src.gid]
    assert len(outlets) == 1 and env.shop.available(outlets[0].gid, "42") == 2
    assert env.unstamped() == [] and env.invariant() == {}


def test_n_zero_lowers_every_outlet_and_stamps():
    env = Env()
    a = env.shop.product("Nike - Outlet A", promo={"42": 1})
    d = env.shop.product("Nike - Outlet B", status="DRAFT", promo={"42": 1})
    env.departed(size="42")
    env.run()
    assert ("SKU1", "42") in held_keys(env)               # multi-outlet
    plan, rep = relevel(env, [("SKU1", "42")])
    assert plan.keys[0].branch == R.BR_LOWER and result(rep, "SKU1", "42").result == "released"
    assert env.shop.available(a.gid, "42") == 0 and env.shop.available(d.gid, "42") == 0
    assert env.unstamped() == [] and held_keys(env) == []


def test_a_held_rows_events_are_stamped_so_a_landed_plus_is_never_re_added():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0})
    env.run()
    _, e = env.be.add_shelf_pair("SKU1", "42", price_outlet="50.00", price_high="100.00")
    env.shop.levels[(env.shop.variant(out.gid, "42").item, PROMO)].available = 1   # the +1 landed...
    env.shop.levels[(env.shop.variant(out.gid, "42").item, PROMO)].on_hand = 1
    env.be.seed_audit(InsertAudit(kind=KIND_RECONCILE, actor="t", action=L.A_ADJUST, status=HELD,
                                  payload={"keys": [["SKU1", "42"]], "event_ids": [e], "purpose": "add",
                                           "share": 1}))                        # ...its answer lost: HELD
    assert held_keys(env) == [("SKU1", "42")]
    plan, rep = relevel(env, [("SKU1", "42")])
    assert plan.keys[0].event_ids == (e,) and plan.keys[0].delta == 0
    assert result(rep, "SKU1", "42").result == "released"
    assert env.shop.available(out.gid, "42") == 1 and env.unstamped() == [] and held_keys(env) == []


# ---------------------------------------------------------------------------
# What it refuses (the key left as it is)
# ---------------------------------------------------------------------------

def test_refusals_leave_the_key_as_it_is():
    env = Env()
    env.shop.product("M - Outlet 1", sku="M", promo={"42": 0})
    env.shop.product("M - Outlet 2", sku="M", promo={"42": 0})
    env.shop.product("P - Outlet", sku="P", promo={"42": 0})                    # no price on P's pairs
    env.shop.product("C - Outlet", sku="C", promo={"42": 0}, committed={"42": 1})
    env.imported(sku="M", size="42", price_outlet="50.00", price_high="100.00")
    env.imported(sku="P", size="42")
    env.departed(sku="C", size="42")
    env.run()
    before = env.shop.count("adjust")
    plan, rep = relevel(env, [("M", "42"), ("P", "42"), ("C", "42")])
    reasons = {(o.sku, o.size): o.reason for o in rep.outcomes}
    assert reasons == {("M", "42"): R.F_MULTI, ("P", "42"): "fix4:price_missing", ("C", "42"): B.R_UNIT_HELD}
    assert {o.result for o in rep.outcomes} == {"refused"} and env.shop.count("adjust") == before


def test_a_key_with_a_row_not_closed_is_refused():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    env.be.seed_audit(InsertAudit(kind=KIND_RECONCILE, actor="t", action=L.A_ADJUST, status=OPEN,
                                  payload={"keys": [["SKU1", "42"]], "event_ids": [], "purpose": "zero",
                                           "target": 0}))
    plan, _ = relevel(env, [("SKU1", "42")])
    assert plan.keys[0].reason == R.F_IN_FLIGHT


def test_a_key_that_moved_since_the_preview_is_held_nothing_written_and_lowered_by_its_excess():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 1})
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    assert held_keys(env) == []
    plan = R.relevel_preview(env.register(), env.shop, [("SKU1", "42")], promo_location_id=PROMO)
    lvl = env.shop.levels[(env.shop.variant(out.gid, "42").item, PROMO)]
    lvl.available, lvl.on_hand = 3, 3                                           # a hand edit
    reg = env.register()
    rep = R.relevel_apply(reg, env.shop, plan, promo_location_id=PROMO, dlq=env.dlq(plan),
                          lease=PassLease(reg, "relevel", sleep=lambda s: None), sleep=lambda s: None)
    assert result(rep, "SKU1", "42").result == "held"
    assert [r.payload["reason"] for r in env.register().audit_rows(KIND_HOLD)] == [R.R_MOVED]
    assert env.shop.available(out.gid, "42") == 1                             # lowered, never set
    assert env.invariant() == {}


def test_the_precondition_and_the_baseline_gate_refuse_before_any_write():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    _, rep = relevel(env, [("SKU1", "42")])
    assert rep.status == R.ST_BASELINE_MISSING
    env.run()
    plan = R.relevel_preview(env.register(), env.shop, [("SKU1", "42")], promo_location_id=PROMO)
    reg = env.register()
    rep = R.relevel_apply(reg, env.shop, plan, promo_location_id=PROMO, dlq=env.dlq(plan, count=1),
                          lease=PassLease(reg, "relevel", sleep=lambda s: None), sleep=lambda s: None)
    assert (rep.status, rep.reason) == (R.ST_PRECONDITION_REFUSED, "dlq_unresolved_entries")


def test_a_run_of_keys_yields_the_lease_between_skus():
    env = Env()
    for sku in ("A", "B", "C"):
        env.shop.product(f"{sku} - Outlet", sku=sku, sizes=("43",), promo={"43": 0})
        env.imported(sku=sku, size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    takes: List[str] = []
    real = env.be.db.connect

    def spy():
        store = real()
        orig = store.lease_take
        store.lease_take = lambda h, t: (takes.append(h), orig(h, t))[1]
        return store

    env.be.db.connect = spy
    _, rep = relevel(env, [("A", "42"), ("B", "42"), ("C", "42")])
    assert len(takes) == 3 and {o.reason for o in rep.outcomes} == {R.F_SIZE}


def test_a_refused_publish_re_run_is_surfaced_and_the_level_still_releases_the_key():
    env = Env()
    out = env.shop.product("Nike - Outlet", sizes=("43",), promo={"43": 0})
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    env.shop.products[out.gid].variants.append(type(env.shop.products[out.gid].variants[0])(
        "gid://shopify/ProductVariant/9001", "SKU1", "42", "gid://shopify/InventoryItem/9001"))
    env.shop.fault("publish", "user_error:INVALID")
    _, rep = relevel(env, [("SKU1", "42")])
    assert result(rep, "SKU1", "42").result == "released" and env.shop.available(out.gid, "42") == 1
    assert "relevel_publish_not_applied" in [i["kind"] for i in rep.surfaced]
    assert not env.shop.products[out.gid].published               # S13 alarms it; a re-level re-runs it


# ---------------------------------------------------------------------------
# F1 (F7.4 baseline review): a re-level never raises a variant of another SKU
# ---------------------------------------------------------------------------
def _f1_outlet(env: Env, *, status="ACTIVE", promo=None):
    return env.shop.product("Nike - Outlet", status=status, sizes=("43",), promo=promo or {"43": 0, "42": 0},
                            extra_variants=(("42", "OTHER"),))


def test_f1_an_active_relevel_up_is_refused_until_the_variant_is_respelled():
    env = Env()
    out = _f1_outlet(env)
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    assert held_keys(env) == [("SKU1", "42")]
    before = len(env.shop.calls)
    plan, rep = relevel(env, [("SKU1", "42")])
    assert (plan.keys[0].branch, plan.keys[0].reason) == (R.BR_REFUSE, f"{R.F_SKU}:OTHER")
    assert result(rep, "SKU1", "42").result == "refused"
    assert not [c for c in env.shop.calls[before:] if c[0] in MUTATIONS]
    assert env.shop.available(out.gid, "42") == 0 and held_keys(env) == [("SKU1", "42")]
    env.shop.variant(out.gid, "42").sku = "SKU1"                               # a person respells the variant
    _, rep = relevel(env, [("SKU1", "42")])
    assert result(rep, "SKU1", "42").result == "released" and env.shop.available(out.gid, "42") == 1


@pytest.mark.parametrize("promo", [1, 2])
def test_f1_an_active_relevel_at_or_above_n_on_a_variant_of_another_sku_is_refused_too(promo):
    """Not the key's variant: a re-level never releases the key onto it (nor stamps,
    nor sets ONLINE), whatever the direction — the hold's own lowering takes the excess."""
    env = Env()
    out = _f1_outlet(env, promo={"43": 0, "42": promo})
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    assert held_keys(env) == [("SKU1", "42")] and env.shop.available(out.gid, "42") == 1
    before = len(env.shop.calls)
    plan, rep = relevel(env, [("SKU1", "42")])
    assert (plan.keys[0].branch, plan.keys[0].reason) == (R.BR_REFUSE, f"{R.F_SKU}:OTHER")
    assert not [c for c in env.shop.calls[before:] if c[0] in MUTATIONS]
    assert held_keys(env) == [("SKU1", "42")] and not env.register().shelf("SKU1")[0].online


def test_f1_a_zero_then_revive_onto_a_variant_of_another_sku_is_refused():
    env = Env()
    out = _f1_outlet(env, status="DRAFT")
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()                                                                   # DRAFT: zero, its +1 pending
    plan, rep = relevel(env, [("SKU1", "42")])
    assert (plan.keys[0].branch, plan.keys[0].reason) == (R.BR_REFUSE, f"{R.F_SKU}:OTHER")
    assert env.shop.available(out.gid, "42") == 0 and env.shop.products[out.gid].status == "DRAFT"


def test_f1_a_create_from_a_source_variant_of_another_sku_is_refused():
    env = Env()
    env.shop.product("Nike Air", outlet=False, sizes=("43",), extra_variants=(("42", "OTHER"),))
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    plan, rep = relevel(env, [("SKU1", "42")])
    assert (plan.keys[0].branch, plan.keys[0].reason) == (R.BR_REFUSE, f"{R.F_SOURCE}:{R.F_SKU}:OTHER")
    assert "duplicate" not in env.shop.names()


@pytest.mark.parametrize("on", ["outlet", "source"])
def test_f1_a_blank_variant_sku_is_refused_like_another_sku(on):
    """D59: a blank-SKU variant's sale CONSUMEs no shelf pair — not the key's variant."""
    env = Env()
    if on == "outlet":
        env.shop.product("Nike - Outlet", sizes=("43",), promo={"43": 0, "42": 0}, extra_variants=(("42", None),))
    else:
        env.shop.product("Nike Air", outlet=False, sizes=("43",), extra_variants=(("42", None),))
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    plan, _rep = relevel(env, [("SKU1", "42")])
    why = f"{R.F_SKU}:{variants.BLANK_SKU}"
    assert (plan.keys[0].branch, plan.keys[0].reason) == (R.BR_REFUSE, why if on == "outlet" else f"{R.F_SOURCE}:{why}")
    assert not [c for c in env.shop.calls if c[0] in ("duplicate", "adjust", "activate")]


def test_f1_a_relevel_of_n0_still_lowers_a_variant_of_another_sku():
    env = Env()
    out = _f1_outlet(env, promo={"43": 0, "42": 1})
    env.departed(size="42")
    env.run()
    assert env.shop.available(out.gid, "42") == 0
    env.shop.levels[(env.shop.variant(out.gid, "42").item, PROMO)].available = 1   # a Shopify-side restock
    plan, rep = relevel(env, [("SKU1", "42")])
    assert plan.keys[0].branch == R.BR_LOWER and env.shop.available(out.gid, "42") == 0


def test_f1_a_variant_respelled_to_another_sku_between_preview_and_confirm_is_moved():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    lvl = env.shop.levels[(env.shop.variant(out.gid, "42").item, PROMO)]
    lvl.available, lvl.on_hand = 0, 0                                           # a hand edit: the re-level adds 1
    plan = R.relevel_preview(env.register(), env.shop, [("SKU1", "42")], promo_location_id=PROMO)
    assert (plan.keys[0].branch, plan.keys[0].delta) == (R.BR_ACTIVE, 1)
    env.shop.variant(out.gid, "42").sku = "OTHER"                               # respelled before the confirm
    reg = env.register()
    rep = R.relevel_apply(reg, env.shop, plan, promo_location_id=PROMO, dlq=env.dlq(plan),
                          lease=PassLease(reg, "relevel", sleep=lambda s: None), sleep=lambda s: None)
    assert result(rep, "SKU1", "42").result == "held" and env.shop.available(out.gid, "42") == 0
