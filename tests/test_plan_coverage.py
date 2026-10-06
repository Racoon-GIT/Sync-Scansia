"""Plan §4 item 4's coverage map (``docs/f7-coverage-map.md``): the items no other
module pinned, as build test cases — rounds 5–7 (§10 W, Y, Z), the rest of §4 item
4, round 8's (§10 AA) left over. Each test names its map id. End to end: the real
``ops`` documents on the stateful fake Shopify, the register on the in-memory store."""
from __future__ import annotations

import time

import pytest

from backend.persistence.records import (
    CARRIED,
    DONE,
    FAILED,
    HELD,
    KIND_BASELINE,
    OPEN,
    SUPERSEDED,
    LeaseLost,
    StoreError,
)
from backend.persistence.register import Register
from backend.services import (
    close_service,
    delete_service,
    exit_service,
    flows,
    normalize_service,
    outlet_service,
    passes,
    pricing_service,
    recovery,
)
from backend.services import ledger as L
from backend.services import relevel_service as R
from backend.services.decrement_worker import DecrementWorker
from backend.services.lease import PassLease

from tests.fake_shop import MAG, PROMO, SimulatedCrashInShop, Variant
from tests.register_fixtures import seed_reconcile
from tests.test_baseline import Env as BaselineEnv
from tests.test_decrement import Env
from tests.test_outlet_service import World, _source
from tests.test_relevel import held_keys, relevel, result
from tests.test_round8_minors import decrement, num, rows_by

K = ("SKU1", "42")
WINDOW_MS = 23 * 3600 * 1000             # the re-send window: first send + 24 h − the 1 h margin


def noop(_s: float) -> None:
    return None


def minus_env(promo=None):
    """An outlet holding K at 2 (43 at 1: still sellable), N 2, then a sale: Δ −1, N 1."""
    env = Env()
    out = env.shop.product("Nike - Outlet", promo=promo or {"42": 2, "43": 1})
    for _ in range(2):
        env.be.add_shelf_pair(*K, reconciled=True)
    return env, out, env.be.consume(*K)


def add_variant(shop, product, sku: str, title: str) -> None:
    """A person adds a variant (no Promo level yet: absent)."""
    tag = f"{sku}{title}".replace(" ", "")
    shop.products[product.gid].variants.append(
        Variant(f"gid://shopify/ProductVariant/9{tag}", sku, title, f"gid://shopify/InventoryItem/9{tag}"))


def crash_once(op: str, nth: int = 0):
    """A hook killing the 'process' right before the ``nth`` call of ``op`` runs."""
    fired = []

    def hook(shop):
        if not fired and shop.count(op) == nth:
            fired.append(1)
            raise SimulatedCrashInShop(f"killed right before {op} #{nth}")

    return hook


# ---------------------------------------------------------------------------
# M2 — the first send's outcome map: CONCURRENT re-sent with its key, MISMATCH open
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("concurrent", [2, len(L.CONCURRENT_BACKOFF_S) + 1])
def test_m2_a_first_send_answered_concurrent_is_re_sent_with_its_own_key_never_failed(concurrent):
    env, out, e = minus_env()
    env.shop.fault("adjust", "concurrent", times=concurrent)
    rep = env.decrement()
    adj = [c[1] for c in env.shop.calls if c[0] == "adjust"]
    assert all(a == adj[0] for a in adj) and adj[0]["idempotencyKey"]        # one key, one body
    (row,) = env.rows()
    if concurrent <= len(L.CONCURRENT_BACKOFF_S):
        assert len(adj) == concurrent + 1 and row.status == DONE and env.unstamped() == []
        assert env.shop.available(out.gid, "42") == 1
    else:
        assert len(adj) == concurrent and row.status == OPEN and env.unstamped() == [e]
        assert "outcome_unknown" in rep.report.kinds() and env.shop.available(out.gid, "42") == 2
        env.decrement()                                          # the next pass: the identical re-send
        assert env.shop.available(out.gid, "42") == 1 and env.unstamped() == []


def test_m2_a_first_send_answered_key_mismatch_stays_open_surfaced_never_failed():
    env, out, e = minus_env()
    env.shop.fault("adjust", "user_error:IDEMPOTENCY_KEY_PARAMETER_MISMATCH")
    rep = env.decrement()
    (row,) = env.rows()
    assert row.status == OPEN and row.sent_at is not None and env.unstamped() == [e]
    assert "idempotency_mismatch" in rep.report.kinds() and env.shop.count("adjust") == 1
    env.decrement()
    assert env.shop.available(out.gid, "42") == 1 and env.unstamped() == []


# ---------------------------------------------------------------------------
# W3, Z9, Z10, AA4 — a recovery re-send answered by each class
# ---------------------------------------------------------------------------

MINUS_RESEND = [
    ("user_error:IDEMPOTENCY_PREVIOUS_ATTEMPT_FAILED", FAILED, {"answer": L.ANS_PREVIOUS_FAILED}),
    ("user_error:INVALID_QUANTITY_DOCUMENT", FAILED, {"answer": L.ANS_INVALID}),
    ("user_error:CHANGE_FROM_QUANTITY_STALE", FAILED, {"answer": L.ANS_STALE, "possibly_applied": True}),
    ("user_error:INVALID_INVENTORY_ITEM", FAILED, {"answer": L.ANS_GONE, "possibly_applied": True}),
    ("user_error:ADJUST_QUANTITIES_FAILED", SUPERSEDED, {}),     # any other error: re-applied at once
    ("concurrent", SUPERSEDED, {}),
    ("timeout_before", SUPERSEDED, {}),                          # Z9: an unknown re-send, re-applied at once
]


@pytest.mark.parametrize("fault,status,outcome", MINUS_RESEND)
def test_w3_z9_aa4_a_minus_re_send_answered_by_each_class(fault, status, outcome):
    env, out, e = minus_env()
    env.shop.fault("adjust", "timeout_before")                   # the first send never ran; outcome unknown
    env.decrement()
    (first,) = env.rows()
    env.shop.fault("adjust", fault)                              # the one in-window re-send
    rep = env.decrement()
    first = env.register().reconcile_rows(row_ids=[first.row_id])[0]
    assert first.status == status
    assert {k: first.outcome.get(k) for k in outcome} == outcome
    (nxt,) = [r for r in env.rows() if r.row_id != first.row_id]
    assert nxt.status == DONE and nxt.action == L.A_ADJUST and L.share(nxt) == -1
    if status == SUPERSEDED:
        assert first.outcome["successor"] == nxt.row_id and nxt.group_id == first.group_id
        assert "possible_extra_minus_one" in rep.report.kinds()
    else:                                                        # events pending: the next lowering, a new group
        assert nxt.group_id != first.group_id
        marked = bool(outcome.get("possibly_applied"))
        assert (L.payload(nxt).get("possibly_applied_before") == [first.row_id]) is marked
        assert ("possible_extra_minus_one" in rep.report.kinds()) is marked
    assert env.shop.available(out.gid, "42") == 1 and env.unstamped() == []


PLUS_RESEND = [
    ("user_error:IDEMPOTENCY_PREVIOUS_ATTEMPT_FAILED", FAILED),
    ("user_error:INVALID_QUANTITY_DOCUMENT", FAILED),
    ("user_error:CHANGE_FROM_QUANTITY_STALE", HELD),               # Z10
    ("user_error:INVALID_INVENTORY_ITEM", HELD),
    ("user_error:ADJUST_QUANTITIES_FAILED", HELD),
    ("timeout_before", HELD),
    ("concurrent", OPEN),                                          # re-sent until the window's end
]


@pytest.mark.parametrize("fault,status", PLUS_RESEND)
def test_w3_z10_a_plus_re_send_answered_by_each_class(fault, status):
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0, "43": 1})
    w.shop.fault("adjust", "timeout_before")
    w.run()
    (add,) = rows_by(w, L.A_ADJUST, L.P_ADD)
    w.shop.fault("adjust", fault)
    w.run()                                                      # a publish-class recovery's one re-send
    add = w.register().reconcile_rows(row_ids=[add.row_id])[0]
    assert add.status == status
    assert (K in {h.key for h in w.register().held_keys()}) is (status != FAILED)
    w.run()
    if status == HELD:
        assert w.shop.available(out.gid, "42") == 0 and w.unstamped() == [e]     # never re-added
    else:
        assert w.shop.available(out.gid, "42") == 1 and w.unstamped() == []      # added once


# ---------------------------------------------------------------------------
# W1, Z7, Z8 — the re-send window's edges; a re-send rewrites neither SENT_AT nor SENT_AVAILABLE
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("offset_ms,re_sent", [(-1000, True), (1000, False)])
def test_w1_a_minus_just_inside_the_window_is_re_sent_identical_just_past_it_re_applied(offset_ms, re_sent):
    env, out, _e = minus_env()
    env.shop.fault("adjust", "timeout_before")
    env.decrement()
    (first,) = env.rows()
    env.be.advance(WINDOW_MS + offset_ms)
    env.decrement()
    first = env.register().reconcile_rows(row_ids=[first.row_id])[0]
    adj = [c[1] for c in env.shop.calls if c[0] == "adjust"]
    assert len(adj) == 2
    if re_sent:
        assert first.status == DONE and adj[1] == adj[0]                      # same key, the stored request
    else:
        assert first.status == SUPERSEDED and adj[1]["idempotencyKey"] != adj[0]["idempotencyKey"]
    assert env.shop.available(out.gid, "42") == 1 and env.unstamped() == []


def test_w1_z7_z8_a_plus_answered_concurrent_is_re_sent_every_pass_until_its_first_send_plus_23_h():
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0, "43": 1})
    w.shop.fault("adjust", "timeout_before")
    w.run()
    (add,) = rows_by(w, L.A_ADJUST, L.P_ADD)
    first = (add.sent_at, add.sent_available, add.request)
    for advance_ms in (3600 * 1000, WINDOW_MS - 3600 * 1000 - 1000):       # up to first send + 23 h − 1 s
        w.be.advance(advance_ms)
        w.shop.fault("adjust", "concurrent")
        sends = w.shop.count("adjust")
        w.run()
        add = w.register().reconcile_rows(row_ids=[add.row_id])[0]
        assert add.status == OPEN and w.shop.count("adjust") == sends + 1
        assert (add.sent_at, add.sent_available, add.request) == first
    w.be.advance(2000)                                                       # first send + 23 h + 1 s
    sends = w.shop.count("adjust")
    w.run()
    add = w.register().reconcile_rows(row_ids=[add.row_id])[0]
    assert add.status == HELD and w.shop.count("adjust") == sends            # past it: held, never sent
    assert w.shop.available(out.gid, "42") == 0 and w.unstamped() == [e]


# ---------------------------------------------------------------------------
# W4, Z5, R2, AA5 — a group beside a FAILED sibling: CARRIED, no stamp, the remainder
# ---------------------------------------------------------------------------

def _split_env():
    env = Env()
    a = env.shop.product("Nike - Outlet", promo={"42": 1})
    b = env.shop.product("Nike - Outlet bis", status="DRAFT", promo={"42": 1})
    for _ in range(2):
        env.be.add_shelf_pair(*K, reconciled=True)
    env.be.consume(*K)
    env.be.consume(*K)                                           # Δ −2: split, ACTIVE first
    return env, a, b


@pytest.mark.parametrize("how", ["refused", "crash", "premise"])
def test_w4_z5_aa5_a_group_beside_a_failed_sibling_carries_the_applied_one_and_re_plans_the_remainder(how):
    env, a, b = _split_env()
    item_b = env.shop.variant(b.gid, "42").item
    if how == "refused":
        env.shop.fault("adjust", "user_error:ADJUST_QUANTITIES_FAILED",
                       when=lambda v: v["input"]["changes"][0]["inventoryItemId"] == item_b)
        env.decrement()
    elif how == "crash":                                         # Z5: after the write-ahead, before B's send
        env.shop.on("read_level", crash_once("read_level", 1))
        with pytest.raises(SimulatedCrashInShop):
            env.decrement()
        env.decrement()                                          # recovery: B never sent → FAILED
    else:                                                        # AA5: B's premise contradicted at rule 8's read
        env.shop.on("adjust", lambda shop: shop.sell(b.gid, "42") if shop.count("adjust") == 0 else None)
        env.decrement()
    rows = sorted(env.rows(), key=lambda r: r.row_id)
    ra, rb = rows[0], rows[1]
    assert ra.product_id == num(a) and rb.product_id == num(b) and ra.group_id == rb.group_id
    assert ra.status == CARRIED and ra.outcome["moved"] == -1
    assert rb.status == FAILED and bool(rb.outcome.get("never_sent")) is (how != "refused")
    env.decrement()                                              # the remainder: −2 − (−1)
    assert env.unstamped() == [] and env.shop.available(a.gid, "42") == 0
    assert env.shop.available(b.gid, "42") == 0
    later = [r for r in env.rows() if r.row_id not in (ra.row_id, rb.row_id) and r.action == L.A_ADJUST]
    assert all(r.group_id != ra.group_id for r in later)         # the remainder is a group of its own pass
    assert env.shop.count("adjust") == {"refused": 3, "crash": 2, "premise": 1}[how]   # premise: stamp only


# ---------------------------------------------------------------------------
# W5, W6, AA8 — activations
# ---------------------------------------------------------------------------

def test_w5_an_activation_applied_beside_a_failed_add_is_carried_and_the_add_re_planned():
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0, "43": 1}, inactive=("42",))
    w.shop.fault("adjust", "user_error:ADJUST_QUANTITIES_FAILED")          # the add refused at its first send
    _plan, rep = w.run()
    (act,) = rows_by(w, L.A_ACTIVATE)
    (add,) = rows_by(w, L.A_ADJUST, L.P_ADD)
    assert (act.status, add.status) == (CARRIED, FAILED) and rep.outcomes[0].status == "PARTIAL"
    assert w.unstamped() == [e] and w.shop.promo(out.gid, "42").active
    w.run()                                                      # the remainder: +1 − 0 (an activation moves none)
    assert w.shop.available(out.gid, "42") == 1 and w.unstamped() == [] and w.shop.count("activate") == 1


def test_w6_an_activation_answered_user_errors_is_read_off_is_active():
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0, "43": 1}, inactive=("42",))
    lvl = w.shop.promo(out.gid, "42")
    w.shop.on("activate", lambda shop: setattr(lvl, "active", True))      # it took effect; the answer says not
    w.shop.fault("activate", "user_error:X")
    w.run()
    (act,) = rows_by(w, L.A_ACTIVATE)
    assert act.status == DONE and act.outcome["read_active"] is True
    assert w.shop.available(out.gid, "42") == 1


@pytest.mark.parametrize("landed", [True, False])
def test_aa8_a_publish_class_re_send_of_an_activation_is_withheld_once_its_level_reads_active(landed):
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0, "43": 1}, inactive=("42",))
    w.shop.on("activate", crash_once("activate"))               # killed right after the activation's SENT_AT
    w.run()
    (act,) = rows_by(w, L.A_ACTIVATE)
    assert act.status == OPEN and act.sent_at is not None
    if landed:
        w.shop.promo(out.gid, "42").active = True                # the call had run after all
    w.run()                                                      # recovery, inside the window
    act = w.register().reconcile_rows(row_ids=[act.row_id])[0]
    assert act.status == CARRIED and w.shop.count("activate") == (0 if landed else 1)
    w.run()
    assert w.shop.available(out.gid, "42") == 1 and w.unstamped() == []


def test_aa8_a_lowering_of_an_inactive_level_sends_no_activation():
    env, out, _e = minus_env(promo={"42": 1, "43": 1})
    env.shop.promo(out.gid, "42").active = False                 # an inactive level keeps its quantities (E15)
    env.decrement()
    assert env.shop.count("activate") == 0 and env.shop.available(out.gid, "42") == 0
    assert env.unstamped() == []


# ---------------------------------------------------------------------------
# R7, W8, W13 — a CREATE refused at each step, and a crash in each window
# ---------------------------------------------------------------------------

CREATE_REFUSALS = [
    ("deactivate", None),                                                          # step 2: a disconnect
    ("adjust", lambda v: v["input"]["changes"][0]["locationId"] == MAG),          # step 2: its zeroing
    ("adjust", lambda v: v["input"]["changes"][0]["locationId"] == PROMO
     and v["input"]["changes"][0]["delta"] < 0),                                   # step 3: a Promo zeroing
    ("adjust", lambda v: v["input"]["changes"][0]["delta"] > 0),                  # step 4: an add
    ("bulk", None),                                                                # step 5
    ("status", None),                                                              # step 6: ACTIVE
    ("publish", None),                                                             # step 6: publish
]


@pytest.mark.parametrize("op,when", CREATE_REFUSALS)
def test_r7_a_create_refused_after_the_duplicate_leaves_its_orphan_drafted_zeroed_and_held(op, when):
    w = World()
    events = w.returned(n=2)
    _source(w, promo={"42": 5, "43": 5}, mag={"42": 3, "43": 3})
    w.shop.fault(op, "user_error:X", when=when)
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "CREATE_ABORTED"
    (create,) = rows_by(w, L.A_CREATE)
    assert create.status == HELD and create.product_id is not None
    dup = outlet_service.product_gid(create.product_id)
    assert w.shop.products[dup].status == "DRAFT"
    assert [w.shop.available(dup, s) for s in ("42", "43")] == [0, 0]
    assert K in set(held_keys(w)) and sorted(w.unstamped()) == sorted(events)


CREATE_CRASHES = [("duplicate", 0), ("deactivate", 0), ("adjust", 0), ("adjust", 4), ("bulk", 0),
                  ("status", 0), ("publish", 0)]


@pytest.mark.parametrize("op,nth", CREATE_CRASHES)
def test_r7_w8_w13_a_crash_in_each_create_window_with_a_sale_before_the_recovery(op, nth):
    w = World()
    events = w.returned(n=2)
    _source(w, promo={"42": 5, "43": 5}, mag={"42": 3, "43": 3})
    w.shop.on(op, crash_once(op, nth))
    w.run()
    (create,) = rows_by(w, L.A_CREATE)
    assert create.status == OPEN and create.sent_at is not None            # SENT_AT precedes every call
    sale = w.be.consume(*K)                                                  # a sale between crash and recovery
    rep = decrement(w)                                                       # e.g. the boot pass: decrement class
    assert w.register().reconcile_rows(row_ids=[create.row_id])[0].status == OPEN
    assert "left_for_publish" in rep.report.kinds() and sale not in w.unstamped()
    w.run()                                                                  # a publish-class recovery
    create = w.register().reconcile_rows(row_ids=[create.row_id])[0]
    assert K in set(held_keys(w)) and set(events) <= set(w.unstamped())
    if op == "duplicate":                                                    # it never ran: nothing to find
        assert create.status == OPEN and create.product_id is None
        assert [p.title for p in w.shop.products.values()] == ["Nike Air"]
    else:
        assert create.status == HELD and create.product_id is not None
        dup = outlet_service.product_gid(create.product_id)
        assert w.shop.products[dup].status == "DRAFT"
        assert [w.shop.available(dup, s) for s in ("42", "43")] == [0, 0]
        assert all(r.status != OPEN for r in w.rows() if r.row_id != create.row_id)


def test_w8_w13_m4_a_tail_crashed_after_its_sent_at_is_left_by_the_decrement_and_re_run_by_the_publish():
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 2})
    w.shop.on("status", crash_once("status"))                   # the tail's step 2, right after its SENT_AT
    w.run()
    (tail,) = rows_by(w, L.A_TAIL)
    assert tail.status == OPEN and tail.sent_at is not None and tail.step == "2:active"
    rep = decrement(w)
    assert w.register().reconcile_rows(row_ids=[tail.row_id])[0].status == OPEN
    assert "left_for_publish" in rep.report.kinds() and w.unstamped() == [e]
    assert w.shop.products[out.gid].status == "DRAFT"
    w.run()                                                      # re-run, idempotent by state
    tail = w.register().reconcile_rows(row_ids=[tail.row_id])[0]
    assert tail.status == DONE and w.shop.products[out.gid].status == "ACTIVE"
    assert w.shop.products[out.gid].published and w.shop.available(out.gid, "42") == 1 and w.unstamped() == []


# ---------------------------------------------------------------------------
# W12, AA1 — an event-less lowering past the window: re-derived from live, never re-sent
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("case,expect", [("landed", FAILED), ("not_landed", SUPERSEDED), ("level_gone", FAILED)])
def test_w12_aa1_an_eventless_lowering_past_the_window_is_re_derived_from_live(case, expect):
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 2})
    w.shop.fault("adjust", "lose_after" if case == "landed" else "timeout_before")
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "REVIVE_STOPPED"
    (zero,) = rows_by(w, L.A_ADJUST, L.P_ZERO)
    assert zero.status == OPEN
    if case == "level_gone":
        del w.shop.levels[(w.shop.variant(out.gid, "43").item, PROMO)]
    w.be.advance(WINDOW_MS + 1000)
    sends = w.shop.count("adjust")
    w.run()
    zero = w.register().reconcile_rows(row_ids=[zero.row_id])[0]
    assert zero.status == expect
    if expect == SUPERSEDED:                                     # re-computed as a new row, then sent
        (succ,) = w.register().reconcile_rows(row_ids=[zero.outcome["successor"]])
        assert succ.status == CARRIED and w.shop.count("adjust") == sends + 1
    else:                                                        # nothing left to lower: nothing sent
        assert zero.outcome["reason"] == "nothing_left_to_lower" and w.shop.count("adjust") == sends
    assert w.shop.available(out.gid, "43") in (0, None)
    _plan, rep = w.run()                                         # the retry: a new revive
    assert rep.outcomes[0].status == "REVIVED" and w.shop.available(out.gid, "42") == 1


# ---------------------------------------------------------------------------
# W16, W17 — the plan bounds the writes, never the held-key sweep; a replaced outlet
# ---------------------------------------------------------------------------

def test_w16_the_publishs_held_key_sweep_lowers_a_sku_absent_from_its_plan():
    w = World(baseline=False)
    _, e2 = w.be.add_shelf_pair("SKU2", "40", price_outlet="50.00", price_high="100.00")
    w.be.seed_baseline(held=[{"key": ["SKU2", "40"], "reason": "moved", "event_ids": [e2]}])
    other = w.shop.product("Puma - Outlet", sku="SKU2", sizes=("40",), promo={"40": 3})
    w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0, "43": 1})
    plan = w.preview()
    assert [a.sku for a in plan.actions] == ["SKU1"]
    w.apply(plan)
    assert w.shop.available(other.gid, "40") == 1 and w.shop.available(out.gid, "42") == 1


def test_w17_an_outlet_replaced_since_the_preview_is_deferred_nothing_written():
    w = World()
    w.returned()
    old = w.shop.product("Nike - Outlet", promo={"42": 0})
    plan = w.preview()
    del w.shop.products[old.gid]
    w.shop.product("Nike - Outlet", promo={"42": 0})             # recreated: another gid
    rep = w.apply(plan)
    assert rep.outcomes[0].status == "VERIFY_FAILED" and w.mutated() == [] and len(w.unstamped()) == 1


# ---------------------------------------------------------------------------
# W18, W19 — re-levels: a mixed pending mix; the delete's HOLD released by a re-level that recreates
# ---------------------------------------------------------------------------

def test_w18_a_relevel_of_a_mixed_pending_mix_levels_to_n_and_stamps_every_event():
    env = BaselineEnv()
    out = env.shop.product("Nike - Outlet", sizes=("43",), promo={"43": 0})
    p1, e1 = env.imported(size="42", price_outlet="50.00", price_high="100.00")
    _, e2 = env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()                                                    # size absent: held with e1, e2
    e3 = env.be.leave_shelf(p1, "VENDUTO", event_type="CONSUME", actor="SP")          # post-hold −1
    _, e4 = env.be.add_shelf_pair("SKU1", "42", price_outlet="50.00", price_high="100.00")   # post-hold +1
    add_variant(env.shop, out, "SKU1", "42")
    plan, rep = relevel(env, [K])
    kp = plan.keys[0]
    assert kp.n == 2 and sorted(kp.event_ids) == sorted([e1, e2, e3, e4])
    assert result(rep, *K).result == "released"
    assert env.shop.available(out.gid, "42") == 2 and env.unstamped() == [] and held_keys(env) == []
    assert env.invariant() == {}


def test_w19_the_deletes_hold_is_released_by_a_relevel_that_recreates_the_outlet():
    from backend.persistence.audit_sink import RegisterAuditSink

    env = BaselineEnv()
    env.shop.product("Nike Air", outlet=False, sizes=("42", "43"))
    out = env.shop.product("Nike Air - Outlet", promo={"42": 0, "43": 0})
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    assert env.shop.available(out.gid, "42") == 1
    reg = env.register()
    plan = delete_service.single_delete_preview(reg, env.shop, out.gid)
    delete_service.delete_single_apply(reg, env.shop, RegisterAuditSink(reg, actor="ale"), plan,
                                       human_gesture="CONFERMO", promo_location_id=PROMO,
                                       lease=PassLease(reg, "delete", sleep=noop), sleep=noop)
    assert out.gid not in env.shop.products and held_keys(env) == [K]
    rplan, rep = relevel(env, [K])
    assert rplan.keys[0].branch == R.BR_CREATE and result(rep, *K).result == "released"
    assert held_keys(env) == [] and env.unstamped() == [] and env.invariant() == {}


# ---------------------------------------------------------------------------
# Z1, AA14, M16 — the held-key sweep's terms
# ---------------------------------------------------------------------------

def _held_env(promo, *, pairs=2):
    env = Env(baseline=False)
    out = env.shop.product("Nike - Outlet", promo=promo)
    held = [env.be.add_shelf_pair(*K)[1] for _ in range(pairs)]
    env.be.seed_baseline(held=[{"key": list(K), "reason": "moved", "event_ids": held}])
    return env, out


def test_z1_a_timed_out_minus_still_open_is_counted_by_the_sweep_so_p_ends_at_n():
    env, out = _held_env({"42": 2, "43": 1})
    e = env.be.consume(*K)                                       # post-hold −1: N 1
    env.shop.fault("adjust", "timeout_before", times=3)          # the send, its re-send, the re-apply: unknown
    env.decrement()
    env.decrement()                                              # recovery leaves a sent −Δ OPEN, then the sweep
    (open_minus,) = [r for r in env.rows(statuses=(OPEN,)) if L.is_minus(r)]
    assert open_minus.sent_at is not None and env.shop.available(out.gid, "42") == 2
    env.decrement()                                              # its re-send lands
    assert env.shop.available(out.gid, "42") == 1 and e not in env.unstamped()     # P = N
    assert [h.key for h in env.register().held_keys()] == [K]   # the in-flight hold ended; the baseline's stays


def test_aa14_the_held_key_sweep_nets_what_a_carried_row_moved():
    env, out = _held_env({"42": 2, "43": 1})
    e = env.be.consume(*K)                                       # post-hold −1: N 1
    seed_reconcile(env.register(), action=L.A_ADJUST, keys=(K,), event_ids=(e,), purpose=L.P_LOWER, share=-1,
                   inventory_item_id=env.shop.variant(out.gid, "42").item, location_id=PROMO,
                   product_id=num(out), sent=True, status=CARRIED, outcome={"moved": -1}, group_id=91)
    env.decrement()                                              # P 2 (a restock beside the carried −1): excess 1
    assert env.shop.available(out.gid, "42") == 1
    assert e not in env.unstamped()                              # the remainder −1 − (−1) = 0: stamped


def test_m16_a_stalled_plus_landing_on_a_held_key_after_its_minus_ran_stamp_only_is_lowered_next_pass():
    w = World()
    (e1,) = w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0, "43": 1})
    w.shop.fault("adjust", "timeout_before")                     # the +1 stalls: outcome unknown
    w.run()
    w.be.advance(WINDOW_MS + 1000)
    w.run()                                                      # past the window: HELD, the key held
    (add,) = rows_by(w, L.A_ADJUST, L.P_ADD)
    assert add.status == HELD
    w.be.consume(*K)                                             # its pair sells at full price: N 0
    assert decrement(w).applied == {"SKU1|42": "stamp_only"}     # Promo reads 0: the −1 ran stamp-only
    lvl = w.shop.promo(out.gid, "42")
    lvl.available, lvl.on_hand = 1, 1                            # the stalled +1 lands
    decrement(w)
    assert w.shop.available(out.gid, "42") == 0 and e1 in w.unstamped()      # lowered; held for the release
    w.be.advance(3600 * 1000)                                    # past the HELD row's 24 h retention
    decrement(w)
    assert w.unstamped() == [] and w.register().held_keys() == []


# ---------------------------------------------------------------------------
# Z11, AA9, AA10, AA11, M13 — the N-0 release
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("case", ["size_absent", "multi_outlet", "negative", "pending_plus", "row_open"])
def test_z11_aa9_aa10_m13_the_n0_release(case):
    env = Env()
    if case == "size_absent":
        out = env.shop.product("Nike - Outlet", sizes=("43",), promo={"43": 1})
    elif case == "negative":
        out = env.shop.product("Nike - Outlet", promo={"42": -1, "43": 1})
    else:
        out = env.shop.product("Nike - Outlet", promo={"42": 0, "43": 1})
    if case == "multi_outlet":
        env.shop.product("Nike - Outlet bis", status="DRAFT", promo={"42": 0})
    if case == "pending_plus":                                   # AA10: N 0 with a +1 pending
        p, e1 = env.be.add_shelf_pair(*K)
        events = [e1, env.be.leave_shelf(p, "ANNULLATO", event_type="MANUAL_CANCEL", delta=0)]
    else:
        events = [env.sold()]                                    # its +1 stamped, then the sale: N 0
    if case == "row_open":                                       # a sent +Δ the decrement leaves open
        seed_reconcile(env.register(), action=L.A_ADJUST, keys=(K,), purpose=L.P_ADD, share=1,
                       inventory_item_id=env.shop.variant(out.gid, "42").item, location_id=PROMO,
                       product_id=num(out), sent=True)
    env.decrement()
    n0 = [b for b in env.register().audit_rows(KIND_BASELINE) if (b.payload or {}).get("reason") == "N 0"]
    if case == "row_open":
        assert n0 == []
    else:
        (rel,) = n0
        assert sorted(rel.payload["stamped"]) == sorted(events) and env.unstamped() == []
        assert env.shop.count("adjust") == 0 and env.shop.count("activate") == 0     # nothing to Shopify
    if case == "negative":
        assert env.shop.available(out.gid, "42") == -1           # a negative is never raised
    if case == "pending_plus":
        assert outlet_service.publish_preview(env.register(), env.shop, promo_location_id=PROMO).actions == ()


def test_aa11_a_pair_arriving_between_the_n0_releases_two_reads_is_never_stamped(monkeypatch):
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0, "43": 1})
    env.sold()                                                   # N 0
    arrived = []

    def arrival_after(name):
        real = getattr(Register, name)

        def wrapped(self, *a, **kw):
            out = real(self, *a, **kw)
            if not arrived:
                arrived.append(env.be.add_shelf_pair(*K)[1])    # lands right after the release's first read
            return out

        monkeypatch.setattr(Register, name, wrapped)

    arrival_after("unstamped_events")
    arrival_after("shelf")
    reg = env.register()
    lease = PassLease(reg, "decrement", sleep=noop)
    lease.take()
    try:
        released = flows.n0_release(flows.Context(L.Ledger(reg, env.shop, lease, PROMO)))
    finally:
        lease.release()
    assert released == [] and arrived and arrived[0] in env.unstamped()


# ---------------------------------------------------------------------------
# M3, M11, R1 — a revive's zeroing under a committed unit; the total rule after a recovered −Δ
# ---------------------------------------------------------------------------

def test_m3_a_revive_zeroes_relatively_under_a_committed_unit_never_on_hand():
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 1}, committed={"43": 1})
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "REVIVED"
    item43 = w.shop.variant(out.gid, "43").item
    (z,) = [c[1]["input"]["changes"][0] for c in w.shop.calls
            if c[0] == "adjust" and c[1]["input"]["changes"][0]["inventoryItemId"] == item43]
    assert (z["delta"], z["changeFromQuantity"]) == (-1, 1)
    lvl = w.shop.promo(out.gid, "43")
    assert (lvl.available, lvl.committed, lvl.on_hand) == (0, 1, 1)


@pytest.mark.parametrize("when", ["in_window_re_send", "past_window_re_apply", "stamp_retry"])
def test_m11_r1_the_product_total_rule_runs_after_a_recovered_minus(when, monkeypatch):
    env, out, _e = minus_env(promo={"42": 1, "43": 0})          # the outlet's last unit
    if when == "stamp_retry":                                    # R1: APPLIED, the stamp failed
        real = Register.stamp
        failed = []

        def stamp_once(self, *a, **kw):
            if not failed:
                failed.append(1)
                raise StoreError("the stamp's transaction failed")
            return real(self, *a, **kw)

        monkeypatch.setattr(Register, "stamp", stamp_once)
    else:
        env.shop.fault("adjust", "timeout_before")
    env.decrement()
    assert env.shop.products[out.gid].status == "ACTIVE"
    if when == "past_window_re_apply":
        env.be.advance(WINDOW_MS + 1000)
    env.decrement()                                              # recovery lands it (or retries the stamp)
    assert env.shop.available(out.gid, "42") == 0 and env.shop.products[out.gid].status == "DRAFT"
    assert env.unstamped() == [] and env.shop.count("adjust") == (1 if when == "stamp_retry" else 2)
    (row,) = [r for r in env.rows() if r.status != SUPERSEDED]
    assert row.status == DONE


# ---------------------------------------------------------------------------
# F6, F7, AA12 — the fence: a send that slipped through; a decrement between two SKUs
# ---------------------------------------------------------------------------

def test_f6_a_send_that_slipped_through_a_take_over_is_answered_by_its_key():
    env, out, e = minus_env()

    def take_over(shop):
        if shop.count("adjust") == 0:
            env.be.advance(301_000)                              # the holder stalled past its lease
            taker = PassLease(env.register(), "publish", sleep=noop)
            taker.take()
            taker.release()

    env.shop.on("adjust", take_over)
    with pytest.raises(LeaseLost):
        env.decrement()                                          # it landed; its outcome cannot be written
    (row,) = env.rows()
    assert row.status == OPEN and env.shop.available(out.gid, "42") == 1
    env.decrement()                                              # the next holder's recovery: the same key
    adj = [c[1] for c in env.shop.calls if c[0] == "adjust"]
    assert len(adj) == 2 and adj[0] == adj[1]
    assert env.shop.available(out.gid, "42") == 1 and env.unstamped() == []      # applied once


def test_f7_aa12_a_decrement_between_two_skus_of_a_relevel_and_the_next_key_re_derived(monkeypatch):
    sweeps = []
    real_recover = passes.recover
    monkeypatch.setattr(passes, "recover", lambda ctx, klass: (sweeps.append(klass), real_recover(ctx, klass))[1])
    env = BaselineEnv()
    outs = {s: env.shop.product(f"{s} - Outlet", sku=s, sizes=("43",), promo={"43": 0}) for s in ("A", "B")}
    env.imported(sku="A", size="42", price_outlet="50.00", price_high="100.00")
    pb, _ = env.imported(sku="B", size="42", price_outlet="50.00", price_high="100.00")
    env.imported(sku="B", size="42", price_outlet="50.00", price_high="100.00")
    env.run()                                                    # both held: size absent
    for s in ("A", "B"):
        add_variant(env.shop, outs[s], s, "42")
    plan = R.relevel_preview(env.register(), env.shop, [("A", "42"), ("B", "42")], promo_location_id=PROMO)
    reg = env.register()
    lease = PassLease(reg, "relevel", sleep=noop)
    real_try = lease.try_take
    takes = []

    def try_take():
        takes.append(1)
        if len(takes) == 2:                                      # between A and B: a sale, its decrement
            env.be.leave_shelf(pb, "VENDUTO", event_type="CONSUME", actor="SP")
            r2 = env.register()
            passes.decrement_pass(r2, env.shop, PROMO, lease=PassLease(r2, "decrement", sleep=noop), sleep=noop)
        return real_try()

    lease.try_take = try_take
    sweeps.clear()
    rep = R.relevel_apply(reg, env.shop, plan, promo_location_id=PROMO, dlq=env.dlq(plan), lease=lease, sleep=noop)
    assert result(rep, "A", "42").result == "released" and result(rep, "B", "42").result == "held"
    assert env.shop.available(outs["A"].gid, "42") == 1 and env.shop.available(outs["B"].gid, "42") is None
    assert sweeps == [recovery.CLASS_PUBLISH, recovery.CLASS_DECREMENT, recovery.CLASS_PUBLISH]


def test_aa12_a_run_of_relevels_with_nobody_in_between_pays_one_sweep(monkeypatch):
    sweeps = []
    real_recover = passes.recover
    monkeypatch.setattr(passes, "recover", lambda ctx, klass: (sweeps.append(klass), real_recover(ctx, klass))[1])
    env = BaselineEnv()
    for s in ("A", "B", "C"):
        env.shop.product(f"{s} - Outlet", sku=s, sizes=("43",), promo={"43": 0})
        env.imported(sku=s, size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    sweeps.clear()
    relevel(env, [("A", "42"), ("B", "42"), ("C", "42")])
    assert sweeps == [recovery.CLASS_PUBLISH]


# ---------------------------------------------------------------------------
# AA13 — each holder's recovery class
# ---------------------------------------------------------------------------

SECTION_3_5 = ["prices_apply", "revert_prices", "delete_single_apply", "cleanup_apply", "exit_apply",
               "normalize_apply", "close_row"]


@pytest.mark.parametrize("vertical", SECTION_3_5)
def test_aa13_every_section_3_5_vertical_runs_the_decrement_class_recovery(vertical, monkeypatch):
    sweeps = []
    real_recover = passes.recover
    monkeypatch.setattr(passes, "recover", lambda ctx, klass: (sweeps.append(klass), real_recover(ctx, klass))[1])
    if vertical in ("prices_apply", "revert_prices"):
        from tests.test_pricing_service import Env as PricingEnv
        from tests.test_pricing_service import pct

        env = PricingEnv()
        env.pair(product=env.outlet())
        rep = env.apply(pricing_service.MODE_PERCENT, pct())
        if vertical == "revert_prices":
            sweeps.clear()
            env.revert(rep.intent_id)
    elif vertical in ("delete_single_apply", "cleanup_apply"):
        from tests.test_delete_service import Env as DeleteEnv

        env = DeleteEnv()
        if vertical == "delete_single_apply":
            env.single(env.outlet(status="ACTIVE", promo={"42": 1}))
        else:
            env.outlet(sku="SKU2")
            env.cleanup(gesture="1")
    elif vertical == "exit_apply":
        from tests.test_exit_service import Env as ExitEnv
        from tests.test_exit_service import two_outlets

        env = ExitEnv()
        keep, _other = two_outlets(env)
        env.apply(env.preview("SKU1", keep))
    elif vertical == "normalize_apply":
        w = World()
        w.shop.product("Nike - Outlet", policy="CONTINUE")
        reg = w.register()
        normalize_service.normalize_apply(
            reg, w.shop, normalize_service.normalize_preview(reg, w.shop, promo_location_id=PROMO),
            promo_location_id=PROMO, lease=PassLease(reg, "normalize", sleep=noop), sleep=noop)
    else:
        w = World()
        out = w.shop.product("Nike - Outlet", promo={"42": 0})
        row = seed_reconcile(w.register(), action=L.A_ADJUST, keys=(K,), purpose=L.P_ADD, share=1,
                             inventory_item_id=w.shop.variant(out.gid, "42").item, location_id=PROMO,
                             product_id=num(out), sent=True)
        reg = w.register()
        close_service.close_row(reg, w.shop, row, expect_status=OPEN, why="checked by hand",
                                promo_location_id=PROMO, lease=PassLease(reg, "close", sleep=noop), sleep=noop)
    assert sweeps and set(sweeps) == {recovery.CLASS_DECREMENT}


@pytest.mark.parametrize("holder", ["decrement", "pricing", "delete", "cleanup", "exit", "normalize", "close",
                                    "publish"])
def test_aa13_a_sent_plus_is_left_by_every_decrement_class_holder_and_resolved_by_the_publish(holder):
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0, "43": 1})
    w.shop.fault("adjust", "lose_after")                         # it landed; its answer lost
    w.run()
    (add,) = rows_by(w, L.A_ADJUST, L.P_ADD)
    reg = w.register()
    if holder == "decrement":
        kinds = decrement(w).report.kinds()
    elif holder == "publish":
        outlet_service.publish_apply(reg, w.shop, outlet_service.Plan(True, (), ()), promo_location_id=PROMO,
                                     lease=PassLease(reg, "publish", sleep=noop), take_bound_s=0)
        kinds = []
    else:
        run = passes.Holder(reg, w.shop, PROMO, holder, sleep=noop)
        run.start()
        run.release()
        kinds = run.report.kinds()
    status = w.register().reconcile_rows(row_ids=[add.row_id])[0].status
    if holder == "publish":
        assert status == DONE and w.unstamped() == [] and w.shop.available(out.gid, "42") == 1
    else:
        assert status == OPEN and "left_for_publish" in kinds and w.unstamped() == [e]


# ---------------------------------------------------------------------------
# AA1, AA2 — a tail refused; a delete of any kind closing its product's rows; a tail's hold
# ---------------------------------------------------------------------------

def test_aa1_a_revives_tail_refused_closes_failed_its_adds_carried_and_nothing_re_added():
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 2})
    w.shop.fault("publish", "user_error:X")
    _plan, rep = w.run()
    (tail,) = rows_by(w, L.A_TAIL)
    (add,) = rows_by(w, L.A_ADJUST, L.P_ADD)
    assert tail.status == FAILED and tail.outcome["refused"] and add.status == CARRIED
    assert w.shop.products[out.gid].status == "ACTIVE" and not w.shop.products[out.gid].published
    assert "tail_refused" in [i["kind"] for i in rep.surfaced] and w.unstamped() == [e]
    decrement(w)                                                 # the remainder +1 − (+1) = 0: stamped
    assert w.unstamped() == [] and w.shop.available(out.gid, "42") == 1 and w.shop.count("adjust") == 2


def test_aa1_the_bulk_cleanup_closes_its_products_open_rows():
    from tests.test_delete_service import Env as DeleteEnv

    env = DeleteEnv()
    gone = env.outlet(sku="SKU2")                                # DRAFT at 0, no shelf pair: a candidate
    row = seed_reconcile(env.register(), action=L.A_ADJUST, keys=(("SKU2", "42"),), purpose=L.P_ADD, share=1,
                         inventory_item_id=env.shop.variant(gone.gid, "42").item, location_id=PROMO,
                         product_id=num(gone), sent=True)
    rep = env.cleanup(gesture="1")
    assert rep.deleted == 1
    (r,) = env.register().reconcile_rows(row_ids=[row])
    assert r.status == FAILED and r.outcome["reason"] == "product_deleted"


def test_aa1_the_multi_outlet_exit_closes_the_deleted_outlets_open_rows():
    from tests.test_exit_service import Env as ExitEnv
    from tests.test_exit_service import two_outlets

    env = ExitEnv()
    keep, other = two_outlets(env)
    env.be.add_shelf_pair("SKU1", "42", reconciled=True, online=True)
    row = seed_reconcile(env.register(), action=L.A_ADJUST, keys=(("SKU1", "43"),), purpose=L.P_ADD, share=1,
                         inventory_item_id=env.shop.variant(other.gid, "43").item, location_id=PROMO,
                         product_id=num(other), sent=True)
    rep = env.apply(env.preview("SKU1", keep))
    assert rep.status == exit_service.ST_APPLIED and other.gid not in env.shop.products
    (r,) = env.register().reconcile_rows(row_ids=[row])
    assert r.status == FAILED and r.outcome["reason"] == "product_deleted"


def test_aa2_an_open_tail_holds_its_keys_and_defers_a_second_revive_of_its_product():
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 0})
    tail = seed_reconcile(w.register(), action=L.A_TAIL, keys=(("SKU1", "43"),), product_id=num(out), sent=True)
    assert ("SKU1", "43") in set(held_keys(w))
    w.shop.fault("bulk", "timeout_before", times=5)              # the tail's re-runs stay unknown
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "DEFERRED"
    assert w.register().reconcile_rows(row_ids=[tail])[0].status == OPEN
    assert w.shop.products[out.gid].status == "DRAFT"


# ---------------------------------------------------------------------------
# T0, T2, T4, M24, AA7, Z14, Z17 — the table's remaining rows
# ---------------------------------------------------------------------------

def test_t0_a_held_keys_delta_zero_group_runs_the_total_rule_and_stamps():
    env, out = _held_env({"42": 0, "43": 0})
    e = env.be.consume(*K, outlet_line=True)                     # Δ 0: Shopify moved itself
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "stamp_only"} and e not in env.unstamped()
    assert env.shop.products[out.gid].status == "DRAFT" and [h.key for h in env.register().held_keys()] == [K]


def test_t2_a_multi_outlet_sku_is_never_added_to():
    w = World()
    (e,) = w.returned()
    w.shop.product("Nike - Outlet", promo={"42": 0})
    w.shop.product("Nike - Outlet bis", promo={"42": 0})
    plan, _rep = w.run()
    assert plan.actions[0].reason == "multi_outlet" and not plan.actions[0].publishable
    assert w.mutated() == [] and w.unstamped() == [e]


def test_t4_a_sku_with_no_outlet_and_no_source_stays_pending_surfaced():
    w = World()
    (e,) = w.returned(sku="GHOST")
    plan, _rep = w.run()
    assert plan.actions[0].reason == "no_source" and w.mutated() == [] and w.unstamped() == [e]


def test_m24_a_resolution_that_raises_is_not_none_the_group_stays_pending(monkeypatch):
    from backend.services import resolvers

    env, _out, e = minus_env()

    def down(_t, _sku):
        raise RuntimeError("resolver down")

    monkeypatch.setattr(resolvers, "outlet_resolver", down)
    rep = env.decrement()
    assert rep.applied == {"SKU1|42": "pending"} and env.unstamped() == [e] and env.shop.count("adjust") == 0


@pytest.mark.parametrize("hold_ends", ["unit_back", "shipped"])
def test_aa7_a_minus_at_zero_under_a_committed_unit_waits_then_applies_or_stamps(hold_ends):
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0, "43": 1}, committed={"42": 1})
    e = env.sold(pairs=2)                                        # N 1
    env.decrement()
    assert env.unstamped() == [e] and env.shop.count("adjust") == 0
    lvl = env.shop.promo(out.gid, "42")
    if hold_ends == "unit_back":
        lvl.committed, lvl.available = 0, 1                      # the draft order is cancelled
    else:
        lvl.committed = 0                                        # it ships: the unit leaves
    env.decrement()
    assert env.unstamped() == [] and env.shop.available(out.gid, "42") == 0
    assert env.shop.count("adjust") == (1 if hold_ends == "unit_back" else 0)


def test_z17_a_revive_sets_online_on_the_pairs_it_published():
    w = World()
    w.returned(n=2)
    w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 0})
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "REVIVED" and {p.online for p in w.register().shelf("SKU1")} == {True}


def test_z14_the_decrement_on_its_own_worker_runs_between_two_skus_of_a_publish():
    w = World()
    w.returned(sku="SKU1")
    w.returned(sku="SKU2")
    w.shop.product("A - Outlet", sku="SKU1", promo={"42": 0})
    b = w.shop.product("B - Outlet", sku="SKU2", promo={"42": 0})
    sold = w.shop.product("C - Outlet", sku="SKU3", promo={"42": 1})
    for _ in range(2):
        w.be.add_shelf_pair("SKU3", "42", reconciled=True)
    plan = w.preview()

    def tick(_s):
        time.sleep(0.002)

    def run_pass():
        reg = w.register()
        return passes.decrement_pass(reg, w.shop, PROMO, lease=PassLease(reg, "decrement", sleep=tick),
                                     sleep=tick, wait_bound_s=20)

    worker = DecrementWorker(run_pass)

    def sale_then_trigger(shop):
        if shop.count("adjust") == 0:                            # SKU1's add: the publish holds the lease
            w.be.consume("SKU3", "42")
            worker.trigger("678434")
            for _ in range(5000):                                # the worker marks itself waiting
                if w.register().lease_state().waiter:
                    return
                time.sleep(0.002)
            raise AssertionError("the decrement never marked itself waiting")

    w.shop.on("adjust", sale_then_trigger)
    reg = w.register()
    rep = outlet_service.publish_apply(reg, w.shop, plan, promo_location_id=PROMO,
                                       lease=PassLease(reg, "publish", sleep=tick), take_bound_s=20)
    for _ in range(5000):
        if not worker.running:
            break
        time.sleep(0.002)
    assert [o.status for o in rep.outcomes] == ["APPLIED", "APPLIED"]
    assert worker.runs == 1 and worker.last["outcome"] == "done"
    item_c, item_b = w.shop.variant(sold.gid, "42").item, w.shop.variant(b.gid, "42").item
    order = [c[1]["input"]["changes"][0]["inventoryItemId"] for c in w.shop.calls if c[0] == "adjust"]
    assert order.index(item_c) < order.index(item_b)             # between SKU1 and SKU2
    assert w.shop.available(sold.gid, "42") == 0 and w.shop.available(b.gid, "42") == 1


# ---------------------------------------------------------------------------
# OJ1–OJ5 — "Inizializza": the orchestrator's judged interpretations, as test cases (D16;
# ~/.claude/racoon-bus/msg/20261006-2032-scansia-f6-sync-scansia.md)
# ---------------------------------------------------------------------------

def test_oj1_a_key_whose_live_level_moved_is_held_and_the_other_keys_applied():
    from tests.test_baseline import outcome
    from backend.services import baseline_service as B

    env = BaselineEnv()
    a = env.shop.product("A - Outlet", sku="A", promo={"42": 0})
    b = env.shop.product("B - Outlet", sku="B", promo={"42": 0})
    env.imported(sku="A", size="42")
    env.imported(sku="B", size="42")
    plan = env.preview()
    lvl = env.shop.promo(b.gid, "42")
    lvl.available, lvl.on_hand = 2, 2                            # a hand edit on B after the preview
    rep = env.apply(plan)
    assert outcome(rep, "A", "42").result == "levelled" and env.shop.available(a.gid, "42") == 1
    assert outcome(rep, "B", "42").reason == B.R_MOVED and env.shop.available(b.gid, "42") == 1
    assert env.invariant() == {}


def test_oj2_a_unit_on_an_outlet_of_no_register_sku_is_listed_as_its_own_class_and_lowered_relatively():
    from tests.test_baseline import by_key
    from backend.services import baseline_service as B

    env = BaselineEnv()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42")
    foreign = env.shop.product("Other - Outlet", sku="ZZ", sizes=("40",), promo={"40": 2, "Solo Personalizzazione": 3},
                               extra_variants=(("Solo Personalizzazione", "ZZ"),))
    plan = env.preview()
    kp = by_key(plan)[("ZZ", "40")]
    assert (kp.action, kp.n, kp.delta) == (B.ACT_LOWER, 0, -2)
    assert [(f.product_id, f.p) for f in kp.outlets] == [(num(foreign), 2)]      # its product, size, live P
    (fix,) = [p for p in plan.products if p.product_id == num(foreign)]
    assert [a for _v, _i, a in fix.custom_lowerings] == [3]                       # rule 7 on the same product
    env.apply(plan)
    item40 = env.shop.variant(foreign.gid, "40").item
    (ch,) = [c[1]["input"]["changes"][0] for c in env.shop.calls
             if c[0] == "adjust" and c[1]["input"]["changes"][0]["inventoryItemId"] == item40]
    assert (ch["delta"], ch["changeFromQuantity"]) == (-2, 2)                     # relative, from the live read
    assert env.shop.available(foreign.gid, "40") == 0
    assert env.shop.available(foreign.gid, "Solo Personalizzazione") == 0


def test_oj3_a_key_held_for_a_committed_unit_is_released_by_a_pass_once_the_unit_is_no_longer_held():
    from backend.services import baseline_service as B

    env = BaselineEnv()
    out = env.shop.product("Nike - Outlet", promo={"42": 0}, committed={"42": 1})
    env.departed(size="42")                                      # N 0, its −1 waiting on a unit only held
    env.run()
    assert env.held() == {K: B.R_UNIT_HELD}
    writes = env.shop.count("adjust")
    env.shop.promo(out.gid, "42").committed = 0                  # the draft order ships
    reg = env.register()
    rep = passes.decrement_pass(reg, env.shop, PROMO, lease=PassLease(reg, "decrement", sleep=noop), sleep=noop)
    assert "n0_released" in rep.report.kinds() and env.unstamped() == [] and held_keys(env) == []
    assert env.shop.count("adjust") == writes and env.shop.count("activate") == 0       # nothing to Shopify


def test_oj4_a_draft_key_at_n_with_its_plus_one_pending_is_zeroed_not_parked():
    from tests.test_baseline import by_key
    from backend.services import baseline_service as B

    env = BaselineEnv()
    out = env.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 1})
    _, e = env.imported(size="42", price_outlet="50.00", price_high="100.00")       # N 1, its +1 pending
    plan = env.preview()
    assert (by_key(plan)[K].action, by_key(plan)[K].delta) == (B.ACT_ZERO, -1)
    env.apply(plan)
    assert env.shop.available(out.gid, "42") == 0 and env.unstamped() == [e] and env.invariant() == {}


def test_oj5_a_relevel_with_a_priceless_shelf_pair_is_refused_whole_naming_it_then_passes_once_priced():
    from decimal import Decimal

    env = BaselineEnv()
    out = env.shop.product("Nike - Outlet", sizes=("43",), promo={"43": 0})
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    p2, _ = env.imported(size="42")                              # no PRICE_HIGH, no PRICE: no compare-at (D25)
    env.run()                                                    # held: size absent
    add_variant(env.shop, out, "SKU1", "42")
    _plan, rep = relevel(env, [K])
    o = result(rep, *K)
    assert o.result == "refused" and o.reason == f"{R.F_PRICE}:no_compare_at:{p2}"
    assert env.shop.available(out.gid, "42") is None and held_keys(env) == [K]
    env.be.db.state.pairs[p2]["PRICE_HIGH"] = Decimal("100.00")  # its full price set on STOCK_Manager's page
    _plan, rep = relevel(env, [K])
    assert result(rep, *K).result == "released" and env.shop.available(out.gid, "42") == 2


# ---------------------------------------------------------------------------
# S1–S10 — §3.5: the orchestrator's judged interpretations, as test cases (D16;
# ~/.claude/racoon-bus/msg/20261006-2050-scansia-f6-sync-scansia.md; S6 is pinned in test_pricing_service.py)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("baseline", [False, True])
def test_s1_a_holders_take_skips_the_held_key_lowering_before_the_baseline_and_runs_it_after(baseline):
    from backend.persistence.records import InsertAudit
    from tests.test_delete_service import Env as DeleteEnv

    env = DeleteEnv(baseline=baseline)
    held = env.shop.product("SKU1 - Outlet", sku="SKU1", promo={"42": 2 if baseline else 1})
    env.be.add_shelf_pair(*K, reconciled=baseline)               # N 1; before the baseline its +1 is unstamped
    env.be.seed_audit(InsertAudit(kind="HOLD", actor="t", action="hold",
                                  payload={"keys": [list(K)], "reason": "outlet deleted", "event_ids": []}))
    out = env.single(env.outlet(sku="SKU2", status="ACTIVE", promo={"42": 0}))
    skipped = "held_key_lowering_skipped" in [i["kind"] for i in out.surfaced]
    assert skipped is (not baseline) and env.shop.available(held.gid, "42") == 1   # never 0; P 2 → N 1 after


def test_s2_a_two_price_sku_blocks_the_publish_until_a_typed_price_resolves_it():
    from decimal import Decimal

    from tests.test_pricing_service import Env as PricingEnv

    env = PricingEnv()
    out = env.outlet(price="90.00", compare_at="100.00")
    env.pair(product=out, price_outlet=Decimal("50.00"))
    env.pair(size="43", product=out, price_outlet=Decimal("55.00"))
    env.be.add_shelf_pair(*K, price_high=Decimal("100.00"))     # a new return: +1

    def publish():
        reg = env.register()
        plan = outlet_service.publish_preview(reg, env.shop, promo_location_id=PROMO)
        return plan, outlet_service.publish_apply(reg, env.shop, plan, promo_location_id=PROMO,
                                                  lease=PassLease(reg, "publish", sleep=noop), take_bound_s=0)

    plan, _rep = publish()
    assert plan.actions[0].reason == "price_conflict" and env.shop.available(out.gid, "42") == 0
    env.apply(pricing_service.MODE_DIRECT, pricing_service.PriceParams(price_by_sku={"SKU1": "60"}))
    _plan, rep = publish()
    assert rep.outcomes[0].status == "APPLIED" and env.shop.available(out.gid, "42") == 1
    assert {(v.price, v.compare_at) for v in env.shop.products[out.gid].variants} == {("60.00", "100.00")}


def test_s3_twenty_percent_on_189_is_151_20_with_a_discount_of_20_00():
    from decimal import Decimal

    from tests.test_pricing_service import Env as PricingEnv
    from tests.test_pricing_service import pct

    env = PricingEnv()
    env.pair(price_high=Decimal("189.00"))
    (d,) = env.preview(pricing_service.MODE_PERCENT, pct(p=0.20)).diffs
    assert (d.price, d.compare_at, d.discount) == ("151.20", "189.00", "20.00")


def _publish(env):
    reg = env.register()
    plan = outlet_service.publish_preview(reg, env.shop, promo_location_id=PROMO)
    return outlet_service.publish_apply(reg, env.shop, plan, promo_location_id=PROMO,
                                        lease=PassLease(reg, "publish", sleep=noop), take_bound_s=0)


def test_s4_a_price_set_while_the_outlet_is_draft_is_pushed_by_its_revives_tail_before_active():
    from decimal import Decimal

    from tests.test_pricing_service import Env as PricingEnv
    from tests.test_pricing_service import pct

    env = PricingEnv()
    out = env.outlet(status="DRAFT")
    env.pair(product=out)
    env.apply(pricing_service.MODE_PERCENT, pct())               # 30 %: the register only (a DRAFT outlet)
    assert env.prices(out) == [("90.00", "100.00")] * 2
    env.be.add_shelf_pair(*K, price_high=Decimal("100.00"))     # a return: the revive
    seen = {}
    env.shop.on("status", lambda shop: seen.setdefault(
        "at_active", [(v.price, v.compare_at) for v in shop.products[out.gid].variants]))
    rep = _publish(env)
    assert rep.outcomes[0].status == "REVIVED" and seen["at_active"] == [("70.00", "100.00")] * 2


def test_s5_a_refused_price_push_is_sent_by_the_next_active_publish():
    from decimal import Decimal

    from tests.test_pricing_service import Env as PricingEnv
    from tests.test_pricing_service import pct

    env = PricingEnv()
    out = env.outlet()
    env.pair(product=out)
    env.shop.fault("bulk", "user_error:INVALID")
    env.apply(pricing_service.MODE_PERCENT, pct())
    assert env.prices(out) == [("90.00", "100.00")] * 2         # Shopify old, the register new
    env.be.add_shelf_pair(*K, price_high=Decimal("100.00"))
    assert _publish(env).outcomes[0].status == "APPLIED"
    assert env.prices(out) == [("70.00", "100.00")] * 2


def test_s7_a_stopped_exit_leaves_the_other_outlet_active_and_online_untouched():
    from tests.test_exit_service import Env as ExitEnv
    from tests.test_exit_service import two_outlets

    env = ExitEnv()
    keep, other = two_outlets(env, committed={"43": 1})
    pid, _ = env.be.add_shelf_pair(*K, reconciled=True, online=True)
    rep = env.apply(env.preview("SKU1", keep))
    assert rep.outcomes[0].status == exit_service.O_STOPPED and other.gid in env.shop.products
    assert env.shop.products[other.gid].status == "ACTIVE" and env.shop.count("status") == 0   # never drafted
    assert env.register().pairs(pair_ids=[pid])[0].online is True


def test_s8_a_draft_continue_outlet_is_normalised_then_deletable_by_the_cleanup():
    from tests.test_delete_service import Env as DeleteEnv

    env = DeleteEnv()
    p = env.outlet(sku="SKU2", policy="CONTINUE")                # DRAFT at 0, CONTINUE: the predicate refuses
    assert delete_service.cleanup_preview(env.register(), env.shop, promo_location_id=PROMO).candidates == ()
    reg = env.register()
    normalize_service.normalize_apply(
        reg, env.shop, normalize_service.normalize_preview(reg, env.shop, promo_location_id=PROMO),
        promo_location_id=PROMO, lease=PassLease(reg, "normalize", sleep=noop), sleep=noop)
    assert all(v.policy == "DENY" and v.tracked for v in env.shop.products[p.gid].variants)
    rep = env.cleanup(gesture="1")
    assert rep.deleted == 1 and p.gid not in env.shop.products


def test_s8_a_customisation_only_promo_that_rose_after_the_preview_is_lowered_from_its_live_value():
    from tests.test_normalize_service import Env as NormalizeEnv

    env = NormalizeEnv()
    p = env.shop.product("A - Outlet", sku="SKU1", promo={"Solo Personalizzazione": 1},
                         extra_variants=(("Solo Personalizzazione", "SKU1"),))
    plan = env.preview()
    lvl = env.shop.promo(p.gid, "Solo Personalizzazione")
    lvl.available, lvl.on_hand = 3, 3                            # it rose after the preview
    env.apply(plan)
    (ch,) = [c[1]["input"]["changes"][0] for c in env.shop.calls if c[0] == "adjust"]
    assert (ch["delta"], ch["changeFromQuantity"]) == (-3, 3)
    assert env.shop.available(p.gid, "Solo Personalizzazione") == 0


def test_s9_a_failed_promo_gate_read_runs_no_pass_and_the_next_trigger_does():
    env, out, e = minus_env()
    env.shop.fault("locations", "timeout_before")
    gen = env.register().lease_state().generation
    worker = DecrementWorker(env.decrement, spawn=lambda fn: fn())
    worker.trigger("678434")
    assert worker.last["outcome"] == "error" and env.unstamped() == [e] and env.shop.count("adjust") == 0
    assert env.register().lease_state().generation == gen        # the lease never taken
    worker.trigger("net")
    assert worker.last["outcome"] == "done" and env.unstamped() == [] and env.shop.available(out.gid, "42") == 1


def test_s10_a_creates_deactivation_whose_location_reads_gone_applies_and_the_group_completes():
    w = World()
    w.returned(n=2)
    _source(w, promo={"42": 5, "43": 5}, mag={"42": 3, "43": 3})

    def location_gone(shop):
        if shop.count("read_level") == 1:                        # the 42 disconnect's rule-8 read
            dup = next(g for g, p in shop.products.items() if p.title == "Nike Air - Outlet")
            del shop.levels[(shop.variant(dup, "42").item, MAG)]

    w.shop.on("read_level", location_gone)
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "APPLIED" and w.unstamped() == []
    deact = rows_by(w, L.A_DEACTIVATE)
    assert deact and all(r.status == DONE for r in deact)
    assert [r.outcome.get("already_inactive") for r in deact].count(True) == 1
