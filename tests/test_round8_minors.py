"""Plan §10's round-8 minors as build test cases (D16; AB1–AB3 mandatory, D18; §4
item 4's "Round 8's targeted re-review" list). The cases the other modules already
pin are named where they live: AB4 / AB11 / AB15 — ``test_close_service.py``;
AB3's zeroing row ``FAILED`` — ``test_outlet_service.py::test_a_revive_stopped_at_a_zeroing_row…``;
AB14 — ``test_relevel.py::test_a_size_absent_hold_is_released…`` (an ACTIVE,
unpublished outlet published by the re-level) and its refusal case; AB2's refusal
on an ACTIVE outlet — ``test_normalize_service.py`` and ``test_close_service.py``
(S13, the watch, is SQL in the standing checks, §5: not this repo's code).
End to end: the real ``ops`` documents on the fake Shopify, the in-memory register."""
from __future__ import annotations

from backend.persistence.records import (
    APPLIED,
    CARRIED,
    DONE,
    FAILED,
    HELD,
    OPEN,
    SUPERSEDED,
)
from backend.services import close_service as cs
from backend.services import flows, passes
from backend.services import ledger as L
from backend.services import recovery
from backend.services.lease import PassLease

from tests.fake_shop import PROMO, SimulatedCrashInShop
from tests.register_fixtures import seed_reconcile
from tests.test_outlet_service import World

K = ("SKU1", "42")
WINDOW_PAST_MS = 23 * 3600 * 1000 + 1000


def num(product) -> int:
    return flows.product_number(product.gid)


def rows_by(w: World, action=None, purpose=None):
    return [r for r in w.rows() if (action is None or r.action == action)
            and (purpose is None or L.purpose(r) == purpose)]


def decrement(w: World):
    reg = w.register()
    return passes.decrement_pass(reg, w.shop, PROMO, lease=PassLease(reg, "decrement", sleep=lambda s: None),
                                 sleep=lambda s: None)


def run_recovery(w: World, klass: str):
    reg = w.register()
    lease = PassLease(reg, "t", sleep=lambda s: None)
    lease.take()
    try:
        ctx = flows.Context(L.Ledger(reg, w.shop, lease, PROMO, sleep=lambda s: None))
        return recovery.recover(ctx, klass), ctx
    finally:
        lease.release()


# ---------------------------------------------------------------------------
# AB1 — an APPLIED row beside a sibling closed HELD closes CARRIED
# ---------------------------------------------------------------------------

def test_ab1_an_activation_applied_beside_its_add_closed_held_is_carried():
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", promo={"42": 0}, inactive=("42",))
    w.shop.fault("adjust", "lose_after")                     # the add landed; its answer lost
    w.run()
    (act,) = rows_by(w, L.A_ACTIVATE)
    (add,) = rows_by(w, L.A_ADJUST, L.P_ADD)
    assert (act.status, add.status) == (APPLIED, OPEN)
    w.be.advance(WINDOW_PAST_MS)
    w.run()                                                    # recovery: an unknown +Δ past the window
    rows = {r.row_id: r for r in w.rows()}
    assert rows[add.row_id].status == HELD and rows[act.row_id].status == CARRIED
    assert w.unstamped() == [e] and K in {h.key for h in w.register().held_keys()}
    assert w.shop.available(out.gid, "42") == 1                # never re-added


def test_ab1_a_minus_split_member_closed_by_a_person_carries_its_applied_sibling():
    from tests.test_decrement import Env

    env = Env()
    a = env.shop.product("Nike - Outlet", promo={"42": 1})
    b = env.shop.product("Nike - Outlet bis", status="DRAFT", promo={"42": 1})
    env.be.add_shelf_pair(*K, reconciled=True)
    env.be.add_shelf_pair(*K, reconciled=True)
    env.be.consume(*K)
    env.be.consume(*K)                                         # Δ −2: split over the two outlets, ACTIVE first
    item_b = b.variants[0].item
    env.shop.fault("adjust", "lose_after", when=lambda v: v["input"]["changes"][0]["inventoryItemId"] == item_b)
    env.decrement()
    rows = sorted(env.rows(), key=lambda r: r.row_id)
    applied = next(r for r in rows if r.product_id == num(a))
    stuck = next(r for r in rows if r.product_id == num(b))
    assert (applied.status, stuck.status) == (APPLIED, OPEN)
    env.shop.fault("read_level", "refuse", times=50)           # recovery cannot resolve it: stays OPEN
    reg = env.register()
    rep = cs.close_row(reg, env.shop, stuck.row_id, expect_status=OPEN, why="checked by hand",
                       promo_location_id=PROMO, lease=PassLease(reg, "close", sleep=lambda s: None),
                       sleep=lambda s: None)
    assert rep.status == cs.ST_CLOSED and rep.closed_as == HELD
    now = {r.row_id: r.status for r in env.rows()}
    assert now[stuck.row_id] == HELD and now[applied.row_id] == CARRIED
    assert set(env.unstamped()) == set(stuck.event_ids)       # held by the row: never re-grouped, never stamped


def test_ab1_a_revives_zeroing_rows_applied_beside_an_add_closed_held_are_carried():
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 2})
    item42 = w.shop.variant(out.gid, "42").item
    w.shop.fault("adjust", "lose_after", when=lambda v: v["input"]["changes"][0]["inventoryItemId"] == item42)
    w.run()
    (zero,) = rows_by(w, L.A_ADJUST, L.P_ZERO)
    (add,) = rows_by(w, L.A_ADJUST, L.P_ADD)
    assert zero.status == APPLIED and add.status == OPEN
    w.be.advance(WINDOW_PAST_MS)
    w.run()
    rows = {r.row_id: r for r in w.rows()}
    assert rows[add.row_id].status == HELD and rows[zero.row_id].status == CARRIED
    assert w.unstamped() == [e]


# ---------------------------------------------------------------------------
# AB3 — the tail, written ahead, closes FAILED "never run"; the gate's states
# ---------------------------------------------------------------------------

def test_ab3_a_revive_ended_at_a_zeroing_row_still_open_closes_its_tail_never_run():
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 2})
    w.shop.fault("adjust", "timeout_before")                  # the zeroing's outcome unknown: OPEN
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "REVIVE_STOPPED"
    (tail,) = rows_by(w, L.A_TAIL)
    (zero,) = rows_by(w, L.A_ADJUST, L.P_ZERO)
    assert tail.status == FAILED and tail.outcome["never_run"] is True and zero.status == OPEN
    assert w.shop.products[out.gid].status == "DRAFT"
    plan2, _rep2 = w.run()             # the key in flight under the zeroing row: nothing planned; the apply's
    assert plan2.actions == ()         # recovery re-sends the zeroing (a preview is a read, it recovers nothing)
    assert w.register().reconcile_rows(row_ids=[zero.row_id])[0].status == CARRIED   # beside its FAILED siblings
    _plan3, rep3 = w.run()                                     # the retry: a new revive, not deferred by the tail
    assert rep3.outcomes[0].status == "REVIVED" and w.shop.available(out.gid, "42") == 1


def test_ab3_a_crash_leaving_the_tail_never_sent_is_closed_never_run_by_recovery():
    w = World()
    w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 0, "43": 2})
    crashed = {"n": 0}

    def crash(shop):
        if not crashed["n"]:
            crashed["n"] += 1
            raise SimulatedCrashInShop("killed right after the revive's write-ahead")

    w.shop.on("read_level", crash)
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "ERROR"
    assert {r.action: r.sent_at for r in w.rows()}.get(L.A_TAIL) is None
    w.run()                                                    # the apply's recovery closes the dead rows
    rows = sorted(w.rows(), key=lambda r: r.row_id)
    first_tail = next(r for r in rows if r.action == L.A_TAIL)
    assert first_tail.status == FAILED and first_tail.outcome.get("never_run") is True
    assert all(r.status == FAILED for r in rows if r.action == L.A_ADJUST)        # never sent
    _plan3, rep3 = w.run()
    assert rep3.outcomes[0].status == "REVIVED" and w.shop.products[out.gid].status == "ACTIVE"


def test_ab3_a_zeroing_row_carried_or_superseded_by_an_applied_successor_passes_the_gate():
    w = World()
    reg = w.register()
    spec = dict(action=L.A_ADJUST, keys=(K,), purpose=L.P_ZERO, target=0, inventory_item_id="i", location_id=PROMO)
    carried = seed_reconcile(reg, group_id=31, sent=True, status=CARRIED, outcome={"moved": -1}, **spec)
    old = seed_reconcile(reg, group_id=31, sent=True, status=SUPERSEDED, **spec)
    succ = seed_reconcile(reg, group_id=31, sent=True, status=APPLIED, outcome={"moved": -1}, **spec)
    from backend.persistence.records import UpdateAudit
    from tests.register_fixtures import StubLease

    stub = StubLease(reg)
    reg.close_rows(stub, [UpdateAudit(row_id=old, expect_status=SUPERSEDED, outcome={"successor": succ})])
    reg.lease_release(stub)
    ctx = flows.Context(L.Ledger(reg, w.shop, None, PROMO))
    assert flows.zeroing_applied(ctx, 31) is True and carried
    seed_reconcile(reg, group_id=32, sent=True, status=FAILED, **spec)
    assert flows.zeroing_applied(ctx, 32) is False
    seed_reconcile(reg, group_id=33, sent=True, **spec)
    assert flows.zeroing_applied(ctx, 33) is None


# ---------------------------------------------------------------------------
# AB5, AB6 — the normalise row: SENT_AT before each call; a never-sent one owed to publish
# ---------------------------------------------------------------------------

def test_ab5_a_normalise_rows_sent_at_and_step_are_written_before_its_call():
    from backend.services import normalize_service as ns

    w = World()
    p = w.shop.product("Nike - Outlet", policy="CONTINUE")
    seen = {}

    def at_bulk(shop):
        (row,) = rows_by(w, L.A_NORMALISE)
        seen.update(sent=row.sent_at, step=row.step)

    w.shop.on("bulk", at_bulk)
    reg = w.register()
    ns.normalize_apply(reg, w.shop, ns.normalize_preview(reg, w.shop, promo_location_id=PROMO),
                       promo_location_id=PROMO, lease=PassLease(reg, "normalize", sleep=lambda s: None),
                       sleep=lambda s: None)
    assert seen["sent"] is not None and seen["step"] == "1:normalise"
    (row,) = rows_by(w, L.A_NORMALISE)
    assert row.status == DONE and row.step == "1:normalise:ok" and num(p) == row.product_id


def test_ab5_a_sent_add_whose_product_reads_gone_is_closed_by_ii_before_its_own_bullet():
    w = World()
    out = w.shop.product("Nike - Outlet", promo={"42": 0})
    add = seed_reconcile(w.register(), action=L.A_ADJUST, keys=(K,), purpose=L.P_ADD, share=1,
                         inventory_item_id=w.shop.variant(out.gid, "42").item, location_id=PROMO,
                         product_id=num(out), sent=True)
    del w.shop.products[out.gid]                               # the product (and its items) gone
    run_recovery(w, recovery.CLASS_PUBLISH)                    # in window: its own bullet would re-send
    (r,) = w.register().reconcile_rows(row_ids=[add])
    assert r.status == HELD and r.outcome["reason"] == "inventory_item_gone" and w.shop.count("adjust") == 0


def test_ab6_a_never_sent_normalise_row_is_left_by_the_decrement_and_run_by_the_publish():
    w = World()
    p = w.shop.product("Nike - Outlet", policy="CONTINUE")
    row = seed_reconcile(w.register(), action=L.A_NORMALISE, keys=(K,), product_id=num(p))
    rep = decrement(w)
    assert w.register().reconcile_rows(row_ids=[row])[0].status == OPEN and "left_for_publish" in rep.report.kinds()
    w.run()                                                    # a publish-class holder's recovery runs it
    assert w.register().reconcile_rows(row_ids=[row])[0].status == DONE
    assert all(v.policy == "DENY" for v in w.shop.products[p.gid].variants)


# ---------------------------------------------------------------------------
# AB7 — (ii) on an APPLIED row → CARRIED; on a sent −Δ → FAILED "possibly applied"
# ---------------------------------------------------------------------------

def test_ab7_ii_on_an_applied_row_carries_it_and_the_remainder_is_stamped_next_pass():
    w = World()
    gone = w.shop.product("Nike - Outlet", promo={"42": 1})
    w.be.add_shelf_pair(*K, reconciled=True)
    w.be.add_shelf_pair(*K, reconciled=True)
    e = w.be.consume(*K)
    applied = seed_reconcile(w.register(), action=L.A_ADJUST, keys=(K,), event_ids=(e,), purpose=L.P_LOWER,
                             share=-1, inventory_item_id=w.shop.variant(gone.gid, "42").item, location_id=PROMO,
                             product_id=num(gone), sent=True, status=APPLIED, outcome={"moved": -1}, group_id=41)
    open_sibling = seed_reconcile(w.register(), action=L.A_ADJUST, keys=(K,), event_ids=(e,), purpose=L.P_LOWER,
                                  share=-1, inventory_item_id=w.shop.variant(gone.gid, "43").item,
                                  location_id=PROMO, product_id=num(gone), sent=True, group_id=41)
    del w.shop.products[gone.gid]
    res, _ctx = run_recovery(w, recovery.CLASS_DECREMENT)
    rows = {r.row_id: r for r in w.rows()}
    assert rows[applied].status == CARRIED and rows[applied].outcome["moved"] == -1
    assert rows[open_sibling].status == FAILED and rows[open_sibling].outcome["possibly_applied"] is True
    assert w.unstamped() == [e]
    rep = decrement(w)                                         # the remainder: −1 − (−1) = 0 → stamped
    assert rep.applied == {"SKU1|42": "stamp_only"} and w.unstamped() == []


# ---------------------------------------------------------------------------
# AB8 — a key held only by a tail / normalise row is neither read nor lowered
# ---------------------------------------------------------------------------

def test_ab8_a_key_held_only_by_a_normalise_row_is_not_lowered_by_the_sweep():
    w = World()
    p = w.shop.product("Nike - Outlet", promo={"42": 2}, policy="CONTINUE")
    w.be.add_shelf_pair(*K, reconciled=True)                   # N 1, P 2: above N
    seed_reconcile(w.register(), action=L.A_NORMALISE, keys=(K,), product_id=num(p))
    decrement(w)
    assert w.shop.available(p.gid, "42") == 2 and w.shop.count("adjust") == 0


# ---------------------------------------------------------------------------
# AB10, AB13, AB16 — activation / deactivation outcomes read off the level
# ---------------------------------------------------------------------------

def test_ab10_a_deactivation_read_absent_is_applied_an_activation_read_absent_is_failed():
    w = World()
    p = w.shop.product("Nike - Outlet", sizes=("42",), absent=("42",))
    item = w.shop.variant(p.gid, "42").item
    deact = seed_reconcile(w.register(), action=L.A_DEACTIVATE, keys=(K,), inventory_item_id=item,
                           location_id=PROMO, product_id=num(p), sent=True, group_id=51)
    act = seed_reconcile(w.register(), action=L.A_ACTIVATE, keys=(K,), inventory_item_id=item,
                         location_id=PROMO, product_id=num(p), sent=True, group_id=52)
    run_recovery(w, recovery.CLASS_DECREMENT)
    rows = {r.row_id: r for r in w.rows()}
    assert rows[deact].status in (APPLIED, DONE) and rows[act].status == FAILED
    assert w.shop.count("activate") == 0 and w.shop.count("deactivate") == 0


def test_ab13_an_activation_open_after_a_crash_is_read_off_is_active_by_the_decrement_never_re_sent():
    w = World()
    p = w.shop.product("Nike - Outlet", sizes=("42",), promo={"42": 0})          # the level reads active
    act = seed_reconcile(w.register(), action=L.A_ACTIVATE, keys=(K,), event_ids=(), inventory_item_id=
                         w.shop.variant(p.gid, "42").item, location_id=PROMO, product_id=num(p), sent=True)
    decrement(w)
    assert w.register().reconcile_rows(row_ids=[act])[0].status in (APPLIED, DONE)
    assert w.shop.count("activate") == 0


def test_ab16_ii_by_kind_variant_or_item_gone_and_an_unreadable_activation():
    w = World()
    p = w.shop.product("Nike - Outlet", sizes=("42", "43", "44"), promo={"42": 1, "43": 0, "44": 1})
    items = {t: w.shop.variant(p.gid, t).item for t in ("42", "43", "44")}
    reg = w.register()
    minus = seed_reconcile(reg, action=L.A_ADJUST, keys=(K,), event_ids=(9,), purpose=L.P_LOWER, share=-1,
                           inventory_item_id=items["42"], location_id=PROMO, product_id=num(p), sent=True,
                           group_id=61)
    act = seed_reconcile(reg, action=L.A_ACTIVATE, keys=(("SKU1", "43"),), inventory_item_id=items["43"],
                         location_id=PROMO, product_id=num(p), sent=True, group_id=62)
    lowering = seed_reconcile(reg, action=L.A_ADJUST, keys=(("SKU1", "44"),), purpose=L.P_ZERO, target=0,
                              inventory_item_id=items["44"], location_id=PROMO, product_id=num(p), sent=True,
                              group_id=63)
    w.shop.products[p.gid].variants = []                       # every variant (so its item) gone
    run_recovery(w, recovery.CLASS_DECREMENT)
    rows = {r.row_id: r for r in w.rows()}
    assert rows[minus].status == FAILED and rows[minus].outcome["possibly_applied"] is True
    assert rows[act].status == HELD and rows[act].outcome["reason"] == "inventory_item_gone"
    assert rows[lowering].status == FAILED and "possibly_applied" not in rows[lowering].outcome


def test_ab7_ii_is_read_first_on_an_applied_row_even_when_its_group_could_stamp():
    """A lone APPLIED row (its group complete: the stamp could run) whose item reads
    gone closes CARRIED by (ii) — read before the APPLIED bullet — and its event is
    stamped by the next pass's remainder (0), never by the stamp retry."""
    w = World()
    gone = w.shop.product("Nike - Outlet", promo={"42": 1})
    w.be.add_shelf_pair(*K, reconciled=True)
    w.be.add_shelf_pair(*K, reconciled=True)
    e = w.be.consume(*K)
    row = seed_reconcile(w.register(), action=L.A_ADJUST, keys=(K,), event_ids=(e,), purpose=L.P_LOWER, share=-1,
                         inventory_item_id=w.shop.variant(gone.gid, "42").item, location_id=PROMO,
                         product_id=num(gone), sent=True, status=APPLIED, outcome={"moved": -1}, group_id=42)
    del w.shop.products[gone.gid]
    run_recovery(w, recovery.CLASS_DECREMENT)
    (r,) = w.register().reconcile_rows(row_ids=[row])
    assert r.status == CARRIED and r.outcome["reason"] == "inventory_item_gone" and w.unstamped() == [e]
    assert decrement(w).applied == {"SKU1|42": "stamp_only"} and w.unstamped() == []
