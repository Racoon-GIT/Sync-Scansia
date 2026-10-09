"""F7.3's tail (brief ``IT/_scansia-programme/briefs/sync-scansia-F7.3-fixes.md``):
CREATE's order on a duplicate whose variants are active only off Promo (S3.8), the
orphan clean-up, the activation body, and the publish check (S2.10). End to end:
the real ``ops`` documents against the stateful fake Shopify, which answers as the
store did in F7.3 run 202610091404 — an item's last active level is never
deactivated (R061), ``available`` with ``onHand`` is refused (R053), a publish on a
DRAFT publishes nothing (R025).

Invariants (the brief's): (a) no ``inventoryDeactivate`` targets a variant's last
active level — the fake records every attempt in ``shop.refused``, so ``[]`` is
the oracle; (b) after CREATE every variant is stocked at Promo only, every
non-Promo level zeroed and deactivated, nothing sellable before step 6; (c) no row
assumes what an activation leaves on a level that held units; (d) rule 2's
recovery covers every row the new order writes.
"""
from __future__ import annotations

from typing import Dict

import pytest

from backend.persistence.records import CARRIED, DONE, FAILED, HELD, OPEN
from backend.services import ledger as L
from backend.services import outlet_service
from backend.shopify import ops

from tests.fake_shop import MAG, PROMO, FakeShop, Level, SimulatedCrashInShop
from tests.test_outlet_service import World, _source
from tests.test_relevel import held_keys
from tests.test_round8_minors import decrement, rows_by

SIZES = ("40", "41", "42", "43")
SHOP = "gid://shopify/Location/SHOP"


def _mixed(w: World):
    """The four shapes a duplicate's variant inherits: Promo + Magazzino (40),
    Magazzino only (41 — 927 of the 932 variants of the 70 CREATE candidates),
    Promo only (42), none (43). Returned: 41 ×1, 42 ×2."""
    events = w.returned(size="41") + w.returned(size="42", n=2)
    _source(w, sizes=SIZES, promo={"40": 2, "42": 1}, absent=("41", "43"), mag={"40": 3, "41": 1})
    return events


def _outlet(shop: FakeShop) -> str:
    return next(g for g, p in shop.products.items() if p.title == "Nike Air - Outlet")


def _levels(shop: FakeShop, gid: str, title: str) -> Dict[str, object]:
    item = shop.variant(gid, title).item
    return {loc: lvl for (it, loc), lvl in shop.levels.items() if it == item}


def _promo_only(shop: FakeShop, gid: str, promo: Dict[str, int]) -> None:
    """(b): every variant's Promo level active, holding its add (0 elsewhere);
    every other level zeroed and inactive."""
    for v in shop.products[gid].variants:
        lv = _levels(shop, gid, v.title)
        assert lv[PROMO].active and lv[PROMO].available == promo.get(v.title, 0), v.title
        assert all(not x.active and x.available == 0 for loc, x in lv.items() if loc != PROMO), v.title


def _orphan_zeroed(shop: FakeShop, gid: str) -> None:
    """The orphan: DRAFT, every level at 0; a non-Promo level left active only as
    its variant's last active one."""
    assert shop.products[gid].status == "DRAFT"
    for v in shop.products[gid].variants:
        lv = _levels(shop, gid, v.title)
        assert all(x.available == 0 for x in lv.values()), v.title
        promo = lv.get(PROMO)
        kept = [loc for loc, x in lv.items() if loc != PROMO and x.active]
        assert len(kept) <= (0 if promo is not None and promo.active else 1), v.title


def _kinds(rep):
    return [s["kind"] for s in rep.surfaced]


# =============================================================================
# The fake answers as the store did
# =============================================================================

def test_the_fake_refuses_an_items_last_active_level_with_the_stores_message():
    shop = FakeShop()
    p = shop.product("X", sizes=("42", "43"), absent=("42",), mag={"42": 1, "43": 1})
    only = shop.levels[(shop.variant(p.gid, "42").item, MAG)]               # 42: Magazzino only
    ans = ops.send_inventory(shop, ops.deactivate_request(only.id))
    assert not ans.ok and ans.codes == ()                                    # plain UserError, no code
    assert ans.errors[0]["message"] == ("The variant couldn't be unstocked from Magazzino because variants "
                                        "need to be stocked at a minimum of 1 location.")
    assert only.active and only.available == 1 and [op for op, _v in shop.refused] == ["deactivate"]
    beside = shop.levels[(shop.variant(p.gid, "43").item, MAG)]             # 43: Promo + Magazzino
    assert ops.send_inventory(shop, ops.deactivate_request(beside.id)).ok and not beside.active


def test_the_fake_refuses_available_with_on_hand_and_sets_available_alone():
    shop = FakeShop()
    p = shop.product("X", sizes=("42",), absent=("42",), mag={"42": 1})
    item = shop.variant(p.gid, "42").item
    both = ops.request_body(
        "mutation($inventoryItemId: ID!, $locationId: ID!, $available: Int, $onHand: Int, $idempotencyKey: String!) "
        "{ inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId, available: $available, "
        "onHand: $onHand) @idempotent(key: $idempotencyKey) { inventoryLevel { id isActive } userErrors { field message } } }",
        {"inventoryItemId": item, "locationId": PROMO, "available": 0, "onHand": 0, "idempotencyKey": "k0"})
    ans = ops.send_inventory(shop, both)
    assert not ans.ok and [e["field"] for e in ans.errors] == [["available"], ["onHand"]]
    assert "not allowed to set available and on_hand quantities at the same time" in ans.errors[0]["message"]
    assert (item, PROMO) not in shop.levels
    assert ops.send_inventory(shop, ops.activate_request(item, PROMO, "k1")).ok
    assert shop.levels[(item, PROMO)].active and shop.levels[(item, PROMO)].available == 0


# =============================================================================
# (a) + (b): the order
# =============================================================================

def test_a_create_on_a_duplicate_active_only_off_promo_applies_and_ends_promo_only():
    w = World()
    events = w.returned(n=2)
    _source(w, absent=("42", "43"), mag={"42": 3, "43": 0})                 # every variant: Magazzino only
    _plan, rep = w.run()
    (o,) = rep.outcomes
    assert o.status == "APPLIED" and sorted(o.stamped_event_ids) == sorted(events) and w.unstamped() == []
    dup = _outlet(w.shop)
    _promo_only(w.shop, dup, {"42": 2})
    assert w.shop.refused == []                                              # (a): nothing refused
    assert w.shop.products[dup].status == "ACTIVE" and w.shop.products[dup].published
    names = w.shop.names()
    acts = [i for i, n in enumerate(names) if n == "activate"]
    writes = [i for i, n in enumerate(names) if n in ("deactivate", "adjust")]
    assert len(acts) == 2 and max(acts) < min(writes)                       # every activation first
    assert any(n == "read_inventory" for n in names[max(acts):min(writes)])  # then the re-read
    assert all(r.status == DONE for r in rows_by(w, L.A_ACTIVATE))
    (create,) = rows_by(w, L.A_CREATE)
    assert "publish_confirmed" not in create.outcome and "publish_unconfirmed" not in _kinds(rep)


def test_mixed_variants_promo_and_off_promo_off_promo_only_promo_only_none_all_end_promo_only():
    w = World()
    events = _mixed(w)
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "APPLIED" and w.unstamped() == [] and len(events) == 3
    dup = _outlet(w.shop)
    _promo_only(w.shop, dup, {"41": 1, "42": 2})
    assert w.shop.refused == []
    activated = {c[1]["inventoryItemId"] for c in w.shop.calls if c[0] == "activate"}
    assert activated == {w.shop.variant(dup, s).item for s in ("41", "43")}   # absent Promo only
    assert "last_active_level_kept" not in _kinds(rep)


def test_without_an_activation_the_create_keeps_fix1s_calls_no_re_read():
    w = World()
    w.returned(n=2)
    _source(w, promo={"42": 5, "43": 5}, mag={"42": 3, "43": 3})             # Promo active everywhere
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "APPLIED" and w.shop.count("activate") == 0
    assert w.shop.count("read_inventory") == 1 and w.shop.refused == []
    _promo_only(w.shop, _outlet(w.shop), {"42": 2})


# =============================================================================
# (c): an activation's effect on held units is never assumed
# =============================================================================

@pytest.mark.parametrize("keeps", [False, True], ids=["activation_drops", "activation_keeps"])
def test_an_inactive_promo_level_holding_units_ends_at_its_add_whatever_the_activation_left(keeps):
    w = World()
    w.returned()                                                             # 42 ×1
    _source(w, promo={"42": 2, "43": 3}, inactive=("42", "43"), mag={"42": 1, "43": 1})
    w.shop.activation_keeps_units = keeps
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "APPLIED" and w.unstamped() == []
    dup = _outlet(w.shop)
    _promo_only(w.shop, dup, {"42": 1})                                      # never 3 or 4: no phantom unit
    promo_zeros = [r for r in rows_by(w, L.A_ADJUST, L.P_ZERO) if L.payload(r)["location_id"] == PROMO]
    assert len(promo_zeros) == (2 if keeps else 0)                           # composed from the re-read
    assert all(r.status == DONE for r in promo_zeros) and w.shop.refused == []


# =============================================================================
# (a) in the orphan clean-up
# =============================================================================

def test_an_activation_refused_stops_the_activations_and_the_orphan_keeps_each_last_level():
    w = World()
    (e,) = w.returned()
    src = _source(w, sizes=("42", "43", "44"), promo={"43": 0}, inactive=("43",), absent=("42", "44"),
                  mag={"42": 3, "43": 0, "44": 2})
    w.shop.locations[SHOP] = "Negozio"                                        # 44: Magazzino + a third location
    w.shop.levels[(w.shop.variant(src.gid, "44").item, SHOP)] = Level("gid://shopify/InventoryLevel/9", True, 1, 0, 1)
    w.shop.fault("activate", "user_error:X", when=lambda v: w.shop.count("activate") == 2)   # 43's inactive Promo
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "CREATE_ABORTED"
    assert "create_aborted:activation_not_applied" in rep.outcomes[0].warnings
    assert w.shop.count("activate") == 2                                     # the third never sent
    (create,) = rows_by(w, L.A_CREATE)
    assert create.status == HELD and ("SKU1", "42") in held_keys(w) and w.unstamped() == [e]
    dup = outlet_service.product_gid(create.product_id)
    _orphan_zeroed(w.shop, dup)
    assert w.shop.refused == []                                              # (a): no last level sent
    assert not _levels(w.shop, dup, "42")[MAG].active                        # 42: Promo active → disconnected
    assert _levels(w.shop, dup, "43")[MAG].active                            # 43: its Promo still inactive
    assert not _levels(w.shop, dup, "44")[MAG].active and _levels(w.shop, dup, "44")[SHOP].active
    kept = [s for s in rep.surfaced if s["kind"] == "last_active_level_kept"]
    assert sorted((s["variant_id"], s["location_id"]) for s in kept) == sorted(
        [(w.shop.variant(dup, "43").id, MAG), (w.shop.variant(dup, "44").id, SHOP)])
    acts = rows_by(w, L.A_ACTIVATE)
    assert [r.status for r in acts] == [CARRIED, FAILED, FAILED]            # applied / read inactive / never sent
    assert acts[1].outcome.get("read_active") is False and acts[2].outcome.get("never_sent") is True
    assert all(r.status != OPEN for r in w.rows())


def test_a_promo_level_found_inactive_after_the_activations_aborts_before_any_deactivation():
    w = World()
    _mixed(w)
    flipped = []

    def flip(shop):
        if shop.count("activate") == 2 and not flipped:                     # right before the re-read
            flipped.append(1)
            shop.levels[(shop.variant(_outlet(shop), "43").item, PROMO)].active = False

    w.shop.on("read_inventory", flip)
    _plan, rep = w.run()
    assert flipped and rep.outcomes[0].status == "CREATE_ABORTED"
    assert "create_aborted:activation_not_applied" in rep.outcomes[0].warnings
    (create,) = rows_by(w, L.A_CREATE)
    assert create.status == HELD and w.shop.refused == []
    _orphan_zeroed(w.shop, outlet_service.product_gid(create.product_id))


REFUSALS = [
    ("activate", None),                                                          # the first activation
    ("deactivate", None),                                                        # a disconnect
    ("adjust", lambda v: v["input"]["changes"][0]["locationId"] == MAG),        # its zeroing
    ("adjust", lambda v: v["input"]["changes"][0]["locationId"] == PROMO
     and v["input"]["changes"][0]["delta"] < 0),                                 # a Promo zeroing
    ("adjust", lambda v: v["input"]["changes"][0]["delta"] > 0),                # an add
    ("bulk", None),                                                              # step 5
    ("status", None),                                                            # step 6: ACTIVE
    ("publish", None),                                                           # step 6: publish
]


@pytest.mark.parametrize("op,when", REFUSALS, ids=[f"{op}{i}" for i, (op, _w) in enumerate(REFUSALS)])
def test_a_create_refused_at_each_step_leaves_an_orphan_zeroed_with_no_last_level_deactivated(op, when):
    w = World()
    events = _mixed(w)
    w.shop.fault(op, "user_error:X", when=when)
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "CREATE_ABORTED"
    (create,) = rows_by(w, L.A_CREATE)
    assert create.status == HELD and create.product_id is not None
    _orphan_zeroed(w.shop, outlet_service.product_gid(create.product_id))
    assert w.shop.refused == []
    assert {("SKU1", "41"), ("SKU1", "42")} <= set(held_keys(w)) and sorted(w.unstamped()) == sorted(events)
    assert all(r.status != OPEN for r in w.rows())


# =============================================================================
# (d): a crash in each window of the new order — the decrement reads, the publish heals
# =============================================================================

def _crash(op: str, nth: int, after_activation: bool = False):
    fired = []

    def hook(shop):
        if fired or shop.count(op) != nth or (after_activation and shop.count("activate") == 0):
            return
        fired.append(1)
        raise SimulatedCrashInShop(f"killed right before {op} #{nth}")

    return hook


CRASHES = [("activate", 0, False), ("activate", 1, False), ("read_inventory", 1, True), ("deactivate", 0, False),
           ("adjust", 0, False), ("adjust", 3, False), ("bulk", 0, False), ("status", 0, False),
           ("publish", 0, False)]


@pytest.mark.parametrize("op,nth,after", CRASHES, ids=[f"{op}{n}" for op, n, _a in CRASHES])
def test_a_crash_in_each_window_of_the_new_order_is_read_by_the_decrement_and_healed_by_the_publish(op, nth, after):
    w = World()
    events = _mixed(w)
    w.shop.on(op, _crash(op, nth, after))
    w.run()
    (create,) = rows_by(w, L.A_CREATE)
    assert create.status == OPEN and create.sent_at is not None
    sent = w.shop.count("activate")
    rep = decrement(w)                                                       # decrement class: reads only
    assert w.register().reconcile_rows(row_ids=[create.row_id])[0].status == OPEN
    assert "left_for_publish" in rep.report.kinds() and w.shop.count("activate") == sent
    assert all(r.status != OPEN for r in rows_by(w, L.A_ACTIVATE))           # read off isActive
    w.run()                                                                  # a publish-class recovery
    create = w.register().reconcile_rows(row_ids=[create.row_id])[0]
    assert create.status == HELD and set(events) <= set(w.unstamped())
    assert {("SKU1", "41"), ("SKU1", "42")} <= set(held_keys(w))
    _orphan_zeroed(w.shop, outlet_service.product_gid(create.product_id))
    assert w.shop.refused == [] and all(r.status != OPEN for r in w.rows())


# =============================================================================
# S2.10: the publish check
# =============================================================================

def test_publish_confirmed_reads_published_on_publication_only():
    assert ops.publish_confirmed({"publishable": {"publishedOnPublication": True}}) is True
    assert ops.publish_confirmed({"publishable": {"publishedOnPublication": False,
                                                  "resourcePublicationsCount": {"count": 0}}}) is False
    assert ops.publish_confirmed({"publishable": None}) is False and ops.publish_confirmed({}) is False


def test_a_create_whose_publish_answers_unconfirmed_is_surfaced_and_recorded_not_aborted():
    w = World()
    events = w.returned(n=2)
    _source(w)
    w.shop.on("publish", lambda shop: setattr(shop.products[_outlet(shop)], "status", "DRAFT"))   # S2.10's no-op
    _plan, rep = w.run()
    (o,) = rep.outcomes
    assert o.status == "APPLIED" and sorted(o.stamped_event_ids) == sorted(events)
    (create,) = rows_by(w, L.A_CREATE)
    assert create.status == DONE and create.step == "6:publish:ok"
    assert create.outcome["publish_confirmed"] is False
    (s,) = [s for s in rep.surfaced if s["kind"] == "publish_unconfirmed"]
    assert s == {"kind": "publish_unconfirmed", "row_id": create.row_id, "product_id": create.product_id}
    assert not w.shop.products[_outlet(w.shop)].published


def test_a_revive_tail_whose_publish_answers_unconfirmed_is_surfaced_and_recorded():
    w = World()
    (e,) = w.returned()
    out = w.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 1, "43": 2})
    w.shop.on("publish", lambda shop: setattr(shop.products[out.gid], "status", "DRAFT"))
    _plan, rep = w.run()
    assert rep.outcomes[0].status == "REVIVED" and w.unstamped() == []
    (tail,) = rows_by(w, L.A_TAIL)
    assert tail.status == DONE and tail.step == "3:publish:ok" and tail.outcome["publish_confirmed"] is False
    assert [s["row_id"] for s in rep.surfaced if s["kind"] == "publish_unconfirmed"] == [tail.row_id]
