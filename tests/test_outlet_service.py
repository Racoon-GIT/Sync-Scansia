"""backend.services.outlet_service — PUBLISH orchestration on the register.

Every collaborator is in-memory: the register is the fake store
(``backend.persistence.memory_store``, MySQL semantics), resolvers and ops are
patched on their module objects so a single ``OpsMock`` records the exact
orchestration call ORDER (the point of the anti-phantom-stock branch). NO HTTP,
NO live store, NO database.

These are the sheet-era publish tests ported to the register — the parity oracle
of the positive-delta flows (plan §4 step 2): CREATE order, phantom-stock
isolation, ACTIVE no-re-inflate, TOCTOU VERIFY_FAILED, FIX4, size-primary
matching (now exact, §3.2), DRY_RUN read-only, the post-review fixes. Retired
with the ``frozen_pre`` branches (§3.3, not ported): the sheet era's
"delta exactly-once" pair (compare-mismatch "already applied" / strict
mismatch) — the register stamps events, and §4 step 4 replaces the
compare-with-frozen decision by a relative write with ``changeFromQuantity``.
"""
from __future__ import annotations

from typing import Dict, List

import pytest

from backend.persistence.records import (
    DONE,
    HOLD_BASELINE,
    BaselineMissing,
    InsertAudit,
)
from backend.services import outlet_service
from backend.services import resolvers
from backend.services.lease import PassLease
from backend.shopify import ops

from tests.register_fixtures import MemoryBackend

PROMO = "gid://shopify/Location/PROMO"
MAG = "gid://shopify/Location/MAG"
PUB = "gid://shopify/Publication/OS"
OUT = "gid://shopify/Product/500"

_MUTATION_OPS = {
    "product_duplicate",
    "inventory_set_quantities",
    "inventory_deactivate",
    "product_variants_bulk_update",
    "product_update_status",
    "product_publish",
}


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

class World:
    """The register (fake) with its whole-register baseline already recorded."""

    def __init__(self) -> None:
        self.be = MemoryBackend()
        self.be.seed_baseline()

    def returned(self, sku, size, n=1, *, price_outlet="50.00", price_high="100.00", **kw) -> List[int]:
        """``n`` pairs of (sku, size) on the shelf, each with its pending +1."""
        out = []
        for _ in range(n):
            _, eid = self.be.add_shelf_pair(sku, size, price_outlet=price_outlet,
                                            price_high=price_high, **kw)
            out.append(eid)
        return out

    def register(self):
        return self.be.register(actor="tester")

    def unstamped(self) -> List[int]:
        return [e.event_id for e in self.be.store().read_unstamped_events()]

    def writes(self) -> List[str]:
        return [x[0] for x in self.be.db.log if x[0] in ("renew", "insert_audit", "stamp", "update_pairs",
                                                          "update_audit", "commit")]


def _level(loc_id, available, on_hand=None, committed=0):
    return {"location_id": loc_id, "location_name": loc_id.split("/")[-1],
            "available": available, "committed": committed,
            "on_hand": available if on_hand is None else on_hand}


def _inv_variant(vid, size, item_id, levels, policy="DENY", truncated=False, sku=None):
    return {"id": vid, "sku": sku, "title": size, "inventoryPolicy": policy,
            "selectedOptions": [{"name": "Size", "value": size}],
            "inventoryItemId": item_id, "levels": levels, "levels_truncated": truncated}


def _src_variant(vid, size, item_id, sku="SKU1"):
    return {"id": vid, "sku": sku, "title": size, "price": "100.00",
            "compareAtPrice": "100.00", "inventoryItem": {"id": item_id},
            "selectedOptions": [{"name": "Size", "value": size}]}


def _outlet_match(gid, status):
    return {"product_gid": gid, "title": "Nike - Outlet", "handle": "nike-outlet",
            "status": status, "is_outlet_member": True, "title_is_outlet": True,
            "review": False, "matched_variant_gids": [gid + "/v"]}


def _source_match(gid, title="Nike Air"):
    return {"product_gid": gid, "title": title, "handle": "nike-air",
            "status": "ACTIVE", "is_outlet_member": False, "title_is_outlet": False,
            "review": False, "matched_variant_gids": [gid + "/v"]}


class OpsMock:
    """Patches every ops.* / resolvers.* used by the service and records calls."""

    def __init__(self, monkeypatch, *, inv_by_gid=None, source_variants=None,
                 outlet=None, source=None, new_gid="gid://shopify/Product/900",
                 outlet_by_sku=None, source_by_sku=None, read_inv_raises=None):
        self.calls: List[tuple] = []
        self.inv_by_gid: Dict[str, list] = inv_by_gid or {}
        self.source_variants: list = source_variants or []
        self.new_gid = new_gid
        self.outlet = outlet if outlet is not None else {"matches": [], "warning": None}
        self.source = source if source is not None else {"matches": [], "warning": None}
        self.outlet_by_sku: Dict[str, dict] = outlet_by_sku or {}
        self.source_by_sku: Dict[str, dict] = source_by_sku or {}
        self.status_by_gid: Dict[str, str] = {}
        self.status_history: List[tuple] = []
        self.read_inv_raises: Dict[str, Exception] = read_inv_raises or {}
        self.on_set = None  # hook: called on each inventory_set_quantities

        monkeypatch.setattr(resolvers, "outlet_resolver", lambda t, sku: self.outlet_by_sku.get(sku, self.outlet))
        monkeypatch.setattr(resolvers, "source_resolver", lambda t, sku: self.source_by_sku.get(sku, self.source))
        monkeypatch.setattr(ops, "product_duplicate", self._duplicate)
        monkeypatch.setattr(ops, "read_variant_inventory", self._read_inv)
        monkeypatch.setattr(ops, "get_product_variants", self._get_variants)
        monkeypatch.setattr(ops, "inventory_set_quantities", self._set_qty)
        monkeypatch.setattr(ops, "inventory_deactivate", self._deactivate)
        monkeypatch.setattr(ops, "product_variants_bulk_update", self._bulk)
        monkeypatch.setattr(ops, "product_update_status", self._status)
        monkeypatch.setattr(ops, "product_publish", self._publish)
        monkeypatch.setattr(ops, "get_online_store_publication_id", lambda t: PUB)

    def _duplicate(self, t, src, title):
        self.calls.append(("product_duplicate", src, title))
        self.status_by_gid[self.new_gid] = "DRAFT"
        self.status_history.append((self.new_gid, "DRAFT"))
        return self.new_gid

    def _read_inv(self, t, gid):
        self.calls.append(("read_variant_inventory", gid))
        if gid in self.read_inv_raises:
            raise self.read_inv_raises[gid]
        return [dict(v) for v in self.inv_by_gid.get(gid, [])]

    def _get_variants(self, t, gid):
        self.calls.append(("get_product_variants", gid))
        return list(self.source_variants)

    def _set_qty(self, t, item, loc, qty, *a, **k):
        self.calls.append(("inventory_set_quantities", item, loc, qty))
        if self.on_set is not None:
            self.on_set(item, loc, qty)
        return {}

    def _deactivate(self, t, item, loc):
        self.calls.append(("inventory_deactivate", item, loc))

    def _bulk(self, t, gid, variants):
        self.calls.append(("product_variants_bulk_update", gid, variants))
        return {}

    def _status(self, t, gid, status):
        self.calls.append(("product_update_status", gid, status))
        self.status_by_gid[gid] = status
        self.status_history.append((gid, status))
        return {}

    def _publish(self, t, gid, pub):
        self.calls.append(("product_publish", gid, pub))
        return {}

    @property
    def names(self) -> List[str]:
        return [c[0] for c in self.calls]

    def idx(self, name) -> List[int]:
        return [i for i, c in enumerate(self.calls) if c[0] == name]

    def sets(self, name="inventory_set_quantities") -> List[tuple]:
        return [c for c in self.calls if c[0] == name]


def _dup_inv_two_sizes():
    """Duplicate inherits stock on Promo (both sizes) AND Magazzino (non-Promo)."""
    return [
        _inv_variant("gid/DUP/v42", "42", "gid/DUP/i42", [_level(PROMO, 5), _level(MAG, 3)]),
        _inv_variant("gid/DUP/v43", "43", "gid/DUP/i43", [_level(PROMO, 5), _level(MAG, 3)]),
    ]


def _preview_apply(w: World, **kw):
    reg = w.register()
    plan = outlet_service.publish_preview(reg, object(), promo_location_id=PROMO)
    report = outlet_service.publish_apply(reg, object(), plan, promo_location_id=PROMO, **kw)
    return plan, report


# ---------------------------------------------------------------------------
# 1) CREATE — exact anti-phantom-stock ORDER
# ---------------------------------------------------------------------------

def test_create_branch_exact_order(monkeypatch):
    w = World()
    events = w.returned("SKU1", "42", 2)
    m = OpsMock(
        monkeypatch,
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42"), _src_variant("V43", "43", "IIT43")],
        inv_by_gid={"gid://shopify/Product/900": _dup_inv_two_sizes()},
    )
    plan, report = _preview_apply(w)
    assert plan.actions[0].branch == "CREATE"
    assert report.outcomes[0].status == "APPLIED"
    assert sorted(report.outcomes[0].stamped_event_ids) == sorted(events)

    names = m.names
    dup_i = m.idx("product_duplicate")
    assert len(dup_i) == 1
    dup_i = dup_i[0]
    inv_mut = [i for i, c in enumerate(m.calls) if c[0] in _MUTATION_OPS - {"product_duplicate"}]
    assert dup_i < min(inv_mut)
    last_deact = max(m.idx("inventory_deactivate"))
    promo_zeros = [i for i, c in enumerate(m.calls)
                   if c[0] == "inventory_set_quantities" and c[2] == PROMO and c[3] == 0]
    assert last_deact < min(promo_zeros)
    delta_i = next(i for i, c in enumerate(m.calls)
                   if c[0] == "inventory_set_quantities" and c[2] == PROMO and c[3] == 2)
    assert max(promo_zeros) < delta_i
    bulk_i = m.idx("product_variants_bulk_update")[0]
    assert delta_i < bulk_i
    assert names[-2] == "product_update_status" and m.calls[-2][2] == "ACTIVE"
    assert names[-1] == "product_publish"
    variants_input = m.calls[bulk_i][2]
    assert all(v["inventoryPolicy"] == "DENY" for v in variants_input)
    assert all(v["price"] == "50.00" and v["compareAtPrice"] == "100.00" for v in variants_input)
    assert w.unstamped() == []


# ---------------------------------------------------------------------------
# 2) CREATE — phantom-stock: only the returned size ends up with Promo stock
# ---------------------------------------------------------------------------

def test_create_isolates_returned_size_only(monkeypatch):
    w = World()
    w.returned("SKU1", "42", 2)  # size 43 NOT returned
    m = OpsMock(
        monkeypatch,
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42"), _src_variant("V43", "43", "IIT43")],
        inv_by_gid={"gid://shopify/Product/900": _dup_inv_two_sizes()},
    )
    _preview_apply(w)
    nonzero = [c for c in m.sets() if c[3] > 0]
    assert nonzero == [("inventory_set_quantities", "gid/DUP/i42", PROMO, 2)]
    assert ("inventory_deactivate", "gid/DUP/i42", MAG) in m.calls
    assert ("inventory_deactivate", "gid/DUP/i43", MAG) in m.calls
    assert ("inventory_set_quantities", "gid/DUP/i43", PROMO, 0) in m.calls


# ---------------------------------------------------------------------------
# 3) ACTIVE — a stamped event is never re-applied (no re-inflate)
# ---------------------------------------------------------------------------

def test_active_does_not_reapply_a_stamped_event(monkeypatch):
    w = World()
    p_old, e_old = w.be.add_shelf_pair("SKU1", "42", reconciled=True,
                                       price_outlet="50.00", price_high="100.00")
    (e_new,) = w.returned("SKU1", "43")
    inv = [
        _inv_variant("v42", "42", "i42", [_level(PROMO, 2)]),
        _inv_variant("v43", "43", "i43", [_level(PROMO, 0)]),
    ]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None},
                inv_by_gid={OUT: inv})
    plan, report = _preview_apply(w)
    assert plan.actions[0].branch == "ACTIVE"
    assert report.outcomes[0].status == "APPLIED"
    assert m.sets() == [("inventory_set_quantities", "i43", PROMO, 1)]
    assert report.outcomes[0].stamped_event_ids == (e_new,)
    assert w.unstamped() == []


# ---------------------------------------------------------------------------
# 4) A key's Δ is the SUM of its events; every event of the group is stamped
# ---------------------------------------------------------------------------

def test_a_key_group_sums_its_events_and_stamps_all_of_them(monkeypatch):
    w = World()
    e1, e2 = w.returned("SKU1", "42", 2)
    e3 = w.be.consume("SKU1", "42")                       # a full-price sale: −1
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)])]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None},
                inv_by_gid={OUT: inv})
    plan, report = _preview_apply(w)
    (st,) = plan.actions[0].size_targets
    assert st.delta == 1 and sorted(st.event_ids) == sorted([e1, e2, e3])
    assert m.sets() == [("inventory_set_quantities", "i42", PROMO, 1)]
    assert sorted(report.outcomes[0].stamped_event_ids) == sorted([e1, e2, e3])


# ---------------------------------------------------------------------------
# 5) No Δ > 0 group → nothing planned, nothing written (Δ ≤ 0 is the decrement's)
# ---------------------------------------------------------------------------

def test_no_positive_group_plans_nothing_and_writes_nothing(monkeypatch):
    w = World()
    p, _ = w.be.add_shelf_pair("SKU1", "42", reconciled=True)
    e0 = w.be.consume("SKU1", "42", outlet_line=True)      # Δ 0: an outlet-line sale
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "DRAFT")], "warning": None})
    plan, report = _preview_apply(w)
    assert plan.actions == () and report.outcomes == ()
    assert not any(n in _MUTATION_OPS for n in m.names)
    assert w.unstamped() == [e0]


# ---------------------------------------------------------------------------
# 6) TOCTOU — live branch changed between preview and apply => VERIFY_FAILED
# ---------------------------------------------------------------------------

def test_toctou_branch_drift_aborts_verify_failed(monkeypatch):
    w = World()
    w.returned("SKU1", "42", 2)
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)])]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None},
                inv_by_gid={OUT: inv})
    reg = w.register()
    plan = outlet_service.publish_preview(reg, object(), promo_location_id=PROMO)
    assert plan.actions[0].branch == "ACTIVE"
    m.outlet = {"matches": [_outlet_match(OUT, "DRAFT")], "warning": None}  # drift ACTIVE->DRAFT
    report = outlet_service.publish_apply(reg, object(), plan, promo_location_id=PROMO)
    assert report.outcomes[0].status == "VERIFY_FAILED"
    assert not any(n in _MUTATION_OPS for n in m.names)
    assert len(w.unstamped()) == 2


# ---------------------------------------------------------------------------
# 7) FIX4 — price 0 and price >= compareAt both BLOCK publish
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("outlet_price,high", [("0.00", "100.00"), ("120.00", "100.00"), (None, "100.00")])
def test_fix4_invalid_price_blocks_publish(monkeypatch, outlet_price, high):
    w = World()
    w.returned("SKU1", "42", 2, price_outlet=outlet_price, price_high=high)
    m = OpsMock(monkeypatch,
                source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
                source_variants=[_src_variant("V42", "42", "IIT42")])
    plan, report = _preview_apply(w)
    act = plan.actions[0]
    assert act.publishable is False and act.reason == "price_invalid"
    assert report.outcomes[0].status == "PRICE_INVALID"
    assert "product_duplicate" not in m.names
    assert len(w.unstamped()) == 2


# ---------------------------------------------------------------------------
# 8) Size-primary match, now EXACT (§3.2): SKU mismatch only surfaced
# ---------------------------------------------------------------------------

def test_size_primary_exact_match_ignores_sku(monkeypatch):
    w = World()
    w.returned("SKU1", "42", 2)
    src = [_src_variant("V42", "42", "IIT42", sku="ZZZ"),
           _src_variant("V43", "43", "IIT43", sku="SKU1")]
    dup_inv = [_inv_variant("dV42", "42", "dI42", [_level(PROMO, 0)]),
               _inv_variant("dV43", "43", "dI43", [_level(PROMO, 0)])]
    m = OpsMock(monkeypatch,
                source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
                source_variants=src, inv_by_gid={"gid://shopify/Product/900": dup_inv})
    plan, _ = _preview_apply(w)
    st = plan.actions[0].size_targets[0]
    assert st.size == "42" and st.matched is True and st.variant_id == "V42"
    assert any(x.startswith("sku_mismatch_on_size:42") for x in plan.actions[0].warnings)
    assert ("inventory_set_quantities", "dI42", PROMO, 2) in m.calls


def test_a_spelling_twin_is_surfaced_never_published(monkeypatch):
    w = World()
    (e_twin,) = w.returned("AR-K-33PC", "42L")
    (e_ok,) = w.returned("AR-K-33PC", "43")
    inv = [_inv_variant("v42", "42 L", "i42", [_level(PROMO, 0)]),
           _inv_variant("v43", "43", "i43", [_level(PROMO, 0)])]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None},
                inv_by_gid={OUT: inv})
    plan, report = _preview_apply(w)
    act = plan.actions[0]
    assert act.unmatched_sizes == ("42L",)
    assert "size_twin:42L:42 L" in act.warnings
    assert m.sets() == [("inventory_set_quantities", "i43", PROMO, 1)]   # never "42 L"
    assert report.outcomes[0].stamped_event_ids == (e_ok,)
    assert w.unstamped() == [e_twin]                                     # pending, surfaced


def test_customisation_only_variant_never_matches(monkeypatch):
    w = World()
    (e,) = w.returned("SKU1", "Solo Personalizzazione")
    inv = [_inv_variant("vc", "Solo Personalizzazione", "ic", [_level(PROMO, 0)])]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None},
                inv_by_gid={OUT: inv})
    plan, _ = _preview_apply(w)
    assert plan.actions[0].unmatched_sizes == ("Solo Personalizzazione",)
    assert m.sets() == [] and w.unstamped() == [e]


# ---------------------------------------------------------------------------
# 9) DRY_RUN — preview mutates NOTHING (Shopify nor register)
# ---------------------------------------------------------------------------

def test_dry_run_preview_is_read_only(monkeypatch):
    w = World()
    w.returned("SKU1", "42", 2)
    m = OpsMock(monkeypatch,
                source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
                source_variants=[_src_variant("V42", "42", "IIT42")])
    w.be.db.log.clear()
    plan = outlet_service.publish_preview(w.register(), object(), promo_location_id=PROMO)
    assert plan.dry_run is True and plan.actions[0].branch == "CREATE"
    assert w.writes() == []                                    # no lease, no write, no stamp
    assert not any(x[0].startswith("lease") for x in w.be.db.log)
    assert not any(n in _MUTATION_OPS for n in m.names)


# ---------------------------------------------------------------------------
# 10) main.py FIX2 — DRY_RUN fail-closed
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# POST-REVIEW FIXES (fix1..fix6) — business-critical publish-outlet hardening
# ---------------------------------------------------------------------------

def test_create_duplicate_is_draft_atomically_fix1(monkeypatch):
    w = World()
    w.returned("SKU1", "42", 2)
    m = OpsMock(
        monkeypatch,
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42")],
        inv_by_gid={"gid://shopify/Product/900": [_inv_variant("dV42", "42", "dI42", [_level(PROMO, 0)])]},
    )
    _, report = _preview_apply(w)
    assert report.outcomes[0].status == "APPLIED"
    assert m.status_history[0] == (m.new_gid, "DRAFT")
    assert m.status_history[-1] == (m.new_gid, "ACTIVE")
    assert m.status_by_gid[m.new_gid] == "ACTIVE"


def test_draft_revive_quarantines_non_promo_stock_fix2(monkeypatch):
    w = World()
    w.returned("SKU1", "42", 2)
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0), _level(MAG, 3)])]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "DRAFT")], "warning": None},
                inv_by_gid={OUT: inv})
    plan, report = _preview_apply(w)
    assert plan.actions[0].branch == "DRAFT"
    assert plan.actions[0].reason == "quarantine" and plan.actions[0].publishable is False
    assert report.outcomes[0].status == "QUARANTINED"
    assert not any(n in _MUTATION_OPS for n in m.names)
    assert len(w.unstamped()) == 2


def test_draft_revive_adds_on_the_live_level_then_activates_last(monkeypatch):
    w = World()
    (e,) = w.returned("SKU1", "42")
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 1)])]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "DRAFT")], "warning": None},
                inv_by_gid={OUT: inv})
    _, report = _preview_apply(w)
    assert report.outcomes[0].status == "REVIVED"
    assert m.sets() == [("inventory_set_quantities", "i42", PROMO, 2)]    # E9: live + Δ
    assert m.names[-2:] == ["product_update_status", "product_publish"]
    assert report.outcomes[0].stamped_event_ids == (e,)


def test_error_isolation_runtime_error_does_not_abort_batch_fix3(monkeypatch):
    w = World()
    w.returned("SKU1", "42", 1)
    (e2,) = w.returned("SKU2", "42", 1)
    gid2 = "gid://shopify/Product/502"
    OpsMock(
        monkeypatch,
        outlet={"matches": [], "warning": None},
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42", sku="SKU1")],
        outlet_by_sku={"SKU2": {"matches": [_outlet_match(gid2, "ACTIVE")], "warning": None}},
        inv_by_gid={gid2: [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)])]},
        read_inv_raises={"gid://shopify/Product/900": RuntimeError("boom")},
    )
    plan, report = _preview_apply(w)
    assert {a.sku: a.branch for a in plan.actions} == {"SKU1": "CREATE", "SKU2": "ACTIVE"}
    by_sku = {o.sku: o for o in report.outcomes}
    assert by_sku["SKU1"].status == "ERROR"
    assert any(x.startswith("unexpected_error:RuntimeError") for x in by_sku["SKU1"].warnings)
    assert by_sku["SKU2"].status == "APPLIED"
    assert by_sku["SKU2"].stamped_event_ids == (e2,)


def test_publication_resolved_eager_before_any_mutation_fix4(monkeypatch):
    w = World()
    w.returned("SKU1", "42", 2)
    m = OpsMock(
        monkeypatch,
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42")],
        inv_by_gid={"gid://shopify/Product/900": [_inv_variant("dV42", "42", "dI42", [_level(PROMO, 0)])]},
    )

    def _boom(_t):
        raise RuntimeError("Online Store publication not found")

    monkeypatch.setattr(ops, "get_online_store_publication_id", _boom)
    _, report = _preview_apply(w)
    assert len(report.outcomes) == 1 and report.outcomes[0].status == "ERROR"
    assert any(x.startswith("publication_resolve_failed") for x in report.outcomes[0].warnings)
    assert not any(n in _MUTATION_OPS for n in m.names)
    assert len(w.unstamped()) == 2


def test_fix5_sku_cross_check_fires_on_active_branch(monkeypatch):
    w = World()
    w.returned("SKU1", "42", 2)
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)], sku="OTHER-SKU")]
    OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None},
            inv_by_gid={OUT: inv})
    plan = outlet_service.publish_preview(w.register(), object(), promo_location_id=PROMO)
    st = plan.actions[0].size_targets[0]
    assert st.matched is True and st.size == "42"
    assert any(x.startswith("sku_mismatch_on_size:42:OTHER-SKU") for x in plan.actions[0].warnings)


def test_create_quarantines_when_levels_truncated_after_duplicate_fix6b(monkeypatch):
    w = World()
    w.returned("SKU1", "42", 2)
    dup_gid = "gid://shopify/Product/900"
    m = OpsMock(
        monkeypatch,
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42")],
        inv_by_gid={dup_gid: [_inv_variant("dV42", "42", "dI42", [_level(PROMO, 0)], truncated=True)]},
    )
    _, report = _preview_apply(w)
    assert report.outcomes[0].status == "QUARANTINED"
    assert any(x.startswith("quarantine:levels_truncated") for x in report.outcomes[0].warnings)
    assert {n for n in m.names if n in _MUTATION_OPS} == {"product_duplicate"}
    assert len(w.unstamped()) == 2


# ---------------------------------------------------------------------------
# The register side of the apply (F7)
# ---------------------------------------------------------------------------

def test_preview_refuses_before_the_whole_register_baseline(monkeypatch):
    from tests.register_fixtures import MemoryBackend as Fresh

    be = Fresh()                                  # no BASELINE row
    be.add_shelf_pair("SKU1", "42")
    OpsMock(monkeypatch)
    with pytest.raises(BaselineMissing):
        outlet_service.publish_preview(be.register(), object(), promo_location_id=PROMO)


def test_each_stamp_is_carried_by_its_reconcile_row_in_one_transaction(monkeypatch):
    w = World()
    (e,) = w.returned("SKU1", "42")
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 3)])]
    OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None},
            inv_by_gid={OUT: inv})
    w.be.db.log.clear()
    _preview_apply(w)
    assert w.writes() == ["renew", "insert_audit", "stamp", "commit"]
    (row,) = w.register().reconcile_rows()
    assert row.status == DONE and row.action == "add" and row.product_id == 500
    assert row.payload["event_ids"] == [e] and row.keys == (("SKU1", "42"),)
    assert (row.payload["level_before"], row.payload["level_after"]) == (3, 4)
    assert row.lease_gen == 1


def test_a_held_positive_key_is_never_added(monkeypatch):
    w = World()
    (e_held,) = w.returned("SKU1", "42")
    (e_free,) = w.returned("SKU1", "43")
    w.be.seed_audit(InsertAudit(
        kind="HOLD", actor="t", action="hold",
        payload={"keys": [["SKU1", "42"]], "reason": "moved", "event_ids": [e_held]}))
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)]),
           _inv_variant("v43", "43", "i43", [_level(PROMO, 0)])]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None},
                inv_by_gid={OUT: inv})
    plan, report = _preview_apply(w)
    assert [st.size for st in plan.actions[0].size_targets] == ["43"]
    assert m.sets() == [("inventory_set_quantities", "i43", PROMO, 1)]
    assert w.unstamped() == [e_held]


def test_a_baseline_held_key_with_a_post_hold_return_is_surfaced_not_added(monkeypatch):
    from tests.register_fixtures import MemoryBackend as Fresh

    be = Fresh()
    _, e_snap = be.add_shelf_pair("SKU1", "42")                 # held with the snapshot
    be.seed_baseline(held=[{"key": ["SKU1", "42"], "reason": "moved", "event_ids": [e_snap]}])
    _, e_new = be.add_shelf_pair("SKU1", "42", price_outlet="50.00", price_high="100.00")
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None})
    plan = outlet_service.publish_preview(be.register(), object(), promo_location_id=PROMO)
    assert plan.actions == ()
    assert "held_positive:SKU1:42:1" in plan.anomalies
    assert {h.kind for h in be.register().held_keys()} == {HOLD_BASELINE}
    assert not any(n in _MUTATION_OPS for n in m.names)


def test_the_keyless_miss_is_surfaced_never_stamped(monkeypatch):
    w = World()
    e = w.be.consume("NOPE", "40")
    OpsMock(monkeypatch)
    plan, _ = _preview_apply(w)
    assert plan.anomalies == (f"consume_miss:{e}",)
    assert w.unstamped() == [e]


def test_lease_unavailable_writes_nothing(monkeypatch):
    w = World()
    w.returned("SKU1", "42")
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)])]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None},
                inv_by_gid={OUT: inv})
    reg = w.register()
    plan = outlet_service.publish_preview(reg, object(), promo_location_id=PROMO)
    other = PassLease(w.register(), "decrement")
    other.take()                                            # another pass holds it
    report = outlet_service.publish_apply(reg, object(), plan, promo_location_id=PROMO)
    assert [o.warnings for o in report.outcomes] == [("lease_unavailable",)]
    assert not any(n in _MUTATION_OPS for n in m.names)
    assert len(w.unstamped()) == 1


def test_a_lost_lease_stops_the_apply(monkeypatch):
    w = World()
    (e1,) = w.returned("SKU1", "42")
    w.returned("SKU2", "42")
    gid2 = "gid://shopify/Product/502"
    m = OpsMock(
        monkeypatch,
        outlet_by_sku={"SKU1": {"matches": [_outlet_match(OUT, "ACTIVE")], "warning": None},
                       "SKU2": {"matches": [_outlet_match(gid2, "ACTIVE")], "warning": None}},
        inv_by_gid={OUT: [_inv_variant("v1", "42", "i1", [_level(PROMO, 0)])],
                    gid2: [_inv_variant("v2", "42", "i2", [_level(PROMO, 0)])]},
    )

    def expire_after_first_write(item, loc, qty):
        if item == "i1":
            w.be.advance(400_000)                           # the lease expires mid-SKU
            PassLease(w.register(), "decrement").take()     # and is taken over

    m.on_set = expire_after_first_write
    _, report = _preview_apply(w)
    assert [(o.sku, o.status, o.warnings) for o in report.outcomes] == [
        ("SKU1", "ERROR", ("lease_lost",)), ("SKU2", "ERROR", ("lease_lost",))]
    assert ("inventory_set_quantities", "i2", PROMO, 1) not in m.calls      # SKU2 never written
    assert e1 in w.unstamped()                       # the stamp after the take-over rolled back
