"""backend.services.outlet_service — PUBLISH orchestration.

Every collaborator is mocked in-memory (ScansiaSheet, resolvers, ops) — NO HTTP,
NO live store, NO live Sheet, per project rules. The ops/resolver leaves are
patched on their module objects (outlet_service calls them through the module),
so a single ``OpsMock`` records the exact orchestration call ORDER, which is the
whole point of the anti-phantom-stock branch.

Coverage: CREATE order, phantom-stock isolation, ACTIVE no-re-inflate, delta
exactly-once (compare-mismatch), DRAFT sold-out, TOCTOU VERIFY_FAILED, FIX4
price rejection, FIX5 size-primary matching, DRY_RUN read-only, and main.py FIX2.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from backend.gsheet.reader import CanonRead, CanonRow
from backend.gsheet.writer import WriteResult
from backend.services import outlet_service
from backend.services import resolvers
from backend.shopify import ops

PROMO = "gid://shopify/Location/PROMO"
MAG = "gid://shopify/Location/MAG"
PUB = "gid://shopify/Publication/OS"

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

def _row(sku, size, qta, *, reconciled=False, product_id="",
         prezzo_high="100.00", prezzo_outlet="50.00", row_uuid=None) -> CanonRow:
    return CanonRow(
        row_uuid=row_uuid or f"u-{sku}-{size}",
        sku=sku, size=size, product_id=product_id,
        prezzo_high=prezzo_high, prezzo_outlet=prezzo_outlet,
        qta=qta, qta_raw=str(qta), online="SI", sconto="",
        reconciled=reconciled, row_index=0, anomalies=[], raw={},
    )


def _level(loc_id, available, on_hand=None):
    return {"location_id": loc_id, "location_name": loc_id.split("/")[-1],
            "available": available, "committed": 0,
            "on_hand": available if on_hand is None else on_hand}


def _inv_variant(vid, size, item_id, levels, policy="DENY", truncated=False, sku=None):
    return {"id": vid, "sku": sku, "inventoryPolicy": policy,
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


class FakeSheet:
    def __init__(self, rows: List[CanonRow]):
        self.rows = rows
        self.marked: List[Any] = []
        self.reads: List[bool] = []

    def read_canonical(self, *, assign_uuids=True) -> CanonRead:
        self.reads.append(assign_uuids)
        return CanonRead(list(self.rows), {}, [])

    def mark_reconciled(self, row_uuid, *, expected_sku) -> WriteResult:
        self.marked.append((row_uuid, expected_sku))
        return WriteResult(True, 1, None)


class OpsMock:
    """Patches every ops.* / resolvers.* used by the service and records calls."""

    def __init__(self, monkeypatch, *, inv_by_gid=None, source_variants=None,
                 outlet=None, source=None, new_gid="gid://shopify/Product/DUP",
                 outlet_by_sku=None, source_by_sku=None, read_inv_raises=None):
        self.calls: List[tuple] = []
        self.inv_by_gid: Dict[str, list] = inv_by_gid or {}
        self.source_variants: list = source_variants or []
        self.new_gid = new_gid
        self.outlet = outlet if outlet is not None else {"matches": [], "warning": None}
        self.source = source if source is not None else {"matches": [], "warning": None}
        # per-sku resolver overrides (default: same outlet/source for every sku).
        self.outlet_by_sku: Dict[str, dict] = outlet_by_sku or {}
        self.source_by_sku: Dict[str, dict] = source_by_sku or {}
        # models the duplicate's product status across the CREATE finalization
        # (fix1): _duplicate seeds "DRAFT", _status records every explicit flip.
        self.status_by_gid: Dict[str, str] = {}
        self.status_history: List[tuple] = []
        # gid -> Exception instance to raise instead of returning inventory
        # (fix3 error-isolation coverage: e.g. a RuntimeError mid-execution).
        self.read_inv_raises: Dict[str, Exception] = read_inv_raises or {}

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

    # recorded ops
    def _duplicate(self, t, src, title):
        self.calls.append(("product_duplicate", src, title))
        # fix1: productDuplicate(newStatus=DRAFT) -> the duplicate is DRAFT
        # atomically at creation, never inheriting the ACTIVE source's status.
        self.status_by_gid[self.new_gid] = "DRAFT"
        self.status_history.append((self.new_gid, "DRAFT"))
        return self.new_gid

    def _read_inv(self, t, gid):
        self.calls.append(("read_variant_inventory", gid))
        if gid in self.read_inv_raises:
            raise self.read_inv_raises[gid]
        return [dict(v) for v in self.inv_by_gid.get(gid, [])]

    def _get_variants(self, t, gid):
        self.calls.append(("get_product_variants", gid)); return list(self.source_variants)

    def _set_qty(self, t, item, loc, qty, *a, **k):
        self.calls.append(("inventory_set_quantities", item, loc, qty)); return {}

    def _deactivate(self, t, item, loc):
        self.calls.append(("inventory_deactivate", item, loc)); return None

    def _bulk(self, t, gid, variants):
        self.calls.append(("product_variants_bulk_update", gid, variants)); return {}

    def _status(self, t, gid, status):
        self.calls.append(("product_update_status", gid, status))
        self.status_by_gid[gid] = status
        self.status_history.append((gid, status))
        return {}

    def _publish(self, t, gid, pub):
        self.calls.append(("product_publish", gid, pub)); return {}

    # helpers
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


# ---------------------------------------------------------------------------
# 1) CREATE — exact anti-phantom-stock ORDER
# ---------------------------------------------------------------------------

def test_create_branch_exact_order(monkeypatch):
    rows = [_row("SKU1", "42", 2)]
    sheet = FakeSheet(rows)
    m = OpsMock(
        monkeypatch,
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42"), _src_variant("V43", "43", "IIT43")],
        inv_by_gid={"gid://shopify/Product/DUP": _dup_inv_two_sizes()},
    )
    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    assert plan.actions[0].branch == "CREATE"
    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)
    assert report.outcomes[0].status == "APPLIED"

    names = m.names
    # (1) duplicate happens ONCE, before every inventory/publish mutation
    dup_i = m.idx("product_duplicate")
    assert len(dup_i) == 1
    dup_i = dup_i[0]
    inv_mut = [i for i, c in enumerate(m.calls)
               if c[0] in {"inventory_set_quantities", "inventory_deactivate",
                           "product_variants_bulk_update", "product_update_status",
                           "product_publish"}]
    assert dup_i < min(inv_mut)
    # (2) every non-Promo disconnect happens BEFORE the first Promo zero
    last_deact = max(m.idx("inventory_deactivate"))
    promo_zeros = [i for i, c in enumerate(m.calls)
                   if c[0] == "inventory_set_quantities" and c[2] == PROMO and c[3] == 0]
    assert last_deact < min(promo_zeros)
    # (3) delta set (Promo, 2) AFTER the Promo zeros
    delta_i = next(i for i, c in enumerate(m.calls)
                   if c[0] == "inventory_set_quantities" and c[2] == PROMO and c[3] == 2)
    assert max(promo_zeros) < delta_i
    # (4) DENY normalize AFTER delta, (5) activate + publish are the LAST two
    bulk_i = m.idx("product_variants_bulk_update")[0]
    assert delta_i < bulk_i
    assert names[-2] == "product_update_status" and m.calls[-2][2] == "ACTIVE"
    assert names[-1] == "product_publish"
    # DENY + outlet prices carried on the bulk update
    variants_input = m.calls[bulk_i][2]
    assert all(v["inventoryPolicy"] == "DENY" for v in variants_input)
    assert all(v["price"] == "50.00" and v["compareAtPrice"] == "100.00" for v in variants_input)


# ---------------------------------------------------------------------------
# 2) CREATE — phantom-stock: only the returned size ends up with Promo stock
# ---------------------------------------------------------------------------

def test_create_isolates_returned_size_only(monkeypatch):
    rows = [_row("SKU1", "42", 2)]  # size 43 NOT returned
    sheet = FakeSheet(rows)
    m = OpsMock(
        monkeypatch,
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42"), _src_variant("V43", "43", "IIT43")],
        inv_by_gid={"gid://shopify/Product/DUP": _dup_inv_two_sizes()},
    )
    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)

    # the ONLY non-zero inventory set is the returned size 42 at Promo
    nonzero = [c for c in m.sets() if c[3] > 0]
    assert nonzero == [("inventory_set_quantities", "gid/DUP/i42", PROMO, 2)]
    # both variants' Magazzino (non-Promo) levels were zeroed AND disconnected
    assert ("inventory_deactivate", "gid/DUP/i42", MAG) in m.calls
    assert ("inventory_deactivate", "gid/DUP/i43", MAG) in m.calls
    assert ("inventory_set_quantities", "gid/DUP/i43", PROMO, 0) in m.calls


# ---------------------------------------------------------------------------
# 3) ACTIVE — a reconciled row is never re-applied (no re-inflate)
# ---------------------------------------------------------------------------

def test_active_does_not_reapply_reconciled_row(monkeypatch):
    rows = [
        _row("SKU1", "42", 2, reconciled=True, row_uuid="u-old"),   # already applied
        _row("SKU1", "43", 1, reconciled=False, row_uuid="u-new"),  # fresh return
    ]
    sheet = FakeSheet(rows)
    gid = "gid://shopify/Product/OUT"
    inv = [
        _inv_variant("v42", "42", "i42", [_level(PROMO, 2)]),
        _inv_variant("v43", "43", "i43", [_level(PROMO, 0)]),
    ]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(gid, "ACTIVE")], "warning": None},
                inv_by_gid={gid: inv})
    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    assert plan.actions[0].branch == "ACTIVE"
    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)

    assert report.outcomes[0].status == "APPLIED"
    # only the fresh size-43 delta is written; size-42 (reconciled) is untouched
    assert m.sets() == [("inventory_set_quantities", "i43", PROMO, 1)]
    assert sheet.marked == [("u-new", "SKU1")]


# ---------------------------------------------------------------------------
# 4) DELTA exactly-once — compare-mismatch (already applied) => no double
# ---------------------------------------------------------------------------

def test_delta_exactly_once_no_double_on_compare_mismatch(monkeypatch):
    rows = [_row("SKU1", "42", 3, row_uuid="u1")]
    sheet = FakeSheet(rows)
    gid = "gid://shopify/Product/OUT"
    inv_pre = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)])]   # preview: pre=0
    inv_live = [_inv_variant("v42", "42", "i42", [_level(PROMO, 3)])]  # apply: already 3
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(gid, "ACTIVE")], "warning": None},
                inv_by_gid={gid: inv_pre})

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    assert plan.actions[0].size_targets[0].frozen_pre == 0

    m.inv_by_gid[gid] = inv_live  # external actor applied the +3 before apply
    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)

    # NO inventory set (would be the double); row still marked reconciled; anomaly surfaced
    assert m.sets() == []
    assert sheet.marked == [("u1", "SKU1")]
    assert any(w.startswith("delta_already_applied") for w in report.outcomes[0].warnings)


# ---------------------------------------------------------------------------
# 5) DRAFT — drafted, no fresh return => sold-out skip + surface
# ---------------------------------------------------------------------------

def test_draft_sold_out_skips_and_surfaces(monkeypatch):
    rows = [_row("SKU1", "42", 0, reconciled=False, row_uuid="u1")]  # non-reconciled, qta 0
    sheet = FakeSheet(rows)
    gid = "gid://shopify/Product/OUT"
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(gid, "DRAFT")], "warning": None})

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    assert plan.actions[0].branch == "DRAFT" and plan.actions[0].reason == "sold_out"
    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)

    assert report.outcomes[0].status == "SOLD_OUT"
    # nothing mutated
    assert not any(n in _MUTATION_OPS for n in m.names)
    assert sheet.marked == []


# ---------------------------------------------------------------------------
# 6) TOCTOU — live branch changed between preview and apply => VERIFY_FAILED
# ---------------------------------------------------------------------------

def test_toctou_branch_drift_aborts_verify_failed(monkeypatch):
    rows = [_row("SKU1", "42", 2, row_uuid="u1")]
    sheet = FakeSheet(rows)
    gid = "gid://shopify/Product/OUT"
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)])]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(gid, "ACTIVE")], "warning": None},
                inv_by_gid={gid: inv})

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    assert plan.actions[0].branch == "ACTIVE"

    m.outlet = {"matches": [_outlet_match(gid, "DRAFT")], "warning": None}  # drift ACTIVE->DRAFT
    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)

    assert report.outcomes[0].status == "VERIFY_FAILED"
    # no publish / activate / inventory mutation executed
    assert not any(n in _MUTATION_OPS for n in m.names)
    assert sheet.marked == []


# ---------------------------------------------------------------------------
# 7) FIX4 — price 0 and price >= compareAt both BLOCK publish
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("outlet_price,high", [("0.00", "100.00"), ("120.00", "100.00")])
def test_fix4_invalid_price_blocks_publish(monkeypatch, outlet_price, high):
    rows = [_row("SKU1", "42", 2, prezzo_outlet=outlet_price, prezzo_high=high)]
    sheet = FakeSheet(rows)
    m = OpsMock(monkeypatch,
                source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
                source_variants=[_src_variant("V42", "42", "IIT42")])

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    act = plan.actions[0]
    assert act.publishable is False and act.reason == "price_invalid"
    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)

    assert report.outcomes[0].status == "PRICE_INVALID"
    assert "product_duplicate" not in m.names  # never created


# ---------------------------------------------------------------------------
# 8) FIX5 — variant matched by SIZE (primary), SKU mismatch only surfaced
# ---------------------------------------------------------------------------

def test_fix5_size_primary_match_ignores_sku(monkeypatch):
    # size-42 variant carries a DIFFERENT sku ("ZZZ"); SKU-equality-first would
    # pick the size-43 variant (sku "SKU1"). Size-primary must pick V42.
    rows = [_row("SKU1", "42", 2)]
    sheet = FakeSheet(rows)
    src = [_src_variant("V42", "42", "IIT42", sku="ZZZ"),
           _src_variant("V43", "43", "IIT43", sku="SKU1")]
    dup_inv = [_inv_variant("dV42", "42", "dI42", [_level(PROMO, 0)]),
               _inv_variant("dV43", "43", "dI43", [_level(PROMO, 0)])]
    m = OpsMock(monkeypatch,
                source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
                source_variants=src, inv_by_gid={"gid://shopify/Product/DUP": dup_inv})

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    st = plan.actions[0].size_targets[0]
    assert st.norm_size == "42" and st.matched is True and st.variant_id == "V42"
    assert any(w.startswith("sku_mismatch_on_size:42") for w in plan.actions[0].warnings)

    outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)
    # apply matched the size-42 duplicate variant (dI42) for the delta, by size not sku
    assert ("inventory_set_quantities", "dI42", PROMO, 2) in m.calls


# ---------------------------------------------------------------------------
# 9) DRY_RUN — preview mutates NOTHING (Shopify nor Sheet)
# ---------------------------------------------------------------------------

def test_dry_run_preview_is_read_only(monkeypatch):
    rows = [_row("SKU1", "42", 2)]
    sheet = FakeSheet(rows)
    m = OpsMock(monkeypatch,
                source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
                source_variants=[_src_variant("V42", "42", "IIT42")])

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)

    assert plan.dry_run is True and plan.actions[0].branch == "CREATE"
    assert sheet.reads == [False]              # assign_uuids=False (no uuid minting)
    assert sheet.marked == []                  # no reconciliation write
    assert not any(n in _MUTATION_OPS for n in m.names)  # no Shopify mutation


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


# ---------------------------------------------------------------------------
# POST-REVIEW FIXES (fix1..fix6) — business-critical publish-outlet hardening
# ---------------------------------------------------------------------------

# fix1 — productDuplicate(newStatus=DRAFT): the duplicate is DRAFT atomically at
# creation, never inheriting the ACTIVE source's status (2025-07 semantics).

def test_create_duplicate_is_draft_atomically_fix1(monkeypatch):
    """The duplicate's FIRST modeled status transition is DRAFT (mirroring the
    real newStatus=DRAFT arg on productDuplicate); the LAST is the explicit
    ACTIVE flip — never a no-op, since the duplicate never started ACTIVE."""
    rows = [_row("SKU1", "42", 2)]
    sheet = FakeSheet(rows)
    m = OpsMock(
        monkeypatch,
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42")],
        inv_by_gid={"gid://shopify/Product/DUP": [_inv_variant("dV42", "42", "dI42", [_level(PROMO, 0)])]},
    )
    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)

    assert report.outcomes[0].status == "APPLIED"
    assert m.status_history[0] == (m.new_gid, "DRAFT")
    assert m.status_history[-1] == (m.new_gid, "ACTIVE")
    assert m.status_by_gid[m.new_gid] == "ACTIVE"


# fix2 — DRAFT-revive quarantine gate (mirrors ACTIVE's _has_non_promo_stock).

def test_draft_revive_quarantines_non_promo_stock_fix2(monkeypatch):
    """A DRAFT outlet that inherited stock at a location != Promo must NOT be
    revived/published with that phantom stock — it goes to QUARANTINED."""
    rows = [_row("SKU1", "42", 2, row_uuid="u1")]
    sheet = FakeSheet(rows)
    gid = "gid://shopify/Product/OUT"
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0), _level(MAG, 3)])]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(gid, "DRAFT")], "warning": None},
                inv_by_gid={gid: inv})

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    assert plan.actions[0].branch == "DRAFT"
    assert plan.actions[0].reason == "quarantine"
    assert plan.actions[0].publishable is False

    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)
    assert report.outcomes[0].status == "QUARANTINED"
    assert not any(n in _MUTATION_OPS for n in m.names)
    assert sheet.marked == []


# fix3 — per-action isolation: an unexpected exception (e.g. RuntimeError from a
# read op) is bounded to that sku's ERROR outcome; the batch always continues.

def test_error_isolation_runtime_error_does_not_abort_batch_fix3(monkeypatch):
    rows = [_row("SKU1", "42", 2, row_uuid="u1"), _row("SKU2", "42", 1, row_uuid="u2")]
    sheet = FakeSheet(rows)
    gid2 = "gid://shopify/Product/OUT2"
    m = OpsMock(
        monkeypatch,
        outlet={"matches": [], "warning": None},  # SKU1: no outlet -> CREATE via source
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42", sku="SKU1")],
        outlet_by_sku={"SKU2": {"matches": [_outlet_match(gid2, "ACTIVE")], "warning": None}},
        inv_by_gid={gid2: [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)])]},
        # the CREATE branch's post-duplicate inventory read blows up mid-execution.
        read_inv_raises={"gid://shopify/Product/DUP": RuntimeError("boom")},
    )

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    assert {a.sku: a.branch for a in plan.actions} == {"SKU1": "CREATE", "SKU2": "ACTIVE"}

    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)
    by_sku = {o.sku: o for o in report.outcomes}

    assert by_sku["SKU1"].status == "ERROR"
    assert any(w.startswith("unexpected_error:RuntimeError") for w in by_sku["SKU1"].warnings)
    # SKU2 is NOT aborted by SKU1's failure — the batch continues and applies it.
    assert by_sku["SKU2"].status == "APPLIED"
    assert sheet.marked == [("u2", "SKU2")]


# fix4 — publication resolved EAGER/fail-fast, before any mutation.

def test_publication_resolved_eager_before_any_mutation_fix4(monkeypatch):
    rows = [_row("SKU1", "42", 2)]
    sheet = FakeSheet(rows)
    m = OpsMock(
        monkeypatch,
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42")],
        inv_by_gid={"gid://shopify/Product/DUP": [_inv_variant("dV42", "42", "dI42", [_level(PROMO, 0)])]},
    )

    def _boom(_t):
        raise RuntimeError("Online Store publication not found")

    monkeypatch.setattr(ops, "get_online_store_publication_id", _boom)

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)

    assert len(report.outcomes) == 1
    assert report.outcomes[0].status == "ERROR"
    assert any(w.startswith("publication_resolve_failed") for w in report.outcomes[0].warnings)
    # clean abort: resolved BEFORE the loop, so NOTHING was ever mutated.
    assert not any(n in _MUTATION_OPS for n in m.names)
    assert sheet.marked == []


# fix5 — read_variant_inventory now selects `sku`, so the secondary cross-check
# fires on ACTIVE/DRAFT too (previously a no-op there).

def test_fix5_sku_cross_check_fires_on_active_branch(monkeypatch):
    rows = [_row("SKU1", "42", 2)]
    sheet = FakeSheet(rows)
    gid = "gid://shopify/Product/OUT"
    inv = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)], sku="OTHER-SKU")]
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(gid, "ACTIVE")], "warning": None},
                inv_by_gid={gid: inv})

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    st = plan.actions[0].size_targets[0]
    assert st.matched is True and st.norm_size == "42"
    assert any(w.startswith("sku_mismatch_on_size:42:OTHER-SKU") for w in plan.actions[0].warnings)


# fix6(a) — DELTA compare-mismatch, STRICT case: base_v < live_v < target.

def test_delta_exactly_once_strict_mismatch_below_target_fix6a(monkeypatch):
    rows = [_row("SKU1", "42", 3, row_uuid="u1")]
    sheet = FakeSheet(rows)
    gid = "gid://shopify/Product/OUT"
    inv_pre = [_inv_variant("v42", "42", "i42", [_level(PROMO, 0)])]   # preview: frozen_pre=0
    inv_live = [_inv_variant("v42", "42", "i42", [_level(PROMO, 1)])]  # apply: live=1 (0 < 1 < 3)
    m = OpsMock(monkeypatch, outlet={"matches": [_outlet_match(gid, "ACTIVE")], "warning": None},
                inv_by_gid={gid: inv_pre})

    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    assert plan.actions[0].size_targets[0].frozen_pre == 0

    m.inv_by_gid[gid] = inv_live
    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)

    assert m.sets() == []  # neither the target set nor a re-add was issued
    assert sheet.marked == []  # not marked reconciled — genuinely ambiguous, needs re-preview
    assert any(w.startswith("delta_compare_mismatch:42") for w in report.outcomes[0].warnings)


# fix6(b) — QUARANTINED: CREATE whose duplicate's inventory read is truncated
# (UNKNOWN level set -> cannot guarantee non-Promo cleanup).

def test_create_quarantines_when_levels_truncated_after_duplicate_fix6b(monkeypatch):
    rows = [_row("SKU1", "42", 2)]
    sheet = FakeSheet(rows)
    dup_gid = "gid://shopify/Product/DUP"
    m = OpsMock(
        monkeypatch,
        source={"matches": [_source_match("gid://shopify/Product/SRC")], "warning": None},
        source_variants=[_src_variant("V42", "42", "IIT42")],
        inv_by_gid={dup_gid: [_inv_variant("dV42", "42", "dI42", [_level(PROMO, 0)], truncated=True)]},
    )
    plan = outlet_service.publish_preview(sheet, object(), promo_location_id=PROMO)
    report = outlet_service.publish_apply(sheet, object(), plan, promo_location_id=PROMO)

    assert report.outcomes[0].status == "QUARANTINED"
    assert any(w.startswith("quarantine:levels_truncated") for w in report.outcomes[0].warnings)
    # the duplicate WAS created (that's how the truncation was discovered), but no
    # further mutation (zero/disconnect/DENY/activate/publish) was ever issued.
    assert {n for n in m.names if n in _MUTATION_OPS} == {"product_duplicate"}
    assert sheet.marked == []
