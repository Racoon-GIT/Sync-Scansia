"""backend.services.delete_service — IRREVERSIBLE outlet delete/cleanup (M5).

Every collaborator is mocked in-memory (ops, ScansiaSheet, DeleteAuditSink) — NO
HTTP, NO live store, NO live Sheet, per project rules. A single shared call log
records the exact op order so the load-bearing safety invariant
("write_durable BEFORE product_delete, and if write_durable raises product_delete
is NEVER reached") is asserted mechanically.

Coverage: the exact per-variant predicate (candidate + every REVIEW route);
Promo anchor fail-closed gate; abort-on-snapshot-failure; human-gesture gate;
over-threshold second confirm; live re-verify (plan_hash drift); col-Q write-back;
reconstructive snapshot fields; single-delete predicate-skip; deny_normalize.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from backend.gsheet.reader import CanonRead, CanonRow
from backend.gsheet.writer import WriteResult
from backend.services import delete_service as ds
from backend.shopify import ops

PROMO = "gid://shopify/Location/PROMO"
MAG = "gid://shopify/Location/MAG"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def _inv_variant(vid, *, policy="DENY", promo_available=0, promo_committed=0,
                 promo_present=True, non_promo=None, truncated=False) -> Dict[str, Any]:
    """One read_variant_inventory row. ``promo_present=False`` => Promo level ABSENT."""
    levels: List[Dict[str, Any]] = []
    if promo_present:
        levels.append({
            "location_id": PROMO, "location_name": "Promo",
            "available": promo_available, "committed": promo_committed,
            "on_hand": max(promo_available, 0),
        })
    for loc, av in (non_promo or {}).items():
        levels.append({"location_id": loc, "location_name": loc,
                       "available": av, "committed": 0, "on_hand": av})
    return {
        "id": vid, "sku": "SKU", "inventoryPolicy": policy,
        "selectedOptions": [{"name": "Size", "value": "42"}],
        "inventoryItemId": vid + "/i", "levels": levels, "levels_truncated": truncated,
    }


def _snap_variant(vid="v1", sku="SKU-42", size="42") -> Dict[str, Any]:
    return {"id": vid, "sku": sku, "price": "50.00", "compareAtPrice": "100.00",
            "inventoryItem": {"id": vid + "/i"},
            "selectedOptions": [{"name": "Size", "value": size}]}


def _row(sku, gid, *, size="42", uuid=None) -> CanonRow:
    return CanonRow(
        row_uuid=uuid or f"u-{sku}", sku=sku, size=size, product_id=gid,
        prezzo_high="100.00", prezzo_outlet="50.00", qta=1, qta_raw="1",
        online="SI", sconto="", reconciled=False, row_index=0, anomalies=[],
        raw={"sku": sku},
    )


class FakeSheet:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.deletes: List[tuple] = []

    def read_canonical(self, *, assign_uuids=True) -> CanonRead:
        return CanonRead(list(self.rows), {}, [])

    def write_delete_state(self, row_uuid, gid, *, expected_sku, field, value) -> WriteResult:
        self.deletes.append((row_uuid, gid, expected_sku, field, value))
        return WriteResult(True, 1, None)


class FakeAudit:
    """Injected delete audit sink; write_durable raise-on-fail is the abort gate."""

    def __init__(self, *, durable_raises=False, log=None):
        self.log = log
        self.durable: List[ds.BeforeSnapshot] = []
        self.outcomes: List[ds.DeleteOutcomeEvent] = []
        self.durable_raises = durable_raises

    def write_durable(self, snapshot: ds.BeforeSnapshot) -> None:
        if self.log is not None:
            self.log.append(("write_durable", snapshot.product_gid))
        if self.durable_raises:
            raise RuntimeError("durable sink down")
        self.durable.append(snapshot)

    def write_outcome(self, event: ds.DeleteOutcomeEvent) -> None:
        self.outcomes.append(event)


class OpsMock:
    """Patch ops.* used by delete_service; record calls into a shared log."""

    def __init__(self, monkeypatch, *, members=None, inv_by_gid=None, variants_by_gid=None,
                 core_by_gid=None, mf_by_gid=None, media_by_gid=None, log=None):
        self.log: List[tuple] = log if log is not None else []
        self.members = members or []
        self.inv_by_gid = inv_by_gid or {}
        self.variants_by_gid = variants_by_gid or {}
        self.core_by_gid = core_by_gid or {}
        self.mf_by_gid = mf_by_gid or {}
        self.media_by_gid = media_by_gid or {}
        monkeypatch.setattr(ops, "enumerate_outlet_products", self._enumerate)
        monkeypatch.setattr(ops, "read_variant_inventory", self._read_inv)
        monkeypatch.setattr(ops, "product_delete", self._delete)
        monkeypatch.setattr(ops, "product_variants_bulk_update", self._bulk)
        monkeypatch.setattr(ops, "product_update_status", self._status)
        monkeypatch.setattr(ops, "get_product_core", self._core)
        monkeypatch.setattr(ops, "get_product_variants", self._variants)
        monkeypatch.setattr(ops, "get_product_metafields", self._mf)
        monkeypatch.setattr(ops, "get_product_media", self._media)

    def _enumerate(self, t, collection_gid=ops.OUTLET_COLLECTION_GID):
        self.log.append(("enumerate", collection_gid))
        return list(self.members)

    def _read_inv(self, t, gid):
        self.log.append(("read_inv", gid))
        return [dict(v) for v in self.inv_by_gid.get(gid, [])]

    def _delete(self, t, gid):
        self.log.append(("product_delete", gid))
        return "gid-del/" + gid

    def _bulk(self, t, gid, variants):
        self.log.append(("bulk", gid, variants))
        return {}

    def _status(self, t, gid, status):
        self.log.append(("status", gid, status))
        return {}

    def _core(self, t, gid):
        self.log.append(("core", gid))
        return self.core_by_gid.get(gid, {
            "id": gid, "title": "T", "handle": "h", "status": "DRAFT",
            "tags": [], "collections": [],
        })

    def _variants(self, t, gid):
        self.log.append(("variants", gid))
        return [dict(v) for v in self.variants_by_gid.get(gid, [])]

    def _mf(self, t, gid):
        self.log.append(("mf", gid))
        return list(self.mf_by_gid.get(gid, []))

    def _media(self, t, gid):
        self.log.append(("media", gid))
        return list(self.media_by_gid.get(gid, []))


# ---------------------------------------------------------------------------
# Predicate (per-variant, exact)
# ---------------------------------------------------------------------------

def test_predicate_all_zero_deny_is_candidate(monkeypatch):
    G = "gid://shopify/Product/C"
    OpsMock(monkeypatch, members=[{"id": G, "title": "c", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0),
                            _inv_variant("v2", promo_available=0)]})
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert [c.product_gid for c in z.candidates] == [G]
    assert z.review == () and z.in_stock == 0


def test_predicate_missing_promo_level_is_review(monkeypatch):
    G = "gid://shopify/Product/M"
    OpsMock(monkeypatch, members=[{"id": G, "title": "m", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0),
                            _inv_variant("v2", promo_present=False)]})
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert z.candidates == ()
    assert z.review[0].reasons == ("promo_level_absent",)


def test_predicate_one_continue_is_review(monkeypatch):
    G = "gid://shopify/Product/K"
    OpsMock(monkeypatch, members=[{"id": G, "title": "k", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0),
                            _inv_variant("v2", promo_available=0, policy="CONTINUE")]})
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert z.candidates == ()
    assert z.review[0].reasons == ("continue_policy",)


def test_predicate_committed_is_review(monkeypatch):
    G = "gid://shopify/Product/CM"
    OpsMock(monkeypatch, members=[{"id": G, "title": "cm", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0, promo_committed=2)]})
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert z.review[0].reasons == ("promo_committed",)


def test_predicate_oversell_is_review(monkeypatch):
    G = "gid://shopify/Product/O"
    OpsMock(monkeypatch, members=[{"id": G, "title": "o", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=-1)]})
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert z.review[0].reasons == ("promo_oversell",)


def test_predicate_non_promo_stock_is_review(monkeypatch):
    G = "gid://shopify/Product/NP"
    OpsMock(monkeypatch, members=[{"id": G, "title": "np", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0, non_promo={MAG: 3})]})
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert z.review[0].reasons == ("non_promo_stock",)


def test_predicate_truncated_is_review(monkeypatch):
    G = "gid://shopify/Product/TR"
    OpsMock(monkeypatch, members=[{"id": G, "title": "tr", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0, truncated=True)]})
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert z.review[0].reasons == ("levels_truncated_unknown",)


def test_predicate_in_stock_is_skipped(monkeypatch):
    G = "gid://shopify/Product/S"
    OpsMock(monkeypatch, members=[{"id": G, "title": "s", "status": "ACTIVE"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=5)]})
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert z.candidates == () and z.review == () and z.in_stock == 1


def test_predicate_read_error_isolated_to_review(monkeypatch):
    G = "gid://shopify/Product/E"
    m = OpsMock(monkeypatch, members=[{"id": G, "title": "e", "status": "DRAFT"}])

    def _boom(t, gid):
        raise RuntimeError("Product not found for GID")
    monkeypatch.setattr(ops, "read_variant_inventory", _boom)
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert z.candidates == () and z.review[0].reasons[0].startswith("read_error:")


def test_predicate_no_variants_is_review(monkeypatch):
    G = "gid://shopify/Product/NV"
    OpsMock(monkeypatch, members=[{"id": G, "title": "nv", "status": "DRAFT"}],
            inv_by_gid={G: []})
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert z.candidates == ()
    assert z.review[0].reasons == ("no_variants",)


def test_predicate_promo_available_unknown_is_review(monkeypatch):
    """HARDENING (D3): a PRESENT Promo level whose ``available`` is None/UNKNOWN
    must route to REVIEW, never CANDIDATE — even though the shared
    ``outlet_service._promo_available`` helper would collapse it to 0."""
    G = "gid://shopify/Product/PU"
    variant = _inv_variant("v1", promo_available=0)
    variant["levels"][0]["available"] = None  # Promo level PRESENT but UNKNOWN
    OpsMock(monkeypatch, members=[{"id": G, "title": "pu", "status": "DRAFT"}],
            inv_by_gid={G: [variant]})
    z = ds.zero_stock_candidates(object(), promo_location_id=PROMO)
    assert z.candidates == ()
    assert z.review[0].reasons == ("promo_available_unknown",)


# ---------------------------------------------------------------------------
# Promo anchor fail-closed gate
# ---------------------------------------------------------------------------

def test_promo_anchor_gate_aborts_before_enumeration(monkeypatch):
    m = OpsMock(monkeypatch, members=[{"id": "g", "title": "x", "status": "DRAFT"}])
    with pytest.raises(ds.PromoAnchorError):
        ds.zero_stock_candidates(object(), promo_location_id="")
    assert m.log == []  # never enumerated


# ---------------------------------------------------------------------------
# Cleanup apply — happy path: delete + col-Q write-back + snapshot-before-delete
# ---------------------------------------------------------------------------

def test_cleanup_apply_deletes_and_writes_back(monkeypatch):
    G = "gid://shopify/Product/D1"
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, members=[{"id": G, "title": "d", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0)]},
            variants_by_gid={G: [_snap_variant()]})
    sheet, audit = FakeSheet(rows=[_row("SKU", G)]), FakeAudit(log=log)

    plan = ds.cleanup_preview(object(), promo_location_id=PROMO)
    assert plan.count == 1 and plan.candidates[0].product_gid == G

    report = ds.cleanup_apply(object(), sheet, audit, plan, human_gesture="1", promo_location_id=PROMO)
    assert report.deleted == 1
    assert ("product_delete", G) in log
    # write-back keyed by col-Q gid (guard), default field online=NO (DoD).
    assert sheet.deletes == [("u-SKU", G, "SKU", "online", "NO")]
    # durable snapshot written BEFORE the delete.
    di = next(i for i, c in enumerate(log) if c[0] == "write_durable")
    pi = next(i for i, c in enumerate(log) if c[0] == "product_delete")
    assert di < pi
    assert audit.durable[0].product_gid == G


# ---------------------------------------------------------------------------
# ABORT-on-snapshot-failure — write_durable raises => product_delete NOT called
# ---------------------------------------------------------------------------

def test_cleanup_abort_on_snapshot_failure_never_deletes(monkeypatch):
    G = "gid://shopify/Product/D2"
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, members=[{"id": G, "title": "d", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0)]},
            variants_by_gid={G: [_snap_variant()]})
    sheet = FakeSheet(rows=[_row("SKU", G)])
    audit = FakeAudit(log=log, durable_raises=True)

    plan = ds.cleanup_preview(object(), promo_location_id=PROMO)
    report = ds.cleanup_apply(object(), sheet, audit, plan, human_gesture="1", promo_location_id=PROMO)

    assert report.deleted == 0
    assert report.outcomes[0].status == ds.STATUS_SNAPSHOT_ABORTED
    assert not any(c[0] == "product_delete" for c in log)  # KEY invariant
    assert sheet.deletes == []


def test_cleanup_snapshot_build_failed_skips_never_deletes(monkeypatch):
    """A read op for the snapshot itself (e.g. get_product_core) raising ->
    STATUS_SNAPSHOT_BUILD_FAILED, product_delete NEVER called (PRE-delete, safe
    per-outlet skip)."""
    G = "gid://shopify/Product/D2b"
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, members=[{"id": G, "title": "d", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0)]},
            variants_by_gid={G: [_snap_variant()]})

    def _boom_core(t, gid):
        raise RuntimeError("core read failed")
    monkeypatch.setattr(ops, "get_product_core", _boom_core)

    sheet, audit = FakeSheet(rows=[_row("SKU", G)]), FakeAudit(log=log)
    plan = ds.cleanup_preview(object(), promo_location_id=PROMO)
    report = ds.cleanup_apply(object(), sheet, audit, plan, human_gesture="1", promo_location_id=PROMO)

    assert report.deleted == 0
    assert report.outcomes[0].status == ds.STATUS_SNAPSHOT_BUILD_FAILED
    assert not any(c[0] == "product_delete" for c in log)


# ---------------------------------------------------------------------------
# Human gesture gate
# ---------------------------------------------------------------------------

def test_cleanup_missing_human_gesture_refused(monkeypatch):
    G = "gid://shopify/Product/D3"
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, members=[{"id": G, "title": "d", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0)]})
    sheet, audit = FakeSheet(rows=[_row("SKU", G)]), FakeAudit(log=log)
    plan = ds.cleanup_preview(object(), promo_location_id=PROMO)
    with pytest.raises(ds.DeleteConfirmationError):
        ds.cleanup_apply(object(), sheet, audit, plan, human_gesture=None, promo_location_id=PROMO)
    assert not any(c[0] == "product_delete" for c in log)


def test_cleanup_wrong_count_gesture_refused(monkeypatch):
    G = "gid://shopify/Product/D3b"
    OpsMock(monkeypatch, members=[{"id": G, "title": "d", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0)]})
    sheet, audit = FakeSheet(rows=[_row("SKU", G)]), FakeAudit()
    plan = ds.cleanup_preview(object(), promo_location_id=PROMO)  # count == 1
    with pytest.raises(ds.DeleteConfirmationError):
        ds.cleanup_apply(object(), sheet, audit, plan, human_gesture="9", promo_location_id=PROMO)


# ---------------------------------------------------------------------------
# Over-threshold second confirmation
# ---------------------------------------------------------------------------

def test_cleanup_threshold_requires_second_confirm(monkeypatch):
    G = "gid://shopify/Product/D4"
    OpsMock(monkeypatch, members=[{"id": G, "title": "d", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0)]},
            variants_by_gid={G: [_snap_variant()]})
    sheet, audit = FakeSheet(rows=[_row("SKU", G)]), FakeAudit()

    plan = ds.cleanup_preview(object(), promo_location_id=PROMO, threshold=0)
    assert plan.requires_second_confirm is True
    with pytest.raises(ds.DeleteConfirmationError):
        ds.cleanup_apply(object(), sheet, audit, plan, human_gesture="1", promo_location_id=PROMO)
    # with the second confirmation it proceeds.
    report = ds.cleanup_apply(object(), sheet, audit, plan, human_gesture="1",
                              promo_location_id=PROMO, second_confirm=True)
    assert report.deleted == 1


def test_cleanup_hard_cap_requires_second_confirm_despite_high_threshold(monkeypatch):
    """An operator-supplied threshold (e.g. 10000) can never silently disable the
    second-confirm gate above CLEANUP_HARD_CAP: count=60 > cap -> still requires
    second_confirm even though count < threshold."""
    G = "gid://shopify/Product/CAP"
    OpsMock(monkeypatch, members=[{"id": G, "title": "d", "status": "DRAFT"}],
            inv_by_gid={G: [_inv_variant("v1", promo_available=0)]})
    sheet, audit = FakeSheet(), FakeAudit()
    fake_plan = ds.CleanupPlan(
        dry_run=True, candidates=(), review=(), count=60, threshold=10000,
        archive_first=False, requires_second_confirm=False, plan_hash="deadbeef",
    )
    with pytest.raises(ds.DeleteConfirmationError):
        ds.cleanup_apply(object(), sheet, audit, fake_plan, human_gesture="60",
                         promo_location_id=PROMO)


# ---------------------------------------------------------------------------
# Live re-verify — plan_hash drift aborts the whole batch
# ---------------------------------------------------------------------------

def test_cleanup_verify_failed_on_drift(monkeypatch):
    G = "gid://shopify/Product/D5"
    m = OpsMock(monkeypatch, members=[{"id": G, "title": "d", "status": "DRAFT"}],
                inv_by_gid={G: [_inv_variant("v1", promo_available=0)]},
                variants_by_gid={G: [_snap_variant()]})
    sheet, audit = FakeSheet(rows=[_row("SKU", G)]), FakeAudit()
    plan = ds.cleanup_preview(object(), promo_location_id=PROMO)
    # the outlet gains stock between preview and apply -> no longer a candidate.
    m.inv_by_gid[G] = [_inv_variant("v1", promo_available=3)]
    report = ds.cleanup_apply(object(), sheet, audit, plan, human_gesture="1", promo_location_id=PROMO)
    assert report.verify_failed is True and report.deleted == 0
    assert audit.durable == [] and sheet.deletes == []


# ---------------------------------------------------------------------------
# Single delete (creation errors) — predicate deliberately NOT applied
# ---------------------------------------------------------------------------

def test_single_delete_ignores_predicate(monkeypatch):
    G = "gid://shopify/Product/S1"
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, variants_by_gid={G: [_snap_variant()]})
    sheet, audit = FakeSheet(rows=[_row("SKU", G)]), FakeAudit(log=log)
    out = ds.delete_single_apply(object(), sheet, audit, G, human_gesture="CONFERMO")
    assert out.status == ds.STATUS_DELETED
    assert ("product_delete", G) in log
    assert sheet.deletes[0][1] == G


def test_single_delete_requires_gesture(monkeypatch):
    G = "gid://shopify/Product/S2"
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log)
    sheet, audit = FakeSheet(), FakeAudit(log=log)
    with pytest.raises(ds.DeleteConfirmationError):
        ds.delete_single_apply(object(), sheet, audit, G, human_gesture="nope")
    assert not any(c[0] == "product_delete" for c in log)


# ---------------------------------------------------------------------------
# Reconstructive snapshot fields
# ---------------------------------------------------------------------------

def test_snapshot_captures_reconstructive_fields(monkeypatch):
    G = "gid://shopify/Product/SN"
    log: List[tuple] = []
    OpsMock(
        monkeypatch, log=log,
        core_by_gid={G: {
            "id": G, "title": "Sneak - Outlet", "handle": "sneak-outlet", "status": "ACTIVE",
            "tags": ["outlet", "nike"],
            "collections": [
                {"id": "c1", "title": "OUTLET", "handle": "outlet", "smart": True},
                {"id": "c2", "title": "Manual", "handle": "man", "smart": False},
            ],
        }},
        variants_by_gid={G: [_snap_variant(vid="v1", sku="SKU-42", size="42")]},
        mf_by_gid={G: [{"namespace": "custom", "key": "k",
                        "type": "single_line_text_field", "value": "x"}]},
        media_by_gid={G: ["https://cdn/img1.jpg", "https://cdn/img2.jpg"]},
    )
    sheet, audit = FakeSheet(), FakeAudit(log=log)
    ds.delete_single_apply(object(), sheet, audit, G, human_gesture="CONFERMO")
    snap = audit.durable[0]
    assert snap.title == "Sneak - Outlet" and snap.handle == "sneak-outlet" and snap.status == "ACTIVE"
    assert snap.tags == ("outlet", "nike")
    v = snap.variants[0]
    assert v.size == "42" and v.price == "50.00" and v.compare_at == "100.00" and v.inventory_item_id == "v1/i"
    assert snap.image_srcs == ("https://cdn/img1.jpg", "https://cdn/img2.jpg")
    assert snap.metafields[0]["namespace"] == "custom"
    assert {c.id: c.smart for c in snap.collections} == {"c1": True, "c2": False}


# ---------------------------------------------------------------------------
# CONTINUE-DENY coupling — deny_normalize forces DENY on every variant
# ---------------------------------------------------------------------------

def test_deny_normalize_forces_deny_on_all_variants(monkeypatch):
    G = "gid://shopify/Product/DN"
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, variants_by_gid={G: [{"id": "v1"}, {"id": "v2"}]})
    n = ds.deny_normalize(object(), G)
    assert n == 2
    bulk = [c for c in log if c[0] == "bulk"]
    assert bulk[0][1] == G
    assert bulk[0][2] == [
        {"id": "v1", "inventoryPolicy": "DENY"},
        {"id": "v2", "inventoryPolicy": "DENY"},
    ]
