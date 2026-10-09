"""The delete PREDICATE and the reconstructive snapshot (plan §3.5; pure reads):
``delete_service.zero_stock_candidates`` / ``classify_outlet`` / ``build_snapshot``
with ``ops`` patched in memory — NO HTTP. The register-side delete paths are in
``tests/test_delete_service.py``."""
from __future__ import annotations

from typing import Any, Dict, List

import pytest

from backend.services import delete_service as ds
from backend.shopify import ops

PROMO = "gid://shopify/Location/PROMO"
MAG = "gid://shopify/Location/MAG"


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
    OpsMock(monkeypatch, members=[{"id": G, "title": "e", "status": "DRAFT"}])

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
# Reconstructive snapshot fields
# ---------------------------------------------------------------------------

def test_snapshot_captures_reconstructive_fields(monkeypatch):
    G = "gid://shopify/Product/SN"
    OpsMock(
        monkeypatch,
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
    snap = ds.build_snapshot(object(), G)
    assert snap.title == "Sneak - Outlet" and snap.handle == "sneak-outlet" and snap.status == "ACTIVE"
    assert snap.tags == ("outlet", "nike")
    v = snap.variants[0]
    assert v.size == "42" and v.price == "50.00" and v.compare_at == "100.00" and v.inventory_item_id == "v1/i"
    assert snap.image_srcs == ("https://cdn/img1.jpg", "https://cdn/img2.jpg")
    assert snap.metafields[0]["namespace"] == "custom"
    assert {c.id: c.smart for c in snap.collections} == {"c1": True, "c2": False}
