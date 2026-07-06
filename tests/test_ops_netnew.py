"""backend.shopify.ops — NET-NEW ops (M3/M5 outlet lifecycle).

Covers the four appended ops with the same in-memory FakeTransport pattern as
test_ops.py (no HTTP, no live store):

* read_variant_inventory — inventoryPolicy pass-through, per-location quantities,
  the load-bearing "quantity 0 vs level ABSENT" distinction, and the
  hasNextPage truncation cap-guard (NOT a naive nodes==10 heuristic);
* product_delete — deletedProductId happy path + fix3 userErrors -> raise;
* get_online_store_publication_id / product_publish — resolve + publish happy
  path, missing-channel fail-closed, and fix3 userErrors -> raise;
* enumerate_outlet_products — multi-page pagination + null-collection raise.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Union

import pytest

from backend.shopify import ops
from backend.shopify.ops import ShopifyUserError


class FakeTransport:
    """Records every ``graphql`` call and returns canned responses.

    ``responses`` may be a single dict (returned every call), a FIFO list of
    dicts (one per call), or a callable ``(query, variables) -> dict``.
    """

    def __init__(
        self,
        responses: Union[Dict[str, Any], List[Dict[str, Any]], Callable[..., Dict[str, Any]]],
    ) -> None:
        self._responses = responses
        self.calls: List[Dict[str, Any]] = []

    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        self.calls.append({"query": query, "variables": variables})
        if callable(self._responses):
            return self._responses(query, variables)
        if isinstance(self._responses, list):
            return self._responses.pop(0)
        return self._responses


# ---------------------------------------------------------------------------
# read_variant_inventory
# ---------------------------------------------------------------------------

def _levels(*quantity_maps: Dict[str, int], has_next: bool = False) -> Dict[str, Any]:
    """Build an inventoryLevels connection; each map -> one location node."""
    nodes = []
    for i, qmap in enumerate(quantity_maps, start=1):
        nodes.append(
            {
                "location": {"id": f"gid://shopify/Location/{i}", "name": f"Loc{i}"},
                "quantities": [{"name": n, "quantity": q} for n, q in qmap.items()],
            }
        )
    return {"pageInfo": {"hasNextPage": has_next, "endCursor": None}, "nodes": nodes}


def _variant_inv_node(vid: str, policy: str, inv_levels: Dict[str, Any], sku: str = "SKU-42") -> Dict[str, Any]:
    return {
        "id": vid,
        "sku": sku,
        "inventoryPolicy": policy,
        "selectedOptions": [{"name": "Size", "value": "42"}],
        "inventoryItem": {"id": f"gid://shopify/InventoryItem/{vid[-1]}", "inventoryLevels": inv_levels},
    }


def _product_variants_page(nodes: List[Dict[str, Any]], has_next: bool = False, cursor=None):
    return {"node": {"variants": {"pageInfo": {"hasNextPage": has_next, "endCursor": cursor}, "nodes": nodes}}}


def test_read_variant_inventory_policy_and_per_location_quantities():
    # Loc1 present at available:0 (KNOWN zero); Loc "Promo" simply absent from list.
    lvls = _levels({"available": 0, "committed": 0, "on_hand": 5})
    node = _variant_inv_node("gid://shopify/ProductVariant/1", "DENY", lvls)
    t = FakeTransport(_product_variants_page([node]))

    out = ops.read_variant_inventory(t, "gid://shopify/Product/1")

    assert len(out) == 1
    v = out[0]
    assert v["sku"] == "SKU-42"  # fix5: sku now selected/exposed for ACTIVE/DRAFT cross-check
    assert v["inventoryPolicy"] == "DENY"  # ProductVariant field, passed through
    assert v["inventoryItemId"] == "gid://shopify/InventoryItem/1"
    assert v["levels_truncated"] is False
    # single present location, KNOWN available == 0 (a real zero, not fabricated)
    assert len(v["levels"]) == 1
    lvl = v["levels"][0]
    assert lvl["location_id"] == "gid://shopify/Location/1"
    assert lvl["available"] == 0
    assert lvl["on_hand"] == 5


def test_read_variant_inventory_distinguishes_zero_from_absent_level():
    # Two variants: one has a level at Loc1 (available 0 = KNOWN); the other has
    # NO levels at all -> that location is ABSENT -> caller must treat as UNKNOWN.
    with_zero = _variant_inv_node("gid://shopify/ProductVariant/1", "DENY", _levels({"available": 0, "committed": 0, "on_hand": 0}))
    with_none = _variant_inv_node("gid://shopify/ProductVariant/2", "CONTINUE", _levels())  # empty nodes
    t = FakeTransport(_product_variants_page([with_zero, with_none]))

    out = ops.read_variant_inventory(t, "gid://shopify/Product/1")

    # variant 1: location present, KNOWN zero
    present = out[0]["levels"]
    assert len(present) == 1
    assert present[0]["available"] == 0
    # variant 2: NO level node -> ABSENT, op does NOT fabricate a 0 entry
    absent = out[1]["levels"]
    assert absent == []
    # membership test the caller would run: location present vs absent is decidable
    assert {l["location_id"] for l in present} == {"gid://shopify/Location/1"}
    assert {l["location_id"] for l in absent} == set()


def test_read_variant_inventory_truncation_flag_from_has_next_page():
    lvls = _levels(*({"available": i, "committed": 0, "on_hand": i} for i in range(10)), has_next=True)
    node = _variant_inv_node("gid://shopify/ProductVariant/1", "DENY", lvls)
    t = FakeTransport(_product_variants_page([node]))

    out = ops.read_variant_inventory(t, "gid://shopify/Product/1")
    assert out[0]["levels_truncated"] is True  # hasNextPage => UNKNOWN, incomplete set


def test_read_variant_inventory_ten_levels_not_truncated_when_has_next_false():
    # Exactly 10 locations with hasNextPage:false is COMPLETE, not truncated.
    # Proves we key off pageInfo, not a false-positive "nodes == 10" heuristic.
    lvls = _levels(*({"available": 1, "committed": 0, "on_hand": 1} for _ in range(10)), has_next=False)
    node = _variant_inv_node("gid://shopify/ProductVariant/1", "DENY", lvls)
    t = FakeTransport(_product_variants_page([node]))

    out = ops.read_variant_inventory(t, "gid://shopify/Product/1")
    assert len(out[0]["levels"]) == 10
    assert out[0]["levels_truncated"] is False


def test_read_variant_inventory_paginates_variants():
    p1 = _product_variants_page([_variant_inv_node("gid://shopify/ProductVariant/1", "DENY", _levels())], has_next=True, cursor="CUR1")
    p2 = _product_variants_page([_variant_inv_node("gid://shopify/ProductVariant/2", "DENY", _levels())], has_next=False)
    t = FakeTransport([p1, p2])

    out = ops.read_variant_inventory(t, "gid://shopify/Product/1")
    assert [v["id"] for v in out] == ["gid://shopify/ProductVariant/1", "gid://shopify/ProductVariant/2"]
    assert len(t.calls) == 2
    assert t.calls[1]["variables"]["variantsAfter"] == "CUR1"
    assert t.calls[0]["variables"]["levelsFirst"] == 10  # cap-guard


def test_read_variant_inventory_raises_when_node_null():
    t = FakeTransport({"node": None})
    with pytest.raises(RuntimeError, match="Product not found"):
        ops.read_variant_inventory(t, "gid://shopify/Product/404")


# ---------------------------------------------------------------------------
# product_delete
# ---------------------------------------------------------------------------

def test_product_delete_returns_deleted_id():
    t = FakeTransport({"productDelete": {"deletedProductId": "gid://shopify/Product/9", "userErrors": []}})
    out = ops.product_delete(t, "gid://shopify/Product/9")
    assert out == "gid://shopify/Product/9"
    assert t.calls[0]["variables"] == {"input": {"id": "gid://shopify/Product/9"}}


def test_product_delete_raises_on_user_errors_fix3():
    # non-existent product -> deletedProductId null + userErrors -> must RAISE
    t = FakeTransport({"productDelete": {"deletedProductId": None, "userErrors": [{"field": ["id"], "message": "Product does not exist"}]}})
    with pytest.raises(ShopifyUserError) as exc:
        ops.product_delete(t, "gid://shopify/Product/404")
    assert exc.value.mutation == "productDelete"


# ---------------------------------------------------------------------------
# product_update_status
# ---------------------------------------------------------------------------

def test_product_update_status_sets_active():
    t = FakeTransport({"productUpdate": {"product": {"id": "gid://shopify/Product/1", "status": "ACTIVE"}, "userErrors": []}})
    out = ops.product_update_status(t, "gid://shopify/Product/1", "ACTIVE")
    assert out["product"]["status"] == "ACTIVE"
    assert t.calls[0]["variables"] == {"input": {"id": "gid://shopify/Product/1", "status": "ACTIVE"}}
    assert "productUpdate" in t.calls[0]["query"]


def test_product_update_status_raises_on_user_errors_fix3():
    t = FakeTransport({"productUpdate": {"product": None, "userErrors": [{"field": ["status"], "message": "invalid"}]}})
    with pytest.raises(ShopifyUserError) as exc:
        ops.product_update_status(t, "gid://shopify/Product/1", "ACTIVE")
    assert exc.value.mutation == "productUpdate"


# ---------------------------------------------------------------------------
# get_online_store_publication_id  +  product_publish
# ---------------------------------------------------------------------------

def test_get_online_store_publication_id_matches_by_name():
    t = FakeTransport({"publications": {"nodes": [
        {"id": "gid://shopify/Publication/1", "name": "Point of Sale"},
        {"id": "gid://shopify/Publication/2", "name": "Online Store"},
    ]}})
    assert ops.get_online_store_publication_id(t) == "gid://shopify/Publication/2"


def test_get_online_store_publication_id_fail_closed_when_absent():
    t = FakeTransport({"publications": {"nodes": [{"id": "gid://shopify/Publication/1", "name": "Point of Sale"}]}})
    with pytest.raises(RuntimeError, match="Online Store"):
        ops.get_online_store_publication_id(t)


def test_product_publish_happy_path():
    t = FakeTransport({"publishablePublish": {
        "publishable": {"publishedOnPublication": True, "resourcePublicationsCount": {"count": 1}},
        "userErrors": [],
    }})
    out = ops.product_publish(t, "gid://shopify/Product/1", "gid://shopify/Publication/2")
    assert out["publishable"]["publishedOnPublication"] is True
    v = t.calls[0]["variables"]
    assert v["id"] == "gid://shopify/Product/1"
    assert v["publicationId"] == "gid://shopify/Publication/2"
    # input is a LIST of PublicationInput (publishablePublish, not deprecated productPublish)
    assert v["input"] == [{"publicationId": "gid://shopify/Publication/2"}]
    assert "publishablePublish" in t.calls[0]["query"]


def test_product_publish_raises_on_user_errors_fix3():
    t = FakeTransport({"publishablePublish": {
        "publishable": None,
        "userErrors": [{"field": ["input", "0", "publicationId"], "message": "Publication does not exist or is not publishable"}],
    }})
    with pytest.raises(ShopifyUserError) as exc:
        ops.product_publish(t, "gid://shopify/Product/1", "gid://shopify/Publication/999")
    assert exc.value.mutation == "publishablePublish"


# ---------------------------------------------------------------------------
# enumerate_outlet_products
# ---------------------------------------------------------------------------

def test_enumerate_outlet_products_walks_multiple_pages():
    page1 = {"collection": {"id": ops.OUTLET_COLLECTION_GID, "products": {
        "pageInfo": {"hasNextPage": True, "endCursor": "CUR1"},
        "nodes": [{"id": "gid://shopify/Product/1", "title": "Nike - Outlet", "status": "ACTIVE"}],
    }}}
    page2 = {"collection": {"id": ops.OUTLET_COLLECTION_GID, "products": {
        "pageInfo": {"hasNextPage": False, "endCursor": None},
        "nodes": [{"id": "gid://shopify/Product/2", "title": "Vans - Outlet", "status": "DRAFT"}],
    }}}
    t = FakeTransport([page1, page2])

    out = ops.enumerate_outlet_products(t)
    assert [p["id"] for p in out] == ["gid://shopify/Product/1", "gid://shopify/Product/2"]
    assert [p["status"] for p in out] == ["ACTIVE", "DRAFT"]
    assert len(t.calls) == 2
    # default collection id + forward cursor threaded on page 2
    assert t.calls[0]["variables"]["id"] == ops.OUTLET_COLLECTION_GID
    assert t.calls[1]["variables"]["after"] == "CUR1"


def test_enumerate_outlet_products_accepts_explicit_collection_gid():
    page = {"collection": {"id": "gid://shopify/Collection/777", "products": {
        "pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [],
    }}}
    t = FakeTransport(page)
    ops.enumerate_outlet_products(t, "gid://shopify/Collection/777")
    assert t.calls[0]["variables"]["id"] == "gid://shopify/Collection/777"


def test_enumerate_outlet_products_raises_when_collection_null():
    t = FakeTransport({"collection": None})
    with pytest.raises(RuntimeError, match="Collection not found"):
        ops.enumerate_outlet_products(t, "gid://shopify/Collection/404")
