"""backend.shopify.ops — stateless GraphQL op-wrappers.

The transport is mocked with an in-memory fake (``.graphql(query, variables)``
returns a predefined dict). No HTTP, no live store, per project rules.

Each op is covered for happy-path + fix3 (non-empty userErrors -> raise). Extra
behavioral assertions per the M1a spec: variables/mutation shape, the
``on_hand`` inventory-set name, idempotent re-activate, null-level deactivate
no-op, and the not-stocked activate+retry.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Union

import pytest

from backend.shopify import ops
from backend.shopify.ops import ShopifyUserError


class FakeTransport:
    """Records every ``graphql`` call and returns canned responses.

    ``responses`` may be:
      * a single dict — returned for every call;
      * a list of dicts — popped FIFO (one per call);
      * a callable ``(query, variables) -> dict`` — routed by inspecting the op.
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
# product_duplicate
# ---------------------------------------------------------------------------

def test_product_duplicate_returns_new_gid():
    t = FakeTransport(
        {"productDuplicate": {"newProduct": {"id": "gid://shopify/Product/999"}, "userErrors": []}}
    )
    gid = ops.product_duplicate(t, "gid://shopify/Product/1", "Nike - OUTLET")
    assert gid == "gid://shopify/Product/999"
    assert t.calls[0]["variables"] == {
        "productId": "gid://shopify/Product/1",
        "newTitle": "Nike - OUTLET",
        "newStatus": "DRAFT",
    }


def test_product_duplicate_forces_new_status_draft_fix1():
    """fix1 (post-review): the duplicate is DRAFT atomically at creation — 2025-07
    ``productDuplicate`` with no ``newStatus`` inherits the ACTIVE source's status,
    which would make the final ``product_update_status(...,"ACTIVE")`` a no-op and
    leave the duplicate ACTIVE (phantom-stock sellable) for the whole finalization
    window. Assert ``newStatus`` is passed explicitly and equals ``"DRAFT"``.
    """
    t = FakeTransport(
        {"productDuplicate": {"newProduct": {"id": "gid://shopify/Product/999", "status": "DRAFT"}, "userErrors": []}}
    )
    ops.product_duplicate(t, "gid://shopify/Product/1", "Nike - OUTLET")
    assert t.calls[0]["variables"]["newStatus"] == "DRAFT"
    assert "$newStatus" in t.calls[0]["query"]
    assert "newStatus: $newStatus" in t.calls[0]["query"]


def test_product_duplicate_raises_on_user_errors():
    t = FakeTransport(
        {"productDuplicate": {"newProduct": None, "userErrors": [{"field": "title", "message": "taken"}]}}
    )
    with pytest.raises(ShopifyUserError) as exc:
        ops.product_duplicate(t, "gid://shopify/Product/1", "dup")
    assert exc.value.mutation == "productDuplicate"
    assert exc.value.errors == [{"field": "title", "message": "taken"}]


def test_shopify_user_error_str_is_generic_and_bounded():
    errors = [{"field": "title", "message": "taken", "secretDetail": "raw-payload-marker"}]
    exc = ShopifyUserError("productDuplicate", errors)
    message = str(exc)
    assert "raw-payload-marker" not in message
    assert message == "productDuplicate failed with 1 userError(s)"
    # structured attrs remain fully populated for programmatic inspection
    assert exc.mutation == "productDuplicate"
    assert exc.errors == errors


def test_product_duplicate_propagates_top_level_transport_error():
    from backend.shopify.transport import ShopifyTransportError

    def boom(_q, _v):
        raise ShopifyTransportError("GraphQL errors: boom")

    t = FakeTransport(boom)
    with pytest.raises(ShopifyTransportError):
        ops.product_duplicate(t, "gid://shopify/Product/1", "dup")


# ---------------------------------------------------------------------------
# get_product_variants
# ---------------------------------------------------------------------------

def _variant_node(vid: str, sku: str, size: str) -> Dict[str, Any]:
    return {
        "id": vid,
        "sku": sku,
        "title": size,
        "price": "100.00",
        "compareAtPrice": "150.00",
        "inventoryItem": {"id": f"gid://shopify/InventoryItem/{vid[-1]}"},
        "selectedOptions": [{"name": "Size", "value": size}],
    }


def test_get_product_variants_flattens_and_preserves_fields():
    n1 = _variant_node("gid://shopify/ProductVariant/1", "SKU1", "42")
    n2 = _variant_node("gid://shopify/ProductVariant/2", "SKU1", "43")
    t = FakeTransport({"node": {"variants": {"edges": [{"node": n1}, {"node": n2}]}}})

    variants = ops.get_product_variants(t, "gid://shopify/Product/1")

    assert variants == [n1, n2]
    for v in variants:
        assert set(v) == {
            "id", "sku", "title", "price", "compareAtPrice",
            "inventoryItem", "selectedOptions",
        }
    # single page capped at 250 (no pagination)
    assert "first: 250" in t.calls[0]["query"]


def test_get_product_variants_raises_when_node_null():
    t = FakeTransport({"node": None})
    with pytest.raises(RuntimeError, match="Product not found"):
        ops.get_product_variants(t, "gid://shopify/Product/404")


# ---------------------------------------------------------------------------
# product_variants_bulk_update  (+ variants_bulk_update_prices broadcast helper)
# ---------------------------------------------------------------------------

def test_bulk_update_empty_list_is_noop_no_transport_call():
    t = FakeTransport({})
    assert ops.product_variants_bulk_update(t, "gid://shopify/Product/1", []) is None
    assert t.calls == []


def test_bulk_update_happy_path_returns_payload():
    t = FakeTransport(
        {"productVariantsBulkUpdate": {"product": {"id": "gid://shopify/Product/1"}, "userErrors": []}}
    )
    variants = [{"id": "gid://shopify/ProductVariant/1", "price": "10", "compareAtPrice": None}]
    payload = ops.product_variants_bulk_update(t, "gid://shopify/Product/1", variants)
    assert payload["product"]["id"] == "gid://shopify/Product/1"
    assert t.calls[0]["variables"]["variants"] == variants


def test_bulk_update_raises_on_user_errors_fix3():
    t = FakeTransport(
        {"productVariantsBulkUpdate": {"product": None, "userErrors": [{"field": "price", "message": "bad"}]}}
    )
    with pytest.raises(ShopifyUserError):
        ops.product_variants_bulk_update(
            t, "gid://shopify/Product/1", [{"id": "v1", "price": "x", "compareAtPrice": None}]
        )


def test_variants_bulk_update_prices_broadcasts_same_price_and_null_compare_at():
    n1 = _variant_node("gid://shopify/ProductVariant/1", "SKU1", "42")
    n2 = _variant_node("gid://shopify/ProductVariant/2", "SKU1", "43")

    def route(query, _variables):
        if "productVariantsBulkUpdate" in query:
            return {"productVariantsBulkUpdate": {"product": {"id": "p1"}, "userErrors": []}}
        return {"node": {"variants": {"edges": [{"node": n1}, {"node": n2}]}}}

    t = FakeTransport(route)
    ops.variants_bulk_update_prices(t, "gid://shopify/Product/1", "49.90", None)

    mutation_call = next(c for c in t.calls if "productVariantsBulkUpdate" in c["query"])
    sent = mutation_call["variables"]["variants"]
    assert [u["id"] for u in sent] == [n1["id"], n2["id"]]
    assert all(u["price"] == "49.90" for u in sent)
    # None compareAtPrice threads through unchanged (serialized as JSON null)
    assert all(u["compareAtPrice"] is None for u in sent)


def test_variants_bulk_update_prices_empty_variants_never_calls_bulk_update():
    t = FakeTransport({"node": {"variants": {"edges": []}}})
    out = ops.variants_bulk_update_prices(t, "gid://shopify/Product/1", "49.90", None)
    assert out is None
    assert not any("productVariantsBulkUpdate" in c["query"] for c in t.calls)


# ---------------------------------------------------------------------------
# metafields
# ---------------------------------------------------------------------------

def test_get_product_metafields_extracts_four_fields():
    mf = {"namespace": "custom", "key": "color", "type": "single_line_text_field", "value": "red"}
    t = FakeTransport({"node": {"metafields": {"edges": [{"node": mf}]}}})
    assert ops.get_product_metafields(t, "gid://shopify/Product/1") == [mf]


def test_get_product_metafields_raises_when_node_null():
    t = FakeTransport({"node": None})
    with pytest.raises(RuntimeError, match="Product not found"):
        ops.get_product_metafields(t, "gid://shopify/Product/404")


def test_metafields_set_empty_is_noop_no_transport_call():
    t = FakeTransport({})
    assert ops.metafields_set(t, []) is None
    assert t.calls == []


def test_metafields_set_happy_path():
    t = FakeTransport(
        {"metafieldsSet": {"metafields": [{"id": "gid://shopify/Metafield/1"}], "userErrors": []}}
    )
    inputs = [{"ownerId": "gid://shopify/Product/2", "namespace": "c", "key": "k", "type": "t", "value": "v"}]
    out = ops.metafields_set(t, inputs)
    assert out == [{"id": "gid://shopify/Metafield/1"}]
    assert t.calls[0]["variables"] == {"metafields": inputs}


def test_metafields_set_raises_on_user_errors_fix3():
    t = FakeTransport(
        {"metafieldsSet": {"metafields": [], "userErrors": [{"field": "value", "message": "invalid", "code": "INVALID"}]}}
    )
    with pytest.raises(ShopifyUserError):
        ops.metafields_set(t, [{"ownerId": "p", "namespace": "c", "key": "k", "type": "t", "value": "v"}])


def test_metafields_set_guards_over_cap():
    t = FakeTransport({})
    over = [{"ownerId": "p", "namespace": "c", "key": str(i), "type": "t", "value": "v"} for i in range(26)]
    with pytest.raises(ValueError, match="at most 25"):
        ops.metafields_set(t, over)
    assert t.calls == []


# ---------------------------------------------------------------------------
# media
# ---------------------------------------------------------------------------

def test_get_product_media_returns_image_urls_in_order_filtering_non_images():
    t = FakeTransport(
        {"product": {"media": {"nodes": [
            {"id": "m1", "alt": "", "image": {"url": "https://cdn/1.jpg"}},
            {},  # non-image media -> empty inline fragment, filtered
            {"id": "m2", "alt": "", "image": {"url": "https://cdn/2.jpg"}},
        ]}}}
    )
    assert ops.get_product_media(t, "gid://shopify/Product/1") == [
        "https://cdn/1.jpg",
        "https://cdn/2.jpg",
    ]


def test_get_product_media_raises_when_product_null():
    t = FakeTransport({"product": None})
    with pytest.raises(RuntimeError, match="Product not found"):
        ops.get_product_media(t, "gid://shopify/Product/404")


def test_set_product_media_replaces_with_files_input():
    t = FakeTransport({"productSet": {"product": {"id": "gid://shopify/Product/2"}, "userErrors": []}})
    out = ops.set_product_media(t, "gid://shopify/Product/2", ["https://cdn/1.jpg", "https://cdn/2.jpg"])
    assert out["product"]["id"] == "gid://shopify/Product/2"
    inp = t.calls[0]["variables"]["input"]
    assert inp["id"] == "gid://shopify/Product/2"
    assert inp["files"] == [
        {"originalSource": "https://cdn/1.jpg", "alt": "", "contentType": "IMAGE"},
        {"originalSource": "https://cdn/2.jpg", "alt": "", "contentType": "IMAGE"},
    ]


def test_set_product_media_raises_on_user_errors_fix3():
    t = FakeTransport(
        {"productSet": {"product": None, "userErrors": [{"code": "X", "field": "files", "message": "bad url"}]}}
    )
    with pytest.raises(ShopifyUserError):
        ops.set_product_media(t, "gid://shopify/Product/2", ["https://cdn/1.jpg"])


# ---------------------------------------------------------------------------
# locations
# ---------------------------------------------------------------------------

def test_get_locations_walks_pagination():
    page1 = {"locations": {
        "edges": [{"node": {"id": "gid://shopify/Location/1", "name": "Promo", "isActive": True}}],
        "pageInfo": {"hasNextPage": True, "endCursor": "CUR1"},
    }}
    page2 = {"locations": {
        "edges": [{"node": {"id": "gid://shopify/Location/2", "name": "Magazzino", "isActive": True}}],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    }}
    t = FakeTransport([page1, page2])
    locs = ops.get_locations(t)
    assert [l["name"] for l in locs] == ["Promo", "Magazzino"]
    assert len(t.calls) == 2
    assert t.calls[1]["variables"]["after"] == "CUR1"


def test_get_location_by_name_returns_gid_node_on_hit():
    page = {"locations": {
        "edges": [{"node": {"id": "gid://shopify/Location/1", "name": "Promo", "isActive": True}}],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    }}
    t = FakeTransport(page)
    node = ops.get_location_by_name(t, "Promo")
    assert node["id"] == "gid://shopify/Location/1"  # GID, not a numeric int


def test_get_location_by_name_returns_none_on_miss():
    page = {"locations": {
        "edges": [{"node": {"id": "gid://shopify/Location/1", "name": "Promo", "isActive": True}}],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    }}
    t = FakeTransport(page)
    assert ops.get_location_by_name(t, "Nonexistent") is None


# ---------------------------------------------------------------------------
# inventory_activate
# ---------------------------------------------------------------------------

def test_inventory_activate_pure_connect_omits_quantities():
    t = FakeTransport({"inventoryActivate": {"inventoryLevel": {"id": "gid://shopify/InventoryLevel/1"}, "userErrors": []}})
    out = ops.inventory_activate(t, "gid://shopify/InventoryItem/1", "gid://shopify/Location/1")
    assert out["inventoryLevel"]["id"] == "gid://shopify/InventoryLevel/1"
    v = t.calls[0]["variables"]
    assert v["inventoryItemId"] == "gid://shopify/InventoryItem/1"
    assert v["locationId"] == "gid://shopify/Location/1"
    # pure connect: no seeded quantities
    assert v["onHand"] is None
    assert v["available"] is None


def test_inventory_activate_idempotent_reactivate_no_raise():
    # already-active item -> no userError -> must not raise
    t = FakeTransport({"inventoryActivate": {"inventoryLevel": {"id": "gid://shopify/InventoryLevel/1"}, "userErrors": []}})
    ops.inventory_activate(t, "gid://shopify/InventoryItem/1", "gid://shopify/Location/1")


def test_inventory_activate_seeds_on_hand_leaves_available_absent():
    t = FakeTransport({"inventoryActivate": {"inventoryLevel": {"id": "gid://shopify/InventoryLevel/1"}, "userErrors": []}})
    ops.inventory_activate(t, "gid://shopify/InventoryItem/1", "gid://shopify/Location/1", on_hand=3)
    v = t.calls[0]["variables"]
    assert v["onHand"] == 3
    assert v["available"] is None


def test_inventory_activate_raises_on_user_errors_fix3():
    t = FakeTransport({"inventoryActivate": {"inventoryLevel": None, "userErrors": [{"field": "locationId", "message": "bad"}]}})
    with pytest.raises(ShopifyUserError):
        ops.inventory_activate(t, "gid://shopify/InventoryItem/1", "gid://shopify/Location/1")


# ---------------------------------------------------------------------------
# inventory_set_quantities
# ---------------------------------------------------------------------------

def test_inventory_set_quantities_uses_on_hand_name_and_threads_quantity():
    t = FakeTransport({"inventorySetQuantities": {"inventoryAdjustmentGroup": {"changes": []}, "userErrors": []}})
    ops.inventory_set_quantities(t, "gid://shopify/InventoryItem/1", "gid://shopify/Location/1", 7)
    inp = t.calls[0]["variables"]["input"]
    assert inp["name"] == "on_hand"
    assert inp["reason"] == "correction"
    assert inp["ignoreCompareQuantity"] is True
    q = inp["quantities"][0]
    assert q == {
        "inventoryItemId": "gid://shopify/InventoryItem/1",
        "locationId": "gid://shopify/Location/1",
        "quantity": 7,
    }


def test_inventory_set_quantities_raises_on_user_errors_fix3():
    t = FakeTransport({"inventorySetQuantities": {"inventoryAdjustmentGroup": None, "userErrors": [{"code": "INVALID", "field": "quantity", "message": "nope"}]}})
    with pytest.raises(ShopifyUserError):
        ops.inventory_set_quantities(t, "gid://shopify/InventoryItem/1", "gid://shopify/Location/1", 3)


def test_inventory_set_quantities_not_stocked_triggers_activate_then_retry():
    calls_order: List[str] = []

    def route(query, _variables):
        if "inventorySetQuantities" in query:
            calls_order.append("set")
            # first set -> not stocked; second set (after activate) -> success
            if calls_order.count("set") == 1:
                return {"inventorySetQuantities": {"inventoryAdjustmentGroup": None,
                        "userErrors": [{"code": "ITEM_NOT_STOCKED_AT_LOCATION", "field": None, "message": "not stocked at location"}]}}
            return {"inventorySetQuantities": {"inventoryAdjustmentGroup": {"changes": []}, "userErrors": []}}
        if "inventoryActivate" in query:
            calls_order.append("activate")
            return {"inventoryActivate": {"inventoryLevel": {"id": "gid://shopify/InventoryLevel/1"}, "userErrors": []}}
        raise AssertionError(f"unexpected query: {query}")

    t = FakeTransport(route)
    out = ops.inventory_set_quantities(t, "gid://shopify/InventoryItem/1", "gid://shopify/Location/1", 5)
    assert out["userErrors"] == []
    assert calls_order == ["set", "activate", "set"]  # single bounded retry


# ---------------------------------------------------------------------------
# inventory_deactivate  (+ get_inventory_level_id)
# ---------------------------------------------------------------------------

def test_get_inventory_level_id_returns_id_and_none():
    hit = FakeTransport({"inventoryItem": {"inventoryLevel": {"id": "gid://shopify/InventoryLevel/9"}}})
    assert ops.get_inventory_level_id(hit, "i", "l") == "gid://shopify/InventoryLevel/9"

    miss = FakeTransport({"inventoryItem": {"inventoryLevel": None}})
    assert ops.get_inventory_level_id(miss, "i", "l") is None


def test_get_inventory_level_id_returns_none_when_item_null():
    t = FakeTransport({"inventoryItem": None})
    assert ops.get_inventory_level_id(t, "i", "l") is None


def test_inventory_deactivate_null_level_is_noop_no_deactivate_call():
    def route(query, _variables):
        assert "inventoryDeactivate" not in query, "must not deactivate a null level"
        return {"inventoryItem": {"inventoryLevel": None}}

    t = FakeTransport(route)
    assert ops.inventory_deactivate(t, "i", "l") is None
    # only the level-id lookup happened
    assert len(t.calls) == 1


def test_inventory_deactivate_happy_path():
    def route(query, _variables):
        if "inventoryDeactivate" in query:
            return {"inventoryDeactivate": {"userErrors": []}}
        return {"inventoryItem": {"inventoryLevel": {"id": "gid://shopify/InventoryLevel/9"}}}

    t = FakeTransport(route)
    assert ops.inventory_deactivate(t, "i", "l") is None
    deact = next(c for c in t.calls if "inventoryDeactivate" in c["query"])
    assert deact["variables"] == {"inventoryLevelId": "gid://shopify/InventoryLevel/9"}


def test_inventory_deactivate_raises_on_user_errors_fix3():
    def route(query, _variables):
        if "inventoryDeactivate" in query:
            return {"inventoryDeactivate": {"userErrors": [{"field": "inventoryLevelId", "message": "has stock"}]}}
        return {"inventoryItem": {"inventoryLevel": {"id": "gid://shopify/InventoryLevel/9"}}}

    t = FakeTransport(route)
    with pytest.raises(ShopifyUserError):
        ops.inventory_deactivate(t, "i", "l")
