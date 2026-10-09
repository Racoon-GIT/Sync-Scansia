"""backend.shopify.ops — stateless GraphQL op-wrappers.

The transport is mocked with an in-memory fake (``.graphql(query, variables)``
returns a predefined dict). No HTTP, no live store, per project rules.

Each op is covered for happy-path + fix3 (non-empty userErrors -> raise). The
inventory writes at the pin (2026-10, F7 §7): a relative ``available`` adjust
carrying ``changeFromQuantity`` and the ``@idempotent`` key, an explicit
activation (0 / 0), a key-less deactivation, a body built once and re-sent as
stored, the answer parsed (codes / changes / level), and the live level read
with ``includeInactive``.
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
        self.bodies: List[str] = []

    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        return self._answer(query, variables)

    def send_body(self, body: str) -> Dict[str, Any]:
        import json

        doc = json.loads(body)
        self.bodies.append(body)
        return self._answer(doc["query"], doc["variables"])

    def _answer(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
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
    dup = ops.product_duplicate(t, "gid://shopify/Product/1", "Nike - OUTLET")
    assert dup.product_gid == "gid://shopify/Product/999"
    assert (dup.image_job_id, dup.image_job_done) == (None, None)     # no imageJob in the answer: tolerated
    assert t.calls[0]["variables"] == {
        "productId": "gid://shopify/Product/1",
        "newTitle": "Nike - OUTLET",
        "newStatus": "DRAFT",
        "includeImages": True,
    }


def test_product_duplicate_copies_the_images_and_returns_the_job_f3():
    """§8 F3 (docs/f7.3-live-tests.md): ``includeImages`` defaults to false at 2026-10,
    so an outlet duplicated without it has no images. D1 sends ``includeImages: true``
    and selects ``imageJob { id done }`` — the copy is an asynchronous job."""
    t = FakeTransport({"productDuplicate": {"newProduct": {"id": "gid://shopify/Product/999"},
                                            "imageJob": {"id": "gid://shopify/Job/7", "done": False},
                                            "userErrors": []}})
    dup = ops.product_duplicate(t, "gid://shopify/Product/1", "Nike - OUTLET")
    assert t.calls[0]["variables"]["includeImages"] is True
    assert "$includeImages: Boolean" in t.calls[0]["query"]
    assert "includeImages: $includeImages" in t.calls[0]["query"]
    assert "imageJob { id done }" in t.calls[0]["query"]
    assert (dup.product_gid, dup.image_job_id, dup.image_job_done) == (
        "gid://shopify/Product/999", "gid://shopify/Job/7", False)


def test_product_duplicate_tolerates_a_null_image_job():
    t = FakeTransport({"productDuplicate": {"newProduct": {"id": "gid://shopify/Product/999"}, "imageJob": None,
                                            "userErrors": []}})
    dup = ops.product_duplicate(t, "gid://shopify/Product/1", "Nike - OUTLET")
    assert (dup.product_gid, dup.image_job_id, dup.image_job_done) == ("gid://shopify/Product/999", None, None)


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
# Inventory writes at the pin (F7 §3.3 rules 2, 3, 9; E15)
# ---------------------------------------------------------------------------

def test_adjust_request_is_relative_fenced_and_keyed():
    import json

    body = ops.adjust_request("gid://shopify/InventoryItem/1", "gid://shopify/Location/9", -1, 3, "key-1",
                              reference="gid://scansia-manager/OutletReconcile/5")
    doc = json.loads(body)
    assert "@idempotent(key: $idempotencyKey)" in doc["query"] and "inventoryAdjustQuantities(" in doc["query"]
    assert doc["variables"]["idempotencyKey"] == "key-1"
    inp = doc["variables"]["input"]
    assert (inp["name"], inp["reason"]) == ("available", "correction")
    assert inp["referenceDocumentUri"] == "gid://scansia-manager/OutletReconcile/5"
    assert inp["changes"] == [{"inventoryItemId": "gid://shopify/InventoryItem/1",
                               "locationId": "gid://shopify/Location/9", "delta": -1, "changeFromQuantity": 3}]
    assert "compareQuantity" not in body and "ignoreCompareQuantity" not in body   # removed at 2026-04


def test_adjust_request_sends_an_explicit_null_change_from():
    import json

    inp = json.loads(ops.adjust_request("i", "l", 1, None, "k"))["variables"]["input"]
    assert inp["changes"][0]["changeFromQuantity"] is None and "referenceDocumentUri" not in inp


def test_activate_request_is_explicit_zero_and_keyed():
    import json

    doc = json.loads(ops.activate_request("i", "l", "k"))
    v = doc["variables"]
    assert (v["available"], v["onHand"], v["idempotencyKey"]) == (0, 0, "k")
    assert "@idempotent(key: $idempotencyKey)" in doc["query"] and "userErrors { field message }" in doc["query"]


def test_deactivate_request_carries_no_key():
    import json

    doc = json.loads(ops.deactivate_request("gid://shopify/InventoryLevel/3"))
    assert doc["variables"] == {"inventoryLevelId": "gid://shopify/InventoryLevel/3"}
    assert "@idempotent" not in doc["query"]


def test_send_inventory_posts_the_stored_body_and_parses_codes_and_changes():
    body = ops.adjust_request("i", "l", -1, 2, "k")
    t = FakeTransport({"inventoryAdjustQuantities": {
        "inventoryAdjustmentGroup": {"changes": [{"name": "available", "delta": -1, "quantityAfterChange": 1}]},
        "userErrors": []}})
    ans = ops.send_inventory(t, body)
    assert t.bodies == [body]
    assert ans.ok and ans.codes == () and ans.changes[0]["quantityAfterChange"] == 1
    t2 = FakeTransport({"inventoryAdjustQuantities": {"inventoryAdjustmentGroup": None, "userErrors": [
        {"field": None, "message": "stale", "code": "CHANGE_FROM_QUANTITY_STALE"}]}})
    ans2 = ops.send_inventory(t2, body)
    assert not ans2.ok and ans2.codes == ("CHANGE_FROM_QUANTITY_STALE",)


def test_send_inventory_reads_an_activation_level_and_codeless_errors():
    body = ops.activate_request("i", "l", "k")
    ok = ops.send_inventory(FakeTransport({"inventoryActivate": {
        "inventoryLevel": {"id": "gid://shopify/InventoryLevel/1", "isActive": True}, "userErrors": []}}), body)
    assert ok.ok and ok.level == {"id": "gid://shopify/InventoryLevel/1", "isActive": True}
    bad = ops.send_inventory(FakeTransport({"inventoryActivate": {"inventoryLevel": None, "userErrors": [
        {"field": ["locationId"], "message": "nope"}]}}), body)
    assert not bad.ok and bad.codes == ()              # plain UserError: no code to read


def test_read_level_includes_inactive_and_tells_absent_from_gone():
    seen = {}

    def route(query, variables):
        seen["q"] = query
        if variables["id"] == "gone":
            return {"inventoryItem": None}
        if variables["loc"] == "absent":
            return {"inventoryItem": {"id": variables["id"], "inventoryLevel": None}}
        return {"inventoryItem": {"id": variables["id"], "inventoryLevel": {
            "id": "L1", "isActive": False, "quantities": [{"name": "available", "quantity": 2},
                                                           {"name": "committed", "quantity": 1},
                                                           {"name": "on_hand", "quantity": 3}]}}}

    t = FakeTransport(route)
    assert ops.read_level(t, "gone", "l") == {"item_found": False, "level": None}
    assert ops.read_level(t, "i", "absent") == {"item_found": True, "level": None}
    assert ops.read_level(t, "i", "l")["level"] == {"id": "L1", "is_active": False, "available": 2,
                                                   "committed": 1, "on_hand": 3}
    assert "includeInactive: true" in seen["q"]


def test_deny_tracked_inputs_cover_every_variant_with_extras():
    rows = ops.deny_tracked_inputs([{"id": "v1"}, {"id": "v2"}], price="50.00")
    assert rows == [{"id": "v1", "inventoryPolicy": "DENY", "inventoryItem": {"tracked": True}, "price": "50.00"},
                    {"id": "v2", "inventoryPolicy": "DENY", "inventoryItem": {"tracked": True}, "price": "50.00"}]


def test_the_retired_inventory_ops_are_gone():
    for name in ("inventory_set_quantities", "inventory_activate", "inventory_deactivate",
                 "get_inventory_level_id", "_is_not_stocked"):
        assert not hasattr(ops, name), name
