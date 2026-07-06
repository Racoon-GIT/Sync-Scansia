"""backend.services.resolvers — outlet_resolver / source_resolver.

Pure resolvers over a mocked transport (no HTTP, no live store). Same
FakeTransport pattern as test_ops_netnew.py. Each resolver issues exactly one
GraphQL call (no MVP auto-pagination), so a single-dict FakeTransport returning
the same payload every call is enough — even when a test drives both resolvers.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Union

from backend.services.resolvers import (
    OUTLET_COLLECTION_GID,
    outlet_resolver,
    source_resolver,
)


class FakeTransport:
    """Records every ``graphql`` call and returns a canned response.

    ``responses`` is either a single dict (returned every call) or a callable
    ``(query, variables) -> dict``.
    """

    def __init__(
        self, responses: Union[Dict[str, Any], Callable[..., Dict[str, Any]]]
    ) -> None:
        self._responses = responses
        self.calls: List[Dict[str, Any]] = []

    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        self.calls.append({"query": query, "variables": variables})
        if callable(self._responses):
            return self._responses(query, variables)
        return self._responses


def _product_node(
    gid: str,
    title: str,
    handle: str,
    status: str,
    in_collection: bool,
    variant_skus: List[Optional[str]],
) -> Dict[str, Any]:
    return {
        "node": {
            "id": gid,
            "title": title,
            "handle": handle,
            "status": status,
            "inCollection": in_collection,
            "variants": {
                "edges": [
                    {"node": {"id": f"{gid}/variant/{i}", "sku": sku}}
                    for i, sku in enumerate(variant_skus, start=1)
                ]
            },
        }
    }


def _products_response(nodes: List[Dict[str, Any]], has_next: bool = False) -> Dict[str, Any]:
    return {"products": {"edges": nodes, "pageInfo": {"hasNextPage": has_next}}}


# ---------------------------------------------------------------------------
# Core requirement: SKU shared by outlet + original -> complementary buckets
# ---------------------------------------------------------------------------

def test_shared_sku_splits_outlet_from_source():
    outlet = _product_node(
        "gid://shopify/Product/OUT", "Nike Air - Outlet", "nike-air-outlet", "ACTIVE", True, ["ABC"]
    )
    original = _product_node(
        "gid://shopify/Product/SRC", "Nike Air", "nike-air", "ACTIVE", False, ["ABC"]
    )
    t = FakeTransport(_products_response([outlet, original]))

    o = outlet_resolver(t, "ABC")
    assert [m["product_gid"] for m in o["matches"]] == ["gid://shopify/Product/OUT"]
    assert o["warning"] is None

    s = source_resolver(t, "ABC")
    assert [m["product_gid"] for m in s["matches"]] == ["gid://shopify/Product/SRC"]
    assert s["warning"] is None


# ---------------------------------------------------------------------------
# 10-SKU -> >1 outlet: return all, warn, never auto-pick
# ---------------------------------------------------------------------------

def test_multiple_outlets_for_sku_returns_all_with_warning():
    o1 = _product_node("gid://shopify/Product/O1", "X - Outlet", "x-outlet", "ACTIVE", True, ["SKU10"])
    o2 = _product_node("gid://shopify/Product/O2", "X - Outlet 2", "x-outlet-1", "DRAFT", True, ["SKU10"])
    t = FakeTransport(_products_response([o1, o2]))

    r = outlet_resolver(t, "SKU10")
    assert {m["product_gid"] for m in r["matches"]} == {
        "gid://shopify/Product/O1",
        "gid://shopify/Product/O2",
    }
    assert r["warning"] is not None
    assert "MULTI_OUTLET" in r["warning"]


# ---------------------------------------------------------------------------
# NEVER treat an outlet as a source (membership OR title-only)
# ---------------------------------------------------------------------------

def test_title_only_outlet_excluded_from_source_and_flagged_review():
    # member=False but title says Outlet: the fragile handle heuristic used to leak
    # this through as "source". source_resolver must exclude it; outlet_resolver
    # must include it AND flag review (membership/title disagree).
    title_only = _product_node(
        "gid://shopify/Product/TO", "Nike - Outlet", "nike-outlet-1", "DRAFT", False, ["ABC"]
    )
    t = FakeTransport(_products_response([title_only]))

    assert source_resolver(t, "ABC")["matches"] == []

    o = outlet_resolver(t, "ABC")
    assert [m["product_gid"] for m in o["matches"]] == ["gid://shopify/Product/TO"]
    assert o["matches"][0]["review"] is True
    assert "REVIEW" in o["warning"]


def test_member_outlet_never_leaks_into_source():
    outlet = _product_node(
        "gid://shopify/Product/OUT", "Plain Title", "plain", "ACTIVE", True, ["ABC"]
    )
    t = FakeTransport(_products_response([outlet]))
    # membership is authoritative: even with a non-outlet title it's not a source.
    assert source_resolver(t, "ABC")["matches"] == []
    assert [m["product_gid"] for m in outlet_resolver(t, "ABC")["matches"]] == [
        "gid://shopify/Product/OUT"
    ]


# ---------------------------------------------------------------------------
# SKU null handling
# ---------------------------------------------------------------------------

def test_null_sku_variant_does_not_crash_and_matches_only_exact():
    p = _product_node(
        "gid://shopify/Product/P", "Nike", "nike", "ACTIVE", False, [None, "ABC"]
    )
    t = FakeTransport(_products_response([p]))
    r = source_resolver(t, "ABC")
    assert len(r["matches"]) == 1
    assert r["matches"][0]["matched_variant_gids"] == ["gid://shopify/Product/P/variant/2"]


def test_null_sku_only_yields_no_exact_warning():
    # server over-returned a product whose only variant SKU is null -> no exact match
    p = _product_node("gid://shopify/Product/P", "Nike", "nike", "ACTIVE", False, [None])
    t = FakeTransport(_products_response([p]))
    r = source_resolver(t, "ABC")
    assert r["matches"] == []
    assert "NO_EXACT" in r["warning"]


# ---------------------------------------------------------------------------
# Dedup, token over-match, mixed SKU, truncation, empty
# ---------------------------------------------------------------------------

def test_duplicate_product_gid_collapsed():
    dup1 = _product_node("gid://shopify/Product/D", "Nike - Outlet", "nike-outlet", "ACTIVE", True, ["ABC"])
    dup2 = _product_node("gid://shopify/Product/D", "Nike - Outlet", "nike-outlet", "ACTIVE", True, ["ABC"])
    t = FakeTransport(_products_response([dup1, dup2]))
    r = outlet_resolver(t, "ABC")
    assert len(r["matches"]) == 1
    assert r["matches"][0]["matched_variant_gids"] == ["gid://shopify/Product/D/variant/1"]


def test_token_over_match_discarded_by_exact_verify():
    noise = _product_node("gid://shopify/Product/N", "Other", "other", "ACTIVE", False, ["ABC-XL"])
    real = _product_node("gid://shopify/Product/R", "Nike", "nike", "ACTIVE", False, ["ABC"])
    t = FakeTransport(_products_response([noise, real]))
    r = source_resolver(t, "ABC")
    assert [m["product_gid"] for m in r["matches"]] == ["gid://shopify/Product/R"]
    assert r["warning"] is None


def test_mixed_sku_same_product_flags_matched_variants():
    mixed = _product_node("gid://shopify/Product/M", "Nike", "nike", "ACTIVE", False, ["ABC", "ABC"])
    t = FakeTransport(_products_response([mixed]))
    r = source_resolver(t, "ABC")
    assert len(r["matches"]) == 1
    assert len(r["matches"][0]["matched_variant_gids"]) == 2
    assert "MIXED_SKU" in r["warning"]


def test_truncated_when_has_next_page():
    p = _product_node("gid://shopify/Product/P", "Nike", "nike", "ACTIVE", False, ["ABC"])
    t = FakeTransport(_products_response([p], has_next=True))
    r = source_resolver(t, "ABC")
    assert "TRUNCATED" in r["warning"]


def test_no_candidates_is_clean_empty():
    t = FakeTransport(_products_response([]))
    r = outlet_resolver(t, "NOPE")
    assert r["matches"] == []
    assert r["warning"] is None  # server returned nothing -> not NO_EXACT


# ---------------------------------------------------------------------------
# Query is parametrized (transport contract) + outlet GID + escaping
# ---------------------------------------------------------------------------

def test_query_passes_sku_as_variable_and_escapes():
    captured: Dict[str, Any] = {}

    def responder(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        captured["query"] = query
        captured["variables"] = variables
        return _products_response([])

    t = FakeTransport(responder)
    outlet_resolver(t, "AB'C")

    assert captured["variables"]["q"] == "sku:AB\\'C"
    assert captured["variables"]["outletId"] == OUTLET_COLLECTION_GID
    # sku value is a VARIABLE, never interpolated into the document
    assert "$q: String!" in captured["query"]
    assert "AB'C" not in captured["query"]
