"""Stateless SKU -> product resolvers (outlet vs source) over the Shopify transport.

Two pure resolvers that classify the products carrying a given SKU into two
complementary buckets:

* :func:`outlet_resolver` — the OUTLET products for that SKU;
* :func:`source_resolver` — the full-price SOURCE products for that SKU.

They replace the fragile legacy heuristics in ``src/sync.py``:
``find_outlet_by_sku`` (297-384), ``find_product_by_sku_non_outlet`` (252-280),
and make ``find_product_by_handle`` (282-295, no live call-site) and every
``handle.endswith('-outlet')`` guess obsolete. Membership in the ground-truth
OUTLET collection (via GraphQL ``inCollection``) is the AUTHORITATIVE signal;
the ``"outlet" in title`` substring is only a cross-check / safety-net that,
when it disagrees with membership, raises ``review`` instead of deciding alone.

Convention mirror of ``backend/shopify/ops.py``: first arg is always the
``transport``; no module state, no HTTP session, no secrets, no cache. Pure
function of ``(store-state, sku)``. READ-ONLY: only ``transport.graphql`` queries,
no mutation, no side effect. Transport-level errors (``ShopifyTransportError``)
propagate — they are not caught here.

Deliberate non-goals: a resolver never "resolves" an ambiguity (>1 outlet, mixed
SKUs, over-match) — it SURFACES it in ``warning`` and returns every match so the
caller (publish / delete / price) decides. ``matches`` is never auto-reduced to
one element.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.shopify.transport import ShopifyTransport

# Ground-truth OUTLET collection (numeric REST id 650952442188 -> GID form).
# inCollection(id:) requires the GID form; this is the authoritative membership set.
OUTLET_COLLECTION_GID = "gid://shopify/Collection/650952442188"
# products(first:) headroom: original + duplicates + <=10 outlets + slack. A set
# larger than this = degenerate/over-tokenized SKU, flagged TRUNCATED, not paged.
CANDIDATE_PAGE_SIZE = 100
# variants(first:) per candidate — shoe size ladders stay well under this.
VARIANTS_PAGE_SIZE = 100


# Single candidate-fetch query. ``$q`` (the "sku:<value>" search) and the outlet
# collection id are passed as VARIABLES, never interpolated into the document
# (transport contract: parametrized queries). ``inCollection`` is the stateless
# per-candidate membership primitive that replaces enumerating all 182 members
# and correctly includes DRAFT/ARCHIVED outlets.
_CANDIDATE_QUERY = """
query($q: String!, $outletId: ID!, $first: Int!, $vfirst: Int!) {
  products(first: $first, query: $q) {
    edges {
      node {
        id
        title
        handle
        status
        inCollection(id: $outletId)
        variants(first: $vfirst) {
          edges { node { id sku } }
        }
      }
    }
    pageInfo { hasNextPage }
  }
}"""


def _escape(value: str) -> str:
    """Neutralize backslash and quotes in a SKU before embedding it in ``sku:<v>``.

    The value goes into the Shopify *search* mini-syntax (still passed as a
    GraphQL variable, so JSON escaping is the transport's job). Backslash first,
    then single/double quotes, so a SKU containing a quote can't break the search
    parse.
    """
    return value.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')


def _resolve_candidates(
    transport: ShopifyTransport, sku: str
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Fetch + exact-verify + dedup + classify the products carrying ``sku``.

    Returns ``(candidates, meta)`` where ``candidates`` is the deduplicated,
    classified list (each item is the ``Match`` shape below) and ``meta`` carries
    the two resolver-level warning inputs that are NOT per-candidate:
    ``{"server_count": int, "truncated": bool}``. (The SPEC nominal signature was
    ``-> list[Candidate]``; ``meta`` is appended because TRUNCATED needs
    ``pageInfo.hasNextPage`` and NO_EXACT needs the raw server-candidate count,
    neither derivable from an empty candidate list.)

    Stages:

    1. **Fetch** — one query. ``sku:`` is a tokenized-AND *superset* filter: it
       never misses a product with an exact-SKU variant but returns token
       over-matches. No auto-pagination in MVP; ``hasNextPage`` -> ``truncated``.
    2. **Exact-verify** — a node is real iff some variant has
       ``(sku or "").strip() == sku`` (the ``or ""`` guard handles a null-SKU
       variant without crashing — it simply doesn't match). Zero exact => drop.
    3. **Dedup by product gid** — duplicate nodes collapse; first occurrence wins,
       ``matched_variant_gids`` unioned.
    4. **Classify** — ``is_outlet_member`` from ``inCollection`` (AUTHORITATIVE),
       ``title_is_outlet`` from ``"outlet" in title.casefold()`` (CROSS-CHECK),
       ``review`` when the two disagree (smart-collection reindex lag, or "outlet"
       as an incidental substring).
    """
    q = "sku:" + _escape(sku)
    data = transport.graphql(
        _CANDIDATE_QUERY,
        {
            "q": q,
            "outletId": OUTLET_COLLECTION_GID,
            "first": CANDIDATE_PAGE_SIZE,
            "vfirst": VARIANTS_PAGE_SIZE,
        },
    )
    connection = data["products"]
    edges = connection["edges"]
    truncated = bool(connection.get("pageInfo", {}).get("hasNextPage"))

    by_gid: "Dict[str, Dict[str, Any]]" = {}
    for edge in edges:
        node = edge["node"]

        # stage 2 — exact-verify (kill token over-match / null-SKU non-matches)
        matched: List[str] = []
        for vedge in node["variants"]["edges"]:
            variant = vedge["node"]
            if (variant.get("sku") or "").strip() == sku:
                vid = variant["id"]
                if vid not in matched:
                    matched.append(vid)
        if not matched:
            continue

        gid = node["id"]
        # stage 3 — dedup by product gid (first wins, matched gids unioned)
        existing = by_gid.get(gid)
        if existing is not None:
            for vid in matched:
                if vid not in existing["matched_variant_gids"]:
                    existing["matched_variant_gids"].append(vid)
            continue

        # stage 4 — classify
        title = node.get("title") or ""
        is_member = bool(node.get("inCollection"))
        title_is_outlet = "outlet" in title.casefold()
        by_gid[gid] = {
            "product_gid": gid,
            "title": title,
            "handle": node.get("handle") or "",
            "status": node.get("status") or "",
            "is_outlet_member": is_member,
            "title_is_outlet": title_is_outlet,
            "review": is_member != title_is_outlet,
            "matched_variant_gids": matched,
        }

    return list(by_gid.values()), {"server_count": len(edges), "truncated": truncated}


def _build_result(
    candidates: List[Dict[str, Any]],
    meta: Dict[str, Any],
    predicate: Callable[[Dict[str, Any]], bool],
    sku: str,
    multi_label: str,
) -> Dict[str, Any]:
    """Apply a bucket predicate to the shared candidates and compute the warning.

    ``matches`` is always returned in full (never auto-picked); ``warning`` is a
    "; "-joined diagnostic string (or ``None`` for the clean single-match case).
    NO_EXACT keys off ``len(candidates) == 0`` (nothing survived exact-verify),
    NOT ``len(matches) == 0`` — an empty bucket where exact matches exist but all
    landed in the *other* bucket (e.g. source_resolver on a SKU that only has an
    outlet) is a legitimate "none here", not an anomaly.

    The result also carries a structured ``truncated`` bool (mirroring the
    ``TRUNCATED`` warning text) so a caller can fold it into its own freshness
    flag instead of parsing the human-readable ``warning`` string.
    """
    matches = [c for c in candidates if predicate(c)]
    warnings: List[str] = []

    if len(matches) > 1:
        gids = [m["product_gid"] for m in matches]
        warnings.append(f"{multi_label}: {len(matches)} distinti per sku={sku}: {gids}")

    for m in matches:
        if m["review"]:
            warnings.append(
                f"REVIEW: {m['product_gid']} membership={m['is_outlet_member']} "
                f"title={m['title_is_outlet']}"
            )

    if not candidates and meta["server_count"] > 0:
        warnings.append(
            f"NO_EXACT: server ha restituito {meta['server_count']} candidati, "
            f"nessuno con variante sku esatto (possibile variante SKU null o "
            f"over-match token)"
        )

    if meta["truncated"]:
        warnings.append(
            "TRUNCATED: set candidati >"
            f"{CANDIDATE_PAGE_SIZE}, sku troppo generico/tokenizzato — restringere"
        )

    for m in matches:
        if len(m["matched_variant_gids"]) > 1:
            warnings.append(
                f"MIXED_SKU: {m['product_gid']} ha più varianti con lo stesso sku input"
            )

    warning: Optional[str] = "; ".join(warnings) if warnings else None
    return {"matches": matches, "warning": warning, "truncated": bool(meta["truncated"])}


def outlet_resolver(transport: ShopifyTransport, sku: str) -> Dict[str, Any]:
    """Resolve the OUTLET product(s) carrying ``sku``.

    Keeps candidates where ``is_outlet_member OR title_is_outlet``: membership is
    sufficient and authoritative; the title match is a second sufficient signal
    that catches a DRAFT/reindex-lag outlet before the smart-rule updates (such a
    title-only outlet is included but flagged ``review=True``). Returns
    ``{"matches": [...], "warning": str | None}`` — every distinct outlet is
    returned (``MULTI_OUTLET`` warning, no auto-pick).
    """
    candidates, meta = _resolve_candidates(transport, sku)
    return _build_result(
        candidates,
        meta,
        lambda c: c["is_outlet_member"] or c["title_is_outlet"],
        sku,
        "MULTI_OUTLET",
    )


def source_resolver(transport: ShopifyTransport, sku: str) -> Dict[str, Any]:
    """Resolve the full-price SOURCE product(s) carrying ``sku``.

    Keeps candidates where ``NOT is_outlet_member AND NOT title_is_outlet``: a
    product is a source only if it fails BOTH outlet signals. This is the strict
    "NEVER return/duplicate a product that is itself an outlet" guard — it excludes
    an outlet-by-title-only (``member=False, title=True``) even before the smart
    collection catches up, the direct fix for the legacy ``handle.endswith('-outlet')``
    that leaked such outlets through as "source". Returns
    ``{"matches": [...], "warning": str | None}`` (``MULTI_SOURCE`` on >1).
    """
    candidates, meta = _resolve_candidates(transport, sku)
    return _build_result(
        candidates,
        meta,
        lambda c: not c["is_outlet_member"] and not c["title_is_outlet"],
        sku,
        "MULTI_SOURCE",
    )
