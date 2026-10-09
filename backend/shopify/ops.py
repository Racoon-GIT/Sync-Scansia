"""Stateless Shopify Admin GraphQL op-wrappers.

Every function is a pure, stateless wrapper around a single GraphQL
operation. The first argument is always a ``ShopifyTransport`` (see
``backend.shopify.transport``); the wrapper only calls
``transport.graphql(query, variables)`` and shapes the result. No module-level
state, no HTTP session ownership, no secrets — those all live in the transport.

M1a migration notes (authoritative source: ``src/sync.py`` legacy methods — removed
with F7 §3.8; the lines cited are ``git show 8aab2a3:src/sync.py``):

* API version: PINNED at 2026-10 (``backend.config.PINNED_API_VERSION``, F7
  §7): every document here is written against it — ``@idempotent`` and
  ``changeFromQuantity`` on inventory writes, explicit activation,
  ``includeInactive`` reads, ``productUpdate(product:)``, ``Collection.sources``.
* **fix3** — every mutation inspects its own ``userErrors`` and RAISES
  (:class:`ShopifyUserError`) when they are non-empty. Legacy variously
  ``logger.warning``'d and swallowed them (price update sync.py:465-467,
  metafields sync.py:562, images sync.py:513-515) or raised only on 4xx. The
  M1a norm is: real failures propagate. Two cases never reach a mutation:
    - an already-active level is never re-activated (``ledger.send_activate``
      reads ``isActive`` first; the store answers ``userErrors`` there — F7.3
      run 202610091404, R053);
    - deactivating a null/absent inventory level is a no-op (the level-id
      lookup returns ``None`` before any mutation runs).
* **fix7** — image copy is a single declarative ``productSet`` (full replace):
  the legacy REST ``/images.json`` delete-all + re-add branch and the
  ``productCreateMedia`` batch branch are gone (no chunking, no sleeps, no env
  toggles).
* IDs are GID strings end-to-end (the REST numeric-int shape is gone); callers
  pass ``v["inventoryItem"]["id"]`` and location GIDs straight through.

The orchestration around these ops (loop over the SKU group, chunking metafields
to <=25/call, dry-run gating, the env-var direct-location-ID bypass) lives in the
service layer, NOT here.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from backend.shopify.transport import ShopifyTransport, request_body

# Shopify 2025-07 hard cap for metafieldsSet inputs per call.
METAFIELDS_SET_MAX = 25
# Single-page connection cap used by the read queries (matches legacy first:250).
CONNECTION_PAGE_SIZE = 250


class ProductNotFound(RuntimeError):
    """A read answered a definitive null for the product (``node`` / ``product``
    null) — terminal path (ii) of F7 §3.3 rule 2 reads it as "gone"."""

    def __init__(self, gid: str) -> None:
        self.gid = gid
        super().__init__(f"Product not found for GID: {gid}")


class ShopifyUserError(RuntimeError):
    """A GraphQL mutation returned a non-empty ``userErrors`` array (fix3).

    Carries the mutation field name and the raw errors list so callers/tests
    can inspect the payload instead of parsing a string. The ``str()``
    message itself stays generic and bounded (no raw userErrors payload) per
    the "never leak raw Error.message to the boundary" invariant; inspect
    ``.mutation`` / ``.errors`` programmatically for details.
    """

    def __init__(self, mutation: str, errors: List[Dict[str, Any]]) -> None:
        self.mutation = mutation
        self.errors = errors
        super().__init__(f"{mutation} failed with {len(errors)} userError(s)")


def _check_user_errors(data: Dict[str, Any], mutation_key: str) -> Dict[str, Any]:
    """Return the mutation payload, raising :class:`ShopifyUserError` on userErrors.

    fix3: never swallow userErrors — this is the single choke point every
    mutation wrapper routes through.
    """
    payload = data.get(mutation_key) or {}
    errors = payload.get("userErrors") or []
    if errors:
        raise ShopifyUserError(mutation_key, errors)
    return payload


# =============================================================================
# Products
# =============================================================================

_PRODUCT_DUPLICATE = """
mutation($productId: ID!, $newTitle: String!, $newStatus: ProductStatus, $includeImages: Boolean) {
  productDuplicate(productId: $productId, newTitle: $newTitle, newStatus: $newStatus, includeImages: $includeImages) {
    newProduct { id title handle status }
    imageJob { id done }
    userErrors { field message }
  }
}"""


@dataclass(frozen=True)
class Duplicated:
    """What ``productDuplicate`` answered: the duplicate's GID and the image copy's
    job (``imageJob``) — ``None`` when Shopify returns no job."""

    product_gid: str
    image_job_id: Optional[str] = None
    image_job_done: Optional[bool] = None


def product_duplicate(transport: ShopifyTransport, source_gid: str, new_title: str) -> Duplicated:
    """Duplicate a product by title, FORCING the duplicate to DRAFT and copying its
    images; return its GID and the image copy's job.

    fix1 (post-review, 2026-07): Shopify 2025-07's ``productDuplicate`` with NO
    ``newStatus`` INHERITS the source product's status. Since the source is a
    full-price ACTIVE product, an omitted ``newStatus`` means the duplicate is
    born ACTIVE — full-price stock visible/sellable — for the entire
    anti-phantom-stock finalization window (zero/disconnect non-Promo, zero
    Promo, set delta, DENY normalize) until the final
    :func:`product_update_status` flip, which becomes a no-op. Passing
    ``newStatus: DRAFT`` here makes DRAFT atomic with creation: the duplicate is
    never ACTIVE for a single instant before the pipeline finishes. Legacy
    (sync.py:386-400) passed only ``productId`` + ``newTitle`` (no
    ``includeImages``/``newStatus``) and relied on the pre-2025-07 default
    behavior (DRAFT), which 2025-07 changed to "inherit source status" — this is
    the fix.

    Images (``docs/f7.3-live-tests.md`` §8 F3): ``includeImages`` defaults to
    false (``productDuplicate.md:52-58`` at 2026-10), so without it every outlet
    was born without images. The copy is the duplicate's own and asynchronous:
    ``imageJob`` is "the asynchronous job that duplicates the product images"
    (``:102-106``), not done when the call returns — its id and ``done`` are
    returned for the caller to record. :func:`set_product_media` is not used:
    its ``productSet`` input carries ``ProductSetInput.id``, deprecated at the
    pin (§8 F1). The handle is the one Shopify generates; no caller rewrites it.
    """
    data = transport.graphql(
        _PRODUCT_DUPLICATE,
        {"productId": source_gid, "newTitle": new_title, "newStatus": "DRAFT", "includeImages": True},
    )
    payload = _check_user_errors(data, "productDuplicate")
    job = payload.get("imageJob") or {}
    return Duplicated(payload["newProduct"]["id"], job.get("id"), job.get("done"))


_GET_PRODUCT_VARIANTS = """
query($id: ID!) {
  node(id: $id) {
    ... on Product {
      variants(first: 250) {
        edges { node {
          id sku title
          price
          compareAtPrice
          inventoryItem { id }
          selectedOptions { name value }
        }}
      }
    }
  }
}"""


def get_product_variants(transport: ShopifyTransport, product_gid: str) -> List[Dict[str, Any]]:
    """Return the product's variants (first 250, single page) as flat dicts.

    Preserves exactly the fields consumed downstream (sync.py:798-850,871-887):
    ``id``, ``sku``, ``title``, ``price``, ``compareAtPrice``,
    ``inventoryItem.id``, ``selectedOptions[{name,value}]`` — these drive the
    price bulk-update (by ``id``) and inventory-by-size matching (``sku``
    equality + ``selectedOptions`` where ``name`` in {size, taglia}).

    No pagination beyond 250 (acceptable for shoe size-counts; caps silently,
    same as legacy). Legacy indexed ``data['node']`` with no null guard (a
    missing GID -> ``TypeError``, failing loudly). Here that loud failure is
    upgraded to an explicit product-not-found error.
    """
    data = transport.graphql(_GET_PRODUCT_VARIANTS, {"id": product_gid})
    node = data.get("node")
    if node is None:
        raise ProductNotFound(product_gid)
    edges = node["variants"]["edges"]
    return [e["node"] for e in edges]


_PRODUCT_VARIANTS_BULK_UPDATE = """
mutation($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    product { id }
    userErrors { field message }
  }
}"""


def product_variants_bulk_update(
    transport: ShopifyTransport,
    product_gid: str,
    variants: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Thin ``productVariantsBulkUpdate`` wrapper over a prebuilt variants list.

    ``variants`` is a list of ``ProductVariantsBulkInput`` dicts, each
    ``{"id": str, "price": str, "compareAtPrice": str|None}``. ``compareAtPrice``
    may be ``None`` and is passed through verbatim as JSON ``null`` (this clears
    the compare-at price) — never coerce or drop it.

    Empty list => EARLY RETURN no-op, the mutation is never issued
    (sync.py:454-455). fix3: non-empty ``userErrors`` RAISE (legacy only
    warned, sync.py:465-467).

    The "fetch all variants then broadcast the same price" orchestration lives
    in :func:`variants_bulk_update_prices`, not here.
    """
    if not variants:
        return None
    data = transport.graphql(
        _PRODUCT_VARIANTS_BULK_UPDATE,
        {"productId": product_gid, "variants": variants},
    )
    return _check_user_errors(data, "productVariantsBulkUpdate")


def variants_bulk_update_prices(
    transport: ShopifyTransport,
    product_gid: str,
    price: str,
    compare_at: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Convenience helper: broadcast the SAME price + compareAtPrice to every variant.

    Fetches the product's variants (:func:`get_product_variants`) then builds one
    ``ProductVariantsBulkInput`` per existing variant with identical ``price``
    and ``compareAtPrice`` (sync.py:443-467). ``compare_at`` may be ``None`` and
    flows through as ``null`` (clears compare-at). Empty product => no-op.
    """
    variants = get_product_variants(transport, product_gid)
    updates = [
        {"id": v["id"], "price": price, "compareAtPrice": compare_at} for v in variants
    ]
    return product_variants_bulk_update(transport, product_gid, updates)


# =============================================================================
# Metafields
# =============================================================================

_GET_PRODUCT_METAFIELDS = """
query($id: ID!) {
  node(id: $id) {
    ... on Product {
      metafields(first: 250) {
        edges { node { namespace key type value }}
      }
    }
  }
}"""


def get_product_metafields(transport: ShopifyTransport, product_gid: str) -> List[Dict[str, Any]]:
    """Read the SOURCE product's metafields (first 250) as ``{namespace,key,type,value}``.

    Read half of legacy ``copy_metafields`` (sync.py:542-553). Exactly the 4
    fields, in API order, no ``id``. No pagination beyond 250 (legacy
    single-page; latent gap for >250 metafields). The caller injects
    ``ownerId=<dest_gid>`` before passing the list to :func:`metafields_set`.
    """
    data = transport.graphql(_GET_PRODUCT_METAFIELDS, {"id": product_gid})
    node = data.get("node")
    if node is None:
        raise ProductNotFound(product_gid)
    return [e["node"] for e in node["metafields"]["edges"]]


_METAFIELDS_SET = """
mutation($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id }
    userErrors { field message code }
  }
}"""


def metafields_set(
    transport: ShopifyTransport, metafields: List[Dict[str, Any]]
) -> Optional[List[Dict[str, Any]]]:
    """Set a single batch of metafields (one ``metafieldsSet`` call).

    Each dict must be a ``MetafieldsSetInput``:
    ``{ownerId: <DEST product GID>, namespace, key, type, value}`` (built from
    :func:`get_product_metafields` output via ``{"ownerId": dest_gid, **m}``).

    Empty list => EARLY SKIP, the mutation is never issued (legacy
    ``if not mfs: return``, sync.py:554-555). Stateless op = ONE call; the
    <=25-per-call chunking is the service layer's job, so this guards against an
    over-cap chunk. fix3: non-empty ``userErrors`` RAISE (legacy assigned the
    result to ``data`` and never inspected it, sync.py:562-568).

    Returns the created/updated ``metafields`` list, or ``None`` for the empty
    no-op.
    """
    if not metafields:
        return None
    if len(metafields) > METAFIELDS_SET_MAX:
        raise ValueError(
            f"metafields_set accepts at most {METAFIELDS_SET_MAX} inputs per call "
            f"(got {len(metafields)}); chunk in the service layer"
        )
    data = transport.graphql(_METAFIELDS_SET, {"metafields": metafields})
    payload = _check_user_errors(data, "metafieldsSet")
    return payload.get("metafields", [])


# =============================================================================
# Media (images)
# =============================================================================

_GET_PRODUCT_MEDIA = """
query($id: ID!) {
  product(id: $id) {
    media(first: 250) {
      nodes {
        ... on MediaImage { id alt image { url } }
      }
    }
  }
}"""


def get_product_media(transport: ShopifyTransport, product_gid: str) -> List[str]:
    """Return the SOURCE product's image URLs IN ORDER (IMAGE media only).

    Read half of legacy ``copy_images`` (sync.py:475-476), REST->GraphQL:
    legacy sorted REST ``/images.json`` by ``position``; ``product.media``
    already returns media in the product's curated order, so preserve it as-is.
    ``MediaImage.image.url`` is the GraphQL equivalent of REST ``image.src``.
    Non-IMAGE media (video/model3d/external_video) produce empty inline-fragment
    nodes and are filtered out. No pagination beyond 250 (legacy single-page).
    """
    data = transport.graphql(_GET_PRODUCT_MEDIA, {"id": product_gid})
    product = data.get("product")
    if product is None:
        raise ProductNotFound(product_gid)
    urls: List[str] = []
    for node in product["media"]["nodes"]:
        image = (node or {}).get("image") or {}
        url = image.get("url")
        if url:
            urls.append(url)
    return urls


_SET_PRODUCT_MEDIA = """
mutation($input: ProductSetInput!) {
  productSet(input: $input) {
    product { id }
    userErrors { code field message }
  }
}"""


def set_product_media(
    transport: ShopifyTransport, dest_gid: str, source_image_urls: List[str]
) -> Dict[str, Any]:
    """Full REPLACE of the destination product's images with ``source_image_urls``.

    fix7: a single declarative ``productSet``. The input carries ONLY ``id`` +
    ``files`` so every other product field is left untouched. ``files`` is a
    list field: the 2025-07 rule "creates new entries, updates existing, and
    deletes existing entries that aren't included in the input" makes this a
    full replace with no separate delete op (verified at
    shopify.dev/.../2025-07/mutations/productSet). Order is preserved by the
    ``files`` array order. ``alt`` is forced to ``""`` on every image (legacy
    deliberate empty alt); ``contentType`` is ``IMAGE`` for every entry. fix3:
    non-empty ``userErrors`` RAISE.
    """
    files = [
        {"originalSource": url, "alt": "", "contentType": "IMAGE"}
        for url in source_image_urls
    ]
    data = transport.graphql(
        _SET_PRODUCT_MEDIA, {"input": {"id": dest_gid, "files": files}}
    )
    return _check_user_errors(data, "productSet")


# =============================================================================
# Locations
# =============================================================================

_GET_LOCATIONS = """
query($first: Int!, $after: String) {
  locations(first: $first, after: $after) {
    edges { node { id name isActive } }
    pageInfo { hasNextPage endCursor }
  }
}"""


def get_locations(transport: ShopifyTransport) -> List[Dict[str, Any]]:
    """Return ALL locations as ``{id (GID), name, isActive}``, walking pagination.

    Strictly more correct than the legacy single un-paginated REST
    ``GET /locations.json`` (which silently capped at the default page,
    sync.py:597). ``node.id`` is a GID string (``gid://shopify/Location/<n>``)
    which the new inventory mutations consume directly.
    """
    results: List[Dict[str, Any]] = []
    after: Optional[str] = None
    while True:
        data = transport.graphql(
            _GET_LOCATIONS, {"first": CONNECTION_PAGE_SIZE, "after": after}
        )
        conn = data["locations"]
        results.extend(e["node"] for e in conn["edges"])
        page = conn["pageInfo"]
        if not page["hasNextPage"]:
            break
        after = page["endCursor"]
    return results


def get_location_by_name(transport: ShopifyTransport, name: str) -> Optional[Dict[str, Any]]:
    """Return the location node whose name matches ``name`` exactly, else ``None``.

    Exact-name match (sync.py:580-611); callers branch on ``None`` -> error/skip.
    Returns a node whose ``id`` is a GID string (not the legacy REST numeric int),
    so downstream inventory ops receive the GID they require directly.
    """
    for node in get_locations(transport):
        if node["name"] == name:
            return node
    return None


# =============================================================================
# Inventory — writes at the pinned version (F7 §3.3 rules 2, 3, 8, 9; E15)
# =============================================================================
#
# Every Promo write is RELATIVE (``inventoryAdjustQuantities`` on ``available``)
# with ``changeFromQuantity`` = the level just read (rule 9's Shopify-side fence)
# and the row's ``@idempotent`` key (mandatory from 2026-04). Activation is an
# explicit step with ``available`` 0 only (the store refuses ``available`` and
# ``onHand`` together, R053), sent only for an absent or inactive level. A write never
# activates by itself and never retries on "not stocked" (2026-10 removed
# ``ITEM_NOT_STOCKED_AT_LOCATION``). Each write is built as a request BODY first
# (:func:`adjust_request`, …): the ``RECONCILE`` row stores that exact text and
# every re-send posts it again (:func:`send_inventory`), never a rebuilt one.
#
# Sources (read 2026-10-06, raw pages): shopify.dev/docs/api/admin-graphql/2026-10/
# mutations/inventoryAdjustQuantities.md, input-objects/InventoryChangeInput.md
# ("changeFromQuantity … mandatory: you must explicitly pass in a value, even if
# that value is null"), enums/InventoryAdjustQuantitiesUserErrorCode.md,
# mutations/inventoryActivate.md (``[UserError!]!``: no ``code``),
# mutations/inventoryDeactivate.md, objects/InventoryItem.md (``inventoryLevel``
# / ``inventoryLevels`` take ``includeInactive``), objects/InventoryLevel.md
# (``isActive``), docs/apps/build/apis/graphql-admin/implementing-idempotency.md.

INVENTORY_REASON = "correction"
QUANTITY_NAME = "available"

_INVENTORY_ADJUST = """
mutation($input: InventoryAdjustQuantitiesInput!, $idempotencyKey: String!) {
  inventoryAdjustQuantities(input: $input) @idempotent(key: $idempotencyKey) {
    inventoryAdjustmentGroup {
      changes { name delta quantityAfterChange location { id } item { id } }
    }
    userErrors { field message code }
  }
}"""

_INVENTORY_ACTIVATE = """
mutation($inventoryItemId: ID!, $locationId: ID!, $available: Int, $idempotencyKey: String!) {
  inventoryActivate(
    inventoryItemId: $inventoryItemId
    locationId: $locationId
    available: $available
  ) @idempotent(key: $idempotencyKey) {
    inventoryLevel { id isActive }
    userErrors { field message }
  }
}"""

_INVENTORY_DEACTIVATE = """
mutation($inventoryLevelId: ID!) {
  inventoryDeactivate(inventoryLevelId: $inventoryLevelId) {
    userErrors { field message }
  }
}"""

_READ_LEVEL = """
query($id: ID!, $loc: ID!) {
  inventoryItem(id: $id) {
    id
    inventoryLevel(locationId: $loc, includeInactive: true) {
      id
      isActive
      quantities(names: ["available", "committed", "on_hand"]) { name quantity }
    }
  }
}"""


@dataclass(frozen=True)
class InventoryAnswer:
    """What an inventory mutation answered (the transport did not raise).

    ``ok`` — no ``userErrors``; ``codes`` — their ``code`` values (adjust only:
    activate / deactivate return plain ``UserError``, no code); ``changes`` —
    the adjustment group's changes; ``level`` — an activation's
    ``inventoryLevel`` (``{"id", "isActive"}``)."""

    ok: bool
    codes: Tuple[str, ...] = ()
    errors: Tuple[Dict[str, Any], ...] = ()
    changes: Tuple[Dict[str, Any], ...] = ()
    level: Optional[Dict[str, Any]] = None


def adjust_request(
    inventory_item_gid: str,
    location_gid: str,
    delta: int,
    change_from: Optional[int],
    idempotency_key: str,
    *,
    reference: Optional[str] = None,
) -> str:
    """The body of one relative ``available`` write, fenced by ``change_from``."""
    payload: Dict[str, Any] = {
        "name": QUANTITY_NAME,
        "reason": INVENTORY_REASON,
    }
    if reference:
        payload["referenceDocumentUri"] = reference
    payload["changes"] = [{
        "inventoryItemId": inventory_item_gid,
        "locationId": location_gid,
        "delta": int(delta),
        "changeFromQuantity": change_from,
    }]
    return request_body(_INVENTORY_ADJUST, {"input": payload, "idempotencyKey": idempotency_key})


def activate_request(inventory_item_gid: str, location_gid: str, idempotency_key: str) -> str:
    """The body of an explicit activation: ``available`` 0, never omitted (E15),
    and no ``onHand`` — the pin refuses the two together («not allowed to set
    available and on_hand quantities at the same time», F7.3 run 202610091404,
    R053). For an absent or inactive level only (the caller's rule); what it
    leaves on an inactive level that held units is unmeasured at the pin (E15)."""
    return request_body(_INVENTORY_ACTIVATE, {
        "inventoryItemId": inventory_item_gid,
        "locationId": location_gid,
        "available": 0,
        "idempotencyKey": idempotency_key,
    })


def deactivate_request(inventory_level_gid: str) -> str:
    """``inventoryDeactivate`` takes no idempotency key (not ``@idempotent``)."""
    return request_body(_INVENTORY_DEACTIVATE, {"inventoryLevelId": inventory_level_gid})


_PAYLOAD_KEY = {
    "inventoryAdjustQuantities": "inventoryAdjustQuantities",
    "inventoryActivate": "inventoryActivate",
    "inventoryDeactivate": "inventoryDeactivate",
}


def _body_field(body: str) -> str:
    for field in _PAYLOAD_KEY:
        if f"{field}(" in body:
            return field
    raise ValueError("not an inventory mutation body")


def send_inventory(transport: ShopifyTransport, body: str) -> InventoryAnswer:
    """POST a stored inventory body; parse its answer. Transport outcomes
    (unknown / refused) propagate as the transport's exceptions."""
    field = _body_field(body)
    data = transport.send_body(body)
    payload = data.get(field) or {}
    errors = tuple(payload.get("userErrors") or ())
    group = payload.get("inventoryAdjustmentGroup") or {}
    return InventoryAnswer(
        ok=not errors,
        codes=tuple(str(e.get("code")) for e in errors if e.get("code")),
        errors=errors,
        changes=tuple(group.get("changes") or ()),
        level=payload.get("inventoryLevel"),
    )


def _quantities(level: Dict[str, Any]) -> Dict[str, Optional[int]]:
    qmap = {q["name"]: q["quantity"] for q in (level.get("quantities") or [])}
    return {"available": qmap.get("available"), "committed": qmap.get("committed"),
            "on_hand": qmap.get("on_hand")}


def read_level(transport: ShopifyTransport, inventory_item_gid: str, location_gid: str) -> Dict[str, Any]:
    """Rule 8's read right before a write (``includeInactive: true``).

    Returns ``{"item_found": bool, "level": None | {"id", "is_active",
    "available", "committed", "on_hand"}}`` — ``item_found`` False is a
    definitive null for the item (terminal path (ii) reads it); ``level`` None is
    an ABSENT level (never stocked), not a zero."""
    data = transport.graphql(_READ_LEVEL, {"id": inventory_item_gid, "loc": location_gid})
    item = data.get("inventoryItem")
    if item is None:
        return {"item_found": False, "level": None}
    lvl = item.get("inventoryLevel")
    if lvl is None:
        return {"item_found": True, "level": None}
    return {"item_found": True, "level": {"id": lvl.get("id"), "is_active": bool(lvl.get("isActive")),
                                          **_quantities(lvl)}}


def deny_tracked_inputs(variants: List[Dict[str, Any]], **extra: Any) -> List[Dict[str, Any]]:
    """``ProductVariantsBulkInput`` rows making every variant DENY + tracked
    (rule 7), with optional extra fields (``price``, ``compareAtPrice``)."""
    return [
        {"id": v["id"], "inventoryPolicy": "DENY", "inventoryItem": {"tracked": True}, **extra}
        for v in variants
    ]


# =============================================================================
# NET-NEW ops (M3/M5 outlet lifecycle: inventory-read, delete, publish, enumerate)
# =============================================================================
#
# Authoritative field-level spec verified against shopify.dev (checked on 2025-10;
# every field below is stable and unchanged across 2025-07..2025-10 — no renames).
# All four are stateless like the ops above: first arg = transport, only
# transport.graphql is called. Mutations route through _check_user_errors (fix3).

# Ground-truth outlet collection GID (numeric REST id 650952442188 -> GID form).
OUTLET_COLLECTION_GID = "gid://shopify/Collection/650952442188"
# Sales channel whose Publication we publish outlets onto.
ONLINE_STORE_PUBLICATION_NAME = "Online Store"
# Cap-guard for a variant's inventory levels: the store has 4 online locations,
# so 10 is ample. Truncation is detected via pageInfo.hasNextPage (authoritative),
# NOT via "nodes == 10" (a false positive when exactly 10 locations exist).
INVENTORY_LEVELS_PAGE_SIZE = 10
# Variants-per-page for the inventory read (shoe size ladders are well under 250).
VARIANT_INVENTORY_PAGE_SIZE = 250
# Single-page cap for the publications enumeration (a store has a handful).
PUBLICATIONS_PAGE_SIZE = 50
# Collection-memberships page size for the M5 delete snapshot read.
PRODUCT_COLLECTIONS_PAGE_SIZE = 100


# -----------------------------------------------------------------------------
# 1) Per-variant inventory READ (inventoryPolicy + per-location quantities)
# -----------------------------------------------------------------------------

_READ_VARIANT_INVENTORY = """
query($id: ID!, $variantsFirst: Int!, $levelsFirst: Int!, $variantsAfter: String) {
  node(id: $id) {
    ... on Product {
      variants(first: $variantsFirst, after: $variantsAfter) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          sku
          title
          inventoryPolicy
          selectedOptions { name value }
          inventoryItem {
            id
            tracked
            inventoryLevels(first: $levelsFirst, includeInactive: true) {
              pageInfo { hasNextPage endCursor }
              nodes {
                id
                isActive
                location { id name }
                quantities(names: ["available", "committed", "on_hand"]) { name quantity }
              }
            }
          }
        }
      }
    }
  }
}"""


def _parse_inventory_levels(inventory_levels: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten one variant's ``inventoryLevels`` connection into per-location dicts.

    Only locations Shopify actually returned are present in the output — a
    location that is NOT in the list means the level is ABSENT (never stocked),
    which the caller must treat as UNKNOWN, never as ``0``. A location present
    with ``available: 0`` is a genuine known-zero. Inactive levels are returned
    too (``includeInactive: true``, F7 E15: an inactive level may hold units,
    and its units count) with ``is_active`` False.

    Each quantity name (``available``/``committed``/``on_hand``) is looked up by
    name; a missing name maps to ``None`` (UNKNOWN for that metric) rather than
    a fabricated ``0``.
    """
    levels: List[Dict[str, Any]] = []
    for lvl in inventory_levels.get("nodes") or []:
        loc = lvl.get("location") or {}
        qmap = {q["name"]: q["quantity"] for q in (lvl.get("quantities") or [])}
        levels.append(
            {
                "level_id": lvl.get("id"),
                "is_active": lvl.get("isActive", True) is not False,
                "location_id": loc.get("id"),
                "location_name": loc.get("name"),
                "available": qmap.get("available"),
                "committed": qmap.get("committed"),
                "on_hand": qmap.get("on_hand"),
            }
        )
    return levels


def read_variant_inventory(
    transport: ShopifyTransport, product_gid: str
) -> List[Dict[str, Any]]:
    """Read per-variant inventory for a product: policy, tracking + per-location quantities.

    Returns one dict per variant::

        {
          "id": <variant GID>,
          "sku": <variant sku>,                     # secondary cross-check key
          "title": <variant title>,                 # F7 §3.2: the exact-size match key
          "inventoryPolicy": "DENY" | "CONTINUE",   # on the VARIANT (not the item)
          "selectedOptions": [{"name","value"}, ...],
          "inventoryItemId": <inventory item GID>,
          "tracked": bool | None,                   # rule 7: DENY + tracked
          "levels": [                               # every level Shopify returned,
            {"level_id","is_active","location_id",  #   inactive ones included
             "location_name","available","committed","on_hand"}, ...
          ],
          "levels_truncated": bool,                 # True => inventory is UNKNOWN
        }

    Two load-bearing distinctions the caller depends on:

    * **quantity 0 vs level ABSENT**: a location present in ``levels`` with
      ``available == 0`` is a known zero; a location NOT in ``levels`` is an
      absent level and MUST be treated as UNKNOWN (never summed as 0).
    * **truncation cap-guard**: ``levels`` is capped at
      ``INVENTORY_LEVELS_PAGE_SIZE`` (10). ``levels_truncated`` reflects the
      authoritative ``inventoryLevels.pageInfo.hasNextPage``.

    Variants are paginated. Product-not-found (null ``node``) raises
    :class:`ProductNotFound` — a definitive null (terminal path (ii) reads it).
    """
    variants_out: List[Dict[str, Any]] = []
    after: Optional[str] = None
    while True:
        data = transport.graphql(
            _READ_VARIANT_INVENTORY,
            {
                "id": product_gid,
                "variantsFirst": VARIANT_INVENTORY_PAGE_SIZE,
                "levelsFirst": INVENTORY_LEVELS_PAGE_SIZE,
                "variantsAfter": after,
            },
        )
        node = data.get("node")
        if node is None:
            raise ProductNotFound(product_gid)
        conn = node["variants"]
        for vn in conn["nodes"]:
            inv_item = vn.get("inventoryItem") or {}
            inv_levels = inv_item.get("inventoryLevels") or {}
            page = inv_levels.get("pageInfo") or {}
            variants_out.append(
                {
                    "id": vn["id"],
                    "sku": vn.get("sku"),
                    "title": vn.get("title"),
                    "inventoryPolicy": vn.get("inventoryPolicy"),
                    "selectedOptions": vn.get("selectedOptions") or [],
                    "inventoryItemId": inv_item.get("id"),
                    "tracked": inv_item.get("tracked"),
                    "levels": _parse_inventory_levels(inv_levels),
                    "levels_truncated": bool(page.get("hasNextPage")),
                }
            )
        vpage = conn["pageInfo"]
        if not vpage["hasNextPage"]:
            break
        after = vpage["endCursor"]
    return variants_out


# -----------------------------------------------------------------------------
# 2) productDelete (hard delete, synchronous)
# -----------------------------------------------------------------------------

_PRODUCT_DELETE = """
mutation($input: ProductDeleteInput!) {
  productDelete(input: $input) {
    deletedProductId
    userErrors { field message }
  }
}"""


def product_delete(transport: ShopifyTransport, product_gid: str) -> Optional[str]:
    """Hard-delete a product (synchronous) and return its ``deletedProductId``.

    Default synchronous form: the payload carries ``deletedProductId``
    immediately (the async ``productDeleteOperation`` is only for Shopify-side
    bulk deletes, not the single-outlet M5 case).

    A non-existent product is NOT a GraphQL top-level error — Shopify returns
    ``deletedProductId: null`` plus a populated ``userErrors``. That path is
    caught by :func:`_check_user_errors` (fix3) and RAISES, so a failed delete
    never looks like a success. ``ProductDeletePayload.userErrors`` is the plain
    ``UserError`` type (``field``, ``message`` — no ``code``).
    """
    data = transport.graphql(_PRODUCT_DELETE, {"input": {"id": product_gid}})
    payload = _check_user_errors(data, "productDelete")
    return payload.get("deletedProductId")


# -----------------------------------------------------------------------------
# 3) Publish to the Online Store sales channel
# -----------------------------------------------------------------------------

_GET_PUBLICATIONS = """
query($first: Int!) {
  publications(first: $first) {
    nodes { id name }
  }
}"""


def get_online_store_publication_id(transport: ShopifyTransport) -> str:
    """Resolve the Publication GID of the "Online Store" sales channel (fail-closed).

    ``publications`` has no by-name filter (its only args are pagination/reverse/
    catalogType), so we enumerate and match ``name == "Online Store"``. If the
    channel is absent this RAISES (``RuntimeError``) rather than returning a
    wrong/None publication — same fail-closed posture as the ``PROMO_LOCATION_ID``
    gate. Resolve once and cache in the service layer.
    """
    data = transport.graphql(_GET_PUBLICATIONS, {"first": PUBLICATIONS_PAGE_SIZE})
    nodes = (data.get("publications") or {}).get("nodes") or []
    for pub in nodes:
        if pub.get("name") == ONLINE_STORE_PUBLICATION_NAME:
            return pub["id"]
    raise RuntimeError(
        f"Publication {ONLINE_STORE_PUBLICATION_NAME!r} not found "
        f"(enumerated {len(nodes)} publications)"
    )


_PRODUCT_PUBLISH = """
mutation($id: ID!, $publicationId: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    publishable {
      publishedOnPublication(publicationId: $publicationId)
      resourcePublicationsCount { count }
    }
    userErrors { field message }
  }
}"""


def product_publish(
    transport: ShopifyTransport, product_gid: str, publication_id: str
) -> Dict[str, Any]:
    """Publish a product onto a sales channel via ``publishablePublish``.

    Uses ``publishablePublish`` (the modern replacement — ``productPublish`` is
    deprecated). ``input`` is a LIST of ``PublicationInput``; we pass the single
    ``{publicationId}`` explicitly as a one-element list. The payload confirms
    the result via ``publishedOnPublication(publicationId:)`` (Boolean) — the
    exact per-channel verify field — plus ``resourcePublicationsCount`` as a
    light cross-check.

    Scope note (operational, not code): ``publishablePublish`` requires the
    ``write_publications`` scope (and ``read_publications`` for
    :func:`get_online_store_publication_id`); grant them on the custom app or the
    publish fails with access-denied. A non-existent/unpublishable publication is
    reported as a ``userError`` (not a top-level error) and RAISES via fix3.

    No ``userErrors`` is not proof of a publish: on a DRAFT the call answers
    ``userErrors`` [], ``publishedOnPublication`` false, count 0 (F7.3 run
    202610091404, R025-S2.10) — the caller reads :func:`publish_confirmed`.
    """
    data = transport.graphql(
        _PRODUCT_PUBLISH,
        {
            "id": product_gid,
            "publicationId": publication_id,
            "input": [{"publicationId": publication_id}],
        },
    )
    return _check_user_errors(data, "publishablePublish")


def publish_confirmed(answer: Dict[str, Any]) -> bool:
    """Whether a :func:`product_publish` answer says the product is on the
    publication (``publishedOnPublication`` true); anything else — false,
    missing — is not a confirmation."""
    return ((answer or {}).get("publishable") or {}).get("publishedOnPublication") is True


# -----------------------------------------------------------------------------
# 3b) Product status flip (DRAFT -> ACTIVE), the LAST outlet-finalization mutation
# -----------------------------------------------------------------------------

_PRODUCT_UPDATE_STATUS = """
mutation($product: ProductUpdateInput!) {
  productUpdate(product: $product) {
    product { id status }
    userErrors { field message }
  }
}"""


def product_update_status(
    transport: ShopifyTransport, product_gid: str, status: str
) -> Dict[str, Any]:
    """Set a product's status ("DRAFT"/"ACTIVE"/"ARCHIVED") via ``productUpdate``.

    Uses the ``product: ProductUpdateInput`` argument — ``input`` is deprecated at
    the pin (``2026-10`` mutations/productUpdate.md, Arguments). The outlet
    lifecycle keeps a freshly duplicated product in DRAFT for the whole
    anti-phantom-stock finalization and flips it to ACTIVE as the LAST mutation
    before publish. fix3: non-empty ``userErrors`` RAISE.
    """
    data = transport.graphql(
        _PRODUCT_UPDATE_STATUS, {"product": {"id": product_gid, "status": status}}
    )
    return _check_user_errors(data, "productUpdate")


# -----------------------------------------------------------------------------
# 4) Enumerate OUTLET collection membership (paginated)
# -----------------------------------------------------------------------------

_ENUMERATE_COLLECTION_PRODUCTS = """
query($id: ID!, $first: Int!, $after: String) {
  collection(id: $id) {
    id
    products(first: $first, after: $after) {
      pageInfo { hasNextPage endCursor }
      nodes { id title status }
    }
  }
}"""


def enumerate_outlet_products(
    transport: ShopifyTransport, collection_gid: str = OUTLET_COLLECTION_GID
) -> List[Dict[str, Any]]:
    """Enumerate ALL members of a collection as ``{id, title, status}``, paginated.

    Defaults to the ground-truth OUTLET collection. Walks
    ``products.pageInfo``/``endCursor`` to page through every member (cap
    250/page); a smart collection returns members of every status
    (ACTIVE/DRAFT/ARCHIVED), so ``status`` lets the caller bucket without extra
    calls. A non-existent collection (null ``collection``) RAISES, matching the
    null-node handling of the other read ops.
    """
    results: List[Dict[str, Any]] = []
    after: Optional[str] = None
    while True:
        data = transport.graphql(
            _ENUMERATE_COLLECTION_PRODUCTS,
            {"id": collection_gid, "first": CONNECTION_PAGE_SIZE, "after": after},
        )
        collection = data.get("collection")
        if collection is None:
            raise RuntimeError(f"Collection not found for GID: {collection_gid}")
        conn = collection["products"]
        results.extend(conn["nodes"])
        page = conn["pageInfo"]
        if not page["hasNextPage"]:
            break
        after = page["endCursor"]
    return results


# -----------------------------------------------------------------------------
# 5) Product CORE read (title/handle/status/tags + collection memberships)
# -----------------------------------------------------------------------------
#
# NET-NEW for the M5 delete before_snapshot: the reconstructive snapshot needs
# five fields no existing read op carries — title/handle/status/tags and the set
# of collections the product belongs to. `enumerate_outlet_products` goes the
# INVERSE direction (collection -> {id,title,status}); there is no product-GID ->
# {tags,handle,collections} read anywhere. One product-query covers all five in a
# single call (all fields stable on Admin 2025-07). READ-ONLY, additive: no
# existing op changes.

_GET_PRODUCT_CORE = """
query($id: ID!, $collFirst: Int!, $collAfter: String) {
  product(id: $id) {
    id
    title
    handle
    status
    tags
    collections(first: $collFirst, after: $collAfter) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        title
        handle
        sources {
          __typename
          ... on CollectionConditionsSource { inclusion { conditions { __typename } } }
        }
      }
    }
  }
}"""


def _is_smart(sources: List[Dict[str, Any]]) -> bool:
    """Rule-based membership: a sub-collections source, or a conditions source
    with inclusion conditions (manual selections alone are not rules)."""
    for s in sources or []:
        kind = s.get("__typename")
        if kind == "CollectionSubCollectionsSource":
            return True
        if kind == "CollectionConditionsSource" and ((s.get("inclusion") or {}).get("conditions") or []):
            return True
    return False


def get_product_core(transport: ShopifyTransport, product_gid: str) -> Dict[str, Any]:
    """Read a product's core scalars + collection memberships (M5 delete snapshot).

    Returns::

        {
          "id": <GID>, "title": str|None, "handle": str|None,
          "status": "ACTIVE"|"DRAFT"|"ARCHIVED"|None,
          "tags": [str, ...],
          "collections": [
            {"id","title","handle","smart": bool}, ...
          ],
        }

    ``smart`` distinguishes rule-based collections — OUTLET/SALDI re-attach
    themselves once tag/price are recreated — from MANUAL ones. It is read from
    ``Collection.sources`` (``ruleSet`` is deprecated at the pin; collections
    using the new source model are filtered out of pre-2026-07 versions —
    shopify.dev changelog "New Collection model and APIs now available",
    2026-06-17). A null ``product`` (not found) raises :class:`ProductNotFound`.
    """
    collections: List[Dict[str, Any]] = []
    core: Optional[Dict[str, Any]] = None
    after: Optional[str] = None
    while True:
        data = transport.graphql(
            _GET_PRODUCT_CORE,
            {
                "id": product_gid,
                "collFirst": PRODUCT_COLLECTIONS_PAGE_SIZE,
                "collAfter": after,
            },
        )
        product = data.get("product")
        if product is None:
            raise ProductNotFound(product_gid)
        if core is None:
            core = {
                "id": product["id"],
                "title": product.get("title"),
                "handle": product.get("handle"),
                "status": product.get("status"),
                "tags": list(product.get("tags") or []),
            }
        conn = product["collections"]
        for n in conn["nodes"]:
            collections.append(
                {
                    "id": n["id"],
                    "title": n.get("title"),
                    "handle": n.get("handle"),
                    "smart": _is_smart(n.get("sources") or []),
                }
            )
        page = conn["pageInfo"]
        if not page["hasNextPage"]:
            break
        after = page["endCursor"]
    core["collections"] = collections
    return core
