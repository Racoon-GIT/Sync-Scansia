"""A stateful fake of the Shopify Admin GraphQL API at the pin (2026-10), as a
transport: ``graphql(query, variables)`` and ``send_body(body)``.

The REAL ``backend.shopify.ops`` documents run against it, so an engine test
exercises the request bodies, the idempotency keys and the answer parsing too.
Semantics modelled (each from the 2026-10 docs the build read):

* inventory levels per (item, location): ``available`` / ``committed`` /
  ``on_hand``, ``isActive`` (inactive levels keep their quantities; reads with
  ``includeInactive: true`` return them);
* ``inventoryAdjustQuantities`` — ``changeFromQuantity`` compared with the
  current ``available`` (``CHANGE_FROM_QUANTITY_STALE``), the ``@idempotent``
  key: a duplicate with the same body → the cached answer; other parameters →
  ``IDEMPOTENCY_KEY_PARAMETER_MISMATCH``;
* ``inventoryActivate`` with explicit quantities (sets them), keyed likewise;
* ``inventoryDeactivate`` → inactive (quantities kept);
* products: status, publication, variants (policy, tracked, title, SKU),
  collection membership (``inCollection``), ``productDuplicate`` (a DRAFT
  copy that inherits the source's levels), ``productDelete``.

Fault injection: ``shop.fault(op, kind, times=1, when=...)`` with kinds
``lose_after`` (applied, then the answer is lost → ``ShopifyOutcomeUnknown``),
``timeout_before`` (never applied, outcome unknown), ``concurrent`` (answers
``IDEMPOTENCY_CONCURRENT_REQUEST``), ``user_error:<CODE>`` (refused with that
code), ``refuse`` (``ShopifyRefused``), ``throttle`` (never reaches the fake:
use the transport tests for it). ``shop.on(op, fn)`` runs ``fn(shop)`` before
an op — a sale between two calls, a take-over, a crash.
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.shopify.ops import OUTLET_COLLECTION_GID
from backend.shopify.transport import ShopifyOutcomeUnknown, ShopifyRefused, request_body

PROMO = "gid://shopify/Location/PROMO"
MAG = "gid://shopify/Location/MAG"
PUB = "gid://shopify/Publication/OS"


class SimulatedCrashInShop(RuntimeError):
    """A test hook killing the 'process' between two calls."""


@dataclass
class Variant:
    id: str
    sku: Optional[str]
    title: str
    item: str
    policy: str = "DENY"
    tracked: bool = True
    price: str = "100.00"
    compare_at: Optional[str] = None


@dataclass
class Product:
    gid: str
    title: str
    status: str
    variants: List[Variant]
    outlet_member: bool = False
    published: bool = False
    handle: str = ""


@dataclass
class Level:
    id: str
    active: bool = True
    available: int = 0
    committed: int = 0
    on_hand: int = 0


@dataclass
class Fault:
    op: str
    kind: str
    times: int = 1
    when: Optional[Callable[[Dict[str, Any]], bool]] = None


class FakeShop:
    def __init__(self) -> None:
        self.products: Dict[str, Product] = {}
        self.levels: Dict[Tuple[str, str], Level] = {}
        self.keys: Dict[str, Tuple[str, Dict[str, Any]]] = {}   # idempotency key → (fingerprint, answer)
        self.calls: List[Tuple[str, Dict[str, Any]]] = []
        self.faults: List[Fault] = []
        self.hooks: Dict[str, List[Callable[["FakeShop"], None]]] = {}
        self._n = 1000
        self.publication = PUB
        self.truncated: set = set()          # product gids whose level set reads truncated
        self.duplicate_truncated = False     # a duplicate's level set reads truncated

    # -- building the store ----------------------------------------------------------
    def _id(self, kind: str) -> str:
        self._n += 1
        return f"gid://shopify/{kind}/{self._n}"

    def product(self, title: str, *, status: str = "ACTIVE", outlet: bool = True,
                sizes: Tuple[str, ...] = ("42", "43"), sku: Optional[str] = "SKU1", gid: Optional[str] = None,
                promo: Optional[Dict[str, int]] = None, mag: Optional[Dict[str, int]] = None,
                policy: str = "DENY", tracked: bool = True, inactive: Tuple[str, ...] = (),
                absent: Tuple[str, ...] = (), committed: Optional[Dict[str, int]] = None,
                extra_variants: Tuple[Tuple[str, Optional[str]], ...] = ()) -> Product:
        """A product with one variant per size (+ ``extra_variants`` (title, sku)),
        Promo levels from ``promo`` (size → available; ``absent`` sizes have none)."""
        gid = gid or self._id("Product")
        vs = []
        for title_v, sku_v in [(s, sku) for s in sizes] + list(extra_variants):
            item = self._id("InventoryItem")
            v = Variant(self._id("ProductVariant"), sku_v, title_v, item, policy=policy, tracked=tracked)
            vs.append(v)
            if title_v not in absent:
                a = (promo or {}).get(title_v, 0)
                c = (committed or {}).get(title_v, 0)
                self.levels[(item, PROMO)] = Level(self._id("InventoryLevel"), title_v not in inactive, a, c, a + c)
            if mag and title_v in mag:
                self.levels[(item, MAG)] = Level(self._id("InventoryLevel"), True, mag[title_v], 0, mag[title_v])
        p = Product(gid, title, status, vs, outlet_member=outlet, handle=title.lower().replace(" ", "-"))
        self.products[gid] = p
        return p

    def variant(self, gid: str, title: str) -> Variant:
        return next(v for v in self.products[gid].variants if v.title == title)

    def promo(self, gid: str, title: str) -> Optional[Level]:
        return self.levels.get((self.variant(gid, title).item, PROMO))

    def available(self, gid: str, title: str) -> Optional[int]:
        lvl = self.promo(gid, title)
        return None if lvl is None else lvl.available

    def sell(self, gid: str, title: str, n: int = 1) -> None:
        """A sale on Shopify: the level moves (an outlet sale moves Shopify at once)."""
        lvl = self.promo(gid, title)
        lvl.available -= n
        lvl.on_hand -= n

    # -- fault injection --------------------------------------------------------------------
    def fault(self, op: str, kind: str, *, times: int = 1, when: Optional[Callable[[Dict[str, Any]], bool]] = None) -> None:
        self.faults.append(Fault(op, kind, times, when))

    def on(self, op: str, fn: Callable[["FakeShop"], None]) -> None:
        self.hooks.setdefault(op, []).append(fn)

    def _fault(self, op: str, variables: Dict[str, Any]) -> Optional[str]:
        for f in self.faults:
            if f.op == op and f.times > 0 and (f.when is None or f.when(variables)):
                f.times -= 1
                return f.kind
        return None

    def names(self) -> List[str]:
        return [c[0] for c in self.calls]

    def count(self, op: str) -> int:
        return sum(1 for c in self.calls if c[0] == op)

    # -- the transport surface ------------------------------------------------------------------
    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        if query.lstrip().startswith("mutation"):
            return self.send_body(request_body(query, variables))
        return self._dispatch(query, variables, body=None)

    def send_body(self, body: str) -> Dict[str, Any]:
        doc = json.loads(body)
        return self._dispatch(doc["query"], doc["variables"], body=body)

    def _dispatch(self, q: str, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        table = [
            ("inventoryAdjustQuantities(", "adjust", self._adjust),
            ("inventoryActivate(", "activate", self._activate),
            ("inventoryDeactivate(", "deactivate", self._deactivate),
            ("productDuplicate(", "duplicate", self._duplicate),
            ("productUpdate(", "status", self._status),
            ("productVariantsBulkUpdate(", "bulk", self._bulk),
            ("publishablePublish(", "publish", self._publish),
            ("productDelete(", "delete", self._delete),
            ("inventoryLevel(locationId", "read_level", self._read_level),
            ("inventoryLevels(first: $levelsFirst", "read_inventory", self._read_inventory),
            ("products(first: $first, query: $q)", "resolve", self._resolve),
            ("publications(first", "publications", self._publications),
            ("collections(first: $collFirst", "core", self._core),
            ("collection(id: $id)", "enumerate", self._enumerate),
            ("variants(first: 250)", "variants", self._variants),
        ]
        for marker, op, fn in table:
            if marker in q:
                for hook in self.hooks.get(op, []):
                    hook(self)
                self.calls.append((op, copy.deepcopy(v)))
                kind = self._fault(op, v)
                if kind == "timeout_before":
                    raise ShopifyOutcomeUnknown("simulated timeout before the call ran")
                if kind == "refuse":
                    raise ShopifyRefused("simulated refusal")
                if kind == "concurrent":
                    return {self._field(op): {"userErrors": [{"field": None, "message": "in progress",
                                                               "code": "IDEMPOTENCY_CONCURRENT_REQUEST"}]}}
                if kind and kind.startswith("user_error:"):
                    code = kind.split(":", 1)[1]
                    return {self._field(op): {"userErrors": [{"field": None, "message": code, "code": code}]}}
                out = fn(v, body)
                if kind == "lose_after":
                    raise ShopifyOutcomeUnknown("simulated lost answer after the call ran")
                return out
        raise AssertionError(f"FakeShop: unknown document: {q[:80]}")

    @staticmethod
    def _field(op: str) -> str:
        return {"adjust": "inventoryAdjustQuantities", "activate": "inventoryActivate",
                "deactivate": "inventoryDeactivate", "duplicate": "productDuplicate",
                "status": "productUpdate", "bulk": "productVariantsBulkUpdate",
                "publish": "publishablePublish", "delete": "productDelete"}[op]

    # -- idempotency ------------------------------------------------------------------------------------
    def _keyed(self, key: str, body: Optional[str], fn: Callable[[], Dict[str, Any]], field: str) -> Dict[str, Any]:
        fingerprint = body or ""
        if key in self.keys:
            fp, answer = self.keys[key]
            if fp != fingerprint:
                return {field: {"userErrors": [{"field": None, "message": "mismatch",
                                                "code": "IDEMPOTENCY_KEY_PARAMETER_MISMATCH"}]}}
            return copy.deepcopy(answer)
        answer = fn()
        self.keys[key] = (fingerprint, copy.deepcopy(answer))
        return answer

    # -- mutations ------------------------------------------------------------------------------------------
    def _adjust(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        def run() -> Dict[str, Any]:
            changes_out = []
            for ch in v["input"]["changes"]:
                if "changeFromQuantity" not in ch:
                    return {"inventoryAdjustQuantities": {"userErrors": [{"field": ["changeFromQuantity"],
                                                                           "message": "required", "code": "INVALID_QUANTITY_DOCUMENT"}]}}
                lvl = self.levels.get((ch["inventoryItemId"], ch["locationId"]))
                if lvl is None:
                    raise AssertionError("the engine adjusted an absent level")
                if ch["changeFromQuantity"] is not None and ch["changeFromQuantity"] != lvl.available:
                    return {"inventoryAdjustQuantities": {"userErrors": [{"field": None, "message": "stale",
                                                                           "code": "CHANGE_FROM_QUANTITY_STALE"}]}}
            for ch in v["input"]["changes"]:
                lvl = self.levels[(ch["inventoryItemId"], ch["locationId"])]
                lvl.available += ch["delta"]
                lvl.on_hand += ch["delta"]
                changes_out.append({"name": "available", "delta": ch["delta"], "quantityAfterChange": lvl.available,
                                    "location": {"id": ch["locationId"]}, "item": {"id": ch["inventoryItemId"]}})
            return {"inventoryAdjustQuantities": {"inventoryAdjustmentGroup": {"changes": changes_out},
                                                  "userErrors": []}}

        return self._keyed(v["idempotencyKey"], body, run, "inventoryAdjustQuantities")

    def _activate(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        def run() -> Dict[str, Any]:
            k = (v["inventoryItemId"], v["locationId"])
            lvl = self.levels.get(k)
            if lvl is None:
                lvl = self.levels[k] = Level(self._id("InventoryLevel"))
            lvl.active = True
            if v.get("available") is not None:
                lvl.available = v["available"]
            if v.get("onHand") is not None:
                lvl.on_hand = v["onHand"]
            return {"inventoryActivate": {"inventoryLevel": {"id": lvl.id, "isActive": True}, "userErrors": []}}

        return self._keyed(v["idempotencyKey"], body, run, "inventoryActivate")

    def _deactivate(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        for lvl in self.levels.values():
            if lvl.id == v["inventoryLevelId"]:
                lvl.active = False
                return {"inventoryDeactivate": {"userErrors": []}}
        return {"inventoryDeactivate": {"userErrors": [{"field": None, "message": "not found"}]}}

    def _duplicate(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        src = self.products[v["productId"]]
        gid = self._id("Product")
        vs = []
        for sv in src.variants:
            item = self._id("InventoryItem")
            vs.append(Variant(self._id("ProductVariant"), sv.sku, sv.title, item, sv.policy, sv.tracked,
                              sv.price, sv.compare_at))
            for (it, loc), lvl in list(self.levels.items()):
                if it == sv.item:
                    self.levels[(item, loc)] = Level(self._id("InventoryLevel"), lvl.active, lvl.available, 0, lvl.available)
        self.products[gid] = Product(gid, v["newTitle"], v.get("newStatus") or src.status, vs, outlet_member=False)
        if self.duplicate_truncated:
            self.truncated.add(gid)
        return {"productDuplicate": {"newProduct": {"id": gid, "title": v["newTitle"], "handle": "", "status": "DRAFT"},
                                     "userErrors": []}}

    def _status(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        p = self.products.get(v["product"]["id"])
        if p is None:
            return {"productUpdate": {"product": None, "userErrors": [{"field": ["id"], "message": "not found"}]}}
        p.status = v["product"]["status"]
        return {"productUpdate": {"product": {"id": p.gid, "status": p.status}, "userErrors": []}}

    def _bulk(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        p = self.products[v["productId"]]
        by_id = {x.id: x for x in p.variants}
        for row in v["variants"]:
            x = by_id[row["id"]]
            if "inventoryPolicy" in row:
                x.policy = row["inventoryPolicy"]
            if "inventoryItem" in row and "tracked" in row["inventoryItem"]:
                x.tracked = row["inventoryItem"]["tracked"]
            if "price" in row:
                x.price = row["price"]
            if "compareAtPrice" in row:
                x.compare_at = row["compareAtPrice"]
        return {"productVariantsBulkUpdate": {"product": {"id": p.gid}, "userErrors": []}}

    def _publish(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        self.products[v["id"]].published = True
        return {"publishablePublish": {"publishable": {"publishedOnPublication": True,
                                                       "resourcePublicationsCount": {"count": 1}}, "userErrors": []}}

    def _delete(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        gid = v["input"]["id"]
        if self.products.pop(gid, None) is None:
            return {"productDelete": {"deletedProductId": None, "userErrors": [{"field": ["id"], "message": "nf"}]}}
        return {"productDelete": {"deletedProductId": gid, "userErrors": []}}

    # -- reads -----------------------------------------------------------------------------------------------------
    def _level_json(self, lvl: Level, loc: str) -> Dict[str, Any]:
        return {"id": lvl.id, "isActive": lvl.active, "location": {"id": loc, "name": loc.split("/")[-1]},
                "quantities": [{"name": "available", "quantity": lvl.available},
                               {"name": "committed", "quantity": lvl.committed},
                               {"name": "on_hand", "quantity": lvl.on_hand}]}

    def _read_level(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        item = v["id"]
        if not any(x.item == item for p in self.products.values() for x in p.variants):
            return {"inventoryItem": None}
        lvl = self.levels.get((item, v["loc"]))
        return {"inventoryItem": {"id": item, "inventoryLevel": self._level_json(lvl, v["loc"]) if lvl else None}}

    def _read_inventory(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        p = self.products.get(v["id"])
        if p is None:
            return {"node": None}
        nodes = []
        for x in p.variants:
            levels = [self._level_json(lvl, loc) for (it, loc), lvl in self.levels.items() if it == x.item]
            nodes.append({"id": x.id, "sku": x.sku, "title": x.title, "inventoryPolicy": x.policy,
                          "selectedOptions": [{"name": "Size", "value": x.title}],
                          "inventoryItem": {"id": x.item, "tracked": x.tracked,
                                            "inventoryLevels": {"pageInfo": {"hasNextPage": p.gid in self.truncated,
                                                                             "endCursor": None},
                                                                "nodes": levels}}})
        return {"node": {"variants": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": nodes}}}

    def _resolve(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        sku = v["q"][len("sku:"):].replace("\\'", "'").replace('\\"', '"').replace("\\\\", "\\")
        edges = []
        for p in self.products.values():
            if any((x.sku or "") == sku for x in p.variants):
                edges.append({"node": {"id": p.gid, "title": p.title, "handle": p.handle, "status": p.status,
                                       "inCollection": p.outlet_member,
                                       "variants": {"edges": [{"node": {"id": x.id, "sku": x.sku}} for x in p.variants]}}})
        return {"products": {"edges": edges, "pageInfo": {"hasNextPage": False}}}

    def _publications(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        return {"publications": {"nodes": [{"id": self.publication, "name": "Online Store"}]}}

    def _core(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        p = self.products.get(v["id"])
        if p is None:
            return {"product": None}
        cols = [{"id": OUTLET_COLLECTION_GID, "title": "Outlet", "handle": "outlet",
                 "sources": [{"__typename": "CollectionConditionsSource",
                              "inclusion": {"conditions": [{"__typename": "C"}]}}]}] if p.outlet_member else []
        return {"product": {"id": p.gid, "title": p.title, "handle": p.handle, "status": p.status, "tags": [],
                            "collections": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": cols}}}

    def _enumerate(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        nodes = [{"id": p.gid, "title": p.title, "status": p.status} for p in self.products.values() if p.outlet_member]
        return {"collection": {"id": v["id"], "products": {"pageInfo": {"hasNextPage": False, "endCursor": None},
                                                           "nodes": nodes}}}

    def _variants(self, v: Dict[str, Any], body: Optional[str]) -> Dict[str, Any]:
        p = self.products.get(v["id"])
        if p is None:
            return {"node": None}
        return {"node": {"variants": {"edges": [{"node": {
            "id": x.id, "sku": x.sku, "title": x.title, "price": x.price, "compareAtPrice": x.compare_at,
            "inventoryItem": {"id": x.item}, "selectedOptions": [{"name": "Size", "value": x.title}]}}
            for x in p.variants]}}}


__all__ = ["FakeShop", "PROMO", "MAG", "PUB", "SimulatedCrashInShop"]
