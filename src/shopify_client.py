import requests, json
from typing import Any, Dict, List, Optional

class ShopifyClient:
    def __init__(self, store: str, token: str, api_version: str="2025-01"):
        self.store = store
        self.token = token
        self.api_version = api_version
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": self.token,
            "Content-Type": "application/json"
        })

    # ---- GraphQL ----
    def graphql(self, query: str, variables: Dict[str, Any] | None=None) -> Dict[str, Any]:
        url = f"https://{self.store}/admin/api/{self.api_version}/graphql.json"
        payload = {"query": query, "variables": variables or {}}
        r = self.session.post(url, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]

    # ---- REST ----
    def rest_get(self, path: str, params: Dict[str, Any] | None=None) -> Dict[str, Any]:
        url = f"https://{self.store}/admin/api/{self.api_version}{path}"
        r = self.session.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def rest_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"https://{self.store}/admin/api/{self.api_version}{path}"
        r = self.session.post(url, json=payload, timeout=60)
        r.raise_for_status()
        return r.json()

    # ---- Lookups ----
    def find_variants_by_sku(self, sku: str) -> List[Dict[str, Any]]:
        q = """
        query($q: String!) {
          productVariants(first: 50, query: $q) {
            edges {
              node {
                id
                sku
                title
                selectedOptions { name value }
                product { id title handle status }
                inventoryItem { id }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"q": f"sku:{sku}"})
        return [e["node"] for e in data["productVariants"]["edges"]]

    def product_duplicate(self, product_id: str) -> Optional[str]:
        q = """
        mutation($id: ID!) {
          productDuplicate(id: $id) {
            newProduct { id title handle status }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        errs = data["productDuplicate"]["userErrors"]
        if errs:
            return None
        newp = data["productDuplicate"]["newProduct"]
        return newp["id"] if newp else None

    def product_update(self, product_id: str, **fields):
        q = """
        mutation($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id title handle status tags }
            userErrors { field message }
          }
        }
        """
        inp = {"id": product_id} | fields
        data = self.graphql(q, {"input": inp})
        errs = data["productUpdate"]["userErrors"]
        if errs:
            raise RuntimeError(f"productUpdate errors: {errs}")
        return data["productUpdate"]["product"]

    def products_search_by_title_active(self, title: str) -> List[Dict[str, Any]]:
        # Cerca prodotti ACTIVE con titolo esatto lato client
        query_str = f'title:{json.dumps(title)} status:active'
        q = """
        query($q: String!) {
          products(first: 10, query: $q) {
            edges { node { id title status handle } }
          }
        }
        """
        data = self.graphql(q, {"q": query_str})
        nodes = [e["node"] for e in data["products"]["edges"]]
        return [n for n in nodes if (n.get("title","") == title and n.get("status") == "ACTIVE")]

    def product_variants_bulk_update(self, product_id: str, variants: List[Dict[str, Any]]):
        q = """
        mutation($pid: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $pid, variants: $variants, allowPartialUpdates: true) {
            productVariants { id sku }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"pid": product_id, "variants": variants})
        errs = data["productVariantsBulkUpdate"]["userErrors"]
        if errs:
            raise RuntimeError(f"productVariantsBulkUpdate errors: {errs}")
        return data["productVariantsBulkUpdate"]["productVariants"]

    def get_product_variants(self, product_id: str) -> List[Dict[str, Any]]:
        q = """
        query($id: ID!) {
          product(id: $id) {
            id
            variants(first: 250) {
              edges { node {
                id sku title
                selectedOptions { name value }
                inventoryItem { id }
              } }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        return [e["node"] for e in data["product"]["variants"]["edges"]]

    # ---- Inventory / Locations (REST) ----
    def get_locations(self) -> list[dict]:
        return self.rest_get("/locations.json").get("locations", [])

    def inventory_levels_for_item(self, inventory_item_id: int | str) -> dict:
        return self.rest_get("/inventory_levels.json", params={"inventory_item_ids": inventory_item_id})

    def inventory_set(self, inventory_item_id: int | str, location_id: int | str, available: int):
        return self.rest_post("/inventory_levels/set.json", {
            "location_id": int(location_id),
            "inventory_item_id": int(inventory_item_id),
            "available": int(available)
        })

    def inventory_connect(self, inventory_item_id: int | str, location_id: int | str):
        return self.rest_post("/inventory_levels/connect.json", {
            "location_id": int(location_id),
            "inventory_item_id": int(inventory_item_id),
        })
