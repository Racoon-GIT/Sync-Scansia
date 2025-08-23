import requests, json, logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("sync.shopify")

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
        logger.debug(f"[GraphQL] POST {url} vars={list((variables or {}).keys())}")
        r = self.session.post(url, json={"query": query, "variables": variables or {}}, timeout=60)
        r.raise_for_status()
        data = r.json()
        if "errors" in data:
            logger.error(f"GraphQL errors: {data['errors']}")
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]

    # ---- REST ----
    def rest_get(self, path: str, params: Dict[str, Any] | None=None) -> Dict[str, Any]:
        url = f"https://{self.store}/admin/api/{self.api_version}{path}"
        logger.debug(f"[REST] GET {url} params={params}")
        r = self.session.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def rest_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"https://{self.store}/admin/api/{self.api_version}{path}"
        logger.debug(f"[REST] POST {url} payload_keys={list(payload.keys())}")
        r = self.session.post(url, json=payload, timeout=60)
        r.raise_for_status()
        return r.json()

    def rest_delete(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"https://{self.store}/admin/api/{self.api_version}{path}"
        logger.debug(f"[REST] DELETE {url} params={params}")
        r = self.session.delete(url, params=params, timeout=60)
        if r.status_code >= 400:
            r.raise_for_status()
        return {}

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
        edges = data["productVariants"]["edges"]
        logger.debug(f"find_variants_by_sku({sku}) → {len(edges)} varianti")
        return [e["node"] for e in edges]

    # --- Duplicate product (new args) ---
    def product_duplicate(self, product_id: str, new_title: str) -> Optional[str]:
        q = """
        mutation($productId: ID!, $newTitle: String!) {
          productDuplicate(productId: $productId, newTitle: $newTitle) {
            newProduct { id title handle status }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"productId": product_id, "newTitle": new_title})
        errs = data["productDuplicate"]["userErrors"]
        if errs:
            logger.error(f"productDuplicate userErrors: {errs}")
            return None
        newp = data["productDuplicate"]["newProduct"]
        logger.debug(f"product_duplicate({product_id}) → {newp}")
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
        logger.debug(f"product_update({product_id}, fields={list(fields.keys())})")
        data = self.graphql(q, {"input": {"id": product_id} | fields})
        errs = data["productUpdate"]["userErrors"]
        if errs:
            logger.error(f"productUpdate userErrors: {errs}")
            raise RuntimeError(f"productUpdate errors: {errs}")
        return data["productUpdate"]["product"]

    def products_search_by_title_active(self, title: str) -> List[Dict[str, Any]]:
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
        exact = [n for n in nodes if (n.get("title","") == title and n.get("status") == "ACTIVE") ]
        logger.debug(f"products_search_by_title_active('{title}') → {len(exact)} match esatti")
        return exact

    def product_variants_bulk_update(self, product_id: str, variants: List[Dict[str, Any]]):
        q = """
        mutation($pid: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $pid, variants: $variants, allowPartialUpdates: true) {
            productVariants { id sku }
            userErrors { field message }
          }
        }
        """
        logger.debug(f"product_variants_bulk_update({product_id}) con {len(variants)} varianti")
        data = self.graphql(q, {"pid": product_id, "variants": variants})
        errs = data["productVariantsBulkUpdate"]["userErrors"]
        if errs:
            logger.error(f"productVariantsBulkUpdate userErrors: {errs}")
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
        nodes = [e["node"] for e in data["product"]["variants"]["edges"]]
        logger.debug(f"get_product_variants({product_id}) → {len(nodes)} varianti")
        return nodes

    # ---- Media ----
    def get_product_media(self, product_id: str) -> List[Dict[str, Any]]:
        q = """
        query($id: ID!) {
          product(id: $id) {
            id
            media(first: 100) {
              edges {
                node {
                  __typename
                  ... on MediaImage {
                    id
                    alt
                    image { id originalSrc }
                  }
                }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        nodes = [e["node"] for e in data["product"]["media"]["edges"]]
        logger.debug(f"get_product_media({product_id}) → {len(nodes)} media")
        return nodes

    def product_update_media_alt(self, product_id: str, media_updates: List[Dict[str, Any]]):
        q = """
        mutation($productId: ID!, $media: [UpdateMediaInput!]!) {
          productUpdateMedia(productId: $productId, media: $media) {
            media { ... on MediaImage { id alt } }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"productId": product_id, "media": media_updates})
        errs = data["productUpdateMedia"]["userErrors"]
        if errs:
            logger.warning(f"productUpdateMedia userErrors: {errs}")
        return data["productUpdateMedia"].get("media", [])

    def file_update(self, file_updates: List[Dict[str, Any]]):
        q = """
        mutation($files: [FileUpdateInput!]!) {
          fileUpdate(files: $files) {
            files { id filename }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"files": file_updates})
        errs = data["fileUpdate"]["userErrors"]
        if errs:
            logger.warning(f"fileUpdate userErrors: {errs}")
        return data["fileUpdate"].get("files", [])

    # ---- Metafields ----
    def get_product_metafields(self, product_id: str) -> List[Dict[str, Any]]:
        q = """
        query($id: ID!) {
          product(id: $id) {
            id
            metafields(first: 100) {
              edges {
                node {
                  id
                  namespace
                  key
                  type
                  value
                }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        nodes = [e["node"] for e in data["product"]["metafields"]["edges"]]
        logger.debug(f"get_product_metafields({product_id}) → {len(nodes)}")
        return nodes

    def metafields_set(self, entries: List[Dict[str, Any]]):
        q = """
        mutation($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            metafields { key namespace value }
            userErrors { field message code }
          }
        }
        """
        data = self.graphql(q, {"metafields": entries})
        errs = data["metafieldsSet"]["userErrors"]
        if errs:
            logger.warning(f"metafieldsSet userErrors: {errs}")
        return data["metafieldsSet"].get("metafields", [])

    # ---- Inventory / Locations (REST) ----
    def get_locations(self) -> list[dict]:
        resp = self.rest_get("/locations.json")
        locs = resp.get("locations", [])
        logger.debug(f"get_locations() → {len(locs)}")
        return locs

    def inventory_levels_for_item(self, inventory_item_id: int | str) -> dict:
        resp = self.rest_get("/inventory_levels.json", params={"inventory_item_ids": inventory_item_id})
        return resp

    def inventory_set(self, inventory_item_id: int | str, location_id: int | str, available: int):
        logger.debug(f"inventory_set(item={inventory_item_id}, loc={location_id}, qty={available})")
        return self.rest_post("/inventory_levels/set.json", {
            "location_id": int(location_id),
            "inventory_item_id": int(inventory_item_id),
            "available": int(available)
        })

    def inventory_connect(self, inventory_item_id: int | str, location_id: int | str):
        logger.debug(f"inventory_connect(item={inventory_item_id}, loc={location_id})")
        return self.rest_post("/inventory_levels/connect.json", {
            "location_id": int(location_id),
            "inventory_item_id": int(inventory_item_id),
        })

    def inventory_delete(self, inventory_item_id: int | str, location_id: int | str):
        logger.debug(f"inventory_delete(item={inventory_item_id}, loc={location_id})")
        return self.rest_delete("/inventory_levels.json", {
            "inventory_item_id": int(inventory_item_id),
            "location_id": int(location_id)
        })
