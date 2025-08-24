import os
import time
import json
import logging
from typing import Dict, Any, List, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

LOG = logging.getLogger("sync.shopify")


class ShopifyClient:
    def __init__(self, store: str, token: str, api_version: str = "2025-01") -> None:
        self.store = store
        self.token = token
        self.api_version = api_version

        self.rest_min_interval = int(os.getenv("SHOPIFY_REST_MIN_INTERVAL_MS", "120")) / 1000.0
        self.gql_min_interval = int(os.getenv("SHOPIFY_GQL_MIN_INTERVAL_MS", "120")) / 1000.0
        self._last_rest = 0.0
        self._last_gql = 0.0

    # --------------------------- Helpers ---------------------------
    def _rest_sleep(self):
        dt = time.time() - self._last_rest
        if dt < self.rest_min_interval:
            time.sleep(self.rest_min_interval - dt)
        self._last_rest = time.time()

    def _gql_sleep(self):
        dt = time.time() - self._last_gql
        if dt < self.gql_min_interval:
            time.sleep(self.gql_min_interval - dt)
        self._last_gql = time.time()

    # --------------------------- REST ---------------------------
    @retry(stop=stop_after_attempt(int(os.getenv("SHOPIFY_MAX_RETRIES", "8"))),
           wait=wait_exponential(multiplier=0.5, min=0.5, max=10),
           retry=retry_if_exception_type(requests.RequestException))
    def rest(self, method: str, path: str, params: Dict[str, Any] = None, payload: Dict[str, Any] = None) -> Dict[str, Any]:
        self._rest_sleep()
        url = f"https://{self.store}/admin/api/{self.api_version}{path}"
        headers = {
            "X-Shopify-Access-Token": self.token,
            "Content-Type": "application/json",
        }
        LOG.debug("[REST] %s %s params=%s", method, url, params or {})
        r = requests.request(method, url, headers=headers, params=params, json=payload, timeout=60)
        if r.status_code >= 400:
            LOG.error("REST %s %s → %s %s", method, path, r.status_code, r.text[:200])
            r.raise_for_status()
        return r.json() if r.text else {}

    # --------------------------- GraphQL ---------------------------
    @retry(stop=stop_after_attempt(int(os.getenv("SHOPIFY_MAX_RETRIES", "8"))),
           wait=wait_exponential(multiplier=0.5, min=0.5, max=10),
           retry=retry_if_exception_type(requests.RequestException))
    def graphql(self, query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
        self._gql_sleep()
        url = f"https://{self.store}/admin/api/{self.api_version}/graphql.json"
        headers = {
            "X-Shopify-Access-Token": self.token,
            "Content-Type": "application/json",
        }
        LOG.debug("[GraphQL] POST %s vars=%s", url, list(variables.keys()) if variables else [])
        r = requests.post(url, headers=headers, json={"query": query, "variables": variables or {}}, timeout=60)
        if r.status_code >= 400:
            LOG.error("GraphQL HTTP %s %s", r.status_code, r.text[:200])
            r.raise_for_status()
        data = r.json()
        if "errors" in data:
            LOG.error("GraphQL errors: %s", data["errors"])
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]

    # --------------------------- Queries/Mutations ---------------------------
    def products_search_by_title_active(self, title: str) -> List[Dict[str, Any]]:
        q = """
        query($q: String!) {
          products(first: 10, query: $q) {
            nodes { id title status handle }
          }
        }
        """
        data = self.graphql(q, {"q": f'title:"{title}" status:active'})
        nodes = data["products"]["nodes"]
        LOG.debug("products_search_by_title_active('%s') → %d match esatti", title, len(nodes))
        return nodes

    def products_search_by_title_any(self, title: str) -> List[Dict[str, Any]]:
        q = """
        query($q: String!) {
          products(first: 10, query: $q) {
            nodes { id title status handle }
          }
        }
        """
        data = self.graphql(q, {"q": f'title:"{title}"'})
        nodes = data["products"]["nodes"]
        LOG.debug("products_search_by_title_any('%s') → %d", title, len(nodes))
        return nodes

    def product_duplicate(self, product_id: str, new_title: str) -> Dict[str, Any]:
        q = """
        mutation($productId: ID!, $newTitle: String!) {
          productDuplicate(productId: $productId, newTitle: $newTitle) {
            newProduct { id title handle status }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"productId": product_id, "newTitle": new_title})
        res = data["productDuplicate"]
        if res["userErrors"]:
            raise RuntimeError(f"productDuplicate errors: {res['userErrors']}")
        newp = res["newProduct"]
        LOG.debug("product_duplicate(%s) → %s", product_id, newp)
        return newp

    def product_update(self, product_id: str, **fields) -> Dict[str, Any]:
        q = """
        mutation($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id title handle status tags }
            userErrors { field message }
          }
        }
        """
        input_obj = {"id": product_id}
        input_obj.update(fields)
        data = self.graphql(q, {"input": input_obj})
        errs = data["productUpdate"]["userErrors"]
        if errs:
            LOG.error("productUpdate userErrors: %s", errs)
            raise RuntimeError(f"productUpdate errors: {errs}")
        LOG.debug("product_update(%s, fields=%s)", product_id, list(fields.keys()))
        return data["productUpdate"]["product"]

    def get_product(self, product_id: str) -> Dict[str, Any]:
        q = """
        query($id: ID!) {
          product(id: $id) { id title handle status tags }
        }
        """
        return self.graphql(q, {"id": product_id})["product"]

    def get_product_media(self, product_id: str) -> List[Dict[str, Any]]:
        q = """
        query($id: ID!) {
          product(id: $id) {
            media(first: 50) {
              nodes {
                ... on MediaImage {
                  id
                  image {
                    url
                    altText
                    id
                    width
                    height
                    originalSrc
                  }
                }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        nodes = data["product"]["media"]["nodes"]
        LOG.debug("get_product_media(%s) → %d media", product_id, len(nodes))
        return nodes

    def get_product_metafields(self, product_id: str) -> List[Dict[str, Any]]:
        q = """
        query($id: ID!) {
          product(id: $id) {
            metafields(first: 250) {
              nodes { namespace key type value }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        nodes = data["product"]["metafields"]["nodes"]
        LOG.debug("get_product_metafields(%s) → %d", product_id, len(nodes))
        return nodes

    def metafields_set(self, owner_id: str, metafields: List[Dict[str, Any]]):
        q = """
        mutation($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            metafields { id key namespace }
            userErrors { field message }
          }
        }
        """
        payload = []
        for m in metafields:
            payload.append({
                "ownerId": owner_id,
                "namespace": m["namespace"],
                "key": m["key"],
                "type": m["type"],
                "value": m["value"],
            })
        data = self.graphql(q, {"metafields": payload})
        errs = data["metafieldsSet"]["userErrors"]
        if errs:
            LOG.warning("metafieldsSet userErrors: %s", errs)
        return data

    def get_product_variants(self, product_id: str) -> List[Dict[str, Any]]:
        q = """
        query($id: ID!) {
          product(id: $id) {
            variants(first: 100) {
              nodes {
                id title sku
                price
                compareAtPrice
                inventoryItem { id }
                selectedOptions { name value }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        nodes = data["product"]["variants"]["nodes"]
        LOG.debug("get_product_variants(%s) → %d varianti", product_id, len(nodes))
        return nodes

    def product_variants_bulk_update(self, product_id: str, variants_updates: List[Dict[str, Any]]):
        q = """
        mutation($pid: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $pid, variants: $variants) {
            product { id }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"pid": product_id, "variants": variants_updates})
        errs = data["productVariantsBulkUpdate"]["userErrors"]
        if errs:
            LOG.error("productVariantsBulkUpdate userErrors: %s", errs)
            raise RuntimeError(f"productVariantsBulkUpdate errors: {errs}")
        return data

    # --------- Variant search by SKU ---------
    def find_variants_by_sku(self, sku: str) -> List[Dict[str, Any]]:
        q = """
        query($q: String!) {
          productVariants(first: 50, query: $q) {
            nodes {
              id sku title
              product { id title handle status }
              inventoryItem { id }
              selectedOptions { name value }
            }
          }
        }
        """
        data = self.graphql(q, {"q": f"sku:{sku}"})
        nodes = data["productVariants"]["nodes"]
        LOG.debug("find_variants_by_sku(%s) → %d varianti", sku, len(nodes))
        return nodes

    # --------- Inventory / Locations ---------
    def get_locations(self) -> List[Dict[str, Any]]:
        data = self.rest("GET", "/locations.json")
        LOG.debug("get_locations() → %d", len(data.get("locations", [])))
        return data.get("locations", [])

    def inventory_connect(self, inventory_item_id: int, location_id: int):
        LOG.debug("inventory_connect(item=%s, loc=%s)", inventory_item_id, location_id)
        return self.rest("POST", "/inventory_levels/connect.json", payload={
            "location_id": location_id,
            "inventory_item_id": inventory_item_id,
        })

    def inventory_set(self, inventory_item_id: int, location_id: int, qty: int):
        LOG.debug("inventory_set(item=%s, loc=%s, qty=%s)", inventory_item_id, location_id, qty)
        return self.rest("POST", "/inventory_levels/set.json", payload={
            "location_id": location_id,
            "inventory_item_id": inventory_item_id,
            "available": qty,
        })

    def inventory_delete(self, inventory_item_id: int, location_id: int):
        LOG.debug("inventory_delete(item=%s, loc=%s)", inventory_item_id, location_id)
        try:
            return self.rest("DELETE", "/inventory_levels.json", params={
                "inventory_item_id": inventory_item_id,
                "location_id": location_id,
            })
        except Exception as e:
            LOG.warning("inventory_delete fallita item=%s loc=%s: %s", inventory_item_id, location_id, e)
            return None

    # --------- Collections cleanup ---------
    def collects_for_product(self, product_numeric_id: int) -> List[Dict[str, Any]]:
        data = self.rest("GET", "/collects.json", params={"product_id": str(product_numeric_id), "limit": 250})
        collects = data.get("collects", [])
        LOG.debug("collects_for_product(%s) → %d", product_numeric_id, len(collects))
        return collects

    def delete_collect(self, collect_id: int):
        LOG.debug("delete_collect(%s)", collect_id)
        return self.rest("DELETE", f"/collects/{collect_id}.json")  # params ignored

    def product_delete_rest(self, product_numeric_id: int):
        LOG.info("product_delete(%s) → OK", product_numeric_id)
        return self.rest("DELETE", f"/products/{product_numeric_id}.json")

    # --------- Images (REST) ---------
    def product_images(self, product_numeric_id: int) -> List[Dict[str, Any]]:
        data = self.rest("GET", f"/products/{product_numeric_id}/images.json")
        return data.get("images", [])

    def product_image_create(self, product_numeric_id: int, src_url: str, alt: str = "") -> Dict[str, Any]:
        payload = {"image": {"src": src_url, "alt": alt}}
        LOG.debug("[REST] POST /products/%s/images.json payload_keys=%s", product_numeric_id, list(payload.keys()))
        return self.rest("POST", f"/products/{product_numeric_id}/images.json", payload=payload)
