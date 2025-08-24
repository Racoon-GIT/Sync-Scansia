# src/shopify_client.py
import os
import time
import json
import logging
import random
from typing import Any, Dict, List, Optional, Tuple
import requests

log = logging.getLogger("sync.shopify")

DEFAULT_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")


def _jitter(base: float) -> float:
    return base * (0.75 + random.random() * 0.5)


class ShopifyClient:
    def __init__(self, store: str, access_token: str, api_version: str = DEFAULT_API_VERSION):
        self.store = store.rstrip("/")
        self.api_version = api_version
        self.base_rest = f"https://{self.store}/admin/api/{self.api_version}"
        self.base_graphql = f"https://{self.store}/admin/api/{self.api_version}/graphql.json"
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        })

        # Rate-limit spacing + retry/backoff
        self._min_rest_interval = float(os.getenv("SHOPIFY_REST_MIN_INTERVAL_MS", "120")) / 1000.0
        self._min_gql_interval  = float(os.getenv("SHOPIFY_GQL_MIN_INTERVAL_MS",  "120")) / 1000.0
        self._last_rest_ts = 0.0
        self._last_gql_ts = 0.0
        self._max_retries = int(os.getenv("SHOPIFY_MAX_RETRIES", "8"))

    def _respect_min_interval(self, is_graphql: bool):
        now = time.time()
        if is_graphql:
            dt = now - self._last_gql_ts
            wait = self._min_gql_interval - dt
            if wait > 0:
                time.sleep(wait)
        else:
            dt = now - self._last_rest_ts
            wait = self._min_rest_interval - dt
            if wait > 0:
                time.sleep(wait)

    def _update_last_ts(self, is_graphql: bool):
        if is_graphql:
            self._last_gql_ts = time.time()
        else:
            self._last_rest_ts = time.time()

    def _handle_rate_headers(self, r: requests.Response):
        limit = r.headers.get("X-Shopify-Shop-Api-Call-Limit")
        if limit:
            try:
                used, cap = [int(x) for x in limit.split("/")]
                if cap > 0 and used / cap > 0.85:
                    time.sleep(_jitter(0.4))
            except Exception:
                pass

    def _rest(self, method: str, path: str, *, params: Dict[str, Any] = None,
              json_body: Dict[str, Any] = None, ok_codes=(200, 201, 202, 204)) -> Dict[str, Any]:
        url = f"{self.base_rest}{path}"
        self._respect_min_interval(is_graphql=False)

        backoff = 1.0
        for attempt in range(self._max_retries + 1):
            try:
                if method == "GET":
                    log.debug("[REST] GET %s params=%s", url, params)
                    r = self.session.get(url, params=params, timeout=60)
                elif method == "POST":
                    log.debug("[REST] POST %s payload_keys=%s", url, list((json_body or {}).keys()))
                    r = self.session.post(url, json=json_body, timeout=60)
                elif method == "DELETE":
                    log.debug("[REST] DELETE %s params=%s", url, params or {})
                    r = self.session.delete(url, params=params or {}, timeout=60)
                else:
                    raise RuntimeError(f"Unsupported method {method}")

                self._update_last_ts(is_graphql=False)
                self._handle_rate_headers(r)

                if r.status_code in ok_codes:
                    return r.json() if r.content else {}
                if r.status_code in (429, 430):
                    ra = r.headers.get("Retry-After")
                    if ra:
                        sleep_s = float(ra)
                    else:
                        sleep_s = _jitter(backoff)
                        backoff = min(backoff * 2, 20)
                    log.warning("REST %s rate-limited (%s). Sleeping %.2fs", url, r.status_code, sleep_s)
                    time.sleep(sleep_s)
                    continue
                if 500 <= r.status_code < 600:
                    sleep_s = _jitter(backoff)
                    backoff = min(backoff * 2, 20)
                    log.warning("REST %s 5xx (%s). Retry in %.2fs", url, r.status_code, sleep_s)
                    time.sleep(sleep_s)
                    continue

                r.raise_for_status()

            except requests.RequestException as e:
                if attempt >= self._max_retries:
                    raise
                sleep_s = _jitter(backoff)
                backoff = min(backoff * 2, 20)
                log.warning("REST exception %s. Retry in %.2fs", e, sleep_s)
                time.sleep(sleep_s)

        raise RuntimeError("REST exhausted retries")

    def rest_get(self, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        return self._rest("GET", path, params=params)

    def rest_post(self, path: str, json_body: Dict[str, Any]) -> Dict[str, Any]:
        return self._rest("POST", path, json_body=json_body)

    def rest_delete(self, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        return self._rest("DELETE", path, params=params)

    def graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._respect_min_interval(is_graphql=True)
        payload = {"query": query, "variables": variables or {}}
        log.debug("[GraphQL] POST %s vars=%s", self.base_graphql, list((variables or {}).keys()))
        backoff = 1.0
        for attempt in range(self._max_retries + 1):
            r = self.session.post(self.base_graphql, data=json.dumps(payload), timeout=90)
            self._update_last_ts(is_graphql=True)

            try:
                body = r.json()
            except Exception:
                body = {}

            if "extensions" in body:
                ext = body["extensions"]
                cost = ext.get("cost") or {}
                throttle = cost.get("throttleStatus") or {}
                if throttle:
                    currently = throttle.get("currentlyAvailable", 0)
                    restore = throttle.get("restoreRate", 50)
                    if currently < max(10, restore // 2):
                        time.sleep(_jitter(0.3))

            if r.status_code == 200:
                if "errors" in body and body["errors"]:
                    raise RuntimeError(f"GraphQL errors: {body['errors']}")
                return body.get("data", {})

            if r.status_code in (429, 430) or r.status_code >= 500:
                ra = r.headers.get("Retry-After")
                if ra:
                    sleep_s = float(ra)
                else:
                    sleep_s = _jitter(backoff)
                    backoff = min(backoff * 2, 20)
                time.sleep(sleep_s)
                continue

            r.raise_for_status()

        raise RuntimeError("GraphQL exhausted retries")

    # -------- Products / queries

    def products_search_by_title_active(self, title: str) -> List[Dict[str, Any]]:
        q = f'title:"{title}" status:active'
        query = """
        query($q: String!) {
          products(first: 10, query: $q) {
            nodes { id title status handle }
          }
        }
        """
        data = self.graphql(query, {"q": q})
        return data["products"]["nodes"]

    def products_search_by_title_any(self, title: str) -> List[Dict[str, Any]]:
        q = f'title:"{title}"'
        query = """
        query($q: String!) {
          products(first: 10, query: $q) {
            nodes { id title status handle }
          }
        }
        """
        data = self.graphql(query, {"q": q})
        return data["products"]["nodes"]

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
        errs = data["productDuplicate"]["userErrors"]
        if errs:
            raise RuntimeError(f"productDuplicate errors: {errs}")
        return data["productDuplicate"]["newProduct"]

    def product_update(self, product_id: str, *, title: Optional[str] = None,
                       handle: Optional[str] = None, status: Optional[str] = None,
                       tags: Optional[List[str]] = None) -> Dict[str, Any]:
        inp: Dict[str, Any] = {"id": product_id}
        if title is not None:  inp["title"]  = title
        if handle is not None: inp["handle"] = handle
        if status is not None: inp["status"] = status
        if tags is not None:   inp["tags"]   = tags

        q = """
        mutation($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id title handle status }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"input": inp})
        errs = data["productUpdate"]["userErrors"]
        if errs:
            raise RuntimeError(f"productUpdate errors: {errs}")
        return data["productUpdate"]["product"]

    def get_product_variants(self, product_id: str) -> List[Dict[str, Any]]:
        q = """
        query($id: ID!) {
          product(id: $id) {
            variants(first: 100) {
              nodes {
                id
                sku
                title
                inventoryItem { id }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        return data["product"]["variants"]["nodes"]

    def product_variants_bulk_update(self, product_id: str,
                                     variants_updates: List[Tuple[str, float, Optional[float]] ]):
        q = """
        mutation($pid: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $pid, variants: $variants) {
            product { id }
            productVariants { id price compareAtPrice }
            userErrors { field message }
          }
        }
        """
        payload = [{"id": vid, "price": price, "compareAtPrice": cmp} for vid, price, cmp in variants_updates]
        data = self.graphql(q, {"pid": product_id, "variants": payload})
        errs = data["productVariantsBulkUpdate"]["userErrors"]
        if errs:
            raise RuntimeError(f"productVariantsBulkUpdate userErrors: {errs}")
        return data["productVariantsBulkUpdate"]

    def get_product_media(self, product_id: str) -> List[Dict[str, Any]]:
        q = """
        query($id: ID!) {
          product(id: $id) {
            media(first: 100) {
              nodes {
                mediaContentType
                alt
                ... on MediaImage {
                  id
                  image { url }
                }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        return data["product"]["media"]["nodes"]

    def product_create_media_from_urls(self, product_id: str, images: List[tuple]) -> List[str]:
        created_ids: List[str] = []
        for url, filename_wo_ext in images:
            q = """
            mutation($productId: ID!, $media: [CreateMediaInput!]!) {
              productCreateMedia(productId: $productId, media: $media) {
                media { id alt }
                mediaUserErrors { field message }
                userErrors { field message }
              }
            }
            """
            media = [{
                "originalSource": url,
                "mediaContentType": "IMAGE",
                "alt": ""
            }]
            data = self.graphql(q, {"productId": product_id, "media": media})
            errs = data["productCreateMedia"]["userErrors"] + data["productCreateMedia"]["mediaUserErrors"]
            if errs:
                raise RuntimeError(f"productCreateMedia errors: {errs}")
            mid = data["productCreateMedia"]["media"][0]["id"]
            created_ids.append(mid)
            # Rinominare filename per includere 'Outlet' (solo parte visibile nel file object)
            self.file_update_filename(mid, filename_wo_ext)
        return created_ids

    def file_update_filename(self, media_id: str, filename_wo_ext: str):
        q = """
        mutation($files: [FileUpdateInput!]!) {
          fileUpdate(files: $files) {
            files { id fileStatus }
            userErrors { field message code }
          }
        }
        """
        files = [{"id": media_id, "filename": f"{filename_wo_ext}-Outlet"}]
        self.graphql(q, {"files": files})

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
        return data["product"]["metafields"]["nodes"]

    def set_product_metafields(self, owner_id: str, metafields: List[Dict[str, Any]]):
        q = """
        mutation($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            metafields { key namespace }
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
            raise RuntimeError(f"metafieldsSet errors: {errs}")

    # -------- Locations / Inventory (REST)

    def get_locations(self) -> List[Dict[str, Any]]:
        data = self.rest_get("/locations.json")
        return data.get("locations", [])

    def inventory_levels_for_item(self, inventory_item_id: int) -> List[Dict[str, Any]]:
        data = self.rest_get("/inventory_levels.json", params={"inventory_item_ids": str(inventory_item_id)})
        return data.get("inventory_levels", [])

    def inventory_connect(self, inventory_item_id: int, location_id: int):
        self.rest_post("/inventory_levels/connect.json", {
            "location_id": location_id,
            "inventory_item_id": inventory_item_id
        })

    def inventory_set(self, inventory_item_id: int, location_id: int, qty: int):
        self.rest_post("/inventory_levels/set.json", {
            "location_id": location_id,
            "inventory_item_id": inventory_item_id,
            "available": qty
        })

    def inventory_delete(self, inventory_item_id: int, location_id: int):
        try:
            self.rest_delete("/inventory_levels.json", params={
                "inventory_item_id": inventory_item_id,
                "location_id": location_id
            })
        except requests.HTTPError as e:
            log.warning("inventory_delete fallita item=%s loc=%s: %s", inventory_item_id, location_id, e)

    # -------- Misc REST helpers

    def product_delete_rest(self, product_numeric_id: int):
        self.rest_delete(f"/products/{product_numeric_id}.json")

    def get_product_images_rest(self, product_numeric_id: int):
        data = self.rest_get(f"/products/{product_numeric_id}/images.json")
        return data.get("images", [])

    def upload_product_image_from_url_rest(self, product_numeric_id: int, url: str, alt: str = ""):
        body = {"image": {"src": url, "alt": alt}}
        data = self.rest_post(f"/products/{product_numeric_id}/images.json", body)
        return data.get("image", {})

    def collects_for_product(self, product_numeric_id: int):
        data = self.rest_get("/collects.json", params={"product_id": str(product_numeric_id), "limit": 250})
        return data.get("collects", [])

    def delete_collect(self, collect_id: int):
        self.rest_delete(f"/collects/{collect_id}.json")
