import os
import logging
import requests

logger = logging.getLogger("sync.shopify")

class ShopifyClient:
    def __init__(self, store, token, api_version="2025-01"):
        self.store = store
        self.token = token
        self.api_version = api_version
        self.base_rest = f"https://{store}/admin/api/{api_version}"
        self.base_graphql = f"https://{store}/admin/api/{api_version}/graphql.json"
        self.sess = requests.Session()
        self.sess.headers.update({
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        })

    # ---------------- GraphQL helper ----------------
    def graphql(self, query, variables=None):
        vars_keys = list((variables or {}).keys())
        logger.debug(f"[GraphQL] POST {self.base_graphql} vars={vars_keys}")
        r = self.sess.post(self.base_graphql, json={"query": query, "variables": variables or {}})
        r.raise_for_status()
        data = r.json()
        if "errors" in data and data["errors"]:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]

    # ---------------- REST helper ----------------
    def _rest_get(self, path, params=None):
        url = f"{self.base_rest}/{path.lstrip('/')}"
        logger.debug(f"[REST] GET {url} params={params}")
        r = self.sess.get(url, params=params or {})
        r.raise_for_status()
        return r.json()

    def _rest_post(self, path, payload):
        url = f"{self.base_rest}/{path.lstrip('/')}"
        payload_keys = list((payload or {}).keys())
        logger.debug(f"[REST] POST {url} payload_keys={payload_keys}")
        r = self.sess.post(url, json=payload or {})
        r.raise_for_status()
        return r.json()

    def _rest_delete(self, path, params=None):
        url = f"{self.base_rest}/{path.lstrip('/')}"
        logger.debug(f"[REST] DELETE {url} params={params or {}}")
        r = self.sess.delete(url, params=params or {})
        r.raise_for_status()
        if r.text:
            try:
                return r.json()
            except Exception:
                return {}
        return {}

    # ---------------- Products / Variants ----------------
    def products_search_by_title_active(self, exact_title):
        q = """
        query($q:String!) {
          products(first:10, query:$q) {
            edges {
              node {
                id
                title
                status
                handle
              }
            }
          }
        }
        """
        # title search; filtreremo poi esatti e ACTIVE
        data = self.graphql(q, {"q": f'title:"{exact_title}"'})
        edges = data["products"]["edges"]
        matches = [e["node"] for e in edges if e["node"]["title"] == exact_title and e["node"]["status"] == "ACTIVE"]
        logger.debug(f"products_search_by_title_active('{exact_title}') → {len(matches)} match esatti")
        return matches

    def products_search_by_title_any(self, exact_title):
        """Cerca per titolo e ritorna tutti gli status (ACTIVE/DRAFT/ARCHIVED)."""
        q = """
        query($q:String!) {
          products(first:20, query:$q) {
            edges {
              node {
                id
                title
                status
                handle
              }
            }
          }
        }
        """
        data = self.graphql(q, {"q": f'title:"{exact_title}"'})
        edges = data["products"]["edges"]
        matches = [e["node"] for e in edges if e["node"]["title"] == exact_title]
        logger.debug(f"products_search_by_title_any('{exact_title}') → {len(matches)}")
        return matches

    def product_duplicate(self, product_id, new_title):
        q = """
        mutation($productId: ID!, $newTitle: String!) {
          productDuplicate(productId: $productId, newTitle: $newTitle) {
            newProduct {
              id
              title
              handle
              status
            }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"productId": product_id, "newTitle": new_title})
        dup = data["productDuplicate"]
        if dup.get("userErrors"):
            logger.error(f"productDuplicate userErrors: {dup['userErrors']}")
            return None
        new = dup["newProduct"]
        logger.debug(f"product_duplicate({product_id}) → {new}")
        return new["id"]

    def product_update(self, product_id, **fields):
        q = """
        mutation($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id title handle status }
            userErrors { field message }
          }
        }
        """
        inp = {"id": product_id}
        inp.update(fields)
        data = self.graphql(q, {"input": inp})
        errs = data["productUpdate"].get("userErrors") or []
        if errs:
            logger.error(f"productUpdate userErrors: {errs}")
            raise RuntimeError(f"productUpdate errors: {errs}")
        logger.debug(f"product_update({product_id}, fields={list(fields.keys())})")
        return True

    def product_delete(self, product_id_gid_or_numeric):
        """Elimina prodotto via REST dato gid o id numerico."""
        if isinstance(product_id_gid_or_numeric, str) and product_id_gid_or_numeric.startswith("gid://"):
            num = product_id_gid_or_numeric.split("/")[-1]
        else:
            num = str(product_id_gid_or_numeric)
        path = f"products/{num}.json"
        self._rest_delete(path)
        logger.info(f"product_delete({num}) → OK")
        return True

    def get_product_media(self, product_id):
        q = """
        query($id: ID!) {
          product(id: $id) {
            media(first: 50) {
              nodes {
                __typename
                ... on MediaImage {
                  id
                  image { originalSrc altText }
                }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        nodes = data["product"]["media"]["nodes"] if data.get("product") else []
        logger.debug(f"get_product_media({product_id}) → {len(nodes)} media")
        return nodes

    def product_images_list(self, product_id):
        pid = product_id.split("/")[-1]
        js = self._rest_get(f"products/{pid}/images.json")
        return js.get("images", [])

    def product_image_create(self, product_id, src_url, position=1, alt=""):
        pid = product_id.split("/")[-1]
        payload = {"image": {"src": src_url, "position": position, "alt": alt or ""}}
        self._rest_post(f"products/{pid}/images.json", payload)

    def product_image_update(self, product_id, image_id, alt=""):
        pid = product_id.split("/")[-1]
        payload = {"image": {"id": image_id, "alt": alt or ""}}
        self._rest_post(f"products/{pid}/images/{image_id}.json", payload)

    def get_product_variants(self, product_id):
        q = """
        query($id: ID!) {
          product(id: $id) {
            variants(first: 100) {
              nodes {
                id
                sku
                selectedOptions { name value }
                inventoryItem { id }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id})
        nodes = data["product"]["variants"]["nodes"] if data.get("product") else []
        logger.debug(f"get_product_variants({product_id}) → {len(nodes)} varianti")
        return nodes

    def product_variants_bulk_update(self, product_id, variants_updates):
        q = """
        mutation($pid: ID!, $variants: [ProductVariantBulkInput!]!) {
          productVariantsBulkUpdate(productId: $pid, variants: $variants) {
            product { id }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"pid": product_id, "variants": variants_updates})
        errs = data["productVariantsBulkUpdate"].get("userErrors") or []
        if errs:
            raise RuntimeError(f"productVariantsBulkUpdate errors: {errs}")
        logger.debug(f"product_variants_bulk_update({product_id}) con {len(variants_updates)} varianti")

    def find_variants_by_sku(self, sku):
        q = """
        query($q:String!) {
          products(first:10, query:$q) {
            edges {
              node {
                id title handle
                variants(first:100) {
                  nodes {
                    id
                    sku
                    selectedOptions { name value }
                    inventoryItem { id }
                    product { id title handle }
                  }
                }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"q": f"sku:{sku}"})
        out = []
        for e in data["products"]["edges"]:
            for v in e["node"]["variants"]["nodes"]:
                if v.get("sku") == sku:
                    out.append(v)
        logger.debug(f"find_variants_by_sku({sku}) → {len(out)} varianti")
        return out

    # ---------------- Metafields ----------------
    def get_product_metafields(self, product_id):
        q = """
        query($metafields: ID!) {
          product(id: $metafields) {
            metafields(first: 100) {
              edges {
                node {
                  namespace key type value
                }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"metafields": product_id})
        edges = data["product"]["metafields"]["edges"] if data.get("product") else []
        out = [e["node"] for e in edges]
        logger.debug(f"get_product_metafields({product_id}) → {len(out)}")
        return out

    def metafields_set(self, items):
        q = """
        mutation($items: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $items) {
            userErrors { field message }
          }
        }
        """
        data = self.graphql(q, {"items": items})
        errs = data["metafieldsSet"].get("userErrors") or []
        if errs:
            raise RuntimeError(f"metafieldsSet errors: {errs}")

    # ---------------- Inventory / Locations / Collections ----------------
    def get_locations(self):
        js = self._rest_get("locations.json")
        locs = js.get("locations", [])
        logger.debug(f"get_locations() → {len(locs)}")
        return locs

    def inventory_levels_for_item(self, inventory_item_id):
        js = self._rest_get("inventory_levels.json", params={"inventory_item_ids": str(inventory_item_id)})
        return js.get("inventory_levels", [])

    def inventory_connect(self, inventory_item_id, location_id):
        self._rest_post("inventory_levels/connect.json", {"location_id": int(location_id), "inventory_item_id": int(inventory_item_id)})
        logger.debug(f"inventory_connect(item={inventory_item_id}, loc={location_id})")

    def inventory_set(self, inventory_item_id, location_id, qty):
        self._rest_post("inventory_levels/set.json", {"location_id": int(location_id), "inventory_item_id": int(inventory_item_id), "available": int(qty)})
        logger.debug(f"inventory_set(item={inventory_item_id}, loc={location_id}, qty={qty})")

    def inventory_delete(self, inventory_item_id, location_id):
        try:
            self._rest_delete("inventory_levels.json", params={"inventory_item_id": int(inventory_item_id), "location_id": int(location_id)})
        except requests.HTTPError as e:
            logger.warning(f"inventory_delete fallita item={inventory_item_id} loc={location_id}: {e}")
            raise

    def collects_for_product(self, product_id):
        pid = product_id.split("/")[-1]
        js = self._rest_get("collects.json", params={"product_id": pid, "limit": 250})
        collects = js.get("collects", [])
        logger.debug(f"collects_for_product({pid}) → {len(collects)}")
        return collects

    def delete_collect(self, collect_id):
        self._rest_delete(f"collects/{collect_id}.json")
