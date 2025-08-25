# -*- coding: utf-8 -*-
"""
sync.py — Workflow Outlet (duplica sorgente, copia metafield/immagini, prezzi, inventario, write-back)

Esegui:
  python -m src.sync --apply

Env richieste:
  SHOPIFY_STORE               es: racoon-lab.myshopify.com
  SHOPIFY_ADMIN_TOKEN
  SHOPIFY_API_VERSION         es: 2025-01 (default)
  PROMO_LOCATION_NAME         es: Promo
  MAGAZZINO_LOCATION_NAME     es: Magazzino

  GSPREAD_SHEET_ID
  GSPREAD_WORKSHEET_TITLE
  GOOGLE_CREDENTIALS_JSON / GOOGLE_APPLICATION_CREDENTIALS

Rate limit:
  SHOPIFY_MIN_INTERVAL_SEC    default 0.7
  SHOPIFY_MAX_RETRIES         default 5
"""

import argparse
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from .gsheets import load_rows, write_product_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("sync")

# -------------------- helpers ------------------------------------------------

def _to_float_price(s: Any) -> Optional[float]:
    if s is None:
        return None
    st = str(s).strip()
    if not st:
        return None
    st = re.sub(r"[^\d,\.]", "", st)
    if st.count(",") == 1 and st.count(".") == 0:
        st = st.replace(",", ".")
    if st.count(".") > 1:
        st = st.replace(".", "")
    try:
        return float(st)
    except Exception:
        return None

def _price_str(s: Any, fallback: str = "0.00") -> str:
    v = _to_float_price(s)
    return f"{v:.2f}" if v is not None else fallback

def _is_online_si(v: Any) -> bool:
    # strettamente "SI" (case-insensitive) come da richiesta
    return isinstance(v, str) and v.strip().upper() == "SI"

def _qta_gt_zero(v: Any) -> bool:
    try:
        q = float(str(v).replace(",", "."))
        return q > 0
    except Exception:
        return False

def _gid_to_numeric(gid: str) -> Optional[str]:
    if not gid:
        return None
    return gid.strip().split("/")[-1]

def _make_outlet_title(base_title: str) -> str:
    if base_title.endswith(" - Outlet"):
        return base_title
    return f"{base_title} - Outlet"

def _make_outlet_handle(base_handle: str) -> str:
    if base_handle.endswith("-outlet"):
        return base_handle
    return f"{base_handle}-outlet"

# -------------------- Shopify client ----------------------------------------

class Shopify:
    def __init__(self):
        self.store = os.environ["SHOPIFY_STORE"]
        self.token = os.environ["SHOPIFY_ADMIN_TOKEN"]
        self.api_version = os.environ.get("SHOPIFY_API_VERSION", "2025-01")
        self.base = f"https://{self.store}/admin/api/{self.api_version}"
        self.gql_url = f"https://{self.store}/admin/api/{self.api_version}/graphql.json"
        self.sess = requests.Session()
        self.sess.headers.update({
            "X-Shopify-Access-Token": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        # throttle / retry
        try:
            self.min_interval = float(os.environ.get("SHOPIFY_MIN_INTERVAL_SEC", "0.7"))
        except Exception:
            self.min_interval = 0.7
        try:
            self.max_retries = int(os.environ.get("SHOPIFY_MAX_RETRIES", "5"))
        except Exception:
            self.max_retries = 5
        self._last_call_ts = 0.0
        self._locations_cache: Optional[Dict[str, Any]] = None

    # ---------- low-level REST with throttle/retry ----------
    def _throttle(self):
        now = time.time()
        elapsed = now - self._last_call_ts
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

    def _request(self, method: str, path: str, **kw) -> requests.Response:
        url = self.base + path
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            r = self.sess.request(method, url, **kw)
            self._last_call_ts = time.time()
            # 429
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                delay = float(ra) if ra else 1.0
                logger.warning("429 %s %s. Retry fra %.2fs (%d/%d).", method, path, delay, attempt, self.max_retries)
                time.sleep(delay)
                continue
            # 5xx
            if 500 <= r.status_code < 600:
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning("%s %s -> %d. Backoff %ss (%d/%d). Body: %s",
                               method, path, r.status_code, backoff, attempt, self.max_retries, r.text[:300])
                time.sleep(backoff)
                continue
            return r
        return r

    def _json_or_raise(self, method: str, path: str, r: requests.Response) -> dict:
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code} {r.text}")
        try:
            return r.json()
        except Exception:
            return {}

    def _get(self, path: str, **kw):
        r = self._request("GET", path, **kw)
        return self._json_or_raise("GET", path, r)

    def _post(self, path: str, json: Dict[str, Any] | None = None, **kw):
        r = self._request("POST", path, json=json, **kw)
        return self._json_or_raise("POST", path, r)

    def _put(self, path: str, json: Dict[str, Any] | None = None, **kw):
        r = self._request("PUT", path, json=json, **kw)
        return self._json_or_raise("PUT", path, r)

    def _delete(self, path: str, **kw):
        r = self._request("DELETE", path, **kw)
        return self._json_or_raise("DELETE", path, r)

    # ---------- GraphQL ----------
    def gql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            r = self.sess.post(self.gql_url, json={"query": query, "variables": variables})
            self._last_call_ts = time.time()
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                delay = float(ra) if ra else 1.0
                logger.warning("429 GraphQL. Retry fra %.2fs (%d/%d).", delay, attempt, self.max_retries)
                time.sleep(delay)
                continue
            if 500 <= r.status_code < 600:
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning("GraphQL %d. Backoff %ss (%d/%d). Body: %s",
                               r.status_code, backoff, attempt, self.max_retries, r.text[:300])
                time.sleep(backoff)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"GraphQL -> {r.status_code} {r.text}")
            data = r.json()
            if "errors" in data:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        raise RuntimeError("GraphQL fallita dopo retry")

    # ---------- high-level ops ----------

    def find_source_product_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """
        Cerca la variante per SKU e ritorna il prodotto NON outlet (handle senza '-outlet').
        """
        q = f'sku:"{sku}"'
        query = """
        query($q:String!){
          productVariants(first:10, query:$q){
            edges{
              node{
                id
                sku
                selectedOptions{ name value }
                product{ id handle title status }
              }
            }
          }
        }
        """
        data = self.gql(query, {"q": q})
        nodes = [e["node"] for e in data.get("productVariants", {}).get("edges", [])]
        if not nodes:
            return None
        # preferisci prodotto con handle non-outlet
        for n in nodes:
            p = n["product"]
            if not p["handle"].endswith("-outlet"):
                return p
        # altrimenti prova a ricavare base handle senza -outlet
        p0 = nodes[0]["product"]
        handle = p0["handle"]
        base = handle[:-7] if handle.endswith("-outlet") else handle
        pb = self.product_by_handle(base)
        return pb or p0

    def product_by_handle(self, handle: str) -> Optional[Dict[str, Any]]:
        query = """
        query($h:String!){
          productByHandle(handle:$h){
            id handle title status
            variants(first:250){
              edges{ node{
                id sku
                selectedOptions{ name value }
                inventoryItem { id }
              } }
            }
            images(first:250){ edges{ node{ src:originalSrc altText } } }
          }
        }
        """
        data = self.gql(query, {"h": handle})
        return data.get("productByHandle")

    def product_duplicate_and_wait(self, source_gid: str, new_title: str, new_handle: str) -> Dict[str, Any]:
        # duplica
        mutation = """
        mutation($id:ID!, $title:String!, $handle:String!){
          productDuplicate(productId:$id, newTitle:$title, newHandle:$handle, published:true){
            duplicateProduct{ id handle title status }
            job{ id status }
            userErrors{ field message }
          }
        }
        """
        out = self.gql(mutation, {"id": source_gid, "title": new_title, "handle": new_handle})
        dup = out["productDuplicate"]
        if dup.get("userErrors"):
            raise RuntimeError(f"productDuplicate errors: {dup['userErrors']}")
        # polling semplice: attendo che productByHandle(new_handle) esista
        for _ in range(60):  # ~ max 2 minuti
            prod = self.product_by_handle(new_handle)
            if prod:
                return prod
            time.sleep(2)
        raise RuntimeError("Timeout in attesa della creazione del prodotto duplicato")

    def get_locations_map(self) -> Dict[str, Dict[str, Any]]:
        if self._locations_cache is None:
            data = self._get("/locations.json")
            self._locations_cache = {loc["name"]: loc for loc in data.get("locations", [])}
        return self._locations_cache

    def product_images(self, product_id_num: str) -> List[Dict[str, Any]]:
        data = self._get(f"/products/{product_id_num}/images.json")
        return data.get("images", [])

    def product_create_image(self, product_id_num: str, src_url: str, alt: Optional[str]) -> None:
        payload = {"image": {"src": src_url}}
        if alt:
            payload["image"]["alt"] = alt
        self._post(f"/products/{product_id_num}/images.json", json=payload)

    def get_product_metafields(self, product_gid: str) -> List[Dict[str, Any]]:
        query = """
        query($id:ID!){
          product(id:$id){
            metafields(first:250){
              edges{ node{ namespace key type value } }
            }
          }
        }
        """
        data = self.gql(query, {"id": product_gid})
        edges = data["product"]["metafields"]["edges"]
        return [e["node"] for e in edges]

    def set_product_metafields(self, owner_gid: str, metafields: List[Dict[str, Any]]) -> None:
        if not metafields:
            return
        m_inputs = []
        for m in metafields:
            # alcuni type potrebbero non essere più validi; se serve si può filtrare
            m_inputs.append({
                "ownerId": owner_gid,
                "namespace": m["namespace"],
                "key": m["key"],
                "type": m["type"],
                "value": m["value"],
            })
        mutation = """
        mutation($m:[MetafieldsSetInput!]!){
          metafieldsSet(metafields:$m){
            metafields{ namespace key }
            userErrors{ field message }
          }
        }
        """
        out = self.gql(mutation, {"m": m_inputs})
        errs = out["metafieldsSet"].get("userErrors")
        if errs:
            logger.warning("metafieldsSet userErrors: %s", errs)

    def remove_manual_collections(self, product_id_num: str) -> None:
        # trova tutti i collects del prodotto e rimuove quelli riferiti a custom_collections
        collects = self._get(f"/collects.json?product_id={product_id_num}").get("collects", [])
        for c in collects:
            cid = c["collection_id"]
            # è custom?
            try:
                self._get(f"/custom_collections/{cid}.json")  # 200 se è manuale
                # è manuale: rimuovi il collect
                self._delete(f"/collects/{c['id']}.json")
            except Exception:
                # se non è custom, potrebbe essere smart; ignora
                pass

    def update_all_variant_prices(self, product_id_num: str, price: str, compare_at: str) -> None:
        prod = self._get(f"/products/{product_id_num}.json").get("product", {})
        for v in prod.get("variants", []):
            payload = {"variant": {"id": v["id"], "price": price, "compare_at_price": compare_at}}
            self._put(f"/variants/{v['id']}.json", json=payload)

    def ensure_location_connected(self, inventory_item_id: int, location_id: int) -> None:
        try:
            self._post("/inventory_levels/connect.json", json={
                "location_id": location_id,
                "inventory_item_id": inventory_item_id
            })
        except Exception as e:
            # se già connesso, Shopify può rispondere errore: ignora
            pass

    def set_inventory(self, inventory_item_id: int, location_id: int, qty: int) -> None:
        self._post("/inventory_levels/set.json", json={
            "location_id": location_id,
            "inventory_item_id": inventory_item_id,
            "available": int(qty),
        })

    def delete_inventory_level(self, inventory_item_id: int, location_id: int) -> None:
        # rimuove il livello (non la location)
        self._request("DELETE", f"/inventory_levels.json?inventory_item_id={inventory_item_id}&location_id={location_id}")

# -------------------- core workflow -----------------------------------------

def process_row(shop: Shopify, row: Dict[str, Any], ws, header_idx) -> str:
    """
    Esegue l'intero flusso Outlet per una singola riga sheet.
    Ritorna azione effettuata (string).
    """
    sku = row["sku"]
    taglia = row["taglia"]
    qta = row["qta"]
    prezzo_pieno = _price_str(row["prezzo_pieno"], "0.00")
    prezzo_scontato = _price_str(row["prezzo_scontato"], prezzo_pieno)

    # 1) trova sorgente by SKU (non-outlet)
    src = shop.find_source_product_by_sku(sku)
    if not src:
        logger.warning("SORGENTE non trovato per SKU=%s → SKIP", sku)
        return "SKIP_NO_SOURCE"

    src_gid = src["id"]
    src_handle = src["handle"]
    src_title = src["title"]

    outlet_handle = _make_outlet_handle(src_handle)
    outlet_title = _make_outlet_title(src_title)

    # 2) outlet già attivo?
    outlet = shop.product_by_handle(outlet_handle)
    if outlet and outlet.get("status") == "ACTIVE":
        # write-back se manca
        gid = outlet["id"]
        ws_row = row["_row_index"]
        try:
            write_product_id(ws, header_idx, ws_row, gid)
        except Exception as e:
            logger.warning("Write-back fallito (già attivo): %s", e)
        logger.info("SKIP_OUTLET_ALREADY_ACTIVE handle=%s", outlet_handle)
        return "SKIP_OUTLET_ACTIVE"

    # se esiste draft con stesso handle/titolo → elimina
    if outlet and outlet.get("status") == "DRAFT":
        pid_num = _gid_to_numeric(outlet["id"])
        if pid_num:
            shop._delete(f"/products/{pid_num}.json")
            logger.info("DELETE_DRAFT_OUTLET handle=%s id=%s", outlet_handle, pid_num)
        outlet = None

    # 3) duplica sorgente → outlet
    new_prod = shop.product_duplicate_and_wait(src_gid, outlet_title, outlet_handle)
    new_gid = new_prod["id"]
    new_id_num = _gid_to_numeric(new_gid)
    logger.info("DUPLICATED src_handle=%s -> outlet_handle=%s id=%s", src_handle, outlet_handle, new_id_num)

    # 4) copia metafield
    try:
        meta = shop.get_product_metafields(src_gid)
        shop.set_product_metafields(new_gid, meta)
    except Exception as e:
        logger.warning("metafields copy warning: %s", e)

    # 5) copia immagini (idempotente)
    try:
        src_images = shop.product_images(_gid_to_numeric(src_gid))
        out_images = shop.product_images(new_id_num)
        out_srcs = {im.get("src") for im in out_images}
        for im in src_images:
            src_url = im.get("src")
            if src_url and src_url not in out_srcs:
                shop.product_create_image(new_id_num, src_url, im.get("alt"))
    except Exception as e:
        logger.warning("images copy warning: %s", e)

    # 6) collections manuali → remove
    try:
        shop.remove_manual_collections(new_id_num)
    except Exception as e:
        logger.warning("remove collections warning: %s", e)

    # 7) prezzi su tutte le varianti
    shop.update_all_variant_prices(new_id_num, prezzo_scontato, prezzo_pieno)

    # 8) inventario
    locs = shop.get_locations_map()
    promo_name = os.environ.get("PROMO_LOCATION_NAME", "Promo")
    mag_name = os.environ.get("MAGAZ
