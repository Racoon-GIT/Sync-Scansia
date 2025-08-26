# -*- coding: utf-8 -*-
"""
sync.py — Workflow OUTLET (rewrite with image-order + magazzino-destock fixes)

Esegui:
    python -m src.sync --apply

ENV richieste:
  # Google Sheets
  GSPREAD_SHEET_ID
  GSPREAD_WORKSHEET_TITLE
  GOOGLE_CREDENTIALS_JSON  (o GOOGLE_SERVICE_ACCOUNT_JSON, o GOOGLE_APPLICATION_CREDENTIALS)

  # Shopify
  SHOPIFY_STORE
  SHOPIFY_ADMIN_TOKEN
  SHOPIFY_API_VERSION       (es: 2025-01)

  # Locations
  PROMO_LOCATION_NAME       (es: Promo)
  MAGAZZINO_LOCATION_NAME   (es: Magazzino)

  # Rate limit (opzionali)
  SHOPIFY_MIN_INTERVAL_SEC  default 0.7
  SHOPIFY_MAX_RETRIES       default 5
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

# GSheets (opzionale per write-back)
try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("sync")

# ----------------------------------------------------------------------------- #
# Utils
# ----------------------------------------------------------------------------- #
def _norm_key(k: str) -> str:
    return (k or "").strip().lower().replace("-", "_").replace(" ", "_")

def _clean_price(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s2 = re.sub(r"[^\d,\.]", "", s)
    if s2.count(",") == 1 and s2.count(".") == 0:
        s2 = s2.replace(",", ".")
    if s2.count(".") > 1:
        s2 = s2.replace(".", "")
    try:
        return f"{float(s2):.2f}"
    except Exception:
        return None

def _truthy_si(v: Any) -> bool:
    if v is True:
        return True
    if isinstance(v, (int, float)):
        return int(v) == 1
    if isinstance(v, str):
        return v.strip().lower() in {"si", "sì", "true", "1", "x", "ok", "yes"}
    return False

def _gid_numeric(gid: str) -> Optional[str]:
    return gid.split("/")[-1] if gid else None

# ----------------------------------------------------------------------------- #
# GSheets IO
# ----------------------------------------------------------------------------- #
def _gs_creds() -> Optional["Credentials"]:
    cj = os.environ.get("GOOGLE_CREDENTIALS_JSON") or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if cj and Credentials:
        info = json.loads(cj)
        return Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if path and Credentials:
        return Credentials.from_service_account_file(path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return None

def _gs_open():
    sheet_id = os.environ["GSPREAD_SHEET_ID"]
    title    = os.environ["GSPREAD_WORKSHEET_TITLE"]

    # Se sono invertiti, sistemiamo
    id_like = bool(re.fullmatch(r"[A-Za-z0-9-_]{30,}", title))
    not_id_like = not re.fullmatch(r"[A-Za-z0-9-_]{30,}", sheet_id or "")
    if id_like and not_id_like:
        log.warning("Rilevata inversione SHEET_ID/TITLE. Correggo automaticamente.")
        sheet_id, title = title, sheet_id

    creds = _gs_creds()
    if not gspread or not creds:
        raise RuntimeError("gspread/credenziali mancanti per la lettura GSheets.")

    sa = getattr(creds, "service_account_email", None) or getattr(creds, "_service_account_email", None)
    client = gspread.authorize(creds)
    log.info("Google Sheet sorgente: id=%s | worksheet=%s | service_account=%s", sheet_id, title, sa or "N/D")
    sh = client.open_by_key(sheet_id)
    return sh.worksheet(title)

def gs_read_rows() -> Tuple[List[Dict[str, Any]], Dict[str, int], Optional["gspread.Worksheet"]]:
    ws = _gs_open()
    values = ws.get_all_values() or []
    if not values:
        return [], {}, ws
    header = values[0]
    col_index = {_norm_key(h): i+1 for i, h in enumerate(header)}

    def norm_row(vs: List[str]) -> Dict[str, Any]:
        m = {}
        for i, cell in enumerate(vs):
            key = _norm_key(header[i]) if i < len(header) else f"col{i+1}"
            m[key] = cell
        if "productid" in m and "product_id" not in m:
            m["product_id"] = m["productid"]
        return m

    rows = [norm_row(v) for v in values[1:]]
    log.info("Caricate %d righe da Google Sheets (worksheet=%s)", len(rows), ws.title)
    return rows, col_index, ws

def gs_write_product_id(ws, sku: str, taglia: str, new_gid: str, col_index: Dict[str, int]) -> bool:
    if not ws:
        return False
    try:
        all_vals = ws.get_all_values()
        if not all_vals:
            return False
        header = all_vals[0]
        idx_sku = col_index.get("sku") or (header.index("SKU")+1 if "SKU" in header else None)
        idx_tag = col_index.get("taglia") or (header.index("TAGLIA")+1 if "TAGLIA" in header else None)
        idx_pid = col_index.get("product_id") or (header.index("Product_Id")+1 if "Product_Id" in header else None)
        if not (idx_sku and idx_tag and idx_pid):
            log.warning("Write-back: colonne SKU/TAGLIA/Product_Id non trovate.")
            return False
        for r_idx, row in enumerate(all_vals[1:], start=2):
            sku_cell = (row[idx_sku-1] if idx_sku-1 < len(row) else "").strip()
            tag_cell = (row[idx_tag-1] if idx_tag-1 < len(row) else "").strip()
            if sku_cell == sku and tag_cell == taglia:
                ws.update_cell(r_idx, idx_pid, new_gid)
                log.info("Write-back Product_Id OK su riga %d (%s / %s)", r_idx, sku, taglia)
                return True
        log.warning("Write-back: riga non trovata per SKU=%s TAGLIA=%s", sku, taglia)
        return False
    except Exception as e:
        log.warning("Write-back fallito: %s (skip)", e)
        return False

# ----------------------------------------------------------------------------- #
# Shopify client
# ----------------------------------------------------------------------------- #
class Shopify:
    def __init__(self):
        self.store = os.environ["SHOPIFY_STORE"]
        self.token = os.environ["SHOPIFY_ADMIN_TOKEN"]
        self.api_version = os.environ.get("SHOPIFY_API_VERSION", "2025-01")
        self.base = f"https://{self.store}/admin/api/{self.api_version}"
        self.graphql_url = f"{self.base}/graphql.json"
        self.sess = requests.Session()
        self.sess.headers.update({
            "X-Shopify-Access-Token": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        try:
            self.min_interval = float(os.environ.get("SHOPIFY_MIN_INTERVAL_SEC", "0.7"))
        except Exception:
            self.min_interval = 0.7
        try:
            self.max_retries = int(os.environ.get("SHOPIFY_MAX_RETRIES", "5"))
        except Exception:
            self.max_retries = 5
        self._last_call_ts = 0.0
        self._location_cache: Dict[str, Any] | None = None

    def _throttle(self):
        now = time.time()
        el = now - self._last_call_ts
        if el < self.min_interval:
            time.sleep(self.min_interval - el)

    def _request(self, method: str, path: str, **kw) -> requests.Response:
        url = self.base + path
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            r = self.sess.request(method, url, **kw)
            self._last_call_ts = time.time()
            if r.status_code == 429:
                ra = float(r.headers.get("Retry-After") or 1.0)
                logging.getLogger("ratelimit").warning("429 %s %s. Retry fra %.2fs (tentativo %d/%d).", method, path, ra, attempt, self.max_retries)
                time.sleep(ra)
                continue
            if 500 <= r.status_code < 600:
                back = min(2 ** (attempt - 1), 8)
                logging.getLogger("net").warning("%s %s -> %d. Backoff %ss (tentativo %d/%d). Body: %s",
                                                 method, path, r.status_code, back, attempt, self.max_retries, r.text[:300])
                time.sleep(back)
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

    # --- GraphQL
    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            r = self.sess.post(self.graphql_url, json={"query": query, "variables": variables})
            self._last_call_ts = time.time()
            if r.status_code == 429:
                ra = float(r.headers.get("Retry-After") or 1.0)
                logging.getLogger("ratelimit").warning("429 GraphQL. Retry fra %.2fs (tentativo %d/%d).", ra, attempt, self.max_retries)
                time.sleep(ra); continue
            if 500 <= r.status_code < 600:
                back = min(2 ** (attempt - 1), 8)
                logging.getLogger("net").warning("GraphQL %d. Backoff %ss (tentativo %d/%d). Body: %s",
                                                 r.status_code, back, attempt, self.max_retries, r.text[:300])
                time.sleep(back); continue
            if r.status_code >= 400:
                raise RuntimeError(f"GraphQL -> {r.status_code} {r.text}")
            data = r.json()
            if "errors" in data:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        raise RuntimeError("GraphQL fallito dopo retry")

    # --- ricerca
    def find_product_by_sku_non_outlet(self, sku: str) -> Optional[Dict[str, Any]]:
        data = self.graphql("""
        query($q:String!){
          products(first:10, query:$q){
            edges{
              node{
                id title handle status
                variants(first:100){
                  edges{ node{ id sku title selectedOptions{ name value } inventoryItem{ id } } }
                }
              }
            }
          }
        }""", {"q": f"sku:{sku}"})
        for e in data["products"]["edges"]:
            p = e["node"]
            if p["handle"].endswith("-outlet"):  # scarta outlet
                continue
            for ve in p["variants"]["edges"]:
                if (ve["node"]["sku"] or "").strip() == sku:
                    return p
        return None

    def find_product_by_handle_any(self, handle: str) -> Optional[Dict[str, Any]]:
        data = self.graphql("""
        query($q:String!){
          products(first:5, query:$q){ edges{ node{ id title handle status } } }
        }""", {"q": f"handle:{handle}"})
        return data["products"]["edges"][0]["node"] if data["products"]["edges"] else None

    # --- duplicate & update
    def product_duplicate(self, source_gid: str, new_title: str) -> str:
        data = self.graphql("""
        mutation($productId:ID!, $newTitle:String!){
          productDuplicate(productId:$productId, newTitle:$newTitle){
            newProduct{ id }
            userErrors{ field message }
          }
        }""", {"productId": source_gid, "newTitle": new_title})
        dup = data["productDuplicate"]
        if dup["userErrors"]:
            raise RuntimeError(f"productDuplicate errors: {dup['userErrors']}")
        nid = dup.get("newProduct", {}).get("id")
        if not nid:
            raise RuntimeError("productDuplicate: newProduct.id mancante")
        return nid

    def product_update(self, product_gid: str, handle: Optional[str]=None,
                       status: Optional[str]=None, tags: Optional[List[str]]=None) -> Dict[str, Any]:
        inp: Dict[str, Any] = {"id": product_gid}
        if handle is not None: inp["handle"] = handle
        if status is not None: inp["status"] = status
        if tags is not None:   inp["tags"]   = tags
        data = self.graphql("""
        mutation($input: ProductInput!){
          productUpdate(input:$input){
            product{ id handle status tags }
            userErrors{ field message }
          }
        }""", {"input": inp})
        errs = data["productUpdate"]["userErrors"]
        if errs:
            log.warning("productUpdate userErrors: %s", errs)
        return data["productUpdate"]["product"] or {}

    # --- media
    def list_images(self, product_numeric_id: str) -> List[Dict[str, Any]]:
        return self._get(f"/products/{product_numeric_id}/images.json").get("images", [])

    def add_image(self, product_numeric_id: str, src_url: str, position: int, alt: str = "") -> None:
        self._post(f"/products/{product_numeric_id}/images.json",
                   json={"image": {"src": src_url, "position": position, "alt": alt}})

    def move_image(self, product_numeric_id: str, image_id: int, position: int, alt: Optional[str] = None) -> None:
        payload = {"image": {"id": image_id, "position": position}}
        if alt is not None:
            payload["image"]["alt"] = alt
        self._put(f"/products/{product_numeric_id}/images/{image_id}.json", json=payload)

    def delete_image(self, product_numeric_id: str, image_id: int) -> None:
        self._delete(f"/products/{product_numeric_id}/images/{image_id}.json")

    # --- metafield
    def list_product_metafields(self, product_gid: str) -> List[Dict[str, Any]]:
        data = self.graphql("""
        query($id:ID!){
          node(id:$id){
            ... on Product{
              metafields(first:250){ edges{ node{ namespace key type value } } }
            }
          }
        }""", {"id": product_gid})
        edges = data.get("node", {}).get("metafields", {}).get("edges", [])
        return [e["node"] for e in edges]

    def metafields_set(self, owner_gid: str, metafields: List[Dict[str, Any]]) -> None:
        CHUNK = 20
        for i in range(0, len(metafields), CHUNK):
            chunk = [{"ownerId": owner_gid, **m} for m in metafields[i:i+CHUNK]]
            data = self.graphql("""
            mutation($metafields:[MetafieldsSetInput!]!){
              metafieldsSet(metafields:$metafields){
                metafields{ id }
                userErrors{ field message }
              }
            }""", {"metafields": chunk})
            errs = data["metafieldsSet"]["userErrors"]
            if errs:
                log.warning("metafieldsSet userErrors: %s", errs)

    # --- collections (manuali)
    def delete_all_collects(self, product_numeric_id: str) -> None:
        collects = self._get("/collects.json", params={"product_id": product_numeric_id}).get("collects", [])
        for c in collects:
            try:
                self._delete(f"/collects/{c['id']}.json")
            except Exception as e:
                log.warning("delete_collect %s fallita: %s", c.get("id"), e)

    # --- varianti/prezzi
    def get_product_variants(self, product_gid: str) -> List[Dict[str, Any]]:
        data = self.graphql("""
        query($id:ID!){
          node(id:$id){
            ... on Product{
              variants(first:250){
                edges{ node{ id sku title price compareAtPrice inventoryItem{ id } selectedOptions{ name value } } }
              }
            }
          }
        }""", {"id": product_gid})
        return [e["node"] for e in data["node"]["variants"]["edges"]]

    def variants_bulk_update_prices(self, product_gid: str, variant_gids: List[str],
                                    price: str, compare_at: Optional[str]) -> List[Dict[str, Any]]:
        variants = [{"id": gid, "price": price, "compareAtPrice": compare_at} for gid in variant_gids]
        data = self.graphql("""
        mutation($productId:ID!, $variants:[ProductVariantsBulkInput!]!){
          productVariantsBulkUpdate(productId:$productId, variants:$variants){
            product{ id }
            userErrors{ field message }
          }
        }""", {"productId": product_gid, "variants": variants})
        errs = data["productVariantsBulkUpdate"]["userErrors"]
        if errs:
            log.warning("productVariantsBulkUpdate userErrors: %s", errs)
        else:
            log.info("Prezzi aggiornati in bulk su %d varianti", len(variant_gids))
        return errs or []

    def variant_update_price_single(self, variant_gid: str, price: str, compare_at: Optional[str]) -> None:
        data = self.graphql("""
        mutation($input:ProductVariantInput!){
          productVariantUpdate(input:$input){
            productVariant{ id price compareAtPrice }
            userErrors{ field message }
          }
        }""", {"input": {"id": variant_gid, "price": price, "compareAtPrice": compare_at}})
        errs = data["productVariantUpdate"]["userErrors"]
        if errs:
            log.warning("productVariantUpdate userErrors: %s", errs)

    # --- inventory/locations
    def get_location_by_name(self, name: str) -> Dict[str, Any]:
        if self._location_cache is None:
            data = self._get("/locations.json")
            self._location_cache = {loc["name"]: loc for loc in data.get("locations", [])}
        if name in self._location_cache:
            return self._location_cache[name]
        raise RuntimeError(f"Location non trovata: {name}")

    def inventory_connect(self, inventory_item_id: int, location_id: int) -> None:
        self._post("/inventory_levels/connect.json",
                   json={"inventory_item_id": inventory_item_id, "location_id": location_id})

    def inventory_set(self, inventory_item_id: int, location_id: int, qty: int) -> None:
        self._post("/inventory_levels/set.json",
                   json={"inventory_item_id": inventory_item_id, "location_id": location_id, "available": int(qty)})

    def inventory_delete_level(self, inventory_item_id: int, location_id: int) -> None:
        self._request("DELETE", f"/inventory_levels.json?inventory_item_id={inventory_item_id}&location_id={location_id}")

    def inventory_level_exists(self, inventory_item_id: int, location_id: int) -> bool:
        resp = self._get("/inventory_levels.json",
                         params={"inventory_item_ids": inventory_item_id, "location_ids": location_id})
        levels = resp.get("inventory_levels", [])
        return any((lvl.get("inventory_item_id")==inventory_item_id and lvl.get("location_id")==location_id) for lvl in levels)

# ----------------------------------------------------------------------------- #
# Workflow riga
# ----------------------------------------------------------------------------- #
def process_row_outlet(shop: Shopify,
                       row: Dict[str, Any],
                       ws, col_index: Dict[str, int],
                       apply: bool) -> Tuple[str, Optional[str]]:
    sku = (row.get("sku") or "").strip()
    taglia = (row.get("taglia") or "").strip()
    qta_raw = row.get("qta") or row.get("qty") or "0"
    try:
        qta = int(float(str(qta_raw).replace(",", ".")))
    except Exception:
        qta = 0

    prezzo_pieno     = _clean_price(row.get("prezzo_pieno"))
    prezzo_scontato  = _clean_price(row.get("prezzo_scontato")) or (prezzo_pieno or "0.00")

    # 1) sorgente
    source = shop.find_product_by_sku_non_outlet(sku)
    if not source:
        log.warning("SOURCE_NOT_FOUND sku=%s -> skip", sku)
        return ("SKIP_SOURCE_NOT_FOUND", None)
    source_gid    = source["id"]
    source_handle = source["handle"]
    source_title  = source["title"]

    # 2) outlet esistenza
    outlet_handle = source_handle + "-outlet" if not source_handle.endswith("-outlet") else source_handle
    outlet_title  = f"{source_title} - Outlet" if not source_title.endswith(" - Outlet") else source_title
    existing = shop.find_product_by_handle_any(outlet_handle)
    if existing:
        if existing["status"] == "ACTIVE":
            log.info("SKIP_OUTLET_ALREADY_ACTIVE handle=%s", outlet_handle)
            return ("SKIP_OUTLET_ALREADY_ACTIVE", existing["id"])
        else:
            if apply:
                nid = _gid_numeric(existing["id"])
                try:
                    shop._delete(f"/products/{nid}.json")
                    log.info("DELETE_DRAFT_OUTLET handle=%s OK", outlet_handle)
                except Exception as e:
                    log.warning("DELETE_DRAFT_OUTLET fallita: %s", e)

    if not apply:
        log.info("DRY-RUN: duplicazione '%s' -> '%s', handle %s, prezzi, media, inventario.", source_title, outlet_title, outlet_handle)
        return ("DRY_RUN", None)

    # 3) duplica
    outlet_gid = shop.product_duplicate(source_gid, outlet_title)
    log.info("DUPLICATED outlet=%s (da %s)", outlet_gid, source_gid)

    # 4) handle/status/tags vuoti (+fallback)
    prod = shop.product_update(outlet_gid, handle=outlet_handle, status="ACTIVE", tags=[])
    if not prod or prod.get("handle") != outlet_handle:
        ok = False
        for i in range(1, 30):
            cand = f"{outlet_handle}-{i}"
            prod = shop.product_update(outlet_gid, handle=cand, status="ACTIVE", tags=[])
            if prod and prod.get("handle") == cand:
                outlet_handle = cand; ok = True; break
        if not ok:
            raise RuntimeError("Impossibile impostare handle per l'outlet")

    # 5) MEDIA: ricostruzione completa per garantire ordine identico e alt=""
    try:
        src_num = _gid_numeric(source_gid); out_num = _gid_numeric(outlet_gid)
        src_imgs = sorted(shop.list_images(src_num), key=lambda i: i.get("position") or 0)
        src_urls = [i.get("src") for i in src_imgs if i.get("src")]

        out_imgs = sorted(shop.list_images(out_num), key=lambda i: i.get("position") or 0)
        out_urls = [i.get("src") for i in out_imgs if i.get("src")]

        needs_rebuild = (src_urls != out_urls) or any((i.get("alt") or "") for i in out_imgs)
        if needs_rebuild:
            # cancella tutto dall'outlet
            for img in out_imgs:
                try:
                    shop.delete_image(out_num, img["id"])
                    time.sleep(0.2)
                except Exception:
                    pass
            # reinserisci in ordine con alt vuoto
            for pos, url in enumerate(src_urls, start=1):
                shop.add_image(out_num, url, position=pos, alt="")
                time.sleep(0.3)
            log.info("MEDIA REBUILT: %d immagini replicate in ordine", len(src_urls))
        else:
            log.info("MEDIA SKIPPED: ordine già identico al sorgente")

    except Exception as e:
        log.warning("Gestione media fallita (non bloccante): %s", e)

    # 6) metafield
    try:
        mfs = shop.list_product_metafields(source_gid)
        if mfs:
            transferable = [{
                "namespace": m["namespace"],
                "key": m["key"],
                "type": m.get("type") or "single_line_text_field",
                "value": m.get("value") or "",
            } for m in mfs]
            shop.metafields_set(outlet_gid, transferable)
    except Exception as e:
        log.warning("Copy metafield fallita (non bloccante): %s", e)

    # 7) collections manuali
    try:
        out_num = _gid_numeric(outlet_gid)
        shop.delete_all_collects(out_num)
    except Exception as e:
        log.warning("Pulizia collects fallita (non bloccante): %s", e)

    # 8) prezzi
    outlet_variants = shop.get_product_variants(outlet_gid)
    variant_gids = [v["id"] for v in outlet_variants]
    errs = shop.variants_bulk_update_prices(outlet_gid, variant_gids, prezzo_scontato, prezzo_pieno)
    if errs:
        for gid in variant_gids:
            shop.variant_update_price_single(gid, prezzo_scontato, prezzo_pieno)
    vpost = shop.get_product_variants(outlet_gid)
    if vpost:
        smpl = [(v["sku"], v.get("price"), v.get("compareAtPrice")) for v in vpost[:3]]
        log.info("PREZZI OK (prime 3 varianti): %s", smpl)

    # 9) inventario
    promo_name = os.environ.get("PROMO_LOCATION_NAME", "").strip()
    mag_name   = os.environ.get("MAGAZZINO_LOCATION_NAME", "").strip()
    promo = shop.get_location_by_name(promo_name) if promo_name else None
    mag   = shop.get_location_by_name(mag_name) if mag_name else None

    # collega tutte a Promo e porta a 0
    for v in outlet_variants:
        inv_item = int(_gid_numeric(v["inventoryItem"]["id"]))
        if promo:
            try:
                shop.inventory_connect(inv_item, promo["id"])
            except Exception:
                pass
            shop.inventory_set(inv_item, promo["id"], 0)

    # imposta QTA solo sulla variante target
    target_variant = None
    for v in outlet_variants:
        if (v.get("sku") or "").strip() == sku:
            if taglia:
                if any((opt["name"] or "").lower() in {"size", "taglia"} and (opt["value"] or "").strip() == taglia
                       for opt in v.get("selectedOptions", [])):
                    target_variant = v; break
            else:
                target_variant = v; break
    if target_variant and promo:
        inv_item = int(_gid_numeric(target_variant["inventoryItem"]["id"]))
        shop.inventory_set(inv_item, promo["id"], qta)

    # Magazzino: se livello esiste -> set 0 e poi DELETE
    if mag:
        for v in outlet_variants:
            inv_item = int(_gid_numeric(v["inventoryItem"]["id"]))
            try:
                if shop.inventory_level_exists(inv_item, mag["id"]):
                    try:
                        shop.inventory_set(inv_item, mag["id"], 0)
                    except Exception:
                        pass
                    # disconnessione per rimuovere definitivamente il livello
                    shop.inventory_delete_level(inv_item, mag["id"])
            except Exception as e:
                log.warning("Magazzino cleanup (inv_item=%s) errore: %s", inv_item, e)

    # 10) write-back
    if _gs_creds() and gspread and ws:
        try:
            gs_write_product_id(ws, sku, taglia, outlet_gid, col_index)
        except Exception as e:
            log.warning("Write-back fallito: %s", e)
    else:
        log.info("Write-back SKIPPED (credenziali GSheets non configurate).")

    return ("OUTLET_CREATED", outlet_gid)

# ----------------------------------------------------------------------------- #
# Driver
# ----------------------------------------------------------------------------- #
def run(do_apply: bool) -> None:
    rows, col_index, ws = gs_read_rows()

    usable: List[Dict[str, Any]] = []
    for r in rows:
        rnorm = { _norm_key(k): v for k, v in r.items() }
        if not _truthy_si(rnorm.get("online")):
            continue
        qraw = rnorm.get("qta") or rnorm.get("qty") or "0"
        try:
            qv = int(float(str(qraw).replace(",", ".")))
        except Exception:
            qv = 0
        if qv <= 0:
            continue
        usable.append(rnorm)

    log.info("Righe totali: %d, selezionate (online==SI & Qta>0): %d", len(rows), len(usable))

    shop = Shopify()

    created = 0
    skipped_active = 0
    skipped_source = 0

    for r in usable:
        try:
            action, _ = process_row_outlet(shop, r, ws if do_apply else None, col_index, do_apply)
            if action == "OUTLET_CREATED":
                created += 1
            elif action == "SKIP_OUTLET_ALREADY_ACTIVE":
                skipped_active += 1
            elif action == "SKIP_SOURCE_NOT_FOUND":
                skipped_source += 1
        except Exception as e:
            log.error("Errore riga SKU=%s TAGLIA=%s: %s", r.get("sku"), r.get("taglia"), e)

    log.info("RIEPILOGO -> OUTLET creati: %d | SKIP già attivi: %d | Sorgente non trovato: %d",
             created, skipped_active, skipped_source)

def main() -> None:
    p = argparse.ArgumentParser(description="Workflow OUTLET")
    p.add_argument("--apply", action="store_true", help="Esegue davvero le operazioni su Shopify")
    args = p.parse_args()
    log.info("Avvio sync - workflow OUTLET")
    log.info("apply=%s", args.apply)
    run(do_apply=args.apply)
    log.info("Termine sync con exit code 0")

if __name__ == "__main__":
    main()
