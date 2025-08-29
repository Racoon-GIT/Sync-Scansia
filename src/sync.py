# -*- coding: utf-8 -*-
"""
sync.py — Workflow OUTLET

Esegui:
    python -m src.sync --apply

ENV richieste (nomi NON cambiati):
  # Google Sheets
  GSPREAD_SHEET_ID
  GSPREAD_WORKSHEET_TITLE
  GOOGLE_CREDENTIALS_JSON  (oppure GOOGLE_APPLICATION_CREDENTIALS -> file)

  # Shopify
  SHOPIFY_STORE                 es: racoon-lab.myshopify.com
  SHOPIFY_ADMIN_TOKEN
  SHOPIFY_API_VERSION           es: 2025-01

  # Locations (inventario)
  PROMO_LOCATION_NAME           es: Promo
  MAGAZZINO_LOCATION_NAME       es: Magazzino

  # Rate limit / retry (opzionali)
  SHOPIFY_MIN_INTERVAL_SEC      default: 0.7
  SHOPIFY_MAX_RETRIES           default: 5
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

# ---- GSheets (lettura e write-back) ----
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("sync")

# =============================================================================
# Utils
# =============================================================================

def _norm_key(k: str) -> str:
    return (k or "").strip().lower().replace("-", "_").replace(" ", "_")

def _clean_price(v: Any) -> Optional[str]:
    """ '€ 129', '129€', '129,90', '129.90' -> '129.90' """
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

def _gid_numeric(gid: str) -> str | None:
    return gid.split("/")[-1] if gid else None

# =============================================================================
# GSheets IO (read + write-back)
# =============================================================================

def _gs_creds() -> Credentials:
    cred_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if cred_json:
        info = json.loads(cred_json)
        return Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path:
        raise RuntimeError("Credenziali GSheets mancanti: imposta GOOGLE_CREDENTIALS_JSON o GOOGLE_APPLICATION_CREDENTIALS")
    return Credentials.from_service_account_file(path, scopes=["https://www.googleapis.com/auth/spreadsheets"])

def _get_env(name: str, *aliases: str, required: bool = False) -> str | None:
    for key in (name, *aliases):
        val = os.environ.get(key)
        if val is not None and str(val).strip() != "":
            return val
    if required:
        raise RuntimeError(f"Variabile mancante: {name} (provati anche alias: {', '.join(aliases)})")
    return None

def _gs_open():
    sheet_id = _get_env("GSPREAD_SHEET_ID", "SPREADSHEET_ID", required=True)
    title    = _get_env("GSPREAD_WORKSHEET_TITLE", "WORKSHEET_NAME", required=True)

    creds = _gs_creds()
    try:
        sa_email = getattr(creds, "service_account_email", None) or getattr(creds, "_service_account_email", None)
    except Exception:
        sa_email = None

    client = gspread.authorize(creds)
    logger.info("Google Sheet sorgente: id=%s | worksheet=%s | service_account=%s",
                sheet_id, title, sa_email or "N/D")

    try:
        sh = client.open_by_key(sheet_id)
    except gspread.SpreadsheetNotFound:
        logger.error("Spreadsheet non trovato (404). Controlla che:\n"
                     "- GSPREAD_SHEET_ID sia l'ID giusto (non l'URL intero)\n"
                     "- Il foglio sia condiviso con: %s\n"
                     "- Se è in uno Shared Drive, che il service account sia membro del drive.",
                     sa_email or "(service account)")
        raise
    ws = sh.worksheet(title)
    return ws

def gs_read_rows() -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    ws = _gs_open()
    values = ws.get_all_values()
    if not values:
        return [], {}
    header = values[0]
    col_index = { _norm_key(h): i+1 for i, h in enumerate(header) }

    def norm_row(vs: List[str]) -> Dict[str, Any]:
        m = {}
        for i, cell in enumerate(vs):
            key = _norm_key(header[i]) if i < len(header) else f"col{i+1}"
            m[key] = cell
        if "productid" in m and "product_id" not in m:
            m["product_id"] = m["productid"]
        return m

    rows = [norm_row(v) for v in values[1:]]
    logger.info("Caricate %d righe da Google Sheets (worksheet=%s)", len(rows), os.environ["GSPREAD_WORKSHEET_TITLE"])
    logger.debug("Header normalizzati: %s", list(col_index.keys()))
    return rows, col_index

def gs_write_product_id(sku: str, taglia: str, new_gid: str, col_index: Dict[str, int]) -> bool:
    ws = _gs_open()
    all_vals = ws.get_all_values()
    header = all_vals[0]
    idx_sku   = col_index.get("sku") or (header.index("SKU")+1 if "SKU" in header else None)
    idx_tag   = col_index.get("taglia") or (header.index("TAGLIA")+1 if "TAGLIA" in header else None)
    idx_pid   = col_index.get("product_id") or (header.index("Product_Id")+1 if "Product_Id" in header else None)
    if not (idx_sku and idx_tag and idx_pid):
        logger.warning("Write-back: colonne SKU/TAGLIA/Product_Id non trovate.")
        return False
    for r_idx, row in enumerate(all_vals[1:], start=2):
        sku_cell = (row[idx_sku-1] if idx_sku-1 < len(row) else "").strip()
        tag_cell = (row[idx_tag-1] if idx_tag-1 < len(row) else "").strip()
        if sku_cell == sku and tag_cell == taglia:
            ws.update_cell(r_idx, idx_pid, new_gid)
            logger.info("Write-back Product_Id OK su riga %d (%s / %s)", r_idx, sku, taglia)
            return True
    logger.warning("Write-back: riga non trovata per SKU=%s TAGLIA=%s", sku, taglia)
    return False

# =============================================================================
# Shopify client (REST + GraphQL) con throttle/retry
# =============================================================================

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

    # ---- throttle/retry base ----

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
            if r.status_code == 429:
                retry_after_hdr = r.headers.get("Retry-After")
                retry_after = float(retry_after_hdr) if retry_after_hdr else 1.0
                logger.warning("429 su %s %s. Retry fra %.2fs (tentativo %d/%d).",
                               method, path, retry_after, attempt, self.max_retries)
                time.sleep(retry_after)
                continue
            if 500 <= r.status_code < 600:
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning("%s %s -> %d. Backoff %ss (tentativo %d/%d). Body: %s",
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
        # DELETE su inventory_levels non ha body JSON in caso di successo; non fare _json_or_raise
        if r.status_code >= 400:
            raise RuntimeError(f"DELETE {path} -> {r.status_code} {r.text}")
        return {}

    # ---- GraphQL ----

    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            r = self.sess.post(self.graphql_url, json={"query": query, "variables": variables})
            self._last_call_ts = time.time()
            if r.status_code == 429:
                retry_after_hdr = r.headers.get("Retry-After")
                retry_after = float(retry_after_hdr) if retry_after_hdr else 1.0
                logger.warning("429 GraphQL. Retry fra %.2fs (tentativo %d/%d).", retry_after, attempt, self.max_retries)
                time.sleep(retry_after)
                continue
            if 500 <= r.status_code < 600:
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning("GraphQL %d. Backoff %ss (tentativo %d/%d). Body: %s",
                               r.status_code, backoff, attempt, self.max_retries, r.text[:300])
                time.sleep(backoff)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"GraphQL -> {r.status_code} {r.text}")
            data = r.json()
            if "errors" in data:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        raise RuntimeError("GraphQL fallito dopo retry")

    # =============================================================================
    # Ricerca prodotto sorgente e outlet
    # =============================================================================

    def find_product_by_sku_non_outlet(self, sku: str) -> Dict[str, Any] | None:
        q = f"sku:{sku}"
        data = self.graphql("""
        query($q: String!) {
          products(first: 10, query: $q) {
            edges { node { id title handle status
              variants(first: 100) { edges { node { id sku title selectedOptions { name value } inventoryItem { id } } } }
            }}
          }
        }""", {"q": q})
        for edge in data["products"]["edges"]:
            p = edge["node"]
            if p["handle"].endswith("-outlet"):
                continue
            for vedge in p["variants"]["edges"]:
                if (vedge["node"]["sku"] or "").strip() == sku:
                    return p
        return None

    def find_product_by_handle_any(self, handle: str) -> Dict[str, Any] | None:
        q = f"handle:{handle}"
        data = self.graphql("""
        query($q:String!){
          products(first:5, query:$q){ edges{ node{ id title handle status } } }
        }""", {"q": q})
        if data["products"]["edges"]:
            return data["products"]["edges"][0]["node"]
        return None

    # =============================================================================
    # Duplicazione + polling
    # =============================================================================

    def product_duplicate(self, source_gid: str, new_title: str) -> str:
    # Shopify ora richiede newTitle non-null (String!)
    safe_title = (new_title or "").strip() or "Outlet"
    data = self.graphql("""
    mutation($productId: ID!, $newTitle: String!) {
      productDuplicate(productId: $productId, newTitle: $newTitle) {
        newProduct { id }
        userErrors { message field }
      }
    }""", {"productId": source_gid, "newTitle": safe_title})

    dup = data["productDuplicate"]
    if dup["userErrors"]:
        raise RuntimeError(f"productDuplicate errors: {dup['userErrors']}")
    new_gid = dup["newProduct"]["id"]
    num = _gid_numeric(new_gid)

    # Poll REST finché il prodotto è disponibile
    for _ in range(60):
        try:
            pr = self._get(f"/products/{num}.json").get("product")
            if pr and pr.get("id"):
                return new_gid
        except Exception:
            pass
        time.sleep(1.0)
    raise RuntimeError("Timeout in duplicazione: nuovo prodotto non trovato")


    # =============================================================================
    # Media / immagini
    # =============================================================================

    def list_images(self, product_numeric_id: str) -> List[Dict[str, Any]]:
        return self._get(f"/products/{product_numeric_id}/images.json").get("images", [])

    def add_image(self, product_numeric_id: str, src_url: str, position: int) -> None:
        self._post(f"/products/{product_numeric_id}/images.json",
                   json={"image": {"src": src_url, "position": position, "alt": ""}})

    def delete_image(self, product_numeric_id: str, image_id: int) -> None:
        self._delete(f"/products/{product_numeric_id}/images/{image_id}.json")

    # =============================================================================
    # Metafield copy
    # =============================================================================

    def list_product_metafields(self, product_gid: str) -> List[Dict[str, Any]]:
        data = self.graphql("""
        query($id:ID!){
          node(id:$id){
            ... on Product {
              metafields(first: 250){ edges{ node{ namespace key type value } } }
            }
          }
        }""", {"id": product_gid})
        edges = data["node"]["metafields"]["edges"] if data.get("node") and data["node"].get("metafields") else []
        return [e["node"] for e in edges]

    def metafields_set(self, owner_gid: str, metafields: List[Dict[str, Any]]) -> None:
        CHUNK = 20
        for i in range(0, len(metafields), CHUNK):
            chunk = [{"ownerId": owner_gid, **m} for m in metafields[i:i+CHUNK]]
            data = self.graphql("""
            mutation($metafields:[MetafieldsSetInput!]!){
              metafieldsSet(metafields:$metafields){
                metafields { id }
                userErrors { field message }
              }
            }""", {"metafields": chunk})
            errs = data["metafieldsSet"]["userErrors"]
            if errs:
                logger.warning("metafieldsSet userErrors: %s", errs)

    # =============================================================================
    # Collections: cancella collect (manuali)
    # =============================================================================

    def delete_all_collects(self, product_numeric_id: str) -> None:
        collects = self._get("/collects.json", params={"product_id": product_numeric_id}).get("collects", [])
        for c in collects:
            try:
                self._delete(f"/collects/{c['id']}.json")
            except Exception as e:
                logger.warning("delete_collect %s fallita: %s", c.get("id"), e)

    # =============================================================================
    # Varianti & prezzi (bulk)
    # =============================================================================

    def get_product_variants(self, product_gid: str) -> List[Dict[str, Any]]:
        data = self.graphql("""
        query($id:ID!){
          node(id:$id){
            ... on Product {
              variants(first: 250){
                edges{ node{ id sku title inventoryItem{ id } selectedOptions{ name value } } }
              }
              title handle status
            }
          }
        }""", {"id": product_gid})
        edges = data["node"]["variants"]["edges"]
        return [e["node"] for e in edges]

    def variants_bulk_update_prices(self, product_gid: str, variant_gids: List[str], price: str, compare_at: str | None):
        variants = [{"id": gid, "price": price, "compareAtPrice": compare_at} for gid in variant_gids]
        data = self.graphql("""
        mutation($productId:ID!, $variants:[ProductVariantsBulkInput!]!){
          productVariantsBulkUpdate(productId:$productId, variants:$variants){
            product { id }
            userErrors { field message }
          }
        }""", {"productId": product_gid, "variants": variants})
        errs = data["productVariantsBulkUpdate"]["userErrors"]
        if errs:
            logger.warning("productVariantsBulkUpdate userErrors: %s", errs)

    # =============================================================================
    # Inventory / Locations
    # =============================================================================

    def get_location_by_name(self, name: str) -> Dict[str, Any]:
        if self._location_cache is None:
            data = self._get("/locations.json")
            self._location_cache = {loc["name"]: loc for loc in data.get("locations", [])}
        if name in self._location_cache:
            return self._location_cache[name]
        raise RuntimeError(f"Location non trovata: {name}")

    def list_inventory_levels(self, inventory_item_id: int) -> List[Dict[str, Any]]:
        resp = self._get("/inventory_levels.json", params={"inventory_item_ids": inventory_item_id})
        return resp.get("inventory_levels", [])

    def inventory_connect(self, inventory_item_id: int, location_id: int) -> None:
        self._post("/inventory_levels/connect.json",
                   json={"inventory_item_id": inventory_item_id, "location_id": location_id})

    def inventory_set(self, inventory_item_id: int, location_id: int, qty: int) -> None:
        self._post("/inventory_levels/set.json",
                   json={"inventory_item_id": inventory_item_id, "location_id": location_id, "available": int(qty)})

    def inventory_delete_level(self, inventory_item_id: int, location_id: int) -> None:
        # ATTENZIONE: path RELATIVO, niente doppio /admin/api/...
        self._delete("/inventory_levels.json",
                     params={"inventory_item_id": inventory_item_id, "location_id": location_id})

    # =============================================================================
    # Product base update (handle/status/tags)
    # =============================================================================

    def update_product_basic(self, product_numeric_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._put(f"/products/{product_numeric_id}.json", json={"product": {"id": int(product_numeric_id), **payload}})

# =============================================================================
# Media helper (idempotente, preserva ordine)
# =============================================================================

def media_rebuild(shop: Shopify, source_num: str, outlet_num: str) -> int:
    """Cancella tutte le immagini dell'outlet e le reinserisce in ordine dal sorgente, alt = ''. Ritorna quante inserite."""
    src_imgs = shop.list_images(source_num)
    order = sorted(src_imgs, key=lambda x: x.get("position") or 9999)
    out_imgs = shop.list_images(outlet_num)
    # delete tutte sull'outlet
    for img in out_imgs:
        try:
            shop.delete_image(outlet_num, img["id"])
        except Exception as e:
            logger.warning("delete immagine %s fallita: %s", img.get("id"), e)
    # reinserisci in ordine
    count = 0
    for i, img in enumerate(order):
        src_url = img.get("src")
        if not src_url:
            continue
        shop.add_image(outlet_num, src_url, i+1)
        count += 1
        # piccolo spacing per rate limit sugli images
        time.sleep(0.15)
    return count

# =============================================================================
# Inventory helper (Promo prima, poi Magazzino a 0 + disconnect)
# =============================================================================

def inventory_apply(shop: Shopify,
                    outlet_variants: List[Dict[str, Any]],
                    sku_target: str,
                    taglia: str | None,
                    qta: int,
                    promo_name: str | None,
                    mag_name: str | None) -> None:
    promo = shop.get_location_by_name(promo_name) if promo_name else None
    mag   = shop.get_location_by_name(mag_name) if mag_name else None

    # Connessione a Promo e reset a 0 su TUTTE le varianti
    if promo:
        for v in outlet_variants:
            inv_item_gid = v["inventoryItem"]["id"]
            inv_num = int(_gid_numeric(inv_item_gid))
            try:
                shop.inventory_connect(inv_num, promo["id"])
            except Exception:
                pass
            shop.inventory_set(inv_num, promo["id"], 0)

    # Trova la variante target per quantità su Promo
    if promo:
        target = None
        for v in outlet_variants:
            if (v.get("sku") or "").strip() == (sku_target or "").strip():
                if taglia:
                    ok = False
                    for opt in v.get("selectedOptions", []):
                        if (opt["name"] or "").lower() in {"size", "taglia"} and (opt["value"] or "").strip() == taglia:
                            ok = True
                            break
                    if ok:
                        target = v
                        break
                else:
                    target = v
                    break
        if target:
            inv_item = int(_gid_numeric(target["inventoryItem"]["id"]))
            shop.inventory_set(inv_item, promo["id"], qta)

    # MAGAZZINO -> set a 0 + disconnect del livello (affinché l'unica disponibilità resti Promo)
    if mag:
        for v in outlet_variants:
            inv_item = int(_gid_numeric(v["inventoryItem"]["id"]))
            # Se esiste un livello in magazzino, prima lo portiamo a 0, poi lo disconnettiamo.
            levels = shop.list_inventory_levels(inv_item)
            for lvl in levels:
                if lvl.get("location_id") == mag["id"]:
                    try:
                        if (lvl.get("available") or 0) != 0:
                            shop.inventory_set(inv_item, mag["id"], 0)
                    except Exception:
                        # anche se set fallisce, proviamo comunque a disconnettere
                        pass
                    try:
                        shop.inventory_delete_level(inv_item, mag["id"])
                    except Exception as e:
                        logger.warning("inventory_delete_level inv_item=%s loc=%s fallita: %s", inv_item, mag["id"], e)

# =============================================================================
# Workflow per UNA riga (SKU/TAGLIA/QTA/PREZZI/ONLINE)
# =============================================================================

def process_row_outlet(shop: Shopify,
                       row: Dict[str, Any],
                       col_index: Dict[str, int]) -> Tuple[str, Optional[str]]:
    sku = (row.get("sku") or "").strip()
    taglia = (row.get("taglia") or "").strip()
    qta_raw = row.get("qta") or row.get("qty") or "0"
    try:
        qta = int(float(str(qta_raw).replace(",", ".")))
    except Exception:
        qta = 0

    prezzo_pieno = _clean_price(row.get("prezzo_pieno"))
    prezzo_scontato = _clean_price(row.get("prezzo_scontato"))
    if not prezzo_scontato:
        prezzo_scontato = prezzo_pieno or "0.00"

    # 1) Risali al prodotto sorgente
    source = shop.find_product_by_sku_non_outlet(sku)
    if not source:
        logger.warning("SOURCE_NOT_FOUND sku=%s -> skip", sku)
        return ("SKIP_SOURCE_NOT_FOUND", None)
    source_gid = source["id"]
    source_handle = source["handle"]
    source_title = source["title"]

    # 2) Handle/titolo Outlet desiderati
    desired_handle = source_handle + "-outlet" if not source_handle.endswith("-outlet") else source_handle
    outlet_title = f"{source_title} - Outlet" if not source_title.strip().endswith(" - Outlet") else source_title

    # 3) Gestione esistenza Outlet
    outlet_existing = shop.find_product_by_handle_any(desired_handle)
    if outlet_existing:
        if outlet_existing["status"] == "ACTIVE":
            logger.info("SKIP_OUTLET_ALREADY_ACTIVE handle=%s", desired_handle)
            return ("SKIP_OUTLET_ALREADY_ACTIVE", outlet_existing["id"])
        else:
            # bozza: elimino
            try:
                nid = _gid_numeric(outlet_existing["id"])
                shop._delete(f"/products/{nid}.json")
                logger.info("DELETE_DRAFT_OUTLET ok handle=%s", desired_handle)
            except Exception as e:
                logger.warning("DELETE_DRAFT_OUTLET fallito: %s", e)

    # 4) DUPLICA (GraphQL compatibile)
    outlet_gid = shop.product_duplicate(source_gid, outlet_title)
    outlet_num = _gid_numeric(outlet_gid)
    source_num = _gid_numeric(source_gid)
    logger.info("DUPLICATED outlet=%s (handle=%s)", outlet_gid, desired_handle)

    # 5) Aggiorna handle+status+tags con fallback
    #    Proviamo handle desiderato; se preso, tentiamo -1, -2, ...
    base = desired_handle
    suffix = 0
    while True:
        try:
            payload = {"handle": base if suffix == 0 else f"{base}-{suffix}",
                       "status": "active",
                       "tags": ""}  # tags vuoto
            shop.update_product_basic(outlet_num, payload)
            break
        except RuntimeError as e:
            text = str(e)
            if "has already been taken" in text or "taken" in text:
                suffix += 1
                continue
            else:
                raise
    final_handle = base if suffix == 0 else f"{base}-{suffix}"

    # 6) Prezzi: bulk su tutte le varianti
    outlet_variants = shop.get_product_variants(outlet_gid)
    variant_gids = [v["id"] for v in outlet_variants]
    shop.variants_bulk_update_prices(outlet_gid, variant_gids, prezzo_scontato, prezzo_pieno)
    # log di controllo (prime 3)
    preview = [(source_title[:22], prezzo_scontato, prezzo_pieno) for _ in range(min(3, len(variant_gids)))]
    logger.info("PREZZI OK (prime 3 varianti): %s", preview)

    # 7) Media: ricostruzione ordine + alt vuoto (idempotente)
    try:
        n = media_rebuild(shop, source_num, outlet_num)
        logger.info("MEDIA REBUILT: %d immagini replicate in ordine", n)
    except Exception as e:
        logger.warning("Copy immagini fallita (non bloccante): %s", e)

    # 8) Metafield: copia
    try:
        mfs = shop.list_product_metafields(source_gid)
        transferable = [{
            "namespace": m["namespace"],
            "key": m["key"],
            "type": m.get("type") or "single_line_text_field",
            "value": m.get("value") or "",
        } for m in mfs]
        if transferable:
            shop.metafields_set(outlet_gid, transferable)
    except Exception as e:
        logger.warning("Copy metafield fallita (non bloccante): %s", e)

    # 9) Collections: elimina tutte le collects manuali
    try:
        shop.delete_all_collects(outlet_num)
    except Exception as e:
        logger.warning("Pulizia collects fallita (non bloccante): %s", e)

    # 10) Locations & inventario (Promo prima, poi Magazzino a 0 + disconnect)
    promo_name = os.environ.get("PROMO_LOCATION_NAME", "").strip() or None
    mag_name   = os.environ.get("MAGAZZINO_LOCATION_NAME", "").strip() or None
    inventory_apply(shop, outlet_variants, sku, taglia, qta, promo_name, mag_name)

    # 11) Write-back Product_Id su GSheet
    try:
        gs_write_product_id(sku, taglia, outlet_gid, col_index)
    except Exception as e:
        logger.warning("Write-back fallito: %s", e)

    return ("OUTLET_CREATED", outlet_gid)

# =============================================================================
# Driver
# =============================================================================

def run(do_apply: bool) -> None:
    rows, col_index = gs_read_rows()

    # Normalizza e filtra
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

    logger.info("Righe totali: %d, selezionate (online==SI & Qta>0): %d", len(rows), len(usable))

    if not do_apply:
        logger.info("DRY-RUN: nessuna azione su Shopify. (--apply per eseguire)")
        return

    shop = Shopify()

    created = 0
    skipped_active = 0
    skipped_source = 0

    for r in usable:
        try:
            action, out_gid = process_row_outlet(shop, r, col_index)
            if action == "OUTLET_CREATED":
                created += 1
            elif action == "SKIP_OUTLET_ALREADY_ACTIVE":
                skipped_active += 1
            elif action == "SKIP_SOURCE_NOT_FOUND":
                skipped_source += 1
        except Exception as e:
            logger.error("Errore riga SKU=%s TAGLIA=%s: %s", r.get("sku"), r.get("taglia"), e)

    logger.info("RIEPILOGO -> OUTLET creati: %d | SKIP già attivi: %d | Sorgente non trovato: %d",
                created, skipped_active, skipped_source)

def main() -> None:
    parser = argparse.ArgumentParser(description="Workflow OUTLET")
    parser.add_argument("--apply", action="store_true", help="Esegue davvero le operazioni su Shopify")
    args = parser.parse_args()

    logger.info("Avvio sync - workflow OUTLET")
    logger.info("apply=%s", args.apply)

    run(do_apply=args.apply)

    logger.info("Termine sync con exit code 0")

if __name__ == "__main__":
    main()
