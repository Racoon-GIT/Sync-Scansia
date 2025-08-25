# -*- coding: utf-8 -*-
"""
sync.py — Workflow OUTLET con fix:
- prezzi (bulk + fallback per-variant, logging dettagliato)
- tag (pulizia corretta con tags=[])
- media (ordine mantenuto via position)
- magazzino (set 0 + delete level + fallback adjust)
"""

from __future__ import annotations
import argparse, json, logging, os, re, time
from typing import Any, Dict, List, Optional, Tuple
import requests

import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("sync")

# ---------- util ----------
def _norm_key(k: str) -> str:
    return (k or "").strip().lower().replace("-", "_").replace(" ", "_")

def _clean_price(v: Any) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    s2 = re.sub(r"[^\d,\.]", "", s)
    if s2.count(",")==1 and s2.count(".")==0: s2 = s2.replace(",", ".")
    if s2.count(".")>1: s2 = s2.replace(".", "")
    try: return f"{float(s2):.2f}"
    except Exception: return None

def _truthy_si(v: Any) -> bool:
    if v is True: return True
    if isinstance(v, (int,float)): return int(v)==1
    if isinstance(v, str): return v.strip().lower() in {"si","sì","true","1","x","ok","yes"}
    return False

def _gid_numeric(gid: str) -> str | None:
    return gid.split("/")[-1] if gid else None

# ---------- gsheets ----------
def _gs_creds() -> Credentials:
    cred_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if cred_json:
        info = json.loads(cred_json)
        return Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path:
        raise RuntimeError("Missing GSheets creds: set GOOGLE_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS")
    return Credentials.from_service_account_file(path, scopes=["https://www.googleapis.com/auth/spreadsheets"])

def _get_env(name: str, *aliases: str, required: bool=False) -> str | None:
    for key in (name, *aliases):
        val = os.environ.get(key)
        if val is not None and str(val).strip()!="": return val
    if required: raise RuntimeError(f"Variabile mancante: {name} (alias: {', '.join(aliases)})")
    return None

def _gs_open():
    sheet_id = _get_env("GSPREAD_SHEET_ID", "SPREADSHEET_ID", required=True)
    title    = _get_env("GSPREAD_WORKSHEET_TITLE", "WORKSHEET_NAME", required=True)
    # auto-fix inversione
    id_like = bool(re.fullmatch(r"[A-Za-z0-9-_]{30,}", title))
    not_id_like = not re.fullmatch(r"[A-Za-z0-9-_]{30,}", sheet_id or "")
    if id_like and not_id_like:
        logger.warning("Rilevata inversione SHEET_ID/TITLE. Correggo automaticamente.")
        sheet_id, title = title, sheet_id
    creds = _gs_creds()
    sa = getattr(creds, "service_account_email", None) or getattr(creds, "_service_account_email", None)
    client = gspread.authorize(creds)
    logger.info("Google Sheet sorgente: id=%s | worksheet=%s | service_account=%s", sheet_id, title, sa or "N/D")
    try:
        sh = client.open_by_key(sheet_id)
    except gspread.SpreadsheetNotFound:
        logger.error("Spreadsheet 404. Verifica ID, condivisione a %s, e accesso a Shared Drive.", sa or "(service account)")
        raise
    return sh.worksheet(title)

def gs_read_rows() -> Tuple[List[Dict[str, Any]], Dict[str,int]]:
    ws = _gs_open()
    values = ws.get_all_values() or []
    if not values: return [], {}
    header = values[0]
    col_index = {_norm_key(h): i+1 for i,h in enumerate(header)}
    def norm_row(vs: List[str]) -> Dict[str,Any]:
        m={}
        for i,cell in enumerate(vs):
            key = _norm_key(header[i]) if i<len(header) else f"col{i+1}"
            m[key]=cell
        if "productid" in m and "product_id" not in m: m["product_id"]=m["productid"]
        return m
    rows = [norm_row(v) for v in values[1:]]
    logger.info("Caricate %d righe da Google Sheets (worksheet=%s)", len(rows), ws.title)
    logger.debug("Header normalizzati: %s", list(col_index.keys()))
    return rows, col_index

def gs_write_product_id(sku: str, taglia: str, new_gid: str, col_index: Dict[str,int]) -> bool:
    ws = _gs_open()
    all_vals = ws.get_all_values()
    if not all_vals: return False
    header = all_vals[0]
    idx_sku = col_index.get("sku") or (header.index("SKU")+1 if "SKU" in header else None)
    idx_tag = col_index.get("taglia") or (header.index("TAGLIA")+1 if "TAGLIA" in header else None)
    idx_pid = col_index.get("product_id") or (header.index("Product_Id")+1 if "Product_Id" in header else None)
    if not (idx_sku and idx_tag and idx_pid):
        logger.warning("Write-back: colonne SKU/TAGLIA/Product_Id non trovate.")
        return False
    for r_idx, row in enumerate(all_vals[1:], start=2):
        sku_cell = (row[idx_sku-1] if idx_sku-1<len(row) else "").strip()
        tag_cell = (row[idx_tag-1] if idx_tag-1<len(row) else "").strip()
        if sku_cell==sku and tag_cell==taglia:
            ws.update_cell(r_idx, idx_pid, new_gid)
            logger.info("Write-back Product_Id OK su riga %d (%s / %s)", r_idx, sku, taglia)
            return True
    logger.warning("Write-back: riga non trovata per SKU=%s TAGLIA=%s", sku, taglia)
    return False

# ---------- Shopify ----------
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
        self.min_interval = float(os.environ.get("SHOPIFY_MIN_INTERVAL_SEC", "0.7"))
        self.max_retries = int(os.environ.get("SHOPIFY_MAX_RETRIES", "5"))
        self._last_call_ts = 0.0
        self._location_cache: Dict[str, Any] | None = None

    def _throttle(self):
        now=time.time(); el=now-self._last_call_ts
        if el<self.min_interval: time.sleep(self.min_interval-el)

    def _request(self, method: str, path: str, **kw) -> requests.Response:
        url = self.base + path
        for attempt in range(1, self.max_retries+1):
            self._throttle()
            r = self.sess.request(method, url, **kw); self._last_call_ts=time.time()
            if r.status_code==429:
                ra = float(r.headers.get("Retry-After") or 1.0)
                logger.warning("429 %s %s. Retry fra %.2fs (tentativo %d/%d).", method, path, ra, attempt, self.max_retries)
                time.sleep(ra); continue
            if 500<=r.status_code<600:
                back=min(2**(attempt-1), 8)
                logger.warning("%s %s -> %d. Backoff %ss (tentativo %d/%d). Body: %s",
                               method, path, r.status_code, back, attempt, self.max_retries, r.text[:300])
                time.sleep(back); continue
            return r
        return r

    def _json_or_raise(self, method: str, path: str, r: requests.Response) -> dict:
        if r.status_code>=400: raise RuntimeError(f"{method} {path} -> {r.status_code} {r.text}")
        try: return r.json()
        except Exception: return {}

    def _get(self, path: str, **kw):  return self._json_or_raise("GET", path, self._request("GET", path, **kw))
    def _post(self, path: str, json: Dict[str,Any]|None=None, **kw): return self._json_or_raise("POST", path, self._request("POST", path, json=json, **kw))
    def _put(self, path: str, json: Dict[str,Any]|None=None, **kw):  return self._json_or_raise("PUT", path, self._request("PUT", path, json=json, **kw))
    def _delete(self, path: str, **kw):                           return self._json_or_raise("DELETE", path, self._request("DELETE", path, **kw))

    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(1, self.max_retries+1):
            self._throttle()
            r = self.sess.post(self.graphql_url, json={"query": query, "variables": variables}); self._last_call_ts=time.time()
            if r.status_code==429:
                ra=float(r.headers.get("Retry-After") or 1.0)
                logger.warning("429 GraphQL. Retry fra %.2fs (tentativo %d/%d).", ra, attempt, self.max_retries)
                time.sleep(ra); continue
            if 500<=r.status_code<600:
                back=min(2**(attempt-1), 8)
                logger.warning("GraphQL %d. Backoff %ss (tentativo %d/%d). Body: %s", r.status_code, back, attempt, self.max_retries, r.text[:300])
                time.sleep(back); continue
            if r.status_code>=400: raise RuntimeError(f"GraphQL -> {r.status_code} {r.text}")
            data=r.json()
            if "errors" in data: raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        raise RuntimeError("GraphQL fallito dopo retry")

    # ---- query helpers ----
    def find_product_by_sku_non_outlet(self, sku: str) -> Dict[str, Any] | None:
        data=self.graphql("""
        query($q:String!){
          products(first:10, query:$q){
            edges{ node{ id title handle status
              variants(first:100){ edges{ node{ id sku title selectedOptions{ name value } inventoryItem{ id } } } }
            }}
          }
        }""", {"q": f"sku:{sku}"})
        for e in data["products"]["edges"]:
            p=e["node"]
            if p["handle"].endswith("-outlet"): continue
            for ve in p["variants"]["edges"]:
                if (ve["node"]["sku"] or "").strip()==sku: return p
        return None

    def find_product_by_handle_any(self, handle: str) -> Dict[str, Any] | None:
        data=self.graphql("""query($q:String!){
          products(first:5, query:$q){ edges{ node{ id title handle status } } }
        }""", {"q": f"handle:{handle}"})
        return data["products"]["edges"][0]["node"] if data["products"]["edges"] else None

    # ---- duplicate & update ----
    def product_duplicate(self, source_gid: str, new_title: str) -> str:
        data=self.graphql("""
        mutation($productId:ID!, $newTitle:String!){
          productDuplicate(productId:$productId, newTitle:$newTitle){
            newProduct{ id }
            userErrors{ field message }
          }
        }""", {"productId": source_gid, "newTitle": new_title})
        dup=data["productDuplicate"]
        if dup["userErrors"]: raise RuntimeError(f"productDuplicate errors: {dup['userErrors']}")
        nid=dup.get("newProduct",{}).get("id")
        if not nid: raise RuntimeError("productDuplicate: newProduct.id mancante")
        return nid

    def product_update_handle_status(self, product_gid: str, handle: str, status: str="ACTIVE", tags: Optional[List[str]]=None) -> bool:
        inp={"id": product_gid, "handle": handle, "status": status}
        if tags is not None: inp["tags"]=tags  # deve essere [] per pulire
        data=self.graphql("""
        mutation($input: ProductInput!){
          productUpdate(input:$input){
            product{ id handle status tags }
            userErrors{ field message }
          }
        }""", {"input": inp})
        errs=data["productUpdate"]["userErrors"]
        if errs:
            logger.warning("productUpdate userErrors: %s", errs); return False
        out=data["productUpdate"]["product"]
        logger.info("productUpdate OK handle=%s status=%s tags=%s", out.get("handle"), out.get("status"), out.get("tags"))
        return True

    # ---- media ----
    def list_images(self, product_numeric_id: str) -> List[Dict[str, Any]]:
        return self._get(f"/products/{product_numeric_id}/images.json").get("images", [])

    def list_image_ids(self, product_numeric_id: str) -> List[int]:
        imgs=self._get(f"/products/{product_numeric_id}/images.json").get("images", [])
        return [img["id"] for img in imgs if "id" in img]

    def add_image(self, product_numeric_id: str, src_url: str, position: Optional[int]=None) -> None:
        payload={"image": {"src": src_url}}
        if position is not None: payload["image"]["position"]=position
        self._post(f"/products/{product_numeric_id}/images.json", json=payload)

    def delete_image(self, product_numeric_id: str, image_id: int) -> None:
        self._delete(f"/products/{product_numeric_id}/images/{image_id}.json")

    # ---- metafields ----
    def list_product_metafields(self, product_gid: str) -> List[Dict[str, Any]]:
        data=self.graphql("""
        query($id:ID!){
          node(id:$id){
            ... on Product{
              metafields(first:250){ edges{ node{ namespace key type value } } }
            }
          }
        }""", {"id": product_gid})
        edges=data.get("node",{}).get("metafields",{}).get("edges",[])
        return [e["node"] for e in edges]

    def metafields_set(self, owner_gid: str, metafields: List[Dict[str, Any]]) -> None:
        CHUNK=20
        for i in range(0, len(metafields), CHUNK):
            chunk=[{"ownerId": owner_gid, **m} for m in metafields[i:i+CHUNK]]
            data=self.graphql("""
            mutation($metafields:[MetafieldsSetInput!]!){
              metafieldsSet(metafields:$metafields){
                metafields{ id }
                userErrors{ field message }
              }
            }""", {"metafields": chunk})
            errs=data["metafieldsSet"]["userErrors"]
            if errs: logger.warning("metafieldsSet userErrors: %s", errs)

    # ---- collections ----
    def delete_all_collects(self, product_numeric_id: str) -> None:
        collects=self._get(f"/collects.json", params={"product_id": product_numeric_id}).get("collects", [])
        for c in collects:
            try: self._delete(f"/collects/{c['id']}.json")
            except Exception as e: logger.warning("delete_collect %s fallita: %s", c.get("id"), e)

    # ---- variants / prezzi ----
    def get_product_variants(self, product_gid: str) -> List[Dict[str, Any]]:
        data=self.graphql("""
        query($id:ID!){
          node(id:$id){
            ... on Product{
              variants(first:250){
                edges{ node{ id sku title inventoryItem{ id } selectedOptions{ name value } } }
              }
            }
          }
        }""", {"id": product_gid})
        edges=data["node"]["variants"]["edges"]
        return [e["node"] for e in edges]

    def wait_variants_ready(self, product_gid: str, timeout_sec: int=60) -> List[Dict[str, Any]]:
        start=time.time(); last=[]
        while time.time()-start<timeout_sec:
            vs=self.get_product_variants(product_gid)
            if vs:
                if last and len(vs)==len(last): return vs
                last=vs
            time.sleep(1.0)
        return last

    def variants_bulk_update_prices(self, product_gid: str, variant_gids: List[str], price: str, compare_at: Optional[str]) -> List[Dict[str, Any]]:
        variants=[{"id": gid, "price": price, "compareAtPrice": compare_at} for gid in variant_gids]
        data=self.graphql("""
        mutation($productId:ID!, $variants:[ProductVariantsBulkInput!]!){
          productVariantsBulkUpdate(productId:$productId, variants:$variants){
            product{ id }
            userErrors{ field message }
          }
        }""", {"productId": product_gid, "variants": variants})
        errs=data["productVariantsBulkUpdate"]["userErrors"]
        if errs: logger.warning("productVariantsBulkUpdate userErrors: %s", errs)
        else: logger.info("Prezzi aggiornati in bulk su %d varianti", len(variant_gids))
        return errs or []

    def variant_update_price_single(self, variant_gid: str, price: str, compare_at: Optional[str]) -> None:
        data=self.graphql("""
        mutation($input:ProductVariantInput!){
          productVariantUpdate(input:$input){
            productVariant{ id price compareAtPrice }
            userErrors{ field message }
          }
        }""", {"input": {"id": variant_gid, "price": price, "compareAtPrice": compare_at}})
        errs=data["productVariantUpdate"]["userErrors"]
        if errs: logger.warning("productVariantUpdate userErrors: %s", errs)

    # ---- locations / inventory ----
    def get_location_by_name(self, name: str) -> Dict[str, Any]:
        if self._location_cache is None:
            data=self._get("/locations.json"); self._location_cache={loc["name"]: loc for loc in data.get("locations", [])}
        if name in self._location_cache: return self._location_cache[name]
        raise RuntimeError(f"Location non trovata: {name}")

    def inventory_connect(self, inventory_item_id: int, location_id: int) -> None:
        self._post("/inventory_levels/connect.json", json={"inventory_item_id": inventory_item_id, "location_id": location_id})

    def inventory_set(self, inventory_item_id: int, location_id: int, qty: int) -> None:
        self._post("/inventory_levels/set.json", json={"inventory_item_id": inventory_item_id, "location_id": location_id, "available": int(qty)})

    def inventory_adjust(self, inventory_item_id: int, location_id: int, delta: int) -> None:
        self._post("/inventory_levels/adjust.json", json={"inventory_item_id": inventory_item_id, "location_id": location_id, "available_adjustment": int(delta)})

    def inventory_delete_level(self, inventory_item_id: int, location_id: int) -> None:
        # DELETE livello (idempotente), non parse JSON (può essere vuoto)
        self._request("DELETE", f"/inventory_levels.json?inventory_item_id={inventory_item_id}&location_id={location_id}")

# ---------- workflow per riga ----------
def process_row_outlet(shop: Shopify, row: Dict[str,Any], col_index: Dict[str,int]) -> Tuple[str, Optional[str]]:
    sku = (row.get("sku") or "").strip()
    taglia = (row.get("taglia") or "").strip()
    qta_raw = row.get("qta") or row.get("qty") or "0"
    try: qta = int(float(str(qta_raw).replace(",", ".")))
    except Exception: qta = 0

    prezzo_pieno = _clean_price(row.get("prezzo_pieno"))
    prezzo_scontato = _clean_price(row.get("prezzo_scontato")) or (prezzo_pieno or "0.00")

    source = shop.find_product_by_sku_non_outlet(sku)
    if not source:
        logger.warning("SOURCE_NOT_FOUND sku=%s -> skip", sku); return ("SKIP_SOURCE_NOT_FOUND", None)
    source_gid, source_handle, source_title = source["id"], source["handle"], source["title"]

    outlet_handle  = source_handle+"-outlet" if not source_handle.endswith("-outlet") else source_handle
    outlet_title   = f"{source_title} - Outlet" if not source_title.strip().endswith(" - Outlet") else source_title

    outlet_existing = shop.find_product_by_handle_any(outlet_handle)
    if outlet_existing:
        if outlet_existing["status"]=="ACTIVE":
            logger.info("SKIP_OUTLET_ALREADY_ACTIVE handle=%s", outlet_handle)
            return ("SKIP_OUTLET_ALREADY_ACTIVE", outlet_existing["id"])
        else:
            nid=_gid_numeric(outlet_existing["id"])
            try:
                shop._delete(f"/products/{nid}.json")
                logger.info("DELETE_DRAFT_OUTLET ok handle=%s", outlet_handle)
            except Exception as e:
                logger.warning("DELETE_DRAFT_OUTLET fallita: %s", e)

    outlet_gid = shop.product_duplicate(source_gid, outlet_title)
    logger.info("DUPLICATED outlet=%s (da %s)", outlet_gid, source_gid)

    # handle + ACTIVE + tags=[]
    desired = outlet_handle
    ok = shop.product_update_handle_status(outlet_gid, desired, status="ACTIVE", tags=[])
    if not ok:
        for i in range(1, 20):
            cand=f"{outlet_handle}-{i}"
            if shop.product_update_handle_status(outlet_gid, cand, status="ACTIVE", tags=[]):
                desired=cand; ok=True; break
    if not ok: raise RuntimeError("Impossibile impostare handle per il nuovo outlet")

    # Fallback REST: pulizia tag anche via REST (alcuni temi/plugins rimettono tag)
    try:
        out_num = _gid_numeric(outlet_gid)
        shop._put(f"/products/{out_num}.json", json={"product": {"id": int(out_num), "tags": ""}})
        logger.info("Fallback REST tags cleared per product %s", out_num)
    except Exception as e:
        logger.warning("Fallback REST tags clear fallito: %s", e)

    # Varianti pronte + prezzi
    outlet_variants = shop.wait_variants_ready(outlet_gid, timeout_sec=60)
    variant_gids = [v["id"] for v in outlet_variants]
    errs = shop.variants_bulk_update_prices(outlet_gid, variant_gids, prezzo_scontato, prezzo_pieno)
    if errs:
        logger.info("Bulk prezzi con errori, passo a per-variant...")
        for gid in variant_gids:
            shop.variant_update_price_single(gid, prezzo_scontato, prezzo_pieno)

    # Media: reset + reinsert ordinato
    try:
        src_num = _gid_numeric(source_gid); out_num = _gid_numeric(outlet_gid)
        src_imgs = shop.list_images(src_num)
        src_urls = [img.get("src") for img in src_imgs if img.get("src")]
        # wipe outlet images
        for iid in shop.list_image_ids(out_num):
            try: shop.delete_image(out_num, iid)
            except Exception as e: logger.warning("Delete image %s fallita: %s", iid, e)
        # insert maintaining order (position=1..N)
        for i, url in enumerate(src_urls, start=1):
            shop.add_image(out_num, url, position=i)
        logger.info("Copiate %d immagini in ordine", len(src_urls))
    except Exception as e:
        logger.warning("Copy immagini (ordinato) fallita: %s", e)

    # Metafields
    try:
        mfs = shop.list_product_metafields(source_gid)
        if mfs:
            shop.metafields_set(outlet_gid, [
                {"namespace": m["namespace"], "key": m["key"], "type": m.get("type") or "single_line_text_field", "value": m.get("value") or ""}
                for m in mfs
            ])
    except Exception as e:
        logger.warning("Copy metafield fallita: %s", e)

    # Collections
    try:
        out_num = _gid_numeric(outlet_gid); shop.delete_all_collects(out_num)
    except Exception as e:
        logger.warning("Pulizia collects fallita: %s", e)

    # Locations & inventory
    promo_name = os.environ.get("PROMO_LOCATION_NAME","").strip()
    mag_name   = os.environ.get("MAGAZZINO_LOCATION_NAME","").strip()
    promo = shop.get_location_by_name(promo_name) if promo_name else None
    mag   = shop.get_location_by_name(mag_name) if mag_name else None

    # collega tutte le varianti a Promo (0)
    for v in outlet_variants:
        inv_item = v["inventoryItem"]["id"]; inv_num=int(_gid_numeric(inv_item))
        if promo:
            try: shop.inventory_connect(inv_num, promo["id"])
            except Exception: pass
            shop.inventory_set(inv_num, promo["id"], 0)

    # set quantità sulla variante target
    target_variant=None
    for v in outlet_variants:
        if (v.get("sku") or "").strip()==sku:
            if taglia:
                if any((opt["name"] or "").lower() in {"size","taglia"} and (opt["value"] or "").strip()==taglia for opt in v.get("selectedOptions", [])):
                    target_variant=v; break
            else:
                target_variant=v; break
    if target_variant and promo:
        inv_item=int(_gid_numeric(target_variant["inventoryItem"]["id"]))
        shop.inventory_set(inv_item, promo["id"], qta)

    # Magazzino → 0 + delete level (+ fallback adjust)
    if mag:
        for v in outlet_variants:
            inv_num=int(_gid_numeric(v["inventoryItem"]["id"]))
            try: shop.inventory_set(inv_num, mag["id"], 0)
            except Exception as e: logger.warning("inventory_set magazzino=0 fallita: %s", e)
            try: shop.inventory_delete_level(inv_num, mag["id"])
            except Exception as e: logger.warning("inventory_delete_level fallita: %s", e)
            try: shop.inventory_adjust(inv_num, mag["id"], 0)  # no-op, ma triggera eventuali sync
            except Exception: pass

    # Write-back
    try: gs_write_product_id(sku, taglia, outlet_gid, col_index)
    except Exception as e: logger.warning("Write-back fallito: %s", e)

    return ("OUTLET_CREATED", outlet_gid)

# ---------- driver ----------
def run(do_apply: bool) -> None:
    rows, col_index = gs_read_rows()

    usable=[]
    for r in rows:
        rnorm={_norm_key(k): v for k,v in r.items()}
        if not _truthy_si(rnorm.get("online")): continue
        qraw=rnorm.get("qta") or rnorm.get("qty") or "0"
        try: qv=int(float(str(qraw).replace(",", ".")))
        except Exception: qv=0
        if qv<=0: continue
        usable.append(rnorm)

    logger.info("Righe totali: %d, selezionate (online==SI & Qta>0): %d", len(rows), len(usable))
    if not do_apply:
        logger.info("DRY-RUN: nessuna azione su Shopify."); return

    shop=Shopify()
    created=skipped_active=skipped_source=0
    for r in usable:
        try:
            action,_=process_row_outlet(shop, r, col_index)
            if action=="OUTLET_CREATED": created+=1
            elif action=="SKIP_OUTLET_ALREADY_ACTIVE": skipped_active+=1
            elif action=="SKIP_SOURCE_NOT_FOUND": skipped_source+=1
        except Exception as e:
            logger.error("Errore riga SKU=%s TAGLIA=%s: %s", r.get("sku"), r.get("taglia"), e)

    logger.info("RIEPILOGO -> OUTLET creati: %d | SKIP già attivi: %d | Sorgente non trovato: %d", created, skipped_active, skipped_source)

def main() -> None:
    p=argparse.ArgumentParser(description="Workflow OUTLET")
    p.add_argument("--apply", action="store_true", help="Esegue davvero le operazioni su Shopify")
    args=p.parse_args()
    logger.info("Avvio sync - workflow OUTLET"); logger.info("apply=%s", args.apply)
    run(do_apply=args.apply)
    logger.info("Termine sync con exit code 0")

if __name__=="__main__":
    main()
