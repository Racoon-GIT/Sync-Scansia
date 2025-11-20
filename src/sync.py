# -*- coding: utf-8 -*-
"""
sync.py — Workflow OUTLET (VERSIONE CORRETTA - Bug Fixed)

Esegui:
    python -m src.sync --apply

ENV richieste:
  # Google Sheets
  GSPREAD_SHEET_ID
  GSPREAD_WORKSHEET_TITLE
  GOOGLE_CREDENTIALS_JSON  (oppure GOOGLE_APPLICATION_CREDENTIALS)

  # Shopify
  SHOPIFY_STORE
  SHOPIFY_ADMIN_TOKEN
  SHOPIFY_API_VERSION

  # Locations
  PROMO_LOCATION_NAME
  MAGAZZINO_LOCATION_NAME
"""

import argparse
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("sync")

# =============================================================================
# Utils
# =============================================================================

def _norm_key(k: str) -> str:
    """Normalizza chiavi colonne"""
    return (k or "").strip().lower().replace("-", "_").replace(" ", "_")

def _clean_price(v: Any) -> Optional[str]:
    """'€ 129', '129,90' -> '129.90'"""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s2 = re.sub(r"[^\d,\.]", "", s)
    if s2.count(",") == 1 and s2.count(".") == 0:
        s2 = s2.replace(",", ".")
    try:
        return f"{float(s2):.2f}"
    except Exception:
        return None

def _truthy_si(v: Any) -> bool:
    """Verifica se valore è 'SI' o equivalente"""
    if v is True:
        return True
    if isinstance(v, (int, float)):
        return int(v) == 1
    if isinstance(v, str):
        return v.strip().lower() in {"si", "sì", "true", "1", "x", "ok", "yes"}
    return False

def _gid_numeric(gid: str) -> Optional[str]:
    """gid://shopify/Product/123 -> '123'"""
    return gid.split("/")[-1] if gid else None

# =============================================================================
# GSheets IO
# =============================================================================

def _gs_creds() -> Credentials:
    """Ottiene credenziali Google"""
    cred_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if cred_json:
        info = json.loads(cred_json)
        return Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path:
        raise RuntimeError("Credenziali mancanti: GOOGLE_CREDENTIALS_JSON o GOOGLE_APPLICATION_CREDENTIALS")
    return Credentials.from_service_account_file(path, scopes=["https://www.googleapis.com/auth/spreadsheets"])

def _gs_open():
    """Apre worksheet Google Sheets"""
    sheet_id = os.environ["GSPREAD_SHEET_ID"]
    title = os.environ["GSPREAD_WORKSHEET_TITLE"]
    
    creds = _gs_creds()
    client = gspread.authorize(creds)
    
    try:
        sh = client.open_by_key(sheet_id)
    except gspread.SpreadsheetNotFound:
        logger.error("Spreadsheet non trovato. Verifica GSPREAD_SHEET_ID e condivisione")
        raise
    
    ws = sh.worksheet(title)
    return ws

def gs_read_rows() -> Tuple[List[Dict[str, Any]], Dict[str, int], Any]:
    """Legge righe da Google Sheets"""
    ws = _gs_open()
    values = ws.get_all_values()
    if not values:
        return [], {}, ws
    
    header = values[0]
    col_index = {_norm_key(h): i+1 for i, h in enumerate(header)}
    
    rows = []
    for row_idx, row in enumerate(values[1:], start=2):
        m = {}
        for i, cell in enumerate(row):
            key = _norm_key(header[i]) if i < len(header) else f"col{i+1}"
            m[key] = cell
        m["_row_index"] = row_idx  # per write-back
        rows.append(m)
    
    logger.info("Caricate %d righe da Google Sheets", len(rows))
    return rows, col_index, ws

def gs_write_product_id(ws, row_index: int, col_index: Dict[str, int], product_gid: str) -> bool:
    """Scrive Product_Id su Google Sheets"""
    pid_col = col_index.get("product_id")
    if not pid_col:
        logger.warning("Colonna Product_Id non trovata")
        return False
    
    try:
        ws.update_cell(row_index, pid_col, product_gid)
        logger.info("Write-back Product_Id OK riga %d -> %s", row_index, product_gid)
        return True
    except Exception as e:
        logger.warning("Write-back fallito: %s", e)
        return False

# =============================================================================
# Shopify Client
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
        })
        
        self.min_interval = float(os.environ.get("SHOPIFY_MIN_INTERVAL_SEC", "0.7"))
        self.max_retries = int(os.environ.get("SHOPIFY_MAX_RETRIES", "5"))
        self._last_call_ts = 0.0
        self._location_cache = None

    def _throttle(self):
        """Rate limiting"""
        now = time.time()
        elapsed = now - self._last_call_ts
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

    def _request(self, method: str, path: str, **kw) -> requests.Response:
        """HTTP request con retry"""
        url = self.base + path
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            r = self.sess.request(method, url, **kw)
            self._last_call_ts = time.time()
            
            if r.status_code == 429:
                retry_after = float(r.headers.get("Retry-After", 1.0))
                logger.warning("429 Rate limit. Retry in %.2fs", retry_after)
                time.sleep(retry_after)
                continue
            
            if 500 <= r.status_code < 600:
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning("Server error %d. Retry in %ds", r.status_code, backoff)
                time.sleep(backoff)
                continue
            
            return r
        return r

    def _get(self, path: str, **kw):
        r = self._request("GET", path, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"GET {path} -> {r.status_code}")
        return r.json() if r.text else {}

    def _post(self, path: str, json=None, **kw):
        r = self._request("POST", path, json=json, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"POST {path} -> {r.status_code}")
        return r.json() if r.text else {}

    def _put(self, path: str, json=None, **kw):
        r = self._request("PUT", path, json=json, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"PUT {path} -> {r.status_code}")
        return r.json() if r.text else {}

    def _delete(self, path: str, **kw):
        r = self._request("DELETE", path, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"DELETE {path} -> {r.status_code}")
        return {}

    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """GraphQL request"""
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            r = self.sess.post(self.graphql_url, json={"query": query, "variables": variables})
            self._last_call_ts = time.time()
            
            if r.status_code == 429:
                retry_after = float(r.headers.get("Retry-After", 1.0))
                time.sleep(retry_after)
                continue
            
            if 500 <= r.status_code < 600:
                backoff = min(2 ** (attempt - 1), 8)
                time.sleep(backoff)
                continue
            
            if r.status_code >= 400:
                raise RuntimeError(f"GraphQL HTTP {r.status_code}")
            
            data = r.json()
            if "errors" in data:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        
        raise RuntimeError("GraphQL failed after retries")

    # --- Metodi specifici ---

    def find_product_by_sku_non_outlet(self, sku: str) -> Optional[Dict[str, Any]]:
        """Trova prodotto sorgente (non-outlet) by SKU"""
        q = f"sku:{sku}"
        data = self.graphql("""
        query($q: String!) {
          products(first: 10, query: $q) {
            edges { node { 
              id title handle status
              variants(first: 100) { 
                edges { node { 
                  id sku title 
                  selectedOptions { name value } 
                  inventoryItem { id } 
                }}
              }
            }}
          }
        }""", {"q": q})
        
        for edge in data["products"]["edges"]:
            p = edge["node"]
            # Skip prodotti outlet
            if p["handle"].endswith("-outlet"):
                continue
            # Verifica SKU nelle varianti
            for vedge in p["variants"]["edges"]:
                if (vedge["node"]["sku"] or "").strip() == sku:
                    return p
        return None

    def find_product_by_handle(self, handle: str) -> Optional[Dict[str, Any]]:
        """Trova prodotto by handle esatto"""
        q = f"handle:{handle}"
        data = self.graphql("""
        query($q: String!) {
          products(first: 5, query: $q) { 
            edges { node { id title handle status }}
          }
        }""", {"q": q})
        
        for edge in data["products"]["edges"]:
            if edge["node"]["handle"] == handle:
                return edge["node"]
        return None

    def find_outlet_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """
        Trova prodotto OUTLET esistente per SKU.
        Cerca prodotti con variante che ha questo SKU ed handle contenente 'outlet'.
        Esclude il prodotto sorgente (senza -outlet).
        """
        q = f"sku:{sku}"
        logger.info("🔍 Query GraphQL: '%s'", q)
        
        data = self.graphql("""
        query($q: String!) {
          products(first: 20, query: $q) {
            edges { node { 
              id title handle status
              variants(first: 5) {
                edges { node { sku }}
              }
            }}
          }
        }""", {"q": q})
        
        num_results = len(data["products"]["edges"])
        logger.info("📊 Ricerca outlet per SKU=%s: trovati %d prodotti totali", sku, num_results)
        
        if num_results == 0:
            logger.warning("⚠️ ATTENZIONE: GraphQL non ha trovato NESSUN prodotto con SKU=%s!", sku)
            logger.warning("⚠️ Verifica che il prodotto outlet esistente su Shopify abbia effettivamente SKU=%s", sku)
            return None
        
        # Cerca prodotto outlet (handle contiene "outlet") con questo SKU
        for i, edge in enumerate(data["products"]["edges"], 1):
            p = edge["node"]
            handle_lower = p["handle"].lower()
            
            # Log TUTTI i prodotti trovati
            logger.info("  [%d/%d] Prodotto trovato:", i, num_results)
            logger.info("      - ID: %s", p["id"])
            logger.info("      - Titolo: %s", p["title"])
            logger.info("      - Handle: %s", p["handle"])
            logger.info("      - Status: %s", p["status"])
            
            # Mostra SKU varianti
            variant_skus = [v["node"]["sku"] for v in p["variants"]["edges"]]
            logger.info("      - SKU varianti: %s", variant_skus)
            
            # FILTRO OUTLET: Handle O Titolo devono indicare che è un outlet
            # Alcuni outlet hanno titolo "... - Outlet" ma handle senza "outlet"!
            handle_has_outlet = "outlet" in handle_lower
            title_lower = p["title"].lower()
            title_has_outlet = "outlet" in title_lower or p["title"].endswith(" - Outlet")
            
            is_outlet = handle_has_outlet or title_has_outlet
            
            if not is_outlet:
                logger.info("      ❌ Scartato: né handle né titolo indicano 'outlet'")
                logger.info("         (handle='%s', titolo='%s')", p["handle"], p["title"])
                continue
            else:
                if handle_has_outlet:
                    logger.info("      ✓ Handle contiene 'outlet'")
                if title_has_outlet:
                    logger.info("      ✓ Titolo indica 'outlet' (termina con '- Outlet' o contiene 'outlet')")
            
            # Verifica che abbia effettivamente una variante con questo SKU
            has_sku = False
            for vedge in p["variants"]["edges"]:
                variant_sku = (vedge["node"]["sku"] or "").strip()
                if variant_sku == sku:
                    has_sku = True
                    logger.info("      ✓ Trovata variante con SKU match: '%s' == '%s'", variant_sku, sku)
                    break
                else:
                    logger.debug("      - Variante SKU '%s' != '%s'", variant_sku, sku)
            
            if not has_sku:
                logger.info("      ❌ Scartato: nessuna variante con SKU esatto '%s'", sku)
                continue
            
            # Trovato outlet!
            logger.info("🎯 MATCH! Trovato outlet esistente:")
            logger.info("   - SKU: %s", sku)
            logger.info("   - Handle: %s", p["handle"])
            logger.info("   - Status: %s", p["status"])
            logger.info("   - ID: %s", p["id"])
            return p
        
        logger.warning("❌ Nessun outlet trovato per SKU=%s dopo verifica %d prodotti", sku, num_results)
        return None

    def product_duplicate(self, source_gid: str, new_title: str) -> str:
        """Duplica prodotto (solo con newTitle, handle viene aggiornato dopo)"""
        data = self.graphql("""
        mutation($productId: ID!, $newTitle: String!) {
          productDuplicate(productId: $productId, newTitle: $newTitle) {
            newProduct { id title handle status }
            userErrors { field message }
          }
        }""", {"productId": source_gid, "newTitle": new_title})
        
        dup = data["productDuplicate"]
        if dup["userErrors"]:
            raise RuntimeError(f"productDuplicate errors: {dup['userErrors']}")
        
        return dup["newProduct"]["id"]

    def delete_product(self, product_gid: str):
        """Elimina prodotto"""
        num_id = _gid_numeric(product_gid)
        self._delete(f"/products/{num_id}.json")
        logger.info("Eliminato prodotto %s", product_gid)

    def update_product_basic(self, product_gid: str, handle: str, status: str, tags: str):
        """Aggiorna handle, status e tags"""
        num_id = _gid_numeric(product_gid)
        payload = {
            "product": {
                # FIX BUG #1: Rimosso campo "id" che causava errore 422
                "handle": handle,
                "status": status,
                "tags": tags
            }
        }
        self._put(f"/products/{num_id}.json", json=payload)

    def get_product_variants(self, product_gid: str) -> List[Dict[str, Any]]:
        """Ottiene varianti prodotto"""
        data = self.graphql("""
        query($id: ID!) {
          node(id: $id) {
            ... on Product {
              variants(first: 250) {
                edges { node { 
                  id sku title 
                  inventoryItem { id }
                  selectedOptions { name value }
                }}
              }
            }
          }
        }""", {"id": product_gid})
        
        edges = data["node"]["variants"]["edges"]
        return [e["node"] for e in edges]

    def variants_bulk_update_prices(self, product_gid: str, price: str, compare_at: Optional[str]):
        """Aggiorna prezzi su TUTTE le varianti"""
        variants = self.get_product_variants(product_gid)
        updates = []
        for v in variants:
            updates.append({
                "id": v["id"],
                "price": price,
                "compareAtPrice": compare_at
            })
        
        if not updates:
            return
        
        data = self.graphql("""
        mutation($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            product { id }
            userErrors { field message }
          }
        }""", {"productId": product_gid, "variants": updates})
        
        errs = data["productVariantsBulkUpdate"]["userErrors"]
        if errs:
            logger.warning("Errori update prezzi: %s", errs)

    def copy_images(self, source_gid: str, dest_gid: str):
        """Copia immagini mantenendo ordine"""
        source_num = _gid_numeric(source_gid)
        dest_num = _gid_numeric(dest_gid)
        
        # Get source images
        src_imgs = self._get(f"/products/{source_num}/images.json").get("images", [])
        src_imgs.sort(key=lambda x: x.get("position", 999))
        
        # Delete existing dest images
        dest_imgs = self._get(f"/products/{dest_num}/images.json").get("images", [])
        for img in dest_imgs:
            try:
                self._delete(f"/products/{dest_num}/images/{img['id']}.json")
            except Exception as e:
                logger.warning("Delete image failed: %s", e)
        
        # Add images in order
        for i, img in enumerate(src_imgs, 1):
            try:
                self._post(f"/products/{dest_num}/images.json", json={
                    "image": {
                        "src": img["src"],
                        "position": i,
                        "alt": ""  # Alt vuoto
                    }
                })
                time.sleep(0.15)  # Rate limit images
            except Exception as e:
                logger.warning("Add image failed: %s", e)

    def copy_metafields(self, source_gid: str, dest_gid: str):
        """Copia metafields"""
        data = self.graphql("""
        query($id: ID!) {
          node(id: $id) {
            ... on Product {
              metafields(first: 250) {
                edges { node { namespace key type value }}
              }
            }
          }
        }""", {"id": source_gid})
        
        mfs = [e["node"] for e in data["node"]["metafields"]["edges"]]
        if not mfs:
            return
        
        updates = [{"ownerId": dest_gid, **m} for m in mfs]
        
        # Batch update
        for i in range(0, len(updates), 20):
            chunk = updates[i:i+20]
            data = self.graphql("""
            mutation($metafields: [MetafieldsSetInput!]!) {
              metafieldsSet(metafields: $metafields) {
                metafields { id }
                userErrors { field message }
              }
            }""", {"metafields": chunk})

    def delete_collects(self, product_gid: str):
        """Elimina collections manuali"""
        num_id = _gid_numeric(product_gid)
        collects = self._get("/collects.json", params={"product_id": num_id}).get("collects", [])
        for c in collects:
            try:
                self._delete(f"/collects/{c['id']}.json")
            except Exception:
                pass

    def get_location_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Trova location by name"""
        if self._location_cache is None:
            locs = self._get("/locations.json").get("locations", [])
            self._location_cache = {loc["name"]: loc for loc in locs}
        return self._location_cache.get(name)

    def inventory_connect(self, inv_item_id: int, location_id: int):
        """Connette inventory item a location"""
        try:
            self._post("/inventory_levels/connect.json", json={
                "inventory_item_id": inv_item_id,
                "location_id": location_id
            })
        except Exception as e:
            # FIX BUG #6: Log solo se non è già connesso
            if "already exists" not in str(e).lower():
                logger.warning("inventory_connect failed item=%s loc=%s: %s", inv_item_id, location_id, e)

    def inventory_set(self, inv_item_id: int, location_id: int, qty: int):
        """Imposta quantità (con retry automatico se item non connesso)"""
        try:
            self._post("/inventory_levels/set.json", json={
                "inventory_item_id": inv_item_id,
                "location_id": location_id,
                "available": qty
            })
        except RuntimeError as e:
            # Se 422 = item non connesso, prova a connettere e riprova
            if "422" in str(e):
                logger.warning("Item %s non connesso a location %s (422), connetto e riprovo", 
                             inv_item_id, location_id)
                try:
                    self.inventory_connect(inv_item_id, location_id)
                    # Piccolo delay per propagazione
                    time.sleep(0.5)
                    # Riprova
                    self._post("/inventory_levels/set.json", json={
                        "inventory_item_id": inv_item_id,
                        "location_id": location_id,
                        "available": qty
                    })
                    logger.info("✓ Retry riuscito dopo connect")
                except Exception as retry_e:
                    logger.error("✗ Retry fallito: %s", retry_e)
                    raise
            else:
                raise

    def inventory_delete_level(self, inv_item_id: int, location_id: int):
        """Rimuove inventory level"""
        try:
            self._delete("/inventory_levels.json", params={
                "inventory_item_id": inv_item_id,
                "location_id": location_id
            })
        except Exception as e:
            logger.warning("inventory_delete_level failed item=%s loc=%s: %s", inv_item_id, location_id, e)

# =============================================================================
# Workflow principale
# =============================================================================

def process_sku_group(shop: Shopify, sku: str, rows: List[Dict[str, Any]], ws, col_index: Dict[str, int]) -> str:
    """
    Processa gruppo di righe con stesso SKU (più taglie).
    Crea UN SOLO outlet con inventory distribuito su tutte le taglie.
    """
    logger.info("=" * 60)
    logger.info("Processing SKU=%s con %d taglie", sku, len(rows))
    
    # Usa prima riga per dati comuni (titolo, handle, immagini, ecc.)
    first_row = rows[0]
    
    # 1. Estrai dati comuni dalla prima riga
    prezzo_pieno = _clean_price(first_row.get("prezzo_pieno"))
    prezzo_scontato = _clean_price(first_row.get("prezzo_scontato"))
    
    if not prezzo_scontato:
        prezzo_scontato = prezzo_pieno or "0.00"
    
    # 2. Trova prodotto sorgente
    source = shop.find_product_by_sku_non_outlet(sku)
    if not source:
        logger.warning("Prodotto sorgente non trovato per SKU=%s", sku)
        return "SKIP_NO_SOURCE"
    
    source_gid = source["id"]
    source_handle = source["handle"]
    source_title = source["title"]
    
    # 3. Prepara dati outlet
    outlet_handle = f"{source_handle}-outlet"
    
    if source_title.endswith(" - Outlet"):
        outlet_title = source_title
    else:
        outlet_title = f"{source_title} - Outlet"
    
    # MODIFICA CONCORDATA: Usa SKU per identificare outlet esistenti
    logger.info("Verifico esistenza outlet per SKU=%s...", sku)
    existing_outlet = shop.find_outlet_by_sku(sku)
    if existing_outlet:
        if existing_outlet["status"] == "ACTIVE":
            logger.info("✓ Outlet già attivo per SKU=%s: %s (handle: %s)", 
                       sku, existing_outlet["title"], existing_outlet["handle"])
            return "SKIP_ALREADY_ACTIVE"
        else:
            # Elimina draft
            logger.info("✗ Draft outlet trovato per SKU=%s, lo elimino: %s", sku, existing_outlet["id"])
            shop.delete_product(existing_outlet["id"])
    else:
        logger.info("✓ Nessun outlet esistente per SKU=%s, creo nuovo", sku)
    
    # 4. Duplica prodotto
    logger.info("Duplicazione: %s -> %s", source_title, outlet_title)
    outlet_gid = shop.product_duplicate(source_gid, outlet_title)
    
    # 5. Aggiorna handle, status, tags
    final_handle = outlet_handle
    suffix = 1
    while True:
        try:
            shop.update_product_basic(outlet_gid, final_handle, "active", "")
            break
        except RuntimeError as e:
            if "taken" in str(e).lower():
                final_handle = f"{outlet_handle}-{suffix}"
                suffix += 1
                if suffix > 10:
                    raise
            else:
                raise
    
    logger.info("Outlet creato: %s (handle: %s)", outlet_gid, final_handle)
    
    # 6. Copia immagini
    try:
        shop.copy_images(source_gid, outlet_gid)
        logger.info("Immagini copiate")
    except Exception as e:
        logger.warning("Errore copia immagini: %s", e)
    
    # 7. Copia metafields
    try:
        shop.copy_metafields(source_gid, outlet_gid)
        logger.info("Metafields copiati")
    except Exception as e:
        logger.warning("Errore copia metafields: %s", e)
    
    # 8. Elimina collections manuali
    try:
        shop.delete_collects(outlet_gid)
        logger.info("Collections pulite")
    except Exception as e:
        logger.warning("Errore pulizia collections: %s", e)
    
    # 9. Aggiorna prezzi su TUTTE le varianti (usa prezzi dalla prima riga)
    shop.variants_bulk_update_prices(outlet_gid, prezzo_scontato, prezzo_pieno)
    logger.info("Prezzi aggiornati: scontato=%s pieno=%s", prezzo_scontato, prezzo_pieno)
    
    # 10. Gestione inventario
    promo_name = os.environ.get("PROMO_LOCATION_NAME")
    mag_name = os.environ.get("MAGAZZINO_LOCATION_NAME")
    
    if promo_name:
        promo = shop.get_location_by_name(promo_name)
        if promo:
            variants = shop.get_product_variants(outlet_gid)
            
            # Reset tutte le varianti a 0 in Promo (PRIMA di impostare quantità specifiche)
            for v in variants:
                inv_id = int(_gid_numeric(v["inventoryItem"]["id"]))
                shop.inventory_connect(inv_id, promo["id"])
            
            # Piccolo delay per propagazione connessioni
            time.sleep(0.3)
            
            # Ora azzera tutte
            for v in variants:
                inv_id = int(_gid_numeric(v["inventoryItem"]["id"]))
                shop.inventory_set(inv_id, promo["id"], 0)
            
            # Imposta inventory per ogni taglia specifica dal Google Sheet
            logger.info("Gestisco %d taglie per outlet:", len(rows))
            for row in rows:
                taglia = (row.get("taglia") or "").strip()
                qta_str = row.get("qta") or row.get("qty") or "0"
                try:
                    qta = int(float(str(qta_str).replace(",", ".")))
                except:
                    qta = 0
                
                # Trova variante per questa taglia
                target_variant = None
                found = False
                
                for v in variants:
                    if (v.get("sku") or "").strip() != sku:
                        continue
                    
                    # Match per taglia se specificata
                    if taglia:
                        for opt in v.get("selectedOptions", []):
                            if opt["name"].lower() in ["size", "taglia"]:
                                if opt["value"].strip() == taglia:
                                    target_variant = v
                                    found = True
                                    break
                        if found:  # Esci anche dal loop esterno
                            break
                    else:
                        target_variant = v
                        break
                
                if target_variant:
                    inv_id = int(_gid_numeric(target_variant["inventoryItem"]["id"]))
                    shop.inventory_set(inv_id, promo["id"], qta)
                    logger.info("  ✓ Taglia %s: Qta=%d", taglia or "unica", qta)
                else:
                    logger.warning("  ✗ Variante non trovata per TAGLIA=%s", taglia)
    
    # FIX CRITICO: Gestione inventario Magazzino
    # Quando si duplica un prodotto, Shopify EREDITA gli inventory levels dal sorgente!
    # Quindi l'outlet parte già connesso a Magazzino con le stesse quantità del sorgente
    # SOLUZIONE: Prima AZZERARE tutto, POI disconnettere
    if mag_name:
        logger.info("Cerco location Magazzino con nome: '%s'", mag_name)
        mag = shop.get_location_by_name(mag_name)
        if mag:
            logger.info("Location Magazzino trovata: ID=%s Nome='%s'", mag["id"], mag["name"])
            variants = shop.get_product_variants(outlet_gid)
            disconnected = 0
            for v in variants:
                inv_id = int(_gid_numeric(v["inventoryItem"]["id"]))
                try:
                    # STEP 1: AZZERA la quantità (eredita stock dal sorgente!)
                    logger.debug("Azzerando stock Magazzino per item=%s", inv_id)
                    shop.inventory_set(inv_id, mag["id"], 0)
                    
                    # STEP 2: ORA disconnetti (funziona solo se stock = 0)
                    shop.inventory_delete_level(inv_id, mag["id"])
                    disconnected += 1
                    logger.debug("Disconnesso inventory item=%s da Magazzino", inv_id)
                except Exception as e:
                    # Se fallisce potrebbe essere già disconnesso o problema API
                    logger.warning("Errore gestione Magazzino item=%s: %s", inv_id, e)
            logger.info("Inventario Magazzino: %d varianti azzerate e disconnesse", disconnected)
        else:
            logger.error("⚠️ Location Magazzino NON TROVATA! Nome cercato: '%s'", mag_name)
            logger.error("⚠️ Verifica MAGAZZINO_LOCATION_NAME e che corrisponda ESATTAMENTE al nome su Shopify")
    else:
        logger.warning("⚠️ MAGAZZINO_LOCATION_NAME non settata - skip gestione Magazzino")
    
    # 11. Write-back Product_Id per TUTTE le righe del gruppo
    if ws:
        for row in rows:
            if "_row_index" in row:
                try:
                    gs_write_product_id(ws, row["_row_index"], col_index, outlet_gid)
                except Exception as e:
                    logger.warning("Write-back fallito per riga %d: %s", row["_row_index"], e)
    
    logger.info("✅ SKU=%s completato (%d taglie)", sku, len(rows))
    return "SUCCESS"

def main():
    parser = argparse.ArgumentParser(description="Workflow OUTLET")
    parser.add_argument("--apply", action="store_true", help="Esegue le operazioni")
    args = parser.parse_args()
    
    if not args.apply:
        logger.info("DRY-RUN MODE - Usa --apply per eseguire")
        return
    
    # Leggi dati da Google Sheets
    rows, col_index, ws = gs_read_rows()
    
    # Filtra righe
    selected = []
    for row in rows:
        # Check online=SI
        online = row.get("online", "")
        if not _truthy_si(online):
            continue
        
        # Check Qta>0
        qta_str = row.get("qta") or row.get("qty") or "0"
        try:
            qta = int(float(str(qta_str).replace(",", ".")))
        except:
            qta = 0
        
        if qta <= 0:
            continue
        
        selected.append(row)
    
    logger.info("Righe selezionate: %d/%d (online=SI e Qta>0)", len(selected), len(rows))
    
    if not selected:
        logger.info("Nessuna riga da processare")
        return
    
    # RAGGRUPPAMENTO PER SKU (gestione multi-taglia)
    grouped_by_sku = {}
    for row in selected:
        sku = (row.get("sku") or "").strip()
        if not sku:
            logger.warning("Riga %d: SKU mancante, skip", row.get("_row_index", "?"))
            continue
        
        if sku not in grouped_by_sku:
            grouped_by_sku[sku] = []
        grouped_by_sku[sku].append(row)
    
    logger.info("Prodotti unici (per SKU): %d", len(grouped_by_sku))
    logger.info("Taglie totali: %d", len(selected))
    
    # Log riepilogo raggruppamento
    for sku, sku_rows in grouped_by_sku.items():
        taglie = [r.get("taglia", "") for r in sku_rows]
        logger.info("  - SKU=%s: %d taglie %s", sku, len(sku_rows), taglie)
    
    # Inizializza Shopify
    shop = Shopify()
    
    # Processa per SKU (non per riga!)
    stats = {"success": 0, "skip_active": 0, "skip_source": 0, "errors": 0, "taglie_gestite": 0}
    
    for sku, sku_rows in grouped_by_sku.items():
        try:
            result = process_sku_group(shop, sku, sku_rows, ws, col_index)
            if result == "SUCCESS":
                stats["success"] += 1
                stats["taglie_gestite"] += len(sku_rows)
            elif result == "SKIP_ALREADY_ACTIVE":
                stats["skip_active"] += 1
            elif result == "SKIP_NO_SOURCE":
                stats["skip_source"] += 1
        except Exception as e:
            logger.error("Errore processando SKU=%s: %s", sku, e, exc_info=True)
            stats["errors"] += 1
    
    # Report finale
    logger.info("=" * 60)
    logger.info("RISULTATI FINALI:")
    logger.info("- Outlet creati: %d", stats["success"])
    logger.info("- Taglie totali gestite: %d", stats["taglie_gestite"])
    logger.info("- Skip (già attivi): %d", stats["skip_active"])
    logger.info("- Skip (no source): %d", stats["skip_source"])
    logger.info("- Errori: %d", stats["errors"])
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
