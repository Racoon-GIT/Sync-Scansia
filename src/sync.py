# -*- coding: utf-8 -*-
"""
sync.py — Esegui con:
    python -m src.sync --apply
Env richieste:
  SHOPIFY_ADMIN_TOKEN
  SHOPIFY_STORE                es: racoon-lab.myshopify.com
  SHOPIFY_API_VERSION          es: 2025-01
  PROMO_LOCATION_NAME          es: Promo  (per inventario)
  DRY_RUN                      "true"/"false" (opzionale; --apply ha priorità)

Agisce così:
- Se product_id è presente -> PUT /products/{id}.json status=active
- Se product_id assente -> POST /products.json con 1 variante (sku, taglia, price) + set inventario sulla location
"""

import argparse
import logging
import os
import re
from collections import Counter, defaultdict
from typing import Dict, List, Any, Tuple, Optional

import requests

from .gsheets import load_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("sync")

# --------------------- util --------------------------------------------------

def _is_selected(v: Any) -> bool:
    if v is True: return True
    try:
        if isinstance(v, (int, float)) and int(v) == 1: return True
    except Exception:
        pass
    if isinstance(v, str):
        return v.strip().lower() in {"1","true","yes","si","sì","x","ok"}
    return False

def _make_key(row: Dict[str, Any]) -> str:
    pid = (row.get("product_id") or "").strip()
    if pid: return pid
    sku = (row.get("sku") or "").strip()
    taglia = (row.get("taglia") or "").strip()
    if sku and taglia: return f"{sku}::{taglia}"
    return sku

def _group_updates(selected_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    bucket: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in selected_rows:
        bucket[_make_key(r)].append(r)
    return bucket

def _price_to_str_num(v: Any) -> Optional[str]:
    """
    Converte '€ 129', '129€', '129,90', '129.90' in '129.90' (string).
    Ritorna None se non interpretabile.
    """
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    # prendi numeri, virgole e punti
    s2 = re.sub(r"[^\d,\.]", "", s)
    if s2.count(",") == 1 and s2.count(".") == 0:
        s2 = s2.replace(",", ".")
    # se ci sono migliaia tipo 1.299,90 -> togli i separatori migliaia
    if s2.count(".") > 1:
        s2 = s2.replace(".", "")
    if s2 == "": return None
    try:
        f = float(s2)
        return f"{f:.2f}"
    except Exception:
        return None

def _gid_to_numeric(gid: str) -> Optional[str]:
    # gid://shopify/Product/123456789 -> 123456789
    if not gid: return None
    return gid.strip().split("/")[-1]

# --------------------- Shopify client ---------------------------------------

class Shopify:
    def __init__(self):
        self.store = os.environ["SHOPIFY_STORE"]
        self.token = os.environ["SHOPIFY_ADMIN_TOKEN"]
        self.api_version = os.environ.get("SHOPIFY_API_VERSION", "2025-01")
        self.base = f"https://{self.store}/admin/api/{self.api_version}"
        self.sess = requests.Session()
        self.sess.headers.update({
            "X-Shopify-Access-Token": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self._location_cache: Optional[Dict[str, Any]] = None

    def _get(self, path: str, **kw):
        r = self.sess.get(self.base + path, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"GET {path} -> {r.status_code} {r.text}")
        return r.json()

    def _post(self, path: str, json: Dict[str, Any], **kw):
        r = self.sess.post(self.base + path, json=json, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"POST {path} -> {r.status_code} {r.text}")
        return r.json()

    def _put(self, path: str, json: Dict[str, Any], **kw):
        r = self.sess.put(self.base + path, json=json, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"PUT {path} -> {r.status_code} {r.text}")
        return r.json()

    # ---- operations

    def ensure_active(self, product_id_numeric: str) -> None:
        payload = {"product": {"id": int(product_id_numeric), "status": "active"}}
        self._put(f"/products/{product_id_numeric}.json", json=payload)

    def get_location_by_name(self, name: str) -> Dict[str, Any]:
        if self._location_cache is None:
            data = self._get("/locations.json")
            self._location_cache = {loc["name"]: loc for loc in data.get("locations", [])}
        if name in self._location_cache:
            return self._location_cache[name]
        # fallback: prima location disponibile
        if self._location_cache:
            return list(self._location_cache.values())[0]
        raise RuntimeError("Nessuna location Shopify disponibile per l'inventario.")

    def create_product_with_one_variant(self, row: Dict[str, Any]) -> Dict[str, Any]:
        titolo = (row.get("titolo") or row.get("title") or row.get("sku") or "Untitled").strip()
        vendor = (row.get("brand") or "").strip()
        sku = (row.get("sku") or "").strip()
        taglia = (row.get("taglia") or "").strip()

        price = _price_to_str_num(row.get("prezzo_scontato")) or _price_to_str_num(row.get("prezzo_pieno")) or "0.00"

        product = {
            "product": {
                "title": titolo,
                "vendor": vendor or None,
                "status": "active",
                "options": [{"name": "Size"}] if taglia else None,
                "variants": [{
                    "sku": sku or None,
                    "option1": taglia or None,
                    "price": price,
                    "inventory_management": "shopify",
                }],
            }
        }
        # pulisci None ricorrenti
        def _purge_none(obj):
            if isinstance(obj, dict):
                return {k: _purge_none(v) for k, v in obj.items() if v is not None}
            if isinstance(obj, list):
                return [_purge_none(x) for x in obj if x is not None]
            return obj
        product = _purge_none(product)

        created = self._post("/products.json", json=product)
        return created["product"]

    def set_inventory(self, variant: Dict[str, Any], location_name: str, qty: int) -> None:
        inv_item_id = variant["inventory_item_id"]
        loc = self.get_location_by_name(location_name)
        payload = {
            "location_id": loc["id"],
            "inventory_item_id": inv_item_id,
            "available": int(qty),
        }
        self._post("/inventory_levels/set.json", json=payload)

# --------------------- apply -------------------------------------------------

def _apply_updates(grouped: Dict[str, List[Dict[str, Any]]], do_apply: bool) -> Tuple[int, List[str]]:
    """
    Applica 1 azione per RIGA:
      - con product_id -> attiva prodotto
      - senza product_id -> crea prodotto + set inventario
    """
    applied = 0
    keys_done: List[str] = []

    # DRY_RUN dall'env (solo se non si è passato --apply)
    if not do_apply:
        dry = os.environ.get("DRY_RUN", "").strip().lower() == "true"
        if dry:
            logger.info("DRY-RUN attivo: nessuna chiamata a Shopify verrà eseguita.")
            return 0, []

    shop = Shopify()
    promo_loc = os.environ.get("PROMO_LOCATION_NAME", "").strip() or None

    for key, rows in grouped.items():
        logger.debug("Processing chiave %s (righe: %d)", key, len(rows))
        for r in rows:
            pid_gid = (r.get("product_id") or "").strip()
            if pid_gid:
                pid = _gid_to_numeric(pid_gid)
                if not pid:
                    logger.warning("product_id non interpretabile: %s", pid_gid)
                    continue
                shop.ensure_active(pid)
                applied += 1
            else:
                # crea prodotto
                product = shop.create_product_with_one_variant(r)
                applied += 1
                # imposta inventario se possibile
                try:
                    variant = product["variants"][0]
                    qty = r.get("qta") or r.get("qty") or 0
                    try:
                        qty = int(float(str(qty).replace(",", ".")))
                    except Exception:
                        qty = 0
                    if promo_loc is not None:
                        shop.set_inventory(variant, promo_loc, qty)
                except Exception as e:
                    logger.warning("Impossibile impostare inventario per SKU %s: %s", r.get("sku"), e)
        keys_done.append(key)

    return applied, keys_done

# --------------------- driver -----------------------------------------------

def run_sync(rows: List[Dict[str, Any]], do_apply: bool) -> None:
    selected = [r for r in rows if _is_selected(r.get("online"))]
    logger.info("Righe totali: %d, selezionate (online=TRUE): %d", len(rows), len(selected))

    keys = [_make_key(r) for r in selected]
    cnt = Counter(keys)
    logger.info("Chiavi uniche tra i selezionati: %d", len(cnt))
    if cnt:
        from itertools import islice
        logger.debug("Esempi chiavi (max 5): %s", list(islice(cnt.keys(), 5)))
    missing_key = sum(1 for k in keys if not k)
    if missing_key:
        logger.warning("Righe selezionate SENZA chiave: %d (product_id/sku/taglia vuoti)", missing_key)

    grouped = _group_updates(selected)
    applied_count, keys_done = _apply_updates(grouped, do_apply=do_apply)
    logger.info("APPLY: applicazione di %d aggiornamenti.", applied_count)
    if applied_count and len(keys_done) <= 10:
        logger.debug("Chiavi processate: %s", keys_done)

    logger.info("Riepilogo: selezionate=%d, chiavi_uniche=%d, applicazioni=%d",
                len(selected), len(cnt), applied_count)

def main() -> None:
    parser = argparse.ArgumentParser(description="Sincronizzazione Scarpe in Scansia")
    parser.add_argument("--apply", action="store_true", help="Esegue davvero le operazioni su Shopify")
    args = parser.parse_args()

    logger.info("Avvio sync")
    logger.info("apply=%s", args.apply)

    rows = load_rows()  # legge da env GSPREAD_SHEET_ID / GSPREAD_WORKSHEET_TITLE via gsheets.py
    run_sync(rows, do_apply=args.apply)
    logger.info("Termine sync con exit code 0")

if __name__ == "__main__":
    main()
