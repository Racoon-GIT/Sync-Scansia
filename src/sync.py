# src/sync.py
import os
import time
import logging
from typing import List, Optional, Tuple

import pandas as pd

from .shopify_client import ShopifyClient
from . import utils
from . import gsheets

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("sync")

STORE = os.getenv("SHOPIFY_STORE", "")
TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")
GSHEET_URL = os.getenv("GSHEET_URL", "")
PROMO_LOCATION_NAME = os.getenv("PROMO_LOCATION_NAME", "Promo")
MAGAZZINO_LOCATION_NAME = os.getenv("MAGAZZINO_LOCATION_NAME", "Magazzino")
RENAME_OUTLET_SUFFIX_TITLE = " - Outlet"
RENAME_OUTLET_SUFFIX_HANDLE = "-outlet"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes")
DELETE_EXISTING_DRAFT = os.getenv("DELETE_EXISTING_DRAFT", "true").lower() in ("1","true","yes")


def ensure_unique_handle(client: ShopifyClient, product_id: str, desired_title: str, desired_handle: str) -> str:
    base = desired_handle
    for i in range(0, 50):
        try_handle = base if i == 0 else f"{base}-{i}"
        try:
            prod = client.product_update(product_id, title=desired_title, handle=try_handle, status="ACTIVE", tags=[])
            log.info("Impostato title/handle/status/tags su prodotto outlet %s (handle=%s)",
                     product_id, prod["handle"])
            return prod["handle"]
        except RuntimeError as e:
            msg = str(e)
            if "Handle" in msg and "already in use" in msg:
                continue
            raise
    raise RuntimeError("Impossibile trovare un handle libero")


def find_location_id(client: ShopifyClient, name: str) -> Optional[int]:
    locs = client.get_locations()
    for l in locs:
        if l.get("name") == name:
            return int(l["id"])
    return None


def prepare_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = utils.normalize_columns(df)
    df["online_raw"] = df["online"].astype(str)
    df["online"] = df["online"].astype(str).str.strip().str.upper().eq("SI")
    df["Qta_raw"] = df["qta"]
    df["qta"] = pd.to_numeric(df["qta"], errors="coerce").fillna(0).astype(int)

    initial = len(df)
    df1 = df[df["online"]]
    log.info("Dopo filtro online==SI: %d (scartate: %d)", len(df1), initial - len(df1))

    df2 = df1[df1["qta"] > 0]
    log.info("Dopo filtro Qta>0 (tra quelle online): %d (scartate per Qta<=0: %d)", len(df2), len(df1) - len(df2))

    if log.isEnabledFor(logging.DEBUG):
        log.debug("Esempi scartati per online!=SI:")
        for _, r in df[~df["online"]].head(10).iterrows():
            log.debug("- SKU=%s Size=%s online_raw=%s", r["sku"], r["taglia"], r["online_raw"])
        log.debug("Esempi scartati per Qta<=0:")
        for _, r in df1[df1["qta"] <= 0].head(10).iterrows():
            log.debug("- SKU=%s Size=%s Qta_raw=%s Qta=%s", r["sku"], r["taglia"], r["Qta_raw"], r["qta"])

    df2["prezzo_pieno"] = df2["prezzo pieno"].apply(utils.parse_price)
    df2["prezzo_scontato"] = df2["prezzo scontato"].apply(utils.parse_price)

    if "product_id" not in df2.columns:
        df2["product_id"] = ""

    log.info("Totale righe pronte all'elaborazione: %d", len(df2))
    return df2


def build_outlet_title(base_title: str) -> str:
    if base_title.endswith(RENAME_OUTLET_SUFFIX_TITLE):
        return base_title
    return f"{base_title}{RENAME_OUTLET_SUFFIX_TITLE}"


def build_outlet_handle_from_title(base_title: str) -> str:
    handle = utils.slugify_handle(base_title)
    if not handle.endswith(RENAME_OUTLET_SUFFIX_HANDLE):
        handle = f"{handle}{RENAME_OUTLET_SUFFIX_HANDLE}"
    return handle


def copy_images_with_rename(client: ShopifyClient, src_pid: str, dst_pid: str, outlet_handle: str) -> int:
    media = client.get_product_media(src_pid)
    img_nodes = [m for m in media if m["mediaContentType"] == "IMAGE" and m.get("image")]
    if not img_nodes:
        log.debug("Nessuna immagine da copiare.")
        return 0

    created = 0
    for idx, m in enumerate(img_nodes, start=1):
        url = m["image"]["url"]
        desired = f"{outlet_handle}-{idx}"
        client.product_create_media_from_urls(dst_pid, [(url, desired)])
        time.sleep(0.2)
        created += 1

    log.info("Copiate %d immagini sul prodotto outlet.", created)
    return created


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="esegue davvero (di default dry-run)")
    args = ap.parse_args()

    dry = DRY_RUN if not args.apply else False

    if not STORE or not TOKEN or not GSHEET_URL:
        raise SystemExit("SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, GSHEET_URL sono obbligatorie")

    client = ShopifyClient(STORE, TOKEN, API_VERSION)

    promo_loc_id = find_location_id(client, PROMO_LOCATION_NAME)
    mag_loc_id = find_location_id(client, MAGAZZINO_LOCATION_NAME)
    log.debug("get_locations() → %s", len(client.get_locations()))
    if promo_loc_id is None:
        raise SystemExit(f"Location '{PROMO_LOCATION_NAME}' non trovata.")
    if mag_loc_id is None:
        log.warning("Location '%s' non trovata: salto de-stoccaggio.", MAGAZZINO_LOCATION_NAME)

    log.info("Sorgente dati: %s", GSHEET_URL)
    df_raw = gsheets.load_table(GSHEET_URL)
    df = prepare_rows(df_raw)

    prodotti_creati = 0
    updates_prezzi = 0
    updates_inventario = 0
    updates_media = 0
    metafield_copiati = 0

    log.info("Processing %d prodotti", len(df))
    rows_for_writeback = []

    for _, r in df.iterrows():
        sku = str(r["sku"]).strip()
        size = str(r["taglia"]).strip()
        qty  = int(r["qta"])
        titolo_base = str(r["titolo"]).strip()
        outlet_title = build_outlet_title(titolo_base)
        outlet_handle = build_outlet_handle_from_title(outlet_title)

        # Prodotto sorgente (già online) da duplicare
        src_candidates = client.products_search_by_title_any(titolo_base)
        if not src_candidates:
            log.warning("Titolo sorgente non trovato su Shopify: %r (SKU=%s)", titolo_base, sku)
            continue
        src_prod = src_candidates[0]
        src_pid = src_prod["id"]

        # Se esiste già un OUTLET attivo, saltiamo
        if client.products_search_by_title_active(outlet_title):
            log.info("OUTLET attivo già esistente per %r → skip", outlet_title)
            continue

        # Se esiste un OUTLET in draft ed è abilitata la pulizia, eliminiamolo e ripartiamo puliti
        if DELETE_EXISTING_DRAFT:
            any_candidates = client.products_search_by_title_any(outlet_title)
            for c in any_candidates:
                if c["status"] == "DRAFT":
                    numeric_id = int(c["id"].split("/")[-1])
                    log.info("Elimino OUTLET DRAFT pre-esistente prima della duplicazione → %s (%s)",
                             c["id"], c["handle"])
                    client.product_delete_rest(numeric_id)

        if dry:
            log.info("[dry-run] Duplicazione di %r", outlet_title)
            continue

        # Duplica
        dup = client.product_duplicate(src_pid, outlet_title)
        new_pid = dup["id"]
        new_numeric_id = int(new_pid.split("/")[-1])

        # Assicura handle univoco + setta anche status ACTIVE e svuota tags
        ensure_unique_handle(client, new_pid, outlet_title, outlet_handle)

        # Copia immagini, rinominando filename e azzerando alt
        updates_media += copy_images_with_rename(client, src_pid, new_pid, outlet_handle)

        # Copia metafield
        src_mf = client.get_product_metafields(src_pid)
        if src_mf:
            client.set_product_metafields(new_pid, src_mf)
            metafield_copiati += len(src_mf)

        # Prezzi su TUTTE le varianti
        variants = client.get_product_variants(new_pid)
        price = float(r["prezzo_scontato"])
        compare_at = float(r["prezzo_pieno"]) if float(r["prezzo_pieno"]) > 0 else None
        updates_payload = [(v["id"], price, compare_at) for v in variants]
        if updates_payload:
            log.info("Aggiorno PREZZI su tutte le %d varianti di %s → price=%.2f compareAt=%s",
                     len(updates_payload), new_pid, price, compare_at if compare_at is not None else "None")
            client.product_variants_bulk_update(new_pid, updates_payload)
            updates_prezzi += len(updates_payload)

        # INVENTARIO: prima assegno tutte le varianti alla location Promo con qty 0
        for v in variants:
            inv_item_id = int(v["inventoryItem"]["id"].split("/")[-1])
            client.inventory_connect(inv_item_id, promo_loc_id)
            client.inventory_set(inv_item_id, promo_loc_id, 0)

        # poi imposto la variante corretta con la quantità indicata
        target_variant = None
        for v in variants:
            if size and size in v["title"]:
                target_variant = v
                break
        if target_variant is None and variants:
            target_variant = variants[0]
        if target_variant:
            inv_item_id = int(target_variant["inventoryItem"]["id"].split("/")[-1])
            client.inventory_set(inv_item_id, promo_loc_id, int(qty))

        # infine de-stocco tutte le varianti dalla location Magazzino
        if mag_loc_id is not None:
            for v in variants:
                inv_item_id = int(v["inventoryItem"]["id"].split("/")[-1])
                client.inventory_set(inv_item_id, mag_loc_id, 0)
                client.inventory_delete(inv_item_id, mag_loc_id)

        prodotti_creati += 1

        # Write-back per Product_Id
        this_row = r.copy()
        this_row["product_id"] = str(new_numeric_id)
        rows_for_writeback.append(this_row)

        # Piccolo spacing anti-rate-limit
        time.sleep(0.35)

    # GSheet write-back (se configurato)
    if rows_for_writeback:
        df_wb = pd.DataFrame(rows_for_writeback)
        try:
            gsheets.writeback_product_id(GSHEET_URL, df_raw, df_wb,
                                         key_cols=("SKU","TAGLIA"), product_id_col="product_id")
        except Exception as e:
            log.info("Write-back GSheet saltato: %s", e)

    log.info("FINITO | prodotti analizzati=%d | creati=%d | già esistenti (skip)=%d | updates_prezzi=%d | updates_inventario=%d | updates_media=%d | metafield_copiati=%d",
             len(df), prodotti_creati, len(df) - prodotti_creati, updates_prezzi, updates_inventario, updates_media, metafield_copiati)


if __name__ == "__main__":
    main()
