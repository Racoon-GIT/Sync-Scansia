import os
import sys
import argparse
import logging
from typing import Dict, Any, List, Tuple

from .utils import setup_logging, download_gsheet_csv, normalize_columns, filter_rows, build_outlet_title, build_outlet_handle, gid_to_id
from .shopify_client import ShopifyClient
from . import gsheets as gs

LOG = logging.getLogger("sync")


def ensure_unique_handle(client: ShopifyClient, product_id: str, title: str, desired_handle: str) -> str:
    # prova handle, se occupato incrementa -1, -2...
    base = desired_handle
    attempt = 0
    while True:
        try_handle = base if attempt == 0 else f"{base}-{attempt}"
        try:
            client.product_update(product_id, title=title, handle=try_handle, status="ACTIVE", tags=[])
            LOG.info("Impostato title/handle/status/tags su prodotto outlet %s (handle=%s)", product_id, try_handle)
            return try_handle
        except RuntimeError as e:
            msg = str(e)
            if "already in use" in msg or "Handle '" in msg:
                attempt += 1
                continue
            raise


def update_all_prices(client: ShopifyClient, product_id: str, price: float, compare_at: float) -> None:
    variants = client.get_product_variants(product_id)
    updates = []
    # price and compareAtPrice expect strings (Decimal) in GraphQL
    price_s = f"{price:.2f}" if price is not None else None
    compare_s = f"{compare_at:.2f}" if compare_at is not None else None
    for v in variants:
        updates.append({
            "id": v["id"],
            "price": price_s,
            "compareAtPrice": compare_s,
        })
    LOG.info("Aggiorno PREZZI su tutte le %d varianti di %s → price=%s compareAt=%s",
             len(variants), product_id, price_s, compare_s)
    LOG.debug("Payload prezzi: %s", [(u['id'], price, compare_at) for u in updates])
    client.product_variants_bulk_update(product_id, updates)


def sync_inventory(client: ShopifyClient, product_id: str, target_size: str, target_qty: int, promo_loc: Dict[str, Any], mag_loc: Dict[str, Any]):
    # Ricava inventory items
    variants = client.get_product_variants(product_id)
    promo_id = promo_loc["id"]
    mag_id = mag_loc["id"]
    # Primo: connetti a Promo tutte le varianti (se non connesse) e mettile a 0
    for v in variants:
        item_id = int(v["inventoryItem"]["id"].split("/")[-1])
        try:
            client.inventory_connect(item_id, promo_id)
        except Exception:
            pass
        client.inventory_set(item_id, promo_id, 0)

    # Imposta quantità corretta solo per la variante target (match su selectedOptions Size/taglia)
    for v in variants:
        size = None
        for opt in v.get("selectedOptions", []):
            if opt.get("name", "").lower() in ("size", "taglia"):
                size = opt.get("value")
                break
        if (size or "").strip() == (target_size or "").strip():
            item_id = int(v["inventoryItem"]["id"].split("/")[-1])
            client.inventory_set(item_id, promo_id, int(target_qty))
            break

    # Secondo: de-stocca tutte le varianti da Magazzino
    for v in variants:
        item_id = int(v["inventoryItem"]["id"].split("/")[-1])
        client.inventory_set(item_id, mag_id, 0)  # garantisce 0
        client.inventory_delete(item_id, mag_id)  # poi prova a disconnettere


def remove_collections(client: ShopifyClient, product_id: str):
    num_id = gid_to_id(product_id)
    collects = client.collects_for_product(num_id)
    for c in collects:
        client.delete_collect(c["id"])


def copy_media_and_alt(client: ShopifyClient, src_product_id: str, dst_product_id: str) -> int:
    # Copia immagini: alt vuoto, filename con 'Outlet' nel nome (nota: REST non consente rename filename reale, ma alt viene pulito)
    media = client.get_product_media(src_product_id)
    created = 0
    dst_num = gid_to_id(dst_product_id)
    for m in media:
        img = m.get("image") or {}
        src_url = img.get("url") or img.get("originalSrc")
        if not src_url:
            continue
        # piccola forzatura di filename aggiungendo query param per caching (non cambia filename sul CDN)
        client.product_image_create(dst_num, src_url, alt="")
        created += 1
    return created


def main():
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Esegue davvero le modifiche (di default dry-run)")
    args = parser.parse_args()
    dry_run = not args.apply

    store = os.getenv("SHOPIFY_STORE")
    token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2025-01")
    gsheet_url = os.getenv("GSHEET_URL")

    promo_name = os.getenv("PROMO_LOCATION_NAME", "Promo")
    mag_name = os.getenv("MAGAZZINO_LOCATION_NAME", "Magazzino")

    if not (store and token and gsheet_url):
        print("Devi configurare SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, GSHEET_URL", file=sys.stderr)
        sys.exit(2)

    LOG.info("Avvio sync | store=%s | api_version=%s | promo_location=%s | magazzino_location=%s | dry_run=%s",
             store, api_version, promo_name, mag_name, dry_run)
    LOG.info("Sorgente dati: %s", gsheet_url)

    # Carica dati
    df = download_gsheet_csv(gsheet_url)
    df = normalize_columns(df)
    ready = filter_rows(df)

    client = ShopifyClient(store, token, api_version)

    # Prepara map location
    locs = client.get_locations()
    promo_loc = next((l for l in locs if l.get("name") == promo_name), None)
    mag_loc = next((l for l in locs if l.get("name") == mag_name), None)
    if not promo_loc or not mag_loc:
        raise RuntimeError("Location Promo/Magazzino non trovate" )

    total = 0
    created = 0
    skipped = 0
    price_updates = 0
    inv_updates = 0
    media_updates = 0
    metafields_copied = 0
    write_back_items: List[Dict[str, Any]] = []

    LOG.info("Righe candidate dopo filtro: %d | con SKU validi: %d | SKU non trovati: %d",
             len(ready), ready['sku'].notna().sum(), 0)

    for _, row in ready.iterrows():
        total += 1
        sku = str(row.get("sku"))
        size = str(row.get("taglia"))
        qta = int(row.get("qta_norm", row.get("qta", 0)))
        price_full = row.get("prezzo pieno") or None
        price_sale = row.get("prezzo scontato") or None

        # Trova prodotto sorgente da SKU (prende una delle varianti)
        variants = client.find_variants_by_sku(sku)
        if not variants:
            LOG.warning("SKU non trovato su Shopify: %s", sku)
            continue
        src_variant = variants[0]
        src_product_id = src_variant["product"]["id"]
        src_title = src_variant["product"]["title"]
        src_handle = src_variant["product"]["handle"]

        outlet_title = build_outlet_title(src_title)
        # Skip se esiste già attivo
        active = client.products_search_by_title_active(outlet_title)
        if active:
            skipped += 1
            continue

        # Se esistono DRAFT con stesso titolo, cancellali
        any_state = client.products_search_by_title_any(outlet_title)
        for p in any_state:
            if p.get("status") == "DRAFT" or p.get("status") == "ARCHIVED":
                pid_num = gid_to_id(p["id"])
                LOG.info("Elimino OUTLET DRAFT pre-esistente prima della duplicazione → %s (%s)", p["id"], p["handle"])
                if not dry_run:
                    client.product_delete_rest(pid_num)

        # Duplica
        if dry_run:
            LOG.info("[dry-run] Duplicazione %s → %s", src_product_id, outlet_title)
            new_pid = src_product_id  # placeholder per simulare
        else:
            dup = client.product_duplicate(src_product_id, outlet_title)
            new_pid = dup["id"]

        # Imposta title/handle e pulisci tag
        outlet_handle = build_outlet_handle(src_handle)
        if not dry_run:
            try_handle = ensure_unique_handle(client, new_pid, outlet_title, outlet_handle)
        else:
            try_handle = outlet_handle
            LOG.info("[dry-run] Imposterei handle=%s", try_handle)

        # Rimuovi da collezioni (come da log operativi approvati)
        if not dry_run:
            remove_collections(client, new_pid)

        # Copia media (alt vuoto)
        if not dry_run:
            created_media = copy_media_and_alt(client, src_product_id, new_pid)
            media_updates += created_media

        # Copia metafields
        if not dry_run:
            meta = client.get_product_metafields(src_product_id)
            if meta:
                client.metafields_set(new_pid, meta)
                metafields_copied += len(meta)

        # Prezzi su tutte le varianti
        if not dry_run:
            p = price_sale if price_sale is not None else price_full
            c = price_full if price_full is not None else None
            if p is not None:
                update_all_prices(client, new_pid, float(p), float(c) if c is not None else None)
                price_updates += 1

        # Inventario: prima Promo poi de-stocca Magazzino
        if not dry_run:
            sync_inventory(client, new_pid, size, qta, promo_loc, mag_loc)
            inv_updates += 1

        # Write-back Product_Id
        if not dry_run:
            write_back_items.append({"sku": sku, "taglia": size, "product_id": gid_to_id(new_pid)})

        created += 1

    # Write-back su GSheet
    if write_back_items:
        updated = gs.write_back_product_ids(gsheet_url, write_back_items)
        LOG.info("Write-back Product_id completato: %d righe aggiornate.", updated)

    LOG.info("FINITO | prodotti analizzati=%d | creati=%d | già esistenti (skip)=%d | updates_prezzi=%d | updates_inventario=%d | updates_media=%d | metafield_copiati=%d",
             total, created, skipped, price_updates, inv_updates, media_updates, metafields_copied)


if __name__ == "__main__":
    main()
