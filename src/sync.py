import os, argparse, math, logging
from dotenv import load_dotenv

from .utils import read_table_from_source, parse_scansia, build_key
from .shopify_client import ShopifyClient
from .gsheets import write_product_ids

def _init_logging():
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, level, logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

_init_logging()
logger = logging.getLogger("sync")
load_dotenv()

def make_outlet_title_handle(title: str, handle: str):
    new_title = f"{title} - Outlet" if not title.lower().endswith(" - outlet") else title.replace(" - outlet"," - Outlet")
    base_handle = f"{handle}-outlet" if not handle.endswith("-outlet") else handle
    return new_title, base_handle

def ensure_unique_handle(client: ShopifyClient, product_id: str, title: str, base_handle: str):
    attempt = 0
    while attempt < 10:
        try_handle = base_handle if attempt == 0 else f"{base_handle}-{attempt+1}"
        try:
            client.product_update(product_id, title=title, handle=try_handle, status="ACTIVE", tags=[])
            logger.info(f"Impostato title/handle/status/tags su prodotto outlet {product_id} (handle={try_handle})")
            return try_handle
        except RuntimeError as e:
            msg = str(e).lower()
            if "handle" in msg and "taken" in msg:
                logger.warning(f"Handle '{try_handle}' già in uso. Riprovo con suffisso...")
                attempt += 1
                continue
            logger.error(f"Errore product_update: {e}")
            raise
    logger.warning("Tutti i tentativi handle falliti; aggiorno solo titolo+status+tags senza handle.")
    client.product_update(product_id, title=title, status="ACTIVE", tags=[])
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", dest="file", help="Percorso locale (xlsx/csv)")
    ap.add_argument("--url", dest="url", help="URL Google Sheet/CSV/XLSX (pubblico)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    store = os.environ.get("SHOPIFY_STORE")
    token = os.environ.get("SHOPIFY_ADMIN_TOKEN")
    api_version = os.environ.get("SHOPIFY_API_VERSION","2025-01")
    promo_location_name = os.environ.get("PROMO_LOCATION_NAME","Promo")
    magazzino_location_name = os.environ.get("MAGAZZINO_LOCATION_NAME","Magazzino")
    env_url = os.environ.get("SCANSIA_URL")
    env_file = os.environ.get("SCANSIA_FILE")
    sample_rows = int(os.environ.get("LOG_SAMPLE_ROWS", "10"))
    ws_name = os.environ.get("GDRIVE_WORKSHEET_NAME") or None

    if not (store and token):
        raise SystemExit("Config mancante: SHOPIFY_STORE/SHOPIFY_ADMIN_TOKEN")

    env_dry = str(os.environ.get("DRY_RUN", "true")).strip().lower() in {"1","true","yes","y","si","sì","on","x"}
    dry_run = True if args.dry_run else (False if args.apply else env_dry)

    logger.info(f"Avvio sync | store={store} | api_version={api_version} | promo_location={promo_location_name} | magazzino_location={magazzino_location_name} | dry_run={dry_run}")

    client = ShopifyClient(store, token, api_version)
    src = args.url or env_url or args.file or env_file
    if not src:
        raise SystemExit("Specifica --url, --file o SCANSIA_URL/SCANSIA_FILE")

    logger.info(f"Sorgente dati: {src}")
    raw_df = read_table_from_source(src)
    df = parse_scansia(raw_df, sample_rows=sample_rows)

    rows = []
    skipped_sku = 0
    for _, r in df.iterrows():
        sku = r["SKU"]
        size = r["Size"]
        qta = int(r["Qta"])
        p_full = r.get("Prezzo Pieno", None)
        p_sale = r.get("Prezzo Scontato", None)

        variants = client.find_variants_by_sku(sku)
        if not variants:
            logger.warning(f"[SKIP] SKU non trovato su Shopify: {sku}")
            skipped_sku += 1
            continue
        v = variants[0]
        product = v["product"]
        rows.append({
            "product_id": product["id"],
            "product_title": product["title"],
            "product_handle": product["handle"],
            "sku": sku,
            "size": size,
            "qta": qta,
            "price_full": (None if (isinstance(p_full, float) and math.isnan(p_full)) else p_full),
            "price_sale": (None if (isinstance(p_sale, float) and math.isnan(p_sale)) else p_sale),
            "key": f"{sku}{size}",
        })

    logger.info(f"Righe candidate dopo filtro: {len(df)} | con SKU validi: {len(rows)} | SKU non trovati: {skipped_sku}")

    from collections import defaultdict
    by_product = defaultdict(list)
    for it in rows:
        by_product[it["product_id"]].append(it)

    locations = client.get_locations()
    promo = next((loc for loc in locations if loc["name"].strip().lower() == promo_location_name.strip().lower()), None)
    if not promo:
        raise SystemExit(f"Location '{promo_location_name}' non trovata su Shopify.")
    promo_id = promo["id"]
    magazzino = next((loc for loc in locations if loc["name"].strip().lower() == magazzino_location_name.strip().lower()), None)
    magazzino_id = magazzino["id"] if magazzino else None

    logger.info(f"Processing {len(by_product)} prodotti")

    processed = created = skipped_existing = price_updates = inv_updates = media_updates = metafield_copied = 0
    writebacks = []

    for pid, items in by_product.items():
        processed += 1
        base_title = items[0]["product_title"]
        base_handle = items[0]["product_handle"]
        outlet_title = f"{base_title} - Outlet" if not base_title.lower().endswith(" - outlet") else base_title.replace(" - outlet"," - Outlet")
        outlet_handle = f"{base_handle}-outlet" if not base_handle.endswith("-outlet") else base_handle

        existing = client.products_search_by_title_active(outlet_title)
        if existing:
            skipped_existing += 1
            logger.info(f"[SKIP] Esiste già OUTLET ACTIVE per '{base_title}' → {existing[0]['id']}")
            continue

        if dry_run:
            new_pid = "gid://shopify/Product/DRYRUN"
            logger.info(f"[DRY-RUN] Duplicazione di {pid} → {new_pid} con titolo '{outlet_title}'")
        else:
            new_pid = client.product_duplicate(pid, outlet_title)
            if not new_pid:
                logger.error(f"[ERR] productDuplicate fallita per {pid}")
                continue
            created += 1

        if not dry_run:
            try:
                client.product_update(new_pid, title=outlet_title, handle=outlet_handle, status="ACTIVE", tags=[])
                logger.info(f"Impostato title/handle/status/tags su prodotto outlet {new_pid} (handle={outlet_handle})")
            except Exception as e:
                logger.warning(f"product_update handle fallita: {e}")

        if not dry_run:
            new_media = client.product_images_list(new_pid)
            if not new_media:
                src_media_nodes = client.get_product_media(pid)
                src_urls = [m["image"]["originalSrc"] for m in src_media_nodes
                            if m.get("__typename") == "MediaImage" and m.get("image") and m["image"].get("originalSrc")]
                pos = 1
                for url in src_urls:
                    try:
                        client.product_image_create(new_pid, url, position=pos, alt="")
                        pos += 1
                        media_updates += 1
                    except Exception as e:
                        logger.warning(f"copy image fallita url={url}: {e}")
            new_media = client.product_images_list(new_pid)
            for im in new_media:
                if im.get("alt"):
                    try:
                        client.product_image_update(new_pid, im["id"], alt="")
                    except Exception as e:
                        logger.warning(f"alt reset fallito image_id={im.get('id')}: {e}")

        if not dry_run:
            try:
                collects = client.collects_for_product(new_pid)
                for c in collects:
                    try:
                        client.delete_collect(c["id"])
                    except Exception as e:
                        logger.warning(f"delete_collect {c.get('id')} fallito: {e}")
            except Exception as e:
                logger.warning(f"collects_for_product fallito: {e}")

        outlet_variants = [] if dry_run else client.get_product_variants(new_pid)
        if not dry_run:
            for node in outlet_variants:
                inv_item = node["inventoryItem"]["id"].split("/")[-1]
                try:
                    client.inventory_connect(inv_item, promo_id)
                except Exception:
                    pass
                try:
                    client.inventory_set(inv_item, promo_id, 0)
                except Exception as e:
                    logger.warning(f"inventory_set@Promo=0 fallita item={inv_item}: {e}")

            for it in items:
                node = next((n for n in outlet_variants if any((opt.get("name","").lower() in ("size","taglia") and opt.get("value","")==it["size"]) for opt in (n.get("selectedOptions") or []))), None)
                if not node:
                    continue
                inv_item = node["inventoryItem"]["id"].split("/")[-1]
                try:
                    client.inventory_set(inv_item, promo_id, int(it["qta"]))
                    inv_updates += 1
                except Exception as e:
                    logger.warning(f"inventory_set@Promo={int(it['qta'])} fallita item={inv_item}: {e}")

            if magazzino_id:
                for node in outlet_variants:
                    inv_item = node["inventoryItem"]["id"].split("/")[-1]
                    try:
                        client.inventory_set(inv_item, magazzino_id, 0)
                    except Exception as e:
                        logger.warning(f"inventory_set@Magazzino=0 fallita item={inv_item}: {e}")
                    try:
                        client.inventory_delete(inv_item, magazzino_id)
                    except Exception as e:
                        logger.warning(f"inventory_delete@Magazzino fallita item={inv_item}: {e}")

        updates = []
        for it in items:
            node = next((n for n in outlet_variants if any((opt.get("name","").lower() in ("size","taglia") and opt.get("value","")==it["size"]) for opt in (n.get("selectedOptions") or []))), None)
            if not node:
                logger.warning(f"[WARN] Variante outlet non trovata per SKU={it['sku']} Size={it['size']}")
                continue
            upd = {"id": node["id"]}
            if it["price_sale"] is not None:
                upd["price"] = float(it["price_sale"])
            if it["price_full"] is not None:
                upd["compareAtPrice"] = float(it["price_full"])
            updates.append(upd)
        if updates and not dry_run:
            client.product_variants_bulk_update(new_pid, updates)
            price_updates += len(updates)

        if not dry_run:
            try:
                src_metafields = client.get_product_metafields(pid)
                batch = []
                for mf in src_metafields:
                    batch.append({
                        "ownerId": new_pid,
                        "namespace": mf["namespace"],
                        "key": mf["key"],
                        "type": mf["type"],
                        "value": mf["value"],
                    })
                    if len(batch) == 25:
                        client.metafields_set(batch); batch = []
                if batch:
                    client.metafields_set(batch)
            except Exception as e:
                logger.warning(f"metafields copy fallita: {e}")

        if not dry_run:
            for it in items:
                writebacks.append({"sku": it["sku"], "size": it["size"], "new_product_id": new_pid})

    if (not dry_run) and src.startswith("http"):
        try:
            written = write_product_ids(src, writebacks, worksheet_name=ws_name)
            logger.info(f"Write-back Product_id completato: {written} righe aggiornate.")
        except Exception as e:
            logger.warning(f"Write-back Product_id fallito: {e}")

    logger.info("FINITO | prodotti analizzati=%d | creati=%d | già esistenti (skip)=%d | updates_prezzi=%d | updates_inventario=%d | updates_media=%d",
                processed, created, skipped_existing, price_updates, inv_updates, media_updates)

if __name__ == "__main__":
    main()
