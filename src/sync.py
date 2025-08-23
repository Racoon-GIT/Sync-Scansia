import os
import argparse
import math
import logging
from dotenv import load_dotenv

from .utils import read_table_from_source, parse_scansia, build_key
from .shopify_client import ShopifyClient
from .gsheets import write_product_ids

def _init_logging():
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

_init_logging()
logger = logging.getLogger("sync")

load_dotenv()

def make_outlet_title_handle(title: str, handle: str):
    """Applica ' - Outlet' al titolo e '-outlet' all'handle."""
    new_title = f"{title} - Outlet" if not title.lower().endswith(" - outlet") else title.replace(" - outlet", " - Outlet")
    base_handle = f"{handle}-outlet" if not handle.endswith("-outlet") else handle
    return new_title, base_handle

def ensure_unique_handle(client: ShopifyClient, product_id: str, title: str, base_handle: str):
    """
    Prova a impostare title/handle/status/tags. Se l'handle desiderato è già usato,
    NON fallisce: applica comunque title/status/tags lasciando l'handle auto-generato da Shopify.
    """
    try:
        client.product_update(product_id, title=title, handle=base_handle, status="ACTIVE", tags=[])
        logger.info(f"Impostato title/handle/status/tags su prodotto outlet {product_id} (handle={base_handle})")
        return base_handle
    except RuntimeError as e:
        msg = str(e).lower()
        if "handle" in msg and ("already in use" in msg or "taken" in msg):
            logger.warning(f"Handle '{base_handle}' già in uso. Mantengo l'handle assegnato da Shopify e aggiorno solo il resto.")
            client.product_update(product_id, title=title, status="ACTIVE", tags=[])
            return None
        logger.error(f"Errore product_update: {e}")
        raise

SIZE_OPTION_NAMES = {"size", "taglia", "eu size", "taglia eu", "shoe size"}

def _option_size_of(variant_node) -> str:
    for opt in (variant_node.get("selectedOptions") or []):
        name = (opt.get("name") or "").strip().lower()
        if name in SIZE_OPTION_NAMES:
            return (opt.get("value") or "").strip()
    return ""

def _find_variant_by_size(variants_nodes, target_size: str):
    t = (target_size or "").strip()
    for n in variants_nodes:
        if _option_size_of(n) == t:
            return n
    return None

def _choose_product_prices(items):
    sale_candidates = [it["price_sale"] for it in items if it.get("price_sale") is not None]
    full_candidates = [it["price_full"] for it in items if it.get("price_full") is not None]
    sale_price = min(sale_candidates) if sale_candidates else None
    full_price = max(full_candidates) if full_candidates else None
    if sale_price is None and full_price is None:
        logger.info("[PREZZI] Nessun prezzo disponibile nel GSheet per questo prodotto; salto update prezzi.")
        return None, None
    if sale_price is not None:
        sale_price = float(round(sale_price, 2))
    if full_price is not None:
        full_price = float(round(full_price, 2))
    if sale_price is not None and full_price is not None and not (full_price > sale_price):
        logger.warning(f"[PREZZI] compareAt ({full_price}) non > price ({sale_price}). Userò solo price.")
        return sale_price, None
    return sale_price, full_price

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", dest="file", help="Percorso locale (xlsx/csv)")
    ap.add_argument("--url", dest="url", help="URL Google Sheet/CSV/XLSX (pubblico)")
    ap.add_argument("--apply", action="store_true", help="Applica modifiche (default: DRY_RUN)")
    ap.add_argument("--dry-run", action="store_true", help="Forza dry-run (ignora --apply)")
    args = ap.parse_args()

    store = os.environ.get("SHOPIFY_STORE")
    token = os.environ.get("SHOPIFY_ADMIN_TOKEN")
    api_version = os.environ.get("SHOPIFY_API_VERSION", "2025-01")
    promo_location_name = os.environ.get("PROMO_LOCATION_NAME", "Promo")
    magazzino_location_name = os.environ.get("MAGAZZINO_LOCATION_NAME", "Magazzino")
    env_url = os.environ.get("SCANSIA_URL")
    env_file = os.environ.get("SCANSIA_FILE")
    sample_rows = int(os.environ.get("LOG_SAMPLE_ROWS", "10"))
    ws_name = os.environ.get("GDRIVE_WORKSHEET_NAME") or None

    if not (store and token):
        raise SystemExit("Config mancante: SHOPIFY_STORE/SHOPIFY_ADMIN_TOKEN")

    env_dry = str(os.environ.get("DRY_RUN", "true")).strip().lower() in {"1", "true", "yes", "y", "si", "sì", "on", "x"}
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
            "key": build_key(sku, size),
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
        outlet_title, outlet_handle = make_outlet_title_handle(base_title, base_handle)

        # 1) se esiste già un OUTLET ACTIVE → skip come prima
        existing_active = client.products_search_by_title_active(outlet_title)
        if existing_active:
            skipped_existing += 1
            logger.info(f"[SKIP] Esiste già OUTLET ACTIVE per '{base_title}' → {existing_active[0]['id']}")
            continue

        # 2) se NON esiste ACTIVE ma esistono DRAFT con esatto titolo → cancellali
        existing_any = client.products_search_by_title_any(outlet_title)
        drafts = [p for p in existing_any if p["title"] == outlet_title and p["status"] == "DRAFT"]
        if drafts and not dry_run:
            for d in drafts:
                logger.info(f"Elimino OUTLET DRAFT pre-esistente prima della duplicazione → {d['id']} ({d['handle']})")
                client.product_delete(d["id"])

        # 3) Duplica prodotto (pulito)
        if dry_run:
            new_pid = "gid://shopify/Product/DRYRUN"
            logger.info(f"[DRY-RUN] Duplicazione di {pid} → {new_pid} con titolo '{outlet_title}'")
        else:
            new_pid = client.product_duplicate(pid, outlet_title)
            if not new_pid:
                logger.error(f"[ERR] productDuplicate fallita per {pid}")
                continue
            created += 1

        # 4) Titolo/handle/status/tags
        if dry_run:
            logger.info(f"[DRY-RUN] Imposterei handle~='{outlet_handle}', status=ACTIVE, tags=[] su {new_pid}")
        else:
            ensure_unique_handle(client, new_pid, outlet_title, outlet_handle)

        # 5) Media: copia se mancano e reset ALT
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
            # reset ALT (anche se già copiate)
            new_media = client.product_images_list(new_pid)
            for im in new_media:
                if im.get("alt"):
                    try:
                        client.product_image_update(new_pid, im["id"], alt="")
                    except Exception as e:
                        logger.warning(f"alt reset fallito image_id={im.get('id')}: {e}")

        # 6) Collections: rimuovi collects
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

        # 7) Varianti del duplicato
        outlet_variants = [] if dry_run else client.get_product_variants(new_pid)

        # 8) INVENTORY: prima Promo (0 a tutte, poi quantità target), poi de-stock Magazzino
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
                node = _find_variant_by_size(outlet_variants, it["size"])
                if not node:
                    logger.warning(f"[WARN] Variante outlet non trovata per size='{it['size']}' (SKU={it['sku']})")
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

        # 9) PREZZI su tutte le varianti
        sale_price_product, full_price_product = _choose_product_prices(items)
        updates = []
        if outlet_variants and (sale_price_product is not None or full_price_product is not None):
            for node in outlet_variants:
                upd = {"id": node["id"]}
                if sale_price_product is not None and full_price_product is not None:
                    if full_price_product > sale_price_product:
                        upd["price"] = float(sale_price_product)
                        upd["compareAtPrice"] = float(full_price_product)
                    else:
                        upd["price"] = float(sale_price_product)
                elif sale_price_product is not None:
                    upd["price"] = float(sale_price_product)
                elif full_price_product is not None:
                    upd["price"] = float(full_price_product)
                updates.append(upd)

            logger.info(
                "Aggiorno PREZZI su tutte le %d varianti di %s → price=%s compareAt=%s",
                len(outlet_variants), new_pid,
                sale_price_product if sale_price_product is not None else full_price_product,
                full_price_product if sale_price_product is not None else None
            )
            logger.debug("Payload prezzi: %s", [(u.get("id"), u.get("price"), u.get("compareAtPrice")) for u in updates])

            if not dry_run and updates:
                client.product_variants_bulk_update(new_pid, updates)
                price_updates += len(updates)

        # 10) Metafield (copia)
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
                        client.metafields_set(batch)
                        metafield_copied += len(batch)
                        batch = []
                if batch:
                    client.metafields_set(batch)
                    metafield_copied += len(batch)
            except Exception as e:
                logger.warning(f"metafields copy fallita: {e}")

        # 11) Write-back Product_id al GSheet
        if not dry_run:
            for it in items:
                writebacks.append({"sku": it["sku"], "size": it["size"], "new_product_id": new_pid})

    # Salvataggio su GSheet
    if (not dry_run) and src.startswith("http"):
        try:
            written = write_product_ids(src, writebacks, worksheet_name=ws_name)
            logger.info(f"Write-back Product_id completato: {written} righe aggiornate.")
        except Exception as e:
            logger.warning(f"Write-back Product_id fallito: {e}")

    logger.info(
        "FINITO | prodotti analizzati=%d | creati=%d | già esistenti (skip)=%d | updates_prezzi=%d | "
        "updates_inventario=%d | updates_media=%d | metafield_copiati=%d",
        processed, created, skipped_existing, price_updates, inv_updates, media_updates, metafield_copied
    )

if __name__ == "__main__":
    main()
