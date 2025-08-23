import os, argparse, math
from dotenv import load_dotenv

from .utils import read_table_from_source, parse_scansia, build_key
from .shopify_client import ShopifyClient

load_dotenv()

def make_outlet_title_handle(title: str, handle: str):
    new_title = f"{title} - outlet" if not title.lower().endswith(" - outlet") else title
    base_handle = f"{handle}-outlet" if not handle.endswith("-outlet") else handle
    return new_title, base_handle

def ensure_unique_handle(client: ShopifyClient, product_id: str, title: str, base_handle: str):
    attempt = 0
    while attempt < 10:
        try_handle = base_handle if attempt == 0 else f"{base_handle}-{attempt+1}"
        try:
            client.product_update(product_id, title=title, handle=try_handle, status="ACTIVE", tags=[])
            return try_handle
        except RuntimeError as e:
            msg = str(e).lower()
            if "handle" in msg and "taken" in msg:
                attempt += 1
                continue
            raise
    client.product_update(product_id, title=title, status="ACTIVE", tags=[])
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", dest="file", help="Percorso locale (xlsx/csv)")
    ap.add_argument("--url", dest="url", help="URL Google Sheet/CSV/XLSX (pubblico)")
    ap.add_argument("--apply", action="store_true", help="Applica modifiche (default dry-run)")
    ap.add_argument("--dry-run", action="store_true", help="Forza dry-run")
    args = ap.parse_args()

    store = os.environ.get("SHOPIFY_STORE")
    token = os.environ.get("SHOPIFY_ADMIN_TOKEN")
    api_version = os.environ.get("SHOPIFY_API_VERSION","2025-01")
    promo_location_name = os.environ.get("PROMO_LOCATION_NAME","Promo")
    env_url = os.environ.get("SCANSIA_URL")
    env_file = os.environ.get("SCANSIA_FILE")

    if not (store and token):
        raise SystemExit("Config mancante: SHOPIFY_STORE/SHOPIFY_ADMIN_TOKEN")

    client = ShopifyClient(store, token, api_version)

    src = args.url or env_url or args.file or env_file
    if not src:
        raise SystemExit("Specifica --url, --file o SCANSIA_URL/SCANSIA_FILE")

    raw_df = read_table_from_source(src)
    df = parse_scansia(raw_df)

    # Preprocessing: per ogni riga, trova prodotto/variante da SKU
    rows = []
    for _, r in df.iterrows():
        sku = r["SKU"]
        size = r["Size"]
        qta = int(r["Qta"])
        p_full = r.get("Prezzo Pieno", None)
        p_sale = r.get("Prezzo Scontato", None)

        variants = client.find_variants_by_sku(sku)
        if not variants:
            print(f"[SKIP] SKU non trovato: {sku}")
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
            "key": build_key(sku, size)
        })

    from collections import defaultdict
    by_product = defaultdict(list)
    for it in rows:
        by_product[it["product_id"]].append(it)

    # Locations
    locations = client.get_locations()
    promo = next((loc for loc in locations if loc["name"].strip().lower() == promo_location_name.strip().lower()), None)
    if not promo:
        raise SystemExit(f"Location '{promo_location_name}' non trovata su Shopify.")
    promo_id = promo["id"]

    dry_run = (not args.apply) or args.dry_run
    print(f"Processing {len(by_product)} prodotti | dry_run={dry_run}")

    for pid, items in by_product.items():
        base_title = items[0]["product_title"]
        base_handle = items[0]["product_handle"]
        outlet_title, outlet_handle = make_outlet_title_handle(base_title, base_handle)

        # Check esistenza Outlet ACTIVE con titolo esatto
        existing = client.products_search_by_title_active(outlet_title)
        if existing:
            print(f"[SKIP] Esiste già OUTLET ACTIVE per '{base_title}' → {existing[0]['id']}")
            continue

        # Duplica
        new_pid = client.product_duplicate(pid)
        if not new_pid:
            print(f"[ERR] productDuplicate fallita per {pid}")
            continue

        # Rinomina + ACTIVE + tags=[]
        if dry_run:
            print(f"[DRY-RUN] Set titolo='{outlet_title}', handle~='{outlet_handle}', status=ACTIVE, tags=[] su {new_pid}")
        else:
            ensure_unique_handle(client, new_pid, outlet_title, outlet_handle)

        # Varianti outlet
        outlet_variants = client.get_product_variants(new_pid)
        var_by_key = {}
        for node in outlet_variants:
            # costruisco KEY con sku + size (dalle selectedOptions)
            size_val = ""
            for opt in (node.get("selectedOptions") or []):
                if (opt.get("name") or "").lower() in ("size","taglia"):
                    size_val = opt.get("value") or ""
                    break
            key = build_key(node.get("sku",""), size_val)
            var_by_key[key] = node

        # INVENTARIO: azzera tutto su tutte le locations
        for node in outlet_variants:
            inv_item = node["inventoryItem"]["id"].split("/")[-1]
            levels = client.inventory_levels_for_item(inv_item)
            for lvl in levels.get("inventory_levels", []):
                loc_id = lvl["location_id"]
                if dry_run:
                    print(f"[DRY-RUN] inventory_set {inv_item} @ loc {loc_id} = 0")
                else:
                    client.inventory_set(inv_item, loc_id, 0)

        # PREZZI (bulk) + INVENTARIO PROMO per varianti presenti in sheet
        updates = []
        for it in items:
            key = it["key"]
            node = var_by_key.get(key)
            if not node:
                # fallback: match per sola taglia
                for n in outlet_variants:
                    size_val = ""
                    for opt in (n.get("selectedOptions") or []):
                        if (opt.get("name") or "").lower() in ("size","taglia"):
                            size_val = opt.get("value") or ""
                            break
                    if size_val == it["size"]:
                        node = n
                        break
            if not node:
                print(f"[WARN] Variante outlet non trovata per KEY={key}")
                continue

            upd = {"id": node["id"]}
            if it["price_sale"] is not None:
                upd["price"] = float(it["price_sale"])
            if it["price_full"] is not None:
                upd["compareAtPrice"] = float(it["price_full"])
            updates.append(upd)

        if updates:
            if dry_run:
                print(f"[DRY-RUN] productVariantsBulkUpdate({len(updates)}) su {new_pid}")
            else:
                client.product_variants_bulk_update(new_pid, updates)

        # INVENTARIO @ Promo per varianti presenti
        for it in items:
            key = it["key"]
            node = var_by_key.get(key)
            if not node:
                continue
            inv_item = node["inventoryItem"]["id"].split("/")[-1]
            qta = int(it["qta"])
            if dry_run:
                print(f"[DRY-RUN] inventory_connect/set {inv_item} @ PROMO {promo_id} = {qta}")
            else:
                try:
                    client.inventory_connect(inv_item, promo_id)
                except Exception:
                    pass
                client.inventory_set(inv_item, promo_id, qta)

if __name__ == "__main__":
    main()
