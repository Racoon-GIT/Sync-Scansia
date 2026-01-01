import os
import sys
import time
import json
import requests
import mysql.connector
from mysql.connector import Error

# ---------- CONFIG -------------------------------------------------
SHOP_DOMAIN  = os.getenv("SHOPIFY_DOMAIN")
ACCESS_TOKEN = os.getenv("SHOPIFY_TOKEN")
DB_HOST      = os.getenv("DB_HOST")
DB_USER      = os.getenv("DB_USER")
DB_PASS      = os.getenv("DB_PASS")
DB_NAME      = os.getenv("DB_NAME")
API_VERSION  = "2024-04"
HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# ---------- LOG ----------------------------------------------------
def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ---------- UTILS --------------------------------------------------
def safe_request(method, url, headers=None, json=None, max_retries=5):
    for attempt in range(max_retries):
        res = requests.request(method, url, headers=headers, json=json)
        if res.status_code == 429:
            wait = 2 ** attempt
            log(f"⚠️ Rate limit, attendo {wait}s...")
            time.sleep(wait)
            continue
        if res.status_code >= 400:
            try:
                error_detail = res.json()
                log(f"❌ Errore API {res.status_code}: {json.dumps(error_detail, indent=2)}")
            except:
                log(f"❌ Errore API {res.status_code}: {res.text}")
        res.raise_for_status()
        return res
    raise Exception("❌ Troppe richieste, impossibile continuare")

# ---------- INVENTORY MANAGEMENT -----------------------------------
def get_inventory_levels(inventory_item_id):
    """Recupera tutti gli inventory levels per un inventory_item_id"""
    try:
        res = safe_request(
            "GET",
            f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/inventory_levels.json?inventory_item_ids={inventory_item_id}",
            headers=HEADERS
        )
        levels = res.json().get("inventory_levels", [])
        return levels
    except Exception as e:
        log(f"⚠️ Errore recupero inventory levels: {e}")
        return []

def set_inventory_level(inventory_item_id, location_id, available):
    """Imposta l'inventory level per una location specifica"""
    try:
        payload = {
            "location_id": location_id,
            "inventory_item_id": inventory_item_id,
            "available": available
        }
        safe_request(
            "POST",
            f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/inventory_levels/set.json",
            headers=HEADERS,
            json=payload
        )
        log(f"  ✅ Inventory impostato: location {location_id} → {available} unità")
        time.sleep(0.6)
        return True
    except Exception as e:
        log(f"  ❌ Errore impostazione inventory: {e}")
        return False

def remove_inventory_level(inventory_item_id, location_id):
    """Rimuove l'inventory level per una location specifica"""
    try:
        safe_request(
            "DELETE",
            f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/inventory_levels.json?inventory_item_id={inventory_item_id}&location_id={location_id}",
            headers=HEADERS
        )
        log(f"  🗑️ Location {location_id} rimossa (non presente nell'originale)")
        time.sleep(0.6)
        return True
    except Exception as e:
        log(f"  ❌ Errore rimozione inventory level: {e}")
        return False

# ---------- DB ------------------------------------------------------
def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

def ensure_temp_tables(cur):
    # Tabella backup varianti
    cur.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS variant_backup (
            id BIGINT,
            product_id BIGINT,
            inventory_item_id BIGINT,
            variant_json TEXT,
            position INT,
            PRIMARY KEY (product_id, id)
        )
    """)
    cur.execute("DELETE FROM variant_backup")
    
    # Tabella backup inventory levels
    cur.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS inventory_backup (
            variant_id BIGINT,
            inventory_item_id BIGINT,
            location_id BIGINT,
            available INT,
            PRIMARY KEY (variant_id, location_id)
        )
    """)
    cur.execute("DELETE FROM inventory_backup")

# ---------- MAIN ----------------------------------------------------
def main():
    product_ids_env = os.getenv("PRODUCT_IDS")
    if not product_ids_env:
        log("❌ Variabile d'ambiente PRODUCT_IDS non definita")
        sys.exit(1)

    product_ids = [pid.strip() for pid in product_ids_env.split(",") if pid.strip()]
    db = connect_db()
    cur = db.cursor()
    ensure_temp_tables(cur)

    for product_id in product_ids:
        log(f"📦 Elaborazione prodotto: {product_id}")

        # ===== STEP 1: FETCH VARIANTI =====
        try:
            res = safe_request(
                "GET",
                f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/products/{product_id}/variants.json",
                headers=HEADERS
            )
            variants = res.json().get("variants", [])
            log(f"🔍 Trovate {len(variants)} varianti")
        except Exception as e:
            log(f"❌ Errore durante l'accesso alle varianti: {e}")
            continue

        if not variants:
            log("⚠️ Nessuna variante trovata, skip prodotto")
            continue

        # ===== STEP 2: BACKUP VARIANTI + INVENTORY =====
        log(f"💾 Backup varianti e inventory levels...")
        
        for idx, v in enumerate(variants):
            # Backup dati variante
            cur.execute(
                "INSERT INTO variant_backup (id, product_id, inventory_item_id, variant_json, position) VALUES (%s, %s, %s, %s, %s)",
                (v["id"], product_id, v.get("inventory_item_id"), json.dumps(v), idx)
            )
            
            # Backup inventory levels (solo se gestito)
            if v.get("inventory_management") and v.get("inventory_item_id"):
                inventory_levels = get_inventory_levels(v["inventory_item_id"])
                
                for level in inventory_levels:
                    cur.execute(
                        "INSERT INTO inventory_backup (variant_id, inventory_item_id, location_id, available) VALUES (%s, %s, %s, %s)",
                        (v["id"], v["inventory_item_id"], level["location_id"], level["available"])
                    )
                    log(f"  💾 Backup inventory: variant {v['id']}, location {level['location_id']}, qty {level['available']}")
        
        db.commit()

        # ===== STEP 3: CANCELLA VARIANTI DALLA 2 ALLA N =====
        log("🗑️ Cancellazione varianti dalla 2 alla N...")
        for v in variants[1:]:
            try:
                safe_request(
                    "DELETE",
                    f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/variants/{v['id']}.json",
                    headers=HEADERS
                )
                log(f"  ✅ Cancellata variante {v['id']} ({v.get('title')})")
                time.sleep(0.6)
            except Exception as e:
                log(f"❌ Errore delete variante {v['id']}: {e}")

        # ===== STEP 4: RICREA VARIANTI 2-N DA BACKUP =====
        log("🔄 Ricreazione varianti dalla 2 alla N...")
        cur.execute("""
            SELECT id, inventory_item_id, variant_json, position
            FROM variant_backup 
            WHERE product_id = %s
            ORDER BY position
        """, (product_id,))
        rows = cur.fetchall()
        
        # Mappa per tracciare old_variant_id -> new_inventory_item_id
        variant_mapping = {}
        
        # Ricrea dalla posizione 1 in poi (skip posizione 0 = prima variante)
        for (old_variant_id, old_inventory_item_id, variant_json, position) in rows[1:]:
            v = json.loads(variant_json)
            
            # FILTRO: Salta varianti con "perso" nel titolo
            if "perso" in v.get("title", "").lower():
                log(f"  ⏭️ Skip variante con 'perso' nel titolo: {v.get('title')}")
                continue
            
            log(f"🔄 Ricreo variante: {v.get('option1')} / {v.get('option2')} / {v.get('option3')}")
            
            # Payload per creare la variante
            payload = {"variant": {
                "option1": v["option1"],
                "option2": v.get("option2"),
                "option3": v.get("option3"),
                "price": v.get("price"),
                "compare_at_price": v.get("compare_at_price"),
                "sku": v.get("sku"),
                "barcode": v.get("barcode"),
                "inventory_management": v.get("inventory_management"),
                "inventory_policy": v.get("inventory_policy"),
                "fulfillment_service": v.get("fulfillment_service"),
                "requires_shipping": v.get("requires_shipping", True),
                "taxable": v.get("taxable", True),
                "weight": v.get("weight", 0),
                "weight_unit": v.get("weight_unit", "kg")
            }}
            
            try:
                res = safe_request(
                    "POST",
                    f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/products/{product_id}/variants.json",
                    headers=HEADERS,
                    json=payload
                )
                new_variant = res.json().get("variant", {})
                new_inventory_item_id = new_variant.get("inventory_item_id")
                
                if new_inventory_item_id:
                    variant_mapping[old_variant_id] = new_inventory_item_id
                    log(f"  ✅ Variante ricreata, nuovo inventory_item_id: {new_inventory_item_id}")
                
                time.sleep(0.6)
            except Exception as e:
                log(f"❌ Errore creazione variante: {e}")
                continue

        # ===== STEP 5: CANCELLA LA PRIMA VARIANTE (ora possibile) =====
        first_variant = variants[0]
        log(f"🗑️ Cancellazione prima variante: {first_variant['id']} ({first_variant.get('title')})")
        try:
            safe_request(
                "DELETE",
                f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/variants/{first_variant['id']}.json",
                headers=HEADERS
            )
            log("  ✅ Prima variante cancellata")
            time.sleep(0.6)
        except Exception as e:
            log(f"❌ Errore cancellazione prima variante: {e}")

        # ===== STEP 6: RICREA LA PRIMA VARIANTE DA BACKUP =====
        log("🔄 Ricreazione prima variante...")
        first_row = rows[0]
        old_variant_id, old_inventory_item_id, variant_json, position = first_row
        v = json.loads(variant_json)
        
        # FILTRO: Salta se contiene "perso"
        if "perso" in v.get("title", "").lower():
            log(f"  ⏭️ Skip prima variante con 'perso' nel titolo: {v.get('title')}")
        else:
            log(f"🔄 Ricreo prima variante: {v.get('option1')} / {v.get('option2')} / {v.get('option3')}")
            
            payload = {"variant": {
                "option1": v["option1"],
                "option2": v.get("option2"),
                "option3": v.get("option3"),
                "price": v.get("price"),
                "compare_at_price": v.get("compare_at_price"),
                "sku": v.get("sku"),
                "barcode": v.get("barcode"),
                "inventory_management": v.get("inventory_management"),
                "inventory_policy": v.get("inventory_policy"),
                "fulfillment_service": v.get("fulfillment_service"),
                "requires_shipping": v.get("requires_shipping", True),
                "taxable": v.get("taxable", True),
                "weight": v.get("weight", 0),
                "weight_unit": v.get("weight_unit", "kg")
            }}
            
            try:
                res = safe_request(
                    "POST",
                    f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/products/{product_id}/variants.json",
                    headers=HEADERS,
                    json=payload
                )
                new_variant = res.json().get("variant", {})
                new_inventory_item_id = new_variant.get("inventory_item_id")
                
                if new_inventory_item_id:
                    variant_mapping[old_variant_id] = new_inventory_item_id
                    log(f"  ✅ Prima variante ricreata, nuovo inventory_item_id: {new_inventory_item_id}")
                
                time.sleep(0.6)
            except Exception as e:
                log(f"❌ Errore creazione prima variante: {e}")

        # ===== STEP 7: RIPRISTINA INVENTORY LEVELS =====
        log("📍 Ripristino inventory levels...")
        cur.execute("""
            SELECT variant_id, location_id, available 
            FROM inventory_backup 
            WHERE variant_id IN (SELECT id FROM variant_backup WHERE product_id = %s)
        """, (product_id,))
        
        inventory_rows = cur.fetchall()
        for (old_variant_id, location_id, available) in inventory_rows:
            new_inventory_item_id = variant_mapping.get(old_variant_id)
            if new_inventory_item_id:
                log(f"  🔄 Ripristino inventory: location {location_id}, qty {available}")
                set_inventory_level(new_inventory_item_id, location_id, available)
            else:
                log(f"  ⚠️ Impossibile ripristinare inventory per variant {old_variant_id} (variante non ricreata o skippata)")

        # ===== STEP 8: RIMUOVI LOCATION EXTRA NON ORIGINALI =====
        log("🧹 Pulizia location inventory non utilizzate...")
        
        for old_variant_id, new_inventory_item_id in variant_mapping.items():
            # Query DB: quali location aveva questa variante nell'originale?
            cur.execute("""
                SELECT DISTINCT location_id
                FROM inventory_backup
                WHERE variant_id = %s
            """, (old_variant_id,))
            
            original_locations_rows = cur.fetchall()
            original_locs = set([row[0] for row in original_locations_rows])
            
            # Se la variante non aveva inventory backup, skip (non aveva inventory_management)
            if not original_locs:
                log(f"  ⏭️ Skip cleanup per variant {old_variant_id} (no inventory management nell'originale)")
                continue
            
            log(f"  🔍 Variant {old_variant_id}: location originali = {original_locs}")
            
            # Ottieni le location attuali della nuova variante
            current_levels = get_inventory_levels(new_inventory_item_id)
            
            for level in current_levels:
                current_location_id = level["location_id"]
                
                # Se questa location NON era nell'originale, rimuovila
                if current_location_id not in original_locs:
                    log(f"  🗑️ Location {current_location_id} da rimuovere (non era nell'originale)")
                    remove_inventory_level(new_inventory_item_id, current_location_id)
                else:
                    log(f"  ✅ Location {current_location_id} mantenuta (era nell'originale)")

        log(f"✅ Prodotto {product_id} completato con successo!\n")

    cur.close()
    db.close()
    log("🎉 Processo completato per tutti i prodotti!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"❌ Errore fatale: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
