"""
Modulo per reset varianti prodotti Shopify.
Logica derivata da reset_variants.py, integrata con shopify_client.
"""
import logging
import time
from typing import Dict, Any, List, Optional

from .shopify_client import ShopifyClient
from .exceptions import ShopifyAPIError
from .utils import gid_to_id

LOG = logging.getLogger("sync.variant_reset")


def reset_product_variants(
    shop: ShopifyClient,
    product_gid: str,
    skip_filter: str = "perso",
    delay: float = 0.6
) -> bool:
    """
    Reset completo varianti prodotto: delete + recreate preservando dati e inventory.

    Strategia (compatibile con metafield su option):
    1. Fetch tutte le varianti
    2. Backup in-memory (dict Python)
    3. Delete varianti 2-N
    4. Recreate varianti 2-N da backup
    5. Delete variante #1
    6. Recreate variante #1 da backup
    7. Ripristina inventory levels
    8. Cleanup location extra

    Args:
        shop: Client Shopify
        product_gid: GID prodotto (gid://shopify/Product/...)
        skip_filter: Skip varianti con questa stringa nel titolo (case-insensitive)
        delay: Delay tra operazioni (sec)

    Returns:
        True se successo, False se errore

    Raises:
        ShopifyAPIError: Se errori critici
    """
    LOG.info("Reset varianti per prodotto: %s", product_gid)

    # ===== STEP 1: FETCH VARIANTI =====
    try:
        variants = shop.get_product_variants(product_gid)
        LOG.info("Trovate %d varianti", len(variants))
    except Exception as e:
        LOG.error("Errore fetch varianti: %s", e)
        return False

    if not variants:
        LOG.warning("Nessuna variante trovata, skip")
        return False

    if len(variants) == 1:
        LOG.info("Solo 1 variante presente, skip reset (non necessario)")
        return True

    # ===== STEP 2: BACKUP VARIANTI + INVENTORY =====
    LOG.info("Backup varianti e inventory levels...")

    variant_backup: List[Dict[str, Any]] = []
    inventory_backup: Dict[str, List[Dict[str, Any]]] = {}

    for idx, v in enumerate(variants):
        # Backup dati variante
        variant_backup.append({
            "position": idx,
            "variant_gid": v["id"],
            "data": v,
        })

        # Backup inventory levels (solo se gestito)
        inventory_item_gid = v.get("inventoryItem", {}).get("id")
        if inventory_item_gid:
            inventory_item_id = gid_to_id(inventory_item_gid)
            if inventory_item_id:
                try:
                    levels = shop.inventory_levels_get(inventory_item_id)
                    inventory_backup[v["id"]] = levels
                    LOG.debug("Backup inventory: variant %s, %d levels", v["id"], len(levels))
                except Exception as e:
                    LOG.warning("Errore backup inventory variant %s: %s", v["id"], e)

    # ===== STEP 3: DELETE VARIANTI 2-N =====
    LOG.info("Cancellazione varianti dalla 2 alla N...")
    for v_backup in variant_backup[1:]:
        v_gid = v_backup["variant_gid"]
        v_title = v_backup["data"].get("title", "")

        # Filtro skip
        if skip_filter and skip_filter.lower() in v_title.lower():
            LOG.info("Skip delete variante con '%s' nel titolo: %s", skip_filter, v_title)
            continue

        try:
            shop.variant_delete(v_gid)
            LOG.info("Cancellata variante: %s (%s)", v_gid, v_title)
            time.sleep(delay)
        except Exception as e:
            LOG.error("Errore delete variante %s: %s", v_gid, e)

    # ===== STEP 4: RECREATE VARIANTI 2-N =====
    LOG.info("Ricreazione varianti dalla 2 alla N...")

    # Mapping old_variant_gid → new_inventory_item_id
    variant_mapping: Dict[str, int] = {}

    for v_backup in variant_backup[1:]:
        old_v_gid = v_backup["variant_gid"]
        v_data = v_backup["data"]
        v_title = v_data.get("title", "")

        # Filtro skip
        if skip_filter and skip_filter.lower() in v_title.lower():
            LOG.info("Skip recreate variante con '%s' nel titolo: %s", skip_filter, v_title)
            continue

        # Build variant input per GraphQL
        variant_input = _build_variant_input(v_data)

        try:
            new_variant = shop.variant_create(product_gid, variant_input)
            new_inv_item_gid = new_variant.get("inventoryItem", {}).get("id")
            if new_inv_item_gid:
                new_inv_item_id = gid_to_id(new_inv_item_gid)
                if new_inv_item_id:
                    variant_mapping[old_v_gid] = new_inv_item_id
                    LOG.info("Variante ricreata: %s → new inventory_item_id: %s", v_title, new_inv_item_id)

            time.sleep(delay)
        except Exception as e:
            LOG.error("Errore recreate variante %s: %s", v_title, e)

    # ===== STEP 5: DELETE PRIMA VARIANTE =====
    first_v_backup = variant_backup[0]
    first_v_gid = first_v_backup["variant_gid"]
    first_v_title = first_v_backup["data"].get("title", "")

    LOG.info("Cancellazione prima variante: %s (%s)", first_v_gid, first_v_title)
    try:
        shop.variant_delete(first_v_gid)
        LOG.info("Prima variante cancellata")
        time.sleep(delay)
    except Exception as e:
        LOG.error("Errore delete prima variante: %s", e)

    # ===== STEP 6: RECREATE PRIMA VARIANTE =====
    LOG.info("Ricreazione prima variante...")

    if skip_filter and skip_filter.lower() in first_v_title.lower():
        LOG.info("Skip recreate prima variante con '%s' nel titolo: %s", skip_filter, first_v_title)
    else:
        variant_input = _build_variant_input(first_v_backup["data"])

        try:
            new_variant = shop.variant_create(product_gid, variant_input)
            new_inv_item_gid = new_variant.get("inventoryItem", {}).get("id")
            if new_inv_item_gid:
                new_inv_item_id = gid_to_id(new_inv_item_gid)
                if new_inv_item_id:
                    variant_mapping[first_v_gid] = new_inv_item_id
                    LOG.info("Prima variante ricreata: %s → new inventory_item_id: %s", first_v_title, new_inv_item_id)

            time.sleep(delay)
        except Exception as e:
            LOG.error("Errore recreate prima variante: %s", e)

    # ===== STEP 7: RIPRISTINA INVENTORY LEVELS =====
    LOG.info("Ripristino inventory levels...")

    for old_v_gid, new_inv_item_id in variant_mapping.items():
        original_levels = inventory_backup.get(old_v_gid, [])

        if not original_levels:
            LOG.debug("Nessun inventory da ripristinare per variant %s (no inventory management)", old_v_gid)
            continue

        for level in original_levels:
            location_id = level.get("location_id")
            available = level.get("available", 0)

            if location_id:
                try:
                    LOG.debug("Ripristino inventory: location %s, qty %s", location_id, available)
                    shop.inventory_set(new_inv_item_id, location_id, available)
                    time.sleep(delay)
                except Exception as e:
                    LOG.warning("Errore ripristino inventory variant %s, location %s: %s", old_v_gid, location_id, e)

    # ===== STEP 8: CLEANUP LOCATION EXTRA =====
    LOG.info("Pulizia location inventory non utilizzate...")

    for old_v_gid, new_inv_item_id in variant_mapping.items():
        original_levels = inventory_backup.get(old_v_gid, [])

        # Se la variante non aveva inventory backup, skip
        if not original_levels:
            LOG.debug("Skip cleanup per variant %s (no inventory management nell'originale)", old_v_gid)
            continue

        original_locations = {level["location_id"] for level in original_levels}
        LOG.debug("Variant %s: location originali = %s", old_v_gid, original_locations)

        # Fetch location attuali
        try:
            current_levels = shop.inventory_levels_get(new_inv_item_id)

            for level in current_levels:
                current_location_id = level["location_id"]

                # Se questa location NON era nell'originale, rimuovila
                if current_location_id not in original_locations:
                    LOG.info("Location %s da rimuovere (non era nell'originale)", current_location_id)
                    try:
                        shop.inventory_delete(new_inv_item_id, current_location_id)
                        time.sleep(delay)
                    except Exception as e:
                        LOG.warning("Errore rimozione location %s: %s", current_location_id, e)
                else:
                    LOG.debug("Location %s mantenuta (era nell'originale)", current_location_id)

        except Exception as e:
            LOG.warning("Errore cleanup location per variant %s: %s", old_v_gid, e)

    LOG.info("Reset varianti completato con successo!")
    return True


def _build_variant_input(variant_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Costruisce input variante per REST API POST /products/{id}/variants.json

    Args:
        variant_data: Dati variante originale (formato GraphQL)

    Returns:
        Dict input per REST API
    """
    selected_options = variant_data.get("selectedOptions", [])

    # Estrai option values (max 3)
    option1 = None
    option2 = None
    option3 = None

    for idx, opt in enumerate(selected_options):
        if idx == 0:
            option1 = opt.get("value")
        elif idx == 1:
            option2 = opt.get("value")
        elif idx == 2:
            option3 = opt.get("value")

    # Formato REST API
    variant_input = {
        "option1": option1,
        "option2": option2,
        "option3": option3,
        "price": str(variant_data.get("price", "0.00")),
        "compare_at_price": variant_data.get("compareAtPrice"),
        "sku": variant_data.get("sku"),
        "barcode": variant_data.get("barcode"),
        "inventory_management": "shopify" if variant_data.get("inventoryItem") else None,
        "inventory_policy": "deny",  # REST usa lowercase
        "requires_shipping": True,
        "taxable": True,
        "weight": 0.0,
        "weight_unit": "kg",
    }

    # Rimuovi None values
    return {k: v for k, v in variant_input.items() if v is not None}
