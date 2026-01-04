#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fix_prices.py — Script per correggere prezzi a zero su outlet esistenti

Scenario:
- Prodotti outlet creati con bug v2.0 hanno prezzi a zero
- Google Sheet contiene i prezzi corretti (prezzo_outlet, prezzo)
- Questo script aggiorna SOLO i prezzi senza toccare altro

Algoritmo:
1. Legge righe da Google Sheet (online=SI, qta>0)
2. Raggruppa per SKU
3. Per ogni SKU:
   a. Estrae prezzi corretti da Sheet (prezzo_outlet = scontato, prezzo = pieno)
   b. Cerca outlet esistente per SKU
   c. Se trovato e ACTIVE, aggiorna prezzi su TUTTE le varianti
4. Report finale con statistiche

Uso:
    python fix_prices.py --dry-run  # Modalità test (nessuna modifica)
    python fix_prices.py --apply    # Applica modifiche

ENV richieste:
    SHOPIFY_ADMIN_TOKEN
    GSPREAD_SHEET_ID
    GSPREAD_WORKSHEET_TITLE
    GOOGLE_CREDENTIALS_JSON
"""

import argparse
import logging
import sys
from typing import Any, Dict, List

# Importa funzioni da sync.py esistente
from src.sync import (
    Shopify,
    gs_read_rows,
    _clean_price,
    _truthy_si,
    logger as sync_logger
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("fix_prices")


def fix_prices_for_sku(shop: Shopify, sku: str, rows: List[Dict[str, Any]], dry_run: bool) -> str:
    """
    Corregge prezzi per un singolo SKU.

    Args:
        shop: Client Shopify
        sku: SKU prodotto
        rows: Righe Google Sheet per questo SKU (può avere più taglie)
        dry_run: Se True, non applica modifiche

    Returns:
        Status: SUCCESS, SKIP_NOT_FOUND, SKIP_DRAFT, ERROR
    """
    logger.info("=" * 60)
    logger.info("Processing SKU=%s", sku)

    # 1. Estrai prezzi dalla prima riga (tutti i prodotti stesso SKU hanno stessi prezzi)
    first_row = rows[0]

    # Colonne secondo specifiche utente:
    # - Colonna H "Prezzo High" → compareAtPrice
    # - Colonna J "Prezzo Outlet" → price
    prezzo_pieno = _clean_price(first_row.get("prezzo_high"))
    prezzo_scontato = _clean_price(first_row.get("prezzo_outlet"))

    # Logica prezzi secondo requisiti:
    # - prezzo_scontato = prezzo_outlet dal foglio (colonna J)
    # - Se prezzo_outlet non valorizzato, usa prezzo_high (colonna H)
    if not prezzo_scontato:
        prezzo_scontato = prezzo_pieno or "0.00"

    # - prezzo_pieno = prezzo_high dal foglio (colonna H)
    # - Se prezzo_high non valorizzato o zero, usa prezzo_outlet (colonna J)
    if not prezzo_pieno or prezzo_pieno == "0.00":
        prezzo_pieno = prezzo_scontato

    logger.info("Prezzi target: price=%s (da Prezzo Outlet), compareAtPrice=%s (da Prezzo High)",
                prezzo_scontato, prezzo_pieno)

    # 2. Cerca outlet esistente usando colonna Q (Product ID)
    # REQUISITO: La colonna Q DEVE essere valorizzata, altrimenti skip
    # Nota: header "Product_Id" viene normalizzato in "product_id"
    product_id_q = (first_row.get("product_id") or "").strip()

    if not product_id_q:
        logger.warning("Colonna Q NON valorizzata per SKU=%s, SKIP (requisito obbligatorio)", sku)
        return "SKIP_NO_PRODUCT_ID"

    # Cerca usando Product ID dalla colonna Q
    try:
        # Se inizia con gid:// è già un GID Shopify
        if product_id_q.startswith("gid://shopify/Product/"):
            outlet_gid = product_id_q
            # Verifica che esista e sia ACTIVE
            variants = shop.get_product_variants(outlet_gid)
            if variants:
                outlet = {"id": outlet_gid, "status": "ACTIVE"}
            else:
                logger.warning("Product ID=%s non trovato su Shopify", product_id_q)
                return "SKIP_NOT_FOUND"
        else:
            # Altrimenti cerca per handle
            outlet = shop.find_product_by_handle(product_id_q)
    except Exception as e:
        logger.error("Errore ricerca outlet per Product ID=%s: %s", product_id_q, e)
        return "ERROR"

    if not outlet:
        logger.warning("Outlet non trovato, skip")
        return "SKIP_NOT_FOUND"

    # 3. Verifica che sia ACTIVE
    if outlet.get("status") != "ACTIVE":
        logger.info("Outlet trovato ma status=%s (non ACTIVE), skip", outlet.get("status"))
        return "SKIP_DRAFT"

    outlet_gid = outlet["id"]
    outlet_handle = outlet.get("handle", "N/A")
    logger.info("Outlet trovato: %s (handle: %s)", outlet_gid, outlet_handle)

    # 4. Fetch varianti correnti per verificare prezzi
    try:
        variants = shop.get_product_variants(outlet_gid)
    except Exception as e:
        logger.error("Errore fetch varianti: %s", e)
        return "ERROR"

    if not variants:
        logger.warning("Nessuna variante trovata, skip")
        return "SKIP_NOT_FOUND"

    # 5. Colonna Q valorizzata (requisito verificato sopra)
    # OVERWRITE forzato: aggiorna SEMPRE i prezzi indipendentemente dal valore attuale
    logger.info("Colonna Q valorizzata: aggiornamento FORZATO prezzi (overwrite)")

    # 6. Aggiorna prezzi
    if dry_run:
        logger.info("[DRY-RUN] Aggiornerei %d varianti con prezzi: scontato=%s, pieno=%s",
                   len(variants), prezzo_scontato, prezzo_pieno)
        return "SUCCESS_DRY"

    try:
        shop.variants_bulk_update_prices(outlet_gid, prezzo_scontato, prezzo_pieno)
        logger.info("✅ Prezzi aggiornati per %d varianti", len(variants))
        return "SUCCESS"
    except Exception as e:
        logger.error("Errore aggiornamento prezzi: %s", e)
        return "ERROR"


def main():
    parser = argparse.ArgumentParser(
        description="Fix prezzi a zero su outlet esistenti",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  %(prog)s --dry-run    # Test senza modifiche
  %(prog)s --apply      # Applica correzioni
        """
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Applica modifiche (default: dry-run)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Modalità test (nessuna modifica, default)"
    )

    args = parser.parse_args()

    # Default a dry-run se nessun flag specificato
    dry_run = not args.apply or args.dry_run

    if dry_run:
        logger.info("=" * 60)
        logger.info("MODALITÀ DRY-RUN - Nessuna modifica sarà applicata")
        logger.info("=" * 60)
    else:
        logger.warning("=" * 60)
        logger.warning("MODALITÀ APPLY - Le modifiche saranno applicate!")
        logger.warning("=" * 60)

    # 1. Leggi Google Sheet
    logger.info("Lettura Google Sheet...")
    try:
        rows, col_index, ws = gs_read_rows()
    except Exception as e:
        logger.error("Errore lettura Google Sheet: %s", e)
        sys.exit(1)

    # 2. Filtra righe (online=SI, qta>0)
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
        sys.exit(0)

    # 3. Raggruppa per SKU
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

    # 4. Inizializza Shopify
    logger.info("Connessione a Shopify...")
    try:
        shop = Shopify()
    except Exception as e:
        logger.error("Errore inizializzazione Shopify: %s", e)
        sys.exit(1)

    # 5. Processa ogni SKU
    stats = {
        "success": 0,
        "success_dry": 0,
        "skip_not_found": 0,
        "skip_draft": 0,
        "skip_no_product_id": 0,
        "errors": 0
    }

    for sku, sku_rows in grouped_by_sku.items():
        try:
            result = fix_prices_for_sku(shop, sku, sku_rows, dry_run)

            if result == "SUCCESS":
                stats["success"] += 1
            elif result == "SUCCESS_DRY":
                stats["success_dry"] += 1
            elif result == "SKIP_NOT_FOUND":
                stats["skip_not_found"] += 1
            elif result == "SKIP_DRAFT":
                stats["skip_draft"] += 1
            elif result == "SKIP_NO_PRODUCT_ID":
                stats["skip_no_product_id"] += 1
            elif result == "ERROR":
                stats["errors"] += 1
        except Exception as e:
            logger.error("Errore processando SKU=%s: %s", sku, e, exc_info=True)
            stats["errors"] += 1

    # 6. Report finale
    logger.info("=" * 60)
    logger.info("RISULTATI FINALI:")
    if dry_run:
        logger.info("- Prodotti da aggiornare: %d", stats["success_dry"])
    else:
        logger.info("- Prodotti aggiornati: %d", stats["success"])
    logger.info("- Skip (colonna Q vuota): %d", stats["skip_no_product_id"])
    logger.info("- Skip (non trovati): %d", stats["skip_not_found"])
    logger.info("- Skip (draft): %d", stats["skip_draft"])
    logger.info("- Errori: %d", stats["errors"])
    logger.info("=" * 60)

    if dry_run and stats["success_dry"] > 0:
        logger.info("")
        logger.info("Per applicare le modifiche, esegui:")
        logger.info("  python fix_prices.py --apply")

    # Exit code
    sys.exit(0 if stats["errors"] == 0 else 1)


if __name__ == "__main__":
    main()
