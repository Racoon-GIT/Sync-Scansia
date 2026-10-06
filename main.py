#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py - Entry point del job cron REORDER

Esecuzione:
    RUN_MODE=REORDER COLLECTION_ID=262965428289 python -m main

ENV Variables:
    RUN_MODE: "REORDER" (obbligatorio)
    COLLECTION_ID: ID numerico collection (es: 262965428289)
    DRY_RUN: DRY-RUN salvo token APPLY esplicito (vedi _resolve_dry_run)

SYNC e FIX_PRICES leggevano il foglio "Scarpe_in_Scansia": ritirati con F7 (il
registro è MySQL, docs/f7-mysql-adapter-plan.md §3.8). Pubblicazione e prezzi
girano dalla web app Scansia Manager (backend/app.py).
"""

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("main")

# FIX2 — DRY_RUN fail-closed. Only an EXPLICIT, allow-listed APPLY token flips a
# run into mutate mode; anything else (unset, empty, unrecognized, or any of the
# DRY tokens) resolves to DRY-RUN. The legacy logic was fail-OPEN (unset DRY_RUN
# -> APPLY), which could mutate the live store on a misconfigured deploy.
_APPLY_TOKENS = frozenset({"apply", "false", "0", "no", "off"})


def _resolve_dry_run() -> bool:
    """Return True (DRY-RUN) unless DRY_RUN carries an explicit APPLY token."""
    raw = os.environ.get("DRY_RUN")
    if raw is None:
        return True
    return raw.strip().lower() not in _APPLY_TOKENS


def main():
    """Entry point del job REORDER"""

    # Leggi RUN_MODE
    run_mode = os.environ.get("RUN_MODE", "").upper()

    if not run_mode:
        logger.error("=" * 70)
        logger.error("ERRORE: ENV variable RUN_MODE non configurata!")
        logger.error("")
        logger.error("Configura RUN_MODE su Render:")
        logger.error('  - RUN_MODE=REORDER    (per riordino collection)')
        logger.error("=" * 70)
        sys.exit(1)

    # Valida RUN_MODE (SYNC e FIX_PRICES non esistono più: vedi la docstring)
    if run_mode != "REORDER":
        logger.error("=" * 70)
        logger.error(f"ERRORE: RUN_MODE='{run_mode}' non valido!")
        logger.error("")
        logger.error("Valori ammessi:")
        logger.error('  - RUN_MODE=REORDER')
        logger.error("=" * 70)
        sys.exit(1)

    logger.info("=" * 70)
    logger.info("ENTRY POINT - Sync-Scansia")
    logger.info(f"RUN_MODE: {run_mode}")
    logger.info("=" * 70)

    run_reorder()

def run_reorder():
    """Esegue reorder_collection.py"""
    logger.info("")
    logger.info("🔄 Esecuzione REORDER - Riordino Collection per Sconto %")
    logger.info("")

    # DEBUG: Verifica variabili Shopify
    shopify_store = os.environ.get("SHOPIFY_STORE", "NOT_SET")
    shopify_token = os.environ.get("SHOPIFY_ADMIN_TOKEN", "NOT_SET")
    logger.info(f"DEBUG: SHOPIFY_STORE = {shopify_store}")
    logger.info(f"DEBUG: SHOPIFY_ADMIN_TOKEN = {'***' if shopify_token != 'NOT_SET' else 'NOT_SET'}")
    logger.info("")

    # Verifica COLLECTION_ID
    collection_id = os.environ.get("COLLECTION_ID", "").strip()
    
    if not collection_id:
        logger.error("=" * 70)
        logger.error("ERRORE: ENV variable COLLECTION_ID non configurata!")
        logger.error("")
        logger.error("Per REORDER è richiesto:")
        logger.error("  COLLECTION_ID=262965428289  (ID numerico collection)")
        logger.error("")
        logger.error("Trova l'ID su Shopify:")
        logger.error("  Admin → Collections → [tua collection]")
        logger.error("  URL: .../collections/[ID]")
        logger.error("=" * 70)
        sys.exit(1)
    
    # Valida che sia numerico
    if not collection_id.isdigit():
        logger.error("=" * 70)
        logger.error(f"ERRORE: COLLECTION_ID='{collection_id}' non è valido!")
        logger.error("")
        logger.error("Deve essere un ID numerico, esempio:")
        logger.error("  COLLECTION_ID=262965428289  ✅")
        logger.error("")
        logger.error("NON usare:")
        logger.error("  gid://shopify/Collection/...  ❌")
        logger.error("=" * 70)
        sys.exit(1)
    
    logger.info(f"Collection ID: {collection_id}")
    logger.info("")

    # FIX2 — fail-closed: default DRY-RUN salvo token APPLY esplicito.
    dry_run = _resolve_dry_run()
    mode_str = "DRY-RUN (nessuna modifica)" if dry_run else "APPLY (applica modifiche)"
    logger.info(f"Modalità: {mode_str}")
    logger.info("")

    try:
        # Import e esegui reorder
        from src import reorder_collection

        # Salva sys.argv originale
        original_argv = sys.argv

        # Imposta args per reorder (aggiungi --apply solo se NON dry-run)
        args = ["reorder_collection.py", "--collection-id", collection_id]
        if not dry_run:
            args.append("--apply")

        sys.argv = args
        
        try:
            # Esegui main di reorder
            reorder_collection.main()
            logger.info("")
            logger.info("✅ REORDER completato con successo")
        finally:
            # Ripristina sys.argv
            sys.argv = original_argv
            
    except Exception as e:
        logger.error("")
        logger.error("❌ REORDER fallito: %s", e, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
