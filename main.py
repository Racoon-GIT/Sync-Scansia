#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py - Entry point unificato per sync e reorder

Esecuzione:
    python -m src.main

ENV Variables:
    RUN_MODE: "SYNC" o "REORDER" (obbligatorio)
    
    Per SYNC:
        (usa tutte le ENV già configurate)
    
    Per REORDER:
        COLLECTION_ID: ID numerico collection (es: 262965428289)
"""

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("main")

def main():
    """Entry point unificato"""
    
    # Leggi RUN_MODE
    run_mode = os.environ.get("RUN_MODE", "").upper()
    
    if not run_mode:
        logger.error("=" * 70)
        logger.error("ERRORE: ENV variable RUN_MODE non configurata!")
        logger.error("")
        logger.error("Configura RUN_MODE su Render:")
        logger.error('  - RUN_MODE=SYNC       (per sincronizzazione outlet)')
        logger.error('  - RUN_MODE=REORDER    (per riordino collection)')
        logger.error("=" * 70)
        sys.exit(1)
    
    # Valida RUN_MODE
    if run_mode not in ["SYNC", "REORDER"]:
        logger.error("=" * 70)
        logger.error(f"ERRORE: RUN_MODE='{run_mode}' non valido!")
        logger.error("")
        logger.error("Valori ammessi:")
        logger.error('  - RUN_MODE=SYNC')
        logger.error('  - RUN_MODE=REORDER')
        logger.error("=" * 70)
        sys.exit(1)
    
    logger.info("=" * 70)
    logger.info("ENTRY POINT UNIFICATO - Sync-Scansia")
    logger.info(f"RUN_MODE: {run_mode}")
    logger.info("=" * 70)
    
    # Esegui tool appropriato
    if run_mode == "SYNC":
        run_sync()
    elif run_mode == "REORDER":
        run_reorder()

def run_sync():
    """Esegue sync.py"""
    logger.info("")
    logger.info("🔄 Esecuzione SYNC - Creazione/Aggiornamento Outlet")
    logger.info("")
    
    try:
        # Import e esegui sync
        from src import sync
        
        # Simula args --apply (modalità reale)
        class Args:
            apply = True
        
        # Salva sys.argv originale
        original_argv = sys.argv
        
        # Imposta args per sync
        sys.argv = ["sync.py", "--apply"]
        
        try:
            # Esegui main di sync
            sync.main()
            logger.info("")
            logger.info("✅ SYNC completato con successo")
        finally:
            # Ripristina sys.argv
            sys.argv = original_argv
            
    except Exception as e:
        logger.error("")
        logger.error("❌ SYNC fallito: %s", e, exc_info=True)
        sys.exit(1)

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
    
    try:
        # Import e esegui reorder
        from src import reorder_collection
        
        # Salva sys.argv originale
        original_argv = sys.argv
        
        # Imposta args per reorder
        sys.argv = ["reorder_collection.py", "--collection-id", collection_id, "--apply"]
        
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
