# src/sync.py
from __future__ import annotations

import argparse
import logging
import os
import sys
import traceback
from typing import Dict, List, Tuple

from . import gsheets as gs

logger = logging.getLogger("sync")
VERSION = "sync-2025-08-25-01"

def configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger.debug("Logging configured at level %s", level)

def build_updates_map(updates: List[dict]) -> Dict[Tuple[str, str], str]:
    mapping: Dict[Tuple[str, str], str] = {
        (u.get("sku", "").strip(), u.get("taglia", "").strip()): str(u["product_id"])
        for u in updates
        if "sku" in u and "product_id" in u
    }
    logger.info("Caricati %d aggiornamenti (chiavi uniche: %d)", len(updates), len(mapping))
    missing = [u for u in updates if "sku" not in u or "product_id" not in u]
    if missing:
        logger.warning("Righe con campi mancanti (sku/product_id): %d", len(missing))
    return mapping

def run_sync_once(apply: bool) -> int:
    source = gs.last_source_used.clear_and_get()  # reset/placeholder
    updates = gs.get_updates()
    source = gs.last_source_used.value or "unknown"
    logger.info("Origine dati: %s", source)
    logger.info("Updates ricevuti: %d", len(updates))

    updates_map = build_updates_map(updates)

    if not apply:
        preview = list(updates_map.items())[:5]
        logger.info("DRY-RUN: prime %d chiavi: %s", len(preview), preview)
        return 0

    applied = len(updates_map)
    logger.info("APPLY: simulata applicazione di %d aggiornamenti.", applied)
    gs.write_run_status(success=True, applied=applied)
    return 0

def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Scansia - Cron runner")
    parser.add_argument("--apply", action="store_true", help="Se presente, applica le modifiche (non solo dry-run).")
    return parser.parse_args(argv)

def main(argv: List[str]) -> int:
    configure_logging()
    logger.info("Avvio sync %s", VERSION)
    args = parse_args(argv)
    logger.info("apply=%s", args.apply)
    code = run_sync_once(apply=args.apply)
    logger.info("Termine sync con exit code %d", code)
    return code

if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Exception:
        traceback.print_exc()
        sys.exit(1)
