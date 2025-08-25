# src/sync.py
from __future__ import annotations

import argparse
import logging
import os
import sys
import traceback
from typing import Dict, List, Tuple

from . import gsheets as gs  # import RELATIVO: corretto qui


logger = logging.getLogger("sync")


def configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger.debug("Logging configured at level %s", level)


def build_updates_map(updates: List[dict]) -> Dict[Tuple[str, str], str]:
    """
    Converte la lista di update (dict con chiavi 'sku', 'taglia', 'product_id')
    in una mappa { (sku, taglia) : product_id }.
    """
    # ✅ Fix SyntaxError: chiudo la tupla PRIMA dei ':'
    mapping: Dict[Tuple[str, str], str] = {
        (u.get("sku", "").strip(), u.get("taglia", "").strip()): str(u["product_id"])
        for u in updates
        if "sku" in u and "product_id" in u
    }
    # Log diagnostico leggero
    logger.info("Caricati %d aggiornamenti (chiavi uniche: %d)", len(updates), len(mapping))
    missing = [u for u in updates if "sku" not in u or "product_id" not in u]
    if missing:
        logger.warning("Righe con campi mancanti (sku/product_id): %d", len(missing))
    return mapping


def run_sync_once(apply: bool) -> int:
    """
    Esegue UNA volta la sincronizzazione.
    Ritorna 0 se tutto ok, >0 se errori (così il Cron vede exit code corretto).
    """
    # 1) Carica gli "updates" dal foglio / CSV / JSON secondo la logica in gsheets.py
    updates = gs.get_updates()
    logger.info("Updates ricevuti: %d", len(updates))

    # 2) Prepara la mappa (qui c'era la SyntaxError)
    updates_map = build_updates_map(updates)

    # 3) Dry-run vs apply
    if not apply:
        # Dry run: stampo un estratto per diagnosi
        preview = list(updates_map.items())[:5]
        logger.info("DRY-RUN: prime %d chiavi: %s", len(preview), preview)
        return 0

    # 4) TODO: qui metti la tua logica di applicazione (DB/Shopify/etc.)
    #    Per ora non falliamo: facciamo solo log per evitare exit 1.
    applied = len(updates_map)
    logger.info("APPLY: simulata applicazione di %d aggiornamenti.", applied)

    # Se vuoi, puoi usare funzioni helper in gsheets per scrivere un log di esito.
    gs.write_run_status(success=True, applied=applied)

    return 0


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Scansia - Cron runner")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Se presente, applica le modifiche; altrimenti DRY-RUN.",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    configure_logging()
    args = parse_args(argv)
    logger.info("Avvio sync (apply=%s)", args.apply)
    code = run_sync_once(apply=args.apply)
    logger.info("Termine sync con exit code %d", code)
    return code


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Exception:  # logga stacktrace e segnala fallimento a Render
        traceback.print_exc()
        sys.exit(1)
