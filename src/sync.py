# -*- coding: utf-8 -*-
"""
sync.py — Orchestratore di sincronizzazione.
Esegui con:
    python -m src.sync --apply
Opzioni:
    --apply            Applica davvero (altrimenti dry-run)
    --spreadsheet-id   Override dello spreadsheet (altrimenti usa env SPREADSHEET_ID)
    --worksheet        Override del worksheet (default/env: Scarpe_in_Scansia)
"""

import argparse
import logging
import os
from collections import Counter, defaultdict
from typing import Dict, List, Any, Tuple

from .gsheets import load_rows

# Config logging di default (puoi delegarlo al tuo runner)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("sync")


def _is_selected(v: Any) -> bool:
    """Flag 'online' tollerante: True/1/'yes'/'si'/'sì'/'x'/'ok'."""
    if v is True:
        return True
    # numerici
    try:
        if isinstance(v, (int, float)) and int(v) == 1:
            return True
    except Exception:
        pass
    # stringhe
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "si", "sì", "x", "ok"}
    return False


def _make_key(row: Dict[str, Any]) -> str:
    """
    Ordine di preferenza:
    1) product_id
    2) sku::taglia
    3) sku
    """
    pid = (row.get("product_id") or "").strip()
    if pid:
        return pid
    sku = (row.get("sku") or "").strip()
    taglia = (row.get("taglia") or "").strip()
    if sku and taglia:
        return f"{sku}::{taglia}"
    return sku  # può essere "" → verrà diagnosticato


def _group_updates(selected_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Raggruppa le righe selezionate per chiave.
    """
    bucket: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in selected_rows:
        bucket[_make_key(r)].append(r)
    return bucket


def _apply_updates(grouped: Dict[str, List[Dict[str, Any]]]) -> Tuple[int, List[str]]:
    """
    Qui va la tua logica di applicazione reale (API Shopify / DB / ecc.).
    Per continuità coi tuoi log, teniamo la semantica "applicazione simulata"
    se non desideri ancora fare side-effect qui dentro.
    Ritorna: (num_applicate, elenco_chiavi_applicate)
    """
    applied = 0
    keys_done: List[str] = []

    # Esempio: 1 update per chiave (primo record del gruppo)
    for key, rows in grouped.items():
        if not key:
            # skip: chiave vuota (diagnosticata già prima)
            continue
        # TODO: sostituisci qui con azione reale (DB/API).
        # Per ora logghiamo e consideriamo "applicata".
        logger.debug("Applico update per chiave %s (righe: %d)", key, len(rows))
        applied += 1
        keys_done.append(key)

    return applied, keys_done


def run_sync(rows: List[Dict[str, Any]], do_apply: bool) -> None:
    # Filtra per flag 'online'
    selected = [r for r in rows if _is_selected(r.get("online"))]
    logger.info("Righe totali: %d, selezionate (online=TRUE): %d", len(rows), len(selected))

    # Diagnostica chiavi
    keys = [_make_key(r) for r in selected]
    cnt = Counter(keys)
    logger.info("Chiavi uniche tra i selezionati: %d", len(cnt))
    if cnt:
        logger.debug("Esempi chiavi (max 5): %s", list(dict(cnt.most_common(5)).keys()))
    missing_key = sum(1 for k in keys if not k)
    if missing_key:
        logger.warning("Righe selezionate SENZA chiave: %d (product_id/sku/taglia vuoti)", missing_key)

    # Raggruppamento
    grouped = _group_updates(selected)

    # Apply o dry-run
    if do_apply:
        applied_count, keys_done = _apply_updates(grouped)
        logger.info("APPLY: applicazione di %d aggiornamenti.", applied_count)
        if applied_count and len(keys_done) <= 10:
            logger.debug("Chiavi applicate: %s", keys_done)
    else:
        logger.info("DRY-RUN: nessuna applicazione eseguita. (Avvia con --apply per applicare)")

    # Extra: riepilogo “perché non applicati”
    not_applied_reasons = []
    if not selected:
        not_applied_reasons.append("Nessuna riga selezionata (flag 'online' non attivo).")
    if missing_key:
        not_applied_reasons.append("Righe senza chiave (product_id/sku/taglia mancanti).")

    if not_applied_reasons:
        for r in not_applied_reasons:
            logger.info("Motivo possibile mancata applicazione: %s", r)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sincronizzazione Scarpe in Scansia")
    parser.add_argument("--apply", action="store_true", help="Applica davvero gli aggiornamenti")
    parser.add_argument(
        "--spreadsheet-id",
        default=os.environ.get("SPREADSHEET_ID"),
        help="ID dello spreadsheet Google (default: env SPREADSHEET_ID)",
    )
    parser.add_argument(
        "--worksheet",
        default=os.environ.get("WORKSHEET_NAME", "Scarpe_in_Scansia"),
        help="Nome del worksheet (default/env: Scarpe_in_Scansia)",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Livello di log (DEBUG, INFO, WARNING, ...)",
    )

    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level.upper())

    logger.info("Avvio sync")
    logger.info("apply=%s", args.apply)

    rows = load_rows(args.spreadsheet_id, args.worksheet)
    run_sync(rows, do_apply=args.apply)

    logger.info("Termine sync con exit code 0")


if __name__ == "__main__":
    main()
