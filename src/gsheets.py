# src/gsheets.py
from __future__ import annotations

import csv
import json
import logging
import os
from pathlib import Path
from typing import Dict, List


logger = logging.getLogger("gsheets")


def _load_from_json_env() -> List[Dict]:
    """
    Se esiste la env UPDATES_JSON con un array JSON di oggetti
    { "sku": "...", "taglia": "...", "product_id": "..." }, lo usa.
    """
    raw = os.getenv("UPDATES_JSON")
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            logger.info("Caricati %d update da env UPDATES_JSON", len(data))
            return data  # type: ignore[return-value]
        logger.warning("UPDATES_JSON non è una lista JSON; ignorato.")
    except Exception as e:
        logger.error("JSON in UPDATES_JSON non valido: %s", e)
    return []


def _load_from_csv_env() -> List[Dict]:
    """
    Se esiste la env UPDATES_CSV che punta a un file CSV con colonne
    sku, taglia, product_id, lo carica. Utile per debugging locale.
    """
    path = os.getenv("UPDATES_CSV")
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        logger.error("File CSV non trovato: %s", p)
        return []
    rows: List[Dict] = []
    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(
                {
                    "sku": (r.get("sku") or "").strip(),
                    "taglia": (r.get("taglia") or "").strip(),
                    "product_id": (r.get("product_id") or "").strip(),
                }
            )
    logger.info("Caricati %d update da CSV %s", len(rows), p)
    return rows


def get_updates() -> List[Dict]:
    """
    Punto d’ingresso usato da sync.py.
    **Ordine di priorità**:
      1) env UPDATES_JSON
      2) env UPDATES_CSV (per debug)
      3) fallback: lista vuota (nessun errore, così il Cron non fallisce)
    Se nel tuo progetto leggi da Google Sheets, sostituisci qui la logica
    con la chiamata al tuo client (es. gspread) e restituisci una lista di dict.
    """
    # 🔒 Non fare self-import qui (Niente: `from . import gsheets as gs`)
    data = _load_from_json_env()
    if data:
        return data
    data = _load_from_csv_env()
    if data:
        return data
    logger.warning("Nessuna fonte configurata (UPDATES_JSON/UPDATES_CSV). Restituisco []")
    return []


def write_run_status(success: bool, applied: int) -> None:
    """
    Hook opzionale per tracciare l’esito del run.
    Puoi adattarlo per scrivere su un foglio di log, su DB, ecc.
    Per ora fa solo log (non introduce dipendenze extra).
    """
    logger.info("Run status: success=%s, applied=%d", success, applied)
