# src/gsheets.py
from __future__ import annotations

import csv
import json
import logging
import os
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger("gsheets")

class _SourceFlag:
    def __init__(self) -> None:
        self.value = None
    def set(self, v: str) -> None:
        self.value = v
    def clear_and_get(self):
        self.value = None
        return self
last_source_used = _SourceFlag()

# ---------------------------
# FONTE 1: ENV JSON (debug)
# ---------------------------
def _load_from_json_env() -> List[Dict]:
    raw = os.getenv("UPDATES_JSON")
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            last_source_used.set("env:UPDATES_JSON")
            logger.info("Caricati %d update da env UPDATES_JSON", len(data))
            return data  # type: ignore[return-value]
        logger.warning("UPDATES_JSON non è una lista JSON; ignorato.")
    except Exception as e:
        logger.error("JSON in UPDATES_JSON non valido: %s", e)
    return []

# ---------------------------
# FONTE 2: CSV (debug)
# ---------------------------
def _load_from_csv_env() -> List[Dict]:
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
    last_source_used.set(f"csv:{p}")
    logger.info("Caricati %d update da CSV %s", len(rows), p)
    return rows

# ---------------------------
# FONTE 3: Google Sheets (prod)
# ---------------------------
def _load_from_gsheets() -> List[Dict]:
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    sheet_id = os.getenv("GSPREAD_SHEET_ID")
    ws_title = os.getenv("GSPREAD_WORKSHEET_TITLE")
    gs_range = os.getenv("GSPREAD_RANGE")

    # Diagnostica early
    if not creds_json or not sheet_id:
        missing = []
        if not creds_json: missing.append("GOOGLE_CREDENTIALS_JSON")
        if not sheet_id:   missing.append("GSPREAD_SHEET_ID")
        if missing:
            logger.warning("GSheets: variabili mancanti: %s", ", ".join(missing))
        return []

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except Exception as e:
        logger.error("Dipendenze gspread/google-auth mancanti: %s", e)
        return []

    try:
        info = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        credentials = Credentials.from_service_account_info(info, scopes=scopes)
        client = gspread.authorize(credentials)

        sh = client.open_by_key(sheet_id)

        if gs_range:
            values = sh.values_get(gs_range).get("values", [])
            if not values:
                last_source_used.set(f"gsheets:range:{gs_range} (vuoto)")
                logger.warning("GSheets: range %s vuoto", gs_range)
                return []
            headers = [h.strip().lower() for h in values[0]]
            rows = []
            for row in values[1:]:
                rec = {headers[i]: (row[i].strip() if i < len(row) else "") for i in range(len(headers))}
                rows.append(
                    {
                        "sku": rec.get("sku", ""),
                        "taglia": rec.get("taglia", ""),
                        "product_id": rec.get("product_id", ""),
                    }
                )
            last_source_used.set(f"gsheets:range:{gs_range}")
            logger.info("Caricati %d update da Google Sheets (range=%s)", len(rows), gs_range)
            return rows

        ws = sh.worksheet(ws_title) if ws_title else sh.sheet1
        data = ws.get_all_records()  # prima riga come header
        rows = []
        for r in data:
            rows.append(
                {
                    "sku": (str(r.get("sku", ""))).strip(),
                    "taglia": (str(r.get("taglia", ""))).strip(),
                    "product_id": (str(r.get("product_id", ""))).strip(),
                }
            )
        last_source_used.set(f"gsheets:worksheet:{ws.title}")
        logger.info("Caricati %d update da Google Sheets (worksheet=%s)", len(rows), ws.title)
        return rows

    except Exception as e:
        logger.error("Errore lettura Google Sheets: %s", e)
        return []

# ---------------------------
# Entry usato da sync.py
# ---------------------------
def get_updates() -> List[Dict]:
    # 1) env JSON
    data = _load_from_json_env()
    if data:
        return data

    # 2) CSV
    data = _load_from_csv_env()
    if data:
        return data

    # 3) Google Sheets
    data = _load_from_gsheets()
    if data:
        return data

    logger.warning("Nessuna fonte configurata (UPDATES_JSON/UPDATES_CSV/GSheets). Restituisco []")
    return []

def write_run_status(success: bool, applied: int) -> None:
    logger.info("Run status: success=%s, applied=%d", success, applied)
