# -*- coding: utf-8 -*-
"""
gsheets.py — Loader Google Sheets con normalizzazione header.
Compatibile con l'ambiente atteso dal tuo progetto.

Richiede credenziali Google già configurate (come prima).
Legge:
- SPREADSHEET_ID (env) se non passato esplicitamente
- WORKSHEET_NAME (env) default: "Scarpe_in_Scansia"
"""

import json
import logging
import os
from typing import Dict, List, Any

# gspread è il client de-facto per Google Sheets
# (usa le stesse credenziali che stavi già impiegando)
import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

# Scope minimo per leggere Sheets
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


def _service_account_from_env() -> Credentials:
    """
    Costruisce le credenziali da variabile d'ambiente GOOGLE_CREDENTIALS_JSON
    (stringa JSON). In alternativa, usa file puntato da GOOGLE_APPLICATION_CREDENTIALS.
    """
    cred_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if cred_json:
        try:
            info = json.loads(cred_json)
            return Credentials.from_service_account_info(info, scopes=_SCOPES)
        except Exception as e:
            logger.error("Errore parsing GOOGLE_CREDENTIALS_JSON: %s", e)
            raise

    # fallback: file su disco (standard Google)
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path:
        raise RuntimeError(
            "Credenziali non trovate. Imposta GOOGLE_CREDENTIALS_JSON o GOOGLE_APPLICATION_CREDENTIALS"
        )
    return Credentials.from_service_account_file(path, scopes=_SCOPES)


def _normalize_key(k: str) -> str:
    """lower + trim + sostituisce spazi e '-' con '_'."""
    return (k or "").strip().lower().replace("-", "_").replace(" ", "_")


def _norm_row(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizza le chiavi dell'intera riga (case-insensitive, alias comuni).
    """
    m = {_normalize_key(k): v for k, v in d.items()}

    # alias comuni per product_id
    if "productid" in m and "product_id" not in m:
        m["product_id"] = m["productid"]

    # Tentativi soft in caso di varianti strane
    for alt in ("product-id", "product id", "product__id"):
        alt_n = _normalize_key(alt)
        if alt_n in m and "product_id" not in m:
            m["product_id"] = m[alt_n]

    return m


def _open_worksheet(spreadsheet_id: str, worksheet_name: str):
    creds = _service_account_from_env()
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    try:
        return sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        # In alcuni casi il nome può avere spazi o maiuscole diverse:
        # facciamo un tentativo più tollerante
        names = [ws.title for ws in sh.worksheets()]
        logger.error(
            "Worksheet '%s' non trovato. Disponibili: %s",
            worksheet_name,
            names,
        )
        raise


def load_rows(
    spreadsheet_id: str | None = None,
    worksheet_name: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Carica righe dal worksheet e restituisce una lista di dict uniformati:
    {
      "sku": str,
      "taglia": str,
      "product_id": str,
      "online": Any
    }
    """
    spreadsheet_id = spreadsheet_id or os.environ.get("SPREADSHEET_ID")
    worksheet_name = worksheet_name or os.environ.get("WORKSHEET_NAME", "Scarpe_in_Scansia")

    if not spreadsheet_id:
        raise RuntimeError("SPREADSHEET_ID non impostato (env o parametro).")

    ws = _open_worksheet(spreadsheet_id, worksheet_name)

    # get_all_records usa la prima riga come header
    raw_rows = ws.get_all_records()
    logger.info(
        "Caricate %d righe da Google Sheets (worksheet=%s)",
        len(raw_rows),
        worksheet_name,
    )

    rows: List[Dict[str, Any]] = []
    for r in raw_rows:
        n = _norm_row(r)
        rows.append(
            {
                "sku": str(n.get("sku", "") or "").strip(),
                "taglia": str(n.get("taglia", "") or "").strip(),
                "product_id": str(n.get("product_id", "") or "").strip(),
                "online": n.get("online"),
            }
        )

    # Log di controllo (primi header rilevati nella prima riga)
    if raw_rows:
        sample_keys = list(_norm_row(raw_rows[0]).keys())
        logger.debug("Header normalizzati (prima riga): %s", sample_keys)

    return rows
