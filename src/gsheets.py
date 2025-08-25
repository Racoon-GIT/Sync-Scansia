# -*- coding: utf-8 -*-
"""
gsheets.py — Lettura/normalizzazione righe da Google Sheets + write-back Product_Id.

Env richieste:
  GSPREAD_SHEET_ID
  GSPREAD_WORKSHEET_TITLE
  GOOGLE_CREDENTIALS_JSON   (oppure GOOGLE_APPLICATION_CREDENTIALS)

Ritorna righe normalizzate con chiavi:
  brand, modello, titolo, sku, taglia, qta, online,
  prezzo_pieno, prezzo_scontato, product_id
"""

import json
import logging
import os
from typing import Any, Dict, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _service_account_from_env() -> Credentials:
    cred_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if cred_json:
        info = json.loads(cred_json)
        return Credentials.from_service_account_info(info, scopes=_SCOPES)
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path:
        raise RuntimeError(
            "Credenziali non trovate. Imposta GOOGLE_CREDENTIALS_JSON o GOOGLE_APPLICATION_CREDENTIALS"
        )
    return Credentials.from_service_account_file(path, scopes=_SCOPES)

def _normalize_key(k: str) -> str:
    return (k or "").strip().lower().replace("-", "_").replace(" ", "_")

# mappa sinonimi -> chiave canonica
_CANON = {
    "brand": "brand",
    "modello": "modello",
    "model": "modello",
    "titolo": "titolo",
    "title": "titolo",
    "sku": "sku",
    "taglia": "taglia",
    "size": "taglia",
    "qta": "qta",
    "qty": "qta",
    "quantita": "qta",
    "quantità": "qta",
    "online": "online",
    "prezzo_pieno": "prezzo_pieno",
    "prezzo_full": "prezzo_pieno",
    "compare_at_price": "prezzo_pieno",
    "prezzo_scontato": "prezzo_scontato",
    "price": "prezzo_scontato",
    "product_id": "product_id",
    "productid": "product_id",
    "product_id_": "product_id",
    "product__id": "product_id",
    "product_id_sheet": "product_id",
}

def _canon_row(d: Dict[str, Any]) -> Dict[str, Any]:
    n = {}
    for k, v in d.items():
        key = _normalize_key(k)
        key = _CANON.get(key, key)
        n[key] = v
    # alias tardivi
    if "product_id" not in n:
        for alt in ("product-id", "product id"):
            altn = _normalize_key(alt)
            if altn in n:
                n["product_id"] = n[altn]
                break
    return n

def _open_ws():
    creds = _service_account_from_env()
    client = gspread.authorize(creds)
    sheet_id = os.environ["GSPREAD_SHEET_ID"]
    ws_title = os.environ["GSPREAD_WORKSHEET_TITLE"]
    sh = client.open_by_key(sheet_id)
    return sh, sh.worksheet(ws_title)

def load_rows() -> Tuple[List[Dict[str, Any]], gspread.Worksheet, Dict[str, int]]:
    """
    Ritorna: (rows_normalized, worksheet, header_index)
    header_index: mappa chiave normalizzata -> col_idx (1-based)
    """
    sh, ws = _open_ws()
    values = ws.get_all_values()
    if not values:
        return [], ws, {}
    header = values[0]
    body = values[1:]

    # mappa header normalizzati -> indice colonna (1-based)
    header_idx: Dict[str, int] = {}
    for i, h in enumerate(header, start=1):
        hn = _normalize_key(h)
        hn = _CANON.get(hn, hn)
        header_idx[hn] = i

    rows: List[Dict[str, Any]] = []
    for ridx, row in enumerate(body, start=2):  # 2 = prima riga dopo header
        raw = {header[i-1]: (row[i-1] if i-1 < len(row) else "") for i in range(1, len(header)+1)}
        n = _canon_row(raw)
        # aggiungi indice riga per write-back
        n["_row_index"] = ridx
        rows.append({
            "brand": str(n.get("brand", "") or "").strip(),
            "modello": str(n.get("modello", "") or "").strip(),
            "titolo": str(n.get("titolo", "") or "").strip(),
            "sku": str(n.get("sku", "") or "").strip(),
            "taglia": str(n.get("taglia", "") or "").strip(),
            "qta": str(n.get("qta", "") or "").strip(),
            "online": n.get("online"),
            "prezzo_pieno": str(n.get("prezzo_pieno", "") or "").strip(),
            "prezzo_scontato": str(n.get("prezzo_scontato", "") or "").strip(),
            "product_id": str(n.get("product_id", "") or "").strip(),
            "_row_index": n["_row_index"],
        })

    logger.debug("Header normalizzati: %s", list(header_idx.keys()))
    logger.info("Caricate %d righe da Google Sheets (worksheet=%s)", len(rows), ws.title)
    return rows, ws, header_idx

def write_product_id(ws: gspread.Worksheet, header_idx: Dict[str, int],
                     row_index: int, product_gid: str) -> None:
    """
    Scrive il gid Shopify nella colonna Product_Id (o equivalente) della riga (1-based).
    """
    col = header_idx.get("product_id")
    if not col:
        # prova a trovare una intestazione compatibile
        for k in ("product_id", "productid", "product-id", "product id"):
            k2 = _normalize_key(k)
            if k2 in header_idx:
                col = header_idx[k2]
                break
    if not col:
        raise RuntimeError("Colonna Product_Id non trovata nel worksheet.")
    ws.update_cell(row_index, col, product_gid)
