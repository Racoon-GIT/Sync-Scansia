import os
import json
import logging
from typing import Optional, List, Dict

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

LOG = logging.getLogger("sync.gsheets")


def read_public_csv(url: str) -> pd.DataFrame:
    import requests, io
    if "/edit" in url:
        url = url.split("/edit")[0] + "/export?format=csv"
    elif "/view" in url:
        url = url.split("/view")[0] + "/export?format=csv"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content))


def _get_client_from_env() -> Optional[gspread.Client]:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        LOG.info("GOOGLE_SERVICE_ACCOUNT_JSON non configurato: write-back disabilitato.")
        return None
    try:
        data = json.loads(raw)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(data, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        LOG.error("Errore creazione client service account: %s", e)
        return None


def write_back_product_ids(sheet_url: str, updates: List[Dict]):
    """Aggiorna la colonna Product_Id per le righe (sku, taglia) passate.
    updates: list di dict { 'sku':..., 'taglia':..., 'product_id': int }
    """
    client = _get_client_from_env()
    if not client:
        LOG.info("Write-back GSheet saltato (no credenziali)." )
        return 0

    # Apri lo sheet e prima worksheet
    try:
        sh = client.open_by_url(sheet_url)
        ws = sh.sheet1
    except Exception as e:
        LOG.error("Errore apertura sheet: %s", e)
        return 0

    # Leggi tutte le righe
    values = ws.get_all_records()
    if not values:
        return 0

    # Trova indici colonne
    header = [h.strip() for h in ws.row_values(1)]
    try:
        col_sku = header.index("SKU") + 1
    except ValueError:
        try:
            col_sku = header.index("sku") + 1
        except ValueError:
            LOG.error("Colonna SKU non trovata per write-back")
            return 0

    def _find_col(names):
        for n in names:
            if n in header:
                return header.index(n) + 1
        return None

    col_taglia = _find_col(["TAGLIA", "taglia", "Size"])
    col_pid = _find_col(["Product_Id", "product_id", "Product ID"]) or len(header) + 1
    if col_pid > len(header):
        # aggiungi intestazione se manca
        ws.update_cell(1, col_pid, "Product_Id")

    # Mappa per ricerca rapida
    updates_map = {(u["sku"], u.get("taglia", ""): u["product_id"]) for u in updates}

    # Aggiorna celle
    updated = 0
    rng_updates = []
    for i, row in enumerate(values, start=2):  # partendo dalla riga 2
        sku = str(row.get("SKU", row.get("sku", ""))).strip()
        taglia = str(row.get("TAGLIA", row.get("taglia", row.get("Size", "")))).strip()
        key = (sku, taglia)
        if key in updates_map:
            pid = list(filter(lambda x: (x[0], x[1]) == key, updates_map))[0][2] if isinstance(updates_map, list) else None
        # simpler
    updated = 0
    # seconda passata più semplice
    for i, row in enumerate(values, start=2):
        sku = str(row.get("SKU", row.get("sku", ""))).strip()
        taglia = str(row.get("TAGLIA", row.get("taglia", row.get("Size", "")))).strip()
        for u in updates:
            if sku == u.get("sku") and (not col_taglia or taglia == u.get("taglia", taglia)):
                ws.update_cell(i, col_pid, u["product_id"])
                updated += 1
                break
    LOG.info("Write-back Product_id completato: %d righe aggiornate.", updated)
    return updated
