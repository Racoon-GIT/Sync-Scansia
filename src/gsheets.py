# src/gsheets.py
import os
import io
import json
import logging
import pandas as pd
import requests
from typing import Optional

log = logging.getLogger("sync.gsheets")


def public_csv_url(sheet_url: str) -> str:
    base = sheet_url.split("/edit")[0]
    return f"{base}/export?format=csv"


def load_table(sheet_url: str) -> pd.DataFrame:
    log.info("Scarico sorgente dati da URL")
    csv_url = public_csv_url(sheet_url)
    log.debug("URL di download: %s", csv_url)
    r = requests.get(csv_url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    log.info("Tabella caricata da URL: %d righe, %d colonne", len(df), len(df.columns))
    log.debug("Colonne: %s", list(df.columns))
    return df


def writeback_product_id(sheet_url: str, df_original: pd.DataFrame, df_processed: pd.DataFrame,
                         key_cols=("SKU", "TAGLIA"), product_id_col="product_id") -> int:
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        log.info("GOOGLE_SERVICE_ACCOUNT_JSON non configurato: write-back disabilitato.")
        log.info("Write-back GSheet saltato (no credenziali).");
        return 0

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except Exception:
        log.warning("Librerie Google non presenti. Aggiungi gspread + google-auth.")
        return 0

    creds_info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)

    spreadsheet_id = sheet_url.split("/d/")[1].split("/")[0]
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.sheet1

    data = ws.get_all_records()
    rows = len(data)
    if rows == 0:
        return 0

    import pandas as pd
    df_g = pd.DataFrame(data)
    updated = 0
    df_idx = df_processed.set_index(list(key_cols))

    if "Product_Id" not in df_g.columns:
        df_g["Product_Id"] = ""

    for i, row in df_g.iterrows():
        key = tuple(str(row[c]) for c in key_cols)
        if key in df_idx.index:
            pid = df_idx.loc[key, product_id_col]
            if pid and str(row.get("Product_Id", "")).strip() == "":
                row_num = i + 2
                col_num = list(df_g.columns).index("Product_Id") + 1
                ws.update_cell(row_num, col_num, pid)
                updated += 1

    log.info("Write-back Product_id completato: %d righe aggiornate.", updated)
    return updated
