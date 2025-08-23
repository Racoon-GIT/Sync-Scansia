import os, json, logging
from typing import List, Dict
import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger("sync.gsheets")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _auth_from_env():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        logger.info("GOOGLE_SERVICE_ACCOUNT_JSON non configurato: write-back disabilitato.")
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.error("GOOGLE_SERVICE_ACCOUNT_JSON non è un JSON valido.")
        return None
    creds = Credentials.from_service_account_info(data, scopes=SCOPES)
    return gspread.authorize(creds)

def _find_or_create_product_id_column(ws) -> int:
    header = ws.row_values(1)
    header_l = [h.strip().lower() for h in header]
    if "product_id" in header_l:
        return header_l.index("product_id") + 1
    col = len(header) + 1
    ws.update_cell(1, col, "Product_id")
    logger.info(f"Aggiunta colonna 'Product_id' in posizione {col}")
    return col

def write_product_ids(sheet_url: str, rows_to_write: List[Dict], worksheet_name: str | None=None):
    gc = _auth_from_env()
    if not gc:
        logger.info("Write-back GSheet saltato (no credenziali).")
        return 0

    sh = gc.open_by_url(sheet_url)
    ws = sh.worksheet(worksheet_name) if worksheet_name else sh.sheet1

    header = ws.row_values(1)
    header_l = [h.strip().lower() for h in header]
    def col_idx(names):
        for n in names:
            if n.lower() in header_l:
                return header_l.index(n.lower()) + 1
        return None

    col_sku = col_idx(["sku"])
    col_size = col_idx(["taglia","size"])
    if not (col_sku and col_size):
        raise RuntimeError("Impossibile trovare colonne SKU e TAGLIA/Size sul foglio.")
    col_pid = _find_or_create_product_id_column(ws)

    values = ws.get_all_values()
    index = {}
    for i in range(2, len(values)+1):
        row = values[i-1]
        sku = row[col_sku-1] if col_sku-1 < len(row) else ""
        size = row[col_size-1] if col_size-1 < len(row) else ""
        index[f"{sku.strip()}{size.strip()}"] = i

    written = 0
    for item in rows_to_write:
        key = f"{(item.get('sku','') or '').strip()}{(item.get('size','') or '').strip()}"
        row_n = index.get(key)
        if not row_n:
            logger.warning(f"Row non trovata su GSheet per key={key}")
            continue
        ws.update_cell(row_n, col_pid, str(item.get("new_product_id","")))
        written += 1

    logger.info(f"Scritte {written} celle Product_id su GSheet.")
    return written
