# src/utils.py
import re
import logging
import pandas as pd

log = logging.getLogger("sync.utils")

NORMALIZE_MAP = {
    "brand": "BRAND",
    "modello": "MODELLO",
    "titolo": "TITOLO",
    "sku": "SKU",
    "taglia": "TAGLIA",
    "qta": "Qta",
    "online": "online",
    "prezzo pieno": "Prezzo Pieno",
    "prezzo scontato": "Prezzo Scontato",
    "sconto": "Sconto",
    "aggiunte il": "Aggiunte il",
    "ordine in entrata": "Ordine in entrata",
    "ordine in uscita": "Ordine in uscita",
    "vendute il": "Vendute il",
    "note": "Note",
    "product_id": "Product_Id",
}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {v: k for k, v in NORMALIZE_MAP.items() if v in df.columns}
    log.debug("Mapping colonne normalizzate → originali: %s", {k: NORMALIZE_MAP[k] for k in mapping.values()})
    out = df.rename(columns={NORMALIZE_MAP[k]: k for k in mapping.values()})
    return out

def slugify_handle(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s.strip("-")

def parse_price(x) -> float:
    if pd.isna(x) or x == "":
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).replace("€", "").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return 0.0
