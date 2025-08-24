import os
import io
import time
import logging
import re
import requests
import pandas as pd
from slugify import slugify

LOG = logging.getLogger("sync.utils")


def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def download_gsheet_csv(gsheet_url: str) -> pd.DataFrame:
    if "/edit" in gsheet_url:
        export_url = gsheet_url.split("/edit")[0] + "/export?format=csv"
    elif "/view" in gsheet_url:
        export_url = gsheet_url.split("/view")[0] + "/export?format=csv"
    else:
        # support already-export-style url
        export_url = gsheet_url
    LOG.info("Scarico sorgente dati da URL")
    LOG.debug("URL di download: %s", export_url)
    r = requests.get(export_url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content))
    LOG.info("Tabella caricata da URL: %s righe, %s colonne", len(df), len(df.columns))
    LOG.debug("Colonne: %s", list(df.columns))
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # mappa flessibile per nomi comuni
    mapping = {
        "brand": ["brand", "BRAND"],
        "modello": ["modello", "MODELLO"],
        "titolo": ["titolo", "TITOLO", "Title"],
        "sku": ["sku", "SKU"],
        "taglia": ["taglia", "TAGLIA", "Size"],
        "qta": ["qta", "Qta", "Qty"],
        "online": ["online", "ONLINE"],
        "prezzo pieno": ["prezzo pieno", "Prezzo Pieno", "Full Price"],
        "prezzo scontato": ["prezzo scontato", "Prezzo Scontato", "Sale Price"],
        "sconto": ["sconto", "Sconto", "Discount"],
        "aggiunte il": ["Aggiunte il"],
        "ordine in entrata": ["Ordine in entrata"],
        "ordine in uscita": ["Ordine in uscita"],
        "vendute il": ["Vendute il"],
        "note": ["Note", "note"],
        "product_id": ["Product_Id", "product_id", "Product ID"],
    }
    colmap = {}
    lower = {c.lower(): c for c in df.columns}
    for norm, candidates in mapping.items():
        found = None
        for c in candidates:
            if c in df.columns:
                found = c
                break
            if c.lower() in lower:
                found = lower[c.lower()]
                break
        if found:
            colmap[norm] = found
    LOG.debug("Mapping colonne normalizzate → originali: %s", colmap)
    # rinomina in-place
    return df.rename(columns={v: k for k, v in colmap.items()})


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    online_value = os.getenv("ONLINE_VALUE", "SI").strip().lower()
    min_qty = int(os.getenv("MIN_QTY_THRESHOLD", "0"))
    df = df.copy()

    if "online" not in df.columns:
        raise ValueError("Colonna 'online' mancante nello sheet")
    if "qta" not in df.columns:
        raise ValueError("Colonna 'Qta'/'qta' mancante nello sheet")
    if "sku" not in df.columns or "taglia" not in df.columns:
        raise ValueError("Colonne 'SKU' e 'TAGLIA' sono obbligatorie")

    df["online_raw"] = df["online"].astype(str)
    df_online = df[df["online"].astype(str).str.strip().str.lower() == online_value]
    LOG.info("Dopo filtro online==%s: %s (scartate: %s)", online_value.upper(), len(df_online), len(df) - len(df_online))

    # Normalizza quantità
    def parse_qty(x):
        try:
            s = str(x).strip()
            if s == "" or s.lower() == "nan" or s == "None":
                return 0
            # supporta '1/3', '2', '1.0'
            if "/" in s:
                a, _ = s.split("/", 1)
                return int(float(a))
            return int(float(s))
        except Exception:
            return 0

    df_online["qta_parsed"] = df_online["qta"].apply(parse_qty)
    LOG.info("Dopo filtro Qta>%d (tra quelle online): %d (scartate per Qta<=%d: %d)",
             min_qty, (df_online["qta_parsed"] > min_qty).sum(), min_qty, (df_online["qta_parsed"] <= min_qty).sum())

    ready = df_online[df_online["qta_parsed"] > min_qty].copy()
    ready.rename(columns={"qta_parsed": "qta_norm"}, inplace=True)

    # pulizia SKU/Size
    ready["sku"] = ready["sku"].astype(str).str.strip()
    ready["taglia"] = ready["taglia"].astype(str).str.strip()

    # pulizia prezzi opzionali
    for col in ("prezzo pieno", "prezzo scontato"):
        if col in ready.columns:
            ready[col] = ready[col].apply(lambda v: _clean_price(v))

    LOG.info("Totale righe pronte all'elaborazione: %d", len(ready))
    # qualche sample scartato in DEBUG
    if LOG.isEnabledFor(logging.DEBUG):
        dropped_online = df[~df.index.isin(df_online.index)].head(10)
        if not dropped_online.empty:
            LOG.debug("Esempi scartati per online!=%s:", online_value.upper())
            for _, r in dropped_online.iterrows():
                LOG.debug("- SKU=%s Size=%s online_raw=%s", r.get("sku"), r.get("taglia"), r.get("online_raw"))
        dropped_qty = df_online[df_online["qta_parsed"] <= min_qty].head(10)
        if not dropped_qty.empty:
            LOG.debug("Esempi scartati per Qta<=%d:", min_qty)
            for _, r in dropped_qty.iterrows():
                LOG.debug("- SKU=%s Size=%s Qta_raw=%s Qta=%s", r.get("sku"), r.get("taglia"), r.get("qta"), r.get("qta_parsed"))
    return ready


def _clean_price(v):
    s = str(v).strip().replace("€", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except Exception:
        return None


def build_outlet_title(title: str) -> str:
    suffix = " - Outlet"
    t = (title or "").strip()
    if t.endswith("- Outlet") or t.endswith(" - Outlet"):
        return t
    return f"{t}{suffix}"


def build_outlet_handle(handle: str) -> str:
    h = (handle or "").strip().rstrip("/")
    if h.endswith("-outlet"):
        return h
    return f"{h}-outlet"


def gid_to_id(gid: str) -> int:
    # gid://shopify/Product/123456789 → 123456789
    m = re.search(r"/(\d+)$", gid or "")
    return int(m.group(1)) if m else None
