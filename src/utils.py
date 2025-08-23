import io, re, requests, logging
import pandas as pd

logger = logging.getLogger("sync.utils")

TRUE_VALUES = {"x","1","true","yes","si","sì","y","ok","on","si'", "si’", "sì'", "sì"}  # include i + accent grave variant

def to_bool_si(x) -> bool:
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in TRUE_VALUES or s == "si"

def norm_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def build_key(sku: str, size: str) -> str:
    return f"{norm_str(sku)}{norm_str(size)}"

def gsheet_to_export_url(url: str) -> str:
    # Converte link "edit" in export CSV
    if "docs.google.com/spreadsheets" in url and "export" not in url:
        gid = None
        m = re.search(r"[?&]gid=(\d+)", url)
        if m:
            gid = m.group(1)
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
        if not m:
            return url
        sid = m.group(1)
        if gid:
            return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv"
    return url

def read_table_from_source(path_or_url: str) -> pd.DataFrame:
    """Legge tabella da URL (CSV/XLSX) o locale (CSV/XLSX)."""
    if re.match(r"^https?://", str(path_or_url), flags=re.I):
        url = gsheet_to_export_url(path_or_url)
        logger.info("Scarico sorgente dati da URL")
        logger.debug(f"URL di download: {url}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        content_type = r.headers.get("Content-Type","").lower()
        data = r.content
        if "text/csv" in content_type or url.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(data))
        else:
            df = pd.read_excel(io.BytesIO(data))
        logger.info(f"Tabella caricata da URL: {len(df)} righe, {len(df.columns)} colonne")
        logger.debug(f"Colonne: {list(df.columns)}")
        return df
    # locale
    logger.info(f"Carico sorgente dati locale: {path_or_url}")
    if str(path_or_url).lower().endswith(".csv"):
        df = pd.read_csv(path_or_url)
    else:
        df = pd.read_excel(path_or_url)
    logger.info(f"Tabella caricata da file: {len(df)} righe, {len(df.columns)} colonne")
    logger.debug(f"Colonne: {list(df.columns)}")
    return df

def parse_scansia(df: pd.DataFrame, sample_rows: int = 10) -> pd.DataFrame:
    """Mappa le colonne del Google Sheet nel formato richiesto e filtra per online/Qta."""
    cols = {c.lower().strip(): c for c in df.columns}
    logger.debug(f"Mapping colonne normalizzate → originali: {cols}")

    def col(*names, required=True):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        if required:
            raise KeyError(f"Colonna richiesta mancante: {names}")
        return None

    SKU = col("sku")
    SIZE = col("taglia","size")
    ONLINE = col("online")
    QTA = col("qta","quantità","qty","q.tà online","q.tà","q.ta online","q.ta", required=False)
    PFULL = col("prezzo pieno","price full", required=False)
    PSALE = col("prezzo scontato","price sale", required=False)

    out = pd.DataFrame()
    out["SKU"] = df[SKU].map(norm_str)
    out["Size"] = df[SIZE].map(norm_str)

    # Salvo valori raw per debug
    out["_online_raw"] = df[ONLINE]
    out["online"] = df[ONLINE].map(to_bool_si)

    if QTA:
        out["_qta_raw"] = df[QTA]
        out["Qta"] = pd.to_numeric(df[QTA], errors="coerce").fillna(0).astype(int)
    else:
        out["_qta_raw"] = None
        out["Qta"] = 0

    out["Prezzo Pieno"] = pd.to_numeric(df[PFULL], errors="coerce") if PFULL else None
    out["Prezzo Scontato"] = pd.to_numeric(df[PSALE], errors="coerce") if PSALE else None

    # Debug: conteggi prima del filtro
    tot = len(out)
    mask_online = out["online"]
    mask_qta = out["Qta"] > 1
    passed = out[mask_online & mask_qta]
    dropped_online = out[~mask_online]
    dropped_qta = out[mask_online & ~mask_qta]

    logger.info(f"Righe iniziali: {tot}")
    logger.info(f"Dopo filtro online==SI: {mask_online.sum()} (scartate: {len(dropped_online)})")
    logger.info(f"Dopo filtro Qta>1 (tra quelle online): {mask_qta.sum()} (scartate per Qta<=1: {len(dropped_qta)})")
    logger.info(f"Totale righe pronte all'elaborazione: {len(passed)}")

    if logger.isEnabledFor(logging.DEBUG):
        if len(dropped_online) > 0:
            logger.debug("Esempi scartati per online!=SI:")
            for i, row in dropped_online.head(sample_rows).iterrows():
                logger.debug(f"- SKU={row['SKU']} Size={row['Size']} online_raw={row['_online_raw']}")
        if len(dropped_qta) > 0:
            logger.debug("Esempi scartati per Qta<=1:")
            for i, row in dropped_qta.head(sample_rows).iterrows():
                logger.debug(f"- SKU={row['SKU']} Size={row['Size']} Qta_raw={row['_qta_raw']} Qta={row['Qta']}")

    passed = passed.drop(columns=[c for c in ["_online_raw","_qta_raw"] if c in passed.columns])
    passed["KEY"] = passed.apply(lambda r: build_key(r["SKU"], r["Size"]), axis=1)
    return passed
