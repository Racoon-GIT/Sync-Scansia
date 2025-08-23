import io, re, requests
import pandas as pd

TRUE_VALUES = {"x","1","true","yes","si","sì","y","ok","on","si'", "si’", "sì'"}

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
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        content_type = r.headers.get("Content-Type","").lower()
        data = r.content
        if "text/csv" in content_type or url.endswith(".csv"):
            return pd.read_csv(io.BytesIO(data))
        return pd.read_excel(io.BytesIO(data))
    if str(path_or_url).lower().endswith(".csv"):
        return pd.read_csv(path_or_url)
    return pd.read_excel(path_or_url)

def parse_scansia(df: pd.DataFrame) -> pd.DataFrame:
    """Mappa colonne del Google Sheet nel formato richiesto e filtra per online/Qta."""
    cols = {c.lower().strip(): c for c in df.columns}

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
    out["online"] = df[ONLINE].map(to_bool_si)
    if QTA:
        out["Qta"] = pd.to_numeric(df[QTA], errors="coerce").fillna(0).astype(int)
    else:
        out["Qta"] = 0
    out["Prezzo Pieno"] = pd.to_numeric(df[PFULL], errors="coerce") if PFULL else None
    out["Prezzo Scontato"] = pd.to_numeric(df[PSALE], errors="coerce") if PSALE else None

    # Filtro: online == SI e Qta > 1
    out = out[(out["online"]) & (out["Qta"] > 1)].copy()
    out["KEY"] = out.apply(lambda r: build_key(r["SKU"], r["Size"]), axis=1)
    return out
