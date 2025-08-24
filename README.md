# Sync-Scansia

Automazione per creare e allineare prodotti **Outlet** su Shopify a partire da un **Google Sheet**.

- Duplica il prodotto sorgente (stesso SKU) e crea la versione **" - Outlet"**.
- Aggiorna titolo, handle (`-outlet`), rimuove tag, pulisce alt text immagini, copia media rinominando con `Outlet` nel filename quando possibile.
- Aggiorna **tutte le varianti**: prezzi (price/compareAtPrice).
- Alloca inventario in **Promo** per la variante presente a sheet; de-stocca tutte le varianti da **Magazzino**.
- Copia i **metafield** prodotto.
- Se esiste un **Outlet in DRAFT**, lo elimina e ricrea la copia pulita.
- Scrive il `Product_Id` creato nella colonna `Product_Id` del Google Sheet (se configurato service account).

## Deploy su Render (Cron Job)

1. Aggiungi questo repo.
2. **Build Command**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
3. **Start/Command**
   ```bash
   python -m src.sync --apply
   ```
4. (Consigliato) Imposta Python 3.12 via `runtime.txt` (già incluso).

## Variabili d'ambiente (Render → Environment)

Obbligatorie:
- `SHOPIFY_STORE` — es. `racoon-lab.myshopify.com`
- `SHOPIFY_API_VERSION` — es. `2025-01`
- `SHOPIFY_ACCESS_TOKEN` — Admin API access token
- `GSHEET_URL` — link *pubblico* allo Sheet (formato `.../edit?usp=sharing`)

Opzionali/avanzate:
- `PROMO_LOCATION_NAME` — default: `Promo`
- `MAGAZZINO_LOCATION_NAME` — default: `Magazzino`
- `MIN_QTY_THRESHOLD` — default: `0` (elabora se Qta > MIN_QTY_THRESHOLD)
- `ONLINE_VALUE` — default: `SI`
- `LOG_LEVEL` — `INFO` (default), `DEBUG`
- `SHOPIFY_GQL_MIN_INTERVAL_MS` — default: `120`
- `SHOPIFY_REST_MIN_INTERVAL_MS` — default: `120`
- `SHOPIFY_MAX_RETRIES` — default: `8`
- `GOOGLE_SERVICE_ACCOUNT_JSON` — contenuto JSON del service account per write-back `Product_Id` (se non presente, il write-back è saltato).

## Esecuzione locale
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export SHOPIFY_STORE=...
export SHOPIFY_ACCESS_TOKEN=...
export GSHEET_URL='https://docs.google.com/spreadsheets/d/XXXXX/edit?usp=sharing'
python -m src.sync --apply
```
