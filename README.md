# Scansia ⇄ Shopify Sync (Outlet Automation)

## Che cosa fa
- Legge un Google Sheet pubblico (o file locale) con `SKU`, `TAGLIA`, `online`, `Qta`, `Prezzo Pieno`, `Prezzo Scontato`.
- Filtra: `online = SI` **e** `Qta > 1`.
- Per ogni prodotto originale (trovato via SKU):
  - Se esiste già un **prodotto ACTIVE** con titolo `"{titolo originale} - outlet"` → **SKIP**.
  - Altrimenti duplica, rinomina (` - outlet` + handle `-outlet`), **rimuove tutti i tag**, imposta **ACTIVE**.
  - Aggiorna prezzi varianti (sale → price; pieno → compare_at_price).
  - Azzera tutte le varianti, poi **stock solo su location "Promo"** per le varianti presenti nel foglio, con quantità = `Qta`.
  - De-stocca tutte le altre locations.

## Uso
```bash
pip install -r requirements.txt
# Dry-run (non modifica nulla)
python -m src.sync --dry-run
# Applica modifiche
python -m src.sync --apply
```

## Configurazione (.env)
Vedi `.env.example` e imposta:
```
SHOPIFY_STORE=...
SHOPIFY_ADMIN_TOKEN=...
SHOPIFY_API_VERSION=2025-01
PROMO_LOCATION_NAME=Promo
SCANSIA_URL=https://docs.google.com/spreadsheets/d/XXXXX/edit#gid=0
DRY_RUN=true
LOG_LEVEL=INFO   # usa DEBUG per log dettagliato
LOG_SAMPLE_ROWS=10
```

## Logging
- **LOG_LEVEL=DEBUG** abilita logging strutturato e dettagliato:
  - mapping delle colonne riconosciute,
  - conteggio righe iniziali e dopo i filtri,
  - motivi di esclusione (esempi fino a `LOG_SAMPLE_ROWS`),
  - esito lookup SKU, duplicazione, update prezzi, inventario.
- Tutto il logging usa il modulo standard `logging` con timestamp.
