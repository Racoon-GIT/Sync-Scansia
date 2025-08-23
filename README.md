# Scansia ⇄ Shopify Sync (Outlet Automation)

## Cosa fa
- Duplica i prodotti selezionati in GSheet, li rinomina con **" - Outlet"** e handle `-outlet`.
- Rimuove **tutti i tag**.
- Copia **metafield di prodotto** dal prodotto sorgente.
- Azzera inventario su tutte le location, **disconnette** la location **Magazzino**, e **stocca solo su "Promo"** le varianti presenti in GSheet con la quantità indicata.
- Azzera gli **ALT tag** delle immagini del prodotto duplicato e tenta di rinominare i filename aggiungendo `-Outlet` (se supportato dall'API).
- Aggiorna prezzi varianti (sale → `price`, pieno → `compare_at_price`).

## Requisiti GSheet
- Colonne minime: `SKU`, `TAGLIA` (o `Size`), `online`, `Qta`.
- Viene filtrato: `online = SI` **e** `Qta > 0`.

## Configurazione (.env)
Vedi `.env.example` e imposta:
```
SHOPIFY_STORE=...
SHOPIFY_ADMIN_TOKEN=...
SHOPIFY_API_VERSION=2025-01
PROMO_LOCATION_NAME=Promo
MAGAZZINO_LOCATION_NAME=Magazzino
SCANSIA_URL=https://docs.google.com/spreadsheets/d/XXXXX/edit#gid=0
DRY_RUN=false
LOG_LEVEL=DEBUG
LOG_SAMPLE_ROWS=10
```

## Uso locale
```bash
pip install -r requirements.txt
python -m src.sync --dry-run
python -m src.sync --apply
```

## Render (Cron Job)
- **Build Command**: `pip install -r requirements.txt`
- **Schedule**: `0 0 1 1 *` (rarissimo; usa **Run job now** per avviare manualmente)
- **Start Command**: `python -m src.sync --apply`
- Imposta le env nel dashboard Render.
