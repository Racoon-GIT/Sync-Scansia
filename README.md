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

Configura le variabili in `.env` (crealo partendo da `.env.example`) oppure come variabili d'ambiente.
