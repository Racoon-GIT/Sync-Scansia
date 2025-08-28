# Sync-Scansia — Workflow OUTLET

Script che duplica i prodotti “sorgente” in versione **Outlet** a partire da un Google Sheet, imposta prezzi/saldi, media, metafield, collezioni, e rialloca l’inventario tra le location **Promo** e **Magazzino**.

## Requisiti

- Python 3.11+ (consigliato 3.12/3.13)
- Dipendenze: `requests`, `gspread`, `google-auth`
- Shopify Admin API (Admin Access Token) con permessi:
  - Products (read/write)
  - Product Listings / Collections (read/write)
  - Inventory (read/write)
  - Metafields (read/write)
- Google Service Account con accesso al foglio (condividi il foglio con l’email del service account)

## Variabili d’ambiente

> **NB**: i nomi **non sono cambiati**. Sono supportati alcuni alias retro-compatibili.

Obbligatorie:
- `GSPREAD_SHEET_ID` (alias `SPREADSHEET_ID`) — **ID** del Google Sheet (non l’URL)
- `GSPREAD_WORKSHEET_TITLE` (alias `WORKSHEET_NAME`) — nome del worksheet (es. `Scarpe_in_Scansia`)
- `GOOGLE_CREDENTIALS_JSON` **oppure** `GOOGLE_APPLICATION_CREDENTIALS` (file path) — credenziali service account
- `SHOPIFY_STORE` — es. `racoon-lab.myshopify.com`
- `SHOPIFY_ADMIN_TOKEN` — Admin API access token
- `SHOPIFY_API_VERSION` — es. `2025-01`
- `PROMO_LOCATION_NAME` — es. `Promo`
- `MAGAZZINO_LOCATION_NAME` — es. `Magazzino`

Opzionali:
- `SHOPIFY_MIN_INTERVAL_SEC` (default `0.7`) — throttle base tra chiamate
- `SHOPIFY_MAX_RETRIES` (default `5`) — tentativi per 429/5xx

## Struttura colonne Google Sheet

Vengono normalizzate (case-insensitive, spazi → underscore). Colonne usate:
- `BRAND`, `MODELLO`, `TITOLO` (facoltative)
- `SKU` (**richiesto**)
- `TAGLIA` (consigliata; se presente viene usata per match preciso della variante)
- `Qta` (**> 0** per essere selezionata)
- `online` (**"SI"** per essere selezionata; ammessi: si/sì/true/1/x/ok/yes)
- `Prezzo Pieno`, `Prezzo Scontato` (accettati anche formati tipo `129,90`, `€ 129`)
- `Product_Id` (write-back)

## Log principali

selezione righe, duplicazione, aggiornamento prezzi, media ricostruiti in ordine
svuotamento tag, pulizia collects manuali
inventario: prima Promo (connect + 0 + set quantità sulla variante target), poi Magazzino (set 0 + DELETE livello)
write-back Product_Id sul Google Sheet

## Comportamento di idempotenza

Se esiste già un Outlet attivo per quel sorgente (handle <handle>-outlet), la riga viene skippata.
Se esiste una bozza Outlet con lo stesso handle, viene rimossa prima di procedere.
Media: le immagini dell’Outlet vengono ricreate in ordine (alt vuoti) ad ogni run, evitando duplicazioni indesiderate.
Prezzi: aggiornati in bulk su tutte le varianti del prodotto Outlet.
Inventario: Promo prima (tutte a 0 + target alla quantità), poi Magazzino azzerato e disconnesso (DELETE livello).
Troubleshooting
SpreadsheetNotFound (404):
GSPREAD_SHEET_ID è l’ID, non l’URL
Condividi il foglio con l’email del service account
Se è in uno Shared Drive, aggiungi il service account al Drive
429 Too Many Requests:
Aumenta SHOPIFY_MIN_INTERVAL_SEC (es. 1.0–1.2)
Lascia fare al retry/backoff integrato
productDuplicate: newHandle non accettato:
Corretto: ora si usa productDuplicate(newTitle) e poi PUT /products/{id}.json per handle/status/tags (con fallback -1, -2, …)
Immagini disordinate / con alt:
Patch attiva: cancelliamo e ricreiamo in ordine con position=1..N e alt=""
Magazzino ancora a stock:
Patch attiva: set 0 + DELETE /inventory_levels.json con retry e verifica finale.
Assicurarsi che MAGAZZINO_LOCATION_NAME corrisponda esattamente al nome della location su Shopify.

## Note di sicurezza

Lo script forza tracked=True sugli InventoryItem duplicati (GraphQL inventoryItemUpdate) per evitare inconsistenze.
I tag dell’Outlet vengono svuotati al momento della promozione (se servono tag specifici, personalizzare nel PUT /products/{id}.json).

## Esecuzione

```bash
# dry-run (nessuna scrittura su Shopify)
python -m src.sync

# applica le modifiche
python -m src.sync --apply
