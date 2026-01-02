# Sync-Scansia — Documentazione Completa

Sistema automatizzato per la gestione prodotti Outlet su Shopify, con sincronizzazione da Google Sheets e riordino automatico collections.

---

## 📋 INDICE

1. [Quick Start](#quick-start)
2. [Prerequisiti e Dipendenze](#prerequisiti-e-dipendenze)
3. [Variabili d'Ambiente](#variabili-dambiente)
4. [Workflow SYNC - Gestione Outlet](#workflow-sync---gestione-outlet)
5. [Workflow REORDER - Ordinamento Collections](#workflow-reorder---ordinamento-collections)
6. [Troubleshooting](#troubleshooting)
7. [Deploy su Render](#deploy-su-render)

---

## 🚀 QUICK START

### Prerequisiti Minimi
- Python 3.11+ (raccomandato 3.12)
- Shopify Admin API Token
- Google Service Account con accesso al foglio

### Installazione
```bash
git clone https://github.com/Racoon-GIT/Sync-Scansia.git
cd Sync-Scansia
pip install -r requirements.txt
```

### Configurazione Base
```bash
# Copia e configura le variabili d'ambiente
export SHOPIFY_STORE=yourstore.myshopify.com
export SHOPIFY_ADMIN_TOKEN=shpat_xxxxx
export SHOPIFY_API_VERSION=2025-01

export GSPREAD_SHEET_ID=1ABC...XYZ
export GSPREAD_WORKSHEET_TITLE=Scarpe_in_Scansia
export GOOGLE_CREDENTIALS_JSON='{"type":"service_account",...}'

export PROMO_LOCATION_NAME=Promo
export MAGAZZINO_LOCATION_NAME=Magazzino
```

### Esecuzione
```bash
# SYNC - Dry-run (anteprima senza modifiche)
python -m src.sync

# SYNC - Apply (applica modifiche)
python -m src.sync --apply

# REORDER - Riordina collection per sconto
python -m src.reorder_collection --collection-id 262965428289 --apply
```

---

## 📦 PREREQUISITI E DIPENDENZE

### Stack Tecnologico
```
┌─────────────────────────────────────────┐
│         Ambiente Esecuzione             │
│  • Locale: Python 3.11+                 │
│  • Produzione: Render.com (Cron)        │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│      Sync-Scansia (Python)              │
│  ┌────────────────────────────────┐     │
│  │  • sync.py (SYNC workflow)     │     │
│  │  • reorder_collection.py       │     │
│  │  • gsheets.py, utils.py        │     │
│  └────────────────────────────────┘     │
└──────┬────────────────────┬─────────────┘
       │                    │
       ▼                    ▼
┌─────────────┐      ┌──────────────┐
│  Shopify    │      │ Google       │
│  Admin API  │      │ Sheets API   │
└─────────────┘      └──────────────┘
```

### Dipendenze Python
```txt
requests>=2.31.0
gspread>=5.12.0
google-auth>=2.23.0
```

### Shopify API - Permessi Richiesti

**Permessi Obbligatori**:
- ✅ `read_products`
- ✅ `write_products`
- ✅ `read_inventory`
- ✅ `write_inventory`
- ✅ `read_product_listings`
- ✅ `write_product_listings`
- ✅ `read_locations` (opzionale - vedi workaround)

**Come Configurare**:
1. Shopify Admin → Settings → Apps and sales channels
2. Trova/Crea Custom App
3. Configure → Admin API access scopes
4. Seleziona permessi sopra elencati
5. Genera Access Token
6. Salva token come `SHOPIFY_ADMIN_TOKEN`

**Workaround Permission `read_locations`**:

Se il token non ha `read_locations`, puoi usare location IDs diretti:

```bash
# Trova gli ID nelle impostazioni Shopify o dai log precedenti
export PROMO_LOCATION_ID=8251572336
export MAGAZZINO_LOCATION_ID=8251572336

# Il sistema userà gli ID invece di chiamare /locations.json
```

### Google Service Account

**Setup**:
1. Google Cloud Console → Create Service Account
2. Genera chiave JSON
3. Condividi Google Sheet con email service account
4. Se foglio in Shared Drive, aggiungi service account al Drive

**Formato Credenziali**:
```bash
# Opzione 1: JSON inline
export GOOGLE_CREDENTIALS_JSON='{"type":"service_account","project_id":"...",...}'

# Opzione 2: File path
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

---

## 🔧 VARIABILI D'AMBIENTE

### Google Sheets (Obbligatorie)
| Variabile | Descrizione | Esempio |
|-----------|-------------|---------|
| `GSPREAD_SHEET_ID` | ID del Google Sheet (non URL) | `1ABC...XYZ` |
| `GSPREAD_WORKSHEET_TITLE` | Nome worksheet | `Scarpe_in_Scansia` |
| `GOOGLE_CREDENTIALS_JSON` | JSON credenziali service account | `{"type":"service_account",...}` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path file credenziali (alternativa) | `/path/to/creds.json` |

**Alias Supportati** (retrocompatibilità):
- `SPREADSHEET_ID` → `GSPREAD_SHEET_ID`
- `WORKSHEET_NAME` → `GSPREAD_WORKSHEET_TITLE`

### Shopify (Obbligatorie)
| Variabile | Descrizione | Esempio |
|-----------|-------------|---------|
| `SHOPIFY_STORE` | Dominio store | `racoon-lab.myshopify.com` |
| `SHOPIFY_ADMIN_TOKEN` | Access token Admin API | `shpat_xxxxxxxxxxxxx` |
| `SHOPIFY_API_VERSION` | Versione API | `2025-01` |

### Locations (Obbligatorie)

**Opzione 1 - By Name** (richiede `read_locations` permission):
```bash
PROMO_LOCATION_NAME=Promo
MAGAZZINO_LOCATION_NAME=Magazzino
```

**Opzione 2 - By ID** (workaround senza `read_locations`):
```bash
PROMO_LOCATION_ID=8251572336
MAGAZZINO_LOCATION_ID=8251572336
```

### Performance (Opzionali)
| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `SHOPIFY_MIN_INTERVAL_SEC` | `0.7` | Intervallo minimo tra chiamate API |
| `SHOPIFY_MAX_RETRIES` | `5` | Tentativi massimi su errore API |

### Controllo Workflow (Opzionali)
| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `RUN_MODE` | N/A | `SYNC` per workflow outlet, `REORDER` per riordino |

---

## 🛠️ WORKFLOW SYNC - GESTIONE OUTLET

### Descrizione
Duplica prodotti "sorgente" in versione **Outlet** partendo da Google Sheet, imposta prezzi/saldi, copia media e metafield, gestisce inventory multi-location.

### Struttura Google Sheet

**Colonne Richieste** (case-insensitive, spazi → underscore):
- `SKU` - **Obbligatorio**, identifica prodotto sorgente
- `Qta` - **> 0** per essere selezionata (supporta formato "1/3")
- `online` - **"SI"** per essere selezionata (ammessi: si/sì/true/1/x/ok/yes)

**Colonne Opzionali**:
- `BRAND`, `MODELLO`, `TITOLO` (usate per logging)
- `TAGLIA` - Se presente, match preciso variante
- `Prezzo Pieno`, `Prezzo Scontato` (formati: `129,90`, `€ 129`, `129.90`)
- `Product_Id` - Write-back GID prodotto outlet creato

**Esempio**:
```
SKU       | TAGLIA | Qta | online | Prezzo Pieno | Prezzo Scontato
ABC123    | 35     | 1   | SI     | 159,90       | 99,90
ABC123    | 36     | 2   | SI     | 159,90       | 99,90
ABC123    | 37     | 1   | NO     | 159,90       | 99,90
```

### Flusso Operativo Dettagliato

```
START
  │
  ├─ 1. Lettura Google Sheets
  │    ├─ Normalizzazione colonne (lowercase, underscore)
  │    ├─ Filtro: online=SI AND Qta>0
  │    └─ Raggruppamento per SKU
  │
  └─ Per ogni gruppo SKU:
       │
       ├─ 2. Ricerca prodotto sorgente (by SKU)
       │
       ├─ 3. Verifica outlet esistente
       │    ├─ Se esiste outlet ATTIVO → SKIP
       │    └─ Se esiste outlet DRAFT → DELETE
       │
       ├─ 4. Duplica prodotto (GraphQL productDuplicate)
       │    └─ Nuovo titolo: "{original} — Outlet"
       │
       ├─ 5. Update handle/status/tags (REST PUT)
       │    ├─ Handle: {original}-outlet (con fallback -1, -2 se occupato)
       │    ├─ Status: active
       │    └─ Tags: vuoti (pulizia)
       │
       ├─ 6. Copia immagini in ordine
       │    ├─ DELETE tutte immagini outlet
       │    ├─ POST immagini originali in sequenza
       │    └─ Position 1..N, alt=""
       │
       ├─ 7. Copia metafields (GraphQL metafieldsSet)
       │
       ├─ 8. Elimina collections manuali
       │    ├─ Query collects
       │    └─ DELETE collect non automatici
       │
       ├─ 9. Update prezzi tutte varianti
       │    ├─ Batch GraphQL productVariantsBulkUpdate
       │    └─ Price e compareAtPrice da Google Sheet
       │
       ├─ 10. Gestione inventory PROMO location
       │    ├─ Connect location a inventory_item (se non connesso)
       │    ├─ Set 0 su tutte le varianti
       │    └─ Set quantità su varianti target (da Google Sheet)
       │
       ├─ 11. Gestione inventory MAGAZZINO location
       │    ├─ Set 0 su tutte le varianti
       │    ├─ DELETE inventory_level (disconnette location)
       │    └─ Verifica finale: stato "Non stoccato"
       │
       └─ 12. Write-back Product_Id su Google Sheet
            └─ Scrive GID prodotto outlet su colonna Product_Id
END
```

### Caratteristiche Tecniche

**Idempotenza**:
- ✅ Outlet esistente attivo → skip automatico
- ✅ Outlet draft duplicato → delete + ricrea
- ✅ Immagini ricreate ad ogni run (no duplicati)
- ✅ Prezzi aggiornati (non creati duplicati)

**Gestione Inventory**:
1. **Promo location**: Connect → Set 0 → Set quantità target
2. **Magazzino location**: Set 0 → DELETE livello → Verifica "Non stoccato"
3. **Propagation delay**: 1.5s tra operazioni per evitare race conditions

**Gestione Errori**:
- Retry automatico 5 volte con backoff esponenziale
- Gestione 429 Rate Limit con `Retry-After` header
- Gestione 5xx Server Errors con backoff (1s, 2s, 4s, 8s)
- Timeout 30s su richieste HTTP
- Logging dettagliato per debugging

**Forced Tracking**:
- Tutti inventory_item vengono forzati a `tracked=true` (GraphQL inventoryItemUpdate)
- Previene inconsistenze inventory

### Esecuzione

```bash
# Dry-run (preview, nessuna modifica)
python -m src.sync

# Apply (applica modifiche)
python -m src.sync --apply

# Via RUN_MODE (per Render.com)
RUN_MODE=SYNC python -m main
```

### Log Principali

```
INFO | Righe selezionate: 12 (online=SI, Qta>0)
INFO | Gruppi SKU: 3
INFO | [ABC123] Trovato prodotto sorgente: gid://shopify/Product/123456
INFO | [ABC123] Outlet esistente DRAFT trovato, eliminazione...
INFO | [ABC123] Duplicazione prodotto...
INFO | [ABC123] Update handle: abc123-outlet
INFO | [ABC123] Immagini: 8 copiate in ordine
INFO | [ABC123] Prezzi aggiornati: 5 varianti
INFO | [ABC123] Inventario Promo: 3 varianti, total 4 unità
INFO | [ABC123] Inventario Magazzino: 3 varianti azzerate e disconnesse
INFO | [ABC123] Write-back Product_Id: gid://shopify/Product/789012
INFO | ✅ SKU=ABC123 completato (3 taglie)
```

---

## 🔄 WORKFLOW REORDER - ORDINAMENTO COLLECTIONS

### Descrizione
Ordina prodotti di una collection Shopify per **sconto percentuale decrescente** (prodotti con sconto maggiore appaiono primi).

### Calcolo Sconto
```python
if compareAtPrice and compareAtPrice > 0:
    discount = ((compareAtPrice - price) / compareAtPrice) * 100
else:
    discount = 0.0
```

**Ordinamento**:
- **Primario**: Sconto % decrescente (45% → 40% → 30% → ...)
- **Secondario**: Titolo alfabetico (per prodotti con stesso sconto)
- **Prodotti senza sconto**: Finiscono in fondo (sconto = 0%)

### Utilizzo

```bash
# Dry-run (preview ordinamento)
python -m src.reorder_collection --collection-id 262965428289

# Apply (applica riordino)
python -m src.reorder_collection --collection-id 262965428289 --apply

# Via RUN_MODE
RUN_MODE=REORDER COLLECTION_ID=262965428289 python -m main
```

### Flusso Operativo

```
START
  │
  ├─ 1. Recupera prodotti dalla collection (GraphQL paginato)
  │    └─ Max 50 prodotti per pagina
  │
  ├─ 2. Calcola sconto per ogni prodotto
  │    └─ Usa prima variante per calcolo
  │
  ├─ 3. Ordina per sconto decrescente + alfabetico
  │
  ├─ 4. Applica riordino su Shopify
  │    ├─ Batch da 250 prodotti (max GraphQL)
  │    ├─ Mutation collectionReorderProducts
  │    ├─ Delay 1s tra batch
  │    └─ Polling job completion (max 60s)
  │
  └─ 5. Report finale
       └─ Distribuzione sconti, tempo esecuzione
END
```

### Performance

- **Paginazione**: 50 prodotti/pagina
- **Batch reorder**: 250 prodotti/mutation
- **Rate limiting**: 0.7s tra chiamate
- **Retry**: 5 tentativi con backoff
- **Tempo stimato**: ~10-15s per 100 prodotti

### Output Esempio

```
======================================================================
REORDER COLLECTION BY DISCOUNT %
Collection ID: 262965428289
Mode: DRY-RUN
======================================================================

INFO: Totale prodotti recuperati: 78
INFO: Primi 10 prodotti dopo ordinamento:
INFO:   1. Converse All Star Platform...      - Sconto:  45.0%
INFO:   2. Dr Martens 1460 Glitter...         - Sconto:  42.5%
INFO:   3. Birkenstock Boston Oro...          - Sconto:  40.0%
...

======================================================================
RIEPILOGO:
Totale prodotti: 78
Distribuzione sconti:
  45%: 2 prodotti
  40%: 5 prodotti
  35%: 6 prodotti
  ...
======================================================================
⚠️  DRY-RUN: Usa --apply per applicare riordino
```

### Attenzioni

**Smart vs Manual Collection**:
- ✅ **Manual Collection**: Ordinamento persiste
- ⚠️ **Smart Collection**: Verificare che Sort = "Manual" (non "Best selling", "Price", etc.)

**Se ordinamento non si applica**:
1. Shopify Admin → Products → Collections
2. Seleziona collection
3. Products → Sort → **Manual**
4. Ri-esegui reorder script

**Prodotti Multi-Variante**:
- Lo script usa **solo la prima variante** per calcolo sconto
- Se varianti hanno prezzi molto diversi, l'ordinamento potrebbe non essere ottimale
- Soluzione: modificare codice per usare sconto medio/massimo

---

## 🐛 TROUBLESHOOTING

### Errori Comuni SYNC

#### `SpreadsheetNotFound (404)`
**Causa**: ID Google Sheet errato o permessi mancanti

**Soluzione**:
1. `GSPREAD_SHEET_ID` deve essere l'ID (non l'URL)
   - URL: `https://docs.google.com/spreadsheets/d/1ABC...XYZ/edit`
   - ID: `1ABC...XYZ`
2. Condividi foglio con email service account
3. Se in Shared Drive, aggiungi service account al Drive

#### `429 Too Many Requests`
**Causa**: Rate limit Shopify superato

**Soluzione**:
1. Aumenta `SHOPIFY_MIN_INTERVAL_SEC=1.0` (default 0.7)
2. Sistema gestisce automaticamente retry con backoff
3. Se persistente, riduci batch size o frequenza esecuzioni

#### `productDuplicate: newHandle non accettato`
**Causa**: Handle già occupato da altro prodotto

**Soluzione**:
- ✅ Già gestito automaticamente con fallback `-1`, `-2`, ecc.
- Se errore persiste, verifica log per handle generato

#### `Immagini disordinate / con alt text`
**Causa**: Bug Shopify o copia precedente non pulita

**Soluzione**:
- ✅ Già risolto: DELETE tutte + ricrea con position e alt=""
- Se persiste, verifica che `--apply` sia usato

#### `Location Magazzino ancora a stock invece di "Non stoccato"`
**Causa**: DELETE inventory_level fallito o nome location errato

**Soluzione**:
1. Verifica `MAGAZZINO_LOCATION_NAME` corrisponde ESATTAMENTE al nome su Shopify
2. Controlla log: deve mostrare "Location Magazzino trovata: ID=..."
3. Se nome corretto ma fallisce, usa workaround con `MAGAZZINO_LOCATION_ID`

#### `403 Forbidden on /locations.json`
**Causa**: Token mancante `read_locations` permission

**Soluzione - Workaround**:
```bash
# Trova location IDs da Shopify Admin → Settings → Locations → URL
export PROMO_LOCATION_ID=8251572336
export MAGAZZINO_LOCATION_ID=8251572336

# Rimuovi location names
unset PROMO_LOCATION_NAME
unset MAGAZZINO_LOCATION_NAME
```

**Soluzione - Permanente**:
1. Shopify Admin → Apps → Configure app
2. Aggiungi scope `read_locations`
3. Rigenera token
4. Aggiorna `SHOPIFY_ADMIN_TOKEN`

### Errori Comuni REORDER

#### `Collection not found`
**Soluzione**:
- Verifica collection ID corretto
- Verifica token ha `read_collections` permission

#### `Ordinamento non si applica`
**Soluzione**:
1. Verifica collection Sort = "Manual" (non automatico)
2. Ricarica pagina Shopify Admin (cache)
3. Attendi 30s (job asincrono)

#### `GraphQL errors`
**Soluzione**:
- Verifica API version compatibility (`2025-01`)
- Controlla formato GID: `gid://shopify/Collection/{id}`

### Performance Issues

#### Script lento (>5 minuti per pochi prodotti)
**Causa**: Troppi retry o rate limit

**Soluzione**:
1. Verifica log per retry frequenti
2. Aumenta `SHOPIFY_MIN_INTERVAL_SEC=1.0`
3. Se Render timeout (300s), considera split in batch

#### Rate limit frequenti
**Soluzione**:
```bash
export SHOPIFY_MIN_INTERVAL_SEC=1.0  # Da 0.7 a 1.0
export SHOPIFY_MAX_RETRIES=3         # Riduci tentativi
```

---

## 🚀 DEPLOY SU RENDER

### Setup Cron Service

**render.yaml** (commit al repository):
```yaml
services:
  - type: cron
    name: sync-scansia
    runtime: python
    schedule: "0 6 * * *"  # Ogni giorno alle 6:00 UTC
    buildCommand: "pip install -r requirements.txt"
    startCommand: "python -m main"
    envVars:
      - key: PYTHON_VERSION
        value: "3.12.4"
      - key: RUN_MODE
        value: SYNC
```

### Configurazione Environment Variables

Render Dashboard → Service → Environment:

**Google Sheets**:
```
GSPREAD_SHEET_ID=1ABC...XYZ
GSPREAD_WORKSHEET_TITLE=Scarpe_in_Scansia
GOOGLE_CREDENTIALS_JSON={"type":"service_account",...}
```

**Shopify**:
```
SHOPIFY_STORE=yourstore.myshopify.com
SHOPIFY_ADMIN_TOKEN=shpat_xxxxx
SHOPIFY_API_VERSION=2025-01
```

**Locations**:
```
PROMO_LOCATION_ID=8251572336
MAGAZZINO_LOCATION_ID=8251572336
```

**Opzionali**:
```
SHOPIFY_MIN_INTERVAL_SEC=0.7
SHOPIFY_MAX_RETRIES=5
```

### Fix Python Version (se errori build pandas)

**Problema**: Render usa Python 3.13, pandas fallisce build

**Soluzione**:
1. Aggiungi in `render.yaml`:
   ```yaml
   envVars:
     - key: PYTHON_VERSION
       value: "3.12.4"
   ```
2. Clear build cache (Settings → Clear build cache)
3. Redeploy

### Monitoraggio Logs

Render Dashboard → Logs:

**SYNC Success**:
```
INFO | Righe selezionate: 12
INFO | ✅ SKU=ABC123 completato (3 taglie)
INFO | ✅ SKU=DEF456 completato (2 taglie)
INFO | Workflow completato: 2 prodotti outlet creati
```

**REORDER Success**:
```
INFO | Totale prodotti recuperati: 78
INFO | ✅ Riordino completato
INFO | ✅ Tutti i job completati in 8.3s
```

### Manual Trigger

Render Dashboard → Manual Deploy → Deploy latest commit

---

## 📊 STRUTTURA PROGETTO

```
Sync-Scansia/
├── main.py                 # Entry point (gestisce RUN_MODE)
├── requirements.txt        # Dipendenze Python
├── render.yaml            # Configurazione Render.com
├── README.md              # Questa documentazione
│
├── src/
│   ├── __init__.py
│   ├── sync.py            # Workflow SYNC (outlet)
│   ├── reorder_collection.py  # Workflow REORDER
│   ├── gsheets.py         # Google Sheets utils
│   └── utils.py           # Utilities comuni
│
└── reorder/
    └── (deprecated docs)
```

---

## 📝 NOTE TECNICHE

### Architettura API

**GraphQL usato per**:
- productDuplicate (creazione outlet)
- metafieldsSet (copia metafields)
- productVariantsBulkUpdate (prezzi)
- collectionReorderProducts (riordino)
- inventoryItemUpdate (force tracking)

**REST usato per**:
- PUT /products/{id}.json (handle/status/tags)
- POST /products/{id}/images.json (immagini)
- DELETE /collects/{id}.json (collections)
- POST /inventory_levels/set.json (inventory)
- DELETE /inventory_levels.json (disconnect)

**Motivo mix GraphQL/REST**:
- GraphQL: operazioni bulk, moderne API
- REST: operazioni singole più affidabili, fallback quando GraphQL limitato

### Rate Limiting & Resilienza

**Limiti Shopify**:
- 2 chiamate/secondo per endpoint (bucket leaky)
- Sleep default: 0.7s tra chiamate

**Gestione Automatica**:
- 429 Rate Limit → rispetta `Retry-After` header
- 5xx Server Errors → backoff esponenziale (1s, 2s, 4s, 8s)
- Timeout → 30s con retry automatico
- Max 5 retry per chiamata

### Sicurezza

**Credenziali**:
- ✅ Token in ENV (mai hardcoded)
- ✅ Logging non include token/password
- ⚠️ Verificare .gitignore per file sensibili

**Permessi Minimi**:
- Solo permessi strettamente necessari
- Service account con accesso limitato a specifico foglio

---

## 🆘 SUPPORTO

**Repository**: https://github.com/Racoon-GIT/Sync-Scansia
**Issues**: https://github.com/Racoon-GIT/Sync-Scansia/issues
**Manutentore**: Racoon s.r.l.
**Email**: it-services@racoon-lab.it

**Documentazione Shopify**:
- [Admin API](https://shopify.dev/api/admin-rest)
- [GraphQL API](https://shopify.dev/api/admin-graphql)
- [Inventory Management](https://shopify.dev/api/admin-rest/2025-01/resources/inventorylevel)

---

## 📜 CHANGELOG

### v2.0 (2026-01-02)
- ✅ Rimozione moduli inutilizzati (variant_reset, channel_manager, config, exceptions)
- ✅ Cleanup codice: rimossi metodi e feature non utilizzate
- ✅ Workaround location IDs per permission `read_locations`
- ✅ Documentazione unificata completa
- ✅ Fix compatibilità con setup originale funzionante

### v1.1 (2025-11-20)
- ✅ REORDER: Resilienza API (retry, backoff, timeout)
- ✅ REORDER: Job polling completion
- ✅ REORDER: Gestione 429 e 5xx automatica

### v1.0 (2025-11-01)
- ✅ Workflow SYNC base
- ✅ Duplicazione prodotti outlet
- ✅ Gestione inventory multi-location
- ✅ Write-back Google Sheets
- ✅ REORDER collection per sconto

---

**Fine Documentazione**
