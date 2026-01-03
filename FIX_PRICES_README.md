# Fix Prices - Guida Utilizzo

Script per correggere i prezzi a zero sui prodotti outlet esistenti causati dal bug v2.0.

## Problema Risolto

Prodotti outlet creati con versione ≤ v2.0 hanno prezzi a zero a causa del bug nel `get_product_variants()`. Questo script legge i prezzi corretti dal Google Sheet e li applica ai prodotti esistenti **senza ricreare o modificare altro**.

## Prerequisiti

1. **Dipendenze installate**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Variabili ambiente configurate**:
   ```bash
   export SHOPIFY_ADMIN_TOKEN=shpat_xxxxx
   export GSPREAD_SHEET_ID=1ABC...XYZ
   export GSPREAD_WORKSHEET_TITLE=Scarpe_in_Scansia
   export GOOGLE_CREDENTIALS_JSON='{"type":"service_account",...}'
   ```

   **Nota**: `SHOPIFY_STORE` è opzionale (default: `racoon-lab.myshopify.com`)

## Uso

### 1. Test (Dry-Run) - RACCOMANDATO

Esegui SEMPRE prima un dry-run per vedere cosa verrà modificato:

```bash
python fix_prices.py --dry-run
```

Output esempio:
```
2026-01-03 10:30:00 | INFO | MODALITÀ DRY-RUN - Nessuna modifica sarà applicata
2026-01-03 10:30:05 | INFO | Processing SKU=ABC123
2026-01-03 10:30:05 | INFO | Prezzi target: scontato=228.00, pieno=269.00
2026-01-03 10:30:06 | INFO | Outlet trovato: gid://shopify/Product/15506299421004
2026-01-03 10:30:07 | INFO | [DRY-RUN] Aggiornerei 3 varianti con prezzi: scontato=228.00, pieno=269.00
...
2026-01-03 10:35:00 | INFO | RISULTATI FINALI:
2026-01-03 10:35:00 | INFO | - Prodotti da aggiornare: 45
2026-01-03 10:35:00 | INFO | - Skip (già corretti): 12
2026-01-03 10:35:00 | INFO | - Skip (non trovati): 3
```

### 2. Applicazione Modifiche

**IMPORTANTE**: Verifica i risultati del dry-run prima di procedere!

```bash
python fix_prices.py --apply
```

Output esempio:
```
2026-01-03 10:40:00 | WARNING | MODALITÀ APPLY - Le modifiche saranno applicate!
2026-01-03 10:40:05 | INFO | Processing SKU=ABC123
2026-01-03 10:40:06 | INFO | ✅ Prezzi aggiornati per 3 varianti
...
2026-01-03 10:45:00 | INFO | RISULTATI FINALI:
2026-01-03 10:45:00 | INFO | - Prodotti aggiornati: 45
2026-01-03 10:45:00 | INFO | - Errori: 0
```

## Algoritmo

Lo script esegue i seguenti passaggi:

1. **Legge Google Sheet**: Carica tutte le righe con `online=SI` e `qta>0`
2. **Raggruppa per SKU**: Ogni SKU può avere più taglie (righe diverse)
3. **Per ogni SKU**:
   - Estrae prezzi corretti da Sheet:
     - `prezzo_outlet` → prezzo scontato (price)
     - `prezzo` → prezzo pieno (compareAtPrice)
   - Cerca outlet esistente per SKU usando `find_outlet_by_sku()`
   - Se trovato e status = ACTIVE:
     - Verifica se prezzi sono già corretti
     - Se no, aggiorna TUTTE le varianti con `variants_bulk_update_prices()`
4. **Report finale**: Mostra statistiche aggiornamenti

## Sicurezza

✅ **Safe Operations**:
- Modifica SOLO i campi `price` e `compareAtPrice`
- Non tocca inventory, immagini, metafields, collections
- Skip automatico se prezzi già corretti
- Skip automatico se outlet non trovato o non ACTIVE

✅ **Dry-Run Default**:
- Se nessun flag specificato, esegue dry-run
- Richiede `--apply` esplicito per modifiche

✅ **Error Handling**:
- Cattura eccezioni per ogni SKU
- Un errore non blocca l'intero processo
- Exit code 1 se ci sono errori, 0 se tutto ok

## Casistiche Gestite

| Caso | Comportamento |
|------|---------------|
| Outlet non trovato per SKU | Skip (log warning) |
| Outlet trovato ma status DRAFT | Skip (non modificare draft) |
| Prezzi già corretti | Skip (evita API calls inutili) |
| Outlet ACTIVE con prezzi a zero | ✅ Aggiorna prezzi |
| Errore API Shopify | Log error, continua con prossimo SKU |
| Google Sheet non accessibile | Exit con errore (prerequisito) |

## Verifica Post-Esecuzione

Dopo l'esecuzione con `--apply`, verifica su Shopify Admin:

1. Apri un prodotto outlet aggiornato
2. Controlla prezzi varianti:
   - **Price** = prezzo_outlet da Google Sheet
   - **Compare at price** = prezzo da Google Sheet
3. Verifica che inventory, immagini, metafields non siano cambiati

## Troubleshooting

### `ModuleNotFoundError: No module named 'requests'`
**Soluzione**: Installa dipendenze
```bash
pip install -r requirements.txt
```

### `RuntimeError: SHOPIFY_ADMIN_TOKEN environment variable not set`
**Soluzione**: Configura variabile ambiente
```bash
export SHOPIFY_ADMIN_TOKEN=shpat_xxxxx
```

### `SpreadsheetNotFound (404)`
**Soluzione**: Verifica `GSPREAD_SHEET_ID` e permessi Google Sheet

### `Prodotti da aggiornare: 0` (dry-run)
**Possibili cause**:
1. Tutti i prezzi sono già corretti (verifica con `--dry-run`)
2. Nessun outlet trovato per gli SKU nel Google Sheet
3. Google Sheet filtri `online=SI` e `qta>0` escludono tutti i prodotti

**Debug**: Controlla log per vedere quali SKU vengono processati e perché vengono skippati

## Note Importanti

- **Google Sheet è source of truth**: I prezzi vengono letti dal Sheet, non da Shopify
- **Batch update efficiente**: Usa `productVariantsBulkUpdate` GraphQL (1 chiamata per prodotto)
- **Rate limiting**: Lo script usa lo stesso client Shopify di sync.py con rate limiting automatico
- **Idempotente**: Puoi eseguire più volte senza problemi (skip se già corretto)

## Quando Usare

✅ **Usa questo script se**:
- Hai prodotti outlet online con prezzo 0.00
- I prezzi corretti sono nel Google Sheet
- Vuoi aggiornare SOLO i prezzi senza ricreare prodotti

❌ **Non usare se**:
- Vuoi creare nuovi outlet → usa workflow SYNC normale
- Vuoi modificare inventory → usa workflow SYNC normale
- Vuoi ricreare varianti → questo script non le tocca

## Esempio Completo

```bash
# 1. Verifica environment variables
echo $SHOPIFY_ADMIN_TOKEN  # Deve essere valorizzato
echo $GSPREAD_SHEET_ID     # Deve essere valorizzato

# 2. Installa dipendenze (se necessario)
pip install -r requirements.txt

# 3. Test dry-run
python fix_prices.py --dry-run

# 4. Leggi output e verifica che tutto sia ok
# Se ci sono errori, risolvili prima di procedere

# 5. Applica modifiche
python fix_prices.py --apply

# 6. Verifica su Shopify Admin che i prezzi siano corretti
```

---

**Script versione**: 1.0 (2026-01-03)
**Compatibile con**: Sync-Scansia v2.2+
