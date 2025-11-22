# 🔄 REORDER COLLECTION BY DISCOUNT %

**Tool**: `reorder_collection.py`  
**Funzione**: Ordina prodotti di una collection per sconto percentuale decrescente  
**API**: Shopify GraphQL `collectionReorderProducts`

---

## 🎯 FUNZIONALITÀ

### Cosa Fa
1. **Recupera** tutti i prodotti dalla collection specificata
2. **Calcola** sconto percentuale per ogni prodotto: `(compareAtPrice - price) / compareAtPrice * 100`
3. **Ordina** prodotti per sconto decrescente (prodotti con sconto più alto primi)
4. **Applica** nuovo ordinamento sulla collection Shopify

### Ordinamento
- **Primario**: Sconto % decrescente (più alto → più basso)
- **Secondario**: Titolo alfabetico (A → Z) per prodotti con stesso sconto
- **Prodotti senza sconto**: Finiscono in fondo (sconto = 0%)

---

## 📋 REQUISITI

### ENV Variables
```bash
SHOPIFY_STORE=yourstore.myshopify.com
SHOPIFY_ADMIN_TOKEN=shpat_xxxxxxxxxxxxx
SHOPIFY_API_VERSION=2025-01  # Opzionale, default 2025-01
```

### Dependencies
```bash
pip install requests
```

### Permessi Shopify
L'access token deve avere:
- `read_products`
- `write_products`
- `read_collections`
- `write_collections`

---

## 🚀 UTILIZZO

### Dry-Run (Preview)
```bash
python reorder_collection.py --collection-id 262965428289
```

Mostra:
- ✅ Elenco prodotti con sconto calcolato
- ✅ Nuovo ordinamento
- ✅ Distribuzione sconti
- ⚠️ NON applica modifiche

### Apply (Esecuzione Reale)
```bash
python reorder_collection.py --collection-id 262965428289 --apply
```

Applica il riordino su Shopify.

---

## 📊 OUTPUT ESEMPIO

### Scenario 1: Esecuzione Normale (senza errori)
```
======================================================================
REORDER COLLECTION BY DISCOUNT %
Collection ID: 262965428289
Collection GID: gid://shopify/Collection/262965428289
Mode: DRY-RUN
======================================================================

INFO: Recupero prodotti dalla collection...
INFO: Pagina 1: 50 prodotti
INFO: Pagina 2: 28 prodotti
INFO: Totale prodotti recuperati: 78

INFO: Calcolo sconti e ordinamento...
INFO: Primi 10 prodotti dopo ordinamento:
INFO:   1. Converse All Star Platform Rosa Borchie...      - Sconto:  45.0%
INFO:   2. Dr Martens 1460 Personalizzate Glitter...       - Sconto:  42.5%
INFO:   3. Birkenstock Boston Oro Limited Edition...       - Sconto:  40.0%
INFO:   4. Nike Air Force Custom Borchie Argento...        - Sconto:  38.0%
INFO:   5. Adidas Superstar Platform Strass Crystal...     - Sconto:  35.0%
INFO:   6. Vans Old Skool Black Studs...                   - Sconto:  33.5%
INFO:   7. Converse Chuck Taylor White Pearl...            - Sconto:  30.0%
INFO:   8. Dr Martens Jadon Boot Platform...               - Sconto:  28.0%
INFO:   9. Birkenstock Arizona Metallic Silver...          - Sconto:  25.0%
INFO:  10. Nike Dunk Low Custom Swarovski...               - Sconto:  22.0%

======================================================================
RIEPILOGO ORDINAMENTO:
Totale prodotti: 78
Distribuzione sconti:
  45%: 2 prodotti
  42%: 3 prodotti
  40%: 5 prodotti
  38%: 4 prodotti
  35%: 6 prodotti
  30%: 8 prodotti
  28%: 7 prodotti
  25%: 10 prodotti
  22%: 8 prodotti
  20%: 12 prodotti
  15%: 9 prodotti
  10%: 4 prodotti
  0%: 0 prodotti
======================================================================
⚠️  DRY-RUN: Usa --apply per applicare riordino
```

### Scenario 2: Con Retry (gestione errori automatica)
```
INFO: Recupero prodotti dalla collection...
INFO: Pagina 1: 50 prodotti
WARNING: 429 Rate limit (tentativo 1/5). Retry in 2.0s
INFO: Pagina 2: 28 prodotti
INFO: Totale prodotti recuperati: 78

INFO: Riordino collection su Shopify...
INFO: Riordino batch 1/1: 78 prodotti
WARNING: Server error 503 (tentativo 1/5). Retry in 1s
INFO: Job creato: gid://shopify/Job/12345, done: false
INFO: Attendo completamento 1 job...
INFO: ✓ Job completato: gid://shopify/Job/12345
INFO: ✅ Tutti i job completati in 8.3s
INFO: ✅ Riordino completato
```

### Scenario 3: Con Timeout e Retry Massimi
```
INFO: Riordino batch 1/2: 250 prodotti
WARNING: Timeout (tentativo 1/5). Retry in 1s
WARNING: Timeout (tentativo 2/5). Retry in 2s
INFO: Job creato: gid://shopify/Job/12345, done: false
INFO: Pausa 1s prima del prossimo batch...
INFO: Riordino batch 2/2: 50 prodotti
WARNING: Server error 502 (tentativo 1/5). Retry in 1s
INFO: Job creato: gid://shopify/Job/12346, done: false
INFO: Attendo completamento 2 job...
INFO: ✓ Job completato: gid://shopify/Job/12345
INFO: ✓ Job completato: gid://shopify/Job/12346
INFO: ✅ Tutti i job completati in 15.2s
INFO: ✅ Riordino completato
```

---

## 🔧 DETTAGLI TECNICI

### Calcolo Sconto
```python
if compareAtPrice and compareAtPrice > 0:
    discount = ((compareAtPrice - price) / compareAtPrice) * 100
else:
    discount = 0.0
```

### Gestione Casi Edge
- **Prodotto senza varianti**: Skip con warning
- **Variante senza compareAtPrice**: Sconto = 0%
- **Price >= compareAtPrice**: Sconto = 0%
- **Prodotti con stesso sconto**: Ordine alfabetico per titolo

### Performance
- **Paginazione**: 50 prodotti per pagina (max GraphQL)
- **Batch reorder**: 250 prodotti per mutation (max Shopify)
- **Rate limit**: 0.7s tra chiamate (configurabile via ENV)
- **Retry**: 5 tentativi con backoff esponenziale (2^n, max 8s)
- **Timeout**: 30s per richiesta HTTP
- **Tempo stimato**: ~10-15s per 100 prodotti (include retry e job polling)

### Resilienza API (Lezioni da sync-scansia)
- **429 Rate Limit**: Rispetta `Retry-After` header automaticamente
- **5xx Server Errors**: Backoff esponenziale (1s, 2s, 4s, 8s)
- **Timeout**: Retry automatico su timeout (30s)
- **Network Errors**: Gestione `RequestException` con retry
- **Max Retries**: 5 tentativi (configurabile `SHOPIFY_MAX_RETRIES`)
- **Job Polling**: Attende completamento job asincroni (max 60s)
- **Delay tra Batch**: 1s tra batch per non saturare API

### Configurazione Resilienza (ENV)
```bash
# Opzionali per tuning
SHOPIFY_MIN_INTERVAL_SEC=0.7  # Intervallo minimo tra chiamate
SHOPIFY_MAX_RETRIES=5         # Max tentativi su errore
```

### API GraphQL Utilizzate

**1. Recupero Prodotti**
```graphql
query {
  collection(id: "gid://shopify/Collection/{id}") {
    products(first: 50, after: "cursor") {
      pageInfo { hasNextPage endCursor }
      edges {
        node {
          id
          title
          handle
          variants(first: 1) {
            edges {
              node {
                id
                price
                compareAtPrice
              }
            }
          }
        }
      }
    }
  }
}
```

**2. Riordino Collection**
```graphql
mutation collectionReorderProducts($id: ID!, $moves: [MoveInput!]!) {
  collectionReorderProducts(id: $id, moves: $moves) {
    job { id done }
    userErrors { field message }
  }
}
```

Dove `moves` è:
```json
[
  { "id": "gid://shopify/Product/123", "newPosition": "0" },
  { "id": "gid://shopify/Product/456", "newPosition": "1" },
  ...
]
```

---

## ⚠️ ATTENZIONI

### Smart Collection vs Manual Collection
- **Smart Collection**: Ordinamento può essere sovrascritto da regole automatiche
  - Verifica impostazioni collection: "Sort" deve essere "Manual"
- **Manual Collection**: Ordinamento persiste

### Conflitti Ordinamento
Se la collection ha impostato ordinamento automatico (es: "Best selling", "Price: low to high"), il riordino manuale viene ignorato.

**Soluzione**: Imposta collection su "Manual order" prima:
1. Shopify Admin → Products → Collections
2. Seleziona collection
3. Products → Sort → Manual

### Prodotti con Varianti Multiple
Il tool usa **solo la prima variante** per calcolare lo sconto. Se un prodotto ha varianti con prezzi diversi, potrebbe non essere ordinato ottimamente.

**Alternative**:
- Calcolare sconto medio di tutte le varianti
- Usare variante con sconto massimo
- Usare variante più venduta

(Modificabile nel codice se necessario)

---

## 🔄 USO RICORRENTE

### Script Automatizzato
```bash
#!/bin/bash
# reorder_outlet.sh

export SHOPIFY_STORE=yourstore.myshopify.com
export SHOPIFY_ADMIN_TOKEN=shpat_xxxxx

python reorder_collection.py \
  --collection-id 262965428289 \
  --apply

echo "Collection outlet riordinata!"
```

### Cron Job (ogni giorno alle 6am)
```cron
0 6 * * * /path/to/reorder_outlet.sh >> /var/log/reorder.log 2>&1
```

---

## 🐛 TROUBLESHOOTING

### Errore: "Collection not found"
- Verifica ID collection
- Verifica token abbia permessi `read_collections`

### Errore: "GraphQL errors: ..."
- Verifica API version compatibility
- Controlla formato GID: `gid://shopify/Collection/{id}`

### Ordinamento non applicato
- Verifica collection sort impostato su "Manual"
- Ricarica pagina admin Shopify (cache)
- Attendi qualche secondo (job asincrono)

### Prodotti mancanti
- Paginazione può avere limiti
- Verifica che tutti i prodotti abbiano varianti
- Controlla log per warning "skip"

---

## 📝 CHANGELOG

**v1.0 (2025-11-20)**
- ✅ Recupero prodotti con paginazione
- ✅ Calcolo sconto percentuale
- ✅ Ordinamento per sconto decrescente + alfabetico
- ✅ Riordino collection via GraphQL
- ✅ Dry-run mode
- ✅ Batch processing (250 prodotti per mutation)
- ✅ Logging dettagliato

---

## 💡 FUTURE ENHANCEMENTS

- [ ] Supporto multi-variante (sconto medio/max)
- [ ] Filtro per tag (solo outlet, solo sale, ecc)
- [ ] Ordinamento per altri criteri (prezzo, data, inventory)
- [ ] Export CSV ordinamento
- [ ] Notifiche email dopo riordino
- [ ] Dry-run con preview visual (HTML)

---

**File**: [reorder_collection.py](computer:///mnt/user-data/outputs/reorder_collection.py)  
**Dimensione**: ~250 righe  
**Dipendenze**: requests  
**Testato**: Shopify API 2025-01
