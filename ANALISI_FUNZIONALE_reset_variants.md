# Analisi Funzionale: Script Reset Varianti Shopify

**Versione**: 3.0  
**Data**: 25 Novembre 2025  
**Autore**: Racoon s.r.l.  
**Sistema**: Shopify Variant Reset & Inventory Management

---

## 1. PANORAMICA

### 1.1 Scopo del Sistema
Script Python per la cancellazione e ricreazione completa delle varianti di prodotti Shopify, preservando tutti i dati originali inclusi gli inventory levels multi-location.

### 1.2 Contesto Operativo
Lo script è utilizzato per:
- Riordinamento varianti quando l'API Shopify non permette modifiche in-place
- Fix di corruzioni dati nelle varianti
- Workaround per limitazioni API su prodotti con metafield collegati alle option
- Pulizia e normalizzazione struttura varianti

### 1.3 Ambiente di Esecuzione
- **Piattaforma**: Render.com (servizio cron)
- **Linguaggio**: Python 3.x
- **Database**: MySQL per backup temporaneo
- **API**: Shopify Admin REST API v2024-04

---

## 2. ARCHITETTURA SISTEMA

### 2.1 Stack Tecnologico

```
┌─────────────────────────────────────────┐
│         Render Cron Service             │
│  (Esecuzione schedulata o manuale)      │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│      reset_variants.py (Python)         │
│  ┌────────────────────────────────┐     │
│  │  • Fetch varianti da Shopify   │     │
│  │  • Backup DB (MySQL temporaneo)│     │
│  │  • Delete & Recreate varianti  │     │
│  │  • Restore inventory levels    │     │
│  │  • Cleanup location non usate  │     │
│  └────────────────────────────────┘     │
└──────┬────────────────────┬─────────────┘
       │                    │
       ▼                    ▼
┌─────────────┐      ┌──────────────┐
│  Shopify    │      │   MySQL DB   │
│  Admin API  │      │  (Temporary  │
│             │      │   Tables)    │
└─────────────┘      └──────────────┘
```

### 2.2 Dipendenze
```python
requests                 # HTTP API calls a Shopify
mysql-connector-python   # Connessione database MySQL
```

---

## 3. CONFIGURAZIONE

### 3.1 Variabili d'Ambiente Richieste

| Variabile | Descrizione | Esempio |
|-----------|-------------|---------|
| `SHOPIFY_DOMAIN` | Dominio store Shopify | `racoon-lab.myshopify.com` |
| `SHOPIFY_TOKEN` | Access token API Admin | `shpat_xxxxxxxxxxxxx` |
| `DB_HOST` | Host database MySQL | `mysql.render.com` |
| `DB_USER` | Username database | `admin` |
| `DB_PASS` | Password database | `********` |
| `DB_NAME` | Nome database | `shopify_sync` |
| `PRODUCT_IDS` | IDs prodotti (comma-separated) | `15389702455628,12345678` |

### 3.2 File di Configurazione

**render.yaml**
```yaml
services:
  - type: cron
    name: shopify-sync
    schedule: "@daily"
    env: python
    buildCommand: ""
    startCommand: "python reset_variants.py"
```

**requirements.txt**
```
requests
mysql-connector-python
```

---

## 4. FLUSSO OPERATIVO DETTAGLIATO

### 4.1 Overview Logico

```
START
  │
  ├─ Connessione DB & Setup tabelle temporanee
  │
  └─ Per ogni PRODUCT_ID:
       │
       ├─ STEP 1: Fetch tutte le varianti da Shopify
       │
       ├─ STEP 2: Backup in MySQL
       │    ├─ Dati variante (JSON completo)
       │    └─ Inventory levels (tutte le location)
       │
       ├─ STEP 3: Cancella varianti 2-N
       │
       ├─ STEP 4: Ricrea varianti 2-N da backup
       │    └─ Skip varianti con "perso" nel titolo
       │
       ├─ STEP 5: Cancella variante #1
       │
       ├─ STEP 6: Ricrea variante #1 da backup
       │    └─ Skip se contiene "perso"
       │
       ├─ STEP 7: Ripristina inventory levels
       │    └─ Per ogni location originale
       │
       └─ STEP 8: Cleanup location extra
            └─ Rimuove location non presenti nell'originale
END
```

### 4.2 STEP 1: Fetch Varianti

**Endpoint**: `GET /admin/api/2024-04/products/{id}/variants.json`

**Dati recuperati per ogni variante**:
- `id`: ID univoco variante
- `inventory_item_id`: ID inventory item
- `option1`, `option2`, `option3`: Valori opzioni
- `price`, `compare_at_price`: Prezzi
- `sku`, `barcode`: Codici identificativi
- `inventory_management`: Tracking inventory (shopify/null)
- `inventory_policy`: Politica vendita (deny/continue)
- `weight`, `weight_unit`: Peso prodotto
- Altri campi (taxable, requires_shipping, etc.)

### 4.3 STEP 2: Backup Database

**Tabelle temporanee create**:

```sql
-- Backup varianti
CREATE TEMPORARY TABLE variant_backup (
    id BIGINT,
    product_id BIGINT,
    inventory_item_id BIGINT,
    variant_json TEXT,           -- JSON completo variante
    position INT,                -- Posizione originale
    PRIMARY KEY (product_id, id)
);

-- Backup inventory levels
CREATE TEMPORARY TABLE inventory_backup (
    variant_id BIGINT,
    inventory_item_id BIGINT,
    location_id BIGINT,          -- ID location Shopify
    available INT,               -- Quantità disponibile
    PRIMARY KEY (variant_id, location_id)
);
```

**Processo backup inventory**:
1. Per ogni variante con `inventory_management != null`
2. Chiama `GET /inventory_levels.json?inventory_item_ids={id}`
3. Salva **tutte** le location con relative quantità

### 4.4 STEP 3-6: Strategia Delete & Recreate

**Logica senza DUMMY** (compatibile con metafield):

```
Varianti originali: [V1, V2, V3, ..., VN]

1. Delete V2-VN     → Resta solo [V1]
2. Recreate V2-VN   → Ora: [V1, V2', V3', ..., VN']
3. Delete V1        → Resta: [V2', V3', ..., VN']
4. Recreate V1      → Finale: [V1', V2', V3', ..., VN']
```

**Motivo della strategia**:
Shopify richiede sempre almeno 1 variante attiva. Non è possibile cancellare tutte le varianti contemporaneamente.

**Filtro "perso"**:
```python
if "perso" in v.get("title", "").lower():
    # Skip ricreazione di questa variante
    continue
```

### 4.5 STEP 7: Ripristino Inventory

**Endpoint**: `POST /admin/api/2024-04/inventory_levels/set.json`

**Payload**:
```json
{
  "location_id": 8251572336,
  "inventory_item_id": 55507789152588,
  "available": 1
}
```

**Mapping varianti**:
- Dizionario `{old_variant_id: new_inventory_item_id}`
- Query al DB: recupera location e quantità per `old_variant_id`
- Set inventory sul `new_inventory_item_id`

### 4.6 STEP 8: Cleanup Location Extra

**Problema risolto**:
Quando Shopify crea una nuova variante con inventory_management attivo, crea automaticamente inventory_levels per **tutte** le location disponibili nel negozio (default a 0).

**Soluzione**:
1. Query DB: quali location aveva la variante originale?
2. Fetch attuali: quali location ha la variante nuova?
3. Per ogni location NON presente nell'originale → DELETE

**Endpoint**: `DELETE /admin/api/2024-04/inventory_levels.json?inventory_item_id={id}&location_id={loc}`

**Risultato**:
- Location originali: mantenute con quantità corrette
- Location extra: rimosse → stato "Non stoccato" in Shopify

---

## 5. GESTIONE ERRORI E RESILIENZA

### 5.1 Rate Limiting

**Strategia Exponential Backoff**:
```python
def safe_request(method, url, max_retries=5):
    for attempt in range(max_retries):
        if res.status_code == 429:
            wait = 2 ** attempt  # 1s, 2s, 4s, 8s, 16s
            sleep(wait)
            continue
```

**Limiti Shopify**:
- 2 chiamate/secondo per endpoint (bucket leaky)
- Sleep 0.6s tra chiamate consecutive (safety margin)

### 5.2 Errori HTTP

**Codici gestiti**:
- `429`: Rate limit → Retry con backoff
- `422`: Validation error → Log dettagliato + skip
- `4xx/5xx`: Altri errori → Log + eccezione

**Logging errori**:
```python
try:
    error_detail = res.json()
    log(f"❌ Errore API {res.status_code}: {json.dumps(error_detail, indent=2)}")
except:
    log(f"❌ Errore API {res.status_code}: {res.text}")
```

### 5.3 Database Connection

**Auto-reconnect**: Non implementato (esecuzione breve)  
**Transazioni**: Commit esplicito dopo ogni backup  
**Tabelle temporanee**: Auto-distrutte a fine sessione

---

## 6. LOGGING E MONITORAGGIO

### 6.1 Formato Log

```
[YYYY-MM-DD HH:MM:SS] {emoji} Messaggio
```

**Emoji utilizzati**:
- 📦 Elaborazione prodotto
- 🔍 Trovate varianti
- 💾 Backup dati
- 🗑️ Cancellazione
- 🔄 Ricreazione
- 📍 Inventory operations
- 🧹 Cleanup
- ✅ Successo
- ❌ Errore
- ⚠️ Warning
- ⏭️ Skip

### 6.2 Livelli di Dettaglio

**Livello 1 - Prodotto**:
```
[timestamp] 📦 Elaborazione prodotto: 15389702455628
[timestamp] ✅ Prodotto 15389702455628 completato con successo!
```

**Livello 2 - Step**:
```
[timestamp] 🔍 Trovate 12 varianti
[timestamp] 💾 Backup varianti e inventory levels...
[timestamp] 🗑️ Cancellazione varianti dalla 2 alla N...
```

**Livello 3 - Dettaglio**:
```
[timestamp]   💾 Backup inventory: variant 56062973968716, location 8251572336, qty 0
[timestamp]   ✅ Cancellata variante 56062975082828 (Outlet - 42)
[timestamp]   🔄 Ripristino inventory: location 8251572336, qty 1
```

---

## 7. CASI D'USO E SCENARI

### 7.1 Caso A: Prodotto Standard (senza metafield)

**Caratteristiche**:
- Taglie NON collegate a metafield
- Inventory in "magazzino" location
- Tutte le varianti da mantenere

**Comportamento**:
✅ Delete & recreate senza errori  
✅ Inventory ripristinato correttamente  
✅ Location "magazzino" mantenuta  
✅ Location extra rimosse

### 7.2 Caso B: Prodotto con Metafield su Option

**Caratteristiche**:
- Taglie collegate a metafield Shopify
- Inventory in location "promo"
- Alcune varianti con "perso" da skippare

**Comportamento**:
✅ Strategia senza DUMMY funziona (no modifica option values)  
✅ Inventory in "promo" ripristinato  
✅ Location "magazzino" rimossa → "Non stoccato"  
✅ Varianti "perso" non ricreate

### 7.3 Caso C: Prodotto Multi-Location

**Caratteristiche**:
- Inventory distribuito su 3+ location
- Quantità diverse per location
- Mix di varianti con/senza inventory tracking

**Comportamento**:
✅ Tutte le location originali mantenute  
✅ Quantità corrette per ogni location  
✅ Varianti senza tracking → skip cleanup  
✅ Location extra rimosse

---

## 8. LIMITAZIONI E VINCOLI

### 8.1 Limitazioni Tecniche

1. **Shopify API Limits**:
   - Max 2 req/sec per endpoint
   - Timeout dopo 5 retry (max 31 secondi)

2. **Varianti con Immagini**:
   - Le associazioni immagine-variante NON sono preservate
   - Richiede associazione manuale post-script

3. **Metafield Variante**:
   - Solo metafield su product/option sono gestiti
   - Metafield custom sulle varianti NON vengono copiati

4. **Limiti MySQL**:
   - Tabelle temporanee: max session lifetime
   - TEXT field: max 65KB per variant_json

### 8.2 Vincoli Operativi

1. **Product IDs**:
   - Devono essere validi e accessibili con il token fornito
   - Prodotto deve avere almeno 1 variante

2. **Database**:
   - Deve essere accessibile durante tutta l'esecuzione
   - Permessi: CREATE TEMPORARY TABLE, SELECT, INSERT, DELETE

3. **Tempo Esecuzione**:
   - ~1 minuto per prodotto con 10 varianti
   - ~30 secondi per backup/restore inventory
   - Render timeout: 300s (5 minuti) per job cron

---

## 9. TROUBLESHOOTING

### 9.1 Errori Comuni

#### Errore: "Cannot set name for an option value linked to a metafield"

**Causa**: Prodotto con metafield su option  
**Soluzione**: Già gestito - strategia senza DUMMY  
**Verifica**: Log mostra strategia delete 2-N → recreate 2-N → delete 1 → recreate 1

#### Errore: "The variant 'X' already exists"

**Causa**: Variante non cancellata correttamente nello step precedente  
**Soluzione**: 
1. Verificare log step 3/5 (cancellazione)
2. Controllare permission token Shopify
3. Retry manuale

#### Errore: "422 Client Error" durante ricreazione

**Causa**: Dati variante non validi (SKU duplicato, option non valida, etc.)  
**Log**: Mostra dettagli errore JSON di Shopify  
**Soluzione**: Correggere dati nel prodotto originale, poi retry

### 9.2 Inventory Non Corretto

#### Location "Magazzino" a 0 invece di "Non stoccato"

**Causa**: Step 8 (cleanup) non eseguito correttamente  
**Verifica**: Cercare nei log "🧹 Pulizia location inventory"  
**Soluzione**: 
1. Verificare che inventory_backup contenga dati
2. Controllare mapping variant_id corretto
3. Manual cleanup via Shopify admin se necessario

#### Quantità perse dopo script

**Causa**: Backup inventory non salvato (inventory_management = null)  
**Verifica**: Log deve mostrare "💾 Backup inventory" per ogni variante  
**Soluzione**: Verificare che varianti originali avevano inventory_management attivo

### 9.3 Performance Issues

#### Script lento (>5 minuti)

**Causa**: Troppi prodotti o varianti  
**Soluzione**: 
1. Ridurre numero prodotti in PRODUCT_IDS
2. Aumentare timeout Render (max 900s per web service)
3. Ottimizzare sleep time (rischio rate limit)

#### Rate limit frequenti

**Causa**: Sleep time insufficiente  
**Soluzione**: Aumentare sleep da 0.6s a 1.0s

---

## 10. MANUTENZIONE E EVOLUZIONE

### 10.1 Aggiornamenti Shopify API

**Versione attuale**: 2024-04  
**Prossimo update**: Verificare deprecation ogni 3 mesi  
**Breaking changes**: Testare su store dev prima di produzione

### 10.2 Monitoraggio Continuo

**KPI da tracciare**:
- Tempo medio elaborazione per prodotto
- Tasso errori 422 (validazione)
- Tasso successo inventory restore
- Numero location cleanup per run

### 10.3 Backup e Recovery

**Backup automatico**:
- Tabelle MySQL temporanee → NON persistenti
- Considerare dump pre-script per prodotti critici

**Recovery procedure**:
1. Identificare prodotto corrotto
2. Recuperare dati originali da store history/backup
3. Manual fix via Shopify admin
4. Retry script

---

## 11. SECURITY E COMPLIANCE

### 11.1 Gestione Credenziali

- ✅ Token API in variabili d'ambiente (non hardcoded)
- ✅ Database password in variabili d'ambiente
- ⚠️ Logging NON deve includere token/password

### 11.2 Accesso Dati

**Permessi token Shopify richiesti**:
- `read_products`
- `write_products`
- `read_inventory`
- `write_inventory`

**Permessi database**:
- CREATE TEMPORARY TABLE
- SELECT, INSERT, DELETE su tabelle temporanee

### 11.3 Audit Trail

**Log conservati su**:
- Render dashboard (7 giorni)
- Considerare export su storage esterno per audit

---

## 12. CONTATTI E SUPPORTO

**Manutentore**: Racoon s.r.l.  
**Piattaforma**: Render.com  
**Store**: racoon-lab.myshopify.com  
**Database**: MySQL on Render

**Documentazione esterna**:
- [Shopify Admin API](https://shopify.dev/api/admin-rest)
- [Inventory Management](https://shopify.dev/api/admin-rest/2024-04/resources/inventorylevel)
- [Render Cron Jobs](https://render.com/docs/cronjobs)

---

## CHANGELOG

### v3.0 (2025-11-25)
- ✅ Aggiunto cleanup location inventory extra (STEP 8)
- ✅ Fix: query DB diretta per original_locations
- ✅ Logging dettagliato per debugging cleanup

### v2.0 (2025-11-25)
- ✅ Eliminata strategia DUMMY (compatibilità metafield)
- ✅ Nuova logica: delete 2-N → recreate 2-N → delete 1 → recreate 1
- ✅ Aggiunto filtro "perso" nel titolo varianti

### v1.0 (2025-11-25)
- ✅ Implementazione iniziale con strategia DUMMY
- ✅ Backup inventory multi-location
- ✅ Ripristino inventory levels

---

**Fine Documento**
