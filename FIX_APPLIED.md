# Fix Applicati - Test Run

## Problema Rilevato

```
ERROR: 'Shopify' object has no attribute 'variant_delete'
ERROR: 'Shopify' object has no attribute 'variant_create'
ERROR: 'Shopify' object has no attribute 'inventory_levels_get'
ERROR: 'Shopify' object has no attribute 'get_publications'
WARNING: Colonna Product_Id non trovata
```

---

## Fix Applicato

**Problema**: I nuovi moduli `variant_reset.py` e `channel_manager.py` chiamavano metodi non presenti nella classe `Shopify` locale in `sync.py`.

**Soluzione**: Aggiunti metodi mancanti alla classe `Shopify` in `src/sync.py` (linee 662-735):

```python
# NUOVI METODI AGGIUNTI:
def inventory_levels_get(inventory_item_id)    # Per variant_reset
def variant_delete(variant_gid)                # Per variant_reset
def variant_create(product_gid, variant_input) # Per variant_reset
def get_publications()                         # Per channel_manager
def unpublish_from_publication(...)            # Per channel_manager
```

**File modificato**: `src/sync.py` (aggiunti 5 metodi alla classe Shopify)

---

## Warning "Colonna Product_Id non trovata"

**NON è un errore critico**. Indica che:
- Google Sheet non ha colonna `Product_Id` dove scrivere il GID del prodotto outlet creato
- Write-back viene skippato (non critico)

**Opzionale - Per abilitare write-back**:
1. Aggiungi colonna `Product_Id` al Google Sheet
2. Al prossimo run, il sistema scriverà automaticamente i GID prodotti outlet

**Vantaggi write-back**:
- Tracciabilità prodotti outlet creati
- Evita duplicazioni su re-run (può verificare se outlet già esiste)

---

## Test Consigliato

Ri-esegui il workflow SYNC per verificare che:
1. ✅ Nessun errore `'Shopify' object has no attribute...`
2. ✅ Reset varianti completa senza errori
3. ✅ Restrizione canali funziona
4. ✅ Prodotto outlet corretto su Shopify

**Comando**:
```bash
RUN_MODE=SYNC python -m main
```

**Verifica su Shopify Admin**:
- Prodotto outlet creato con handle `-outlet`
- Pubblicato **solo** su "Online Store" (non su altri canali)
- Varianti ordinate correttamente
- Inventory corretto su location Promo
- Location Magazzino disconnessa

---

## Log Attesi (Successo)

```
INFO | Reset varianti per prodotto: gid://shopify/Product/...
INFO | Trovate 10 varianti
INFO | Backup varianti e inventory levels...
INFO | Cancellazione varianti dalla 2 alla N...
INFO | Ricreazione varianti dalla 2 alla N...
INFO | Cancellazione prima variante: ...
INFO | Ricreazione prima variante...
INFO | Ripristino inventory levels...
INFO | Pulizia location inventory non utilizzate...
INFO | Reset varianti completato con successo!
INFO | Reset varianti completato
INFO | Restrizione canali per prodotto: ...
INFO | Trovate X publications totali
INFO | Unpublished da publication: ...
INFO | Canali vendita ristretti: solo Online Store
INFO | ✅ SKU=... completato (X taglie)
```

---

## Disabilitare Nuove Funzionalità (Temporaneo)

Se necessario, disabilita singolarmente:

```bash
# Disabilita reset varianti
ENABLE_VARIANT_RESET=false

# Disabilita restrizione canali
ENABLE_CHANNEL_RESTRICTION=false

# Disabilita batch image upload
ENABLE_BATCH_IMAGE_UPLOAD=false

# Disabilita location cache
ENABLE_LOCATION_CACHE=false
```

**Per tornare a v1.0 completo**:
```bash
ENABLE_VARIANT_RESET=false
ENABLE_CHANNEL_RESTRICTION=false
ENABLE_BATCH_IMAGE_UPLOAD=false
ENABLE_LOCATION_CACHE=false
```

---

## Note Tecniche

**Architettura**:
- `sync.py` usa classe `Shopify` locale (legacy)
- `shopify_client.py` ha classe `ShopifyClient` (nuovo)
- Fix: metodi duplicati in `Shopify` per compatibilità

**Future refactoring** (opzionale):
- Migrare `sync.py` a usare `ShopifyClient` da `shopify_client.py`
- Eliminare duplicazione metodi
- Pro: codice più pulito
- Con: refactoring significativo, testing richiesto
