# Workaround Location IDs (Temporaneo)

## Problema
Token API non ha `read_locations` permission → 403 su `/locations.json`

## Soluzione Temporanea

Invece di cercare location by name, usa **ID diretti**.

### Trova i tuoi Location IDs

**Metodo 1 - Dai log precedenti**:
```
Location Magazzino trovata: ID=8251572336 Nome='Magazzino'
Location Promo trovata: ID=XXXXXX Nome='Promo'
```

**Metodo 2 - GraphQL Explorer** (se hai permission):
```graphql
query {
  locations(first: 10) {
    edges {
      node {
        id
        name
      }
    }
  }
}
```

**Metodo 3 - Shopify Admin**:
1. Settings → Locations
2. Click su location → guarda URL: `.../locations/[ID]`

### Configurazione ENV Variables

Invece di:
```bash
PROMO_LOCATION_NAME=Promo
MAGAZZINO_LOCATION_NAME=Magazzino
```

Usa:
```bash
PROMO_LOCATION_ID=8251572336        # Sostituisci con ID Promo
MAGAZZINO_LOCATION_ID=8251572336    # Sostituisci con ID Magazzino
```

### Codice da Modificare

**File**: `src/sync.py` linea ~848

**PRIMA**:
```python
promo_name = os.environ.get("PROMO_LOCATION_NAME")
if promo_name:
    promo = shop.get_location_by_name(promo_name)
```

**DOPO**:
```python
promo_id = os.environ.get("PROMO_LOCATION_ID")
if promo_id:
    promo = {"id": int(promo_id), "name": "Promo"}
```

Stesso per `MAGAZZINO_LOCATION_ID`.

---

## Vantaggi Workaround

✅ Funziona senza `read_locations` permission
✅ Più veloce (no API call)
✅ Compatibile con location cache

## Svantaggi

❌ Devi conoscere gli ID in anticipo
❌ Se cambi location, devi aggiornare ENV

---

## Come Tornare alla Soluzione Normale

Quando avrai sistemato le permissions:
1. Rimuovi `PROMO_LOCATION_ID` e `MAGAZZINO_LOCATION_ID`
2. Ripristina `PROMO_LOCATION_NAME` e `MAGAZZINO_LOCATION_NAME`
3. Il codice tornerà automaticamente a funzionare con nomi
