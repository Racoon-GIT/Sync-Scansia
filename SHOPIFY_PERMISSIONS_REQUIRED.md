# Permessi Shopify Richiesti

## Problema Restrizione Canali

**Errore**:
```
Access denied for publications field.
Required access: `read_publications` access scope.
```

**Causa**: Token API Shopify non ha permission `read_publications`

---

## Permessi Richiesti (Completi)

### Per SYNC Workflow (Base)
✅ `read_products`
✅ `write_products`
✅ `read_inventory`
✅ `write_inventory`
✅ `read_product_listings`
✅ `write_product_listings`

### Per Restrizione Canali (NUOVO)
❌ `read_publications` - **MANCANTE**
❌ `write_publications` - **MANCANTE**

---

## Come Aggiungere Permessi

### Opzione 1: Shopify Admin UI

1. **Admin Shopify** → **Settings** → **Apps and sales channels**
2. Trova app/custom app con token in uso
3. Click **Configure** → **Admin API access scopes**
4. Aggiungi scopes:
   - ✅ `read_publications`
   - ✅ `write_publications`
5. **Save** → Genera nuovo token
6. Aggiorna `SHOPIFY_ADMIN_TOKEN` su Render

### Opzione 2: Shopify CLI (Custom App)

```bash
shopify app config push

# Modifica shopify.app.toml
[access_scopes]
scopes = "read_products,write_products,read_inventory,write_inventory,read_publications,write_publications"

# Reinstalla app
shopify app install
```

---

## Workaround Temporaneo

**Se non puoi aggiungere permissions subito**, disabilita restrizione canali:

```bash
# Su Render.com → Environment Variables
ENABLE_CHANNEL_RESTRICTION=false
```

**Effetto**:
- ✅ Sync continua a funzionare
- ✅ Reset varianti funziona
- ❌ Prodotti outlet pubblicati su TUTTI i canali (non solo Online Store)
- ⚠️ Dovrai unpublish manualmente da canali indesiderati

---

## Verifica Permessi Attuali

### Via GraphQL Explorer

```graphql
query {
  shop {
    name
    currencyCode
  }
  publications(first: 5) {
    nodes {
      id
      name
    }
  }
}
```

**Se errore** → Permission mancante
**Se funziona** → Permission OK

### Via REST API

```bash
curl -X GET "https://YOUR-STORE.myshopify.com/admin/api/2025-01/publications.json" \
  -H "X-Shopify-Access-Token: YOUR_TOKEN"
```

**200 OK** → Permission OK
**403 Forbidden** → Permission mancante

---

## Lista Completa Permessi Consigliati

```
# Prodotti
read_products
write_products
read_product_listings
write_product_listings

# Inventory
read_inventory
write_inventory

# Canali Vendita (NUOVO)
read_publications
write_publications

# Collections (già usato)
read_collections
write_collections

# Metafields (già usato)
read_metafields
write_metafields

# Opzionali (per future feature)
read_locations
read_orders
read_customers
```

---

## Test Dopo Aggiunta Permessi

1. Aggiorna token su Render
2. Restart service
3. Esegui sync
4. Verifica log:

```
INFO | Trovate X publications totali
INFO | Mantengo publication: Online Store (id: ...)
INFO | Da rimuovere publication: Facebook Shop (id: ...)
INFO | Unpublished da publication: ...
INFO | Canali vendita ristretti: solo Online Store
```

✅ Se vedi questi log → Funzionante
❌ Se vedi errore `Access denied` → Token non aggiornato

---

## Note

- **Custom App**: Permissions configurabili
- **Public App**: Richieste durante OAuth install
- **Private App** (legacy): Da migrare a Custom App
- Token cambia dopo modifica permissions → Aggiorna ENV
