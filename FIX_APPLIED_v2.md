# Fix Applicati - Round 2

## Problemi GraphQL API (2° Test Run)

### 1. `productVariantDelete` Non Esiste
**Errore**:
```
Field 'productVariantDelete' doesn't exist on type 'Mutation'
```

**Causa**: Shopify API 2025-01 non supporta questa mutation GraphQL

**Fix**: Migrato a **REST API DELETE**
```python
# PRIMA (GraphQL ❌)
mutation($id: ID!) {
  productVariantDelete(id: $id) { ... }
}

# DOPO (REST ✅)
DELETE /admin/api/2025-01/variants/{variant_id}.json
```

---

### 2. Type Mismatch `ProductVariantInput`
**Errore**:
```
Type mismatch on variable $input and argument variants
([ProductVariantInput!]! / [ProductVariantsBulkInput!]!)
```

**Causa**: Schema GraphQL richiede `ProductVariantsBulkInput`, non `ProductVariantInput`

**Fix**: Migrato a **REST API POST**
```python
# PRIMA (GraphQL ❌)
mutation($productId: ID!, $input: [ProductVariantInput!]!) {
  productVariantsBulkCreate(productId: $productId, variants: $input) { ... }
}

# DOPO (REST ✅)
POST /admin/api/2025-01/products/{product_id}/variants.json
{
  "variant": {
    "option1": "35",
    "price": "99.00",
    "sku": "ABC123",
    "inventory_management": "shopify",
    "inventory_policy": "deny"
  }
}
```

---

### 3. Missing `variables` Argument
**Errore**:
```
Shopify.graphql() missing 1 required positional argument: 'variables'
```

**Fix**: Aggiunto parametro vuoto
```python
# PRIMA
data = self.graphql("""query { ... }""")

# DOPO
data = self.graphql("""query { ... }""", {})
```

---

## Modifiche ai File

### `src/sync.py` (Classe Shopify)

#### Metodo `variant_delete()` (linea 669-677)
```python
def variant_delete(self, variant_gid: str):
    """Elimina variante via REST"""
    variant_id = _gid_numeric(variant_gid)
    if not variant_id:
        raise RuntimeError(f"Invalid variant GID: {variant_gid}")

    # DELETE via REST
    self._delete(f"/variants/{variant_id}.json")
```

#### Metodo `variant_create()` (linea 679-707)
```python
def variant_create(self, product_gid: str, variant_input: Dict[str, Any]) -> Dict[str, Any]:
    """Crea variante via REST"""
    product_id = _gid_numeric(product_gid)
    if not product_id:
        raise RuntimeError(f"Invalid product GID: {product_gid}")

    # POST via REST
    payload = {"variant": variant_input}
    response = self._post(f"/products/{product_id}/variants.json", json=payload)

    variant_data = response.get("variant", {})
    if not variant_data:
        raise RuntimeError("variant_create: nessuna variante creata")

    # Converti response REST in formato GraphQL-like
    return {
        "id": f"gid://shopify/ProductVariant/{variant_data.get('id')}",
        "title": variant_data.get("title"),
        "sku": variant_data.get("sku"),
        "price": variant_data.get("price"),
        "compareAtPrice": variant_data.get("compare_at_price"),
        "inventoryItem": {
            "id": f"gid://shopify/InventoryItem/{variant_data.get('inventory_item_id')}"
        } if variant_data.get("inventory_item_id") else None,
        "selectedOptions": [
            {"name": "Size", "value": variant_data.get("option1")}
        ] if variant_data.get("option1") else []
    }
```

#### Metodo `get_publications()` (linea 709-718)
```python
def get_publications(self) -> List[Dict[str, Any]]:
    """Recupera canali di pubblicazione"""
    data = self.graphql("""
    query {
      publications(first: 50) {
        nodes { id name }
      }
    }
    """, {})  # FIX: Aggiungi variables vuoto
    return data.get("publications", {}).get("nodes", [])
```

---

### `src/variant_reset.py`

#### Funzione `_build_variant_input()` (linea 236-279)
```python
def _build_variant_input(variant_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Costruisce input variante per REST API POST /products/{id}/variants.json

    Conversione formato GraphQL → REST:
    - "options" array → "option1", "option2", "option3" fields
    - "inventoryManagement" → "inventory_management" (lowercase)
    - "weightUnit" → "weight_unit" (snake_case)
    """
    selected_options = variant_data.get("selectedOptions", [])

    # Estrai option values (max 3)
    option1 = option2 = option3 = None

    for idx, opt in enumerate(selected_options):
        if idx == 0:
            option1 = opt.get("value")
        elif idx == 1:
            option2 = opt.get("value")
        elif idx == 2:
            option3 = opt.get("value")

    # Formato REST API
    variant_input = {
        "option1": option1,
        "option2": option2,
        "option3": option3,
        "price": str(variant_data.get("price", "0.00")),
        "compare_at_price": variant_data.get("compareAtPrice"),
        "sku": variant_data.get("sku"),
        "barcode": variant_data.get("barcode"),
        "inventory_management": "shopify" if variant_data.get("inventoryItem") else None,
        "inventory_policy": "deny",  # REST usa lowercase
        "requires_shipping": True,
        "taxable": True,
        "weight": 0.0,
        "weight_unit": "kg",  # REST usa snake_case
    }

    # Rimuovi None values
    return {k: v for k, v in variant_input.items() if v is not None}
```

---

## Differenze GraphQL vs REST

| Aspetto | GraphQL Format | REST Format |
|---------|---------------|-------------|
| Options | `options: ["35", "Nero"]` | `option1: "35", option2: "Nero"` |
| Inventory mgmt | `inventoryManagement: "SHOPIFY"` | `inventory_management: "shopify"` |
| Policy | `inventoryPolicy: "DENY"` | `inventory_policy: "deny"` |
| Weight unit | `weightUnit: "KILOGRAMS"` | `weight_unit: "kg"` |
| Response | Nested objects | Flat structure |
| Bulk operations | Supported (bulkCreate) | Single item only |

---

## Test Prossimo Run

**Aspettative**:
✅ Nessun errore GraphQL
✅ Delete varianti funzionante (REST)
✅ Create varianti funzionante (REST)
✅ Get publications funzionante
✅ Unpublish funzionante
✅ Reset varianti completato

**Comando**:
```bash
RUN_MODE=SYNC python -m main
```

**Monitorare log**:
- ✅ "Cancellazione varianti dalla 2 alla N..." → OK
- ✅ "Ricreazione varianti dalla 2 alla N..." → OK
- ✅ "Variante ricreata: ... → new inventory_item_id: ..." → OK
- ✅ "Reset varianti completato con successo!" → OK
- ✅ "Trovate X publications totali" → OK
- ✅ "Canali vendita ristretti: solo Online Store" → OK

---

## Note Tecniche

**Perché REST invece di GraphQL?**:
1. Shopify API 2025-01 ha rimosso `productVariantDelete` da GraphQL
2. `productVariantsBulkCreate` richiede `ProductVariantsBulkInput` (schema complesso)
3. REST API più stabile per operazioni CRUD su varianti
4. Conversione automatica REST response → GraphQL format per compatibilità

**Performance**:
- REST crea 1 variante per volta (vs bulk GraphQL)
- Per 10 varianti: ~10s (GraphQL bulk sarebbe ~1s)
- Accettabile per reset varianti (operazione occasionale)
- Benefit: maggiore affidabilità, meno errori schema
