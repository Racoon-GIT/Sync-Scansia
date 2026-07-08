# "Inizializza" (init / cutover reconciliation) — locked spec 2026-07-08 (with owner)

The one-time **"Inizializza"** the operator runs **once, from within the GUI** (a first-run banner in the
dashboard, not a separate admin URL) before using the tool. It combines two concerns.

## A. Baseline — anti re-inflate (sheet-only, no Shopify) — ALREADY BUILT
`ScansiaSheet.backfill_cutover()` (`writer.py:160-198`): add the control columns (`row_uuid`,
`reconciled`, `_scansia_cutover`) to the RIGHT; stamp every existing data row with a `row_uuid` and
`reconciled=true`; write the `_scansia_cutover` sentinel LAST. Idempotent (sentinel), zero Shopify impact.
`reconciled=true` on ALL rows regardless of the online outcome below → the tool never re-adds
already-live stock. This half is unchanged.

## B. Online-flag reconciliation against Shopify reality — NEW, **Shopify-MUTATING**
For each sheet row with **`online=si`** (rows with `online=no` are left untouched — no claim to verify):

1. Resolve the **outlet** product for `(SKU + Size)` via the outlet-resolver (OUTLET membership + dedup)
   and match the variant on the row's **Size**. Multi-match (>1 outlet for the SKU) → **review**, do NOT
   auto-demote (ambiguous).
2. **"Truly online"** := outlet product **exists** AND status **ACTIVE** (not DRAFT) AND the variant for
   **this row's Size** has **`available > 0` on the Promo location** (`61184966721`).
3. **If truly online** → leave `online=si`. No change on either side.
4. **Otherwise** (product missing, OR DRAFT, OR this-size variant at 0 on Promo):
   - **If the product EXISTS** (DRAFT, or ACTIVE-but-this-size-zero) → set it to **DRAFT** on Shopify
     (`productUpdate status=DRAFT`; already-DRAFT = Shopify no-op). *(confirmed by owner: Q1 = this row's
     size only; Q2 = missing product ⇒ nothing to draft.)*
   - **If the product does NOT exist** → nothing to draft.
   - **Sheet (both cases):** set `online=NO`; set **`Vendute il` = current instant, UTC ISO 8601**
     matching Make's `created_at` format — **`YYYY-MM-DDTHH:MM:SS.000Z`** (owner-confirmed 2026-07-09,
     example `2025-04-25T06:54:44.000Z`). NOT date-only, NOT Europe/Rome — a full UTC timestamp so the
     column never ends up with two incompatible formats. *(Q3 = owner "esatto": written in ALL demotion
     cases, incl. non-sale ones like draft-with-stock.)*

## Safety / UX (this is the first live bulk Shopify mutation)
- **preview → confirm → audit**, same pattern as publish/delete/prices. Preview lists, split by reason:
  `kept-online` · `demote:missing` · `demote:draft` · `demote:sold-out-size` · `review:multi-match`.
- Confirm gesture + audit snapshot (before: online flags + product statuses; after: outcome). Reuse the
  hard-cap / second-confirm pattern if the demotion count is large.
- **GUI**: first-run banner/action inside the dashboard; disappears once the `_scansia_cutover` sentinel
  is present. No separate admin endpoint/URL (owner: "attiviamo/disattiviamo direttamente dalla GUI").
- **Ordering**: run baseline (A) first (adds control cols + reconciled), then the online-reconcile (B).
  Whole thing behind the owner deploy-gate + explicit confirm (first live Shopify write).
- Idempotent: A via sentinel; B naturally (a demoted row is now `online=no` → not re-checked; a
  truly-online row is left as-is).

## Existing building blocks to reuse (do NOT reinvent)
- baseline: `writer.backfill_cutover`, `_ensure_columns`.
- resolve + size-match: `services/resolvers.py` (outlet-resolver, dedup, multi-match→warning),
  `outlet_service` size-match helpers.
- read status + Promo inventory: `shopify/ops.py` `read_variant_inventory` (0-vs-absent), product status.
- set DRAFT: the status path used by publish (`productUpdate`/status) — reuse, don't add a new op if one exists.
- sheet write-back by header name (incl. `online`, `Vendute il`): `writer.write_back(row_uuid, fields, *, expected_sku)` (CAS).
- preview/confirm/audit: the `api/` vertical pattern (confirm-token HMAC drift-gate + `AuditSink`).

## Out of scope / non-goals
- No stock quantity changes on Shopify (drafting only; the returned-size stock is already live).
- Does not publish anything (online=si is only ever set by the operator's manual GUI action, not here).
