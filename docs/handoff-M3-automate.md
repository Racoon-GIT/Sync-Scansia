# Handoff → AUTOMATE (Make) — return-signal integration (unblocks M3)

Scansia Manager = new single-origin FastAPI web service replacing the 3 Sync-Scansia cron jobs (already suspended). Branch `feat/scansia-manager`. Library layer complete and tested (246 tests local / 344 in a venv with fastapi); this handoff defines the Make ⇄ tool sheet contract that M3 (publish-live) depends on.

> AUTOMATE is not one of the 3 IT peers (SVILUPPO/SERVER/CLIENT) → this is an owner/Ale action or a future AUTOMATE session, NOT an `OPEN-HANDOFFS` peer row.

> ⛔ **BLOCKING M3.** M3 (publish-live) must not ship until this contract is settled AND the one-time cutover backfill has run. **Without the backfill, the first publish re-inflates the stock of every already-published outlet** — a stock-corruption incident (phantom double stock → oversellable pairs). The schema-position + header-vs-index agreement (§7) is an explicit M3 gate.

> **Verified 2026-07-08** (`make-operator`, live Make REST API — org "My Lab" / team "My Team", zone
> `eu1.make.com`). No external "Make owner" to ask — it's Racoon's own account, inspected directly:
> scenario blueprints for `2215567` ("TRELLO - Rientrate", the return-append) and `678434` ("Shopify -
> Nuovo Ordine") pulled and read module-by-module; live sheet header row confirmed with Ale. All 5 open
> questions in §7 resolved with evidence. **One net-new, previously-unknown risk found: a second
> concurrent sheet-writer — see §9.**

---

## 1. Context — the return-signal model

The sheet is **append-oriented**. Make appends **one row per physical return** (cambio taglia / reso) carrying `(SKU, Size, qty)`. Critically:

- **`qty` is the quantity of that single return event — a DELTA to ADD, NOT an absolute/accumulated stock figure.** The tool must never derive correct stock from the static `Qta` column. (`parse_qta`, `reader.py:218-236`; delta add, `outlet_service.py:311,641`.)
- The append **is** the fresh-return signal. The tool consumes each appended row as a **one-shot idempotent delta**: it resolves the row to the outlet (col Q), checks it is genuine (`not reconciled`), **sums** the qty onto the Promo sell-location `available` **exactly once**, then marks the row `reconciled`. (`iter_unreconciled`, `reader.py:416-425`; one-shot `_apply_delta`, `outlet_service.py:632-653`.)
- **Idempotency is by construction** (PULL model): consumers select pending rows via `iter_unreconciled` (yields rows where `reconciled` is falsy); a reconciled row is never re-emitted, so no re-add. `_apply_delta` compares live Promo `available` vs the frozen preview baseline: if already at/above target it marks reconciled and warns `delta_already_applied` with **no re-add** — it prefers under-count to double-count.
- **Duplicate `(SKU, Size)` rows are LEGITIMATE.** Multiple size-swaps on the same model over time are distinct return events and are **SUMMED, never deduped/rejected**: per normalized-size bucket `delta += r.qta` and each `row_uuid` is collected, one aggregate delta applied, then every contributing `row_uuid` marked reconciled. There is no per-`(SKU,Size)` dedup key. (`outlet_service.py:306-311,333`.)

Ingestion is **PULL by default (MVP)**: the tool reads non-reconciled rows on demand, decoupled from Make's timing. A Make webhook PUSH is only an optional lower-latency enhancement addable later — **Make is NOT required to push anything; the append alone is the contract.**

---

## 2. Sheet schema the tool now expects

Addressing is **always by normalized header NAME** (`_norm_key` lowercases/trims, maps `-`/` ` → `_`), **never by fixed column index** (`reader.py:20-21,198-200`). Column letters below are legacy positional hints from code comments only. First non-empty synonym wins per row.

| Column (header synonyms) | Purpose | Maps to Shopify | Owner |
|---|---|---|---|
| `prezzo_high` \| `prezzo_pieno` \| `full_price` (col H) | Full/original price, cleaned to 2-dec string (`_clean_price`) | variant `compareAtPrice` | Make/source |
| `prezzo_outlet` \| `prezzo_scontato` \| `sale_price` (col J) | Discounted/outlet price | variant `price` | Make/source |
| `size` \| `taglia` | Variant match key (PRIMARY), normalized (`42.0`→`42`, `42,5`→`42.5`) | matched to variant `Size` selectedOption | Make/source |
| `qta` \| `qty` | **Return-event delta** (see §1), summed per size | added to Promo-location `available` | Make/source |
| `sconto` \| `discount` | Discount %, raw string | none (informational) | Make/source |
| `online` | Eligibility flag ({si,sì,true,1,x,ok,yes}). Read by the `eligible_rows` helper (`reader.py:238-264`, currently defined but not wired into the live return-delta path); **the return-delta path never reads it** | none by the return path | see open Q (§7) |
| `sku` | Group key + **CAS identity guard** (`expected_sku`) on every write-back | none (not written) | Make/source |
| `product_id` (col Q) | Existing outlet product GID; MATCH anchor + SYNC write-back target | product GID (write-back anchor) | **TOOL-private** |
| **`row_uuid`** ⟶ NEW | Stable per-row identity; the **only** write-back key (never `(SKU,Size)`) | none (tool-internal id) | **TOOL-owned** |
| **`reconciled`** ⟶ NEW | Delta-applied state; falsy/empty = pending, truthy = applied | none (tool-internal state) | **TOOL-owned** |
| `_scansia_cutover` ⟶ NEW | Cutover sentinel — **presence** of the header means backfill has run; absence makes reads fail closed | none (idempotency marker) | **TOOL-owned** |
| any other column | Preserved verbatim in `CanonRow.raw`, addressed by header name | none (passthrough) | Make/source |

**The three control headers (`row_uuid`, `reconciled`, `_scansia_cutover`) are constants (`reader.py:32-34`) pinned to the RIGHT of Make's append range on purpose** (`_ensure_columns`, `writer.py:54-77`) so Make's header-name appends never collide with them. Written cell-by-cell (`update_cell`), never a bulk range rewrite → Make's unknown/extra columns are left untouched.

---

## 3. What Make must do / guarantee

- [x] **Append by HEADER NAME, not fixed column index.** **⚠ SUPERSEDED by §10 (2026-07-08 correction): the module is `includesHeaders: true` = already header-name/reorder-robust; the reading below misread the mapper's internal ordinal slots as positional addressing.** ~~CONFIRMED FALSE (2026-07-08)~~ — Make's
  `google-sheets:addRow` (scenario `2215567`, module `15`) maps `values` by literal numeric column index
  (`"0","1","2"…`), not header name. Today's indices happen to match the live header row, but a column
  insert/reorder in the sheet would silently misalign it with **no way for the tool to detect the drift**.
  Decision needed from Ale: either edit the Make scenario to append by header name (Make-side change), or
  accept fixed-index as a standing operational risk to monitor. See §7 Q3 and §9.
- [ ] **Accept that `row_uuid` and `reconciled` are pinned to the RIGHT** of Make's append range (safe default) so their addition cannot shift any Make write target. **Confirmed safe** — neither Make writer (`2215567` nor `678434`) touches past column index 14 ("Vendute il"); new tool columns from index 18+ (past the existing "Check", index 17) are untouched by Make.
- [x] **Return-append is Make's ONLY write to the sheet.** **CONFIRMED FALSE (2026-07-08)** — a second
  writer exists: scenario `678434` "Shopify - Nuovo Ordine" also writes to this sheet for outlet-matched
  order lines (row-number-based `updateRow`, no CAS). Full detail in §9 — this is a new, load-bearing
  finding, not just an unconfirmed assumption.
- [ ] **Never write/overwrite the tool-owned columns** `row_uuid` (tool writes it on first read) and `reconciled` (tool writes it after applying the delta).
- [ ] **Keep appending one row per physical return; never collapse/dedupe `(SKU,Size)` duplicates** — each duplicate is a distinct legitimate return the tool applies exactly once.
- [ ] **Never write the tool's internal write-back columns** — `Product_Id` / col Q (published outlet GID) and the delete/status write-back column are tool-private.

---

## 4. THE BACKFILL CUTOVER (one-time, BLOCKING, stock-corruption risk)

**What.** A one-time migration run **by the TOOL** at the FIRST ingestion of the sheet: `ScansiaSheet.backfill_cutover()` (`writer.py:160-198`). It (1) reads all values; (2) no-ops if the `_scansia_cutover` sentinel is already present → `BackfillReport(0, already_done=True)`; (3) appends `row_uuid` + `reconciled` to the RIGHT; (4) re-reads; (5) for **every** data row, stamps a fresh `row_uuid` if empty and sets `reconciled='true'` **UNCONDITIONALLY**; (6) **writes the `_scansia_cutover` sentinel header LAST**.

**Why.** Every pre-existing row's stock is **already live on Shopify** (those outlets are published, their returned-size qty already on the Promo location). Without the backfill, the first run would see every historical row as un-reconciled and **re-add its qty to Promo → re-inflating the stock of every already-published outlet** (a unique returned pair shows phantom double stock and becomes oversellable). Marking every pre-existing row `reconciled=true` is the guard that makes the tool apply deltas **only from returns appended after cutover** — `iter_unreconciled` then returns ZERO rows on a freshly-cut sheet.

**Safety properties.**
- **Idempotent** via the sentinel: a second call sees `_scansia_cutover` present and returns immediately. Sentinel written LAST → a crash mid-backfill leaves it absent and the next run safely redoes the pass (uuid stamping is skip-if-present).
- **Fail-closed downstream**: while the sentinel is absent, `read_canonical` / `iter_unreconciled` raise `CutoverNotDoneError` (`reader.py:323-326`) — historical rows are never auto-treated as pending. **So the very first operational prerequisite for Make integration is a single `backfill_cutover()` run.**

**DoD test.** Migrating a sheet of already-published outlets, the **FIRST run applies ZERO stock delta** (no Promo quantity change on any existing outlet). Only rows Make appends AFTER cutover produce a `+qty`.

---

## 5. Ownership of the `reconciled` flag

**The TOOL owns and is the sole writer of `reconciled` and `row_uuid`. Make never touches them.**

- `row_uuid` is minted by the tool (`str(uuid.uuid4())`), never by Make. Assigned at three points: (1) `backfill_cutover` stamps every pre-existing row lacking one; (2) `read_canonical(assign_uuids=True)` mints on FIRST read for any post-cutover Make-appended row lacking one (writes the empty uuid cell, plus `reconciled='false'` if that cell is also empty — see (c) below); (3) `read_canonical(assign_uuids=False)` [DRY_RUN/preview] generates an **ephemeral in-memory** uuid never persisted — reads never mutate the live sheet. (`reader.py:194-195,292-392`.)
- `reconciled` is written by the tool three ways: (a) `backfill_cutover` sets `'true'` unconditionally on every pre-existing row; (b) `mark_reconciled(row_uuid, expected_sku)` sets `true` after a delta is applied; (c) assign-on-read stamps `'false'` **only on a genuinely empty** cell while minting a missing uuid. Anti-re-inflate guard: a row already `reconciled=true` is **never** forced back to false because its uuid is missing. (`writer.py:40-45,146-148,190-193`; (c) false-stamp + anti-re-inflate guard live in `reader.py:375-383`, NOT writer.)
- **Make is expected to append rows with `reconciled` LEFT EMPTY (= pending).** The code only handles the empty→pending case and never depends on Make writing this column. (This is the *expected* Make behavior, to be confirmed with the owner — not asserted from code.)
- Reconciliation is guarded by `row_uuid` + `expected_sku` only (`mark_reconciled` carries **no** `product_id_guard`, `writer.py:148`).

**Write-back CAS** (GSheet has no native compare-and-swap): every mutation goes through `write_back(row_uuid, fields, *, expected_sku, product_id_guard=None)` (`writer.py:80-136`), keyed by `row_uuid` (never `(SKU,Size)`). It does ONE immediate re-read and, against that snapshot, verifies before writing: row_uuid still exists (else `row_not_found`), re-read `sku` == `expected_sku` (TOCTOU identity guard, else `sku_mismatch`), and if a `product_id_guard` is given the col-Q cell is empty or == guard (else `product_id_conflict`). Any mismatch aborts with **no write**.

---

## 6. Acceptance / DoD

- [x] Make confirmed to **append by header name** (not fixed index) — §7 Q3 resolved: **it's fixed
  index, not header name**. Ale to decide: fix the Make scenario, or accept + monitor the risk.
- [x] Make confirmed that **return-append is its ONLY sheet write** — §7 Q2 resolved: **it is not**.
  `678434` also writes (§9). Needs a sequencing decision before M3, not just a reconciled assumption.
- [ ] `row_uuid` / `reconciled` positions — **no Ale action needed**: confirmed safe at index 18+, both
  Make writers stay within index 0-14.
- [ ] `backfill_cutover()` executed once on the live sheet; `BackfillReport.rows_stamped` matches the historical row count, `already_done` false on first run / true on re-run.
- [ ] **Cutover DoD**: first post-cutover run applies **zero** Promo stock delta across all existing outlets.
- [ ] A post-cutover Make append of `(SKU, Size, qty>0)` produces exactly `+qty` on the Promo location once, then flips that row's `reconciled` to `true`; re-running the flow re-adds nothing.
- [ ] A duplicate `(SKU, Size)` append is summed (not rejected) and both `row_uuid`s end reconciled.
- [x] `online`-column read question (§7) resolved: `online` is the **legacy stock-availability flag**
  driven by both Make writers (NO on append, NO again on sellout) — **never** reuse it for delete
  write-back; use a tool-private column (`_scansia_status`) as already scoped as the fallback option.
- [ ] **NEW**: sequencing decision on the second writer (`678434`, §9) — row-number-based, no CAS, races
  against the tool's own row mutations. Needs Ale's call before M3 go-live.
- [ ] **NEW**: "Prezzo Outlet" (column index 9) is written by **neither** inspected Make scenario — unclear
  who populates it, if anyone. Ask Ale; not a Make-technical question.

---

## 7. Open questions — RESOLVED 2026-07-08 (verified live via Make REST API, `make-operator`)

Live header row (confirmed with Ale, 2026-07-08):
`BRAND(0) MODELLO BASE(1) TITOLO(2) SKU(3) TAGLIA(4) Qta(5) online(6) Prezzo High(7) Prezzo(8) Prezzo Outlet(9) Sconto(10) Aggiunte il(11) Ordine in entrata(12) Ordine in uscita(13) Vendute il(14) Note(15) Product_Id(16) Check(17)`

- [x] **Q1 — Does Make (or any other consumer) READ/WRITE the `online` column?** **Both — and it writes
  it, not just reads it.** `online` is the **legacy per-row stock-availability flag** of the *existing*
  Make automation, unrelated to any "delete" semantics: (a) scenario `2215567` writes `online="NO"` on
  every return-append (index 6, literal, in the same `addRow` — a fresh return starts "not yet online");
  (b) scenario `678434` flips it back to `"NO"` when that specific row's `Qta` is decremented to zero by
  a matching sale (module `168`, `{{if(146.\`5\`=1;"NO";146.\`6\`)}}`). **Verdict: do NOT reuse `online`
  for the tool's delete write-back** — it would collide with a still-live legacy accounting flag. Use the
  already-scoped fallback: a tool-private column (`_scansia_status=deleted`).
- [x] **Q2 — Is return-append truly Make's ONLY write to the sheet?** **No — and this is exhaustively
  confirmed, not a spot-check.** Verified two ways across **all 50 scenarios** in the account: (1)
  full-text scan of every blueprint for the sheet's file ID / name; (2) enumeration of **all 110**
  `google-sheets:*` modules account-wide with their actual `spreadsheetId`. Only **3 scenarios**
  reference this sheet: `2215567` (the return-append), `678434` "Shopify - Nuovo Ordine" (order
  decrement, full mechanics in §9), and `4100540` — an **`isinvalid` duplicate** of `678434` (same
  modules, same target; Make blocks invalid scenarios from executing, so it's dormant, not a live third
  writer, but is a broken duplicate worth cleaning up). One false-positive ruled out: `3077704` "…
  OnDemand" contains the string "Scansia" only as a Make-editor **module label** ("Controllo Scansia"),
  its actual `google-sheets` modules target an unrelated spreadsheet. **Two live writers, confirmed
  exhaustive, no third found.**
- [x] **Q3 — Column-position / header-vs-index agreement.** **⚠ SUPERSEDED by §10 (2026-07-08): actually `includesHeaders: true` = header-name stable-id, reorder-robust.** ~~Fixed index, confirmed~~ — Make's `addRow`
  (scenario `2215567`, module `15`) writes `values` keyed `"0".."12"` (skipping 9, 10), matched against
  the live header above. Today the indices happen to line up correctly (e.g. index 7 = "Prezzo High" ✓,
  index 16 = "Product_Id" ✓) — but this is **incidental alignment, not an enforced contract**: nothing
  stops a future column insert from silently breaking it, and the tool has no way to detect that drift
  from its side. `row_uuid`/`reconciled`/`_scansia_cutover` are safe to pin at index 18+ (past "Check",
  the last existing column at index 17) — confirmed neither Make writer touches past index 14.
- [x] **Q4 — Ownership of the flags (row_uuid/reconciled).** Consistent with the tool's assumption: Make
  does not reference these column names/positions in either inspected scenario (they don't exist in the
  sheet yet). Both Make writers stay within index 0-14, well clear of where the tool's new columns will
  live (18+). Low residual risk.
- [x] **Q5 — Draft-on-sale mechanics.** **Two separate actions happen on sellout, at different
  granularities.** Scenario `678434` route: when a specific row's `Qta` hits zero from a sale, module
  `168` flips that row's `online` to `"NO"` (per-row, see Q1). Separately, `shopify:searchProducts`
  (module `206`) + `shopify:updateProduct` (module `210`) draft the Shopify product when the **aggregate**
  "Totale_Paia" across the model reaches 0 (per-model, not per-row). The tool's existing design — reading
  DRAFT status live from Shopify, never re-deriving it from `Qta` — is correct and unaffected by either.

**New, not previously listed:**

- [ ] **"Prezzo Outlet" (index 9) is never written** by either inspected Make scenario. `2215567` writes
  the current `Price` to index 8 ("Prezzo") instead, one column short of "Prezzo Outlet". Unclear whether
  a separate manual/other process populates index 9, or if this is a genuine gap. **Ask Ale** — business
  logic question, not a Make-technical one.

---

## 8. Sequencing

**Blocks M3** (publish-live = "decommission cron; Make return-signal integration completed"). The one-time cutover backfill is explicitly labeled **BLOCKING M3**. Critical path: `M1a → M1b → [SERVER handoff + shared-secrets handoff] → M2 → M3 (gated by this AUTOMATE handoff) → M4 → M5`. AUTOMATE is one of the three external handoffs that drive the schedule (with SERVER for hosting/auth and shared-secrets for the dedicated SA); M1a/M1b are local and proceed in parallel while this matures.

**The tool side is already built and tested** — the reader/writer/service contract described here is implemented (`backend/gsheet/reader.py`, `backend/gsheet/writer.py`, `backend/services/outlet_service.py`; 246 tests local / 344 in a venv). What remains is the **Make-owner coordination in §3/§7 and the one-time `backfill_cutover()` run** before M3 ships.

---

## 9. NEW FINDING (2026-07-08) — second concurrent sheet-writer: `Shopify - Nuovo Ordine` (`678434`)

Verified directly via Make REST API (`make-operator`), not previously known to this handoff. Full blueprint
walked module-by-module (scenario `678434`, the load-bearing order scenario per `../../system-map.md`,
trigger: polling every 900s).

**The split (router `93`, gate: last iteration of a per-unit quantity repeat loop):**

- **Route 0 — non-outlet** (filter `"SE NO OUTLET - SI SKU - NO SCANSIA"`: title does NOT contain
  "Outlet" AND sku exists) → `mysql:StoredProcedure` (module `190`) checks live stock in `racoon` →
  `shopify:updateInventoryLevel` (out-of-stock → email alert; in-stock → set level). **This is the only
  MySQL-touching branch — it does NOT run for outlet lines.** (Correction from an earlier draft of this
  section: outlet orders decrement **nothing** via MySQL — the sheet is the sole ledger for outlet stock,
  not a third parallel system alongside MySQL. Thank Ale for the catch.)
- **Route 1 — outlet, matched in Scarpe_in_Scansia** (gated on the earlier `google-sheets:filterRows`,
  module `146`, finding ≥1 row where `SKU`=order SKU, `TAGLIA`=order variant, `Qta`>0) →
  `google-sheets:updateRow` (module `168`, filter `"SE SCANSIA aggiorno excel"`) on the row found by
  `146`, **by row number** (`{{146.__ROW_NUMBER__}}`) — **no stable id, no compare-and-swap**:
  - `Qta = Qta - 1`
  - `online = "NO"` **iff** the pre-decrement `Qta` was exactly 1, else unchanged (see §7 Q1)
  - `"Ordine in uscita"` (index 13) = order name; `"Vendute il"` (index 14) = order `created_at`
  - Then `shopify:searchProducts`/`updateProduct` (modules `206`/`210`) draft the Shopify product once
    the **aggregate** "Totale_Paia" across the model reaches 0 (§7 Q5).

**Why this matters for M3.** The tool's row_uuid+reconciled write-back (`writer.py:80-136`) assumes it and
Make never race on the same row identity. `678434` mutates rows **by row number**, read-then-write, on a
**15-minute poll** — independent of, and blind to, the tool's own reads/writes. If the tool inserts,
deletes, or reorders rows (e.g. during `backfill_cutover()`, or a future delete/cleanup pass) between
`678434`'s `filterRows` and `updateRow` calls, the row-number target can drift and the wrong row gets
decremented. This is a **standing, load-bearing race** the original contract didn't know to guard against
— it needs an explicit decision from Ale before M3 go-live (e.g.: accept the residual risk given low
concurrency odds in practice; add a re-verify-before-write step on the tool side; or migrate this Make
logic to also use `row_uuid`, which is a Make-scenario change, not a tool change).

---

## 10. SVILUPPO analysis & resolution (2026-07-08)

Reviewed the `make-operator` findings against the tool code, then a 2nd make-operator pass re-inspected the Make module schema. **All three concerns resolve to "no Make change needed"**; one business question remains for Ale.

**§9 second-writer race — NEUTRALIZED by a verified tool invariant.** The tool performs **zero row operations** on the sheet: a grep of `backend/gsheet/` + `backend/services/` shows no `insert_row` / `delete_row` / `append_row` / `sort` / `deleteDimension` — every mutation is a single-cell `update_cell`, plus columns appended to the RIGHT (`_ensure_columns`). **Row numbers are therefore stable under all tool operations**, so `678434`'s row-number-based `updateRow` cannot be broken by the tool shifting rows. Reinforcing this: (a) the concurrent poller `678434` writes **disjoint columns** — `Qta`(5), `online`(6), `Ordine in uscita`(13), `Vendute il`(14) — vs the tool's `Product_Id`(16), control cols (18+), price (8/9); per-cell `update_cell` writes are independent; (b) the tool **never derives stock from `Qta`**(5), the only cell `678434` decrements that could matter; (c) `2215567` only *appends* rows at the bottom → no existing row-number shift, no collision with the tool's existing-row cell writes. **Residual**: this rests on the invariant *"the tool never inserts/deletes/reorders sheet rows"* — it must be preserved (a future row-deleting feature would reintroduce the race). Recorded as load-bearing. → **No active race; no Ale decision needed beyond keeping the invariant.**

**`online` reuse — ALREADY handled in code.** `writer.mark_deleted` (`writer.py:153-157`, guard **CI-6**) writes a **parameterized** field, explicitly NOT hardcoded `online` — *"if Make reads the online column, callers pass a tool-private field."* So the make-operator verdict (leave `online` alone — it's a live legacy flag driven by BOTH Make writers) needs only that delete-callers pass **`_scansia_status`**, not a redesign. → **Decision: adopt `_scansia_status=deleted` for delete write-back** (tool creates it to the right; no Make impact; confirm the name with Ale).

**~~GENUINE OPEN DECISION — fixed-index append~~ RESOLVED: this was a MISREAD (corrected 2026-07-08, 2nd make-operator pass — SUPERSEDES §3 item 1, §6, §7 Q3).** `2215567`'s `addRow` (module `15`) has **`includesHeaders: true`** = header-name **stable-id** addressing per Make's documented contract (mappings survive column add / remove / reorder; break **only** on header **rename**). The blueprint's ordinal keys (`"0".."12"`) are the mapper's internal slots, NOT positional addressing — Make's addRow schema *only* accepts ordinal keys, so "map by header name" is not a separate representable mode: `includesHeaders: Yes` **is** that mode and it is already on (confirmed across ~48 google-sheets modules in the account — none key by name; none can). **→ Nothing to switch, no Make change, no Ale decision here.** Values `2215567` writes today: BRAND(A)…Ordine in entrata(M), skipping Prezzo Outlet(J) & Sconto(K); `online="NO"`, `Qta=1`.
- **Residual (narrow):** the stable-id breaks only if a header is **renamed** → operational rule *"don't rename columns in Scarpe_in_Scansia"* (add / reorder are safe). One community-reported edge case exists (mid-sheet insert with a manually-fixed header range → buffer-column workaround) — low probability, worth watching on this live sheet.
- Optional defense-in-depth: a tool-side startup assertion that known columns (`Qta`, `online`, `Product_Id`) still resolve by name.

**BUSINESS QUESTION for Ale (not Make-technical) — "Prezzo Outlet"(9) is populated by neither Make scenario.** `2215567` writes current price to `Prezzo`(8). The pricing module reads `prezzo_outlet`(9) → variant `price`. If idx 9 is unpopulated, pricing "repair-from-sheet" (modalità 2) has no source. Shopify prices are already sane (discharge-debt=0), so the store side is fine — but **who fills "Prezzo Outlet" on the sheet** (manual / old fix_prices / nobody)? Determines whether the sheet is a valid price source or the pricing GUI must write it fresh.

---

Blocks: M3
