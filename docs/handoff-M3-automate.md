# Handoff → AUTOMATE (Make) — return-signal integration (unblocks M3)

Scansia Manager = new single-origin FastAPI web service replacing the 3 Sync-Scansia cron jobs (already suspended). Branch `feat/scansia-manager`. Library layer complete and tested (246 tests local / 344 in a venv with fastapi); this handoff defines the Make ⇄ tool sheet contract that M3 (publish-live) depends on.

> AUTOMATE is not one of the 3 IT peers (SVILUPPO/SERVER/CLIENT) → this is an owner/Ale action or a future AUTOMATE session, NOT an `OPEN-HANDOFFS` peer row.

> ⛔ **BLOCKING M3.** M3 (publish-live) must not ship until this contract is settled AND the one-time cutover backfill has run. **Without the backfill, the first publish re-inflates the stock of every already-published outlet** — a stock-corruption incident (phantom double stock → oversellable pairs). The schema-position + header-vs-index agreement (§7) is an explicit M3 gate.

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

- [ ] **Append by HEADER NAME, not fixed column index.** Fixed-position appends silently misalign the moment any column is inserted/moved. **Must be explicitly confirmed with the Make owner** (load-bearing — see §7).
- [ ] **Accept that `row_uuid` and `reconciled` are pinned to the RIGHT** of Make's append range (safe default) so their addition cannot shift any Make write target.
- [ ] **Return-append is Make's ONLY write to the sheet.** The append ⇄ tool write-back race lives **outside** the tool's intra-process mutex (a file/app lock does not reach Make's host); the `row_uuid` + `reconciled` design is what makes concurrent Make-append vs tool-write safe, and only if Make writes nowhere else.
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

- [ ] Make confirmed to **append by header name** (not fixed index) — §7 open Q resolved.
- [ ] Make confirmed that **return-append is its ONLY sheet write** (or all other writes enumerated and reconciled with §7).
- [ ] `row_uuid` / `reconciled` positions agreed with the Make owner, pinned to the RIGHT of the append range.
- [ ] `backfill_cutover()` executed once on the live sheet; `BackfillReport.rows_stamped` matches the historical row count, `already_done` false on first run / true on re-run.
- [ ] **Cutover DoD**: first post-cutover run applies **zero** Promo stock delta across all existing outlets.
- [ ] A post-cutover Make append of `(SKU, Size, qty>0)` produces exactly `+qty` on the Promo location once, then flips that row's `reconciled` to `true`; re-running the flow re-adds nothing.
- [ ] A duplicate `(SKU, Size)` append is summed (not rejected) and both `row_uuid`s end reconciled.
- [ ] `online`-column read question (§7) resolved before any `online=NO` delete write-back is enabled.

---

## 7. Open questions to resolve with the Make owner

- [ ] **Does Make (or any other consumer) READ the `online` column?** The tool wants to write `online=NO` on delete write-back. If `online` is read downstream, the tool must instead use a tool-private status column (e.g. `_scansia_status=deleted`) and leave `online` untouched. **Blocking prereq** before enabling the `online=NO` write-back. (Delete→publish loop safety needs a reliable per-row "outlet deleted" mark, disambiguated by GID / col Q, since some SKUs map to >1 outlet.)
- [ ] **Is return-append truly Make's ONLY write to the sheet?** Must be confirmed — the append ⇄ tool write-back race is outside the intra-process mutex and the `row_uuid` + `reconciled` safety model assumes Make writes nowhere else.
- [ ] **Column-position / header-vs-index agreement (M3 schema gate):** does Make append by header name or fixed index, and where exactly do `row_uuid` and `reconciled` sit? (Default if uncertain/index-based → pin both to the RIGHT.)
- [ ] **Ownership of the flags:** confirm the tool is the sole writer of `reconciled` and `row_uuid`, and Make never touches them.
- [ ] **Draft-on-sale mechanics:** when Make drafts a sold-out outlet, does it also write anything to the sheet (e.g. flip `online`), or does it only draft the product on Shopify? This determines whether draft-on-sale is a second Make sheet-write. (Tool reads the resulting DRAFT status **live from Shopify**, never re-sets it from the static `Qta`.)

---

## 8. Sequencing

**Blocks M3** (publish-live = "decommission cron; Make return-signal integration completed"). The one-time cutover backfill is explicitly labeled **BLOCKING M3**. Critical path: `M1a → M1b → [SERVER handoff + shared-secrets handoff] → M2 → M3 (gated by this AUTOMATE handoff) → M4 → M5`. AUTOMATE is one of the three external handoffs that drive the schedule (with SERVER for hosting/auth and shared-secrets for the dedicated SA); M1a/M1b are local and proceed in parallel while this matures.

**The tool side is already built and tested** — the reader/writer/service contract described here is implemented (`backend/gsheet/reader.py`, `backend/gsheet/writer.py`, `backend/services/outlet_service.py`; 246 tests local / 344 in a venv). What remains is the **Make-owner coordination in §3/§7 and the one-time `backfill_cutover()` run** before M3 ships.

Blocks: M3
