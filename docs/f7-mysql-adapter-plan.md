# F7 — Scansia Manager on MySQL: plan for GATE:1

Programme "Scansia → MySQL", phase F7 (`plan.md` §4: *"Adapter MySQL dietro `persistence/ports.py`,
utente DB dedicato, delta −1; go-live con i suoi gate"*, lanes Sync-Scansia + SERVER). Drafted
2026-09-30 by the coordinator `scansia-f6` at effort `max`. **Status: APPROVED 2026-10-06 — F7's `gate-1: Ale 2026-10-06` (D25, `PROGRAM.md`@`e87719d`:1724-1751; `gate1-receipts.log` line 661, plan_ref `docs/f7-mysql-adapter-plan.md`) on this file at `30718ec7…` (`f2714fb`); B-2's 10 NULL compare-ats decided with it (D25: `PRICE`) and folded in afterwards — E10, §6 B-2, nothing else; next: the two `GATE:spawn` (§7). Before that: REVISED 2026-10-06 — B-3 DECIDED 2026-10-06 10:18Z: *Event + working-window net* (D23, `PROGRAM.md`@`94d78d3`:1688-1704; §6 B-3). 2c ANSWERED 2026-10-05 22:30Z (D20, `PROGRAM.md`@`1b07888`:1527-1551): B-1, B-2 and the AUDIT tabs decided (§2 E7, §6); B-3 re-opened by Ale's question on an event-triggered decrement — the event option written into §6 B-3 on 2026-10-06 (D21 step 18; its facts re-read at their sources), diff-reviewed (D22 step 19: every cite re-read, 5 fixes, §10's last paragraph) and re-asked (`GATE:1 B-3`, D23); gate-1 not yet given. Before that: round 8's targeted re-review judged (§10 AB1–AB16; 3 major: AB1–AB3); its fixes applied (change list r8t, local); their diff review done (part 1: pins, design choices, 3 edits; part 2: the applier's 13 flags, 12 edits — §10); AB1–AB3 CLOSED (D18: every claim the fixes introduce re-read at its pin; mandatory build tests, §4; no reviewer round); next: 2c.** Round-1
fixes applied to §1–§9 by a sub-agent and diff-reviewed by the coordinator the same day (every hunk → an
R-row; the review's corrections and T31 in §10's intro). Do not show it to Ale before the re-review is judged. **A plan, not code**: nothing here is built
until Ale records `gate-1` for F7 in `PROGRAM.md` and grants the two `GATE:spawn` it names (§7). At
spawn, the Sync-Scansia lane imports this file verbatim as `docs/f7-mysql-adapter-plan.md`.

**Sources, pinned** (every `file:line` below resolves against these; re-check before quoting):

| Source | Revision |
|---|---|
| Sync-Scansia (the app) | local `feat/scansia-manager` **`fdfaf2f`** — 3 ahead of `origin/feat/scansia-manager`, `backend/` clean against the branch (2026-09-30); re-checked 2026-10-01 07:56Z, unchanged |
| STOCK_Manager (register owner): every path this file cites — `docs/scansia-design.md`, `docs/scansia-sp-spec.md`, `prisma/schema.prisma`, the 2026-09-11 migration, the two route files of E2 | **`origin/main` `92f6f90`**, kept as the citation pin; never read those paths from the working tree. Re-checked 2026-10-01 13:28Z: `origin/main` = local `main` = `9d99f18`; since `92f6f90` only `docs/scansia-design.md` moved, in `c867e58` and `abfcc8f` (§8.2 + §13 deviation 12, UI; `5eadda1` itself is TRACKER-only), none of the three since `a9fcf7a`; at HEAD design lines past ~596 sit +2, past ~940 +18. Re-checked 2026-10-03 ~21:16Z: `origin/main` = local `main` = `aa77acf`; of the three and the migration, only `docs/scansia-design.md` moved since `92f6f90`. Re-checked 2026-10-05 ~09:30Z: `origin/main` = local `main` = `06390aa` (= `git ls-remote`); `git ls-tree` finds the four paths (`docs/scansia-design.md`, `docs/scansia-sp-spec.md`, `prisma/schema.prisma`, `prisma/manual-migrations`); `92f6f90..06390aa` on them = `docs/scansia-design.md` 21+/3− only. |
| Make 678434 (order intake) | live **v265** since 2026-10-03 08:32:45Z (AUTOMATE `05f1d92`), canonical sha `12c085b3…d82efee9` (GETs 2026-10-03 08:36Z and 19:31Z); v264 (`dfc695e3…b5036`) is the 2026-09-24 → 10-03 text E4 and E11 also cite |
| `racoon` register | fifth production reading, 2026-09-30 00:06–00:10Z (`PROGRAM.md`@`96a20a7`:35); E13 at 2026-09-30 08:12–08:20Z; §10 §Background numbers at 23:29–23:34Z |
| Racoon-LAB — the 2026-09-24 stall: `INC` (`IT/AUTOMATE/docs/incident-2026-09-24-intake-block.md`) and `IT/SERVER/infra-changelog.md` | **`3d21853`** (SERVER's §5 settlement), **`bab9d57`** (idle-holder watch live), **`619ebf2`** (INC's last commit: holder named, LOOKUP fixed — every `INC@<rev>:line` below carries its revision), **`7afe058`** (SERVER's lock-free `SP_SCANSIA_LOOKUP`) |
| 678434's doc (`IT/AUTOMATE/docs/make-scenarios/678434-shopify-nuovo-ordine.md`) | **`0329757`** |
| Workspace docs cited by line: `IT/AUTOMATE/docs/f6-scansia-cutover-runbook.md`, `IT/_scansia-programme/register-checks.md`, `IT/SVILUPPO/docs/shared-database.md`, `IT/SVILUPPO/docs/deploy-inventory.md`, `IT/AUTOMATE/docs/make-scenarios/678443-shopify-ordine-cancellato.md` | **`9a78dca`**, **`c983e42`**, **`208d75b`**, **`5c7c4f7`**, **`93d89e5`** — each path's last commit as of 2026-10-04 (`git log -1`) |

---

## 1. What F7 is, and why it is more than "one adapter"

Scansia Manager (FastAPI on Render, `https://scansia-manager.onrender.com`) publishes returned pairs as
Shopify **outlet** products ("<title> - Outlet", variants sharing the full-price SKU — design LD21) and
keeps their **Promo**-location stock, prices and deletion. Its whole store is the Google Sheet
`Scarpe_in_Scansia` — Ale confirmed on 2026-09-30 that the running service's `GSPREAD_SHEET_ID` is that
file. Since the cutover of 2026-09-21 that sheet is frozen and the register lives in `racoon`, so the
app now reads a dead store. **F7 moves the app onto the register.**

Ale's decision Q-persistenza (2026-07-06) promised that move would be *"1 adapter, no redesign"*. The
code says that promise holds for **half** of the store:

- **It holds for the audit trail and the tokens.** `backend/persistence/ports.py` declares exactly two
  Protocols, `AuditSink` (`:33`) and `TokenService` (`:76`). The audit sink is a real port with one
  implementation (`GSheetAuditSink`, `gsheet_audit.py:78`) and fakes in every service test; tokens are
  stateless HMAC (`tokens.py:53`, secret from env) and need no storage at all.
- **It does not hold for the register.** The register tab has **no port**: services take `sheet: Any`
  built by `sheet_factory = ScansiaSheet.open` (`backend/app.py:118-119`) and call its duck-typed
  methods. And the model behind those methods does not exist in MySQL:
  1. **Reconciliation is per sheet row, positive only.** A row carries `reconciled` true/false; publish
     sums `qta` of the unreconciled rows per (SKU, size) and adds it to Promo
     (`outlet_service.py:292-311`, `:382`). The register instead records **signed** deltas per **event**
     (`OUTLET_DELTA` −1/0/+1, `OUTLET_RECONCILED_AT`; design §2.2, §3.5) — and the −1 of a full-price
     sale is exactly what `plan.md` T6 assigns to F7 (*"la vendita a prezzo pieno registra un evento −1
     che l'adapter di Scansia Manager (F7) applicherà a Promo"*). The sheet-era code has no negative
     branch at all.
  2. **The app writes register state.** "Inizializza" demotes a row by writing `online = "NO"` and
     **`Vendute il` = now** (`init_service.py:116-120`, `:555-561`) — on the sheet, "this pair is sold".
     In the register `VENDUTO` is reached only through `SP_SCANSIA_CONSUME` (X3), and Scansia Manager
     is **not an actor** in the approved state machine (design §3.2–§3.3). That write cannot be ported;
     it has to be dropped.

So F7 = **one true port swap** (audit) **+ a register adapter behind a new, declared port + the
reconciliation logic rewritten on signed events + "Inizializza" re-specified without state writes**.
Said plainly so the build is sized for what it is.

## 2. Facts this plan stands on

| # | Fact | Evidence |
|---|---|---|
| E1 | The design already allots F7 its columns: `PRICE_OUTLET`, `DISCOUNT`, `OUTLET_PRODUCT_ID` on `sc_returned_pair` ("Written by Scansia Manager (F7) only") and `OUTLET_RECONCILED_AT` on `sc_returned_pair_event` ("the only column ever updated") | design §2.1, §2.2; `schema.prisma` comment above `ReturnedPairEvent` |
| E2 | Nothing else writes those three pair columns; STOCK_Manager writes `ONLINE` only as `false` at birth; `SP_SCANSIA_CONSUME` sets `ONLINE = 0` (X3) | `git grep -w` on `origin/main` `92f6f90`: `gate/scansia/register/route.ts:53`, `returned-pairs/route.ts:78` (+ the retired import); design §3.3 X3 |
| E3 | "The baseline Scansia Manager starts from is F7's decision" | design §3.5, last sentence |
| E4 | The order path reads the register only through the SPs; the live blueprint (v265, unchanged from v264 on this point) reads 168's output only as `{{first(map(168.recordset; "remaining"))}}` and `{{length(first(map(168.recordset; "outlet_product_id")))}}` (206's filter) and `{{first(map(168.recordset; "outlet_product_id"))}}` (217), and reads **no `online` field** (`online` / `online_before` appear only in the interface metadata of 146 and 168, plus the label of 210's filter, "Se non ci sono paia online" — a name, not an expression) | `make-operator` GET of the live v265 blueprint 2026-10-03 19:31:21Z, canonical sha `12c085b3…d82efee9` (method: `IT/AUTOMATE/docs/f6-scansia-cutover-runbook.md` §0), 3 references to `168.` in all (raw: the coordinator's scratchpad, local); the same three in v264's body (`IT/AUTOMATE/blueprints/2026-09-25-incident-key126-break/patched-step4+5.body.json`, INC@619ebf2:600-602: PATCHed → v264, re-read canonically equal); `IT/AUTOMATE/docs/f6-scansia-cutover-runbook.md:77`, `:79` |
| E5 | The SPs return an `outlet_product_id` only for a product the `online_products` mirror holds: `OUTLET_PRODUCT_ID` first (Opus F12), else SKU + title `%- outlet%`; the live SPs carry SP spec §5's text. **The mirror holds no outlet** — 0 of 9 096 rows match `%- outlet%`, 0 of the 182 outlet product ids are in it (it held 99 `%- outlet%` rows on 2026-09-12; cause of the change unrecorded) — and no pair carries `OUTLET_PRODUCT_ID`, so `outlet_product_id` is NULL on every pair and **678434's 206→210 branch is dead today**. AUTOMATE fixed the branch in v265 (2026-10-03, `05f1d92`; 678434 doc @0329757:20): **T31 met 2026-10-03** — but the branch has never run on a real line; its first live run (below) is the oracle. F7's id would reach 206 only through the mirror: never while the mirror holds no outlet, at once if it ever does (a sync change, a tagged outlet) — and so would the title fallback, without F7. The branch sits on route 1 — the Scansia branch, taken for any line whose pair `SP_SCANSIA_LOOKUP` finds — filtered on `remaining` = 0 and an id (678434 doc @0329757:85-86; runbook:77): its first live run is the first scansia sale that leaves `remaining` 0 with a mirrored id, F7's or the title fallback's — a full-price sale of a key's last pair is T6's trigger, and a run that drafts nothing is still a run. It is not F7's first id write (the 678434 doc's "still dead until scansia F7 writes `OUTLET_PRODUCT_ID`", @0329757:20, is short of this); that run is AUTOMATE's oracle (§7 F7.4). | SP spec §5 (`scansia-sp-spec.md:248-262`); the counts — `shopify-operator` 2026-10-01 13:31–13:33Z, re-read on the raw file; `PROGRAM.md`@`4673b97`:482-485 (the re-check), :1073-1074 (T31); the live `SP_SCANSIA_LOOKUP` body changed 2026-10-03 (step 1, `7afe058`); its applied text keeps §5's `COALESCE` (`IT/SERVER/_deliverables/scansia-sp-lookup-2026-10-03-ddl.md:62-67`, `:79-84` at `7afe058` — SERVER's record, not a `SHOW CREATE`); CONSUME's — the SP 206 reads, as 168 — applied text carries it too (`IT/SERVER/_deliverables/scansia-sp-2026-09.md`@7afe058:157, ×3) and that file records the mirror's 99 outlet rows (:39); T31 met: `05f1d92`; 678434 doc @0329757:85-86; `IT/AUTOMATE/docs/f6-scansia-cutover-runbook.md:77` |
| E6 | `AUDIT_PRICE` is read back (price revert, first row whose `intent_id` matches); `AUDIT` is read back (`GET /audit`); `AUDIT_DELETE` is write-ahead only — the failed append **is** the abort gate before `productDelete` | `gsheet_audit.py:186-196`, `:276`; `delete_service.py:537-545` |
| E7 | The audit tabs are created lazily on the first write; Ale found none in the file; the app never ran a production mutation → **no audit history to migrate** — Ale confirmed 2026-10-05 that no tab of those four names is in the file's tab bar (D20 «Nessuna di quelle», `PROGRAM.md`@`1b07888`:1537-1538) | `gsheet_audit.py:111-122`; Sync-Scansia `TRACKER.md` Current state |
| E8 | Legacy code outside the app wiring still uses the sheet: `src/sync.py` writes `product_id` by row index (`:138`), `fix_prices.py:212` reads it, `RUN_MODE=SYNC` opens it directly (`outlet_service.py:835`); the Render **cron** is suspended | Explore sweep 2026-09-30; `deploy-inventory.md:54` |
| E9 | `DRAFT` revive adds the delta to whatever Promo stock the draft still holds (`live + delta`) | `outlet_service.py:681-711` |
| E10 | A publish is blocked when the outlet price is missing, zero or ≥ compare-at (FIX4); `PRICE_OUTLET` is NULL on every register row today. FIX4 also rejects a missing or zero compare-at, and today's compare-at is the pair's `PRICE_HIGH` (read-only for F7); B-2, decided (D20, D25), takes `PRICE` where `PRICE_HIGH` is 0.00 or NULL (§3.5, §6 B-2). 2026-09-30 23:29Z: of 251 `IN_SCANSIA` pairs `PRICE_HIGH` NULL 10, 0.00 9, > 0 232 → **19 fail FIX4 on the compare-at unless B-2's compare-at rule covers them** (§6 B-2). 2026-10-03 21:17Z: of 249 `IN_SCANSIA` pairs `PRICE_HIGH` NULL 10, 0.00 9, > 0 230; `PRICE` is set on all 9 at 0.00 (149.00–219.00) and on 7 of the 10 NULL. 2026-10-05 22:16–22:18Z: of 251 `IN_SCANSIA` pairs `PRICE_HIGH` NULL 10 (7 with `PRICE`), 0.00 **10**, all 10 with `PRICE` 149.00–219.00; `PRICE_OUTLET` set on 0 — the build re-counts | `outlet_service.py:251` `_validate_price`, `:265-272` (`compare_at_missing` / `compare_at_zero`); `gsheet/reader.py:96` ("Prezzo High (col H) -> Shopify compareAtPrice"); design §2.1, `scansia-design.md:695`; B-2 measurement; §10 §Background numbers; `shopify-operator` 2026-10-03 21:17Z (raw re-read by the coordinator); design:79 (`PRICE_HIGH` / `PRICE` = Compare_AT_Price / Price); `shopify-operator` 2026-10-05 22:16–22:18Z (raw re-read by step 14, `PROGRAM.md`@`1b07888`:161-165) |
| E11 | On 2026-09-24 `SP_SCANSIA_CONSUME` waited on a row lock until 1205 and, 678434 being `sequential`, order intake stalled 6 h 07 m. SERVER's 2026-09-30 settlement: the holder took locks and **never committed a write**; CONSUME's step 3 locks one `idx_rp_pick` record + its PRIMARY (one gap on a sold-out size); **no in-SP 1205 retry** (v264's Break handler is the retry layer). `innodb_lock_wait_timeout` bounds the waiter, not the holder. Since 2026-09-30 07:59Z an idle-lock-holder watch (`trx_watch` user + per-minute sampler) and `innodb_print_all_deadlocks` are live. **Not transient — systematic**: every `ACTOR 'SP'` CONSUME up to 2026-10-03 (events 709, 714, 716, 718) came after a 1205 on 168 — 709 held intake the 6 h 07 m above (before v264: Ale's DLQ retry); 714, 716, 718 were re-run by v264's Break ~5 min later, each holding intake ~5 min; a Break-handled failure logs `status 1` and shows in `/dlqs` and as the watchdog's `dlqCount` alert followed by `RIENTRATO`. **Holder named by SERVER 2026-10-03** (`trx_watch.log`, the two 2026-10-01 waits): 146's own connection in the same execution — the deployed `SP_SCANSIA_LOOKUP` step 1 was `SET v_pair = (SELECT …)`, a locking read under REPEATABLE-READ, holding `S,GAP` on `sc_returned_pair_event.uk_rpe_dedup_key` (the gap the new `consume:` key goes into) until the execution ended, 146's connection running with autocommit off (inferred, not measured on Make). This **overturns** the 2026-09-30 exclusion of `SP_SCANSIA_LOOKUP`. **Fixed by SERVER 2026-10-03T08:55:19Z** (`GATE:ddl`, `7afe058`: step 1 → `SELECT MAX(e.PAIR_ID) INTO`; nothing changed on Make); oracle open: the next scansia sale passes 168 on the first run (no `/dlqs` entry, MySQL's 1205 counter stays at 2). Server: `wait_timeout` 28800 s, `interactive_timeout` 28800, `innodb_lock_wait_timeout` 50, REPEATABLE-READ | INC@619ebf2:15, :20, :43-45, :210, :596-599 (the `dlqCount` alert, then `RIENTRATO`), :619-625 (the 09-30 16:15Z recurrence, event 714; pair 23 at :625), :629-640 (systematic; holder; fix); `IT/SERVER/infra-changelog.md` at `3d21853`:21-39, at `bab9d57`:17-28; Racoon-LAB `7afe058`; `PROGRAM.md`@`4673b97`:93-95 (6 h 07 m 48 s), :100-101 (07:59Z); §10 §Background numbers |
| E12 | `mysql-connector-python` verifies the racoon CA by IP (works); `rowcount` 0 is ambiguous without `CLIENT_FOUND_ROWS`; a literal `%` in a statement eats a bound parameter | `SVILUPPO/docs/shared-database.md:38-39`, `:90-93`, `:103-109` |
| E13 | **Outlet vs shelf, measured 2026-09-30 08:12–08:20Z** (`shopify-operator`, read-only; join re-checked by the coordinator on the raw CSV): 252 shelf pairs on 236 (SKU, size) keys; 182 outlet products = **118 ACTIVE / 64 DRAFT**, Σ Promo on ACTIVE **152**. Per key: **82 aligned** (N = P); **69 over-exposed — every one N = 0, P = 1 on an ACTIVE outlet**; 3 under-published (N 6, P 3); **57 keys / 64 pairs** with an outlet for the SKU but the size not sellable (25 pairs on an ACTIVE size at 0, 32 only on a DRAFT, 7 size missing) — 3 of the 7 are spelling twins (`AR-K-33PC` pairs `37L` / `42L` / `43L` vs DRAFT variants `37 L` / `42 L` / `43 L`, `c7_join_sku_size.csv:88-90`, `:411`, `:421`, `:423`); **93 keys / 99 pairs never published** (9 of them SKU not in the catalogue). 14 keys park Promo stock on DRAFT products; of them, five DRAFT outlets hold a Promo unit on a size with no shelf pair as spelled (`c7_join_sku_size.csv:421`, `:782`, `:1221`, `:1343`, `:1438`; none in D15's set, §10 R4) — **four stale units + one spelling twin**: `:421` is `AR-K-33PC` `42 L` on 15506338513228, the variant of the shelf pair spelled `42L` (§10 S7; since U3 a twin is respelled at the source (X8 + B2, §3.4), never matched by normalisation — §3.2). **Derived after D15** (D15's write set applied to this measurement; not re-measured; `PROGRAM.md`@`4673b97`:519-523): over-exposed 69 → 0; ACTIVE size at 0: 21 keys / 25 pairs → 12 / 16; only on a DRAFT: 29 / 32 → 38 / 41; size missing 7 pairs, aligned 82 keys, under-published 3 keys (N 6, P 3), never published 93 keys / 99 pairs — unchanged; products 118 ACTIVE / 64 DRAFT → 79 / 103; Σ Promo on ACTIVE not re-measured. **Since the measurement**: 3 shelf pairs sold (events 714, 716, 718), `IN_SCANSIA` 249 at 2026-10-03 08:36Z — every figure here carries its instant; the baseline preview recounts on the day (§3.4) | `measures/2026-09-30-f7/c7_join_sku_size.csv`, `c7_summary.json`; `PROGRAM.md`@`4673b97`:519-523 (derived); `PROGRAM.md`@`573b4df`:37 (249) |
| E14 | **No outlet product has been created since 2026-01-03** (167 in 2026-01, the last on 2026-01-03); `ONLINE = 1` on 100 shelf pairs; the sheet's J "Prezzo Outlet" and K "Sconto" are cell objects at `IMPORTED.PAYLOAD.raw.J.v` / `raw.K.v`: J filled on **107** `IN_SCANSIA` pairs (`PRICE_HIGH` > 0 103, NULL 4), K on 111 (4 at 0, 1 text) | `b6_outlets_created_by_month.csv`, `c7_summary.json` `b6.last_created`, `a_register_summary.json`; §10 §Background numbers |
| E15 | The Admin API version is the env var `SHOPIFY_API_VERSION` (`backend/config.py:51`), default `2025-07` (`:17`); `render.yaml` pins `2025-01` for the cron (`:13-14`); what the Render web service really uses is not readable from here — **ASSUMPTION: the default `2025-07`**. Measured: `2025-07` is now served as `2025-10`, where `collection(id:)` answers `null` for the outlet collection — **`enumerate_outlet_products` would raise "Collection not found"**; `2026-07` works. The support table (shopify.dev/docs/api/usage/versioning, read 2026-10-04) lists, in its "Accessible until" column (`:68`), `2025-10` until 2026-10-16 15:00 UTC (status "Unsupported"), `2026-01` until 2027-01-16, `2026-10` until 2027-10-16; an inaccessible version "falls forward" to the oldest accessible stable one — what an unpinned call is served as today is F7.3's to measure. After 2026-10-16 15:00 UTC an unpinned `2025-07` falls forward to the oldest accessible stable (`2026-01` by the table) — the `collection(id:)` measurement above (2026-09-30, on `2025-10`) may change; F7.3 measures. The versions after `2026-01` change F7's own calls: from `2026-04` `@idempotent` is mandatory on `inventorySetQuantities`, `inventoryAdjustQuantities`, `inventoryActivate`, `compareQuantity` / `ignoreCompareQuantity` are removed (sent at `ops.py:495-501`), `changeFromQuantity` is required (and `2026-04.md:112` includes it for `productVariantsBulkUpdate`, on its delta input `quantityAdjustments` — `[InventoryAdjustmentInput!]`, "Adjust inventory quantities with deltas" (`2026-10` `ProductVariantsBulkInput.md:97-101`; `changeFromQuantity`, `InventoryAdjustmentInput.md:31`), its `inventoryQuantities` being "Supported as input with the `productVariantsBulkCreate` mutation only" (`:55-59`) — neither of which F7 sends: it uses the call for rule 7's DENY + tracked only, §3.3 rule 2), and `inventoryActivate` no longer defaults omitted quantities to 0 (the app passes `null`s, `ops.py:406-445`); `2026-07` moves collection rules toward `Collection.sources` ("instead of only `Collection.ruleSet`"); `2026-10` removes the obsolete `ITEM_NOT_STOCKED_AT_LOCATION` error also from `InventorySetQuantitiesUserErrorCode` (`2026-10.md:44`: "The obsolete `ITEM_NOT_STOCKED_AT_LOCATION` error is removed from `InventoryAdjustQuantities`, `InventoryMoveQuantities`, and `InventorySetOnHandQuantities`, and from the `InventorySetQuantitiesUserErrorCode` enum. Remove app logic that handles this specific error.") — so at the pin `_is_not_stocked` (`ops.py:460-467`) never fires and the activate-and-retry (`:513-522`) is dead. Plan: Promo activation is an explicit step **of a write that raises a Promo level** (an add, a revive's or a CREATE's per-size write, a re-level's upward adjust) — before such a write the flow reads the variant's Promo level with `includeInactive: true` (`2026-04.md:78`; `InventoryLevel.isActive`, `objects/InventoryLevel.md:70`) and activates **only an absent or inactive level**, with explicit `available` 0 and `onHand` 0 (`inventoryActivate`, its own `RECONCILE` row, §3.3 rule 2; in a revive, only once every zeroing row has applied — `APPLIED`, `DONE` or `CARRIED`, or `SUPERSEDED` by a successor that has, §3.3); an active level is never re-activated — there the call's quantities would be an absolute, unfenced set (F7.3 measures what it does on an active level at the pin). Shopify's pages disagree on omitted quantities — `mutations/inventoryActivate.md:21-23` ("If you don't specify quantities, then `available` and `onHand` default to zero") vs release notes `2026-04.md:90` ("`inventoryActivate` no longer defaults `available` or `onHand` quantities to zero when those arguments are omitted") — labelled, moot: F7 always sends both. The retry branch is removed, not ported. A write to an inactive level is accepted — "inventory quantities can now be adjusted at any location" (changelog `removal-of-itemnotstockedatlocation-error.md:45`), "Inactive levels can be adjusted just like active ones" (changelog `add-isactive-field-to-inventorylevel.md:45`) — and reads omit such a level by default ("Existing queries continue to return only active inventory levels unless `includeInactive: true`", release notes `2026-04.md:78`): so a +Δ is sent only once its activation row is `APPLIED` (§3.3 rule 2's Groups), never onto a level that stays inactive, and a lowering never activates (an inactive level is lowered as it is; an absent one holds nothing to lower). So every F7 read of a Promo level — the planning read, rule 8's, the held-key sweep, recovery's — passes `includeInactive: true`: the app's `read_variant_inventory` passes none (`ops.py:631`, `inventoryLevels(first: $levelsFirst)`), and "Starting with version 2026-04, deactivating an inventory level no longer clears its quantities" (changelog `add-isactive-field-to-inventorylevel.md:45`) — read by default, an inactive level holding units reads absent and is never lowered. Its units count in P; a raise that activates it sends the explicit 0, an absolute set that drops them (fewer, rule 4). F7.3 still tests a write to a not-stocked level at the pin. The "outlet variants share the full-price SKU" premise holds for 2 051 of 2 215. Customisation-only variants ("Solo Personalizzazione"): 37 with a **blank SKU**, **seven with a real SKU**; 1 shelf pair has a blank SKU: matched naively it pairs with 28 variants. `c7_summary.json` `b5`, its own labels: `inventoryPolicy_active` CONTINUE 28; `tracked_active` false 6; `active_variants_promo_lt0_blank_sku` 2; `sum_promo_available_active_blank_sku` −2 | `backend/config.py:17`, `:51`; `render.yaml:2`, `:13-14`; operator measurement 2026-09-30 (version, `collection(id:)`); `c7_summary.json` `sku_sharing`, `b5`; `c7_join_sku_size.csv:428`, `:579`, `:599`, `:738`, `:758`, `:778`, `:841` (the seven, re-counted 2026-09-30), `:2271`, `:2273` (the blank-SKU pair, its 28 variants); shopify.dev/docs/api/usage/versioning; shopify.dev changelog "Making idempotency mandatory for inventory adjustments and refund mutations" (2025-12-12); shopify.dev/changelog/release-notes/2026-04.md, `2026-07.md`, `2026-10.md`; `ops.py:406-445`, `:460-467`, `:495-501`, `:513-522`; `2026-04.md:78`, `:112`; `2026-10.md:44`; changelog `removal-of-itemnotstockedatlocation-error.md:45`, `add-isactive-field-to-inventorylevel.md:45`; `2026-10` `ProductVariantsBulkInput.md:55-59`, `:97-101`, `InventoryAdjustmentInput.md:31` |
| E16 | 172 refunds since 2026-01-01; refunded lines on outlet products 2, both `CANCEL` with restock (July 2026), 0 `RETURN`, 0 `NO_RESTOCK`. 678443 (order cancelled) restocks non-outlet items only. A Shopify-side restock raises Promo with no register event | §10 §Background numbers, R15; `IT/AUTOMATE/docs/make-scenarios/678443-shopify-ordine-cancellato.md:11-14` |
| E17 | Outlet-delta rules: +1 when a pair enters `IN_SCANSIA`, −1 when it leaves, 0 for a sale on an outlet line (Shopify already moved it); `RESTORE` (X9) and `IMPORT_SYNC` (X10) re-base to target − Σ previous (target 1 for `IN_SCANSIA`, 0 otherwise); `IMPORTED` +1 into `IN_SCANSIA`, 0 otherwise — so a shelf pair's events sum to +1 (I3). Imports are `IMPORTED` and `IMPORT_SYNC`, both `ACTOR = 'IMPORT'`; no `EVENT_TYPE = 'IMPORT'` exists | `scansia-design.md:213-222` (re-read), I3 `:132-134`; `register-checks.md:533-534` |
| E18 | Sync-Scansia's README sets `PROMO_LOCATION_ID=8251572336` in its env examples — the location Shopify names "Magazzino"; Promo is `61184966721`. `config.py` checks the variable's presence only and leaves "resolves … to a location named "Promo"" to a later startup gate (M1b): F7.2 makes that gate a test case and F7.3 confirms the service's value resolves to Promo before any write | `README.md:111`, `:216`, `:277`, `:688`, `:810`, `backend/config.py:8-10` at `fdfaf2f` (re-read 2026-10-05 ~23:05Z); the two locations' names and ids — `shopify-operator` 2026-10-05 22:16–22:18Z, reported (`PROGRAM.md`@`1b07888`:166-168) |

## 3. Target design

### 3.1 Ports — declare what the services actually need

- **`RegisterPort`** (new Protocol in `ports.py`), named for the operations, not for the sheet:

  | Operation | Semantics |
  |---|---|
  | `shelf(sku=None)` | pairs `STATE = 'IN_SCANSIA'` as read-model rows (§3.2) |
  | `pending_deltas()` | events with `OUTLET_RECONCILED_AT IS NULL` — Δ 0 included, so an outlet-line sale's group runs the product-total rule (§3.3) — joined to their pair's (SKU, SIZE), **minus the held events** (`held_keys()`, which includes the events of every `RECONCILE` row not closed) |
  | `stamp(event_ids)` | `UPDATE … SET OUTLET_RECONCILED_AT = COALESCE(OUTLET_RECONCILED_AT, UTC_TIMESTAMP(3)) WHERE ID IN (…)`, batched — **by PRIMARY KEY only**: a predicate on the indexed `OUTLET_RECONCILED_AT` (`idx_rpe_outlet_reconciled`, `prisma/manual-migrations/2026-09-11-sc-returned-pair.sql:119`) can next-key-lock the range CONSUME's event inserts land in (§3.7 rule 3); EXPLAIN-tested beside CONSUME on staging. **In one DB-only transaction, opened by the lease's guarded renew (§3.3 rule 6), with the close of its `RECONCILE` row (`STATUS = DONE`)** (§3.7 rule 1) — in this order: the renew, the row's close, then the stamp, the register `UPDATE` last before `COMMIT`; with `CLIENT_FOUND_ROWS` (§3.7 rule 6) its count is rows **matched**, and `COALESCE` keeps an earlier stamp, so a count equal to the list's length — the list being the distinct union of the event ids of the rows it closes (every row of a group carries the group's ids, §3.3 rule 2) — means every id is stamped — an id already stamped counts as stamped, no re-read; a smaller count → ROLLBACK and an error surfaced to the caller, never ignored |
  | `set_outlet_price(sku, price, discount)` | one price per SKU: that SKU's `IN_SCANSIA` pair ids by a plain SELECT, then `UPDATE` by PRIMARY KEY, guarded by `STATE`, `UPDATED_AT` explicit; the publish also calls it to write the SKU's price onto its shelf pairs that lack one (§3.5) |
  | `set_outlet_product(pair_ids, product_id)` / `clear_outlet_product(product_id)` | write / null `OUTLET_PRODUCT_ID`, by PRIMARY KEY after a plain SELECT; set only for a product that is an outlet by title **and** by collection, never a resolver `review` match (`resolvers.py:154-157`); any pass that resolves the SKU to exactly one non-`review` outlet sets the id on the SKU's shelf pairs that lack it (a fresh CREATE is a `review` match until the collection re-indexes: it gets its id on a later pass); an aborted delete leaves the id cleared — absence means the SPs' title fallback (E5). Allowed since T31 was met (2026-10-03, v265); while the mirror holds no outlet the SPs return no id either way (E5) |
  | `set_online(pair_ids, value)` | guarded `STATE = 'IN_SCANSIA'` when setting 1. Set by the baseline from its result (§3.4); to 1 by the publish on the pairs whose +1 it applied, and by a re-level or a revive on the shelf pairs of each key it leaves exposed (an ACTIVE outlet, the key's real-size Promo > 0); to 0 by Part B (§3.4), deletion (§3.5) and drafting (§3.3) |
  | `baseline()` / `record_baseline(…)` | the `BASELINE` rows (§3.4): the whole-register one — explicit stamped id list, the held keys (key, reason, held event ids), rule, counts — and one per per-key re-level; no whole-register row → refuse; never read as an empty or zero cut |
  | `held_keys()` | keys held by the baseline, by rule 2's recovery (`RECONCILE` rows `HELD`) or by a `HOLD` row (§3.6: the delete's hold, "outlet deleted", §3.5, and any hold made outside a baseline or recovery), not released by a later per-key `BASELINE` row; **and the keys of every `RECONCILE` row not closed** (`OPEN` — a CONCURRENT or MISMATCH answer, a lost or timed-out response awaiting its one in-window re-send, an "orphan unknown" CREATE, a revive tail or a normalisation not complete — or `APPLIED` awaiting its stamp; every row records the keys it serves and its product id (a CREATE's once `productDuplicate` answers), a tail or normalise row included, §3.6): the **in-flight hold**, its own kind — it ends when that row closes, with no gesture and no `BASELINE` row; every row kind has a terminal path (§3.3 rule 2's "Terminal paths": its last answer, its product read gone or deleted, a person's close → `HELD`, or `FAILED` for a revive-tail or normalise row), so no in-flight hold outlives its row; the in-flight hold's event ids are excluded the same way: no pass or re-derive re-groups them under a new nonce (§3.3 rule 2); the held-key lowering counts the Δ of the key's sent −Δ rows still `OPEN` (§3.3); each with its held event ids and its kind — baseline, recovery `HELD`, `HOLD`, in flight (§3.4) |

  Every `UPDATE` goes by PRIMARY KEY after a plain, non-locking SELECT (§3.7 rule 3). Every F7 register write appends one audit row (§3.6): `PRICE`, `REGISTER` (`OUTLET_PRODUCT_ID`, `ONLINE`), or the `RECONCILE` / `BASELINE` row that carries a stamp.

  Services depend on this Protocol; `sheet_factory` becomes `register_factory`. The MySQL adapter is its
  only production implementation; an in-memory fake replaces the per-test `FakeSheet`s.
- **`AuditSink`** — unchanged contract, **plus** the three methods services already call but no
  Protocol declares (`write_init_before`, `write_event`, `read_recent`; the app's own follow-up (8)).
  The API layer stops constructing `GSheetAuditSink` directly (`api/prices.py:258,287`,
  `api/delete_outlet.py:286,329`, `api/init.py:179`) and takes the injected factory.
- **`TokenService`** — no change.

### 3.2 Mapping — sheet concept → register

| Sheet (app) | Register | Note |
|---|---|---|
| row, `qta` | one `sc_returned_pair` row = one pair | `qta` disappears; counts are `COUNT(*)` |
| `row_uuid` | `PAIR_UUID` | stable external reference (design §2.1) |
| `reconciled` (per row) | `OUTLET_RECONCILED_AT` (per **event**) | the model change of §1 |
| `sku`, `size` | `SKU`, `SIZE` (utf8mb4_bin, exact) | **exact**: a pair publishes onto a variant only when `SIZE` equals the variant title exactly — the SPs' rule (SP spec `scansia-sp-spec.md:40-42`, `:47-48`: the order sends `variant_title`, matched spelling-exact). A match found only after normalisation (case, comma / dot, inner spaces: `42L` vs `42 L`) is surfaced for respelling at the source (STOCK_Manager's page — X8 + B2, §3.4 — or the Shopify variant), never published: a `42L` pair published on `42 L` would sell unrecorded. Today's `_norm_size` (`outlet_service.py:174-180`) goes. Reverses §10 S7 (U3) |
| `product_id` (GID or handle) | `OUTLET_PRODUCT_ID` (BIGINT) | numeric id only; the GID is rebuilt in the app |
| `prezzo_high` / `prezzo_outlet` / `sconto` | `PRICE_HIGH` / `PRICE_OUTLET` / `DISCOUNT` | `PRICE_HIGH` read-only for F7; NULL ≠ 0.00 (`register-checks.md` §1) |
| `online` | `ONLINE` | informational only: **reconciliation never reads it** (§3.3) |
| `Vendute il` | — | **not written by F7, ever** (§1) |
| sentinel `_scansia_cutover` | the whole-register `BASELINE` row (§3.4) | reader refuses publish and decrement before it exists |

### 3.3 Reconciliation on signed deltas

Group `pending_deltas()` by (SKU, SIZE); Δ = the sum; the event ids travel with the group. A **held** key (§3.4) is
grouped on its events after the hold only — its held events are never read: Δ < 0 and Δ = 0 run as below; Δ > 0 stays
pending and surfaced, **never added**, until the key's hold ends (below: the in-flight hold with its row's close, the
N-0 release; any other hold at a per-key re-level).
A held key is never raised but **is lowered**: every pass (publish, decrement, baseline, re-level) and every recovery
reads the live real-size Promo of **every held key** — except a key held only by an open revive-tail or normalise row,
which moves no quantity (lowered under such a row, a Shopify-side restock ahead of its same-day `RESTORE`, §8, would
leave P = N − 1 once the `RESTORE` lands; S13 still alarms the key above N, §5) — per held SKU, the resolver's
`products` query (no quantities,
`resolvers.py:49-66`) and one `read_variant_inventory` per outlet product (`ops.py:618-640`; with `includeInactive: true`,
E15), which B-3's bound counts
(§6); on a re-take only when another holder held the lease in between (rule 6) — and lowers that Promo, relatively
(`changeFromQuantity`), by clamp(P) + Σ pending (post-hold events, net of the shares their `CARRIED` rows already moved
— the remainder, rule 2's Groups) + Σ Δ of the key's sent −Δ rows still `OPEN` − N when positive — a −Δ in flight may
not have landed, so its unit is counted as leaving and never taken twice (a not-closed +Δ row is not counted: had it
landed, counting it would lower a published pair's unit; an `APPLIED` row landed, P shows it); the lowering runs after
the run's recovery (rule 6: recovery first), so it reads only the rows recovery could not close; the confirm does the
same for a key it holds with live P > N (lowering errs to fewer). **The N-0 release**: a key with N 0 (no shelf pair
left — its last pair sold at full price, or cancelled by X8, e.g. a D6 phantom), held or not, whose real-size Promo is
at most 0 (clamp(P) = 0: a negative `available` is never raised, rule 4) on every outlet that has its size after the
lowering above (size present or absent), with no `RECONCILE` row of the key not closed and none closed `HELD` whose
first send (`SENT_AT`) is inside the 24 h retention (rule 2: a stalled +Δ could still land) — its pending and held
events are stamped and, if held, the hold is released by a per-key `BASELINE` row (reason "N 0"); nothing is written
to Shopify. It runs **before grouping**: a Δ > 0 group of a key with N 0 is never added (if the release's other
conditions fail, it stays pending, surfaced). It reads the key's events **first** and N after (or both in one
consistent read): a pair arriving between the two reads is not among the events read, and N then counts it — only the
events read are stamped (rule 5). Two holds end without a human gesture: the in-flight hold, when its row closes
(§3.1), and the N-0 release; every other only by a per-key re-level (§3.4).

| Δ | Outlet state for the SKU | Action | Who triggers |
|---|---|---|---|
| < 0 | ACTIVE **or DRAFT** | live `available` > 0 → lower the matched real-size variant's Promo `available` by `min(|Δ|, available)` — a **relative** adjustment, never an absolute set; `available` ≤ 0 → stamp only (rule 4) — with Promo `committed` > 0 on that level the group stays pending instead (rule 4); if the product total becomes 0 and it is ACTIVE → `DRAFT` — **F7's own drafting rule**, not 678434's: v265 removed 208/209, and its aggregator 220 counts a variant sellable on a SKU and `inventoryQuantity` > 0 across all locations, or untracked (678434 doc @0329757:30-34), so the two drafters can disagree on a product (named here, not reconciled). "Product total" = Σ `available` of its real-size Promo variants, each clamped at 0; customisation-only variants never count (rule 7). No real-size variant titled exactly `SIZE` (§3.2) on an outlet that exists → the group stays pending and surfaced, **never stamped** — unless the key meets the N-0 release (above the table: N 0, live P ≤ 0, no row open), which stamps it, held or not, size present or absent | **decision B-3** — decided (D23): 678434's trigger after a scansia sale + the working-window net (§6 B-3) |
| < 0 | none = a clean resolution with 0 matches | nothing on Shopify; stamp. A truncated or failed resolution is **not** none: the group stays pending and surfaced (`resolvers.py:187-189`, `:205-209`). **A clean multi-outlet resolution** (`outlet_service.py:393-403`) applies −Δ as a lowering only: on the outlet product(s) holding `available` > 0 for the exact size (the group's planning read, rule 2's Groups), `min(|Δ|, Σ available)`, ACTIVE first, each relative with `changeFromQuantity`, then the product-total rule on each; a group split over several rows is stamped only when every row of the group — a `SUPERSEDED` one aside — is `APPLIED` (rule 2's Groups: a `FAILED` row → no stamp, its siblings `CARRIED`, only the remainder re-applied; a `HELD` row → its siblings `CARRIED`, the events held with its key); a multi-outlet key is never added to (held, §3.4) | same |
| = 0 | any | no stock written; if an outlet exists the product-total rule runs (an outlet-line sale — `OUTLET_DELTA` 0, e.g. event 718 — moved Shopify itself); stamp | same |
| > 0 | ACTIVE / DRAFT / none | the add / revive / create flows with FIX4 validation of the outlet price **and** the compare-at (E10, §3.5); each +Δ written relative with compare-and-set (rule 9), no level precondition — today's `frozen_pre` branches (`outlet_service.py:640-650`: exactly-once decided by comparing live with a frozen level, then an absolute set) are retired, not ported (§4 step 2); every variant the flow exposes is DENY + tracked first (rule 7); a revive is **deferred** — not started, its events pending — while any `RECONCILE` row not closed records the product's id or one of the product's keys (§3.6) (an in-flight hold ends with its row, and a variant zeroed under it would stay at 0); otherwise it first lowers `available` to 0, relatively, on every real-size Promo variant with no shelf pair, on every held key's real-size variant (its units return only through the re-level) and on every customisation-only variant with Promo > 0 (rule 7) (CREATE's step 3 zeroes every Promo variant, `outlet_service.py:602-604`; a revive only those) — never `on_hand` := 0, which under a committed unit leaves `available` −1 — and **every later step — activation, the per-size adds, the tail — runs only once every zeroing row has applied** (`APPLIED`, `DONE` or `CARRIED`, or `SUPERSEDED` by a successor that has): a zeroing row `FAILED` or still `OPEN` (a timed-out send) ends the revive there — nothing activated, added or exposed, its events pending, its tail row closed `FAILED` ("never run"), surfaced; once they have, the revive activates (only an absent or inactive level, explicit 0 / 0, E15), each per-size add sent only once its activation row is `APPLIED` (rule 2's Groups); the revive's tail (DENY + tracked bulk update, ACTIVE, publish, `outlet_service.py:704-711`) gets its own `RECONCILE` row (`ACTION` "revive-tail", §3.6 — the product id and the revive's keys on it), written ahead with the revive's rows, before the revive's first call (rule 2's Groups; it carries no event ids), closed when the tail completes, or `FAILED` on a refusal (rule 2's terminal paths) or when the revive stops before it ("never run", rule 2: nothing exposed, surfaced — so it defers no later revive of the product, the early end's retry included); recovery re-runs an open tail **only under a publish-class holder** (rule 2) and only once every zeroing row of that revive has applied — status and publish are idempotent by state; a size the outlet lacks (no variant titled exactly `SIZE`; a normalised-only match is surfaced for respelling, §3.2) or a SKU not in the catalogue stays pending and surfaced: no flow adds a variant (`outlet_service.py:206-215`, `:612-614`, `:673-676`, `:696-699`), a person does; a held key's +Δ is never added (§3.4), nor a +Δ of a key with N 0 (the N-0 release runs before grouping, above the table); publish writes `RECONCILE` rows (rule 2) and runs under the pass lease (rule 6) | the existing preview → confirm gesture |

**The requirement** (the mechanism is the lane's, proven in tests): **a +Δ reaches Shopify at most
once, a −Δ at least once, and every outcome the app cannot know resolves toward FEWER outlet units and
is surfaced — never more.** Rules that follow from it:

1. **Decrement drafts too** — because of E9: a draft left holding stale Promo stock would carry it into
   the next revive.
2. **Write-ahead, then Shopify, then the outcome, then stamp — and recovery never reads success off a level.** Before
   the Shopify call the app appends an audit row `RECONCILE` {event ids, variant, Δ, the idempotency key} with its
   lease generation (rule 6), `STATUS = OPEN` — for an event group, every row of the group at once, as its planning
   read composed it, in the renew transaction before the group's first call (Groups, below); the `available` the live
   read returns (rule 8 — it follows the write-ahead, rule 6), which is the write's `changeFromQuantity`, is written
   with `SENT_AT`
   (`SENT_AVAILABLE`); on an inventory row `SENT_AT` is the **first** send's instant, set once, fenced, right before
   that send, with `SENT_AVAILABLE` and `REQUEST` — a re-send rewrites none of the three (they describe the stored
   request, and the re-send window counts from `SENT_AT`, below); a CREATE, revive-tail or normalise row sets `SENT_AT`,
   fenced, right before **every** call it makes (`productDuplicate`, each later step, each tail or normalisation call —
   with `STEP`); so
   `SENT_AT` NULL always means never sent; for a CREATE the row is written **before `productDuplicate`** and
   records the source product, the title and Δ per size; **the new product id is written on the row as soon as `productDuplicate`
   answers, then the last step reached after each later step**. **Right after the call the outcome is written on the
   row** — one fenced transaction (rule 6), guarded `STATUS = 'OPEN'`; a lost lease leaves it to the taker's recovery:
   `APPLIED` (with the response's quantities) or, on `userErrors` to an inventory adjust / set (the map below) or to
   a CREATE's step 1 (`_check_user_errors`, `ops.py:61-70`), `FAILED` and closed — nothing was applied by that row (a
   re-apply's `OUTCOME` records "possibly applied", below, as does the `OUTCOME` of a sent −Δ row closed by terminal
   path (ii) or (iii)), its events stay pending for the next pass.
   `inventoryAdjustQuantities` and `inventorySetQuantities` select `userErrors { field message code }`;
   `inventoryActivate` and `inventoryDeactivate` return plain `UserError` — `field`, `message`, no `code` (`2026-10`
   `mutations/inventoryActivate.md:115-117`, `objects/UserError.md:22-36`; today `inventoryActivate` selects no code,
   `ops.py:415`) — so an activation's idempotency answers cannot be told apart, and a deactivation has none:
   `inventoryDeactivate` is not `@idempotent` (absent from the changelog's list,
   `adding-idempotency-for-inventory-adjustments-and-refund-mutations.md:49-67`) and carries no key. Any `userErrors`
   or unknown answer on either is an unknown outcome — the row stays `OPEN` and the level is re-read
   (`includeInactive: true`, `isActive` — a state no sale moves, not a quantity): an activation active → `APPLIED`,
   inactive or absent → `FAILED`; a deactivation inactive or absent → `APPLIED` (`inventoryDeactivate` "Removes an
   inventory item's quantities from a location, and turns off inventory at the location", its example "Deletes an
   inventory level from a location": `2026-10` `mutations/inventoryDeactivate.md:23`, `:51`), active → `FAILED`. Neither
   is re-sent blind: a deactivation is never re-sent (no key; recovery reads it, below), and an activation's explicit
   quantities would be an absolute set on a level since made active (E15: F7.3 measures it at the pin), so recovery
   re-reads `isActive` before any re-send of it —
   active → `APPLIED`, nothing sent. The outcome map of the two adjust / set mutations, on a
   row's first send: `IDEMPOTENCY_CONCURRENT_REQUEST` → not an outcome: the row stays `OPEN`, re-sent later
   with the same key after a back-off (shopify.dev/docs/apps/build/apis/graphql-admin/implementing-idempotency `:307`);
   `IDEMPOTENCY_KEY_PARAMETER_MISMATCH` → outcome unknown, the row stays `OPEN`, surfaced (with the per-row keys below it
   means a client defect); any other `userErrors` → `FAILED`, as above. A recovery re-send's answer is read by the
   re-send window below — there an error may answer an earlier send. A CREATE's `userErrors` at steps 2–6 is not
   `FAILED`: the duplicate exists — the row takes the CREATE path below. After `APPLIED`, the stamp and the row's close
   in one transaction (§3.1 `stamp`), `DONE`. **Groups**: every row written for one event group — a multi-outlet −Δ
   split over products, a +Δ with its activation row — carries the group's id (`GROUP_ID`, §3.6; a CREATE's rows
   follow the CREATE path below). The group's composition — over which products a −Δ splits (those holding
   `available` > 0 for the size), whether a +Δ needs an activation row (its level absent or inactive, E15), which
   variants a revive zeroes — comes from a **planning read** made before the write-ahead; rule 8's read, after it,
   gives each row's `changeFromQuantity`. A row whose premise that read contradicts (a split share above live
   `available`, a level whose activity changed) is not sent: it closes `FAILED` ("never sent", below) and the next pass
   plans the remainder — except an activation whose level already reads active, which closes `APPLIED`, nothing sent.
   Each row is written ahead with all its siblings, in one renew transaction before the group's first call (rule 6), so
   the group's row set is complete before anything is sent: a crash between two sends leaves the unsent sibling `OPEN`
   with `SENT_AT` NULL, which closes `FAILED` ("never sent", below). An activation row carries its group's event ids
   (share 0) and stamps nothing on its own; its +Δ is sent only once it is `APPLIED` — an activation `FAILED` closes
   its +Δ `FAILED`, never sent (a write to an inactive level is accepted, E15). A row closed `SUPERSEDED` leaves the
   group; its successor (a re-apply, below) inherits its `GROUP_ID` and stands in for it. The group's events are
   stamped, and its rows closed `DONE`, only when every row of the group not closed `SUPERSEDED` is `APPLIED`. If any of
   them ends `FAILED` or `HELD`, nothing of it is stamped: each `APPLIED` sibling closes `CARRIED` — applied, unstamped,
   the quantity it moved (from its answer) recorded in `OUTCOME`. Beside a `HELD` sibling (a +Δ `HELD` after its
   activation or its revive's zeroing rows applied, a split member closed by a person — (iv), below) the group's events
   stay held by that row, its key held (§3.4), until the key's per-key re-level — which levels to N from live P, what
   the `CARRIED` rows moved included, and stamps them — or the N-0 release: no remainder is written. Beside `FAILED`
   siblings only, the group's events stay pending; the next pass writes only the remainder, as a new group of that pass
   (its own `GROUP_ID`): the group's Δ minus the shares of the
   `CARRIED` rows over those events (matched on event ids; an activation's share is 0, and so is a revive's zeroing
   row's: it lowers no key the group adds to — held, N 0 or customisation-only), re-read live and, for a −Δ,
   bounded by rule 4 (surfaced). While a sibling is not closed, the group waits for it (§3.1 `held_keys()`).
   **Recovery runs before every
   pass and after any stamp failure**, not only at start-up (a restart loses in-flight jobs, `app.py:25`; `mysqld` has been
   OOM-killed, INC@619ebf2:198). It never infers from live stock whether its own write landed: a sale between the crash
   and the recovery moves the level too (an outlet sale moves Shopify at once and records Δ 0, SP spec
   `scansia-sp-spec.md:206-208`). Per row not closed — the recovery classes (below) bound which bullets a holder runs,
   and terminal paths (ii) and (iii) (below) bind every row kind and are read first, before the row's own bullet:
   - **`OPEN`, `SENT_AT` NULL** (never sent — e.g. a dead generation's row; `SENT_AT` precedes every call, above): an
     inventory or CREATE row closes `FAILED` ("never sent") under any holder, the decrement included, nothing held,
     its events pending (an unsent sibling of a group: Groups, above); a revive-tail or normalise row stays open for a
     publish-class holder (owed, below: re-run, idempotent by state — a tail whose revive stopped before it closes
     `FAILED`, "never run"). A CREATE, revive-tail or normalise row with `SENT_AT` set is read by its own bullets below
     (after (ii) and (iii)), never by the inventory ones.
   - **`APPLIED`**: every row of its group — a `SUPERSEDED` one aside — `APPLIED` (a stamp failure) → retry the stamp;
     nothing on Shopify. A sibling `FAILED` or `HELD` → the row closes `CARRIED`, no stamp (Groups, above). A sibling
     not closed → it waits for that sibling's
     recovery.
   - **`OPEN`, sent, −Δ** (no recorded outcome — a crash or a lost response): inside the re-send window, re-sent once,
     identical (below); past it, or when that one re-send's answer is neither a success nor a `FAILED` class
     (CONCURRENT included), re-apply at once, relative to live `available`, bounded by rule 4, as a new row with its
     own key (the old row closes `SUPERSEDED`, below) — never left waiting for the window's end, which would keep a
     sold unit on sale for up to ~23 h, past B-3's bound; if the first write had landed, the double −1 errs to fewer.
     Then stamp + close, run the product-total rule, and surface it (a possible extra −1, rule 8). A re-apply that
     closes `FAILED` on its own first send (a STALE `changeFromQuantity` included: a sale moved the level) records
     "possibly applied" on its `OUTCOME` — its superseded original may have landed — as a STALE re-send does (below).
   - **`OPEN`, sent, +Δ** (add, revive): **never re-add, never stamp**: the row closes `HELD` and its key is held (§3.4) —
     its events excluded from every pass until a per-key re-level levels the key to N and releases it, or the N-0
     release does (N 0, the row's first send past the 24 h retention, above); surfaced (unless inside the re-send
     window: below).
   - **An open revive tail** (its own row, the table's Δ > 0 row) **or normalisation** (one row per product, `ACTION`
     "normalise", below), sent or not: re-run **only under a publish-class holder**, idempotent by state — a tail only
     once every zeroing row of that revive has applied (the table's gate: `APPLIED`, `DONE` or `CARRIED`, or
     `SUPERSEDED` by a successor that has); a tail whose revive stopped before it — a zeroing row closed `FAILED`, or
     still `OPEN` when the revive ended or when recovery finds the tail never sent — closes `FAILED` ("never run":
     nothing was exposed), surfaced, and defers no revive of its product (the retry is a new revive, with its own tail);
     a re-run answered `userErrors` closes the tail or normalise row `FAILED` (terminal paths, below);
     a decrement-class holder's recovery leaves such a row open and surfaced (Recovery classes, below).
   - **CREATE, failed after step 1 or `OPEN`** — six steps (`outlet_service.py:585-628`: step 1 `productDuplicate` `:586`,
     then 2–6, each raising on `userErrors` through `_check_user_errors`, `ops.py:61-70`). **Product id recorded** (any
     failure after step 1, `userErrors` or unknown): the orphan, by that id, is drafted, every Promo variant lowered to 0
     relatively, its non-Promo stock zeroed / disconnected (CREATE's step 2 rule, `:593-600`) — after steps 1–2 it may carry
     the source's stock (`ops.py:576`; 10 non-outlet variants hold Promo > 0, `c7_summary.json`
     `b5_promo_location_crosscheck.non_outlet_levels_available_gt0`), after step 5 (`:619-624`) and step 6 (`:626-628`) it is
     ACTIVE and published; the row closes `HELD` and **every key of its Δ-per-size list** is held; surfaced. **No id
     recorded** (a crash or a lost answer at step 1): every key of its list is held and the row stays `OPEN` ("orphan
     unknown"); each recovery searches every `sku:` outlet match whose title equals the recorded title, collection
     membership regardless (the resolver searches `sku:`, `resolvers.py:108`; a fresh duplicate is a title-only `review`
     match, `:149-157`, `:221-238`; a re-indexed duplicate is no longer `review`, `resolvers.py:157`) and, once found,
     treats it as above; otherwise the row closes only by a person (an audit-recorded close, `HELD`: terminal path
     (iv), below). A later re-level treats the orphan as the key's DRAFT outlet and levels it to N (§3.4).
   - **Event-less rows** (a revive / orphan / held-key / customisation-only lowering) and **activation rows** (their
     group's event ids, share 0 — Groups): nothing to stamp on their own.
     Inside the re-send window → re-sent identical (below) — an activation only once its level's `isActive` still
     reads inactive or absent (active → `APPLIED`, nothing sent: its explicit quantities would be an absolute set,
     E15). Past the window, or on an unknown answer: a lowering is re-computed
     from live as a new row (the old closes `SUPERSEDED`) — its target is 0 or the held key's excess, a level re-read,
     not a delta, so a landed first write leaves nothing more to lower; an activation's outcome is read off the level's
     `isActive` (active → `APPLIED`, closed with its group; inactive or absent → `FAILED`) — under every holder, the
     decrement included (a read, no write; it never re-sends one, Recovery classes, below).
   - **Deactivation rows** (`inventoryDeactivate`, no key): never re-sent; under every holder — the decrement
     included: a read, no write — the outcome is read off the level's `isActive`: inactive or absent → `APPLIED`
     (above), active → `FAILED` (a CREATE's orphan path disconnects the level again under a publish-class holder,
     above).
   - **Terminal paths** — no row stays open for ever: (i) a revive-tail or normalise call answered `userErrors`, first
     send or re-run, closes its row `FAILED` (a state write refused is an answer, not an unknown), surfaced — the
     product stays as that call left it: a DRAFT or unpublished outlet holding Promo > 0 is S13's alarm (§5) — an ACTIVE
     one left unpublished (a tail refused at its publish step, `SS` `outlet_service.py:709-710`) is healed by a per-key
     re-level, which re-runs the publish (§3.4); a refused tail exposes no variant it has not made DENY + tracked (its
     bulk update precedes ACTIVE, `outlet_service.py:704-709`), but a refused normalisation leaves its targets —
     CONTINUE or untracked variants, on outlets F7 keeps ACTIVE too (`measures/2026-09-30-f7/c7_summary.json` `b5`) — on
     sale, rule 7's forbidden case: a normalise row closed `FAILED`, here or by a person's close ((iv)), on an ACTIVE
     product still carrying a variant not DENY + tracked (re-read) drafts that product (F7's drafting, the table's < 0
     row), surfaced, and S13 alarms on any such variant on an ACTIVE outlet (§5); (ii) a row whose product, variant,
     inventory item or location reads gone (a definitive null, not a failed read — definitive only as F7.3 tests it at
     the pin: E15 records a `null` for an existing collection on `2025-10`) closes, surfaced, under whichever holder
     reads it: an `APPLIED` row `CARRIED` (its moved quantity netted, Groups); a sent −Δ row `FAILED`, "possibly
     applied" on its `OUTCOME`; a sent +Δ, activation or CREATE row `HELD`, its keys held (§3.4) — an activation whose
     level can no longer be read for that reason included — since a null that is no deletion would otherwise re-add a
     landed +Δ; any other row `FAILED` — nothing can be added to, or taken twice from, what no longer exists; (iii)
     every delete — single, bulk cleanup, multi-outlet exit — closes its product's rows not closed ("product deleted",
     the `DELETE` row's id in `OUTCOME`) once `productDelete` has answered success or the product reads gone (§3.5): an
     `APPLIED` row `CARRIED` (netted, Groups), a sent −Δ row `FAILED` with "possibly applied" on its `OUTCOME`, every
     other row `FAILED` (a +Δ's, an activation's or a CREATE's units left with the product); (iv) any row not closed may
     be closed by a person: an audit-recorded close (who, when, why) under the pass lease (rule 6 — a decrement-class
     holder), guarded on the row's `STATUS` still the one read (0 rows → nothing closed, re-read), that sets `HELD` —
     never `FAILED` or `APPLIED` — so its keys stay held (§3.4) until a per-key re-level or the N-0 release: a person's
     close never re-adds and never stamps (the re-level that releases the key stamps the row's events, §3.4); a
     revive-tail or normalise row (no event ids: nothing to re-add or stamp) closes `FAILED` instead, surfaced, with
     (i)'s consequences.
   - **Recovery classes.** A publish-class holder — publish, baseline, re-level, the normalisation at a re-level's
     confirm — runs every bullet above except (iv), a person's close. A decrement-class holder — the decrement job
     (B-3: it never adds, activates or creates), pricing, delete, the bulk cleanup, the multi-outlet exit,
     deny-normalize (§3.5), a person's close ((iv)) — in its recovery re-sends and closes only −Δ rows, lowerings and
     stamp retries, reads deactivation and activation rows off `isActive` (a read, no write; it never re-sends an
     activation), closes never-sent inventory and CREATE rows `FAILED` (above) and any row by terminal path (ii) or
     (iii); every other sent +Δ and CREATE row, and every revive-tail and normalise row, sent or not, stays as it is,
     surfaced, its keys held (§3.1), for the next publish-class holder, whose recovery runs first (rule 6) — so no
     decrement-class holder runs, in recovery, a CREATE's orphan search, a tail or a normalisation.

   **Idempotency key.** Every inventory mutation (`inventoryAdjustQuantities`, `inventorySetQuantities`,
   `inventoryActivate`) carries `@idempotent` (mandatory at the pin, E15); the key is **generated once per write
   intent** — a random nonce written on that write's `RECONCILE` row at the write-ahead, before any send — one row per
   Shopify inventory write (event-group writes, re-levels, CREATE steps 2–4 per variant and location, revive / orphan
   / held-key / customisation-only lowerings, every `inventoryActivate`). `inventoryDeactivate` (CREATE step 2's
   disconnect of a non-Promo level, `outlet_service.py:599-600`) gets its own row with no key (above), sent only after
   that level's set-0 row is `APPLIED`. `productVariantsBulkUpdate` (CREATE step 5, the revive tail, rule 7's
   normalisation — DENY + tracked; F7 sends no inventory quantity through it) is a state write, like status and
   publish: recorded on its CREATE or tail row (`STEP`) or, for the normalisation, on one `RECONCILE` row per product
   (`ACTION` "normalise" — the product id and the keys of its real-size variants on it, §3.6) that recovery re-runs
   under a publish-class holder, idempotent by state, `FAILED` on a refusal (terminal paths, above). Every send of a
   keyed row, first or recovery, uses
   the row's key and **the row's stored request** — the exact body of its first send, kept as text (`REQUEST`, §3.6),
   never rebuilt: rebuilt from the `PAYLOAD` JSON its keys come back re-ordered (MySQL sorts the keys of a JSON object,
   dev.mysql.com `json.html`), and "parameter order can affect fingerprinting" (implementing-idempotency `:285`) — a
   MISMATCH on every re-send. A recovery that must re-apply with new parameters (a −Δ past the re-send window, or
   after its one in-window re-send answered unknown or CONCURRENT, above) writes a new row (new key, the old row's
   `GROUP_ID`) and closes the old one `SUPERSEDED` (outcome: the successor row id). A stalled holder's rows were
   written before the take-over (every write-ahead is fenced, rule 6),
   so the taker's recovery sees them and re-sends their own keys. A repeated key returns "the result of the original
   operation instead of creating a new one" (shopify.dev changelog, 2025-12-12). The retention is stated: "24 hours from
   the original request" (implementing-idempotency `:56`). **The re-send window** is the first send's `SENT_AT` + 24 h
   − a margin (proposed 1 h, the lane's to set: clock skew and the re-send's own flight — a re-send landing after
   expiry is a separate operation, `:84`); a re-send never moves it — it rewrites neither `SENT_AT` nor
   `SENT_AVAILABLE` — and its local deadline counts from its own renew (rule 6). Recovery of an `OPEN` inventory row
   inside the window re-sends the identical request (an activation only while its level reads inactive or absent, a
   deactivation never: the row bullets above) —
   same key, the stored body (`changeFromQuantity` included) — and reads its answer so: a success → `APPLIED`, its
   quantities recorded as the answer's — a cached response "is constructed from database records, which may have changed
   since the original successful response" (`:66`), so never read as the level this write left;
   `IDEMPOTENCY_PREVIOUS_ATTEMPT_FAILED` ("A previous request with this idempotency key failed. Retry with a new
   idempotency key.", `InventorySetQuantitiesUserErrorCode.md:45-47`; also
   `InventoryAdjustQuantitiesUserErrorCode.md:41`) or an input-validation error (`INVALID_…`) → `FAILED`, events
   pending — nothing applied by that row (a re-apply's `OUTCOME` records "possibly applied", above) — except
   `INVALID_INVENTORY_ITEM` / `INVALID_LOCATION`, "could not be found", which on a duplicate may also mean a
   completed original whose item or location was deleted since (`:309-331`): such a row closes as terminal path (ii)
   closes a row that reads gone, per kind (a −Δ `FAILED` "possibly applied", a +Δ or activation `HELD`, its key
   held) — "could not be found" is definitive only as F7.3 tests it at the pin, like a null read;
   `CHANGE_FROM_QUANTITY_STALE` → on a −Δ row `FAILED` (had the original landed, the re-derived −Δ errs to fewer — so
   the row's `OUTCOME` records "possibly applied", and the next row that lowers any of its events records it too and
   is surfaced as a possible extra −1, rule 8), on a +Δ row `HELD`, its key held — a landed original's duplicate may
   answer STALE rather than its cached result
   (thread 28236 #5, Shopify staff 2026-01-13: duplicates "will fail") — until F7.3 shows on the store that it answers
   with the cached result, after which STALE on a +Δ re-send reads `FAILED`; `IDEMPOTENCY_CONCURRENT_REQUEST` → on a
   +Δ row still `OPEN`, re-sent after a back-off until the window's end; **any other answer is unknown** — a duplicate
   of a failed original may answer "an error with a suitable message" (`:77-80`), one of a completed original a
   `NOT_FOUND` once its data changed (`:309-331`), a timeout or MISMATCH says nothing — so a +Δ row closes `HELD`, its
   key held, and a −Δ row, on this one re-send's unknown or CONCURRENT answer, is re-applied at once, bounded, as a
   new row (the old `SUPERSEDED`; the `OPEN`, sent, −Δ bullet above). Past
   the window, and for every non-inventory mutation (`productDuplicate`, `productVariantsBulkUpdate`, status,
   publish): the bullets above (−Δ
   re-applied bounded, as a new row; +Δ / CREATE held). The guarantee was rolled out per
   store in January 2026 (community.shopify.dev thread 28236, Shopify staff 2026-01-12): F7.3 tests on the store itself
   that a repeated key applies once, and also records the answer shape of a duplicate (cached result vs an error, the
   thread's staff note says duplicates "fail") and of CONCURRENT — the shape that settles how a +Δ re-send's STALE is
   read (above). Rule 9's `changeFromQuantity` stays: it fences a write
   computed from a stale read.
3. **Relative adjustments for the new branch, and a latent defect to fix in the old ones.** The leaf
   op `inventory_set_quantities` sets **`on_hand`** absolutely (`ops.py:470-500`) while the positive
   branches compute the target from **`available`** (`outlet_service.py:643` ACTIVE, `:701` DRAFT
revive; CREATE at `:604`/`:615` sets on a fresh duplicate, where nothing is committed yet). With a
   unit `committed` to an unfulfilled outlet order, `on_hand ≠ available` and that write loses the
   committed unit(s) — its docstring's "robust if committed>0" does not hold for a target derived from
   `available`. No harm done yet (no production mutation has ever run), but F7 fixes it: **relative
   adjustments only**, or a compare-and-set that refuses when the quantity moved — never a blind absolute
   set (today's leaf op passes `ignoreCompareQuantity: True`, `ops.py:495-501` — an argument the pinned version no longer
   has, E15).
4. **Under-count, never over-count.** A negative group whose variant already shows `available` ≤ 0 is
   stamped without touching Shopify (an outlet sale or a hand edit got there first) — unless that level's Promo
   `committed` > 0: a unit only held, by a draft order, a transfer or a shipment, whose holds now sit in `committed`
   (changelog `draft-order-and-transfer-shipment-inventory-is-moving-from-reserved-to-committed.md:35`, August 5,
   2026; `:43`, `:49`), reads 0 too and returns to sale when the hold ends; that group stays pending (S11's age
   alarm fires on it, its reason listed by the pass) and is applied once the hold is released, or stamped then if
   `available` stays ≤ 0 (the unit shipped); in a −Δ group a negative
   `available` is never raised (a −Δ only lowers).
5. **Only the events read are stamped.** Events that land during a run stay pending for the next one;
   the stamp is `WHERE ID IN (…)`, never a range or a date — the baseline too stamps an explicit id list (§3.4).
6. **One pass at a time, across processes — a fenced lease.** `JobStore` is one slot per process (`api/jobs.py:7-10`,
   `app.py:143`); a second process holds a second slot (a deploy overlap — ASSUMPTION, not read from Render). The
   database holds a **lease** for the whole pass, not per group — every holder but the baseline (publish, re-level,
   pricing, delete and the bulk cleanup, the multi-outlet exit, deny-normalize — §3.5 — and the normalisation at a
   re-level's confirm, rule 7, plus a person's close of one row, rule 2's terminal path (iv); each holder's recovery
   class: rule 2's Recovery classes) releases and re-takes it
   between SKUs (between products, for the exit, the cleanup and the normalisations), so B-3's decrement can run in
   between (a baseline does not: the reader refuses the decrement until the whole-register `BASELINE` row, written
   last, exists — §3.4): holder id, a **generation** (fencing token)
   every
   take-over increments, an expiry. **Hand-off**: a decrement that finds the lease held marks itself waiting (proposed:
   a waiter flag on the lease with its own expiry, set by one guarded statement — the mechanism the lane's); a holder
   releasing between SKUs (products) re-takes only once no decrement waits, so a waiting decrement takes the lease
   within one SKU's (product's) flow of whichever holder; past that bound plus a margin it ends its run, surfaced (S11
   ages the pending −Δ). Taken over atomically — one
   guarded statement, never check-then-insert — only once
   expired; expiry alone tells a crashed holder from a live deploy-overlap peer. After every re-take the holder
   re-derives the next SKU's groups from `pending_deltas()` as they are now (never from the plan confirmed before the
   release) and, for a re-level, re-reads that key's N, max event id (any Δ) and live `available` right before its write
   (a baseline's per-key re-read is its confirm's, §3.4) — a key that moved since the confirm is held, nothing written.
   The approved plan only bounds what may be written: a SKU absent from it gets no add (the held-key lowering, §3.3,
   runs on every pass and is exempt); a re-derived +Δ larger than the confirmed one is **deferred to the next preview**
   — not written now, no hold, its events pending; and the re-derive re-reads, per SKU, the outlet's gid and status the
   confirm approved (today's per-SKU check, `outlet_service.py:405-410`, `:786-792`) — a SKU whose outlet changed is
   deferred the same way. **Fencing**: every `RECONCILE` row carries the
   generation; **every F7 write to the database** — a `RECONCILE` row's write-ahead `INSERT`, its `SENT_AT` mark, its
   outcome, the stamp + close, a `BASELINE` row, every register write — runs in a transaction that opens with a guarded
   renew of the holder's own lease (holder and generation match, not expired; 0 rows → ROLLBACK, stop) — a guarded
   `UPDATE`, never a SELECT inside a write (§3.7 rule 2). Every F7 flow that writes the database or Shopify takes the
   pass lease — publish, decrement, baseline, re-level, pricing, delete, the bulk cleanup, the multi-outlet exit,
   deny-normalize (§3.5), the normalisation at confirm, a person's close of a `RECONCILE` row (rule 2's terminal path
   (iv)). The decrement runs on its own worker — never on
   `app.state.executor`, a single worker (`ThreadPoolExecutor(max_workers=1)`, `app.py:122-123`) where it would queue
   behind the whole publish — nor inline on the event loop. The renew and the write-ahead `INSERT`s — every row of an
   event group at once, as its planning read composed it (rule 2's Groups) — are **one** transaction; the live read
   (rule 8) follows it; `SENT_AT` and the `available` read are set (fenced) right before the first call, and every
   Shopify call is sent only before a
   **local deadline** counted from the renew opening the transaction just before it — the `SENT_AT` transaction for a
   first send, a renew alone for a re-send, which rewrites neither (rule 2) — (the lease expiry minus a margin for
   clock skew and the call's connect), else the holder renews and re-reads first; the baseline's `READ ONLY` snapshot
   (§3.4) is a read, renewed just before it, outside it (MySQL refuses an
   `UPDATE` inside a read-only transaction); every other Shopify write — `productDuplicate`, a CREATE's later steps, a
   revive tail's calls, a normalisation's calls, status, publish — follows a fenced renew that sets its row's
   `SENT_AT` (and `STEP`) the same way (rule 2), under the same local deadline; a lost lease stops the holder. The
   expiry exceeds the longest single Shopify call with its retries (the renew precedes each call), not the whole pass. On
   Shopify's side the fence is rule 9's `changeFromQuantity` — a write computed before a take-over is refused once the
   taker moved the quantity — and rule 2's idempotency key: two holders of the same write send the same key (the row's, rule 2), so an
   identical re-send returns the original's result, or CONCURRENT while it is in flight; a re-send with other parameters
   is refused (MISMATCH). Publish, decrement, the baseline and each re-level run under it and write `RECONCILE`
   rows (`api/publish.py:12`: publish has no audit sink today). Whoever takes the lease — first start or take-over —
   runs rule 2's recovery (its class's, rule 2) and then the held-key lowering (§3.3) before anything else; a holder
   re-taking it after its own release between SKUs (products) runs them again only if another holder held the lease
   in between (read off the lease — the mechanism the lane's): its own open rows wait for the next take by another
   holder or the next pass, and one confirm that re-levels a run of keys pays one sweep. Mechanism the lane's,
   proven in tests: a crashed holder taken over, a
   live one refused, a stalled holder resuming after a take-over (its transactions roll back, its Shopify writes stop
   at the next renew). After a crash, a pending −Δ waits up to the lease expiry plus the next pass (under the event option, chosen
   (D23), the next trigger, boot pass or net sweep — §6 B-3; one sweep under a Scheduler option, not taken).
7. **Customisation-only variants** are identified by option value ("Solo Personalizzazione", every spelling — three
   in `c7_join_sku_size.csv:2272-2274`), not by blank SKU (seven carry a real SKU, E15), and excluded from every match,
   count and total — **not from normalisation**. DENY + tracked is a precondition for every variant of an outlet F7
   exposes or keeps ACTIVE, customisation-only included: a customisation-only variant with a real SKU and CONTINUE on
   an ACTIVE outlet sells at any level, unrecorded — the line's title carries "Outlet" and no pair matches its size, so
   678434 runs neither route (678434 doc @0329757:85; e.g. `AR-K-GLIK` on 15506384421196,
   `measures/2026-09-30-over69/step2_decisions.json`). The baseline preview lists them and normalises them under Ale's
   confirm (customisation-only: DENY + tracked; a positive Promo lowered to 0 relatively, a negative left — DENY makes it
   unsellable); CREATE and revive set tracked (today CREATE sets DENY but never tracking, `outlet_service.py:620-623`;
   the ACTIVE add sets neither, `:664-666`). **The blank-SKU shelf pair** (SIZE «Solo Personalizzazione»,
   `c7_join_sku_size.csv:2271`; its 28 naive matches `:2273`, E15) is excluded from every pass and listed; its key held (reason "blank SKU");
   nothing resolves a SKU `''`.
8. **Every Promo write reads live `available` immediately before it** (`includeInactive: true`, E15); nothing is written from a stale
   read, and every write is relative (rule 3), so a Shopify-side restock (E16) or a hand edit is never
   overwritten — except that a recovery re-apply of a −Δ (rule 2) can take one unit off it, the side rule 2
   accepts and surfaces.
9. **No mutation re-posted blind.** Today's transport re-posts every call, mutations included, on timeout /
   `RequestException` / 5xx / 429 (`transport.py:84-89`, `:120-127`, `:140-146`): a relative +Δ whose response was
   lost would land twice (the absolute set it replaces was retry-safe). **Both**: (a) every relative Promo write
   carries `changeFromQuantity` = the value just read (rule 8; D6's procedure already uses it,
   `PROGRAM.md`@`4673b97`:126-128) — it is
   also rule 6's Shopify-side fence; (b) the transport re-sends a mutation only on an answer that says it did not run
   (a 429 / a throttled refusal — the lane names them from the pinned version's docs): after a timeout, a
   `RequestException` or a 5xx the outcome is unknown and the row stays `OPEN` for rule 2. So on a row's first send a
   `userErrors` answer — a stale `changeFromQuantity` included — answers the only send, and rule 2's `FAILED` means
   nothing was applied by that row (a re-apply's superseded original may have landed: its `OUTCOME` records
   "possibly applied", rule 2), except `IDEMPOTENCY_CONCURRENT_REQUEST` and `IDEMPOTENCY_KEY_PARAMETER_MISMATCH`,
   which never close a row `FAILED`; a recovery re-send's answer is read by rule 2's re-send window.
   `changeFromQuantity` exists from Admin API `2026-01` (shopify.dev changelog "New changeFromQuantity field to manage
   inventory", 2025-12-12, `effectiveApiVersion: 2026-01` —
   shopify.dev/changelog/compare-and-swap-for-inventory-mutations-with-change-from-quantity) and is required from
   `2026-04`; the app's default is `2025-07`
   (`backend/config.py:17`, E15): F7.2 pins one named version and F7.3 verifies it on the service (E15's breaking changes). Tested.

**Why deltas and not "set Promo to the shelf count".** A level-setting sweep looks more robust, and
it would be wrong at the worst moment: an outlet sale moves Shopify immediately, while the register
learns of it only when 678434 runs `SP_SCANSIA_CONSUME`. During an intake stall — six hours on
2026-09-24 — a level sweep would see "shelf 1, Promo 0" and **put back every unit just sold**. Deltas
move only on register events, so a stalled intake freezes them instead of reversing sales. Level-setting
is used only at the baseline and per key (a re-level, §3.4 — a held key, or a key not held that the preview lists, e.g.
a DRAFT-parked one), each under a human gesture, as relative adjustments, with
no outlet sale parked or in flight (§3.4, B-1).

### 3.4 "Inizializza" on MySQL

- **Part A — the baseline.** Which events the outlet already reflects, and whether Promo is levelled
  first, is **decision B-1** — decided 2026-10-05: level once, after D6 (D20 «Allineo dopo il controllo», `PROGRAM.md`@`1b07888`:1532-1533; §6 B-1); the baseline runs so:
  - **One snapshot**, one non-locking consistent read — one statement, or a `READ ONLY` consistent-snapshot
    transaction closed before any Shopify call (the stated exception to §3.7 rule 1): shelf count N per (SKU,
    SIZE), each key's max event id (any Δ), the pending event ids (a re-level's: every unstamped one of its key, held
    too — it stamps both, below); then live Promo per real-size variant.
  - **Per key**, it declares the events the outlet already reflects, as an explicit id list it stamps (§3.3 rule 5);
    every other event stays pending. **Tested invariant, on every key**: afterwards live Promo (real-size, exact title,
    clamped at 0) + Σ pending Δ (net of `CARRIED` shares, §3.3) = N — **or the key is held**: `BASELINE` records it
    (key, reason, its snapshot event ids); its held events are excluded from every pass (never an add; its later
    events run as §3.3 says for a held
    key); it is listed (S11, S13); only a **per-key re-level** releases it — the same preview → confirm, under this
    section's precondition and the lease, recorded by a `BASELINE` row scoped to the key; **two holds end without a
    human gesture**: the in-flight hold, when its row closes (§3.1 `held_keys()`), and **the N-0 release** (§3.3): a
    held key with N 0 and live P ≤ 0 after the lowering, no row of it open and none `HELD` inside the 24 h retention,
    size present or absent, is released by a pass (nothing to expose, nothing written to Shopify; the release is a
    per-key `BASELINE` row, reason "N 0"). **A
    re-level may also be run on a key not held** — the invariant can hold on a key
    whose pairs are unexposed: a DRAFT-parked key (N 1, its unit on a DRAFT outlet, nothing pending: 1 + 0 = 1 — nine
    such keys on 2026-09-30, `c7_join_sku_size.csv:85-86`, `:94`, `:108-109`, `:111`, `:121`, `:130`, `:132`, pre-D15)
    is never revived by a pass; its re-level levels it to N the same way (zero-then-revive). The invariant keeps
    counting DRAFT Promo: scoped to ACTIVE Promo, a DRAFT-parked key with a pending +1 would pass it and a revive would
    add onto the parked unit (E9: live + Σ pending) — two units for one pair. **A re-level's target is N
    whatever its key's pending mix**: it levels to N (zero-then-revive / create as the flow needs; on an ACTIVE outlet
    left unpublished — a tail refused at its publish step, §3.3 rule 2's terminal path (i) — it re-runs the publish,
    idempotent by state, surfaced) and its `RECONCILE` row records the shelf pair ids, N and the event ids it stamps:
    every unstamped event of its key up to its snapshot, pending and held — a `HELD` row's included, so a landed +Δ it
    releases is never re-added — as the N-0 release does. FIX4 applies to the re-level (E10, §3.5); rule 2
    governs its outcome (§3.3). A re-level whose key still
    fails the precondition leaves it held. Rule 2's recovery holds a key the same way (an open +Δ or CREATE).
  - **Per pair**: all of a pair's **import** events (`IMPORTED` + `IMPORT_SYNC`) are taken together; an `IMPORT_SYNC` re-base is never applied on
    its own (E17: a shelf pair's events sum to +1).
  - **Audit**: `INIT_BEFORE` (the before-snapshot, written before any Shopify write) and `BASELINE` (the
    stamped list, the held keys, rule, counts — written last). A crashed baseline leaves no `BASELINE`: the reader keeps
    refusing; re-run from a fresh preview (a relative adjust toward a target is idempotent on re-run). A crashed re-level leaves its key held.
  - **Precondition for the baseline and every re-level** (never the decrement job): the person confirming has the
    coordinator run a `make-operator` GET right before the preview — the app holds no Make credential
    (`backend/config.py:48-53`) — showing **no unresolved
    `/dlqs` entry** (resolved ones persist: `allDlqCount` 4 on 2026-10-03, all resolved) and no 678434 execution
    in flight — since v264 a parked line re-runs itself after ~5 min, so a lock wait shows only in `/dlqs` and as
    the watchdog's `dlqCount` alert, then `RIENTRATO` (INC@619ebf2:596-599). At confirm the app re-reads, per key,
    N and its max event id (any Δ, so an outlet sale's Δ 0 CONSUME counts) and each live `available`: a key where
    any changed, or whose upward adjust meets a `committed` > 0 on the variant, is **held**, nothing written — except
    that the confirm lowers a key it holds with live P > N, as §3.3 lowers a held key (lowering errs to fewer).
    Events after the snapshot stay pending; the residual seconds between the per-key re-read and its write are accepted, stated. The
    GET's instant and `dlqCount` are typed into the confirm and kept in the `BASELINE` row.
  - **`ONLINE`** is set from the result (§3.1): under Level, 1 on the shelf pairs of keys levelled on an ACTIVE outlet
    that has the size, else 0; under Parity, Part B's classification; afterwards a re-level and a revive set it on the
    pairs they expose (§3.1). One audit row per write (§3.6).
  - **The preview lists first**: the four stale DRAFT units and the one spelling twin (its `42 L` unit lowered with the
    four; E13 — respelled at the source: X8 cancel «Inserita per errore» + B2 re-insert on STOCK_Manager's page,
    `scansia-design.md:512-513`, `:609-613` — or the Shopify variant renamed; never matched by normalisation, §3.2);
    size-absent and not-in-catalogue pairs (§3.3); CONTINUE or untracked variants, real-size and customisation-only
    alike, normalised at confirm (DENY + tracked; customisation-only also a positive Promo lowered to 0, relatively, a
    negative left — §3.3 rule 7; `delete_service.py:70-76`, `:745-752`); multi-outlet keys (e.g.
    `GLP-W-LEOGD-33PCGD-DIR`, two outlet products: `c7_join_sku_size.csv:258` — N 0 on every size at 2026-09-30, so a
    multi-outlet hold only if D6 registers a pair of it); the DRAFT-parked keys at N (a re-level
    each, above); and every key the confirm would hold, with its reason.
- **Part B — the online flags.** Under Parity it writes as below; **under Level, Part A sets `ONLINE` and Part B only
  lists discrepancies** — no write. For `ONLINE = 1` pairs, the sheet-era classification (kept-online /
  demote:missing / demote:draft / demote:sold-out-size / review:multi-match, `init_service.py:31-57`)
  stays, **but a demotion writes only `ONLINE = 0` — it no longer drafts the product (drafting follows
  §3.3's product-total rule). It never writes `CONSUMED_AT`, never changes `STATE`.** A pair the register holds on the shelf while Shopify
  shows its outlet sold out is listed as a discrepancy for the operators — the register
  is corrected only through STOCK_Manager's page (X5 / X8), by a person.
- The reader refuses publish and decrement until the whole-register `BASELINE` row exists (the sentinel's job today); a
  missing baseline is never read as an empty cut.

### 3.5 Prices and deletion

- **Pricing** writes one price per SKU. The SKU's price is the one non-NULL `PRICE_OUTLET` among its shelf
  pairs — today publish reads the first row's (`outlet_service.py:257-258`, `:381-385`), so a new return
  (`PRICE_OUTLET` NULL) would block its whole SKU; two different values → the SKU is listed and blocked; the
  publish writes the price onto the SKU's pairs that lack it. The write:
  `PRICE_OUTLET` / `DISCOUNT` on the SKU's `IN_SCANSIA` pairs, ids by a plain SELECT, `UPDATE` by
  PRIMARY KEY (history keeps the price it was sold at), and records the priors in `PRICE` audit rows
  keyed by a **unique** `INTENT_ID` — today's "first matching row wins" becomes a unique-key lookup. The compare-at likewise: one per SKU — B-2's rule applied to the SKU's shelf pairs (today read from the first row,
  `outlet_service.py:383-385`); pairs that yield different compare-ats → the SKU is listed and blocked.
- **Deletion order**: (0) the preview lists the SKU's `IN_SCANSIA` pairs and confirm holds their keys (a `HOLD` row,
  §3.6, reason "outlet deleted"), released by a per-key re-level that recreates (§3.4) — the delete predicate reads only Shopify (Promo
  `available` 0 on every variant, Σ Promo `committed` 0, every variant DENY, no non-Promo stock:
  `delete_service.py:25-37`) — the bulk cleanup and the exit apply it; the single delete (`delete_single_apply`, for
  a creation-error product) applies none, by design (`delete_service.py:17-20`, `:719-723`) — ported as is, under
  step (0)'s hold and the lease; (1) `clear_outlet_product(id)`, by PRIMARY KEY after a plain SELECT — safe even if
  the delete then fails, because the SPs fall back to the title lookup (E5); (2) the durable `DELETE` snapshot,
  whose failure aborts (unchanged gate); (3) `productDelete` — once it answers success, or the product reads gone,
  the product's `RECONCILE` rows not closed close by §3.3 rule 2's terminal path (iii) ("product deleted": `FAILED`, an
  `APPLIED` row `CARRIED`, a sent −Δ "possibly applied");
  (4) `ONLINE = 0` on that SKU's shelf pairs.
- **The multi-outlet exit** (§8; a SKU with two or more outlet products, its keys held, §3.4): a person chooses the
  outlet to keep; on each other outlet the app lowers, relatively, every Promo variant with `available` > 0 to 0, one
  `RECONCILE` row each — every real-size variant, not only the held key's (the delete predicate is product-wide,
  `delete_service.py:25-37`), and the customisation-only ones as §3.3 rule 7 lowers them — then deletes that outlet by
  steps (1)–(3) above: step (0)'s hold is the keys' existing hold, step (4) is skipped (`ONLINE` untouched — the shelf
  pairs stay on the kept outlet); a variant the predicate still refuses (a negative, a `committed` unit) stops the exit
  before the delete, listed; each held
  key is then released by a per-key re-level on the kept outlet (§3.4). It runs under the pass lease (§3.3 rule 6).
  E.g. `GLP-W-LEOGD-33PCGD-DIR` (`c7_join_sku_size.csv:258`; the other outlet's sizes read 0 at 2026-09-30,
  `:1715-1722`) — N 0 on every size at 2026-09-30, so a multi-outlet hold only if D6 registers a pair of it; with N 0
  its keys need no exit (the N-0 release, §3.3).
- **The bulk cleanup and deny-normalize — ported, not retired.** `/outlet/cleanup/preview` + `/apply`
  (`api/delete_outlet.py:197`, `:234`) applies the deletion order above, one product at a time, and **never deletes
  an outlet whose SKU has an `IN_SCANSIA` pair**: such an outlet is listed apart, not deletable, at preview and
  again at apply (the app's predicate — a DRAFT outlet, `delete_service.py:12-16`, meeting the delete predicate above,
  `:25-37` — also matches the DRAFT outlets B-1's Level (b) zeroes for their
  revive, §6: deleting them before the first publish would turn up to 38 keys / 41 pairs, E13, from a revive into a
  re-level that recreates), so steps (0) and (4) find no shelf pair; `/outlet/deny-normalize`
  (`:342`) becomes §3.3 rule 7's normalisation (DENY + tracked; a customisation-only positive Promo lowered to 0,
  relatively), with its own preview → confirm (`CONFERMO`). Both run under the pass lease and yield to a waiting
  decrement between products (§3.3 rule 6); the read-only `/outlet/zero-stock` (`:177`) takes no lease.

### 3.6 Storage objects (SERVER, `GATE:ddl`)

- **One audit table**, proposed name `sm_outlet_audit` (own prefix: `sc_` is STOCK_Manager's), append-only:
  `ID` BIGINT AI · `KIND` (`AUDIT` · `DELETE` · `PRICE` · `REGISTER` (one row per F7 write of `OUTLET_PRODUCT_ID`
  or `ONLINE`: pair ids, old → new) · `INIT_BEFORE` · `BASELINE` (whole-register, one key's re-level, or the N-0
  release, §3.3) ·
  `RECONCILE` · `HOLD` (holds outside a baseline / recovery, §3.1 `held_keys()`), + the pass lease — holder, generation,
  expiry, waiter (§3.3 rule 6) — if the lane keeps it in this table) · `ACTOR` · `ACTION` ·
  `PLAN_HASH` · `INTENT_ID` CHAR(32) **UNIQUE NULL** · `LEASE_GEN` BIGINT NULL (`RECONCILE`) · `SENT_AT` DATETIME(3)
  NULL (an inventory row's first send, set once; a CREATE, tail or normalise row's latest call — §3.3 rule 2) ·
  `SENT_AVAILABLE`
  INT NULL (the first send's `changeFromQuantity` read) · `IDEMPOTENCY_KEY` CHAR(n) NULL · `PRODUCT_ID`
  BIGINT NULL (the product a `RECONCILE` row writes, recorded at its write-ahead — a revive tail's and a
  normalisation's included, the keys they serve in `PAYLOAD` — so the revive's deferral, `held_keys()` and a delete
  find the row; a CREATE's duplicate, written when `productDuplicate` answers) · `STEP` (last step reached) ·
  `GROUP_ID` BIGINT NULL (the event group a row serves
  — §3.3 rule 2's Groups) · `REQUEST` MEDIUMTEXT NULL (the exact request body of the first send, written with `SENT_AT`;
  every re-send sends it, §3.3 rule 2) — names proposed, the lane's to fix · `STATUS`
  (`OPEN` / `APPLIED` / `DONE` / `CARRIED` / `FAILED` / `HELD` / `SUPERSEDED` for `RECONCILE`, else NULL) · `OUTCOME`
  JSON NULL
  (the Shopify call's result) ·
  `PAYLOAD` JSON (snapshot / priors / rows / targets / result) · `CREATED_AT` DATETIME(3) UTC · `CLOSED_AT`
  DATETIME(3) NULL. Time rule as the register: UTC, explicit, no defaults (design §2). Only `SENT_AT` / `SENT_AVAILABLE`
  / `REQUEST` / `PRODUCT_ID` / `STEP` / `STATUS` / `OUTCOME` / `CLOSED_AT` of a `RECONCILE` row — and the lease's holder
  / generation / expiry / waiter, if kept here — are ever updated.
- **One DB user** (T8), proposed `scansia_mgr`, `REQUIRE SSL`, CA-pinned client (E12), grants:

  | Object | Privilege |
  |---|---|
  | `sc_returned_pair` | `SELECT`; `UPDATE (PRICE_OUTLET, DISCOUNT, OUTLET_PRODUCT_ID, ONLINE, UPDATED_AT)` |
  | `sc_returned_pair_event` | `SELECT`; `UPDATE (OUTLET_RECONCILED_AT)` |
  | `sm_outlet_audit` | `SELECT, INSERT`; `UPDATE (SENT_AT, SENT_AVAILABLE, REQUEST, PRODUCT_ID, STEP, STATUS, OUTCOME, CLOSED_AT)` (+ the lease's holder / generation / expiry / waiter columns if kept here) |

  No `INSERT`/`DELETE` on either register table, no `STATE`, no `CONSUME_KEY`: **the grant itself makes
  Scansia Manager unable to move a pair through the state machine**, whatever its code does.
- `racoon_staging` does not exist (0 rows in `SCHEMATA`, `USE` → 1049, although `RacAdmin` holds grants
  on it — §10 §Background numbers): F7.1 creates the schema; the register tables from STOCK_Manager's DDL
  at `92f6f90` **with `sc_app_user`, their FK target** (`prisma/manual-migrations/2026-09-11-sc-returned-pair.sql:28-31`,
  `:122`); `SP_SCANSIA_LOOKUP` and `SP_SCANSIA_CONSUME` at their live text (`SHOW CREATE` on the day — the
  LOOKUP body changed 2026-10-03, E11); `online_products`, which both SPs read (SP spec `scansia-sp-spec.md:26-27`) —
  its live definition and collations, fixture rows including an outlet title; `sm_outlet_audit`; the same grants. Integration tests run there, never
  against `racoon` — including F7's writes running beside `SP_SCANSIA_CONSUME`: CONSUME waits at most one F7
  statement's commit on a row both touch — tested and bounded on staging.

### 3.7 Connection discipline — hard rules, from E11

The register is on the sale path; a new writer is a new candidate lock holder.

1. **Autocommit on**, asserted at connect and tested (`mysql-connector-python` starts with autocommit off, as PEP 249 requires — §10 R9);
   one statement per write (the lease take-over is one guarded statement, atomic on its own — §3.3 rule 6); an
   explicit transaction only around writes that must be atomic together — the lease's guarded renew opening every F7
   write: + the write-ahead `INSERT` (a group's rows in one, §3.3 rule 2), + `SENT_AT`, + the outcome, + the stamp and
   its `RECONCILE` row close (§3.1, §3.3 rules 2 and 6) — and one stated read exception, the baseline's `READ ONLY`
   snapshot (§3.4); each
   **never across a Shopify call or any network wait**; in every renew-first transaction the register `UPDATE`
   (`sc_returned_pair` / `sc_returned_pair_event`) is the last statement before `COMMIT`; every transaction closed in
   `finally` (ROLLBACK unless committed).
2. **No locking read** (no `FOR UPDATE` / `LOCK IN SHARE MODE`) — STOCK_Manager's own rule
   (`scansia-design.md:287`: "No `SELECT … FOR UPDATE` in the app"). **And no
   SELECT inside a write or an assignment** — `SET v = (SELECT …)`, `INSERT … SELECT`, `UPDATE … (SELECT …)`:
   under REPEATABLE-READ they read with shared locks, and that was the 2026-10-01 holder (E11, `7afe058`).
3. **Every `UPDATE` by PRIMARY KEY after a plain SELECT.** `OUTLET_PRODUCT_ID` has no index (the table's
   keys: `prisma/manual-migrations/2026-09-11-sc-returned-pair.sql:92-99`; `scansia-design.md:104-105`),
   and an `UPDATE` through a scan or a secondary index next-key-locks more than the rows it changes
   (REPEATABLE-READ: INC@619ebf2:252-254; §10 R10).
4. No connection outlives a job; a short session `wait_timeout`, proposed 60 s (the lane's to tune, tested; the
   server's is 28800 s: an abandoned session keeps its locks up to 8 h — §10 §Background numbers); nothing
   idle-in-transaction.
5. A short session `innodb_lock_wait_timeout` (proposed 5 s) is only the app's own give-up: it bounds the
   app's waits, never what the app holds (INC@619ebf2:210).
6. `CLIENT_FOUND_ROWS` on, so a guarded `UPDATE`'s count means "rows matched" (E12).
7. No literal `%` in a statement with bound parameters; any `LIKE` pattern is a bound value (E12).

### 3.8 What goes away

`gspread` and `backend/gsheet/`; `src/sync.py`, `fix_prices.py`, the `RUN_MODE=SYNC` path; the Render
**cron** service (`render.yaml`, suspended — its deletion is a Render change for
SERVER/Ale); the `GSPREAD_*` and Google credential env vars of the web service. After the first clean
run on MySQL the app no longer reads `Scarpe_in_Scansia`, which is what makes Ale's 2026-09-23 proposal
(back it up, delete it from Drive) safe — an F8 decision, recorded then.

## 4. Build shape (Sync-Scansia lane)

1. Settle the owed push of `feat/scansia-manager` (3 commits; `1e1c16d` tracks `.claude/settings.json`,
   so it is not docs-only) under that lane's own rules, before any F7 commit.
2. `RegisterPort` + in-memory fake; migrate the service tests from `FakeSheet` to the fake — the
   existing tests are the parity oracle for the positive-delta flows, except the `frozen_pre` branches and their tests — retired,
   not ported (§3.3).
3. MySQL adapters (register, audit) on `mysql-connector-python`; integration tests on `racoon_staging`,
   created by F7.1 (schema, register tables with `sc_app_user`, the two SPs at their live text, `online_products`, audit table).
4. Reconciliation rewrite (§3.3) and "Inizializza" rewrite (§3.4), with tests for: every row of §3.3's
   table, held keys included; every recovery case of rule 2 (`APPLIED` unclosed → stamp retried when every row of its
   group — a `SUPERSEDED` one aside — is `APPLIED`, closed `CARRIED` beside a `FAILED` sibling, waiting while a
   sibling is open; `FAILED` on
   `userErrors`, events pending; `OPEN` −Δ landed / not landed → re-applied; `OPEN` +Δ → key held; a CREATE's
   `userErrors` at each of its six steps and a crash in each window, product id recorded or not (orphan unknown: row
   kept open, found later by title) → orphan drafted and zeroed, every key of its Δ-per-size list held — each with a
   sale between crash and recovery; at start-up, before a pass, after a stamp failure); row close + stamp atomic and
   fenced, in that order (renew, row close, stamp, `COMMIT`), an already-stamped id counted as stamped by the matched count (no
   re-read), the stamp by PRIMARY KEY only (EXPLAIN beside CONSUME); the fenced
   lease — a crashed holder taken over, a live one refused, a stalled holder resuming after a take-over, including
   right after its renew — its write-ahead refused, its send past the local deadline refused, a send that slips through
   answered by the idempotency key (rule 6); a decrement interleaved between two SKUs of a publish / re-level, the next
   SKU re-derived (never inside a baseline: refused before its `BASELINE` row); a dead generation's never-sent row
   closed without holding; no mutation
   re-posted blind (rule 9); `IDEMPOTENCY_CONCURRENT_REQUEST` and `IDEMPOTENCY_KEY_PARAMETER_MISMATCH` answers; revive
   zeroing as a relative adjust with `committed > 0`; a revive's unfinished tail re-run by a publish-class holder's
   recovery and left open by the decrement's; F7's writes beside `SP_SCANSIA_CONSUME` on staging, CONSUME's wait bounded
   to one F7 statement's commit (§3.6); exact size match — a `42L`
   pair never published on `42 L`, the normalised-only match surfaced; a size-absent group pending, never stamped;
   DENY + tracked before any variant is exposed, customisation-only normalised to DENY + tracked, a positive Promo
   lowered to 0, a negative left; explicit activation only before a write that raises a Promo level (E15); the product-total rule on Δ = 0 groups and on re-applies; the §3.4 invariant on every key, or
   the key held — at confirm (moved key, `committed > 0`, multi-outlet, size-absent, spelling twin) and by recovery —
   excluded from every pass and released only by a per-key re-level, the N-0 release (size present or absent) or, for
   an in-flight hold, its row's close; a held key lowered by its excess, never raised; a quiet held
   key with P > N lowered by the next decrement; a stalled +1 landing on a held key after its −1 ran stamp-only; a
   revive zeroing every held key's real-size variant; every Level branch stamping the events of pairs no longer on the
   shelf; the SKU price and compare-at, one per SKU; `ONLINE` set by the baseline, the publish, a re-level and a
   revive; one audit row per
   F7 register write; `available` ≤ 0; customisation-only exclusion from counts; a failed / multi / truncated
   resolution ≠ none; a multi-outlet −Δ lowered on the product(s) holding the size, ACTIVE first; `BASELINE` missing →
   refusal; Part B writes
   `ONLINE` only — no statement names `STATE`, `CONSUMED_AT` or `CONSUME_KEY`; autocommit asserted, no locking read,
   every `UPDATE` by PRIMARY KEY. Round 5–6 mechanisms (§10 W, Y): the re-send window's edge — a row just inside and
   just past it, the past-window −Δ re-applied as a new row and the old closed `SUPERSEDED`; a re-send answered by
   each class (a cached success; `CHANGE_FROM_QUANTITY_STALE` → −Δ `FAILED` / +Δ `HELD`;
   `IDEMPOTENCY_PREVIOUS_ATTEMPT_FAILED`; an input-validation error; CONCURRENT → −Δ re-applied at once / +Δ re-sent
   until the window's end; any other error → −Δ re-applied at once / +Δ `HELD`); a split group with a `FAILED` sibling
   — no stamp, the
   siblings `CARRIED`, only the remainder re-applied — and an activation `APPLIED` beside a `FAILED` +Δ; an activation's
   `userErrors` resolved on `isActive`; a not-closed row's key held and its events excluded, never re-grouped under a
   new nonce; `SENT_AT` before `productDuplicate` and each tail call, a crash right after it; the stored request re-sent
   byte-identical; activation only of an absent or inactive level, explicit 0 / 0; a revive's activation, adds and
   tail only after its zeroing rows have applied;
   event-less rows past the window; the decrement's recovery leaving sent +Δ / CREATE rows and tail rows, reading an
   activation row off `isActive` (AB13); a decrement
   waiting on a held lease and the hand-off between SKUs, within its bound; a re-derived +Δ larger than confirmed
   deferred, the held-key lowering on a SKU absent from the plan, an outlet whose gid or status changed since the
   confirm deferred; a re-level of a mixed pending mix and of a key not held (a DRAFT-parked key); `HOLD` rows (the
   delete's hold, released by a re-level that recreates); the multi-outlet exit; the N-0 release. Round 7 mechanisms
   (§10 Z): a timed-out −Δ, not landed, then the held-key lowering → P = N (the sent `OPEN` −Δ counted), its in-flight
   hold ended by its row's close; a revive deferred while its product has a row not closed; a revive whose zeroing row
   ends `FAILED` or stays `OPEN` → nothing activated, added or exposed, events pending; a group's write-ahead atomic
   (a crash inside it leaves no row of the group); a crash after it, before a sibling's send → that sibling `FAILED`
   (never sent), the sent ones `CARRIED`, the remainder a new group; a `SUPERSEDED` member's successor inheriting its
   `GROUP_ID`; a re-send rewriting neither `SENT_AT` nor `SENT_AVAILABLE`, a row re-sent every pass leaving the window
   at its first send + 24 h − the margin; an unknown −Δ re-applied at once after its one in-window re-send; a +Δ
   re-send answered STALE → `HELD`; the N-0 release with the size present, on a multi-outlet key, and refused inside a
   `HELD` row's retention; a deactivate's outcome read off `isActive`; every lease holder yielding to a waiting
   decrement between SKUs / products, the bulk cleanup and deny-normalize included; the decrement on its own worker,
   interleaved with a publish in one process; a customisation-only Promo > 0 lowered by a revive and alarmed by S13; a
   never-sent row closed `FAILED` under the decrement; `ONLINE = 1` set by a re-level and a revive. Round 8
   mechanisms (§10 AA): a terminal path for every row kind — a deactivation read off `isActive` under the decrement,
   a tail or normalise call answered `userErrors` → `FAILED`, a row whose product, variant, inventory item or location
   reads gone, and an activation whose level can no longer be read, each closed by its kind (AA1, AB7), a delete
   (single, bulk, exit) closing its product's open rows, a person's close → `HELD` (AA1); a tail and a normalise row
   holding their keys, a second revive of their product deferred by its id (AA2); the bulk cleanup listing apart an
   outlet whose SKU has an `IN_SCANSIA` pair, at preview and at apply (AA3); a −Δ re-send or re-apply closed
   `FAILED` on STALE, its events' next lowering surfaced (AA4); a group's rows counted against its planning read, a
   premise contradicted at rule 8's read → that row `FAILED`, never sent (AA5); a group stamped whose rows repeat
   its event ids (AA6); a −Δ at `available` 0 with `committed` > 0 kept pending, applied when the hold is released,
   stamped when the unit ships (AA7); a +Δ never sent beside an activation not `APPLIED`, an activation re-send
   withheld once `isActive` reads active, a lowering of an inactive level with no activation (AA8); the N-0 release
   at P < 0 (AA9), before grouping with a pending +Δ (AA10), with a pair arriving between its two reads (AA11); a
   re-take with and without another holder in between, a run of re-levels under one sweep (AA12); each holder's
   recovery class (AA13); the lowering and S13 net of a `CARRIED` share (AA14). Round 8's targeted re-review (§10 AB;
   **AB1–AB3 mandatory, D18**): an activation `APPLIED` beside its +Δ closed `HELD`, a −Δ split member closed by a
   person beside `APPLIED` ones, a revive's zeroing rows `APPLIED` beside an add closed `HELD` — each `APPLIED` sibling
   `CARRIED`, the events held until the re-level or the N-0 release, nothing re-added (AB1); a normalisation refused on
   an ACTIVE outlet, and one closed by a person → the product drafted, S13 alarming a CONTINUE or untracked variant on
   an ACTIVE outlet (AB2); a revive ended at a zeroing row `FAILED`, and at one still `OPEN`, and a crash leaving its
   tail never sent beside one → the tail, written ahead, closed `FAILED` ("never run"), the retry not deferred by it; a
   zeroing row `CARRIED`, or `SUPERSEDED` by an applied successor, passing the tail's gate (AB3); a person's close under
   the lease, refused on a row closed meanwhile, and the releasing re-level stamping the `HELD` row's events (AB4); a
   normalise row's `SENT_AT` set before each call, a sent +Δ whose product reads gone closed by (ii) before its own
   bullet (AB5); a never-sent normalise row re-run by a publish-class holder, left by the decrement (AB6); (ii) and
   (iii) on an `APPLIED` row → `CARRIED`, on a sent −Δ → `FAILED` "possibly applied" (AB7); a key held only by a tail or
   normalise row neither read nor lowered by the held-key sweep, S13 still alarming it above N (AB8); a deactivation
   read absent → `APPLIED`, an activation read absent → `FAILED` (AB10); a person's close of a tail or normalise row →
   `FAILED` (AB11); an activation `OPEN` after a crash read off `isActive` by the decrement, never re-sent (AB13); a
   re-level re-running the publish of an ACTIVE, unpublished outlet (AB14); an "orphan unknown" CREATE whose orphan the
   single delete removed, closed by a person (AB15). B-3, decided (D23): the trigger route and the boot pass, with the
   build tests §6 B-3 lists.
5. Removal of §3.8; `requirements.txt` gains the driver, loses `gspread`.
6. `GATE:2` — `mutate-verify.sh` PASS on the suite, then `/ship`.

## 5. Proposed additions to the standing watch (to be written into `register-checks.md` at go-live)

- **S11 — deltas waiting**: per (SKU, SIZE) group, held events excluded: count, Σ and age of the oldest pending
  non-zero event (`OUTLET_RECONCILED_AT` NULL),
  and positive deltas left unpublished (count, Σ, oldest age); held keys (count, oldest age, reason — in-flight holds
  apart, with their row's `SENT_AT`); a negative Δ older than one sweep interval is the alarm, and so is a sent −Δ
  `RECONCILE` row still `OPEN` past B-3's bound — its events are held, so the first alarm cannot see it (per B-3: §7
  Exit).
- **S12 — nothing but F7's columns moved**: pairs whose `UPDATED_AT` is later than their last event,
  listed; each must be explained by an F7 audit row (`PRICE`, `REGISTER`).
- **S3 amended**: `register-checks.md` §1 S3 lists a "pair moved with NO non-import event" and must
  return 0 rows (`:95-98`, `:106`), so it fires on an F7 write that sets `UPDATED_AT` on such a pair;
  go-live amends it with S11/S12, F7's writes excluded through the audit table.
- **S13 — outlet vs shelf**: the E13 join (live Promo per real-size key vs `IN_SCANSIA` count) as a
  standing check (Shopify read + register SELECT); per key not held, the alarm is clamp(real-size Promo) + Σ pending Δ >
  N; on a held key, clamp(real-size Promo) + Σ pending Δ of its post-hold events + Σ Δ of its sent −Δ rows still
  `OPEN` > N — the held-key lowering's own term (§3.3), crossed by a Shopify-side restock (E16) or a hand edit — below
  N is listed, not alarmed; in both terms, Σ pending is net of the shares their `CARRIED` rows already moved (§3.3);
  on both, a sale waiting for its
  decrement keeps the two sides equal (its −1 is pending), so the normal gap never fires; plus any non-Promo `available`
  on an outlet
  product; held keys listed; a DRAFT or unpublished outlet holding real-size Promo > 0 with no open `RECONCILE` row is
  an alarm; so is any customisation-only variant with Promo > 0 on an outlet (a hand edit or a Shopify-side restock
  after the baseline: it sells unrecorded, §3.3 rule 7; the next revive lowers it to 0); and so is any CONTINUE or
  untracked variant, real-size or customisation-only, on an ACTIVE outlet — rule 7's forbidden case, it sells at any
  level, unrecorded (a hand edit, or a refused normalisation whose drafting did not land, §3.3 rule 2's terminal path
  (i)).
- **S14 — the Admin API pin**: the pinned version (F7.2, verified in F7.3) and its "Accessible until" date from
  shopify.dev/docs/api/usage/versioning (the table's column, `:68`); the app exposes its pinned version and the last
  `X-Shopify-API-Version` header it received (versioning `:87`); the standing watch reads both weekly — alarm 60 days
  before "Accessible until", and at once on a header ≠ pin — an expired pin falls forward silently (E15). A re-pin's
  precondition: every surface deprecated at the old pin migrated (F7.2's record, §7) —
  "Deprecated fields or types are removed in a subsequent release" (versioning `:101`).
- The SQL is authored at go-live, at effort `max`, as `register-checks.md` requires.

## 6. Decisions for Ale at GATE:1

**B-1 · Baseline — `GATE:1 B-1` — DECIDED 2026-10-05: *Level once, after D6*** (D20 «Allineo dopo il controllo»,
`PROGRAM.md`@`1b07888`:1532-1533; the other options below are kept as the record). Which outlet state F7 starts from (E13). The figures below are the
2026-09-30 08:12Z measurement **derived after D15** (E13), not re-measured; since then 3 shelf pairs sold
(events 714, 716, 718), `IN_SCANSIA` 249 at 2026-10-03 08:36Z (`PROGRAM.md`@`573b4df`:37) — the preview recounts on the
day.
- *Parity* (the sheet-era "Inizializza"): stamp every `ACTOR = 'IMPORT'` event (`IMPORTED` + `IMPORT_SYNC`, E17 — 682
  events at 2026-10-01 08:07Z: `IMPORTED` +1 254 / 0 422, `IMPORT_SYNC` −1 4 / 0 2, none reconciled;
  `PROGRAM.md`@`cfb1b15`:42); later events stay pending. Premise: the outlet reflects the import state — E13 shows it
  does not for many keys, and under §3.4's invariant every key where it fails is held until a per-key re-level (an
  N-0 key until the N-0 release, §3.3): Parity
  defers the levelling key by key, it does not avoid it — and because Parity stamps their import events, every
  DRAFT-only or no-outlet key reaches N only through a per-key re-level that revives or creates (§3.4): 41 + 99 pairs, 9
  of them with no source product (a person) — each re-level a lease take that sweeps every held SKU (§3.3: the
  resolver + one inventory read per outlet product), once per confirm when one confirm re-levels a run of keys
  (§3.3 rule 6); a key that gets a new arrival before its re-level is held like any other —
  no longer "for ever" (§10 W12). **Stock written**: no add; the confirm lowers every key it holds with live P > N by
  its excess (§3.3, §3.4) — so the five stale DRAFT units (the four + the `42 L` twin unit) are lowered to 0 at confirm,
  as under Level. Result: the 167 unexposed shelf pairs (16 on an ACTIVE size at 0 + 3 under-published + 41 only on a
  DRAFT + 7 size missing + 99 never published + the blank-SKU pair, §3.3 rule 7) stay unexposed, their keys held where
  the invariant fails; so are the keys whose pair left after the cutover with live P ≤ 0 — a full-price `CONSUME` −1,
  an X8 cancel −1 (e.g. a D6 phantom; `scansia-design.md:185`, `:612-613`): the −1 is held with the snapshot and the
  invariant reads 0 − 1 ≠ N — released by the first pass through the N-0 release where N is 0 (§3.3; nothing to
  expose), else at their re-level; the preview counts the class on the day; **nine DRAFT-parked keys** (N 1, the unit
  on a DRAFT outlet: `c7_join_sku_size.csv:85-86`, `:94`,
  `:108-109`, `:111`, `:121`, `:130`, `:132`, 2026-09-30, pre-D15) satisfy it (1 + 0 = 1): not held, nothing pending,
  never revived by a pass — each needs a re-level of a key not held (§3.4); until then S13's DRAFT alarm fires on them
  from day 1 (§5) and §7 Exit (S13 "clean for a week") waits. **Cost on D6's 69 keys — certain, not conditional**: D6's
  procedure restocks every found pair by hand **and** registers it (`PROGRAM.md`@`4673b97`:126-128), so each such key
  reads Promo 1 + a
  pending +1 for one pair — held under the invariant (without it, 2 units for 1 pair), each needing a re-level; a Parity
  that also stamps the events D6 registers on the 69 keys avoids it.
- *Level once, after D6* (**recommended; chosen, D20**), under preview → confirm and §3.4's precondition. Every branch stamps the
  snapshot events of pairs no longer on the shelf (I3: an outlet-sold pair nets +1 and would otherwise be published).
  (a) a key on one ACTIVE outlet that has its size (a variant titled exactly `SIZE`, §3.2): adjust that real-size
  Promo variant to N (relative: N − live `available`) — from any level, a negative one included (a re-level sets
  the level; it is not a −Δ group) — and stamp all the key's events in the snapshot; (b) a key whose
  only outlet is a DRAFT: lower its real-size Promo `available` to 0, relatively (the four stale units included);
  each shelf pair's events stay pending (+1 each, E17), so the revive restores exactly N (E9: revive = live + Σ
  pending) — its DRAFT outlet kept for it: the bulk cleanup never deletes an outlet whose SKU has an `IN_SCANSIA`
  pair (§3.5); (c) a key with no outlet: nothing written, its shelf pairs' events stay pending — the publish creates the
  outlet with N. **Held** (§3.4), nothing written, listed: a key whose outlet lacks the size as spelled — a person
  adds the variant or respells the pair at the source (X8 + B2, §3.4), then a per-key re-level; a multi-outlet key (its
  exit: §3.5); a key that moved
  between preview and confirm; a key whose upward adjust meets `committed` > 0. Result: +16 + 3 units on ACTIVE sizes
  (less any key held at confirm); −5 stale DRAFT units (the four + the `42 L` twin unit, `AR-K-33PC` on 15506338513228,
  `c7_join_sku_size.csv:421`; its `42L` pair's key held until respelled), and any unit a DRAFT holds on a size with
  pairs (zeroed, then restored by the revive); 41 + 90 pairs wait for the normal publish (price B-2, compare-at E10)
  and 9 have no source product (SKU not in the catalogue, E13) — a person; the 7 size-missing pairs and the blank-SKU
  pair (reason "blank SKU") are held — 3 spelling twins until respelled at the source (X8 + B2, §3.4), 4 until a
  person adds the variant; CONTINUE / untracked variants, customisation-only
  included, listed and normalised at confirm (DENY + tracked, §3.3 rule 7).
- **Not waiting for F7 — closed:** D15 (2026-09-30, `PROGRAM.md`@`4673b97`:1063) zeroed the 69 sizes and
  drafted 39 products (118 → 79 ACTIVE, E13); the operators look for the 69 at D6.

**B-2 · Who sets the outlet price of a new return, and the compare-at — `BUSINESS` — DECIDED 2026-10-05 and 2026-10-06: seed one
price per SKU from J, then the app's pricing screen; compare-at `PRICE_HIGH` where > 0, else `PRICE` where 0.00 or NULL** (D20
«Foglio, poi app», `PROGRAM.md`@`1b07888`:1534-1536; NULL: D25 «Prezzo di vendita», `PROGRAM.md`@`e87719d`:1739-1748). **The 10 NULL compare-ats — DECIDED (D25)**: D20's option asked Ale
to write their rule in a note and D20 records none; at gate-1 he chose `PRICE`, as for 0.00 — set on 7 of the 10; the 3 with
no `PRICE` (pairs 221, 367, 421, 2026-10-06) have no compare-at and fail FIX4 — unpublished until `PRICE_HIGH` is
corrected on STOCK_Manager's page. `PRICE_OUTLET` is
NULL everywhere, and FIX4 needs it **and** a compare-at — `PRICE_HIGH` is NULL on 10 shelf pairs, 0.00 on
9 (E10; 10 at 2026-10-05 22:16Z); F7 never writes `PRICE_HIGH`. The sheet's hand-typed J exists for 107 shelf pairs (E14).
Price options: seed one price per SKU from J at "Inizializza" — a SKU whose pairs carry different J
values is listed for Ale, never auto-picked — and set the rest in the app's pricing screen, per model
(**recommended; chosen, D20** — it is today's manual practice, moved); or a rule (e.g. a fixed discount on
`PRICE_HIGH`) that Ale states. Compare-at — **recommended; chosen, D20**: `PRICE_HIGH` where > 0,
else `PRICE` where `PRICE_HIGH` is 0.00 (the design maps `PRICE_HIGH` / `PRICE` to Compare_AT_Price / Price,
`scansia-design.md:79`; 0.00 records "not discounted", the full price being `PRICE`): measured 2026-10-03 21:17Z,
`PRICE` is set on all 9 (149.00–219.00) — e.g. pair 452 `GH-W-34PC-DIR` 43, `PRICE` 159.00, whose SKU's other shelf
pair carries `PRICE_HIGH` 159.00. The 10 NULL — **chosen, D25**: `PRICE`, set on 7 of them (not taken: the
full-price product's current compare-at, or its price; `PRICE_HIGH` corrected by hand on STOCK_Manager's page; no rule). One compare-at per SKU (§3.5):
a SKU whose shelf pairs yield different values is listed and blocked — under D20 + D25, of the 18 SKUs of the 20 NULL / 0.00
pairs, only `SMBW-W-C-BR-LACEND-ALCBG-CROB-LLACEND` (2026-10-06 11:39Z, `measures/2026-10-06-d25/siblings.txt`, local):
pair 631 (`PRICE_HIGH` NULL, `PRICE` 239.00) → 239.00, its other shelf pair 323 (`PRICE_HIGH` 279.00) → 279.00 — listed
for Ale and blocked (D25). D25 keeps the 3 pairs out, not their SKUs: pair 221's SKU `ASLPL-W-33PC70-DIR` has another
shelf pair, 118 (37, compare-at 159.00), which 221 does not block and whose compare-at 221 is not lent — a build test case (D16). Examples (§10):
pair 21 `AR-K-GLIGD` 35 — J 149, K 21.16 %, `PRICE_HIGH` 189.00; pair 103 `BO-K-CRTW` 39 — J 169,
K 10.58 %, `PRICE_HIGH` NULL.

**B-3 · When a sale's −1 reaches the outlet listing — `GATE:1 B-3` — DECIDED 2026-10-06: *Event + working-window net***
(D23 «A ogni vendita + riserva», `PROGRAM.md`@`94d78d3`:1695-1699; the other options below are kept as the record). At 2c
Ale chose none of the options below and asked «perchè non farlo event based e scalare/cancellare outlet quado il paio viene
venduto ?» (D20, `PROGRAM.md`@`1b07888`:1539-1549): the event-triggered option, studied on facts re-read at their sources
(D21, `PROGRAM.md`@`1b07888`:1552-1568), is the first bullet below; B-3 was re-asked with it and Ale chose it (D23). Orders arrive at any hour (678434
runs on `orders/create`, immediately — 678434 doc @0329757:65-66); a full-price sale of a
published pair leaves its outlet unit on sale until the −1 is applied; shelf changes and discards happen
in working hours (the logistics gate runs `*/15 8-19 * * 1-5` Europe/Rome, INC@619ebf2:178; for discards, an
assumption: operators' hours).
- *Event + working-window net* (**recommended; chosen, D23**; Ale's question, D20): 678434 calls, right after 168 on router 93 route 1 —
  scansia sales only (678434 doc @0329757:85-86; INC@619ebf2:576-577) — the "decrement only" endpoint of the next bullet,
  once, as a **trigger with no payload the endpoint trusts**: it runs the same pass (`pending_deltas()`, `stamp()`, §3.1);
  the register is the queue — a duplicate or late trigger finds the events stamped and no-ops; one arriving while a
  decrement runs or waits coalesces, at most one waiting (a design choice). 168's CONSUME commits inside the SP
  (`scansia-sp-spec.md`@`92f6f90`:63, :65, :72), so before the call — the deployed body's commit re-read at build (the
  deployed LOOKUP once deviated from its spec, E11); a parked 168 (Break 219) re-runs 168 and what follows it (678434 doc
  @0329757:67; INC@619ebf2:574), so no trigger precedes its CONSUME. **Never blocking an order**: the call carries a 5 s
  Timeout (Make's HTTP module: 1–300 s, no default stated, `apps.make.com/http.md:133`) and `onerror` `builtin:Ignore`,
  unfiltered — the codebase's never-stop handler (678443's module 8, its doc @93d89e5:26; not 216 on 206, an Ignore
  filtered on "ID not found", `IT/AUTOMATE/docs/f6-scansia-cutover-runbook.md`@`9a78dca`:78) — without a handler a
  failure leaves an incomplete execution, which stops the sequential queue (`help.make.com/scenario-settings.md:16`;
  `IT/AUTOMATE/docs/make-inventory.md`@`9736cdc`:188; the 09-24 stall); the endpoint answers at once and runs the pass on
  the decrement's own worker (next bullet). Cost to orders: ≤ 5 s on a scansia sale's execution and the orders queued
  behind it — 4 such sales from the cutover to 2026-10-05 (`PROGRAM.md`@`0fcb1d3`:63). **Asleep**: a Free service spins
  down after 15 min without inbound traffic and spins up on the next HTTP request in about one minute
  (`render.com/docs/free.md:59`, `:61`): the call ends within 5 s and is ignored; Sync-Scansia runs one decrement pass **at
  boot**, so the wake-up the call caused — or a person opening the app, or a restart (`free.md:130`) — applies what is
  pending (a design choice; that an abandoned request still completes the spin-up is inferred from `:61`, measured at
  build). At zero it drafts, never deletes (the decrement class); cancellations and restocks trigger nothing — every +1
  path stays where the plan puts it (E16; §3.3 rule 8). **The net**: the working-window job (third bullet), unchanged — it
  catches a lost trigger, an unknown −Δ's re-send (§3.3 rule 2) and the pass after a crash (§3.3 rule 6). Exposure:
  awake, 678434 reaching 168 + the endpoint's pass (the next bullet's terms after its ~15 min and cold start: one SKU's or
  product's flow + one recovery + that take's held-key sweep); asleep, + about one minute; a lost trigger, an unknown −Δ,
  or a crash (+ the lease expiry): the next pass — the next scansia sale's trigger, a boot pass or the window's next
  sweep (up to ~60 h over a weekend, third bullet). Cost: the window's awake hours + ≥ 15 min per off-hours wake-up
  (`free.md:59`) — whether Render's Free instance hours suffice stays not assessed, SERVER's to read (D23). Lanes and gates (§7 F7.5): AUTOMATE adds the call to 678434, the live order path — the 09-24 risk class,
  one module without its handler stops intake — under `GATE:live` (`IT/AUTOMATE/CLAUDE.md`@`73c1dd7`:59); the trigger route and the
  boot pass are F7.2's build; the net is a Scheduler job. Build tests: the call's failure paths (timeout, an
  error answer, asleep) never stop intake; a duplicate and a late trigger no-op; a trigger during a pass coalesces; the
  boot pass applies a pending −Δ. **Rejected as the source**, a Shopify webhook to Sync-Scansia: it races 678434 (each
  hook its own queue, `help.make.com/webhooks.md:53`, `:63`; 678434 `sequential`, its doc @0329757:68), so its pass may
  run before the CONSUME exists; 5 s per delivery (`shopify.dev/docs/apps/build/webhooks/verify-deliveries.md:133`)
  against a one-minute spin-up, and 8 consecutive failures delete a shop-specific subscription (`:143`;
  `shopify.dev/docs/apps/build/webhooks/subscribe.md:87`); duplicates (`verify-deliveries.md:110`), no ordering
  (`shopify.dev/docs/apps/build/webhooks.md:83`); a new route outside Basic
  Auth (Sync-Scansia `backend/app.py:10-11` @`fdfaf2f`) checked by an HMAC whose signing secret, for an admin-created
  custom app, was not found.
- *Automatic 24/7* (not taken, D23): a Scheduler job every 15 min at every hour calls a secret-guarded
  "decrement only" endpoint — it lowers and drafts; it never creates, activates or adds — under the pass lease (§3.3
  rule 6), **outside the `JobStore`** (its single slot answers 409 while any job runs, `api/jobs.py:7-10`) and on its
  own worker, not the jobs' single-worker executor (`app.py:122-123`, §3.3 rule 6); every holder but the baseline
  releases the lease between SKUs (products) so a decrement can take it — a waiting decrement takes it within one
  SKU's (product's) flow (the hand-off, §3.3 rule 6; a baseline holds it throughout, and before its `BASELINE` row the
  reader refuses the decrement) — and after every re-take re-derives the next SKU's groups (§3.3 rule 6). Exposure ≤
  ~15 min + a cold start + one SKU's or product's flow (the longest any holder keeps the lease between two releases;
  0.7 s per call, `transport.py:39`) + one recovery (each take runs its class's recovery first, §3.3 rule 2's
  Recovery classes: the decrement's, one re-send per open row — no decrement-class holder runs a CREATE's orphan
  search — bounded by the open rows, each surfaced; a publish-class holder's take-time recovery, orphan searches
  included, counts in its first flow before it releases) + that take's held-key sweep (per held SKU: the resolver's
  `products` query, which carries no quantities, `resolvers.py:49-66`, `:108-118`, + one `read_variant_inventory`
  per outlet product, `ops.py:618-640` — 2 calls or more for a SKU with an outlet, 0.7 s each; under Parity most of
  the 167 unexposed pairs' SKUs — the preview counts them on the day; a re-take after the holder's own release skips
  it unless another holder held the lease in between, §3.3 rule 6); for an unknown −Δ, + one sweep (its one
  in-window re-send, then the re-apply, §3.3
  rule 2); after a crash, + the lease expiry (§3.3 rule 6). Cost: the free Render instance never
  sleeps (`render.com/docs/free.md:59`): ~720–744 h a month (arithmetic) against the 750 Free instance hours a workspace gets per
  calendar month (`free.md:79`), shared with its other Free services (`feed-exporter-ws`,
  `IT/SVILUPPO/docs/deploy-inventory.md`@`5c7c4f7`:32); running out suspends every Free web service until the month
  ends (`free.md:82`), and Free is "not for production" (`free.md:9`). The hours used today are SERVER's to read, not
  assumed; if it does not fit, the event option or the window.
- *Automatic in the working window* (not taken alone, D23 — it is the chosen option's net; `*/15 8-19 * * 1-5` Europe/Rome, the gate sweep's cron): a −1
  recorded after Friday's last pass (19:45) waits until Monday 08:00 — about 60 h; each weeknight about
  12 h. Cost: the instance awake in those hours only.
- *Manual* (not taken, D23): an "apply exits" button — exposure until someone presses it, the human-dependent recovery
  the 2026-09-24 incident showed to be the weak point.
- Any automatic form: a cold start takes about one minute (`render.com/docs/free.md:61`) and a Scheduler job's timeout is 1–60 s
  (`IT/SVILUPPO/Scheduler` `src/lib/validation.ts:178` @`cbd2f8b`), so no job can be relied on to wait one out; the boot
  pass (first bullet) applies what is pending once the instance is up, whoever woke it.

**Mine, technical, overridable at GATE:1:** deltas not levels (§3.3); decrement drafts too;
write-ahead + the outcome recorded right after each call + a recovery that never reads success off a level — an open −Δ
re-applied, an open +Δ or CREATE holds its key (§3.3 rule 2); held keys released only by a per-key re-level (§3.4; the
in-flight hold, ended by its row's close, and the N-0 release aside, §3.3);
one pass at a time, a fenced database lease (§3.3 rule 6); no mutation re-posted blind (§3.3 rule 9); exact size
match (§3.2); DENY + tracked on every exposed variant, customisation-only variants out of every count (§3.3 rule 7); a per-key baseline — explicit
stamped list, tested invariant (§3.4); `ONLINE` informational; F7 never writes state (§3.4); an outlet
product id written only for an outlet by title and collection, cleared before a delete (§3.1, §3.5); one
audit table with a unique intent id, `INIT_BEFORE` / `BASELINE` apart; the grant set of §3.6; §3.7
(autocommit asserted, no locking read, `UPDATE` by PRIMARY KEY, short `wait_timeout`); the outlet cancel / return
operator rule (§8); **F7 goes live only after the D6 physical check** — the baseline trusts the register's shelf
counts, and D6 is what checks them (2026-10-05).

## 7. Phases, lanes, gates

| Step | What | Lane | Gate |
|---|---|---|---|
| F7.0 | Read-only measures — done 2026-09-30 (§2, §6, §10) | coordinator | none |
| F7.1 | `racoon_staging` (absent today: schema, register tables from STOCK_Manager's DDL with `sc_app_user`, the two SPs at their live text, `online_products`, `sm_outlet_audit`) then `racoon` (`sm_outlet_audit` only); user `scansia_mgr` + grants on both; how Render reaches the server (TLS, host pattern) | SERVER | `GATE:spawn`, `GATE:ddl` |
| F7.2 | Build §4; pins one named Admin API version and writes against it (`@idempotent`, `changeFromQuantity`, explicit activation; E15); migrates the two surfaces deprecated at the pin — `productUpdate(input:)` to its `product` argument (`productUpdate.md:115-117`), `Collection.ruleSet` to `Collection.sources` (`Collection.md:1014-1018`) — or records why not, and then S14's re-pin names their migration as its precondition (§5; "Deprecated fields or types are removed in a subsequent release", versioning `:101`) | Sync-Scansia | `GATE:spawn`, `GATE:2`, push per that lane |
| F7.3 | Render env (DB creds, CA), deploy (autoDeploy is OFF = owner deploy gate); read the service's `SHOPIFY_API_VERSION` and verify on the service the version pinned in F7.2 — **one named version**, the newest stable on the build day (`2026-10` on 2026-10-04, accessible until 2027-10-16; E15), which the build writes against (`@idempotent`, `changeFromQuantity`, explicit activation) —, list every document in `ops.py` and `resolvers.py` at `fdfaf2f` and re-test each on it, as F7.2 left it (`products(query: "sku:…")` + `inCollection`, `collection(id:)`, `node`, `inventoryLevel`, `publications`, `collections{ruleSet}`, `productUpdate(input:)`, `productDuplicate`, the three inventory mutations with `@idempotent` + `changeFromQuantity`, `inventoryActivate` with explicit quantities — and what it does on an already-active level (F7 never sends it there, E15) —, `productVariantsBulkUpdate` (`2026-04.md:112` — `changeFromQuantity` on its `quantityAdjustments` input, `ProductVariantsBulkInput.md:97-101`, which F7 never sends — DENY + tracked only, §3.3 rule 2), `inventoryDeactivate`, `publishablePublish`, `productDelete`, `metafieldsSet`, `productSet`, `locations`, `product` (media) — the lane lists each with its 2026-04 / 07 / 10 note or "none", and with its deprecations at the pin beside its breaking changes — `productUpdate(input:)`'s is in none of the three notes, `productUpdate.md:119-123`, `2026-04.md:234`); test a write to a not-stocked level at the pin (E15), and a read with `includeInactive: true` of an inactive Promo level holding units (E15); test on the store that a repeated key applies once (§3.3 rule 2). A stated proposal, not a fact: F7.3's live write tests name their item and gate — a test product outside the outlet collection, Promo only, Ale's `confermo` | SERVER + Ale | Ale's deploy |
| F7.4 | After D6: the baseline (B-1, "Inizializza" on MySQL) with §3.4's precondition, preview → Ale confirms; first publish (Ale's gesture); watch S11–S13 and the amended S3; T31 met (v265): 678434's 206→210 branch stays dead while the `online_products` mirror holds no outlet, F7's `OUTLET_PRODUCT_ID` or not; its first live run — the first scansia sale leaving `remaining` 0 with a mirrored id, whether it drafts or not — is AUTOMATE's oracle (E5) | coordinator + Ale | Ale's gestures in the app |
| F7.5 | B-3's trigger, decided (D23, event + working-window net): the call in 678434 route 1 after 168 and the net, a Scheduler job in the working window (§6 B-3) | SVILUPPO (Scheduler); AUTOMATE (678434) | Ale's `confermo` (live automation); 678434 under `GATE:live` (`IT/AUTOMATE/CLAUDE.md`@`73c1dd7`:59) |

**Exit**: one publish and one decrement observed on real events; S11–S13 and the amended S3 clean for a week;
the sheet unread by any process (then F8). S11's bound per B-3: event + window net, chosen (D23) → the working window's (a lost
trigger is the net's to catch, not an alarm: the event shortens the usual wait, not the bound); the options not taken (D23), as the record: automatic 24/7 → no negative Δ older than one sweep
interval + one SKU's or product's flow + one recovery and its held-key sweep (per held SKU: the resolver + one
inventory read per outlet product, §6 B-3) (+ one sweep for an unknown −Δ; + the lease expiry after a crash), and
no sent −Δ `RECONCILE` row `OPEN` past that bound; working window → none older than
the next
window's first pass; manual → reported (count,
oldest age), no alarm — the button is the bound.

## 8. Risks

| Risk | Mitigation |
|---|---|
| A new lock holder on the sale path (E11) | §3.7 (autocommit asserted, no locking read, `UPDATE` by PRIMARY KEY, short `wait_timeout`); the grant excludes every state column; the idle-holder watch logs a repeat (E11) |
| Oversell from a full-price sale of a published pair | B-3; decrement also on drafts; a relative adjustment right after a live read (§3.3 rules 4, 8) |
| B-3's event option, chosen (D23): a new call in 678434, the live order path (the 09-24 risk class: one module without its handler stops intake) | a 5 s Timeout + `onerror` `builtin:Ignore`, its failure paths tested at build; `GATE:live` (§6 B-3, §7 F7.5); the working-window net catches a lost trigger |
| A crash between the Shopify write and the stamp | write-ahead `RECONCILE` row (a CREATE's before `productDuplicate`); the outcome written right after the call; stamp + row close in one fenced transaction; recovery before every pass and after a stamp failure, for rows with no recorded outcome and for a CREATE failed after step 1 (its product id recorded right after `productDuplicate`), never reading success off a level (§3.3 rule 2): an open −Δ re-applied, bounded by rule 4; an open +Δ or CREATE holds its key — never re-added, never stamped; an orphan drafted and zeroed |
| Two passes at once (deploy overlap; `JobStore` per process) | a fenced database lease — a generation on every `RECONCILE` row, a guarded renew opening every F7 write, the write-ahead in the renew's transaction, each send before a local deadline, the idempotency key at Shopify — taken over only once expired, recovery first; after every re-take the next SKU's groups re-derived from `pending_deltas()` as they are now and, for a re-level, the key re-read right before its write; a waiting decrement handed the lease within one SKU's (product's) flow of any holder but the baseline (§3.3 rule 6) |
| A mutation re-posted by the transport after a lost response (`transport.py:84-89`, `:120-127`, `:140-146`) | §3.3 rule 9: `changeFromQuantity` on every relative write (required at the version pinned in F7.2) and the idempotency key (rule 2) **and** no transport re-send of a mutation unless the answer says it did not run; tested |
| A pair spelled differently from its variant (`42L` / `42 L`): an outlet sale finds no pair and the pair stays `IN_SCANSIA` | exact match only (§3.2); a normalised-only match surfaced for respelling at the source (X8 + B2, §3.4) |
| A key whose level F7 cannot establish (moved at confirm, `committed` > 0, multi-outlet, size-absent, an unknown +Δ or CREATE) | held: no add, listed in S11 / S13, released only by a per-key re-level, a human gesture (§3.4) — except the in-flight hold, ended by its row's close (§3.1), and the N-0 release (N 0, live P ≤ 0, size present or absent, §3.3); a multi-outlet key's exit: §3.5's multi-outlet exit — a person chooses the outlet to keep, every other outlet's Promo lowered to 0 and the outlet deleted, then a per-key re-level on the kept one — e.g. `GLP-W-LEOGD-33PCGD-DIR` (`c7_join_sku_size.csv:258`; N 0 on every size at 2026-09-30: a hold only if D6 registers a pair of it) |
| The existing positive branches losing a `committed` unit | §3.3 rule 3 — fixed in F7, tested with `committed > 0` |
| An `OUTLET_PRODUCT_ID` reaching 678434's 206→210 branch — fixed in v265 (T31 met 2026-10-03) but never run on a real line; it first runs on a scansia sale leaving `remaining` 0 once the mirror holds that outlet — through F7's id or the SPs' title fallback alike (E5) | only for an outlet by title and collection, never a `review` match; cleared before a delete; the branch and its first real run are AUTOMATE's (R5 row closed, `05f1d92`; §7 F7.4) |
| Baseline built on a wrong shelf count | go-live after D6 |
| Two writers of outlet stock (a person by hand + the app) | no outlet product created since 2026-01-03 (E14); hand edits of Promo stock cannot be excluded by the API — B-1's preview lists every change before it is made; afterwards S13 alarms on a hand edit that exposes a unit (a key above N, a customisation-only Promo > 0, a CONTINUE or untracked variant on an ACTIVE outlet) |
| A Shopify-side restock of an outlet line raises Promo with no event (E16) | §3.3 rule 8; S13; operator rule: an outlet line **cancelled before shipment** is restocked on Shopify and its pair `RESTORE`d (X9, admin/manager) on STOCK_Manager's page the same day — `RESTORE` writes 1 − Σ previous = 0 for an outlet-sold pair, so F7 adds nothing (E17; `scansia-design.md:186`, `:218-221`); an outlet line **returned** is refunded **with no restock** — the pair comes back through the gate as a new pair (RESO), whose +1 F7 publishes; a restock there would show two units for one pair, and S13 catches the slip (design `:16-17`, `:176`; `plan.md` T2) |
| The app failing on the Admin API version it really uses (E15, assumption) | F7.2 pins one named version and builds against it; F7.3 reads the env var, verifies the pin and re-tests every call on it (E15's breaking changes; each document's deprecations at the pin, F7.2 migrating `productUpdate(input:)` and `Collection.ruleSet`); S14 watches its "Accessible until" date and the served version header; keep the resolver's title leg; customisation-only variants excluded by option value (§3.3 rule 7) |

## 9. Not in F7

STOCK_Manager's code and page (the register owner's); the Make scenarios; 678434's 206→210 branch
(AUTOMATE's; fixed in v265, T31 met 2026-10-03) and the blank-SKU sale-path question (AUTOMATE's); the SPs; the D6 check itself;
F8's archive/delete of the sheet; the lime label and the Butler button (F8).

## 10. Review dispositions

Round 1 — two fresh adversarial reviews of 2026-09-30 (`reviews/f7-review-2026-09-30-fable.txt`, 19
findings; `…-opus.txt`, 17 + 4 mandate notes; local only), merged into 23 distinct defects and judged by
the coordinator `scansia-f6` (context `12ce47d2`) on 2026-09-30 23:25–23:40Z. Every cited line was
re-read at its pin: `SS` = Sync-Scansia `fdfaf2f`, `SM` = STOCK_Manager `92f6f90` (at `a9fcf7a`, today's
`origin/main`, none of the three pinned files moved since `5eadda1`), `INC` = the incident doc at
`3d21853`. "Change" is what §1–§9 must say after the fix (step 1f). Applied to §1–§9 on 2026-10-01
(context `0eb67596`); plan:N in "Line re-read" = this file at `e5495c7`. Three applications go past the
Change column's letter, flagged: R7's exposure is computed from the window's cron (last Friday pass 19:45
→ about 60 h, not 19:05 → 61 h); R2/R12's baseline stamps per pair with a stated invariant (§3.4), since
an `IMPORT_SYNC` re-base stamped apart from its pair's other events breaks E17's sum; B-1 Level's (b)
zeroes DRAFT-held units on sizes with pairs too, closing an N + p over-count on revive (E9) that round 1
did not name. **Diff review, 2026-10-01 (coordinator, context `76399a15`)**: every hunk of `git diff e5495c7 --
f7-plan.md` maps to an R-row or to the status / pins / this intro; nothing else moved; spot-checked at the
source: E4's three `168.recordset` references, `bab9d57`'s "not exercised", X9 `RESTORE` from `VENDUTO`
(`scansia-design.md:186`, `:218-221`). Corrected in the review: pins (`9d99f18`; the design moved in
`c867e58` / `abfcc8f`); E11 + the 2026-09-30 16:15Z recurrence; S11's and §8's leftovers of the old cut and
compare-and-set; B-1 Parity's event counts and Level's DRAFT zeroing in its result; §3.7 rule 1 and §3.3 rule 6
sourced or labelled. **R5's "reachable today" is superseded by T31** (`PROGRAM.md` ## Decisions), re-checked
2026-10-01 13:32Z: the branch is dead and F7's id cannot reach it while the mirror holds no outlet — E5,
§3.1, §7, §8, §9 rewritten; R5's row below stays as the round-1 record.

| # | Defect | Reviewers | Verdict | Line re-read | Change |
|---|---|---|---|---|---|
| R1 | Crash rule 2 is inverted: a −Δ row "closed as applied" loses the −1 when the crash preceded Shopify; a +Δ whose stamp failed is re-added; the rule runs only at start-up | Fable F1, Opus F1 | accepted | plan:133-137; SS `outlet_service.py:326` (`frozen_pre`), `:700-701` (`live + st.delta`); SS `app.py:25` ("A restart LOSES in-flight jobs"); INC:198 (mysqld OOM kill) | Recovery compares the row's recorded `available` with live: −Δ not applied → re-apply (bounded by R21), applied → stamp, ambiguous → apply; +Δ applied → stamp only, ambiguous → never add, surface. Runs before every pass and on any stamp failure, not only at start-up |
| R2 | `ID > baseline_cut` (cut = max event id) leaves every pre-init shelf pair with no pending event, so "the normal publish" never sees them | Fable F2, Opus F2 | accepted | plan:87, :162-163, :251-252; SS `outlet_service.py:382` (publish reads unreconciled rows only) | Drop the ID clause. The baseline is per key: stamp exactly the event ids the chosen B-1 option declares reflected (an explicit list, rule 5); every other event stays pending |
| R3 | The one-shot level re-creates the in-flight hazard of §3.3 (an outlet sale parked or in flight at level time is put back); the quantity it sets and the cut's snapshot are unstated | Opus F3 | accepted | plan:153-158, :249-251; `PROGRAM.md` Current state (since v264 a parked 678434 line hides; `/dlqs` is its oracle) | Level = relative adjust (target − `available`), never an `on_hand` set; only with `/dlqs` empty and 678434 idle, re-checked at confirm; shelf counts and event ids read in one snapshot; later events stay pending |
| R4 | Five DRAFT outlets hold a Promo unit with no pair; a revive puts it on sale | Fable F3, Opus F4 | accepted | `c7_join_sku_size.csv:421` (`AR-K-33PC 42 L`, 15506338513228; `:86` 36 L has a pair on it), `:782`, `:1221`, `:1343`, `:1438`; none in `over69/pre_state.csv`; SS `outlet_service.py:700-711` vs `:603-604` (CREATE zeroes, revive does not) | Revive zeroes every Promo size that has no shelf pair before activating (CREATE's rule); B-1's preview lists the five units for removal |
| R5 | 678434's 206→210 branch (no handler on 210; 209 sums blank-SKU variants) | Fable F4, Opus F12 | F4 partly: the branch is reachable **today** — SP §5 falls back to any mirror product with the SKU and a `%- outlet%` title, so `outlet_product_id` is not NULL now; AUTOMATE's risk, not F7's switch. F12 accepted: F7's id overrides that fallback | SM `scansia-sp-spec.md:248-256`; RB:77-81; `678434-shopify-nuovo-ordine.md:9-11`; INC:88 ("unexercised"); SS `resolvers.py:154-157` (`review`) | F7 writes `OUTLET_PRODUCT_ID` only for a product that is an outlet by title **and** collection, never a `review` match; §8 row rewritten; 206/209/210 routed to AUTOMATE (`/handoff`) |
| R6 | Customisation-only variants: seven carry a real SKU; CONTINUE / untracked variants sell at any level; "Promo total becomes 0" undefined; `deny_normalize` absent | Fable F5, F11; Opus F6 | accepted | `c7_summary.json` b5: ACTIVE CONTINUE 28, untracked 6, blank-SKU Promo < 0 on 2 (Σ −2); `c7_join_sku_size.csv`: 7 "Solo Personalizzazione" rows with a SKU (re-counted); SS `delete_service.py:70-76`, `:745-752`; `resolvers.py:130` | Identify them by option value (every spelling), not by blank SKU; exclude them from every match, count and total. Product total = Σ `available` of real-size Promo variants, each clamped at 0. B-1's preview lists CONTINUE / untracked real-size variants for `deny_normalize` + tracking before any level |
| R7 | B-3 "working window" while sales and CONSUME run 24/7 | Fable F6 | accepted | plan:266-267; `678434-shopify-nuovo-ordine.md:49-50` (`orders/create`, `immediately`) | B-3 restated: 24/7 vs window, the window's exposure in hours (Friday 19:05 → Monday 08:00 ≈ 61 h) and the cost of each |
| R8 | `register-checks.md` §1 S3 ("must return 0 rows") fires on every F7 write that sets `UPDATED_AT` | Fable F7 | accepted | `register-checks.md:95-98`, `:106`; plan:89, :194 | Go-live amends S3 together with S11/S12 (F7 writes excluded through the audit table), authored at `max` |
| R9 | E11 and §3.7 predate the settlement: the lock-wait timeout bounds the waiter, not the holder; the driver's autocommit is off by default; locking reads are not barred; no idle bound | Fable F8, Opus F5 | accepted (idle bound: §Background numbers, below) | INC:210 ("bounds the **waiter**, not the holder"); `infra-changelog.md` at `3d21853`:26, :40; at `bab9d57`:17-18; SM `scansia-design.md:287` ("No `SELECT … FOR UPDATE` in the app") | E11 restated from `3d21853` + `bab9d57`. §3.7: autocommit asserted and tested; no locking read; every transaction closed in `finally` (ROLLBACK); a short session `wait_timeout`; the lock-wait timeout kept only as the app's own give-up |
| R10 | `clear_outlet_product(product_id)` filters an unindexed column: table-wide next-key locks | Fable F14, Opus F5 | accepted | SM `manual-migrations/2026-09-11-sc-returned-pair.sql:92-99`; `scansia-design.md:104-105` | Every register UPDATE goes by PRIMARY KEY after a plain SELECT — a §3.7 rule |
| R11 | B-1 describes the pre-D15 state | Fable F9, Opus F9 | accepted | `PROGRAM.md` Current state, "Derived post-D15 figures" | B-1 restated on the derived figures, labelled derived: over-exposed 69 → 0; ACTIVE size at 0 25 → 16 pairs; DRAFT-only 32 → 41; products 79 ACTIVE / 103 DRAFT; the "Not waiting for F7" bullet is closed by D15 |
| R12 | "Stamp every `IMPORT` event" names no event type; `IMPORT_SYNC` carries re-base deltas | Fable F10, Opus F2 | accepted | plan:246; SM `scansia-design.md:219-220`; `register-checks.md:533` | Name them `ACTOR = 'IMPORT'` (`IMPORTED` + `IMPORT_SYNC`); re-base deltas are stamped by the baseline, never applied |
| R13 | Part B keeps the sheet rule that drafts a whole product for one sold-out size, keyed on `ONLINE` | Fable F12, Opus F14 | accepted (minor; errs to fewer; 0 products on 2026-09-30 data) | SS `init_service.py:43-44`, `:271-275`, `:480-482` | Part B writes `ONLINE = 0` only; drafting follows §3.3's product-total rule |
| R14 | FIX4 also rejects a missing or zero compare-at, and compare-at is `PRICE_HIGH` (read-only for F7) | Fable F13, Opus F8 | accepted; count below | SS `outlet_service.py:265-272`; `gsheet/reader.py:96` ("Prezzo High (col H) -> Shopify compareAtPrice"); SM `scansia-design.md:695`; `register-checks.md:142-147` (pair 713 at 0.00) | B-2 widened to the compare-at of NULL / 0.00 pairs (Ale's rule); S11 also reports positive deltas left unpublished |
| R15 | A Shopify refund/cancel restock of an outlet line raises Promo with no event; the returning pair adds +1 | Fable F15, Opus F7 | settled by §Background numbers, below | `678443-shopify-ordine-cancellato.md:11-14` (restocks non-outlet items only); SM `scansia-design.md:16-17`, `:176` | Every Promo write reads live `available`; the E13 join becomes a standing statement; an operator rule for outlet refunds if the numbers show restocks |
| R16 | B-2 seeds a price per pair; publish takes the first row's price per SKU | Fable F16 | accepted | SS `outlet_service.py:381-385` | Seed one price per SKU; a SKU whose pairs carry different J values is listed for Ale, never auto-picked |
| R17 | The B-3 endpoint is not bound to the single-slot `JobStore` (per process: a deploy overlap runs two); publish has no audit sink | Fable F17, Opus F15 | accepted | SS `api/jobs.py:7-10`; `app.py:123`; `api/publish.py:12` | A DB-held single-run guard (an `OPEN` `RECONCILE` row refuses a second pass) plus `JobStore`; publish writes `RECONCILE` rows too |
| R18 | E15's version is an env var; no target version | Fable F18, Opus F16 | accepted | SS `config.py:17`, `:51`; `render.yaml:13-14` (cron `2025-01`) | E15 a labelled assumption; F7.3 reads the service's `SHOPIFY_API_VERSION` and pins a named version, re-testing `collection(id:)` and `inCollection` on it |
| R19 | Citations that do not carry their claim | Fable F19, Opus F16 | accepted | `SVILUPPO/docs/deploy-inventory.md:54` (no date); `b6_outlets_created_by_month.csv` (monthly); plan:65, :72 | E8 date from a source or dropped; E14 "167 in 2026-01, last on 2026-01-03"; E4 quotes `first(map(168.recordset; …))`; pins row gains `5eadda1` / `a9fcf7a` |
| R20 | "< 0 · none" undefined: a multi-outlet, truncated or failed resolution can read as absence | Opus F10 | accepted | SS `resolvers.py:187-189`; `outlet_service.py:399-403`; `c7_join_sku_size.csv:258` (`GLP-W-LEOGD-33PCGD-DIR` on ACTIVE and DRAFT) | "none" = a clean answer with 0 matches; anything else leaves the group pending and surfaced |
| R21 | `min(|Δ|, available)` with `available` < 0 raises stock | Opus F11 | accepted | plan:122, :148-149; `c7_summary.json` b5 (2 ACTIVE variants < 0) | Rule 4: `available` ≤ 0 → stamp, no Shopify write |
| R22 | `INIT` is both the init before-snapshot (written before any Shopify write) and the baseline marker the reader gates on | Opus F13 | accepted | SS `gsheet_audit.py:227-231`, `:254`; plan:163, :171, :185 | Two kinds: `INIT_BEFORE` (snapshot) and `BASELINE` (the cut, written last); the reader gates on `BASELINE`; a missing cut is never read as 0 |
| R23 | Integration tests assume `racoon_staging` holds the register tables; F7.1 provisions only the audit table | Opus F17 | settled by §Background numbers, below | plan:200, :228, :283 | F7.1 creates the register tables in `racoon_staging` from STOCK_Manager's DDL if absent |

Mandate notes (Opus, 4): the plan was committed after `3d21853`; the holder is a class, not a process;
`bab9d57` put the diagnostics live; seven customisation variants carry a SKU — all four accepted (R9, R6).

**§Background numbers** (`shopify-operator`, read-only, 2026-09-30 23:29–23:34Z; figures re-read by the
coordinator on the raw output):
- **R9** — `@@GLOBAL.wait_timeout` 28800 s, `interactive_timeout` 28800, `innodb_lock_wait_timeout` 50,
  `transaction_isolation` REPEATABLE-READ: an abandoned session keeps its locks up to 8 h → accepted; the app
  sets a short session `wait_timeout` and never keeps a connection past a job.
- **R14** — `IN_SCANSIA` 251: `PRICE_HIGH` NULL 10 · 0.00 9 · > 0 232 → 19 pairs fail FIX4 on the compare-at,
  whatever B-2 decides for the outlet price.
- **R15** — 172 refunds since 2026-01-01; outlet refund lines 2, both `CANCEL` with restock (RAC-40298IT,
  RAC-40555IT, July 2026); 0 `RETURN`, 0 `NO_RESTOCK` → accepted: the path occurs.
- **R23** — schema `racoon_staging` does not exist (0 rows in `SCHEMATA`; `USE` → 1049), although `RacAdmin`
  holds grants on it → accepted and wider: F7.1 creates the schema, the register tables and the audit table.
- **B-2 / E14** — J and K are cell objects: the value is `IMPORTED.PAYLOAD.raw.J.v` / `raw.K.v`. J is filled
  on 107 `IN_SCANSIA` pairs (`PRICE_HIGH` > 0 103, NULL 4, 0.00 0); K on 111 (4 at 0, 1 text). Examples:
  pair 21 `AR-K-GLIGD` 35 — J 149, K 21.16 %, `PRICE_HIGH` 189.00; pair 103 `BO-K-CRTW` 39 — J 169,
  K 10.58 %, `PRICE_HIGH` NULL.

Round 2 — two fresh re-reviews of this file at `37ece8a`, 2026-10-03 (`reviews/f7-review-2026-10-03-fable-r2.txt`, 14;
`…-opus-r2.txt`, 11; local only), merged into 17 distinct defects (S17 the coordinator's own), judged by `scansia-f6`
(context `3093a4b3`) on 2026-10-03 13:00–13:30Z; every cited line re-read at its pin (`SS` `fdfaf2f`, `SM` `92f6f90`,
`INC@<rev>`). Tally: 14 accepted, 3 partly, 0 rejected (one sub-claim of S4 rejected). **Fixes applied 2026-10-03** by a sub-agent from `reviews/f7-2b-change-list-2026-10-03.txt` (local only);
**diff-reviewed** by the coordinator (context `55723832`) the same evening: all 49 hunks of `4d41591..2a9deaf` map to
the list's items, nothing else moved. The sub-agent's four flags, settled: rule 2's −Δ is now surfaced, as rule 8
says; §3.7 rule 1 — the lease take-over is one guarded statement, not a transaction; §6 Parity's "zeroes" →
lowered to 0 relatively; §7 Exit split into sentences. Two defects of the list itself, corrected: (1) E5, §3.1
`set_outlet_product`, §7 F7.4 and §8 said F7's first id write is 678434's 206→210 branch's first live run — but
both SP branches return an id only for a product the mirror holds (SP spec §5, `scansia-sp-spec.md:248-262` at
`92f6f90`), so the branch runs once the mirror holds an outlet, F7's id or not; (2) E11 said 709 was re-run by
v264's Break — 709 was the 6 h stall, before v264. E5's evidence now cites SERVER's applied LOOKUP text
(`7afe058`). Round 3 — a full re-review of this text — is pending.

| # | Defect | Reviewers | Verdict | Line re-read | Change |
|---|---|---|---|---|---|
| S1 | Rule 2 infers from a level comparison whether its own write landed: a sale between crash and recovery fools it (−Δ "applied", +Δ "not applied", +Δ "ambiguous" all over-count); CREATE records no variant and its orphan is revived to 2Δ; rule 8's "never overwritten" is false under re-apply | Fable 1, 9, 13; Opus 1 | accepted | plan:138-145, :390; SS `outlet_service.py:326`, `:586-617`, `:700-701`; SM sp-spec:206-208 (outlet CONSUME Δ 0); SS `TRACKER.md:31`; `resolvers.py:235` | Recovery never reads success off a level. Open −Δ row → re-apply relative to live, bounded by rule 4 (a double −1 errs to fewer); open +Δ row (ADD, REVIVE, CREATE) → never re-add: stamp its events, row surfaced for a person. CREATE writes its row before `productDuplicate` (source, title, Δ per size); an orphan keeps what it holds for stamped shelf pairs (E9-consistent), stays DRAFT, surfaced. Requirement: +Δ at most once, −Δ at least once, an unknown outcome resolves toward fewer and is surfaced; an idempotency key or `changeFromQuantity` re-send, if the pinned API offers it, replaces the rule (lane checks). Rule 8, §8 reworded |
| S2 | Stamp and row close are two autocommits and the stamp errors on an already-stamped id: a crash between them fails every later recovery, the guard never releases | Fable 2 | accepted | plan:91, :139-140, :166, :251-254 | Stamp + `STATUS = DONE` in one DB-only transaction, named in §3.7 rule 1; an id already stamped counts as stamped (error only for an id neither updated nor already stamped) |
| S3 | Guard and recovery do not compose: a crash leaves the guard held, nothing tells a dead holder from a deploy-overlap peer; R17's "an OPEN `RECONCILE` row refuses a second pass" is check-then-insert | Opus 2 | accepted | plan:140, :161-166, :286, :446; SS `app.py:25` | The guard is a lease (holder id, expiry beyond the longest pass, renewed by a live holder, taken over atomically once expired); the taker runs recovery first; §4 tests a crashed holder taken over and a live one refused. R17's wording superseded |
| S4 | B-1 Parity double-counts each pair D6 finds and registers (hand +1 + pending OPERATOR +1: 2 units for 1 pair, up to 69 keys); Parity's ACTOR split vs the per-pair rule's letter | Fable 3, Opus 3 | partly — D6 accepted; Fable's gate-born pair sold on an outlet line rejected: per key Promo + Σ pending = N still holds (Shopify −1, pair gone, CONSUME 0) | `PROGRAM.md:117-118`; SM design:132-134 (I3), :169, :186, :825-828; plan:188-190, :312-317 | Parity's result names the cost, or Parity also stamps the events D6 registers on the 69 keys; plan:190 reads "a pair's import events together" |
| S5 | §8's outlet-refund rule over-counts a return: RESO enters through the gate as a new pair (ARRIVE +1) on top of the Shopify restock | Fable 4, Opus 4 | accepted | plan:396; SM design:16-17, :176; `plan.md` T2 | Cancel before shipment → restock + X9 `RESTORE` (0); return → refund with **no** restock, the gate's +1 publishes it; S13 catches a slip |
| S6 | Level's precondition cannot see an outlet sale whose CONSUME lands after the preview (Δ 0 never enters the pending set; N not re-read at confirm); `/dlqs` "empty" never holds (resolved entries persist); N and the pending ids from separate reads | Opus 5, Fable 8 | accepted | plan:90, :186, :195-199; SM sp-spec:206-208; `PROGRAM.md` ninth watch (`allDlqCount` 4, all resolved) | "No unresolved `/dlqs` entry"; at confirm re-read per key N and its max event id (any Δ) — changed → skip, surface; an upward adjust only where the variant's `committed` is 0; N and event ids from one non-locking consistent read (one statement or a READ ONLY snapshot, closed before any Shopify call — a stated §3.7 exception) |
| S7 | Level (c) "the publish adds N" is false: no flow adds a missing size (`unmatched_size … continue`; no variant-create op); the 7 size-missing pairs sit on DRAFT-only SKUs; three are spelling twins (`AR-K-33PC` `37L`/`42L`/`43L` vs `37 L`/`42 L`/`43 L`), so the "five stale DRAFT units" include a backed one | Fable 5, Opus 6 | accepted | SS `outlet_service.py:180-181`, `:612-614`, `:673-676`, `:696-699`; `git grep` BulkCreate: none; c7:88-90, :411, :421, :423; `c7_summary.json` bucket2_sub / bucket3_sub | Size normalisation drops inner spaces (`42L` = `42 L`), tested; size-absent and not-in-catalogue pairs stay pending, surfaced (a person adds the variant); E13's five → four + one twin; Δ < 0 with no matching variant → pending, surfaced, never stamped |
| S8 | Rule 3's "set `on_hand` from `on_hand`" and the revive zeroing are blind absolute writes; zeroing a DRAFT that holds a committed unit leaves `available` −1 | Opus 7 | accepted | SS `ops.py:495-501` (`ignoreCompareQuantity: True`); `outlet_service.py:603-604`; plan:130, :155 | Relative adjusts only (or compare-and-set); revive zeroing = lower `available` to 0 relatively, never `on_hand` := 0 |
| S9 | Exit: S11 vacuous under manual B-3; S13 unscoped, alarms for ever on DRAFT-held units | Opus 8 | partly | plan:297, :303-305, :381-382 | S13's alarm = ACTIVE real-size Promo > shelf; DRAFT-held units listed; Exit states S11's bound per B-3 option |
| S10 | `racoon_staging` cannot be built from the register DDL alone (FK to `sc_app_user`); F7's writes are never tested beside CONSUME's locks | Fable 11, Opus 9 | accepted | SM `2026-09-11-sc-returned-pair.sql:28-31`, `:122`; plan:242-245, :284-289, :375 | F7.1 also creates `sc_app_user` and the two SPs at their live text; §4 adds F7's writes running beside `SP_SCANSIA_CONSUME` on staging — CONSUME never waits on F7 |
| S11 | Stale since 2026-10-03: holder named (LOOKUP's `SET v = (SELECT …)`, a locking read under REPEATABLE READ, autocommit off on 146's connection, S,GAP on `uk_rpe_dedup_key`), fixed by SERVER 08:55:19Z (`7afe058`); 4 of 4 SP CONSUMEs through a 1205; v265 live, T31 met, the R5 row closed | Fable 7; both mandate notes | accepted | INC@HEAD:631-636; `7afe058`; `678434-shopify-nuovo-ordine.md` HEAD:20; `05f1d92` | E11 restated (overturns "ruled out: `SP_SCANSIA_LOOKUP`"); §3.7 rule 2 adds: no SELECT inside SET / INSERT…SELECT / UPDATE…(SELECT); pins row 678434 v265; T31 "met 2026-10-03, branch never run on a real line"; §8/§9 R5 closed; E4's `168.` references re-counted on v265 (a `make-operator` GET) |
| S12 | Citations: INC lines resolve at two revisions; INC@3d21853:597-599 "hides" is retracted at :573-576 (the watchdog alerts on `dlqCount`, then `RIENTRATO`); the trigger is at HEAD:65 (:49-50 is another text); E5's `SHOW CREATE` lines point at a scratchpad file | Fable 6, Opus 10 | accepted | INC@3d21853:573-576, :597-599, :619-620; INC@7ca9bbf:619-620; `678434-…md`@37ece8a:49-50, HEAD:65 | Every INC citation carries its revision; "hides" → "alerts on `dlqCount`, then self-heals"; trigger cited at HEAD:65; E5 cites SP spec §5 only (the live bodies changed 2026-10-03) |
| S13 | `set_online(…, 1)` has no trigger | Fable 10 | accepted | plan:94, :117, :204-209, :298-299; SM `returned-pair-queries.ts:221` | Publish sets `ONLINE = 1` on the pairs whose +1 it applied; Part B, deletion and drafting set 0 |
| S14 | The SKU price is read from the first row: a new return (`PRICE_OUTLET` NULL) can block its whole SKU | Fable 12 | accepted | SS `outlet_service.py:257-258`, `:381-385`; plan:92, :215 | The SKU price = the one non-NULL `PRICE_OUTLET` of its shelf pairs (two values → listed, SKU blocked); publish writes it onto the pairs that lack it |
| S15 | `OUTLET_PRODUCT_ID` lifecycle: a fresh CREATE is a `review` match until the collection re-indexes; an aborted delete leaves the id cleared | Fable 14 | partly (moot while the mirror holds no outlet, E5) | SS `resolvers.py:148-157` | Any pass resolving the SKU to one non-`review` outlet sets the id on the SKU's shelf pairs lacking it; absence = the SPs' title fallback |
| S16 | B-1's figures still count pairs sold after 2026-09-30 08:12Z (pair 23, event 714) | Opus 11 | accepted | plan:73, :316, :326; c7:124; INC@7ca9bbf:619 | Figures carry their instant; "since: 3 shelf pairs sold (events 714, 716, 718), `IN_SCANSIA` 249 at 2026-10-03 08:36Z"; the preview recounts on the day |
| S17 | §6 labels B-1 / B-3 `TRADEOFF` while 2c asks them as `GATE:1` (guard-ask refuses "(Consigliato)" under `TRADEOFF`) | coordinator | accepted | plan:310, :342 | Headers `GATE:1 B-1`, `GATE:1 B-3` |

Mandate notes (both): the INC pin went stale mid-review (`8ee8e46`, `3b23a71`); Opus: the 678434 doc is missing from the
pinned sources — accepted (S11, S12, pins row). S1–S3 and S6 rewrite §3.3 rules 2/6 and §3.4: the coordinator recommends a
targeted round-3 re-review of those rules after the fixes, before 2c. (Superseded: the diff review chose a full round 3.)

Round 3 — two fresh full re-reviews of this file at `6ad125c` (sha `c4067726…`), 2026-10-03 20:16–20:43Z
(`reviews/f7-review-2026-10-03-opus-r3.txt`, 16; `…-fable-r3.txt`, 13; local only), merged into 17 distinct defects (U1–U17),
judged by `scansia-f6` (context `0332b0f6`) on 2026-10-03 ~21:05–21:20Z; cited lines re-read at their pins (`SS` `fdfaf2f`,
`SM` `92f6f90` — STOCK_Manager `origin/main` `aa77acf`, only `scansia-design.md` moved since —, `INC@619ebf2`, 678434 doc
`@0329757`). Tally: 15 accepted, 2 partly, 0 rejected; 1 blocker (U1, introduced by S6), 5 major. **Fixes applied 2026-10-03** by a sub-agent from `reviews/f7-2b-change-list-r3-2026-10-03.txt` (local only). **Diff review done 2026-10-04** (`scansia-f6`, context `6cafe1fb`): the 44 hunks of `42c1e1d..c9fba76`
map to the list's 49 items, nothing else moved; every claim the list introduced re-read at its pin (U11's raw file, the
migration's `:119`, SP spec `:26-27`/`:40-42`/`:47-48`, design `:79`/`:695`, INC `:619-625`, 678434 doc `:20`/`:30-34`/
`:85-86`, runbook `:77`, SERVER's `scansia-sp-2026-09.md` `:39`/`:157`, `c7_summary.json` b5, `step2_decisions.json`).
Settled at review: E5's "first real outlet draft" → the run; rule 6 renews in each **write** transaction (the baseline's
`READ ONLY` snapshot is renewed before it — MySQL refuses an `UPDATE` inside it); **rule 9 needs both** `changeFromQuantity`
(rule 6's Shopify-side fence) **and** no blind re-send of a mutation — U2's "or" let the fence go, and a re-sent mutation
refused as stale would have read `FAILED` after its first send landed; **`changeFromQuantity` exists from Admin API
`2026-01`** (shopify.dev changelog, 2025-12-12) while the app defaults to `2025-07` (`config.py:17`): F7.3 pins `2026-01`
or later and re-tests every mutation; "the `BASELINE` row" → the whole-register one (§3.2, §3.4); B-1 Parity's "held"
wording; B-2's pair-631 example conditional on the NULL rule; E10 names today's compare-at and B-2's recommendation; one
rule-7 citation. Two line citations corrected: CREATE's step 2 is `outlet_service.py:593-600` and step 3 `:602-604` (the
list and U6 said `:585-590` / `:600-606`); "no flow adds a variant" cited `_norm_size` (`:180-181`), now `_match_variant`
`:206-215`. **Round 4
required, full**: U1's fix adds a state (held keys) that crosses §3.3, §3.4, §5 and §6, and rounds 2 and 3 each found a
blocker the previous round's fix introduced.

| # | Defect | Reviewers | Verdict | Line re-read | Change |
|---|---|---|---|---|---|
| U1 | Level leaves a key's snapshot events pending while Shopify holds its units — skipped at confirm, upward adjust barred by `committed > 0` (unstated), multi-outlet, size-absent; once `BASELINE` exists publish adds Σ pending on top (e.g. `AF1-W-PRL-SWA-LRW` 40.5: a second pair arrives between preview and confirm → Promo 3 for 2 pairs); departed outlet-sold pairs (I3: net +1) stay pending in the non-levelled branches | Opus F1 (blocker), Fable F9 | accepted, blocker | plan:201-203, :212-214, :227, :359-362; SS `outlet_service.py:640-643` (`target = base_v + st.delta`); SM design:132-134 | After the baseline every key satisfies clamp(P) + Σ pending = N, or `BASELINE` records it **held**: its snapshot events are excluded from every pass (never an add), it is listed (S11/S13), and only a per-key re-level releases it (same preview → confirm, §3.4 precondition). Every branch stamps the events of pairs no longer on the shelf. Invariant tested on every key. "Level at most once" (§3.3 closing paragraph) → at the baseline and per held key, each a human gesture |
| U2 | The transport re-posts every call, mutations included, on timeout / `RequestException` / 5xx / 429: a relative +Δ whose response was lost lands twice (the absolute set it replaces was retry-safe) | Opus F2 | accepted, major | SS `transport.py:84-89`, `:120-127`, `:140-146` | No Promo mutation re-posted blind: every relative write carries `changeFromQuantity` = the value just read (D6's procedure already uses it, `PROGRAM.md` D6), or mutations are not retried by the transport; a refused or unknown outcome goes to rule 2. Tested |
| U3 | S7's size normalisation publishes `42L` pairs on `42 L` variants, but the SPs match `SIZE = p_taglia` exactly on `variant_title`: an outlet sale finds no pair, runs neither route, the pair stays `IN_SCANSIA` after shipping. `_norm_size` (casefold, comma→dot) has the same reach | Opus F3 | accepted, major — **S7's normalisation reversed** | SM sp-spec:40-42 ("case-, accent- and spelling-exact"), :47-48 (no trim or normalise; `variant_title`); SS `outlet_service.py:174-180` | A pair publishes onto a variant only when `SIZE` equals the variant title exactly (the SPs' rule); a match only after normalisation is surfaced for respelling at the source (STOCK_Manager page or the Shopify variant). E13's twins stay listed; §3.2, §3.3, §4, §6 follow |
| U4 | The lease is not fenced: a holder stalled past expiry resumes and applies groups the taker also applies; "already stamped counts as stamped" hides the double add. After a crash, decrements wait for the expiry: B-3's exposure is the no-crash bound only | Opus F4, Fable F2 | accepted, major | plan:92, :169-177, :388; SS `api/jobs.py:3-4` (throttled fan-out) | A lease generation (fencing token) on every `RECONCILE` row; each DB transaction opens with a guarded renew of its own lease (0 rows → ROLLBACK, stop) — never a SELECT inside a write (§3.7 rule 2); a holder re-checks before each Shopify write and stops once the lease is lost; expiry > the longest single Shopify call with its retries. §4 tests a stalled holder resuming after a take-over. B-3 states exposure after a crash = sweep + expiry |
| U5 | Recovery treats a known outcome as unknown: after a stamp failure it re-applies a −Δ that just landed (a certain extra −1); a +Δ refused with `userErrors` (certainly not applied) gets its events stamped, its pairs lost; "+Δ stamp + surface" leaves a pair unexposed with no in-app path (only a hand edit, §8's second writer) | Opus F5, Fable F5, F6 | accepted, minor | plan:143-151, :289-290, :436; SS `ops.py:61-66` (userErrors raise) | The Shopify outcome is written on the row right after the call (one autocommit); recovery handles only rows with no recorded outcome; a stamp failure retries the stamp first; a refused +Δ closes its row as failed, events stay pending; an open +Δ of unknown outcome holds its key (U1) instead of stamping |
| U6 | CREATE is six mutations; one row written before `productDuplicate` cannot tell where it stopped: after (1)–(2) the orphan carries the source's Promo stock (10 non-outlet variants hold Promo > 0) and a revive puts it on sale; after (6) it is ACTIVE, not DRAFT; "CREATE's rule" mislabels the revive zeroing (CREATE zeroes every Promo variant) | Fable F1 (major), Opus F6 | accepted, major (r2 S1 partly not closed) | SS `outlet_service.py:585-590`, `:600-606` (step 3 zeroes ALL), `:618-628` (5 DENY, 6 ACTIVE + publish); `c7_summary.json` b5 `non_outlet_levels_available_gt0` 10 | An open CREATE row → the orphan is drafted, every Promo variant lowered to 0 relatively, non-Promo stock zeroed / disconnected (step 2's rule), its key held (U1). Label fixed. §4 tests each of the six windows |
| U7 | CONTINUE / untracked variants are only listed; a customisation-only variant with a real SKU and CONTINUE on an ACTIVE outlet sells at any level, unrecorded (title has "Outlet", no pair: neither route); CREATE sets DENY but never tracking, the ACTIVE add sets neither | Fable F3 (major), Opus F11 | accepted, major (R6 partly not closed) | `over69/step2_decisions.json` 15506384421196 (`AR-K-GLIK`, "Solo Personalizzazione" with SKU — re-read; CONTINUE as reported); c7:599; SS `:620-623`, `:664-666`; 678434 doc@0329757:85 | DENY + tracked is a precondition for every variant of an outlet F7 exposes or keeps ACTIVE, customisation-only included; the baseline preview lists them and normalises them under Ale's confirm (customisation-only: DENY, Promo 0); CREATE and revive set tracked. Rule 7 excludes them from counts, not from normalisation |
| U8 | "Parity with 678434's 209/210" is stale: v265 removed 208/209; 220 counts a variant sellable on a SKU and `inventoryQuantity > 0` (all locations) or untracked — the two drafters disagree. A Δ = 0 group (an outlet-line sale, e.g. event 718) never runs the product-total rule | Opus F9, Fable F4 | accepted, minor | 678434 doc@0329757:30-34; plan:128, :130; `PROGRAM.md` D15 (:772, "se un prodotto outlet non ha più taglie disponibili eliminalo o archivialo") | F7's drafting rule stated as its own, its divergence from 220 named; the product-total rule also runs on Δ = 0 groups and on rule 2's re-applies |
| U9 | The stamp's `AND OUTLET_RECONCILED_AT IS NULL` filters an indexed column (the NULL set only grows): on ~682 ids a plan through that index next-key-locks the range every CONSUME event insert lands in — §3.7 rule 3's case | Opus F7 | accepted, minor (sale-path class; the transaction is short) | SM `2026-09-11-sc-returned-pair.sql:119` (`idx_rpe_outlet_reconciled`); INC@619ebf2:252-254 | Stamp by PRIMARY KEY only: `SET OUTLET_RECONCILED_AT = COALESCE(OUTLET_RECONCILED_AT, UTC_TIMESTAMP(3)) WHERE ID IN (…)`, batched; "already stamped" from the re-read; EXPLAIN-tested beside CONSUME on staging |
| U10 | Both SPs read `online_products`; F7.1 does not create it in `racoon_staging`: the "beside CONSUME" test cannot run | Opus F8 | accepted, minor (S10 not fully closed) | SM sp-spec:26-27 | F7.1 also creates `online_products` (live definition and collations, fixture rows incl. an outlet title) |
| U11 | Compare-at mapped to `PRICE_HIGH` only; for the 9 at 0.00 the design records a claim ("not discounted"), the full price being `PRICE`; the compare-at is read from the first row | Opus F10 | partly — a customer-visible price stays Ale's (B-2), now with a recommended rule | SM design:79 (`PRICE_HIGH` / `PRICE` = Compare_AT_Price / Price), :695 (NULL ≠ 0.00, never normalise); SS `outlet_service.py:383-385` | B-2 recommends compare-at = `PRICE_HIGH` if > 0, else `PRICE` for 0.00; NULL rows Ale's rule; one compare-at per SKU (as S14). `PRICE` on the 9 is not measured — measure before 2c |
| U12 | E5 evidences "the live SPs carry §5" by LOOKUP's text only (206 reads 168 = CONSUME); the mirror held 99 `%- outlet%` rows on 2026-09-12, 0 on 10-01, cause unrecorded. The branch's oracle: 206→210 sits on route 1, the Scansia branch for any line, filtered `remaining = 0` and an id — a full-price sale of the last pair is T6's trigger; a run that drafts nothing is still a run | Opus F12, Fable F10 | accepted, minor | `IT/SERVER/_deliverables/scansia-sp-2026-09.md`@7afe058:39 (99 rows), :157 (COALESCE ×3); 678434 doc@0329757:85-86; runbook:77 | E5 cites CONSUME's record and the 99 → 0 change; oracle = the first scansia sale leaving `remaining` 0 with a mirrored id — the run, not the draft; §7 F7.4, §8 follow |
| U13 | S13's alarm ("ACTIVE real-size Promo > shelf") fires on every full-price sale until its decrement — "clean for a week" unreachable under window or manual B-3; non-Promo stock on an outlet product unseen | Opus F13 | accepted, minor (S9's other half) | plan:334-337, :420 | Alarm per non-held key on clamp(Promo) + Σ pending Δ > N (below N: listed), plus any non-Promo `available` on an outlet product |
| U14 | "Today's flows" decide exactly-once by comparing live with `frozen_pre` (level inference) and set absolutely; §4 keeps their tests as the parity oracle | Fable F7 | accepted, minor | SS `outlet_service.py:640-650` | Δ > 0 writes relative with compare-and-set, no level precondition; the `frozen_pre` branches and their tests are retired, not ported; §4 step 2's oracle excludes them |
| U15 | `ONLINE = 1` only by the publish: pairs Level (a) exposes read 0 on STOCK_Manager's page; Part B lists discrepancies "for the D6 check" though go-live follows D6; S3/S12 exclude F7 writes "through the audit table" but no audit row is specified for price, product id, online | Opus F14, Fable F8 | accepted, minor | plan:95, :221-226, :329-333, :406-407 | The baseline sets `ONLINE` from its result (1 on pairs of keys levelled on an ACTIVE outlet with the size, else 0); one audit row per F7 register write; "the D6 check" → "the operators" |
| U16 | "41 + 99 wait for the normal publish": 9 of the 99 have no source product; Parity's "unless no hand restock" vs D6's procedure, which restocks every found pair before Ale answers B-1 | Opus F15, Fable F12 | accepted, minor | `c7_summary.json` `bucket3_sub.SKU_not_in_catalogue` 9; `PROGRAM.md` D6 ("put back … **and** registered") | "41 + 90 wait; 9 have no source product"; Parity's cost certain per found pair, or Parity stamps the events D6 registers on the 69 keys |
| U17 | Wording / citations: §3.6 "the bodies changed" (only LOOKUP's did); E11 cites INC:20 for the stall and :619-620 for event 714 / pair 23; §10's "targeted" vs Status "full" | Fable F11, F13; Opus F16 | F11, F16 accepted; F13 partly — INC@619ebf2:20 does carry the stall ("`sequential: true` … halts **all** order intake"); pair 23 is at :625 | INC@619ebf2:18-20, :624-625; plan:265-266, :5, :549-550 | "The LOOKUP body changed"; E11 cites `:619-625`; §10's note superseded (above) |

Mandate notes: both — the mandate's "the 2026-09-24 holder as INC@619ebf2 and `7afe058` now state it" was wrong (both name
the 10-01 holder only; 09-24 "fits … not re-measurable"); E11 already says it right. Opus — `PROGRAM.md` said the seven
SKU-carrying customisation variants were not re-counted; both reviewers count 7 (c7:428, 579, 599, 738, 758, 778, 841): the
plan stands. Opus — the plan cites STOCK_Manager paths beyond the three pinned (the migration, `gate/scansia/register/route.ts`,
`returned-pairs/route.ts`), which resolve at `92f6f90`: the pins row says "every STOCK_Manager path at `92f6f90`".

Round 4 — two fresh full re-reviews of this file at `3eb99af` (sha `8e0b48c2…`), 2026-10-04 ~07:15–07:50Z
(`reviews/f7-review-2026-10-04-opus-r4.txt`, 11; `…-fable-r4.txt`, 13; local only), merged into 16 distinct defects (V1–V16),
judged by `scansia-f6` (context `46ef6cb0`) on 2026-10-04 ~09:10–09:40Z; cited lines re-read at their pins (`SS` `fdfaf2f`,
`SM` `92f6f90`) and the version claims on shopify.dev (pages named in V2). Tally: 15 accepted, 1 partly (V2: one of its
dates is wrong), 0 rejected; 0 blocker, 6 major (V1–V6). **Fixes applied 2026-10-04** by a sub-agent from `reviews/f7-2b-change-list-r4-2026-10-04.txt` (local only); the four
claims marked "not re-read here" were re-read first (the retention's page, implementing-idempotency, was not among them:
round 5 W4; the list's header: the key's retention is not stated on the pages
read; `2025-10`'s status column reads "Unsupported" beside its 2026-10-16 date); the coordinator diff-reviewed it 2026-10-04 (context `9951e31b`): the 40 hunks of `94b563d` map to items 0a–14a, nothing else
moved (1b skipped rightly: E12 states the CA fact `:38-39` carries); every code, CSV, design-doc and shopify.dev claim the
list introduced re-read at its pin (`2026-07` and thread 28236 not re-read again). Corrected: **the write-ahead recorded
the `available` read although 7a moves the live read after it** — the read is now written with `SENT_AT`, the local
deadline counts from that renew, rule 6's old "renews before each Shopify write" folded in; **E15/F7.3: `2026-10` drops
`ITEM_NOT_STOCKED_AT_LOCATION` from `inventoryAdjustQuantities`, `inventoryMoveQuantities`, `inventorySetOnHandQuantities`,
not from `inventorySetQuantities`** (release notes `2026-10.md`), the only call `_is_not_stocked` guards; the pins row's
"last commit on 2026-10-04" (dates 07-27 to 10-03); the delete predicate's DENY term; the blank-SKU pair cite (`:2271`;
`:2273` its matches); E15's second b5; four punctuation sites. **Round 5 required, full**: V1, V2, V5, V6 and V3/V15 change
rules, a recovery path and the re-level.

| # | Defect | Reviewers | Verdict | Line re-read | Change |
|---|---|---|---|---|---|
| V1 | A CREATE's `userErrors` at steps 2–6 closes its row `FAILED` ("nothing was applied") while the duplicate exists — possibly with inherited stock or some sizes set; recovery handles only rows with no outcome, so the orphan is never drafted or zeroed and the next pass revives it, +Δ on top (or a second CREATE → multi-outlet, V4). "No orphan found → held" closes the row and nothing looks again; "its key" (singular) for a row carrying Δ per size; "found by the recorded title" names a lookup that does not exist (the resolver searches `sku:`; a fresh duplicate is a title-only `review` match) | Opus F2, Fable F1, F6 | accepted, major (U6 partly not closed) | SS `outlet_service.py:585-628` (step 1 `:586`, then 2–6 each raise through `_check_user_errors`, `ops.py:61-70`; step 4 `ops.py:523`); `ops.py:576` (inherited Magazzino stock); `resolvers.py:108`, `:149-157`, `:221-238`; `c7_summary.json` `b5_promo_location_crosscheck.non_outlet_levels_available_gt0` 10 | CREATE records per-step outcomes: the new product id written on its row right after `productDuplicate` answers, then the last step reached. A CREATE that fails after step 1 (`userErrors` or unknown) takes the open-CREATE path — the orphan (by its recorded id) drafted, every Promo variant lowered to 0 relatively, non-Promo zeroed / disconnected, **every key of its Δ-per-size list** held. A crash before the id is recorded: the row stays open ("orphan unknown") and each recovery searches the SKU's title-only `review` matches for the recorded title until found or closed by a person. §4 adds `userErrors` at each of the six steps |
| V2 | The version rule is a lower bound ("`2026-01` or later") while the versions it admits change the contract of F7's own calls: `@idempotent` optional at `2026-01`, **mandatory from `2026-04`** on `inventorySetQuantities`, `inventoryAdjustQuantities`, `inventoryActivate` (runtime error without it); `2026-04` removes `compareQuantity` / `ignoreCompareQuantity` (sent at `ops.py:500`), requires `changeFromQuantity` (explicit `null` to opt out) and stops `inventoryActivate` defaulting omitted quantities to 0 (the app passes `null`s and relies on it); `2026-10` removes `ITEM_NOT_STOCKED_AT_LOCATION`, which `_is_not_stocked` keys on. Rule 2's "an idempotency key, if the pinned version offers one … the lane checks" has an answer: it does, and an event-derived key would fence V6's cross-holder double add. No renewal rule: an expired pin falls forward silently. E15's "served as `2025-10`" ends 2026-10-16 15:00 UTC | Opus F1, Fable F3, F11 | **partly** — accepted, major; Fable's dates rejected: `2025-10` is supported until **2026-10-16 15:00 UTC** and `2026-01` until **2027-01-16 15:00 UTC** (Opus right), so E15 holds today and goes stale in 12 days | shopify.dev/docs/api/usage/versioning (support table; "falls forward and responds using the oldest accessible stable version"); shopify.dev/changelog/making-idempotency-mandatory-for-inventory-adjustments-and-refund-mutations (2025-12-12, `2026-04`, the list incl. `inventoryActivate`); shopify.dev/changelog/release-notes/2026-04.md (changeFromQuantity, removal, `inventoryActivate` default); shopify.dev/docs/api/admin-graphql/2026-01/mutations/inventorySetQuantities ("optional … As of 2026-04 … required"); shopify.dev/changelog/release-notes/2026-10.md (`ITEM_NOT_STOCKED_AT_LOCATION` removed); SS `ops.py:406-445`, `:496-501`. **Not re-read here**: Opus's implementing-idempotency page (24 h key retention; same key returns the original result), Fable's `2026-07` `ruleSet` → `Collection.sources` | F7.3 pins **one named version** (recommended: the newest stable on the build day — `2026-10` today, supported to 2027-10-16), and the lane lists every query and mutation the app sends against that schema (incl. `products(query: "sku:…")` + `inCollection`, `node`, `inventoryLevel`, `publications`, `collections{ruleSet}`, `productUpdate(input:)`). E15 / rule 3 / rule 9 / §8 state the pin's breaking changes. **Rules 2 and 9 redesigned on a deterministic idempotency key** derived from the write's content (event ids + variant + Δ — never the `RECONCILE` row id, which differs between holders): recovery of an open inventory write younger than the key's retention re-sends the identical mutation with the same key and `changeFromQuantity` (landed → the original result, `APPLIED`; not landed → applies, or a stale refusal → `FAILED`, events pending); older → today's rule (−Δ re-apply bounded, +Δ held). Non-inventory mutations (`productDuplicate`, status, publish) keep the hold path. The watch gains the pin's support end (S11 or §5) |
| V3 | Parity "defers the levelling key by key, it does not avoid it" is false for DRAFT-only and never-published keys: Parity stamps their import events, and re-level branches (b)/(c) restore N only through pending events — after Parity 0 + 0 ≠ N and the key stays held for ever: 41 + 99 pairs (9 with no source product) unexposable through the app. A GATE:1 option mis-costed | Fable F2 | accepted, major | plan:240-242, :402-412, :416-420, :132 | One rule closes V3 and V15: a **per-key re-level whose shelf pairs have no pending +Δ** (stamped by Parity, or after a delete) reaches N through the revive / create flow itself — its `RECONCILE` row records the shelf pair ids and N, FIX4 applies, rule 2 governs it (§3.3's "level-setting per held key" already allows it). B-1 Parity states the cost: every DRAFT-only or no-outlet key needs that re-level (41 + 99 pairs, 9 without a source product) |
| V4 | A multi-outlet SKU's −Δ "stays pending and surfaced" for ever (a full-price sale leaves the outlet unit on sale — "a −Δ at least once" broken), and the plan removes today's way out: the stored product id picks one outlet among several, but `OUTLET_PRODUCT_ID` is set only on exactly one outlet. `GLP-W-LEOGD-33PCGD-DIR` 36.5 sits on two products; at D6 a found pair puts one back ACTIVE with a unit | Opus F3 | accepted, major | SS `outlet_service.py:393-403` (MATCH step 1: stored id among several, else `multi_outlet`); plan:94, :130; c7:258 (as the plan cites); `PROGRAM.md` D6 | A clean multi-outlet resolution applies −Δ as a lowering only: on the outlet product(s) holding `available` > 0 for the exact size, `min(|Δ|, Σ available)`, ACTIVE first, each relative with `changeFromQuantity`; never an add on a multi-outlet key (held, as today). Tested |
| V5 | A held key keeps whatever Promo it holds: the hold writes nothing, a revive triggered by another size lowers only real-size variants **with no shelf pair**, the quarantine checks non-Promo stock only, S13 does not alarm on held keys — an orphan's inherited unit or an excess found at confirm goes on sale at the next activation | Opus F4 | accepted, major (U1's hold, introduced) | SS `outlet_service.py:687-691`, `:700-701`; plan:132, :253, :391-394 | A held key is never raised but **is lowered**: where clamp(P) + Σ pending > N at the re-read, lowered relatively by the excess; a revive lowers every held key's real-size variant to 0 relatively (its units return only through the re-level); S13 alarms on a held key whose clamp(P) exceeds N |
| V6 | The fence has a gap between the renew and the Shopify call: the write-ahead `INSERT` and the outcome write are single autocommit statements outside any renew; `LEASE_GEN` is written but checked nowhere. A holder stalled after its renew resumes after a take-over, inserts its row, reads the level **after** the taker's write (rule 8), sends +Δ with a matching `changeFromQuantity` → double add, silent ("already stamped counts as stamped"). Also a dead-generation `OPEN` row of a never-sent write holds a key needlessly. "Each DB write transaction opens with a guarded renew" is false as written | Opus F5 (major), Fable F4 (minor) | accepted, major | plan:92, :143-145, :182-195, :320-324 | Renew + write-ahead `INSERT` in **one** transaction; the live read after it; the send only before a local deadline counted from the renew (expiry − margin), else renew again; the V2 idempotency key fences the residual at Shopify; rule 6 names exactly which writes are fenced; a `RECONCILE` row records "sent" before the call, and recovery closes a dead-generation row never sent without holding. §4 tests a stall right after a renew |
| V7 | STOCK_Manager's page cannot "respell" a pair: no field edit; the path is X8 cancel (admin / manager, «Inserita per errore») + B2 re-insert. The old-spelling key is left with N 0 and a −1 that §3.3 keeps "pending, never stamped" (no exact variant), its size-absent hold never released → S11 alarms for ever, Exit unreachable — for the plan's own remedy for the 3 twins. S11 does not say whether held events count | Opus F6, Fable F10 | accepted, minor | SM design:512-513 (B2 POST; `PATCH` actions arrive / not_resellable / cancel / restore only), :609-613 (X8, its reasons); c7:88-90 | "Respelled at the source" → X8 cancel «Inserita per errore» + B2 re-insert (STOCK_Manager's page; or the Shopify variant renamed). A key with N 0 and no variant titled exactly `SIZE`: its group is stamped with its held events and the hold released (a per-key `BASELINE` row, reason). S11 defined per group, held events excluded (held keys counted apart) |
| V8 | Rule 4 "a negative `available` is never raised" vs rule 7's "customisation-only: DENY, Promo 0, relatively" (2 ACTIVE blank-SKU variants at −2) and Level (a)'s "N − live `available`" on a negative real-size variant | Opus F7, Fable F5 | accepted, minor | plan:175-177, :202-203, :259-260, :416; `c7_summary.json` b5 (as cited, E15) | Rule 4 scoped to −Δ groups; customisation-only normalisation = DENY + tracked, a positive Promo lowered to 0, a negative left (DENY makes it unsellable); Level (a) sets a real-size variant to N from any level, stated |
| V9 | B-1's accounting: the unexposed shelf pairs are **167**, not 166 — the blank-SKU pair (SIZE «Solo Personalizzazione») has no rule, its +1 pending for ever, its resolution by SKU `''` undefined; the `42 L` twin unit (N 0, on a DRAFT) is outside "−4 stale DRAFT units" in both options | Opus F8 | accepted, minor | `c7_summary.json` buckets: 82 + 64 + 99 + 6 + 1 (`z_blank_SKU_register_side`) = 252 → 252 − 82 − 3 = 167; c7:421 (`AR-K-33PC` `42 L`, N 0, `DRAFT`, 15506338513228) | The blank-SKU pair: excluded from every pass and listed (its key held, reason "blank SKU"); 167 in both results; the twin's `42 L` unit lowered to 0 relatively with the four (−5), its `42L` pair's key held until respelled (V7) |
| V10 | B-3's "≤ ~15 min" ignores contention: a publish, re-level or baseline holds the lease for the whole pass (the first publish carries CREATEs for 84 SKUs behind a 0.7 s throttle), and the single-slot `JobStore` answers 409 while any job runs; the plan does not say the decrement endpoint bypasses it | Opus F9 | accepted, minor | SS `api/jobs.py:7-10` (single slot, 409); `transport.py:39` (0.7 s); `c7_summary.json` `bucket3_sub.SKU_on_non_outlet_product` 84 | The decrement endpoint runs outside the `JobStore`; a publish / re-level releases the lease between SKUs so a decrement can take it; B-3 and S11's bound state the contention term (one SKU's flow) |
| V11 | The precondition — a `make-operator` GET of `/dlqs` "right before the preview" — is an agent step the app can neither run nor verify (no Make credential), yet binds every re-level and, read literally ("any Promo write"), the 15-min decrement job | Opus F10, Fable F9 | accepted, minor | plan:240-242, :248-251; SS `config.py:48-53` (as reported; `:51` re-read in round 1) | Scoped to the baseline and re-levels (never the decrement); run by the person confirming (the coordinator through `make-operator`), its instant and `dlqCount` typed into the confirm and kept in the `BASELINE` row |
| V12 | Citations: `:618-628` for "after step 6 it is ACTIVE" (step 5 `:619-624`, step 6 `:626-628`); "`c7_summary.json` b5" (the key is `b5_promo_location_crosscheck`); runbook, `register-checks.md`, `shared-database.md`, `deploy-inventory.md`, the 678443 doc cited by line with no revision | Opus F11 | accepted, minor | SS `outlet_service.py:619-628` (re-read); `c7_summary.json` (re-read) | Cites corrected; those five files pinned at their last commit on the change day (pins table) |
| V13 | The revive's tail (DENY + tracked bulk update, ACTIVE, publish) has no `RECONCILE` row and, under rule 9, no re-send: adds landed and stamped, activation not → a DRAFT outlet holding the right units for stamped pairs, nothing re-triggers it, S13 sees P + 0 = N — fewer, not surfaced | Fable F7 | accepted, minor | SS `outlet_service.py:704-711` | The revive's activation recorded on its row (a step outcome, as V1); an unfinished tail is re-run by recovery (status / publish are idempotent by state); S13 alarms on a DRAFT or unpublished outlet holding real-size Promo > 0 with no open row |
| V14 | Under Level two writers set `ONLINE` in one run with different rules: Part A from its result, Part B still reading as always running and demoting | Fable F8 | accepted, minor | plan:255-256, :263-268 | Part B's `ONLINE` writes run under Parity only; under Level, Part A sets `ONLINE` and Part B only lists discrepancies |
| V15 | Nothing refuses deleting an outlet whose SKU still has `IN_SCANSIA` pairs; their events stamped, the publish never recreates it, S13 lists "below N" without alarm | Fable F12 | accepted, minor | plan:282-284; the delete predicate (Promo 0 on every variant) as reported — **not re-read here** | The delete preview lists the SKU's `IN_SCANSIA` pairs; confirm holds their keys (reason "outlet deleted"), released by V3's re-level |
| V16 | B-3's job "never creates, activates or adds" yet the Δ < 0 / Δ = 0 rows draft inside the decrement pass | Fable F13 | accepted, minor | plan:129, :131, :454-455 | "It lowers and drafts; it never creates, activates or adds" |

Mandate notes: both — round 4's mandate placed the version requirement in "§2, §4, §7"; §4 carries none (it is in E15,
rule 9, F7.3, §8) — the coordinator's error. Opus — the STOCK_Manager diff named `prisma/migrations`, absent at `92f6f90`;
re-run over `prisma/manual-migrations`: empty (the conclusion held, the check did not cover the path). Fable — the version
dates: see V2.

Round 5 — two fresh full re-reviews of this file at `6fe8c16` (sha `fbd85285…`), 2026-10-04 ~11:58–12:25Z
(`reviews/f7-review-2026-10-04-opus-r5.txt`, 17 = O1–O17; `…-fable-r5.txt`, 11 = F1–F11; local only), merged into 18
distinct defects (W1–W18), judged by `scansia-f6` (context `6823a9d5`) on 2026-10-04 ~18:05–18:40Z. Re-read: `SS`
`fdfaf2f` (`outlet_service.py:760-761`, `:789`, `:408`, `:594-604`, `:700-701`; `ops.py:415`, `:460-467`, `:513-522`;
`resolvers.py:157`); `c7_summary.json` keys; shopify.dev, the pages' `.md` by curl: implementing-idempotency (`:56`,
`:64`, `:79`, `:84`, `:252`, `:265`, `:307`), release notes `2026-10.md:44`, `2026-04.md:112`, versioning (`:68`,
`:72`, `:79`, `:87`). Tally: 17 accepted, 1 partly (W15's O14 half), 0 rejected; 0 blocker, 6 major (W1, W2, W3, W5,
W6, W12; W2's severity — Opus major, Fable minor — settled major: an add applied twice). The fixes change rules,
recovery and storage → **round 6 (full) after they are applied and diff-reviewed**. Change list:
`reviews/f7-2b-change-list-r5-2026-10-04.txt` (local only; items 1–19). **Applied** 2026-10-04 ~18:13–18:33Z by a
`general-purpose` opus sub-agent (Edit only; sha `a5bc183d…` → `a65ae998…`, 101+/64−, 35 hunks); **diff-reviewed** by
`scansia-f6` (context `d5475ccc`) ~18:33–18:40Z: the 35 hunks map to items 1–19, nothing else moved; every claim the list
introduced re-read at its pin — the pages above by curl of their `.md` (plus `2026-04.md:78`, `includeInactive`), thread
28236 by its raw text (post #5, Kellan-Shopify 2026-01-13: duplicates "will fail"), `c7_join_sku_size.csv:258`, the
GraphQL documents of `ops.py` / `resolvers.py` at `fdfaf2f` (17 root fields, all now in F7.3); proposals read as
proposals. **Corrected in the review** (text the list left stale): rule 6 "derive the same key"; the −Δ recovery bullet
(a new row, `SUPERSEDED`); the orphan's re-level pointer (§3.4, not B-1 (b), which describes the one-time baseline);
"F7.3 pins" ×4 → pinned in F7.2, verified in F7.3 (the F7.2 row names it); §8 "support end"; F7.3's "take the retention
from shopify.dev's text"; §4 "CONSUME never waiting"; E15's sources (`b5`, line ranges); item 7's "KIND step" →
`ACTION`; item 18's two parentheticals merged; B-1's multi-outlet hold points to §8's exit (item 13's "§3.4 :482" was
§6). **One wrong fact of the judgement**: W17's "`draft_parked` is not a key" (row corrected). **For round 6**: how an
`APPLIED` row closes when its split group has a `FAILED` sibling (rule 2, item 8).

| # | Defect | Reviewers | Verdict | Line re-read | Change (list item) |
|---|---|---|---|---|---|
| W1 | The lease released between SKUs lets a decrement stamp a group the resumed publish then adds (and a baseline / re-level level from a stale N) | O2, F1 | accepted, major | `SS outlet_service.py:760-761` (actions built once), `:789` (verify = gid + status, `:408`) | 1: re-derive groups / re-read the key after every re-take |
| W2 | `IDEMPOTENCY_CONCURRENT_REQUEST` / `KEY_PARAMETER_MISMATCH` arrive as `userErrors` and must not close `FAILED` | O1, F4 | accepted, major | implementing-idempotency `:79`, `:252`, `:265`, `:307`; `ops.py:415` (no `code`) | 2 |
| W3 | The content-derived key collides (`HELD` uncounted, same-N re-levels, no step / location); no key for event-less writes | O4, F7 | accepted, major | implementing-idempotency `:64`, `:265`; `outlet_service.py:594-604` | 3: a per-row nonce |
| W4 | The 24 h retention is stated; the round-4 intro overstated its re-read | O1, F5 | accepted | implementing-idempotency `:56`, `:84` | 4, 18 |
| W5 | `2026-10` drops `ITEM_NOT_STOCKED_AT_LOCATION` from `InventorySetQuantitiesUserErrorCode` too — round 4's diff-review correction was wrong (r4 Fable F11 was right) | O3, F3 | accepted, major | `2026-10.md:44`; `ops.py:460-467`, `:513-522` | 5: explicit activation |
| W6 | §3.6's columns and `UPDATE` grant cannot hold rules 2 / 6's writes | O5, F2 | accepted, major | plan `:336-352` vs `:143-147`, `:214-219` | 6 |
| W7 | A revive's tail has no open row; the decrement's recovery would activate | O13, F2 | accepted | plan `:133`, `:518-519` | 7 |
| W8 | A multi-outlet −Δ stamped at its first `APPLIED` row loses a `FAILED` share | O15 | accepted | plan `:131`, `:149-151` | 8 |
| W9 | The held-key lowering has no trigger (confirm, quiet keys, a stalled +1) | O10, O16, F9 | accepted | plan `:92`, `:126`, `:294-295` | 9 |
| W10 | The "orphan unknown" search, limited to `review` matches, cannot succeed after re-index | O7 | accepted | `resolvers.py:157` | 10 |
| W11 | Pricing / delete / normalisation hold no lease; the delete's hold has no storage | O11 | accepted | plan `:98`, `:213-217`, `:328-329` | 11 |
| W12 | A re-level of a mixed key (some pairs pending, some stamped) restores only Σ pending | O6 | accepted, major | `outlet_service.py:700-701`; plan `:278-282` | 12 |
| W13 | A multi-outlet key has no stated exit | F8 | accepted | plan `:282`, `:482`, `:571` | 13 |
| W14 | F7.3's call list omits `productVariantsBulkUpdate` and others; the pin is chosen after the build | O8, F6 | accepted | `2026-04.md:112` | 14 |
| W15 | "Supported until" is the "Accessible until" column; S14 has no mechanism | O14, F10 | O14 partly (its "since 10-01 served as `2026-01`" is wrong: `2025-10` is accessible until 10-16, versioning `:72`, `:79`); F10 accepted | versioning `:68`, `:72`, `:79`, `:87` | 15 |
| W16 | Renew-first transactions hold a pair lock across a round trip; `wait_timeout` unnumbered; "CONSUME never waits" overstated | O12 | accepted | plan `:362`, `:368-374`, `:383-384` | 16 |
| W17 | Under Parity S13 fires from day 1: Exit unreachable until every stale-DRAFT key is re-levelled | O17, F11 | accepted (F11's figures are real keys — `c7_summary.json` `c7.draft_parked.keys_with_N_gt0` 9, `b5.draft_products_with_promo_available_gt0` 12 — read 2026-09-30 08:16Z, before D15: not used as today's count; the judgement's "not a key" was wrong, corrected in the diff review) | plan `:449-450`, `:468-469`, `:556` | 17 |
| W18 | E15's labels sit under `c7_summary.json` `b5`, not `b5_promo_location_crosscheck` | O9 | accepted | `c7_summary.json` keys | 18 |

Mandate notes: both — STOCK_Manager moved to `cfca94d` (PR #24) minutes after the 11:55Z check; the four pinned paths
unchanged (`92f6f90..cfca94d`: `docs/scansia-design.md` only); §4 again named as carrying the version pin — round 4's
note repeated (the coordinator's error; round 6's mandate drops it). Opus — the re-apply got the same key, not a new
one (moot after W3).

Round 6 — two fresh full re-reviews of this file at `232a69e` (sha `c452a622…`), 2026-10-04 ~23:02–23:28Z
(`reviews/f7-review-2026-10-05-opus-r6.txt`, 15 = O1–O15; `…-fable-r6.txt`, 10 = F1–F10; local only), merged into 17
distinct defects (Y1–Y17; "X" is the design's transition prefix), judged by `scansia-f6` (context `d8ef0be8`) on
2026-10-05 ~08:40–09:05Z. Every cited plan line re-read on this file; re-read at their pins: `SS` `fdfaf2f`
(`outlet_service.py:405-410`, `:786-792`; `ops.py:428-437`; `resolvers.py:222-236`; `delete_service.py:25-37`);
`c7_join_sku_size.csv:85-86`, `:94`, `:108-109`, `:111`, `:121`, `:130`, `:132`, `:258`, `:1715-1722`; `PROGRAM.md`
@`cfb1b15`:42, @`573b4df`:37; shopify.dev, the pages' `.md` by curl: `2026-10` `mutations/inventoryActivate` (`:23`,
`:59`, `:115-117`), `objects/UserError` (`:22-36`: `field`, `message` only), `enums/InventorySetQuantitiesUserErrorCode`
(`:25`, `:37-47`), `enums/InventoryAdjustQuantitiesUserErrorCode` (`:33-43`), `objects/InventoryLevel` (`:70`
`isActive`), implementing-idempotency (`:56`, `:64`, `:66`, `:77-80`, `:285`, `:307`, `:329`), release notes
`2026-04.md` (`:78`, `:90`); dev.mysql.com `json.html` ("MySQL also sorts the keys of a JSON object"). Tally: 17
accepted (Y8: Fable F6's "cannot satisfy the predicate" not shown — the other outlet's sizes read 0 at 2026-09-30,
c7:1715-1722; the product-wide predicate vs a per-key exit stands), 0 rejected; 0 blocker, 5 major (Y1, Y3, Y4, Y5, Y6).
Y2's severity (Opus minor, Fable major) settled **minor**: the document fails validation at the first test, and Y11's
"activate only an inactive level, 0/0" removes the harm of a misread activation. The fixes change recovery, the
outcome map, `held_keys()`, storage and B-1 → **round 7 (full) after they are applied and diff-reviewed**. Change list:
`reviews/f7-2b-change-list-r6-2026-10-05.txt` (local; items 1–52 ↔ Y1–Y17), applied 2026-10-05 by a `general-purpose`
opus sub-agent; **diff review done 2026-10-05 ~10:08–10:16Z** (context `d3fee497`): the plan, whitespace-collapsed,
equals `95823c6` + the 52 items exactly (script), its 43 hunks ↔ items, nothing else moved; every claim the list
introduced re-read at its pin (by curl: implementing-idempotency `:56/:64/:66/:77-80/:84/:285/:307/:309/:329`, both
`UserErrorCode` enums, `inventoryActivate.md`, `UserError.md`, `InventoryLevel.md:70`, `2026-04.md:78/:90`, MySQL
`json.html`; `PROGRAM.md`@`cfb1b15`:42-43, @`573b4df`:37; STOCK_Manager `06390aa` (now `bf9b71d`, the four paths
unchanged); Sync-Scansia `fdfaf2f` `outlet_service.py:405-410/:786-792`, `delete_service.py:25-37`;
`c7_join_sku_size.csv` the nine rows, `:258`, `:1715-1722`). Corrected: E15's `inventoryActivate.md:22-23` → `:21-23`;
the applier's seven flags (rule 2's `userErrors` → `FAILED` narrowed to adjust / set; §3.5's exit sentence re-ordered;
§4 item 4's `CARRIED` / waiting case and its close-then-stamp order; §3.4's N-0 release writes the DB, not Shopify;
S13's causes; a blank line before **Idempotency key.**), its two GREP hits (§4, §6 "Mine": the N-0 release), two
orphan words re-joined.

| # | Defect | Reviewers | Verdict | Line re-read | Change (to list) |
|---|---|---|---|---|---|
| Y1 | Recovery's `APPLIED` bullet (:164) stamps a split group whose sibling is `FAILED` (:155-157): its −Δ share is lost; the same for an activation row `APPLIED` beside a `FAILED` +Δ; §3.6 has no group column | O3, F1 | accepted, major | plan `:131`, `:155-158`, `:164`, `:360-368`; `:436` (test without the sibling condition) | a group id on `RECONCILE`; an `APPLIED` row of a group with a `FAILED` sibling closes without a stamp, carrying its applied share; the re-apply bounded by the shares applied |
| Y2 | `inventoryActivate` returns `[UserError!]!` (`field`, `message`): "every inventory mutation selects `code`" fails validation; its CONCURRENT / MISMATCH cannot be read; `inventoryDeactivate` likewise | O7, F2 | accepted, minor (settled) | `inventoryActivate.md:115-117`; `UserError.md:22-36`; plan `:150-151` | `code` only on the adjust / set payloads; activation errors classified as unknown (re-read the level) |
| Y3 | A recovery re-send's error is read as `FAILED` ("nothing applied") while a duplicate may answer an error: a landed +Δ re-added; `IDEMPOTENCY_PREVIOUS_ATTEMPT_FAILED` unnamed; no margin at 24 h; the cached result is rebuilt from current records | O4, F3 | accepted, major | implementing-idempotency `:64`, `:66`, `:77-80`, `:329`; `InventorySetQuantitiesUserErrorCode.md:45-47`; plan `:148-154`, `:196-203` | on a re-send only `CHANGE_FROM_QUANTITY_STALE`, `IDEMPOTENCY_PREVIOUS_ATTEMPT_FAILED` or input validation close `FAILED`, any other answer unknown (−Δ `OPEN`, +Δ `HELD`); a margin before 24 h; `APPLIED` quantities labelled as the cache's |
| Y4 | A not-closed `RECONCILE` row (CONCURRENT, MISMATCH, a timed-out re-send, an "orphan unknown" CREATE) holds no key: `pending_deltas()` / `held_keys()` read only `HELD` / `HOLD` / baseline holds — the next pass or a re-derive re-groups its events under a new nonce: a double add | O1, O2(a) | accepted, major | plan `:92`, `:98`, `:151-153`, `:179-181`, `:226-227` | every not-closed `RECONCILE` row's events excluded and its keys held until it closes |
| Y5 | A CREATE (and a revive-tail) row has no `SENT_AT`: recovery's "`OPEN`, `SENT_AT` NULL → `FAILED`, never sent" (:162) also matches a sent `productDuplicate` → the orphan revived on inherited stock | O2(b) | accepted, major | plan `:143-146`, `:162-163`, `:177-178`, `:239` ("a CREATE's later steps") | `SENT_AT` fenced before `productDuplicate` and each tail call; the CREATE / tail bullets take precedence |
| Y6 | B-1 Parity misstated: (a) the confirm and every pass lower held keys with P > N, so the five stale DRAFT units are lowered at the baseline — "touch no stock", "stay in place", "S13 fires from day 1" are false; (b) nine DRAFT-only keys with N 1, P_draft 1 satisfy the invariant, are not held, have nothing pending and are never revived | O5 | accepted, major | plan `:126`, `:283`, `:294-297`, `:315-316`, `:494`, `:499-507`; `c7_join_sku_size.csv:85-86`, `:94`, `:108-109`, `:111`, `:121`, `:130`, `:132` (2026-09-30, pre-D15) | restate Parity's result; the invariant scoped to exposable (ACTIVE) Promo, or a re-level allowed on a key not held; the 2c draft's Q1 re-checked |
| Y7 | S13's held-key alarm omits Σ pending: every sale of a held key's pair alarms until its decrement — Exit hostage | O6, F5 | accepted | plan `:126` vs `:476-478`, `:592` | the held-key term = clamp(P) + Σ pending (post-hold) > N |
| Y8 | The multi-outlet exit through the delete flow: §3.5 holds every key of the SKU for a re-level "that recreates", writes `ONLINE = 0` on all its shelf pairs, the predicate is product-wide; the lowering belongs to no lease-taking flow | O11, F6 | accepted (F6's example partly, above) | plan `:234-235`, `:349-353`, `:607`; `delete_service.py:25-37`; c7 `:258`, `:1715-1722` | a stated multi-outlet variant of the delete (every real-size variant of the outlet not kept zeroed; release by re-level on the kept outlet; `ONLINE` untouched), named among the lease-taking flows |
| Y9 | "A re-derived +Δ larger than the confirmed one is held for the next preview" — the formal hold or a deferral; "a SKU absent from it is not written" vs the held-key lowering on every pass; the re-derive drops today's per-SKU gid + status check | O13, F7 | accepted | plan `:126`, `:225-230`, `:297`; `outlet_service.py:405-410`, `:786-792` | "deferred to the next preview" (no hold); the held-key lowering exempted; the re-derive re-reads the outlet's gid + status |
| Y10 | The decrement's recovery re-sends young `OPEN` +Δ / activation rows (only the tail is scoped); the "N 0, size absent" release is a `BASELINE` row with no gesture, against :297-298, :359, :607; §4's tail test lacks the publish-class condition | O10, F10 | accepted | plan `:130`, `:171-172`, `:196-199`, `:297-298`, `:359`, `:446`, `:553`, `:607` | the decrement's recovery leaves +Δ / activation rows to a publish-class holder; the N-0 release named as the one exception; the test conditioned |
| Y11 | Explicit activation's quantities unnamed: on an active level an absolute, unfenced set; no rule for event-less rows older than 24 h; activation not gated on the revive's zeroing rows; `inventoryActivate.md:23` ("default to zero") vs `2026-04.md:90` ("no longer defaults") unlabelled | O12, F4 | accepted | `inventoryActivate.md:23`, `:59`; `2026-04.md:78`, `:90`; `InventoryLevel.md:70`; `ops.py:428-437`; plan `:79`, `:133`, `:199-200`, `:211-214`, `:588` | activate only an absent / inactive level (`includeInactive`, `isActive`), explicit 0/0; F7.3 measures activate on an active level; a recovery rule for event-less rows; activation after the zeroing rows are `APPLIED`; the disagreement labelled in E15 |
| Y12 | Parameters rebuilt from `PAYLOAD` JSON lose their order (MySQL sorts JSON keys); Shopify: "parameter order can affect fingerprinting" → MISMATCH on every re-send | O8 | accepted | implementing-idempotency `:285`; dev.mysql.com `json.html`; plan `:190-191`, `:367` | the exact request body stored as text, or a canonical field order on every send |
| Y13 | The decrement's behaviour on a held lease and the hand-off between SKUs are unstated (a publish re-taking at once can starve it); every take's recovery cost is outside B-3's bound | O9 | accepted | plan `:222-223`, `:245-246`, `:552-557`, `:592-594` | state the hand-off (a waiter flag or a bounded wait) and put the recovery cost into the bound |
| Y14 | The stamp's re-read "after the update" vs rule 1's "register `UPDATE` is the last statement before `COMMIT`" | O14 | accepted | plan `:93`, `:399-400` | `CLIENT_FOUND_ROWS`' matched count replaces the re-read; order: row close, stamp, `COMMIT` |
| Y15 | §4's tests omit round 5's mechanisms (24 h boundary + `SUPERSEDED`, split group with a `FAILED` sibling, mixed-key re-level, `HOLD` rows, multi-outlet exit, N-0 release, the tail under the decrement) | O15 | accepted | plan `:435-460` | add each, and Y1–Y14's |
| Y16 | "A baseline releases the lease between SKUs so the decrement can run" and its §4 test describe a path the reader refuses (no decrement before the whole-register `BASELINE` row, written last) | F8 | accepted | plan `:222-223`, `:305-306`, `:336-337`, `:444` | the interleaving and its test scoped to publish and re-level |
| Y17 | "`PROGRAM.md` eighth watch" / "ninth watch (249)" do not exist at HEAD (Current state is rewritten per reading); the pins row still reads `aa77acf` (STOCK_Manager at `425b91b` since) | F9, O (mandate note) | accepted | `PROGRAM.md`@`cfb1b15`:42, @`573b4df`:37 (figures hold); plan `:16`, `:77`, `:491`, `:494` | cite `PROGRAM.md@<rev>:line`; the pins row re-checked |

Mandate notes: Opus — the version pin is also carried by rule 9, S14 and §8 (round 7's mandate names them); the
pins row (Y17). Fable — no wrong statement; shopify.dev disagrees with itself on `inventoryActivate`'s defaults (Y11)
and the `2026-10` release notes still head "Release candidate" while the versioning table says latest stable (F7.3's
date holds).

Round 7 — two fresh full re-reviews of this file at `82f3dfd` (sha `4b38639b…`), 2026-10-05 ~10:57–11:32Z
(`reviews/f7-review-2026-10-05-opus-r7.txt`, 12 = O1–O12; `…-fable-r7.txt`, 11 = F1–F11; local only), merged into 20
distinct defects (Z1–Z20), judged by `scansia-f6` (context `88b8f9d0`) on 2026-10-05 ~12:09–12:20Z. Every cited plan line
re-read on this file (unchanged since `82f3dfd`); re-read at their pins: `SS` `fdfaf2f` (`app.py:20-26`, `:119-125`;
`api/mutations.py:358-363`; `outlet_service.py:596-601`, `:619-626`, `:680-684`, `:704-711`; `api/delete_outlet.py:195-198`,
`:232-235`, `:340-343`; `delete_service.py:10-17`, `:105-112`; `pricing_service.py:10-14`, `:36-40`; `ops.py:857-866`,
`:952-957`); `SM` `92f6f90` `scansia-design.md:182-187`, `:608-613`; `c7_join_sku_size.csv:124`, `:258`, `:1715-1722`, and
a script over the whole file (SKUs on two or more outlet products: `GLP-W-LEOGD-33PCGD-DIR`, N 0 on every size, and the
blank SKU); `INC`@`619ebf2`:624-626; `PROGRAM.md`@`96a20a7`:35, @`573b4df`:116-117 and at HEAD (no "fifth production
reading" section; the D6 decision bullet at `:782`, the D6 section at `:104`); shopify.dev and community, the raw `.md` by
curl: `2026-10` `enums/InventoryAdjustQuantitiesUserErrorCode` (`:25-27`, `:53-55`, `:61-63`, `:97-99`),
`enums/InventorySetQuantitiesUserErrorCode` (`:49-55`), implementing-idempotency (`:305-331`), thread 28236 (#5,
Kellan-Shopify, 2026-01-13 15:08:02 UTC, `:122-124`), `mutations/productUpdate` (`:115-123`), `objects/Collection`
(`:1012-1019`), versioning (`:99-102`), the changelog posts `compare-and-swap-for-inventory-mutations-with-change-from-quantity.md`
(`:2`, `:12`) and `adding-idempotency-for-inventory-adjustments-and-refund-mutations.md` (`:45-66`), release notes
`2026-04.md` / `2026-07.md` / `2026-10.md` (grep `productUpdate`: `2026-04.md:234` only, the new `identifier` argument).
Tally: 20 accepted (Z8 (a) partly: the letter contradicts itself, the effect is harmless), 0 rejected; 0 blocker,
**4 major** (Z1, Z2, Z3, Z4). Z1's severity (Fable major, Opus minor) settled **major**: the held-key lowering makes a −Δ
whose first send did not land certain to be applied twice — the case :183-184 leaves exact — silently (S13 lists a
key below N, no alarm), and its fix changes a rule. **The fixes change the design** (Z1 the lowering's term and the
in-flight hold, Z2 the revive's gate, Z3 the group write-ahead, Z5 the N-0 release, Z6–Z7 the re-send window): change
list → application → diff review → **round 8 (full)**. Fable F2's example pair (pair 23 = `GL-GLIK-33TS-DIR` 38) is
reported, not re-read (c7:124 and INC:625 are): a SELECT if the example enters the plan. Opus's versioning `:101` quote
("might be removed") is on the page, as `:101`'s second sentence — "Deprecated fields or types are removed in a
subsequent release—for example, something deprecated in `2026-10` might be removed in `2027-01`." (this record had
quoted only the first, "deprecated across all supported stable versions and announced in the developer changelog";
corrected in Round 8, AA15); Z17 stands on `productUpdate.md` and `Collection.md`. Change list:
`reviews/f7-2b-change-list-r7-2026-10-05.txt` (local; items 1–52 ↔ Z1–Z20), applied 2026-10-05 by a `general-purpose`
opus sub-agent (`401b4724…` → `3cfc6a01…` at `3a67f25`); **diff review done 2026-10-05 ~15:27–15:45Z** (context
`c1030544`): the plan, whitespace-collapsed (blank lines kept as a token), equals `1357d13` + the 64 replacements
exactly (script), nothing else moved; every claim the list introduced re-read at its pin (`PROGRAM.md`@`96a20a7`:35,
@`4673b97`:93-95, :100-101, :126-128, :482-485, :519-523, :1063, :1073-1074; Sync-Scansia `fdfaf2f` `app.py:122-123`,
`api/delete_outlet.py:7-9`, `:36-37`, `:177`, `:197`, `:234`, `:300`, `:342`, `api/jobs.py:7-10`,
`outlet_service.py:593-600`; STOCK_Manager `92f6f90` `scansia-design.md:185`, `:612-613`; `c7_join_sku_size.csv:258`,
`:1715-1722`; by curl: the `adding-idempotency-…` changelog `:49-67` (no `inventoryDeactivate`), `compare-and-swap-…`
`:2`, `:12`, thread 28236 #5 `:118-124`, `2026-10` `productUpdate.md:113-123`, `Collection.md:1014-1018`,
`InventoryAdjustQuantitiesUserErrorCode.md:53-55`, `:61-63`, release notes `2026-04.md:112`, `:234`, `2026-07.md:230`,
`2026-10.md` (no `productUpdate`)). Corrected here: rule 2's changelog cite `:47-65` → `:49-67`; the applier's flags —
"Every send of that row" → "of a keyed row"; three "until a per-key re-level" sentences (§3.3's grouping, rule 2's
`OPEN` +Δ, §6 Parity's premise) now name the in-flight hold's end and the N-0 release; its GREP hits — E15's revive
gate (`APPLIED` or `DONE`), E15's and F7.3's `productVariantsBulkUpdate` (F7 sends it no inventory quantity), F7.3's
documents "as F7.2 left it", §4's stamp retry ("a `SUPERSEDED` one aside"), §3.7 rule 1's group write-ahead; "Mine"
no longer opens a line with "+". Kept, each consistent with the new rules: "re-send window" (5 sites: rule 2's
window), "stays `OPEN`" (first-send answers, the CREATE orphan, the transport — not re-sends), rule 6's per-row fence,
"every held key" (§3.4's confirm; the revive waits for its product's open rows), `SENT_AVAILABLE` (item 34 keeps it),
`inventoryDeactivate` in F7.3's list, E15's `ruleSet` quote, the soft-wrapped short lines. The applier's one
deviation (a helper script in its scratchpad) touched no repo file: `3a67f25` changes `f7-plan.md` only. Next: round 8
(full, the last — D16).

| # | Defect | Reviewers | Verdict | Line re-read | Change |
|---|---|---|---|---|---|
| Z1 | Y4's in-flight hold meets the held-key lowering: a not-closed −Δ row holds its key and excludes its events, so the lowering (clamp(P) + Σ pending − N) takes the unit the row has not yet taken; the re-send then answers `CHANGE_FROM_QUANTITY_STALE` → `FAILED` → the −Δ is re-applied (or, unknown, re-applied past the window) → P = N − 1. Also: these holds end without a gesture (contradicting "the one hold released without a human gesture", :130, :350-353, :671, :708), and the revive zeroes an in-flight-held key's variant that no re-level then restores | F1, O5 | accepted, major (severity settled) | plan `:98`, `:126`, `:130`, `:133`, `:183-185`, `:232-238`, `:350-353`, `:671`, `:708` | name the in-flight hold as its own kind, released when its row closes; the lowering adds the Δ of the key's not-closed −Δ rows to Σ pending (+Δ rows not counted); recovery resolves its rows before any lowering in the same run; a revive whose product has a not-closed row is deferred; the "one exception" sentences re-worded; §4: a timed-out −Δ, not landed, then the lowering → P = N |
| Z2 | The revive's tail (ACTIVE + publish — what exposes a unit) is not gated on the zeroing rows; Y11 put "once those zeroing rows are `APPLIED`" on `inventoryActivate`, whose explicit 0 / 0 exposes nothing; a `FAILED` (STALE, `ADJUST_QUANTITIES_FAILED`, `SERVICE_UNAVAILABLE`) or timed-out zeroing then the tail sells a no-pair unit, and nothing lowers it afterwards | O2 | accepted, major | plan `:133`, `:189-190`; `SS` `outlet_service.py:682`, `:708-710`; `InventoryAdjustQuantitiesUserErrorCode.md:25-27`, `:97-99` | the tail, and recovery's re-run of it, require every zeroing row of that revive `APPLIED`; a `FAILED` or `OPEN` zeroing row ends the revive, events pending, surfaced; §4 test |
| Z3 | A group's rows are written ahead one per call and nothing records how many there will be: a crash between two siblings' write-aheads leaves "every row of its group `APPLIED`" true → the stamp retry stamps the whole group (a multi-outlet split: the second share lost, a sold pair's unit left on sale; a +Δ after its activation row: its events stamped unapplied). The activation row is "matched on event ids" (:169-170) yet "event-less" (:204), and "closes … `APPLIED`" (:207) names a not-closed status | O1 | accepted, major | plan `:98`, `:141-143`, `:164-171`, `:180-182`, `:204-208`, `:284-285` | write every row of a group ahead in one renew transaction before its first call (or the planned row count on each row) — a never-sent sibling then closes `FAILED` and the `CARRIED` path runs; state whether an activation row carries event ids; §4: a crash between two siblings' write-aheads |
| Z4 | B-1 Parity mis-costed (V3 / Y6's class): a key whose pair left after the cutover with P ≤ 0 (a full-price `CONSUME` −1, an X8 cancel −1 — "the F2 duplicate / D6 phantom path", D6's «non trovato a scaffale») fails the invariant (0 − 1 ≠ 0) and is held, its −1 never read; the N-0 release needs the size absent; only a re-level with nothing to write releases it. Level covers the class (:604-605); Parity's result omits it | F2 | accepted, major | plan `:347-353`, `:586-603`, `:604-605`; `SM` `scansia-design.md:185`, `:612-613`; c7 `:124`; `INC`@`619ebf2`:625 | closed by Z5's release (any baseline); Parity's result names the class and its count on the day; 2c's Q1 re-checked |
| Z5 | The one gesture-free release needs "N 0 with its size absent": a held key whose last pair leaves with the size present, or a multi-outlet key after its exit, stays held and listed with nothing to expose (V7's size-present half) | F4 | accepted | plan `:130`, `:351-353`; c7 `:258`, `:1715-1722` | release on N 0 and live P 0 after the lowering, no row of the key open and none `HELD` inside the 24 h retention, size present or not (reason "N 0") |
| Z6 | The re-send window is anchored on a `SENT_AT` that rule 2 and rule 6 re-set before every call, re-sends included: a row re-sent every pass never leaves the window (an unknown −Δ is never re-applied as a new row, its key held), a CONCURRENT loop can outlive Shopify's 24 h, and `SENT_AVAILABLE` stops matching the stored `REQUEST` | O3 | accepted | plan `:144-146`, `:217-218`, `:226-228`, `:285-287` (implementing-idempotency `:56`, quoted at :226) | the window counts from the first send (its own column, or `SENT_AT` set once on inventory rows); a re-send rewrites neither `SENT_AT` nor `SENT_AVAILABLE`; its local deadline counts from its own renew |
| Z7 | An unknown-answer −Δ row waits for the window's end — up to ~23 h — with its unit on sale; S11 excludes held events, B-3's bound and Exit's S11 bound do not carry it (the lowering covers it only by Z1's accident) | F3 | accepted (one choice with Z1) | plan `:183-185`, `:235-238`, `:557-560`, `:654-657`, `:693-694` | after one in-window re-send with an unknown answer the −Δ is re-applied at once as a new row (bounded by rule 4, the old row `SUPERSEDED`, an extra −1 errs to fewer and is surfaced) — recommended, B-3's bound being the business requirement; else the 23 h stated in B-3 / Exit and an S11 alarm on a not-closed −Δ row older than one sweep |
| Z8 | The re-send map: (a) `INVALID_INVENTORY_ITEM` / `INVALID_LOCATION` are "could not be found" codes, and a duplicate's not-found means "the original request was successfully completed" — filed `FAILED` while the same sentence files `NOT_FOUND` as unknown; (b) STALE → `FAILED` assumes a landed original's duplicate answers with its cached result, while thread 28236 #5 says duplicates "will fail" and F7.3 only records the shape | O6 | (a) partly — harmless: on such an answer the item or location no longer exists, so no unit can be doubled; (b) accepted | plan `:232-238`, `:241-243`; `InventoryAdjustQuantitiesUserErrorCode.md:53-55`, `:61-63`; `InventorySetQuantitiesUserErrorCode.md:49-55`; implementing-idempotency `:309-329`; thread 28236 `:122-124` | (a) say why `FAILED` is harmless for the two codes; (b) on a recovery re-send of a +Δ row, STALE → `HELD` until F7.3 shows a landed original's duplicate answers with its cached result (a −Δ keeps `FAILED`: it errs to fewer) |
| Z9 | The hand-off bound ("within one SKU's flow") names publish and re-level; pricing, delete, the multi-outlet exit and the normalisation also hold the lease and release nothing; the app's bulk cleanup (`/outlet/cleanup/*`) and `/outlet/deny-normalize` are neither ported (§3.5) nor removed (§3.8) — no "cleanup" or "deny-normalize" in §1–§9 | F9, O8 | accepted | plan `:262-269`, `:282-284`, `:410-414`, `:494-496`, `:654-655`; `SS` `api/delete_outlet.py:197`, `:234`, `:342`; `delete_service.py:12-16`, `:107-111`; `pricing_service.py:12-13`, `:38-39` | port or retire the cleanup and deny-normalize explicitly; every lease holder yields to a waiting decrement between products, or the bound names its exclusions |
| Z10 | The decrement runs "outside the `JobStore`" but every job runs on a single-worker executor: queued there it waits behind the whole publish (the waiter flag only stalls the publish until it expires); inline it blocks the event loop | O9 | accepted | plan `:265-268`, `:651`; `SS` `app.py:23-24`, `:122-123`; `api/mutations.py:361` | the decrement runs on its own worker, not `app.state.executor`; §4 tests the interleave in one process |
| Z11 | B-3's recovery term omits the held-key reads every take makes (:126; under Parity most of the 167 unexposed pairs' keys, a product read each at 0.7 s per call) and names the orphan search, which the decrement's recovery never runs (:209-211) | O7 | accepted (Y13 partly) | plan `:126`, `:209-211`, `:654-657`, `:693-695` | count the held-key reads in the bound (or cap them per take); drop the orphan search from the decrement's term |
| Z12 | A customisation-only variant's Promo > 0 after the baseline (a hand edit, a Shopify-side restock) is watched by nothing — the revive zeroes only real-size variants, S13 reads real-size keys — and sells unrecorded, a revive exposing it | O4 | accepted | plan `:133`, `:303-307`, `:566-573`, `:712` | S13 alarms on any customisation-only Promo > 0 on an outlet; the revive lowers it to 0, relatively, as CREATE's step 3 does |
| Z13 | `inventoryDeactivate` (CREATE step 2, two calls per location; not in the changelog's `@idempotent` list) and `productVariantsBulkUpdate` (CREATE step 5, the tail, rule 7's normalisation) are in neither of rule 2's lists; ":153 their idempotency answers" is void for the deactivate | F7 | accepted | plan `:153`, `:213-217`, `:239`; `SS` `outlet_service.py:599-600`, `:624`, `:708`; `adding-idempotency-…md:47-65` | list both, with their rows; say whether a deactivate shares its set-0 row; :153 re-worded |
| Z14 | Groups have no term for a `SUPERSEDED` member: "every row of the group `APPLIED`" never holds once a −Δ re-apply supersedes one; the successor's (and a remainder's) `GROUP_ID` is unstated | F5 | accepted | plan `:164-171`, `:183-184`, `:221-223` | the group test runs over rows not closed `SUPERSEDED`; a successor inherits the `GROUP_ID`; a remainder after `CARRIED` siblings belongs to the next pass's group |
| Z15 | A never-sent +Δ / CREATE row under the decrement is closed `FAILED` by :176-177 and kept "as it is … its keys held" by :209-211 | F6 | accepted (wording; both DB-only, safe) | plan `:176-177`, `:209-211` | the never-sent close is holder-independent |
| Z16 | `set_online(…, 1)` names the baseline and the publish only: a re-level onto an ACTIVE outlet and a revive's exposure set no `ONLINE = 1`, and Part B lists those pairs for ever | F10 | accepted | plan `:96`, `:357`, `:380-381` | the re-level and the revive set `ONLINE` on the pairs they expose |
| Z17 | Two documents use deprecated surface at the pin — `productUpdate(input:)` (F7's drafting, every status flip) and `Collection.ruleSet` — and `productUpdate`'s is in none of the `2026-04/07/10` notes F7.3 reads | O10 | accepted | `SS` `ops.py:859-865`, `:955`; `productUpdate.md:119-123`; `Collection.md:1014-1018`; `2026-04.md:234` | F7.3 lists each document's deprecations at the pin beside its breaking changes; F7.2 migrates both (`productUpdate`'s `product` argument, `productUpdate.md:115-117`; `Collection.sources`) or records why not |
| Z18 | The only multi-outlet example has no shelf pair: `GLP-W-LEOGD-33PCGD-DIR` 36.5 N 0 (P_active 1, zeroed by D15), every other size N 0; the measurement holds no other SKU on two outlet products but the blank SKU | O11 | accepted | c7 `:258`, `:1715-1722`; the script above; plan `:387-388`, `:422-423`, `:708` | label it "N 0 at 2026-09-30; a multi-outlet hold only if D6 registers its pair" |
| Z19 | `PROGRAM.md` still cited by section with no revision (Y17 partly): the pins row's "fifth production reading" (gone since `044cf47`; `PROGRAM.md`@`96a20a7`:35), "F7 subsection (derived)", "D6" (two texts: the decision bullet vs the D6 section, whose "put back … `changeFromQuantity: 0` … **and** registered" rule 9 and Parity rely on), "## Decisions" | F8, O12 | accepted | plan `:18`, `:77`, `:319`, `:601`, `:623`; `PROGRAM.md`@`96a20a7`:35, @`573b4df`:116-117 | `PROGRAM.md@<rev>:line` at each |
| Z20 | Rule 9 quotes the changelog post's slug as its title | F11 | accepted (the claim holds) | plan `:326-327`; `compare-and-swap-…md:2` ("New changeFromQuantity field to manage inventory"), `:12` (`effectiveApiVersion: 2026-01`) | the title, or the URL |

Mandate notes: both — Racoon-LAB HEAD moved during the review (`31e4428`, `OPEN-HANDOFFS.md` only; then `fb8035f`), the
plan unchanged (since `2f29533` a mandate names the plan by file commit + sha only). Opus — "§3.4 :335-336" is §3.3's "Why
deltas" paragraph (the coordinator's label, wrong); `2025-10` and `2026-01` also lie between the app's default and the pin
(`2025-10`'s release notes answer 404). Fable — dev.mysql.com answered 403 to curl (the JSON key-order quote not re-read
this round); the `2026-10` release notes still head "Release candidate" (F7.3's date holds).

Round 8 — two fresh full re-reviews of this file at `8654a80` (sha `a3e73ed8…`), 2026-10-05 ~15:59–16:41Z
(`reviews/f7-review-2026-10-05-opus-r8.txt`, 11 = O1–O11; `…-fable-r8.txt`, 8 = F1–F8; local only), merged into 16
distinct defects (AA1–AA16), judged by step session `scansia-step-2` (context `98007a86`, spawned by `scansia-f6`, D17) on
2026-10-05 ~16:46–17:00Z. The plan was re-read at the cited ranges (§3.3 whole; E11–E15, §3.1, §3.4–§3.6, §4–§8 at the
lines cited), not whole: the context budget did not hold a full read. Re-read at their pins: `SS` `fdfaf2f` (`ops.py:58-72`,
`:612-640`; `resolvers.py:49-66`, `:104-120`; `delete_service.py:8-40`, `:108-116`, `:340-356`, `:625-634`, `:712-728`;
`app.py:118-146`; `api/jobs.py:1-12`); `SM` `92f6f90` `scansia-sp-spec.md:203-210` (`origin/main` = `main` = `1eaa400`;
`92f6f90..1eaa400` on the four pinned paths = `docs/scansia-design.md` 21+/3− only); 678434 doc @`0329757`:63-67 (still
its last commit); shopify.dev by curl of the raw `.md` (16:50Z): the changelog posts
`draft-order-and-transfer-shipment-inventory-is-moving-from-reserved-to-committed` (`:35`, `:43-57`),
`removal-of-itemnotstockedatlocation-error` (`:12`, `:43-47`), `add-isactive-field-to-inventorylevel` (`:10`, `:41-55`);
`2026-10` `mutations/inventoryActivate` (`:18-23`), `input-objects/ProductVariantsBulkInput` (`:43-59`, `:97-101`),
`InventoryLevelInput` (`:23-33`), `InventoryAdjustmentInput` (`:31`), `InventoryItemInput` (`:71-73`); versioning
(`:99-103`); release notes `2026-04.md:110-112`. Tally: 16 accepted, 0 rejected; 0 blocker, **1 major (AA1)**, 15 minor.
AA1's severity (Opus major) settled **major**: a row with no terminal path holds its key with no release but DB surgery,
and its fix adds rules. Fable F1's ("minor (arguably major)") settled **minor** (AA4): the side is the accepted one
(fewer), S13 lists the key below N, and the fix is a surfacing sentence. **Per D16**: AA1's fix, with AA2's (they share
the rows' identity), gets a targeted re-review — two fresh reviewers, opus + fable, mandate = those fixes' hunks only;
AA3–AA16 close in the coordinator's diff review; no round 9; minors left → the Sync-Scansia build as test cases. Next:
the change list (its own session), its application, the diff review, the targeted re-review, then 2c (Q1 against Y6,
Z4/Z5, AA3, AA9–AA12).
Diff review (D16; three step sessions, 2026-10-05): part 1 (context `0dac9b03`) — `ef17bcf` = `bd1617b` + the list's 55
replacements (`verify_apply.py` `ALL OK`); every claim the list introduced re-read at its pin, all hold. Part 2a
(`ae17c632`) — E15: every F7 read of a Promo level passes `includeInactive: true` (`SS` `fdfaf2f` `ops.py:631` passes
none; "deactivating an inventory level no longer clears its quantities" from `2026-04`, changelog
`add-isactive-field-to-inventorylevel.md:45`). Part 3 (`d7b94b6c`) — the applier's flags: fixed — §4's activation scope
(AA8); "nothing applied" → "by that row" in rule 2's outcome and the re-send window, "possibly applied" recorded on the
row's `OUTCOME` (three sites, §3.6 grants no event column); the re-send window's activation / deactivation exception;
Recovery classes — a decrement-class holder also closes by terminal paths (ii) / (iii), a publish-class holder runs every
bullet except (iv); "Past the window"; `held_keys()`'s "the in-flight hold's event ids"; the cleanup cites the delete
predicate instead of restating it; kept — Exit's two parentheticals (the second carries the conditional terms) and one
short line. GREP hits: none states a replaced rule (rule 4's first line is qualified by its next; §4 tests AA7's case).
Design choices re-checked on the text: AA1–AA5, AA7, AA8, AA12–AA14 and part 2a's (a raise that activates drops an
inactive level's units — fewer, as its alternative) hold. Blank lines 81 → 81, table rows 228 → 228 (243 at any indent).
The targeted re-review's hunks: items 4, 8, 10, 13, 14, 15, 16, 17, 27, 29, 30, with this review's edits inside them.

| # | Defect | Reviewers | Verdict | Line re-read | Change |
|---|---|---|---|---|---|
| AA1 | Rows with no terminal path hold their keys for ever (the in-flight hold ends only when its row closes; the only person-close is the "orphan unknown" CREATE's): a sent `inventoryDeactivate` row with no outcome — no recovery bullet names deactivation rows, the decrement's included; the `isActive` read is the first answer's rule —; a revive-tail or normalise row whose re-run keeps answering `userErrors` (every op raises, `ops.py:61-70`); a delete that leaves its product's open rows (the app's single delete applies no predicate, `delete_service.py:719-723`); an activation row whose level can no longer be read. Held for ever: no add, no N-0 release, every revive of the product deferred | O1 | accepted, **major** | plan `:98`, `:136-142`, `:149`, `:178-181`, `:205-252`, `:262-263`, `:486-490`, `:652`; `SS` `ops.py:61-71`, `delete_service.py:17-20`, `:719-723` | a terminal rule per row kind: a deactivation read off `isActive` in recovery, under every holder; a re-run answering `userErrors`, or a product gone, → `FAILED`, surfaced; every delete (single, bulk, exit) closes its product's open rows, and the single delete's missing predicate is named; an audit-recorded person-close for any row not closed; §4 tests each |
| AA2 | The revive-tail row and the per-product normalise row carry no event ids, keys or product id (§3.6: `PRODUCT_ID` is "a CREATE's duplicate"), yet `held_keys()` and the revive's deferral read open rows: an open tail holds and defers nothing, and a second revive of the product can start under it | F6 | accepted, minor | plan `:98`, `:149`, `:262-263`, `:519-520` | both rows carry the product id and the keys they serve (§3.6's `PRODUCT_ID` widened); the deferral reads the product id; with AA1 |
| AA3 | The ported bulk cleanup keeps the app's predicate (DRAFT; Promo `available` 0 on every variant, `committed` 0, all DENY): once Level (b) has zeroed the DRAFT-only keys' outlets for their revive, a cleanup run before the first publish deletes them — up to 38 keys / 41 pairs (E13, derived) turned from a revive into per-key re-levels that create | O5, F3 | accepted, minor | plan `:486-488`, `:502-507`, `:700`, `:722-725`; `SS` `delete_service.py:12-16`, `:25-37`, `:112-113`, `:345-353`, `:629-630` | a cleanup candidate is never an outlet whose SKU has an `IN_SCANSIA` pair: listed apart, not deletable |
| AA4 | A −Δ closed `FAILED` on `CHANGE_FROM_QUANTITY_STALE` — the in-window re-send, or a re-apply's own first send — hands its events to the next pass, which lowers again: had the original landed, P = N − 1, unsurfaced (the re-apply path surfaces its possible extra −1, `:220`; S13 lists below N, no alarm); under thread 28236 #5 a landed −Δ whose response was lost may take this path | F1 | accepted, minor (severity settled, above) | plan `:215-220`, `:288-291`, `:392-395`, `:673-676` | a `FAILED` close on STALE of a re-send or of a re-apply marks its events "possibly applied once"; their next lowering is surfaced as `:220`'s possible extra −1 |
| AA5 | Z3 writes a group's rows ahead before the only live read, but the group's composition depends on live state: whether a +Δ needs an activation row (E15), over which products a multi-outlet −Δ splits (`:147`), which variants a revive zeroes (`:149`) | F2 | accepted, minor | plan `:79`, `:147`, `:149`, `:157-161`, `:188-192`, `:350-352` | a planning read (the composition) before the write-ahead, rule 8's read (`changeFromQuantity`) after it; the group test counts the rows the planning read produced |
| AA6 | Every row of a group carries the group's event ids, so a stamp list rebuilt from the rows repeats ids: `ID IN (…)` matches fewer rows than the list's length → ROLLBACK every time, the group never closes | O10 | accepted, minor | plan `:93`, `:158`, `:192-199`, `:211` | the stamp list is the distinct union of the group's event ids |
| AA7 | Rule 4 stamps a −Δ "without touching Shopify" at `available` ≤ 0, reading a sale or a hand edit; a unit only held also reads 0 — draft-order, transfer and shipment holds now sit in `committed` — and returns to sale when the hold ends, its pair gone and its −1 stamped; nothing lowers a key not held (S13 alarms) | O2 | accepted, minor | plan `:146`, `:315-317`, `:449-451`; changelog post `draft-order-and-transfer-…md:35` (August 5, 2026), `:43`, `:55` | `available` ≤ 0 with Promo `committed` > 0 → the −Δ stays pending, surfaced with its reason (S11), until the hold clears; tested |
| AA8 | Activation: (i) no rule sends a +Δ only once its activation row is `APPLIED` — a write to an inactive level succeeds, so a +Δ beside a `FAILED` activation lands unsellable, closes `CARRIED` and is stamped as a 0 remainder; E15 still labels that an inference; (ii) "a re-send of either sets a state, not a quantity" is false for `inventoryActivate` with explicit quantities — an in-window re-send processed fresh on a level since made active is the absolute set E15 forbids; (iii) E15's "before any Promo write … activates" also binds lowerings, against the decrement's "never … activates" (`:764`) and the revive's zero-then-activate (`:149`) | O3 | accepted, minor | plan `:79`, `:149`, `:178-181`, `:242-247`, `:764`; `removal-of-itemnotstockedatlocation-error.md:45` ("inventory quantities can now be adjusted at any location"); `add-isactive-field-to-inventorylevel.md:45` ("Inactive levels can be adjusted just like active ones"); `inventoryActivate.md:18-23` | a +Δ is sent only once its activation row is `APPLIED` (else it closes `FAILED`, never sent); `isActive` re-read before any activation re-send (active → `APPLIED`, nothing sent); activation scoped to +Δ writes; E15 cites the two posts, not an inference |
| AA9 | The N-0 release asks real-size Promo exactly 0; a negative `available` is never raised (rule 4; the lowering acts "when positive"), so a key with N 0 and P < 0 is never released by a pass | O4 | accepted, minor | plan `:131-137`, `:146`, `:316-317`, `:424-427`, `:829` | clamp(P) = 0 (P ≤ 0) in the four statements |
| AA10 | A key not held with N 0 and Σ pending > 0 (a stale unit sold on an outlet line while a fresh unpublished pair was on the shelf: CONSUME takes the pair with Δ 0) meets both the N-0 release and the add flow (the Δ > 0 row has no N term); which runs first is unstated — the add puts a unit on sale for no pair | F4 | accepted, minor | plan `:136-141`, `:146`, `:149`; `SM` `scansia-sp-spec.md:206-208` | the N-0 release runs before grouping; a Δ > 0 group of a key with N 0 is never added |
| AA11 | The N-0 release reads N and the key's events with no read discipline: N read first, a pair arriving before the events are read has its +1 stamped unapplied (rule 5 stamps the events read) — the pair silently unexposed (S13 lists it below N) | F5 | accepted, minor | plan `:136-141`, `:318-319`, `:335-337`, `:415-417` | the key's events read first, N after (or one consistent read); only those events stamped |
| AA12 | Z11's held-key term understated: (a) a held SKU with an outlet costs ≥ 2 calls — the resolver's `products` query carries no quantities, then `read_variant_inventory` per product — not "a read each" (per SKU: a SKU's keys share them); (b) the sweep runs at every take, and each per-key re-level is a take: under Parity (41 + 99 pairs' keys, D6's 69) it repeats once per re-level, uncounted in Parity's result; whether a re-take after the holder's own release runs recovery (`:366`: "first start or take-over") is unstated | O6, F7 | accepted, minor (F7's count holds per SKU, not per key) | plan `:130-131`, `:328-337`, `:366`, `:696-702`, `:713-717`, `:770-775`, `:812-814`; `SS` `resolvers.py:49-66`, `:108-118`, `ops.py:618-640` | B-3 and the Exit: the resolver + one inventory read per outlet product, per held SKU; whether a re-take runs recovery stated (proposed: only when another holder held the lease in between); Parity's result names the per-re-level sweep, or one sweep serves a run of re-levels |
| AA13 | Recovery has two classes (the decrement's; publish-class: publish, baseline, re-level); pricing, delete, the bulk cleanup, the multi-outlet exit, deny-normalize and the normalisation take the lease and run recovery with none: whether a price edit re-sends +Δ rows, re-runs tails / normalise rows or searches for orphans is unstated, and B-3's hand-off bound depends on it | O8 | accepted, minor | plan `:225-228`, `:248-252`, `:262-263`, `:322-324`, `:346-348`, `:366` | each holder's class named (proposed: the normalisation at a re-level's confirm publish-class, the others the decrement's) |
| AA14 | Σ pending counts events whose `CARRIED` shares already moved P: on a held key with a `CARRIED` −Δ share the lowering's and S13's term understate the excess by that share (N 2 → 0 over two ACTIVE outlets with 1 each, A `APPLIED` → `CARRIED`, B `FAILED`: clamp(1) − 2 < 0 — nothing lowered, S13 silent until the next pass's remainder); on a key not held a `CARRIED` +Δ share counts twice | O9 | accepted, minor | plan `:131-134`, `:196-200`, `:673-676` | in the lowering and in S13, Σ pending is net of the `CARRIED` shares over those events (the remainder, Groups) |
| AA15 | §10 Round 7 records Opus's versioning `:101` quote as "not on the page as quoted": the page carries it, as `:101`'s second sentence — "Deprecated fields or types are removed in a subsequent release—for example, something deprecated in `2026-10` might be removed in `2027-01`." (curl 16:50Z; Round 7 quoted the line's first sentence only) — so F7.2's "or records why not" leaves both deprecated surfaces to S14's forced re-pin | O7 | accepted, minor | plan `:682-685`, `:806`, `:1163-1165`; versioning `.md:101` | Round 7's sentence corrected, pointing here; F7.2 migrates both surfaces, or S14's re-pin names the migration as its precondition |
| AA16 | Cites: "`JobStore` is one slot per process" cites `app.py:123`, the `ThreadPoolExecutor` (the `JobStore` is built at `:143`); "immediately" cites 678434 doc @`0329757`:65, the trigger row (scheduling: `:66`); `productVariantsBulkUpdate`'s "inventory input" — its delta field is `quantityAdjustments` (`[InventoryAdjustmentInput]`, carrying `changeFromQuantity`), `inventoryQuantities` being `productVariantsBulkCreate`-only (F7 sends neither: "never sends" holds) | O11, F8 | accepted, minor | plan `:79`, `:320-321`, `:758-759`, `:807`; `SS` `app.py:122-123`, `:143`; 678434 doc @`0329757`:64-66; `ProductVariantsBulkInput.md:55-59`, `:97-101`; `InventoryAdjustmentInput.md:31`; `2026-04.md:112` | `app.py:143`; `:65-66`; `quantityAdjustments` named at `:79` and `:807` |

Mandate notes: both — no statement of the mandate found wrong; the pins and line lists re-measured as stated. Opus —
dev.mysql.com answered 403, also with a browser user agent (`:266`'s quote not re-read); the release-notes index lists
`2025-04` and `2026-01` → `2026-10` only (`2025-07.md` / `2025-10.md` 404; `2025-10` checked through changelog posts).
Fable — Z8 (a) holds at its pin; community.shopify.dev serves no `.md` (`/t/28236.md` 404), its HTML carries #2 / #5 with
the dates the plan quotes; the `2026-10` release notes head "Release candidate" while their table says "Latest stable";
`2026-10` `inventorySetQuantities.md`'s prose still describes `compareQuantity` though its input objects no longer carry
it — the plan's claim stands on the input objects.

Round 8 — targeted re-review (D16, D18) — two fresh reviewers of round 8's [T] hunks at `c329b16` (sha `fa206a28…`),
2026-10-05 ~20:23–20:52Z (`reviews/f7-review-2026-10-05-opus-r8t.txt`, 7 = O1–O7; `…-fable-r8t.txt`, 12 = F1–F12; local
only), merged into 16 distinct defects (AB1–AB16), judged by step session `scansia-step` (context `1a44a2b0`, spawned by
`scansia-f6`; D19's step 9) on 2026-10-05 ~21:00–21:20Z. Re-read: this file at the cited ranges (`:79`, `:88-98`,
`:124-160`, `:170-345`, `:393-399`, `:416-431`, `:474-496`, `:516-521`, `:546-562`, `:570-573`, `:584-590`, `:722-766`,
`:806-810`, `:927-930`, `:1123`, `:1352-1353`); the r8 change list's design choices (`:43-59`, sha `f2f6480f…`); `SS`
`fdfaf2f` `ops.py:430-436`, `outlet_service.py:703-711`; `measures/2026-09-30-f7/c7_summary.json` `b5`; shopify.dev by curl
21:06Z, U+200B and `\_` stripped — `2026-10` `inventoryDeactivate.md:23`, `:51`, `inventoryActivate.md:59`. Severity:
**AB1–AB3 major** — AA1's class (a row the machine never closes, so its hold outlives the gesture the plan names) twice
inside AA1's fix, and the one path of terminal path (i) toward more; per **D18** each is fixed in this file, closed in the
coordinator's diff review (every claim the fix introduces re-read at its pin) and a **mandatory build test**; no reviewer
round. Minors close in the same fix pass or go to the build as test cases (D16). 0 rejected.

| # | Defect | Reviewers | Verdict | Line re-read | Change |
|---|---|---|---|---|---|
| AB1 | An `APPLIED` row whose group sibling closes `HELD` has no rule: Groups and the `APPLIED` bullet name all-`APPLIED` → stamp, a sibling `FAILED` → `CARRIED`, a sibling not closed → wait — `HELD` is closed and none of these. Reached by AA8's own order (the activation `APPLIED` first, its +Δ sent after, `HELD` on an unknown answer, STALE or past the window), by (iv) on one member of a −Δ split, and by a revive's zeroing rows (group members: the composition names "which variants a revive zeroes"). The row stays `APPLIED` for ever: an in-flight hold no `BASELINE` row ends, so the re-level that releases the `HELD` key leaves it held, the N-0 release is refused and every revive of the product deferred; only (iv) plus a second re-level ends it | O1 (major), F2 (minor) | accepted, **major** | plan `:98`, `:143-144`, `:157`, `:200-219`, `:230-233`, `:242-245`, `:331-341` | Groups and the `APPLIED` bullet: a sibling closed `FAILED` **or `HELD`** → each `APPLIED` sibling closes `CARRIED` (its moved quantity in `OUTCOME`, no stamp); the group's events stay held by the `HELD` row until the key's re-level or N-0 release (not O1's `HELD`: the key is already held, and `CARRIED` records what landed); §4 tests an activation beside a `HELD` +Δ, a split member closed by (iv), zeroing rows beside a `HELD` add |
| AB2 | Terminal path (i)'s "a variant not DENY + tracked is never exposed (rule 7)" is false for a normalise row: the normalisation's targets are CONTINUE / untracked variants of ACTIVE outlets (`c7_summary.json` `b5`: ACTIVE 1436 = DENY 1408 + CONTINUE 28; `tracked_active` false 6), so a refusal closes `FAILED` on a product F7 keeps ACTIVE — rule 7's own forbidden case ("sells at any level, unrecorded") — with no retry and no standing alarm (S13 has no policy term). True for the tail only (DENY before ACTIVE, `SS` `outlet_service.py:704-709`) | O2 (major), F3 (minor, "arguably major") | accepted, **major** | plan `:274-277`, `:300-304`, `:427-431`, `:518-520`, `:571-573`, `:755-765`; `SS` `outlet_service.py:703-711`; `c7_summary.json` `b5` | a normalise row closed `FAILED` — by (i) or by a person's close (AB11) — on an ACTIVE product still carrying a variant not DENY + tracked drafts the product (F7's drafter), surfaced; S13 alarms on a CONTINUE or untracked variant on an ACTIVE outlet; (i)'s "never exposed" scoped to the tail; §4 tests it |
| AB3 | The revive-tail row's write instant is unstated ("opened after the per-size rows"). Written ahead with the revive's rows (`:208-210`'s doctrine): a revive that ends at a zeroing row leaves its tail `OPEN`, never sent — the never-sent bullet keeps it owed, the tail bullet never re-runs it ("surfaced, never re-run"), nothing closes it, and its `PRODUCT_ID` defers every revive of the product, the early end's own retry included, until (iv). Inserted late: the gate is dead text and V13's crash window (`:1123`) reopens. Also reached when a zeroing row closes `CARRIED` beside a `FAILED` add, or `FAILED` by (ii), after the tail opened | F1 (major), O7 (a) (minor) | accepted, **major** | plan `:157`, `:202-213`, `:225-229`, `:246-249`, `:1123` | the tail row is written ahead with the revive's rows; a tail whose revive's zeroing rows will not all apply (one `FAILED`, or the revive ended there) closes `FAILED` ("never run", nothing exposed), surfaced — the revive's retry is then not deferred; `CARRIED` and a `SUPERSEDED` row's applied successor count as applied in the gate; §4 tests an early-ended revive and its retry |
| AB4 | (iv) is an F7 database write (`STATUS`, `OUTCOME`, close) with no lease, fence or class, though every F7 database write opens with a guarded renew; and its "never re-adds" needs the releasing re-level to stamp the `HELD` row's events, while §3.4 says it stamps "the pending event ids" and `pending_deltas()` excludes held events — read so, a landed +Δ is re-added once released | O3 | accepted, minor | plan `:92`, `:144-145`, `:282-284`, `:393-399`, `:492-494` | the person's close runs under the pass lease, guarded on the row still not closed; §3.4: a re-level stamps every unstamped event of its key up to its snapshot — pending and held — as the N-0 release does |
| AB5 | Precedence and wording against the new bullets: (a) "read only by its own bullets below" can read as excluding CREATE / tail rows from (ii)–(iv) and the classes; (b) a sent +Δ whose product reads gone is `HELD` by `:242` and `FAILED` by (ii), no order; (c) "no decrement-class holder runs … a normalisation" while deny-normalize is decrement-class and is rule 7's normalisation; (d) a normalise row's `SENT_AT` undefined, though the never-sent bullet, the N-0 release and S11 read it | O4 | (a) partly — "its own bullets" plainly include the terminal paths that name the row; (b)–(d) accepted; minor | plan `:173-175`, `:225-229`, `:242`, `:274-292`, `:571-573`, `:584-585`, `:143-144`, `:747` | Terminal paths and Recovery classes bind every row kind and are read first; (ii) / (iii) before the per-kind bullets (dispositions: AB7); `:292` "in recovery"; a normalise row sets `SENT_AT`, fenced, before every call, as a tail row (rule 2, §3.6) |
| AB6 | A never-sent normalise row has no disposition: the never-sent bullet names inventory, CREATE and tail rows, the classes only sent normalise rows; its keys stay held in flight until some holder decides | F6, O4 (d) | accepted, minor | plan `:225-229`, `:285-292`, `:302-304` | a never-sent normalise row is owed to a publish-class holder, as a tail row (re-run, idempotent by state) |
| AB7 | (ii) and (iii) close `FAILED` whatever the row's state: an `APPLIED` row, against `FAILED`'s "nothing was applied by that row", its share not netted as `CARRIED` and re-applied on another outlet; a sent −Δ of unknown outcome with no "possibly applied" — on a multi-outlet exit a certain extra −1 on the kept outlet until the exit's re-level; a sent +Δ / activation / CREATE closed `FAILED` by (ii) re-adds its events, safe only if the null means gone — E15 records a null for an existing collection on `2025-10` | O5, F8 | accepted, minor (toward fewer, except a +Δ re-added on a null that is no deletion) | plan `:79`, `:180-181`, `:215-218`, `:277-282`, `:327-333`, `:551-552`, `:559-561` | (ii) / (iii) close an `APPLIED` row `CARRIED` (netted); a sent −Δ row `FAILED` with "possibly applied" on its `OUTCOME`; (ii) on a sent +Δ / activation / CREATE row → `HELD` (a null is definitive only as F7.3 tests it at the pin); (iii) keeps `FAILED` for them (the product's units left with it) |
| AB8 | AA2's design choice says the widened held-key lowering "lowers nothing", but it acts on keys held only by a tail / normalise row: a Shopify-side restock ahead of its same-day `RESTORE` (§8) is lowered while the row is open → P = N − 1 after the `RESTORE`, listed, not alarmed; unheld, the `RESTORE` realigns it | O6 | accepted, minor | plan `:130-140`, `:303`, `:756-759`, `:929`; list `:56-59` | the held-key lowering skips keys held only by a tail or normalise row (they move no quantity); S13's held-key term still alarms above N |
| AB9 | The same design choice records "P = N + Σ pending there"; the invariant is clamp(P) + Σ pending = N — read as written the lowering would be 2·Σ pending. The plan's text is right | F7 | accepted, minor | plan `:135-136`, `:476-477`; list `:57-58` | none in the plan: this row records the relation (the list is a local, sha-pinned record) |
| AB10 | The deactivation read maps inactive / active, not absent, while `inventoryDeactivate.md:23` reads "Removes an inventory item's quantities from a location, and turns off inventory at the location" and its example `:51` "Deletes an inventory level from a location"; the past-window activation read maps active / inactive only, the in-window clause "inactive or absent" | F4 | accepted, minor | plan `:188-190`, `:265-273`; curl 21:06Z `inventoryDeactivate.md:23`, `:51` | absent → `APPLIED` for a deactivation, → `FAILED` for an activation read past the window |
| AB11 | (iv) sets `HELD` on a tail / normalise row too: every key it serves (each real-size key of the product) stays held, one per-key re-level each — k gestures for a state write that moves no quantity | F11 | accepted, minor | plan `:98`, `:282-284`, `:303` | a person's close of a tail or normalise row (no event ids: nothing to re-add or stamp) closes it `FAILED`, surfaced, with (i)'s consequences (AB2's drafting) |
| AB12 | `:190-193` states as fact that "an activation's explicit quantities are an absolute set on a level since made active (E15)"; E15 leaves it to F7.3, `SS` `ops.py:432-434` asserts the opposite shape ("returns the existing ``inventoryLevel`` with NO userError"), `inventoryActivate.md:59` says "initial quantities". The rule is safe either way | F5 | accepted, minor | plan `:79`, `:190-193`, `:265-267`; `SS` `ops.py:430-436`; curl 21:06Z `inventoryActivate.md:59` | carry E15's label: "would be … until F7.3 measures", as `:266` |
| AB13 | A decrement-class holder reads deactivation rows off `isActive` but leaves activation rows as they are, though the read is the same: an activation `OPEN` after a crash holds its key and defers its product's revives until a publish-class holder | F9 | accepted, minor | plan `:269-273`, `:285-292` | every holder reads an activation row off `isActive` (`APPLIED` / `FAILED`); the decrement never re-sends it |
| AB14 | A tail `FAILED` at its publish step leaves an ACTIVE, unpublished outlet (status before publish, `SS` `outlet_service.py:708-709`): no F7 flow re-publishes (the add flow has no publish, a revive runs on DRAFT only), and S13 alarms for ever | F10 | accepted, minor | plan `:157`, `:274-277`, `:763`; `SS` `outlet_service.py:703-711` | name the heal: a re-level of a key whose outlet reads ACTIVE and unpublished re-runs the publish (idempotent by state), surfaced |
| AB15 | An "orphan unknown" CREATE (`PRODUCT_ID` NULL) whose orphan the single delete removed: (iii) finds rows by product id, the orphan search then finds nothing — the row stays open until (iv) | O7 (b) | partly — (iv) is that row's designed close (`:261-262`); no new hole | plan `:257-262`, `:547-548`, `:589` | none in the plan; a build test case (D16) |
| AB16 | §4's test sentence names only "a row whose product reads gone": (ii)'s variant / item / location cases, an unreadable activation and the states above are untested, though AA1's Change promised "§4 tests each" | F12 | accepted, minor | plan `:725-738`, `:1352` | §4 adds (ii)'s four cases, the unreadable activation, AB1–AB3 (mandatory, D18), AB6, AB10, AB13, AB14 |

Mandate notes: both — STOCK_Manager local `main` moved to `9ddd288` during the review (2 commits, 20:22Z / 20:40Z), the
four pinned paths unchanged `3117c92..9ddd288`; Opus — Racoon-LAB HEAD moved to `74ac5f3`, the plan's sha unchanged;
Fable — every other statement measured as stated; `InventoryLevel.md` (`2026-10`) `isActive` at `:70`, its description `:76`.

Diff review of the AB list (`reviews/f7-2b-change-list-r8t-2026-10-05.txt`, sha `8ead97b9…`), PART 1 — 2026-10-05
~21:53–22:05Z, step session `scansia-step` (context `507fc661`, spawned by `scansia-f6`; D19's step 12), on `7ce4701e…` at
`8b3db5c` (clean). Mapping: `reviews/verify_apply.py b408245 <list>` re-run → ALL OK (26 hunks, each on an anchor span;
blank lines 85 → 85, table rows 246 → 246); the word-diff `-U0` read whole: nothing outside the list moved. **The list's
claims, re-read at their pins**: `SS` `fdfaf2f` (`feat/scansia-manager` still there) `outlet_service.py` by `git show` —
`:704-708` the DENY bulk update, `:709` `product_update_status(… "ACTIVE")`, `:710` `product_publish`: the order holds, but
"status before publish" is **`:709-710`**, not `:708-709` (item 16; AB14's row cites the same) → fixed at `:297`;
`c7_summary.json` `b5` — `inventoryPolicy_active` DENY 1408 / CONTINUE 28, `tracked_active` false 6: holds; shopify.dev
`2026-10` `mutations/inventoryDeactivate.md` by curl 21:55Z (U+200B and `\_` stripped) — `:23` "Removes an inventory item's
quantities from a location, and turns off inventory at the location.", `:51` "Deletes an inventory level from a location":
both hold; E15 `:79` — the `null` of `collection(id:)` on `2025-10` and "F7.3 measures what it does on an active level at
the pin": both there. **Design choices re-checked on the text**: AB1 (Groups `:222-229`, the `APPLIED` bullet, the < 0 row,
§3.4's re-level stamping held events), AB2 ((i)'s drafting, (iv)'s tail / normalise close, §5 S13's new term), AB3 (the
> 0 row, the gate's four states, the open-tail bullet) — hold. Two edits beyond the cite: the > 0 row's "before its first
call" (the tail's, by its grammar) → "before the revive's first call" (the design choice: written ahead with the
revive's rows); **a pre-existing gap the AB text leans on** — Groups' remainder nets "the shares of the `CARRIED` rows"
naming only an activation's share as 0, while AB1 / AB3 now name a revive's zeroing rows `CARRIED`: read as its moved
quantity, a zeroing row's −x would raise the remainder to Δ + x (more) → "and so is a revive's zeroing row's: it lowers no
key the group adds to — held, N 0 or customisation-only" (`:231-232`; a build test with AB1's). **Part 2 (next session)**:
the applier's flags a1–a7, b1–b4, c1–c2 (`PROGRAM.md` Current state; a1 first); AB1–AB3 close there (a6, b1, b2 touch
their text).

**Diff review, part 2 — the applier's flags (2026-10-05 ~22:02–22:12Z, step session `scansia-step`, context `56d9b20d`,
D19 step 13)**: plan `a1714845…` (clean) → this commit's, 12 hunks, no blank line added. Each flag re-read at its range
±3 (lines at `a1714845…`). **a1** accepted — §4 round 5–6's test: the decrement's recovery leaves sent +Δ / CREATE rows
and tail rows and reads an activation row off `isActive` (AB13). **a2** accepted — rule 6's fenced-renew list gains "a
normalisation's calls" (rule 2 `:176-178` already set its `SENT_AT`). **a3** accepted (a pre-existing gap AB7 exposed;
a design choice of this session): a re-send answered `INVALID_INVENTORY_ITEM` / `INVALID_LOCATION` closes as terminal
path (ii) per kind (−Δ `FAILED` "possibly applied", +Δ / activation `HELD`) — the plan's own `:376` says a completed
original's duplicate may answer not-found once its data changed, so `FAILED` on a +Δ could re-add a landed unit; no
source re-read (the cite `:309-331` unchanged). **a4** rejected — rule 9's `:487-489` is scoped to a first send's
`userErrors`; (ii) / (iii) are no such answer. **a5** accepted — §3.4's snapshot reads a re-level's held ids too (AB4
stamps them). **a6** partly — rule 6's holder list names a person's close (one row, (iv)); `:456`'s "Whoever takes the
lease" already covers its recovery + sweep (intended: toward fewer; a row closed meanwhile → (iv)'s guard, 0 rows);
B-3's "every holder" generic — unchanged. **a7** accepted — §8's S13 summary gains the CONTINUE / untracked term (AB2).
**b1** the tail / normalise bullet names "the tail or normalise row"; **b2** "FIX4 applies to the re-level"; **b3**
"lowers that Promo"; **b4** "once they have, the revive activates" — accepted. **c1** accepted — the decrement class
"in its recovery" (person's close cited "((iv))", as at `:302`). **c2** accepted — (iii)'s §3.5 summary adds "a sent −Δ
'possibly applied'". GREP hits: none states a replaced rule beyond a3 / a5. **AB1–AB3 CLOSED (D18)**: fixed (`8b3db5c`),
every claim re-read at its pin (part 1), design choices held (part 1), their text re-read here (b1 touches AB3's
bullet), mandatory build tests (§4 `:783-791`). Minors left → the build's tests (D16). Next: 2c.

**B-3's event option — the coordinator's diff review (D22 step 19, 2026-10-06 09:46–09:56Z by `date -u`, context `41970536`).**
`git diff --word-diff -U0 cd9cb4e a5d9327 -- f7-plan.md` read whole: 9 hunks (Status line, §3.3 rule 6's crash wait, §6
B-3's header, the event bullet, the 24/7 bullet's head and cost, "Any automatic form", §7 F7.5, Exit's S11 bound, one §8
row), each from the study (`reviews/f7-b3-event-study-2026-10-06.txt`, local, sha `b7023d1b…`) or a design choice below;
nothing else moved. Every cite the new text introduces re-read by one script (`git show` at the cited rev; the web by
`curl` of the `.md` page, 09:50Z): 32 cites, all hold but these, fixed here: (1) "216 on 206, runbook:78" — 216 is an
Ignore **filtered** on "ID not found" (`IT/AUTOMATE/docs/f6-scansia-cutover-runbook.md`@`9a78dca`:78), not a never-stop
handler: the text now says the new call's Ignore is unfiltered; (2) "each hook its own queue" is
`help.make.com/webhooks.md:53` («Each webhook has its own queue»), `:57` is parallel-by-default — `:57` → `:53`; (3) short
cites expanded: `subscribe.md:87` and `webhooks.md:83` → their shopify.dev paths (`:87` is the shop-specific column,
«Failing subscriptions will be deleted»), bare `free.md` → `render.com/docs/free.md` first in the 24/7 and "Any automatic
form" bullets; (4) `IT/AUTOMATE/CLAUDE.md:59` pinned `@73c1dd7` (its last commit, 2026-10-03; tree clean) in §6 B-3 and §7
F7.5; (5) asleep, "the call times out" → "ends within 5 s": whether Render holds a non-browser request while spinning up or
answers it is not stated (`free.md:61` names a loading page for browsers) — either way the order is not blocked; measured
at build. Held as written: no default Timeout on `apps.make.com/http.md` (no "default" on the page); 168's Break 219
(678434 doc @0329757:67: «onerror 218/219 builtin:Break … on 146 and 168»); 678443 module 8 → 38 `builtin:Ignore`
(@93d89e5:26). Design choices (a)–(f) re-checked on the text — hold: a trigger during a running or waiting decrement
coalesces, at most one waiting (nothing lost: the waiting pass reads `pending_deltas()`); the boot pass for every automatic
form; S11's bound = the window's; one §8 row; at zero it drafts — the endpoint "lowers and drafts; it never creates,
activates or adds", no delete path; a crash waits the lease expiry + the next pass (§3.3 rule 6). Noted, not changed: the
24/7 bullet's "never sleeps" (a pass every 15 min against a 15-min spin-down, `free.md:59`) is at the limit — a late run
lets it sleep, and the boot pass covers that case. Then: the new `GATE:1 B-3` payload (step 20), asked 2026-10-06
10:18:11Z, answered 10:18:49Z — this option chosen (D23, `PROGRAM.md`@`94d78d3`:1688-1704); folded in by D22 step 21
(Status line, §3.3's table and rule 6, §4 item 4, §6 B-3, §7 F7.5 and Exit, §8).
