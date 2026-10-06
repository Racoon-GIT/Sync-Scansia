# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: build · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: pending

**Phase**: F7.2 (build) of the programme "Scansia → MySQL" — orchestrator `scansia-f6`, mandate `IT/_scansia-programme/briefs/sync-scansia-F7.2.md`, programme state `IT/_scansia-programme/PROGRAM.md`. Gate-1: D25 (`gate1-receipts.log` line 661); this lane's spawn: D27 (line 667); its third context: D36. The plan is the contract, imported verbatim at sha `6dadcdd7…` (`71dfc51`): never edited here — a statement that looks wrong goes to the orchestrator. The register is `racoon.sc_returned_pair` + `sc_returned_pair_event` (owner STOCK_Manager) since the F6 cutover (2026-09-21); `Scarpe_in_Scansia` is frozen. Build state (2026-10-06): every vertical of §3 is on the register; suite 709 offline green + 28 on `racoon_staging` (re-run 2026-10-06 19:45Z after the pair SELECT gained `BRAND`: 28/28, ~2 min 15 s).

**Next** (plan §4, in order):
- [x] §4.1–§4.3 — the owed push (`ea9db46..e3c2bff`), ports + fake, the MySQL store and audit sink. Staging run: `SCANSIA_STAGING=1 pytest tests/test_register_store.py tests/test_consume_beside_f7.py -k "mysql or consume_beside"`; it leaves 0 `f7test` rows and the lease free.
- [x] §4.4 — the engine (`ledger`, `flows`, `recovery`, `passes`, publish), "Inizializza", the per-key re-level, B-3's trigger + boot pass + worker (contexts `13a63901`, `b64bd592`); then (`6670a20`, context `e5b7e96e`) §3.5 on the register: pricing + revert, the deletion order (single delete with a preview and its `HOLD`; the bulk cleanup with `protected`), the multi-outlet exit, deny-normalize, a person's close (`/outlet/reconcile/open|close`), the read verticals (`/scansia` = the shelf, the exact-size join, `/audit`), E18's Promo gate, recovery's terminal path (ii) on `APPLIED` rows, §10's round-8 minors as tests (`tests/test_round8_minors.py` + per-module). Mechanisms and interpretations: `docs/f7-build-notes.md` §3.5.
- [ ] §4.4 coverage map — every item of plan §4 item 4 (rounds 5–7 and the rest) mapped to the test that pins it; write the missing ones. Before §4.6.
- [ ] §4.5 — remove §3.8: `gspread`, `backend/gsheet/`, `persistence/gsheet_audit.py`, `src/sync.py`, `fix_prices.py`, `RUN_MODE=SYNC`, the app's `sheet_factory` / `audit_factory` wiring, the sheet error codes, their tests; `requirements.txt` loses `gspread`; rewrite the project `CLAUDE.md` (still the legacy `RUN_MODE` cron jobs and the sheet) and `docs/testing.md`'s sheet lines in the same step.
- [ ] §4.6 — gate-2: `IT/SVILUPPO/docs/adversarial-verify.md`, `mutate-verify.sh` PASS rows (stash `.claude/settings.json` first: receipts are `-dirty` otherwise), `/ship`; then `[SYNC-SCANSIA→SCANSIA] DONE|BLOCKED` to `scansia-f6`.

**GATE-2 prompt, self-grade (2026-10-06, context `e5b7e96e`, on `6670a20`)**: **9/10 → 10/10** after one fix round — a cleanup product's Shopify failure aborted the whole batch (now per product); percent mode reported "price missing" for a missing compare-at; the AB7 test did not isolate (ii) on an `APPLIED` row (its mutant survived — a new test kills it). 20 hand mutants of the new guards, all killed. The Delete-tab UI was written by a delegated executor: reviewed for escaping (every dynamic cell through `esc` / `textContent`), its node harness ran 28 scenarios; no browser run (house pattern). Earlier grades (context `b64bd592`): "Inizializza" 10/10 after two rounds, B-3 10/10, the store slice 10/10. The engine commits (`ff69522` and before) are graded at §4.6.

**Active constraints & decisions**:
- F7.2 writes nothing to `racoon`, Shopify, Make, production Trello, the Scheduler or Render — F7.3 is the deploy and the live tests (SERVER + Ale's `confermo`). Integration tests on `racoon_staging` only (`scansia_mgr_stg` / `scansia_fixture_stg`, option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (SERVER's reading; confirmed by `scansia-f6` 2026-10-06 14:23Z) — **never push or merge to `main`**. `feat/scansia-manager` deploys nothing and stays local in F7.2 (`scansia-f6`'s call): it reaches the remote at F7.3 under Ale's decision; that push carries earlier contexts' commits, so `guard-foreign-push` needs Ale's typed `FOREIGN_PUSH_OK`. `origin/main` = `00c7682` (TRACKER-only) is not in this branch: a merge conflicts on `TRACKER.md` — keep this branch's side.
- D28: staging has its own app user; `scansia_mgr` is `racoon` only. D33 (2026-10-06): the N-0 release waits while an exact-size Promo level has `committed > 0`.
- E18 for F7.3: `PROMO_LOCATION_ID` may be the GID or the numeric id; every writing pass refuses (503 `promo_anchor_invalid`) unless it names the location called "Promo".
- One connect retry (`mysql_store.CONNECT_ATTEMPTS`): from the Mac a connect to `78.46.244.227:3306` fails now and then (errno 2003, OS 60); the Render → VPS path is F7.3's to measure.
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project CLAUDE.md rule 2).
- Working tree: the uncommitted `.claude/settings.json` change is not this programme's — left as found, never committed by this lane.

**Reads made in F7.2 (facts for build steps)**: `racoon` none. `racoon_staging` 2026-10-06 (as the staging users): server status and `sm_outlet_lease` column types (probe), the post-run counts of `f7test` rows; facts in `docs/f7-build-notes.md` §Facts.

**Tried & discarded (still relevant)**:
- The sheet as store (Q-persistenza, 2026-07-06): voided by the F6 cutover — the register is MySQL.
- Level-setting sweeps ("set Promo to the shelf count"): they put back units sold during an intake stall (plan §3.3).
- A Shopify-only price revert: the next publish re-sends the register's price on every variant, undoing it.

**Key refs**: plan `docs/f7-mysql-adapter-plan.md` (§3 design, §4 build list, §10 review minors); `docs/f7-build-notes.md` (layering, engine, §3.5, parameters, staging harness, facts — read before building); `backend/persistence/ports.py`; `backend/services/`; `docs/testing.md` (venv for the FastAPI suite — one ready while `/private/tmp` lasts: `/private/tmp/claude-501/-Users-alesap-DATI-Racoon-LAB-IT-SVILUPPO-Sync-Scansia/13a63901-7c1c-460f-8849-4c78ed0bcfcb/scratchpad/venv/bin/python -m pytest -q -o addopts=""`); F1 history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
