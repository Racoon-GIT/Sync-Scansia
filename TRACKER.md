# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: build · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: pending

**Phase**: F7.2 (build) of the programme "Scansia → MySQL" — orchestrator `scansia-f6`, mandate `IT/_scansia-programme/briefs/sync-scansia-F7.2.md`, programme state `IT/_scansia-programme/PROGRAM.md`. Gate-1: D25 (`gate1-receipts.log` line 661); this lane's spawn: D27 (line 667). The plan is the contract, imported verbatim at sha `6dadcdd7…` (`71dfc51`): never edited here — a statement that looks wrong goes to the orchestrator. The register is `racoon.sc_returned_pair` + `sc_returned_pair_event` (owner STOCK_Manager) since the F6 cutover (2026-09-21); `Scarpe_in_Scansia` is frozen. Build state (2026-10-06): §4.1–§4.3 and the §7 pin done; §4.4 in progress — "Inizializza" built (baseline + per-key re-level), B-3's trigger + boot pass + worker built; suite 636 offline green + 27 staging (not re-run: no store change since `6fbc7ac`; the new flows use contract-tested store primitives only).

**Next** (plan §4, in order):
- [x] §4.1 — owed push landed 2026-10-06 (`ea9db46..e3c2bff` on `feat/scansia-manager`, re-read by `git ls-remote`).
- [x] §4.2 — `RegisterPort` / `RegisterStore` (`persistence/ports.py`), `Register`, the fake `memory_store.py`, `services/lease.py`, `services/variants.py`; publish on the register; layering in `docs/f7-build-notes.md`.
- [x] §4.3 (2026-10-06) — `persistence/mysql_store.py` (`mysql-connector-python==9.6.0`), `audit_sink.py`, production `register_factory`. On `racoon_staging` after `scansia-f6`'s INFO: the store contract 25/25 and `tests/test_consume_beside_f7.py` 2/2 (stamp and pair update by PRIMARY KEY — EXPLAIN; a CONSUME beside an open stamp never waits; one on a pair F7 writes waits one F7 commit at most). Run: `SCANSIA_STAGING=1 pytest tests/test_register_store.py tests/test_consume_beside_f7.py -k "mysql or consume_beside"` (~2.5 min). Staging left with 0 `f7test` rows, lease free.
- [~] §4.4 — DONE: `services/ledger.py`, `flows.py`, `recovery.py`, `passes.py`, publish on the engine (`outlet_service.py`), `tests/fake_shop.py`; D33's test (N-0 release vs a committed unit); the store reads `read_snapshot` / `read_import_payloads`; **"Inizializza" — the baseline** (`services/baseline_service.py`: Level per key, holds, normalisations, J seed, Part B listed, `INIT_BEFORE` → `BASELINE` last with its explicit stamp, the `/dlqs` precondition typed and kept; `api/init.py` on `register_factory`, the confirm applies the kept plan; the UI banner; the sheet-era `init_service` removed). **The per-key re-level** (`services/relevel_service.py`, `/relevel` in `api/init.py`, the UI panel: ACTIVE to N / zero-then-revive / CREATE / N 0, FIX4, the key's every unstamped event stamped, a per-key `BASELINE` row releases). **B-3** (`services/decrement_worker.py`, `api/decrement.py` — `POST /outlet/decrement/trigger` behind its own `TRIGGER_SECRET` bearer, the boot pass at startup, coalescing). Mechanisms and interpretations: `docs/f7-build-notes.md` §Baseline, §Re-level, §B-3 (F7.3/F7.5 wiring listed there). **Next** (TODO, in order): pricing / delete / cleanup / exit / deny-normalize on the register (§3.5), a person's close (iv), the API verticals on the register, the rest of §4 item 4's list and §10's minors as tests (AB1–AB3 mandatory, D18). Fixed in the publish (found in review, a parity regression of `ff69522`): the ACTIVE branch again refreshes the SKU's price and compare-at on every variant (`run_normalise` with the row's prices), and every publish writes `OUTLET_PRODUCT_ID` (§3.1 "any pass").
- [x] §7 pin — Admin API `2026-10` (`config.PINNED_API_VERSION`); sources in `docs/f7-build-notes.md` §engine.
- [ ] §4.5 — remove §3.8 (`gspread`, `backend/gsheet/`, `src/sync.py`, `fix_prices.py`, `RUN_MODE=SYNC`); `requirements.txt` loses `gspread`; rewrite the project `CLAUDE.md` (still the legacy `RUN_MODE` cron jobs and the sheet) to the web service on MySQL in the same step.
- [ ] §4.6 — gate-2: `IT/SVILUPPO/docs/adversarial-verify.md`, `mutate-verify.sh` PASS rows (stash `.claude/settings.json` first: receipts are `-dirty` otherwise), `/ship`; then `[SYNC-SCANSIA→SCANSIA] DONE|BLOCKED` to `scansia-f6`.

**GATE-2 prompt, self-grade (2026-10-06, context `b64bd592`)**: "Inizializza" (`c9328d4`, `572b714`) **9/10 → 10/10** after two fix rounds — per-key fresh re-read at confirm (was per SKU), the invariant guarded under (b)/(c) (`shelf_events_not_n`), a DRAFT-parked key left at N not held, N 0 with a `committed` unit held (D33's reading), outlet ids written (§3.1 "any pass"), cross-module private calls made public, a dead status field removed; every new guard killed by a hand mutant. Not covered by the grade: the UI in a browser (no automated test — house pattern) and a staging run (no store change). B-3's trigger + worker: **10/10** after one fix (a thread that cannot start no longer wedges the worker); its five guards killed by hand mutants. Earlier, this lane's slice (`e90e37e`, `6fbc7ac`) **10/10** after one fix round — findings fixed: string surgery on a SQL constant, untyped port methods, a dead test helper matching events on `pair_id`, an overstated test name, a pre-existing 1-in-16 flaky token test; a citation added for "events are append-only". The engine commits (`ff69522` and before) are graded at §4.6 with adversarial-verify + `mutate-verify.sh`, not here.

**Active constraints & decisions**:
- F7.2 writes nothing to `racoon`, Shopify, Make, production Trello, the Scheduler or Render — F7.3 is the deploy and the live tests (SERVER + Ale's `confermo`). Integration tests on `racoon_staging` only (`scansia_mgr_stg` / `scansia_fixture_stg`, option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (SERVER's reading; confirmed by `scansia-f6` 2026-10-06 14:23Z) — **never push or merge to `main`**. `feat/scansia-manager` deploys nothing and stays local in F7.2 (`scansia-f6`'s call): it reaches the remote at F7.3 under Ale's decision; that push carries earlier contexts' commits, so `guard-foreign-push` needs Ale's typed `FOREIGN_PUSH_OK`. `origin/main` = `00c7682` (TRACKER-only) is not in this branch: a merge conflicts on `TRACKER.md` — keep this branch's side.
- D28: staging has its own app user; `scansia_mgr` is `racoon` only. D33 (2026-10-06): the N-0 release waits while an exact-size Promo level has `committed > 0` (the lane's reading, confirmed).
- One connect retry (`mysql_store.CONNECT_ATTEMPTS`): from the Mac a connect to `78.46.244.227:3306` failed 5 times in five suite runs (errno 2003, OS 60); the Render → VPS path is F7.3's to measure.
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project CLAUDE.md rule 2).
- Working tree: the uncommitted `.claude/settings.json` change is not this programme's — left as found, never committed by this lane.

**Reads made in F7.2 (facts for build steps)**: `racoon` none. `racoon_staging` 2026-10-06 (as the staging users): server status and `sm_outlet_lease` column types (probe), the post-run count of `f7test` rows; facts in `docs/f7-build-notes.md` §Facts.

**Tried & discarded (still relevant)**:
- The sheet as store (Q-persistenza, 2026-07-06): voided by the F6 cutover — the register is MySQL.
- Level-setting sweeps ("set Promo to the shelf count"): they put back units sold during an intake stall (plan §3.3).

**Key refs**: plan `docs/f7-mysql-adapter-plan.md` (§3 design, §4 build list, §10 review minors); `docs/f7-build-notes.md` (layering, engine, parameters, staging harness, facts — read before building); `backend/persistence/ports.py`; `backend/services/`; `docs/testing.md` (venv for the FastAPI suite); F1 history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
