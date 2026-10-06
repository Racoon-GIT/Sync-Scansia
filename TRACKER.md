# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: build · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: pending

**Phase**: F7.2 (build) of the programme "Scansia → MySQL" — orchestrator `scansia-f6`, mandate `IT/_scansia-programme/briefs/sync-scansia-F7.2.md`, programme state `IT/_scansia-programme/PROGRAM.md`. Gate-1: D25 (`gate1-receipts.log` line 661, plan_ref `docs/f7-mysql-adapter-plan.md`); this lane's spawn: D27 (line 667). The plan is the contract, imported verbatim at sha `6dadcdd7…` (`71dfc51`): never edited here — a statement that looks wrong goes to the orchestrator. Since the F6 cutover (2026-09-21) the register is `racoon.sc_returned_pair` + `sc_returned_pair_event` (owner STOCK_Manager) and `Scarpe_in_Scansia` is frozen; F1 (the sheet-era MVP) is live on Render from `feat/scansia-manager` but never ran a production mutation. Build state (2026-10-06): §4.2 done (suite 459 green); §4.3–§4.6 not written.

**Next** (plan §4, in order):
- [x] §4.1 — owed push landed 2026-10-06: `ea9db46..e3c2bff` on `feat/scansia-manager` (the 3 foreign commits on D30's confirmation, typed by `scansia-f6` on Ale's behalf; no app code), upstream set, re-read by `git ls-remote`.
- [x] §4.2 (2026-10-06) — `RegisterPort` + `RegisterStore` in `persistence/ports.py`, `Register` (`register.py`), the fake `memory_store.py`, the lease client `services/lease.py`, `services/variants.py` (exact size, twins, customisation-only); publish on the register (`register_factory`), its tests the parity oracle, the two `frozen_pre` tests retired; layering and parameters in `docs/f7-build-notes.md`. Deferred, stated: pricing / delete / init and their tests leave `FakeSheet` with their §3.4/§3.5 rewrite (§4.4); the API's injected audit factory with the MySQL audit sink (§4.3).
- [~] §4.3 — code done 2026-10-06: `persistence/mysql_store.py` (the store on `mysql-connector-python==9.6.0`), `persistence/audit_sink.py`, production `register_factory` (env `SCANSIA_DB_*`); offline tests through the driver's own substitution (`tests/mysql_offline.py`). OPEN: the store contract on `racoon_staging` (`SCANSIA_STAGING=1 pytest tests/test_register_store.py`, 21 cases skipped today) and the CONSUME-beside-F7 tests, after the orchestrator's INFO that SERVER completed F7.1 (D26; SERVER's deliverable: A01–A04 applied, next A05).
- [~] §4.4 — DONE so far (2026-10-06): the engine `services/ledger.py` (rows, send protocol, outcome maps, settle), `flows.py` (−Δ split, Δ 0, product-total rule, held-key lowering, N-0 release, tail, normalise, orphan), `recovery.py` (both classes), `passes.py` (prologue, `decrement_pass`), publish rewritten on it (`outlet_service.py`: ACTIVE / revive / CREATE rows, one price + compare-at per SKU, D25 pairs kept out, lease yielded between SKUs); tests on a stateful fake Shopify (`tests/fake_shop.py`). TODO: "Inizializza" (§3.4 baseline Level + re-level + Part B), pricing / delete / cleanup / exit / deny-normalize on the register (§3.5), a person's close (iv), B-3's trigger route + boot pass + decrement worker, the API verticals on the register, the rest of §4 item 4's list and §10's minors as tests (AB1–AB3 mandatory).
- [x] F7.2 per §7 — Admin API pinned `2026-10` (2026-10-06; `config.PINNED_API_VERSION`, env may only restate it); written against it: `@idempotent` + `changeFromQuantity` adjusts, explicit activation, `includeInactive`; `productUpdate(product:)` and `Collection.sources` migrated; transport never re-posts a mutation blind (rule 9). Sources and choices: `docs/f7-build-notes.md` §engine.
- [ ] §4.5 — remove §3.8 (`gspread`, `backend/gsheet/`, `src/sync.py`, `fix_prices.py`, `RUN_MODE=SYNC`); `requirements.txt` gains the driver, loses `gspread`; the project `CLAUDE.md` still describes the legacy `RUN_MODE` cron jobs and the sheet — rewrite it to the web service on MySQL in the same step.
- [ ] §4.6 — gate-2: `IT/SVILUPPO/docs/adversarial-verify.md`, `mutate-verify.sh` PASS rows, `/ship`; then `[SYNC-SCANSIA→SCANSIA] DONE|BLOCKED` to `scansia-f6`.

**Active constraints & decisions**:
- F7.2 writes nothing to `racoon`, Shopify, Make, production Trello, the Scheduler or Render (env, deploy) — F7.3 is the deploy and the live tests (SERVER + Ale's `confermo`). Reads only where a build step needs a fact, each named below.
- Render `scansia-manager` deploys **branch `main`, autoDeploy on commit** (SERVER's reading, `IT/SERVER/_deliverables/scansia-f7-2026-10.md`; confirmed live by `scansia-f6` 2026-10-06 14:23Z: live deploy = `00c7682` = `origin/main`) — against `infra-inventory.md:47` and plan F7.3's "autoDeploy is OFF" (the orchestrator's to correct; Ale picks the deploy route before F7.3). So **never push or merge to `main`: it deploys to production at once** (D27: nothing goes to production in F7.2). `feat/scansia-manager` deploys nothing: F7 work goes there under this lane's rules (§4.1's push has landed). M2's "notify SERVER on each push" assumed the service tracked this branch: pushes here are reported to `scansia-f6`, which relays if needed.
- Lane decisions 2026-10-06 (sent to `scansia-f6`, `~/.claude/racoon-bus/msg/20261006-1547-sync-scansia-scansia.md`): the pass lease lives in its own one-row table `sm_outlet_lease` (seeded by the DDL; app SELECT + UPDATE only); requested a staging-only `scansia_fixture_stg` (DML + EXECUTE on the staging SPs) for the integration tests. D28 (orchestrator): staging gets its own app user `scansia_mgr_stg`; `scansia_mgr` is granted on `racoon` only. `sm_outlet_audit` DDL as SERVER's A08/B01.
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project CLAUDE.md rule 2).
- Working tree: the uncommitted `.claude/settings.json` change (drops the project-level `inject-model-policy.sh` hook, also registered at user level) is not this programme's — left as found, never committed by this lane. `.claude/settings.local.json` stays untracked.
- `origin/main` = `00c7682` (2026-09-28, TRACKER-only "main copy stale") is not in this branch: a merge to `main` conflicts on `TRACKER.md` — keep this branch's side.

**Reads made in F7.2 (facts for build steps)**: none yet.

**Tried & discarded (still relevant)**:
- The sheet as store (Q-persistenza, 2026-07-06): voided by the F6 cutover — the register is MySQL.
- Level-setting sweeps ("set Promo to the shelf count"): they put back units sold during an intake stall (plan §3.3).

**Key refs**: plan `docs/f7-mysql-adapter-plan.md` (§3 design, §4 build list, §10 review minors); the lane's mechanisms, parameters and build facts `docs/f7-build-notes.md` (lease statements, users, `GROUP_ID`, margins — read before §4.2); `backend/persistence/ports.py`; `backend/services/`; `docs/testing.md` (venv for the FastAPI suite); F1 history, its owner questions (Q-*) and follow-ups (1)–(8): `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
