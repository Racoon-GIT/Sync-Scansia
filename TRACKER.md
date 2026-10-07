# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: build · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: pending

**Phase**: F7.2 (build) of the programme "Scansia → MySQL" — orchestrator `scansia-f6`, mandate `IT/_scansia-programme/briefs/sync-scansia-F7.2.md`, programme state `IT/_scansia-programme/PROGRAM.md`. Gate-1: D25 (`gate1-receipts.log` line 661); spawn D27; fresh contexts D36, D37, D38 (`a9277e56`). The plan is the contract (sha `6dadcdd7…`, `71dfc51`): never edited here — a statement that looks wrong goes to the orchestrator. Register: `racoon.sc_returned_pair` + `sc_returned_pair_event` (owner STOCK_Manager) since the F6 cutover (2026-09-21); `Scarpe_in_Scansia` frozen, read by no code since §4.5. Build state (2026-10-07): §4.1–§4.5 done; suite 779 offline green (venv) + 28 on `racoon_staging` (opt-in; last staging run 2026-10-06 19:45Z, 28/28 — nothing since touched store code); bare interpreter 648 + 159 skipped (`docs/testing.md`).

**Next** (plan §4, in order):
- [x] §4.1–§4.4 — push settled, ports + fake, MySQL store + audit sink, the engine and every §3 vertical on the register, the coverage map (`docs/f7-coverage-map.md`). Mechanisms: `docs/f7-build-notes.md`.
- [x] §4.5 (`7d6f001`) — §3.8 removed: the sheet code, its wiring, error codes, tests and deps (`gspread`, Google auth, + `pandas` / `python-slugify` / `tenacity`, imported by nothing); `RUN_MODE=REORDER` kept; `CLAUDE.md`, `README.md`, `docs/testing.md` rewritten; build notes §4.5. The orchestrator's two readings (21:17Z verdict: both accepted, 0 changed) are test cases RD1 / RD2 (map §9).
- [ ] §4.6 — gate-2 (`IT/SVILUPPO/docs/adversarial-verify.md`): set A (the author's 15 mutants: §4.5's code + the RD seams) **15/15 PASS on `7d6f001`** in `docs/gate-2-log.md`. Set V (≥ 45 mutants by a fresh verifier, blind to the tests): the first verifier died on an API error (2026-10-07 09:46Z), nothing to reuse — re-spawn it from `~/.claude/racoon-bus/msg/sync-scansia-gate2/verifier-mandate.txt` (how-to on its first lines), save its JSON lines as `verifier_mutants.jsonl`, `build_queue.py <repo> <V ids>`, stash the foreign `.claude/settings.json`, `drive.sh queue.tsv 1 run-V`, pop and compare the sha with the backup.
- [ ] Each survivor → a test, or the equivalence argued in the map; re-run that mutant + a control on the new sha (adversarial-verify §Rules).
- [ ] GATE-2 prompt on the final code, grade written here; `/ship` = a local commit (`docs/gate-2-log.md` with the rest; no push in F7.2); `[SYNC-SCANSIA→SCANSIA] DONE|BLOCKED` to `scansia-f6`.

**GATE-2 prompt, self-grade (2026-10-07, context `a9277e56`, on §4.5 + RD1/RD2)**: **9/10 → 10/10** — read on the diff: a retired-mode branch in `main.py` changed only a log line (no test could tell it apart: removed — `SYNC` / `FIX_PRICES` are unknown modes, exit 1); `ops.py` / `resolvers.py` cited lines of a deleted file (now `git show 8aab2a3:src/sync.py`); the removed sheet cases were the only pins of subclass-before-base ordering at the boundary (now the register family, map + HTTP 502); test count reconciled exactly (819 − 46 removed + 6 RD = 779); pyflakes clean; the 15 set-A mutants all killed. Earlier grades (engine, "Inizializza", §3.5, the map): TRACKER at `643cefd`; the engine is graded by set V.

**Active constraints & decisions**:
- F7.2 writes nothing to `racoon`, Shopify, Make, production Trello, the Scheduler or Render — F7.3 is the deploy and the live tests (SERVER + Ale's `confermo`). Integration tests on `racoon_staging` only (`scansia_mgr_stg` / `scansia_fixture_stg`, option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (`IT/SERVER/infra-inventory.md:47`) — **never push or merge to `main`**. `feat/scansia-manager` stays local in F7.2 (`scansia-f6`'s call); it reaches the remote at F7.3 under Ale's decision, with earlier contexts' commits (`guard-foreign-push` needs Ale's typed `FOREIGN_PUSH_OK`). `origin/main` = `00c7682` (TRACKER-only) is not in this branch: a merge conflicts on `TRACKER.md` — keep this branch's side.
- For F7.3, SERVER / Ale: delete the suspended `Sync-Scansia` cron (`render.yaml` stays until then) and the web service's `GSPREAD_*` / Google env vars — no code reads them. E18: `PROMO_LOCATION_ID` may be the GID or the numeric id; every writing pass refuses (503 `promo_anchor_invalid`) unless it names the location called "Promo".
- D28: staging has its own app user; `scansia_mgr` is `racoon` only. D33: the N-0 release waits while an exact-size Promo level has `committed > 0`.
- One connect retry (`mysql_store.CONNECT_ATTEMPTS`): from the Mac a connect to `78.46.244.227:3306` fails now and then (errno 2003, OS 60); the Render → VPS path is F7.3's to measure.
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project `CLAUDE.md` rule 2).
- Working tree: the uncommitted `.claude/settings.json` (sha `638e2594…`) is not this programme's — left as found; stashed only while `mutate-verify.sh` runs (receipts are `-dirty` otherwise), restored byte-identical (backup in the gate-2 folder).

**Reads made in F7.2 (facts for build steps)**: `racoon` none. `racoon_staging` 2026-10-06 (as the staging users): server status and `sm_outlet_lease` column types (probe), the post-run counts of `f7test` rows; facts in `docs/f7-build-notes.md` §Facts.

**Tried & discarded (still relevant)**:
- The sheet as store (Q-persistenza, 2026-07-06): voided by the F6 cutover — the register is MySQL.
- Level-setting sweeps ("set Promo to the shelf count"): they put back units sold during an intake stall (plan §3.3).
- A Shopify-only price revert: the next publish re-sends the register's price on every variant, undoing it.
- Bypassing `mutate-verify.sh`'s thermal refusal: a throttled run forges kills — the Mac drops to `CPU_Speed_Limit` 27–70 under the suite; `drive.sh` waits for 100.

**Key refs**: plan `docs/f7-mysql-adapter-plan.md`; `docs/f7-coverage-map.md` (gate-2's checklist); `docs/f7-build-notes.md`; receipts `docs/gate-2-log.md`; gate-2 tools `~/.claude/racoon-bus/msg/sync-scansia-gate2/` (`mut.py`, `build_queue.py`, `drive.sh`, `author_mutants.py`, `verifier-mandate.txt`); `docs/testing.md` (a venv while `/private/tmp` lasts: `/private/tmp/claude-501/-Users-alesap-DATI-Racoon-LAB-IT-SVILUPPO-Sync-Scansia/a9277e56-fd5c-4790-ada2-895a41a4cf57/scratchpad/venv/bin/python`; a reboot empties it — rebuild per `docs/testing.md`); F1 history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
