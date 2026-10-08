# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: gate-2 · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: pending

**Phase**: F7.2 (build) of the programme "Scansia → MySQL" — orchestrator `scansia-f6`, mandate `IT/_scansia-programme/briefs/sync-scansia-F7.2.md`, programme state `IT/_scansia-programme/PROGRAM.md`. Gate-1: D25 (`gate1-receipts.log` line 661); spawn D27; fresh contexts D36–D39 (D39: context `edc42fc1`, 2026-10-07 20:20Z). The plan is the contract (sha `6dadcdd7…`, `71dfc51`): never edited here — a statement that looks wrong goes to the orchestrator. Register: `racoon.sc_returned_pair` + `sc_returned_pair_event` (owner STOCK_Manager) since the F6 cutover (2026-09-21); `Scarpe_in_Scansia` frozen, read by no code since §4.5. Build state (2026-10-08): §4.1–§4.6 done; suite 795 offline green (venv) + 28 on `racoon_staging` (opt-in; last staging run 2026-10-06 19:45Z, 28/28 — nothing since touched store code); bare interpreter 664 + 159 skipped (`docs/testing.md`).

**Next**:
- [x] §4.1–§4.5 — push settled, ports + fake, MySQL store + audit sink, the engine and every §3 vertical on the register, the coverage map, the sheet removed (`7d6f001`). Mechanisms: `docs/f7-build-notes.md`.
- [x] §4.6 — gate-2 (`IT/SVILUPPO/docs/adversarial-verify.md`), receipts in `docs/gate-2-log.md`: set A (the author's 15) 15/15 PASS on `7d6f001`; set V (70 by a fresh verifier blind to the tests) 60 PASS + 10 SURVIVOR on `0f18084`; the 10 pinned by tests (`49399fd`, map §12) and re-run with a control: 11/11 PASS on `49399fd`. `/ship` = the local commit carrying this TRACKER (no push in F7.2).
- [ ] `[SYNC-SCANSIA→SCANSIA] DONE` to `scansia-f6`, right after that commit (`~/.claude/racoon-bus/log.md` records the attempt). Then the lane waits: the `gate-2` field and `GATE:2` with Ale are the orchestrator's (`IT/session-protocol.md` §3); F7.3 (deploy, live tests, the push of this branch) is SERVER's + Ale's.

**GATE-2 prompt, self-grade (2026-10-08, context `edc42fc1`, on the engine as set V graded it + this context's 16 cases)**: **9/10 → 10/10**. Before: the suite killed 60 of 70 blind mutants; the 10 survivors were gaps of the F7 diff (`git blame`), none a code defect — wiring (the send path's keepalive), defaults every holder runs on (keepalive 60 s, send margin 60 s, the 10 s throttle cap), branches nobody ran (a size the plan never confirmed; a truncated level set on ACTIVE / DRAFT; the revert of a repair-mode SKU), half a predicate (DENY but untracked), a written field (`possibly_applied` on a failed re-apply), an orphan-path status (a CREATE's sent add `HELD`, whose retention the N-0 release waits out). After: each a test (`49399fd`), its divergent path named in map §12, killed in a sandbox first, then by `mutate-verify.sh` (11/11 with the control V37). Read whole — ledger, recovery, flows, lease, passes — no defect; one defensive branch left as is: `send_activate` / `send_deactivate` / `step` renew without re-reading past the send deadline, unreachable by the arithmetic (keepalive ≤ 60 s + the longest read ≈ 177 s < 240 s) — changing it would re-open all 85 mutants for a path no clock reaches. pyflakes clean on the changed files (2 old warnings in `src/reorder_collection.py`, outside F7). Earlier grades: TRACKER at `0f18084` (§4.5) and `643cefd`.

**Active constraints & decisions**:
- F7.2 writes nothing to `racoon`, Shopify, Make, production Trello, the Scheduler or Render — F7.3 is the deploy and the live tests (SERVER + Ale's `confermo`). Integration tests on `racoon_staging` only (`scansia_mgr_stg` / `scansia_fixture_stg`, option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (`IT/SERVER/infra-inventory.md:47`) — **never push or merge to `main`**. `feat/scansia-manager` stays local in F7.2 (`scansia-f6`'s call); it reaches the remote at F7.3 under Ale's decision, with earlier contexts' commits (`guard-foreign-push` needs Ale's typed `FOREIGN_PUSH_OK`). `origin/main` = `00c7682` (TRACKER-only) is not in this branch: a merge conflicts on `TRACKER.md` — keep this branch's side. A merge carrying `gate-2-log.md` receipts is a merge commit, never a rebase or squash (the receipts name pre-merge shas).
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
- Bypassing `mutate-verify.sh`'s thermal refusal: a throttled run forges kills — the Mac drops to `CPU_Speed_Limit` 81–97 under the suite; `drive.sh` waits for 100 and stops after 40 attempts (exit 9: resume at the `--start` its last log names).

**Key refs**: plan `docs/f7-mysql-adapter-plan.md`; `docs/f7-coverage-map.md` (gate-2's checklist; §12 = set V's survivors); `docs/f7-build-notes.md`; receipts `docs/gate-2-log.md`; gate-2 tools `~/.claude/racoon-bus/msg/sync-scansia-gate2/` (`mut.py`, `build_queue.py`, `drive.sh`, `author_mutants.py`, `verifier-mandate.txt`, `verifier_mutants.jsonl`, the `run-*` logs); `docs/testing.md` (a venv while `/private/tmp` lasts: `/private/tmp/claude-501/-Users-alesap-DATI-Racoon-LAB-IT-SVILUPPO-Sync-Scansia/a9277e56-fd5c-4790-ada2-895a41a4cf57/scratchpad/venv/bin/python`, named in `drive.sh` too; a reboot empties it — rebuild per `docs/testing.md`); F1 history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
