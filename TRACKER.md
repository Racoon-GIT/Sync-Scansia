# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: shipped · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: Ale 2026-10-08

**Phase**: F7 **live**; F7.3 closed 2026-10-09. `feat/scansia-manager` (`7b2c18d`) merged into `main` as `ff0894f` (a merge commit, parents `00c7682` + `7b2c18d`; its tree is `7b2c18d`'s, and `1356027..7b2c18d` touches `TRACKER.md` + `docs/` only: the code is gate-2's); tip `619e6ae`, pushed once under `GATE:push` D42 after `/ship` (steps 0–7 pass; lint and lockfile n/a) — `ls-remote` `main` = `619e6ae` at 08:52:54Z. The deploy, verified by the orchestrator `scansia-f6` (its DONE, 2026-10-09): Render deploy `dep-db4amps9v7es73af6qgg` on `619e6ae` live 08:53:52Z; `/health` 200; the boot pass stopped at `BaselineMissing` (config, Promo gate, MySQL TLS and the lease passed — the expected stop before F7.4); `POST /outlet/decrement/trigger` without a bearer → 401. Programme "Scansia → MySQL": state `IT/_scansia-programme/PROGRAM.md`. The plan is the contract (sha `6dadcdd7…`, `71dfc51`): never edited here — a statement that looks wrong goes to the orchestrator. Register: `racoon.sc_returned_pair` + `sc_returned_pair_event` (owner STOCK_Manager) since the F6 cutover (2026-09-21); `Scarpe_in_Scansia` frozen, read by no code. Suite on `619e6ae` (2026-10-09, venv on Python 3.13): 795 passed, 28 skipped — all 28 the staging opt-in; the Render pin is 3.12.4 (`.python-version`; `runtime.txt` agrees): `vermin -t=3.12-` on `backend/` + `main.py`, no violation (minimum 3.9).

**Next**:
- [x] F7.2 — the engine and every §3 vertical on the register, the sheet removed (`7d6f001`); gate-2 D41 on `1356027` (receipts `docs/gate-2-log.md`; GATE-2 self-grade 10/10, its reasons in the TRACKER at `1356027`).
- [x] F7.3 — the merge, `/ship`, D42's one push, the orchestrator's deploy checks (above). The stage's GATE-2 prompt on this work: no code written in F7.3 — the merge's tree is `7b2c18d`'s, so gate-2's 10/10 stands; runtime parity with 3.12.4 was static only here (no 3.12 on the Mac), and the first boot on Render reached `BaselineMissing` (the orchestrator's read).
- [x] F7.3's live write tests (D44 list, D45 run): runner on local branch **`f73-live-runner` @ `dc06b32`** (new files only: `f73_live/`, `tests/`); run `202610091404` — COMPLETED, residue empty; FAIL S2.10, S2.14, S3.4, S3.5, S3.8, T5 refused (S3.9–S3.11 skipped): the orchestrator judges them, a code change comes as a new brief. Log `~/.claude/racoon-bus/msg/f73-live-202610091404/`.
- [ ] §8 F3 (an outlet F7 creates has no images): fixed on local branch **`fix/f7-outlet-media` @ `3352fcb`** (from `ba2d713`: `b4a4c35` `includeImages: true` on D1, `imageJob` on the CREATE row; `3352fcb` its gate-2 `PASS` row; suite 799 / 28) — before F7.4's first CREATE; its push is its own `GATE:push`; a live check proposed (`…-1350-…-f3-fix.md`), not covered by D45's run.
- [ ] Not granted, each its own gate: F7.4's baseline ("Inizializza") — until then every decrement pass stops at `BaselineMissing`; the suspended cron's deletion and the `GSPREAD_*` / Google env vars' removal (SERVER / Ale).

**Active constraints & decisions**:
- No write to `racoon`, Shopify, Make, production Trello, the Scheduler or Render outside a recorded gate; D45's run of the list is done — this lane runs nothing live. Integration tests on `racoon_staging` only (`scansia_mgr_stg` / `scansia_fixture_stg`, option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (`IT/SERVER/infra-inventory.md:47`): every push to `main` is a deploy — `GATE:push`, except a docs-only diff (`IT/session-protocol.md` §7). `render.yaml` is the suspended `Sync-Scansia` cron: it stays until SERVER / Ale delete the cron.
- Branches: `origin/main` = `619e6ae` (F7 live); local `main` carries docs only on top of it (this TRACKER, the list, the build notes), not pushed (D46: no push — every push to `main` is a deploy). Local only, not pushed: `f73-live-runner` (`dc06b32`) and `fix/f7-outlet-media` (`3352fcb`), both above. The runner pins `619e6ae`'s `ops.py`, `resolvers.py`, `transport.py`, `config.py` (sha256): with the F3 fix in the tree it refuses to start and its pin test fails. Local `feat/scansia-manager` (`7b2c18d`) is 27 ahead of `origin/feat/scansia-manager` (`e3c2bff`), contained in `main`: no work left on it.
- E18: `PROMO_LOCATION_ID` may be the GID or the numeric id; every writing pass refuses (503 `promo_anchor_invalid`) unless it names the location called "Promo". D28: staging has its own app user; `scansia_mgr` is `racoon` only. D33: the N-0 release waits while an exact-size Promo level has `committed > 0`.
- One connect retry (`mysql_store.CONNECT_ATTEMPTS`): from the Mac a connect to `78.46.244.227:3306` fails now and then (errno 2003, OS 60); from Render the first boot connected (MySQL TLS passed — the orchestrator's read, 2026-10-09).
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project `CLAUDE.md` rule 2).
- Working tree: the uncommitted `.claude/settings.json` (sha `638e2594…`) is not this programme's — left as found (carried across the switch to `main`: same committed blob on both sides, sha and mtime unchanged).

**Reads made in F7.3**: none on `racoon` or `racoon_staging`; on Shopify only R1–R5 for the test list (read-only, 2026-10-09 10:11Z, run by `scansia-f6`'s `shopify-operator` at this lane's ASK — `docs/f7.3-live-tests.md` §4.1); D45's run itself (its writes and reads: the run log above).

**Tried & discarded (still relevant)**:
- The sheet as store (Q-persistenza, 2026-07-06): voided by the F6 cutover — the register is MySQL.
- Level-setting sweeps ("set Promo to the shelf count"): they put back units sold during an intake stall (plan §3.3).
- A Shopify-only price revert: the next publish re-sends the register's price on every variant, undoing it.
- Bypassing `mutate-verify.sh`'s thermal refusal: a throttled run forges kills — the Mac drops to `CPU_Speed_Limit` 81–97 under the suite; `drive.sh` waits for 100 and stops after 40 attempts (exit 9: resume at the `--start` its last log names).

**Key refs**: plan `docs/f7-mysql-adapter-plan.md`; `docs/f7-coverage-map.md` (gate-2's checklist; §12 = set V's survivors); `docs/f7-build-notes.md`; receipts `docs/gate-2-log.md`; gate-2 tools `~/.claude/racoon-bus/msg/sync-scansia-gate2/` (`mut.py`, `build_queue.py`, `drive.sh`, `author_mutants.py`, `verifier-mandate.txt`, `verifier_mutants.jsonl`, the `run-*` logs); `docs/testing.md` (the venv lives in a session scratchpad and a reboot empties it — rebuild it there from `/usr/local/bin/python3`, 3.13, then point `drive.sh`'s `SUITE` at it); F1 history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
