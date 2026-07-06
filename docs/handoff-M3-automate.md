# Handoff → AUTOMATE (Make) — return-signal integration (unblocks M3)

Scansia Manager = new single-origin FastAPI web service replacing the 3 Sync-Scansia cron jobs (already suspended). Branch `feat/scansia-manager`. Library layer complete (175 tests); this handoff unblocks deploy/integration.

> AUTOMATE is not one of the 3 IT peers (SVILUPPO/SERVER/CLIENT) → this is an owner/Ale action or a future AUTOMATE session, NOT an `OPEN-HANDOFFS` peer row.

## Requests

- [ ] Make does TWO things on the sheet: (1) **draft-on-sale** (drafts the outlet listing when it sells out), (2) **return-append** (appends a return row for each return: SKU, Size, qty). Confirm Make's appends are the ONLY writes Make makes to the sheet.
- [ ] **Integration contract**: define the merge/check contract; who owns the `reconciled` flag; the tool consumes each return row as a one-shot idempotent delta (`row_uuid` + `reconciled` → +qty to Promo once).
- [ ] **Schema coordination (M3 gate)**: agree on the position of the `row_uuid`/`reconciled` columns; **confirm Make appends by header NAME, not fixed column index** (otherwise inserting columns silently misaligns its writes). If in doubt → pin the new columns to the RIGHT of Make's append range.
- [ ] **Cutover backfill (BLOCKING M3)**: on the FIRST ingestion, mark ALL pre-existing rows `reconciled=true` (stock already live) → otherwise every outlet's stock gets re-inflated. DoD: migrating a sheet of already-published outlets, the first run applies ZERO stock deltas.

Blocks: M3
