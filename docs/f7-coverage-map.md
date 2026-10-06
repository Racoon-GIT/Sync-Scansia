# F7 — plan §4 item 4: the coverage map

Every item of `docs/f7-mysql-adapter-plan.md` §4 item 4 (the contract, sha `6dadcdd7…`), mapped to the test that pins
it, plus the test cases the orchestrator's verdicts named (D16). Built 2026-10-06 (F7.2, §4.4's last step, before
§4.6). Each pinned test was read, not matched by name; the items no other module pinned live in
`tests/test_plan_coverage.py`, each named by its id below. Suite at the map's commit: 808 offline green + 28 on
`racoon_staging` (opt-in). The map changed code where an item had no mechanism behind it (§10 below).

Module keys: `cov` test_plan_coverage · `dec` test_decrement · `out` test_outlet_service · `r8` test_round8_minors ·
`reg` test_register · `store` test_register_store · `mysql` test_mysql_store · `lease` test_lease · `base`
test_baseline · `rel` test_relevel · `del` test_delete_service · `exit` test_exit_service · `norm`
test_normalize_service · `close` test_close_service · `price` test_pricing_service · `trig` test_decrement_trigger ·
`stg` test_consume_beside_f7 (staging) · `tr` test_transport · `ops` test_ops · `read` test_api_read · `init`
test_api_init · `gate` test_promo_gate.

## 1. §3.3's table, held keys included

| Id | Row / mechanism | Pinned by |
|---|---|---|
| T1 | Δ < 0, ACTIVE or DRAFT: relative lower by min(\|Δ\|, available) | `dec` a_full_price_sale_lowers_relatively_once_and_drafts_at_zero; a_minus_two_split_over_two_outlets_is_one_group (a DRAFT lowered) |
| T1a | available ≤ 0 → stamp only; a negative never raised | `dec` available_at_or_below_zero_stamps_without_touching_shopify; a_negative_available_is_never_raised |
| T1b | Promo `committed` > 0 → pending (rule 4) | `dec` a_committed_unit_keeps_the_group_pending; `cov` aa7 (then applied / stamped) |
| T1c | product total 0 on ACTIVE → DRAFT; customisation-only never counts | `dec` a_full_price_sale_…_drafts_at_zero; customisation_only_units_do_not_keep_an_outlet_active |
| T1d | no variant titled SIZE → pending, never stamped — unless the N-0 release | `dec` a_size_absent_group_stays_pending_never_stamped; `cov` z11_aa9_aa10_m13 [size_absent] |
| T2 | none (a clean 0-match answer) → stamp | `dec` no_outlet_on_a_clean_answer_stamps |
| T2a | a truncated or failed resolution is not none | `dec` a_failed_resolution_is_not_none… (truncated); `cov` m24 (raising) |
| T2b | clean multi-outlet −Δ: holders of the size, ACTIVE first, split | `dec` a_multi_outlet_lowering_takes_the_active_outlet_first; a_minus_two_split_… |
| T2c | split group: stamped only when every row is APPLIED; FAILED → CARRIED + remainder; HELD → CARRIED, events held | `cov` w4_z5_aa5 [refused, crash, premise]; `r8` ab1_a_minus_split_member_closed_by_a_person… |
| T2d | a multi-outlet key is never added to | `cov` t2; `base` a_multi_outlet_key_is_held_and_lowered_by_its_excess… |
| T3 | Δ = 0: the product-total rule, stamp | `dec` an_outlet_line_sale_runs_the_product_total_rule_and_stamps; `cov` t0 (held key) |
| T4 | Δ > 0: FIX4 on price and compare-at | `out` fix4_invalid_price_blocks_publish; a_pair_with_no_compare_at_is_kept_out…_d25; compare_at_is_price_high_else_price_b2 |
| T4a | relative, `changeFromQuantity`, keyed, no level precondition (`frozen_pre` retired) | `out` an_add_is_relative_fenced_and_keyed_with_no_level_precondition; active_does_not_reapply_a_stamped_event |
| T4b | every exposed variant DENY + tracked first | `out` every_exposed_variant_is_deny_and_tracked_before_the_add; create_branch_exact_order |
| T4c | a revive deferred while a row not closed names its product or a key | `out` a_revive_is_deferred_while_a_row_of_its_product_is_open; `cov` aa2 (a tail row) |
| T4d | a revive zeroes no-shelf-pair, held-key and customisation-only variants, relatively | `out` draft_revive_adds_on_the_live_level_then_activates_last; a_revive_lowers_customisation_only_and_held_keys_to_zero; `cov` m3 |
| T4e | later steps only once every zeroing row applied | `out` a_revive_stopped_at_a_zeroing_row…; `r8` ab3 ×3 |
| T4f | activation: absent / inactive only, 0/0; the add only once it is APPLIED | `out` activation_is_explicit_and_only_for_an_absent_or_inactive_level; an_add_is_never_sent_beside_an_activation_that_failed |
| T4g | the tail row: written ahead, closed on completion, FAILED on a refusal or "never run" | `r8` ab3 ×3; `cov` aa1_a_revives_tail_refused…; w8_w13_m4 |
| T4h | size the outlet lacks / SKU not in the catalogue: pending, surfaced | `out` a_spelling_twin_is_surfaced_never_published; customisation_only_variant_never_matches; `cov` t4 |
| T4i | a held key's +Δ, a +Δ of a key with N 0: never added | `out` a_held_positive_key_is_never_added; a_positive_group_of_a_key_with_n0_is_never_added; `cov` z11 [pending_plus] |
| T0 | a held key: post-hold events only; Δ < 0 and Δ = 0 run, Δ > 0 waits | `reg` post_hold_events_of_a_held_key_stay_readable; `dec` the_held_key_lowering_counts_a_pending_minus_as_leaving; `cov` t0 |
| T0a | the held-key lowering: clamp(P) + Σ pending (net of CARRIED) + Σ open sent −Δ − N | `dec` a_quiet_held_key_above_n…; the_held_key_lowering_counts_a_pending_minus…; `cov` z1; aa14 |
| T0b | a key held only by a tail / normalise row: neither read nor lowered | `r8` ab8 |
| N0 | the N-0 release: N 0, P ≤ 0 everywhere, no row open, no HELD row in retention; before grouping; events read first | `dec` the_n0_release ×3; d33 ×2; `cov` z11_aa9_aa10_m13 ×5; aa11 |

## 2. Rule 2's recovery cases

| Id | Case | Pinned by |
|---|---|---|
| R1 | APPLIED, group complete → the stamp retried (after a stamp failure) | `cov` m11_r1 [stamp_retry] |
| R2 | APPLIED beside a FAILED sibling → CARRIED | `cov` w4_z5_aa5; `r8` ab7_ii_on_an_applied_row… |
| R3 | APPLIED while a sibling is open → waits | `r8` ab1_an_activation_applied_beside_its_add… (APPLIED beside OPEN); ab1_a_minus_split_member… |
| R4 | FAILED on `userErrors`, events pending | `dec` a_stale_change_from_quantity_fails_the_row_and_the_next_pass_re_plans |
| R5 | OPEN −Δ landed / not landed → re-sent identical, re-applied past the window | `dec` a_lost_answer_leaves_the_row_open…; a_send_that_never_ran…; past_the_window_an_unknown_minus… |
| R6 | OPEN +Δ → HELD, key held | `out` a_lost_add_answer_is_never_re_added_and_holds_the_key_past_the_window |
| R7 | CREATE: `userErrors` at each step after the duplicate; a crash in each window; id recorded or not; a sale between crash and recovery; recovery at start-up (decrement class: left), before a pass, after a stamp failure | `cov` r7_a_create_refused… ×7; r7_w8_w13_a_crash… ×7; m11_r1; `out` a_create_failing_after_step_1…; a_lost_duplicate_answer…; create_quarantines_when_levels_truncated… |
| R8 | close + stamp atomic, fenced, in order | `reg` stamp_closes_its_rows_then_stamps_in_one_transaction; `mysql` fenced_write_is_renew_then_ops_then_commit_register_last |
| R9 | an already-stamped id counted by the matched count | `store` stamp_coalesces_and_counts_an_already_stamped_id_as_matched |
| R10 | the stamp by PRIMARY KEY beside CONSUME (EXPLAIN) | `stg` a_consume_beside_an_open_stamp_never_waits_and_the_stamp_is_by_primary_key |

## 3. The fenced lease

| Id | Case | Pinned by |
|---|---|---|
| F1–F2 | a crashed holder taken over; a live one refused | `lease` a_crashed_holder_is_taken_over_once_expired; `store` take_increments_generation_and_refuses_a_live_holder |
| F3–F5 | a stalled holder after a take-over, right after its renew: write-ahead refused, send past the local deadline refused | `lease` a_stalled_holder_right_after_its_renew_cannot_write; send_deadline_counts_from_the_renew_start; `store` a_stale_generation_is_refused_after_a_take_over |
| F6 | a send that slips through, answered by the idempotency key | `cov` f6 |
| F7 | a decrement between two SKUs of a publish / re-level; the next SKU re-derived | `out` the_publish_yields_the_lease_between_skus…; `cov` f7_aa12; z14 |
| F8 | never inside a baseline (refused before its `BASELINE` row) | `base` the_decrement_is_refused_until_the_baseline_row_exists; the_baseline_keeps_the_lease_throughout… |
| F9 | a dead generation's never-sent row closed without holding | `dec` a_dead_generations_never_sent_row_closes_failed_without_holding |

## 4. The rest of item 4

| Id | Item | Pinned by |
|---|---|---|
| M1 | no mutation re-posted blind (rule 9) | `tr` a_mutation_whose_answer_was_lost…; …5xx…; …internal_server_error…; …partial_data…; …429…; …throttled…; …refused_4xx…; …failing_validation… |
| M2 | `IDEMPOTENCY_CONCURRENT_REQUEST` / `…_KEY_PARAMETER_MISMATCH` (first send) | `cov` m2 ×3 |
| M3 | revive zeroing relative under `committed` > 0 | `cov` m3 |
| M4 | a revive's unfinished tail: re-run by a publish-class recovery, left by the decrement's | `cov` w8_w13_m4 |
| M5 | F7's writes beside `SP_SCANSIA_CONSUME`, CONSUME's wait bounded to one F7 commit | `stg` both tests |
| M6 | exact size; the normalised-only match surfaced | `out` a_spelling_twin_is_surfaced_never_published; `store` sizes_are_exact_not_normalised; `read` join_matches_sizes_exactly_and_surfaces_a_twin |
| M7 | a size-absent group pending, never stamped | `dec` a_size_absent_group_stays_pending_never_stamped |
| M8–M9 | DENY + tracked before exposure; customisation-only normalised, positive → 0, negative left | `out` every_exposed_variant…; `norm` apply_makes_every_variant_deny_and_tracked…; a_negative_custom_level_is_left…; `base` continue_and_untracked_variants_are_normalised_before_any_raise |
| M10 | explicit activation only before a raising write | `out` activation_is_explicit…; `base` a_activates_an_absent_or_inactive_level…; `cov` aa8_a_lowering_of_an_inactive_level… |
| M11 | the product-total rule on Δ = 0 and on re-applies | `dec` an_outlet_line_sale…; `cov` m11_r1 ×3 |
| M12 | §3.4's invariant on every key or held — at confirm (moved, committed, multi-outlet, size-absent, twin) and by recovery | `base` the_invariant_holds_on_every_key_of_a_mixed_register; a_live_level_that_moved…; a_upward_write_meeting_committed…; a_multi_outlet_key_is_held…; a_spelling_twin_holds… (size-absent + twin); recovery_runs_first_at_the_take… |
| M13 | held keys released only by a re-level, the N-0 release (size present / absent) or the in-flight row's close | `reg` a_per_key_baseline_releases_older_holds_only; an_in_flight_hold_ends_with_its_row…; `rel` a_size_absent_hold_is_released…; `dec` the_n0_release_stamps_and_releases…; `cov` z11 [size_absent] |
| M14–M15 | a held key lowered by its excess, never raised; a quiet held key lowered by the next decrement | `dec` a_quiet_held_key_above_n_is_lowered_by_its_excess_never_raised; `base` a_held_key_short_of_n_is_never_raised |
| M16 | a stalled +1 landing on a held key after its −1 ran stamp-only | `cov` m16 |
| M17 | a revive zeroing every held key's real-size variant | `out` a_revive_lowers_customisation_only_and_held_keys_to_zero |
| M18 | every Level branch stamps the events of departed pairs | `base` every_level_branch_stamps_the_events_of_pairs_no_longer_on_the_shelf |
| M19 | one price and compare-at per SKU | `out` one_price_per_sku…; a_new_return_with_no_price…; active_refreshes_price_and_compare_at…; `price` two_compare_ats_on_one_sku_list_and_block_it |
| M20 | `ONLINE` set by the baseline, the publish, a re-level, a revive | `base` online_is_set_from_the_result…; `out` online_is_set_on_the_pairs_whose_plus_one_applied; `rel` a_size_absent_hold_is_released…; `cov` z17 |
| M21 | one audit row per register write | `reg` every_register_write_is_one_transaction_with_its_audit_row; outlet_product_set_on_shelf_and_cleared_in_any_state |
| M22 | `available` ≤ 0 | `dec` available_at_or_below_zero…; a_negative_available_is_never_raised |
| M23 | customisation-only excluded from counts | `dec` customisation_only_units…; `base` a_customisation_only_variant_never_counts_as_a_key; `read` join_zero_promo_is_sold_out_and_customisation_only_never_counts |
| M24 | a failed / multi / truncated resolution ≠ none | `dec` a_failed_resolution_is_not_none…; `cov` m24; t2 |
| M25 | a multi-outlet −Δ on the holders of the size, ACTIVE first | `dec` a_multi_outlet_lowering_takes_the_active_outlet_first |
| M26 | `BASELINE` missing → refusal | `reg` pending_refuses_before_the_whole_register_baseline; `out` preview_refuses_before…; `dec` the_reader_refuses_the_decrement_before_the_baseline; `rel` the_precondition_and_the_baseline_gate… |
| M27 | Part B writes `ONLINE` only; no statement names `STATE`, `CONSUMED_AT`, `CONSUME_KEY` | `base` part_b_lists_the_online_pairs_against_shopify_and_writes_nothing; `mysql` a_pair_update_sets_only_granted_columns; `store` pair_update_refuses_columns_outside_the_grant |
| M28 | autocommit asserted, no locking read, every `UPDATE` by PRIMARY KEY | `mysql` connect_pins_tls_by_ip_found_rows_and_autocommit; refuses_a_session_with_autocommit_off; no_locking_read_and_every_update_by_primary_key; `stg` EXPLAIN (PRIMARY) |

## 5. Rounds 5–6 (§10 W, Y)

| Id | Item | Pinned by |
|---|---|---|
| W1 | the re-send window's edge: just inside → identical re-send; just past → re-applied | `cov` w1 ×2; w1_z7_z8 |
| W2 | the past-window −Δ re-applied as a new row, the old `SUPERSEDED` | `dec` past_the_window_an_unknown_minus_is_re_applied_as_a_new_row |
| W3 | a re-send answered by each class (cached; STALE; PREVIOUS_ATTEMPT_FAILED; INVALID_; CONCURRENT; any other; INVALID_INVENTORY_ITEM) × (−Δ, +Δ) | `dec` a_lost_answer… (cached); concurrent_on_the_one_in_window_re_send…; `cov` w3_z9_aa4 ×7; w3_z10 ×7 |
| W4 | a split group with a FAILED sibling: no stamp, CARRIED, the remainder only | `cov` w4_z5_aa5 ×3 |
| W5 | an activation APPLIED beside a FAILED +Δ | `cov` w5 |
| W6 | an activation's `userErrors` resolved on `isActive` | `cov` w6 (active → APPLIED); `out` an_add_is_never_sent_beside_an_activation_that_failed (inactive → FAILED) |
| W7 | a not-closed row's key held, its events excluded, never re-grouped under a new nonce | `reg` every_hold_kind_is_derived_and_its_events_subtracted; `dec` a_lost_answer… (one key, one body) |
| W8 | `SENT_AT` before `productDuplicate` and each tail call; a crash right after it | `cov` r7_w8_w13 [duplicate…]; w8_w13_m4; `r8` ab5_a_normalise_rows_sent_at… |
| W9 | the stored request re-sent byte-identical | `dec` a_lost_answer…; `store` request_text_comes_back_byte_identical; `tr` send_body_posts_the_stored_bytes_unchanged |
| W10 | activation only of an absent or inactive level, 0/0 | `out` activation_is_explicit…; `ops` activate_request_is_explicit_zero_and_keyed |
| W11 | a revive's activation, adds and tail only after its zeroing applied | `out` a_revive_stopped_at_a_zeroing_row…; `r8` ab3 ×3 |
| W12 | event-less rows past the window: re-derived from live | `cov` w12_aa1 ×3 |
| W13 | the decrement's recovery leaves sent +Δ / CREATE / tail rows; reads an activation off `isActive` | `out` the_decrements_recovery_leaves_a_sent_plus…; `cov` r7_w8_w13; w8_w13_m4; `r8` ab13 |
| W14 | a decrement waiting on a held lease; the hand-off between SKUs, within its bound | `lease` the_waiting_decrement_is_handed_the_lease_between_skus; the_decrement_ends_its_run_past_its_bound; `cov` z14 |
| W15 | a re-derived +Δ larger than confirmed: deferred | `out` a_re_derived_plus_larger_than_confirmed_is_deferred |
| W16 | the held-key lowering on a SKU absent from the plan | `cov` w16 |
| W17 | an outlet whose gid or status changed since the confirm: deferred | `out` toctou_branch_drift_aborts_verify_failed (status); `cov` w17 (gid) |
| W18 | a re-level of a mixed pending mix, and of a key not held (DRAFT-parked) | `cov` w18; `rel` a_draft_parked_key_not_held_is_levelled_to_n… |
| W19 | `HOLD` rows: the delete's hold, released by a re-level that recreates | `del` a_deleted_outlets_key_takes_no_add_until_a_relevel; `cov` w19 |
| W20 | the multi-outlet exit | `exit` ×8; `cov` s7 |
| W21 | the N-0 release | §1 N0 |

## 6. Round 7 (§10 Z)

| Id | Item | Pinned by |
|---|---|---|
| Z1 | a timed-out −Δ not landed, then the sweep: P = N (the open −Δ counted); its hold ended by its close | `cov` z1 |
| Z2–Z3 | a revive deferred by an open row; a zeroing row FAILED or OPEN → nothing exposed, events pending | `out` a_revive_is_deferred…; a_revive_stopped_at_a_zeroing_row…; `r8` ab3_a_revive_ended_at_a_zeroing_row_still_open… |
| Z4 | a group's write-ahead atomic | `reg` write_ahead_is_all_or_nothing |
| Z5 | a crash after it, before a sibling's send | `cov` w4_z5_aa5 [crash] |
| Z6 | a `SUPERSEDED` member's successor inherits its `GROUP_ID` | `reg` supersede_writes_the_successor…; `dec` past_the_window… |
| Z7–Z8 | a re-send rewrites neither `SENT_AT` nor `SENT_AVAILABLE`; re-sent every pass until first send + 23 h | `cov` w1_z7_z8; `store` audit_update_guards_status_and_first_send |
| Z9 | an unknown −Δ re-applied at once after its one in-window re-send | `cov` w3_z9_aa4 [timeout_before, concurrent, other]; `dec` concurrent_on_the_one_in_window… |
| Z10 | a +Δ re-send answered STALE → HELD | `cov` w3_z10 [STALE] |
| Z11 | the N-0 release: size present, multi-outlet, refused inside a HELD row's retention | `dec` the_n0_release ×3; `cov` z11 [multi_outlet] |
| Z12 | a deactivation's outcome read off `isActive` | `r8` ab10 |
| Z13 | every holder yields to a waiting decrement (cleanup, deny-normalize included) | `out` the_publish_yields…; `rel` a_run_of_keys_yields_the_lease_between_skus; `price` pricing_holds_the_lease_yields_between_skus…; `del` cleanup_yields_between_products…; `norm` normalize_yields_between_products; `exit` the_exit_yields_between_outlets |
| Z14 | the decrement on its own worker, interleaved with a publish in one process | `cov` z14 (real threads); `trig` the_trigger_never_waits_for_the_jobs_slot |
| Z15 | a customisation-only Promo > 0 lowered by a revive (S13 is SQL, §5) | `out` a_revive_lowers_customisation_only_and_held_keys_to_zero |
| Z16 | a never-sent row closed FAILED under the decrement | `dec` a_dead_generations_never_sent_row… |
| Z17 | `ONLINE` = 1 set by a re-level and a revive | `rel` a_size_absent_hold_is_released…; `cov` z17 |

## 7. Round 8 (§10 AA) and its targeted re-review (AB)

| Id | Item | Pinned by |
|---|---|---|
| AA1 | a terminal path for every row kind; a delete of each kind closing its product's rows; a person's close → HELD | `r8` ab10; ab16; ab5_a_sent_add_whose_product_reads_gone…; ab7 ×2; `norm` a_refused_normalisation_on_an_active_outlet…; `cov` aa1 ×3; w12_aa1 [level_gone]; `del` terminal_path_iii…; a_delete_closes_a_sent_add…; `close` a_persons_close_holds_the_add… |
| AA2 | a tail / normalise row holds its keys; a second revive deferred by its id | `cov` aa2; `out` a_revive_is_deferred… |
| AA3 | the bulk cleanup lists apart an outlet whose SKU has a shelf pair, at preview and apply | `del` cleanup_lists_an_outlet_whose_sku_has_a_shelf_pair_apart…; cleanup_rechecks_each_product_right_before_its_delete |
| AA4 | a −Δ re-send closed FAILED on STALE; its events' next lowering surfaced | `cov` w3_z9_aa4 [STALE, INVALID_INVENTORY_ITEM] |
| AA5 | a premise contradicted at rule 8's read → that row FAILED, never sent | `cov` w4_z5_aa5 [premise] |
| AA6 | a group stamped whose rows repeat its event ids | `reg` stamp_closes_its_rows_then_stamps_in_one_transaction; `dec` a_minus_two_split_… |
| AA7 | a −Δ at 0 under `committed` > 0: pending, applied when released, stamped when shipped | `cov` aa7 ×2; `dec` d33 ×2 |
| AA8 | a +Δ never sent beside an activation not APPLIED; an activation re-send withheld once active; a lowering of an inactive level, no activation | `out` an_add_is_never_sent_beside…; `r8` ab13; `cov` aa8 ×3 |
| AA9–AA11 | the N-0 release at P < 0; before grouping with a pending +Δ; a pair arriving between its reads | `cov` z11 [negative, pending_plus]; aa11 |
| AA12 | a re-take with and without another holder; a run of re-levels under one sweep | `lease` first_take_and_retakes_report_whether_another_holder_came_between; `cov` aa12; f7_aa12 |
| AA13 | each holder's recovery class | `cov` aa13 ×7 (every §3.5 vertical: decrement class) and ×8 (behaviour per class); `price` pricing_recovery_is_decrement_class…; `base` recovery_runs_first_at_the_take… |
| AA14 | the lowering net of a `CARRIED` share (S13: SQL, §5) | `cov` aa14 |
| AB1–AB16 | the targeted re-review | `r8` (module docstring maps each); `close` (AB4, AB11, AB15); `rel` (AB14); `norm` + `close` (AB2) |

## 8. B-3 (D23)

The trigger coalesces; a duplicate and a late trigger no-op; the boot pass applies a pending −Δ; the trigger never waits
for the jobs' slot; its own bearer, fail-closed: `trig` ×9. The call's failure paths in 678434 (5 s timeout,
`onerror` Ignore, asleep) are AUTOMATE's (F7.5): this repo answers 202 at once and runs the pass on its own worker.

## 9. The orchestrator's judged interpretations, as test cases (D16)

"Inizializza" (`~/.claude/racoon-bus/msg/20261006-2032-scansia-f6-sync-scansia.md`, all five accepted):

| Id | Test case | Pinned by |
|---|---|---|
| OJ1 | a confirm after a restart refused, nothing written; a moved key held, the others applied | `init` a_plan_this_process_no_longer_keeps_is_refused; `cov` oj1 |
| OJ2 | a unit on an outlet of no register SKU listed as its own class (product, size, live P), lowered relatively; its customisation-only Promo by rule 7 | `cov` oj2; `base` a_stale_unit_is_lowered_to_zero_on_any_outlet_never_held |
| OJ3 | N 0 + `committed` 1: held at the baseline, refused by a re-level; `committed` 0, P ≤ 0 → released by a pass, nothing written | `base` a_key_with_no_shelf_pair_and_a_committed_unit…; `rel` refusals_leave_the_key_as_it_is; `cov` oj3 |
| OJ4 | (i) N 1, P 1 on a DRAFT, +1 pending → zero, −1; (ii) the parked branch writes nothing and is listed for a re-level; (iii) shelf events ≠ N → held | `cov` oj4; `base` b_a_draft_parked_key_is_left_at_n_not_held (class `parked`); shelf_events_that_do_not_sum_to_n_hold_the_key |
| OJ5 | a re-level with a priceless shelf pair refused whole, the pair named; passes once priced | `cov` oj5 |

§3.5 (`~/.claude/racoon-bus/msg/20261006-2050-scansia-f6-sync-scansia.md`, all ten accepted):

| Id | Test case | Pinned by |
|---|---|---|
| S1 | before the baseline a holder's take skips the lowering, surfaced; after it, the lowering runs | `cov` s1 ×2; `price` pricing_runs_before_the_baseline_with_the_held_key_lowering_skipped |
| S2 | a typed price resolves a two-price SKU; repair push and publish blocked until then | `price` direct_mode_typed_price_resolves…; direct_repair_with_two_register_prices…; `cov` s2 |
| S3 | 20 % on 189 → 151.20, `DISCOUNT` 20.00 | `cov` s3; `price` percent_mode_prices_on_the_compare_at… |
| S4 | no id → register only; a DRAFT outlet takes the price at its revive, before ACTIVE | `price` no_outlet_product_id_writes_the_register_only…; `cov` s4 |
| S5 | a refused push: register new, Shopify old, surfaced; the next ACTIVE publish sends it | `price` a_refused_push_leaves_the_register_written_and_surfaced; `cov` s5 |
| S6 | revert: a sold pair keeps its price; a later pricing → `SKIP_CHANGED_SINCE`; a NULL prior restored | `price` revert_never_touches_a_pair_sold_since; revert_leaves_a_sku_repriced_since…; revert_restores_every_pairs_prior… |
| S7 | the exit drafts nothing; a STOPPED exit leaves its outlet ACTIVE, listed, nothing deleted | `exit` apply_lowers_every_positive…; a_committed_unit_stops_the_exit…; `cov` s7 |
| S8 | a DRAFT CONTINUE outlet normalised, then deletable; a customisation-only Promo risen since the preview lowered from live | `cov` s8 ×2 |
| S9 | a wrong anchor → 503, lease not taken, nothing written; a failed `locations` read → no pass, the next trigger runs it | `gate` ×3; `cov` s9 |
| S10 | a CREATE's deactivation whose location reads gone → APPLIED, the group completes; a lowering → FAILED | `cov` s10; w12_aa1 [level_gone] |

## 10. What the map changed in code

| Gap found | Change | Pinned by |
|---|---|---|
| A −Δ that recovery landed (in-window re-send, re-apply) or re-stamped never ran the product-total rule (rule 2: "then stamp + close, run the product-total rule") | `recovery.recover` runs it on each product a recovered −Δ lowered, after the settle | `cov` m11_r1 ×3 |
| A CREATE's own step rows were recovered by the generic bullets after its orphan was zeroed: a sent, unlanded add was re-sent onto the drafted orphan | "a CREATE's rows follow the CREATE path": `flows.orphan` closes the CREATE's open step rows (never sent → FAILED; sent +Δ → HELD; other → FAILED) | `cov` r7_w8_w13 [adjust-4] |
| DRAFT-parked keys were listed as `zero` with Δ 0, not as needing a re-level (§3.4's preview list; OJ4 (ii)) | action `parked` (preview table, counts; nothing written) | `base` b_a_draft_parked_key_is_left_at_n_not_held; `cov` oj4 |
| The re-level's D25 refusal did not name the priceless pair (OJ5) | `fix4:no_compare_at:<pair ids>` | `cov` oj5 |

## 11. Not this repo's to pin

S13 and the standing watch (SQL, plan §5, at go-live); 678434's HTTP module (AUTOMATE, F7.5); the Render → VPS path,
Shopify's live idempotency answers (cached result vs error, CONCURRENT's shape) and "definitive null" at the pin
(F7.3, live, under Ale's `confermo`).
