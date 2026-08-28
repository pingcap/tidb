# b118 baseline gate (clean tree, before any edits)

Gate command (from `rust/`):

```
cargo nextest run --locked -p tidb-session -E 'not test(/bench/)' --no-fail-fast
```

## Run 1 — clean tree (baseline failure list, 123 unique failures)

Output: `Starting 1385 tests across 2 binaries (16 tests skipped)`, run FAILED.
123 distinct failing tests recorded verbatim in the b118 receipt
(`baseline-failing:` lines).

## Run 2 — clean tree, rerun (130 failures)

Same command, same clean tree (changes stashed): `1385 tests run: 1255
passed, 130 failed, 16 skipped`. The failure set is run 1's set PLUS these
seven, all in modules this batch never touches:

- tidb-session tests_partition::every_unsigned_handle_predicate_reads_the_same_rows_ranged_as_filtered
- tidb-session tests_union_all_predicate_push_down::a_union_all_side_can_drive_an_index_join
- tidb-session tests_union_scan::a_staged_delete_alone_marks_the_table
- tidb-session tests_union_scan::a_staged_insert_alone_marks_the_table
- tidb-session tests_union_scan::a_staged_update_alone_marks_the_table
- tidb-session tests_union_scan::rows_that_tie_on_the_index_key_come_back_in_handle_order
- tidb-session tests_window::specs::window_feeds_the_ordinary_pipeline

`tests_union_scan.rs:213` asserts row order that comes from a std HashMap
iteration (per-process random seed), so these flip run to run independent of
any edit. Verified: the CLEAN tree's run 2 produces the exact 130-failure
set, and the after-edits gate produces an IDENTICAL 130-failure set (`diff`
empty). Subset property holds in both directions against run 2.

## After edits

`cargo nextest run --locked -p tidb-session -E 'not test(/bench/)'
--no-fail-fast` → `1396 tests run: 1266 passed, 130 failed, 19 skipped`
(+11 = this batch's new passing tests, +3 = this batch's new `#[ignore]`
gap tests). Failure set identical to clean-tree run 2.
