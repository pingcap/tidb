# pkg/ddl part7 (b106) gate baseline — recorded BEFORE any edits

Tree: clean `testport/tikv2-b106` at 26b6b8a. Command:

```
cargo nextest run --locked -p tidb-executor -E 'not test(/bench/)' --no-fail-fast
Summary [  51.123s] 1277 tests run: 1267 passed, 10 failed, 7 skipped
```

Baseline failing set (full nextest ids, exact):

- tidb-executor::driver::tests::aggregates::global_count_over_index_ranges_uses_gos_stream_agg_and_index_reader
- tidb-executor::driver::tests::aggregates::grouped_partial_count_carries_the_group_key
- tidb-executor::driver::tests::aggregates::tpcc_condition_two_orders_group_uses_the_covering_index_range
- tidb-executor::driver::tests::aggregates::tpcc_grouped_common_handle_uses_partial_and_final_stream_agg
- tidb-executor::driver::tests::aggregates::tpcc_condition_six_simplifies_and_pushes_through_derived_tables
- tidb-executor::driver::tests::aggregates::tpcc_condition_four_streams_across_a_grouped_derived_table
- tidb-executor::driver::tests::joins::tpcc_check_seven_propagates_the_warehouse_range_to_both_leaves
- tidb-executor::driver::tests::point_get::residual_selection_uses_logical_rows_over_access_rows
- tidb-executor::driver::tests::predicate_pushdown::a_common_dnf_equality_is_a_hash_join_key
- tidb-executor::driver::tests::subqueries::tpcc_conditions_ten_and_twelve_decorrelate_scalar_sums

Four of the ten are pre-listed in /root/work/known-baseline-failures.txt
(tpcc_condition_four_streams_across_a_grouped_derived_table,
tpcc_check_seven_propagates_the_warehouse_range_to_both_leaves,
residual_selection_uses_logical_rows_over_access_rows,
tpcc_conditions_ten_and_twelve_decorrelate_scalar_sums) as pre-existing
integration-branch failures unrelated to testport batches.

## Batch enumeration note

`origin/master` ref is not present in this offline worktree and the batch
rules forbid remote operations, so the pkg/ddl Test*/Benchmark* enumeration
was computed from the local hparser-integration checkout at
/root/work/tidb/pkg/ddl (file read only; no git commands run there):
978 items sorted by (file path, line number); part7 = items 361-420.
The supervisor's manifest counted 1002 items on true master, so the slice
boundaries may differ from master by a few items at the edges; every test
named in the receipt below is a concrete pkg/ddl test function present in
the read snapshot.
