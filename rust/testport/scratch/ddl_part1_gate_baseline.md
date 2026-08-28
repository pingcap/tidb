# b100 gate baseline (clean tree, before any edits)

Command (from `rust/`):
`cargo nextest run --locked -p tidb-executor -E 'not test(/bench/)' --no-fail-fast`

Summary: `1273 tests run: 1264 passed (1 slow), 9 failed, 7 skipped`

Baseline failing set (full nextest ids):

- tidb-executor::driver::tests::aggregates::global_count_over_index_ranges_uses_gos_stream_agg_and_index_reader
- tidb-executor::driver::tests::aggregates::grouped_partial_count_carries_the_group_key
- tidb-executor::driver::tests::aggregates::tpcc_grouped_common_handle_uses_partial_and_final_stream_agg
- tidb-executor::driver::tests::aggregates::tpcc_condition_two_orders_group_uses_the_covering_index_range
- tidb-executor::driver::tests::aggregates::tpcc_condition_four_streams_across_a_grouped_derived_table
- tidb-executor::driver::tests::aggregates::tpcc_condition_six_simplifies_and_pushes_through_derived_tables
- tidb-executor::driver::tests::joins::tpcc_check_seven_propagates_the_warehouse_range_to_both_leaves
- tidb-executor::driver::tests::point_get::residual_selection_uses_logical_rows_over_access_rows
- tidb-executor::driver::tests::subqueries::tpcc_conditions_ten_and_twelve_decorrelate_scalar_sums

Scope derivation (same deterministic rule as b069/b078): all 1002
`func Test*` / `func Benchmark*` under `pkg/ddl/**` on `origin/master`, sorted
by (file path, line number), chunked into groups of 60. Part1 = items 1-60:
affinity_test.go (1-5), attributes_sql_test.go (6-13), backfill_metrics_test.go
(14-16), backfilling_dist_scheduler_test.go (17-21), backfilling_test.go
(22-31), backfilling_txn_executor_test.go (32), bdr/bdr_test.go (33-35),
bench_test.go (36-37), cancel_test.go (38-40), cluster_test.go (41-44),
column_change_test.go (45-47), column_modify_test.go (48-56), column_test.go
(57-60).
