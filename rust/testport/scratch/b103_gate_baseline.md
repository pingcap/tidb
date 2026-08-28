# b103 gate baseline (clean tree, branch testport/pod1-b103 @ 95357f9f6f)

Command: cargo nextest run --locked -p tidb-executor -E 'not test(/bench/)' --no-fail-fast
Summary: 1273 tests run: 1264 passed, 9 failed, 7 skipped

baseline-failing: tidb-executor::driver::tests::aggregates::global_count_over_index_ranges_uses_gos_stream_agg_and_index_reader
baseline-failing: tidb-executor::driver::tests::aggregates::grouped_partial_count_carries_the_group_key
baseline-failing: tidb-executor::driver::tests::aggregates::tpcc_condition_two_orders_group_uses_the_covering_index_range
baseline-failing: tidb-executor::driver::tests::aggregates::tpcc_grouped_common_handle_uses_partial_and_final_stream_agg
baseline-failing: tidb-executor::driver::tests::aggregates::tpcc_condition_six_simplifies_and_pushes_through_derived_tables
baseline-failing: tidb-executor::driver::tests::aggregates::tpcc_condition_four_streams_across_a_grouped_derived_table
baseline-failing: tidb-executor::driver::tests::joins::tpcc_check_seven_propagates_the_warehouse_range_to_both_leaves
baseline-failing: tidb-executor::driver::tests::point_get::residual_selection_uses_logical_rows_over_access_rows
baseline-failing: tidb-executor::driver::tests::subqueries::tpcc_conditions_ten_and_twelve_decorrelate_scalar_sums
