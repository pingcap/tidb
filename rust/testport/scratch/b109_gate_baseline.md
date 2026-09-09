# b109 gate baseline (recorded before any edits)

Command (from `rust/`):
    cargo nextest run --locked -p tidb-executor -E 'not test(/bench/)' --no-fail-fast

Result (clean tree @ 6caa7fb1, HEAD of testport/tikv2-b109):
    Summary [ 135.507s] 1229 tests run: 1166 passed, 63 failed, 7 skipped

Failure set (63, exact nextest ids as `<binary>::<test>`):

baseline-failing: tidb-executor::access_path::tests::a_common_handle_row_in_reads_only_the_named_prefixes
baseline-failing: tidb-executor::access_path::tests::explain_analyze_act_rows_reflect_the_truncation
baseline-failing: tidb-executor::access_path::tests::the_double_read_issues_one_batch_get_per_index_batch
baseline-failing: tidb-executor::driver::tests::aggregates::aggregate_having_and_order_by
baseline-failing: tidb-executor::driver::tests::aggregates::distinct_range_orders_gos_hash_agg_over_reader_tree
baseline-failing: tidb-executor::driver::tests::aggregates::grouped_partial_count_carries_the_group_key
baseline-failing: tidb-executor::driver::tests::aggregates::joined_integer_sum_uses_root_stream_agg
baseline-failing: tidb-executor::driver::tests::aggregates::select_distinct
baseline-failing: tidb-executor::driver::tests::aggregates::tpcc_condition_eight_uses_index_join_and_carries_warehouse_ytd
baseline-failing: tidb-executor::driver::tests::aggregates::tpcc_condition_eleven_pushes_filters_through_nested_derived_joins
baseline-failing: tidb-executor::driver::tests::aggregates::tpcc_condition_four_streams_across_a_grouped_derived_table
baseline-failing: tidb-executor::driver::tests::aggregates::tpcc_condition_nine_rebuilds_grouped_history_over_index_lookup
baseline-failing: tidb-executor::driver::tests::aggregates::tpcc_condition_six_simplifies_and_pushes_through_derived_tables
baseline-failing: tidb-executor::driver::tests::aggregates::tpch_q14_matches_recorded_hash_join_plan
baseline-failing: tidb-executor::driver::tests::aggregates::tpch_q1_splits_avg_and_sorts_the_restored_output
baseline-failing: tidb-executor::driver::tests::aggregates::tpch_q3_keeps_go_projections_around_grouped_topn
baseline-failing: tidb-executor::driver::tests::dml::limit_zero_dml_still_validates_the_statement
baseline-failing: tidb-executor::driver::tests::join_reorder::the_advanced_greedy_defers_non_equality_edges_until_the_second_round
baseline-failing: tidb-executor::driver::tests::joins::index_join_probe_rows_use_only_the_access_paths_join_keys
baseline-failing: tidb-executor::driver::tests::joins::tpcc_check_five_keeps_only_the_cross_leaf_residual
baseline-failing: tidb-executor::driver::tests::joins::tpcc_check_seven_propagates_the_warehouse_range_to_both_leaves
baseline-failing: tidb-executor::driver::tests::joins::tpcc_customer_warehouse_join_uses_two_point_gets
baseline-failing: tidb-executor::driver::tests::joins::tpcc_stock_level_bounds_both_join_leaves
baseline-failing: tidb-executor::driver::tests::mem_quota::selection_cached_chunk_is_part_of_the_query_quota
baseline-failing: tidb-executor::driver::tests::point_get::a_handle_point_with_an_extra_conjunct_wins_over_the_unique_index_like_go
baseline-failing: tidb-executor::driver::tests::point_get::a_point_plan_keys_by_the_constant_in_the_columns_domain
baseline-failing: tidb-executor::driver::tests::point_get::exact_handle_range_uses_the_go_cop_projection_tree
baseline-failing: tidb-executor::driver::tests::point_get::ordered_handle_range_keeps_the_go_cop_projection_below_sort
baseline-failing: tidb-executor::driver::tests::point_get::prepared_fast_point_get_binds_common_handle_without_cloning_template
baseline-failing: tidb-executor::driver::tests::predicate_pushdown::a_single_table_read_ends_in_the_cop_task_go_prints
baseline-failing: tidb-executor::driver::tests::predicate_pushdown::tpch_q6_selection_keeps_go_conditions_and_cardinality_after_pruning
baseline-failing: tidb-executor::driver::tests::predicate_pushdown::unknown_table_is_rejected
baseline-failing: tidb-executor::driver::tests::primary_keys::the_clustered_index_mode_decides_the_handle
baseline-failing: tidb-executor::driver::tests::select_clauses::an_empty_correlated_having_subquery_is_null_and_drops_its_row
baseline-failing: tidb-executor::driver::tests::select_clauses::an_unknown_column_names_its_clause
baseline-failing: tidb-executor::driver::tests::select_clauses::plain_having_filters_and_sees_only_the_select_list
baseline-failing: tidb-executor::driver::tests::set_operations::a_distinct_fixpoint_dedups_incrementally
baseline-failing: tidb-executor::driver::tests::set_operations::a_recursive_block_is_cast_into_the_seed_schema
baseline-failing: tidb-executor::driver::tests::set_operations::common_table_expressions
baseline-failing: tidb-executor::driver::tests::set_operations::single_use_cte_explain_keeps_base_statistics_and_multiple_uses_materialize
baseline-failing: tidb-executor::driver::tests::subqueries::a_having_subquery_may_only_correlate_to_the_aggregations_output
baseline-failing: tidb-executor::driver::tests::subqueries::correlated_avg_predicate_decorrelates_to_grouped_join
baseline-failing: tidb-executor::driver::tests::subqueries::correlated_exists_under_or_is_explainable
baseline-failing: tidb-executor::driver::tests::subqueries::correlated_subqueries
baseline-failing: tidb-executor::driver::tests::subqueries::correlated_sum_predicate_pulls_above_unique_outer_join
baseline-failing: tidb-executor::driver::tests::subqueries::explaining_a_correlated_scalar_type_reads_no_storage
baseline-failing: tidb-executor::driver::tests::subqueries::grouped_correlated_subqueries
baseline-failing: tidb-executor::driver::tests::subqueries::grouped_in_subquery_reuses_its_unique_group_output
baseline-failing: tidb-executor::driver::tests::subqueries::plain_explain_evaluates_and_labels_an_uncorrelated_scalar_subquery
baseline-failing: tidb-executor::driver::tests::subqueries::subqueries
baseline-failing: tidb-executor::driver::tests::subqueries::tpcc_conditions_ten_and_twelve_decorrelate_scalar_sums
baseline-failing: tidb-executor::driver::tests::subqueries::tpcds_q10_correlated_exists_under_or_is_explainable
baseline-failing: tidb-executor::driver::tests::subqueries::tpch_q16_non_null_not_in_is_an_anti_semi_join
baseline-failing: tidb-executor::driver::tests::subqueries::tpch_q2_correlated_min_matches_recorded_hash_join_plan
baseline-failing: tidb-executor::driver::tests::table_round_trip::count_star_field_keeps_its_written_label
baseline-failing: tidb-executor::driver::tests::through_proj::a_computed_key_over_two_relations_is_declined
baseline-failing: tidb-executor::driver::tests::through_proj::an_equality_inside_one_relation_is_not_injected
baseline-failing: tidb-executor::driver::tests::through_proj::an_expression_join_key_gets_an_injected_column
baseline-failing: tidb-executor::driver::tests::through_proj::the_index_joins_outer_leaf_is_asked_for_the_order_through_the_derived_table
baseline-failing: tidb-executor::driver::tests::through_proj::the_injected_wrapper_is_pruned_and_its_leaf_takes_the_covering_index
baseline-failing: tidb-executor::hash_agg_spill_tests::test_get_correct_result
baseline-failing: tidb-executor::remote_scan::tests::a_pushed_predicate_does_not_move_an_aggregate_value
baseline-failing: tidb-executor::remote_scan::tests::an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request
