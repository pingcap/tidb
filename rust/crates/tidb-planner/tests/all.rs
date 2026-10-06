// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! All topology-independent `tidb-planner` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod cardinality_avg_col_size_source;
mod cardinality_exponential_backoff_source;
mod cardinality_live_index_choice_source;
mod cardinality_mock_stats_ranges_source;
mod cardinality_ndv_skew_source;
mod cardinality_selectivity_greedy_source;
mod cascades_base_hash_equaler_source;
mod casetest_logicalplan_builder_source;
mod casetest_parallel_apply_suite_source;
mod casetest_physicalplantest_hint_plans_source;
mod clustered_signed_bigint_ranger_source;
mod configured_multi_relation_catalog_source;
mod core_expression_eval_source;
mod core_logical_cte_topn_prune_source;
mod core_logical_plans_source;
mod core_rule_list_flag_alignment_source;
mod cost_factors_source;
mod explain_source;
mod fts_resolve_index_source;
mod hint_optimizer_cost_factor_setvar_scenarios_source;
mod logicalop_hash64_equals_source;
mod logicalop_hash64_expand_apply_join_agg_source;
mod logicalop_rule_util_copy_on_write_source;
mod logicalop_window_frame_handlecols_identity_source;
mod parser_util_consumer_source;
mod physical_bigint_selection_source;
mod physicalop_final_mode_agg_split_source;
mod physicalop_memory_trace_clone_stream_count_source;
mod planner_util_index_col_projection_source;
mod planner_util_path_compare_lengths_source;
mod prepared_dml_source;
mod prepared_param_marker_source;
mod read_only_bigint_selection_source;
mod read_only_clustered_pk_range_source;
mod read_only_prepared_order_source;
mod read_only_scan_source;
mod redact_explain_limit_redaction_source;
mod row_count_estimator_source;
mod rule_inject_extra_projection_wrap_cast_source;
mod rule_partition_pruning_pruner_source;
mod selectivity_pseudo_source;
mod tests_pointget_plan_cache_source;
mod tiflash_mpp_root_shape_source;
mod tikv_table_read_task_runtime_source;
mod wired_scan_reader_plan_source;
