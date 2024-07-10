// Copyright 2023 PingCAP, Inc.
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

package variable

var isHintUpdatableVerified = map[string]struct{}{
	"tidb_opt_agg_push_down":                           {},
	"tidb_opt_derive_topn":                             {},
	"tidb_opt_broadcast_cartesian_join":                {},
	"tidb_opt_mpp_outer_join_fixed_build_side":         {},
	"tidb_opt_distinct_agg_push_down":                  {},
	"tidb_opt_skew_distinct_agg":                       {},
	"tidb_opt_three_stage_distinct_agg":                {},
	"tidb_broadcast_join_threshold_size":               {},
	"tidb_broadcast_join_threshold_count":              {},
	"tidb_prefer_broadcast_join_by_exchange_data_size": {},
	"tidb_opt_write_row_id":                            {},
	"tidb_optimizer_selectivity_level":                 {},
	"tidb_enable_new_only_full_group_by_check":         {},
	"tidb_enable_outer_join_reorder":                   {},
	"tidb_enable_null_aware_anti_join":                 {},
	"tidb_replica_read":                                {},
	// This var is used during the planner's preprocess phase. Need to fix its behavior.
	// "tidb_read_staleness":                              {},
	"tidb_enable_paging":                              {},
	"tidb_read_consistency":                           {},
	"tidb_distsql_scan_concurrency":                   {},
	"tidb_opt_insubq_to_join_and_agg":                 {},
	"tidb_opt_prefer_range_scan":                      {},
	"tidb_opt_enable_correlation_adjustment":          {},
	"tidb_opt_limit_push_down_threshold":              {},
	"tidb_opt_correlation_threshold":                  {},
	"tidb_opt_correlation_exp_factor":                 {},
	"tidb_opt_cpu_factor":                             {},
	"tidb_opt_copcpu_factor":                          {},
	"tidb_opt_tiflash_concurrency_factor":             {},
	"tidb_opt_network_factor":                         {},
	"tidb_opt_scan_factor":                            {},
	"tidb_opt_desc_factor":                            {},
	"tidb_opt_seek_factor":                            {},
	"tidb_opt_memory_factor":                          {},
	"tidb_opt_disk_factor":                            {},
	"tidb_opt_concurrency_factor":                     {},
	"tidb_opt_force_inline_cte":                       {},
	"tidb_opt_use_invisible_indexes":                  {},
	"tidb_index_join_batch_size":                      {},
	"tidb_index_lookup_size":                          {},
	"tidb_index_serial_scan_concurrency":              {},
	"tidb_init_chunk_size":                            {},
	"tidb_allow_batch_cop":                            {},
	"tidb_allow_mpp":                                  {},
	"tidb_enforce_mpp":                                {},
	"tidb_max_bytes_before_tiflash_external_join":     {},
	"tidb_max_bytes_before_tiflash_external_group_by": {},
	"tidb_max_bytes_before_tiflash_external_sort":     {},
	"tidb_max_chunk_size":                             {},
	"tidb_min_paging_size":                            {},
	"tidb_max_paging_size":                            {},
	"tidb_enable_cascades_planner":                    {},
	"tidb_merge_join_concurrency":                     {},
	"tidb_index_merge_intersection_concurrency":       {},
	"tidb_opt_projection_push_down":                   {},
	"tidb_enable_vectorized_expression":               {},
	"tidb_opt_join_reorder_threshold":                 {},
	"tidb_enable_index_merge":                         {},
	"tidb_enable_extended_stats":                      {},
	"tidb_isolation_read_engines":                     {},
	"tidb_executor_concurrency":                       {},
	"tidb_partition_prune_mode":                       {},
	"tidb_enable_index_merge_join":                    {},
	"tidb_enable_ordered_result_mode":                 {},
	"tidb_enable_pseudo_for_outdated_stats":           {},
	"tidb_stats_load_sync_wait":                       {},
	"tidb_cost_model_version":                         {},
	"tidb_index_join_double_read_penalty_cost_rate":   {},
	"tidb_default_string_match_selectivity":           {},
	"tidb_enable_prepared_plan_cache":                 {},
	"tidb_enable_non_prepared_plan_cache":             {},
	"tidb_plan_cache_max_plan_size":                   {},
	"tidb_opt_range_max_size":                         {},
	"tidb_opt_advanced_join_hint":                     {},
	"tidb_opt_prefix_index_single_scan":               {},
	"tidb_store_batch_size":                           {},
	"mpp_version":                                     {},
	"tidb_enable_inl_join_inner_multi_pattern":        {},
	"tidb_opt_enable_late_materialization":            {},
	"tidb_opt_ordering_index_selectivity_threshold":   {},
	"tidb_opt_ordering_index_selectivity_ratio":       {},
	"tidb_opt_enable_mpp_shared_cte_execution":        {},
	"tidb_opt_fix_control":                            {},
	"tidb_runtime_filter_type":                        {},
	"tidb_runtime_filter_mode":                        {},
	"tidb_session_alias":                              {},
	"tidb_opt_objective":                              {},
	"mpp_exchange_compression_mode":                   {},
	"tidb_allow_fallback_to_tikv":                     {},
	"tiflash_fastscan":                                {},
	"tiflash_fine_grained_shuffle_batch_size":         {},
	"tiflash_fine_grained_shuffle_stream_count":       {},
	// Variables that is compatible with MySQL.
	"cte_max_recursion_depth": {},
	"sql_mode":                {},
	"max_execution_time":      {},
}

func setHintUpdatable(vars []*SysVar) {
	for _, v := range vars {
		if _, ok := isHintUpdatableVerified[v.Name]; ok {
			v.IsHintUpdatableVerified = true
		}
	}
}
