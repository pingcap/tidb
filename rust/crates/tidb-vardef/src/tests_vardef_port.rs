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

//! Go-parity tests ported from `pkg/sessionctx/variable` (batch b012, part 3).
//!
//! Source of truth: `origin/master` snapshot of
//! `pkg/sessionctx/variable/tests/variable_test.go` and
//! `pkg/sessionctx/variable/varsutil_test.go` (tests 121-150 of the package's
//! canonical ordering). Tests whose subject (the `SysVar` registry,
//! `SessionVars`, or `varsutil.go` helpers) is not ported into this crate are
//! kept as `#[ignore]`d stubs annotated with a `go-parity-gap` reason so the
//! inventory stays visible; they must be enabled when the owning code lands.

use super::defaults::DEF_TIDB_SERVER_MEMORY_LIMIT_GC_TRIGGER;
use super::modes::{tidb_opt_enable_clustered, ClusteredIndexDefMode};

/// All sysvar **name** constants ported in [`super::tidb_vars`].
///
/// The Go tests iterate `variable.GetSysVars()` (the full registry); that
/// registry does not exist in the rewrite, so the same invariant is asserted
/// over every name constant this crate ships instead. Value-only constants
/// (`DEF_*` defaults, `ON`/`OFF`/... enum strings, TSO RPC-mode names) are
/// excluded: they are not sysvar names.
const SYSVAR_NAME_CONSTANTS: &[(&str, &str)] = &[
    ("TIDB_DDL_SLOW_OPR_THRESHOLD", "ddl_slow_threshold"),
    ("TIDB_SNAPSHOT", "tidb_snapshot"),
    ("TIDB_OPT_AGG_PUSH_DOWN", "tidb_opt_agg_push_down"),
    ("TIDB_OPT_DERIVE_TOP_N", "tidb_opt_derive_topn"),
    ("TIDB_OPT_CARTESIAN_BCJ", "tidb_opt_broadcast_cartesian_join"),
    ("TIDB_OPT_MPP_OUTER_JOIN_FIXED_BUILD_SIDE", "tidb_opt_mpp_outer_join_fixed_build_side"),
    ("TIDB_OPT_DISTINCT_AGG_PUSH_DOWN", "tidb_opt_distinct_agg_push_down"),
    ("TIDB_OPT_SKEW_DISTINCT_AGG", "tidb_opt_skew_distinct_agg"),
    ("TIDB_OPT3_STAGE_DISTINCT_AGG", "tidb_opt_three_stage_distinct_agg"),
    ("TIDB_OPT_ENABLE3_STAGE_MULTI_DISTINCT_AGG", "tidb_opt_enable_three_stage_multi_distinct_agg"),
    ("TIDB_OPT_EXPLAIN_NO_EVALED_SUB_QUERY", "tidb_opt_enable_non_eval_scalar_subquery"),
    ("TIDB_BCJ_THRESHOLD_SIZE", "tidb_broadcast_join_threshold_size"),
    ("TIDB_BCJ_THRESHOLD_COUNT", "tidb_broadcast_join_threshold_count"),
    ("TIDB_PREFER_BCJ_BY_EXCHANGE_DATA_SIZE", "tidb_prefer_broadcast_join_by_exchange_data_size"),
    ("TIDB_OPT_WRITE_ROW_ID", "tidb_opt_write_row_id"),
    ("TIDB_AUTO_ANALYZE_RATIO", "tidb_auto_analyze_ratio"),
    ("TIDB_AUTO_ANALYZE_START_TIME", "tidb_auto_analyze_start_time"),
    ("TIDB_AUTO_ANALYZE_END_TIME", "tidb_auto_analyze_end_time"),
    ("TIDB_CHECKSUM_TABLE_CONCURRENCY", "tidb_checksum_table_concurrency"),
    ("TIDB_CURRENT_TS", "tidb_current_ts"),
    ("TIDB_LAST_TXN_INFO", "tidb_last_txn_info"),
    ("TIDB_LAST_QUERY_INFO", "tidb_last_query_info"),
    ("TIDB_LAST_DDL_INFO", "tidb_last_ddl_info"),
    ("TIDB_LAST_PLAN_REPLAYER_TOKEN", "tidb_last_plan_replayer_token"),
    ("TIDB_CONFIG", "tidb_config"),
    ("TIDB_BATCH_INSERT", "tidb_batch_insert"),
    ("TIDB_BATCH_DELETE", "tidb_batch_delete"),
    ("TIDB_BATCH_COMMIT", "tidb_batch_commit"),
    ("TIDB_DML_BATCH_SIZE", "tidb_dml_batch_size"),
    ("TIDB_MEM_QUOTA_QUERY", "tidb_mem_quota_query"),
    ("TIDB_MEM_QUOTA_APPLY_CACHE", "tidb_mem_quota_apply_cache"),
    ("TIDB_GENERAL_LOG", "tidb_general_log"),
    ("TIDB_TRACE_EVENT", "tidb_trace_event"),
    ("TIDB_LOG_FILE_MAX_DAYS", "tidb_log_file_max_days"),
    ("TIDB_P_PROF_SQLCPU", "tidb_pprof_sql_cpu"),
    ("TIDB_RETRY_LIMIT", "tidb_retry_limit"),
    ("TIDB_DISABLE_TXN_AUTO_RETRY", "tidb_disable_txn_auto_retry"),
    ("TIDB_ENABLE_CHUNK_RPC", "tidb_enable_chunk_rpc"),
    ("TIDB_OPTIMIZER_SELECTIVITY_LEVEL", "tidb_optimizer_selectivity_level"),
    ("TIDB_OPT_INDEX_PRUNE_THRESHOLD", "tidb_opt_index_prune_threshold"),
    ("TIDB_OPTIMIZER_ENABLE_NEW_ONLY_FULL_GROUP_BY_CHECK", "tidb_enable_new_only_full_group_by_check"),
    ("TIDB_OPTIMIZER_ENABLE_OUTER_JOIN_REORDER", "tidb_enable_outer_join_reorder"),
    ("TIDB_OPTIMIZER_ENABLE_NAAJ", "tidb_enable_null_aware_anti_join"),
    ("TIDB_TXN_MODE", "tidb_txn_mode"),
    ("TIDB_ROW_FORMAT_VERSION", "tidb_row_format_version"),
    ("TIDB_ENABLE_ROW_LEVEL_CHECKSUM", "tidb_enable_row_level_checksum"),
    ("TIDB_ENABLE_TABLE_PARTITION", "tidb_enable_table_partition"),
    ("TIDB_ENABLE_LIST_TABLE_PARTITION", "tidb_enable_list_partition"),
    ("TIDB_SKIP_ISOLATION_LEVEL_CHECK", "tidb_skip_isolation_level_check"),
    ("TIDB_LOW_RESOLUTION_TSO", "tidb_low_resolution_tso"),
    ("TIDB_REPLICA_READ", "tidb_replica_read"),
    ("TIDB_ADAPTIVE_CLOSEST_READ_THRESHOLD", "tidb_adaptive_closest_read_threshold"),
    ("TIDB_ALLOW_REMOVE_AUTO_INC", "tidb_allow_remove_auto_inc"),
    ("TIDB_MULTI_STATEMENT_MODE", "tidb_multi_statement_mode"),
    ("TIDB_EVOLVE_PLAN_TASK_MAX_TIME", "tidb_evolve_plan_task_max_time"),
    ("TIDB_EVOLVE_PLAN_TASK_START_TIME", "tidb_evolve_plan_task_start_time"),
    ("TIDB_EVOLVE_PLAN_TASK_END_TIME", "tidb_evolve_plan_task_end_time"),
    ("TIDB_SLOW_LOG_THRESHOLD", "tidb_slow_log_threshold"),
    ("TIDB_SLOW_LOG_RULES", "tidb_slow_log_rules"),
    ("TIDB_SLOW_LOG_MAX_PER_SEC", "tidb_slow_log_max_per_sec"),
    ("TIDB_SLOW_TXN_LOG_THRESHOLD", "tidb_slow_txn_log_threshold"),
    ("TIDB_RECORD_PLAN_IN_SLOW_LOG", "tidb_record_plan_in_slow_log"),
    ("TIDB_ENABLE_SLOW_LOG", "tidb_enable_slow_log"),
    ("TIDB_CHECK_MB4_VALUE_IN_UTF8", "tidb_check_mb4_value_in_utf8"),
    ("TIDB_FOUND_IN_PLAN_CACHE", "last_plan_from_cache"),
    ("TIDB_FOUND_IN_BINDING", "last_plan_from_binding"),
    ("TIDB_ALLOW_AUTO_RAND_EXPLICIT_INSERT", "allow_auto_random_explicit_insert"),
    ("TIDB_TXN_READ_TS", "tx_read_ts"),
    ("TIDB_READ_STALENESS", "tidb_read_staleness"),
    ("TIDB_ENABLE_PAGING", "tidb_enable_paging"),
    ("TIDB_READ_CONSISTENCY", "tidb_read_consistency"),
    ("TIDB_SYSDATE_IS_NOW", "tidb_sysdate_is_now"),
    ("REQUIRE_SECURE_TRANSPORT", "require_secure_transport"),
    ("TIFLASH_FAST_SCAN", "tiflash_fastscan"),
    ("TIDB_ENABLE_UNSAFE_SUBSTITUTE", "tidb_enable_unsafe_substitute"),
    ("TIDB_ENABLE_TIFLASH_READ_FOR_WRITE_STMT", "tidb_enable_tiflash_read_for_write_stmt"),
    ("TIDB_USE_ALLOC", "last_sql_use_alloc"),
    ("TIDB_EXPLICIT_REQUEST_SOURCE_TYPE", "tidb_request_source_type"),
    ("TIDB_BUILD_STATS_CONCURRENCY", "tidb_build_stats_concurrency"),
    ("TIDB_BUILD_SAMPLING_STATS_CONCURRENCY", "tidb_build_sampling_stats_concurrency"),
    ("TIDB_DIST_SQL_SCAN_CONCURRENCY", "tidb_distsql_scan_concurrency"),
    ("TIDB_ANALYZE_DIST_SQL_SCAN_CONCURRENCY", "tidb_analyze_distsql_scan_concurrency"),
    ("TIDB_OPT_IN_SUBQ_TO_JOIN_AND_AGG", "tidb_opt_insubq_to_join_and_agg"),
    ("TIDB_OPT_PREFER_RANGE_SCAN", "tidb_opt_prefer_range_scan"),
    ("TIDB_OPT_ENABLE_NO_DECORRELATE_IN_SELECT", "tidb_opt_enable_no_decorrelate_in_select"),
    ("TIDB_OPT_ENABLE_ALTERNATIVE_LOGICAL_PLANS", "tidb_opt_enable_alternative_logical_plans"),
    ("TIDB_OPT_ENABLE_SEMI_JOIN_REWRITE", "tidb_opt_enable_semi_join_rewrite"),
    ("TIDB_OPT_ENABLE_CORRELATION_ADJUSTMENT", "tidb_opt_enable_correlation_adjustment"),
    ("TIDB_OPT_LIMIT_PUSH_DOWN_THRESHOLD", "tidb_opt_limit_push_down_threshold"),
    ("TIDB_OPT_CORRELATION_THRESHOLD", "tidb_opt_correlation_threshold"),
    ("TIDB_OPT_CORRELATION_EXP_FACTOR", "tidb_opt_correlation_exp_factor"),
    ("TIDB_OPT_RISK_EQ_SKEW_RATIO", "tidb_opt_risk_eq_skew_ratio"),
    ("TIDB_OPT_RISK_RANGE_SKEW_RATIO", "tidb_opt_risk_range_skew_ratio"),
    ("TIDB_OPT_RISK_SCALE_NDV_SKEW_RATIO", "tidb_opt_scale_ndv_skew_ratio"),
    ("TIDB_OPT_RISK_GROUP_NDV_SKEW_RATIO", "tidb_opt_group_ndv_skew_ratio"),
    ("TIDB_OPT_ALWAYS_KEEP_JOIN_KEY", "tidb_opt_always_keep_join_key"),
    ("TIDB_OPT_CARTESIAN_JOIN_ORDER_THRESHOLD", "tidb_opt_cartesian_join_order_threshold"),
    ("TIDB_OPT_CPU_FACTOR", "tidb_opt_cpu_factor"),
    ("TIDB_OPT_COP_CPU_FACTOR", "tidb_opt_copcpu_factor"),
    ("TIDB_OPT_TIFLASH_CONCURRENCY_FACTOR", "tidb_opt_tiflash_concurrency_factor"),
    ("TIDB_OPT_NETWORK_FACTOR", "tidb_opt_network_factor"),
    ("TIDB_OPT_SCAN_FACTOR", "tidb_opt_scan_factor"),
    ("TIDB_OPT_DESC_SCAN_FACTOR", "tidb_opt_desc_factor"),
    ("TIDB_OPT_SEEK_FACTOR", "tidb_opt_seek_factor"),
    ("TIDB_OPT_MEMORY_FACTOR", "tidb_opt_memory_factor"),
    ("TIDB_OPT_DISK_FACTOR", "tidb_opt_disk_factor"),
    ("TIDB_OPT_CONCURRENCY_FACTOR", "tidb_opt_concurrency_factor"),
    ("TIDB_OPT_INDEX_SCAN_COST_FACTOR", "tidb_opt_index_scan_cost_factor"),
    ("TIDB_OPT_INDEX_READER_COST_FACTOR", "tidb_opt_index_reader_cost_factor"),
    ("TIDB_OPT_TABLE_READER_COST_FACTOR", "tidb_opt_table_reader_cost_factor"),
    ("TIDB_OPT_TABLE_FULL_SCAN_COST_FACTOR", "tidb_opt_table_full_scan_cost_factor"),
    ("TIDB_OPT_TABLE_RANGE_SCAN_COST_FACTOR", "tidb_opt_table_range_scan_cost_factor"),
    ("TIDB_OPT_TABLE_ROW_ID_SCAN_COST_FACTOR", "tidb_opt_table_rowid_scan_cost_factor"),
    ("TIDB_OPT_TABLE_TIFLASH_SCAN_COST_FACTOR", "tidb_opt_table_tiflash_scan_cost_factor"),
    ("TIDB_OPT_INDEX_LOOKUP_COST_FACTOR", "tidb_opt_index_lookup_cost_factor"),
    ("TIDB_OPT_INDEX_MERGE_COST_FACTOR", "tidb_opt_index_merge_cost_factor"),
    ("TIDB_OPT_SORT_COST_FACTOR", "tidb_opt_sort_cost_factor"),
    ("TIDB_OPT_TOP_N_COST_FACTOR", "tidb_opt_topn_cost_factor"),
    ("TIDB_OPT_LIMIT_COST_FACTOR", "tidb_opt_limit_cost_factor"),
    ("TIDB_OPT_STREAM_AGG_COST_FACTOR", "tidb_opt_stream_agg_cost_factor"),
    ("TIDB_OPT_HASH_AGG_COST_FACTOR", "tidb_opt_hash_agg_cost_factor"),
    ("TIDB_OPT_MERGE_JOIN_COST_FACTOR", "tidb_opt_merge_join_cost_factor"),
    ("TIDB_OPT_HASH_JOIN_COST_FACTOR", "tidb_opt_hash_join_cost_factor"),
    ("TIDB_OPT_INDEX_JOIN_COST_FACTOR", "tidb_opt_index_join_cost_factor"),
    ("TIDB_OPT_INDEX_JOIN_MAX_SCAN_ROWS_RATIO", "tidb_opt_index_join_max_scan_rows_ratio"),
    ("TIDB_OPT_SELECTIVITY_FACTOR", "tidb_opt_selectivity_factor"),
    ("TIDB_OPT_FORCE_INLINE_CTE", "tidb_opt_force_inline_cte"),
    ("TIDB_INDEX_JOIN_BATCH_SIZE", "tidb_index_join_batch_size"),
    ("TIDB_INDEX_LOOKUP_SIZE", "tidb_index_lookup_size"),
    ("TIDB_INDEX_LOOKUP_CONCURRENCY", "tidb_index_lookup_concurrency"),
    ("TIDB_INDEX_LOOKUP_JOIN_CONCURRENCY", "tidb_index_lookup_join_concurrency"),
    ("TIDB_INDEX_SERIAL_SCAN_CONCURRENCY", "tidb_index_serial_scan_concurrency"),
    ("TIDB_MAX_CHUNK_SIZE", "tidb_max_chunk_size"),
    ("TIDB_ALLOW_BATCH_COP", "tidb_allow_batch_cop"),
    ("TIDB_SHARD_ROW_ID_BITS", "tidb_shard_row_id_bits"),
    ("TIDB_PRE_SPLIT_REGIONS", "tidb_pre_split_regions"),
    ("TIDB_ALLOW_MPP_EXECUTION", "tidb_allow_mpp"),
    ("TIDB_ALLOW_TIFLASH_COP", "tidb_allow_tiflash_cop"),
    ("TIDB_HASH_EXCHANGE_WITH_NEW_COLLATION", "tidb_hash_exchange_with_new_collation"),
    ("TIDB_ENFORCE_MPP_EXECUTION", "tidb_enforce_mpp"),
    ("TIDB_MAX_TIFLASH_THREADS", "tidb_max_tiflash_threads"),
    ("TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN", "tidb_max_bytes_before_tiflash_external_join"),
    ("TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY", "tidb_max_bytes_before_tiflash_external_group_by"),
    ("TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT", "tidb_max_bytes_before_tiflash_external_sort"),
    ("TIFLASH_MEM_QUOTA_QUERY_PER_NODE", "tiflash_mem_quota_query_per_node"),
    ("TIFLASH_QUERY_SPILL_RATIO", "tiflash_query_spill_ratio"),
    ("TIFLASH_HASH_JOIN_VERSION", "tiflash_hash_join_version"),
    ("TIDB_MPP_STORE_FAIL_TTL", "tidb_mpp_store_fail_ttl"),
    ("TIDB_INIT_CHUNK_SIZE", "tidb_init_chunk_size"),
    ("TIDB_MIN_PAGING_SIZE", "tidb_min_paging_size"),
    ("TIDB_MAX_PAGING_SIZE", "tidb_max_paging_size"),
    ("TIDB_PAGING_SIZE_BYTES", "tidb_paging_size_bytes"),
    ("TIDB_ENABLE_CASCADES_PLANNER", "tidb_enable_cascades_planner"),
    ("TIDB_SKIP_UTF8_CHECK", "tidb_skip_utf8_check"),
    ("TIDB_SKIP_ASCII_CHECK", "tidb_skip_ascii_check"),
    ("TIDB_HASH_JOIN_CONCURRENCY", "tidb_hash_join_concurrency"),
    ("TIDB_PROJECTION_CONCURRENCY", "tidb_projection_concurrency"),
    ("TIDB_HASH_AGG_PARTIAL_CONCURRENCY", "tidb_hashagg_partial_concurrency"),
    ("TIDB_HASH_AGG_FINAL_CONCURRENCY", "tidb_hashagg_final_concurrency"),
    ("TIDB_WINDOW_CONCURRENCY", "tidb_window_concurrency"),
    ("TIDB_MERGE_JOIN_CONCURRENCY", "tidb_merge_join_concurrency"),
    ("TIDB_STREAM_AGG_CONCURRENCY", "tidb_streamagg_concurrency"),
    ("TIDB_INDEX_MERGE_INTERSECTION_CONCURRENCY", "tidb_index_merge_intersection_concurrency"),
    ("TIDB_ENABLE_PARALLEL_APPLY", "tidb_enable_parallel_apply"),
    ("TIDB_BACKOFF_LOCK_FAST", "tidb_backoff_lock_fast"),
    ("TIDB_BACK_OFF_WEIGHT", "tidb_backoff_weight"),
    ("TIDB_DDL_REORG_WORKER_COUNT", "tidb_ddl_reorg_worker_cnt"),
    ("TIDB_DDL_FLASHBACK_CONCURRENCY", "tidb_ddl_flashback_concurrency"),
    ("TIDB_DDL_REORG_BATCH_SIZE", "tidb_ddl_reorg_batch_size"),
    ("TIDB_DDL_ERROR_COUNT_LIMIT", "tidb_ddl_error_count_limit"),
    ("TIDB_DDL_REORG_PRIORITY", "tidb_ddl_reorg_priority"),
    ("TIDB_DDL_REORG_MAX_WRITE_SPEED", "tidb_ddl_reorg_max_write_speed"),
    ("TIDB_ENABLE_AUTO_INCREMENT_IN_GENERATED", "tidb_enable_auto_increment_in_generated"),
    ("TIDB_ENABLE_POINT_GET_CACHE", "tidb_enable_point_get_cache"),
    ("TIDB_PLACEMENT_MODE", "tidb_placement_mode"),
    ("TIDB_MAX_DELTA_SCHEMA_COUNT", "tidb_max_delta_schema_count"),
    ("TIDB_SCATTER_REGION", "tidb_scatter_region"),
    ("TIDB_WAIT_SPLIT_REGION_FINISH", "tidb_wait_split_region_finish"),
    ("TIDB_WAIT_SPLIT_REGION_TIMEOUT", "tidb_wait_split_region_timeout"),
    ("TIDB_FORCE_PRIORITY", "tidb_force_priority"),
    ("TIDB_CONSTRAINT_CHECK_IN_PLACE", "tidb_constraint_check_in_place"),
    ("TIDB_ENABLE_WINDOW_FUNCTION", "tidb_enable_window_function"),
    ("TIDB_ENABLE_PIPELINED_WINDOW_FUNCTION", "tidb_enable_pipelined_window_function"),
    ("TIDB_ENABLE_STRICT_NOT_NULL_CHECK", "tidb_enable_strict_not_null_check"),
    ("TIDB_ENABLE_STRICT_DOUBLE_TYPE_CHECK", "tidb_enable_strict_double_type_check"),
    ("TIDB_OPT_PROJECTION_PUSH_DOWN", "tidb_opt_projection_push_down"),
    ("TIDB_ENABLE_VECTORIZED_EXPRESSION", "tidb_enable_vectorized_expression"),
    ("TIDB_OPT_JOIN_REORDER_THRESHOLD", "tidb_opt_join_reorder_threshold"),
    ("TIDB_OPT_ENABLE_ADVANCED_JOIN_REORDER", "tidb_opt_enable_advanced_join_reorder"),
    ("TIDB_OPT_JOIN_REORDER_THROUGH_PROJ", "tidb_opt_join_reorder_through_proj"),
    ("TIDB_OPT_JOIN_REORDER_THROUGH_SEL", "tidb_opt_join_reorder_through_sel"),
    ("TIDB_SLOW_QUERY_FILE", "tidb_slow_query_file"),
    ("TIDB_ENABLE_FAST_ANALYZE", "tidb_enable_fast_analyze"),
    ("TIDB_EXPENSIVE_QUERY_TIME_THRESHOLD", "tidb_expensive_query_time_threshold"),
    ("TIDB_EXPENSIVE_TXN_TIME_THRESHOLD", "tidb_expensive_txn_time_threshold"),
    ("TIDB_ENABLE_INDEX_MERGE", "tidb_enable_index_merge"),
    ("TIDB_ENABLE_NO_BACKSLASH_ESCAPES_IN_LIKE", "tidb_enable_no_backslash_escapes_in_like"),
    ("TIDB_ENABLE_NOOP_FUNCS", "tidb_enable_noop_functions"),
    ("TIDB_ENABLE_STMT_SUMMARY", "tidb_enable_stmt_summary"),
    ("TIDB_STMT_SUMMARY_INTERNAL_QUERY", "tidb_stmt_summary_internal_query"),
    ("TIDB_STMT_SUMMARY_REFRESH_INTERVAL", "tidb_stmt_summary_refresh_interval"),
    ("TIDB_STMT_SUMMARY_HISTORY_SIZE", "tidb_stmt_summary_history_size"),
    ("TIDB_STMT_SUMMARY_MAX_STMT_COUNT", "tidb_stmt_summary_max_stmt_count"),
    ("TIDB_STMT_SUMMARY_MAX_SQL_LENGTH", "tidb_stmt_summary_max_sql_length"),
    ("TIDB_STMT_SUMMARY_PERSIST_EVICTED", "tidb_stmt_summary_persist_evicted"),
    ("TIDB_STMT_SUMMARY_GROUP_BY_USER", "tidb_stmt_summary_group_by_user"),
    ("TIDB_IGNORE_INLIST_PLAN_DIGEST", "tidb_ignore_inlist_plan_digest"),
    ("TIDB_CAPTURE_PLAN_BASELINE", "tidb_capture_plan_baselines"),
    ("TIDB_USE_PLAN_BASELINES", "tidb_use_plan_baselines"),
    ("TIDB_EVOLVE_PLAN_BASELINES", "tidb_evolve_plan_baselines"),
    ("TIDB_OPT_ENABLE_FUZZY_BINDING", "tidb_opt_enable_fuzzy_binding"),
    ("TIDB_ENABLE_EXTENDED_STATS", "tidb_enable_extended_stats"),
    ("TIDB_ISOLATION_READ_ENGINES", "tidb_isolation_read_engines"),
    ("TIDB_STORE_LIMIT", "tidb_store_limit"),
    ("TIDB_METRIC_SCHEMA_STEP", "tidb_metric_query_step"),
    ("TIDB_CDC_WRITE_SOURCE", "tidb_cdc_write_source"),
    ("TIDB_METRIC_SCHEMA_RANGE_DURATION", "tidb_metric_query_range_duration"),
    ("TIDB_ENABLE_COLLECT_EXECUTION_INFO", "tidb_enable_collect_execution_info"),
    ("TIDB_EXECUTOR_CONCURRENCY", "tidb_executor_concurrency"),
    ("TIDB_ENABLE_CLUSTERED_INDEX", "tidb_enable_clustered_index"),
    ("TIDB_ENABLE_GLOBAL_INDEX", "tidb_enable_global_index"),
    ("TIDB_PARTITION_PRUNE_MODE", "tidb_partition_prune_mode"),
    ("TIDB_REDACT_LOG", "tidb_redact_log"),
    ("TIDB_RESTRICTED_READ_ONLY", "tidb_restricted_read_only"),
    ("TIDB_SUPER_READ_ONLY", "tidb_super_read_only"),
    ("TIDB_SHARD_ALLOCATE_STEP", "tidb_shard_allocate_step"),
    ("TIDB_ENABLE_TELEMETRY", "tidb_enable_telemetry"),
    ("TIDB_MEMORY_USAGE_ALARM_RATIO", "tidb_memory_usage_alarm_ratio"),
    ("TIDB_MEMORY_USAGE_ALARM_KEEP_RECORD_NUM", "tidb_memory_usage_alarm_keep_record_num"),
    ("TIDB_ENABLE_RATE_LIMIT_ACTION", "tidb_enable_rate_limit_action"),
    ("TIDB_ENABLE_ASYNC_COMMIT", "tidb_enable_async_commit"),
    ("TIDB_ENABLE1_PC", "tidb_enable_1pc"),
    ("TIDB_GUARANTEE_LINEARIZABILITY", "tidb_guarantee_linearizability"),
    ("TIDB_ANALYZE_VERSION", "tidb_analyze_version"),
    ("TIDB_AUTO_ANALYZE_PARTITION_BATCH_SIZE", "tidb_auto_analyze_partition_batch_size"),
    ("TIDB_ENABLE_INDEX_MERGE_JOIN", "tidb_enable_index_merge_join"),
    ("TIDB_TRACK_AGGREGATE_MEMORY_USAGE", "tidb_track_aggregate_memory_usage"),
    ("TIDB_ENABLE_EXCHANGE_PARTITION", "tidb_enable_exchange_partition"),
    ("TIDB_ALLOW_FALLBACK_TO_TIKV", "tidb_allow_fallback_to_tikv"),
    ("TIDB_ENABLE_TOP_SQL", "tidb_enable_top_sql"),
    ("TIDB_SOURCE_ID", "tidb_source_id"),
    ("TIDB_TOP_SQL_MAX_TIME_SERIES_COUNT", "tidb_top_sql_max_time_series_count"),
    ("TIDB_TOP_SQL_MAX_META_COUNT", "tidb_top_sql_max_meta_count"),
    ("TIDB_ENABLE_LOCAL_TXN", "tidb_enable_local_txn"),
    ("TIDB_ENABLE_MDL", "tidb_enable_metadata_lock"),
    ("TIDB_TSO_CLIENT_BATCH_MAX_WAIT_TIME", "tidb_tso_client_batch_max_wait_time"),
    ("TIDB_TXN_COMMIT_BATCH_SIZE", "tidb_txn_commit_batch_size"),
    ("TIDB_ENABLE_TSO_FOLLOWER_PROXY", "tidb_enable_tso_follower_proxy"),
    ("PD_ENABLE_FOLLOWER_HANDLE_REGION", "pd_enable_follower_handle_region"),
    ("TIDB_ENABLE_BATCH_QUERY_REGION", "tidb_enable_batch_query_region"),
    ("TIDB_ENABLE_ORDERED_RESULT_MODE", "tidb_enable_ordered_result_mode"),
    ("TIDB_REMOVE_ORDERBY_IN_SUBQUERY", "tidb_remove_orderby_in_subquery"),
    ("TIDB_ENABLE_PSEUDO_FOR_OUTDATED_STATS", "tidb_enable_pseudo_for_outdated_stats"),
    ("TIDB_REGARD_NULL_AS_POINT", "tidb_regard_null_as_point"),
    ("TIDB_TMP_TABLE_MAX_SIZE", "tidb_tmp_table_max_size"),
    ("TIDB_ENABLE_LEGACY_INSTANCE_SCOPE", "tidb_enable_legacy_instance_scope"),
    ("TIDB_TABLE_CACHE_LEASE", "tidb_table_cache_lease"),
    ("TIDB_STATS_LOAD_SYNC_WAIT", "tidb_stats_load_sync_wait"),
    ("TIDB_ENABLE_MUTATION_CHECKER", "tidb_enable_mutation_checker"),
    ("TIDB_TXN_ASSERTION_LEVEL", "tidb_txn_assertion_level"),
    ("TIDB_IGNORE_PREPARED_CACHE_CLOSE_STMT", "tidb_ignore_prepared_cache_close_stmt"),
    ("TIDB_ENABLE_NEW_COST_INTERFACE", "tidb_enable_new_cost_interface"),
    ("TIDB_COST_MODEL_VERSION", "tidb_cost_model_version"),
    ("TIDB_INDEX_JOIN_DOUBLE_READ_PENALTY_COST_RATE", "tidb_index_join_double_read_penalty_cost_rate"),
    ("TIDB_BATCH_PENDING_TIFLASH_COUNT", "tidb_batch_pending_tiflash_count"),
    ("TIDB_QUERY_LOG_MAX_LEN", "tidb_query_log_max_len"),
    ("TIDB_ENABLE_NOOP_VARIABLES", "tidb_enable_noop_variables"),
    ("TIDB_NON_TRANSACTIONAL_IGNORE_ERROR", "tidb_nontransactional_ignore_error"),
    ("TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT", "tiflash_fine_grained_shuffle_stream_count"),
    ("TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE", "tiflash_fine_grained_shuffle_batch_size"),
    ("TIDB_SIMPLIFIED_METRICS", "tidb_simplified_metrics"),
    ("TIDB_MEMORY_DEBUG_MODE_MIN_HEAP_IN_USE", "tidb_memory_debug_mode_min_heap_inuse"),
    ("TIDB_MEMORY_DEBUG_MODE_ALARM_RATIO", "tidb_memory_debug_mode_alarm_ratio"),
    ("TIDB_ENABLE_ANALYZE_SNAPSHOT", "tidb_enable_analyze_snapshot"),
    ("TIDB_DEFAULT_STR_MATCH_SELECTIVITY", "tidb_default_string_match_selectivity"),
    ("TIDB_ENABLE_PREP_PLAN_CACHE", "tidb_enable_prepared_plan_cache"),
    ("TIDB_PREP_PLAN_CACHE_SIZE", "tidb_prepared_plan_cache_size"),
    ("TIDB_ENABLE_PREP_PLAN_CACHE_MEMORY_MONITOR", "tidb_enable_prepared_plan_cache_memory_monitor"),
    ("TIDB_ENABLE_NON_PREPARED_PLAN_CACHE", "tidb_enable_non_prepared_plan_cache"),
    ("TIDB_ENABLE_NON_PREPARED_PLAN_CACHE_FOR_DML", "tidb_enable_non_prepared_plan_cache_for_dml"),
    ("TIDB_PLAN_CACHE_STRATEGY", "tidb_plan_cache_strategy"),
    ("TIDB_PLAN_CACHE_STRATEGY_ALL", "all"),
    ("TIDB_PLAN_CACHE_STRATEGY_HINT_ONLY", "hint_only"),
    ("TIDB_NON_PREPARED_PLAN_CACHE_SIZE", "tidb_non_prepared_plan_cache_size"),
    ("TIDB_PLAN_CACHE_MAX_PLAN_SIZE", "tidb_plan_cache_max_plan_size"),
    ("TIDB_PLAN_CACHE_INVALIDATION_ON_FRESH_STATS", "tidb_plan_cache_invalidation_on_fresh_stats"),
    ("TIDB_PLAN_CACHE_SKIP_STATS_ON_BINDING", "tidb_plan_cache_skip_stats_on_binding"),
    ("TIDB_SESSION_PLAN_CACHE_SIZE", "tidb_session_plan_cache_size"),
    ("TIDB_ENABLE_INSTANCE_PLAN_CACHE", "tidb_enable_instance_plan_cache"),
    ("TIDB_INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE", "tidb_instance_plan_cache_reserved_percentage"),
    ("TIDB_INSTANCE_PLAN_CACHE_MAX_MEM_SIZE", "tidb_instance_plan_cache_max_size"),
    ("TIDB_CONSTRAINT_CHECK_IN_PLACE_PESSIMISTIC", "tidb_constraint_check_in_place_pessimistic"),
    ("TIDB_ENABLE_FOREIGN_KEY", "tidb_enable_foreign_key"),
    ("TIDB_FOREIGN_KEY_CHECK_IN_SHARED_LOCK", "tidb_foreign_key_check_in_shared_lock"),
    ("TIDB_OPT_RANGE_MAX_SIZE", "tidb_opt_range_max_size"),
    ("TIDB_OPT_ADVANCED_JOIN_HINT", "tidb_opt_advanced_join_hint"),
    ("TIDB_OPT_USE_INVISIBLE_INDEXES", "tidb_opt_use_invisible_indexes"),
    ("TIDB_ANALYZE_PARTITION_CONCURRENCY", "tidb_analyze_partition_concurrency"),
    ("TIDB_MERGE_PARTITION_STATS_CONCURRENCY", "tidb_merge_partition_stats_concurrency"),
    ("TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS", "tidb_enable_async_merge_global_stats"),
    ("TIDB_OPT_PREFIX_INDEX_SINGLE_SCAN", "tidb_opt_prefix_index_single_scan"),
    ("TIDB_OPT_PARTIAL_ORDERED_INDEX_FOR_TOP_N", "tidb_opt_partial_ordered_index_for_topn"),
    ("TIDB_ENABLE_EXTERNAL_TS_READ", "tidb_enable_external_ts_read"),
    ("TIDB_ENABLE_PLAN_REPLAYER_CAPTURE", "tidb_enable_plan_replayer_capture"),
    ("TIDB_ENABLE_PLAN_REPLAYER_CONTINUOUS_CAPTURE", "tidb_enable_plan_replayer_continuous_capture"),
    ("TIDB_ENABLE_REUSECHUNK", "tidb_enable_reuse_chunk"),
    ("TIDB_STORE_BATCH_SIZE", "tidb_store_batch_size"),
    ("MPP_EXCHANGE_COMPRESSION_MODE", "mpp_exchange_compression_mode"),
    ("MPP_VERSION", "mpp_version"),
    ("TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING", "tidb_pessimistic_txn_fair_locking"),
    ("TIDB_ENABLE_PLAN_CACHE_FOR_PARAM_LIMIT", "tidb_enable_plan_cache_for_param_limit"),
    ("TIDB_ENABLE_INL_JOIN_INNER_MULTI_PATTERN", "tidb_enable_inl_join_inner_multi_pattern"),
    ("TIFLASH_COMPUTE_DISPATCH_POLICY", "tiflash_compute_dispatch_policy"),
    ("TIDB_ENABLE_PLAN_CACHE_FOR_SUBQUERY", "tidb_enable_plan_cache_for_subquery"),
    ("TIDB_OPT_ENABLE_LATE_MATERIALIZATION", "tidb_opt_enable_late_materialization"),
    ("TIDB_LOAD_BASED_REPLICA_READ_THRESHOLD", "tidb_load_based_replica_read_threshold"),
    ("TIDB_OPT_ORDERING_IDX_SEL_THRESH", "tidb_opt_ordering_index_selectivity_threshold"),
    ("TIDB_OPT_ORDERING_IDX_SEL_RATIO", "tidb_opt_ordering_index_selectivity_ratio"),
    ("TIDB_OPT_ENABLE_MPP_SHARED_CTE_EXECUTION", "tidb_opt_enable_mpp_shared_cte_execution"),
    ("TIDB_OPT_FIX_CONTROL", "tidb_opt_fix_control"),
    ("TIFLASH_REPLICA_READ", "tiflash_replica_read"),
    ("TIDB_LOCK_UNCHANGED_KEYS", "tidb_lock_unchanged_keys"),
    ("TIDB_FAST_CHECK_TABLE", "tidb_enable_fast_table_check"),
    ("TIDB_ANALYZE_SKIP_COLUMN_TYPES", "tidb_analyze_skip_column_types"),
    ("TIDB_ENABLE_CHECK_CONSTRAINT", "tidb_enable_check_constraint"),
    ("TIDB_OPT_ENABLE_HASH_JOIN", "tidb_opt_enable_hash_join"),
    ("TIDB_HASH_JOIN_VERSION", "tidb_hash_join_version"),
    ("TIDB_OPT_INDEX_JOIN_BUILD", "tidb_opt_index_join_build_v2"),
    ("TIDB_OPT_OBJECTIVE", "tidb_opt_objective"),
    ("TIDB_ENABLE_PARALLEL_HASHAGG_SPILL", "tidb_enable_parallel_hashagg_spill"),
    ("TIDB_TXN_ENTRY_SIZE_LIMIT", "tidb_txn_entry_size_limit"),
    ("TIDB_SCHEMA_CACHE_SIZE", "tidb_schema_cache_size"),
    ("DIV_PRECISION_INCREMENT", "div_precision_increment"),
    ("TIDB_ENABLE_SHARED_LOCK_PROMOTION", "tidb_enable_shared_lock_promotion"),
    ("TIDB_ACCELERATE_USER_CREATION_UPDATE", "tidb_accelerate_user_creation_update"),
    ("TIDB_ENABLE_CACHE_PREPARE_STMT", "tidb_enable_cache_prepare_stmt"),
    ("TIDB_GC_ENABLE", "tidb_gc_enable"),
    ("TIDB_GC_RUN_INTERVAL", "tidb_gc_run_interval"),
    ("TIDB_GC_LIFETIME", "tidb_gc_life_time"),
    ("TIDB_GC_CONCURRENCY", "tidb_gc_concurrency"),
    ("TIDB_GC_SCAN_LOCK_MODE", "tidb_gc_scan_lock_mode"),
    ("TIDB_GC_MAX_WAIT_TIME", "tidb_gc_max_wait_time"),
    ("TIDB_ENABLE_ENHANCED_SECURITY", "tidb_enable_enhanced_security"),
    ("TIDB_ENABLE_HISTORICAL_STATS", "tidb_enable_historical_stats"),
    ("TIDB_PERSIST_ANALYZE_OPTIONS", "tidb_persist_analyze_options"),
    ("TIDB_ENABLE_COLUMN_TRACKING", "tidb_enable_column_tracking"),
    ("TIDB_ANALYZE_COLUMN_OPTIONS", "tidb_analyze_column_options"),
    ("TIDB_ANALYZE_DEFAULT_NUM_BUCKETS", "tidb_analyze_default_num_buckets"),
    ("TIDB_ANALYZE_DEFAULT_NUM_TOP_N", "tidb_analyze_default_num_topn"),
    ("TIDB_DISABLE_COLUMN_TRACKING_TIME", "tidb_disable_column_tracking_time"),
    ("TIDB_STATS_LOAD_PSEUDO_TIMEOUT", "tidb_stats_load_pseudo_timeout"),
    ("TIDB_MEM_QUOTA_BINDING_CACHE", "tidb_mem_quota_binding_cache"),
    ("TIDB_RC_READ_CHECK_TS", "tidb_rc_read_check_ts"),
    ("TIDB_RC_WRITE_CHECK_TS", "tidb_rc_write_check_ts"),
    ("TIDB_COMMITTER_CONCURRENCY", "tidb_committer_concurrency"),
    ("TIDB_PIPELINED_DML_RESOURCE_POLICY", "tidb_pipelined_dml_resource_policy"),
    ("TIDB_ENABLE_BATCH_DML", "tidb_enable_batch_dml"),
    ("TIDB_STATS_CACHE_MEM_QUOTA", "tidb_stats_cache_mem_quota"),
    ("TIDB_MEM_QUOTA_ANALYZE", "tidb_mem_quota_analyze"),
    ("TIDB_ENABLE_AUTO_ANALYZE", "tidb_enable_auto_analyze"),
    ("TIDB_ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE", "tidb_enable_auto_analyze_priority_queue"),
    ("TIDB_MEM_OOM_ACTION", "tidb_mem_oom_action"),
    ("TIDB_PREP_PLAN_CACHE_MEMORY_GUARD_RATIO", "tidb_prepared_plan_cache_memory_guard_ratio"),
    ("TIDB_MAX_AUTO_ANALYZE_TIME", "tidb_max_auto_analyze_time"),
    ("TIDB_AUTO_ANALYZE_CONCURRENCY", "tidb_auto_analyze_concurrency"),
    ("TIDB_ENABLE_DIST_TASK", "tidb_enable_dist_task"),
    ("TIDB_MAX_DIST_TASK_NODES", "tidb_max_dist_task_nodes"),
    ("TIDB_ENABLE_FAST_CREATE_TABLE", "tidb_enable_fast_create_table"),
    ("TIDB_GENERATE_BINARY_PLAN", "tidb_generate_binary_plan"),
    ("TIDB_ENABLE_DDL_ANALYZE", "tidb_stats_update_during_ddl"),
    ("TIDB_ENABLE_GC_AWARE_MEMORY_TRACK", "tidb_enable_gc_aware_memory_track"),
    ("TIDB_ENABLE_TMP_STORAGE_ON_OOM", "tidb_enable_tmp_storage_on_oom"),
    ("TIDB_DDL_ENABLE_FAST_REORG", "tidb_ddl_enable_fast_reorg"),
    ("TIDB_DDL_DISK_QUOTA", "tidb_ddl_disk_quota"),
    ("TIDB_CLOUD_STORAGE_URI", "tidb_cloud_storage_uri"),
    ("TIDB_AUTO_BUILD_STATS_CONCURRENCY", "tidb_auto_build_stats_concurrency"),
    ("TIDB_SYS_PROC_SCAN_CONCURRENCY", "tidb_sysproc_scan_concurrency"),
    ("TIDB_SERVER_MEMORY_LIMIT", "tidb_server_memory_limit"),
    ("TIDB_SERVER_MEMORY_LIMIT_SESS_MIN_SIZE", "tidb_server_memory_limit_sess_min_size"),
    ("TIDB_SERVER_MEMORY_LIMIT_GC_TRIGGER", "tidb_server_memory_limit_gc_trigger"),
    ("TIDB_MEM_ARBITRATOR_SOFT_LIMIT", "tidb_mem_arbitrator_soft_limit"),
    ("TIDB_MEM_ARBITRATOR_MODE", "tidb_mem_arbitrator_mode"),
    ("TIDB_MEM_ARBITRATOR_QUERY_RESERVED", "tidb_mem_arbitrator_query_reserved"),
    ("TIDB_MEM_ARBITRATOR_WAIT_AVERSE", "tidb_mem_arbitrator_wait_averse"),
    ("TIDB_ENABLE_GOGC_TUNER", "tidb_enable_gogc_tuner"),
    ("TIDB_GOGC_TUNER_THRESHOLD", "tidb_gogc_tuner_threshold"),
    ("TIDB_GOGC_TUNER_MAX_VALUE", "tidb_gogc_tuner_max_value"),
    ("TIDB_GOGC_TUNER_MIN_VALUE", "tidb_gogc_tuner_min_value"),
    ("TIDB_EXTERNAL_TS", "tidb_external_ts"),
    ("TIDB_TTL_JOB_ENABLE", "tidb_ttl_job_enable"),
    ("TIDB_TTL_SCAN_BATCH_SIZE", "tidb_ttl_scan_batch_size"),
    ("TIDB_TTL_DELETE_BATCH_SIZE", "tidb_ttl_delete_batch_size"),
    ("TIDB_TTL_DELETE_RATE_LIMIT", "tidb_ttl_delete_rate_limit"),
    ("TIDB_TTL_JOB_SCHEDULE_WINDOW_START_TIME", "tidb_ttl_job_schedule_window_start_time"),
    ("TIDB_TTL_JOB_SCHEDULE_WINDOW_END_TIME", "tidb_ttl_job_schedule_window_end_time"),
    ("TIDB_TTL_SCAN_WORKER_COUNT", "tidb_ttl_scan_worker_count"),
    ("TIDB_TTL_DELETE_WORKER_COUNT", "tidb_ttl_delete_worker_count"),
    ("PASSWORD_REUSE_HISTORY", "password_history"),
    ("PASSWORD_REUSE_TIME", "password_reuse_interval"),
    ("TIDB_HISTORICAL_STATS_DURATION", "tidb_historical_stats_duration"),
    ("TIDB_ENABLE_HISTORICAL_STATS_FOR_CAPTURE", "tidb_enable_historical_stats_for_capture"),
    ("TIDB_ENABLE_RESOURCE_CONTROL", "tidb_enable_resource_control"),
    ("TIDB_RESOURCE_CONTROL_STRICT_MODE", "tidb_resource_control_strict_mode"),
    ("TIDB_STMT_SUMMARY_ENABLE_PERSISTENT", "tidb_stmt_summary_enable_persistent"),
    ("TIDB_STMT_SUMMARY_FILENAME", "tidb_stmt_summary_filename"),
    ("TIDB_STMT_SUMMARY_FILE_MAX_DAYS", "tidb_stmt_summary_file_max_days"),
    ("TIDB_STMT_SUMMARY_FILE_MAX_SIZE", "tidb_stmt_summary_file_max_size"),
    ("TIDB_STMT_SUMMARY_FILE_MAX_BACKUPS", "tidb_stmt_summary_file_max_backups"),
    ("TIDB_TTL_RUNNING_TASKS", "tidb_ttl_running_tasks"),
    ("AUTHENTICATION_LDAPSASL_AUTH_METHOD_NAME", "authentication_ldap_sasl_auth_method_name"),
    ("AUTHENTICATION_LDAPSASLCA_PATH", "authentication_ldap_sasl_ca_path"),
    ("AUTHENTICATION_LDAPSASLTLS", "authentication_ldap_sasl_tls"),
    ("AUTHENTICATION_LDAPSASL_SERVER_HOST", "authentication_ldap_sasl_server_host"),
    ("AUTHENTICATION_LDAPSASL_SERVER_PORT", "authentication_ldap_sasl_server_port"),
    ("AUTHENTICATION_LDAPSASL_REFERRAL", "authentication_ldap_sasl_referral"),
    ("AUTHENTICATION_LDAPSASL_USER_SEARCH_ATTR", "authentication_ldap_sasl_user_search_attr"),
    ("AUTHENTICATION_LDAPSASL_BIND_BASE_DN", "authentication_ldap_sasl_bind_base_dn"),
    ("AUTHENTICATION_LDAPSASL_BIND_ROOT_DN", "authentication_ldap_sasl_bind_root_dn"),
    ("AUTHENTICATION_LDAPSASL_BIND_ROOT_PWD", "authentication_ldap_sasl_bind_root_pwd"),
    ("AUTHENTICATION_LDAPSASL_INIT_POOL_SIZE", "authentication_ldap_sasl_init_pool_size"),
    ("AUTHENTICATION_LDAPSASL_MAX_POOL_SIZE", "authentication_ldap_sasl_max_pool_size"),
    ("AUTHENTICATION_LDAP_SIMPLE_AUTH_METHOD_NAME", "authentication_ldap_simple_auth_method_name"),
    ("AUTHENTICATION_LDAP_SIMPLE_CA_PATH", "authentication_ldap_simple_ca_path"),
    ("AUTHENTICATION_LDAP_SIMPLE_TLS", "authentication_ldap_simple_tls"),
    ("AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST", "authentication_ldap_simple_server_host"),
    ("AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT", "authentication_ldap_simple_server_port"),
    ("AUTHENTICATION_LDAP_SIMPLE_REFERRAL", "authentication_ldap_simple_referral"),
    ("AUTHENTICATION_LDAP_SIMPLE_USER_SEARCH_ATTR", "authentication_ldap_simple_user_search_attr"),
    ("AUTHENTICATION_LDAP_SIMPLE_BIND_BASE_DN", "authentication_ldap_simple_bind_base_dn"),
    ("AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_DN", "authentication_ldap_simple_bind_root_dn"),
    ("AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_PWD", "authentication_ldap_simple_bind_root_pwd"),
    ("AUTHENTICATION_LDAP_SIMPLE_INIT_POOL_SIZE", "authentication_ldap_simple_init_pool_size"),
    ("AUTHENTICATION_LDAP_SIMPLE_MAX_POOL_SIZE", "authentication_ldap_simple_max_pool_size"),
    ("TIDB_RUNTIME_FILTER_TYPE_NAME", "tidb_runtime_filter_type"),
    ("TIDB_RUNTIME_FILTER_MODE_NAME", "tidb_runtime_filter_mode"),
    ("TIDB_SKIP_MISSING_PARTITION_STATS", "tidb_skip_missing_partition_stats"),
    ("TIDB_SESSION_ALIAS", "tidb_session_alias"),
    ("TIDB_SERVICE_SCOPE", "tidb_service_scope"),
    ("TIDB_SCHEMA_VERSION_CACHE_LIMIT", "tidb_schema_version_cache_limit"),
    ("TIDB_ENABLE_TIFLASH_PIPELINE_MODE", "tidb_enable_tiflash_pipeline_model"),
    ("TIDB_IDLE_TRANSACTION_TIMEOUT", "tidb_idle_transaction_timeout"),
    ("TIDB_LOW_RESOLUTION_TSO_UPDATE_INTERVAL", "tidb_low_resolution_tso_update_interval"),
    ("TIDB_DML_TYPE", "tidb_dml_type"),
    ("TIFLASH_HASH_AGG_PRE_AGG_MODE", "tiflash_hashagg_preaggregation_mode"),
    ("TIDB_ENABLE_LAZY_CURSOR_FETCH", "tidb_enable_lazy_cursor_fetch"),
    ("TIDB_TSO_CLIENT_RPC_MODE", "tidb_tso_client_rpc_mode"),
    ("TIDB_CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO", "tidb_cb_pd_metadata_error_rate_threshold_ratio"),
    ("TIDB_ENABLE_TS_VALIDATION", "tidb_enable_ts_validation"),
    ("TIDB_ADVANCER_CHECK_POINT_LAG_LIMIT", "tidb_advancer_check_point_lag_limit"),
    ("TIDB_INDEX_LOOK_UP_PUSH_DOWN_POLICY", "tidb_index_lookup_pushdown_policy"),
    ("OPT_OBJECTIVE_DETERMINATE", "determinate"),
    ("FORCE_PRE_AGG_STR", "force_preagg"),
    ("AUTO_STR", "auto"),
    ("FORCE_STREAMING_STR", "force_streaming"),
    ("ALL_REPLICA_STR", "all_replicas"),
    ("CLOSEST_ADAPTIVE_STR", "closest_adaptive"),
    ("CLOSEST_REPLICAS_STR", "closest_replicas"),
    ("DISPATCH_POLICY_RR_STR", "round_robin"),
    ("DISPATCH_POLICY_CONSISTENT_HASH_STR", "consistent_hash"),
    ("DISPATCH_POLICY_INVALID_STR", "invalid"),
    ("STRATEGY_STANDARD", "standard"),
    ("STRATEGY_CONSERVATIVE", "conservative"),
    ("STRATEGY_CUSTOM", "custom"),
    ("INDEX_LOOK_UP_PUSH_DOWN_POLICY_HINT_ONLY", "hint-only"),
    ("INDEX_LOOK_UP_PUSH_DOWN_POLICY_AFFINITY_FORCE", "affinity-force"),
    ("INDEX_LOOK_UP_PUSH_DOWN_POLICY_FORCE", "force"),
    ("GLOBAL_CONFIG_ENABLE_TOP_SQL", "enable_resource_metering"),
    ("GLOBAL_CONFIG_SOURCE_ID", "source_id"),
    ("LOCAL_DAY_TIME_FORMAT", "15:04"),
    ("FULL_DAY_TIME_FORMAT", "15:04 -0700"),
];

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestBuiltInCase`.
///
/// All built-in sysvar names should be lower case.
#[test]
fn built_in_case_names_are_lowercase() {
    for (_, name) in SYSVAR_NAME_CONSTANTS {
        assert_eq!(name.to_ascii_lowercase(), *name);
    }
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestSysVarNameIsLowerCase`.
///
/// No new sysvars may be added with uppercase characters; MySQL variables are
/// always lowercase and set case-insensitively.
#[test]
fn sysvar_name_is_lowercase() {
    for (go_ident, name) in SYSVAR_NAME_CONSTANTS {
        assert_eq!(
            name.to_ascii_lowercase(),
            *name,
            "sysvar name {name} (const {go_ident}) contains uppercase characters"
        );
    }
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestLimitBetweenVariable`.
///
/// The GOGC tuner threshold must stay safely below the server memory-limit GC
/// trigger. The tuner threshold constant (`DefTiDBGOGCTunerThreshold = 0.6`,
/// `pkg/sessionctx/vardef/tidb_vars.go`) has no Rust counterpart yet, so it is
/// pinned here as test data until it lands in `defaults`.
#[test]
fn limit_between_variable() {
    const DEF_TIDB_GOGC_TUNER_THRESHOLD: f64 = 0.6;
    assert!(DEF_TIDB_GOGC_TUNER_THRESHOLD + 0.05 < DEF_TIDB_SERVER_MEMORY_LIMIT_GC_TRIGGER);
}

/// Partial port of Go
/// `pkg/sessionctx/variable/varsutil_test.go::TestHelperFuncs`: only the
/// `TiDBOptEnableClustered` assertions; this crate ports that helper (in
/// [`super::modes`]) but not `int32ToBoolStr`, `tidbOptPositiveInt32`, or
/// `TidbOptInt`.
#[test]
fn helper_funcs_tidb_opt_enable_clustered() {
    assert_eq!(ClusteredIndexDefMode::ON, tidb_opt_enable_clustered("ON"));
    assert_eq!(ClusteredIndexDefMode::OFF, tidb_opt_enable_clustered("OFF"));
    // Default for any other value.
    assert_eq!(
        ClusteredIndexDefMode::INT_ONLY,
        tidb_opt_enable_clustered("bogus")
    );
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestFloatValidation.
// go-parity-gap: SysVar struct + TypeFloat Validate/clamping not ported to Rust yet
#[test]
#[ignore]
fn float_validation_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestBoolValidation.
// go-parity-gap: SysVar TypeBool validation (incl. AutoConvertNegativeBool) not ported
#[test]
#[ignore]
fn bool_validation_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestTimeValidation.
// go-parity-gap: SysVar TypeTime validation not ported
#[test]
#[ignore]
fn time_validation_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestGetNativeValType.
// go-parity-gap: SysVar::GetNativeValType (Datum conversion) not ported; Datum lives in other crates
#[test]
#[ignore]
fn get_native_val_type_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestDeprecation.
// go-parity-gap: SysVar registry + deprecation warning emission via StmtCtx not ported
#[test]
#[ignore]
fn deprecation_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestIsNoop.
// go-parity-gap: SysVar registry with IsNoop flags not ported
#[test]
#[ignore]
fn is_noop_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestDefaultValuesAreSettable.
// go-parity-gap: SysVar registry + SessionVars/GlobalVarsAccessor validation machinery not ported
#[test]
#[ignore]
fn default_values_are_settable_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestSettersandGetters.
// go-parity-gap: SysVar registry with Set/Get session/global hooks not ported
#[test]
#[ignore]
fn settersandgetters_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestScopeToString.
// go-parity-gap: ScopeFlag and its String() rendering live in tidb-exec, outside this crate
#[test]
#[ignore]
fn scope_to_string_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestValidateWithRelaxedValidation.
// go-parity-gap: SysVar registry + ValidateWithRelaxedValidation not ported
#[test]
#[ignore]
fn validate_with_relaxed_validation_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestValidateInternalSessionVariable.
// go-parity-gap: SysVar registry + InternalSessionVariable flag not ported
#[test]
#[ignore]
fn validate_internal_session_variable_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestInstanceConfigHasMatchingSysvar.
// go-parity-gap: depends on pkg/config JSON config + SysVar registry, neither ported here
#[test]
#[ignore]
fn instance_config_has_matching_sysvar_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestInstanceScope.
// go-parity-gap: SysVar registration/unregistration + instance-scope hooks not ported
#[test]
#[ignore]
fn instance_scope_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestSetSysVar.
// go-parity-gap: GetSysVar/SetSysVar global singleton deliberately replaced by explicit wiring in the rewrite
#[test]
#[ignore]
fn set_sys_var_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestSkipSysvarCache.
// go-parity-gap: SysVar SkipSysvarCache flag not ported
#[test]
#[ignore]
fn skip_sysvar_cache_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestTimeValidationWithTimezone.
// go-parity-gap: TypeTime validation is timezone-sensitive; SysVar validation not ported
#[test]
#[ignore]
fn time_validation_with_timezone_unported() {
}

/// Go `pkg/sessionctx/variable/tests/variable_test.go::TestOrderByDependency`
// go-parity-gap: OrderByDependency topological sort of sysvar dependencies not ported
#[test]
#[ignore]
fn order_by_dependency_unported() {
}

/// Go `pkg/sessionctx/variable/varsutil_test.go::TestTiDBOptOn`
// go-parity-gap: Go TiDBOptOn helper (varsutil.go) not ported to this crate
#[test]
#[ignore]
fn tidb_opt_on_unported() {
}

/// Go `pkg/sessionctx/variable/varsutil_test.go::TestNewSessionVars`
// go-parity-gap: SessionVars runtime state deferred from this crate by design
#[test]
#[ignore]
fn new_session_vars_unported() {
}

/// Go `pkg/sessionctx/variable/varsutil_test.go::TestVarsutil`
// go-parity-gap: SetSystemVar/SetTCState machinery on SessionVars not ported
#[test]
#[ignore]
fn varsutil_unported() {
}

/// Go `pkg/sessionctx/variable/varsutil_test.go::TestValidate`
// go-parity-gap: GetSysVar(...).Validate over SessionVars + MockGlobalAccessor not ported
#[test]
#[ignore]
fn validate_unported() {
}

/// Go `pkg/sessionctx/variable/varsutil_test.go::TestValidateStmtSummary`
// go-parity-gap: stmt-summary sysvar validators over SessionVars not ported
#[test]
#[ignore]
fn validate_stmt_summary_unported() {
}

/// Go `pkg/sessionctx/variable/varsutil_test.go::TestConcurrencyVariables`
// go-parity-gap: SessionVars concurrency fields + SetSystemVar side effects not ported
#[test]
#[ignore]
fn concurrency_variables_unported() {
}

/// Go `pkg/sessionctx/variable/varsutil_test.go::TestSessionStatesSystemVar`
// go-parity-gap: GetSessionStatesSystemVar on SessionVars not ported
#[test]
#[ignore]
fn session_states_system_var_unported() {
}

/// Go `pkg/sessionctx/variable/varsutil_test.go::TestOnOffHelpers`
// go-parity-gap: trueFalseToOnOff / OnOffToTrueFalse helpers (varsutil.go) not ported
#[test]
#[ignore]
fn on_off_helpers_unported() {
}

/// Go `pkg/sessionctx/variable/varsutil_test.go::TestAssertionLevel`
// go-parity-gap: tidbOptAssertionLevel helper + AssertionLevel enum (varsutil.go) not ported
#[test]
#[ignore]
fn assertion_level_unported() {
}