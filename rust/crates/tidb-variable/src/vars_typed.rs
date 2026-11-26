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

//! Go `SessionVars`' typed fields a statement reads, each maintained by its
//! variable's `SetSession` hook (`sysvar.go`) and initialized as
//! `NewSessionVars` does, from the variable's registry default. Session
//! start copies the global tier through the same hooks
//! (`loadCommonGlobalVariablesIfNeeded`), so a statement reads the field and
//! never the variable table.

use crate::vars::SessionVars;
use crate::VarError;

/// The value `NewSessionVars` starts a field from: the variable's effective
/// default (its registry value, or what the server set it to at startup, Go
/// `SetSysVar`).
fn registry_default(name: &str) -> std::borrow::Cow<'static, str> {
    crate::sysvar::get_sys_var(name).map_or(std::borrow::Cow::Borrowed(""), crate::sysvar::effective_default_value)
}

pub(crate) fn registry_default_collation_for_utf8mb4() -> String {
    parse_default_collation_for_utf8mb4(&registry_default("default_collation_for_utf8mb4"))
}

fn parse_default_collation_for_utf8mb4(value: &str) -> String {
    value.to_owned()
}

pub(crate) fn registry_allow_write_row_id() -> bool {
    parse_allow_write_row_id(&registry_default("tidb_opt_write_row_id"))
}

fn parse_allow_write_row_id(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_stable_result_mode() -> bool {
    parse_enable_stable_result_mode(&registry_default("tidb_enable_ordered_result_mode"))
}

fn parse_enable_stable_result_mode(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_sysdate_is_now() -> bool {
    parse_sysdate_is_now(&registry_default("tidb_sysdate_is_now"))
}

fn parse_sysdate_is_now(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_skip_ascii_check() -> bool {
    parse_skip_ascii_check(&registry_default("tidb_skip_ascii_check"))
}

fn parse_skip_ascii_check(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_skip_utf8_check() -> bool {
    parse_skip_utf8_check(&registry_default("tidb_skip_utf8_check"))
}

fn parse_skip_utf8_check(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_allow_auto_rand_explicit_insert() -> bool {
    parse_allow_auto_rand_explicit_insert(&registry_default("allow_auto_random_explicit_insert"))
}

fn parse_allow_auto_rand_explicit_insert(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_shard_allocate_step() -> u64 {
    parse_shard_allocate_step(&registry_default("tidb_shard_allocate_step"))
}

fn parse_shard_allocate_step(value: &str) -> u64 {
    value.parse::<u64>().unwrap_or(i64::MAX as u64)
}

pub(crate) fn registry_enable_no_backslash_escapes_in_like() -> bool {
    parse_enable_no_backslash_escapes_in_like(&registry_default("tidb_enable_no_backslash_escapes_in_like"))
}

fn parse_enable_no_backslash_escapes_in_like(value: &str) -> bool {
    !matches!(value, "OFF" | "off" | "0")
}

pub(crate) fn registry_div_precision_increment() -> u32 {
    parse_div_precision_increment(&registry_default("div_precision_increment"))
}

fn parse_div_precision_increment(value: &str) -> u32 {
    value.parse::<u32>().ok().filter(|value| *value > 0).unwrap_or(4)
}

pub(crate) fn registry_windowing_use_high_precision() -> bool {
    parse_windowing_use_high_precision(&registry_default("windowing_use_high_precision"))
}

fn parse_windowing_use_high_precision(value: &str) -> bool {
    !matches!(value, "OFF" | "off" | "0")
}

pub(crate) fn registry_enable_pipelined_window_exec() -> bool {
    parse_enable_pipelined_window_exec(&registry_default("tidb_enable_pipelined_window_function"))
}

fn parse_enable_pipelined_window_exec(value: &str) -> bool {
    !matches!(value, "OFF" | "off" | "0")
}

pub(crate) fn registry_cte_max_recursion_depth() -> i64 {
    parse_cte_max_recursion_depth(&registry_default("cte_max_recursion_depth"))
}

fn parse_cte_max_recursion_depth(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(1000)
}

pub(crate) fn registry_tidb_opt_join_reorder_threshold() -> i32 {
    parse_tidb_opt_join_reorder_threshold(&registry_default("tidb_opt_join_reorder_threshold"))
}

fn parse_tidb_opt_join_reorder_threshold(value: &str) -> i32 {
    value.parse::<i32>().unwrap_or(tidb_vardef::defaults::DEF_TIDB_OPT_JOIN_REORDER_THRESHOLD as i32)
}

pub(crate) fn registry_default_str_match_selectivity() -> f64 {
    parse_default_str_match_selectivity(&registry_default("tidb_default_string_match_selectivity"))
}

fn parse_default_str_match_selectivity(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or(0.0)
}

pub(crate) fn registry_enable_pseudo_for_outdated_stats() -> bool {
    parse_enable_pseudo_for_outdated_stats(&registry_default("tidb_enable_pseudo_for_outdated_stats"))
}

fn parse_enable_pseudo_for_outdated_stats(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_stats_load_sync_wait() -> u64 {
    parse_stats_load_sync_wait(&registry_default("tidb_stats_load_sync_wait"))
}

fn parse_stats_load_sync_wait(value: &str) -> u64 {
    value.parse::<u64>().unwrap_or(tidb_vardef::defaults::DEF_TIDB_STATS_LOAD_SYNC_WAIT as u64)
}

pub(crate) fn registry_opt_index_prune_threshold() -> i32 {
    parse_opt_index_prune_threshold(&registry_default("tidb_opt_index_prune_threshold"))
}

fn parse_opt_index_prune_threshold(value: &str) -> i32 {
    value.parse::<i32>().unwrap_or(20)
}

pub(crate) fn registry_range_max_size() -> i64 {
    parse_range_max_size(&registry_default("tidb_opt_range_max_size"))
}

fn parse_range_max_size(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(tidb_vardef::defaults::DEF_TIDB_OPT_RANGE_MAX_SIZE)
}

pub(crate) fn registry_opt_prefix_index_single_scan() -> bool {
    parse_opt_prefix_index_single_scan(&registry_default("tidb_opt_prefix_index_single_scan"))
}

fn parse_opt_prefix_index_single_scan(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_allow_prefer_range_scan() -> bool {
    parse_allow_prefer_range_scan(&registry_default("tidb_opt_prefer_range_scan"))
}

fn parse_allow_prefer_range_scan(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_optimizer_use_invisible_indexes() -> bool {
    parse_optimizer_use_invisible_indexes(&registry_default("tidb_opt_use_invisible_indexes"))
}

fn parse_optimizer_use_invisible_indexes(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_always_keep_join_key() -> bool {
    parse_always_keep_join_key(&registry_default("tidb_opt_always_keep_join_key"))
}

fn parse_always_keep_join_key(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_allow_agg_push_down() -> bool {
    parse_allow_agg_push_down(&registry_default("tidb_opt_agg_push_down"))
}

fn parse_allow_agg_push_down(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_3_stage_distinct_agg() -> bool {
    parse_enable_3_stage_distinct_agg(&registry_default("tidb_opt_three_stage_distinct_agg"))
}

fn parse_enable_3_stage_distinct_agg(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_3_stage_multi_distinct_agg() -> bool {
    parse_enable_3_stage_multi_distinct_agg(&registry_default("tidb_opt_enable_three_stage_multi_distinct_agg"))
}

fn parse_enable_3_stage_multi_distinct_agg(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_tiflash_pre_agg_mode() -> String {
    parse_tiflash_pre_agg_mode(&registry_default("tiflash_hashagg_preaggregation_mode"))
}

fn parse_tiflash_pre_agg_mode(value: &str) -> String {
    value.to_owned()
}

pub(crate) fn registry_opt_partial_ordered_index_for_topn() -> String {
    parse_opt_partial_ordered_index_for_topn(&registry_default("tidb_opt_partial_ordered_index_for_topn"))
}

fn parse_opt_partial_ordered_index_for_topn(value: &str) -> String {
    value.to_owned()
}

pub(crate) fn registry_enable_unsafe_substitute() -> bool {
    parse_enable_unsafe_substitute(&registry_default("tidb_enable_unsafe_substitute"))
}

fn parse_enable_unsafe_substitute(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_force_inline_cte() -> bool {
    parse_enable_force_inline_cte(&registry_default("tidb_opt_force_inline_cte"))
}

fn parse_enable_force_inline_cte(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_mpp_shared_cte_execution() -> bool {
    parse_enable_mpp_shared_cte_execution(&registry_default("tidb_opt_enable_mpp_shared_cte_execution"))
}

fn parse_enable_mpp_shared_cte_execution(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_advanced_join_hint() -> bool {
    parse_enable_advanced_join_hint(&registry_default("tidb_opt_advanced_join_hint"))
}

fn parse_enable_advanced_join_hint(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_semi_join_rewrite() -> bool {
    parse_enable_semi_join_rewrite(&registry_default("tidb_opt_enable_semi_join_rewrite"))
}

fn parse_enable_semi_join_rewrite(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_alternative_logical_plans() -> bool {
    parse_enable_alternative_logical_plans(&registry_default("tidb_opt_enable_alternative_logical_plans"))
}

fn parse_enable_alternative_logical_plans(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_explain_non_evaled_sub_query() -> bool {
    parse_explain_non_evaled_sub_query(&registry_default("tidb_opt_enable_non_eval_scalar_subquery"))
}

fn parse_explain_non_evaled_sub_query(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_set_allow_in_subq_to_join_and_agg() -> bool {
    parse_set_allow_in_subq_to_join_and_agg(&registry_default("tidb_opt_insubq_to_join_and_agg"))
}

fn parse_set_allow_in_subq_to_join_and_agg(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_no_decorrelate_in_select() -> bool {
    parse_enable_no_decorrelate_in_select(&registry_default("tidb_opt_enable_no_decorrelate_in_select"))
}

fn parse_enable_no_decorrelate_in_select(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_skew_distinct_agg() -> bool {
    parse_enable_skew_distinct_agg(&registry_default("tidb_opt_skew_distinct_agg"))
}

fn parse_enable_skew_distinct_agg(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_advanced_join_reorder() -> bool {
    parse_enable_advanced_join_reorder(&registry_default("tidb_opt_enable_advanced_join_reorder"))
}

fn parse_enable_advanced_join_reorder(value: &str) -> bool {
    !matches!(value, "OFF" | "off" | "0")
}

pub(crate) fn registry_constraint_check_in_place() -> bool {
    parse_constraint_check_in_place(&registry_default("tidb_constraint_check_in_place"))
}

fn parse_constraint_check_in_place(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_opt_ordering_idx_sel_ratio() -> f64 {
    parse_opt_ordering_idx_sel_ratio(&registry_default("tidb_opt_ordering_index_selectivity_ratio"))
}

fn parse_opt_ordering_idx_sel_ratio(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or(0.01)
}

pub(crate) fn registry_index_join_max_scan_rows_ratio() -> f64 {
    parse_index_join_max_scan_rows_ratio(&registry_default("tidb_opt_index_join_max_scan_rows_ratio"))
}

fn parse_index_join_max_scan_rows_ratio(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or(0.0)
}

pub(crate) fn registry_opt_ordering_idx_sel_thresh() -> f64 {
    parse_opt_ordering_idx_sel_thresh(&registry_default("tidb_opt_ordering_index_selectivity_threshold"))
}

fn parse_opt_ordering_idx_sel_thresh(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or(tidb_vardef::defaults::DEF_TIDB_OPT_ORDERING_IDX_SEL_THRESH)
}

pub(crate) fn registry_allow_projection_push_down() -> bool {
    parse_allow_projection_push_down(&registry_default("tidb_opt_projection_push_down"))
}

fn parse_allow_projection_push_down(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_allow_distinct_agg_push_down() -> bool {
    parse_allow_distinct_agg_push_down(&registry_default("tidb_opt_distinct_agg_push_down"))
}

fn parse_allow_distinct_agg_push_down(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_cascades_planner() -> bool {
    parse_enable_cascades_planner(&registry_default("tidb_enable_cascades_planner"))
}

fn parse_enable_cascades_planner(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_hash_join() -> bool {
    parse_enable_hash_join(&registry_default("tidb_opt_enable_hash_join"))
}

fn parse_enable_hash_join(value: &str) -> bool {
    !matches!(value, "OFF" | "off" | "0")
}

pub(crate) fn registry_optimizer_enable_naaj() -> bool {
    parse_optimizer_enable_naaj(&registry_default("tidb_enable_null_aware_anti_join"))
}

fn parse_optimizer_enable_naaj(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_limit_push_down_threshold() -> u64 {
    parse_limit_push_down_threshold(&registry_default("tidb_opt_limit_push_down_threshold"))
}

fn parse_limit_push_down_threshold(value: &str) -> u64 {
    value.parse::<u64>().unwrap_or(tidb_vardef::defaults::DEF_OPT_LIMIT_PUSH_DOWN_THRESHOLD as u64)
}

pub(crate) fn registry_read_staleness() -> i64 {
    parse_read_staleness(&registry_default("tidb_read_staleness"))
}

fn parse_read_staleness(value: &str) -> i64 {
    value.trim().parse::<i64>().unwrap_or(0)
}

pub(crate) fn registry_index_lookup_push_down_policy() -> String {
    parse_index_lookup_push_down_policy(&registry_default("tidb_index_lookup_pushdown_policy"))
}

fn parse_index_lookup_push_down_policy(value: &str) -> String {
    value.to_owned()
}

pub(crate) fn registry_tidb_opt_join_reorder_through_proj() -> bool {
    parse_tidb_opt_join_reorder_through_proj(&registry_default("tidb_opt_join_reorder_through_proj"))
}

fn parse_tidb_opt_join_reorder_through_proj(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_tidb_opt_join_reorder_through_sel() -> bool {
    parse_tidb_opt_join_reorder_through_sel(&registry_default("tidb_opt_join_reorder_through_sel"))
}

fn parse_tidb_opt_join_reorder_through_sel(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_outer_join_reorder() -> bool {
    parse_enable_outer_join_reorder(&registry_default("tidb_enable_outer_join_reorder"))
}

fn parse_enable_outer_join_reorder(value: &str) -> bool {
    !matches!(value, "OFF" | "off" | "0")
}

pub(crate) fn registry_enable_index_merge() -> bool {
    parse_enable_index_merge(&registry_default("tidb_enable_index_merge"))
}

fn parse_enable_index_merge(value: &str) -> bool {
    !matches!(value, "OFF" | "off" | "0")
}

pub(crate) fn registry_skip_missing_partition_stats() -> bool {
    parse_skip_missing_partition_stats(&registry_default("tidb_skip_missing_partition_stats"))
}

fn parse_skip_missing_partition_stats(value: &str) -> bool {
    !matches!(value, "OFF" | "off" | "0")
}

pub(crate) fn registry_optimizer_enable_new_only_full_group_by_check() -> bool {
    parse_optimizer_enable_new_only_full_group_by_check(&registry_default("tidb_enable_new_only_full_group_by_check"))
}

fn parse_optimizer_enable_new_only_full_group_by_check(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_enable_auto_increment_in_generated() -> bool {
    parse_enable_auto_increment_in_generated(&registry_default("tidb_enable_auto_increment_in_generated"))
}

fn parse_enable_auto_increment_in_generated(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_primary_key_required() -> bool {
    parse_primary_key_required(&registry_default("sql_require_primary_key"))
}

fn parse_primary_key_required(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_tmp_table_size() -> i64 {
    parse_tmp_table_size(&registry_default("tidb_tmp_table_max_size"))
}

fn parse_tmp_table_size(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(64 << 20)
}

pub(crate) fn registry_remove_orderby_in_subquery() -> bool {
    parse_remove_orderby_in_subquery(&registry_default("tidb_remove_orderby_in_subquery"))
}

fn parse_remove_orderby_in_subquery(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_analyze_version() -> i64 {
    parse_analyze_version(&registry_default("tidb_analyze_version"))
}

fn parse_analyze_version(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(0)
}

pub(crate) fn registry_enabled_rate_limit_action() -> bool {
    parse_enabled_rate_limit_action(&registry_default("tidb_enable_rate_limit_action"))
}

fn parse_enabled_rate_limit_action(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_lock_wait_timeout() -> u64 {
    parse_lock_wait_timeout(&registry_default("innodb_lock_wait_timeout"))
}

fn parse_lock_wait_timeout(value: &str) -> u64 {
    // Go `SetSession`: `s.LockWaitTimeout = lockWaitSec * 1000`.
    value.parse::<u64>().unwrap_or(tidb_vardef::defaults::DEF_INNODB_LOCK_WAIT_TIMEOUT as u64) * 1000
}

pub(crate) fn registry_replica_closest_read_threshold() -> i64 {
    parse_replica_closest_read_threshold(&registry_default("tidb_adaptive_closest_read_threshold"))
}

fn parse_replica_closest_read_threshold(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(4096)
}

pub(crate) fn registry_load_based_replica_read_threshold() -> i64 {
    parse_load_based_replica_read_threshold(&registry_default("tidb_load_based_replica_read_threshold"))
}

fn parse_load_based_replica_read_threshold(value: &str) -> i64 {
    tidb_config_configtypes::parse_duration(value).unwrap_or(1_000_000_000)
}

pub(crate) fn registry_tikv_client_read_timeout() -> u64 {
    parse_tikv_client_read_timeout(&registry_default("tikv_client_read_timeout"))
}

fn parse_tikv_client_read_timeout(value: &str) -> u64 {
    value.parse::<u64>().unwrap_or(0)
}

pub(crate) fn registry_isolation_read_engines() -> String {
    parse_isolation_read_engines(&registry_default("tidb_isolation_read_engines"))
}

fn parse_isolation_read_engines(value: &str) -> String {
    value.to_owned()
}

pub(crate) fn registry_init_chunk_size() -> usize {
    parse_init_chunk_size(&registry_default("tidb_init_chunk_size"))
}

fn parse_init_chunk_size(value: &str) -> usize {
    value.parse::<usize>().unwrap_or(tidb_vardef::defaults::DEF_INIT_CHUNK_SIZE as usize)
}

pub(crate) fn registry_max_chunk_size() -> usize {
    parse_max_chunk_size(&registry_default("tidb_max_chunk_size"))
}

fn parse_max_chunk_size(value: &str) -> usize {
    value.parse::<usize>().unwrap_or(tidb_vardef::defaults::DEF_MAX_CHUNK_SIZE as usize)
}

pub(crate) fn registry_metric_schema_step() -> i64 {
    parse_metric_schema_step(&registry_default("tidb_metric_query_step"))
}

fn parse_metric_schema_step(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(tidb_vardef::defaults::DEF_TIDB_METRIC_SCHEMA_STEP)
}

pub(crate) fn registry_metric_schema_range_duration() -> i64 {
    parse_metric_schema_range_duration(&registry_default("tidb_metric_query_range_duration"))
}

fn parse_metric_schema_range_duration(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(tidb_vardef::defaults::DEF_TIDB_METRIC_SCHEMA_RANGE_DURATION)
}

pub(crate) fn registry_enable_parallel_apply() -> bool {
    parse_enable_parallel_apply(&registry_default("tidb_enable_parallel_apply"))
}

fn parse_enable_parallel_apply(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_allow_derive_topn() -> bool {
    parse_allow_derive_topn(&registry_default("tidb_opt_derive_topn"))
}

fn parse_allow_derive_topn(value: &str) -> bool {
    value.eq_ignore_ascii_case("on") || value == "1"
}

pub(crate) fn registry_mem_quota_apply_cache() -> i64 {
    parse_mem_quota_apply_cache(&registry_default("tidb_mem_quota_apply_cache"))
}

fn parse_mem_quota_apply_cache(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_APPLY_CACHE)
}

pub(crate) fn registry_executor_concurrency() -> usize {
    parse_executor_concurrency(&registry_default("tidb_executor_concurrency"))
}

pub(crate) fn registry_pipelined_dml_config() -> crate::session_vars::PipelinedDmlConfig {
    parse_pipelined_dml_resource_policy(registry_default("tidb_pipelined_dml_resource_policy").trim())
        .unwrap_or_default()
}

/// Go `setPipelinedDmlResourcePolicy`'s parse of a trimmed value: `standard`,
/// `conservative`, or `custom{key=value, ...}` over `concurrency`,
/// `resolve_concurrency` and `write_throttle_ratio`; `None` is Go's
/// `ErrWrongValueForVar`.
fn parse_pipelined_dml_resource_policy(value: &str) -> Option<crate::session_vars::PipelinedDmlConfig> {
    use tidb_vardef::defaults::{DEFAULT_FLUSH_CONCURRENCY, DEFAULT_RESOLVE_CONCURRENCY};
    use tidb_vardef::tidb_vars::{
        CONSERVATIVE_FLUSH_CONCURRENCY, CONSERVATIVE_RESOLVE_CONCURRENCY, MAX_PIPELINED_DML_CONCURRENCY,
        MIN_PIPELINED_DML_CONCURRENCY, STRATEGY_CONSERVATIVE, STRATEGY_CUSTOM, STRATEGY_STANDARD,
    };
    let low_val = value.to_lowercase();
    if low_val == STRATEGY_STANDARD {
        return Some(crate::session_vars::PipelinedDmlConfig {
            pipelined_flush_concurrency: DEFAULT_FLUSH_CONCURRENCY,
            pipelined_resolve_lock_concurrency: DEFAULT_RESOLVE_CONCURRENCY,
            pipelined_write_throttle_ratio: 0.0,
        });
    }
    if low_val == STRATEGY_CONSERVATIVE {
        return Some(crate::session_vars::PipelinedDmlConfig {
            pipelined_flush_concurrency: CONSERVATIVE_FLUSH_CONCURRENCY,
            pipelined_resolve_lock_concurrency: CONSERVATIVE_RESOLVE_CONCURRENCY,
            pipelined_write_throttle_ratio: 0.0,
        });
    }
    // A temporary config holds the new values to avoid partial application.
    let mut new_config = crate::session_vars::PipelinedDmlConfig {
        pipelined_flush_concurrency: DEFAULT_FLUSH_CONCURRENCY,
        pipelined_resolve_lock_concurrency: DEFAULT_RESOLVE_CONCURRENCY,
        pipelined_write_throttle_ratio: 0.0,
    };
    let remaining = low_val.strip_prefix(STRATEGY_CUSTOM)?.trim();
    if remaining.len() < 2 || !remaining.starts_with('{') || !remaining.ends_with('}') {
        return None;
    }
    let content = remaining[1..remaining.len() - 1].trim();
    if content.is_empty() {
        return None;
    }
    let concurrency = |text: &str| {
        text.parse::<i64>()
            .ok()
            .filter(|concurrency| (MIN_PIPELINED_DML_CONCURRENCY..=MAX_PIPELINED_DML_CONCURRENCY).contains(concurrency))
    };
    for raw_param in content.split(',') {
        let param = raw_param.trim();
        if param.is_empty() {
            return None;
        }
        // Go `strings.FieldsFunc(param, '=' or ':')`: empty fields vanish.
        let parts = param.split(['=', ':']).filter(|part| !part.is_empty()).collect::<Vec<_>>();
        let [key, text] = parts.as_slice() else {
            return None;
        };
        let (key, text) = (key.trim(), text.trim());
        match key {
            "concurrency" => new_config.pipelined_flush_concurrency = concurrency(text)?,
            "resolve_concurrency" => new_config.pipelined_resolve_lock_concurrency = concurrency(text)?,
            "write_throttle_ratio" => {
                new_config.pipelined_write_throttle_ratio =
                    text.parse::<f64>().ok().filter(|ratio| (0.0..1.0).contains(ratio))?;
            }
            _ => return None,
        }
    }
    Some(new_config)
}

fn parse_executor_concurrency(value: &str) -> usize {
    value.parse::<usize>().ok().filter(|value| *value > 0).unwrap_or(tidb_vardef::defaults::DEF_EXECUTOR_CONCURRENCY as usize)
}

pub(crate) fn registry_hash_agg_partial_concurrency() -> i64 {
    parse_hash_agg_partial_concurrency(&registry_default("tidb_hashagg_partial_concurrency"))
}

fn parse_hash_agg_partial_concurrency(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(-1)
}

pub(crate) fn registry_hash_agg_final_concurrency() -> i64 {
    parse_hash_agg_final_concurrency(&registry_default("tidb_hashagg_final_concurrency"))
}

fn parse_hash_agg_final_concurrency(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(-1)
}

pub(crate) fn registry_ddl_reorg_priority() -> i32 {
    parse_ddl_reorg_priority(&registry_default("tidb_ddl_reorg_priority"))
}

fn parse_ddl_reorg_priority(value: &str) -> i32 {
    match value { "PRIORITY_NORMAL" => 0, "PRIORITY_HIGH" => 2, _ => 1 }
}

pub(crate) fn registry_mem_arbitrator_wait_averse() -> Option<bool> {
    parse_mem_arbitrator_wait_averse(&registry_default("tidb_mem_arbitrator_wait_averse"))
}

fn parse_mem_arbitrator_wait_averse(value: &str) -> Option<bool> {
    match value { "nolimit" => None, "1" => Some(true), _ => Some(false) }
}

pub(crate) fn registry_mem_arbitrator_query_reserved() -> i64 {
    parse_mem_arbitrator_query_reserved(&registry_default("tidb_mem_arbitrator_query_reserved"))
}

fn parse_mem_arbitrator_query_reserved(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(0)
}

impl SessionVars {
    /// Go `default_collation_for_utf8mb4`'s `SetSession`.
    pub(crate) fn set_session_default_collation_for_utf8mb4(&mut self, value: &str) -> Result<(), VarError> {
        self.default_collation_for_utf8mb4 = parse_default_collation_for_utf8mb4(value);
        Ok(())
    }

    /// Go `SessionVars.DefaultCollationForUTF8MB4` (`default_collation_for_utf8mb4`).
    #[must_use]
    pub fn default_collation_for_utf8mb4(&self) -> &str {
        &self.default_collation_for_utf8mb4
    }

    /// Go `tidb_opt_write_row_id`'s `SetSession`.
    pub(crate) fn set_session_allow_write_row_id(&mut self, value: &str) -> Result<(), VarError> {
        self.allow_write_row_id = parse_allow_write_row_id(value);
        Ok(())
    }

    /// Go `SessionVars.AllowWriteRowID` (`tidb_opt_write_row_id`).
    #[must_use]
    pub const fn allow_write_row_id(&self) -> bool {
        self.allow_write_row_id
    }

    /// Go `tidb_enable_ordered_result_mode`'s `SetSession`.
    pub(crate) fn set_session_enable_stable_result_mode(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_stable_result_mode = parse_enable_stable_result_mode(value);
        Ok(())
    }

    /// Go `SessionVars.EnableStableResultMode` (`tidb_enable_ordered_result_mode`).
    #[must_use]
    pub const fn enable_stable_result_mode(&self) -> bool {
        self.enable_stable_result_mode
    }

    /// Go `tidb_sysdate_is_now`'s `SetSession`.
    pub(crate) fn set_session_sysdate_is_now(&mut self, value: &str) -> Result<(), VarError> {
        self.sysdate_is_now = parse_sysdate_is_now(value);
        Ok(())
    }

    /// Go `SessionVars.SysdateIsNow` (`tidb_sysdate_is_now`).
    #[must_use]
    pub const fn sysdate_is_now(&self) -> bool {
        self.sysdate_is_now
    }

    /// Go `tidb_skip_ascii_check`'s `SetSession`.
    pub(crate) fn set_session_skip_ascii_check(&mut self, value: &str) -> Result<(), VarError> {
        self.skip_ascii_check = parse_skip_ascii_check(value);
        Ok(())
    }

    /// Go `SessionVars.SkipASCIICheck` (`tidb_skip_ascii_check`).
    #[must_use]
    pub const fn skip_ascii_check(&self) -> bool {
        self.skip_ascii_check
    }

    /// Go `tidb_skip_utf8_check`'s `SetSession`.
    pub(crate) fn set_session_skip_utf8_check(&mut self, value: &str) -> Result<(), VarError> {
        self.skip_utf8_check = parse_skip_utf8_check(value);
        Ok(())
    }

    /// Go `SessionVars.SkipUTF8Check` (`tidb_skip_utf8_check`).
    #[must_use]
    pub const fn skip_utf8_check(&self) -> bool {
        self.skip_utf8_check
    }

    /// Go `allow_auto_random_explicit_insert`'s `SetSession`.
    pub(crate) fn set_session_allow_auto_rand_explicit_insert(&mut self, value: &str) -> Result<(), VarError> {
        self.allow_auto_rand_explicit_insert = parse_allow_auto_rand_explicit_insert(value);
        Ok(())
    }

    /// Go `SessionVars.AllowAutoRandExplicitInsert` (`allow_auto_random_explicit_insert`).
    #[must_use]
    pub const fn allow_auto_rand_explicit_insert(&self) -> bool {
        self.allow_auto_rand_explicit_insert
    }

    /// Go `tidb_shard_allocate_step`'s `SetSession`.
    pub(crate) fn set_session_shard_allocate_step(&mut self, value: &str) -> Result<(), VarError> {
        self.shard_allocate_step = parse_shard_allocate_step(value);
        Ok(())
    }

    /// Go `SessionVars.ShardAllocateStep` (`tidb_shard_allocate_step`).
    #[must_use]
    pub const fn shard_allocate_step(&self) -> u64 {
        self.shard_allocate_step
    }

    /// Go `tidb_enable_no_backslash_escapes_in_like`'s `SetSession`.
    pub(crate) fn set_session_enable_no_backslash_escapes_in_like(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_no_backslash_escapes_in_like = parse_enable_no_backslash_escapes_in_like(value);
        Ok(())
    }

    /// Go `SessionVars.EnableNoBackslashEscapesInLike` (`tidb_enable_no_backslash_escapes_in_like`).
    #[must_use]
    pub const fn enable_no_backslash_escapes_in_like(&self) -> bool {
        self.enable_no_backslash_escapes_in_like
    }

    /// Go `div_precision_increment`'s `SetSession`.
    pub(crate) fn set_session_div_precision_increment(&mut self, value: &str) -> Result<(), VarError> {
        self.div_precision_increment = parse_div_precision_increment(value);
        Ok(())
    }

    /// Go `SessionVars.DivPrecisionIncrement` (`div_precision_increment`).
    #[must_use]
    pub const fn div_precision_increment(&self) -> u32 {
        self.div_precision_increment
    }

    /// Go `windowing_use_high_precision`'s `SetSession`.
    pub(crate) fn set_session_windowing_use_high_precision(&mut self, value: &str) -> Result<(), VarError> {
        self.windowing_use_high_precision = parse_windowing_use_high_precision(value);
        Ok(())
    }

    /// Go `SessionVars.WindowingUseHighPrecision` (`windowing_use_high_precision`).
    #[must_use]
    pub const fn windowing_use_high_precision(&self) -> bool {
        self.windowing_use_high_precision
    }

    /// Go `tidb_enable_pipelined_window_function`'s `SetSession`.
    pub(crate) fn set_session_enable_pipelined_window_exec(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_pipelined_window_exec = parse_enable_pipelined_window_exec(value);
        Ok(())
    }

    /// Go `SessionVars.EnablePipelinedWindowExec` (`tidb_enable_pipelined_window_function`).
    #[must_use]
    pub const fn enable_pipelined_window_exec(&self) -> bool {
        self.enable_pipelined_window_exec
    }

    /// Go `cte_max_recursion_depth`'s `SetSession`.
    pub(crate) fn set_session_cte_max_recursion_depth(&mut self, value: &str) -> Result<(), VarError> {
        self.cte_max_recursion_depth = parse_cte_max_recursion_depth(value);
        Ok(())
    }

    /// Go `SessionVars.CTEMaxRecursionDepth` (`cte_max_recursion_depth`).
    #[must_use]
    pub const fn cte_max_recursion_depth(&self) -> i64 {
        self.cte_max_recursion_depth
    }

    /// Go `tidb_opt_join_reorder_threshold`'s `SetSession`.
    pub(crate) fn set_session_tidb_opt_join_reorder_threshold(&mut self, value: &str) -> Result<(), VarError> {
        self.tidb_opt_join_reorder_threshold = parse_tidb_opt_join_reorder_threshold(value);
        Ok(())
    }

    /// Go `SessionVars.TiDBOptJoinReorderThreshold` (`tidb_opt_join_reorder_threshold`).
    #[must_use]
    pub const fn tidb_opt_join_reorder_threshold(&self) -> i32 {
        self.tidb_opt_join_reorder_threshold
    }

    /// Go `tidb_default_string_match_selectivity`'s `SetSession`.
    pub(crate) fn set_session_default_str_match_selectivity(&mut self, value: &str) -> Result<(), VarError> {
        self.default_str_match_selectivity = parse_default_str_match_selectivity(value);
        Ok(())
    }

    /// Go `SessionVars.DefaultStrMatchSelectivity` (`tidb_default_string_match_selectivity`).
    #[must_use]
    pub const fn default_str_match_selectivity(&self) -> f64 {
        self.default_str_match_selectivity
    }

    /// Go `tidb_enable_pseudo_for_outdated_stats`'s `SetSession`.
    pub(crate) fn set_session_enable_pseudo_for_outdated_stats(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_pseudo_for_outdated_stats = parse_enable_pseudo_for_outdated_stats(value);
        Ok(())
    }

    /// Go `SessionVars.EnablePseudoForOutdatedStats` (`tidb_enable_pseudo_for_outdated_stats`).
    #[must_use]
    pub const fn enable_pseudo_for_outdated_stats(&self) -> bool {
        self.enable_pseudo_for_outdated_stats
    }

    /// Go `tidb_stats_load_sync_wait`'s `SetSession`.
    pub(crate) fn set_session_stats_load_sync_wait(&mut self, value: &str) -> Result<(), VarError> {
        self.stats_load_sync_wait = parse_stats_load_sync_wait(value);
        Ok(())
    }

    /// Go `SessionVars.StatsLoadSyncWait` (`tidb_stats_load_sync_wait`).
    #[must_use]
    pub const fn stats_load_sync_wait(&self) -> u64 {
        self.stats_load_sync_wait
    }

    /// Go `tidb_opt_index_prune_threshold`'s `SetSession`.
    pub(crate) fn set_session_opt_index_prune_threshold(&mut self, value: &str) -> Result<(), VarError> {
        self.opt_index_prune_threshold = parse_opt_index_prune_threshold(value);
        Ok(())
    }

    /// Go `SessionVars.OptIndexPruneThreshold` (`tidb_opt_index_prune_threshold`).
    #[must_use]
    pub const fn opt_index_prune_threshold(&self) -> i32 {
        self.opt_index_prune_threshold
    }

    /// Go `tidb_opt_range_max_size`'s `SetSession`.
    pub(crate) fn set_session_range_max_size(&mut self, value: &str) -> Result<(), VarError> {
        self.range_max_size = parse_range_max_size(value);
        Ok(())
    }

    /// Go `tidb_opt_range_max_count`'s `SetSession`:
    /// `TidbOptInt64(val, DefTiDBOptRangeMaxCount)`.
    pub(crate) fn set_session_range_max_count(&mut self, value: &str) -> Result<(), VarError> {
        self.range_max_count =
            crate::varsutil::tidb_opt_int64(value, tidb_vardef::defaults::DEF_TIDB_OPT_RANGE_MAX_COUNT);
        Ok(())
    }

    /// Go `SessionVars.RangeMaxCount` (`tidb_opt_range_max_count`).
    #[must_use]
    pub const fn range_max_count(&self) -> i64 {
        self.range_max_count
    }

    /// Go `SessionVars.RangeMaxSize` (`tidb_opt_range_max_size`).
    #[must_use]
    pub const fn range_max_size(&self) -> i64 {
        self.range_max_size
    }

    /// Go `tidb_opt_prefix_index_single_scan`'s `SetSession`.
    pub(crate) fn set_session_opt_prefix_index_single_scan(&mut self, value: &str) -> Result<(), VarError> {
        self.opt_prefix_index_single_scan = parse_opt_prefix_index_single_scan(value);
        Ok(())
    }

    /// Go `SessionVars.OptPrefixIndexSingleScan` (`tidb_opt_prefix_index_single_scan`).
    #[must_use]
    pub const fn opt_prefix_index_single_scan(&self) -> bool {
        self.opt_prefix_index_single_scan
    }

    /// Go `tidb_opt_prefer_range_scan`'s `SetSession`.
    pub(crate) fn set_session_allow_prefer_range_scan(&mut self, value: &str) -> Result<(), VarError> {
        self.allow_prefer_range_scan = parse_allow_prefer_range_scan(value);
        Ok(())
    }

    /// Go `SessionVars.allowPreferRangeScan` (`tidb_opt_prefer_range_scan`).
    #[must_use]
    pub const fn allow_prefer_range_scan(&self) -> bool {
        self.allow_prefer_range_scan
    }

    /// Go `tidb_opt_use_invisible_indexes`'s `SetSession`.
    pub(crate) fn set_session_optimizer_use_invisible_indexes(&mut self, value: &str) -> Result<(), VarError> {
        self.optimizer_use_invisible_indexes = parse_optimizer_use_invisible_indexes(value);
        Ok(())
    }

    /// Go `SessionVars.OptimizerUseInvisibleIndexes` (`tidb_opt_use_invisible_indexes`).
    #[must_use]
    pub const fn optimizer_use_invisible_indexes(&self) -> bool {
        self.optimizer_use_invisible_indexes
    }

    /// Go `tidb_opt_always_keep_join_key`'s `SetSession`.
    pub(crate) fn set_session_always_keep_join_key(&mut self, value: &str) -> Result<(), VarError> {
        self.always_keep_join_key = parse_always_keep_join_key(value);
        Ok(())
    }

    /// Go `SessionVars.AlwaysKeepJoinKey` (`tidb_opt_always_keep_join_key`).
    #[must_use]
    pub const fn always_keep_join_key(&self) -> bool {
        self.always_keep_join_key
    }

    /// Go `tidb_opt_agg_push_down`'s `SetSession`.
    pub(crate) fn set_session_allow_agg_push_down(&mut self, value: &str) -> Result<(), VarError> {
        self.allow_agg_push_down = parse_allow_agg_push_down(value);
        Ok(())
    }

    /// Go `SessionVars.AllowAggPushDown` (`tidb_opt_agg_push_down`).
    #[must_use]
    pub const fn allow_agg_push_down(&self) -> bool {
        self.allow_agg_push_down
    }

    /// Go `tidb_opt_three_stage_distinct_agg`'s `SetSession`.
    pub(crate) fn set_session_enable_3_stage_distinct_agg(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_3_stage_distinct_agg = parse_enable_3_stage_distinct_agg(value);
        Ok(())
    }

    /// Go `SessionVars.Enable3StageDistinctAgg` (`tidb_opt_three_stage_distinct_agg`).
    #[must_use]
    pub const fn enable_3_stage_distinct_agg(&self) -> bool {
        self.enable_3_stage_distinct_agg
    }

    /// Go `tidb_opt_enable_three_stage_multi_distinct_agg`'s `SetSession`.
    pub(crate) fn set_session_enable_3_stage_multi_distinct_agg(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_3_stage_multi_distinct_agg = parse_enable_3_stage_multi_distinct_agg(value);
        Ok(())
    }

    /// Go `SessionVars.Enable3StageMultiDistinctAgg` (`tidb_opt_enable_three_stage_multi_distinct_agg`).
    #[must_use]
    pub const fn enable_3_stage_multi_distinct_agg(&self) -> bool {
        self.enable_3_stage_multi_distinct_agg
    }

    /// Go `tiflash_hashagg_preaggregation_mode`'s `SetSession`.
    pub(crate) fn set_session_tiflash_pre_agg_mode(&mut self, value: &str) -> Result<(), VarError> {
        self.tiflash_pre_agg_mode = parse_tiflash_pre_agg_mode(value);
        Ok(())
    }

    /// Go `SessionVars.TiFlashPreAggMode` (`tiflash_hashagg_preaggregation_mode`).
    #[must_use]
    pub fn tiflash_pre_agg_mode(&self) -> &str {
        &self.tiflash_pre_agg_mode
    }

    /// Go `tidb_opt_partial_ordered_index_for_topn`'s `SetSession`.
    pub(crate) fn set_session_opt_partial_ordered_index_for_topn(&mut self, value: &str) -> Result<(), VarError> {
        self.opt_partial_ordered_index_for_topn = parse_opt_partial_ordered_index_for_topn(value);
        Ok(())
    }

    /// Go `SessionVars.OptPartialOrderedIndexForTopN` (`tidb_opt_partial_ordered_index_for_topn`).
    #[must_use]
    pub fn opt_partial_ordered_index_for_topn(&self) -> &str {
        &self.opt_partial_ordered_index_for_topn
    }

    /// Go `tidb_enable_unsafe_substitute`'s `SetSession`.
    pub(crate) fn set_session_enable_unsafe_substitute(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_unsafe_substitute = parse_enable_unsafe_substitute(value);
        Ok(())
    }

    /// Go `SessionVars.EnableUnsafeSubstitute` (`tidb_enable_unsafe_substitute`).
    #[must_use]
    pub const fn enable_unsafe_substitute(&self) -> bool {
        self.enable_unsafe_substitute
    }

    /// Go `tidb_opt_force_inline_cte`'s `SetSession`.
    pub(crate) fn set_session_enable_force_inline_cte(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_force_inline_cte = parse_enable_force_inline_cte(value);
        Ok(())
    }

    /// Go `SessionVars.enableForceInlineCTE` (`tidb_opt_force_inline_cte`).
    #[must_use]
    pub const fn enable_force_inline_cte(&self) -> bool {
        self.enable_force_inline_cte
    }

    /// Go `tidb_opt_enable_mpp_shared_cte_execution`'s `SetSession`.
    pub(crate) fn set_session_enable_mpp_shared_cte_execution(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_mpp_shared_cte_execution = parse_enable_mpp_shared_cte_execution(value);
        Ok(())
    }

    /// Go `SessionVars.EnableMPPSharedCTEExecution` (`tidb_opt_enable_mpp_shared_cte_execution`).
    #[must_use]
    pub const fn enable_mpp_shared_cte_execution(&self) -> bool {
        self.enable_mpp_shared_cte_execution
    }

    /// Go `tidb_opt_advanced_join_hint`'s `SetSession`.
    pub(crate) fn set_session_enable_advanced_join_hint(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_advanced_join_hint = parse_enable_advanced_join_hint(value);
        Ok(())
    }

    /// Go `SessionVars.EnableAdvancedJoinHint` (`tidb_opt_advanced_join_hint`).
    #[must_use]
    pub const fn enable_advanced_join_hint(&self) -> bool {
        self.enable_advanced_join_hint
    }

    /// Go `tidb_opt_enable_semi_join_rewrite`'s `SetSession`.
    pub(crate) fn set_session_enable_semi_join_rewrite(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_semi_join_rewrite = parse_enable_semi_join_rewrite(value);
        Ok(())
    }

    /// Go `SessionVars.EnableSemiJoinRewrite` (`tidb_opt_enable_semi_join_rewrite`).
    #[must_use]
    pub const fn enable_semi_join_rewrite(&self) -> bool {
        self.enable_semi_join_rewrite
    }

    /// Go `tidb_opt_enable_alternative_logical_plans`'s `SetSession`.
    pub(crate) fn set_session_enable_alternative_logical_plans(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_alternative_logical_plans = parse_enable_alternative_logical_plans(value);
        Ok(())
    }

    /// Go `SessionVars.EnableAlternativeLogicalPlans` (`tidb_opt_enable_alternative_logical_plans`).
    #[must_use]
    pub const fn enable_alternative_logical_plans(&self) -> bool {
        self.enable_alternative_logical_plans
    }

    /// Go `tidb_opt_enable_non_eval_scalar_subquery`'s `SetSession`.
    pub(crate) fn set_session_explain_non_evaled_sub_query(&mut self, value: &str) -> Result<(), VarError> {
        self.explain_non_evaled_sub_query = parse_explain_non_evaled_sub_query(value);
        Ok(())
    }

    /// Go `SessionVars.ExplainNonEvaledSubQuery` (`tidb_opt_enable_non_eval_scalar_subquery`).
    #[must_use]
    pub const fn explain_non_evaled_sub_query(&self) -> bool {
        self.explain_non_evaled_sub_query
    }

    /// Go `tidb_opt_insubq_to_join_and_agg`'s `SetSession`.
    pub(crate) fn set_session_set_allow_in_subq_to_join_and_agg(&mut self, value: &str) -> Result<(), VarError> {
        self.set_allow_in_subq_to_join_and_agg = parse_set_allow_in_subq_to_join_and_agg(value);
        Ok(())
    }

    /// Go `SessionVars.allowInSubqToJoinAndAgg` (`tidb_opt_insubq_to_join_and_agg`).
    #[must_use]
    pub const fn set_allow_in_subq_to_join_and_agg(&self) -> bool {
        self.set_allow_in_subq_to_join_and_agg
    }

    /// Go `tidb_opt_enable_no_decorrelate_in_select`'s `SetSession`.
    pub(crate) fn set_session_enable_no_decorrelate_in_select(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_no_decorrelate_in_select = parse_enable_no_decorrelate_in_select(value);
        Ok(())
    }

    /// Go `SessionVars.EnableNoDecorrelateInSelect` (`tidb_opt_enable_no_decorrelate_in_select`).
    #[must_use]
    pub const fn enable_no_decorrelate_in_select(&self) -> bool {
        self.enable_no_decorrelate_in_select
    }

    /// Go `tidb_opt_skew_distinct_agg`'s `SetSession`.
    pub(crate) fn set_session_enable_skew_distinct_agg(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_skew_distinct_agg = parse_enable_skew_distinct_agg(value);
        Ok(())
    }

    /// Go `SessionVars.EnableSkewDistinctAgg` (`tidb_opt_skew_distinct_agg`).
    #[must_use]
    pub const fn enable_skew_distinct_agg(&self) -> bool {
        self.enable_skew_distinct_agg
    }

    /// Go `tidb_opt_enable_advanced_join_reorder`'s `SetSession`.
    pub(crate) fn set_session_enable_advanced_join_reorder(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_advanced_join_reorder = parse_enable_advanced_join_reorder(value);
        Ok(())
    }

    /// Go `SessionVars.EnableAdvancedJoinReorder` (`tidb_opt_enable_advanced_join_reorder`).
    #[must_use]
    pub const fn enable_advanced_join_reorder(&self) -> bool {
        self.enable_advanced_join_reorder
    }

    /// Go `tidb_constraint_check_in_place`'s `SetSession`.
    pub(crate) fn set_session_constraint_check_in_place(&mut self, value: &str) -> Result<(), VarError> {
        self.constraint_check_in_place = parse_constraint_check_in_place(value);
        Ok(())
    }

    /// Go `SessionVars.ConstraintCheckInPlace` (`tidb_constraint_check_in_place`).
    #[must_use]
    pub const fn constraint_check_in_place(&self) -> bool {
        self.constraint_check_in_place
    }

    /// Go `tidb_opt_ordering_index_selectivity_ratio`'s `SetSession`.
    pub(crate) fn set_session_opt_ordering_idx_sel_ratio(&mut self, value: &str) -> Result<(), VarError> {
        self.opt_ordering_idx_sel_ratio = parse_opt_ordering_idx_sel_ratio(value);
        Ok(())
    }

    /// Go `SessionVars.OptOrderingIdxSelRatio` (`tidb_opt_ordering_index_selectivity_ratio`).
    #[must_use]
    pub const fn opt_ordering_idx_sel_ratio(&self) -> f64 {
        self.opt_ordering_idx_sel_ratio
    }

    /// Go `tidb_opt_index_join_max_scan_rows_ratio`'s `SetSession`.
    pub(crate) fn set_session_index_join_max_scan_rows_ratio(&mut self, value: &str) -> Result<(), VarError> {
        self.index_join_max_scan_rows_ratio = parse_index_join_max_scan_rows_ratio(value);
        Ok(())
    }

    /// Go `SessionVars.IndexJoinMaxScanRowsRatio` (`tidb_opt_index_join_max_scan_rows_ratio`).
    #[must_use]
    pub const fn index_join_max_scan_rows_ratio(&self) -> f64 {
        self.index_join_max_scan_rows_ratio
    }

    /// Go `tidb_opt_ordering_index_selectivity_threshold`'s `SetSession`.
    pub(crate) fn set_session_opt_ordering_idx_sel_thresh(&mut self, value: &str) -> Result<(), VarError> {
        self.opt_ordering_idx_sel_thresh = parse_opt_ordering_idx_sel_thresh(value);
        Ok(())
    }

    /// Go `SessionVars.OptOrderingIdxSelThresh` (`tidb_opt_ordering_index_selectivity_threshold`).
    #[must_use]
    pub const fn opt_ordering_idx_sel_thresh(&self) -> f64 {
        self.opt_ordering_idx_sel_thresh
    }

    /// Go `tidb_opt_projection_push_down`'s `SetSession`.
    pub(crate) fn set_session_allow_projection_push_down(&mut self, value: &str) -> Result<(), VarError> {
        self.allow_projection_push_down = parse_allow_projection_push_down(value);
        Ok(())
    }

    /// Go `SessionVars.AllowProjectionPushDown` (`tidb_opt_projection_push_down`).
    #[must_use]
    pub const fn allow_projection_push_down(&self) -> bool {
        self.allow_projection_push_down
    }

    /// Go `tidb_opt_distinct_agg_push_down`'s `SetSession`.
    pub(crate) fn set_session_allow_distinct_agg_push_down(&mut self, value: &str) -> Result<(), VarError> {
        self.allow_distinct_agg_push_down = parse_allow_distinct_agg_push_down(value);
        Ok(())
    }

    /// Go `SessionVars.AllowDistinctAggPushDown` (`tidb_opt_distinct_agg_push_down`).
    #[must_use]
    pub const fn allow_distinct_agg_push_down(&self) -> bool {
        self.allow_distinct_agg_push_down
    }

    /// Go `tidb_enable_cascades_planner`'s `SetSession`.
    pub(crate) fn set_session_enable_cascades_planner(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_cascades_planner = parse_enable_cascades_planner(value);
        Ok(())
    }

    /// Go `SessionVars.enableCascadesPlanner` (`tidb_enable_cascades_planner`).
    #[must_use]
    pub const fn enable_cascades_planner(&self) -> bool {
        self.enable_cascades_planner
    }

    /// Go `tidb_opt_enable_hash_join`'s `SetSession`.
    pub(crate) fn set_session_enable_hash_join(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_hash_join = parse_enable_hash_join(value);
        Ok(())
    }

    /// Go `SessionVars.DisableHashJoin (negated)` (`tidb_opt_enable_hash_join`).
    #[must_use]
    pub const fn enable_hash_join(&self) -> bool {
        self.enable_hash_join
    }

    /// Go `tidb_enable_null_aware_anti_join`'s `SetSession`.
    pub(crate) fn set_session_optimizer_enable_naaj(&mut self, value: &str) -> Result<(), VarError> {
        self.optimizer_enable_naaj = parse_optimizer_enable_naaj(value);
        Ok(())
    }

    /// Go `SessionVars.OptimizerEnableNAAJ` (`tidb_enable_null_aware_anti_join`).
    #[must_use]
    pub const fn optimizer_enable_naaj(&self) -> bool {
        self.optimizer_enable_naaj
    }

    /// Go `tidb_opt_limit_push_down_threshold`'s `SetSession`.
    pub(crate) fn set_session_limit_push_down_threshold(&mut self, value: &str) -> Result<(), VarError> {
        self.limit_push_down_threshold = parse_limit_push_down_threshold(value);
        Ok(())
    }

    /// Go `SessionVars.LimitPushDownThreshold` (`tidb_opt_limit_push_down_threshold`).
    #[must_use]
    pub const fn limit_push_down_threshold(&self) -> u64 {
        self.limit_push_down_threshold
    }

    /// Go `tidb_read_staleness`'s `SetSession`.
    pub(crate) fn set_session_read_staleness(&mut self, value: &str) -> Result<(), VarError> {
        self.read_staleness = parse_read_staleness(value);
        Ok(())
    }

    /// Go `SessionVars.ReadStaleness` (`tidb_read_staleness`).
    #[must_use]
    pub const fn read_staleness(&self) -> i64 {
        self.read_staleness
    }

    /// Go `tidb_index_lookup_pushdown_policy`'s `SetSession`.
    pub(crate) fn set_session_index_lookup_push_down_policy(&mut self, value: &str) -> Result<(), VarError> {
        self.index_lookup_push_down_policy = parse_index_lookup_push_down_policy(value);
        Ok(())
    }

    /// Go `SessionVars.IndexLookUpPushDownPolicy` (`tidb_index_lookup_pushdown_policy`).
    #[must_use]
    pub fn index_lookup_push_down_policy(&self) -> &str {
        &self.index_lookup_push_down_policy
    }

    /// Go `tidb_opt_join_reorder_through_proj`'s `SetSession`.
    pub(crate) fn set_session_tidb_opt_join_reorder_through_proj(&mut self, value: &str) -> Result<(), VarError> {
        self.tidb_opt_join_reorder_through_proj = parse_tidb_opt_join_reorder_through_proj(value);
        Ok(())
    }

    /// Go `SessionVars.TiDBOptJoinReorderThroughProj` (`tidb_opt_join_reorder_through_proj`).
    #[must_use]
    pub const fn tidb_opt_join_reorder_through_proj(&self) -> bool {
        self.tidb_opt_join_reorder_through_proj
    }

    /// Go `tidb_opt_join_reorder_through_sel`'s `SetSession`.
    pub(crate) fn set_session_tidb_opt_join_reorder_through_sel(&mut self, value: &str) -> Result<(), VarError> {
        self.tidb_opt_join_reorder_through_sel = parse_tidb_opt_join_reorder_through_sel(value);
        Ok(())
    }

    /// Go `SessionVars.TiDBOptJoinReorderThroughSel` (`tidb_opt_join_reorder_through_sel`).
    #[must_use]
    pub const fn tidb_opt_join_reorder_through_sel(&self) -> bool {
        self.tidb_opt_join_reorder_through_sel
    }

    /// Go `tidb_enable_outer_join_reorder`'s `SetSession`.
    pub(crate) fn set_session_enable_outer_join_reorder(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_outer_join_reorder = parse_enable_outer_join_reorder(value);
        Ok(())
    }

    /// Go `SessionVars.EnableOuterJoinReorder` (`tidb_enable_outer_join_reorder`).
    #[must_use]
    pub const fn enable_outer_join_reorder(&self) -> bool {
        self.enable_outer_join_reorder
    }

    /// Go `tidb_enable_index_merge`'s `SetSession`.
    pub(crate) fn set_session_enable_index_merge(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_index_merge = parse_enable_index_merge(value);
        Ok(())
    }

    /// Go `SessionVars.enableIndexMerge` (`tidb_enable_index_merge`).
    #[must_use]
    pub const fn enable_index_merge(&self) -> bool {
        self.enable_index_merge
    }

    /// Go `tidb_skip_missing_partition_stats`'s `SetSession`.
    pub(crate) fn set_session_skip_missing_partition_stats(&mut self, value: &str) -> Result<(), VarError> {
        self.skip_missing_partition_stats = parse_skip_missing_partition_stats(value);
        Ok(())
    }

    /// Go `SessionVars.SkipMissingPartitionStats` (`tidb_skip_missing_partition_stats`).
    #[must_use]
    pub const fn skip_missing_partition_stats(&self) -> bool {
        self.skip_missing_partition_stats
    }

    /// Go `tidb_enable_new_only_full_group_by_check`'s `SetSession`.
    pub(crate) fn set_session_optimizer_enable_new_only_full_group_by_check(&mut self, value: &str) -> Result<(), VarError> {
        self.optimizer_enable_new_only_full_group_by_check = parse_optimizer_enable_new_only_full_group_by_check(value);
        Ok(())
    }

    /// Go `SessionVars.OptimizerEnableNewOnlyFullGroupByCheck` (`tidb_enable_new_only_full_group_by_check`).
    #[must_use]
    pub const fn optimizer_enable_new_only_full_group_by_check(&self) -> bool {
        self.optimizer_enable_new_only_full_group_by_check
    }

    /// Go `tidb_enable_auto_increment_in_generated`'s `SetSession`.
    pub(crate) fn set_session_enable_auto_increment_in_generated(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_auto_increment_in_generated = parse_enable_auto_increment_in_generated(value);
        Ok(())
    }

    /// Go `SessionVars.EnableAutoIncrementInGenerated` (`tidb_enable_auto_increment_in_generated`).
    #[must_use]
    pub const fn enable_auto_increment_in_generated(&self) -> bool {
        self.enable_auto_increment_in_generated
    }

    /// Go `sql_require_primary_key`'s `SetSession`.
    pub(crate) fn set_session_primary_key_required(&mut self, value: &str) -> Result<(), VarError> {
        self.primary_key_required = parse_primary_key_required(value);
        Ok(())
    }

    /// Go `SessionVars.PrimaryKeyRequired` (`sql_require_primary_key`).
    #[must_use]
    pub const fn primary_key_required(&self) -> bool {
        self.primary_key_required
    }

    /// Go `tidb_tmp_table_max_size`'s `SetSession`.
    pub(crate) fn set_session_tmp_table_size(&mut self, value: &str) -> Result<(), VarError> {
        self.tmp_table_size = parse_tmp_table_size(value);
        Ok(())
    }

    /// Go `SessionVars.TMPTableSize` (`tidb_tmp_table_max_size`).
    #[must_use]
    pub const fn tmp_table_size(&self) -> i64 {
        self.tmp_table_size
    }

    /// Go `tidb_remove_orderby_in_subquery`'s `SetSession`.
    pub(crate) fn set_session_remove_orderby_in_subquery(&mut self, value: &str) -> Result<(), VarError> {
        self.remove_orderby_in_subquery = parse_remove_orderby_in_subquery(value);
        Ok(())
    }

    /// Go `SessionVars.RemoveOrderbyInSubquery` (`tidb_remove_orderby_in_subquery`).
    #[must_use]
    pub const fn remove_orderby_in_subquery(&self) -> bool {
        self.remove_orderby_in_subquery
    }

    /// Go `tidb_analyze_version`'s `SetSession`.
    pub(crate) fn set_session_analyze_version(&mut self, value: &str) -> Result<(), VarError> {
        self.analyze_version = parse_analyze_version(value);
        Ok(())
    }

    /// Go `SessionVars.AnalyzeVersion` (`tidb_analyze_version`).
    #[must_use]
    pub const fn analyze_version(&self) -> i64 {
        self.analyze_version
    }

    /// Go `tidb_enable_rate_limit_action`'s `SetSession`.
    pub(crate) fn set_session_enabled_rate_limit_action(&mut self, value: &str) -> Result<(), VarError> {
        self.enabled_rate_limit_action = parse_enabled_rate_limit_action(value);
        Ok(())
    }

    /// Go `SessionVars.EnabledRateLimitAction` (`tidb_enable_rate_limit_action`).
    #[must_use]
    pub const fn enabled_rate_limit_action(&self) -> bool {
        self.enabled_rate_limit_action
    }

    /// Go `innodb_lock_wait_timeout`'s `SetSession`.
    pub(crate) fn set_session_lock_wait_timeout(&mut self, value: &str) -> Result<(), VarError> {
        self.lock_wait_timeout = parse_lock_wait_timeout(value);
        Ok(())
    }

    /// Go `SessionVars.LockWaitTimeout` (`innodb_lock_wait_timeout`).
    #[must_use]
    pub const fn lock_wait_timeout(&self) -> u64 {
        self.lock_wait_timeout
    }

    /// Go `tidb_adaptive_closest_read_threshold`'s `SetSession`.
    pub(crate) fn set_session_replica_closest_read_threshold(&mut self, value: &str) -> Result<(), VarError> {
        self.replica_closest_read_threshold = parse_replica_closest_read_threshold(value);
        Ok(())
    }

    /// Go `SessionVars.ReplicaClosestReadThreshold` (`tidb_adaptive_closest_read_threshold`).
    #[must_use]
    pub const fn replica_closest_read_threshold(&self) -> i64 {
        self.replica_closest_read_threshold
    }

    /// Go `tidb_load_based_replica_read_threshold`'s `SetSession`.
    pub(crate) fn set_session_load_based_replica_read_threshold(&mut self, value: &str) -> Result<(), VarError> {
        self.load_based_replica_read_threshold = parse_load_based_replica_read_threshold(value);
        Ok(())
    }

    /// Go `SessionVars.LoadBasedReplicaReadThreshold` (`tidb_load_based_replica_read_threshold`).
    #[must_use]
    pub const fn load_based_replica_read_threshold(&self) -> i64 {
        self.load_based_replica_read_threshold
    }

    /// Go `tikv_client_read_timeout`'s `SetSession`.
    pub(crate) fn set_session_tikv_client_read_timeout(&mut self, value: &str) -> Result<(), VarError> {
        self.tikv_client_read_timeout = parse_tikv_client_read_timeout(value);
        Ok(())
    }

    /// Go `SessionVars.TiKVClientReadTimeout` (`tikv_client_read_timeout`).
    #[must_use]
    pub const fn tikv_client_read_timeout(&self) -> u64 {
        self.tikv_client_read_timeout
    }

    /// Go `tidb_isolation_read_engines`'s `SetSession`.
    pub(crate) fn set_session_isolation_read_engines(&mut self, value: &str) -> Result<(), VarError> {
        self.isolation_read_engines = parse_isolation_read_engines(value);
        Ok(())
    }

    /// Go `SessionVars.IsolationReadEngines` (`tidb_isolation_read_engines`).
    #[must_use]
    pub fn isolation_read_engines(&self) -> &str {
        &self.isolation_read_engines
    }

    /// Go `tidb_init_chunk_size`'s `SetSession`.
    pub(crate) fn set_session_init_chunk_size(&mut self, value: &str) -> Result<(), VarError> {
        self.init_chunk_size = parse_init_chunk_size(value);
        Ok(())
    }

    /// Go `SessionVars.InitChunkSize` (`tidb_init_chunk_size`).
    #[must_use]
    pub const fn init_chunk_size(&self) -> usize {
        self.init_chunk_size
    }

    /// Go `tidb_max_chunk_size`'s `SetSession`.
    pub(crate) fn set_session_max_chunk_size(&mut self, value: &str) -> Result<(), VarError> {
        self.max_chunk_size = parse_max_chunk_size(value);
        Ok(())
    }

    /// Go `SessionVars.MaxChunkSize` (`tidb_max_chunk_size`).
    #[must_use]
    pub const fn max_chunk_size(&self) -> usize {
        self.max_chunk_size
    }

    /// Go `tidb_metric_query_step`'s `SetSession`.
    pub(crate) fn set_session_metric_schema_step(&mut self, value: &str) -> Result<(), VarError> {
        self.metric_schema_step = parse_metric_schema_step(value);
        Ok(())
    }

    /// Go `SessionVars.MetricSchemaStep` (`tidb_metric_query_step`).
    #[must_use]
    pub const fn metric_schema_step(&self) -> i64 {
        self.metric_schema_step
    }

    /// Go `tidb_metric_query_range_duration`'s `SetSession`.
    pub(crate) fn set_session_metric_schema_range_duration(&mut self, value: &str) -> Result<(), VarError> {
        self.metric_schema_range_duration = parse_metric_schema_range_duration(value);
        Ok(())
    }

    /// Go `SessionVars.MetricSchemaRangeDuration` (`tidb_metric_query_range_duration`).
    #[must_use]
    pub const fn metric_schema_range_duration(&self) -> i64 {
        self.metric_schema_range_duration
    }

    /// Go `tidb_enable_parallel_apply`'s `SetSession`.
    pub(crate) fn set_session_enable_parallel_apply(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_parallel_apply = parse_enable_parallel_apply(value);
        Ok(())
    }

    /// Go `SessionVars.EnableParallelApply` (`tidb_enable_parallel_apply`).
    #[must_use]
    pub const fn enable_parallel_apply(&self) -> bool {
        self.enable_parallel_apply
    }

    /// Go `tidb_opt_derive_topn`'s `SetSession`.
    pub(crate) fn set_session_allow_derive_topn(&mut self, value: &str) -> Result<(), VarError> {
        self.allow_derive_topn = parse_allow_derive_topn(value);
        Ok(())
    }

    /// Go `SessionVars.AllowDeriveTopN` (`tidb_opt_derive_topn`).
    #[must_use]
    pub const fn allow_derive_topn(&self) -> bool {
        self.allow_derive_topn
    }

    /// Go `tidb_mem_quota_apply_cache`'s `SetSession`.
    pub(crate) fn set_session_mem_quota_apply_cache(&mut self, value: &str) -> Result<(), VarError> {
        self.mem_quota_apply_cache = parse_mem_quota_apply_cache(value);
        Ok(())
    }

    /// Go `SessionVars.MemQuotaApplyCache` (`tidb_mem_quota_apply_cache`).
    #[must_use]
    pub const fn mem_quota_apply_cache(&self) -> i64 {
        self.mem_quota_apply_cache
    }

    /// Go `setPipelinedDmlResourcePolicy`, `tidb_pipelined_dml_resource_policy`'s
    /// `SetSession`: a refused value leaves the config as it was.
    pub(crate) fn set_session_pipelined_dml_resource_policy(&mut self, value: &str) -> Result<(), VarError> {
        let value = value.trim();
        self.pipelined_dml_config = parse_pipelined_dml_resource_policy(value).ok_or_else(|| {
            tidb_terror::MysqlError::from_terror(&crate::variable_errors::ERR_WRONG_VALUE_FOR_VAR.gen_with_stack_by_args(&[
                tidb_vardef::tidb_vars::TIDB_PIPELINED_DML_RESOURCE_POLICY.into(),
                value.to_owned().into(),
            ]))
        })?;
        Ok(())
    }

    /// Go `SessionVars.PipelinedDMLConfig`.
    #[must_use]
    pub const fn pipelined_dml_config(&self) -> crate::session_vars::PipelinedDmlConfig {
        self.pipelined_dml_config
    }

    /// Go `tidb_executor_concurrency`'s `SetSession`.
    pub(crate) fn set_session_executor_concurrency(&mut self, value: &str) -> Result<(), VarError> {
        self.executor_concurrency = parse_executor_concurrency(value);
        Ok(())
    }

    /// Go `SessionVars.ExecutorConcurrency` (`tidb_executor_concurrency`).
    #[must_use]
    pub const fn executor_concurrency(&self) -> usize {
        self.executor_concurrency
    }

    /// Go `tidb_hashagg_partial_concurrency`'s `SetSession`.
    pub(crate) fn set_session_hash_agg_partial_concurrency(&mut self, value: &str) -> Result<(), VarError> {
        self.hash_agg_partial_concurrency = parse_hash_agg_partial_concurrency(value);
        Ok(())
    }

    /// Go `SessionVars.hashAggPartialConcurrency` (`tidb_hashagg_partial_concurrency`).
    #[must_use]
    pub const fn hash_agg_partial_concurrency(&self) -> i64 {
        self.hash_agg_partial_concurrency
    }

    /// Go `tidb_hashagg_final_concurrency`'s `SetSession`.
    pub(crate) fn set_session_hash_agg_final_concurrency(&mut self, value: &str) -> Result<(), VarError> {
        self.hash_agg_final_concurrency = parse_hash_agg_final_concurrency(value);
        Ok(())
    }

    /// Go `SessionVars.hashAggFinalConcurrency` (`tidb_hashagg_final_concurrency`).
    #[must_use]
    pub const fn hash_agg_final_concurrency(&self) -> i64 {
        self.hash_agg_final_concurrency
    }

    /// Go `tidb_ddl_reorg_priority`'s `SetSession`.
    pub(crate) fn set_session_ddl_reorg_priority(&mut self, value: &str) -> Result<(), VarError> {
        self.ddl_reorg_priority = parse_ddl_reorg_priority(value);
        Ok(())
    }

    /// Go `SessionVars.DDLReorgPriority` (`tidb_ddl_reorg_priority`).
    #[must_use]
    pub const fn ddl_reorg_priority(&self) -> i32 {
        self.ddl_reorg_priority
    }

    /// Go `tidb_mem_arbitrator_wait_averse`'s `SetSession`.
    pub(crate) fn set_session_mem_arbitrator_wait_averse(&mut self, value: &str) -> Result<(), VarError> {
        self.mem_arbitrator_wait_averse = parse_mem_arbitrator_wait_averse(value);
        Ok(())
    }

    /// Go `SessionVars.MemArbitrator.WaitAverse` (`tidb_mem_arbitrator_wait_averse`).
    #[must_use]
    pub const fn mem_arbitrator_wait_averse(&self) -> Option<bool> {
        self.mem_arbitrator_wait_averse
    }

    /// Go `tidb_mem_arbitrator_query_reserved`'s `SetSession`.
    pub(crate) fn set_session_mem_arbitrator_query_reserved(&mut self, value: &str) -> Result<(), VarError> {
        self.mem_arbitrator_query_reserved = parse_mem_arbitrator_query_reserved(value);
        Ok(())
    }

    /// Go `SessionVars.MemArbitrator.QueryReserved` (`tidb_mem_arbitrator_query_reserved`).
    #[must_use]
    pub const fn mem_arbitrator_query_reserved(&self) -> i64 {
        self.mem_arbitrator_query_reserved
    }

}

#[cfg(test)]
mod tests {
    use crate::vars::SessionVars;

    /// Go `SessionVars.SetSystemVar` runs the variable's `SetSession`, which
    /// keeps the typed field in step; `NewSessionVars` starts it from the
    /// variable's default; a statement-scoped write's restore runs the hook
    /// again with the restored value.
    #[test]
    fn typed_fields_follow_set_session_and_restore() {
        let mut vars = SessionVars::default();
        assert!(!vars.allow_agg_push_down());
        assert_eq!(vars.cte_max_recursion_depth(), 1000);
        assert_eq!(vars.tiflash_pre_agg_mode(), "force_preagg");
        assert_eq!(vars.mem_arbitrator_wait_averse(), Some(false));
        assert_eq!(vars.lock_wait_timeout(), 50_000);

        vars.set_system("tidb_opt_agg_push_down", "ON".to_owned()).unwrap();
        vars.set_system("cte_max_recursion_depth", "7".to_owned()).unwrap();
        vars.set_system("tiflash_hashagg_preaggregation_mode", "auto".to_owned()).unwrap();
        vars.set_system("tidb_mem_arbitrator_wait_averse", "nolimit".to_owned()).unwrap();
        vars.set_system("innodb_lock_wait_timeout", "3".to_owned()).unwrap();
        assert!(vars.allow_agg_push_down());
        assert_eq!(vars.cte_max_recursion_depth(), 7);
        assert_eq!(vars.tiflash_pre_agg_mode(), "auto");
        assert_eq!(vars.mem_arbitrator_wait_averse(), None);
        assert_eq!(vars.lock_wait_timeout(), 3_000);

        let saved = vars.snapshot_system("cte_max_recursion_depth");
        vars.set_system("cte_max_recursion_depth", "9".to_owned()).unwrap();
        assert_eq!(vars.cte_max_recursion_depth(), 9);
        vars.restore_system(saved);
        assert_eq!(vars.cte_max_recursion_depth(), 7);
    }

    /// Go `tests/realtikvtest/pipelineddmltest.TestVariable`'s
    /// `tidb_pipelined_dml_resource_policy` half: the policy's `SetSession`
    /// fills `PipelinedDMLConfig`, and a refused value leaves it as it was.
    #[test]
    fn test_variable() {
        let mut vars = SessionVars::default();
        let config = |vars: &SessionVars| {
            let config = vars.pipelined_dml_config();
            (
                config.pipelined_flush_concurrency,
                config.pipelined_resolve_lock_concurrency,
                config.pipelined_write_throttle_ratio,
            )
        };
        let mut set = |vars: &mut SessionVars, value: &str| {
            vars.set_system("tidb_pipelined_dml_resource_policy", value.to_owned())
        };
        assert!(!vars.bulk_dml_enabled());
        vars.set_system("tidb_dml_type", "bulk".to_owned()).unwrap();
        assert!(vars.bulk_dml_enabled());
        vars.set_system("tidb_dml_type", "standard".to_owned()).unwrap();
        assert!(!vars.bulk_dml_enabled());
        // not supported yet.
        assert!(vars.set_system("tidb_dml_type", "bulk(10)".to_owned()).is_err());

        // Basic policy tests
        assert_eq!(config(&vars), (128, 8, 0.0));
        set(&mut vars, "STANDARD").unwrap();
        assert_eq!(config(&vars), (128, 8, 0.0));
        set(&mut vars, "conserVative").unwrap();
        assert_eq!(config(&vars), (2, 2, 0.0));
        set(&mut vars, "Standard").unwrap();
        assert_eq!(config(&vars), (128, 8, 0.0));

        // Test custom configuration with valid values
        set(&mut vars, "custom{concurrency=64}").unwrap();
        assert_eq!((config(&vars).0, config(&vars).1), (64, 8));
        set(&mut vars, "custom{write_throttle_ratio=0.5}").unwrap();
        assert_eq!(config(&vars).2, 0.5);
        // Test multiple parameters
        set(&mut vars, "custom{concurrency=32,write_throttle_ratio=0.3}").unwrap();
        assert_eq!((config(&vars).0, config(&vars).2), (32, 0.3));
        // Test different separators
        set(&mut vars, "custom{concurrency:64,write_throttle_ratio:0.4}").unwrap();
        assert_eq!((config(&vars).0, config(&vars).2), (64, 0.4));
        // Test whitespace handling
        set(&mut vars, "  custom  {  concurrency  =  64  ,  write_throttle_ratio  =  0.4  }  ").unwrap();
        assert_eq!((config(&vars).0, config(&vars).2), (64, 0.4));
        // Test case insensitivity
        set(&mut vars, "CUSTOM{CONCURRENCY=64,WRITE_THROTTLE_RATIO=0.4}").unwrap();
        assert_eq!((config(&vars).0, config(&vars).2), (64, 0.4));
        // Test boundary values
        set(&mut vars, "custom{concurrency=1}").unwrap();
        assert_eq!(config(&vars).0, 1);
        set(&mut vars, "custom{write_throttle_ratio=0.999}").unwrap();
        assert_eq!(config(&vars).2, 0.999);

        // Test error cases and state consistency
        let orig = config(&vars);
        for bad in [
            "custom",
            "custom{}",
            "custom{unknown=1}",
            "custom{concurrency=8193}",
            "custom{write_throttle_ratio=1.1}",
            "custom{concurrency=abc}",
        ] {
            assert!(set(&mut vars, bad).is_err(), "{bad}");
            assert_eq!(config(&vars), orig, "{bad}");
        }
    }
}
