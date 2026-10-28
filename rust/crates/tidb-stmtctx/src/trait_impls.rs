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

//! The planner and DDL interfaces a statement context implements (Go's
//! `sessionctx.Context` is what `pkg/planner` and `pkg/ddl` read).

use crate::StmtContext;

pub fn new_metabuild_context_with_sctx(ctx: &StmtContext) -> tidb_expr::metabuild::Context {
    use tidb_expr::metabuild::{
        with_clustered_index_def_mode, with_enable_auto_increment_in_generated, with_expr_ctx,
        with_info_schema, with_pre_split_regions, with_primary_key_required,
        with_shard_row_id_bits, Context, MetaOnlyInfoSchema,
    };
    Context::new([
        with_expr_ctx(std::sync::Arc::new(ctx.clone())),
        with_enable_auto_increment_in_generated(ctx.auto_increment_in_generated()),
        with_primary_key_required(ctx.primary_key_required()),
        with_clustered_index_def_mode(ctx.clustered_index_def_mode()),
        with_shard_row_id_bits(ctx.shard_row_id_bits()),
        with_pre_split_regions(ctx.pre_split_regions()),
        with_info_schema(
            ctx.latest_index_schema()
                .map(|schema| schema as std::sync::Arc<dyn MetaOnlyInfoSchema>),
        ),
    ])
}

/// Go's `sessionctx.Context` as `pkg/ddl` reads it: a statement's context.
impl tidb_ddl_build::session_context::DdlSessionContext for StmtContext {
    fn metabuild_context(&self) -> tidb_expr::metabuild::Context {
        new_metabuild_context_with_sctx(self)
    }

    fn enable_check_constraint(&self) -> bool {
        StmtContext::enable_check_constraint(self)
    }

    fn foreign_key_checks(&self) -> bool {
        StmtContext::foreign_key_checks(self)
    }

    fn allow_remove_auto_inc(&self) -> bool {
        StmtContext::allow_remove_auto_inc(self)
    }

    fn auto_increment_in_generated(&self) -> bool {
        StmtContext::auto_increment_in_generated(self)
    }

    fn ddl_query(&self) -> &str {
        StmtContext::ddl_query(self)
    }

    fn ddl_sql_mode(&self) -> i64 {
        StmtContext::ddl_sql_mode(self)
    }

    fn sql_mode(&self) -> tidb_parser::SqlMode {
        StmtContext::sql_mode(self)
    }

    fn ddl_query_rewrite(&self) -> Option<(String, String)> {
        StmtContext::ddl_query_rewrite(self)
    }

    fn rewrite_ddl_job_query(&self, original: String, replacement: String) {
        StmtContext::rewrite_ddl_job_query(self, original, replacement);
    }

    fn ddl_cdc_write_source(&self) -> u64 {
        StmtContext::ddl_cdc_write_source(self)
    }

    fn ddl_reorg_vars(&self) -> &tidb_ddl_build::session_context::DdlReorgVars {
        StmtContext::ddl_reorg_vars(self)
    }

    fn ddl_trace_id(&self) -> &[u8] {
        StmtContext::ddl_trace_id(self)
    }

    fn ddl_session_alias(&self) -> &str {
        StmtContext::ddl_session_alias(self)
    }

    fn ddl_reorg_priority(&self) -> i64 {
        StmtContext::ddl_reorg_priority(self)
    }

    fn ddl_connection_id(&self) -> u64 {
        StmtContext::ddl_connection_id(self)
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        StmtContext::connection_charset_info(self)
    }

    fn session_zone(&self) -> tidb_datatype::SessionTimeZone {
        StmtContext::session_zone(self)
    }

    fn like_default_escape(&self) -> u8 {
        StmtContext::like_default_escape(self)
    }

    fn default_collation_for_utf8mb4(&self) -> &str {
        StmtContext::default_collation_for_utf8mb4(self)
    }

    fn append_warning_parts(&self, code: u16, message: &str) {
        StmtContext::append_warning_parts(self, code, message);
    }

    fn append_error_parts(&self, code: u16, message: &str) {
        StmtContext::append_error_parts(self, code, message);
    }

    fn append_note_parts(&self, code: u16, message: &str) {
        StmtContext::append_note_parts(self, code, message);
    }

    fn set_hypo_tiflash_replica(&self, schema: &str, table: &str, add: bool) {
        StmtContext::set_hypo_tiflash_replica(self, schema, table, add);
    }

    fn add_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: tidb_model::index::IndexInfo,
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        StmtContext::add_hypo_index(self, schema, table, index)
    }

    fn drop_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: &str,
        if_exists: bool,
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        StmtContext::drop_hypo_index(self, schema, table, index, if_exists)
    }

    fn append_sql_warn(&self, warning: tidb_util::context::SqlWarn) {
        StmtContext::append_sql_warn(self, warning);
    }

    fn take_warnings(&self) -> Vec<tidb_util::context::SqlWarn> {
        StmtContext::take_warnings(self)
    }

    fn with_isolated_warnings(&self) -> Box<dyn tidb_ddl_build::session_context::DdlSessionContext> {
        Box::new(StmtContext::with_isolated_warnings(self))
    }
}

impl tidb_planner::logical::rule::PlanCacheMarker for StmtContext {
    fn set_skip_plan_cache(&self, reason: &str) {
        StmtContext::set_skip_plan_cache(self, reason);
    }
}

impl tidb_planner::logical::rule::HintWarningSink for StmtContext {
    fn set_hint_warning(&self, message: &str) {
        self.append_warning_parts(1815, message);
    }

    fn set_hint_warning_from_error(&self, code: u16, message: &str) {
        self.append_warning_parts(code, message);
    }
}

impl tidb_planner::find_best_task::dispatch::MppWarningSink for StmtContext {
    fn raise_mpp_warning(&self, message: &str) {
        StmtContext::append_mpp_warning(self, message);
    }

    fn set_hint_warning(&self, message: &str) {
        self.append_warning_parts(1815, message);
    }

    fn append_planner_note(&self, message: &str) {
        StmtContext::append_planner_note(self, message);
    }
}

/// Go `base.PlanContext` as `pkg/planner/core`'s optimizer reads it.
impl tidb_planner::optimize::OptimizeContext for StmtContext {
    fn session_zone(&self) -> tidb_expr::SessionTimeZone {
        StmtContext::session_zone(self)
    }
    fn static_partition_prune(&self) -> bool {
        StmtContext::static_partition_prune(self)
    }
    fn stable_result_mode(&self) -> bool {
        StmtContext::stable_result_mode(self)
    }
    fn optimizer_cost_env(&self) -> &tidb_planner::find_best_task::coster::CostEnv {
        StmtContext::optimizer_cost_env(self)
    }
    fn allow_derive_topn(&self) -> bool {
        StmtContext::allow_derive_topn(self)
    }
    fn expr_pushdown_blacklist(&self) -> &tidb_expr::infer_pushdown::ExprPushDownBlacklist {
        StmtContext::expr_pushdown_blacklist(self)
    }
    fn logical_rule_disabled(&self, name: &str) -> bool {
        StmtContext::logical_rule_disabled(self, name)
    }
    fn opt_index_prune_threshold(&self) -> i32 {
        StmtContext::opt_index_prune_threshold(self)
    }
    fn opt_prefix_index_single_scan(&self) -> bool {
        StmtContext::opt_prefix_index_single_scan(self)
    }
    fn index_merge(&self) -> bool {
        StmtContext::index_merge(self)
    }
    fn no_index_merge_hint(&self) -> bool {
        StmtContext::no_index_merge_hint(self)
    }
    fn in_explain_stmt(&self) -> bool {
        StmtContext::in_explain_stmt(self)
    }
    fn range_max_size(&self) -> i64 {
        StmtContext::range_max_size(self)
    }
    fn selectivity_factor(&self) -> f64 {
        StmtContext::selectivity_factor(self)
    }
    fn range_fallback_handler(&self) -> &tidb_util::context::RangeFallbackHandler {
        StmtContext::range_fallback_handler(self)
    }
    fn always_keep_join_key(&self) -> bool {
        StmtContext::always_keep_join_key(self)
    }
    fn enable_unsafe_substitute(&self) -> bool {
        StmtContext::enable_unsafe_substitute(self)
    }
    fn enable_semi_join_rewrite(&self) -> bool {
        StmtContext::enable_semi_join_rewrite(self)
    }
    fn enable_null_aware_anti_join(&self) -> bool {
        StmtContext::enable_null_aware_anti_join(self)
    }
    fn enable_no_decorrelate_in_select(&self) -> bool {
        StmtContext::enable_no_decorrelate_in_select(self)
    }
    fn optimizer_fix_control(&self) -> &tidb_planner::fix_control::OptimizerFixControl {
        StmtContext::optimizer_fix_control(self)
    }
    fn join_reorder_threshold(&self) -> i32 {
        StmtContext::join_reorder_threshold(self)
    }
    fn allow_agg_push_down(&self) -> bool {
        StmtContext::allow_agg_push_down(self)
    }
    fn advanced_join_reorder(&self) -> bool {
        StmtContext::advanced_join_reorder(self)
    }
    fn cartesian_join_order_threshold(&self) -> f64 {
        StmtContext::cartesian_join_order_threshold(self)
    }
    fn join_reorder_through_proj(&self) -> bool {
        StmtContext::join_reorder_through_proj(self)
    }
    fn join_reorder_through_sel(&self) -> bool {
        StmtContext::join_reorder_through_sel(self)
    }
    fn outer_join_reorder(&self) -> bool {
        StmtContext::outer_join_reorder(self)
    }
    fn advanced_join_hint(&self) -> bool {
        StmtContext::advanced_join_hint(self)
    }
    fn get_index_force(&self) -> bool {
        StmtContext::get_index_force(self)
    }
    fn enable_skew_distinct_agg(&self) -> bool {
        StmtContext::enable_skew_distinct_agg(self)
    }
    fn enable_3_stage_distinct_agg(&self) -> bool {
        StmtContext::enable_3_stage_distinct_agg(self)
    }
    fn enable_3_stage_multi_distinct_agg(&self) -> bool {
        StmtContext::enable_3_stage_multi_distinct_agg(self)
    }
    fn tiflash_pre_agg_mode(&self) -> &str {
        StmtContext::tiflash_pre_agg_mode(self)
    }
    fn partial_ordered_index_for_topn(&self) -> bool {
        StmtContext::partial_ordered_index_for_topn(self)
    }
    fn ordering_index_selectivity_ratio(&self) -> f64 {
        StmtContext::ordering_index_selectivity_ratio(self)
    }
    fn ordering_index_selectivity_threshold(&self) -> f64 {
        StmtContext::ordering_index_selectivity_threshold(self)
    }
    fn allow_projection_push_down(&self) -> bool {
        StmtContext::allow_projection_push_down(self)
    }
    fn enable_inl_join_inner_multi_pattern(&self) -> bool {
        StmtContext::enable_inl_join_inner_multi_pattern(self)
    }
    fn limit_push_down_threshold(&self) -> u64 {
        StmtContext::limit_push_down_threshold(self)
    }
    fn enable_hash_join(&self) -> bool {
        StmtContext::enable_hash_join(self)
    }
    fn apply_cache_capacity(&self) -> i64 {
        StmtContext::apply_cache_capacity(self)
    }
    fn enable_parallel_apply(&self) -> bool {
        StmtContext::enable_parallel_apply(self)
    }
    fn executor_concurrency(&self) -> usize {
        StmtContext::executor_concurrency(self)
    }
    fn in_transaction(&self) -> bool {
        self.pessimistic_transaction() || self.optimistic_transaction()
    }
    fn is_chunk_alloc_valid(&self) -> bool {
        StmtContext::is_chunk_alloc_valid(self)
    }
    fn set_chunk_alloc_valid(&self, valid: bool) {
        StmtContext::set_chunk_alloc_valid(self, valid);
    }
    fn max_chunk_size(&self) -> usize {
        self.executor_chunk_sizes().1
    }
    fn append_task_warning(&self, note: bool, code: u16, message: &str) {
        let level = if note {
            tidb_distsql::WarningLevel::Note
        } else {
            tidb_distsql::WarningLevel::Warning
        };
        self.append_leveled(level, code, message);
    }
    fn set_logical_opt(&self, duration: std::time::Duration) {
        self.phase_durations().set_logical_opt(duration);
    }
    fn finish_stats_derive(&self, begin: std::time::Instant) {
        self.phase_durations().finish_stats_derive(begin);
    }
    fn take_begin_physical_opt(&self) -> Option<std::time::Instant> {
        self.phase_durations().take_begin_physical_opt()
    }
    fn set_physical_opt(&self, duration: std::time::Duration) {
        self.phase_durations().set_physical_opt(duration);
    }
}
