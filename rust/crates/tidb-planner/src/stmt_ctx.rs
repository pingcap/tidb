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

//! The statement context as Go's planner reads it (`base.PlanContext`): the
//! planner's traits over `tidb_sessionctx::StmtContext`, and the planner state
//! the context carries for it -- the cost-model-v2 environment and the ranger
//! switches derived from it.

use std::sync::Arc;

use tidb_sessionctx::{CostSessionValues, StmtContext, StmtContextData};

use crate::find_best_task::coster::CostEnv;
use tidb_ranger::detacher::RangerOptions;

/// The planner's state on a statement context.
pub trait PlannerStmtCtx {
    /// The statement's cost-model-v2 environment; the default one when none
    /// is attached.
    fn optimizer_cost_env(&self) -> &CostEnv;

    /// Attaches the resolved statement snapshot used by cost model v2.
    #[must_use]
    fn with_optimizer_cost_env(self, env: impl Into<Arc<CostEnv>>) -> Self;

    /// Attaches Go's two resolved HashAgg worker counts.
    ///
    /// Go's cost model reads `HashAggFinalConcurrency()` directly from the
    /// session, so the optimizer cost environment has to see the same value;
    /// otherwise a serial statement would still be costed as if five final
    /// workers were running and would pick a HashAgg over the StreamAgg Go
    /// chooses.
    #[must_use]
    fn with_hashagg_concurrency(self, partial: usize, final_: usize) -> Self;

    /// Go `sctx.GetRangerCtx()`'s session switches for this statement: the
    /// estimator snapshot's, with this statement's prefix-index switch.
    fn ranger_options(&self) -> RangerOptions;

    /// Go `domain.GetDomain(sctx).InfoSchema()` as
    /// `planner/util/domainmisc.GetLatestIndexInfo` reads it: the statement's
    /// latest infoschema, when one is attached.
    fn latest_index_schema(&self) -> Option<Arc<tidb_infoschema::table_source::LatestIndexSchema>>;
}

/// The cost environment's values the statement context reads itself.
fn cost_session_values(env: &CostEnv) -> CostSessionValues {
    CostSessionValues {
        index_lookup_concurrency: env.session.index_lookup_concurrency,
        index_lookup_join_concurrency: env.session.index_lookup_join_concurrency,
        index_join_batch_size: env.session.index_join_batch_size,
        index_lookup_size: env.session.index_lookup_size,
        projection_concurrency: env.session.projection_concurrency,
        mpp_enforced: env.session.mpp_enforced,
    }
}

/// The attached cost environment, or the shared default.
fn cost_env_of(slot: Option<&Arc<dyn std::any::Any + Send + Sync>>) -> &CostEnv {
    // Standalone contexts share defaults only if costing is requested;
    // session contexts attach their resolved snapshot without allocating and
    // immediately discarding a default environment.
    static DEFAULT: std::sync::OnceLock<CostEnv> = std::sync::OnceLock::new();
    slot.and_then(|env| env.downcast_ref::<CostEnv>())
        .unwrap_or_else(|| DEFAULT.get_or_init(CostEnv::default))
}

/// The cost environment `slot` holds with `final_` HashAgg final workers,
/// or `None` when it already has them.
fn with_hashagg_final(
    slot: Option<&Arc<dyn std::any::Any + Send + Sync>>,
    final_: usize,
) -> Option<Arc<CostEnv>> {
    let current = cost_env_of(slot);
    if slot.is_some() && current.session.hashagg_final_concurrency == final_ as f64 {
        return None;
    }
    let mut env = current.clone();
    env.session.hashagg_final_concurrency = final_ as f64;
    Some(Arc::new(env))
}

impl PlannerStmtCtx for StmtContext {
    fn optimizer_cost_env(&self) -> &CostEnv {
        cost_env_of(self.optimizer_cost_env_any())
    }

    fn with_optimizer_cost_env(self, env: impl Into<Arc<CostEnv>>) -> Self {
        let env: Arc<CostEnv> = env.into();
        let values = cost_session_values(&env);
        let union_concurrency = env.session.union_concurrency;
        self.with_optimizer_cost_env_any(env, values, union_concurrency)
    }

    fn with_hashagg_concurrency(self, partial: usize, final_: usize) -> Self {
        let ctx = self.with_hashagg_concurrency_values(partial, final_);
        match with_hashagg_final(ctx.optimizer_cost_env_any(), final_) {
            Some(env) => ctx.replace_optimizer_cost_env_any(env),
            None => ctx,
        }
    }

    fn latest_index_schema(&self) -> Option<Arc<tidb_infoschema::table_source::LatestIndexSchema>> {
        self.latest_info_schema()?.downcast()
    }

    fn ranger_options(&self) -> RangerOptions {
        RangerOptions {
            opt_prefix_index_single_scan: self.opt_prefix_index_single_scan(),
            ..PlannerStmtCtx::optimizer_cost_env(self)
                .session
                .estimator_options
                .ranger_options()
        }
    }
}

/// [`PlannerStmtCtx`]'s setters on the context's builder.
pub trait PlannerStmtCtxData {
    /// [`PlannerStmtCtx::with_optimizer_cost_env`].
    fn with_optimizer_cost_env(&mut self, env: impl Into<Arc<CostEnv>>) -> &mut Self;

    /// [`PlannerStmtCtx::with_hashagg_concurrency`].
    fn with_hashagg_concurrency(&mut self, partial: usize, final_: usize) -> &mut Self;
}

impl PlannerStmtCtxData for StmtContextData {
    fn with_optimizer_cost_env(&mut self, env: impl Into<Arc<CostEnv>>) -> &mut Self {
        let env: Arc<CostEnv> = env.into();
        let values = cost_session_values(&env);
        let union_concurrency = env.session.union_concurrency;
        self.with_optimizer_cost_env_any(env, values, union_concurrency)
    }

    fn with_hashagg_concurrency(&mut self, partial: usize, final_: usize) -> &mut Self {
        let _ = self.with_hashagg_concurrency_values(partial, final_);
        match with_hashagg_final(self.optimizer_cost_env_any(), final_) {
            Some(env) => self.replace_optimizer_cost_env_any(env),
            None => self,
        }
    }
}

impl crate::find_best_task::dispatch::MppWarningSink for StmtContext {
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
impl crate::optimize::OptimizeContext for StmtContext {
    fn session_zone(&self) -> tidb_datatype::SessionTimeZone {
        StmtContext::session_zone(self)
    }
    fn static_partition_prune(&self) -> bool {
        StmtContext::static_partition_prune(self)
    }
    fn stable_result_mode(&self) -> bool {
        StmtContext::stable_result_mode(self)
    }
    fn optimizer_cost_env(&self) -> &crate::find_best_task::coster::CostEnv {
        PlannerStmtCtx::optimizer_cost_env(self)
    }
    fn allow_derive_topn(&self) -> bool {
        StmtContext::allow_derive_topn(self)
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
    fn range_fallback_handler(&self) -> &tidb_context::RangeFallbackHandler {
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
    fn optimizer_fix_control(&self) -> &tidb_fixcontrol::OptimizerFixControl {
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
            tidb_context::WarningLevel::Note
        } else {
            tidb_context::WarningLevel::Warning
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

#[cfg(test)]
mod tests {
    use tidb_sessionctx::StmtContext;

    use super::PlannerStmtCtx;

    #[test]
    fn enforced_mpp_warning_uses_go_explain_and_extra_handlers() {
        let mut env = crate::find_best_task::coster::CostEnv::default();
        env.session.mpp_enforced = true;

        let explain = StmtContext::for_query()
            .with_optimizer_cost_env(env.clone())
            .with_in_explain_stmt(true);
        explain.append_mpp_warning("explain refusal");
        assert_eq!(
            explain.take_warnings().iter().map(tidb_context::SqlWarn::parts).collect::<Vec<_>>(),
            vec![(
                tidb_context::WarningLevel::Warning,
                1105,
                "explain refusal".to_owned()
            )]
        );
        assert!(explain.take_extra_warnings().is_empty());

        let ordinary = StmtContext::for_query().with_optimizer_cost_env(env);
        ordinary.append_mpp_warning("ordinary refusal");
        assert!(ordinary.take_warnings().is_empty());
        assert_eq!(
            ordinary.take_extra_warnings().iter().map(tidb_context::SqlWarn::parts).collect::<Vec<_>>(),
            vec![(
                tidb_context::WarningLevel::Warning,
                1105,
                "ordinary refusal".to_owned()
            )]
        );

        let unenforced = StmtContext::for_query();
        unenforced.append_mpp_warning("ignored refusal");
        assert!(unenforced.take_warnings().is_empty());
        assert!(unenforced.take_extra_warnings().is_empty());
    }
    #[test]
    fn index_lookup_join_concurrency_uses_statement_override_and_fallback() {
        let mut env = crate::find_best_task::coster::CostEnv::default();
        env.session.union_concurrency = 9.0;
        env.session.index_lookup_concurrency = 3.0;
        env.session.index_lookup_join_concurrency = 2.0;
        env.session.index_join_batch_size = 17.0;
        env.session.index_lookup_size = 19.0;
        let overridden = StmtContext::for_query().with_optimizer_cost_env(env);
        assert_eq!(overridden.executor_concurrency(), 9);
        assert_eq!(overridden.index_lookup_concurrency(), 3);
        assert_eq!(overridden.index_lookup_join_concurrency(), 2);
        assert_eq!(overridden.index_join_batch_size(), 17);
        assert_eq!(overridden.index_lookup_size(), 19);

        let mut env = crate::find_best_task::coster::CostEnv::default();
        env.session.union_concurrency = 7.0;
        env.session.index_lookup_concurrency = -1.0;
        env.session.index_lookup_join_concurrency = -1.0;
        env.session.index_join_batch_size = 0.0;
        env.session.index_lookup_size = 0.0;
        let fallback = StmtContext::for_query().with_optimizer_cost_env(env);
        assert_eq!(fallback.index_lookup_concurrency(), 7);
        assert_eq!(fallback.index_lookup_join_concurrency(), 7);
        assert_eq!(fallback.index_join_batch_size(), 1);
        assert_eq!(fallback.index_lookup_size(), 1);
    }
}
