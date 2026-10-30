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

//! The rule context these rules' tests run under. Core's own hooks are not
//! reachable from this crate; a test that needs them lives in the planner.

use tidb_planner_core_base::{PlanError, PlanIdAllocator};
use tidb_expr::schema::Schema;
use tidb_expr::expr_util::builder::PreservingFunctionBuilder;
use tidb_planner_core_operator_logicalop::plan_context::{DisabledLogicalRules, RuleContext};
use tidb_planner_property::stats_info::StatsInfo;

pub(crate) const TEST_BUILDER: PreservingFunctionBuilder = PreservingFunctionBuilder;

/// Core's hooks, which these tests must not reach.
struct NoCore;

impl tidb_planner_core_operator_logicalop::rewrite::DataSourceStatsDeriver for NoCore {
    fn derive_stats_4_data_source(
        &self,
        _source: &mut tidb_planner_core_operator_logicalop::data_source::DataSource,
        _self_schema: &Schema,
        _source_had_stats: bool,
        _env: &tidb_planner_core_operator_logicalop::rewrite::DeriveStatsEnv<'_>,
    ) -> Result<(StatsInfo, bool), PlanError> {
        unreachable!("core's DataSource statistics: a planner test")
    }
}

impl tidb_planner_core_operator_logicalop::plan_context::AccessPathResolver for NoCore {
    fn apply_table_index_hints(
        &self,
        _table: &tidb_infoschema::table_source::SourceTable,
        _public_paths: &[tidb_planner_util::path::PossiblePath],
        _hints: &[tidb_ast::IndexHint],
        _comment_hints: &[tidb_planner_core_operator_logicalop::data_source::DataSourceIndexHint],
        _remove_global_indexes: bool,
        _force_no_index_lookup_push_down: bool,
        _session: tidb_sessionctx::IndexLookupPushDownSession,
        _tikv_in_isolation_read: bool,
        _isolation_read_engines_value: &str,
    ) -> Result<tidb_planner_util::path::ResolvedIndexPaths, PlanError> {
        unreachable!("core's access-path hint resolution: a planner test")
    }
}

pub(crate) fn test_context(allocator: &PlanIdAllocator) -> RuleContext<'_> {
    static COLUMN_ALLOCATOR: tidb_planner_core_base::ColumnIdAllocator =
        tidb_planner_core_base::ColumnIdAllocator::new();
    RuleContext {
        estimator_options: Default::default(),
        allocator,
        column_allocator: &COLUMN_ALLOCATOR,
        builder: &TEST_BUILDER,
        eval_context: &tidb_exprctx::NoColumns,
        use_plan_cache: false,
        plan_cache_marker: None,
        // Go's `AllowDeriveTopN` defaults ON in `sessionVars`.
        allow_derive_topn: true,
        expr_pushdown_blacklist: Default::default(),
        disabled_rules: DisabledLogicalRules::default(),
        statistics_load: None,
        partition_pruning: None,
        opt_index_prune_threshold: 20,
        opt_prefix_index_single_scan: true,
        index_merge_enabled: true,
        no_index_merge_hint: false,
        in_explain_stmt: false,
        range_max_size: 0,
        selectivity_factor: tidb_planner_core_cost::SELECTION_FACTOR,
        range_fallback_handler: None,
        always_keep_join_key: true,
        enable_unsafe_substitute: false,
        enable_semi_join_rewrite: false,
        // Go's `OptimizerEnableNAAJ` defaults ON in `sessionVars`.
        enable_null_aware_anti_join: true,
        enable_no_decorrelate_in_select: false,
        allow_eliminate_apply: true,
        join_reorder_threshold: 0,
        group_ndv_skew_ratio: tidb_vardef::defaults::DEF_OPT_RISK_GROUP_NDV_SKEW_RATIO,
        scale_ndv_skew_ratio: tidb_planner_cardinality::derive_stats::DEF_SCALE_NDV_SKEW_RATIO,
        allow_agg_push_down: false,
        advanced_join_reorder: true,
        cartesian_join_order_threshold: 0.0,
        join_reorder_through_proj: false,
        join_reorder_through_sel: false,
        outer_join_reorder: true,
        advanced_join_hint: true,
        hint_warning_sink: None,
        used_stats_recorder: None,
        cte_optimizer: None,
        data_source_stats: &NoCore,
        predicate_simplifier: &crate::rule_predicate_simplification::RulePredicateSimplifier,
        access_path_resolver: &NoCore,
    }
}
