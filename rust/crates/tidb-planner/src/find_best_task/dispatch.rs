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

//! The GENERAL `findBestTask` dispatcher: Go's volcano search over
//! `(logical operator, required property)` for the whole ported operator
//! set, not only joins.
//!
//! Go sources, ported body by body:
//! * `findBestTask` (`pkg/planner/core/find_best_task.go:605`) — the
//!   memoized entry: the task-map lookup, the non-root task-type gate, the
//!   un-enforced exhaust, the enforcer branch re-exhausting under the empty
//!   property, and the two-pass enumeration.
//! * `enumeratePhysicalPlans4Task` (`:112`) and its helper (`:156`) — the
//!   candidate loop: plan every child under the candidate's child property,
//!   attach, convert to root, enforce.
//! * `compareTaskCost` (`:479`) / `getTaskPlanCost` (`:498`) — an invalid
//!   task prices at `MaxFloat64` and never wins; pricing itself arrives
//!   through [`TaskCoster`], which keeps cost inputs at the dispatch boundary:
//!   the cost formulas live in [`crate::plan_cost_ver2`] but the profile
//!   inputs are the caller's.
//!
//! # Narrowings, each naming its Go symbol
//!
//! * `applyLogicalHintVarEigen`: join hints are applied after child tasks
//!   attach, matching Go's `applyLogicalJoinHint`; TopN/Limit and
//!   aggregation hints remain outside this dispatcher.
//! * `checkOpSelfSatisfyPropTaskTypeRequirement` and the MPP property
//!   fields the enforcer branch resets: no TiFlash tier.
//! * `optimizeByShuffle`: the TiDB-side parallel shuffle rewrite is an
//!   executor-parallelism optimization, absent here.
//! * The task map is keyed by the property's ESSENTIAL fields
//!   ([`prop_key`]) per plan id, standing in for Go's `prop.HashCode()`
//!   over fields this port does not carry.
//!
//! # Operator routing
//!
//! Go gives some operators their own `findBestTask` override instead of an
//! exhaust: the dual, the CTE table, and the two shows are born directly in
//! root tasks. [`find_best_task`] routes them to those ported bodies first,
//! exactly as Go's function-pointer wiring does. `DataSource` and
//! `LogicalMemTable` refuse by name (`findBestTask4LogicalDataSource` is
//! the access-path chooser this crate carries separately;
//! `findBestTask4LogicalMemTable` recurses through the enforcer re-entry).
//! Joins refuse toward [`crate::find_best_task`]'s own specialized search,
//! which owns candidate enumeration for them.

use std::collections::HashMap;

use crate::enforce::enforce_property;
use crate::logical::LogicalPlan;
use crate::physical::{self, PhysicalPlan};
use crate::physical_property::PhysicalProperty;
use crate::plan_base::{PlanError, PlanIdAllocator};
use crate::task::{attach2_task, Task};
use crate::task_type::TaskType;

/// Go `getTaskPlanCost`'s pricing half: what a built task costs.
///
/// Cost formulas are in [`crate::plan_cost_ver2`], while row counts and factor
/// profiles are supplied by the caller.
pub trait TaskCoster {
    /// The task's plan cost; called only on VALID tasks — the invalid-task
    /// `MaxFloat64` arm is [`compare_task_cost`]'s own, as in Go.
    fn task_cost(&self, task: &Task) -> Result<f64, PlanError>;
}

/// Go `compareTaskCost` (`find_best_task.go:479`): whether `cur` beats
/// `best`. An invalid current task never wins; an invalid best always
/// loses; otherwise strictly-lower cost wins.
pub fn compare_task_cost(
    coster: &dyn TaskCoster,
    cur: &Task,
    best: &Task,
) -> Result<bool, PlanError> {
    if cur.invalid() {
        return Ok(false);
    }
    if best.invalid() {
        return Ok(true);
    }
    Ok(coster.task_cost(cur)? < coster.task_cost(best)?)
}

/// Everything one search shares: the id allocator the built operators draw
/// from, the coster, the NDV skew ratio the stats scaling reads, and Go's
/// per-operator task map.
pub struct DispatchContext<'a> {
    /// The plan id allocator for built physical operators.
    pub allocator: &'a PlanIdAllocator,
    /// The pricing seam.
    pub coster: &'a dyn TaskCoster,
    /// The `tidb_opt_skew_ratio` the NDV scaling reads; 1.0 is Go's default.
    pub skew_ratio: f64,
    /// Go `SessionVars.OptOrderingIdxSelRatio`, used by the ordered LIMIT
    /// row-count adjustment for table and index scans.
    pub ordering_index_selectivity_ratio: f64,
    /// Go `SessionVars.AllowProjectionPushDown`, used when Projection
    /// enumerates its TiKV coprocessor candidate.
    pub allow_projection_push_down: bool,
    /// Go `SessionVars.LimitPushDownThreshold`, used by TopN's normal
    /// coprocessor preference. A physical Limit always prefers cop.
    pub limit_push_down_threshold: u64,
    /// Go `SessionVars.EnablePaging`. Reader costing writes the selected
    /// paging mode back onto `PhysicalIndexLookUpReader`.
    pub enable_paging: bool,
    /// Go `SessionVars.HashJoinConcurrency()`, stamped onto every hash-join
    /// candidate by `NewPhysicalHashJoin`.
    pub hash_join_concurrency: usize,
    /// Go `SessionVars.MemQuotaApplyCache`, used by
    /// `exhaustPhysicalPlans4LogicalApply` after estimating the correlated
    /// value hit ratio.
    pub apply_cache_capacity: i64,
    /// Go `SessionVars.IsMPPAllowed()`, which controls MPP candidates.
    pub mpp_allowed: bool,
    /// Go `BaseLogicalPlan.taskMap`, keyed here by `(plan id, prop key)`.
    task_map: HashMap<(i32, String), Task>,
    /// Go `SessionVars.AllocPlanColumnID`: the column-id allocator the
    /// aggregate partial/final split draws fresh columns from. `None` keeps
    /// pre-split searches working; a search that can push aggregates sets it
    /// through [`DispatchContext::with_column_ids`].
    pub column_ids: Option<&'a crate::expression_rewriter::ColumnIdAllocator>,
}

impl<'a> DispatchContext<'a> {
    /// A fresh search context.
    #[must_use]
    pub fn new(
        allocator: &'a PlanIdAllocator,
        coster: &'a dyn TaskCoster,
        skew_ratio: f64,
    ) -> Self {
        Self {
            allocator,
            coster,
            skew_ratio,
            ordering_index_selectivity_ratio: 0.01,
            allow_projection_push_down: true,
            // Go `vardef.DefOptLimitPushDownThreshold`.
            limit_push_down_threshold: 5_000,
            enable_paging: true,
            hash_join_concurrency: 5,
            apply_cache_capacity: 0,
            // Go `vardef.DefTiDBAllowMPPExecution` is true.
            mpp_allowed: true,
            task_map: HashMap::new(),
            column_ids: None,
        }
    }

    /// The same context with the statement's ordering-index selectivity
    /// ratio attached.
    #[must_use]
    pub const fn with_ordering_index_selectivity_ratio(mut self, ratio: f64) -> Self {
        self.ordering_index_selectivity_ratio = ratio;
        self
    }

    /// The same context with the session's projection-pushdown switch.
    #[must_use]
    pub const fn with_projection_push_down(mut self, allow: bool) -> Self {
        self.allow_projection_push_down = allow;
        self
    }

    /// The same context with the session's TopN pushdown threshold.
    #[must_use]
    pub const fn with_limit_push_down_threshold(mut self, threshold: u64) -> Self {
        self.limit_push_down_threshold = threshold;
        self
    }

    /// The same context with the session's coprocessor-paging switch.
    #[must_use]
    pub const fn with_paging(mut self, enable: bool) -> Self {
        self.enable_paging = enable;
        self
    }

    /// The same context with the resolved hash-join concurrency.
    #[must_use]
    pub const fn with_hash_join_concurrency(mut self, concurrency: usize) -> Self {
        self.hash_join_concurrency = concurrency;
        self
    }

    /// The same context with Go's per-session Apply-cache quota.
    #[must_use]
    pub const fn with_apply_cache_capacity(mut self, capacity: i64) -> Self {
        self.apply_cache_capacity = capacity;
        self
    }

    /// The same context with Go's resolved `IsMPPAllowed()` value.
    #[must_use]
    pub const fn with_mpp_allowed(mut self, allowed: bool) -> Self {
        self.mpp_allowed = allowed;
        self
    }

    /// The same context with the session's column-id allocator attached.
    #[must_use]
    pub fn with_column_ids(
        mut self,
        column_ids: &'a crate::expression_rewriter::ColumnIdAllocator,
    ) -> Self {
        self.column_ids = Some(column_ids);
        self
    }
}

/// The task-map key: the property's essential fields, standing in for Go's
/// `PhysicalProperty.HashCode()` over the ported field set.
fn prop_key(prop: &PhysicalProperty) -> String {
    use std::fmt::Write as _;
    let mut key = format!(
        "{:?}|{}|{}",
        prop.task_tp, prop.expected_cnt, prop.can_add_enforcer
    );
    for item in &prop.sort_items {
        let _ = write!(key, "|{}:{}", item.col, item.desc);
    }
    for item in &prop.advisory_sort_items {
        let _ = write!(key, "|advisory:{}:{}", item.col, item.desc);
    }
    if let Some(runtime) = &prop.index_join_prop {
        let _ = write!(
            key,
            "|ij:{}:{}",
            runtime.table_range_scan, runtime.avg_inner_row_count
        );
        for column in &runtime.outer_join_keys {
            let _ = write!(key, ":o{}", column.unique_id);
        }
        for column in &runtime.inner_join_keys {
            let _ = write!(key, ":i{}", column.unique_id);
        }
    }
    key
}

/// Go `exhaustPhysicalPlans` over the enum's ported operator set: the
/// candidate lists one operator offers under `prop`. Each inner list is one
/// preference slice, as Go's `[][]base.PhysicalPlan` is.
fn exhaust_physical_plans(
    plan: &LogicalPlan,
    prop: &PhysicalProperty,
    ctx: &DispatchContext<'_>,
) -> Result<Vec<Vec<PhysicalPlan>>, PlanError> {
    let one = |plans: Vec<PhysicalPlan>| -> Vec<Vec<PhysicalPlan>> {
        if plans.is_empty() {
            Vec::new()
        } else {
            vec![plans]
        }
    };
    match plan {
        LogicalPlan::Selection(op) => {
            Ok(one(physical::exhaust_physical_plans_4_logical_selection(
                op,
                prop,
                ctx.allocator,
                ctx.skew_ratio,
            )))
        }
        LogicalPlan::Projection(op) => {
            Ok(one(physical::exhaust_physical_plans_4_logical_projection(
                op,
                prop,
                ctx.allocator,
                ctx.skew_ratio,
                ctx.allow_projection_push_down,
            )))
        }
        LogicalPlan::Sort(op) => Ok(one(physical::exhaust_physical_plans_4_logical_sort(
            op,
            prop,
            ctx.allocator,
            ctx.skew_ratio,
        ))),
        LogicalPlan::Limit(op) => Ok(one(physical::exhaust_physical_plans_4_logical_limit(
            op,
            prop,
            ctx.allocator,
        ))),
        LogicalPlan::Lock(op) => Ok(one(physical::exhaust_physical_plans_4_logical_lock(
            op,
            prop,
            ctx.allocator,
            ctx.skew_ratio,
        ))),
        LogicalPlan::MaxOneRow(op) => Ok(one(
            physical::exhaust_physical_plans_4_logical_max_one_row(op, prop, ctx.allocator),
        )),
        LogicalPlan::UnionAll(op) => Ok(one(physical::exhaust_physical_plans_4_logical_union_all(
            op,
            prop,
            ctx.allocator,
            ctx.skew_ratio,
        ))),
        LogicalPlan::PartitionUnionAll(op) => Ok(one(
            physical::exhaust_physical_plans_4_logical_partition_union_all(
                op,
                prop,
                ctx.allocator,
                ctx.skew_ratio,
            ),
        )),
        LogicalPlan::Sequence(op) => Ok(one(physical::exhaust_physical_plans_4_logical_sequence(
            op,
            prop,
            ctx.allocator,
        ))),
        LogicalPlan::TopN(op) => {
            // `ExhaustPhysicalPlans4LogicalTopN` (`physical_topn.go:55`):
            // admitted only when the required order matches the by-items
            // (`MatchItems`); TWO preference slices — the TopN operators
            // and the LIMIT half. The TopN half needs the expression-borne
            // push-down machinery (`getPhysTopN`/`CanExprsPushDown`) and
            // refuses as an EMPTY slice here, named; the LIMIT half rides
            // the keep-order paths (`getPhysLimits`).
            if !physical::match_items(prop, &op.by_items) {
                return Ok(Vec::new());
            }
            // Go's two preference slices, in order: the TopN operators
            // (`getPhysTopN`), then the LIMIT half (`getPhysLimits`).
            let mut slices = Vec::with_capacity(2);
            let topns = physical::get_phys_topn(op, prop, ctx.allocator, ctx.mpp_allowed);
            if !topns.is_empty() {
                slices.push(topns);
            }
            let limits = physical::get_phys_limits(op, prop, ctx.allocator);
            if !limits.is_empty() {
                slices.push(limits);
            }
            Ok(slices)
        }
        LogicalPlan::Aggregation(op) => {
            // `ExhaustPhysicalPlans4LogicalAggregation`
            // (`base_physical_agg.go:935`): Go enumerates HashAgg first and
            // immediately returns it when HASH_AGG applies, then enumerates
            // StreamAgg and immediately returns it when STREAM_AGG applies.
            // With no applicable hint both families share one cost search in
            // that same hash-then-stream order.
            let mut hash_aggs = physical::get_hash_aggs(op, prop, ctx.allocator, ctx.skew_ratio);
            if !hash_aggs.is_empty()
                && op.prefer_agg_type & crate::expression_rewriter::PREFER_HASH_AGG != 0
            {
                return Ok(vec![hash_aggs]);
            }
            let stream_aggs = physical::get_stream_aggs(op, prop, ctx.allocator, ctx.skew_ratio);
            if !stream_aggs.is_empty()
                && op.prefer_agg_type & crate::expression_rewriter::PREFER_STREAM_AGG != 0
            {
                return Ok(vec![stream_aggs]);
            }
            hash_aggs.extend(stream_aggs);
            let aggs = hash_aggs;
            Ok(if aggs.is_empty() {
                Vec::new()
            } else {
                vec![aggs]
            })
        }
        LogicalPlan::Join(op) => {
            use crate::find_best_task::JoinStrategy;
            use crate::plan_builder::from::join_hint_flags;

            let reduced = crate::find_best_task::project_one_join(op, plan)?;
            let (left_columns, right_columns, is_null_eq, _) = op.get_join_keys();
            let column = |columns: &[tidb_expr::column::Column], id: i64| {
                columns
                    .iter()
                    .find(|column| column.unique_id == id)
                    .cloned()
            };
            let mut joins = Vec::new();
            for candidate in crate::find_best_task::exhaust_join(&reduced, prop) {
                let strategy = candidate.strategy.clone();
                let filtered = match &strategy {
                    JoinStrategy::Hash(_) => op.prefer_any(&[join_hint_flags::NO_HASH_JOIN]),
                    JoinStrategy::Merge { .. } => op.prefer_any(&[join_hint_flags::NO_MERGE_JOIN]),
                    JoinStrategy::Index { kind, .. } => match kind {
                        crate::plan_cost_ver2::IndexJoinKind::IndexJoin => {
                            op.prefer_any(&[join_hint_flags::NO_INDEX_JOIN])
                        }
                        crate::plan_cost_ver2::IndexJoinKind::IndexHashJoin => {
                            op.prefer_any(&[join_hint_flags::NO_INDEX_HASH_JOIN])
                        }
                        crate::plan_cost_ver2::IndexJoinKind::IndexMergeJoin => {
                            op.prefer_any(&[join_hint_flags::NO_INDEX_MERGE_JOIN])
                        }
                    },
                };
                if filtered {
                    continue;
                }
                let mut child_props = candidate.child_props;
                if let JoinStrategy::Index {
                    outer_idx,
                    table_range_scan,
                    ..
                } = &strategy
                {
                    let inner_idx = 1 - *outer_idx;
                    let (outer_join_keys, inner_join_keys) = if *outer_idx == 0 {
                        (left_columns.clone(), right_columns.clone())
                    } else {
                        (right_columns.clone(), left_columns.clone())
                    };
                    let outer_rows = plan
                        .children()
                        .get(*outer_idx)
                        .and_then(LogicalPlan::stats_info)
                        .map_or(1.0, crate::stats_info::StatsInfo::row_count)
                        .max(1.0);
                    let joined_rows = op
                        .base
                        .base
                        .stats_info()
                        .map_or(outer_rows, crate::stats_info::StatsInfo::row_count);
                    child_props[inner_idx].index_join_prop =
                        Some(crate::physical_property::IndexJoinRuntimeProp {
                            other_conditions: op.other_conditions.clone(),
                            outer_join_keys,
                            inner_join_keys,
                            avg_inner_row_count: (joined_rows / outer_rows).max(0.0),
                            table_range_scan: *table_range_scan,
                        });
                }
                let mut base = physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    op.base.base.tp(),
                    op.base.base.query_block_offset(),
                );
                base.base.set_stats(op.base.base.stats_info().cloned());
                base.base.set_schema(op.base.base.schema().cloned());
                base.set_children_req_props(child_props.into_iter().map(Some).collect());
                let physical = match strategy {
                    JoinStrategy::Hash(shape) => {
                        PhysicalPlan::HashJoin(physical::PhysicalHashJoin {
                            base,
                            concurrency: ctx.hash_join_concurrency,
                            join_type: op.join_type,
                            inner_child_idx: shape.inner_idx,
                            use_outer_to_build: shape.use_outer_to_build,
                            left_join_keys: left_columns.clone(),
                            right_join_keys: right_columns.clone(),
                            equal_conditions: op.equal_conditions.clone(),
                            na_equal_conditions: op.na_eq_conditions.clone(),
                            left_conditions: op.left_conditions.clone(),
                            right_conditions: op.right_conditions.clone(),
                            other_conditions: op.other_conditions.clone(),
                            default_values: op.default_values.clone(),
                        })
                    }
                    JoinStrategy::Merge {
                        left_keys,
                        right_keys,
                        desc,
                    } => {
                        let Some(left_join_keys) = left_keys
                            .into_iter()
                            .map(|id| column(&left_columns, id))
                            .collect::<Option<Vec<_>>>()
                        else {
                            continue;
                        };
                        let Some(right_join_keys) = right_keys
                            .into_iter()
                            .map(|id| column(&right_columns, id))
                            .collect::<Option<Vec<_>>>()
                        else {
                            continue;
                        };
                        PhysicalPlan::MergeJoin(physical::PhysicalMergeJoin {
                            base,
                            join_type: op.join_type,
                            left_join_keys,
                            right_join_keys,
                            left_conditions: op.left_conditions.clone(),
                            right_conditions: op.right_conditions.clone(),
                            other_conditions: op.other_conditions.clone(),
                            default_values: op.default_values.clone(),
                            desc,
                        })
                    }
                    JoinStrategy::Index {
                        outer_idx,
                        kind,
                        keep_outer_order,
                        ..
                    } => {
                        let (outer_join_keys, inner_join_keys) = if outer_idx == 0 {
                            (left_columns.clone(), right_columns.clone())
                        } else {
                            (right_columns.clone(), left_columns.clone())
                        };
                        PhysicalPlan::IndexJoin(physical::PhysicalIndexJoin {
                            base,
                            join_type: op.join_type,
                            inner_child_idx: 1 - outer_idx,
                            kind,
                            keep_outer_order,
                            inner_access_table_id: None,
                            inner_access_index_id: None,
                            left_join_keys: left_columns.clone(),
                            right_join_keys: right_columns.clone(),
                            outer_join_keys,
                            inner_join_keys,
                            is_null_eq: is_null_eq.clone(),
                            left_conditions: op.left_conditions.clone(),
                            right_conditions: op.right_conditions.clone(),
                            other_conditions: op.other_conditions.clone(),
                            default_values: op.default_values.clone(),
                            outer_hash_keys: Vec::new(),
                            inner_hash_keys: Vec::new(),
                            equal_conditions: op.equal_conditions.clone(),
                            ranges: crate::ranger::types::Ranges::default(),
                            key_off2_idx_off: Vec::new(),
                            idx_col_lens: Vec::new(),
                            compare_filters: None,
                            range_rebuild: None,
                        })
                    }
                };
                joins.push(physical);
            }
            Ok(one(joins))
        }
        LogicalPlan::Apply(op) => {
            // Go `exhaustPhysicalPlans4LogicalApply`: Apply can preserve only
            // an order supplied by its OUTER child and never runs as MPP.
            let outer_schema = plan
                .children()
                .first()
                .and_then(LogicalPlan::schema)
                .ok_or_else(|| PlanError::internal("LogicalApply has no outer schema"))?;
            if prop.task_tp == TaskType::Mpp
                || !prop.sort_items.iter().all(|item| {
                    outer_schema
                        .columns
                        .iter()
                        .any(|column| column.unique_id == item.col)
                })
            {
                return Ok(Vec::new());
            }

            let outer_rows = plan
                .children()
                .first()
                .and_then(LogicalPlan::stats_info)
                .map_or(0.0, crate::stats_info::StatsInfo::row_count);
            let stats = op.base().base.stats_info().cloned();
            let apply_rows = stats
                .as_ref()
                .map_or(0.0, crate::stats_info::StatsInfo::row_count);
            // Go `physicalop.CalcChildExpectedCnt`.
            let outer_expected_cnt = if prop.expected_cnt < apply_rows
                || (!prop.is_sort_item_empty()
                    && ctx.ordering_index_selectivity_ratio > 0.0
                    && outer_rows > apply_rows
                    && prop.expected_cnt < outer_rows
                    && apply_rows > 0.0)
            {
                let rows_to_meet_first = if prop.is_sort_item_empty() {
                    0.0
                } else {
                    ((outer_rows - apply_rows) * ctx.ordering_index_selectivity_ratio).max(0.0)
                };
                outer_rows * prop.expected_cnt / apply_rows + rows_to_meet_first
            } else {
                f64::MAX
            };
            let outer_prop = PhysicalProperty {
                sort_items: prop.sort_items.clone(),
                task_tp: TaskType::Root,
                expected_cnt: outer_expected_cnt,
                can_add_enforcer: false,
                mpp_partition_cols: Vec::new(),
                mpp_partition_tp: Default::default(),
                sort_items_for_partition: Vec::new(),
                cte_producer_status: prop.cte_producer_status,
                no_cop_push_down: true,
                advisory_sort_items: Vec::new(),
                index_join_prop: None,
            };
            let inner_prop = PhysicalProperty {
                cte_producer_status: prop.cte_producer_status,
                no_cop_push_down: prop.no_cop_push_down,
                ..PhysicalProperty::default()
            };

            let can_use_cache = stats.as_ref().is_some_and(|stats| {
                if stats.row_count() == 0.0 || ctx.apply_cache_capacity <= 0 {
                    return false;
                }
                let ids = op
                    .cor_cols
                    .iter()
                    .map(|column| column.column.unique_id)
                    .collect::<Vec<_>>();
                let (ndv, _) = crate::cardinality::derive_stats::estimate_cols_ndv_with_matched_len(
                    &ids, stats,
                );
                1.0 - ndv / stats.row_count() > 0.1
            });
            let (left_join_keys, right_join_keys, _, _) = op.join.get_join_keys();
            let mut base = physical::BasePhysicalPlan::new(
                ctx.allocator,
                crate::logical::LogicalApply::TYPE,
                op.base().base.query_block_offset(),
            );
            base.base.set_stats(
                stats.map(|stats| stats.scale_by_expect_cnt(prop.expected_cnt, ctx.skew_ratio)),
            );
            base.base.set_schema(op.base().base.schema().cloned());
            base.set_children_req_props(vec![Some(outer_prop), Some(inner_prop)]);
            let apply = physical::PhysicalApply {
                hash_join: physical::PhysicalHashJoin {
                    base,
                    concurrency: ctx.hash_join_concurrency,
                    join_type: op.join.join_type,
                    inner_child_idx: 1,
                    use_outer_to_build: false,
                    left_join_keys,
                    right_join_keys,
                    equal_conditions: op.join.equal_conditions.clone(),
                    na_equal_conditions: op.join.na_eq_conditions.clone(),
                    left_conditions: op.join.left_conditions.clone(),
                    right_conditions: op.join.right_conditions.clone(),
                    other_conditions: op.join.other_conditions.clone(),
                    default_values: op.join.default_values.clone(),
                },
                can_use_cache,
                concurrency: 0,
                keep_order: !prop.is_sort_item_empty(),
                outer_schema: op.cor_cols.clone(),
                no_decorrelate: op.no_decorrelate,
            };
            Ok(one(vec![PhysicalPlan::Apply(apply)]))
        }
        other => Err(PlanError::internal(format!(
            "exhaustPhysicalPlans over {} is not ported to the dispatcher",
            other.tp()
        ))),
    }
}

/// Go `findBestTask` (`find_best_task.go:605`) over the enum world.
pub fn find_best_task(
    plan: &LogicalPlan,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    // Operators with their own findBestTask override are routed first, as
    // Go's function-pointer wiring does. They are born in root tasks and
    // are never memoized poorly: the general tail below still stores them.
    let key = (plan.id(), prop_key(prop));
    if let Some(cached) = ctx.task_map.get(&key) {
        return Ok(cached.copy());
    }
    let mut best = find_best_task_uncached(plan, prop, ctx)?;
    // `IndexJoinProp` is not merely a costing hint: Go's selected inner task
    // must return `IndexJoinInfo` from the access path that accepted the
    // runtime probe. A structurally valid task that lost this bottom-up
    // receipt is not an index-join inner candidate. Mark it invalid here so
    // enumeration can continue with MergeJoin/HashJoin instead of aborting
    // later in `completePhysicalIndexJoin`.
    if prop.index_join_prop.is_some() && !task_has_index_join_info(&best) {
        best = Task::invalid_task();
    }
    if let Some(plan) = best.plan_mut() {
        apply_reader_cost_side_effects(plan, ctx.enable_paging);
    }
    ctx.task_map.insert(key, best.copy());
    Ok(best)
}

/// Go's lookup-reader cost functions set `PhysicalIndexLookUpReader.Paging`
/// on the winning physical tree. The Rust coster is deliberately read-only,
/// so apply that same cost side effect before memoizing the selected task.
fn apply_reader_cost_side_effects(plan: &mut PhysicalPlan, enable_paging: bool) {
    match plan {
        PhysicalPlan::TableReader(reader) => {
            if let Some(child) = reader.table_plan.as_deref_mut() {
                apply_reader_cost_side_effects(child, enable_paging);
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(child) = reader.index_plan.as_deref_mut() {
                apply_reader_cost_side_effects(child, enable_paging);
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            reader.paging = enable_paging
                && reader.expect_cnt > 0
                && reader.expect_cnt <= crate::plan_cost_ver2::PAGING_THRESHOLD;
            if let Some(child) = reader.index_plan.as_deref_mut() {
                apply_reader_cost_side_effects(child, enable_paging);
            }
            if let Some(child) = reader.table_plan.as_deref_mut() {
                apply_reader_cost_side_effects(child, enable_paging);
            }
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            for child in &mut reader.partial_plans_raw {
                apply_reader_cost_side_effects(child, enable_paging);
            }
            if let Some(child) = reader.table_plan.as_deref_mut() {
                apply_reader_cost_side_effects(child, enable_paging);
            }
        }
        _ => {}
    }
    for child in plan.base_mut().children_mut() {
        apply_reader_cost_side_effects(child, enable_paging);
    }
}

fn task_has_index_join_info(task: &Task) -> bool {
    match task {
        Task::Root(root) => root.index_join_info.is_some(),
        Task::Cop(cop) => cop.index_join_info.is_some(),
        Task::Mpp(_) => false,
    }
}

fn find_best_task_uncached(
    plan: &LogicalPlan,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    match plan {
        LogicalPlan::TableDual(op) => {
            return Ok(physical::find_best_task_4_logical_table_dual(
                op,
                prop,
                ctx.allocator,
            ));
        }
        LogicalPlan::CTETable(op) => {
            return Ok(physical::find_best_task_4_logical_cte_table(
                op,
                prop,
                ctx.allocator,
            ));
        }
        LogicalPlan::CTE(op) if op.base.children().is_empty() => {
            return physical::find_best_task_4_logical_cte(op, prop, ctx.allocator);
        }
        LogicalPlan::Show(op) => {
            return Ok(physical::find_best_task_4_logical_show(
                op,
                prop,
                ctx.allocator,
            ));
        }
        LogicalPlan::ShowDDLJobs(op) => {
            return Ok(physical::find_best_task_4_logical_show_ddl_jobs(
                op,
                prop,
                ctx.allocator,
            ));
        }
        LogicalPlan::DataSource(op) => {
            return find_best_task_4_logical_data_source(op, prop, ctx);
        }
        LogicalPlan::MemTable(_) => {
            return Err(PlanError::internal(
                "findBestTask4LogicalMemTable recurses through FindBestTask \
                 for its enforcer re-entry; not ported",
            ));
        }
        _ => {}
    }

    // `prop.TaskTp != RootTaskType && !IsFlashProp()` — with no TiFlash
    // tier: any non-root requirement is the invalid task, Go's own early
    // answer ("Currently all plan cannot totally push down to TiKV").
    if prop.task_tp != TaskType::Root {
        return Ok(Task::invalid_task());
    }

    let mut can_add_enforcer = prop.can_add_enforcer;
    // An unhinted enumeration always answers hintWorksWithProp = true, so
    // Go's !hintWorksWithProp trigger cannot fire; the narrowed condition
    // is exactly the caller's CanAddEnforcer.
    let _ = &mut can_add_enforcer;

    let mut new_prop = prop.clone_essential_fields();
    // Go restores `IndexJoinProp` immediately after
    // `CloneEssentialFields`, whose contract deliberately omits it. The
    // operator-specific `admitIndexJoinProp(s)` functions then decide which
    // child may inherit it.
    new_prop.index_join_prop = prop.index_join_prop.clone();
    let plans_fits_prop = exhaust_physical_plans(plan, &new_prop, ctx)?;

    let plans_need_enforce = if can_add_enforcer {
        let mut empty = new_prop.clone_essential_fields();
        empty.sort_items = Vec::new();
        empty.sort_items_for_partition = Vec::new();
        empty.expected_cnt = f64::MAX;
        exhaust_physical_plans(plan, &empty, ctx)?
    } else {
        Vec::new()
    };

    let best_task = enumerate_physical_plans_4_task(plan, &plans_fits_prop, prop, false, ctx)?;
    let cur_task = enumerate_physical_plans_4_task(plan, &plans_need_enforce, prop, true, ctx)?;
    if compare_task_cost(ctx.coster, &cur_task, &best_task)? {
        return Ok(cur_task);
    }
    Ok(best_task)
}

/// Go `tryToGetDualTask` (`find_best_task.go:749`): a pushed-down constant
/// that evaluates false turns the whole source into a dual — Go's
/// `WHERE FALSE` short-circuit, before any path is considered.
///
/// Go runs `expression.EvalBool`, which coerces EVERY constant type; this
/// port answers the spellings the rewriter produces for a constant-false
/// predicate — integer zero and NULL — and leaves other constant types
/// unshort-circuited rather than mis-coercing them (the source still plans,
/// just without the dual fast path).
fn try_to_get_dual_task(
    ds: &crate::logical::DataSource,
    ctx: &DispatchContext<'_>,
) -> Option<Task> {
    use tidb_expr::expression::Expression;
    for cond in &ds.pushed_down_conds {
        let Expression::Constant(constant) = cond else {
            continue;
        };
        if constant.deferred_expr.is_some() || constant.param_marker.is_some() {
            continue;
        }
        let is_false = matches!(constant.value, tidb_datatype::Datum::Null)
            || constant.value.as_int() == Some(0);
        if is_false {
            let mut base = crate::physical::BasePhysicalPlan::new(
                ctx.allocator,
                crate::logical::LogicalTableDual::TYPE,
                ds.base.base.query_block_offset(),
            );
            base.base.set_stats(ds.base.base.stats_info().cloned());
            base.base.set_schema(ds.base.base.schema().cloned());
            let dual =
                PhysicalPlan::TableDual(crate::physical::PhysicalTableDual { base, row_count: 0 });
            let mut root = crate::task::RootTask::default();
            root.set_plan(dual);
            return Some(Task::Root(root));
        }
    }
    None
}

/// Go `findBestTask4LogicalDataSource` (`find_best_task.go:2027`), the
/// TABLE-PATH slice: the dual short-circuit, then one cop-task candidate
/// per TABLE access path (`convertToTableScan` without ranger — the scan is
/// the full range the enumerated path carries), finished through
/// [`crate::task::Task::convert_to_root_task`]'s table branch.
///
/// # Narrowings, each naming its Go symbol
///
/// * A non-empty required order answers the invalid task: the
///   `isMatchProp` handle-order admission of `convertToTableScan`
///   (`:2834`) is keep-order work over ranges this slice does not build.
/// * INDEX paths (`convertToIndexScan`), `PointGet`/`BatchPointGet`, and
///   index merge enumerate NO candidate here — fewer candidates than Go,
///   the same class of narrowing as the projection's cop branch. The
///   skyline prune (`skylinePruning`) has nothing to prune with one
///   candidate shape.
/// * `isolation read engines`, `IsForUpdateRead` filtering (`:2036`), and
///   the TiFlash arms narrow with the absent tiers.
/// Go `matchProperty`'s INT-HANDLE arm (`find_best_task.go:1082`): a table
/// path over an integer handle delivers the required order exactly when the
/// property is ONE sort item on the pk-is-handle column (asc or desc; Go's
/// TiFlash-desc refusal narrows with the tier). Cluster tables, vector
/// properties, and the index-column prefix walk (`:1095`) are later slices,
/// named here.
fn table_path_matches_order(ds: &crate::logical::DataSource, prop: &PhysicalProperty) -> bool {
    if ds.pk_is_handle && ds.handle_is_int {
        let Some(pk_col) = ds.handle_cols.first() else {
            return false;
        };
        let [item] = prop.sort_items.as_slice() else {
            return false;
        };
        return item.col == pk_col.unique_id;
    }
    let (all_same, _) = prop.all_same_order();
    if !all_same || prop.sort_items.is_empty() {
        return false;
    }
    let fixed = equality_fixed_ids(ds);
    let mut handle_offset = 0;
    for item in &prop.sort_items {
        let mut found = false;
        while let Some(column) = ds.common_handle_cols.get(handle_offset) {
            let length = ds.common_handle_lens.get(handle_offset).copied();
            handle_offset += 1;
            if length == Some(tidb_datatype::UNSPECIFIED_LENGTH) && column.unique_id == item.col {
                found = true;
                break;
            }
            if fixed.contains(&column.unique_id) {
                continue;
            }
            return false;
        }
        if !found {
            return false;
        }
    }
    true
}

/// The basic index-prefix arm of Go `matchProperty` (`find_best_task.go:1095`):
/// the required order matches an index when every sort item is the same
/// direction (`AllSameOrder`) and the items follow the index's columns,
/// mapped to unique ids through the source's schema (an index column's
/// `Offset` addresses the TABLE column list, which is the scan's schema
/// order). Index columns fixed to one constant by access conditions may be
/// skipped, matching Go's `path.ConstCols` case.
/// Go `DataSource.IsSingleScan` (`logical_datasource.go:677`) over the
/// catalog's offset/name model, in the `ColsRequiringFullLen == nil`
/// fallback branch this pipeline is always in (column pruning does not fill
/// that list here): every schema column must be covered by the index or the
/// handle (`IsIndexCoveringColumns`).
///
/// `indexCoveringColumn`, ported arm by arm: the int-handle primary key
/// covers its column (`stateCoveredByIntHandle`); a plain index column
/// covers only at FULL length (`isIndexColsCoveringCol` refuses a prefix
/// unless ignoreLen, and this caller never ignores). The common-handle and
/// new-collation clustered-index arms sit behind the unported
/// common-handle world and refuse conservatively with it.
fn index_path_is_single_scan(
    ds: &crate::logical::DataSource,
    source_index: &crate::plan_builder::catalog::SourceIndex,
) -> bool {
    ds.columns.iter().all(|column| {
        if ds.pk_is_handle && column.is_primary_key {
            return true;
        }
        source_index.columns.iter().any(|index_column| {
            index_column.length < 0 && index_column.name.eq_ignore_ascii_case(&column.name)
        })
    })
}

fn index_path_matches_order(
    ds: &crate::logical::DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    prop: &PhysicalProperty,
) -> bool {
    let (all_same, _) = prop.all_same_order();
    if prop.is_sort_item_empty() || !all_same {
        return false;
    }
    let fixed = equality_fixed_ids(ds);
    let mut index_offset = 0;
    for item in &prop.sort_items {
        let mut found = false;
        while let Some(index_column) = index.columns.get(index_offset) {
            index_offset += 1;
            let schema_column = ds.schema_column_for_index_column(index_column);
            if index_column.length < 0
                && schema_column.is_some_and(|column| column.unique_id == item.col)
            {
                found = true;
                break;
            }
            if schema_column.is_some_and(|column| fixed.contains(&column.unique_id)) {
                continue;
            }
            return false;
        }
        if !found {
            return false;
        }
    }
    true
}

fn equality_fixed_ids(ds: &crate::logical::DataSource) -> Vec<i64> {
    use tidb_expr::expression::Expression;
    let mut ids = Vec::new();
    for condition in &ds.pushed_down_conds {
        let Expression::ScalarFunction(function) = condition else {
            continue;
        };
        let id = match function.get_args() {
            [Expression::Column(column), Expression::Constant(_)]
            | [Expression::Constant(_), Expression::Column(column)]
                if function.func_name.lowercase() == "eq" =>
            {
                Some(column.unique_id)
            }
            _ => None,
        };
        if let Some(id) = id {
            ids.push(id);
        }
    }
    ids
}

/// The path admission half of Go's
/// `buildDataSource2{Table,Index}ScanByIndexJoinProp`: a table-range
/// candidate must probe the clustered handle; an index-range candidate must
/// cover a leading run made of runtime join keys and equality-fixed columns.
fn path_matches_index_join_runtime(
    ds: &crate::logical::DataSource,
    path: &crate::access_path::PossiblePath,
    runtime: &crate::physical_property::IndexJoinRuntimeProp,
) -> bool {
    let inner_ids: Vec<i64> = runtime
        .inner_join_keys
        .iter()
        .map(|column| column.unique_id)
        .collect();
    match path {
        crate::access_path::PossiblePath::Table { .. } => {
            if !runtime.table_range_scan {
                return false;
            }
            if ds.handle_is_int {
                return ds
                    .handle_cols
                    .first()
                    .is_some_and(|column| inner_ids.contains(&column.unique_id));
            }
            // Go's common-handle table-range builder follows the PRIMARY KEY
            // from its first column. Runtime join keys and equality-fixed
            // columns may jointly cover that leading run, but a later handle
            // column cannot be probed across an unfixed earlier one.
            let fixed = equality_fixed_ids(ds);
            let mut matched_runtime_key = false;
            for column in &ds.common_handle_cols {
                if inner_ids.contains(&column.unique_id) {
                    matched_runtime_key = true;
                } else if !fixed.contains(&column.unique_id) {
                    break;
                }
            }
            matched_runtime_key
        }
        crate::access_path::PossiblePath::Index { index } => {
            if runtime.table_range_scan {
                return false;
            }
            let Some(index) = ds.indexes.get(*index) else {
                return false;
            };
            let Some(schema) = ds.base.base.schema() else {
                return false;
            };
            let fixed = equality_fixed_ids(ds);
            let mut matched_runtime_key = false;
            for index_column in &index.columns {
                let Some(column) = schema.columns.get(index_column.offset) else {
                    return false;
                };
                if inner_ids.contains(&column.unique_id) {
                    matched_runtime_key = true;
                } else if !fixed.contains(&column.unique_id) {
                    break;
                }
            }
            matched_runtime_key
        }
    }
}

/// Go `completeIndexJoinFeedBackInfo`: return the selected access's complete
/// prefix lengths and map every logical inner key to the chosen key column.
/// A key left at `-1` becomes a residual equality when the parent completes
/// `PhysicalIndexJoin`.
fn index_join_feedback(
    ds: &crate::logical::DataSource,
    path: &crate::access_path::PossiblePath,
    runtime: &crate::physical_property::IndexJoinRuntimeProp,
    ranges: crate::ranger::types::Ranges,
) -> crate::task::IndexJoinInfo {
    let (access_columns, idx_col_lens) = match path {
        crate::access_path::PossiblePath::Table { .. } if ds.handle_is_int => (
            ds.handle_cols.iter().take(1).collect::<Vec<_>>(),
            Vec::new(),
        ),
        crate::access_path::PossiblePath::Table { .. } => (
            ds.common_handle_cols.iter().collect::<Vec<_>>(),
            ds.common_handle_lens.clone(),
        ),
        crate::access_path::PossiblePath::Index { index } => {
            let source_index = ds.indexes.get(*index);
            let schema = ds.base.base.schema();
            let columns = source_index
                .into_iter()
                .flat_map(|index| &index.columns)
                .filter_map(|column| schema.and_then(|schema| schema.columns.get(column.offset)))
                .collect::<Vec<_>>();
            let lengths = source_index
                .map(|index| index.columns.iter().map(|column| column.length).collect())
                .unwrap_or_default();
            (columns, lengths)
        }
    };
    let fixed = equality_fixed_ids(ds);
    let mut key_off2_idx_off = vec![-1; runtime.inner_join_keys.len()];
    let mut matched_runtime_key = false;
    let mut first_unmatched_index_offset = access_columns.len();
    for (idx_off, column) in access_columns.iter().copied().enumerate() {
        if let Some(key_off) = runtime
            .inner_join_keys
            .iter()
            .position(|key| key.unique_id == column.unique_id)
        {
            key_off2_idx_off[key_off] = i64::try_from(idx_off).unwrap_or(i64::MAX);
            matched_runtime_key = true;
        } else if !fixed.contains(&column.unique_id) {
            first_unmatched_index_offset = idx_off;
            break;
        }
    }
    let compare_filters = matched_runtime_key
        .then(|| access_columns.get(first_unmatched_index_offset).copied())
        .flatten()
        .and_then(|target_col| {
            index_join_compare_filters(
                ds,
                runtime,
                target_col,
                first_unmatched_index_offset,
                &idx_col_lens,
            )
        });
    crate::task::IndexJoinInfo {
        table_id: ds.physical_table_id,
        index_id: match path {
            crate::access_path::PossiblePath::Index { index } => {
                ds.indexes.get(*index).map(|index| index.id)
            }
            crate::access_path::PossiblePath::Table { .. } => None,
        },
        ranges,
        idx_col_lens,
        key_off2_idx_off,
        compare_filters,
    }
}

/// Go `indexJoinPathBuildColManager`: comparisons against the first index
/// column after the equality prefix become one retained per-outer-row range
/// manager. The expression on the other side must depend on the outer schema
/// and must not reference the inner data source.
fn index_join_compare_filters(
    ds: &crate::logical::DataSource,
    runtime: &crate::physical_property::IndexJoinRuntimeProp,
    target_col: &tidb_expr::column::Column,
    target_index_offset: usize,
    idx_col_lens: &[i64],
) -> Option<crate::physical::IndexJoinCompareFilters> {
    let inner_schema = ds.base.base.schema()?;
    let mut ops = Vec::new();
    let mut args = Vec::new();
    for condition in &runtime.other_conditions {
        let tidb_expr::expression::Expression::ScalarFunction(function) = condition else {
            continue;
        };
        let [left, right] = function.args.as_slice() else {
            continue;
        };
        let name = function.func_name.lowercase();
        let (op, argument) = if left
            .as_column()
            .is_some_and(|column| column.unique_id == target_col.unique_id)
        {
            let op = match name {
                "ge" => crate::physical::IndexJoinCompareOp::Ge,
                "gt" => crate::physical::IndexJoinCompareOp::Gt,
                "lt" => crate::physical::IndexJoinCompareOp::Lt,
                "le" => crate::physical::IndexJoinCompareOp::Le,
                _ => continue,
            };
            (op, right)
        } else if right
            .as_column()
            .is_some_and(|column| column.unique_id == target_col.unique_id)
        {
            let op = match name {
                "ge" => crate::physical::IndexJoinCompareOp::Le,
                "gt" => crate::physical::IndexJoinCompareOp::Lt,
                "lt" => crate::physical::IndexJoinCompareOp::Gt,
                "le" => crate::physical::IndexJoinCompareOp::Ge,
                _ => continue,
            };
            (op, left)
        } else {
            continue;
        };
        let affected = tidb_expr::simple_expr::extract_columns(argument);
        if affected.is_empty() || affected.iter().any(|column| inner_schema.contains(column)) {
            continue;
        }
        ops.push(op);
        args.push(argument.clone());
    }
    (!ops.is_empty()).then(|| crate::physical::IndexJoinCompareFilters {
        target_col: target_col.clone(),
        target_index_offset,
        col_length: idx_col_lens
            .get(target_index_offset)
            .copied()
            .unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
        ops,
        args,
    })
}

fn find_best_task_4_logical_data_source(
    ds: &crate::logical::DataSource,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    // Go `findBestTask4LogicalDataSource` handles `CanAddEnforcer` inside
    // the DataSource override, before entering the path loop.  First price
    // a path that satisfies the requested order directly.  Then clear the
    // order, price the best unordered path, and enforce the original
    // property above it.  This branch is what makes
    // `getEnforcedStreamAggs`' sorted child property executable when no
    // access path naturally provides the group order.
    //
    // IndexJoinProp takes Go's earlier, dedicated runtime-range return and
    // therefore never enters the enforcer branch.
    if prop.can_add_enforcer && prop.index_join_prop.is_none() {
        let mut direct_prop = prop.clone();
        direct_prop.can_add_enforcer = false;
        let direct = find_best_task_4_logical_data_source_without_enforcer(ds, &direct_prop, ctx)?;

        let mut unordered_prop = prop.clone();
        unordered_prop.can_add_enforcer = false;
        unordered_prop.sort_items.clear();
        let unordered =
            find_best_task_4_logical_data_source_without_enforcer(ds, &unordered_prop, ctx)?;
        let enforced = enforce_property(prop, unordered, ctx.allocator)?;
        if compare_task_cost(ctx.coster, &direct, &enforced)? {
            return Ok(direct);
        }
        return Ok(enforced);
    }

    find_best_task_4_logical_data_source_without_enforcer(ds, prop, ctx)
}

fn find_best_task_4_logical_data_source_without_enforcer(
    ds: &crate::logical::DataSource,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    // Go's DataSource findBestTask serves both COP property kinds. The
    // single-read property admits table and covering-index scans; the
    // multi-read property admits only a non-covering index path whose COP
    // task still owns both the index and table halves. Aggregation needs the
    // latter so `attach2Task4PhysicalHashAgg` can put its partial stage on
    // the lookup's table half before converting the task to root.
    let cop_answer = match prop.task_tp {
        TaskType::Root => false,
        TaskType::CopSingleRead | TaskType::CopMultiRead => true,
        _ => return Ok(Task::invalid_task()),
    };
    let cop_multi_read = prop.task_tp == TaskType::CopMultiRead;
    if let Some(dual) = try_to_get_dual_task(ds, ctx) {
        return Ok(dual);
    }
    // Per-path admission, the shape of Go's candidate loop: an empty
    // property admits every path unordered; a required order admits the
    // paths that MATCH it — the int-handle arm for the table path
    // (`matchProperty:1082`), the basic prefix arm for an index
    // (`matchProperty:1095`) — and the admitted scan carries
    // `KeepOrder`/`Desc` (`convertToTableScan:2834`).
    let ordered = !prop.is_sort_item_empty();
    let desc = ordered && prop.sort_items[0].desc;
    let mut best = Task::invalid_task();
    for path in &ds.enumerated_paths {
        if let Some(runtime) = &prop.index_join_prop {
            if !path_matches_index_join_runtime(ds, path, runtime) {
                continue;
            }
        }
        let cop = match path {
            crate::access_path::PossiblePath::Table { primary_index, .. } => {
                if cop_multi_read {
                    continue;
                }
                let keep_order = ordered;
                if keep_order && !table_path_matches_order(ds, prop) {
                    continue;
                }
                let mut base = crate::physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    "TableScan",
                    ds.base.base.query_block_offset(),
                );
                base.base.set_schema(ds.base.base.schema().cloned());
                // Go `buildTableRange`: integer handles use the table ranger;
                // clustered common handles use the primary-index ranger over
                // the complete handle tuple.
                let handle_column = ds
                    .base
                    .base
                    .schema()
                    .and_then(|schema| ds.get_pk_is_handle_col(schema));
                let handle_type = handle_column
                    .and_then(|col| col.ret_type.clone())
                    .unwrap_or_else(|| {
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                    });
                let common_handle = primary_index.and_then(|index| ds.indexes.get(index));
                let common_columns = common_handle.map_or_else(Vec::new, |index| {
                    index
                        .columns
                        .iter()
                        .filter_map(|column| ds.schema_column_for_index_column(column).cloned())
                        .collect::<Vec<_>>()
                });
                let common_lengths = common_handle.map_or_else(Vec::new, |index| {
                    index
                        .columns
                        .iter()
                        .map(|column| column.length)
                        .collect::<Vec<_>>()
                });
                let common_detach = common_handle.and_then(|index| {
                    (!ds.pushed_down_conds.is_empty()
                        && common_columns.len() == index.columns.len())
                    .then(|| {
                        crate::ranger::detacher::detach_cond_and_build_range_for_index(
                            &ds.pushed_down_conds,
                            &common_columns,
                            &common_lengths,
                            0,
                        )
                        .ok()
                    })
                    .flatten()
                });
                let int_access_conditions = handle_column.map_or_else(Vec::new, |handle| {
                    crate::ranger::detacher::extract_access_conditions_for_column(
                        &ds.pushed_down_conds,
                        handle,
                        true,
                    )
                });
                let ranges = if common_handle.is_some() {
                    common_detach
                        .as_ref()
                        .map_or_else(crate::ranger::points::full_range, |result| {
                            result.ranges.clone()
                        })
                } else if int_access_conditions.is_empty() {
                    crate::ranger::points::full_int_range(handle_type.is_unsigned())
                } else {
                    match crate::ranger::ranger::build_table_range(
                        &int_access_conditions,
                        &handle_type,
                        0,
                    ) {
                        Ok(result) => result.ranges,
                        Err(_) => crate::ranger::points::full_int_range(handle_type.is_unsigned()),
                    }
                };
                let table_access_conds = common_detach.as_ref().map_or_else(
                    || int_access_conditions.clone(),
                    |result| result.access_conds.clone(),
                );
                let table_filters = if common_handle.is_some() {
                    common_detach.as_ref().map_or_else(
                        || ds.pushed_down_conds.clone(),
                        |result| result.remained_conds.clone(),
                    )
                } else {
                    crate::ranger::detacher::remove_conditions(
                        &ds.pushed_down_conds,
                        &table_access_conds,
                    )
                };
                let table_stats = ds
                    .table_stats
                    .clone()
                    .or_else(|| ds.base.base.stats_info().cloned());
                let mut count_after_access = table_stats.as_ref().map(|stats| stats.row_count());
                if !table_access_conds.is_empty() {
                    count_after_access = ds.table_path_count_after_access.or_else(|| {
                        let base_stats = table_stats.as_ref()?;
                        Some(if common_handle.is_some() {
                            crate::ranger::stats_bridge::pseudo_count_by_ranges(
                                &ranges,
                                base_stats.row_count(),
                            )
                        } else {
                            crate::ranger::stats_bridge::pseudo_count_by_int_ranges(
                                &ranges,
                                base_stats.row_count(),
                                handle_type.is_unsigned(),
                            )
                        })
                    });
                }
                if let (Some(count), Some(ds_stats), Some(base_stats)) = (
                    count_after_access.as_mut(),
                    ds.base.base.stats_info(),
                    table_stats.as_ref(),
                ) {
                    if *count + crate::cost_factors::TOLERANCE_FACTOR < ds_stats.row_count() {
                        *count = (ds_stats.row_count() / crate::cost_factors::SELECTION_FACTOR)
                            .min(base_stats.row_count());
                    }
                }
                let mut stats = table_stats.as_ref().map(|stats| {
                    stats.scale_by_expect_cnt(
                        count_after_access.unwrap_or_else(|| stats.row_count()),
                        ctx.skew_ratio,
                    )
                });
                if prop.index_join_prop.is_none() {
                    if let (Some(path_stats), Some(ds_stats), Some(table_stats)) = (
                        stats.as_ref(),
                        ds.base.base.stats_info(),
                        ds.table_stats.as_ref(),
                    ) {
                        let count_after_access = path_stats.row_count();
                        let original_row_count = ds_stats.row_count();
                        if prop.expected_cnt + crate::cost_factors::TOLERANCE_FACTOR
                            < original_row_count
                            || (keep_order
                                && original_row_count.min(prop.expected_cnt) < count_after_access
                                && !table_access_conds.is_empty())
                        {
                            let mut row_count = count_after_access;
                            if prop.expected_cnt < original_row_count {
                                let selectivity = original_row_count / count_after_access;
                                row_count = count_after_access.min(prop.expected_cnt / selectivity);
                            }
                            if keep_order && count_after_access > row_count {
                                let ratio = ctx.ordering_index_selectivity_ratio;
                                if ratio > 0.0 {
                                    row_count += (count_after_access - row_count).max(0.0) * ratio;
                                }
                            }
                            stats =
                                Some(table_stats.scale_by_expect_cnt(row_count, ctx.skew_ratio));
                        }
                    }
                }
                if let Some(runtime) = &prop.index_join_prop {
                    stats = Some(crate::stats_info::StatsInfo::new(
                        runtime.avg_inner_row_count,
                        [],
                    ));
                }
                base.base.set_stats(stats.clone());
                let scan_kind = if ranges
                    .iter()
                    .all(|range| range.is_full_range(handle_type.is_unsigned()))
                {
                    crate::access_path::ResolvedTableScanKind::Full
                } else {
                    crate::access_path::ResolvedTableScanKind::Range
                };
                let scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                    base,
                    table_id: ds.physical_table_id,
                    table_as_name: ds.table_as_name.clone(),
                    cost_columns: ds.table_columns.clone(),
                    store_type: crate::physical_table_reader::StoreType::TiKv,
                    keep_order,
                    desc,
                    ranges: ranges.clone(),
                    range_rebuild: if table_access_conds.is_empty() {
                        None
                    } else if common_handle.is_some() {
                        Some(
                            crate::physical_plan_cache::TableRangeRebuild::common_handle(
                                table_access_conds.clone(),
                                common_columns,
                                common_lengths,
                            ),
                        )
                    } else {
                        Some(crate::physical_plan_cache::TableRangeRebuild::int_handle(
                            table_access_conds.clone(),
                            handle_type.clone(),
                            handle_type.is_unsigned(),
                        ))
                    },
                    table_scan_penalty: ds.table_scan_penalty,
                    tikv_pushdown: None,
                    resolved_descriptor: Some(crate::access_path::ResolvedTableDescriptor::new(
                        ds.physical_table_id,
                        common_handle.is_some(),
                        scan_kind,
                        crate::access_path::TableScanExplainIdSuffix::IncludePlanId,
                    )),
                });
                let table_plan = if table_filters.is_empty() {
                    scan
                } else {
                    // Go `addPushedDownSelection`: access cardinality belongs
                    // to the scan; the DataSource's post-filter cardinality
                    // belongs to the Selection above it.
                    let mut selection_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        "Selection",
                        ds.base.base.query_block_offset(),
                    );
                    selection_base
                        .base
                        .set_schema(ds.base.base.schema().cloned());
                    selection_base
                        .base
                        .set_stats(ds.base.base.stats_info().cloned());
                    selection_base.set_children(vec![scan]);
                    PhysicalPlan::Selection(crate::physical::PhysicalSelection {
                        base: selection_base,
                        conditions: table_filters,
                        from_data_source: true,
                    })
                };
                Task::Cop(crate::task::CopTask {
                    table_plan: Some(Box::new(table_plan)),
                    index_plan_finished: true,
                    keep_order,
                    expect_cnt: prop.expected_cnt as u64,
                    index_join_info: prop
                        .index_join_prop
                        .as_ref()
                        .map(|runtime| index_join_feedback(ds, path, runtime, ranges.clone())),
                    ..crate::task::CopTask::default()
                })
            }
            crate::access_path::PossiblePath::Index { index } => {
                let Some(source_index) = ds.indexes.get(*index) else {
                    continue;
                };
                let keep_order = ordered;
                if keep_order && !index_path_matches_order(ds, source_index, prop) {
                    continue;
                }
                // `convertToIndexScan`: a path that is NOT a single scan
                // reads the table rows back through an IndexLookUp double
                // read (`BuildIndexLookUpTask` at conversion) — the cop task
                // carries BOTH halves, exactly Go's shape.
                let single_scan = index_path_is_single_scan(ds, source_index);
                // The two COP property kinds are disjoint: a covering index
                // is single-read, while a lookup is multi-read.
                if (prop.task_tp == TaskType::CopSingleRead && !single_scan)
                    || (cop_multi_read && single_scan)
                {
                    continue;
                }
                let mut base = crate::physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    "IndexScan",
                    ds.base.base.query_block_offset(),
                );
                base.base.set_schema(ds.base.base.schema().cloned());
                // Go `detachCondAndBuildRangeForPath`: the index columns
                // (schema columns at the index's offsets) detach the pushed
                // conditions into this path's ranges.
                // Go retains the usable leading `IdxCols` after logical
                // column pruning. A missing later index column does not
                // invalidate ranges on an earlier prefix; only the first
                // unresolved position ends that prefix.
                let mut resolved_index_prefix = source_index
                    .columns
                    .iter()
                    .map_while(|index_column| {
                        ds.schema_column_for_index_column(index_column)
                            .cloned()
                            .map(|column| (column, index_column.length))
                    })
                    .collect::<Vec<_>>();
                let declared_index_prefix_complete =
                    resolved_index_prefix.len() == source_index.columns.len();
                // Go `fillIndexPath` appends the signed integer table handle
                // to every complete non-unique secondary-index prefix.  It
                // is a real trailing execution key part, so predicates on
                // the handle participate in range detachment.  Unique and
                // primary indexes do not append it, an unsigned handle is
                // deliberately excluded because its encoded ordering is
                // signed, and an index that already declares the handle must
                // not add it twice.
                if !source_index.unique
                    && !source_index.primary
                    && declared_index_prefix_complete
                    && ds.handle_is_int
                {
                    if let Some(handle) = ds.handle_cols.first().filter(|handle| {
                        !handle.ret_type.as_ref().is_some_and(|ty| ty.is_unsigned())
                            && !resolved_index_prefix
                                .iter()
                                .any(|(column, _)| column.unique_id == handle.unique_id)
                    }) {
                        resolved_index_prefix
                            .push((handle.clone(), tidb_datatype::UNSPECIFIED_LENGTH));
                    }
                }
                let index_cols = resolved_index_prefix
                    .iter()
                    .map(|(column, _)| column.clone())
                    .collect::<Vec<_>>();
                let index_lengths = resolved_index_prefix
                    .iter()
                    .map(|(_, length)| *length)
                    .collect::<Vec<_>>();
                let detach = if ds.pushed_down_conds.is_empty() || index_cols.is_empty() {
                    None
                } else {
                    crate::ranger::detacher::detach_cond_and_build_range_for_index(
                        &ds.pushed_down_conds,
                        &index_cols,
                        &index_lengths,
                        0,
                    )
                    .ok()
                };
                let ranges = detach
                    .as_ref()
                    .map_or_else(crate::ranger::points::full_range, |result| {
                        result.ranges.clone()
                    });
                let remained_conds = detach.as_ref().map_or_else(
                    || ds.pushed_down_conds.clone(),
                    |result| result.remained_conds.clone(),
                );
                let table_stats = ds
                    .table_stats
                    .clone()
                    .or_else(|| ds.base.base.stats_info().cloned());
                let mut count_after_access = table_stats.as_ref().map(|stats| stats.row_count());
                if detach.is_some() {
                    count_after_access = ds
                        .index_path_count_after_access
                        .get(&source_index.id)
                        .copied()
                        .or_else(|| {
                            let base_stats = table_stats.as_ref()?;
                            // Go `deriveIndexPathStats` trims the signed handle
                            // appended by `fillIndexPath` back to the declared
                            // index columns before estimating CountAfterAccess.
                            // The handle remains in `ranges` for execution.
                            let estimate_ranges =
                                if resolved_index_prefix.len() > source_index.columns.len() {
                                    ranges
                                        .iter()
                                        .cloned()
                                        .map(|mut range| {
                                            range.low_val.truncate(source_index.columns.len());
                                            range.high_val.truncate(source_index.columns.len());
                                            range.collators.truncate(source_index.columns.len());
                                            range
                                        })
                                        .collect()
                                } else {
                                    ranges.clone()
                                };
                            Some(crate::ranger::stats_bridge::pseudo_count_by_ranges(
                                &estimate_ranges,
                                base_stats.row_count(),
                            ))
                        });
                }
                if let (Some(count), Some(ds_stats), Some(base_stats)) = (
                    count_after_access.as_mut(),
                    ds.base.base.stats_info(),
                    table_stats.as_ref(),
                ) {
                    if *count + crate::cost_factors::TOLERANCE_FACTOR < ds_stats.row_count() {
                        *count = (ds_stats.row_count() / crate::cost_factors::SELECTION_FACTOR)
                            .min(base_stats.row_count());
                    }
                }
                // Go `GetOriginalPhysicalIndexScan` calls
                // `AdjustRowCountForIndexScanByLimit` before pricing the
                // scan. With pseudo statistics its cross-estimation arm
                // reduces to the uniform estimate below; residual filters on
                // an ordering index then add the session's risk ratio. The
                // missing adjustment made an ordered LIMIT price the whole
                // index, so Rust chose `TopN -> TableReader` where Go chooses
                // the bounded IndexLookUp path.
                if let (Some(count), Some(ds_stats)) =
                    (count_after_access.as_mut(), ds.base.base.stats_info())
                {
                    if (keep_order || prop.is_sort_item_empty())
                        && prop.expected_cnt < ds_stats.row_count()
                        && *count > 0.0
                    {
                        let selectivity = ds_stats.row_count() / *count;
                        let mut adjusted = (*count).min(prop.expected_cnt / selectivity);
                        if keep_order && !remained_conds.is_empty() && *count > adjusted {
                            let ratio = ctx.ordering_index_selectivity_ratio;
                            if ratio > 0.0 {
                                adjusted += (*count - adjusted).max(0.0) * ratio;
                            }
                        }
                        *count = adjusted;
                    }
                }
                let mut stats = table_stats.as_ref().map(|stats| {
                    stats.scale_by_expect_cnt(
                        count_after_access.unwrap_or_else(|| stats.row_count()),
                        ctx.skew_ratio,
                    )
                });
                if let Some(runtime) = &prop.index_join_prop {
                    stats = Some(crate::stats_info::StatsInfo::new(
                        runtime.avg_inner_row_count,
                        [],
                    ));
                }
                base.base.set_stats(stats.clone());
                let mut cost_columns = source_index
                    .columns
                    .iter()
                    .filter_map(|column| ds.table_columns.get(column.offset).cloned())
                    .collect::<Vec<_>>();
                // Go `convertToIndexScan` appends the table handle columns to
                // the physical index schema even when the same logical
                // columns already occur in the secondary index.  Cost model
                // v2 prices that physical schema verbatim: for a four-column
                // secondary index over a three-column common handle this is
                // seven INT slots, not the four-column set-union.  Retaining
                // the duplicates is therefore both an execution-schema and
                // a plan-cost contract, not an index-width heuristic.
                cost_columns.extend(ds.common_handle_cols.iter().cloned());
                cost_columns.extend(ds.handle_cols.iter().cloned());
                let scan = PhysicalPlan::IndexScan(crate::physical::PhysicalIndexScan {
                    base,
                    table_id: ds.physical_table_id,
                    table_as_name: ds.table_as_name.clone(),
                    cost_columns,
                    index_id: source_index.id,
                    index_name: source_index.name.clone(),
                    keep_order,
                    desc,
                    ranges: ranges.clone(),
                    // Go retains `AccessCondition` on PhysicalIndexScan, not
                    // every pushed predicate. Rebuilding the latter would
                    // feed residual filters back into the ranger and make a
                    // safe parameter change look uncacheable.
                    range_rebuild: declared_index_prefix_complete
                        .then_some(detach.as_ref())
                        .flatten()
                        .filter(|result| !result.access_conds.is_empty())
                        .map(|result| {
                            crate::physical_plan_cache::IndexRangeRebuild::new(
                                result.access_conds.clone(),
                                index_cols.clone(),
                                index_lengths.clone(),
                            )
                        }),
                    covering_ranges: Vec::new(),
                    tikv_pushdown: None,
                });
                let fully_covered_columns = source_index
                    .columns
                    .iter()
                    .filter(|column| column.length < 0)
                    .filter_map(|column| {
                        ds.schema_column_for_index_column(column)
                            .map(|column| column.unique_id)
                    })
                    .collect::<std::collections::BTreeSet<_>>();
                let (index_filters, table_filters): (Vec<_>, Vec<_>) =
                    remained_conds.into_iter().partition(|condition| {
                        tidb_expr::simple_expr::extract_columns(condition)
                            .iter()
                            .all(|column| fully_covered_columns.contains(&column.unique_id))
                    });
                let index_plan = if index_filters.is_empty() {
                    scan
                } else {
                    let mut selection_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        "Selection",
                        ds.base.base.query_block_offset(),
                    );
                    selection_base
                        .base
                        .set_schema(ds.base.base.schema().cloned());
                    selection_base.base.set_stats(if table_filters.is_empty() {
                        ds.base.base.stats_info().cloned()
                    } else {
                        stats.clone()
                    });
                    selection_base.set_children(vec![scan]);
                    PhysicalPlan::Selection(crate::physical::PhysicalSelection {
                        base: selection_base,
                        conditions: index_filters,
                        from_data_source: true,
                    })
                };
                let (table_side, root_task_conds) = if single_scan {
                    // A condition that is not covered by the index key (for
                    // example an unsigned integer handle predicate) remains
                    // above the covering IndexReader as Go's
                    // `CopTask.RootTaskConds`; it must never disappear merely
                    // because no table probe is required.
                    (None, table_filters)
                } else {
                    // Go `convertToIndexScan` builds the lookup's table side
                    // over the source's schema and stats.
                    let mut table_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        "TableScan",
                        ds.base.base.query_block_offset(),
                    );
                    table_base
                        .base
                        .set_stats(index_plan.base().base.stats_info().cloned());
                    table_base.base.set_schema(ds.base.base.schema().cloned());
                    let table_scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                        base: table_base,
                        table_id: ds.physical_table_id,
                        table_as_name: ds.table_as_name.clone(),
                        cost_columns: ds.table_columns.clone(),
                        store_type: crate::physical_table_reader::StoreType::TiKv,
                        keep_order: false,
                        desc: false,
                        // The lookup's table side reads BY HANDLE from
                        // the index rows, not by its own ranges.
                        ranges: crate::ranger::types::Ranges::new(),
                        range_rebuild: None,
                        table_scan_penalty: ds.table_scan_penalty,
                        tikv_pushdown: None,
                        resolved_descriptor: Some(
                            crate::access_path::ResolvedTableDescriptor::new(
                                ds.physical_table_id,
                                !ds.common_handle_cols.is_empty(),
                                crate::access_path::ResolvedTableScanKind::RowId,
                                crate::access_path::TableScanExplainIdSuffix::IncludePlanId,
                            ),
                        ),
                    });
                    let table_plan = if table_filters.is_empty() {
                        table_scan
                    } else {
                        let mut selection_base = crate::physical::BasePhysicalPlan::new(
                            ctx.allocator,
                            "Selection",
                            ds.base.base.query_block_offset(),
                        );
                        selection_base
                            .base
                            .set_schema(ds.base.base.schema().cloned());
                        selection_base
                            .base
                            .set_stats(ds.base.base.stats_info().cloned());
                        selection_base.set_children(vec![table_scan]);
                        PhysicalPlan::Selection(crate::physical::PhysicalSelection {
                            base: selection_base,
                            conditions: table_filters,
                            from_data_source: true,
                        })
                    };
                    (Some(Box::new(table_plan)), Vec::new())
                };
                Task::Cop(crate::task::CopTask {
                    index_plan: Some(Box::new(index_plan)),
                    table_plan: table_side,
                    root_task_conds,
                    index_plan_finished: false,
                    keep_order,
                    expect_cnt: prop.expected_cnt as u64,
                    index_join_info: prop
                        .index_join_prop
                        .as_ref()
                        .map(|runtime| index_join_feedback(ds, path, runtime, ranges.clone())),
                    ..crate::task::CopTask::default()
                })
            }
        };
        let cur = if cop_answer {
            cop
        } else {
            cop.convert_to_root_task(ctx.allocator)?
        };
        if best.invalid() || compare_task_cost(ctx.coster, &cur, &best)? {
            best = cur;
        }
    }
    Ok(best)
}

#[derive(Default)]
struct EnumerateState {
    topn_cop_exists: bool,
    limit_cop_exists: bool,
}

/// Go `enumeratePhysicalPlans4Task` + helper (`find_best_task.go:112,156`):
/// preserve preference slices, hint priority, and the normal cop preference
/// for pushed TopN/Limit candidates while comparing peers by cost.
fn enumerate_physical_plans_4_task(
    plan: &LogicalPlan,
    physical_plans_slice: &[Vec<PhysicalPlan>],
    prop: &PhysicalProperty,
    add_enforcer: bool,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    if physical_plans_slice.is_empty() {
        return Ok(Task::invalid_task());
    }
    let mut outer_normal_task = Task::invalid_task();
    let mut outer_hint_task = Task::invalid_task();
    for ops in physical_plans_slice {
        let mut normal_iter_task = Task::invalid_task();
        let mut normal_prefer_task = Task::invalid_task();
        let mut hint_task = Task::invalid_task();
        let mut state = EnumerateState::default();
        for pp in ops {
            let child_len = plan.children().len();
            let mut child_tasks = Vec::with_capacity(child_len);
            for (i, child) in plan.children().iter().enumerate() {
                let Some(child_prop) = pp.base().child_req_prop(i) else {
                    break;
                };
                let child_prop = child_prop.clone();
                let child_task = find_best_task(child, &child_prop, ctx)?;
                if child_task.invalid() || !task_type_satisfied(&child_prop, &child_task) {
                    break;
                }
                child_tasks.push(child_task);
            }
            // "This check makes sure that there is no invalid child task."
            if child_tasks.len() != child_len {
                continue;
            }
            let child_is_cop = matches!(child_tasks.first(), Some(Task::Cop(_)));
            let child_is_root = matches!(child_tasks.first(), Some(Task::Root(_)));
            let child_is_mpp = matches!(child_tasks.first(), Some(Task::Mpp(_)));
            let hint_applicable = logical_hint_applies(plan, pp, child_is_cop);
            let normally_preferred = normal_preference_applies(
                plan,
                pp,
                child_is_cop,
                child_is_root,
                child_is_mpp,
                ctx.limit_push_down_threshold,
                &mut state,
            );
            let mut cur_task = match attach2_task(
                pp.clone_shallow(),
                child_tasks,
                ctx.column_ids,
                ctx.allocator,
            ) {
                Ok(task) => task,
                // An unported attach body refuses; Go has no such arm, so a
                // refusal must SURFACE rather than silently skip a
                // candidate Go would have priced.
                Err(error) => return Err(error),
            };
            if cur_task.invalid() {
                continue;
            }
            if !matches!(cur_task, Task::Root(_)) && prop.task_tp == TaskType::Root {
                cur_task = cur_task.convert_to_root_task(ctx.allocator)?;
            }
            if add_enforcer {
                cur_task = enforce_property(prop, cur_task, ctx.allocator)?;
            }
            if hint_applicable {
                if hint_task.invalid() || compare_task_cost(ctx.coster, &cur_task, &hint_task)? {
                    hint_task = cur_task;
                }
            } else if hint_task.invalid() && normally_preferred {
                if normal_prefer_task.invalid()
                    || compare_task_cost(ctx.coster, &cur_task, &normal_prefer_task)?
                {
                    normal_prefer_task = cur_task;
                }
            } else if hint_task.invalid() && normal_prefer_task.invalid() {
                if normal_iter_task.invalid()
                    || compare_task_cost(ctx.coster, &cur_task, &normal_iter_task)?
                {
                    normal_iter_task = cur_task;
                }
            }
        }
        let (slice_task, slice_is_hint) = if !hint_task.invalid() {
            (hint_task, true)
        } else if !normal_prefer_task.invalid() {
            (normal_prefer_task, false)
        } else {
            (normal_iter_task, false)
        };
        if slice_is_hint {
            if outer_hint_task.invalid()
                || compare_task_cost(ctx.coster, &slice_task, &outer_hint_task)?
            {
                outer_hint_task = slice_task;
            }
        } else if outer_normal_task.invalid()
            || compare_task_cost(ctx.coster, &slice_task, &outer_normal_task)?
        {
            outer_normal_task = slice_task;
        }
    }
    if outer_hint_task.invalid() {
        Ok(outer_normal_task)
    } else {
        Ok(outer_hint_task)
    }
}

fn task_type_satisfied(required: &PhysicalProperty, task: &Task) -> bool {
    match required.task_tp {
        TaskType::Root => matches!(task, Task::Root(_) | Task::Cop(_) | Task::Mpp(_)),
        TaskType::CopSingleRead | TaskType::CopMultiRead => matches!(task, Task::Cop(_)),
        TaskType::Mpp => matches!(task, Task::Mpp(_)),
        TaskType::Unknown(_) => false,
    }
}

fn logical_hint_applies(plan: &LogicalPlan, physical: &PhysicalPlan, child_is_cop: bool) -> bool {
    if logical_join_hint_applies(plan, physical) {
        return true;
    }
    match plan {
        LogicalPlan::TopN(topn) => topn.prefer_limit_to_cop && child_is_cop,
        LogicalPlan::Limit(limit) => limit.prefer_limit_to_cop && child_is_cop,
        LogicalPlan::Aggregation(aggregation) => aggregation.prefer_agg_to_cop && child_is_cop,
        _ => false,
    }
}

fn normal_preference_applies(
    plan: &LogicalPlan,
    physical: &PhysicalPlan,
    child_is_cop: bool,
    child_is_root: bool,
    child_is_mpp: bool,
    limit_push_down_threshold: u64,
    state: &mut EnumerateState,
) -> bool {
    let meets_threshold = match plan {
        LogicalPlan::Limit(_) => true,
        LogicalPlan::TopN(topn) => {
            matches!(physical, PhysicalPlan::Limit(_))
                || topn.count.saturating_add(topn.offset) <= limit_push_down_threshold
        }
        _ => false,
    };
    if !meets_threshold {
        return false;
    }
    let cop_exists = if matches!(physical, PhysicalPlan::TopN(_)) {
        &mut state.topn_cop_exists
    } else {
        &mut state.limit_cop_exists
    };
    if *cop_exists {
        return child_is_cop || child_is_mpp;
    }
    if child_is_cop {
        *cop_exists = true;
        return true;
    }
    child_is_root || child_is_mpp
}

/// Go `applyLogicalJoinHint`: a hint becomes applicable only after the
/// candidate and both child tasks have been built successfully.  A valid
/// hinted task outranks ordinary candidates regardless of cost; multiple
/// valid hinted tasks still compare by cost.
fn logical_join_hint_applies(plan: &LogicalPlan, physical: &PhysicalPlan) -> bool {
    use crate::plan_builder::from::join_hint_flags as hint;
    use crate::plan_cost_ver2::IndexJoinKind;

    let LogicalPlan::Join(join) = plan else {
        return false;
    };
    match physical {
        PhysicalPlan::MergeJoin(_) => join.prefer_any(&[hint::MERGE_JOIN]),
        PhysicalPlan::IndexJoin(index) => {
            let inner_is_left = index.inner_child_idx == 0;
            match index.kind {
                IndexJoinKind::IndexJoin => {
                    (inner_is_left && join.prefer_any(&[hint::LEFT_AS_INLJ_INNER]))
                        || (!inner_is_left && join.prefer_any(&[hint::RIGHT_AS_INLJ_INNER]))
                }
                IndexJoinKind::IndexHashJoin => {
                    (inner_is_left && join.prefer_any(&[hint::LEFT_AS_INLHJ_INNER]))
                        || (!inner_is_left && join.prefer_any(&[hint::RIGHT_AS_INLHJ_INNER]))
                }
                IndexJoinKind::IndexMergeJoin => {
                    (inner_is_left && join.prefer_any(&[hint::LEFT_AS_INLMJ_INNER]))
                        || (!inner_is_left && join.prefer_any(&[hint::RIGHT_AS_INLMJ_INNER]))
                }
            }
        }
        PhysicalPlan::HashJoin(hash) => {
            let mut force_left_to_build =
                join.prefer_any(&[hint::LEFT_AS_HJ_BUILD, hint::RIGHT_AS_HJ_PROBE]);
            let mut force_right_to_build =
                join.prefer_any(&[hint::RIGHT_AS_HJ_BUILD, hint::LEFT_AS_HJ_PROBE]);
            if force_left_to_build && force_right_to_build {
                force_left_to_build = false;
                force_right_to_build = false;
            }
            let hash_hint = join.prefer_any(&[hint::HASH_JOIN]);
            if hash_hint && !force_left_to_build && !force_right_to_build {
                return true;
            }
            (force_left_to_build && hash.inner_child_idx == 0)
                || (force_right_to_build && hash.inner_child_idx == 1)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    // Go's coverage for this loop is planner-integration bound (casetest
    // plans); these pin the transcreated control flow over a fixture coster.

    use super::*;
    use crate::logical::{BaseLogicalPlan, LogicalSelection, LogicalTableDual};
    use crate::physical_property::SortItem;
    use crate::stats_info::StatsInfo;

    struct CountCoster;
    impl TaskCoster for CountCoster {
        fn task_cost(&self, task: &Task) -> Result<f64, PlanError> {
            fn count(plan: &PhysicalPlan) -> f64 {
                1.0 + plan.children().iter().map(count).sum::<f64>()
            }
            Ok(task.plan().map_or(f64::MAX, count))
        }
    }

    fn dual(allocator: &PlanIdAllocator, rows: f64) -> LogicalPlan {
        let mut base = BaseLogicalPlan::new(allocator, LogicalTableDual::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        LogicalPlan::TableDual(LogicalTableDual::new(base, 1))
    }

    #[test]
    fn a_selection_over_a_dual_plans_end_to_end() {
        // The dispatcher's whole loop on the smallest real tree: exhaust the
        // selection, recurse into the dual (its own findBestTask override),
        // attach, and answer a root task.
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(10.0, [])));
        base.set_children(vec![dual(&allocator, 10.0)]);
        let selection = LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()));

        let task =
            find_best_task(&selection, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        assert!(matches!(
            plan.children().first(),
            Some(PhysicalPlan::TableDual(_))
        ));
    }

    #[test]
    fn a_one_row_dual_satisfies_the_order_without_a_sort() {
        // The fits-prop pass wins WITHOUT an enforcer: a Selection passes
        // the required order down (`CloneEssentialFields` keeps SortItems)
        // and a 1-row dual satisfies any order vacuously
        // (`findBestTask4LogicalTableDual`), so Go's cheaper sort-free plan
        // is the answer even with `CanAddEnforcer` set.
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(10.0, [])));
        base.set_children(vec![dual(&allocator, 1.0)]);
        let selection = LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()));

        let prop = PhysicalProperty {
            sort_items: vec![SortItem::new(7, false)],
            can_add_enforcer: true,
            ..PhysicalProperty::default()
        };
        let task = find_best_task(&selection, &prop, &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(
            matches!(plan, PhysicalPlan::Selection(_)),
            "no Sort: the order rode down to the 1-row dual, got {plan:?}"
        );
    }

    #[test]
    fn an_unsatisfiable_order_is_enforced_with_a_sort_on_top() {
        // A CTE table refuses EVERY required order
        // (`findBestTask4LogicalCTETable`), so the fits-prop pass dies at
        // the child and the ENFORCED branch — empty-property re-exhaust
        // plus `EnforceProperty` — produces the Sort that wins by validity.
        use crate::logical::LogicalCTETable;
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let cte = {
            let mut base = BaseLogicalPlan::new(&allocator, LogicalCTETable::TYPE, 0);
            base.base.set_stats(Some(StatsInfo::new(4.0, [])));
            LogicalPlan::CTETable(LogicalCTETable {
                base,
                seed_stat: None,
                name: "c".to_owned(),
                id_for_storage: 1,
                seed_schema: None,
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(4.0, [])));
        base.set_children(vec![cte]);
        let selection = LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()));

        let prop = PhysicalProperty {
            sort_items: vec![SortItem::new(7, false)],
            can_add_enforcer: true,
            ..PhysicalProperty::default()
        };
        let task = find_best_task(&selection, &prop, &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(
            matches!(plan, PhysicalPlan::Sort(_)),
            "the enforcer Sort tops the plan, got {plan:?}"
        );
        assert!(matches!(
            plan.children().first(),
            Some(PhysicalPlan::Selection(_))
        ));
        assert!(matches!(
            plan.children()[0].children().first(),
            Some(PhysicalPlan::CTETable(_))
        ));
    }

    #[test]
    fn a_data_source_plans_a_table_reader_through_the_dispatcher() {
        // The table-path slice end to end: DataSource -> TableScan in a cop
        // task -> convertToRootTaskImpl's reader -> the Selection attaches
        // above it. The first table query plannable start to finish.
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(50.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 42,
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(10.0, [])));
        base.set_children(vec![source]);
        let selection = LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()));

        let task =
            find_best_task(&selection, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!(
                "a TableReader under the selection, got {:?}",
                plan.children()
            );
        };
        let Some(PhysicalPlan::TableScan(scan)) = reader.table_plan.as_deref() else {
            panic!("the scan hangs off TablePlan");
        };
        assert_eq!(scan.table_id, 42);
    }

    #[test]
    fn a_hash_join_candidate_retains_the_sessions_concurrency() {
        // Go `NewPhysicalHashJoin` copies
        // `SessionVars.HashJoinConcurrency()` onto every candidate, and the
        // plan cost later reads the candidate field. This must not fall back
        // to the default after the shared physical tree is cached or cloned.
        use crate::logical::LogicalJoin;

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_hash_join_concurrency(13);
        let mut base = BaseLogicalPlan::new(&allocator, LogicalJoin::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(5.0, [])));
        base.set_children(vec![dual(&allocator, 10.0), dual(&allocator, 20.0)]);
        let join = LogicalPlan::Join(LogicalJoin {
            base,
            ..LogicalJoin::default()
        });

        let task = find_best_task(&join, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let Some(PhysicalPlan::HashJoin(hash_join)) = task.plan() else {
            panic!("a hash join candidate, got {:?}", task.plan());
        };
        assert_eq!(hash_join.concurrency, 13);
        let cloned = task.plan().expect("plan").clone_shallow();
        let PhysicalPlan::HashJoin(cloned) = cloned else {
            unreachable!();
        };
        assert_eq!(cloned.concurrency, 13, "cached-plan clones retain it");
    }

    #[test]
    fn a_false_pushed_constant_short_circuits_into_a_dual() {
        // `tryToGetDualTask` (`find_best_task.go:749`): WHERE FALSE never
        // touches a path.
        use crate::logical::DataSource;
        use tidb_datatype::Datum;
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(50.0, [])));
            LogicalPlan::DataSource(DataSource {
                base,
                pushed_down_conds: vec![Expression::Constant(Constant::new(
                    Datum::Int(0),
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                ))],
                ..DataSource::default()
            })
        };
        let task =
            find_best_task(&source, &PhysicalProperty::default(), &mut ctx).expect("answers");
        assert!(
            matches!(task.plan(), Some(PhysicalPlan::TableDual(_))),
            "the dual short-circuit, got {:?}",
            task.plan()
        );
    }

    #[test]
    fn a_handle_ordered_requirement_admits_a_keep_order_scan() {
        // `matchProperty`'s int-handle arm (`find_best_task.go:1082`): ONE
        // sort item on the pk-is-handle column admits the scan with
        // KeepOrder (and Desc for a descending item); any other order
        // refuses.
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::column::Column;

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = |ctx_alloc: &PlanIdAllocator| {
            let mut base = BaseLogicalPlan::new(ctx_alloc, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(50.0, [])));
            LogicalPlan::DataSource(DataSource {
                base,
                pk_is_handle: true,
                handle_is_int: true,
                handle_cols: vec![Column::new(9, FieldType::new(FieldTypeCode::LongLong))],
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };

        // ORDER BY pk DESC: admitted, KeepOrder + Desc.
        let prop = PhysicalProperty::new(TaskType::Root, &[9], true, f64::MAX, false);
        let task = find_best_task(&source(&allocator), &prop, &mut ctx).expect("plans");
        let Some(PhysicalPlan::TableReader(reader)) = task.plan() else {
            panic!("a TableReader, got {:?}", task.plan());
        };
        let Some(PhysicalPlan::TableScan(scan)) = reader.table_plan.as_deref() else {
            panic!("the scan hangs off TablePlan");
        };
        assert!(scan.keep_order && scan.desc);

        // ORDER BY a non-handle column: refused.
        let prop = PhysicalProperty::new(TaskType::Root, &[1], false, f64::MAX, false);
        let task = find_best_task(&source(&allocator), &prop, &mut ctx).expect("answers");
        assert!(task.invalid());
    }

    #[test]
    fn pushed_conditions_become_scan_ranges() {
        // The ranger wire-in: `WHERE pk > 5` on the int handle fills the
        // table scan's ranges with `(5, +inf]`; an indexed `b = 7` fills
        // the index scan's ranges with the point.
        use crate::access_path::PossiblePath;
        use crate::logical::data_source::DataSourceColumn;
        use crate::logical::DataSource;
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let pk = Column::new(11, {
            let mut ft = FieldType::new(FieldTypeCode::LongLong);
            ft.set_flags(ft.flags() | tidb_datatype::FieldTypeFlags::PRI_KEY);
            ft
        });
        let b = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let cmp = |name: &str, col: &Column, v: i64| {
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new(name),
                FieldType::new(FieldTypeCode::LongLong),
                vec![
                    Expression::Column(col.clone()),
                    Expression::Constant(Constant::new(
                        Datum::Int(v),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
            ))
        };
        let build = |conds: Vec<Expression>, paths: Vec<PossiblePath>| {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            let mut schema = Schema::default();
            schema.columns = vec![pk.clone(), b.clone()];
            base.base.set_schema(Some(schema));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 7,
                pk_is_handle: true,
                columns: vec![
                    DataSourceColumn {
                        id: 1,
                        name: "pk".to_owned(),
                        is_primary_key: true,
                    },
                    DataSourceColumn {
                        id: 2,
                        name: "b".to_owned(),
                        is_primary_key: false,
                    },
                ],
                pushed_down_conds: conds,
                enumerated_paths: paths,
                indexes: vec![SourceIndex {
                    id: 3,
                    name: "ib".to_owned(),
                    columns: vec![SourceIndexColumn {
                        name: "b".to_owned(),
                        offset: 1,
                        length: -1,
                    }],
                    ..SourceIndex::default()
                }],
                ..DataSource::default()
            })
        };

        // Table path with pk > 5.
        let source = build(
            vec![cmp("gt", &pk, 5)],
            vec![PossiblePath::Table {
                is_int_handle: true,
                primary_index: None,
            }],
        );
        let task = find_best_task(&source, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let Some(PhysicalPlan::TableReader(reader)) = task.plan() else {
            panic!("a TableReader, got {:?}", task.plan());
        };
        let scan = match reader.table_plan.as_deref() {
            Some(PhysicalPlan::TableScan(scan)) => scan,
            Some(PhysicalPlan::Selection(selection)) => {
                let Some(PhysicalPlan::TableScan(scan)) = selection.base.children().first() else {
                    panic!("the residual selection's scan");
                };
                scan
            }
            other => panic!("the scan, got {other:?}"),
        };
        assert_eq!(scan.ranges.len(), 1);
        assert_eq!(scan.ranges[0].to_display_string(), "(5,+inf]");
        assert!(!crate::ranger::types::has_full_range(&scan.ranges, false));
        // The pseudo CountAfterAccess: 100 rows / pseudoLessRate(3).
        let scanned = scan.base.base.stats_info().expect("stats").row_count();
        assert!((scanned - 100.0 / 3.0).abs() < 1e-9, "{scanned}");

        // A predicate on an ordinary column is residual to the table path.
        // It must not be interpreted as an integer-handle point/range merely
        // because it was pushed into the DataSource.
        let source = build(
            vec![cmp("eq", &b, 7)],
            vec![PossiblePath::Table {
                is_int_handle: true,
                primary_index: None,
            }],
        );
        let task = find_best_task(&source, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let Some(PhysicalPlan::TableReader(reader)) = task.plan() else {
            panic!("a TableReader, got {:?}", task.plan());
        };
        let scan = match reader.table_plan.as_deref() {
            Some(PhysicalPlan::TableScan(scan)) => scan,
            Some(PhysicalPlan::Selection(selection)) => {
                let Some(PhysicalPlan::TableScan(scan)) = selection.base.children().first() else {
                    panic!("the residual selection's scan");
                };
                scan
            }
            other => panic!("the scan, got {other:?}"),
        };
        assert!(crate::ranger::types::has_full_range(&scan.ranges, false));
        assert_eq!(
            scan.base.base.stats_info().expect("stats").row_count(),
            100.0,
            "a non-handle predicate must not scale table-range cardinality",
        );
        assert!(scan.range_rebuild.is_none());
    }

    #[test]
    fn a_non_covering_index_plans_the_lookup_double_read() {
        // `IsSingleScan` end to end: with the catalog's column list filled,
        // an index that lacks a schema column is NOT a single scan, so its
        // cop task carries BOTH halves and converts through
        // `BuildIndexLookUpTask` — while a covering index still plans the
        // plain IndexReader.
        use crate::access_path::PossiblePath;
        use crate::logical::data_source::DataSourceColumn;
        use crate::logical::DataSource;
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::schema::Schema;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let build = |index_columns: Vec<SourceIndexColumn>| {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(50.0, [])));
            let mut schema = Schema::default();
            schema.columns = vec![
                Column::new(11, FieldType::new(FieldTypeCode::LongLong)),
                Column::new(12, FieldType::new(FieldTypeCode::LongLong)),
            ];
            base.base.set_schema(Some(schema));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 7,
                columns: vec![
                    DataSourceColumn {
                        id: 1,
                        name: "a".to_owned(),
                        is_primary_key: false,
                    },
                    DataSourceColumn {
                        id: 2,
                        name: "b".to_owned(),
                        is_primary_key: false,
                    },
                ],
                enumerated_paths: vec![
                    PossiblePath::Table {
                        is_int_handle: true,
                        primary_index: None,
                    },
                    PossiblePath::Index { index: 0 },
                ],
                indexes: vec![SourceIndex {
                    id: 3,
                    name: "ib".to_owned(),
                    columns: index_columns,
                    ..SourceIndex::default()
                }],
                ..DataSource::default()
            })
        };
        let order_by_b = PhysicalProperty::new(TaskType::Root, &[12], false, f64::MAX, false);

        // Index on (b) alone: column `a` is uncovered — the double read
        // plans, its index side keeping the order and its table side reading
        // the rows back.
        let narrow = build(vec![SourceIndexColumn {
            name: "b".to_owned(),
            offset: 1,
            length: -1,
        }]);
        let task = find_best_task(&narrow, &order_by_b, &mut ctx).expect("plans");
        let Some(PhysicalPlan::IndexLookUpReader(lookup)) = task.plan() else {
            panic!("an IndexLookUpReader, got {:?}", task.plan());
        };
        assert!(lookup.keep_order, "the double read carries the order");
        assert!(
            matches!(lookup.index_plan.as_deref(), Some(PhysicalPlan::IndexScan(scan)) if scan.keep_order),
            "the index side keeps order"
        );
        assert!(
            matches!(
                lookup.table_plan.as_deref(),
                Some(PhysicalPlan::TableScan(_))
            ),
            "the table side reads rows back"
        );

        // Index on (b, a): covering, the reader plans as before.
        let covering = build(vec![
            SourceIndexColumn {
                name: "b".to_owned(),
                offset: 1,
                length: -1,
            },
            SourceIndexColumn {
                name: "a".to_owned(),
                offset: 0,
                length: -1,
            },
        ]);
        let task = find_best_task(&covering, &order_by_b, &mut ctx).expect("plans");
        assert!(
            matches!(task.plan(), Some(PhysicalPlan::IndexReader(_))),
            "got {:?}",
            task.plan()
        );

        // A PREFIX index column neither covers its own column
        // (`isIndexColsCoveringCol` requires full length) nor carries an
        // order (`isMatchProp` walks `FullIdxCols`): the ordered property
        // has no server at all.
        let prefix = build(vec![
            SourceIndexColumn {
                name: "b".to_owned(),
                offset: 1,
                length: 10,
            },
            SourceIndexColumn {
                name: "a".to_owned(),
                offset: 0,
                length: -1,
            },
        ]);
        let task = find_best_task(&prefix, &order_by_b, &mut ctx).expect("answers");
        assert!(task.invalid(), "a prefix column serves no order");
    }

    #[test]
    fn a_constant_index_prefix_is_skipped_when_matching_order() {
        use crate::logical::data_source::DataSourceColumn;
        use crate::logical::DataSource;
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let a = Column::new(11, FieldType::new(FieldTypeCode::LongLong));
        let b = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let mut base = BaseLogicalPlan::with_id(1, "DataSource", 0);
        base.base.set_stats(Some(StatsInfo::new(100.0, [])));
        base.base.set_schema(Some(Schema::new(vec![a.clone(), b])));
        let source = DataSource {
            base,
            columns: vec![
                DataSourceColumn {
                    id: 1,
                    name: "a".to_owned(),
                    is_primary_key: false,
                },
                DataSourceColumn {
                    id: 2,
                    name: "b".to_owned(),
                    is_primary_key: false,
                },
            ],
            pushed_down_conds: vec![Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("eq"),
                FieldType::new(FieldTypeCode::LongLong),
                vec![
                    Expression::Column(a),
                    Expression::Constant(Constant::new(
                        Datum::Int(1),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
            ))],
            indexes: vec![SourceIndex {
                columns: vec![
                    SourceIndexColumn {
                        name: "a".to_owned(),
                        offset: 0,
                        length: -1,
                    },
                    SourceIndexColumn {
                        name: "b".to_owned(),
                        offset: 1,
                        length: -1,
                    },
                ],
                ..SourceIndex::default()
            }],
            ..DataSource::default()
        };
        let order_by_b = PhysicalProperty::new(TaskType::Root, &[12], false, f64::MAX, false);

        assert!(index_path_matches_order(
            &source,
            &source.indexes[0],
            &order_by_b
        ));
    }

    #[test]
    fn a_constant_common_handle_prefix_is_skipped_when_matching_order() {
        use crate::logical::DataSource;
        use tidb_datatype::{Datum, FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let warehouse = Column::new(11, FieldType::new(FieldTypeCode::LongLong));
        let district = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let mut base = BaseLogicalPlan::with_id(1, "DataSource", 0);
        base.base
            .set_schema(Some(Schema::new(vec![warehouse.clone(), district.clone()])));
        let source = DataSource {
            base,
            pushed_down_conds: vec![Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("eq"),
                FieldType::new(FieldTypeCode::LongLong),
                vec![
                    Expression::Column(warehouse.clone()),
                    Expression::Constant(Constant::new(
                        Datum::Int(1),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
            ))],
            common_handle_cols: vec![warehouse, district],
            common_handle_lens: vec![UNSPECIFIED_LENGTH; 2],
            ..DataSource::default()
        };
        let order_by_district =
            PhysicalProperty::new(TaskType::Root, &[12], false, f64::MAX, false);

        assert!(table_path_matches_order(&source, &order_by_district));
    }

    #[test]
    fn an_index_join_cannot_probe_only_a_later_common_handle_column() {
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use crate::physical_property::IndexJoinRuntimeProp;
        use tidb_datatype::{FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
        use tidb_expr::column::Column;

        let part = Column::new(11, FieldType::new(FieldTypeCode::LongLong));
        let supplier = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let source = DataSource {
            handle_cols: vec![part.clone(), supplier.clone()],
            handle_is_int: false,
            common_handle_cols: vec![part, supplier.clone()],
            common_handle_lens: vec![UNSPECIFIED_LENGTH; 2],
            ..DataSource::default()
        };
        let table_path = PossiblePath::Table {
            is_int_handle: false,
            primary_index: Some(0),
        };
        let runtime = IndexJoinRuntimeProp {
            other_conditions: Vec::new(),
            outer_join_keys: vec![Column::new(21, FieldType::new(FieldTypeCode::LongLong))],
            inner_join_keys: vec![supplier],
            avg_inner_row_count: 1.0,
            table_range_scan: true,
        };

        assert!(!path_matches_index_join_runtime(
            &source,
            &table_path,
            &runtime,
        ));
    }

    #[test]
    fn a_limit_pushes_its_partial_half_into_the_reader() {
        // The single-read push-down chain end to end
        // (`attach2Task4PhysicalLimit`, `task.go:619`): the Limit's cop
        // child property reaches the DataSource, which answers the raw COP
        // task; the attach pushes a partial limit — Count = Offset + Count,
        // offset removed — under the reader, and the ROOT limit above keeps
        // the offset. `derive_limit_stats`' recorded verdict ("awaiting
        // core/task.go") is hereby closed: its Go caller arrived.
        use crate::access_path::PossiblePath;
        use crate::logical::{DataSource, LogicalLimit};

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 5,
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, LogicalLimit::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(7.0, [])));
        base.set_children(vec![source]);
        let limit = LogicalPlan::Limit(LogicalLimit::new(base, 2, 5));

        let task = find_best_task(&limit, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        let PhysicalPlan::Limit(root_limit) = plan else {
            panic!("the root Limit tops the plan, got {plan:?}");
        };
        assert_eq!((root_limit.offset, root_limit.count), (2, 5));
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!(
                "a TableReader under the root limit, got {:?}",
                plan.children()
            );
        };
        let Some(PhysicalPlan::Limit(pushed)) = reader.table_plan.as_deref() else {
            panic!("the pushed partial limit inside the reader");
        };
        assert_eq!(
            (pushed.offset, pushed.count),
            (0, 7),
            "offset removed, Count = Offset + Count"
        );
        assert!(
            (pushed.base.base.stats_info().expect("stats").row_count() - 7.0).abs() < f64::EPSILON,
            "DeriveLimitStats caps the pushed profile at the new count"
        );
        assert!(matches!(
            pushed.base.children().first(),
            Some(PhysicalPlan::TableScan(_))
        ));
    }

    #[test]
    fn a_topn_over_the_handle_plans_as_a_keep_order_limit_chain() {
        // TopN's LIMIT half end to end (`getPhysLimits`,
        // `physical_limit.go:198`): ORDER BY pk LIMIT plans the keep-order
        // scan through the child property's order, the pushed partial limit
        // inside the reader, and the root limit above — no Sort anywhere.
        use crate::access_path::PossiblePath;
        use crate::logical::{DataSource, LogicalTopN};
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::aggregation::ByItems;
        use tidb_expr::column::Column;
        use tidb_expr::expression::Expression;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 5,
                pk_is_handle: true,
                handle_is_int: true,
                handle_cols: vec![Column::new(9, FieldType::new(FieldTypeCode::LongLong))],
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, LogicalTopN::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(3.0, [])));
        base.set_children(vec![source]);
        let topn = LogicalPlan::TopN(LogicalTopN {
            base,
            by_items: vec![ByItems::new(
                Expression::Column(Column::new(9, FieldType::new(FieldTypeCode::LongLong))),
                false,
            )],
            offset: 1,
            count: 2,
            ..LogicalTopN::default()
        });

        let task = find_best_task(&topn, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        let PhysicalPlan::Limit(root_limit) = plan else {
            panic!("the root Limit tops the plan, got {plan:?}");
        };
        assert_eq!((root_limit.offset, root_limit.count), (1, 2));
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!("a TableReader, got {:?}", plan.children());
        };
        let Some(PhysicalPlan::Limit(pushed)) = reader.table_plan.as_deref() else {
            panic!("the pushed partial limit inside the reader");
        };
        assert_eq!((pushed.offset, pushed.count), (0, 3));
        let Some(PhysicalPlan::TableScan(scan)) = pushed.base.children().first() else {
            panic!("the scan under the pushed limit");
        };
        assert!(scan.keep_order, "the child property's order rode down");
    }

    #[test]
    fn a_topn_pushes_its_partial_half_and_wins_over_the_limit_slice() {
        // Batches 33-34 end to end: ORDER BY a non-handle column LIMIT n
        // cannot ride keep-order (the LIMIT slice dies at the child), so
        // the TOPN slice wins — the pushed partial TopN sits inside the
        // reader (`getPushedDownTopN`'s simple half: Count = Offset +
        // Count, offset removed, DeriveLimitStats) and the root TopN keeps
        // the offset. Go's exact plan for this query shape.
        use crate::access_path::PossiblePath;
        use crate::logical::{DataSource, LogicalTopN};
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::aggregation::ByItems;
        use tidb_expr::column::Column;
        use tidb_expr::expression::Expression;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 5,
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, LogicalTopN::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(4.0, [])));
        base.set_children(vec![source]);
        let topn = LogicalPlan::TopN(LogicalTopN {
            base,
            by_items: vec![ByItems::new(
                Expression::Column(Column::new(77, FieldType::new(FieldTypeCode::LongLong))),
                true,
            )],
            offset: 1,
            count: 3,
            ..LogicalTopN::default()
        });

        let task = find_best_task(&topn, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        let PhysicalPlan::TopN(root_topn) = plan else {
            panic!("the root TopN tops the plan, got {plan:?}");
        };
        assert_eq!((root_topn.offset, root_topn.count), (1, 3));
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!("a TableReader, got {:?}", plan.children());
        };
        let Some(PhysicalPlan::TopN(pushed)) = reader.table_plan.as_deref() else {
            panic!("the pushed partial TopN inside the reader");
        };
        assert_eq!((pushed.offset, pushed.count), (0, 4));
        assert!(
            (pushed.base.base.stats_info().expect("stats").row_count() - 4.0).abs() < f64::EPSILON,
            "DeriveLimitStats caps the pushed profile"
        );
    }

    #[test]
    fn an_aggregate_plans_above_the_reader() {
        // GROUP BY over a table, end to end: the cop arm of
        // `attach2Task4PhysicalHashAgg` now SPLITS — the partial half rides
        // inside the TableReader next to the scan, and the final half
        // merges above it.
        use crate::access_path::PossiblePath;
        use crate::logical::{DataSource, LogicalAggregation};

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let column_ids = crate::expression_rewriter::ColumnIdAllocator::new();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_column_ids(&column_ids);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 5,
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, "HashAgg", 0);
        base.base.set_stats(Some(StatsInfo::new(10.0, [])));
        base.set_children(vec![source]);
        let agg = LogicalPlan::Aggregation(LogicalAggregation {
            base,
            ..LogicalAggregation::default()
        });

        let task = find_best_task(&agg, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::HashAgg(_)), "got {plan:?}");
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!("a TableReader, got {:?}", plan.children());
        };
        let Some(PhysicalPlan::HashAgg(_)) = reader.table_plan.as_deref() else {
            panic!("the partial aggregate rides inside the reader");
        };

        // A required order enumerates nothing (getHashAggs' first gate);
        // with CanAddEnforcer the enforcer branch sorts ABOVE the agg.
        let prop = PhysicalProperty {
            sort_items: vec![crate::physical_property::SortItem::new(1, false)],
            can_add_enforcer: true,
            ..PhysicalProperty::default()
        };
        let task = find_best_task(&agg, &prop, &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Sort(_)), "got {plan:?}");
        assert!(matches!(
            plan.children().first(),
            Some(PhysicalPlan::HashAgg(_))
        ));
    }

    #[test]
    fn a_global_aggregate_costs_stream_and_hash_over_the_same_child() {
        use crate::access_path::PossiblePath;
        use crate::logical::{DataSource, LogicalAggregation};
        use crate::plan_base::PossiblePropertiesInfo;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let column_ids = crate::expression_rewriter::ColumnIdAllocator::new();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_column_ids(&column_ids);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(250.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 5,
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, "Aggregation", 0);
        base.base.set_stats(Some(StatsInfo::new(1.0, [])));
        base.set_children(vec![source]);
        let agg = LogicalPlan::Aggregation(LogicalAggregation {
            base,
            input_count: 250.0,
            possible_properties: PossiblePropertiesInfo {
                orders: vec![Vec::new()],
                has_tiflash: false,
            },
            ..LogicalAggregation::default()
        });

        let task = find_best_task(&agg, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(
            matches!(plan, PhysicalPlan::StreamAgg(_)),
            "Go's global StreamAgg avoids HashAgg's fixed start cost: {plan:?}"
        );
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!(
                "a TableReader under the global aggregate: {:?}",
                plan.children()
            );
        };
        assert!(
            matches!(
                reader.table_plan.as_deref(),
                Some(PhysicalPlan::StreamAgg(_))
            ),
            "the partial stage keeps the same family"
        );
    }

    #[test]
    fn a_non_root_requirement_is_the_invalid_task() {
        // Go's early answer: "Currently all plan cannot totally push down
        // to TiKV."
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let selection = {
            let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
            base.set_children(vec![dual(&allocator, 1.0)]);
            LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()))
        };
        let prop = PhysicalProperty {
            task_tp: crate::task_type::TaskType::CopSingleRead,
            ..PhysicalProperty::default()
        };
        let task = find_best_task(&selection, &prop, &mut ctx).expect("answers");
        assert!(task.invalid());
    }

    #[test]
    fn the_task_map_memoizes_per_plan_and_property() {
        // Go's taskMap lookup: the second ask answers from the map. Pinned
        // by planning twice and checking the map holds entries for both the
        // selection and its child.
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let selection = {
            let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
            base.base.set_stats(Some(StatsInfo::new(10.0, [])));
            base.set_children(vec![dual(&allocator, 1.0)]);
            LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()))
        };
        let first =
            find_best_task(&selection, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let entries = ctx.task_map.len();
        assert!(entries >= 2, "the selection and its child are both stored");
        let second = find_best_task(&selection, &PhysicalProperty::default(), &mut ctx)
            .expect("answers from the map");
        assert_eq!(ctx.task_map.len(), entries, "no new entries on a hit");
        assert_eq!(
            format!("{:?}", first.plan().map(super::PhysicalPlan::tp)),
            format!("{:?}", second.plan().map(super::PhysicalPlan::tp)),
        );
    }
}
