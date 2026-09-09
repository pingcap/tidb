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
//!   through [`TaskCoster`], the same seam decision
//!   [`crate::find_best_task::JoinCostModel`] made and for the same reason:
//!   the cost formulas live in [`crate::plan_cost_ver2`] but the profile
//!   inputs are the caller's.
//!
//! # Narrowings, each naming its Go symbol
//!
//! * Hash build/probe hints and their sort-enforcer retry follow Go.
//!   Index/MPP family enumeration and `applyLogicalHintVarEigen`
//!   remain unported here.
//! * IndexJoinProp participates in memo keys, pattern admission and integer/common
//!   handle inner scans; secondary statistics/selection and full family enumeration
//!   remain unported ([`super::index_join`]).
//! * `checkOpSelfSatisfyPropTaskTypeRequirement` and the MPP property
//!   fields the enforcer branch resets: no TiFlash tier.
//! * `optimizeByShuffle`: the TiDB-side parallel shuffle rewrite is an
//!   executor-parallelism optimization, absent here.
//! * The task map uses Go's represented hash-code fields via [`prop_key`],
//!   including CTE status and exact floating-point expected counts.
//!
//! # Operator routing
//!
//! Go gives some operators their own `findBestTask` override instead of an
//! exhaust: the dual, the CTE table, and the two shows are born directly in
//! root tasks. [`find_best_task`] routes them to those ported bodies first,
//! exactly as Go's function-pointer wiring does. DataSource chooses its
//! range-bearing scans; MemTable's enforcer re-entry remains unported.
//! Forced hash/merge joins and null-aware joins use native candidates. Unhinted
//! joins still require native index alternatives before this dispatcher
//! can replace the live reduced-tree search without changing its search space.

use std::collections::HashMap;

use crate::enforce::enforce_property;
use crate::logical::functional_dependencies::FdContext;
use crate::logical::LogicalPlan;
use crate::physical::hash_join::{get_hash_joins, HashJoinSettings};
use crate::physical::merge_join::{get_merge_joins, MergeJoinSettings};
use crate::physical::{self, PhysicalPlan};
use crate::physical_property::{CteProducerStatus, PhysicalProperty, SortItem};
use crate::plan_base::{PlanError, PlanIdAllocator};
use crate::task::{attach2_task, Task};
use crate::task_type::TaskType;

/// Go `getTaskPlanCost`'s pricing half: what a built task costs.
///
/// The seam exists for [`crate::find_best_task::JoinCostModel`]'s reason:
/// the formulas are in [`crate::plan_cost_ver2`], but row counts and factor
/// profiles are the caller's to provide.
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
    /// Go EnableINLJoinInnerMultiPattern (default true).
    pub index_join_inner_multi_pattern: bool,
    /// Go cardinality.Selectivity over the inner datasource's current table
    /// histograms, applied to residual conditions only, before access predicates
    /// are restored as explicit inner selections.
    pub index_join_selectivity: Option<
        &'a dyn Fn(
            &crate::logical::DataSource,
            &[tidb_expr::expression::Expression],
        ) -> Result<f64, PlanError>,
    >,
    /// Current parameters, range quota and cache admission for inner paths.
    pub index_join_ranger: super::index_join::IndexJoinRangerSettings<'a>,
    /// The plan id allocator for built physical operators.
    pub allocator: &'a PlanIdAllocator,
    /// The pricing seam.
    pub coster: &'a dyn TaskCoster,
    /// The `tidb_opt_skew_ratio` the NDV scaling reads; 1.0 is Go's default.
    pub skew_ratio: f64,
    /// Go `BaseLogicalPlan.taskMap`, keyed here by `(plan id, prop key)`.
    task_map: HashMap<(i32, PropertyKey), Task>,
    /// Current session/runtime configuration for physical hash joins.
    pub hash_join: HashJoinSettings,
    /// Current merge costing setting and its plan-cache relevance.
    pub merge_join: MergeJoinSettings,
    /// Current Apply cache quota and group-NDV risk setting.
    pub apply: physical::apply::ApplySettings,
    /// Whether merge child costing read the ordering selectivity setting.
    pub uses_ordering_index_selectivity: bool,
    /// Go Fix46177: compare an enforced scan even when an ordered scan exists.
    pub explore_enforced_data_source: bool,
    /// Statement-owned column IDs and metadata for logical FD extraction.
    pub functional_dependencies: Option<FdContext<'a>>,
    /// Go statement hint warnings, in emission order.
    pub hint_warnings: Vec<String>,
    /// Go SetSkipPlanCache when correlated index access is promoted.
    pub skip_plan_cache_reason: Option<&'static str>,
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
            index_join_inner_multi_pattern: true,
            index_join_selectivity: None,
            index_join_ranger: Default::default(),
            allocator,
            coster,
            skew_ratio,
            task_map: HashMap::new(),
            column_ids: None,
            hash_join: HashJoinSettings::default(),
            merge_join: MergeJoinSettings::default(),
            apply: physical::apply::ApplySettings::default(),
            uses_ordering_index_selectivity: false,
            explore_enforced_data_source: true,
            functional_dependencies: None,
            hint_warnings: Vec::new(),
            skip_plan_cache_reason: None,
        }
    }

    /// The same context with the session's column-id allocator attached.
    #[must_use]
    pub fn with_column_ids(
        mut self,
        column_ids: &'a crate::expression_rewriter::ColumnIdAllocator,
    ) -> Self {
        self.column_ids = Some(column_ids);
        self.functional_dependencies = Some(FdContext::new(column_ids));
        self
    }
}

/// Go PhysicalProperty.HashCode over the represented fields. Use the exact
/// floating-point representation, not formatted text; CTE status is part of
/// Go's key even for a root task. SortItemsForPartition is not hashed by Go.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PropertyKey {
    index_join: Option<Vec<u8>>,
    task_type: TaskType,
    expected_count: u64,
    can_add_enforcer: bool,
    no_cop_push_down: bool,
    sort_items: Vec<SortItem>,
    cte_producer_status: CteProducerStatus,
}

fn prop_key(prop: &PhysicalProperty) -> PropertyKey {
    PropertyKey {
        index_join: prop.index_join.as_ref().map(|lookup| lookup.hash_code().to_vec()),
        task_type: prop.task_tp,
        expected_count: prop.expected_cnt.to_bits(),
        can_add_enforcer: prop.can_add_enforcer,
        no_cop_push_down: prop.no_cop_push_down,
        sort_items: prop.sort_items.clone(),
        cte_producer_status: prop.cte_producer_status,
    }
}

/// Go `exhaustPhysicalPlans` over the enum's ported operator set: the
/// candidate lists one operator offers under `prop`. Each inner list is one
/// preference slice, as Go's `[][]base.PhysicalPlan` is.
fn exhaust_physical_plans(
    plan: &LogicalPlan,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<(Vec<Vec<PhysicalPlan>>, bool), PlanError> {
    let one = |plans: Vec<PhysicalPlan>| -> Vec<Vec<PhysicalPlan>> {
        if plans.is_empty() {
            Vec::new()
        } else {
            vec![plans]
        }
    };
    if let LogicalPlan::Join(join) = plan {
        let candidates = get_hash_joins(
            join,
            prop,
            ctx.hash_join,
            ctx.allocator,
            ctx.skew_ratio,
            &mut ctx.hint_warnings,
        )?;
        let hint_works = join.prefer_join_type == 0 || candidates.forced;
        if prop.index_join.is_some() {
            return Ok((one(candidates.plans), hint_works));
        }
        if candidates.forced && !candidates.plans.is_empty() {
            return Ok((one(candidates.plans), true));
        }
        if join.na_eq_conditions.is_empty()
            && join.prefer_join_type & crate::plan_builder::from::join_hint_flags::MERGE_JOIN != 0
        {
            let fd = ctx.functional_dependencies.as_ref().ok_or_else(|| {
                PlanError::internal("native merge search requires the statement column allocator")
            })?;
            let merges = get_merge_joins(
                join,
                prop,
                fd,
                ctx.merge_join,
                ctx.hash_join.disable_hash_join,
                ctx.allocator,
                ctx.skew_ratio,
                &mut ctx.hint_warnings,
            )?;
            ctx.uses_ordering_index_selectivity |= merges.uses_ordering_index_selectivity;
            if !merges.plans.is_empty() {
                return Ok((one(merges.plans), true));
            }
        }
        // Go returns a forced hash family before enumerating merge/index,
        // and a null-aware anti join never enumerates those families.
        // Do not silently choose hash when their native alternatives are absent.
        if !candidates.forced && join.na_eq_conditions.is_empty() && prop.is_sort_item_empty() {
            return Err(PlanError::internal(
                "native index join candidate enumeration is not implemented",
            ));
        }
        return Ok((one(candidates.plans), hint_works));
    }
    let plans = match plan {
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
            )))
        }
        LogicalPlan::Limit(op) => Ok(one(physical::exhaust_physical_plans_4_logical_limit(
            op,
            prop,
            ctx.allocator,
        )?)),
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
                return Ok((Vec::new(), true));
            }
            // Go's two preference slices, in order: the TopN operators
            // (`getPhysTopN`), then the LIMIT half (`getPhysLimits`).
            let mut slices = Vec::with_capacity(2);
            let topns = physical::get_phys_topn(op, prop, ctx.allocator)?;
            if !topns.is_empty() {
                slices.push(topns);
            }
            let limits = physical::get_phys_limits(op, prop, ctx.allocator)?;
            if !limits.is_empty() {
                slices.push(limits);
            }
            Ok(slices)
        }
        LogicalPlan::Aggregation(op) => {
            // `ExhaustPhysicalPlans4LogicalAggregation`
            // (`base_physical_agg.go:935`): the hash-agg candidates; the
            // stream-agg half (`getStreamAggs`, order-riding) is a later
            // slice, named.
            // Go appends `getStreamAggs` then `getHashAggs` into ONE
            // list; the stream candidates ride a covered order, the hash
            // candidates need none, and cost picks between them.
            let mut aggs = physical::get_stream_aggs(op, prop, ctx.allocator, ctx.skew_ratio);
            aggs.extend(physical::get_hash_aggs(
                op,
                prop,
                ctx.allocator,
                ctx.skew_ratio,
            ));
            Ok(if aggs.is_empty() {
                Vec::new()
            } else {
                vec![aggs]
            })
        }
        LogicalPlan::Join(_) => unreachable!("joins dispatched above"),
        LogicalPlan::Apply(op) => {
            let candidates = physical::apply::get_apply(
                op,
                prop,
                ctx.apply,
                ctx.merge_join.ordering_index_selectivity_ratio,
                ctx.hash_join.concurrency,
                ctx.allocator,
                ctx.skew_ratio,
            )?;
            ctx.uses_ordering_index_selectivity |= candidates.uses_ordering_index_selectivity;
            Ok(one(candidates.plans))
        }
        other => Err(PlanError::internal(format!(
            "exhaustPhysicalPlans over {} is not ported to the dispatcher",
            other.tp()
        ))),
    }?;
    Ok((plans, true))
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
    if prop.index_join.as_ref().is_some_and(|lookup| {
        !super::index_join::admits_inner(plan, lookup, ctx.index_join_inner_multi_pattern)
    }) {
        return Ok(Task::invalid_task());
    }
    let best = find_best_task_uncached(plan, prop, ctx)?;
    ctx.task_map.insert(key, best.copy());
    Ok(best)
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

    let mut new_prop = prop.clone_essential_fields();
    new_prop.index_join = prop.index_join.clone();
    let (mut plans_fits_prop, hint_works) = exhaust_physical_plans(plan, &new_prop, ctx)?;
    let can_add_enforcer = prop.can_add_enforcer
        || (!hint_works && !new_prop.is_sort_item_empty() && new_prop.index_join.is_none());
    let mut plans_need_enforce = Vec::new();
    if can_add_enforcer {
        let mut empty = new_prop.clone_essential_fields();
        empty.index_join = new_prop.index_join.clone();
        empty.sort_items.clear();
        empty.sort_items_for_partition.clear();
        empty.expected_cnt = f64::MAX;
        let (enforced, hint_can_work) = exhaust_physical_plans(plan, &empty, ctx)?;
        plans_need_enforce = enforced;
        // The native tree has no index-join candidates yet. This is Go's
        // non-index branch: honor a hint rescued by removing the order.
        if hint_can_work && !hint_works {
            plans_fits_prop.clear();
        }
        if !hint_can_work && !hint_works && !prop.can_add_enforcer {
            plans_need_enforce.clear();
        }
    }

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
pub(super) fn table_path_matches_order(ds: &crate::logical::DataSource, prop: &PhysicalProperty) -> bool {
    if !ds.pk_is_handle || !ds.handle_is_int {
        return false;
    }
    let Some(pk_col) = ds.handle_cols.first() else {
        return false;
    };
    let [item] = prop.sort_items.as_slice() else {
        return false;
    };
    item.col == pk_col.unique_id
}

/// The basic index-prefix arm of Go `matchProperty` (`find_best_task.go:1095`):
/// the required order matches an index when every sort item is the same
/// direction (`AllSameOrder`) and the items are a PREFIX of the index's
/// columns, mapped to unique ids through retained table-order columns, not
/// the pruned output schema. Constant-column skipping and the common-handle suffix
/// extension are ranger-fed refinements, named as later slices.
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
    if index.columns.len() < prop.sort_items.len() {
        return false;
    }
    prop.sort_items
        .iter()
        .zip(&index.columns)
        .all(|(item, col)| {
            // Go's `isMatchProp` walks `FullIdxCols`, where a prefix-length
            // column has no entry: it cannot carry an order.
            col.length < 0
                && ds
                    .table_columns
                    .get(col.offset)
                    .is_some_and(|schema_col| schema_col.unique_id == item.col)
        })
}

fn scan_selection(
    child: PhysicalPlan,
    conditions: Vec<tidb_expr::expression::Expression>,
    ds: &crate::logical::DataSource,
    ctx: &DispatchContext<'_>,
) -> PhysicalPlan {
    if conditions.is_empty() {
        return child;
    }
    let mut base = crate::physical::BasePhysicalPlan::new(
        ctx.allocator,
        "Selection",
        ds.base.base.query_block_offset(),
    );
    base.base.set_schema(child.schema().cloned());
    base.base.set_stats(ds.base.base.stats_info().cloned());
    base.set_children(vec![child]);
    PhysicalPlan::Selection(crate::physical::PhysicalSelection {
        base,
        conditions,
        from_data_source: true,
    })
}

fn promote_correlated_index_access(
    result: &mut crate::ranger::detacher::DetachRangeResult,
    columns: &[tidb_expr::column::Column],
    lengths: &[i64],
    ctx: &mut DispatchContext<'_>,
) -> bool {
    if result.eq_or_in_count != result.access_conds.len() {
        return false;
    }
    let (access, remained) = crate::access_path::split_correlated_access_conditions(
        &result.remained_conds,
        columns,
        lengths,
        result.eq_or_in_count,
    );
    let promoted = !access.is_empty();
    if promoted {
        ctx.skip_plan_cache_reason = Some("Correlated subquery is not cached currently");
    }
    result.access_conds.extend(access);
    result.remained_conds = remained;
    promoted
}

fn find_best_task_4_logical_data_source(
    ds: &crate::logical::DataSource,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    if prop.index_join.is_some() {
        return super::index_join::find_inner_task(ds, prop, ctx);
    }
    // Go findBestTask4LogicalDataSource owns its enforcer branch; this
    // operator bypasses the general logical-plan search above.
    if prop.can_add_enforcer {
        let mut ordered = prop.clone_essential_fields();
        ordered.can_add_enforcer = false;
        let natural = find_best_task_4_logical_data_source(ds, &ordered, ctx)?;
        if !natural.invalid() && !ctx.explore_enforced_data_source {
            return Ok(natural);
        }
        let mut unordered = prop.clone_essential_fields();
        unordered.sort_items.clear();
        let scan = find_data_source_scan(ds, &unordered, ctx)?;
        let enforced = enforce_property(prop, scan, ctx.allocator)?;
        if !natural.invalid() && compare_task_cost(ctx.coster, &natural, &enforced)? {
            return Ok(natural);
        }
        return Ok(enforced);
    }
    find_data_source_scan(ds, prop, ctx)
}

fn find_data_source_scan(
    ds: &crate::logical::DataSource,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    // Go's DataSource findBestTask serves COP-typed properties too: a
    // single-read child property (`CopSingleReadTaskType`) answers the COP
    // task itself, for the parent's push-down attach to grow
    // (`convertToTableScan` refuses only `CopMultiReadTaskType`, the
    // double-read type, which this slice's lookup-less world cannot serve).
    let cop_answer = match prop.task_tp {
        TaskType::Root => false,
        TaskType::CopSingleRead => true,
        _ => return Ok(Task::invalid_task()),
    };
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
        let cop = match path {
            crate::access_path::PossiblePath::Table { .. } => {
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
                // Go `buildTableRange` over the pushed conditions: the
                // int-handle scan's key ranges (full when nothing pushed).
                let pk_column = ds
                    .pk_is_handle
                    .then(|| {
                        ds.table_columns.iter().find(|col| {
                            col.ret_type.as_ref().is_some_and(|tp| {
                                tp.flags() & tidb_datatype::FieldTypeFlags::PRI_KEY != 0
                            })
                        })
                    })
                    .flatten()
                    .cloned();
                let handle_type = pk_column
                    .as_ref()
                    .and_then(|col| col.ret_type.clone())
                    .unwrap_or_else(|| {
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                    });
                let mut correlated_access = false;
                let (ranges, access_conditions, filters) = if !ds.common_handle_cols.is_empty() {
                    let mut result =
                        crate::ranger::detacher::detach_cond_and_build_range_for_index(
                            &ds.pushed_down_conds,
                            &ds.common_handle_cols,
                            &ds.common_handle_lens,
                            0,
                        )
                        .map_err(|error| {
                            PlanError::internal(format!(
                                "cannot build common-handle ranges: {error:?}"
                            ))
                        })?;
                    correlated_access = promote_correlated_index_access(
                        &mut result,
                        &ds.common_handle_cols,
                        &ds.common_handle_lens,
                        ctx,
                    );
                    (result.ranges, result.access_conds, result.remained_conds)
                } else {
                    let (mut access, mut filters) = pk_column.as_ref().map_or_else(
                        || (Vec::new(), ds.pushed_down_conds.clone()),
                        |col| {
                            crate::ranger::detacher::detach_conds_for_column(
                                &ds.pushed_down_conds,
                                col,
                                true,
                            )
                        },
                    );
                    if access.is_empty() {
                        if let Some(pk) = &pk_column {
                            use tidb_expr::expression::Expression;
                            if let Some(index) = filters.iter().position(|expr| {
                                let Expression::ScalarFunction(fun) = expr else { return false; };
                                if fun.func_name.lowercase() != "eq" { return false; }
                                matches!(fun.args.as_slice(), [Expression::Column(col), Expression::CorrelatedColumn(_)]
                                    | [Expression::CorrelatedColumn(_), Expression::Column(col)] if col.unique_id == pk.unique_id)
                            }) { access.push(filters.remove(index)); correlated_access = true; }
                        }
                    }
                    if correlated_access {
                        (
                            crate::ranger::points::full_int_range(handle_type.is_unsigned()),
                            access,
                            filters,
                        )
                    } else {
                        let result =
                            crate::ranger::ranger::build_table_range(&access, &handle_type, 0)
                                .map_err(|error| {
                                    PlanError::internal(format!(
                                        "cannot build table ranges: {error:?}"
                                    ))
                                })?;
                        if result.remained_conds.is_empty() {
                            (result.ranges, access, filters)
                        } else {
                            let ranges = result.ranges;
                            filters.extend(access);
                            (ranges, Vec::new(), filters)
                        }
                    }
                };
                // Go `CountAfterAccess`: the pseudo row count over the
                // built ranges shapes the scan's stats when conditions
                // pushed.
                let mut stats = ds.base.base.stats_info().cloned();
                if !ds.pushed_down_conds.is_empty() {
                    if let Some(base_stats) = &stats {
                        let count = if correlated_access {
                            if ds.common_handle_cols.is_empty() {
                                1.0
                            } else {
                                crate::cardinality::pseudo::pseudo_avg_count_per_value(
                                    base_stats.row_count(),
                                )
                            }
                        } else if !ds.common_handle_cols.is_empty() {
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
                        };
                        stats = Some(crate::stats_info::StatsInfo::new(
                            count.min(base_stats.row_count()),
                            [],
                        ));
                    }
                }
                base.base.set_stats(stats);
                let scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                    base,
                    table_id: ds.physical_table_id,
                    store_type: crate::physical_table_reader::StoreType::TiKv,
                    keep_order,
                    desc,
                    ranges,
                    access_conditions,
                    pk_column,
                    common_handle_cols: ds.common_handle_cols.clone(),
                    common_handle_lens: ds.common_handle_lens.clone(),
                    ..Default::default()
                });
                let scan = scan_selection(scan, filters, ds, ctx);
                Task::Cop(crate::task::CopTask {
                    table_plan: Some(Box::new(scan)),
                    index_plan_finished: true,
                    keep_order,
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
                // Go `convertToIndexScan`: a single-read property cannot be
                // served by a double read.
                if cop_answer && !single_scan {
                    continue;
                }
                let mut base = crate::physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    "IndexScan",
                    ds.base.base.query_block_offset(),
                );
                base.base.set_schema(ds.base.base.schema().cloned());
                // Go `detachCondAndBuildRangeForPath`: the index columns
                // (table columns at the index's offsets) detach the pushed
                // conditions into this path's ranges.
                let index_cols: Vec<tidb_expr::column::Column> = source_index
                    .columns
                    .iter()
                    .map(|index_column| {
                        ds.table_columns
                            .get(index_column.offset)
                            .cloned()
                            .ok_or_else(|| {
                                PlanError::internal("index offset is outside table columns")
                            })
                    })
                    .collect::<Result<_, _>>()?;
                let index_lengths: Vec<i64> = source_index
                    .columns
                    .iter()
                    .map(|index_column| index_column.length)
                    .collect();
                let mut result = crate::ranger::detacher::detach_cond_and_build_range_for_index(
                    &ds.pushed_down_conds,
                    &index_cols,
                    &index_lengths,
                    0,
                )
                .map_err(|error| {
                    PlanError::internal(format!("cannot build index ranges: {error:?}"))
                })?;
                let correlated_access =
                    promote_correlated_index_access(&mut result, &index_cols, &index_lengths, ctx);
                let ranges = result.ranges;
                let (index_filters, table_filters): (Vec<_>, Vec<_>) =
                    result.remained_conds.into_iter().partition(|condition| {
                        tidb_expr::simple_expr::extract_columns(condition)
                            .iter()
                            .all(|column| {
                                ds.handle_cols.iter().any(|handle| {
                                    ds.pk_is_handle && handle.unique_id == column.unique_id
                                }) || index_cols.iter().zip(&index_lengths).any(
                                    |(index_column, length)| {
                                        *length < 0 && index_column.unique_id == column.unique_id
                                    },
                                )
                            })
                    });
                let mut stats = ds.base.base.stats_info().cloned();
                if !ds.pushed_down_conds.is_empty() {
                    if let Some(base_stats) = &stats {
                        let count = if correlated_access {
                            crate::cardinality::pseudo::pseudo_avg_count_per_value(
                                base_stats.row_count(),
                            )
                        } else {
                            crate::ranger::stats_bridge::pseudo_count_by_ranges(
                                &ranges,
                                base_stats.row_count(),
                            )
                        };
                        stats = Some(crate::stats_info::StatsInfo::new(
                            count.min(base_stats.row_count()),
                            [],
                        ));
                    }
                }
                base.base.set_stats(stats);
                let scan = PhysicalPlan::IndexScan(crate::physical::PhysicalIndexScan {
                    base,
                    data_source_schema: ds.base.base.schema().cloned().map(std::sync::Arc::new),
                    table_id: ds.physical_table_id,
                    index_id: source_index.id,
                    index_name: source_index.name.clone(),
                    keep_order,
                    desc,
                    ranges,
                    access_conditions: result.access_conds,
                    idx_cols: index_cols,
                    idx_col_lens: index_lengths,
                });
                let scan = scan_selection(scan, index_filters, ds, ctx);
                let table_side = if single_scan {
                    if !table_filters.is_empty() {
                        return Err(PlanError::internal("covering index has uncovered filters"));
                    }
                    None
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
                        .set_stats(ds.base.base.stats_info().cloned());
                    table_base.base.set_schema(ds.base.base.schema().cloned());
                    let table_scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                        base: table_base,
                        table_id: ds.physical_table_id,
                        store_type: crate::physical_table_reader::StoreType::TiKv,
                        keep_order: false,
                        desc: false,
                        // The lookup's table side reads BY HANDLE from
                        // the index rows, not by its own ranges.
                        ranges: crate::ranger::types::Ranges::new(),
                        ..Default::default()
                    });
                    Some(Box::new(scan_selection(table_scan, table_filters, ds, ctx)))
                };
                Task::Cop(crate::task::CopTask {
                    index_plan: Some(Box::new(scan)),
                    table_plan: table_side,
                    index_plan_finished: false,
                    keep_order,
                    ..crate::task::CopTask::default()
                })
            }
        };
        let cur = if cop_answer {
            cop
        } else {
            cop.into_root_task()?
        };
        if best.invalid() || compare_task_cost(ctx.coster, &cur, &best)? {
            best = cur;
        }
    }
    Ok(best)
}

/// Go `enumeratePhysicalPlans4Task` + helper (`find_best_task.go:112,156`),
/// without the hint half: plan every child under the candidate's child
/// property, attach, convert to root, enforce when asked, keep the
/// cheapest.
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
    let mut normal_task = Task::invalid_task();
    for ops in physical_plans_slice {
        for pp in ops {
            let child_len = plan.children().len();
            let mut child_tasks = Vec::with_capacity(child_len);
            for (i, child) in plan.children().iter().enumerate() {
                let Some(child_prop) = pp.base().child_req_prop(i) else {
                    break;
                };
                let child_prop = child_prop.clone();
                let child_task = find_best_task(child, &child_prop, ctx)?;
                if child_task.invalid() {
                    break;
                }
                child_tasks.push(child_task);
            }
            // "This check makes sure that there is no invalid child task."
            if child_tasks.len() != child_len {
                continue;
            }
            let mut cur_task = match attach2_task(pp.clone_shallow(), child_tasks, ctx.column_ids) {
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
                cur_task = cur_task.into_root_task()?;
            }
            if add_enforcer {
                cur_task = enforce_property(prop, cur_task, ctx.allocator)?;
            }
            if normal_task.invalid() || compare_task_cost(ctx.coster, &cur_task, &normal_task)? {
                normal_task = cur_task;
            }
        }
    }
    Ok(normal_task)
}
