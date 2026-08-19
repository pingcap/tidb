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
//! * Hints: `hintWorksWithProp`/`hintCanWork` and
//!   `applyLogicalHintVarEigen` — an unhinted plan answers `true`, so the
//!   only enforcer trigger left is `prop.CanAddEnforcer`, which is what
//!   this port implements. A hinted enumeration is planner-hint work.
//! * `prop.IndexJoinProp` and `admitIndexJoinInnerChildPattern`: the
//!   index-join runtime property is unported ([`crate::task`] header).
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
    /// The plan id allocator for built physical operators.
    pub allocator: &'a PlanIdAllocator,
    /// The pricing seam.
    pub coster: &'a dyn TaskCoster,
    /// The `tidb_opt_skew_ratio` the NDV scaling reads; 1.0 is Go's default.
    pub skew_ratio: f64,
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
            task_map: HashMap::new(),
            column_ids: None,
        }
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
    let mut key = format!("{:?}|{}|{}", prop.task_tp, prop.expected_cnt, prop.can_add_enforcer);
    for item in &prop.sort_items {
        let _ = write!(key, "|{}:{}", item.col, item.desc);
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
        LogicalPlan::Selection(op) => Ok(one(physical::exhaust_physical_plans_4_logical_selection(
            op,
            prop,
            ctx.allocator,
            ctx.skew_ratio,
        ))),
        LogicalPlan::Projection(op) => Ok(one(
            physical::exhaust_physical_plans_4_logical_projection(
                op,
                prop,
                ctx.allocator,
                ctx.skew_ratio,
            ),
        )),
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
            let topns = physical::get_phys_topn(op, ctx.allocator);
            if !topns.is_empty() {
                slices.push(topns);
            }
            let limits = physical::get_phys_limits(op, ctx.allocator);
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
            aggs.extend(physical::get_hash_aggs(op, prop, ctx.allocator, ctx.skew_ratio));
            Ok(if aggs.is_empty() { Vec::new() } else { vec![aggs] })
        }
        LogicalPlan::Join(_) | LogicalPlan::Apply(_) => Err(PlanError::internal(
            "exhaustPhysicalPlans4LogicalJoin: joins are enumerated by \
             crate::find_best_task's specialized search, not this dispatcher",
        )),
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

    let mut can_add_enforcer = prop.can_add_enforcer;
    // An unhinted enumeration always answers hintWorksWithProp = true, so
    // Go's !hintWorksWithProp trigger cannot fire; the narrowed condition
    // is exactly the caller's CanAddEnforcer.
    let _ = &mut can_add_enforcer;

    let new_prop = prop.clone_essential_fields();
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
            let dual = PhysicalPlan::TableDual(crate::physical::PhysicalTableDual {
                base,
                row_count: 0,
            });
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
fn table_path_matches_order(
    ds: &crate::logical::DataSource,
    prop: &PhysicalProperty,
) -> bool {
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
/// columns, mapped to unique ids through the source's schema (an index
/// column's `Offset` addresses the TABLE column list, which is the scan's
/// schema order). Constant-column skipping and the common-handle suffix
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
    let Some(schema) = ds.base.base.schema() else {
        return false;
    };
    if index.columns.len() < prop.sort_items.len() {
        return false;
    }
    prop.sort_items.iter().zip(&index.columns).all(|(item, col)| {
        // Go's `isMatchProp` walks `FullIdxCols`, where a prefix-length
        // column has no entry: it cannot carry an order.
        col.length < 0
            && schema
                .columns
                .get(col.offset)
                .is_some_and(|schema_col| schema_col.unique_id == item.col)
    })
}

fn find_best_task_4_logical_data_source(
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
                base.base.set_stats(ds.base.base.stats_info().cloned());
                base.base.set_schema(ds.base.base.schema().cloned());
                let scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                    base,
                    table_id: ds.physical_table_id,
                    store_type: crate::physical_table_reader::StoreType::TiKv,
                    keep_order,
                    desc,
                });
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
                base.base.set_stats(ds.base.base.stats_info().cloned());
                base.base.set_schema(ds.base.base.schema().cloned());
                let scan = PhysicalPlan::IndexScan(crate::physical::PhysicalIndexScan {
                    base,
                    table_id: ds.physical_table_id,
                    index_id: source_index.id,
                    index_name: source_index.name.clone(),
                    keep_order,
                    desc,
                });
                let table_side = if single_scan {
                    None
                } else {
                    // Go `convertToIndexScan` builds the lookup's table side
                    // over the source's schema and stats.
                    let mut table_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        "TableScan",
                        ds.base.base.query_block_offset(),
                    );
                    table_base.base.set_stats(ds.base.base.stats_info().cloned());
                    table_base.base.set_schema(ds.base.base.schema().cloned());
                    Some(Box::new(PhysicalPlan::TableScan(
                        crate::physical::PhysicalTableScan {
                            base: table_base,
                            table_id: ds.physical_table_id,
                            store_type: crate::physical_table_reader::StoreType::TiKv,
                            keep_order: false,
                            desc: false,
                        },
                    )))
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
            cop.convert_to_root_task()?
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
                cur_task = cur_task.convert_to_root_task()?;
            }
            if add_enforcer {
                cur_task = enforce_property(prop, cur_task, ctx.allocator)?;
            }
            if normal_task.invalid()
                || compare_task_cost(ctx.coster, &cur_task, &normal_task)?
            {
                normal_task = cur_task;
            }
        }
    }
    Ok(normal_task)
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

        let task = find_best_task(&selection, &PhysicalProperty::default(), &mut ctx)
            .expect("plans");
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
            base.base.set_schema(Some(tidb_expr::schema::Schema::default()));
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

        let task = find_best_task(&selection, &PhysicalProperty::default(), &mut ctx)
            .expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!("a TableReader under the selection, got {:?}", plan.children());
        };
        let Some(PhysicalPlan::TableScan(scan)) = reader.table_plan.as_deref() else {
            panic!("the scan hangs off TablePlan");
        };
        assert_eq!(scan.table_id, 42);
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
        let task = find_best_task(&source, &PhysicalProperty::default(), &mut ctx)
            .expect("answers");
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
                    PossiblePath::Table { is_int_handle: true, primary_index: None },
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
            matches!(lookup.table_plan.as_deref(), Some(PhysicalPlan::TableScan(_))),
            "the table side reads rows back"
        );

        // Index on (b, a): covering, the reader plans as before.
        let covering = build(vec![
            SourceIndexColumn { name: "b".to_owned(), offset: 1, length: -1 },
            SourceIndexColumn { name: "a".to_owned(), offset: 0, length: -1 },
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
            SourceIndexColumn { name: "b".to_owned(), offset: 1, length: 10 },
            SourceIndexColumn { name: "a".to_owned(), offset: 0, length: -1 },
        ]);
        let task = find_best_task(&prefix, &order_by_b, &mut ctx).expect("answers");
        assert!(task.invalid(), "a prefix column serves no order");
    }

    fn an_index_prefix_order_plans_an_index_reader() {
        // The index-read column end to end: ORDER BY the index's first
        // column admits the index path (`matchProperty:1095` basic prefix),
        // the cop's index half converts through `convertToRootTaskImpl`'s
        // index branch (`task_base.go:563`), and the reader carries the
        // scan on its IndexPlan field.
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::schema::Schema;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
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
                enumerated_paths: vec![
                    PossiblePath::Table { is_int_handle: true, primary_index: None },
                    PossiblePath::Index { index: 0 },
                ],
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

        // ORDER BY the index's column (unique id 12): only the index path
        // matches, and the plan is an IndexReader over a keep-order scan.
        let prop = PhysicalProperty::new(TaskType::Root, &[12], false, f64::MAX, false);
        let task = find_best_task(&source, &prop, &mut ctx).expect("plans");
        let Some(PhysicalPlan::IndexReader(reader)) = task.plan() else {
            panic!("an IndexReader, got {:?}", task.plan());
        };
        let Some(PhysicalPlan::IndexScan(scan)) = reader.index_plan.as_deref() else {
            panic!("the scan hangs off IndexPlan");
        };
        assert_eq!(scan.index_id, 3);
        assert!(scan.keep_order && !scan.desc);

        // A two-item order the one-column index cannot cover refuses.
        let prop = PhysicalProperty::new(TaskType::Root, &[12, 11], false, f64::MAX, false);
        let task = find_best_task(&source, &prop, &mut ctx).expect("answers");
        assert!(task.invalid());
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
            base.base.set_schema(Some(tidb_expr::schema::Schema::default()));
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

        let task = find_best_task(&limit, &PhysicalProperty::default(), &mut ctx)
            .expect("plans");
        let plan = task.plan().expect("a plan");
        let PhysicalPlan::Limit(root_limit) = plan else {
            panic!("the root Limit tops the plan, got {plan:?}");
        };
        assert_eq!((root_limit.offset, root_limit.count), (2, 5));
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!("a TableReader under the root limit, got {:?}", plan.children());
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
            (pushed
                .base
                .base
                .stats_info()
                .expect("stats")
                .row_count()
                - 7.0)
                .abs()
                < f64::EPSILON,
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
            base.base.set_schema(Some(tidb_expr::schema::Schema::default()));
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

        let task = find_best_task(&topn, &PhysicalProperty::default(), &mut ctx)
            .expect("plans");
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
            base.base.set_schema(Some(tidb_expr::schema::Schema::default()));
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

        let task = find_best_task(&topn, &PhysicalProperty::default(), &mut ctx)
            .expect("plans");
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
            (pushed.base.base.stats_info().expect("stats").row_count() - 4.0).abs()
                < f64::EPSILON,
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
        let mut ctx =
            DispatchContext::new(&allocator, &coster, 1.0).with_column_ids(&column_ids);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            base.base.set_schema(Some(tidb_expr::schema::Schema::default()));
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

        let task = find_best_task(&agg, &PhysicalProperty::default(), &mut ctx)
            .expect("plans");
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
        let first = find_best_task(&selection, &PhysicalProperty::default(), &mut ctx)
            .expect("plans");
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
