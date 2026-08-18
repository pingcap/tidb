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
        }
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
        LogicalPlan::DataSource(_) => {
            return Err(PlanError::internal(
                "findBestTask4LogicalDataSource is the access-path chooser; \
                 this crate carries it separately from the dispatcher",
            ));
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
            let mut cur_task = match attach2_task(pp.clone_shallow(), child_tasks) {
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
