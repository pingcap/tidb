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

//! The enforcer: the Sort a property inserts when no child order satisfies it.
//!
//! Go source: `pkg/planner/core/operator/physicalop/enforce.go` (99 lines),
//! whole. Its three functions are `EnforceProperty` (here as
//! [`enforce_property`]) and `MppTask.EnforceExchanger` /
//! `EnforceExchangerImpl` (here as [`crate::task::MppTask::enforce_exchanger`],
//! a refusal — below).
//!
//! This is the production body behind
//! [`crate::find_best_task::JoinCostModel::enforce`]: `findBestTask`'s
//! enforcer branch prices `EnforceProperty(prop, task, ...)` against the
//! un-enforced candidates, and until this file the crate had only the seam.
//!
//! # Refusals, each naming its Go symbol
//!
//! * `MppTask.EnforceExchanger` (`enforce.go:63`) needs
//!   `property.NeedEnforceExchanger` (partition-property matching over a
//!   `funcdep.FDSet`) and `EnforceExchangerImpl` builds a
//!   `PhysicalExchangeSender`/`PhysicalExchangeReceiver` pair — both
//!   operators unported. Every MPP-property path through `EnforceProperty`
//!   runs through it (Go calls it even for an empty sort property), so the
//!   arm refuses rather than skipping an exchange Go would insert.
//!
//! # Narrowings
//!
//! * `ctx.GetSessionVars().RaiseWarningWhenMPPEnforced(...)`
//!   (`enforce.go:37`): the session-vars warning sink is unported; the
//!   not-all-for-partition MPP arm returns the invalid task Go returns, and
//!   the warning text is not raised anywhere.
//! * `funcdep.FDSet` is unported; it is only read inside the refused
//!   `NeedEnforceExchanger`, so no parameter carries it.

use crate::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalSort};
use crate::physical_property::{CteProducerStatus, PhysicalProperty};
use crate::plan_base::{PlanError, PlanIdAllocator};
use crate::task::{attach2_task, MppTask, Task};
use crate::task_type::TaskType;

impl MppTask {
    /// Go `MppTask.EnforceExchanger(prop, fd)` (`enforce.go:63`): insert an
    /// exchange pair above the task when the partition property demands one.
    ///
    /// REFUSED: the guard is `property.NeedEnforceExchanger(t.partTp,
    /// t.HashCols, prop, fd)` — partition-property matching this port does
    /// not carry (`HashCols` itself is a named boundary on [`MppTask`]) —
    /// and `EnforceExchangerImpl` builds a `PhysicalExchangeSender` /
    /// `PhysicalExchangeReceiver` pair, operators that are not ported.
    /// Skipping the exchange instead would emit an MPP plan Go would never
    /// run; refusing is the loud version of the same gap.
    pub fn enforce_exchanger(&self) -> Result<MppTask, PlanError> {
        Err(PlanError::internal(
            "MppTask.EnforceExchanger (enforce.go) is not ported: \
             property.NeedEnforceExchanger and the \
             PhysicalExchangeSender/PhysicalExchangeReceiver pair of \
             EnforceExchangerImpl are missing",
        ))
    }
}

/// Go `EnforceProperty(p, tsk, ctx, fd)` (`enforce.go:30`): the portal that
/// makes `tsk` satisfy `p`, by exchange (MPP) and/or an inserted Sort.
///
/// The body is Go's, in Go's order:
///
/// 1. an MPP property first checks the task IS a valid MPP task (else the
///    invalid task), then that the sort is partition-local (else Go warns —
///    narrowed, module header — and returns the invalid task), then runs
///    `EnforceExchanger` — refused here, see
///    [`MppTask::enforce_exchanger`];
/// 2. an empty sort property or an invalid task returns the task unchanged;
/// 3. a non-MPP task converts to a root task ([`Task::convert_to_root_task`],
///    whose cop/MPP reader-building refusals apply);
/// 4. a `PhysicalSort` is built over the task plan's stats and query-block
///    offset, its child property `{RootTaskType, p.SortItems, MaxFloat64}`,
///    its `ByItems` copied from `p.SortItems`, `IsPartialSort` from
///    `p.IsSortItemAllForPartition()`, and attached via
///    [`attach2_task`]'s Sort arm.
pub fn enforce_property(
    prop: &PhysicalProperty,
    mut task: Task,
    allocator: &PlanIdAllocator,
) -> Result<Task, PlanError> {
    if prop.task_tp == TaskType::Mpp {
        let Task::Mpp(mpp) = &task else {
            return Ok(Task::invalid_task());
        };
        if task.invalid() {
            return Ok(Task::invalid_task());
        }
        if !prop.is_sort_item_all_for_partition() {
            // Go: RaiseWarningWhenMPPEnforced("MPP mode may be blocked
            // because operator `Sort` is not supported now.") — narrowed.
            return Ok(Task::invalid_task());
        }
        task = Task::Mpp(mpp.enforce_exchanger()?);
    }
    if prop.is_sort_item_empty() || task.invalid() {
        return Ok(task);
    }
    if prop.task_tp != TaskType::Mpp {
        task = task.convert_to_root_task()?;
    }
    let sort_req_prop = PhysicalProperty {
        task_tp: TaskType::Root,
        sort_items: prop.sort_items.clone(),
        expected_cnt: f64::MAX,
        can_add_enforcer: false,
        sort_items_for_partition: Vec::new(),
        cte_producer_status: CteProducerStatus::default(),
    };
    let child = task
        .plan()
        .ok_or_else(|| PlanError::internal("EnforceProperty: the task has no plan to sort"))?;
    let mut base = BasePhysicalPlan::new(allocator, "Sort", child.query_block_offset());
    base.base.set_stats(child.stats_info().cloned());
    base.set_children_req_props(vec![Some(sort_req_prop)]);
    let sort = PhysicalPlan::Sort(PhysicalSort {
        base,
        by_items: prop.sort_items.clone(),
        is_partial_sort: prop.is_sort_item_all_for_partition(),
    });
    attach2_task(sort, vec![task])
}

#[cfg(test)]
mod tests {
    // Go has no enforce_test.go; `EnforceProperty` is exercised through
    // planner-integration suites. These pin the transcreated body's
    // branches directly.

    use super::*;
    use crate::physical::PhysicalTableDual;
    use crate::physical_property::SortItem;
    use crate::plan_base::BasePlan;
    use crate::stats_info::StatsInfo;
    use crate::task::RootTask;

    fn op_with_stats(tp: &str, rows: f64) -> BasePhysicalPlan {
        let allocator = PlanIdAllocator::new();
        let mut base = BasePhysicalPlan::default();
        base.base = BasePlan::new(&allocator, tp, 7);
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        base
    }

    fn root_task_over(rows: f64) -> Task {
        let mut root = RootTask::default();
        root.set_plan(PhysicalPlan::TableDual(PhysicalTableDual {
            base: op_with_stats("Dual", rows),
            ..PhysicalTableDual::default()
        }));
        Task::Root(root)
    }

    fn sorted_prop(items: &[(i64, bool)]) -> PhysicalProperty {
        PhysicalProperty {
            sort_items: items
                .iter()
                .map(|&(col, desc)| SortItem::new(col, desc))
                .collect(),
            ..PhysicalProperty::default()
        }
    }

    #[test]
    fn an_empty_order_returns_the_task_unchanged() {
        // `if p.IsSortItemEmpty() || tsk.Invalid() { return tsk }`.
        let allocator = PlanIdAllocator::new();
        let task = enforce_property(
            &PhysicalProperty::default(),
            root_task_over(10.0),
            &allocator,
        )
        .expect("passes through");
        assert!(matches!(task.plan(), Some(PhysicalPlan::TableDual(_))));
    }

    #[test]
    fn an_invalid_task_passes_through() {
        // Go returns the invalid task itself, not an error: findBestTask
        // prices it as invalid downstream.
        let allocator = PlanIdAllocator::new();
        let task = enforce_property(
            &sorted_prop(&[(3, false)]),
            Task::invalid_task(),
            &allocator,
        )
        .expect("passes through");
        assert!(task.invalid());
    }

    #[test]
    fn a_sort_is_enforced_above_a_root_task() {
        // The built Sort carries: ByItems from p.SortItems, IsPartialSort
        // from IsSortItemAllForPartition (false: partition list empty), the
        // child plan's stats and query-block offset, and the
        // `{RootTaskType, SortItems, MaxFloat64}` child property.
        let allocator = PlanIdAllocator::new();
        let prop = sorted_prop(&[(3, false), (5, true)]);
        let task = enforce_property(&prop, root_task_over(10.0), &allocator).expect("enforces");
        let Some(PhysicalPlan::Sort(sort)) = task.plan() else {
            panic!("a Sort tops the task, got {:?}", task.plan());
        };
        assert_eq!(
            sort.by_items,
            vec![SortItem::new(3, false), SortItem::new(5, true)]
        );
        assert!(!sort.is_partial_sort);
        let plan = task.plan().expect("plan");
        assert!((plan.stats_info().expect("stats").row_count() - 10.0).abs() < f64::EPSILON);
        assert_eq!(plan.query_block_offset(), 7);
        assert_eq!(plan.children().len(), 1, "the old plan became the child");
        let child_prop = plan.base().child_req_prop(0).expect("the sort req prop");
        assert_eq!(child_prop.sort_items, prop.sort_items);
        assert_eq!(child_prop.task_tp, TaskType::Root);
        assert!((child_prop.expected_cnt - f64::MAX).abs() < f64::EPSILON);
        assert!(!child_prop.can_add_enforcer);
    }

    #[test]
    fn a_non_mpp_task_under_an_mpp_property_is_invalid() {
        // `mpp, ok := tsk.(*MppTask); if !ok || mpp.Invalid() { return
        // base.InvalidTask }`.
        let allocator = PlanIdAllocator::new();
        let mut prop = sorted_prop(&[]);
        prop.task_tp = TaskType::Mpp;
        let task =
            enforce_property(&prop, root_task_over(10.0), &allocator).expect("invalid, not error");
        assert!(task.invalid());
    }

    #[test]
    fn an_mpp_sort_that_is_not_partition_local_is_invalid() {
        // `!p.IsSortItemAllForPartition()` warns (narrowed) and returns the
        // invalid task.
        let allocator = PlanIdAllocator::new();
        let mut prop = sorted_prop(&[(3, false)]);
        prop.task_tp = TaskType::Mpp;
        let mpp = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(PhysicalTableDual {
                base: op_with_stats("Dual", 10.0),
                ..PhysicalTableDual::default()
            }),
            crate::physical_property::MppPartitionType::Any,
            [],
        ));
        let task = enforce_property(&prop, mpp, &allocator).expect("invalid, not error");
        assert!(task.invalid());
    }

    #[test]
    fn the_mpp_exchanger_refuses_by_name() {
        // A valid MPP task under a partition-local property reaches
        // EnforceExchanger, which is not ported — even for an EMPTY sort
        // property, because Go calls it before the empty-sort check.
        let allocator = PlanIdAllocator::new();
        let mut prop = sorted_prop(&[]);
        prop.task_tp = TaskType::Mpp;
        let mpp = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(PhysicalTableDual {
                base: op_with_stats("Dual", 10.0),
                ..PhysicalTableDual::default()
            }),
            crate::physical_property::MppPartitionType::Any,
            [],
        ));
        let error = enforce_property(&prop, mpp, &allocator).expect_err("refuses");
        assert!(
            format!("{error}").contains("EnforceExchanger"),
            "the refusal names its Go symbol: {error}"
        );
    }
}
