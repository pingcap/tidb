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
        task = task.into_root_task()?;
    }
    let sort_req_prop = PhysicalProperty {
        index_join: None,
        task_tp: TaskType::Root,
        sort_items: prop.sort_items.clone(),
        expected_cnt: f64::MAX,
        can_add_enforcer: false,
        no_cop_push_down: false,
        sort_items_for_partition: Vec::new(),
        cte_producer_status: CteProducerStatus::default(),
    };
    let child = task
        .plan()
        .ok_or_else(|| PlanError::internal("EnforceProperty: the task has no plan to sort"))?;
    let mut base = BasePhysicalPlan::new(allocator, "Sort", child.query_block_offset());
    base.base.set_stats(child.stats_info().cloned());
    base.set_children_req_props(vec![Some(sort_req_prop)]);
    let by_items =
        crate::physical_property::ColumnSortItem::from_property(&prop.sort_items, child.schema())?
            .into_iter()
            .map(|item| {
                tidb_expr::aggregation::ByItems::new(
                    tidb_expr::expression::Expression::Column(item.col),
                    item.desc,
                )
            })
            .collect();
    let sort = PhysicalPlan::Sort(PhysicalSort {
        base,
        by_items,
        is_partial_sort: prop.is_sort_item_all_for_partition(),
    });
    attach2_task(sort, vec![task], None)
}
