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

//! Go `pkg/planner/cascades/impl/impl_and_cost.go`: the physical search
//! over the memo. A group's best task for a required property is the
//! cheapest of its expressions' `FindBestTask`, memoized on the group.

use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_physicalop::task::Task;
use tidb_planner_property::PhysicalProperty;

use super::memo::{GroupId, Memo};
use crate::find_best_task::dispatch::{compare_task_cost, find_best_task_4_group_expression, DispatchContext};

/// Go `ImplementMemoAndCost(rootGroup)`'s search: the root group's best task
/// for the root property (`TaskTp: RootTaskType, ExpectedCnt: MaxFloat64`).
/// The caller finishes as Go does (the invalid-task error, the task's
/// warnings, `ResolveIndices`, `GetPlanCost`).
///
/// # Errors
///
/// The physical search's failure.
pub fn implement_memo_and_cost(
    memo: &Memo<'_>,
    root_group: GroupId,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    let root_prop = PhysicalProperty::default();
    Ok(implement_group_and_cost(memo, root_group, &root_prop, f64::MAX, ctx)?
        .unwrap_or_else(Task::invalid_task))
}

/// Go `ImplementGroupAndCost(group, prop, costLimit)`: the group's memoized
/// best task when it costs no more than `cost_limit` (`None` when it costs
/// more), else the cheapest of the group's expressions' tasks, memoized.
///
/// # Errors
///
/// The physical search's failure.
pub fn implement_group_and_cost(
    memo: &Memo<'_>,
    group: GroupId,
    prop: &PhysicalProperty,
    cost_limit: f64,
    ctx: &mut DispatchContext<'_>,
) -> Result<Option<Task>, PlanError> {
    if let Some(task) = memo.group(group).get_best_task(prop) {
        // Go `utilfuncp.GetTaskPlanCost(task)`: an invalid task costs
        // `MaxFloat64` and is reported invalid.
        if task.invalid() {
            return Ok(Some(Task::invalid_task()));
        }
        let task_cost = ctx.coster.task_cost(&task)?;
        if task_cost <= cost_limit {
            return Ok(Some(task));
        }
        return Ok(None);
    }
    let mut best_task = Task::invalid_task();
    for ge in memo.group(group).get_logical_expressions().to_vec() {
        let task = find_best_task_4_group_expression(memo, ge, prop, ctx)?;
        if compare_task_cost(ctx.coster, &task, &best_task)? {
            best_task = task;
        }
    }
    memo.group(group).set_best_task(prop, &best_task);
    Ok(Some(best_task))
}
