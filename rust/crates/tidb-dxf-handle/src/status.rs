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

//! Go `status.go`: the schedule status the next-gen cluster controller
//! reads.

use std::collections::BTreeMap;

use tidb_dxf::schstatus::{Flag, TtlFlag, PAUSE_SCALE_IN_FLAG};
use tidb_dxf::{TaskBase, IMPORT_INTO, IMPORT_STEP_COLLECT_CONFLICTS, IMPORT_STEP_CONFLICT_RESOLUTION, IMPORT_STEP_POST_PROCESS};
use tidb_dxf_storage::{get_dxf_cpu_count, get_dxf_svc_task_mgr, get_task_manager, ActiveTaskSummary, HistoryTaskPage, TaskManager};
use tidb_log::{Field, Value};

use crate::{HandleError, Result};

/// Go `GetActiveTaskSummary`: the number of active tasks (rows of
/// `mysql.tidb_global_task`) and their breakdown by keyspace.
///
/// # Errors
/// No task manager is set, or the query failed.
pub fn get_active_task_summary(ctx: &tidb_gocontext::Context) -> Result<ActiveTaskSummary> {
    Ok(get_task_manager()?.get_active_task_counts_by_keyspace(ctx)?)
}

/// Go `ListHistoryTasks`: the history tasks, with keyset pagination and an
/// optional keyspace filter.
///
/// # Errors
/// No task manager is set, the page size is out of bounds, or the query
/// failed.
pub fn list_history_tasks(ctx: &tidb_gocontext::Context, page_size: isize, page_token: i64, keyspace: &str) -> Result<HistoryTaskPage> {
    Ok(get_dxf_svc_task_mgr()?.list_history_tasks(ctx, page_size, page_token, keyspace)?)
}

/// Go `GetNodesInfo`: the number of managed nodes and their CPU count.
///
/// # Errors
/// The query failed.
pub fn get_nodes_info(ctx: &tidb_gocontext::Context, manager: &TaskManager) -> Result<(usize, isize)> {
    let nodes = manager.get_all_nodes(ctx)?;
    let cpu_count = match nodes.first() {
        Some(node) => node.cpu_count,
        None => {
            // shouldn't happen normally as every node will register itself to the meta
            // table.
            let cpu_count = get_dxf_cpu_count();
            tidb_logutil::bg_logger().warn(
                "no managed nodes found, use local usable CPU count instead",
                &[Field::new("usableCPUCount", Value::I64(cpu_count as i64))],
            );
            cpu_count
        }
    };
    Ok((nodes.len(), cpu_count))
}

/// Go `CalculateRequiredNodes`: simulates how the scheduler and the balancer
/// place the tasks, and counts the nodes they need. `tasks` must be in rank
/// order (see `TaskBase`).
#[must_use]
pub fn calculate_required_nodes(tasks: &[TaskBase], cpu_count: isize) -> usize {
    let mut avail_resources: Vec<isize> = Vec::with_capacity(tasks.len());
    // for each task, at most MaxNodeCount subtasks can be run in parallel, and
    // on each node, each task can have at most 1 subtask running. we will try to
    // run the subtask on existing nodes, if not enough resources, we will create
    // new nodes.
    for t in tasks {
        let mut needed = get_needed_nodes(t);
        for avail in &mut avail_resources {
            if needed <= 0 {
                break;
            }
            if *avail >= t.required_slots {
                *avail -= t.required_slots;
                needed -= 1;
            }
        }
        for _ in 0..needed.max(0) {
            // we have restricted the required slots of each task to be less than
            // node CPU count at submit time, so t.RequiredSlots > cpuCount should
            // not happen.
            avail_resources.push(cpu_count - t.required_slots);
        }
    }
    // make sure 1 node exist for DXF owner and works as a reserved node, to make
    // small tasks more responsive.
    avail_resources.len().max(1)
}

/// Go `getNeededNodes`.
pub(crate) fn get_needed_nodes(task: &TaskBase) -> isize {
    // for below steps of IMPORT INTO task, we only have 1 subtask and only need
    // 1 node.
    if task.tp == IMPORT_INTO
        && (task.step == IMPORT_STEP_COLLECT_CONFLICTS
            || task.step == IMPORT_STEP_CONFLICT_RESOLUTION
            || task.step == IMPORT_STEP_POST_PROCESS)
    {
        return 1;
    }
    task.max_node_count
}

/// Go `GetScheduleFlags`: the schedule flags, such as pause-scale-in.
///
/// # Errors
/// The query failed or the stored flag does not parse.
pub fn get_schedule_flags(ctx: &tidb_gocontext::Context, manager: &TaskManager) -> Result<BTreeMap<Flag, TtlFlag>> {
    let mut flags = BTreeMap::new();
    let pause_scale_in = get_pause_scale_in_flag(ctx, manager)?;
    if pause_scale_in.enabled {
        flags.insert(PAUSE_SCALE_IN_FLAG, pause_scale_in);
    }
    Ok(flags)
}

/// Go `getPauseScaleInFlag`: the stored flag, reset once expired.
fn get_pause_scale_in_flag(ctx: &tidb_gocontext::Context, manager: &TaskManager) -> Result<TtlFlag> {
    let rs = manager.execute_sql_with_new_session(ctx, 
        "SELECT VARIABLE_VALUE from mysql.tidb WHERE VARIABLE_NAME = %?",
        &[tidb_sqlescape::SqlArg::String(tidb_meta_tidbvar::DXF_SCHEDULE_PAUSE_SCALE_IN.as_bytes())],
    )?;
    let mut flag = match rs.first().and_then(|row| row.first()) {
        // not set before, use default
        None => TtlFlag::default(),
        Some(value) => {
            let value = match value {
                tidb_datatype::Datum::String(value) => value.bytes().to_vec(),
                tidb_datatype::Datum::Bytes(value) => value.clone(),
                _ => Vec::new(),
            };
            serde_json::from_slice(&value).map_err(|error| HandleError::Other(error.to_string()))?
        }
    };
    // pause-scale-in flag is not cleaned up even if it is expired, reset it.
    if flag.enabled && flag.ttl_info.expire_time < chrono::Local::now().fixed_offset() {
        flag = TtlFlag::default();
    }
    Ok(flag)
}

#[cfg(test)]
mod tests {
    use tidb_dxf::{
        TaskBase, IMPORT_INTO, IMPORT_STEP_COLLECT_CONFLICTS, IMPORT_STEP_CONFLICT_RESOLUTION,
        IMPORT_STEP_ENCODE_AND_SORT, IMPORT_STEP_POST_PROCESS, STEP_ONE, TASK_TYPE_EXAMPLE,
    };

    use super::{calculate_required_nodes, get_needed_nodes};

    /// Go `TestCalculateRequiredNodes`.
    #[test]
    fn test_calculate_required_nodes() {
        // (cpuCount, [requiredSlots, max-node-count]..., expected)
        let cases: &[(isize, &[[isize; 2]], usize)] = &[
            // no task
            (8, &[], 1),
            // single task cases
            (8, &[[1, 1]], 1),
            (8, &[[1, 3]], 3),
            (8, &[[3, 1]], 1),
            (8, &[[8, 1]], 1),
            (8, &[[8, 4]], 4),
            // multiple tasks of requiredSlots < cpuCount
            (8, &[[1, 1], [2, 1], [2, 1], [3, 1]], 1),
            (8, &[[1, 3], [2, 4]], 4),
            (8, &[[1, 3], [2, 4], [6, 1]], 4),
            (8, &[[1, 3], [2, 4], [3, 2]], 4),
            (8, &[[1, 3], [2, 4], [3, 2], [2, 2]], 4),
            (8, &[[1, 3], [2, 4], [3, 2], [5, 2]], 4),
            (8, &[[1, 3], [2, 4], [3, 2], [2, 3]], 4),
            (8, &[[1, 3], [2, 4], [3, 2], [2, 3], [6, 1]], 4),
            (8, &[[3, 2], [5, 2]], 2),
            (8, &[[3, 2], [6, 2]], 4),
            // multiple tasks that use all available CPUs
            (8, &[[8, 4], [8, 6], [8, 5]], 15),
            (8, &[[8, 4], [8, 6], [8, 5], [8, 20]], 35),
            // mixing cases
            (8, &[[8, 5], [1, 3], [2, 4]], 9),
            (8, &[[8, 5], [1, 3], [2, 4], [8, 2]], 11),
            (8, &[[8, 5], [1, 3], [8, 2], [2, 4]], 11),
            (8, &[[3, 3], [8, 5], [6, 4]], 12),
            (8, &[[3, 3], [8, 5], [5, 4]], 9),
            (8, &[[3, 3], [5, 4], [8, 5]], 9),
        ];
        for (i, (cpu_count, params, expected)) in cases.iter().enumerate() {
            let tasks: Vec<TaskBase> = params
                .iter()
                .map(|[required_slots, max_node_count]| TaskBase {
                    required_slots: *required_slots,
                    max_node_count: *max_node_count,
                    ..TaskBase::default()
                })
                .collect();
            assert_eq!(calculate_required_nodes(&tasks, *cpu_count), *expected, "case-{i}");
        }
    }

    /// Go `TestGetNeededNodes`.
    #[test]
    fn test_get_needed_nodes() {
        let cases = [
            ("import-collect-conflicts", IMPORT_INTO, IMPORT_STEP_COLLECT_CONFLICTS, 8, 1),
            ("import-conflict-resolution", IMPORT_INTO, IMPORT_STEP_CONFLICT_RESOLUTION, 8, 1),
            ("import-post-process", IMPORT_INTO, IMPORT_STEP_POST_PROCESS, 8, 1),
            ("import-regular-step", IMPORT_INTO, IMPORT_STEP_ENCODE_AND_SORT, 6, 6),
            ("non-import-type", TASK_TYPE_EXAMPLE, STEP_ONE, 5, 5),
        ];
        for (name, tp, step, max_nodes, expected) in cases {
            let task = TaskBase { tp, step, max_node_count: max_nodes, ..TaskBase::default() };
            assert_eq!(get_needed_nodes(&task), expected, "{name}");
        }
    }
}
