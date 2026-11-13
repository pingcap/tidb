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

//! Go `balancer.go`.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use tidb_dxf::{SubtaskBase, Task, TaskBase, SUBTASK_STATE_RUNNING};
use tidb_gocontext::Context;
use tidb_log::{Field, Value};

use crate::interface::{Error, Param};
use crate::nodes::filter_by_scope;
use crate::scheduler::{get_eligible_nodes, plain};
use crate::scheduler_manager::Manager;
use crate::slots::filter_nodes_with_enough_slots;
use crate::Scheduler;

/// Go `balanceCheckInterval`: the interval of checking whether to balance
/// subtasks.
fn balance_check_interval() -> Duration {
    3 * crate::scheduler::check_task_finished_interval()
}

/// Go `balancer`: balances subtasks over the managed nodes, when nodes scale
/// in or out, and when nodes run their subtasks at different speeds or the
/// data of subtasks differs. Tasks are balanced in task order; subtasks go to
/// nodes with enough slots, and a task without such nodes is skipped.
pub(crate) struct Balancer {
    param: Param,
    logger: tidb_logutil::Logger,
    /// The used slots of each node during a balance, to avoid passing it
    /// around.
    curr_used_slots: Mutex<HashMap<String, isize>>,
}

impl Balancer {
    /// Go `newBalancer`.
    pub(crate) fn new(param: Param) -> Self {
        let mut logger = tidb_logutil::err_verbose_logger();
        if tidb_intest::IN_TEST {
            logger = logger.with_fields(&[Field::new("server-id", Value::Str(param.server_id.clone()))]);
        }
        Self { param, logger, curr_used_slots: Mutex::new(HashMap::new()) }
    }

    /// Go `balanceLoop`.
    pub(crate) fn balance_loop(&self, ctx: &Context, sm: &Manager) {
        loop {
            if ctx.sleep(balance_check_interval()) {
                return;
            }
            self.balance(ctx, sm);
        }
    }

    /// Go `balance`.
    pub(crate) fn balance(&self, ctx: &Context, sm: &Manager) {
        // we will use currUsedSlots to calculate adjusted eligible nodes during balance,
        // it's initial value depends on the managed nodes, to have a consistent view,
        // DO NOT call getNodes twice during 1 balance.
        let managed_nodes = self.param.node_mgr.get_nodes();
        *self.curr_used_slots.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
            managed_nodes.iter().map(|n| (n.id.clone(), 0)).collect();

        for sch in sm.get_schedulers() {
            let task = sch.get_task();
            let node_ids = filter_by_scope(&managed_nodes, &task.base.target_scope);
            if let Err(err) = self.balance_subtasks(ctx, sch.as_ref(), node_ids) {
                self.logger.warn(
                    "failed to balance subtasks",
                    &[
                        Field::new("task-id", Value::I64(task.base.id)),
                        Field::new("task-key", Value::Str(task.base.key.clone())),
                        Field::new("error", Value::Str(err.to_string())),
                    ],
                );
                return;
            }
        }
    }

    /// Go `balanceSubtasks`.
    pub(crate) fn balance_subtasks(&self, ctx: &Context, sch: &dyn Scheduler, managed_nodes: Vec<String>) -> Result<(), Error> {
        let task = sch.get_task();
        let eligible_nodes = get_eligible_nodes(ctx, sch, &task, managed_nodes)?;
        if eligible_nodes.is_empty() {
            return Err(plain("no eligible nodes to balance subtasks"));
        }
        self.do_balance_subtasks(ctx, &task, eligible_nodes)
    }

    /// Go `doBalanceSubtasks`.
    pub(crate) fn do_balance_subtasks(&self, ctx: &tidb_gocontext::Context, task: &Task, eligible_nodes: Vec<String>) -> Result<(), Error> {
        let subtasks = self.param.task_mgr.get_active_subtasks(ctx, task.base.id).map_err(|err| std::sync::Arc::new(err) as Error)?;
        if subtasks.is_empty() {
            return Ok(());
        }

        // balance subtasks only to nodes with enough slots, from the view of all
        // managed nodes, subtasks of task might not be balanced.
        let curr_used_slots = self.curr_used_slots.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        #[allow(unused_mut)]
        let mut adjusted_nodes =
            filter_nodes_with_enough_slots(&curr_used_slots, self.param.slot_mgr.get_capacity(), &eligible_nodes, task.base.required_slots);
        #[cfg(any(test, feature = "failpoints"))]
        tidb_failpoint::inject("github.com/pingcap/tidb/pkg/dxf/framework/scheduler/mockNoEnoughSlots", |_| adjusted_nodes = Vec::new());
        let adjusted_nodes = filter_nodes_by_max_node_cnt(adjusted_nodes, &subtasks, task.base.max_node_count);
        if adjusted_nodes.is_empty() {
            // no node has enough slots to run the subtasks, skip balance and skip
            // update used slots.
            return Ok(());
        }
        let adjusted_node_map: HashSet<&String> = adjusted_nodes.iter().collect();
        // Go balances the subtasks through pointers, so the exec IDs it
        // rewrites are the subtasks' own, which updateUsedNodes then counts;
        // here the lists hold indexes into `subtasks`.
        let mut subtasks = subtasks;

        let average_subtask_cnt = subtasks.len() / adjusted_nodes.len();
        let average_subtask_remainder = subtasks.len() - average_subtask_cnt * adjusted_nodes.len();
        let mut executor_subtasks: HashMap<String, Vec<usize>> =
            adjusted_nodes.iter().map(|node| (node.clone(), Vec::with_capacity(average_subtask_cnt + 1))).collect();
        let mut executor_pending_cnts: HashMap<String, usize> = HashMap::with_capacity(adjusted_nodes.len());
        for (idx, subtask) in subtasks.iter().enumerate() {
            // put running subtask in the front of slice.
            // if subtask fail-over, it's possible that there are multiple running
            // subtasks for one task executor.
            let sts = executor_subtasks.entry(subtask.exec_id.clone()).or_default();
            if subtask.state == SUBTASK_STATE_RUNNING {
                sts.insert(0, idx);
            } else {
                sts.push(idx);
                *executor_pending_cnts.entry(subtask.exec_id.clone()).or_insert(0) += 1;
            }
        }

        let mut subtasks_need_schedule: Vec<usize> = Vec::new();
        let mut remainder = average_subtask_remainder;
        let mut executor_with_one_more_subtask: HashSet<String> = HashSet::with_capacity(remainder);
        let nodes: Vec<String> = executor_subtasks.keys().cloned().collect();
        for node in nodes {
            let sts = executor_subtasks.get(&node).expect("listed above").clone();
            if !adjusted_node_map.contains(&node) {
                self.logger.info(
                    "dead node or not have enough slots, schedule subtasks away",
                    &[
                        Field::new("task-id", Value::I64(task.base.id)),
                        Field::new("task-key", Value::Str(task.base.key.clone())),
                        Field::new("node", Value::Str(node.clone())),
                        Field::new("slot-capacity", Value::I64(self.param.slot_mgr.get_capacity() as i64)),
                        Field::new("used-slots", Value::I64(curr_used_slots.get(&node).copied().unwrap_or(0) as i64)),
                    ],
                );
                // dead node or not have enough slots
                subtasks_need_schedule.extend(sts);
                executor_subtasks.remove(&node);
                continue;
            }
            let pending = executor_pending_cnts.get(&node).copied().unwrap_or(0);
            if remainder > 0 {
                // first remainder nodes will get 1 more subtask.
                if sts.len() >= average_subtask_cnt + 1 {
                    let need_schedule_cnt = sts.len() - (average_subtask_cnt + 1);
                    // running subtasks are never balanced.
                    let need_schedule_cnt = pending.min(need_schedule_cnt);
                    subtasks_need_schedule.extend_from_slice(&sts[sts.len() - need_schedule_cnt..]);
                    executor_subtasks.insert(node.clone(), sts[..sts.len() - need_schedule_cnt].to_vec());

                    executor_with_one_more_subtask.insert(node);
                    remainder -= 1;
                }
            } else if sts.len() > average_subtask_cnt {
                // running subtasks are never balanced.
                let cnt = pending.min(sts.len() - average_subtask_cnt);
                subtasks_need_schedule.extend_from_slice(&sts[sts.len() - cnt..]);
                executor_subtasks.insert(node, sts[..sts.len() - cnt].to_vec());
            }
        }
        if subtasks_need_schedule.is_empty() {
            self.update_used_nodes(&task.base, &subtasks);
            return Ok(());
        }

        for node in &adjusted_nodes {
            if remainder == 0 {
                break;
            }
            if executor_with_one_more_subtask.insert(node.clone()) {
                remainder -= 1;
            }
        }

        let mut fill_idx = 0;
        for node in &adjusted_nodes {
            let sts_len = executor_subtasks.get(node).map_or(0, Vec::len);
            let target_subtask_cnt =
                if executor_with_one_more_subtask.contains(node) { average_subtask_cnt + 1 } else { average_subtask_cnt };
            let mut i = sts_len;
            while i < target_subtask_cnt && fill_idx < subtasks_need_schedule.len() {
                subtasks[subtasks_need_schedule[fill_idx]].exec_id = node.clone();
                fill_idx += 1;
                i += 1;
            }
        }

        let rescheduled: Vec<SubtaskBase> = subtasks_need_schedule.iter().map(|idx| subtasks[*idx].clone()).collect();
        self.param.task_mgr.update_subtasks_exec_ids(ctx, &rescheduled).map_err(|err| std::sync::Arc::new(err) as Error)?;
        self.logger.info(
            "balance subtasks",
            &[Field::new(
                "subtasks",
                Value::Str(format!("{:?}", rescheduled.iter().map(ToString::to_string).collect::<Vec<_>>())),
            )],
        );
        self.update_used_nodes(&task.base, &subtasks);
        Ok(())
    }

    /// Go `updateUsedNodes`: see the task executor's `slotManager.alloc`.
    pub(crate) fn update_used_nodes(&self, task: &TaskBase, subtasks: &[SubtaskBase]) {
        let mut used: HashMap<&str, isize> = HashMap::new();
        for st in subtasks {
            used.entry(st.exec_id.as_str()).or_insert(task.required_slots);
        }

        let mut curr_used_slots = self.curr_used_slots.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        for (node, slots) in used {
            *curr_used_slots.entry(node.to_owned()).or_insert(0) += slots;
        }
    }

    #[cfg(test)]
    pub(crate) fn curr_used_slots(&self) -> HashMap<String, isize> {
        self.curr_used_slots.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn set_curr_used_slots(&self, slots: HashMap<String, isize>) {
        *self.curr_used_slots.lock().unwrap() = slots;
    }
}

/// Go `filterNodesByMaxNodeCnt`: the max-node-count nodes with the most
/// subtasks of the task.
pub(crate) fn filter_nodes_by_max_node_cnt(mut nodes: Vec<String>, subtasks: &[SubtaskBase], max_node_cnt: isize) -> Vec<String> {
    if max_node_cnt == 0 || nodes.len() <= max_node_cnt as usize {
        return nodes;
    }
    // Order nodes by subtask count.
    let mut node_subtask_cnt: HashMap<&str, usize> = HashMap::with_capacity(nodes.len());
    for st in subtasks {
        *node_subtask_cnt.entry(st.exec_id.as_str()).or_insert(0) += 1;
    }
    let cnt = |node: &String| node_subtask_cnt.get(node.as_str()).copied().unwrap_or(0);
    nodes.sort_by(|a, b| cnt(b).cmp(&cnt(a)));
    nodes.truncate(max_node_cnt as usize);
    nodes
}

#[cfg(test)]
#[path = "balancer_tests.rs"]
mod tests;
