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

//! Go `balancer_test.go`.

use std::collections::HashMap;
use std::sync::Arc;

use tidb_dxf::{ManagedNode, SubtaskBase, SubtaskState, Task, TaskBase, SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING};
use tidb_dxf_storage::StorageError;
use tidb_gocontext::Context;

use super::Balancer;
use crate::interface::{Error, Param};
use crate::mock::{MockScheduler, MockTaskManager};
use crate::scheduler::plain;
use crate::{NodeManager, Scheduler, SlotManager, TaskManager};

fn st(id: i64, exec_id: &str, state: &SubtaskState) -> SubtaskBase {
    SubtaskBase { id, exec_id: exec_id.to_owned(), state: state.clone(), ..SubtaskBase::default() }
}

fn slots(entries: &[(&str, isize)]) -> HashMap<String, isize> {
    entries.iter().map(|(id, n)| ((*id).to_owned(), *n)).collect()
}

fn strs(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|id| (*id).to_owned()).collect()
}

/// Subtasks compared as Go's `require.Equal` does on these fields.
fn view(subtasks: &[SubtaskBase]) -> Vec<(i64, String, String)> {
    subtasks.iter().map(|st| (st.id, st.exec_id.clone(), st.state.to_string())).collect()
}

/// Go `balanceTestCase`.
#[derive(Default)]
struct BalanceTestCase {
    subtasks: Vec<SubtaskBase>,
    eligible_nodes: Vec<String>,
    max_node_count: isize,
    init_used_slots: HashMap<String, isize>,
    expected_subtasks: Vec<SubtaskBase>,
    expected_used_slots: HashMap<String, isize>,
}

fn new_balancer(task_mgr: Arc<MockTaskManager>, slot_mgr: SlotManager) -> Balancer {
    Balancer::new(Param {
        task_mgr: task_mgr as Arc<dyn TaskManager>,
        node_mgr: Arc::new(NodeManager::new("")),
        slot_mgr: Arc::new(slot_mgr),
        server_id: String::new(),
        allocated_slots: false,
        node_res: None,
        task_runtime: None,
    })
}

fn slot_manager_of(capacity: isize) -> SlotManager {
    let slot_mgr = SlotManager::new();
    slot_mgr.update_capacity(capacity);
    slot_mgr
}

fn task_of(base: TaskBase) -> Task {
    Task { base, ..Task::default() }
}

/// Go `TestBalanceOneTask`.
#[test]
fn test_balance_one_task() {
    let test_cases = vec![

		// no subtasks to balance, no need to do anything.
		BalanceTestCase {
			subtasks:          vec![],
			eligible_nodes:     strs(&["tidb1"]),
			init_used_slots:     slots(&[("tidb1", 0)]),
			expected_subtasks:  vec![],
			expected_used_slots: slots(&[("tidb1", 0)]),
			..BalanceTestCase::default()
		},
		// balanced, no need to do anything.
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_RUNNING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
			],
			eligible_nodes: strs(&["tidb1", "tidb2"]),
			init_used_slots: slots(&[("tidb1", 0), ("tidb2", 0)]),
			expected_subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_RUNNING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
			],
			expected_used_slots: slots(&[("tidb1", 16), ("tidb2", 16)]),
			..BalanceTestCase::default()
		},
		// balanced case 2, make sure the remainder calculate part is right, so we don't
		// balance subtasks to 2:2:0
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb2", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_PENDING),
				st(3, "tidb1", &SUBTASK_STATE_PENDING),
				st(4, "tidb3", &SUBTASK_STATE_PENDING),
			],
			eligible_nodes: strs(&["tidb1", "tidb2", "tidb3"]),
			init_used_slots: slots(&[("tidb1", 0), ("tidb2", 0), ("tidb3", 0)]),
			expected_subtasks: vec![
				st(1, "tidb2", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_PENDING),
				st(3, "tidb1", &SUBTASK_STATE_PENDING),
				st(4, "tidb3", &SUBTASK_STATE_PENDING),
			],
			expected_used_slots: slots(&[("tidb1", 16), ("tidb2", 16), ("tidb3", 16)]),
			..BalanceTestCase::default()
		},
		// no eligible nodes to run those subtasks, leave it unbalanced.
		// used slots will not be changed.
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb1", &SUBTASK_STATE_PENDING),
			],
			eligible_nodes: strs(&["tidb1", "tidb2"]),
			init_used_slots: slots(&[("tidb1", 8), ("tidb2", 8)]),
			expected_subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb1", &SUBTASK_STATE_PENDING),
			],
			expected_used_slots: slots(&[("tidb1", 8), ("tidb2", 8)]),
			..BalanceTestCase::default()
		},
		// balance subtasks to eligible nodes, tidb1 has 8 used slots cannot run target subtasks.
		// all subtasks will be balanced to tidb2.
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb1", &SUBTASK_STATE_PENDING),
			],
			eligible_nodes: strs(&["tidb1", "tidb2"]),
			init_used_slots: slots(&[("tidb1", 8), ("tidb2", 0)]),
			expected_subtasks: vec![
				st(1, "tidb2", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_PENDING),
			],
			expected_used_slots: slots(&[("tidb1", 8), ("tidb2", 16)]),
			..BalanceTestCase::default()
		},
		// running subtasks are not re-scheduled if the node is eligible, we leave it un-balanced.
		// task executor should mark those subtasks as pending, then we can balance them.
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb1", &SUBTASK_STATE_RUNNING),
				st(3, "tidb1", &SUBTASK_STATE_RUNNING),
			],
			eligible_nodes: strs(&["tidb1", "tidb2"]),
			init_used_slots: slots(&[("tidb1", 0), ("tidb2", 0)]),
			expected_subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb1", &SUBTASK_STATE_RUNNING),
				st(3, "tidb1", &SUBTASK_STATE_RUNNING),
			],
			expected_used_slots: slots(&[("tidb1", 16), ("tidb2", 0)]),
			..BalanceTestCase::default()
		},
		// balance from 1:4 to 2:3
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_RUNNING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
				st(4, "tidb2", &SUBTASK_STATE_PENDING),
				st(5, "tidb2", &SUBTASK_STATE_PENDING),
			],
			eligible_nodes: strs(&["tidb1", "tidb2"]),
			init_used_slots: slots(&[("tidb1", 0), ("tidb2", 0)]),
			expected_subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_RUNNING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
				st(4, "tidb2", &SUBTASK_STATE_PENDING),
				st(5, "tidb1", &SUBTASK_STATE_PENDING),
			],
			expected_used_slots: slots(&[("tidb1", 16), ("tidb2", 16)]),
			..BalanceTestCase::default()
		},
		// scale out, balance from 5 to 2:2:1
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb1", &SUBTASK_STATE_PENDING),
				st(3, "tidb1", &SUBTASK_STATE_PENDING),
				st(4, "tidb1", &SUBTASK_STATE_PENDING),
				st(5, "tidb1", &SUBTASK_STATE_PENDING),
			],
			eligible_nodes: strs(&["tidb1", "tidb2", "tidb3"]),
			init_used_slots: slots(&[("tidb1", 0), ("tidb2", 0), ("tidb3", 0)]),
			expected_subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb1", &SUBTASK_STATE_PENDING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
				st(4, "tidb2", &SUBTASK_STATE_PENDING),
				st(5, "tidb3", &SUBTASK_STATE_PENDING),
			],
			expected_used_slots: slots(&[("tidb1", 16), ("tidb2", 16), ("tidb3", 16)]),
			..BalanceTestCase::default()
		},
		// scale out case 2: balance from 4 to 2:1:1
		// this case checks the remainder part is right, so we don't balance it as 2:2:0.
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb2", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_PENDING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
				st(4, "tidb2", &SUBTASK_STATE_PENDING),
			],
			eligible_nodes: strs(&["tidb1", "tidb2", "tidb3"]),
			init_used_slots: slots(&[("tidb1", 0), ("tidb2", 0), ("tidb3", 0)]),
			expected_subtasks: vec![
				st(1, "tidb2", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_PENDING),
				st(3, "tidb1", &SUBTASK_STATE_PENDING),
				st(4, "tidb3", &SUBTASK_STATE_PENDING),
			],
			expected_used_slots: slots(&[("tidb1", 16), ("tidb2", 16), ("tidb3", 16)]),
			..BalanceTestCase::default()
		},
		// scale in, balance from 1:3:1 to 3:2
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_RUNNING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
				st(4, "tidb2", &SUBTASK_STATE_PENDING),
				st(5, "tidb3", &SUBTASK_STATE_RUNNING),
			],
			eligible_nodes: strs(&["tidb1", "tidb3"]),
			init_used_slots: slots(&[("tidb1", 0), ("tidb3", 0)]),
			expected_subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb1", &SUBTASK_STATE_RUNNING),
				st(3, "tidb1", &SUBTASK_STATE_PENDING),
				st(4, "tidb3", &SUBTASK_STATE_PENDING),
				st(5, "tidb3", &SUBTASK_STATE_RUNNING),
			],
			expected_used_slots: slots(&[("tidb1", 16), ("tidb3", 16)]),
			..BalanceTestCase::default()
		},
		// scale in and out at the same time, balance from 2:1 to 2:1
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb1", &SUBTASK_STATE_PENDING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
			],
			eligible_nodes: strs(&["tidb2", "tidb3"]),
			init_used_slots: slots(&[("tidb2", 0), ("tidb3", 0)]),
			expected_subtasks: vec![
				st(1, "tidb2", &SUBTASK_STATE_RUNNING),
				st(2, "tidb3", &SUBTASK_STATE_PENDING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
			],
			expected_used_slots: slots(&[("tidb2", 16), ("tidb3", 16)]),
			..BalanceTestCase::default()
		},
		// balanced, but max node count is limited.
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_RUNNING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
			],
			max_node_count:  1,
			eligible_nodes: strs(&["tidb1", "tidb2"]),
			init_used_slots: slots(&[("tidb1", 0), ("tidb2", 0)]),
			expected_subtasks: vec![
				st(1, "tidb2", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_RUNNING),
				st(3, "tidb2", &SUBTASK_STATE_PENDING),
			],
			expected_used_slots: slots(&[("tidb1", 0), ("tidb2", 16)]),
			..BalanceTestCase::default()
		},
		// scale out, but max node count is limited.
		BalanceTestCase {
			subtasks: vec![
				st(1, "tidb3", &SUBTASK_STATE_RUNNING),
				st(2, "tidb3", &SUBTASK_STATE_PENDING),
				st(3, "tidb3", &SUBTASK_STATE_PENDING),
				st(4, "tidb3", &SUBTASK_STATE_PENDING),
				st(5, "tidb3", &SUBTASK_STATE_PENDING),
			],
			eligible_nodes: strs(&["tidb1", "tidb2", "tidb3"]),
			max_node_count:  2,
			init_used_slots: slots(&[("tidb1", 0), ("tidb2", 0), ("tidb3", 0)]),
			expected_subtasks: vec![
				st(1, "tidb3", &SUBTASK_STATE_RUNNING),
				st(2, "tidb3", &SUBTASK_STATE_PENDING),
				st(3, "tidb3", &SUBTASK_STATE_PENDING),
				st(4, "tidb1", &SUBTASK_STATE_PENDING),
				st(5, "tidb1", &SUBTASK_STATE_PENDING),
			],
			expected_used_slots: slots(&[("tidb1", 16), ("tidb2", 0), ("tidb3", 16)]),
			..BalanceTestCase::default()
		},
    ];

    let ctx = Context::background();
    for (i, mut c) in test_cases.into_iter().enumerate() {
        let mock_task_mgr = Arc::new(MockTaskManager::default());
        mock_task_mgr.expect::<tidb_dxf_storage::Result<Vec<SubtaskBase>>>("GetActiveSubtasks", Ok(c.subtasks.clone()));
        let needs_update = view(&c.subtasks) != view(&c.expected_subtasks);
        if needs_update {
            mock_task_mgr.expect::<tidb_dxf_storage::Result<()>>("UpdateSubtasksExecIDs", Ok(()));
        }
        let mock_scheduler = MockScheduler::new(task_of(TaskBase {
            id: 1,
            required_slots: 16,
            max_node_count: c.max_node_count,
            ..TaskBase::default()
        }));
        mock_scheduler.ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(Vec::new()));

        let b = new_balancer(Arc::clone(&mock_task_mgr), slot_manager_of(16));
        b.set_curr_used_slots(c.init_used_slots.clone());
        b.balance_subtasks(&ctx, &mock_scheduler, c.eligible_nodes.clone()).unwrap();
        assert_eq!(b.curr_used_slots(), c.expected_used_slots, "case {i}");
        // c.subtasks is updated in-place
        mock_task_mgr.apply_updated_exec_ids(&mut c.subtasks);
        assert_eq!(view(&c.subtasks), view(&c.expected_subtasks), "case {i}");
        mock_task_mgr.assert_satisfied();
        mock_scheduler.ext.assert_satisfied();
    }

    // scheduler err or no instance
    let mock_task_mgr = Arc::new(MockTaskManager::default());
    let mock_scheduler = MockScheduler::new(task_of(TaskBase { id: 1, ..TaskBase::default() }));
    mock_scheduler.ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Err(plain("mock error")));
    let b = new_balancer(Arc::clone(&mock_task_mgr), slot_manager_of(16));
    let error = b.balance_subtasks(&ctx, &mock_scheduler, strs(&["tidb1"])).unwrap_err();
    assert!(error.to_string().contains("mock error"));
    mock_scheduler.ext.assert_satisfied();

    mock_scheduler.ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(Vec::new()));
    let error = b.balance_subtasks(&ctx, &mock_scheduler, Vec::new()).unwrap_err();
    assert!(error.to_string().contains("no eligible nodes to balance subtasks"));
    mock_scheduler.ext.assert_satisfied();

    // task mgr failed
    let mock_task_mgr = Arc::new(MockTaskManager::default());
    mock_task_mgr.expect::<tidb_dxf_storage::Result<Vec<SubtaskBase>>>("GetActiveSubtasks", Err(StorageError::Other("mock error".to_owned())));
    let mock_scheduler = MockScheduler::new(task_of(TaskBase { id: 1, required_slots: 16, ..TaskBase::default() }));
    mock_scheduler.ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(strs(&["tidb1"])));

    let b = new_balancer(Arc::clone(&mock_task_mgr), slot_manager_of(16));
    let error = b.balance_subtasks(&ctx, &mock_scheduler, strs(&["tidb1"])).unwrap_err();
    assert!(error.to_string().contains("mock error"));
    mock_task_mgr.assert_satisfied();
    mock_scheduler.ext.assert_satisfied();

    b.set_curr_used_slots(slots(&[("tidb1", 0), ("tidb2", 0)]));
    mock_task_mgr.expect::<tidb_dxf_storage::Result<Vec<SubtaskBase>>>(
        "GetActiveSubtasks",
        Ok(vec![st(1, "tidb1", &SUBTASK_STATE_RUNNING), st(2, "tidb1", &SUBTASK_STATE_PENDING)]),
    );
    mock_task_mgr.expect::<tidb_dxf_storage::Result<()>>("UpdateSubtasksExecIDs", Err(StorageError::Other("mock error2".to_owned())));
    mock_scheduler.ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(Vec::new()));
    let error = b.balance_subtasks(&ctx, &mock_scheduler, strs(&["tidb1", "tidb2"])).unwrap_err();
    assert!(error.to_string().contains("mock error2"));
    // not updated
    assert_eq!(b.curr_used_slots(), slots(&[("tidb1", 0), ("tidb2", 0)]));
    mock_task_mgr.assert_satisfied();
    mock_scheduler.ext.assert_satisfied();
}

/// Go's `TestBalanceMultipleTasks` case.
#[derive(Default)]
struct TaskCase {
    required_slots: isize,
    subtasks: Vec<SubtaskBase>,
    expected_subtasks: Vec<SubtaskBase>,
}

/// Go `TestBalanceMultipleTasks`.
#[test]
fn test_balance_multiple_tasks() {
    let mock_task_mgr = Arc::new(MockTaskManager::default());

    let mut task_cases = vec![

		// task 1 is balanced
		// used slots will be {tidb1: 8, tidb2: 8, tidb3: 0}
		TaskCase {
			required_slots: 8,
			subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_RUNNING),
			],
			expected_subtasks: vec![
				st(1, "tidb1", &SUBTASK_STATE_RUNNING),
				st(2, "tidb2", &SUBTASK_STATE_RUNNING),
			],
			..TaskCase::default()
		},
		// task 2 require balance
		// used slots will be {tidb1: 16, tidb2: 16, tidb3: 8}
		TaskCase {
			required_slots: 8,
			subtasks: vec![
				st(3, "tidb1", &SUBTASK_STATE_RUNNING),
				st(4, "tidb4", &SUBTASK_STATE_RUNNING),
				st(5, "tidb4", &SUBTASK_STATE_PENDING),
			],
			expected_subtasks: vec![
				st(3, "tidb1", &SUBTASK_STATE_RUNNING),
				st(4, "tidb2", &SUBTASK_STATE_RUNNING),
				st(5, "tidb3", &SUBTASK_STATE_PENDING),
			],
			..TaskCase::default()
		},
		// task 3 require balance, but no eligible node, so it's not balanced, and
		// used slots are not updated
		// used slots will be {tidb1: 16, tidb2: 16, tidb3: 8}
		TaskCase {
			required_slots: 16,
			subtasks: vec![
				st(6, "tidb4", &SUBTASK_STATE_PENDING),
				st(7, "tidb4", &SUBTASK_STATE_PENDING),
			],
			expected_subtasks: vec![
				st(6, "tidb4", &SUBTASK_STATE_PENDING),
				st(7, "tidb4", &SUBTASK_STATE_PENDING),
			],
			..TaskCase::default()
		},
		// task 4 require balance
		// used slots will be {tidb1: 16, tidb2: 16, tidb3: 16}
		TaskCase {
			required_slots: 8,
			subtasks: vec![
				st(8, "tidb1", &SUBTASK_STATE_PENDING),
			],
			expected_subtasks: vec![
				st(8, "tidb3", &SUBTASK_STATE_PENDING),
			],
			..TaskCase::default()
		},
    ];
    let ctx = Context::background();

    let manager = crate::Manager::new(
        &ctx,
        Arc::clone(&mock_task_mgr) as Arc<dyn TaskManager>,
        "1",
        tidb_dxf::NODE_RESOURCE_FOR_TEST.clone(),
        Arc::new(crate::mock::MockServerInfo::new(&[])),
    );
    manager.slot_mgr().update_capacity(16);
    let node = |id: &str| ManagedNode { id: id.to_owned(), ..ManagedNode::default() };
    manager.node_mgr().store_nodes(vec![node("tidb1"), node("tidb2"), node("tidb3")]);
    let b = Balancer::new(Param {
        task_mgr: Arc::clone(&mock_task_mgr) as Arc<dyn TaskManager>,
        node_mgr: Arc::clone(manager.node_mgr()),
        slot_mgr: Arc::clone(manager.slot_mgr()),
        server_id: String::new(),
        allocated_slots: false,
        node_res: None,
        task_runtime: None,
    });
    let mut schedulers = Vec::new();
    for (i, tc) in task_cases.iter().enumerate() {
        let task_id = i as i64 + 1;
        let sch = Arc::new(MockScheduler::new(task_of(TaskBase { id: task_id, required_slots: tc.required_slots, ..TaskBase::default() })));
        manager.add_scheduler_for_test(task_id, Arc::clone(&sch) as Arc<dyn Scheduler>);
        schedulers.push(sch);
    }
    assert_eq!(manager.get_schedulers().len(), 4);

    // fail fast if balance failed on some task
    schedulers[0].ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Err(plain("mock error")));
    b.balance(&ctx, &manager);
    for sch in &schedulers {
        sch.ext.assert_satisfied();
    }
    mock_task_mgr.assert_satisfied();

    // balance multiple tasks
    for (i, c) in task_cases.iter().enumerate() {
        schedulers[i].ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(Vec::new()));
        mock_task_mgr.expect::<tidb_dxf_storage::Result<Vec<SubtaskBase>>>("GetActiveSubtasks", Ok(c.subtasks.clone()));
        if view(&c.subtasks) != view(&c.expected_subtasks) {
            mock_task_mgr.expect::<tidb_dxf_storage::Result<()>>("UpdateSubtasksExecIDs", Ok(()));
        }
    }
    b.balance(&ctx, &manager);
    assert_eq!(b.curr_used_slots(), slots(&[("tidb1", 16), ("tidb2", 16), ("tidb3", 16)]));
    mock_task_mgr.assert_satisfied();
    for sch in &schedulers {
        sch.ext.assert_satisfied();
    }
    for c in &mut task_cases {
        mock_task_mgr.apply_updated_exec_ids(&mut c.subtasks);
        assert_eq!(view(&c.subtasks), view(&c.expected_subtasks));
    }
}

/// Go `TestBalancerUpdateUsedNodes`.
#[test]
fn test_balancer_update_used_nodes() {
    let b = new_balancer(Arc::new(MockTaskManager::default()), SlotManager::new());
    b.update_used_nodes(
        &TaskBase { required_slots: 16, ..TaskBase::default() },
        &[st(1, "tidb1", &SUBTASK_STATE_RUNNING), st(2, "tidb1", &SUBTASK_STATE_PENDING)],
    );
    assert_eq!(b.curr_used_slots(), slots(&[("tidb1", 16)]));
    b.update_used_nodes(
        &TaskBase { required_slots: 12, ..TaskBase::default() },
        &[st(3, "tidb1", &SUBTASK_STATE_RUNNING), st(4, "tidb2", &SUBTASK_STATE_PENDING), st(5, "tidb3", &SUBTASK_STATE_PENDING)],
    );
    assert_eq!(b.curr_used_slots(), slots(&[("tidb1", 28), ("tidb2", 12), ("tidb3", 12)]));
}
