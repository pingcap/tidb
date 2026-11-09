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

//! Go `slots.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, RwLock};

use tidb_dxf::TaskBase;
use tidb_log::{Field, Value};

use crate::{NodeManager, TaskManager};

/// Go `taskStripes`.
#[derive(Debug, Clone)]
struct TaskStripes {
    task: TaskBase,
    stripes: isize,
}

#[derive(Debug, Default)]
struct Reserved {
    /// The stripes reserved by each task, in task order. Reserving by a
    /// task's minimum resource still appends here, so the sum might exceed
    /// the capacity.
    reserved_stripes: Vec<TaskStripes>,
    /// `reserved_stripes`' index of each task, for fast deletes.
    task2_index: HashMap<i64, usize>,
    /// The slots each task reserved on each node. The exec ID is only used to
    /// reserve the minimum resource when starting a scheduler; the subtasks
    /// may or may not be scheduled on that node.
    reserved_slots: HashMap<String, isize>,
}

/// Go `SlotManager`: the resource slots and stripes.
///
/// A slot is the resource unit of the framework on each node: one CPU core,
/// and that core's share of memory, disk, etc. A stripe is the unit across
/// nodes: one slot on every managed node. The number of stripes equals the
/// number of slots on each node, as the managed nodes are assumed
/// isomorphic. Stripes reserved for a task bound the resources it can use,
/// though it might not use them all; to maximize utilization, as many tasks
/// as the used slots of each node and the tasks' minimum resources allow
/// are scheduled, regardless of task order.
#[derive(Debug)]
pub struct SlotManager {
    /// The total number of slots and stripes.
    capacity: AtomicI32,
    mu: RwLock<Reserved>,
    /// The slots taken on each node. It might exceed the capacity: the
    /// current step of a high ranking task has few subtasks, so tasks of low
    /// ranking get scheduled, but its next step has many. Once initialized,
    /// it has one entry per managed node.
    used_slots: RwLock<Arc<HashMap<String, isize>>>,
}

impl SlotManager {
    /// Go `newSlotManager`. This node might not be managed by the framework,
    /// but the capacity starts as its CPU count, and the node manager updates
    /// it.
    pub(crate) fn new() -> Self {
        let s = Self {
            capacity: AtomicI32::new(0),
            mu: RwLock::new(Reserved::default()),
            used_slots: RwLock::new(Arc::new(HashMap::new())),
        };
        s.update_capacity(tidb_cpu::get_cpu_count() as isize);
        s
    }

    fn load_used_slots(&self) -> Arc<HashMap<String, isize>> {
        Arc::clone(&self.used_slots.read().unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    pub(crate) fn store_used_slots(&self, used_slots: HashMap<String, isize>) {
        *self.used_slots.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(used_slots);
    }

    /// Go `update`: the used slots of each managed node.
    pub(crate) fn update(&self, ctx: &tidb_gocontext::Context, node_mgr: &NodeManager, task_mgr: &dyn TaskManager) -> tidb_dxf_storage::Result<()> {
        let nodes = node_mgr.get_nodes();
        let slots_on_nodes = task_mgr.get_used_slots_on_nodes(ctx)?;
        let new_used_slots =
            nodes.iter().map(|node| (node.id.clone(), slots_on_nodes.get(&node.id).copied().unwrap_or(0))).collect();
        self.store_used_slots(new_used_slots);
        Ok(())
    }

    /// Go `canReserve`: whether the task has enough resources, and the exec
    /// ID it reserves on when it reserves by slots ("" by stripes). The used
    /// slots update asynchronously, so it might refuse when resources
    /// suffice, or accept on a shortage while some task schedules subtasks.
    pub(crate) fn can_reserve(&self, task: &TaskBase) -> (String, bool) {
        if tidb_config_kerneltype::is_next_gen() {
            // in nextgen kernel, node resource will be scaled automatically by cluster
            // controller which will use the schedule status API to make sure the
            // required resource is available, so we always return true here.
            return (String::new(), true);
        }
        let used_slots = self.load_used_slots();
        let capacity = self.get_capacity();
        let reserved = self.mu.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        if used_slots.is_empty() {
            // no node managed by dist framework
            return (String::new(), false);
        }

        let mut reserved_for_higher_rank = 0;
        for s in &reserved.reserved_stripes {
            if s.task.compare(task) != std::cmp::Ordering::Less {
                break;
            }
            reserved_for_higher_rank += s.stripes;
        }
        if task.required_slots + reserved_for_higher_rank <= capacity {
            return (String::new(), true);
        }

        for (id, count) in used_slots.iter() {
            if count + reserved.reserved_slots.get(id).copied().unwrap_or(0) + task.required_slots <= capacity {
                return (id.clone(), true);
            }
        }
        (String::new(), false)
    }

    /// Go `reserve`: reserves resources for the task; pairs with
    /// [`Self::un_reserve`] of the same arguments.
    pub(crate) fn reserve(&self, task: &TaskBase, exec_id: &str) {
        if tidb_config_kerneltype::is_next_gen() {
            // see comments in can_reserve.
            return;
        }
        let task_clone = task.clone();

        let mut reserved = self.mu.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let stripes = task_clone.required_slots;
        reserved.reserved_stripes.push(TaskStripes { task: task_clone, stripes });
        reserved.reserved_stripes.sort_by(|a, b| a.task.compare(&b.task));
        let indexes: Vec<(i64, usize)> =
            reserved.reserved_stripes.iter().enumerate().map(|(i, s)| (s.task.id, i)).collect();
        reserved.task2_index.extend(indexes);

        if !exec_id.is_empty() {
            *reserved.reserved_slots.entry(exec_id.to_owned()).or_insert(0) += stripes;
        }
    }

    /// Go `unReserve`: releases the task's resources.
    pub(crate) fn un_reserve(&self, task: &TaskBase, exec_id: &str) {
        if tidb_config_kerneltype::is_next_gen() {
            // see comments in can_reserve.
            return;
        }
        let mut reserved = self.mu.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(idx) = reserved.task2_index.get(&task.id).copied() else {
            return;
        };
        reserved.reserved_stripes.remove(idx);
        reserved.task2_index.remove(&task.id);
        let indexes: Vec<(i64, usize)> =
            reserved.reserved_stripes.iter().enumerate().map(|(i, s)| (s.task.id, i)).collect();
        reserved.task2_index.extend(indexes);

        if !exec_id.is_empty() {
            let slots = reserved.reserved_slots.entry(exec_id.to_owned()).or_insert(0);
            *slots -= task.required_slots;
            if *slots == 0 {
                reserved.reserved_slots.remove(exec_id);
            }
        }
    }

    /// Go `getCapacity`.
    pub(crate) fn get_capacity(&self) -> isize {
        self.capacity.load(Ordering::SeqCst) as isize
    }

    /// Go `adjustEligibleNodes`: the nodes with enough slots first; all the
    /// eligible nodes when there are none.
    pub(crate) fn adjust_eligible_nodes(&self, eligible_nodes: &[String], required_slot: isize) -> Vec<String> {
        let used_slots = self.load_used_slots();
        let nodes = filter_nodes_with_enough_slots(&used_slots, self.get_capacity(), eligible_nodes, required_slot);
        if nodes.is_empty() {
            return eligible_nodes.to_vec();
        }
        nodes
    }

    /// Go `updateCapacity`.
    pub(crate) fn update_capacity(&self, cpu_count: isize) {
        let old = self.capacity.load(Ordering::SeqCst) as isize;
        if cpu_count > 0 && cpu_count != old {
            self.capacity.store(cpu_count as i32, Ordering::SeqCst);
            if old == 0 {
                tidb_logutil::bg_logger().info("initialize slot capacity", &[Field::new("capacity", Value::I64(cpu_count as i64))]);
            } else {
                tidb_logutil::bg_logger().info(
                    "update slot capacity",
                    &[Field::new("old", Value::I64(old as i64)), Field::new("new", Value::I64(cpu_count as i64))],
                );
            }
        }
    }
}

/// Go `filterNodesWithEnoughSlots`.
pub(crate) fn filter_nodes_with_enough_slots(
    used_slots: &HashMap<String, isize>,
    capacity: isize,
    eligible_nodes: &[String],
    required_slots: isize,
) -> Vec<String> {
    eligible_nodes
        .iter()
        .filter(|node| used_slots.get(*node).is_some_and(|slots| slots + required_slots <= capacity))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tidb_dxf::{ManagedNode, TaskBase, NORMAL_PRIORITY};

    use super::SlotManager;
    use crate::mock::MockTaskManager;
    use crate::NodeManager;

    fn slots(entries: &[(&str, isize)]) -> HashMap<String, isize> {
        entries.iter().map(|(id, n)| ((*id).to_owned(), *n)).collect()
    }

    impl SlotManager {
        fn reserved_stripes(&self) -> Vec<isize> {
            self.mu.read().unwrap().reserved_stripes.iter().map(|s| s.stripes).collect()
        }
        fn task2_index(&self) -> HashMap<i64, usize> {
            self.mu.read().unwrap().task2_index.clone()
        }
        fn reserved_slots(&self) -> HashMap<String, isize> {
            self.mu.read().unwrap().reserved_slots.clone()
        }
    }

    /// Go `TestSlotManagerReserveNextGen`.
    #[test]
    fn test_slot_manager_reserve_next_gen() {
        if tidb_config_kerneltype::is_classic() {
            // this test is for nextgen kernel only
            return;
        }
        let sm = SlotManager::new();
        sm.update_capacity(16);
        // no node
        // this case will return false in classic kernel, but in nextgen kernel, we
        // will not reserve slots, and always return true.
        let (node_id, ok) = sm.can_reserve(&TaskBase { required_slots: 1, ..TaskBase::default() });
        assert!(ok);
        assert_eq!(node_id, "");
    }

    /// Go `TestSlotManagerReserve`.
    #[test]
    fn test_slot_manager_reserve() {
        if tidb_config_kerneltype::is_next_gen() {
            // in nextgen kernel, we will start scheduler without reserving slots, as
            // cluster controller will scale nodes on demand
            return;
        }
        let sm = SlotManager::new();
        sm.update_capacity(16);
        // no node
        assert!(!sm.can_reserve(&TaskBase { required_slots: 1, ..TaskBase::default() }).1);

        // reserve by stripes
        sm.store_used_slots(slots(&[("tidb-1", 16)]));
        let task = TaskBase {
            priority: NORMAL_PRIORITY,
            required_slots: 16,
            create_time: chrono::Local::now().fixed_offset(),
            ..TaskBase::default()
        };
        let with = |id: i64, required_slots: isize| TaskBase { id, required_slots, ..task.clone() };
        let task10 = with(10, 4);
        let (exec_id10, ok) = sm.can_reserve(&task10);
        assert_eq!(exec_id10, "");
        assert!(ok);
        sm.reserve(&task10, &exec_id10);

        let task20 = with(20, 8);
        let (exec_id20, ok) = sm.can_reserve(&task20);
        assert_eq!(exec_id20, "");
        assert!(ok);
        sm.reserve(&task20, &exec_id20);

        let task30 = with(30, 8);
        let (exec_id30, ok) = sm.can_reserve(&task30);
        assert_eq!(exec_id30, "");
        assert!(!ok);
        assert_eq!(sm.reserved_stripes(), [4, 8]);
        assert_eq!(sm.task2_index(), HashMap::from([(10, 0), (20, 1)]));
        assert!(sm.reserved_slots().is_empty());
        // high ranking task can preempt lower rank task
        let task9 = with(9, 16);
        assert!(sm.can_reserve(&task9).1);
        // 4 slots are reserved for high ranking tasks, so cannot reserve.
        let mut task11 = with(11, 16);
        assert!(!sm.can_reserve(&task11).1);
        // lower required slots
        task11.required_slots = 12;
        assert!(sm.can_reserve(&task11).1);

        // reserve by slots
        sm.store_used_slots(slots(&[("tidb-1", 12), ("tidb-2", 8)]));
        let mut task40 = with(40, 16);
        let (exec_id40, ok) = sm.can_reserve(&task40);
        assert_eq!(exec_id40, "");
        assert!(!ok);
        task40.required_slots = 8;
        let (exec_id40, ok) = sm.can_reserve(&task40);
        assert_eq!(exec_id40, "tidb-2");
        assert!(ok);
        sm.reserve(&task40, &exec_id40);
        assert_eq!(sm.reserved_stripes(), [4, 8, 8]);
        assert_eq!(sm.task2_index(), HashMap::from([(10, 0), (20, 1), (40, 2)]));
        assert_eq!(sm.reserved_slots(), slots(&[("tidb-2", 8)]));
        // high ranking task stop task 15 to run
        let task15 = with(15, 16);
        let (exec_id15, ok) = sm.can_reserve(&task15);
        assert_eq!(exec_id15, "");
        assert!(!ok);
        // finish task of id 10
        sm.un_reserve(&task10, &exec_id10);
        assert_eq!(sm.reserved_stripes(), [8, 8]);
        assert_eq!(sm.task2_index(), HashMap::from([(20, 0), (40, 1)]));
        assert_eq!(sm.reserved_slots(), slots(&[("tidb-2", 8)]));
        // now task 15 can run
        let (exec_id15, ok) = sm.can_reserve(&task15);
        assert_eq!(exec_id15, "");
        assert!(ok);
        sm.reserve(&task15, &exec_id15);
        assert_eq!(sm.reserved_stripes(), [16, 8, 8]);
        assert_eq!(sm.task2_index(), HashMap::from([(15, 0), (20, 1), (40, 2)]));
        assert_eq!(sm.reserved_slots(), slots(&[("tidb-2", 8)]));
        // task 50 cannot run
        let task50 = with(50, 8);
        assert!(!sm.can_reserve(&task50).1);
        // finish task 40
        sm.un_reserve(&task40, &exec_id40);
        assert_eq!(sm.reserved_stripes(), [16, 8]);
        assert_eq!(sm.task2_index(), HashMap::from([(15, 0), (20, 1)]));
        assert!(sm.reserved_slots().is_empty());
        // now task 50 can run
        let (exec_id50, ok) = sm.can_reserve(&task50);
        assert_eq!(exec_id50, "tidb-2");
        assert!(ok);
        sm.reserve(&task50, &exec_id50);
        // task 60 can run too
        let task60 = with(60, 4);
        let (exec_id60, ok) = sm.can_reserve(&task60);
        assert_eq!(exec_id60, "tidb-1");
        assert!(ok);
        sm.reserve(&task60, &exec_id60);
        assert_eq!(sm.reserved_stripes(), [16, 8, 8, 4]);
        assert_eq!(sm.task2_index(), HashMap::from([(15, 0), (20, 1), (50, 2), (60, 3)]));
        assert_eq!(sm.reserved_slots(), slots(&[("tidb-1", 4), ("tidb-2", 8)]));

        // un-reserve all tasks
        sm.un_reserve(&task15, &exec_id15);
        sm.un_reserve(&task20, &exec_id20);
        sm.un_reserve(&task50, &exec_id50);
        sm.un_reserve(&task60, &exec_id60);
        assert!(sm.reserved_stripes().is_empty());
        assert!(sm.task2_index().is_empty());
        assert!(sm.reserved_slots().is_empty());
    }

    /// Go `TestSlotManagerUpdate`.
    #[test]
    fn test_slot_manager_update() {
        let node = |id: &str| ManagedNode { id: id.to_owned(), ..ManagedNode::default() };
        let node_mgr = NodeManager::new("");
        let task_mgr = MockTaskManager::default();
        node_mgr.store_nodes(vec![node("tidb-1"), node("tidb-2"), node("tidb-3")]);
        task_mgr.expect_get_used_slots_on_nodes(Ok(slots(&[("tidb-1", 12), ("tidb-2", 8)])));
        let sm = SlotManager::new();
        sm.update_capacity(16);
        assert!(sm.load_used_slots().is_empty());
        assert!(sm.reserved_slots().is_empty());
        sm.update(&tidb_gocontext::Context::background(), &node_mgr, &task_mgr).unwrap();
        assert!(sm.reserved_slots().is_empty());
        assert_eq!(*sm.load_used_slots(), slots(&[("tidb-1", 12), ("tidb-2", 8), ("tidb-3", 0)]));
        task_mgr.assert_satisfied();

        // some node scaled in, should be reflected
        node_mgr.store_nodes(vec![node("tidb-1")]);
        task_mgr.expect_get_used_slots_on_nodes(Ok(slots(&[("tidb-1", 12), ("tidb-2", 8)])));
        sm.update(&tidb_gocontext::Context::background(), &node_mgr, &task_mgr).unwrap();
        assert!(sm.reserved_slots().is_empty());
        assert_eq!(*sm.load_used_slots(), slots(&[("tidb-1", 12)]));
        task_mgr.assert_satisfied();
        // on error, the usedSlots should not be changed
        task_mgr.expect_get_used_slots_on_nodes(Err(tidb_dxf_storage::StorageError::Other("mock err".to_owned())));
        let error = sm.update(&tidb_gocontext::Context::background(), &node_mgr, &task_mgr).unwrap_err();
        assert!(error.to_string().contains("mock err"));
        assert!(sm.reserved_slots().is_empty());
        assert_eq!(*sm.load_used_slots(), slots(&[("tidb-1", 12)]));
    }

    /// Go `TestSchedulerAdjustEligibleNodes`.
    #[test]
    fn test_scheduler_adjust_eligible_nodes() {
        let slot_mgr = SlotManager::new();
        slot_mgr.update_capacity(16);

        let all_nodes = [":4000".to_owned(), ":4001".to_owned(), ":4002".to_owned()];
        assert_eq!(slot_mgr.adjust_eligible_nodes(&all_nodes, 10), all_nodes);

        slot_mgr.store_used_slots(slots(&[(":4000", 12), (":4001", 4), (":4003", 0)])); // :4003 is a stale node
        assert_eq!(slot_mgr.adjust_eligible_nodes(&all_nodes, 10), [":4001"]);
    }

    /// Go `TestSlotManagerUpdateCapacity`.
    #[test]
    fn test_slot_manager_update_capacity() {
        let sm = SlotManager::new();
        sm.update_capacity(16);
        assert_eq!(sm.get_capacity(), 16);
        sm.update_capacity(32);
        assert_eq!(sm.get_capacity(), 32);
        sm.update_capacity(0);
        assert_eq!(sm.get_capacity(), 32);
    }
}
