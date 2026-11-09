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

//! Go `slot.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::RwLock;

use tidb_dxf::TaskBase;

/// Go `slotManager`: the slots of the node's executors.
pub struct SlotManager {
    inner: RwLock<SlotInner>,
    capacity: isize,
    /// The slots the executors may use; it starts at the node's CPU count.
    pub(crate) available: AtomicIsize,
}

#[derive(Default)]
pub(crate) struct SlotInner {
    /// Task ID -> index of the task in `executor_tasks`.
    pub(crate) task_id2_index: HashMap<i64, usize>,
    /// The tasks running on the node, in reverse task rank order.
    pub(crate) executor_tasks: Vec<TaskBase>,
}

impl SlotManager {
    /// Go `newSlotManager`.
    #[must_use]
    pub fn new(capacity: isize) -> Self {
        Self { inner: RwLock::default(), capacity, available: AtomicIsize::new(capacity) }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, SlotInner> {
        self.inner.read().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, SlotInner> {
        self.inner.write().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `alloc`: the subtasks of a task run in serial, so they take the
    /// task's required slots.
    pub(crate) fn alloc(&self, task: &TaskBase) -> bool {
        let mut inner = self.write();
        let (can_alloc, tasks_need_free) = self.can_alloc0(&inner, task);
        if !can_alloc || !tasks_need_free.is_empty() {
            return false;
        }

        inner.executor_tasks.push(task.clone());
        inner.executor_tasks.sort_by(|a, b| b.compare(a));
        Self::reindex(&mut inner);
        self.available.fetch_sub(task.required_slots, Ordering::SeqCst);
        true
    }

    fn reindex(inner: &mut SlotInner) {
        let SlotInner { task_id2_index, executor_tasks } = inner;
        for (index, slot_info) in executor_tasks.iter().enumerate() {
            task_id2_index.insert(slot_info.id, index);
        }
    }

    /// Go `free`.
    pub(crate) fn free(&self, task_id: i64) {
        let mut inner = self.write();
        let Some(&index) = inner.task_id2_index.get(&task_id) else {
            return;
        };
        self.available.fetch_add(inner.executor_tasks[index].required_slots, Ordering::SeqCst);
        inner.executor_tasks.remove(index);

        inner.task_id2_index.remove(&task_id);
        Self::reindex(&mut inner);
    }

    /// Go `canAlloc`.
    pub(crate) fn can_alloc(&self, task: &TaskBase) -> (bool, Vec<TaskBase>) {
        let inner = self.read();
        self.can_alloc0(&inner, task)
    }

    /// Go `canAlloc0`: whether the node can run the task, and the tasks to
    /// preempt before it does.
    fn can_alloc0(&self, inner: &SlotInner, task: &TaskBase) -> (bool, Vec<TaskBase>) {
        if self.available_slots() >= task.required_slots {
            return (true, Vec::new());
        }

        let mut tasks_need_free = Vec::new();
        let mut used_slots = 0;
        for slot_info in &inner.executor_tasks {
            if slot_info.compare(task) == std::cmp::Ordering::Less {
                break;
            }
            tasks_need_free.push(slot_info.clone());
            used_slots += slot_info.required_slots;
            if self.available_slots() + used_slots >= task.required_slots {
                return (true, tasks_need_free);
            }
        }

        (false, Vec::new())
    }

    /// Go `exchange`: the old task's slots for the new one's; false when
    /// the node has not enough.
    pub(crate) fn exchange(&self, new_task: &TaskBase) -> bool {
        let mut inner = self.write();
        let Some(&idx) = inner.task_id2_index.get(&new_task.id) else {
            return false;
        };

        let delta = new_task.required_slots - inner.executor_tasks[idx].required_slots;
        if delta > 0 && self.available_slots() < delta {
            return false;
        }

        self.available.fetch_sub(delta, Ordering::SeqCst);
        inner.executor_tasks[idx] = new_task.clone();
        true
    }

    /// Go `availableSlots`.
    pub(crate) fn available_slots(&self) -> isize {
        self.available.load(Ordering::SeqCst)
    }

    /// Go `usedSlots`.
    pub(crate) fn used_slots(&self) -> isize {
        self.capacity - self.available.load(Ordering::SeqCst)
    }

    /// The running tasks, in reverse rank order (Go's `executorTasks`).
    #[cfg(test)]
    pub(crate) fn executor_tasks(&self) -> Vec<TaskBase> {
        self.read().executor_tasks.clone()
    }

    /// The task's entry (Go's `executorTasks[taskID2Index[id]]`).
    #[cfg(test)]
    pub(crate) fn executor_task(&self, task_id: i64) -> TaskBase {
        let inner = self.read();
        inner.executor_tasks[inner.task_id2_index[&task_id]].clone()
    }

    /// Go `len(taskID2Index)`.
    #[cfg(test)]
    pub(crate) fn task_id2_index_len(&self) -> usize {
        self.read().task_id2_index.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: i64, priority: isize, required_slots: isize) -> TaskBase {
        TaskBase { id, priority, required_slots, ..TaskBase::default() }
    }

    fn ids(tasks: &[TaskBase]) -> Vec<i64> {
        tasks.iter().map(|task| task.id).collect()
    }

    /// Go `TestSlotManager`.
    #[test]
    fn test_slot_manager() {
        let sm = SlotManager::new(10);

        // rank(required slots in parenthesis): task1(1), task2(10)
        let task1 = task(1, 1, 1);
        let mut task2 = task(2, 2, 10);

        let (can_alloc, tasks_need_free) = sm.can_alloc(&task1);
        assert!(can_alloc);
        assert!(tasks_need_free.is_empty());
        assert!(sm.alloc(&task1));
        assert_eq!(sm.executor_tasks().len(), 1);
        assert_eq!(sm.executor_task(task1.id), task1);
        assert_eq!(sm.available_slots(), 9);

        // the available slots is not enough for task2
        let (can_alloc, tasks_need_free) = sm.can_alloc(&task2);
        assert!(!can_alloc);
        assert!(tasks_need_free.is_empty());
        // call alloc will fail
        assert!(!sm.alloc(&task2));

        // rank: task2(10), task1(1)
        // increase the priority of task2, task2 is waiting for allocation
        task2.priority = 0;
        let (can_alloc, tasks_need_free) = sm.can_alloc(&task2);
        assert!(can_alloc);
        assert_eq!(tasks_need_free, vec![task1.clone()]);
        // call alloc still fail as task is still holding the resources
        assert!(!sm.alloc(&task2));

        // rank: task3(1), task2(10), task1(1)
        // task with higher priority can alloc
        let task3 = task(3, -1, 1);
        let (can_alloc, tasks_need_free) = sm.can_alloc(&task3);
        assert!(can_alloc);
        assert!(tasks_need_free.is_empty());
        assert!(sm.alloc(&task3));
        assert_eq!(sm.executor_tasks().len(), 2);
        assert_eq!(sm.executor_task(task3.id), task3);
        assert_eq!(sm.available_slots(), 8);

        // rank: task3(1), task2(10), task1(1), task4(1)
        let task4 = task(4, 1, 1);
        let (can_alloc, tasks_need_free) = sm.can_alloc(&task4);
        assert!(can_alloc);
        assert!(tasks_need_free.is_empty());
        assert!(sm.alloc(&task4));
        assert_eq!(sm.available_slots(), 7);
        // rank: task3(1), task2(10), task5(1), task1(1), task4(1)
        let task5 = task(5, 0, 1);
        let (can_alloc, tasks_need_free) = sm.can_alloc(&task5);
        assert!(can_alloc);
        assert!(tasks_need_free.is_empty());
        assert!(sm.alloc(&task5));
        assert_eq!(sm.executor_tasks().len(), 4);
        // test the order of executorTasks
        assert_eq!(ids(&sm.executor_tasks()), vec![task4.id, task1.id, task5.id, task3.id]);
        assert_eq!(sm.available_slots(), 6);

        // rank: task3(1), task2(10), task5(1), task6(8), task1(1), task4(1)
        let mut task6 = task(6, 0, 8);
        let (can_alloc, tasks_need_free) = sm.can_alloc(&task6);
        assert!(can_alloc);
        assert_eq!(ids(&tasks_need_free), vec![task4.id, task1.id]);
        // rank: task3(1), task2(10), task5(1), task6(9), task1(1), task4(1)
        task6.required_slots = 9;
        let (can_alloc, tasks_need_free) = sm.can_alloc(&task6);
        assert!(!can_alloc);
        assert!(tasks_need_free.is_empty());

        sm.free(task4.id);
        sm.free(task5.id);
        sm.free(task3.id);
        assert_eq!(sm.executor_tasks().len(), 1);
        assert_eq!(sm.executor_task(task1.id), task1);
        assert_eq!(sm.available_slots(), 9);

        // task2 is waiting for allocation again
        let (can_alloc, tasks_need_free) = sm.can_alloc(&task2);
        assert!(can_alloc);
        assert_eq!(tasks_need_free, vec![task1.clone()]);

        sm.free(task1.id);
        assert!(sm.executor_tasks().is_empty());
        assert_eq!(sm.task_id2_index_len(), 0);
        assert_eq!(sm.available_slots(), 10);

        assert!(sm.alloc(&task2));
        assert_eq!(sm.executor_tasks().len(), 1);
        assert_eq!(sm.executor_task(task2.id), task2);
        assert_eq!(sm.available_slots(), 0);
        sm.free(task2.id);
        assert!(sm.executor_tasks().is_empty());
        assert_eq!(sm.task_id2_index_len(), 0);
        assert_eq!(sm.available_slots(), 10);
    }

    /// Go `TestSlotManagerExchangeSlots`.
    #[test]
    fn test_slot_manager_exchange_slots() {
        let task1 = TaskBase { id: 1, required_slots: 4, ..TaskBase::default() };
        let task2 = TaskBase { id: 2, required_slots: 8, ..TaskBase::default() };
        let slots = |id: i64, required_slots: isize| TaskBase { id, required_slots, ..TaskBase::default() };

        // not exist
        {
            let sm = SlotManager::new(16);
            assert!(!sm.exchange(&slots(1, 8)));
        }

        // exchange for more slots but not enough
        {
            let sm = SlotManager::new(16);
            assert!(sm.alloc(&task1));
            assert_eq!(sm.executor_task(task1.id).required_slots, 4);
            assert_eq!(sm.available_slots(), 12);
            assert!(!sm.exchange(&slots(1, 32)));
            assert_eq!(sm.executor_task(task1.id).required_slots, 4);
            assert_eq!(sm.available_slots(), 12);
        }

        // exchange for more slots
        {
            let sm = SlotManager::new(16);
            assert!(sm.alloc(&task1));
            assert_eq!(sm.available_slots(), 12);
            assert!(sm.exchange(&slots(1, 8)));
            assert_eq!(sm.executor_tasks().len(), 1);
            assert_eq!(sm.executor_task(task1.id).required_slots, 8);
            assert_eq!(sm.available_slots(), 8);
        }

        // exchange for less slots, and exchange twice with same required slots
        {
            let sm = SlotManager::new(16);
            assert!(sm.alloc(&task1));
            assert_eq!(sm.available_slots(), 12);
            assert!(sm.exchange(&slots(1, 2)));
            assert_eq!(sm.executor_tasks().len(), 1);
            assert_eq!(sm.executor_task(task1.id).required_slots, 2);
            assert_eq!(sm.available_slots(), 14);
            // exchange again, no change
            assert!(sm.exchange(&slots(1, 2)));
            assert_eq!(sm.executor_tasks().len(), 1);
            assert_eq!(sm.executor_task(task1.id).required_slots, 2);
            assert_eq!(sm.available_slots(), 14);
        }

        // exchange for more slots makes concurrent alloc fail
        {
            let sm = SlotManager::new(16);
            assert!(sm.alloc(&task1));
            assert_eq!(sm.available_slots(), 12);
            let (can_alloc, tasks_need_free) = sm.can_alloc(&task2);
            assert!(can_alloc);
            assert!(tasks_need_free.is_empty());
            assert!(sm.exchange(&slots(1, 10)));
            assert_eq!(sm.executor_tasks().len(), 1);
            assert_eq!(sm.executor_task(task1.id).required_slots, 10);
            assert_eq!(sm.available_slots(), 6);
            assert!(!sm.alloc(&task2));
            assert_eq!(sm.available_slots(), 6);
        }
    }
}
