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

//! Go `subtask.go`: the subtask record, its state vocabulary, and the
//! allocatable resource budget a step executor runs inside.

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};

use chrono::{DateTime, Utc};

use crate::node::step_resource_bytes_size;
use crate::step::Step;
use crate::task::TaskType;

go_string_type! {
    /// Go `SubtaskState`: the state of a subtask.
    SubtaskState
}

/// Go `SubtaskStatePending`.
pub const SUBTASK_STATE_PENDING: SubtaskState = SubtaskState::from_static("pending");
/// Go `SubtaskStateRunning`.
pub const SUBTASK_STATE_RUNNING: SubtaskState = SubtaskState::from_static("running");
/// Go `SubtaskStateSucceed`.
pub const SUBTASK_STATE_SUCCEED: SubtaskState = SubtaskState::from_static("succeed");
/// Go `SubtaskStateFailed`.
pub const SUBTASK_STATE_FAILED: SubtaskState = SubtaskState::from_static("failed");
/// Go `SubtaskStateCanceled`.
pub const SUBTASK_STATE_CANCELED: SubtaskState = SubtaskState::from_static("canceled");
/// Go `SubtaskStatePaused`.
pub const SUBTASK_STATE_PAUSED: SubtaskState = SubtaskState::from_static("paused");

/// Go `SubtaskBase`: the basic information of a subtask, split out so that the
/// possibly very large subtask meta need not be loaded into memory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubtaskBase {
    /// Subtask ID.
    pub id: i64,
    /// The step this subtask belongs to.
    pub step: Step,
    /// The type of the owning task.
    pub tp: TaskType,
    /// The owning task's ID, taken from `task_key` of the subtask table.
    pub task_id: i64,
    /// Subtask state.
    pub state: SubtaskState,
    /// The concurrency of the subtask.
    ///
    /// It is initialized as the task's required slots and is NOT used now. If
    /// the required slots of the task are modified, the concurrency of its
    /// unfinished subtasks is updated too. Some subtasks, like post-process of
    /// `IMPORT INTO`, do not consume many resources and could lower this; the
    /// field exists so that feature can be built later.
    pub concurrency: i64,
    /// The ID of the target executor. Right now it equals `instance_id` and
    /// its value is `IP:PORT`, see `GenerateExecID`.
    pub exec_id: String,
    /// Creation time; `None` is Go's zero `time.Time`.
    pub create_time: Option<DateTime<Utc>>,
    /// The time the subtask started; `None` if it has not started yet.
    pub start_time: Option<DateTime<Utc>>,
    /// The ordinal of the subtask, unique for a given task and step, starting
    /// from 1.
    pub ordinal: i64,
}

impl SubtaskBase {
    /// Go `IsDone`: whether the subtask reached a terminal state.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.state == SUBTASK_STATE_SUCCEED
            || self.state == SUBTASK_STATE_CANCELED
            || self.state == SUBTASK_STATE_FAILED
    }
}

impl fmt::Display for SubtaskBase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[ID={}, Step={}, Type={}, TaskID={}, State={}, ExecID={}]",
            self.id, self.step.0, self.tp, self.task_id, self.state, self.exec_id
        )
    }
}

/// Go `Subtask`: the subtask of the distributed framework.
///
/// Subtasks of a task run in parallel on different nodes, but on each node at
/// most one subtask runs at a time, see `StepExecutor`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subtask {
    /// The embedded [`SubtaskBase`]; Go embeds it anonymously.
    pub base: SubtaskBase,
    /// The time the subtask was updated. It doubles as the subtask end time
    /// once finished, and is `None` if it has not started yet.
    pub update_time: Option<DateTime<Utc>>,
    /// The metadata of the subtask, which should not be empty; the metas of
    /// different subtasks of the same step must differ too.
    ///
    /// NOTE: `StepExecutor::OnFinished` may change this to store a result, and
    /// the framework then updates the subtask meta in storage. On every other
    /// code path this field is read-only.
    pub meta: Vec<u8>,
    /// A human-readable summary of the subtask.
    pub summary: String,
}

impl Subtask {
    /// Go `NewSubtask`: creates a new subtask.
    #[must_use]
    pub fn new(
        step: Step,
        task_id: i64,
        tp: TaskType,
        exec_id: impl Into<String>,
        concurrency: i64,
        meta: Vec<u8>,
        ordinal: i64,
    ) -> Self {
        Self {
            base: SubtaskBase {
                step,
                tp,
                task_id,
                exec_id: exec_id.into(),
                concurrency,
                ordinal,
                ..SubtaskBase::default()
            },
            meta,
            ..Self::default()
        }
    }

    /// Go `(*SubtaskBase).IsDone` reached through the embedded base.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.base.is_done()
    }
}

/// Go `Allocatable`: a resource with a capacity that can be allocated. It is
/// routine safe.
#[derive(Debug)]
pub struct Allocatable {
    capacity: i64,
    used: AtomicI64,
}

impl Allocatable {
    /// Go `NewAllocatable`.
    #[must_use]
    pub fn new(capacity: i64) -> Self {
        Self {
            capacity,
            used: AtomicI64::new(0),
        }
    }

    /// Go `Capacity`.
    #[must_use]
    pub fn capacity(&self) -> i64 {
        self.capacity
    }

    /// Go `Used`.
    #[must_use]
    pub fn used(&self) -> i64 {
        self.used.load(Ordering::SeqCst)
    }

    /// Go `Alloc`: allocates `n`, returning whether it fit.
    pub fn alloc(&self, n: i64) -> bool {
        loop {
            let used = self.used.load(Ordering::SeqCst);
            if used + n > self.capacity {
                return false;
            }
            if self
                .used
                .compare_exchange(used, used + n, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Go `Free`: returns `n` to the budget.
    pub fn free(&self, n: i64) {
        self.used.fetch_sub(n, Ordering::SeqCst);
    }
}

/// Go `StepResource`: the max resource a task step can use.
///
/// It is also the max resource a subtask can use, because subtasks of a task
/// step run in sequence.
#[derive(Debug)]
pub struct StepResource {
    /// The CPU slot budget.
    pub cpu: Allocatable,
    /// The memory budget, in bytes.
    pub mem: Allocatable,
}

impl StepResource {
    /// Go `MemoryPerCore`: the memory per core of the step resource. When the
    /// CPU capacity is not positive it falls back to the total memory.
    #[must_use]
    pub fn memory_per_core(&self) -> i64 {
        if self.cpu.capacity() <= 0 {
            return self.mem.capacity();
        }
        self.mem.capacity() / self.cpu.capacity()
    }
}

impl fmt::Display for StepResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mem = step_resource_bytes_size(self.mem.capacity() as f64);
        write!(f, "[CPU={}, Mem={}]", self.cpu.capacity(), mem)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// Go `TestSubtaskIsDone`.
    #[test]
    fn test_subtask_is_done() {
        let cases = [
            (SUBTASK_STATE_PENDING, false),
            (SUBTASK_STATE_RUNNING, false),
            (SUBTASK_STATE_SUCCEED, true),
            (SUBTASK_STATE_FAILED, true),
            (SUBTASK_STATE_PAUSED, false),
            (SUBTASK_STATE_CANCELED, true),
        ];
        for (state, done) in cases {
            let subtask = Subtask {
                base: SubtaskBase {
                    state,
                    ..SubtaskBase::default()
                },
                ..Subtask::default()
            };
            assert_eq!(subtask.is_done(), done);
        }
    }

    /// Go `TestAllocatable`.
    ///
    /// Adaptation: Go seeds `math/rand` per goroutine from the wall clock; no
    /// random-number crate is reachable offline, so each thread runs the same
    /// splitmix64 generator seeded from its own index and the clock. What the
    /// test proves — that concurrent `Alloc`/`Free` pairs balance out — does
    /// not depend on the generator.
    #[test]
    fn test_allocatable() {
        let allocatable = Arc::new(Allocatable::new(123_456));
        assert_eq!(allocatable.capacity(), 123_456);
        assert_eq!(allocatable.used(), 0);

        assert!(!allocatable.alloc(123_457));
        assert_eq!(allocatable.used(), 0);
        assert!(allocatable.alloc(123));
        assert_eq!(allocatable.used(), 123);
        allocatable.free(123);
        assert_eq!(allocatable.used(), 0);

        let mut handles = Vec::new();
        for i in 0..10u64 {
            let allocatable = Arc::clone(&allocatable);
            handles.push(std::thread::spawn(move || {
                let mut state = i
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    .wrapping_add(0x2545_F491_4F6C_DD1D);
                for _ in 0..10_000 {
                    state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                    let mut z = state;
                    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                    z ^= z >> 31;
                    let n = i64::try_from(z % 1000).unwrap();
                    if allocatable.alloc(n) {
                        allocatable.free(n);
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(allocatable.used(), 0);
    }

    /// Not a Go test: pins [`StepResource`]'s rendering, which is the only
    /// consumer of the hand-rolled `units.BytesSize` replacement.
    #[test]
    fn test_step_resource_string() {
        let r = StepResource {
            cpu: Allocatable::new(8),
            mem: Allocatable::new(8 * 1024 * 1024 * 1024),
        };
        assert_eq!(r.to_string(), "[CPU=8, Mem=8GiB]");
        assert_eq!(r.memory_per_core(), 1024 * 1024 * 1024);

        let zero_cpu = StepResource {
            cpu: Allocatable::new(0),
            mem: Allocatable::new(42),
        };
        assert_eq!(zero_cpu.memory_per_core(), 42);
    }
}
