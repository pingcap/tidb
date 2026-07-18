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

//! Dependency-closed live lifecycle from Go `queue.go`.
//!
//! Session pools, InfoSchema, statistics-cache scans, SQL execution and timer
//! scheduling remain external producers. This owner consumes already-created
//! jobs and preserves the queue's synchronized state transitions.

use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;

use super::{PriorityHeap, PriorityHeapError, PriorityHeapItem};

#[derive(Debug, Default)]
struct LiveQueueState {
    initialized: bool,
    heap: PriorityHeap,
    running_jobs: HashSet<i64>,
    must_retry_jobs: HashMap<i64, PriorityHeapItem>,
    locked_tables: HashSet<i64>,
    last_dml_update_version: u64,
}

/// Stable dependency-closed snapshot of the live queue state.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LiveQueueSnapshot {
    /// Jobs currently waiting, sorted by descending weight.
    pub current_jobs: Vec<PriorityHeapItem>,
    /// Table IDs currently executing.
    pub running_jobs: Vec<i64>,
    /// Table IDs deferred until the retry pass.
    pub must_retry_jobs: Vec<i64>,
    /// Last DML cache version consumed by the queue.
    pub last_dml_update_version: u64,
}

/// One already-materialized statistics-cache change and its source version.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DmlJobChange {
    /// Replacement or newly-created queue job.
    pub job: PriorityHeapItem,
    /// `statistics.Table.Version` used by Go's incremental-fetch gate.
    pub version: u64,
}

/// Thread-safe canonical owner of a live auto-analyze priority queue.
#[derive(Debug, Default)]
pub struct LiveAnalysisQueue {
    state: Mutex<LiveQueueState>,
}

impl LiveAnalysisQueue {
    /// Creates an uninitialized queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Initializes from dependency-closed jobs. Repeated initialization is a
    /// no-op, matching Go. Locked identities are excluded at the boundary.
    pub fn initialize(
        &self,
        jobs: impl IntoIterator<Item = PriorityHeapItem>,
        locked_tables: impl IntoIterator<Item = i64>,
        next_dml_version: u64,
    ) -> Result<(), PriorityHeapError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.initialized {
            return Ok(());
        }
        state.heap = PriorityHeap::new();
        state.running_jobs.clear();
        state.must_retry_jobs.clear();
        state.locked_tables = locked_tables.into_iter().collect();
        for job in jobs {
            if !Self::job_is_locked(job, &state.locked_tables) {
                state.heap.add_or_update(job)?;
            }
        }
        state.last_dml_update_version = next_dml_version;
        state.initialized = true;
        Ok(())
    }

    /// Returns whether initialization completed and close has not reset it.
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.lock_state().initialized
    }

    /// Returns the running identities without requiring initialization.
    ///
    /// Go's `GetRunningJobs` is intentionally outside the initialized gate so
    /// callers can observe an empty set before initialization and after close.
    #[must_use]
    pub fn running_jobs(&self) -> Vec<i64> {
        let mut running_jobs: Vec<_> = self.lock_state().running_jobs.iter().copied().collect();
        running_jobs.sort_unstable();
        running_jobs
    }

    /// Closes idempotently and resets all synchronized fields so the same
    /// queue can be initialized again.
    pub fn close(&self) {
        let mut state = self.lock_state();
        if !state.initialized {
            return;
        }
        *state = LiveQueueState::default();
    }

    /// Returns queue emptiness through the source initialization gate.
    pub fn is_empty(&self) -> Result<bool, PriorityHeapError> {
        let state = self.lock_state();
        Self::require_initialized(&state)?;
        Ok(state.heap.is_empty())
    }

    /// Returns the number of waiting jobs through the initialization gate.
    pub fn len(&self) -> Result<usize, PriorityHeapError> {
        let state = self.lock_state();
        Self::require_initialized(&state)?;
        Ok(state.heap.len())
    }

    /// Returns the highest-priority waiting job without consuming it.
    pub fn peek(&self) -> Result<PriorityHeapItem, PriorityHeapError> {
        let state = self.lock_state();
        Self::require_initialized(&state)?;
        state.heap.peek()
    }

    /// Pops the highest-priority job and marks its real table identity running.
    pub fn pop(&self) -> Result<PriorityHeapItem, PriorityHeapError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        let job = state.heap.pop()?;
        state.running_jobs.insert(job.table_id);
        Ok(job)
    }

    /// Applies the source success/failure hook transition.
    ///
    /// Completion after close is intentionally a no-op. A retry retains the
    /// complete job rather than only its ID because the absent InfoSchema
    /// producer cannot recreate it locally.
    pub fn complete(&self, job: PriorityHeapItem, must_retry: bool) {
        let mut state = self.lock_state();
        if !state.initialized {
            return;
        }
        state.running_jobs.remove(&job.table_id);
        if must_retry {
            state.must_retry_jobs.insert(job.table_id, job);
        }
    }

    /// Processes already-materialized DML job changes and a current lock set.
    pub fn process_dml_changes(
        &self,
        jobs: impl IntoIterator<Item = PriorityHeapItem>,
        locked_tables: impl IntoIterator<Item = i64>,
        next_dml_version: u64,
    ) -> Result<(), PriorityHeapError> {
        self.process_versioned_dml_changes(
            jobs.into_iter().map(|job| DmlJobChange {
                job,
                version: next_dml_version,
            }),
            locked_tables,
            next_dml_version,
        )
    }

    /// Processes materialized DML changes through Go's strict
    /// `stats.Version > lastFetchTimestamp` gate.
    pub fn process_versioned_dml_changes(
        &self,
        changes: impl IntoIterator<Item = DmlJobChange>,
        locked_tables: impl IntoIterator<Item = i64>,
        next_dml_version: u64,
    ) -> Result<(), PriorityHeapError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        state.locked_tables = locked_tables.into_iter().collect();
        let locked = state.locked_tables.clone();
        for job in state.heap.list() {
            if Self::job_is_locked(job, &locked) {
                state.heap.delete(job.table_id)?;
            }
        }
        state
            .must_retry_jobs
            .retain(|_, job| !Self::job_is_locked(*job, &locked));
        let last_version = state.last_dml_update_version;
        for change in changes {
            if change.version <= last_version {
                continue;
            }
            let job = change.job;
            if Self::job_is_locked(job, &locked) {
                continue;
            }
            if state.running_jobs.contains(&job.table_id) {
                state.must_retry_jobs.insert(job.table_id, job);
            } else if !state.must_retry_jobs.contains_key(&job.table_id) {
                state.heap.add_or_update(job)?;
            }
        }
        if next_dml_version > state.last_dml_update_version {
            state.last_dml_update_version = next_dml_version;
        }
        Ok(())
    }

    /// Requeues retry jobs that are no longer running or locked.
    pub fn requeue_must_retry_jobs(&self) -> Result<(), PriorityHeapError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        let retry_jobs = std::mem::take(&mut state.must_retry_jobs);
        for (table_id, job) in retry_jobs {
            if state.running_jobs.contains(&table_id) {
                // recreateAndPushJobForTable eventually reaches
                // pushWithoutLock, which marks a still-running identity for
                // retry again instead of losing it.
                state.must_retry_jobs.insert(table_id, job);
            } else if !Self::job_is_locked(job, &state.locked_tables) {
                state.heap.add_or_update(job)?;
            }
        }
        Ok(())
    }

    /// Replaces live jobs with refreshed indicators/weights and deletes jobs
    /// whose source table disappeared.
    pub fn refresh_jobs(
        &self,
        refreshed: impl IntoIterator<Item = PriorityHeapItem>,
        live_table_ids: impl IntoIterator<Item = i64>,
    ) -> Result<(), PriorityHeapError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        let live: HashSet<_> = live_table_ids.into_iter().collect();
        for table_id in state.heap.list_keys() {
            if !live.contains(&table_id) {
                state.heap.delete(table_id)?;
            }
        }
        for job in refreshed {
            if live.contains(&job.table_id) && state.heap.get(job.table_id).is_some() {
                state.heap.update(job)?;
            }
        }
        Ok(())
    }

    /// Runs one background maintenance body with Go's panic-recovery cleanup.
    /// Returns `false` when the body panicked.
    pub fn run_with_recovery(&self, body: impl FnOnce()) -> bool {
        let completed = catch_unwind(AssertUnwindSafe(body)).is_ok();
        if !completed {
            self.close();
        }
        completed
    }

    /// Returns a deterministic snapshot while preserving live job identity.
    pub fn snapshot(&self) -> Result<LiveQueueSnapshot, PriorityHeapError> {
        let state = self.lock_state();
        Self::require_initialized(&state)?;
        let mut current_jobs = state.heap.list();
        current_jobs.sort_by(|left, right| {
            right
                .weight
                .partial_cmp(&left.weight)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut running_jobs: Vec<_> = state.running_jobs.iter().copied().collect();
        running_jobs.sort_unstable();
        let mut must_retry_jobs: Vec<_> = state.must_retry_jobs.keys().copied().collect();
        must_retry_jobs.sort_unstable();
        Ok(LiveQueueSnapshot {
            current_jobs,
            running_jobs,
            must_retry_jobs,
            last_dml_update_version: state.last_dml_update_version,
        })
    }

    fn require_initialized(state: &LiveQueueState) -> Result<(), PriorityHeapError> {
        if state.initialized {
            Ok(())
        } else {
            Err(PriorityHeapError::NotInitialized)
        }
    }

    fn job_is_locked(job: PriorityHeapItem, locked: &HashSet<i64>) -> bool {
        locked.contains(&job.table_id) || locked.contains(&job.global_table_id)
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, LiveQueueState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
