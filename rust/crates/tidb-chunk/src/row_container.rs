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

//! `pkg/util/chunk/row_container.go`: rows held in memory until a statement's
//! memory quota is exceeded, then moved wholesale to a row-addressed spill
//! file.
//!
//! A [`RowContainer`] is a cheap handle to one shared state. The state owns the
//! in-memory [`List`], optional [`DataInDiskByRows`], memory and disk trackers,
//! spill action, and an explicit operation coordinator. A shallow copy is an
//! [`Arc`] clone, the idiomatic equivalent of Go's `ShallowCopyWithNewMutex`:
//! every handle observes the same rows, spill, reset, error, and close.
//!
//! # Spill coordination
//!
//! [`SpillDiskAction`] holds a weak route back to the shared state. An action
//! fired by an unrelated allocation while the container is idle therefore
//! spills immediately; it does not need a later `add` to notice a flag. When
//! `List::add` or `List::reset` fires the action reentrantly while holding the
//! records write lock, the coordinator records a pending spill and returns.
//! The outer operation releases records, atomically claims `Spilling`, and
//! performs that spill synchronously. The coordinator mutex is never held
//! while locking records, touching disk, or invoking a tracker fallback.
//!
//! # Rows and lifetimes
//!
//! Accessors either return a guard-backed live in-memory view, materialize an
//! owned disk chunk, or copy into caller-owned storage. A bare borrow can
//! therefore never escape the records read guard, and readers may safely cross
//! an in-memory-to-disk transition between chunks.
//!
//! # Deliberately not reproduced
//!
//! Go uses a spill goroutine, per-handle read locks, write-lock fan-out, cache
//! padding, and `WaitForTest` to coordinate that runtime shape. None changes
//! row values, ordering, tracker accounting, spill errors, or fallback order,
//! so the Rust coordinator does not reproduce them. `SortedRowContainer` and
//! `SortAndSpillDiskAction` remain a separate package obligation.

use std::ops::Deref;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};
use std::thread::ThreadId;

use tidb_datatype::FieldType;
use tidb_util::disk;
use tidb_util::memory::{
    ActionOnExceed, ArcAction, Tracker, DEF_SPILL_PRIORITY, LABEL_FOR_ROW_CONTAINER,
};

use crate::chunk::Chunk;
use crate::chunk_in_disk::DiskError;
use crate::list::{List, RowPtr};
use crate::row::Row;
use crate::row_in_disk::DataInDiskByRows;

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn read_unpoisoned<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_unpoisoned<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn wait_unpoisoned<'a, T>(cond: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    cond.wait(guard)
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Public compatibility view of Go `spillStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpillStatus {
    /// Go `notSpilled`.
    NotSpilled,
    /// Go `spilling`.
    Spilling,
    /// Go `spilledYet`.
    SpilledYet,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CoordinatorPhase {
    MemoryIdle,
    AddingMemory,
    AddingDisk,
    Spilling,
    DiskIdle,
    Failed,
    ResettingMemory,
    ResettingDisk,
    Closing,
    Closed,
}

#[derive(Debug)]
struct Coordinator {
    phase: CoordinatorPhase,
    pending_spill: bool,
    armed: bool,
    active_mutator: Option<ThreadId>,
    generation: u64,
    fallback_active: bool,
}

impl Coordinator {
    fn new() -> Self {
        Coordinator {
            phase: CoordinatorPhase::MemoryIdle,
            pending_spill: false,
            armed: true,
            active_mutator: None,
            generation: 0,
            fallback_active: false,
        }
    }
}

/// Restores a non-busy phase and wakes waiters if a non-spill operation
/// unwinds. It never locks records and never performs disk I/O.
struct PhaseLease<'a> {
    shared: &'a RowContainerShared,
    active: CoordinatorPhase,
    unwind_to: CoordinatorPhase,
    armed_on_unwind: bool,
    live: bool,
}

impl<'a> PhaseLease<'a> {
    fn new(
        shared: &'a RowContainerShared,
        active: CoordinatorPhase,
        unwind_to: CoordinatorPhase,
        armed_on_unwind: bool,
    ) -> Self {
        PhaseLease {
            shared,
            active,
            unwind_to,
            armed_on_unwind,
            live: true,
        }
    }

    fn disarm(&mut self) {
        self.live = false;
    }

    fn transition(
        &mut self,
        active: CoordinatorPhase,
        unwind_to: CoordinatorPhase,
        armed_on_unwind: bool,
    ) {
        self.active = active;
        self.unwind_to = unwind_to;
        self.armed_on_unwind = armed_on_unwind;
    }
}

impl Drop for PhaseLease<'_> {
    fn drop(&mut self) {
        if !self.live {
            return;
        }
        let mut coordinator = lock_unpoisoned(&self.shared.coordinator);
        if coordinator.phase == self.active {
            coordinator.phase = self.unwind_to;
            coordinator.armed = self.armed_on_unwind;
            coordinator.active_mutator = None;
        }
        drop(coordinator);
        self.shared.phase_changed.notify_all();
    }
}

/// Releases the coordinator's fallback slot and wakes lifecycle waiters even
/// when a fallback callback unwinds. Its destructor performs no I/O or
/// callback work.
struct FallbackLease<'a> {
    shared: &'a RowContainerShared,
}

impl Drop for FallbackLease<'_> {
    fn drop(&mut self) {
        let mut coordinator = lock_unpoisoned(&self.shared.coordinator);
        coordinator.fallback_active = false;
        drop(coordinator);
        self.shared.phase_changed.notify_all();
    }
}

/// Go `SpillDiskAction` + `baseSpillDiskAction`: the `ActionOnExceed` a
/// [`RowContainer`] registers on the statement's memory tracker.
pub struct SpillDiskAction {
    shared: Weak<RowContainerShared>,
    fallback: Mutex<Option<ArcAction>>,
    finished: AtomicBool,
}

impl SpillDiskAction {
    fn new(shared: Weak<RowContainerShared>) -> Self {
        SpillDiskAction {
            shared,
            fallback: Mutex::new(None),
            finished: AtomicBool::new(false),
        }
    }

    /// Go `getStatus`, projected from the richer coordinator phase.
    #[must_use]
    pub fn status(&self) -> SpillStatus {
        let Some(shared) = self.shared.upgrade() else {
            return SpillStatus::SpilledYet;
        };
        let phase = lock_unpoisoned(&shared.coordinator).phase;
        match phase {
            CoordinatorPhase::MemoryIdle
            | CoordinatorPhase::AddingMemory
            | CoordinatorPhase::ResettingMemory => SpillStatus::NotSpilled,
            CoordinatorPhase::Spilling => SpillStatus::Spilling,
            CoordinatorPhase::AddingDisk
            | CoordinatorPhase::DiskIdle
            | CoordinatorPhase::Failed
            | CoordinatorPhase::ResettingDisk
            | CoordinatorPhase::Closing
            | CoordinatorPhase::Closed => SpillStatus::SpilledYet,
        }
    }

    /// Whether a reentrant in-memory operation has handed off a pending spill.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        self.shared.upgrade().is_some_and(|shared| {
            let coordinator = lock_unpoisoned(&shared.coordinator);
            coordinator.pending_spill
        })
    }

    fn fallback(&self) -> Option<ArcAction> {
        let mut fallback = lock_unpoisoned(&self.fallback);
        while let Some(action) = fallback.clone() {
            if !action.is_finished() {
                return Some(action);
            }
            *fallback = action.get_fallback();
        }
        None
    }

    fn invoke_fallback_if_needed(&self, tracker: &Arc<Tracker>) {
        if !tracker.check_exceed() {
            return;
        }
        if let Some(fallback) = self.fallback() {
            fallback.action(tracker);
        }
    }

    /// Claim the serialized fallback slot for `generation`. `false` means a
    /// reset won the terminal race and the caller must re-enter as an action
    /// in the new generation.
    fn run_fallback_for_generation(
        &self,
        shared: &Arc<RowContainerShared>,
        tracker: &Arc<Tracker>,
        generation: u64,
    ) -> bool {
        #[cfg(test)]
        shared.run_before_fallback_hook();
        let mut coordinator = lock_unpoisoned(&shared.coordinator);
        loop {
            if coordinator.generation != generation {
                return false;
            }
            if coordinator.phase == CoordinatorPhase::Closed {
                return true;
            }
            if coordinator.phase == CoordinatorPhase::MemoryIdle && coordinator.pending_spill {
                return false;
            }
            if coordinator.fallback_active
                || matches!(
                    coordinator.phase,
                    CoordinatorPhase::AddingMemory
                        | CoordinatorPhase::AddingDisk
                        | CoordinatorPhase::Spilling
                        | CoordinatorPhase::ResettingMemory
                        | CoordinatorPhase::ResettingDisk
                        | CoordinatorPhase::Closing
                )
            {
                coordinator = wait_unpoisoned(&shared.phase_changed, coordinator);
                continue;
            }
            coordinator.fallback_active = true;
            drop(coordinator);
            let _lease = FallbackLease { shared };
            self.invoke_fallback_if_needed(tracker);
            return true;
        }
    }

    /// Finish the action generation that was observed while another thread
    /// owned a memory mutation or while disk/spill work was active.
    ///
    /// `true` means the later call is complete. `false` means reset published
    /// a new generation before this action could claim fallback, so the caller
    /// must re-enter the action state machine.
    fn wait_for_generation(
        &self,
        shared: &Arc<RowContainerShared>,
        tracker: &Arc<Tracker>,
        generation: u64,
    ) -> bool {
        let mut coordinator = lock_unpoisoned(&shared.coordinator);
        loop {
            if coordinator.generation != generation {
                return false;
            }
            if !matches!(
                coordinator.phase,
                CoordinatorPhase::AddingMemory
                    | CoordinatorPhase::AddingDisk
                    | CoordinatorPhase::Spilling
                    | CoordinatorPhase::ResettingMemory
                    | CoordinatorPhase::ResettingDisk
                    | CoordinatorPhase::Closing
            ) {
                break;
            }
            coordinator = wait_unpoisoned(&shared.phase_changed, coordinator);
        }

        if coordinator.phase == CoordinatorPhase::Closed {
            return true;
        }
        if coordinator.phase == CoordinatorPhase::MemoryIdle && coordinator.pending_spill {
            coordinator.pending_spill = false;
            coordinator.armed = false;
            coordinator.phase = CoordinatorPhase::Spilling;
            coordinator.active_mutator = None;
            let mut lease = PhaseLease::new(
                shared,
                CoordinatorPhase::Spilling,
                CoordinatorPhase::MemoryIdle,
                false,
            );
            drop(coordinator);
            shared.perform_spill(&mut lease);
            return self.run_fallback_for_generation(shared, tracker, generation);
        }

        drop(coordinator);
        self.run_fallback_for_generation(shared, tracker, generation)
    }
}

impl ActionOnExceed for SpillDiskAction {
    /// The first trigger spills and returns; concurrent/later triggers wait for
    /// active work to settle and then may invoke the fallback.
    fn action(&self, t: &Arc<Tracker>) {
        let Some(shared) = self.shared.upgrade() else {
            self.finished.store(true, SeqCst);
            return;
        };
        let thread_id = std::thread::current().id();

        'generation: loop {
            let mut coordinator = lock_unpoisoned(&shared.coordinator);
            let generation = coordinator.generation;
            loop {
                match coordinator.phase {
                    CoordinatorPhase::AddingMemory | CoordinatorPhase::ResettingMemory => {
                        let first = coordinator.armed;
                        if first {
                            coordinator.armed = false;
                            coordinator.pending_spill = true;
                        }
                        let owns_mutation = coordinator.active_mutator == Some(thread_id);
                        drop(coordinator);
                        if first {
                            tracing::info!(
                                consumed = t.bytes_consumed(),
                                quota = t.get_bytes_limit(),
                                "memory exceeds quota, spill to disk now."
                            );
                            #[cfg(test)]
                            if owns_mutation {
                                shared.run_reentrant_action_hook();
                            }
                            return;
                        }
                        if owns_mutation {
                            return;
                        }
                        #[cfg(test)]
                        shared.run_later_action_hook();
                        if self.wait_for_generation(&shared, t, generation) {
                            return;
                        }
                        continue 'generation;
                    }
                    CoordinatorPhase::MemoryIdle if coordinator.armed => {
                        coordinator.armed = false;
                        coordinator.pending_spill = false;
                        coordinator.phase = CoordinatorPhase::Spilling;
                        coordinator.active_mutator = None;
                        let mut lease = PhaseLease::new(
                            &shared,
                            CoordinatorPhase::Spilling,
                            CoordinatorPhase::MemoryIdle,
                            false,
                        );
                        drop(coordinator);
                        tracing::info!(
                            consumed = t.bytes_consumed(),
                            quota = t.get_bytes_limit(),
                            "memory exceeds quota, spill to disk now."
                        );
                        shared.perform_spill(&mut lease);
                        return;
                    }
                    CoordinatorPhase::MemoryIdle if coordinator.pending_spill => {
                        coordinator.pending_spill = false;
                        coordinator.phase = CoordinatorPhase::Spilling;
                        coordinator.active_mutator = None;
                        let mut lease = PhaseLease::new(
                            &shared,
                            CoordinatorPhase::Spilling,
                            CoordinatorPhase::MemoryIdle,
                            false,
                        );
                        drop(coordinator);
                        shared.perform_spill(&mut lease);
                        if self.run_fallback_for_generation(&shared, t, generation) {
                            return;
                        }
                        continue 'generation;
                    }
                    CoordinatorPhase::AddingDisk | CoordinatorPhase::Spilling => {
                        drop(coordinator);
                        #[cfg(test)]
                        shared.run_later_action_hook();
                        if self.wait_for_generation(&shared, t, generation) {
                            return;
                        }
                        continue 'generation;
                    }
                    CoordinatorPhase::ResettingDisk | CoordinatorPhase::Closing => {
                        coordinator = wait_unpoisoned(&shared.phase_changed, coordinator);
                        if coordinator.generation != generation {
                            drop(coordinator);
                            continue 'generation;
                        }
                    }
                    CoordinatorPhase::MemoryIdle
                    | CoordinatorPhase::DiskIdle
                    | CoordinatorPhase::Failed => {
                        drop(coordinator);
                        if self.run_fallback_for_generation(&shared, t, generation) {
                            return;
                        }
                        continue 'generation;
                    }
                    CoordinatorPhase::Closed => return,
                }
            }
        }
    }

    fn set_fallback(&self, a: Option<ArcAction>) {
        *lock_unpoisoned(&self.fallback) = a;
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.fallback()
    }

    fn get_priority(&self) -> i64 {
        DEF_SPILL_PRIORITY
    }

    fn set_finished(&self) {
        self.finished.store(true, SeqCst);
    }

    fn is_finished(&self) -> bool {
        self.finished.load(SeqCst)
    }
}

/// Go `rowContainerRecord`: the in-memory half, the on-disk half, and the
/// error a failed spill leaves behind.
struct RowContainerRecord {
    in_memory: List,
    in_disk: Option<DataInDiskByRows>,
    spill_error: Option<String>,
}

enum RowContainerChunkInner<'a> {
    InMemory {
        records: RwLockReadGuard<'a, RowContainerRecord>,
        chunk_index: usize,
    },
    Owned(Chunk),
}

/// A chunk read from a [`RowContainer`].
///
/// In-memory reads keep the shared records read guard alive and dereference to
/// the live chunk, matching Go's no-copy access. Disk reads own the decoded
/// chunk. Public mutating container methods retain `&mut self`, so safe
/// same-handle code cannot hold this view and then deadlock itself on spill or
/// reset; another shallow handle remains the concurrent path.
pub struct RowContainerChunk<'a> {
    inner: RowContainerChunkInner<'a>,
}

impl Deref for RowContainerChunk<'_> {
    type Target = Chunk;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            RowContainerChunkInner::InMemory {
                records,
                chunk_index,
            } => records.in_memory.get_chunk(*chunk_index),
            RowContainerChunkInner::Owned(chunk) => chunk,
        }
    }
}

impl AsRef<Chunk> for RowContainerChunk<'_> {
    fn as_ref(&self) -> &Chunk {
        self
    }
}

impl RowContainerChunk<'_> {
    fn into_snapshot(self) -> Chunk {
        match self.inner {
            RowContainerChunkInner::InMemory {
                records,
                chunk_index,
            } => records.in_memory.get_chunk(chunk_index).clone(),
            RowContainerChunkInner::Owned(chunk) => chunk,
        }
    }
}

struct RowContainerShared {
    records: RwLock<RowContainerRecord>,
    coordinator: Mutex<Coordinator>,
    phase_changed: Condvar,
    mem_tracker: Arc<Tracker>,
    disk_tracker: Arc<disk::Tracker>,
    action_spill: Mutex<Option<Arc<SpillDiskAction>>>,
    #[cfg(test)]
    spill_start_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    reentrant_action_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    later_action_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_fallback_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl RowContainerShared {
    fn perform_spill(&self, lease: &mut PhaseLease<'_>) {
        #[cfg(test)]
        if let Some(hook) = lock_unpoisoned(&self.spill_start_hook).clone() {
            hook();
        }

        let mut records = write_unpoisoned(&self.records);
        let spill = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
            if records.in_disk.is_none() {
                let in_disk = DataInDiskByRows::new(records.in_memory.field_types().to_vec());
                in_disk.disk_tracker().attach_to(&self.disk_tracker);
                records.in_disk = Some(in_disk);
            }

            let RowContainerRecord {
                in_memory, in_disk, ..
            } = &mut *records;
            let in_disk = in_disk.as_mut().expect("spill created disk storage");
            for chunk_index in 0..in_memory.num_chunks() {
                in_disk
                    .add(in_memory.get_chunk(chunk_index))
                    .map_err(|error| error.to_string())?;
                in_memory.mem_tracker().handle_kill_signal();
            }
            in_memory.clear();
            Ok(())
        }));

        let new_error = match spill {
            Ok(Ok(())) => None,
            Ok(Err(message)) => Some(message),
            Err(payload) => Some(panic_message(payload.as_ref())),
        };
        if let Some(message) = &new_error {
            records.spill_error = Some(message.clone());
        }
        let failed = records.spill_error.is_some();
        drop(records);

        if let Some(message) = &new_error {
            tracing::warn!(error = %message, "spill to disk failed");
        }
        let mut coordinator = lock_unpoisoned(&self.coordinator);
        coordinator.pending_spill = false;
        coordinator.active_mutator = None;
        coordinator.phase = if failed {
            CoordinatorPhase::Failed
        } else {
            CoordinatorPhase::DiskIdle
        };
        lease.disarm();
        drop(coordinator);
        self.phase_changed.notify_all();
    }

    fn finish_add(&self, mode: AddMode, lease: &mut PhaseLease<'_>) -> bool {
        let mut coordinator = lock_unpoisoned(&self.coordinator);
        let spill = match mode {
            AddMode::Memory if coordinator.pending_spill => {
                coordinator.pending_spill = false;
                coordinator.phase = CoordinatorPhase::Spilling;
                coordinator.active_mutator = None;
                lease.transition(
                    CoordinatorPhase::Spilling,
                    CoordinatorPhase::MemoryIdle,
                    false,
                );
                true
            }
            AddMode::Memory => {
                coordinator.phase = CoordinatorPhase::MemoryIdle;
                coordinator.active_mutator = None;
                false
            }
            AddMode::Disk => {
                coordinator.phase = CoordinatorPhase::DiskIdle;
                coordinator.active_mutator = None;
                false
            }
        };
        if !spill {
            lease.disarm();
        }
        drop(coordinator);
        if !spill {
            self.phase_changed.notify_all();
        }
        spill
    }

    fn action(&self) -> Option<Arc<SpillDiskAction>> {
        lock_unpoisoned(&self.action_spill).clone()
    }

    #[cfg(test)]
    fn set_spill_start_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *lock_unpoisoned(&self.spill_start_hook) = hook;
    }

    #[cfg(test)]
    fn run_reentrant_action_hook(&self) {
        let hook = lock_unpoisoned(&self.reentrant_action_hook).take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn set_reentrant_action_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *lock_unpoisoned(&self.reentrant_action_hook) = hook;
    }

    #[cfg(test)]
    fn run_later_action_hook(&self) {
        let hook = lock_unpoisoned(&self.later_action_hook).take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn set_later_action_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *lock_unpoisoned(&self.later_action_hook) = hook;
    }

    #[cfg(test)]
    fn run_before_fallback_hook(&self) {
        let hook = lock_unpoisoned(&self.before_fallback_hook).take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn set_before_fallback_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *lock_unpoisoned(&self.before_fallback_hook) = hook;
    }
}

impl Drop for RowContainerShared {
    fn drop(&mut self) {
        if let Some(action) = lock_unpoisoned(&self.action_spill).as_ref() {
            action.set_finished();
        }
        self.mem_tracker.detach();
        self.disk_tracker.detach();
        let records = self
            .records
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut in_disk) = records.in_disk.take() {
            in_disk.close();
        }
        records.in_memory.clear();
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
        })
        .unwrap_or_else(|| "spill to disk failed".to_owned())
}

#[derive(Clone, Copy)]
enum AddMode {
    Memory,
    Disk,
}

/// Go `RowContainer`, with shallow-copy semantics provided by [`Clone`].
#[derive(Clone)]
pub struct RowContainer {
    shared: Arc<RowContainerShared>,
}

impl RowContainer {
    /// Go `NewRowContainer`.
    #[must_use]
    pub fn new(field_types: &[FieldType], chunk_size: usize) -> Self {
        let list = List::new(field_types, chunk_size, chunk_size);
        let mem_tracker = Tracker::new(LABEL_FOR_ROW_CONTAINER, -1);
        list.mem_tracker().attach_to(&mem_tracker);
        RowContainer {
            shared: Arc::new(RowContainerShared {
                records: RwLock::new(RowContainerRecord {
                    in_memory: list,
                    in_disk: None,
                    spill_error: None,
                }),
                coordinator: Mutex::new(Coordinator::new()),
                phase_changed: Condvar::new(),
                mem_tracker,
                disk_tracker: disk::new_tracker(LABEL_FOR_ROW_CONTAINER, -1),
                action_spill: Mutex::new(None),
                #[cfg(test)]
                spill_start_hook: Mutex::new(None),
                #[cfg(test)]
                reentrant_action_hook: Mutex::new(None),
                #[cfg(test)]
                later_action_hook: Mutex::new(None),
                #[cfg(test)]
                before_fallback_hook: Mutex::new(None),
            }),
        }
    }

    /// Idiomatic equivalent of Go `ShallowCopyWithNewMutex`.
    #[must_use]
    pub fn shallow_copy(&self) -> Self {
        self.clone()
    }

    /// Go `alreadySpilled` and `AlreadySpilledSafeForTest`.
    #[must_use]
    pub fn already_spilled(&self) -> bool {
        read_unpoisoned(&self.shared.records).in_disk.is_some()
    }

    /// Go `GetMemTracker`.
    #[must_use]
    pub fn mem_tracker(&self) -> &Arc<Tracker> {
        &self.shared.mem_tracker
    }

    /// Go `GetDiskTracker`.
    #[must_use]
    pub fn disk_tracker(&self) -> &Arc<disk::Tracker> {
        &self.shared.disk_tracker
    }

    /// The error a failed spill recorded, if any (Go `records.spillError`).
    #[must_use]
    pub fn spill_error(&self) -> Option<String> {
        read_unpoisoned(&self.shared.records).spill_error.clone()
    }

    /// Go `ActionSpill`: the action, created on first use.
    pub fn action_spill(&mut self) -> Arc<SpillDiskAction> {
        let mut action = lock_unpoisoned(&self.shared.action_spill);
        Arc::clone(
            action.get_or_insert_with(|| {
                Arc::new(SpillDiskAction::new(Arc::downgrade(&self.shared)))
            }),
        )
    }

    /// Go `NumRow`.
    #[must_use]
    pub fn num_row(&self) -> usize {
        let records = read_unpoisoned(&self.shared.records);
        match &records.in_disk {
            Some(in_disk) => in_disk.len(),
            None => records.in_memory.len(),
        }
    }

    /// Whether the container holds no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.num_row() == 0
    }

    /// Go `NumChunks`.
    #[must_use]
    pub fn num_chunks(&self) -> usize {
        let records = read_unpoisoned(&self.shared.records);
        match &records.in_disk {
            Some(in_disk) => in_disk.num_chunks(),
            None => records.in_memory.num_chunks(),
        }
    }

    /// Go `NumRowsOfChunk`.
    #[must_use]
    pub fn num_rows_of_chunk(&self, chk_id: usize) -> usize {
        let records = read_unpoisoned(&self.shared.records);
        match &records.in_disk {
            Some(in_disk) => in_disk.num_rows_of_chunk(chk_id),
            None => records.in_memory.num_rows_of_chunk(chk_id),
        }
    }

    /// Go `AllocChunk`.
    pub fn alloc_chunk(&mut self) -> Chunk {
        write_unpoisoned(&self.shared.records)
            .in_memory
            .alloc_chunk()
    }

    /// Go `Add`: appends a chunk, to memory or to the spill file.
    ///
    /// The spill the memory-quota action asked for happens HERE, on the way
    /// out; see the module doc.
    pub fn add(&mut self, chk: Chunk) -> Result<(), DiskError> {
        let (mode, mut lease) = self.begin_add()?;
        let result = {
            let mut records = write_unpoisoned(&self.shared.records);
            match mode {
                AddMode::Memory => {
                    records.in_memory.add(chk);
                    Ok(())
                }
                AddMode::Disk => records
                    .in_disk
                    .as_mut()
                    .expect("disk phase has disk storage")
                    .add(&chk),
            }
        };
        if self.shared.finish_add(mode, &mut lease) {
            self.shared.perform_spill(&mut lease);
        }
        result
    }

    fn begin_add(&self) -> Result<(AddMode, PhaseLease<'_>), DiskError> {
        let mut coordinator = lock_unpoisoned(&self.shared.coordinator);
        loop {
            match coordinator.phase {
                CoordinatorPhase::MemoryIdle => {
                    let armed = coordinator.armed;
                    coordinator.phase = CoordinatorPhase::AddingMemory;
                    coordinator.active_mutator = Some(std::thread::current().id());
                    return Ok((
                        AddMode::Memory,
                        PhaseLease::new(
                            &self.shared,
                            CoordinatorPhase::AddingMemory,
                            CoordinatorPhase::MemoryIdle,
                            armed,
                        ),
                    ));
                }
                CoordinatorPhase::DiskIdle => {
                    coordinator.phase = CoordinatorPhase::AddingDisk;
                    coordinator.active_mutator = Some(std::thread::current().id());
                    return Ok((
                        AddMode::Disk,
                        PhaseLease::new(
                            &self.shared,
                            CoordinatorPhase::AddingDisk,
                            CoordinatorPhase::DiskIdle,
                            false,
                        ),
                    ));
                }
                CoordinatorPhase::Failed => {
                    drop(coordinator);
                    let error = read_unpoisoned(&self.shared.records)
                        .spill_error
                        .clone()
                        .unwrap_or_else(|| "row container spill failed".to_owned());
                    return Err(DiskError::Owned(error));
                }
                CoordinatorPhase::Closed => {
                    return Err(DiskError::Owned("row container is closed".to_owned()));
                }
                CoordinatorPhase::AddingMemory
                | CoordinatorPhase::AddingDisk
                | CoordinatorPhase::Spilling
                | CoordinatorPhase::ResettingMemory
                | CoordinatorPhase::ResettingDisk
                | CoordinatorPhase::Closing => {
                    coordinator = wait_unpoisoned(&self.shared.phase_changed, coordinator);
                }
            }
        }
    }

    /// Go `SpillToDisk`/`spillToDisk(nil)`: move every in-memory chunk into a
    /// fresh [`DataInDiskByRows`] and release the memory.
    pub fn spill_to_disk(&mut self) {
        let mut coordinator = lock_unpoisoned(&self.shared.coordinator);
        loop {
            match coordinator.phase {
                CoordinatorPhase::MemoryIdle => {
                    coordinator.armed = false;
                    coordinator.pending_spill = false;
                    coordinator.phase = CoordinatorPhase::Spilling;
                    let mut lease = PhaseLease::new(
                        &self.shared,
                        CoordinatorPhase::Spilling,
                        CoordinatorPhase::MemoryIdle,
                        false,
                    );
                    drop(coordinator);
                    self.shared.perform_spill(&mut lease);
                    return;
                }
                CoordinatorPhase::AddingMemory
                | CoordinatorPhase::AddingDisk
                | CoordinatorPhase::Spilling
                | CoordinatorPhase::ResettingMemory
                | CoordinatorPhase::ResettingDisk
                | CoordinatorPhase::Closing => {
                    coordinator = wait_unpoisoned(&self.shared.phase_changed, coordinator);
                }
                CoordinatorPhase::DiskIdle
                | CoordinatorPhase::Failed
                | CoordinatorPhase::Closed => return,
            }
        }
    }

    /// Go `GetChunk`: a guard-backed live view in memory and an owned decoded
    /// chunk on disk.
    pub fn get_chunk(&self, chk_idx: usize) -> Result<RowContainerChunk<'_>, DiskError> {
        let records = read_unpoisoned(&self.shared.records);
        if records.in_disk.is_none() {
            return Ok(RowContainerChunk {
                inner: RowContainerChunkInner::InMemory {
                    records,
                    chunk_index: chk_idx,
                },
            });
        }
        if let Some(error) = &records.spill_error {
            return Err(DiskError::Owned(error.clone()));
        }
        let chunk = records
            .in_disk
            .as_ref()
            .expect("disk storage was present")
            .get_chunk(chk_idx)?;
        Ok(RowContainerChunk {
            inner: RowContainerChunkInner::Owned(chunk),
        })
    }

    /// Owned chunk snapshot for cursors that must outlive the records guard.
    /// Public `get_chunk` deliberately keeps the in-memory no-copy contract.
    pub(crate) fn get_chunk_snapshot(&self, chk_idx: usize) -> Result<Chunk, DiskError> {
        self.get_chunk(chk_idx)
            .map(RowContainerChunk::into_snapshot)
    }

    /// Go `GetRowAndAlwaysAppendToChunk`: append the row `ptr` points at to
    /// `chk`, whether the container has spilled or not, and return its index
    /// in `chk`.
    pub fn get_row_and_always_append_to_chunk(
        &self,
        ptr: RowPtr,
        chk: &mut Chunk,
    ) -> Result<usize, DiskError> {
        let records = read_unpoisoned(&self.shared.records);
        match &records.in_disk {
            Some(in_disk) => {
                if let Some(error) = &records.spill_error {
                    return Err(DiskError::Owned(error.clone()));
                }
                in_disk.get_row_and_append_to_existing_chunk(ptr, chk)
            }
            None => {
                chk.append_row(records.in_memory.get_row(ptr));
                Ok(chk.num_rows() - 1)
            }
        }
    }

    /// The container's field types, owned for safe use outside the records
    /// guard.
    #[must_use]
    pub fn field_types(&self) -> Vec<FieldType> {
        read_unpoisoned(&self.shared.records)
            .in_memory
            .field_types()
            .to_vec()
    }

    /// Go `Reset`.
    pub fn reset(&mut self) {
        let (was_disk, mut lease) = match self.begin_reset() {
            Some(reset) => reset,
            None => return,
        };
        {
            let mut records = write_unpoisoned(&self.shared.records);
            if was_disk {
                if let Some(mut in_disk) = records.in_disk.take() {
                    in_disk.close();
                }
            } else {
                records.in_memory.reset();
            }
        }

        let mut coordinator = lock_unpoisoned(&self.shared.coordinator);
        let spill = !was_disk && coordinator.pending_spill;
        coordinator.pending_spill = false;
        if spill {
            coordinator.phase = CoordinatorPhase::Spilling;
            coordinator.armed = false;
            coordinator.active_mutator = None;
            lease.transition(
                CoordinatorPhase::Spilling,
                CoordinatorPhase::MemoryIdle,
                false,
            );
        } else {
            coordinator.generation = coordinator.generation.wrapping_add(1);
            coordinator.phase = CoordinatorPhase::MemoryIdle;
            coordinator.armed = true;
            coordinator.active_mutator = None;
        }
        if !spill {
            lease.disarm();
        }
        drop(coordinator);
        if spill {
            self.shared.perform_spill(&mut lease);
        } else {
            self.shared.phase_changed.notify_all();
        }
    }

    fn begin_reset(&self) -> Option<(bool, PhaseLease<'_>)> {
        let mut coordinator = lock_unpoisoned(&self.shared.coordinator);
        loop {
            if coordinator.fallback_active {
                coordinator = wait_unpoisoned(&self.shared.phase_changed, coordinator);
                continue;
            }
            match coordinator.phase {
                CoordinatorPhase::MemoryIdle => {
                    let armed = coordinator.armed;
                    coordinator.phase = CoordinatorPhase::ResettingMemory;
                    coordinator.active_mutator = Some(std::thread::current().id());
                    return Some((
                        false,
                        PhaseLease::new(
                            &self.shared,
                            CoordinatorPhase::ResettingMemory,
                            CoordinatorPhase::MemoryIdle,
                            armed,
                        ),
                    ));
                }
                CoordinatorPhase::DiskIdle | CoordinatorPhase::Failed => {
                    let unwind_to = coordinator.phase;
                    coordinator.phase = CoordinatorPhase::ResettingDisk;
                    coordinator.active_mutator = Some(std::thread::current().id());
                    return Some((
                        true,
                        PhaseLease::new(
                            &self.shared,
                            CoordinatorPhase::ResettingDisk,
                            unwind_to,
                            false,
                        ),
                    ));
                }
                CoordinatorPhase::Closed => return None,
                CoordinatorPhase::AddingMemory
                | CoordinatorPhase::AddingDisk
                | CoordinatorPhase::Spilling
                | CoordinatorPhase::ResettingMemory
                | CoordinatorPhase::ResettingDisk
                | CoordinatorPhase::Closing => {
                    coordinator = wait_unpoisoned(&self.shared.phase_changed, coordinator);
                }
            }
        }
    }

    /// Go `Close`.
    ///
    /// Go nils out `records.inMemory` so a later use panics; the list is
    /// cleared here instead, so a later read sees an empty container rather
    /// than a crash.
    pub fn close(&mut self) {
        let mut lease = match self.begin_close() {
            Some(lease) => lease,
            None => return,
        };
        self.shared.mem_tracker.detach();
        self.shared.disk_tracker.detach();
        {
            let mut records = write_unpoisoned(&self.shared.records);
            if let Some(mut in_disk) = records.in_disk.take() {
                in_disk.close();
            }
            records.in_memory.clear();
        }
        if let Some(action) = self.shared.action() {
            action.set_finished();
        }

        let mut coordinator = lock_unpoisoned(&self.shared.coordinator);
        coordinator.phase = CoordinatorPhase::Closed;
        coordinator.pending_spill = false;
        coordinator.armed = false;
        coordinator.active_mutator = None;
        lease.disarm();
        drop(coordinator);
        self.shared.phase_changed.notify_all();
    }

    fn begin_close(&self) -> Option<PhaseLease<'_>> {
        let mut coordinator = lock_unpoisoned(&self.shared.coordinator);
        loop {
            if coordinator.fallback_active {
                coordinator = wait_unpoisoned(&self.shared.phase_changed, coordinator);
                continue;
            }
            match coordinator.phase {
                CoordinatorPhase::MemoryIdle
                | CoordinatorPhase::DiskIdle
                | CoordinatorPhase::Failed => {
                    let unwind_to = coordinator.phase;
                    let armed = coordinator.armed;
                    coordinator.phase = CoordinatorPhase::Closing;
                    coordinator.pending_spill = false;
                    coordinator.armed = false;
                    coordinator.active_mutator = Some(std::thread::current().id());
                    return Some(PhaseLease::new(
                        &self.shared,
                        CoordinatorPhase::Closing,
                        unwind_to,
                        armed,
                    ));
                }
                CoordinatorPhase::Closed => return None,
                CoordinatorPhase::AddingMemory
                | CoordinatorPhase::AddingDisk
                | CoordinatorPhase::Spilling
                | CoordinatorPhase::ResettingMemory
                | CoordinatorPhase::ResettingDisk
                | CoordinatorPhase::Closing => {
                    coordinator = wait_unpoisoned(&self.shared.phase_changed, coordinator);
                }
            }
        }
    }

    #[cfg(test)]
    fn phase(&self) -> CoordinatorPhase {
        lock_unpoisoned(&self.shared.coordinator).phase
    }

    #[cfg(test)]
    fn set_spill_start_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        self.shared.set_spill_start_hook(hook);
    }

    #[cfg(test)]
    fn set_reentrant_action_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        self.shared.set_reentrant_action_hook(hook);
    }

    #[cfg(test)]
    fn set_later_action_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        self.shared.set_later_action_hook(hook);
    }

    #[cfg(test)]
    fn set_before_fallback_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        self.shared.set_before_fallback_hook(hook);
    }
}

/// Go `iterator4RowContainer`: the row iterator over a [`RowContainer`].
///
/// NOT a [`crate::iterator::ChunkIterator`]. That trait promises rows valid
/// for the ITERATOR'S OWN lifetime parameter, which an in-memory container can
/// honour but a spilled one cannot: a spilled row is decoded into a chunk this
/// iterator owns, so it lives exactly as long as the borrow of `self`. The
/// cursor arithmetic below is Go's, line for line, and `end` is `None` for the
/// same reason it is in `iterator.rs`.
pub struct Iterator4RowContainer<'a> {
    c: &'a RowContainer,
    chk_idx: usize,
    /// Go starts `Begin` at `rowIdx = -1`, so this is signed.
    row_idx: isize,
    /// The owned chunk carrying the current row.
    scratch: Option<Chunk>,
    err: Option<String>,
}

impl<'a> Iterator4RowContainer<'a> {
    /// Go `NewIterator4RowContainer`.
    #[must_use]
    pub fn new(c: &'a RowContainer) -> Self {
        Iterator4RowContainer {
            c,
            chk_idx: 0,
            row_idx: 0,
            scratch: None,
            err: None,
        }
    }

    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.c.num_row()
    }

    /// Whether the container has no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Go `Error`.
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.err.as_deref()
    }

    /// Go `ReachEnd`.
    pub fn reach_end(&mut self) {
        self.chk_idx = self.c.num_chunks();
        self.row_idx = 0;
    }

    /// Go `setNextPtr`.
    fn set_next_ptr(&mut self) {
        self.row_idx += 1;
        if self.row_idx as usize == self.c.num_rows_of_chunk(self.chk_idx) {
            self.row_idx = 0;
            self.chk_idx += 1;
        }
    }

    /// Go `Begin`.
    pub fn begin(&mut self) -> Option<Row<'_>> {
        self.chk_idx = 0;
        self.row_idx = -1;
        self.next_row()
    }

    /// Go `Next`.
    ///
    /// Go's `Next` moves the cursor and calls `Current`, which re-reads the
    /// row; the read happens here instead, because it needs `&mut self` to
    /// hold the decoded row. `Current` then returns what this read produced,
    /// which is what Go's re-read would produce.
    pub fn next_row(&mut self) -> Option<Row<'_>> {
        if self.chk_idx >= self.c.num_chunks() {
            self.reach_end();
            return None;
        }
        self.set_next_ptr();
        self.load_current();
        self.current()
    }

    /// The read half of Go's `Current`.
    fn load_current(&mut self) {
        self.scratch = None;
        if self.row_idx < 0 || self.chk_idx >= self.c.num_chunks() {
            return;
        }
        let ptr = RowPtr::new(self.chk_idx as u32, self.row_idx as u32);
        let mut chk = Chunk::new_with_capacity(&self.c.field_types(), 1);
        match self.c.get_row_and_always_append_to_chunk(ptr, &mut chk) {
            Ok(_) => self.scratch = Some(chk),
            Err(error) => {
                self.err = Some(error.to_string());
                self.reach_end();
            }
        }
    }

    /// Go `Current`.
    #[must_use]
    pub fn current(&self) -> Option<Row<'_>> {
        if self.row_idx < 0 || self.chk_idx >= self.c.num_chunks() {
            return None;
        }
        self.scratch
            .as_ref()
            .map(|chk| chk.get_row(chk.num_rows() - 1))
    }

    /// Go `End`: the invalid end position.
    #[must_use]
    pub fn end(&self) -> Option<Row<'_>> {
        None
    }
}

#[cfg(test)]
#[path = "row_container_test_hooks.rs"]
mod row_container_test_hooks;

#[cfg(test)]
mod tests {
    use super::row_container_test_hooks::*;
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{mpsc, Barrier};
    use tidb_datatype::FieldTypeCode as C;
    use tidb_util::memory::BaseOomAction;

    use crate::test_temp_storage::guard as temp_dir_guard;

    use crate::test_temp_storage::scratch_dir as scratch_temp_dir;

    fn int64_fields() -> Vec<FieldType> {
        vec![FieldType::new(C::LongLong)]
    }

    fn int64_chunk(sz: usize) -> Chunk {
        let fields = int64_fields();
        let mut chk = Chunk::new_with_capacity(&fields, sz);
        for i in 0..sz {
            chk.append_int64(0, i as i64);
        }
        chk
    }

    #[derive(Default)]
    struct CountingFallback {
        base: BaseOomAction,
        calls: AtomicUsize,
    }

    impl ActionOnExceed for CountingFallback {
        fn action(&self, _tracker: &Arc<Tracker>) {
            self.calls.fetch_add(1, SeqCst);
        }

        fn set_fallback(&self, action: Option<ArcAction>) {
            self.base.set_fallback(action);
        }

        fn get_fallback(&self) -> Option<ArcAction> {
            self.base.get_fallback()
        }

        fn get_priority(&self) -> i64 {
            0
        }

        fn set_finished(&self) {
            self.base.set_finished();
        }

        fn is_finished(&self) -> bool {
            self.base.is_finished()
        }
    }

    struct PausingFallback {
        base: BaseOomAction,
        calls: AtomicUsize,
        started: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    impl ActionOnExceed for PausingFallback {
        fn action(&self, _tracker: &Arc<Tracker>) {
            self.calls.fetch_add(1, SeqCst);
            self.started.wait();
            self.release.wait();
        }

        fn set_fallback(&self, action: Option<ArcAction>) {
            self.base.set_fallback(action);
        }

        fn get_fallback(&self) -> Option<ArcAction> {
            self.base.get_fallback()
        }

        fn get_priority(&self) -> i64 {
            0
        }

        fn set_finished(&self) {
            self.base.set_finished();
        }

        fn is_finished(&self) -> bool {
            self.base.is_finished()
        }
    }

    /// Every row of the container, in order, as its first column's int64.
    fn iterate(rc: &RowContainer) -> Vec<i64> {
        let mut out = Vec::new();
        let mut it = Iterator4RowContainer::new(rc);
        let mut row = it.begin();
        while row.is_some() {
            out.push(row.expect("row").get_int64(0));
            row = it.next_row();
        }
        assert_eq!(it.error(), None);
        out
    }

    /// Go `TestNewRowContainer`.
    #[test]
    fn a_new_row_container_has_not_spilled() {
        let rc = RowContainer::new(&int64_fields(), 1024);
        assert!(!rc.already_spilled());
        assert_eq!(rc.num_row(), 0);
    }

    /// In memory `GetChunk` exposes the live chunk under a records read guard;
    /// it does not deep-clone row buffers merely because the container state is
    /// shared.
    #[test]
    fn get_chunk_keeps_the_live_in_memory_view() {
        let mut rc = RowContainer::new(&int64_fields(), 4);
        rc.add(int64_chunk(4)).expect("add");
        let stored = {
            let records = read_unpoisoned(&rc.shared.records);
            records.in_memory.get_chunk(0) as *const Chunk
        };
        let view = rc.get_chunk(0).expect("live chunk view");
        assert_eq!(&*view as *const Chunk, stored);
        assert_eq!(view.get_row(3).get_int64(0), 3);
    }

    /// Go `TestSel`: the selection vector survives the move to disk.
    ///
    /// Go drives this through `NewMultiIterator(NewIterator4RowContainer(rc),
    /// NewIterator4Chunk(chk))`; [`Iterator4RowContainer`] is not a
    /// `ChunkIterator` (see its doc), so the container half is iterated on its
    /// own here and the trailing chunk is checked separately.
    #[test]
    fn a_selection_vector_survives_the_spill() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("sel");
        disk::set_temp_storage_path(&dir);

        let fields = int64_fields();
        let sz = 4usize;
        let n = 64usize;
        let mut rc = RowContainer::new(&fields, sz);
        let mut chk = Chunk::new_with_capacity(&fields, sz);
        let mut num_rows = 0;
        for i in 0..(n - sz) {
            chk.append_int64(0, i as i64);
            if chk.num_rows() == sz {
                chk.set_sel(Some(vec![0, 2]));
                num_rows += 2;
                rc.add(chk).expect("add");
                chk = Chunk::new_with_capacity(&fields, sz);
            }
        }
        assert_eq!(rc.num_chunks(), num_rows / 2);
        assert_eq!(rc.num_row(), num_rows);

        // Rows 0 and 2 of each four-row chunk.
        let want: Vec<i64> = (0..(n - sz) as i64)
            .filter(|i| i % 4 == 0 || i % 4 == 2)
            .collect();
        assert_eq!(iterate(&rc), want, "in memory");

        rc.spill_to_disk();
        assert_eq!(rc.spill_error(), None);
        assert!(rc.already_spilled());
        assert_eq!(iterate(&rc), want, "after spilling");

        rc.close();
        assert_eq!(rc.mem_tracker().bytes_consumed(), 0);
        assert!(rc.mem_tracker().max_consumed() > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `TestSpillAction`: the second chunk pushes the tracker past its
    /// limit, the container moves to disk, and later adds go straight there.
    #[test]
    fn the_spill_action_moves_the_container_to_disk() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("spillaction");
        disk::set_temp_storage_path(&dir);

        let fields = int64_fields();
        let sz = 4;
        let mut rc = RowContainer::new(&fields, sz);
        let chk = int64_chunk(sz);
        let action = rc.action_spill();
        rc.mem_tracker().set_bytes_limit(chk.memory_usage() + 1);
        rc.mem_tracker()
            .fallback_old_and_set_new_action(Arc::clone(&action) as ArcAction);

        assert!(!rc.already_spilled());
        rc.add(chk.clone()).expect("add");
        assert!(!rc.already_spilled(), "one chunk is within the quota");
        assert_eq!(rc.mem_tracker().bytes_consumed(), chk.memory_usage());

        // Go's comment: adding the same chunk twice double-counts its memory;
        // that is the point, it is how the quota is crossed.
        rc.add(chk.clone()).expect("add");
        assert!(rc.already_spilled(), "the quota was crossed");

        {
            let res = rc.get_chunk(0).expect("get_chunk");
            assert_eq!(res.num_rows(), chk.num_rows());
            for row_idx in 0..res.num_rows() {
                assert_eq!(
                    res.get_row(row_idx).get_int64(0),
                    chk.get_row(row_idx).get_int64(0)
                );
            }
        }

        // Written again, this time straight to the spill file.
        rc.add(chk.clone()).expect("add");
        assert!(rc.already_spilled());
        {
            let res = rc.get_chunk(2).expect("get_chunk");
            assert_eq!(res.num_rows(), chk.num_rows());
            for row_idx in 0..res.num_rows() {
                assert_eq!(
                    res.get_row(row_idx).get_int64(0),
                    chk.get_row(row_idx).get_int64(0)
                );
            }
        }

        rc.reset();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `List::add` may make two positive `Consume` calls when its tail was not
    /// accounted yet. Both calls reenter the same action stack: the first arms
    /// pending spill and every later call must return rather than wait on the
    /// add that cannot finish until it returns.
    #[test]
    fn repeated_reentrant_actions_return_to_the_same_add() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("repeated-reentrant-add");
        disk::set_temp_storage_path(&dir);

        let mut rc = RowContainer::new(&int64_fields(), 4);
        let seed = int64_chunk(1);
        write_unpoisoned(&rc.shared.records)
            .in_memory
            .append_row(seed.get_row(0));
        assert_eq!(rc.mem_tracker().bytes_consumed(), 0, "tail is unaccounted");

        let action = rc.action_spill();
        rc.mem_tracker().set_bytes_limit(1);
        rc.mem_tracker()
            .set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));
        rc.add(int64_chunk(1)).expect("reentrant add finishes");

        assert!(rc.already_spilled());
        assert_eq!(rc.mem_tracker().bytes_consumed(), 0);
        assert_eq!(iterate(&rc), vec![0, 0]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A repeat on the mutating thread must return to `List::add`, but a
    /// second thread is a later action: it waits for the pending spill and
    /// then checks fallback instead of disappearing with the reentrant call.
    #[test]
    fn a_concurrent_second_action_waits_for_the_reentrant_add_spill() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("concurrent-second-action");
        disk::set_temp_storage_path(&dir);

        let mut rc = RowContainer::new(&int64_fields(), 4);
        let statement_tracker = Tracker::new(-1, 1);
        rc.mem_tracker().attach_to(&statement_tracker);
        let unrelated_tracker = Tracker::new(-2, -1);
        unrelated_tracker.attach_to(&statement_tracker);
        unrelated_tracker.consume(2);

        let action = rc.action_spill();
        let fallback = Arc::new(CountingFallback::default());
        action.set_fallback(Some(Arc::clone(&fallback) as ArcAction));
        statement_tracker.set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));
        let (reentrant_started, reentrant_release) = pause_next_reentrant_action(&rc);
        let (later_started, later_release) = pause_next_later_action(&rc);

        let mut adding = rc.shallow_copy();
        let add_handle = std::thread::spawn(move || adding.add(int64_chunk(4)));
        reentrant_started.wait();

        let later_action = Arc::clone(&action);
        let later_tracker = Arc::clone(&statement_tracker);
        let (done_tx, done_rx) = mpsc::channel();
        let later_handle = std::thread::spawn(move || {
            later_action.action(&later_tracker);
            done_tx.send(()).expect("report later action");
        });
        later_started.wait();
        assert_eq!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));

        later_release.wait();
        reentrant_release.wait();
        add_handle
            .join()
            .expect("add thread")
            .expect("reentrant add");
        later_handle.join().expect("later action thread");
        done_rx.recv().expect("later action completion");

        assert!(rc.already_spilled());
        assert_eq!(fallback.calls.load(SeqCst), 1);
        unrelated_tracker.consume(-2);
        rc.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `List::reset` accounts its final unaccounted tail and can therefore
    /// reenter the spill action. Reset releases records before processing that
    /// pending spill, which clears the accounted freelist memory.
    #[test]
    fn resetting_memory_processes_its_reentrant_spill() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("reentrant-reset");
        disk::set_temp_storage_path(&dir);

        let mut rc = RowContainer::new(&int64_fields(), 4);
        let seed = int64_chunk(1);
        write_unpoisoned(&rc.shared.records)
            .in_memory
            .append_row(seed.get_row(0));
        let action = rc.action_spill();
        rc.mem_tracker().set_bytes_limit(1);
        rc.mem_tracker()
            .set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));

        rc.reset();

        assert!(rc.already_spilled());
        assert_eq!(rc.num_row(), 0);
        assert_eq!(rc.mem_tracker().bytes_consumed(), 0);
        assert_eq!(action.status(), SpillStatus::SpilledYet);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A quota action belongs to the shared statement tracker, so any child
    /// allocation that crosses that quota must be able to spill this
    /// container. The spill cannot depend on a later `RowContainer::add`.
    #[test]
    fn an_unrelated_parent_allocation_spills_without_another_add() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("unrelated-parent-allocation");
        disk::set_temp_storage_path(&dir);

        let fields = int64_fields();
        let mut rc = RowContainer::new(&fields, 4);
        let statement_tracker = Tracker::new(-1, -1);
        rc.mem_tracker().attach_to(&statement_tracker);
        let unrelated_tracker = Tracker::new(-2, -1);
        unrelated_tracker.attach_to(&statement_tracker);

        let action = rc.action_spill();
        statement_tracker.fallback_old_and_set_new_action(Arc::clone(&action) as ArcAction);
        let chk = int64_chunk(4);
        let container_bytes = chk.memory_usage();
        statement_tracker.set_bytes_limit(container_bytes + 1);

        rc.add(chk).expect("final add");
        assert!(!rc.already_spilled(), "the final add is below quota");
        assert_eq!(rc.mem_tracker().bytes_consumed(), container_bytes);
        assert_eq!(iterate(&rc), vec![0, 1, 2, 3]);

        unrelated_tracker.consume(2);

        assert!(
            rc.already_spilled(),
            "the parent action must spill without another RowContainer::add"
        );
        assert_eq!(rc.mem_tracker().bytes_consumed(), 0);
        assert!(rc.disk_tracker().bytes_consumed() > 0);
        assert_eq!(iterate(&rc), vec![0, 1, 2, 3]);

        rc.close();
        unrelated_tracker.consume(-2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The first trigger is reserved for spill even when unrelated memory
    /// remains above quota. Only a later trigger may invoke fallback.
    #[test]
    fn fallback_runs_only_after_the_first_trigger_finishes_spilling() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("fallback-after-spill");
        disk::set_temp_storage_path(&dir);

        let mut rc = RowContainer::new(&int64_fields(), 4);
        let statement_tracker = Tracker::new(-1, -1);
        rc.mem_tracker().attach_to(&statement_tracker);
        let unrelated_tracker = Tracker::new(-2, -1);
        unrelated_tracker.attach_to(&statement_tracker);
        let action = rc.action_spill();
        let fallback = Arc::new(CountingFallback::default());
        action.set_fallback(Some(Arc::clone(&fallback) as ArcAction));
        statement_tracker.set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));

        let chk = int64_chunk(4);
        let bytes = chk.memory_usage();
        statement_tracker.set_bytes_limit(bytes + 1);
        rc.add(chk).expect("add below quota");
        unrelated_tracker.consume(bytes + 2);

        assert!(rc.already_spilled());
        assert_eq!(fallback.calls.load(SeqCst), 0, "first trigger spills only");
        unrelated_tracker.consume(1);
        assert_eq!(fallback.calls.load(SeqCst), 1, "later trigger falls back");

        rc.close();
        unrelated_tracker.consume(-(bytes + 3));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// If reset publishes a new generation before a waiting action claims the
    /// fallback slot, that action re-enters as the first trigger of the new
    /// generation. It must not run a stale fallback from the spilled state.
    #[test]
    fn reset_wins_the_race_with_a_waiting_fallback() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("reset-wins-fallback-race");
        disk::set_temp_storage_path(&dir);

        let mut rc = RowContainer::new(&int64_fields(), 4);
        let statement_tracker = Tracker::new(-1, 1);
        rc.mem_tracker().attach_to(&statement_tracker);
        let unrelated_tracker = Tracker::new(-2, -1);
        unrelated_tracker.attach_to(&statement_tracker);
        unrelated_tracker.consume(2);
        let action = rc.action_spill();
        let fallback = Arc::new(CountingFallback::default());
        action.set_fallback(Some(Arc::clone(&fallback) as ArcAction));
        statement_tracker.set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));
        rc.add(int64_chunk(4)).expect("initial spill");
        assert!(rc.already_spilled());

        let (claim_started, claim_release) = pause_next_fallback_claim(&rc);
        let waiting_action = Arc::clone(&action);
        let waiting_tracker = Arc::clone(&statement_tracker);
        let action_handle = std::thread::spawn(move || waiting_action.action(&waiting_tracker));
        claim_started.wait();

        rc.reset();
        assert_eq!(rc.phase(), CoordinatorPhase::MemoryIdle);
        claim_release.wait();
        action_handle.join().expect("new-generation action");

        assert!(rc.already_spilled(), "the re-entered first action spills");
        assert_eq!(
            fallback.calls.load(SeqCst),
            0,
            "the old-generation fallback must not run"
        );
        unrelated_tracker.consume(-2);
        rc.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// If a later action claims fallback first, reset waits for that callback
    /// to finish before it closes storage and publishes the next generation.
    #[test]
    fn fallback_wins_the_race_with_reset() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("fallback-wins-reset-race");
        disk::set_temp_storage_path(&dir);

        let mut rc = RowContainer::new(&int64_fields(), 4);
        let statement_tracker = Tracker::new(-1, 1);
        rc.mem_tracker().attach_to(&statement_tracker);
        let unrelated_tracker = Tracker::new(-2, -1);
        unrelated_tracker.attach_to(&statement_tracker);
        unrelated_tracker.consume(2);
        let action = rc.action_spill();
        let fallback_started = Arc::new(Barrier::new(2));
        let fallback_release = Arc::new(Barrier::new(2));
        let fallback = Arc::new(PausingFallback {
            base: BaseOomAction::default(),
            calls: AtomicUsize::new(0),
            started: Arc::clone(&fallback_started),
            release: Arc::clone(&fallback_release),
        });
        action.set_fallback(Some(Arc::clone(&fallback) as ArcAction));
        statement_tracker.set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));
        rc.add(int64_chunk(4)).expect("initial spill");
        assert!(rc.already_spilled());

        let waiting_action = Arc::clone(&action);
        let waiting_tracker = Arc::clone(&statement_tracker);
        let action_handle = std::thread::spawn(move || waiting_action.action(&waiting_tracker));
        fallback_started.wait();

        let mut resetting = rc.shallow_copy();
        let (reset_tx, reset_rx) = mpsc::channel();
        let reset_handle = std::thread::spawn(move || {
            resetting.reset();
            reset_tx.send(()).expect("report reset");
        });
        assert_eq!(reset_rx.try_recv(), Err(mpsc::TryRecvError::Empty));

        fallback_release.wait();
        action_handle.join().expect("fallback action");
        reset_handle.join().expect("reset after fallback");
        reset_rx.recv().expect("reset completion");

        assert_eq!(fallback.calls.load(SeqCst), 1);
        assert_eq!(rc.phase(), CoordinatorPhase::MemoryIdle);
        assert!(!rc.already_spilled());
        unrelated_tracker.consume(-2);
        rc.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `TestRowContainerResetAndAction`: after a reset the container spills
    /// again, which only works if the action's `once` was re-armed.
    #[test]
    fn a_reset_container_spills_again() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("resetaction");
        disk::set_temp_storage_path(&dir);

        let fields = int64_fields();
        let sz = 20;
        let mut rc = RowContainer::new(&fields, sz);
        let chk = int64_chunk(sz);
        let action = rc.action_spill();
        rc.mem_tracker().set_bytes_limit(chk.memory_usage() + 1);
        rc.mem_tracker()
            .fallback_old_and_set_new_action(Arc::clone(&action) as ArcAction);

        rc.add(chk.clone()).expect("add");
        assert_eq!(rc.disk_tracker().bytes_consumed(), 0);
        rc.add(chk.clone()).expect("add");
        assert!(rc.disk_tracker().bytes_consumed() > 0);

        rc.reset();
        assert_eq!(rc.disk_tracker().bytes_consumed(), 0);
        assert!(!rc.already_spilled());
        assert_eq!(action.status(), SpillStatus::NotSpilled);

        rc.add(chk.clone()).expect("add");
        rc.add(chk.clone()).expect("add");
        assert!(rc.disk_tracker().bytes_consumed() > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `TestActionBlocked`, case 1: ten adds under a small quota end with
    /// the action in `spilledYet`, the memory released, and disk in use.
    #[test]
    fn ten_adds_under_quota_end_spilled_with_the_memory_released() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("actionblocked1");
        disk::set_temp_storage_path(&dir);

        let fields = int64_fields();
        let sz = 4;
        let mut rc = RowContainer::new(&fields, sz);
        let action = rc.action_spill();
        rc.mem_tracker().set_bytes_limit(1450);
        rc.mem_tracker()
            .fallback_old_and_set_new_action(Arc::clone(&action) as ArcAction);
        for _ in 0..10 {
            rc.add(int64_chunk(sz)).expect("add");
        }
        assert_eq!(action.status(), SpillStatus::SpilledYet);
        assert_eq!(rc.mem_tracker().bytes_consumed(), 0);
        assert!(rc.mem_tracker().max_consumed() > 0);
        assert!(rc.disk_tracker().bytes_consumed() > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `TestActionBlocked`, case 2: an action that arrives while a spill is
    /// in flight WAITS for it instead of falling through to the fallback,
    /// because the memory is about to be released.
    #[test]
    fn an_action_blocks_while_a_spill_is_in_flight() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("action-waits-for-spill");
        disk::set_temp_storage_path(&dir);

        let mut rc = RowContainer::new(&int64_fields(), 4);
        rc.add(int64_chunk(4)).expect("add");
        let tracker = Arc::clone(rc.mem_tracker());
        let action = rc.action_spill();
        let (started, release) = pause_next_spill(&rc);
        let mut spilling = rc.shallow_copy();
        let spill_handle = std::thread::spawn(move || spilling.spill_to_disk());
        started.wait();

        let (done_tx, done_rx) = mpsc::channel();
        let waiting_action = Arc::clone(&action);
        let action_handle = std::thread::spawn(move || {
            waiting_action.action(&tracker);
            done_tx.send(()).expect("report completion");
        });
        assert_eq!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));

        release.wait();
        spill_handle.join().expect("spill thread");
        action_handle.join().expect("action thread");
        done_rx.recv().expect("action completion");
        rc.set_spill_start_hook(None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `TestSpillActionDeadLock`: an action firing CONCURRENTLY with `Add`
    /// must not deadlock. Go needs a goroutine to avoid taking the write lock
    /// under the caller's read lock. Here the reentrant action only arms the
    /// coordinator; `add` releases records before it performs the spill.
    #[test]
    fn a_concurrent_action_and_add_do_not_deadlock() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("deadlock");
        disk::set_temp_storage_path(&dir);

        let fields = int64_fields();
        let sz = 4;
        let mut rc = RowContainer::new(&fields, sz);
        let tracker = Arc::clone(rc.mem_tracker());
        let action = rc.action_spill();
        rc.mem_tracker().set_bytes_limit(1);
        rc.mem_tracker()
            .fallback_old_and_set_new_action(Arc::clone(&action) as ArcAction);

        let hammer = Arc::clone(&action);
        let hammer_tracker = Arc::clone(&tracker);
        let handle = std::thread::spawn(move || {
            for _ in 0..100 {
                hammer.action(&hammer_tracker);
            }
        });
        rc.add(int64_chunk(sz)).expect("add");
        handle.join().expect("the action thread must finish");
        assert!(rc.already_spilled());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Shallow handles share records and synchronization rather than closing
    /// or snapshotting one another's state.
    #[test]
    fn shallow_copy_observes_spill_reset_and_close() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("shallow-copy");
        disk::set_temp_storage_path(&dir);

        let mut rc = RowContainer::new(&int64_fields(), 4);
        rc.add(int64_chunk(4)).expect("first add");
        rc.add(int64_chunk(4)).expect("second add");
        let mut copy = rc.shallow_copy();
        assert!(Arc::ptr_eq(&rc.shared, &copy.shared));

        let reading = copy.shallow_copy();
        let reader = std::thread::spawn(move || iterate(&reading));
        rc.spill_to_disk();
        assert_eq!(reader.join().expect("reader"), vec![0, 1, 2, 3, 0, 1, 2, 3]);
        assert!(copy.already_spilled());

        copy.reset();
        assert_eq!(rc.phase(), CoordinatorPhase::MemoryIdle);
        assert_eq!(rc.num_row(), 0);
        rc.add(int64_chunk(2)).expect("add after reset");
        assert_eq!(iterate(&copy), vec![0, 1]);

        rc.close();
        assert_eq!(copy.phase(), CoordinatorPhase::Closed);
        assert!(copy.add(int64_chunk(1)).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Reset and close claim lifecycle phases only after an active spill has
    /// published its terminal disk phase; neither may strand a waiter.
    #[test]
    fn reset_and_close_serialize_with_spill() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("lifecycle-vs-spill");
        disk::set_temp_storage_path(&dir);

        let mut reset_rc = RowContainer::new(&int64_fields(), 4);
        reset_rc.add(int64_chunk(4)).expect("reset add");
        let (started, release) = pause_next_spill(&reset_rc);
        let mut spilling = reset_rc.shallow_copy();
        let spill_handle = std::thread::spawn(move || spilling.spill_to_disk());
        started.wait();
        let (reset_tx, reset_rx) = mpsc::channel();
        let mut resetting = reset_rc.shallow_copy();
        let reset_handle = std::thread::spawn(move || {
            resetting.reset();
            reset_tx.send(()).expect("report reset");
        });
        assert_eq!(reset_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release.wait();
        spill_handle.join().expect("spill before reset");
        reset_handle.join().expect("reset thread");
        reset_rx.recv().expect("reset completion");
        reset_rc.set_spill_start_hook(None);
        assert_eq!(reset_rc.phase(), CoordinatorPhase::MemoryIdle);
        assert!(!reset_rc.already_spilled());

        let mut close_rc = RowContainer::new(&int64_fields(), 4);
        close_rc.add(int64_chunk(4)).expect("close add");
        let (started, release) = pause_next_spill(&close_rc);
        let mut spilling = close_rc.shallow_copy();
        let spill_handle = std::thread::spawn(move || spilling.spill_to_disk());
        started.wait();
        let (close_tx, close_rx) = mpsc::channel();
        let mut closing = close_rc.shallow_copy();
        let close_handle = std::thread::spawn(move || {
            closing.close();
            close_tx.send(()).expect("report close");
        });
        assert_eq!(close_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release.wait();
        spill_handle.join().expect("spill before close");
        close_handle.join().expect("close thread");
        close_rx.recv().expect("close completion");
        assert_eq!(close_rc.phase(), CoordinatorPhase::Closed);
        assert_eq!(close_rc.mem_tracker().bytes_consumed(), 0);
        assert_eq!(close_rc.disk_tracker().bytes_consumed(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The iterator's cursor protocol, on a container that never spills.
    #[test]
    fn the_iterator_walks_an_in_memory_container() {
        let fields = int64_fields();
        let mut rc = RowContainer::new(&fields, 4);
        rc.add(int64_chunk(4)).expect("add");
        rc.add(int64_chunk(4)).expect("add");

        let mut it = Iterator4RowContainer::new(&rc);
        assert_eq!(it.len(), 8);
        assert_eq!(it.begin().expect("first").get_int64(0), 0);
        let mut seen = 1;
        while it.next_row().is_some() {
            seen += 1;
        }
        assert_eq!(seen, 8);
        // Past the end the cursor stays parked.
        assert!(it.current().is_none());
        assert!(it.next_row().is_none());
    }

    /// Go `TestInterruptedDuringSpilling`: a KILL raised while a long spill is
    /// running is noticed, because the spill loop polls the session killer
    /// after every chunk.
    ///
    /// Go proves it by timing -- 102400 chunks, a kill after 200ms, and the
    /// spill must stop inside a second. The rule under the timing is the
    /// per-chunk poll, so it is checked directly here: the signal is pending
    /// before the spill starts, and the first poll must raise it.
    #[test]
    fn a_kill_signal_stops_a_spill_in_progress() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("killduringspill");
        disk::set_temp_storage_path(&dir);

        let root = Tracker::new(-1, -1);
        root.is_root_tracker_of_sess
            .store(true, std::sync::atomic::Ordering::SeqCst);
        root.killer.conn_id.store(1, SeqCst);

        let fields = int64_fields();
        let mut rc = RowContainer::new(&fields, 20);
        rc.mem_tracker().attach_to(&root);
        rc.add(int64_chunk(20)).expect("add");
        rc.add(int64_chunk(20)).expect("add");

        root.killer
            .send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
        rc.spill_to_disk();
        // Go recovers the kill panic inside `spillToDisk` and leaves it in
        // `spillError`, which every later read reports.
        let error = rc.spill_error().expect("the kill must abort the spill");
        assert!(
            error.contains("1317") || error.to_lowercase().contains("interrupt"),
            "{error}"
        );
        let mut chk = Chunk::new_with_capacity(&fields, 1);
        assert!(rc
            .get_row_and_always_append_to_chunk(RowPtr::new(0, 0), &mut chk)
            .is_err());

        rc.reset();
        assert_eq!(
            rc.spill_error().as_deref(),
            Some(error.as_str()),
            "Go preserves records.spillError across reset"
        );
        rc.spill_to_disk();
        assert_eq!(rc.spill_error().as_deref(), Some(error.as_str()));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
