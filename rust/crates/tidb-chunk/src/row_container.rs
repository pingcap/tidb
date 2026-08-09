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
//! Go's spill goroutine, per-handle lock fan-out, cache padding, and
//! `WaitForTest` do not change package behavior and are not reproduced.

use std::cmp::Ordering;
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

    /// Whether an in-memory operation handed off a pending spill.
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

    pub(crate) fn action_with_admission(&self, tracker: &Arc<Tracker>, admitted: bool) {
        if admitted {
            self.action(tracker);
        } else {
            let Some(shared) = self.shared.upgrade() else {
                self.finished.store(true, SeqCst);
                return;
            };
            let thread_id = std::thread::current().id();
            let mut coordinator = lock_unpoisoned(&shared.coordinator);
            loop {
                let reentrant = coordinator.active_mutator == Some(thread_id);
                if coordinator.fallback_active
                    || (!reentrant
                        && matches!(
                            coordinator.phase,
                            CoordinatorPhase::AddingMemory
                                | CoordinatorPhase::AddingDisk
                                | CoordinatorPhase::Spilling
                                | CoordinatorPhase::ResettingMemory
                                | CoordinatorPhase::ResettingDisk
                                | CoordinatorPhase::Closing
                        ))
                {
                    coordinator = wait_unpoisoned(&shared.phase_changed, coordinator);
                    continue;
                }
                if coordinator.phase == CoordinatorPhase::Closed {
                    return;
                }
                coordinator.fallback_active = true;
                break;
            }
            drop(coordinator);
            let _lease = FallbackLease { shared: &shared };
            self.invoke_fallback_if_needed(tracker);
        }
    }

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

struct RowContainerRecord {
    in_memory: List,
    in_disk: Option<DataInDiskByRows>,
    spill_error: Option<String>,
}
type PreSpill = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;
enum RowContainerChunkInner<'a> {
    InMemory {
        records: RwLockReadGuard<'a, RowContainerRecord>,
        chunk_index: usize,
    },
    Owned(Chunk),
}

/// A live guarded in-memory chunk or owned decoded disk chunk.
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
    storage: Arc<disk::SpillStorage>,
    action_spill: Mutex<Option<Arc<SpillDiskAction>>>,
    pre_spill: Mutex<Option<PreSpill>>,
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

        let pre_spill = lock_unpoisoned(&self.pre_spill).clone();
        let pre_spill_error =
            pre_spill.and_then(
                |prepare| match catch_unwind(AssertUnwindSafe(|| prepare())) {
                    Ok(Ok(())) => None,
                    Ok(Err(message)) => Some(message),
                    Err(payload) => Some(panic_message(payload.as_ref())),
                },
            );

        let mut records = write_unpoisoned(&self.records);
        let spill = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
            if records.in_disk.is_none() {
                let in_disk = DataInDiskByRows::new(
                    records.in_memory.field_types().to_vec(),
                    Arc::clone(&self.storage),
                );
                in_disk.disk_tracker().attach_to(&self.disk_tracker);
                records.in_disk = Some(in_disk);
            }

            if let Some(message) = pre_spill_error {
                return Err(message);
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
    pub fn new(
        field_types: &[FieldType],
        chunk_size: usize,
        storage: Arc<disk::SpillStorage>,
    ) -> Self {
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
                storage,
                action_spill: Mutex::new(None),
                pre_spill: Mutex::new(None),
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

    /// Idiomatic Go `ShallowCopyWithNewMutex` equivalent.
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
        self.action_spill_shared()
    }

    pub(crate) fn action_spill_shared(&self) -> Arc<SpillDiskAction> {
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

    /// Go `Add`: appends a chunk to memory or the spill file.
    pub fn add(&mut self, chk: Chunk) -> Result<(), DiskError> {
        self.add_shared_with_prepare(chk, || Ok(()))
    }

    pub(crate) fn add_shared_with_prepare<Prepare, Guard>(
        &self,
        chk: Chunk,
        prepare: Prepare,
    ) -> Result<(), DiskError>
    where
        Prepare: FnOnce() -> Result<Guard, DiskError>,
    {
        let (mode, mut lease) = self.begin_add()?;
        let guard = match prepare() {
            Ok(guard) => guard,
            Err(error) => {
                if self.shared.finish_add(mode, &mut lease) {
                    self.shared.perform_spill(&mut lease);
                }
                return Err(error);
            }
        };
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
        drop(guard);
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

    /// Go `SpillToDisk`/`spillToDisk(nil)`.
    pub fn spill_to_disk(&mut self) {
        self.spill_to_disk_shared();
    }

    pub(crate) fn spill_to_disk_shared(&self) {
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

    /// Go `GetChunk`.
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

    pub(crate) fn get_chunk_snapshot(&self, chk_idx: usize) -> Result<Chunk, DiskError> {
        self.get_chunk(chk_idx)
            .map(RowContainerChunk::into_snapshot)
    }

    /// Go `GetRowAndAlwaysAppendToChunk`.
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

    /// Returns an owned copy of the configured field types.
    #[must_use]
    pub fn field_types(&self) -> Vec<FieldType> {
        read_unpoisoned(&self.shared.records)
            .in_memory
            .field_types()
            .to_vec()
    }

    pub(crate) fn in_memory_row_ptrs(&self) -> Result<Vec<RowPtr>, DiskError> {
        let records = read_unpoisoned(&self.shared.records);
        if records.in_disk.is_some() {
            return Err(DiskError::Owned(
                "cannot initialize sorted row pointers after spill".to_owned(),
            ));
        }
        let mut pointers = Vec::with_capacity(records.in_memory.len());
        for chunk_index in 0..records.in_memory.num_chunks() {
            for row_index in 0..records.in_memory.num_rows_of_chunk(chunk_index) {
                pointers.push(RowPtr::new(chunk_index as u32, row_index as u32));
            }
        }
        Ok(pointers)
    }

    pub(crate) fn sort_in_memory_row_ptrs_by(
        &self,
        pointers: &mut [RowPtr],
        mut compare: impl FnMut(Row<'_>, Row<'_>) -> Ordering,
    ) -> Result<(), DiskError> {
        let records = read_unpoisoned(&self.shared.records);
        if records.in_disk.is_some() {
            return Err(DiskError::Owned(
                "cannot sort row pointers after spill".to_owned(),
            ));
        }
        pointers.sort_unstable_by(|left, right| {
            compare(
                records.in_memory.get_row(*left),
                records.in_memory.get_row(*right),
            )
        });
        Ok(())
    }

    /// Go `Reset`.
    pub fn reset(&mut self) {
        self.reset_shared();
    }

    pub(crate) fn reset_shared(&self) {
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

    /// Go `Close`; later safe Rust reads observe the cleared container.
    pub fn close(&mut self) {
        self.close_shared();
    }

    pub(crate) fn close_shared(&self) {
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

    pub(crate) fn set_pre_spill(&self, prepare: PreSpill) {
        let mut pre_spill = lock_unpoisoned(&self.shared.pre_spill);
        assert!(
            pre_spill.is_none(),
            "row container pre-spill hook already set"
        );
        *pre_spill = Some(prepare);
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

    #[cfg(test)]
    pub(crate) fn set_spill_error_for_test(&self, message: impl Into<String>) {
        write_unpoisoned(&self.shared.records).spill_error = Some(message.into());
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
#[path = "row_container_tests.rs"]
mod tests;
