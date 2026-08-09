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

//! The sorted half of `pkg/util/chunk/row_container.go`.
//!
//! [`SortedRowContainer`] keeps the underlying chunks in insertion order and
//! sorts only eight-byte row pointers. Its plain [`RowContainer`] remains the
//! sole authority for spill files, stored errors, fallback ordering, reset,
//! close, and concurrent quota actions. A weak pre-spill callback seals and
//! sorts the pointers before that authority takes its records write lock.

use std::cmp::Ordering;
use std::ops::Deref;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use tidb_datatype::FieldType;
use tidb_util::disk::{SpillStorage, Tracker as DiskTracker};
use tidb_util::memory::{ActionOnExceed, ArcAction, Tracker};

use crate::chunk::Chunk;
use crate::chunk_in_disk::DiskError;
use crate::compare::CompareFunc;
use crate::list::{RowPtr, ROW_PTR_SIZE};
use crate::row::Row;
use crate::row_container::{RowContainer, RowContainerChunk, SpillDiskAction, SpillStatus};

/// Go `SignalCheckpointForSort`.
pub const SIGNAL_CHECKPOINT_FOR_SORT: usize = 10_240;

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        .unwrap_or_else(|| "sort row container failed".to_owned())
}

#[derive(Default)]
struct SortedState {
    row_ptrs: Option<Vec<RowPtr>>,
    comparisons: usize,
}

struct SortedInner {
    rows: RowContainer,
    state: Mutex<SortedState>,
    by_items_desc: Vec<bool>,
    key_columns: Vec<usize>,
    key_compare_funcs: Vec<Option<CompareFunc>>,
    mem_tracker: Arc<Tracker>,
    pointer_bytes: AtomicI64,
    resetting: std::sync::atomic::AtomicBool,
    action_spill: Mutex<Option<Arc<SortAndSpillDiskAction>>>,
    #[cfg(test)]
    sort_error: Mutex<Option<String>>,
    #[cfg(test)]
    before_add_prepare: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

struct ResetAdmission<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for ResetAdmission<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

impl SortedInner {
    fn is_sorted(&self) -> bool {
        lock_unpoisoned(&self.state).row_ptrs.is_some()
    }

    fn sort(&self) -> Result<(), DiskError> {
        let mut state = lock_unpoisoned(&self.state);
        if state.row_ptrs.is_some() {
            return Ok(());
        }
        state.row_ptrs = Some(self.rows.in_memory_row_ptrs()?);
        let mut comparisons = state.comparisons;
        let pointers = state
            .row_ptrs
            .as_mut()
            .expect("sorted pointers initialized");
        let sorted = catch_unwind(AssertUnwindSafe(|| -> Result<(), DiskError> {
            #[cfg(test)]
            if let Some(message) = lock_unpoisoned(&self.sort_error).clone() {
                return Err(DiskError::Owned(message));
            }

            self.rows
                .sort_in_memory_row_ptrs_by(pointers, |left, right| {
                    if comparisons >= SIGNAL_CHECKPOINT_FOR_SORT {
                        self.mem_tracker.handle_kill_signal();
                        comparisons = 0;
                    }
                    comparisons += 1;
                    self.compare_rows(left, right)
                })
        }));
        state.comparisons = comparisons;
        match sorted {
            Ok(result) => result,
            Err(payload) => Err(DiskError::Owned(panic_message(payload.as_ref()))),
        }
    }

    fn compare_rows(&self, left: Row<'_>, right: Row<'_>) -> Ordering {
        for ((column, descending), compare) in self
            .key_columns
            .iter()
            .zip(&self.by_items_desc)
            .zip(&self.key_compare_funcs)
        {
            let Some(compare) = compare else {
                continue;
            };
            let order = compare(left, *column, right, *column);
            if order != Ordering::Equal {
                return if *descending { order.reverse() } else { order };
            }
        }
        Ordering::Equal
    }

    fn clear_pointer_state(&self) {
        let bytes = self
            .pointer_bytes
            .swap(0, std::sync::atomic::Ordering::SeqCst);
        let mut state = lock_unpoisoned(&self.state);
        state.row_ptrs = None;
        state.comparisons = 0;
        drop(state);
        if bytes != 0 {
            self.mem_tracker.consume(-bytes);
        }
    }
}

impl Drop for SortedInner {
    fn drop(&mut self) {
        self.rows.close_shared();
        self.clear_pointer_state();
        self.mem_tracker.detach();
    }
}

/// Go `SortedRowContainer`.
pub struct SortedRowContainer {
    inner: Arc<SortedInner>,
}

impl SortedRowContainer {
    /// Go `NewSortedRowContainer`.
    #[must_use]
    pub fn new(
        field_types: &[FieldType],
        chunk_size: usize,
        by_items_desc: Vec<bool>,
        key_columns: Vec<usize>,
        key_compare_funcs: Vec<Option<CompareFunc>>,
        storage: Arc<SpillStorage>,
    ) -> Self {
        assert_eq!(
            by_items_desc.len(),
            key_columns.len(),
            "one descending flag is required for each sorted key"
        );
        assert_eq!(
            key_compare_funcs.len(),
            key_columns.len(),
            "one comparator slot is required for each sorted key"
        );
        let rows = RowContainer::new(field_types, chunk_size, storage);
        let mem_tracker = Tracker::new(tidb_util::memory::LABEL_FOR_ROW_CONTAINER, -1);
        rows.mem_tracker().attach_to(&mem_tracker);
        let inner = Arc::new(SortedInner {
            rows,
            state: Mutex::new(SortedState::default()),
            by_items_desc,
            key_columns,
            key_compare_funcs,
            mem_tracker,
            pointer_bytes: AtomicI64::new(0),
            resetting: std::sync::atomic::AtomicBool::new(false),
            action_spill: Mutex::new(None),
            #[cfg(test)]
            sort_error: Mutex::new(None),
            #[cfg(test)]
            before_add_prepare: Mutex::new(None),
        });
        let weak = Arc::downgrade(&inner);
        inner.rows.set_pre_spill(Arc::new(move || {
            let Some(inner) = weak.upgrade() else {
                return Ok(());
            };
            inner.sort().map_err(|error| error.to_string())
        }));
        SortedRowContainer { inner }
    }

    /// Go `Add` with the pointer reservation in the same coordinated adding
    /// transaction as the chunk insertion.
    pub fn add(&mut self, chunk: Chunk) -> Result<(), DiskError> {
        if self.inner.is_sorted() {
            return Err(DiskError::CannotAddBecauseSorted);
        }
        let pointer_bytes = (chunk.num_rows() * ROW_PTR_SIZE) as i64;
        let result = self.inner.rows.add_shared_with_prepare(chunk, || {
            #[cfg(test)]
            if let Some(hook) = lock_unpoisoned(&self.inner.before_add_prepare).take() {
                hook();
            }
            let state = lock_unpoisoned(&self.inner.state);
            if state.row_ptrs.is_some() {
                return Err(DiskError::CannotAddBecauseSorted);
            }
            self.inner
                .pointer_bytes
                .fetch_add(pointer_bytes, std::sync::atomic::Ordering::SeqCst);
            self.inner.mem_tracker.consume(pointer_bytes);
            Ok(state)
        });
        match result {
            Err(_) if self.inner.is_sorted() => Err(DiskError::CannotAddBecauseSorted),
            other => other,
        }
    }

    /// Seals and orders the row pointers. Repeated calls are no-ops.
    pub fn sort(&self) -> Result<(), DiskError> {
        self.inner.sort()
    }

    /// Sorts and spills through the plain container's single spill authority.
    pub fn spill_to_disk(&mut self) {
        self.inner.rows.spill_to_disk_shared();
    }

    /// Go `GetSortedRow`.
    pub fn get_sorted_row(&self, index: usize) -> Result<SortedRow<'_>, DiskError> {
        let pointer = lock_unpoisoned(&self.inner.state)
            .row_ptrs
            .as_ref()
            .expect("sorted rows requested before Sort")[index];
        let chunk = self.inner.rows.get_chunk(pointer.chk_idx as usize)?;
        Ok(SortedRow {
            chunk,
            row_index: pointer.row_idx as usize,
        })
    }

    /// Go `GetSortedRowAndAlwaysAppendToChunk`.
    pub fn get_sorted_row_and_always_append_to_chunk(
        &self,
        index: usize,
        chunk: &mut Chunk,
    ) -> Result<usize, DiskError> {
        let pointer = lock_unpoisoned(&self.inner.state)
            .row_ptrs
            .as_ref()
            .expect("sorted rows requested before Sort")[index];
        self.inner
            .rows
            .get_row_and_always_append_to_chunk(pointer, chunk)
    }

    /// Go `ActionSpill`.
    pub fn action_spill(&mut self) -> Arc<SortAndSpillDiskAction> {
        let mut action = lock_unpoisoned(&self.inner.action_spill);
        Arc::clone(action.get_or_insert_with(|| {
            Arc::new(SortAndSpillDiskAction {
                inner: Arc::downgrade(&self.inner),
                base: self.inner.rows.action_spill_shared(),
                serial: Mutex::new(()),
                #[cfg(test)]
                after_admission: Mutex::new(None),
            })
        }))
    }

    /// Go `GetMemTracker`.
    #[must_use]
    pub fn mem_tracker(&self) -> &Arc<Tracker> {
        &self.inner.mem_tracker
    }

    /// Promoted Go `GetDiskTracker`.
    #[must_use]
    pub fn disk_tracker(&self) -> &Arc<DiskTracker> {
        self.inner.rows.disk_tracker()
    }

    /// Promoted Go `AlreadySpilledSafeForTest`.
    #[must_use]
    pub fn already_spilled(&self) -> bool {
        self.inner.rows.already_spilled()
    }

    /// Promoted Go `NumRow`.
    #[must_use]
    pub fn num_row(&self) -> usize {
        self.inner.rows.num_row()
    }

    /// Whether the promoted container holds no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.rows.is_empty()
    }

    /// Promoted Go `NumChunks`.
    #[must_use]
    pub fn num_chunks(&self) -> usize {
        self.inner.rows.num_chunks()
    }

    /// Promoted Go `NumRowsOfChunk`.
    #[must_use]
    pub fn num_rows_of_chunk(&self, chunk_index: usize) -> usize {
        self.inner.rows.num_rows_of_chunk(chunk_index)
    }

    /// Promoted Go `AllocChunk`.
    pub fn alloc_chunk(&mut self) -> Chunk {
        self.inner.rows.shallow_copy().alloc_chunk()
    }

    /// Returns an owned copy of the promoted container's field types.
    #[must_use]
    pub fn field_types(&self) -> Vec<FieldType> {
        self.inner.rows.field_types()
    }

    /// Promoted Go `GetChunk`; physical chunk order remains insertion order.
    pub fn get_chunk(&self, index: usize) -> Result<RowContainerChunk<'_>, DiskError> {
        self.inner.rows.get_chunk(index)
    }

    /// Promoted stored spill error.
    #[must_use]
    pub fn spill_error(&self) -> Option<String> {
        self.inner.rows.spill_error()
    }

    /// The configured descending flags.
    #[must_use]
    pub fn by_items_desc(&self) -> &[bool] {
        &self.inner.by_items_desc
    }

    /// Resets both storage and sorted pointer accounting for idiomatic reuse.
    pub fn reset(&mut self) {
        self.inner
            .resetting
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _reset_admission = ResetAdmission(&self.inner.resetting);
        let action = lock_unpoisoned(&self.inner.action_spill).clone();
        let _serial = action
            .as_ref()
            .map(|action| lock_unpoisoned(&action.serial));
        self.inner.clear_pointer_state();
        self.inner.rows.reset_shared();
        self.inner.clear_pointer_state();
    }

    /// Releases storage and pointer accounting. Repeated calls are harmless.
    pub fn close(&mut self) {
        self.inner.rows.close_shared();
        self.inner.clear_pointer_state();
    }

    #[cfg(test)]
    fn set_sort_error(&self, message: Option<&str>) {
        *lock_unpoisoned(&self.inner.sort_error) = message.map(str::to_owned);
    }

    #[cfg(test)]
    fn set_before_add_prepare(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *lock_unpoisoned(&self.inner.before_add_prepare) = hook;
    }
}

/// One row selected by sorted pointer order.
pub struct SortedRow<'a> {
    chunk: RowContainerChunk<'a>,
    row_index: usize,
}

impl SortedRow<'_> {
    /// Returns the row cursor over the guarded/owned chunk.
    #[must_use]
    pub fn row(&self) -> Row<'_> {
        self.chunk.get_row(self.row_index)
    }
}

impl Deref for SortedRow<'_> {
    type Target = Chunk;

    fn deref(&self) -> &Self::Target {
        &self.chunk
    }
}

/// Go `SortAndSpillDiskAction`, sharing the plain action's state and fallback.
pub struct SortAndSpillDiskAction {
    inner: Weak<SortedInner>,
    base: Arc<SpillDiskAction>,
    serial: Mutex<()>,
    #[cfg(test)]
    after_admission: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl SortAndSpillDiskAction {
    /// Shared spill status.
    #[must_use]
    pub fn status(&self) -> SpillStatus {
        self.base.status()
    }

    #[cfg(test)]
    fn set_after_admission(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *lock_unpoisoned(&self.after_admission) = hook;
    }
}

impl ActionOnExceed for SortAndSpillDiskAction {
    fn action(&self, tracker: &Arc<Tracker>) {
        if self
            .inner
            .upgrade()
            .is_some_and(|inner| inner.resetting.load(std::sync::atomic::Ordering::SeqCst))
        {
            self.base.action_with_admission(tracker, false);
            return;
        }
        let _serial = lock_unpoisoned(&self.serial);
        let admitted = self.inner.upgrade().is_some_and(|inner| {
            !inner.resetting.load(std::sync::atomic::Ordering::SeqCst)
                && inner
                    .pointer_bytes
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0
                && inner.mem_tracker.bytes_consumed() > tracker.get_bytes_limit() / 10
        });
        #[cfg(test)]
        if let Some(hook) = lock_unpoisoned(&self.after_admission).take() {
            hook();
        }
        self.base.action_with_admission(tracker, admitted);
    }

    fn set_fallback(&self, action: Option<ArcAction>) {
        self.base.set_fallback(action);
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }

    fn get_priority(&self) -> i64 {
        self.base.get_priority()
    }

    fn set_finished(&self) {
        self.base.set_finished();
    }

    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
    use std::sync::mpsc;
    use std::sync::Barrier;
    use std::time::{Duration, Instant};

    use tidb_datatype::FieldTypeCode;

    use super::*;
    use crate::compare::get_compare_func;
    use crate::test_temp_storage::{isolated_storage, storage};
    use tidb_util::disk::SpillEncryptionMethod;
    use tidb_util::sqlkiller::KillSignal;

    fn fields(width: usize) -> Vec<FieldType> {
        vec![FieldType::new(FieldTypeCode::LongLong); width]
    }

    fn sorted(width: usize, desc: Vec<bool>, keys: Vec<usize>) -> SortedRowContainer {
        let field_types = fields(width);
        let compares = keys
            .iter()
            .map(|column| get_compare_func(&field_types[*column]))
            .collect();
        SortedRowContainer::new(
            &field_types,
            32,
            desc,
            keys,
            compares,
            isolated_storage("sorted", SpillEncryptionMethod::Plaintext),
        )
    }

    #[derive(Default)]
    struct CountingFallback {
        calls: AtomicUsize,
    }

    impl ActionOnExceed for CountingFallback {
        fn action(&self, _tracker: &Arc<Tracker>) {
            self.calls.fetch_add(1, SeqCst);
        }

        fn set_fallback(&self, _action: Option<ArcAction>) {}

        fn get_fallback(&self) -> Option<ArcAction> {
            None
        }

        fn get_priority(&self) -> i64 {
            0
        }

        fn set_finished(&self) {}

        fn is_finished(&self) -> bool {
            false
        }
    }

    #[test]
    fn constructor_rejects_mismatched_sort_key_vectors() {
        let field_types = fields(1);
        assert!(catch_unwind(AssertUnwindSafe(|| {
            SortedRowContainer::new(
                &field_types,
                1,
                vec![],
                vec![0],
                vec![get_compare_func(&field_types[0])],
                storage(),
            )
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            SortedRowContainer::new(&field_types, 1, vec![false], vec![0], vec![], storage())
        }))
        .is_err());
    }

    #[test]
    fn multi_key_sort_is_pointer_only_and_idempotent() {
        let mut rows = sorted(2, vec![false, true], vec![0, 1]);
        let mut chunk = Chunk::new(&fields(2), 4, 32);
        for (first, second) in [(2, 1), (1, 1), (1, 3), (1, 2)] {
            chunk.append_int64(0, first);
            chunk.append_int64(1, second);
        }
        rows.add(chunk).unwrap();
        assert!(!rows.is_empty());
        assert_eq!(rows.num_chunks(), 1);
        assert_eq!(rows.num_rows_of_chunk(0), 4);
        assert_eq!(rows.field_types().len(), 2);
        assert_eq!(rows.alloc_chunk().num_cols(), 2);
        rows.sort().unwrap();
        rows.sort().unwrap();

        let sorted_values = (0..4)
            .map(|index| {
                let row = rows.get_sorted_row(index).unwrap();
                (row.row().get_int64(0), row.row().get_int64(1))
            })
            .collect::<Vec<_>>();
        assert_eq!(sorted_values, [(1, 3), (1, 2), (1, 1), (2, 1)]);

        let physical = rows.get_chunk(0).unwrap();
        assert_eq!(physical.get_row(0).get_int64(0), 2);
        assert_eq!(physical.get_row(0).get_int64(1), 1);
    }

    #[test]
    fn pointer_charge_trigger_sorts_and_spills_without_deadlock() {
        let field_types = fields(1);
        let mut rows = SortedRowContainer::new(
            &field_types,
            20,
            vec![false],
            vec![0],
            vec![get_compare_func(&field_types[0])],
            storage(),
        );
        let mut chunk = Chunk::new(&field_types, 20, 20);
        for value in 0..20 {
            chunk.append_int64(0, value);
        }
        let one_chunk_bytes = chunk.memory_usage() + 20 * ROW_PTR_SIZE as i64;
        rows.mem_tracker().set_bytes_limit(one_chunk_bytes + 1);
        let action = rows.action_spill();
        rows.mem_tracker().fallback_old_and_set_new_action(action);
        rows.add(chunk.clone()).unwrap();
        assert!(!rows.already_spilled());
        assert_eq!(rows.mem_tracker().bytes_consumed(), one_chunk_bytes);

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = rows.add(chunk);
            tx.send((rows, result)).unwrap();
        });
        let (rows, result) = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("pointer-triggered spill must not deadlock");
        result.unwrap();
        assert!(rows.already_spilled());
        assert_eq!(rows.num_row(), 40);
        for index in 0..40 {
            assert_eq!(
                rows.get_sorted_row(index).unwrap().row().get_int64(0),
                (index / 2) as i64
            );
        }
    }

    #[test]
    fn concurrent_second_action_waits_for_spill_then_invokes_fallback() {
        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let first_compare = Arc::new(AtomicBool::new(true));
        let compare: CompareFunc = Box::new({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            move |left, left_col, right, right_col| {
                if first_compare.swap(false, SeqCst) {
                    started.wait();
                    release.wait();
                }
                left.get_int64(left_col).cmp(&right.get_int64(right_col))
            }
        });
        let field_types = fields(1);
        let mut rows = SortedRowContainer::new(
            &field_types,
            2,
            vec![false],
            vec![0],
            vec![Some(compare)],
            storage(),
        );
        let statement = Tracker::new(-994, 1);
        rows.mem_tracker().attach_to(&statement);
        let unrelated = Tracker::new(-995, -1);
        unrelated.attach_to(&statement);
        unrelated.consume(2);

        let action = rows.action_spill();
        let fallback = Arc::new(CountingFallback::default());
        action.set_fallback(Some(Arc::clone(&fallback) as ArcAction));
        statement.set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));

        let mut chunk = Chunk::new(&field_types, 2, 2);
        chunk.append_int64(0, 2);
        chunk.append_int64(0, 1);
        let mut adding = SortedRowContainer {
            inner: Arc::clone(&rows.inner),
        };
        let add = std::thread::spawn(move || adding.add(chunk));
        started.wait();

        let later_action = Arc::clone(&action);
        let later_statement = Arc::clone(&statement);
        let (done_tx, done_rx) = mpsc::channel();
        let later = std::thread::spawn(move || {
            later_action.action(&later_statement);
            done_tx.send(()).unwrap();
        });
        assert_eq!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release.wait();

        add.join().unwrap().unwrap();
        later.join().unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("later action completes after spill");
        assert!(rows.already_spilled());
        assert_eq!(fallback.calls.load(SeqCst), 1);
        unrelated.consume(-2);
        rows.close();
    }

    #[test]
    fn admission_is_strictly_greater_than_ten_percent() {
        let mut rows = sorted(1, vec![false], vec![0]);
        let mut chunk = Chunk::new(&fields(1), 1, 32);
        chunk.append_int64(0, 7);
        rows.add(chunk).unwrap();
        let bytes = rows.mem_tracker().bytes_consumed();
        let action = rows.action_spill();
        let trigger = Tracker::new(-991, bytes * 10);
        action.action(&trigger);
        assert!(!rows.already_spilled());
        trigger.set_bytes_limit(bytes * 10 - 1);
        action.action(&trigger);
        assert!(rows.already_spilled());
    }

    #[test]
    fn sort_error_is_stored_but_late_add_keeps_the_sentinel() {
        let mut rows = sorted(1, vec![false], vec![0]);
        let mut chunk = Chunk::new(&fields(1), 1, 32);
        chunk.append_int64(0, 1);
        rows.add(chunk).unwrap();
        rows.set_sort_error(Some("sort meet error"));
        rows.spill_to_disk();
        assert!(rows.already_spilled());
        let error = match rows.get_sorted_row(0) {
            Ok(_) => panic!("sort error must be replayed"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "sort meet error");

        let late = Chunk::new(&fields(1), 1, 32);
        assert!(matches!(
            rows.add(late),
            Err(DiskError::CannotAddBecauseSorted)
        ));
    }

    #[test]
    fn reset_and_drop_release_pointer_accounting() {
        let parent = Tracker::new(-992, -1);
        let mut rows = sorted(1, vec![false], vec![0]);
        rows.mem_tracker().attach_to(&parent);
        let mut chunk = Chunk::new(&fields(1), 2, 32);
        chunk.append_int64(0, 2);
        chunk.append_int64(0, 1);
        rows.add(chunk).unwrap();
        let before_reset = parent.bytes_consumed();
        let action = rows.action_spill();
        rows.sort().unwrap();
        rows.reset();
        assert_eq!(
            before_reset - parent.bytes_consumed(),
            2 * ROW_PTR_SIZE as i64
        );
        let trigger = Tracker::new(-996, 1);
        trigger.consume(2);
        action.action(&trigger);
        assert!(!rows.already_spilled());

        let mut chunk = Chunk::new(&fields(1), 1, 32);
        chunk.append_int64(0, 3);
        rows.add(chunk).unwrap();
        assert!(parent.bytes_consumed() > 0);
        drop(rows);
        assert_eq!(parent.bytes_consumed(), 0);
    }

    #[test]
    fn reset_drains_admission_observed_before_reset() {
        let mut rows = sorted(1, vec![false], vec![0]);
        let mut chunk = Chunk::new(&fields(1), 1, 32);
        chunk.append_int64(0, 1);
        rows.add(chunk).unwrap();
        let action = rows.action_spill();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        action.set_after_admission(Some(Arc::new({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || {
                entered.wait();
                release.wait();
            }
        })));

        let trigger = Tracker::new(-997, 1);
        let action_thread = std::thread::spawn({
            let action = Arc::clone(&action);
            move || action.action(&trigger)
        });
        entered.wait();

        let reset_state = Arc::clone(&rows.inner);
        let (reset_tx, reset_rx) = mpsc::channel();
        let reset_thread = std::thread::spawn(move || {
            rows.reset();
            reset_tx.send(rows).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while !reset_state
            .resetting
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            assert!(Instant::now() < deadline, "reset must disable admission");
            std::thread::yield_now();
        }
        release.wait();

        action_thread.join().unwrap();
        reset_thread.join().unwrap();
        let mut rows = reset_rx.recv().unwrap();
        assert!(!rows.already_spilled());
        assert!(rows.is_empty());
        let mut chunk = Chunk::new(&fields(1), 1, 32);
        chunk.append_int64(0, 2);
        rows.add(chunk).unwrap();
        rows.sort().unwrap();
        assert_eq!(rows.get_sorted_row(0).unwrap().row().get_int64(0), 2);
    }

    #[test]
    fn add_and_sort_linearize_without_omitting_the_racing_row() {
        let mut rows = sorted(1, vec![false], vec![0]);
        let mut first = Chunk::new(&fields(1), 1, 32);
        first.append_int64(0, 1);
        rows.add(first).unwrap();

        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        rows.set_before_add_prepare(Some(Arc::new({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || {
                entered.wait();
                release.wait();
            }
        })));
        let mut add_handle = SortedRowContainer {
            inner: Arc::clone(&rows.inner),
        };
        let sort_handle = SortedRowContainer {
            inner: Arc::clone(&rows.inner),
        };
        let add = std::thread::spawn(move || {
            let mut second = Chunk::new(&fields(1), 1, 32);
            second.append_int64(0, 2);
            add_handle.add(second)
        });
        entered.wait();
        sort_handle.sort().unwrap();
        release.wait();
        assert!(matches!(
            add.join().unwrap(),
            Err(DiskError::CannotAddBecauseSorted)
        ));
        assert_eq!(rows.num_row(), 1);
        assert_eq!(rows.get_sorted_row(0).unwrap().row().get_int64(0), 1);
    }

    #[test]
    fn close_waits_for_pre_spill_sort_without_lock_inversion() {
        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let first_compare = Arc::new(AtomicBool::new(true));
        let compare: CompareFunc = Box::new({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            move |left, left_col, right, right_col| {
                if first_compare.swap(false, SeqCst) {
                    started.wait();
                    release.wait();
                }
                left.get_int64(left_col).cmp(&right.get_int64(right_col))
            }
        });
        let field_types = fields(1);
        let mut rows = SortedRowContainer::new(
            &field_types,
            2,
            vec![false],
            vec![0],
            vec![Some(compare)],
            storage(),
        );
        let mut chunk = Chunk::new(&field_types, 2, 2);
        chunk.append_int64(0, 2);
        chunk.append_int64(0, 1);
        rows.add(chunk).unwrap();
        let mut spill_handle = SortedRowContainer {
            inner: Arc::clone(&rows.inner),
        };
        let (spill_tx, spill_rx) = mpsc::channel();
        std::thread::spawn(move || {
            spill_handle.spill_to_disk();
            spill_tx.send(()).unwrap();
        });
        started.wait();
        let (close_tx, close_rx) = mpsc::channel();
        std::thread::spawn(move || {
            rows.close();
            close_tx.send(()).unwrap();
        });
        release.wait();
        spill_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("spill completes");
        close_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("close completes after spill");
    }

    #[test]
    fn empty_sort_seals_and_long_sort_polls_the_query_killer() {
        let mut empty =
            SortedRowContainer::new(&fields(1), 1, vec![false], vec![0], vec![None], storage());
        empty.sort().unwrap();
        assert!(matches!(
            empty.add(Chunk::new(&fields(1), 0, 1)),
            Err(DiskError::CannotAddBecauseSorted)
        ));

        let mut rows = sorted(1, vec![false], vec![0]);
        let mut chunk = Chunk::new(&fields(1), 20_000, 20_000);
        for value in (0..20_000).rev() {
            chunk.append_int64(0, value);
        }
        rows.add(chunk).unwrap();
        let session = Tracker::new(-993, -1);
        session
            .is_root_tracker_of_sess
            .store(true, std::sync::atomic::Ordering::SeqCst);
        rows.mem_tracker().attach_to(&session);
        session
            .killer
            .send_kill_signal(KillSignal::QueryInterrupted);
        let error = rows.sort().expect_err("sort must observe query kill");
        assert!(error
            .to_string()
            .contains("Query execution was interrupted"));
    }
}
