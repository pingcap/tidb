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

//! `pkg/util/chunk/row_container.go`: rows held in memory until the query's
//! memory quota is exceeded, then MOVED WHOLESALE to a row-addressed spill
//! file.
//!
//! The container is one of two things at any moment -- a [`List`] in memory,
//! or a [`DataInDiskByRows`] on disk -- and every accessor switches on which.
//! The switch happens once, in [`RowContainer::spill_to_disk`], driven by
//! [`SpillDiskAction`] registered on a memory tracker.
//!
//! # Where the spill happens (faithful adaptation, not a behavior change)
//!
//! Go's action does not spill: it starts a GOROUTINE that spills, and returns
//! at once. It must, because the caller is inside `RowContainer.Add`, which
//! holds the container's read lock, and the spill needs the write lock --
//! spilling inline would self-deadlock. `TestSpillActionDeadLock` guards
//! exactly that.
//!
//! Here the action raises a flag ([`SpillDiskAction::is_triggered`]) and
//! [`RowContainer::add`] performs the spill on its way out, after the tracker
//! consume that raised the flag. The rows spilled are the same rows Go's
//! goroutine finds (the just-added chunk included), and no lock is held across
//! the action at all, so the deadlock Go's goroutine avoids CANNOT ARISE --
//! the special case disappears instead of being handled. This is the shape
//! `tidb_executor::sort_partition` already uses for the sort's spill.
//!
//! # Rows and lifetimes
//!
//! Go's `GetRow` returns a `Row` that keeps alive either the in-memory chunk
//! or a fresh chunk read from disk. A Rust [`Row`] is a borrow that cannot
//! own the chunk it points into, so the accessors here either append into a
//! chunk the CALLER owns ([`RowContainer::get_row_and_always_append_to_chunk`])
//! or hand back the chunk itself ([`RowContainer::get_chunk`], borrowed when
//! in memory and owned when read from disk, which is exactly what Go returns
//! in each case).
//!
//! # Not ported, named
//!
//! - `ShallowCopyWithNewMutex` and `mutexForRowContainer`'s `wLocks` fan-out:
//!   a lock-contention optimisation for many goroutines sharing one container.
//!   This port has no lock to contend on, so there is nothing to clone.
//! - `ActionSpillForTest`/`WaitForTest`: they exist to join the spill
//!   goroutine. The spill is synchronous here, so there is nothing to wait
//!   for; the plain [`RowContainer::action_spill`] covers both Go entry points.
//! - `SortedRowContainer` and `SortAndSpillDiskAction` (the second half of the
//!   Go file): the sorted variant. `tidb_executor::sort_partition` holds the
//!   rows and the spill file directly rather than through a container, so
//!   nothing in this port needs the sorted variant yet.
//! - `failpoint` injection points (`spillToDiskOutOfDiskQuota`,
//!   `testRowContainerDeadLock`).

use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex};

use tidb_datatype::FieldType;
use tidb_util::disk;
use tidb_util::memory::{
    ActionOnExceed, ArcAction, BaseOomAction, Tracker, DEF_SPILL_PRIORITY, LABEL_FOR_ROW_CONTAINER,
};

use crate::chunk::Chunk;
use crate::chunk_in_disk::DiskError;
use crate::list::{List, RowPtr};
use crate::row::Row;
use crate::row_in_disk::DataInDiskByRows;

/// Go `spillStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpillStatus {
    /// Go `notSpilled`.
    NotSpilled,
    /// Go `spilling`.
    Spilling,
    /// Go `spilledYet`.
    SpilledYet,
}

/// Go `spillStatusCond`: the status plus the condition variable every waiting
/// action blocks on while a spill is in flight.
struct SpillStatusCond {
    status: Mutex<SpillStatus>,
    cond: Condvar,
}

impl SpillStatusCond {
    fn new() -> Self {
        SpillStatusCond {
            status: Mutex::new(SpillStatus::NotSpilled),
            cond: Condvar::new(),
        }
    }
}

/// Go `SpillDiskAction` + `baseSpillDiskAction`: the `ActionOnExceed` a
/// [`RowContainer`] registers on the statement's memory tracker.
pub struct SpillDiskAction {
    base: BaseOomAction,
    /// Go `m`: serialises concurrent `Action` calls.
    action_lock: Mutex<()>,
    /// Go `once`: only the FIRST exceeding call starts a spill.
    once: Mutex<bool>,
    cond: SpillStatusCond,
    /// Raised for [`RowContainer::add`] to observe; see the module doc.
    triggered: AtomicBool,
}

impl SpillDiskAction {
    fn new() -> Self {
        SpillDiskAction {
            base: BaseOomAction::default(),
            action_lock: Mutex::new(()),
            once: Mutex::new(false),
            cond: SpillStatusCond::new(),
            triggered: AtomicBool::new(false),
        }
    }

    /// Go `getStatus`.
    #[must_use]
    pub fn status(&self) -> SpillStatus {
        *self.cond.status.lock().unwrap()
    }

    /// Go `setStatus`.
    pub fn set_status(&self, status: SpillStatus) {
        *self.cond.status.lock().unwrap() = status;
    }

    /// Go `cond.Broadcast`.
    pub fn broadcast(&self) {
        self.cond.cond.notify_all();
    }

    /// Whether a spill has been asked for and not yet performed.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        self.triggered.load(SeqCst)
    }

    fn take_trigger(&self) -> bool {
        self.triggered.swap(false, SeqCst)
    }

    /// Go `Reset`: back to `notSpilled`, and the `once` armed again.
    pub fn reset(&self) {
        let _guard = self.action_lock.lock().unwrap();
        self.set_status(SpillStatus::NotSpilled);
        *self.once.lock().unwrap() = false;
        self.triggered.store(false, SeqCst);
    }
}

impl ActionOnExceed for SpillDiskAction {
    /// Go `baseSpillDiskAction.action` with `RowContainer`'s
    /// `hasEnoughDataToSpill`, which is unconditionally true.
    fn action(&self, t: &Arc<Tracker>) {
        let _guard = self.action_lock.lock().unwrap();

        if self.status() == SpillStatus::NotSpilled {
            let mut once = self.once.lock().unwrap();
            if !*once {
                *once = true;
                tracing::info!(
                    consumed = t.bytes_consumed(),
                    quota = t.get_bytes_limit(),
                    "memory exceeds quota, spill to disk now."
                );
                self.triggered.store(true, SeqCst);
            }
            return;
        }

        // A spill is under way: wait for it rather than falling through to the
        // fallback, because the memory is about to be released.
        let mut status = self.cond.status.lock().unwrap();
        while *status == SpillStatus::Spilling {
            status = self.cond.cond.wait(status).unwrap();
        }
        drop(status);

        if !t.check_exceed() {
            return;
        }
        if let Some(fallback) = self.get_fallback() {
            fallback.action(t);
        }
    }

    fn set_fallback(&self, a: Option<ArcAction>) {
        self.base.set_fallback(a);
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }

    fn get_priority(&self) -> i64 {
        DEF_SPILL_PRIORITY
    }

    fn set_finished(&self) {
        self.base.set_finished();
    }

    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

/// Go `rowContainerRecord`: the in-memory half, the on-disk half, and the
/// error a failed spill leaves behind.
struct RowContainerRecord {
    in_memory: List,
    in_disk: Option<DataInDiskByRows>,
    spill_error: Option<String>,
}

/// Go `RowContainer`.
pub struct RowContainer {
    records: RowContainerRecord,
    mem_tracker: Arc<Tracker>,
    disk_tracker: Arc<disk::Tracker>,
    action_spill: Option<Arc<SpillDiskAction>>,
}

impl RowContainer {
    /// Go `NewRowContainer`.
    #[must_use]
    pub fn new(field_types: &[FieldType], chunk_size: usize) -> Self {
        let list = List::new(field_types, chunk_size, chunk_size);
        let mem_tracker = Tracker::new(LABEL_FOR_ROW_CONTAINER, -1);
        list.mem_tracker().attach_to(&mem_tracker);
        RowContainer {
            records: RowContainerRecord {
                in_memory: list,
                in_disk: None,
                spill_error: None,
            },
            mem_tracker,
            disk_tracker: disk::new_tracker(LABEL_FOR_ROW_CONTAINER, -1),
            action_spill: None,
        }
    }

    /// Go `alreadySpilled` (and `AlreadySpilledSafeForTest`, which differs
    /// only by taking the read lock this port does not have).
    #[must_use]
    pub fn already_spilled(&self) -> bool {
        self.records.in_disk.is_some()
    }

    /// Go `GetMemTracker`.
    #[must_use]
    pub fn mem_tracker(&self) -> &Arc<Tracker> {
        &self.mem_tracker
    }

    /// Go `GetDiskTracker`.
    #[must_use]
    pub fn disk_tracker(&self) -> &Arc<disk::Tracker> {
        &self.disk_tracker
    }

    /// The error a failed spill recorded, if any (Go `records.spillError`).
    #[must_use]
    pub fn spill_error(&self) -> Option<&str> {
        self.records.spill_error.as_deref()
    }

    /// Go `ActionSpill`: the action, created on first use.
    pub fn action_spill(&mut self) -> Arc<SpillDiskAction> {
        Arc::clone(
            self.action_spill
                .get_or_insert_with(|| Arc::new(SpillDiskAction::new())),
        )
    }

    /// Go `NumRow`.
    #[must_use]
    pub fn num_row(&self) -> usize {
        match &self.records.in_disk {
            Some(in_disk) => in_disk.len(),
            None => self.records.in_memory.len(),
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
        match &self.records.in_disk {
            Some(in_disk) => in_disk.num_chunks(),
            None => self.records.in_memory.num_chunks(),
        }
    }

    /// Go `NumRowsOfChunk`.
    #[must_use]
    pub fn num_rows_of_chunk(&self, chk_id: usize) -> usize {
        match &self.records.in_disk {
            Some(in_disk) => in_disk.num_rows_of_chunk(chk_id),
            None => self.records.in_memory.num_rows_of_chunk(chk_id),
        }
    }

    /// Go `AllocChunk`.
    pub fn alloc_chunk(&mut self) -> Chunk {
        self.records.in_memory.alloc_chunk()
    }

    /// Go `Add`: appends a chunk, to memory or to the spill file.
    ///
    /// The spill the memory-quota action asked for happens HERE, on the way
    /// out; see the module doc.
    pub fn add(&mut self, chk: Chunk) -> Result<(), DiskError> {
        let result = if self.already_spilled() {
            if let Some(error) = &self.records.spill_error {
                return Err(DiskError::Owned(error.clone()));
            }
            self.records.in_disk.as_mut().expect("spilled").add(&chk)
        } else {
            self.records.in_memory.add(chk);
            Ok(())
        };
        if let Some(action) = &self.action_spill {
            if action.take_trigger() {
                self.spill_to_disk();
            }
        }
        result
    }

    /// Go `SpillToDisk`/`spillToDisk(nil)`: move every in-memory chunk into a
    /// fresh [`DataInDiskByRows`] and release the memory.
    pub fn spill_to_disk(&mut self) {
        if self.already_spilled() {
            return;
        }
        if let Some(action) = &self.action_spill {
            if action.status() == SpillStatus::SpilledYet {
                // The container has been closed.
                return;
            }
            action.set_status(SpillStatus::Spilling);
        }

        let mut in_disk = DataInDiskByRows::new(self.records.in_memory.field_types().to_vec());
        in_disk.disk_tracker().attach_to(&self.disk_tracker);

        // Go wraps the copy in `defer recover()`: a panic on the spill path --
        // an out-of-quota temporary directory, or the KILL that
        // `HandleKillSignal` raises after every chunk -- becomes the container's
        // `spillError` and is reported to the next reader, not unwound into the
        // caller. `catch_unwind` is that `recover`.
        let records = &mut self.records;
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            for i in 0..records.in_memory.num_chunks() {
                let chk = records.in_memory.get_chunk(i);
                if let Err(error) = in_disk.add(chk) {
                    records.spill_error = Some(error.to_string());
                    return;
                }
                records.in_memory.mem_tracker().handle_kill_signal();
            }
        }));
        if let Err(payload) = panicked {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                .unwrap_or_else(|| "spill to disk failed".to_owned());
            tracing::warn!(error = %message, "spill to disk failed");
            self.records.spill_error = Some(message);
        }
        self.records.in_disk = Some(in_disk);
        if self.records.spill_error.is_none() {
            self.records.in_memory.clear();
        }

        if let Some(action) = &self.action_spill {
            action.set_status(SpillStatus::SpilledYet);
            action.broadcast();
        }
    }

    /// Go `GetChunk`.
    ///
    /// In memory Go hands back the live chunk; from disk it builds a new one.
    /// The [`Cow`] is that same distinction, made explicit.
    pub fn get_chunk(&self, chk_idx: usize) -> Result<Cow<'_, Chunk>, DiskError> {
        match &self.records.in_disk {
            None => Ok(Cow::Borrowed(self.records.in_memory.get_chunk(chk_idx))),
            Some(in_disk) => {
                if let Some(error) = &self.records.spill_error {
                    return Err(DiskError::Owned(error.clone()));
                }
                Ok(Cow::Owned(in_disk.get_chunk(chk_idx)?))
            }
        }
    }

    /// Go `GetRowAndAlwaysAppendToChunk`: append the row `ptr` points at to
    /// `chk`, whether the container has spilled or not, and return its index
    /// in `chk`.
    pub fn get_row_and_always_append_to_chunk(
        &self,
        ptr: RowPtr,
        chk: &mut Chunk,
    ) -> Result<usize, DiskError> {
        match &self.records.in_disk {
            Some(in_disk) => {
                if let Some(error) = &self.records.spill_error {
                    return Err(DiskError::Owned(error.clone()));
                }
                in_disk.get_row_and_append_to_chunk(ptr, chk)
            }
            None => {
                chk.append_row(self.records.in_memory.get_row(ptr));
                Ok(chk.num_rows() - 1)
            }
        }
    }

    /// The in-memory row `ptr` points at, or `None` once spilled.
    ///
    /// Go's `GetRowAndAppendToChunkIfInDisk` returns the live in-memory row
    /// and a nil chunk in this case; a Rust `Row` borrows the container, so
    /// the borrow IS the return value and the disk case has to go through
    /// [`RowContainer::get_row_and_always_append_to_chunk`].
    #[must_use]
    pub fn in_memory_row(&self, ptr: RowPtr) -> Option<Row<'_>> {
        if self.already_spilled() {
            return None;
        }
        Some(self.records.in_memory.get_row(ptr))
    }

    /// Go `Reset`.
    pub fn reset(&mut self) {
        if self.already_spilled() {
            if let Some(mut in_disk) = self.records.in_disk.take() {
                in_disk.close();
            }
            if let Some(action) = &self.action_spill {
                action.reset();
            }
        } else {
            self.records.in_memory.reset();
        }
        self.records.spill_error = None;
    }

    /// Go `Close`.
    ///
    /// Go nils out `records.inMemory` so a later use panics; the list is
    /// cleared here instead, so a later read sees an empty container rather
    /// than a crash.
    pub fn close(&mut self) {
        if let Some(action) = &self.action_spill {
            // Set status to spilledYet to avoid spilling.
            action.set_status(SpillStatus::SpilledYet);
            action.broadcast();
            action.set_finished();
        }
        self.mem_tracker.detach();
        self.disk_tracker.detach();
        if let Some(mut in_disk) = self.records.in_disk.take() {
            in_disk.close();
        }
        self.records.in_memory.clear();
    }
}

impl Drop for RowContainer {
    fn drop(&mut self) {
        self.close();
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
    /// The chunk a spilled row is decoded into; empty while in memory.
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
        if !self.c.already_spilled() {
            return;
        }
        let ptr = RowPtr::new(self.chk_idx as u32, self.row_idx as u32);
        let mut chk = Chunk::new_with_capacity(self.c.records.in_memory.field_types(), 1);
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
        match &self.scratch {
            Some(chk) => Some(chk.get_row(chk.num_rows() - 1)),
            None => self
                .c
                .in_memory_row(RowPtr::new(self.chk_idx as u32, self.row_idx as u32)),
        }
    }

    /// Go `End`: the invalid end position.
    #[must_use]
    pub fn end(&self) -> Option<Row<'_>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode as C;

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

        let res = rc.get_chunk(0).expect("get_chunk");
        assert_eq!(res.num_rows(), chk.num_rows());
        for row_idx in 0..res.num_rows() {
            assert_eq!(
                res.get_row(row_idx).get_int64(0),
                chk.get_row(row_idx).get_int64(0)
            );
        }

        // Written again, this time straight to the spill file.
        rc.add(chk.clone()).expect("add");
        assert!(rc.already_spilled());
        let res = rc.get_chunk(2).expect("get_chunk");
        assert_eq!(res.num_rows(), chk.num_rows());
        for row_idx in 0..res.num_rows() {
            assert_eq!(
                res.get_row(row_idx).get_int64(0),
                chk.get_row(row_idx).get_int64(0)
            );
        }

        rc.reset();
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
        let mut rc = RowContainer::new(&int64_fields(), 4);
        let tracker = Arc::clone(rc.mem_tracker());
        let action = rc.action_spill();
        action.set_status(SpillStatus::Spilling);

        let waker = Arc::clone(&action);
        let start = std::time::Instant::now();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            waker.set_status(SpillStatus::SpilledYet);
            waker.broadcast();
        });
        action.action(&tracker);
        assert!(start.elapsed() >= std::time::Duration::from_millis(200));
        handle.join().expect("join");
    }

    /// Go `TestSpillActionDeadLock`: an action firing CONCURRENTLY with `Add`
    /// must not deadlock. Go needs a goroutine to avoid taking the write lock
    /// under the caller's read lock; here the action touches no container
    /// state at all, so the deadlock cannot be constructed -- this test proves
    /// both threads finish and the container still spills.
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
        let _ = std::fs::remove_dir_all(&dir);
    }
}
