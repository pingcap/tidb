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

//! `pkg/executor/sortexec/sort_partition.go` + the `sortPartitionSpillDiskAction`
//! half of `sort_spill.go`: one run of the external sort.
//!
//! A partition collects rows in memory until the statement's memory quota is
//! exceeded. At that point it SORTS what it holds, writes the sorted rows out
//! to a [`DataInDiskByChunks`] spill file, releases the memory, and the sort
//! starts a fresh partition. Each partition is therefore a sorted run, and the
//! final output is a multi-way merge of the runs (`sort.go`
//! `externalSorting`).
//!
//! FAITHFUL ADAPTATION (concurrency shape, not behavior): Go's
//! `sortPartitionSpillDiskAction.Action` spawns a goroutine that performs the
//! spill while other goroutines wait on a condition variable. This tier's sort
//! is serial, so the action only RAISES A FLAG
//! ([`SpillDiskAction::need_spill`]) and the fetch loop performs the spill
//! itself at the next safe point -- which is the same point Go's `add`
//! observes `isSpillTriggered()` and rolls to a new partition. There is no
//! window in which rows are added to a partition that is being spilled,
//! because there is no second thread.
//!
//! DEFERRED (named): the parallel sort's `parallelSortSpillHelper` and
//! `parallelSortSpillAction`, and the `topn` spill. Go runs the parallel path
//! by default (`IsUnparallel = false`); this port has only the unparallel
//! path, so it is the unparallel spill that is ported here.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DataInDiskByChunks;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::Columns;
use tidb_util::disk;
use tidb_util::memory::{
    ActionOnExceed, ArcAction, BaseOomAction, Tracker, DEF_SPILL_PRIORITY, LABEL_FOR_ROW_CONTAINER,
};

use crate::executor::ExecError;
use crate::sort::{eval_sort_key, less_by_items, SortByItem};

/// Go `spillChunkSize`: rows per chunk written to the spill file.
pub const SPILL_CHUNK_SIZE: usize = 1024;

/// Go `sortPartitionSpillDiskAction`: the `ActionOnExceed` a spilling sort
/// registers on the session tracker.
///
/// Go's `executeAction` spills only when the partition holds enough data
/// (`hasEnoughDataToSpill`: more than `spillLimit`, itself a tenth of the
/// query quota) -- otherwise a query whose overrun comes from somewhere else
/// would open a spill file per chunk. When the partition is too small the
/// action falls through to whatever action it replaced, which for a `CANCEL`
/// session is the 8175 cancellation.
pub struct SpillDiskAction {
    base: BaseOomAction,
    /// Raised for the fetch loop to observe; see the module doc.
    need_spill: Arc<AtomicBool>,
    /// The current partition's memory tracker, which `hasEnoughDataToSpill`
    /// reads.
    partition_tracker: Arc<Tracker>,
    /// Go `sortPartition.spillLimit`.
    spill_limit: i64,
}

impl SpillDiskAction {
    fn has_enough_data_to_spill(&self) -> bool {
        self.partition_tracker.bytes_consumed() > self.spill_limit
    }
}

impl ActionOnExceed for SpillDiskAction {
    fn action(&self, t: &Arc<Tracker>) {
        // Go's action first does `for getIsSpillingNoLock() { cond.Wait() }`:
        // once a spill is under way, no consumer may fall through to the
        // cancellation, because the memory is about to be released. This tier
        // cannot block (the spill runs on this same thread, a few rows later),
        // so it returns instead of waiting -- the same outcome, no cancel.
        if self.need_spill.load(SeqCst) {
            return;
        }
        if self.has_enough_data_to_spill() {
            tracing::info!(
                consumed = t.bytes_consumed(),
                quota = t.get_bytes_limit(),
                "memory exceeds quota, spill to disk now."
            );
            self.need_spill.store(true, SeqCst);
            return;
        }
        // Go: `if !t.CheckExceed() { return nil }` then `return s.GetFallback()`
        // -- the caller runs the fallback. Running it here is the same chain.
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

/// Go `sortPartition`: one sorted run, in memory or spilled.
pub struct SortPartition {
    field_types: Vec<FieldType>,
    /// The materialized rows, while in memory.
    chunks: Vec<Chunk>,
    /// `(chunk index, row index)` per row, permuted into sorted order by
    /// [`SortPartition::sort`].
    rows: Vec<(usize, usize)>,
    /// The sort key of `rows[i]`, permuted alongside it.
    keys: Vec<Vec<Datum>>,
    sorted: bool,
    /// Go `inDisk`: `None` until the spill fires.
    in_disk: Option<DataInDiskByChunks>,
    /// Go `memTracker`, attached to the sort's operator tracker.
    mem_tracker: Arc<Tracker>,
    /// Go `diskTracker`.
    disk_tracker: Arc<disk::Tracker>,
    spill_chunk_size: usize,

    // --- read cursor (Go `sliceIter` in memory, `dataCursor` on disk) ---
    cursor: usize,
    disk_chunk_idx: usize,
    disk_chunk: Option<Chunk>,
    disk_row: usize,
    /// The key of the row the cursor currently sits on, for the multi-way
    /// merge to compare without materializing the row.
    head_key: Option<Vec<Datum>>,
}

impl SortPartition {
    /// Go `newSortPartition`.
    pub fn new(field_types: Vec<FieldType>, parent: &Arc<Tracker>) -> Self {
        let mem_tracker = Tracker::new(LABEL_FOR_ROW_CONTAINER, -1);
        mem_tracker.attach_to(parent);
        SortPartition {
            field_types,
            chunks: Vec::new(),
            rows: Vec::new(),
            keys: Vec::new(),
            sorted: false,
            in_disk: None,
            mem_tracker,
            disk_tracker: disk::new_tracker(LABEL_FOR_ROW_CONTAINER, -1),
            spill_chunk_size: SPILL_CHUNK_SIZE,
            cursor: 0,
            disk_chunk_idx: 0,
            disk_chunk: None,
            disk_row: 0,
            head_key: None,
        }
    }

    /// Test hook for Go `SetSmallSpillChunkSizeForTest`.
    pub fn set_spill_chunk_size(&mut self, size: usize) {
        self.spill_chunk_size = size;
    }

    /// Go `sortPartition.getMemTracker`.
    pub fn mem_tracker(&self) -> &Arc<Tracker> {
        &self.mem_tracker
    }

    /// Go `sortPartition.getDiskTracker`.
    pub fn disk_tracker(&self) -> &Arc<disk::Tracker> {
        &self.disk_tracker
    }

    /// Whether this partition's rows live on disk (Go `isSpillTriggered`).
    pub fn is_spilled(&self) -> bool {
        self.in_disk.is_some()
    }

    /// Rows held, in memory or on disk.
    pub fn num_rows(&self) -> usize {
        match &self.in_disk {
            Some(in_disk) => in_disk.num_rows() as usize,
            None => self.rows.len(),
        }
    }

    /// Go `sortPartition.add`: materialize `chk`'s rows and account for them.
    ///
    /// Go stores `chunk.Row` handles into the caller's chunk and charges
    /// `chunk.RowSize*rowNum + chk.MemoryUsage()`; this port owns the chunk,
    /// which is the same memory, charged the same way.
    pub fn add<C: Columns>(
        &mut self,
        chunk: Chunk,
        by_items: &[SortByItem],
        ctx: &C,
    ) -> Result<(), ExecError> {
        let rows = i64::try_from(chunk.num_rows()).unwrap_or(i64::MAX);
        self.mem_tracker
            .consume(chunk.memory_usage() + tidb_chunk::row::ROW_SIZE * rows);

        let chunk_index = self.chunks.len();
        for row_index in 0..chunk.num_rows() {
            let key = eval_sort_key(by_items, ctx, chunk.get_row(row_index))?;
            // OVER-COUNT vs Go, deliberately (and unchanged from this port's
            // pre-spill behavior): Go re-reads the chunk cell on every
            // comparison and keeps no materialized key, so `keys` is memory
            // THIS port holds and Go does not. Counting it is what makes the
            // tracker describe the process rather than the source.
            let mut key_bytes = i64::try_from(size_of::<Vec<Datum>>()).unwrap_or(i64::MAX);
            for datum in &key {
                key_bytes += i64::try_from(datum.estimated_mem_usage()).unwrap_or(i64::MAX);
            }
            self.mem_tracker.consume(key_bytes);
            self.keys.push(key);
            self.rows.push((chunk_index, row_index));
        }
        self.chunks.push(chunk);
        self.sorted = false;
        Ok(())
    }

    /// Go `sortPartition.sortNoLock`: order the rows this partition holds.
    ///
    /// DIVERGENCE (documented, unchanged from the in-memory port): Go's
    /// `sort.Slice` is unstable; this is Rust's stable sort, so only the order
    /// of exactly-tying rows can differ -- an order Go does not guarantee.
    pub fn sort(&mut self, by_items: &[SortByItem]) -> Result<(), ExecError> {
        if self.sorted {
            return Ok(());
        }
        let keys = &self.keys;
        let mut sort_err: Option<ExecError> = None;
        let mut indices: Vec<usize> = (0..self.rows.len()).collect();
        indices.sort_by(|&a, &b| match less_by_items(by_items, &keys[a], &keys[b]) {
            Ok(ordering) => ordering,
            Err(error) => {
                if sort_err.is_none() {
                    sort_err = Some(error);
                }
                Ordering::Equal
            }
        });
        if let Some(error) = sort_err {
            return Err(error);
        }
        self.rows = indices.iter().map(|&i| self.rows[i]).collect();
        self.keys = {
            let mut keys = std::mem::take(&mut self.keys);
            let mut permuted: Vec<Option<Vec<Datum>>> = keys.drain(..).map(Some).collect();
            indices
                .iter()
                .map(|&i| permuted[i].take().expect("each key moved once"))
                .collect()
        };
        self.sorted = true;
        Ok(())
    }

    /// Go `sortPartition.spillToDisk` + `spillToDiskImpl`: sort, write every
    /// row out in sorted order, then release the in-memory rows.
    pub fn spill_to_disk(&mut self, by_items: &[SortByItem]) -> Result<(), ExecError> {
        self.sort(by_items)?;
        if self.rows.is_empty() {
            // Go `errSpillEmptyChunk`. Reached only if the action fires on a
            // partition that has taken no rows, which the `spillLimit` guard
            // makes unreachable in practice.
            return Err(ExecError::SpillFailed(
                "can not spill empty chunk to disk".to_owned(),
            ));
        }

        let mut in_disk = DataInDiskByChunks::new(self.field_types.clone(), "");
        in_disk.disk_tracker().attach_to(&self.disk_tracker);
        let mut tmp = Chunk::new_with_capacity(&self.field_types, self.spill_chunk_size);
        for &(chunk_index, row_index) in &self.rows {
            tmp.append_row(self.chunks[chunk_index].get_row(row_index));
            if tmp.num_rows() >= self.spill_chunk_size {
                in_disk.add(&tmp).map_err(spill_error)?;
                tmp.reset();
            }
        }
        // Go: do not spill an empty tail chunk -- `Add` rejects it.
        if tmp.num_rows() > 0 {
            in_disk.add(&tmp).map_err(spill_error)?;
        }
        self.in_disk = Some(in_disk);

        // Release memory as all data have been spilled to disk.
        self.chunks = Vec::new();
        self.rows = Vec::new();
        self.keys = Vec::new();
        self.mem_tracker.replace_bytes_used(0);
        Ok(())
    }

    /// Positions the cursor on the next unread row and computes its key.
    /// Returns `false` once the partition is exhausted (Go's empty `chunk.Row`).
    pub fn load_head<C: Columns>(
        &mut self,
        by_items: &[SortByItem],
        ctx: &C,
    ) -> Result<bool, ExecError> {
        if self.head_key.is_some() {
            return Ok(true);
        }
        if self.in_disk.is_some() {
            if !self.position_disk_cursor()? {
                return Ok(false);
            }
            let chunk = self.disk_chunk.as_ref().expect("positioned chunk");
            self.head_key = Some(eval_sort_key(by_items, ctx, chunk.get_row(self.disk_row))?);
            return Ok(true);
        }
        if self.cursor >= self.rows.len() {
            return Ok(false);
        }
        self.head_key = Some(self.keys[self.cursor].clone());
        Ok(true)
    }

    /// The key of the row the cursor sits on; `load_head` must have run.
    pub fn head_key(&self) -> Option<&[Datum]> {
        self.head_key.as_deref()
    }

    /// Appends the cursor's row to `req` and advances past it.
    pub fn take_head_into(&mut self, req: &mut Chunk) {
        if self.in_disk.is_some() {
            let chunk = self.disk_chunk.as_ref().expect("positioned chunk");
            req.append_row(chunk.get_row(self.disk_row));
            self.disk_row += 1;
        } else {
            let (chunk_index, row_index) = self.rows[self.cursor];
            req.append_row(self.chunks[chunk_index].get_row(row_index));
            self.cursor += 1;
        }
        self.head_key = None;
    }

    /// Go `reloadCursor`: makes sure `disk_chunk`/`disk_row` name a real row.
    fn position_disk_cursor(&mut self) -> Result<bool, ExecError> {
        loop {
            if let Some(chunk) = &self.disk_chunk {
                if self.disk_row < chunk.num_rows() {
                    return Ok(true);
                }
            }
            let in_disk = self.in_disk.as_mut().expect("spilled partition");
            if self.disk_chunk_idx >= in_disk.num_chunks() {
                self.disk_chunk = None;
                return Ok(false);
            }
            let chunk = in_disk
                .get_chunk(self.disk_chunk_idx)
                .map_err(spill_error)?;
            self.disk_chunk_idx += 1;
            self.disk_row = 0;
            self.disk_chunk = Some(chunk);
        }
    }

    /// Go `sortPartition.close`: drops the rows and the spill file and returns
    /// the bytes to the statement's budget.
    pub fn close(&mut self) {
        if let Some(in_disk) = &mut self.in_disk {
            in_disk.close();
        }
        self.in_disk = None;
        self.chunks = Vec::new();
        self.rows = Vec::new();
        self.keys = Vec::new();
        self.disk_chunk = None;
        self.mem_tracker.replace_bytes_used(0);
    }
}

/// Builds the action a sort registers for `partition`, and the flag the fetch
/// loop polls.
pub fn spill_action(
    partition: &SortPartition,
    spill_limit: i64,
) -> (Arc<SpillDiskAction>, Arc<AtomicBool>) {
    let need_spill = Arc::new(AtomicBool::new(false));
    let action = Arc::new(SpillDiskAction {
        base: BaseOomAction::default(),
        need_spill: Arc::clone(&need_spill),
        partition_tracker: Arc::clone(partition.mem_tracker()),
        spill_limit,
    });
    (action, need_spill)
}

fn spill_error(error: tidb_chunk::chunk_in_disk::DiskError) -> ExecError {
    ExecError::SpillFailed(error.to_string())
}
