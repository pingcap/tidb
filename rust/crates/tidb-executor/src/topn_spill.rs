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

//! `pkg/executor/sortexec/topn_spill.go`: the `TopN` spill.
//!
//! # What Go's TopN spill actually does
//!
//! It does NOT abandon the heap for a sort-plus-limit. `spillHeap` takes the
//! heap AS IT STANDS, sorts its retained row pointers ASCENDING
//! (`slices.SortFunc(rowPtrs, keyColumnsCompare)`), writes those rows out to
//! one `DataInDiskByChunks` -- a SORTED RUN of at most `offset + count` rows --
//! and clears the heap. Processing then continues on a fresh heap, so each
//! spill produces one more run. At the end the runs are merged
//! (`generateResultWithMultiWayMerge`, `multi_way_merge.go`) and the merge
//! stops at `offset + count` rows, emitting from `offset`.
//!
//! The bound is what makes this cheap: a run is never larger than the heap,
//! and the heap is never larger than `offset + count`. A TopN that spills
//! writes `runs * (offset + count)` rows, not the whole input.
//!
//! # Single-threaded adaptation, and where it differs
//!
//! Go runs the post-spill phase on a pool of `topNWorker`s, each with its own
//! heap, all fed from one chunk channel; this tier has one thread, so it has
//! one heap and the "workers" phase is the same loop the pre-spill phase runs.
//! Two consequences, both named:
//!
//! * Go's worker heap accumulates whole chunks until it holds at least
//!   `offset + count` rows and then never pops down to that bound, so a Go run
//!   can hold up to a chunk more than this port's does. Both are SUPERSETS of
//!   the segment's true top-N and the merge cuts both at `offset + count`, so
//!   the answer is the same.
//! * Go also re-checks for a spill WHILE EMITTING results
//!   (`generateTopNResultsWhenNoSpillTriggered` polls every 10 rows, and
//!   `inMemoryThenSpillFlag` marks that case). That trigger exists because
//!   another goroutine can push the query over the quota while the TopN is
//!   emitting; nothing on this tier consumes memory during emission, so the
//!   flag is always false here and the branch it guards is unreachable.
//!
//! The shared `tidb-chunk` spill container now implements Go's optional
//! `aes128-ctr` file stack. The bounded server still has no top-level config
//! loader to call that container's process-wide config seam, so its startup
//! remains plaintext and rejects unsupported command-line options loudly.

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DataInDiskByChunks;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::Columns;
use tidb_util::disk::{self, SpillStorage};
use tidb_util::memory::{
    ActionOnExceed, ArcAction, BaseOomAction, Tracker, DEF_SPILL_PRIORITY, LABEL_FOR_ROW_CONTAINER,
};

use crate::executor::ExecError;
use crate::sort::{eval_sort_key, SortByItem};

/// Go `spillChunkSize` for the TopN's `tmpSpillChunk`: rows per chunk written
/// to a run.
pub const SPILL_CHUNK_SIZE: usize = 1024;

/// Go `topNSpillAction`.
///
/// Go registers this on the tracker's HARD-limit slot
/// (`MemTracker.FallbackOldAndSetNewAction`), unlike the aggregation's action
/// which sits on the soft-limit slot -- so a TopN spills at the quota itself,
/// and the cancellation it displaces becomes its fallback for the case where
/// the TopN holds too little to be worth a file.
pub struct TopNSpillAction {
    base: BaseOomAction,
    /// Raised for the fetch loop to observe, the same stand-in for Go's
    /// spill goroutine that [`crate::sort_partition::SpillDiskAction`] uses.
    need_spill: Arc<AtomicBool>,
    /// The TopN's own tracker, which `hasEnoughDataToSpill` reads.
    topn_tracker: Arc<Tracker>,
}

impl TopNSpillAction {
    /// Builds the action and the flag the TopN's loop polls.
    #[must_use]
    pub fn new(topn_tracker: &Arc<Tracker>) -> (Arc<TopNSpillAction>, Arc<AtomicBool>) {
        let need_spill = Arc::new(AtomicBool::new(false));
        let action = Arc::new(TopNSpillAction {
            base: BaseOomAction::default(),
            need_spill: Arc::clone(&need_spill),
            topn_tracker: Arc::clone(topn_tracker),
        });
        (action, need_spill)
    }

    /// Go `hasEnoughDataToSpill`, shared with the aggregation: a fifth of the
    /// quota, read off the operator's own tracker.
    fn has_enough_data_to_spill(&self, t: &Arc<Tracker>) -> bool {
        crate::agg_spill::has_enough_data_to_spill(&self.topn_tracker, t)
    }
}

impl ActionOnExceed for TopNSpillAction {
    /// Go `topNSpillAction.Action`.
    fn action(&self, t: &Arc<Tracker>) {
        // Go waits out an in-flight spill on a condition variable; this tier
        // performs the spill on this same thread a few rows later, so an
        // already-raised flag means the same thing -- do not fall through to
        // the cancellation, the memory is about to be released.
        if self.need_spill.load(SeqCst) {
            return;
        }
        let has_enough_data = self.has_enough_data_to_spill(t);
        if t.check_exceed() && has_enough_data {
            tracing::info!(
                consumed = t.bytes_consumed(),
                quota = t.get_bytes_limit(),
                "memory exceeds quota, spill to disk now."
            );
            self.need_spill.store(true, SeqCst);
            return;
        }
        if t.check_exceed() && !has_enough_data {
            if let Some(fallback) = self.get_fallback() {
                fallback.action(t);
            }
        }
    }

    fn set_fallback(&self, a: Option<ArcAction>) {
        self.base.set_fallback(a);
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }

    /// Go `topNSpillAction.GetPriority`.
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

/// One spilled run: the rows of one heap, in ASCENDING by-item order, on disk.
///
/// Go's `spillHelper.sortedRowsInDisk[i]` plus the cursor
/// `multi_way_merge.go`'s `diskSource` keeps over it.
pub struct SpilledRun {
    in_disk: DataInDiskByChunks,
    disk_tracker: Arc<disk::Tracker>,
    // --- read cursor ---
    chunk_index: usize,
    chunk: Option<Chunk>,
    row: usize,
    /// The key of the row the cursor sits on, so the merge can compare heads
    /// without re-reading the cell.
    head_key: Option<Vec<Datum>>,
}

impl SpilledRun {
    /// Go `spillHeap`: writes `rows` -- ALREADY in ascending order -- out as
    /// one run.
    ///
    /// `Add` rejects an empty chunk, so the tail chunk is only written when it
    /// holds something, exactly as [`crate::sort_partition::SortPartition`]
    /// does.
    pub fn write(
        field_types: &[FieldType],
        chunks: &[Chunk],
        row_ptrs: &[(usize, usize)],
        spill_chunk_size: usize,
        parent: &Arc<disk::Tracker>,
        spill_storage: Arc<SpillStorage>,
    ) -> Result<SpilledRun, ExecError> {
        let disk_tracker = disk::new_tracker(LABEL_FOR_ROW_CONTAINER, -1);
        disk_tracker.attach_to(parent);
        let mut in_disk = DataInDiskByChunks::new(field_types.to_vec(), "", spill_storage);
        in_disk.disk_tracker().attach_to(&disk_tracker);
        let mut tmp = Chunk::new_with_capacity(field_types, spill_chunk_size);
        for &(chunk_index, row_index) in row_ptrs {
            tmp.append_row(chunks[chunk_index].get_row(row_index));
            if tmp.num_rows() >= spill_chunk_size {
                in_disk.add(&tmp).map_err(spill_error)?;
                tmp.reset();
            }
        }
        if tmp.num_rows() > 0 {
            in_disk.add(&tmp).map_err(spill_error)?;
        }
        Ok(SpilledRun {
            in_disk,
            disk_tracker,
            chunk_index: 0,
            chunk: None,
            row: 0,
            head_key: None,
        })
    }

    /// Rows this run holds.
    #[must_use]
    pub fn num_rows(&self) -> i64 {
        self.in_disk.num_rows()
    }

    /// Positions the cursor on the next unread row and computes its key.
    /// Returns `false` once the run is exhausted.
    pub fn load_head<C: Columns>(
        &mut self,
        by_items: &[SortByItem],
        ctx: &C,
    ) -> Result<bool, ExecError> {
        if self.head_key.is_some() {
            return Ok(true);
        }
        if !self.position_cursor()? {
            return Ok(false);
        }
        let chunk = self.chunk.as_ref().expect("positioned chunk");
        self.head_key = Some(eval_sort_key(by_items, ctx, chunk.get_row(self.row))?);
        Ok(true)
    }

    /// The key of the row the cursor sits on; `load_head` must have run.
    #[must_use]
    pub fn head_key(&self) -> Option<&[Datum]> {
        self.head_key.as_deref()
    }

    /// Appends the cursor's row to `req` and advances past it.
    pub fn take_head_into(&mut self, req: &mut Chunk) {
        let chunk = self.chunk.as_ref().expect("positioned chunk");
        req.append_row(chunk.get_row(self.row));
        self.row += 1;
        self.head_key = None;
    }

    /// Skips the cursor's row without emitting it -- the merge's way of
    /// walking past the first `offset` rows.
    pub fn drop_head(&mut self) {
        self.row += 1;
        self.head_key = None;
    }

    /// Go `dataCursor.reloadCursor`: makes sure the cursor names a real row.
    fn position_cursor(&mut self) -> Result<bool, ExecError> {
        loop {
            if let Some(chunk) = &self.chunk {
                if self.row < chunk.num_rows() {
                    return Ok(true);
                }
            }
            if self.chunk_index >= self.in_disk.num_chunks() {
                self.chunk = None;
                return Ok(false);
            }
            let chunk = self
                .in_disk
                .get_chunk(self.chunk_index)
                .map_err(spill_error)?;
            self.chunk_index += 1;
            self.row = 0;
            self.chunk = Some(chunk);
        }
    }

    /// Bytes this run occupies on disk.
    #[must_use]
    pub fn bytes_in_disk(&self) -> i64 {
        self.disk_tracker.bytes_consumed()
    }

    /// Go `close`: removes the spill file.
    pub fn close(&mut self) {
        self.in_disk.close();
        self.chunk = None;
        self.head_key = None;
    }
}

fn spill_error(error: tidb_chunk::chunk_in_disk::DiskError) -> ExecError {
    ExecError::SpillFailed(error.to_string())
}
