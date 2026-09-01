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
//! For the explicit unparallel test path, Go's
//! `sortPartitionSpillDiskAction.Action` spawns a goroutine that performs the
//! spill while other goroutines wait on a condition variable. The Rust
//! unparallel path raises a flag
//! ([`SpillDiskAction::need_spill`]) and the fetch loop performs the spill
//! itself at the next safe point -- which is the same point Go's `add`
//! observes `isSpillTriggered()` and rolls to a new partition. There is no
//! window in which rows are added to a partition that is being spilled,
//! because that path has no second thread. The default parallel path uses
//! [`crate::parallel_sort_spill_helper`] and its coordinated spill action in
//! [`crate::sort`]; TopN spill lives in [`crate::topn_spill`].

use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DataInDiskByChunks;
use tidb_chunk::row::OwnedRow;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::Columns;
use tidb_util::disk;
use tidb_util::memory::{
    ActionOnExceed, ArcAction, BaseOomAction, Tracker, DEF_SPILL_PRIORITY, LABEL_FOR_ROW_CONTAINER,
};
use tidb_util::spill_storage::SpillStorage;

use crate::executor::ExecError;
use crate::sort::{compare_rows, eval_sort_key, SortByItem};
use tidb_chunk::compare::ColumnCompareFunc;
use tidb_chunk::ColumnRead;

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
    sorted: bool,
    /// Go `inDisk`: `None` until the spill fires.
    in_disk: Option<DataInDiskByChunks>,
    /// Go `memTracker`, attached to the sort's operator tracker.
    mem_tracker: Arc<Tracker>,
    /// Go `diskTracker`.
    disk_tracker: Arc<disk::Tracker>,
    /// The statement's immutable physical spill authority.
    spill_storage: Arc<SpillStorage>,
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
    pub fn new(
        field_types: Vec<FieldType>,
        parent: &Arc<Tracker>,
        spill_storage: Arc<SpillStorage>,
    ) -> Self {
        let mem_tracker = Tracker::new(LABEL_FOR_ROW_CONTAINER, -1);
        mem_tracker.attach_to(parent);
        SortPartition {
            field_types,
            chunks: Vec::new(),
            rows: Vec::new(),
            sorted: false,
            in_disk: None,
            mem_tracker,
            disk_tracker: disk::new_tracker(LABEL_FOR_ROW_CONTAINER, -1),
            spill_storage,
            spill_chunk_size: SPILL_CHUNK_SIZE,
            cursor: 0,
            disk_chunk_idx: 0,
            disk_chunk: None,
            disk_row: 0,
            head_key: None,
        }
    }

    /// Wraps one sorted run produced by Go's parallel spill helper. The disk
    /// object already accounts to the sort's disk tracker; this wrapper owns
    /// its read cursor and closes the file with the rest of the sort runs.
    pub(crate) fn from_spilled(
        field_types: Vec<FieldType>,
        parent: &Arc<Tracker>,
        spill_storage: Arc<SpillStorage>,
        in_disk: DataInDiskByChunks,
    ) -> Self {
        let mut partition = Self::new(field_types, parent, spill_storage);
        partition.in_disk = Some(in_disk);
        partition.sorted = true;
        partition
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

    /// Number of source chunks retained by this in-memory run.
    #[cfg(test)]
    pub(crate) fn in_memory_chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Go `sortPartition.add`: materialize `chk`'s rows and account for them.
    ///
    /// Go stores `chunk.Row` handles into the caller's chunk and charges
    /// `chunk.RowSize*rowNum + chk.MemoryUsage()`; this port owns the chunk,
    /// which is the same memory, charged the same way.
    pub fn add(&mut self, chunk: Chunk) {
        let rows = i64::try_from(chunk.num_rows()).unwrap_or(i64::MAX);
        self.mem_tracker
            .consume(chunk.memory_usage() + tidb_chunk::row::ROW_SIZE * rows);

        let chunk_index = self.chunks.len();
        for row_index in 0..chunk.num_rows() {
            self.rows.push((chunk_index, row_index));
        }
        self.chunks.push(chunk);
        self.sorted = false;
    }

    /// Go `sortPartition.sortNoLock`: order the rows this partition holds.
    ///
    pub fn sort<C: Columns>(
        &mut self,
        by_items: &[SortByItem],
        compare_funcs: &[Option<ColumnCompareFunc>],
        ctx: &C,
    ) -> Result<(), ExecError> {
        if self.sorted {
            return Ok(());
        }
        let chunks = &self.chunks;
        // Go's row handles point straight at their chunk columns. Retain the
        // equivalent Rust read views for the whole sort so the comparator
        // neither reopens a ColumnSlot nor copies a shared variable-width
        // cell on every comparison.
        let column_views: Vec<Option<Vec<ColumnRead<'_>>>> = by_items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                compare_funcs[index].as_ref()?;
                let column = usize::try_from(item.expr.as_column()?.index).ok()?;
                chunks
                    .iter()
                    .all(|chunk| column < chunk.num_cols())
                    .then(|| chunks.iter().map(|chunk| chunk.column(column)).collect())
            })
            .collect();
        let mut sort_err: Option<ExecError> = None;
        self.rows
            .sort_unstable_by(|&(left_chunk, left_row), &(right_chunk, right_row)| {
                let result = (|| {
                    for (index, item) in by_items.iter().enumerate() {
                        let ordering = match (&compare_funcs[index], &column_views[index]) {
                            (Some(compare), Some(columns)) => {
                                let mut ordering = compare(
                                    &columns[left_chunk],
                                    left_row,
                                    &columns[right_chunk],
                                    right_row,
                                );
                                if item.desc {
                                    ordering = ordering.reverse();
                                }
                                ordering
                            }
                            _ => compare_rows(
                                std::slice::from_ref(item),
                                std::slice::from_ref(&compare_funcs[index]),
                                ctx,
                                chunks[left_chunk].get_row(left_row),
                                chunks[right_chunk].get_row(right_row),
                            )?,
                        };
                        if ordering != Ordering::Equal {
                            return Ok(ordering);
                        }
                    }
                    Ok(Ordering::Equal)
                })();
                match result {
                    Ok(ordering) => ordering,
                    Err(error) => {
                        if sort_err.is_none() {
                            sort_err = Some(error);
                        }
                        Ordering::Equal
                    }
                }
            });
        if let Some(error) = sort_err {
            return Err(error);
        }
        self.sorted = true;
        Ok(())
    }

    /// Copies this in-memory run into the owned-row representation used by
    /// the parallel worker's local K-way merge, then releases the worker's
    /// retained chunks. Go keeps borrowed `chunk.Row` handles here; Rust must
    /// own them because the worker buffers are cleared before the spill or
    /// result merge outlives the worker lock.
    pub(crate) fn take_sorted_owned_rows(&mut self) -> Vec<OwnedRow> {
        debug_assert!(self.sorted);
        debug_assert!(self.in_disk.is_none());
        let rows = self
            .rows
            .iter()
            .map(|&(chunk_index, row_index)| {
                self.chunks[chunk_index].get_row(row_index).copy_construct()
            })
            .collect();
        self.chunks.clear();
        self.rows.clear();
        self.mem_tracker.replace_bytes_used(0);
        rows
    }

    /// Go `parallelSortWorker.multiWayMergeLocalSortedRows`, retaining the
    /// fetched chunks and merging only their lightweight row cursors. Go's
    /// returned `[]chunk.Row` keeps the worker chunks alive; the Rust run owns
    /// those chunks and stores the equivalent `(chunk, row)` cursor pairs.
    pub(crate) fn merge_sorted_in_memory<C: Columns>(
        partitions: Vec<Self>,
        by_items: &[SortByItem],
        compare_funcs: &[Option<ColumnCompareFunc>],
        ctx: &C,
    ) -> Result<Option<Self>, ExecError> {
        if partitions.is_empty() {
            return Ok(None);
        }
        if partitions.len() == 1 {
            return Ok(partitions.into_iter().next());
        }
        debug_assert!(
            partitions
                .iter()
                .all(|partition| partition.sorted && partition.in_disk.is_none()),
            "parallel worker batches are sorted in memory before their local merge"
        );

        let mut heads = partitions
            .iter()
            .enumerate()
            .filter(|(_, partition)| !partition.rows.is_empty())
            .map(|(partition_id, _)| crate::sort_util::RowWithPartition {
                row: 0usize,
                partition_id,
            })
            .collect::<Vec<_>>();
        let compare_head = |left: &crate::sort_util::RowWithPartition<usize>,
                            right: &crate::sort_util::RowWithPartition<usize>,
                            error: &mut Option<ExecError>| {
            let left_partition = &partitions[left.partition_id];
            let (left_chunk, left_row) = left_partition.rows[left.row];
            let right_partition = &partitions[right.partition_id];
            let (right_chunk, right_row) = right_partition.rows[right.row];
            match compare_rows(
                by_items,
                compare_funcs,
                ctx,
                left_partition.chunks[left_chunk].get_row(left_row),
                right_partition.chunks[right_chunk].get_row(right_row),
            ) {
                Ok(ordering) => ordering == Ordering::Less,
                Err(compare_error) => {
                    if error.is_none() {
                        *error = Some(compare_error);
                    }
                    false
                }
            }
        };

        let mut compare_error = None;
        crate::topn_chunk_heap::go_heap::init(&mut heads, &mut |left, right| {
            compare_head(left, right, &mut compare_error)
        });
        if let Some(error) = compare_error {
            return Err(error);
        }

        let mut order = Vec::with_capacity(
            partitions
                .iter()
                .map(|partition| partition.rows.len())
                .sum(),
        );
        while !heads.is_empty() {
            let head = heads[0];
            order.push((head.partition_id, head.row));
            let next_row = head.row + 1;
            let mut compare_error = None;
            if next_row < partitions[head.partition_id].rows.len() {
                heads[0].row = next_row;
                crate::topn_chunk_heap::go_heap::fix(&mut heads, 0, &mut |left, right| {
                    compare_head(left, right, &mut compare_error)
                });
            } else {
                crate::topn_chunk_heap::go_heap::remove(&mut heads, 0, &mut |left, right| {
                    compare_head(left, right, &mut compare_error)
                });
            }
            if let Some(error) = compare_error {
                return Err(error);
            }
        }

        let mut chunk_bases = Vec::with_capacity(partitions.len());
        let mut chunk_count = 0usize;
        for partition in &partitions {
            chunk_bases.push(chunk_count);
            chunk_count += partition.chunks.len();
        }
        let merged_rows = order
            .into_iter()
            .map(|(partition_id, row_offset)| {
                let (chunk, row) = partitions[partition_id].rows[row_offset];
                (chunk_bases[partition_id] + chunk, row)
            })
            .collect();

        let mut partitions = partitions.into_iter();
        let mut merged = partitions.next().expect("non-empty partitions");
        let mut chunks = std::mem::take(&mut merged.chunks);
        for mut partition in partitions {
            chunks.append(&mut partition.chunks);
            let bytes = partition.mem_tracker.bytes_consumed();
            partition.mem_tracker.replace_bytes_used(0);
            partition.mem_tracker.detach();
            merged.mem_tracker.consume(bytes);
        }
        merged.chunks = chunks;
        merged.rows = merged_rows;
        merged.sorted = true;
        merged.cursor = 0;
        merged.head_key = None;
        Ok(Some(merged))
    }

    /// Transfers this retained in-memory run from the worker's detached
    /// accounting tree to the Sort executor tracker.
    pub(crate) fn attach_memory_to(&mut self, parent: &Arc<Tracker>) {
        self.mem_tracker.attach_to(parent);
    }

    /// Go `sortPartition.spillToDisk` + `spillToDiskImpl`: sort, write every
    /// row out in sorted order, then release the in-memory rows.
    pub fn spill_to_disk<C: Columns>(
        &mut self,
        by_items: &[SortByItem],
        compare_funcs: &[Option<ColumnCompareFunc>],
        ctx: &C,
    ) -> Result<(), ExecError> {
        self.sort(by_items, compare_funcs, ctx)?;
        if self.rows.is_empty() {
            // Go `errSpillEmptyChunk`. Reached only if the action fires on a
            // partition that has taken no rows, which the `spillLimit` guard
            // makes unreachable in practice.
            return Err(ExecError::SpillFailed(
                "can not spill empty chunk to disk".to_owned(),
            ));
        }

        let mut in_disk = DataInDiskByChunks::new(
            self.field_types.clone(),
            "",
            Arc::clone(&self.spill_storage),
        );
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
        let (chunk_index, row_index) = self.rows[self.cursor];
        self.head_key = Some(eval_sort_key(
            by_items,
            ctx,
            self.chunks[chunk_index].get_row(row_index),
        )?);
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

    /// Streams one already-sorted run directly into `req`.
    ///
    /// Go's one-partition path advances its slice/disk iterator directly; it
    /// does not build merge-head keys for a merge with no second run.
    pub fn append_sorted_rows_into(
        &mut self,
        req: &mut Chunk,
        limit: usize,
    ) -> Result<(), ExecError> {
        while req.num_rows() < limit {
            if self.in_disk.is_some() {
                if !self.position_disk_cursor()? {
                    break;
                }
                let chunk = self.disk_chunk.as_ref().expect("positioned chunk");
                req.append_row(chunk.get_row(self.disk_row));
                self.disk_row += 1;
            } else {
                let Some(&(chunk_index, row_index)) = self.rows.get(self.cursor) else {
                    break;
                };
                req.append_row(self.chunks[chunk_index].get_row(row_index));
                self.cursor += 1;
            }
        }
        Ok(())
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
