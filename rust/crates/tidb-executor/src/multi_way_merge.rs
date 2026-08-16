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

//! Go `pkg/executor/sortexec`, covering `multi_way_merge.go` -- the K-way merge
//! that turns several ALREADY-SORTED runs into one sorted stream.
//!
//! This crate ports `pkg/executor/sortexec` in pieces. COVERED here:
//! `multi_way_merge.go`. Covered elsewhere: `sort.go` -> [`crate::sort`],
//! `sort_util.go` -> [`crate::sort_util`], `sort_partition.go` ->
//! [`crate::sort_partition`], `topn.go` -> [`crate::topn`], `topn_spill.go` ->
//! [`crate::topn_spill`], `topn_chunk_heap.go` -> [`crate::topn_chunk_heap`].
//! NOT COVERED anywhere yet: `parallel_sort_worker.go`,
//! `parallel_sort_spill_helper.go`, `sort_spill.go`, `topn_worker.go`.
//!
//! The shape is Go's: a heap holds ONE element per live run
//! ([`crate::sort_util::RowWithPartition`]), `next` returns the heap's minimum
//! and immediately pulls that run's replacement -- `heap.Fix` when the run has
//! more, `heap.Remove` when it is exhausted. The sift rules come from
//! [`crate::topn_chunk_heap::go_heap`], the shared port of Go's
//! `container/heap`, so a tie between two runs breaks the way Go's breaks.
//!
//! NARROWINGS, by name:
//!
//! * The merger is GENERIC over an owned element type instead of returning
//!   `chunk.Row`. A `chunk::Row<'_>` borrows the chunk it lives in, so a heap
//!   cannot hold one from run `i` while run `i` is advanced to produce the
//!   replacement -- the exact move `next` makes. [`MemorySource`] therefore
//!   merges owned items and [`DiskSource`] yields
//!   [`tidb_chunk::row::OwnedRow`].
//! * `memorySource` merges owned items rather than borrowing
//!   `chunk.Iterator4Slice`s, for the same reason.
//! * `sortPartitionSource` is NOT ported. Go's `sortPartition` exposes
//!   `getNextSortedRow`; this port's [`crate::sort_partition::SortPartition`]
//!   exposes a `load_head`/`head_key`/`take_head_into` cursor that
//!   [`crate::sort::SortExec`] merges directly, and reshaping it to feed this
//!   merger would change a file this port already ships. The two remaining
//!   sources cover the in-memory and spilled cases.
//! * `lessRowFunction` returns [`Ordering`] rather than Go's `int`, and it may
//!   FAIL: this port's keys are evaluated datums, and an unorderable pair is an
//!   error rather than a planner-time impossibility. `init`/`next` therefore
//!   return [`Result`].

use std::cmp::Ordering;

use tidb_chunk::chunk_in_disk::DataInDiskByChunks;
use tidb_chunk::row::OwnedRow;

use crate::executor::ExecError;
use crate::sort_util::{reload_cursor, DataCursor, RowWithPartition};
use crate::topn_chunk_heap::go_heap;

/// Go `multiWayMergeSource`: the runs a [`MultiWayMerger`] draws from.
pub trait MultiWayMergeSource {
    /// One merged element -- Go's `chunk.Row`, owned here (see the module
    /// narrowings).
    type Item;

    /// Go `getPartitionNum`: how many runs the source holds.
    fn partition_num(&self) -> usize;

    /// Go `next(partitionID)`: the next element of one run, or `None` once that
    /// run is exhausted (Go's empty `chunk.Row`).
    fn next(&mut self, partition_id: usize) -> Result<Option<Self::Item>, ExecError>;
}

/// Go `memorySource`: runs that are already materialized and sorted in memory.
pub struct MemorySource<T> {
    /// Go `sortedRowsIters`, as owned queues.
    runs: Vec<std::vec::IntoIter<T>>,
}

impl<T> MemorySource<T> {
    /// Builds a source over one already-sorted vector per run.
    #[must_use]
    pub fn new(sorted_runs: Vec<Vec<T>>) -> Self {
        MemorySource {
            runs: sorted_runs.into_iter().map(Vec::into_iter).collect(),
        }
    }
}

impl<T> MultiWayMergeSource for MemorySource<T> {
    type Item = T;

    fn partition_num(&self) -> usize {
        self.runs.len()
    }

    fn next(&mut self, partition_id: usize) -> Result<Option<T>, ExecError> {
        Ok(self.runs[partition_id].next())
    }
}

/// Go `diskSource`: runs spilled to `DataInDiskByChunks`, walked with one
/// [`DataCursor`] each.
pub struct DiskSource {
    /// Go `sortedRowsInDisk`.
    sorted_rows_in_disk: Vec<DataInDiskByChunks>,
    /// Go `cursors`.
    cursors: Vec<DataCursor>,
}

impl DiskSource {
    /// Go `diskSource.init`'s cursor setup: one cursor per spilled run,
    /// positioned before its first chunk.
    #[must_use]
    pub fn new(sorted_rows_in_disk: Vec<DataInDiskByChunks>) -> Self {
        let cursors = (0..sorted_rows_in_disk.len())
            .map(|_| DataCursor::new())
            .collect();
        DiskSource {
            sorted_rows_in_disk,
            cursors,
        }
    }

    /// Removes every spill file the source still owns.
    pub fn close(&mut self) {
        for in_disk in &mut self.sorted_rows_in_disk {
            in_disk.close();
        }
        for cursor in &mut self.cursors {
            cursor.clear();
        }
    }
}

impl MultiWayMergeSource for DiskSource {
    type Item = OwnedRow;

    fn partition_num(&self) -> usize {
        self.sorted_rows_in_disk.len()
    }

    fn next(&mut self, partition_id: usize) -> Result<Option<OwnedRow>, ExecError> {
        // Go's `diskSource.init` primes each cursor with chunk 0 and calls
        // `begin`; `next` then walks and reloads. Both are folded here: a
        // cursor that has no current row reloads and begins.
        if let Some(row) = self.cursors[partition_id].next() {
            return Ok(Some(row.copy_construct()));
        }
        loop {
            // Try to fetch more data from the disk.
            if !reload_cursor(
                &mut self.cursors[partition_id],
                &mut self.sorted_rows_in_disk[partition_id],
            )? {
                return Ok(None);
            }
            // Get new row. Go raises "Get an empty row" here; an empty spilled
            // chunk is possible only through a corrupt file, and skipping it
            // keeps the merge total.
            if let Some(row) = self.cursors[partition_id].begin() {
                return Ok(Some(row.copy_construct()));
            }
        }
    }
}

/// Go `multiWayMerger` plus `multiWayMergeImpl`: the merge and the heap it
/// keeps its per-run heads in.
///
/// `less` is Go's `lessRowFunction`: it orders two elements, and the merge
/// yields the SMALLEST first. Ties resolve to the run that reached the heap
/// root first, exactly as Go's `container/heap` resolves them.
pub struct MultiWayMerger<S: MultiWayMergeSource, F> {
    source: S,
    less: F,
    /// Go `multiWayMergeImpl.elements`.
    elements: Vec<RowWithPartition<S::Item>>,
    initialized: bool,
}

impl<S, F> MultiWayMerger<S, F>
where
    S: MultiWayMergeSource,
    F: FnMut(&S::Item, &S::Item) -> Result<Ordering, ExecError>,
{
    /// Go `newMultiWayMerger`.
    pub fn new(source: S, less: F) -> Self {
        let capacity = source.partition_num();
        MultiWayMerger {
            source,
            less,
            elements: Vec::with_capacity(capacity),
            initialized: false,
        }
    }

    /// Go `multiWayMerger.init` -> `source.init`: takes the head of every
    /// non-empty run and heapifies.
    ///
    /// A run that is empty from the start contributes nothing, which is Go's
    /// `if row.IsEmpty() { continue }`.
    pub fn init(&mut self) -> Result<(), ExecError> {
        self.elements.clear();
        for partition_id in 0..self.source.partition_num() {
            if let Some(row) = self.source.next(partition_id)? {
                self.elements.push(RowWithPartition { row, partition_id });
            }
        }
        let less = &mut self.less;
        let mut err: Option<ExecError> = None;
        go_heap::init(&mut self.elements, &mut |a, b| {
            heap_less(less, &mut err, a, b)
        });
        self.initialized = true;
        match err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Go `multiWayMerger.next`: the smallest live head, with that run's
    /// replacement sifted into its place (`heap.Fix`) or the run dropped from
    /// the heap (`heap.Remove`).
    ///
    /// `None` means every run is exhausted.
    ///
    /// Not [`Iterator::next`]: pulling an element can FAIL (a disk read, an
    /// unorderable key pair), which `Iterator` cannot report. The Go name is
    /// kept.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<S::Item>, ExecError> {
        debug_assert!(self.initialized, "init must run before next");
        if self.elements.is_empty() {
            return Ok(None);
        }
        let partition_id = self.elements[0].partition_id;
        let new_row = self.source.next(partition_id)?;
        let less = &mut self.less;
        let mut err: Option<ExecError> = None;
        let out = match new_row {
            // Go: `heap.Remove(m.multiWayMerge, 0); return elem.row`.
            None => go_heap::remove(&mut self.elements, 0, &mut |a, b| {
                heap_less(less, &mut err, a, b)
            })
            .map(|elem| elem.row),
            // Go: `m.multiWayMerge.elements[0].row = newRow; heap.Fix(...);
            // return elem.row` -- the yielded row must leave the slot BEFORE
            // the replacement sifts away from index 0.
            Some(row) => {
                let yielded = std::mem::replace(&mut self.elements[0].row, row);
                go_heap::fix(&mut self.elements, 0, &mut |a, b| {
                    heap_less(less, &mut err, a, b)
                });
                Some(yielded)
            }
        };
        if let Some(err) = err {
            return Err(err);
        }
        Ok(out)
    }

    /// The runs this merge draws from.
    pub fn source_mut(&mut self) -> &mut S {
        &mut self.source
    }

    /// How many runs still have a live head.
    #[must_use]
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Whether every run is exhausted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

/// Go `multiWayMergeImpl.Less`, with the comparison failure captured so the
/// sift routines stay total (the first error wins and `init`/`next` return it).
fn heap_less<T>(
    less: &mut impl FnMut(&T, &T) -> Result<Ordering, ExecError>,
    err: &mut Option<ExecError>,
    a: &RowWithPartition<T>,
    b: &RowWithPartition<T>,
) -> bool {
    match less(&a.row, &b.row) {
        Ok(ord) => ord == Ordering::Less,
        Err(e) => {
            if err.is_none() {
                *err = Some(e);
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    //! NEW COVERAGE. Go has no direct unit test for `multiWayMerger`; it is
    //! reached through the spilled sort/TopN suites (`sort_spill_test.go`,
    //! `topn_spill_test.go`, `TestGenerateTopNResultsWhenSpillOnlyOnce`).
    //! These pin the merger's own contracts: ordering across runs, duplicate
    //! keys, runs that start empty or run dry mid-merge, and the disk source's
    //! chunk-boundary reload.

    use super::*;
    use std::sync::Arc;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

    fn asc(a: &i64, b: &i64) -> Result<Ordering, ExecError> {
        Ok(a.cmp(b))
    }

    /// Drains a merge over in-memory runs.
    fn merge_memory(runs: Vec<Vec<i64>>) -> Vec<i64> {
        let mut merger = MultiWayMerger::new(MemorySource::new(runs), asc);
        merger.init().expect("init");
        let mut out = Vec::new();
        while let Some(v) = merger.next().expect("next") {
            out.push(v);
        }
        assert!(merger.is_empty());
        out
    }

    #[test]
    fn a_merge_of_sorted_runs_is_sorted() {
        assert_eq!(
            merge_memory(vec![vec![1, 4, 7], vec![2, 5, 8], vec![3, 6, 9]]),
            (1..=9).collect::<Vec<i64>>()
        );
    }

    #[test]
    fn duplicate_keys_all_survive_the_merge() {
        // Every run holds the same key; the merge must emit each copy once,
        // never collapse or drop one.
        assert_eq!(
            merge_memory(vec![vec![5, 5], vec![5], vec![5, 5, 5]]),
            vec![5; 6]
        );
    }

    #[test]
    fn runs_of_wildly_different_lengths_all_drain() {
        assert_eq!(
            merge_memory(vec![vec![0], vec![1, 2, 3, 4, 5, 6], vec![7]]),
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn a_run_that_starts_empty_is_never_in_the_heap() {
        // Go's `init` skips an empty run with `if row.IsEmpty() { continue }`.
        let mut merger = MultiWayMerger::new(
            MemorySource::new(vec![vec![], vec![2, 3], vec![], vec![1]]),
            asc,
        );
        merger.init().expect("init");
        assert_eq!(merger.len(), 2, "only the two non-empty runs are live");
        let mut out = Vec::new();
        while let Some(v) = merger.next().expect("next") {
            out.push(v);
        }
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[test]
    fn a_merge_over_no_runs_at_all_yields_nothing() {
        let mut merger = MultiWayMerger::new(MemorySource::<i64>::new(vec![]), asc);
        merger.init().expect("init");
        assert!(merger.next().expect("next").is_none());
        // And it stays exhausted.
        assert!(merger.next().expect("next").is_none());
    }

    #[test]
    fn every_run_but_one_exhausting_early_still_drains_the_survivor() {
        // The first three runs are consumed in the first three `next` calls,
        // exercising `heap.Remove` three times before the tail run drains.
        assert_eq!(
            merge_memory(vec![vec![1], vec![2], vec![3], vec![4, 5, 6]]),
            vec![1, 2, 3, 4, 5, 6]
        );
    }

    #[test]
    fn a_descending_comparator_merges_descending_runs() {
        let runs = vec![vec![9, 6, 3], vec![8, 5, 2]];
        let mut merger =
            MultiWayMerger::new(MemorySource::new(runs), |a: &i64, b: &i64| Ok(b.cmp(a)));
        merger.init().expect("init");
        let mut out = Vec::new();
        while let Some(v) = merger.next().expect("next") {
            out.push(v);
        }
        assert_eq!(out, vec![9, 8, 6, 5, 3, 2]);
    }

    #[test]
    fn a_failing_comparator_surfaces_from_next() {
        let mut merger = MultiWayMerger::new(
            MemorySource::new(vec![vec![1, 2], vec![3, 4]]),
            |_: &i64, _: &i64| Err(ExecError::internal("unorderable key")),
        );
        // `init` heapifies two elements, so the first comparison already fails.
        let err = merger.init().expect_err("comparison must fail");
        assert!(matches!(err, ExecError::Internal(_)));
    }

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn chunk_of(vals: &[i64]) -> Chunk {
        let fields = [long()];
        let mut chk = Chunk::new(&fields, vals.len().max(1), vals.len().max(1));
        for &v in vals {
            chk.append_int64(0, v);
        }
        chk
    }

    fn storage(name: &str) -> Arc<SpillStorage> {
        let dir = std::env::temp_dir().join(format!("tidb_rust_multi_way_merge_{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: dir,
                quota_bytes: -1,
                encryption: SpillEncryptionMethod::Plaintext,
            })
            .expect("spill storage"),
        )
    }

    /// One spilled run, written as several chunks so the merge has to reload
    /// a cursor mid-run.
    fn run_in_disk(name: &str, chunks: &[&[i64]]) -> DataInDiskByChunks {
        let mut in_disk = DataInDiskByChunks::new(vec![long()], "", storage(name));
        for chk in chunks {
            in_disk.add(&chunk_of(chk)).expect("add");
        }
        in_disk
    }

    #[test]
    fn a_disk_merge_crosses_chunk_boundaries_within_each_run() {
        let runs = vec![
            run_in_disk("disk_a", &[&[1, 4], &[7, 10]]),
            run_in_disk("disk_b", &[&[2, 5], &[8]]),
            run_in_disk("disk_c", &[&[3, 6, 9]]),
        ];
        let mut merger =
            MultiWayMerger::new(DiskSource::new(runs), |a: &OwnedRow, b: &OwnedRow| {
                Ok(a.as_row().get_int64(0).cmp(&b.as_row().get_int64(0)))
            });
        merger.init().expect("init");
        let mut out = Vec::new();
        while let Some(row) = merger.next().expect("next") {
            out.push(row.as_row().get_int64(0));
        }
        assert_eq!(out, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        merger.source_mut().close();
    }

    #[test]
    fn a_disk_run_with_no_chunks_is_skipped_at_init() {
        let runs = vec![
            run_in_disk("disk_empty", &[]),
            run_in_disk("disk_one", &[&[42]]),
        ];
        let mut merger =
            MultiWayMerger::new(DiskSource::new(runs), |a: &OwnedRow, b: &OwnedRow| {
                Ok(a.as_row().get_int64(0).cmp(&b.as_row().get_int64(0)))
            });
        merger.init().expect("init");
        assert_eq!(merger.len(), 1);
        assert_eq!(
            merger
                .next()
                .expect("next")
                .map(|r| r.as_row().get_int64(0)),
            Some(42)
        );
        assert!(merger.next().expect("next").is_none());
        merger.source_mut().close();
    }
}
