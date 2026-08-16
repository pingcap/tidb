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

//! Go `pkg/executor/sortexec`, covering `topn_chunk_heap.go` -- the BOUNDED
//! max-heap a `TopN` keeps its survivors in, plus the Go stdlib
//! `container/heap` sift rules both this file and `multi_way_merge.go` import.
//!
//! This crate ports `pkg/executor/sortexec` in pieces. COVERED here:
//! `topn_chunk_heap.go`. Covered elsewhere: `sort.go` -> [`crate::sort`],
//! `sort_util.go` -> [`crate::sort_util`], `sort_partition.go` ->
//! [`crate::sort_partition`], `topn.go` -> [`crate::topn`], `topn_spill.go` ->
//! [`crate::topn_spill`], `multi_way_merge.go` -> [`crate::multi_way_merge`].
//! NOT COVERED anywhere yet: `parallel_sort_worker.go`,
//! `parallel_sort_spill_helper.go`, `sort_spill.go`, `topn_worker.go`.
//!
//! [`TopNChunkHeap`] is the row store AND the heap over it, exactly as Go's
//! `topNChunkHeap` is: rows land in a chunk list, `rowPtrs` index them, and the
//! heap orders the pointers by [`SortByItem`] with the MAX at the root, so the
//! worst retained row is the one a new row is measured against.
//!
//! KNOWN DUPLICATION, to be resolved: [`crate::topn::TopNExec`] was written
//! before this file and carries its OWN executor-fused copy of the same store
//! and sift rules (`heap_down`/`heap_up`/`heap_init`/`heap_pop`/
//! `heap_fix_root`/`do_compaction`, over its inline `chunks`/`keys`/`row_ptrs`
//! fields), because its spill path threads the store straight into
//! [`crate::topn_spill::SpilledRun::write`]. The two agree rule for rule --
//! this file's `go_heap` is the same algorithm `TopNExec` inlines, and
//! `TopNExec`'s suite pins it against a recorded probe of Go's own heap -- but
//! they are not yet ONE implementation. Folding `TopNExec` onto
//! [`TopNChunkHeap`] is a follow-up refactor, deliberately not bundled with
//! this port so the executor's spill tests move separately.
//!
//! Why the sift rules are ported rather than delegated to [`std::collections::BinaryHeap`]:
//! [`TopNChunkHeap::greater_keys`] returns FALSE for equal keys, so a tie never
//! evicts the incumbent, and WHICH of several tied rows survives at the `count`
//! boundary depends on the heap's exact shape. Reproducing Go's `down`/`up`
//! makes a query with ties at the boundary return the rows Go returns.
//!
//! NARROWINGS, by name:
//!
//! * `memTracker` is NOT a field here. Go attaches the chunk list's own tracker
//!   to the TopN's; this port has the heap report [`TopNChunkHeap::memory_usage`]
//!   and lets [`crate::topn::TopNExec`] replace the tracked total, which is the
//!   only place the total moves and so cannot drift. `doCompaction`'s
//!   `ReplaceChild` bookkeeping disappears with it.
//! * `init` takes the CHILD's field types, `initCap`, and `maxChunkSize`
//!   directly instead of a `*TopNExec` to read them off. Go's comment explains
//!   the values must come from the child (inline projection changes the TopN's
//!   own schema); the port passes exactly those values.
//! * Keys are MATERIALIZED. Go's planner guarantees a TopN's by-items are plain
//!   child columns, so `keyColumnsCompare` re-reads the chunk cell; this tier's
//!   driver allows arbitrary by-item expressions, so each stored row carries its
//!   evaluated key. `process_chk` therefore takes the row keys alongside the
//!   chunk -- the executor evaluates them, because only it holds the eval
//!   context.
//! * `TestKillSignalInTopN` is NOT ported: it is a Go test helper exported from
//!   the production file so an external test package can reach unexported
//!   fields, and it drives the parallel spill helper this crate has not ported.

use std::cmp::Ordering;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, FieldType};

use crate::executor::ExecError;
use crate::sort::{less_by_items, SortByItem};

/// Go stdlib `container/heap`, which both `topn_chunk_heap.go` and
/// `multi_way_merge.go` import.
///
/// The sift order of Go's implementation is observable whenever the comparison
/// has ties, so it is reproduced rather than approximated. `less` takes the two
/// ELEMENTS instead of two indices, which lets a caller keep its comparison
/// state (evaluated keys, a captured comparison error) outside `data`.
pub(crate) mod go_heap {
    /// Go `container/heap.down`. Returns whether the element moved.
    pub(crate) fn down<T>(
        data: &mut [T],
        i0: usize,
        n: usize,
        less: &mut dyn FnMut(&T, &T) -> bool,
    ) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && less(&data[j2], &data[j1]) {
                j = j2;
            }
            if !less(&data[j], &data[i]) {
                break;
            }
            data.swap(i, j);
            i = j;
        }
        i > i0
    }

    /// Go `container/heap.up`.
    pub(crate) fn up<T>(data: &mut [T], j0: usize, less: &mut dyn FnMut(&T, &T) -> bool) {
        let mut j = j0;
        loop {
            // Go's `i := (j - 1) / 2` on a signed int gives 0 for j == 0
            // (division truncates toward zero), which is what ends the walk at
            // the root; an unsigned `j - 1` would wrap instead.
            let i = if j == 0 { 0 } else { (j - 1) / 2 };
            if i == j || !less(&data[j], &data[i]) {
                break;
            }
            data.swap(i, j);
            j = i;
        }
    }

    /// Go `container/heap.Init`.
    pub(crate) fn init<T>(data: &mut [T], less: &mut dyn FnMut(&T, &T) -> bool) {
        let n = data.len();
        for i in (0..n / 2).rev() {
            down(data, i, n, less);
        }
    }

    /// Go `container/heap.Fix`.
    pub(crate) fn fix<T>(data: &mut [T], i: usize, less: &mut dyn FnMut(&T, &T) -> bool) {
        let n = data.len();
        if !down(data, i, n, less) {
            up(data, i, less);
        }
    }

    /// Go `container/heap.Pop`: the root is swapped to the end, the rest is
    /// sifted, and the tail element is removed and returned.
    pub(crate) fn pop<T>(data: &mut Vec<T>, less: &mut dyn FnMut(&T, &T) -> bool) -> Option<T> {
        let n = data.len().checked_sub(1)?;
        data.swap(0, n);
        down(data, 0, n, less);
        data.pop()
    }

    /// Go `container/heap.Remove`.
    pub(crate) fn remove<T>(
        data: &mut Vec<T>,
        i: usize,
        less: &mut dyn FnMut(&T, &T) -> bool,
    ) -> Option<T> {
        let n = data.len().checked_sub(1)?;
        if n != i {
            data.swap(i, n);
            if !down(data, i, n, less) {
                up(data, i, less);
            }
        }
        data.pop()
    }
}

/// Go `chunk.RowPtr`: `(chunk index, row index)` into the heap's row store.
pub type RowPtr = (usize, usize);

/// Go `topNChunkHeap`: the retained rows of a `TopN` and the max-heap over
/// them.
pub struct TopNChunkHeap {
    /// Go `compareRow`/`greaterRow`, which this port derives from the by-items
    /// rather than carrying as two closures.
    by_items: Vec<SortByItem>,
    /// The CHILD's output types -- see the module narrowings for why they are
    /// not the TopN's own.
    field_types: Vec<FieldType>,
    init_cap: usize,
    max_chunk_size: usize,
    /// Go `rowChunks`: the chunk list holding row values.
    row_chunks: Vec<Chunk>,
    /// The evaluated by-item keys, indexed exactly as `row_chunks` is, so one
    /// [`RowPtr`] addresses a row and its key.
    keys: Vec<Vec<Vec<Datum>>>,
    /// Go `rowPtrs`.
    row_ptrs: Vec<RowPtr>,
    /// Go `isInitialized`.
    is_initialized: bool,
    /// Go `isRowPtrsInit`.
    is_row_ptrs_init: bool,
    /// Go `totalLimit` = `Limit.Offset + Limit.Count`.
    total_limit: u64,
    /// Go `idx`: the emit cursor, seeded with `Limit.Offset`.
    idx: usize,
    /// The first comparison error a sift or a sort saw. Go's `keyCmpFuncs`
    /// reject unorderable key types up front, so an error here likewise
    /// invalidates the whole result and the executor returns it from `Next`.
    cmp_err: Option<ExecError>,
}

impl Default for TopNChunkHeap {
    fn default() -> Self {
        TopNChunkHeap::new()
    }
}

impl TopNChunkHeap {
    /// Go's zero value `&topNChunkHeap{}`, before `init`.
    #[must_use]
    pub fn new() -> Self {
        TopNChunkHeap {
            by_items: Vec::new(),
            field_types: Vec::new(),
            init_cap: 0,
            max_chunk_size: 0,
            row_chunks: Vec::new(),
            keys: Vec::new(),
            row_ptrs: Vec::new(),
            is_initialized: false,
            is_row_ptrs_init: false,
            total_limit: 0,
            idx: 0,
            cmp_err: None,
        }
    }

    /// Go `init`: binds the comparison, the child-shaped chunk list, the bound,
    /// and the emit cursor.
    pub fn init(
        &mut self,
        by_items: Vec<SortByItem>,
        field_types: Vec<FieldType>,
        init_cap: usize,
        max_chunk_size: usize,
        total_limit: u64,
        idx: usize,
    ) {
        self.by_items = by_items;
        self.field_types = field_types;
        self.init_cap = init_cap;
        self.max_chunk_size = max_chunk_size;
        self.total_limit = total_limit;
        self.idx = idx;
        self.is_initialized = true;
    }

    /// Go `isInitialized`.
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.is_initialized
    }

    /// Go `isRowPtrsInit`.
    #[must_use]
    pub fn is_row_ptrs_init(&self) -> bool {
        self.is_row_ptrs_init
    }

    /// Go `totalLimit`.
    #[must_use]
    pub fn total_limit(&self) -> u64 {
        self.total_limit
    }

    /// Go `initPtrs`/`initPtrsImpl`: one pointer per stored row, in store
    /// order. (Go's `initPtrs` also charges the pointers to the tracker; here
    /// [`TopNChunkHeap::memory_usage`] already counts them.)
    pub fn init_ptrs(&mut self) {
        self.row_ptrs = (0..self.row_chunks.len())
            .flat_map(|c| (0..self.row_chunks[c].num_rows()).map(move |r| (c, r)))
            .collect();
        self.is_row_ptrs_init = true;
    }

    /// Go `clear`: drops every stored row and un-initializes the heap.
    pub fn clear(&mut self) {
        self.row_chunks.clear();
        self.keys.clear();
        self.row_ptrs.clear();
        self.is_row_ptrs_init = false;
        self.is_initialized = false;
        self.idx = 0;
    }

    /// Go `chunk.List.Add`: takes a whole chunk, with its rows' keys, into the
    /// store.
    pub fn add_chunk(&mut self, chunk: Chunk, keys: Vec<Vec<Datum>>) {
        debug_assert_eq!(chunk.num_rows(), keys.len());
        self.row_chunks.push(chunk);
        self.keys.push(keys);
    }

    /// Go `chunk.List.AppendRow`: appends into the last chunk while it has
    /// capacity, else allocates one, and returns the row's pointer.
    pub fn append_row(&mut self, row: Row<'_>, key: Vec<Datum>) -> RowPtr {
        let need_new = match self.row_chunks.last() {
            None => true,
            Some(last) => last.num_rows() >= last.capacity(),
        };
        if need_new {
            self.row_chunks.push(Chunk::new(
                &self.field_types,
                self.init_cap,
                self.max_chunk_size,
            ));
            self.keys.push(Vec::new());
        }
        let chk_idx = self.row_chunks.len() - 1;
        let row_idx = self.row_chunks[chk_idx].num_rows();
        self.row_chunks[chk_idx].append_row(row);
        self.keys[chk_idx].push(key);
        (chk_idx, row_idx)
    }

    /// Go `chunk.List.Len`: how many rows the store holds, retained or not.
    #[must_use]
    pub fn stored_len(&self) -> usize {
        self.row_chunks.iter().map(Chunk::num_rows).sum()
    }

    /// Go `Len`: how many rows the heap retains.
    #[must_use]
    pub fn len(&self) -> usize {
        self.row_ptrs.len()
    }

    /// Whether the heap retains nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_ptrs.is_empty()
    }

    /// The retained pointers, in heap order (or, after
    /// [`TopNChunkHeap::sort_row_ptrs_ascending`], in output order).
    #[must_use]
    pub fn row_ptrs(&self) -> &[RowPtr] {
        &self.row_ptrs
    }

    /// The row store, for the spill writer.
    #[must_use]
    pub fn chunks(&self) -> &[Chunk] {
        &self.row_chunks
    }

    /// The row a retained pointer names.
    #[must_use]
    pub fn row_at(&self, i: usize) -> Row<'_> {
        let (c, r) = self.row_ptrs[i];
        self.row_chunks[c].get_row(r)
    }

    /// Go `idx`: the emit cursor.
    #[must_use]
    pub fn idx(&self) -> usize {
        self.idx
    }

    /// Moves the emit cursor forward one row.
    pub fn advance_idx(&mut self) {
        self.idx += 1;
    }

    /// A copy of the key at `ptr`. Copying keeps the borrow checker out of the
    /// sift routines; a key is one datum per `ORDER BY` item.
    fn key_at(&self, ptr: RowPtr) -> Vec<Datum> {
        self.keys[ptr.0][ptr.1].clone()
    }

    /// Go `keyColumnsCompare`: the by-item order of two stored rows.
    ///
    /// A comparison error is captured (see [`TopNChunkHeap::take_cmp_err`]) and
    /// reported as `Equal`, so the sift routines stay total.
    pub fn key_columns_compare(&mut self, i: RowPtr, j: RowPtr) -> Ordering {
        let a = self.key_at(i);
        let b = self.key_at(j);
        self.compare_keys(&a, &b)
    }

    /// Go `compareRow` on two evaluated keys.
    fn compare_keys(&mut self, a: &[Datum], b: &[Datum]) -> Ordering {
        match less_by_items(&self.by_items, a, b) {
            Ok(cmp) => cmp,
            Err(err) => {
                if self.cmp_err.is_none() {
                    self.cmp_err = Some(err);
                }
                Ordering::Equal
            }
        }
    }

    /// Go `TopNExec.greaterRow`: strictly greater in by-item order. EQUAL is
    /// FALSE, which is what keeps a tie from evicting the incumbent.
    pub fn greater_keys(&mut self, a: &[Datum], b: &[Datum]) -> bool {
        self.compare_keys(a, b) == Ordering::Greater
    }

    /// Go `Less`: the heap is a MAX-heap, so "less" means "greater row".
    pub fn less(&mut self, i: usize, j: usize) -> bool {
        let a = self.row_ptrs[i];
        let b = self.row_ptrs[j];
        self.key_columns_compare(a, b) == Ordering::Greater
    }

    /// Runs `f` with the pointer slice detached, so the closure can hold the
    /// heap mutably for its comparisons.
    fn with_ptrs<R>(&mut self, f: impl FnOnce(&mut Self, &mut Vec<RowPtr>) -> R) -> R {
        let mut ptrs = std::mem::take(&mut self.row_ptrs);
        let out = f(self, &mut ptrs);
        self.row_ptrs = ptrs;
        out
    }

    /// Go `heap.Init(h)`.
    pub fn heap_init(&mut self) {
        self.with_ptrs(|heap, ptrs| {
            go_heap::init(ptrs, &mut |a, b| {
                heap.key_columns_compare(*a, *b) == Ordering::Greater
            });
        });
    }

    /// Go `heap.Pop(h)`: drops the heap's MAX row from the retained set.
    pub fn heap_pop(&mut self) -> Option<RowPtr> {
        self.with_ptrs(|heap, ptrs| {
            go_heap::pop(ptrs, &mut |a, b| {
                heap.key_columns_compare(*a, *b) == Ordering::Greater
            })
        })
    }

    /// Go `heap.Fix(h, 0)`.
    pub fn heap_fix_root(&mut self) {
        self.with_ptrs(|heap, ptrs| {
            go_heap::fix(ptrs, 0, &mut |a, b| {
                heap.key_columns_compare(*a, *b) == Ordering::Greater
            });
        });
    }

    /// Go `heap.Pop` until the heap holds at most `totalLimit` rows.
    pub fn trim_to_total_limit(&mut self) {
        while self.row_ptrs.len() as u64 > self.total_limit {
            self.heap_pop();
        }
    }

    /// Go `update`: a row strictly smaller than the heap's max evicts it, and
    /// the root is sifted back down.
    pub fn update(&mut self, new_row: Row<'_>, new_key: Vec<Datum>) {
        let heap_max_key = self.key_at(self.row_ptrs[0]);
        if self.greater_keys(&heap_max_key, &new_key) {
            // Evict heap max, keep the next row.
            let ptr = self.append_row(new_row, new_key);
            self.row_ptrs[0] = ptr;
            self.heap_fix_root();
        }
    }

    /// Go `processChk`: every row of `chk`, one at a time, against the heap's
    /// max. `keys` holds the evaluated by-item key of each of its rows.
    pub fn process_chk(&mut self, chk: &Chunk, keys: Vec<Vec<Datum>>) {
        debug_assert_eq!(chk.num_rows(), keys.len());
        for (i, key) in keys.into_iter().enumerate() {
            self.update(chk.get_row(i), key);
        }
    }

    /// Go `doCompaction`: rebuild the chunks and row pointers from the retained
    /// rows only, releasing the evicted ones.
    ///
    /// Without it, an input that is already ascending while the query wants a
    /// descending TopN would keep every row in memory. On randomly distributed
    /// data this runs `log(n)` times.
    pub fn do_compaction(&mut self) {
        let old_chunks = std::mem::take(&mut self.row_chunks);
        let old_keys = std::mem::take(&mut self.keys);
        let old_ptrs = std::mem::take(&mut self.row_ptrs);
        let mut new_ptrs = Vec::with_capacity(old_ptrs.len());
        for (c, r) in old_ptrs {
            let ptr = self.append_row(old_chunks[c].get_row(r), old_keys[c][r].clone());
            new_ptrs.push(ptr);
        }
        self.row_ptrs = new_ptrs;
    }

    /// The ASCENDING order of `generateTopNResults`' first step, and of
    /// `spillHeap` before it writes a run.
    ///
    /// The emit cursor is re-seeded with `offset`, as Go seeds `chkHeap.idx`.
    pub fn sort_row_ptrs_ascending(&mut self, offset: usize) -> Result<(), ExecError> {
        let ptrs = std::mem::take(&mut self.row_ptrs);
        let mut order: Vec<usize> = (0..ptrs.len()).collect();
        // `sort_by` cannot borrow `self` mutably, so the keys are lifted out
        // first and the comparison error is captured as in `SortExec`.
        let keys: Vec<Vec<Datum>> = ptrs.iter().map(|&ptr| self.key_at(ptr)).collect();
        let by_items = &self.by_items;
        let mut sort_err: Option<ExecError> = None;
        order.sort_by(|&a, &b| match less_by_items(by_items, &keys[a], &keys[b]) {
            Ok(cmp) => cmp,
            Err(err) => {
                if sort_err.is_none() {
                    sort_err = Some(err);
                }
                Ordering::Equal
            }
        });
        if let Some(err) = sort_err {
            self.row_ptrs = ptrs;
            return Err(err);
        }
        self.row_ptrs = order.into_iter().map(|i| ptrs[i]).collect();
        self.idx = offset;
        Ok(())
    }

    /// What the store currently holds, as bytes.
    ///
    /// Go tracks this incrementally through `chunk.List`'s own tracker and
    /// `ReplaceChild` on compaction; recomputing the total and REPLACING the
    /// tracker's value has the same effect at the only points where the total
    /// moves, and cannot drift.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        let mut bytes: i64 = 0;
        for chunk in &self.row_chunks {
            bytes += chunk.memory_usage();
            bytes +=
                tidb_chunk::row::ROW_SIZE * i64::try_from(chunk.num_rows()).unwrap_or(i64::MAX);
        }
        for chunk_keys in &self.keys {
            for key in chunk_keys {
                bytes += i64::try_from(size_of::<Vec<Datum>>()).unwrap_or(i64::MAX);
                for datum in key {
                    bytes += i64::try_from(datum.estimated_mem_usage()).unwrap_or(i64::MAX);
                }
            }
        }
        bytes += i64::try_from(size_of::<RowPtr>() * self.row_ptrs.len()).unwrap_or(i64::MAX);
        bytes
    }

    /// Surfaces the first comparison error a sift or sort captured.
    pub fn take_cmp_err(&mut self) -> Result<(), ExecError> {
        match self.cmp_err.take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    //! NEW COVERAGE. Go has no direct unit test for `topNChunkHeap`; it is
    //! exercised through `TopNExec` (`rank_topn_test.go`,
    //! `topn_spill_test.go`), and [`crate::topn`]'s suite covers that path
    //! including a recorded probe of Go's own heap. These tests pin the data
    //! structure's own contracts: the bound, replacement, tie behavior, null
    //! and collation ordering, and compaction.

    use super::*;
    use tidb_datatype::{Collation, FieldTypeCode, GoString};
    use tidb_expr::column::Column;
    use tidb_expr::expression::Expression;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn col(idx: usize, ft: FieldType) -> Expression {
        let mut c = Column::new(idx as i64 + 1, ft);
        c.index = idx as i64;
        Expression::Column(c)
    }

    fn asc(idx: usize, ft: FieldType) -> Vec<SortByItem> {
        vec![SortByItem {
            expr: col(idx, ft),
            desc: false,
        }]
    }

    /// A heap bounded at `limit`, ordered by the first column.
    fn heap_of(limit: u64, fields: Vec<FieldType>, by: Vec<SortByItem>) -> TopNChunkHeap {
        let mut heap = TopNChunkHeap::new();
        heap.init(by, fields, 32, 32, limit, 0);
        heap
    }

    /// Feeds `vals` through the Go phase order: fill to the bound, heapify,
    /// trim, then stream the rest through `process_chk`.
    fn run_ints(limit: u64, vals: &[i64], desc: bool) -> Vec<i64> {
        let fields = vec![long()];
        let by = vec![SortByItem {
            expr: col(0, long()),
            desc,
        }];
        let mut heap = heap_of(limit, fields.clone(), by);

        let split = (limit as usize).min(vals.len());
        let mut first = Chunk::new(&fields, 32, 32);
        let mut first_keys = Vec::new();
        for &v in &vals[..split] {
            first.append_int64(0, v);
            first_keys.push(vec![Datum::Int(v)]);
        }
        heap.add_chunk(first, first_keys);
        heap.init_ptrs();
        heap.heap_init();
        heap.trim_to_total_limit();

        if split < vals.len() {
            let mut rest = Chunk::new(&fields, 1024, 1024);
            let mut rest_keys = Vec::new();
            for &v in &vals[split..] {
                rest.append_int64(0, v);
                rest_keys.push(vec![Datum::Int(v)]);
            }
            heap.process_chk(&rest, rest_keys);
        }

        heap.sort_row_ptrs_ascending(0).expect("no compare error");
        heap.take_cmp_err().expect("no compare error");
        (0..heap.len())
            .map(|i| heap.row_at(i).get_int64(0))
            .collect()
    }

    #[test]
    fn the_heap_keeps_exactly_the_bound_and_the_smallest_rows() {
        assert_eq!(run_ints(3, &[9, 1, 8, 2, 7, 3], false), vec![1, 2, 3]);
    }

    #[test]
    fn a_descending_heap_keeps_the_largest_rows() {
        assert_eq!(run_ints(3, &[9, 1, 8, 2, 7, 3], true), vec![9, 8, 7]);
    }

    #[test]
    fn a_bound_wider_than_the_input_keeps_everything() {
        assert_eq!(run_ints(10, &[3, 1, 2], false), vec![1, 2, 3]);
    }

    #[test]
    fn a_worse_row_never_replaces_the_heap_max() {
        // 1,2,3 fill the heap; every later row is worse, so none is stored
        // beyond the fill and the store never grows.
        let fields = vec![long()];
        let mut heap = heap_of(3, fields.clone(), asc(0, long()));
        let mut chk = Chunk::new(&fields, 32, 32);
        let mut keys = Vec::new();
        for v in [1_i64, 2, 3] {
            chk.append_int64(0, v);
            keys.push(vec![Datum::Int(v)]);
        }
        heap.add_chunk(chk, keys);
        heap.init_ptrs();
        heap.heap_init();
        heap.trim_to_total_limit();
        let stored_before = heap.stored_len();

        let mut rest = Chunk::new(&fields, 32, 32);
        let mut rest_keys = Vec::new();
        for v in [4_i64, 5, 6] {
            rest.append_int64(0, v);
            rest_keys.push(vec![Datum::Int(v)]);
        }
        heap.process_chk(&rest, rest_keys);

        assert_eq!(heap.stored_len(), stored_before);
        assert_eq!(heap.len(), 3);
    }

    #[test]
    fn an_equal_row_does_not_evict_the_incumbent() {
        // `greater_keys` is FALSE on ties, so a duplicate of the current max is
        // dropped rather than stored.
        let fields = vec![long()];
        let mut heap = heap_of(2, fields.clone(), asc(0, long()));
        let mut chk = Chunk::new(&fields, 32, 32);
        chk.append_int64(0, 1);
        chk.append_int64(0, 5);
        heap.add_chunk(chk, vec![vec![Datum::Int(1)], vec![Datum::Int(5)]]);
        heap.init_ptrs();
        heap.heap_init();
        heap.trim_to_total_limit();
        let stored_before = heap.stored_len();

        let mut rest = Chunk::new(&fields, 32, 32);
        rest.append_int64(0, 5);
        heap.process_chk(&rest, vec![vec![Datum::Int(5)]]);
        assert_eq!(heap.stored_len(), stored_before);

        // A strictly smaller row does replace it.
        let mut better = Chunk::new(&fields, 32, 32);
        better.append_int64(0, 4);
        heap.process_chk(&better, vec![vec![Datum::Int(4)]]);
        heap.sort_row_ptrs_ascending(0).expect("no compare error");
        let out: Vec<i64> = (0..heap.len())
            .map(|i| heap.row_at(i).get_int64(0))
            .collect();
        assert_eq!(out, vec![1, 4]);
    }

    #[test]
    fn nulls_order_below_every_value_and_desc_negates_that() {
        let fields = vec![long()];
        let by_asc = asc(0, long());
        let by_desc = vec![SortByItem {
            expr: col(0, long()),
            desc: true,
        }];
        // The bound is wide enough to keep all three rows, so the whole order
        // is visible: NULL first ascending, NULL last descending.
        for (by, want) in [
            (by_asc, vec![None, Some(1), Some(7)]),
            (by_desc, vec![Some(7), Some(1), None]),
        ] {
            let mut heap = heap_of(3, fields.clone(), by);
            let mut chk = Chunk::new(&fields, 32, 32);
            chk.append_null(0);
            chk.append_int64(0, 1);
            chk.append_int64(0, 7);
            heap.add_chunk(
                chk,
                vec![vec![Datum::Null], vec![Datum::Int(1)], vec![Datum::Int(7)]],
            );
            heap.init_ptrs();
            heap.heap_init();
            heap.trim_to_total_limit();
            heap.sort_row_ptrs_ascending(0).expect("no compare error");
            let out: Vec<Option<i64>> = (0..heap.len())
                .map(|i| {
                    let row = heap.row_at(i);
                    if row.is_null(0) {
                        None
                    } else {
                        Some(row.get_int64(0))
                    }
                })
                .collect();
            assert_eq!(out, want);
        }
    }

    #[test]
    fn a_case_insensitive_key_orders_by_its_own_collation() {
        // `ORDER BY ci_col` must give a, A, b -- not the byte order A, a, b.
        let ft =
            FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Utf8Mb4GeneralCi);
        let fields = vec![ft.clone()];
        let by = asc(0, ft);
        let mut heap = heap_of(2, fields.clone(), by);
        let mut chk = Chunk::new(&fields, 32, 32);
        let vals = ["b", "A", "a"];
        let mut keys = Vec::new();
        for v in vals {
            chk.append_string(0, GoString::from(v));
            keys.push(vec![Datum::Bytes(v.as_bytes().to_vec())]);
        }
        heap.add_chunk(chk, keys);
        heap.init_ptrs();
        heap.heap_init();
        heap.trim_to_total_limit();
        heap.sort_row_ptrs_ascending(0).expect("no compare error");
        // "A" and "a" tie under the CI collation, so both survive and "b" is
        // the row the bound drops.
        let out: Vec<String> = (0..heap.len())
            .map(|i| heap.row_at(i).get_string(0).to_string())
            .collect();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|s| s.eq_ignore_ascii_case("a")), "{out:?}");
    }

    #[test]
    fn compaction_keeps_the_retained_rows_and_drops_the_rest() {
        // A descending TopN over ascending input evicts on every row, which is
        // the case `doCompaction` exists for.
        let fields = vec![long()];
        let by = vec![SortByItem {
            expr: col(0, long()),
            desc: true,
        }];
        let mut heap = heap_of(2, fields.clone(), by);
        let mut chk = Chunk::new(&fields, 32, 32);
        chk.append_int64(0, 1);
        chk.append_int64(0, 2);
        heap.add_chunk(chk, vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]);
        heap.init_ptrs();
        heap.heap_init();
        heap.trim_to_total_limit();

        let mut rest = Chunk::new(&fields, 32, 32);
        let mut rest_keys = Vec::new();
        for v in 3_i64..=20 {
            rest.append_int64(0, v);
            rest_keys.push(vec![Datum::Int(v)]);
        }
        heap.process_chk(&rest, rest_keys);
        assert!(heap.stored_len() > heap.len());

        heap.do_compaction();
        assert_eq!(heap.stored_len(), heap.len());
        heap.sort_row_ptrs_ascending(0).expect("no compare error");
        let out: Vec<i64> = (0..heap.len())
            .map(|i| heap.row_at(i).get_int64(0))
            .collect();
        assert_eq!(out, vec![20, 19]);
    }

    #[test]
    fn clear_forgets_the_store_and_the_initialization() {
        let fields = vec![long()];
        let mut heap = heap_of(2, fields.clone(), asc(0, long()));
        let mut chk = Chunk::new(&fields, 32, 32);
        chk.append_int64(0, 1);
        heap.add_chunk(chk, vec![vec![Datum::Int(1)]]);
        heap.init_ptrs();
        assert!(heap.is_initialized() && heap.is_row_ptrs_init());
        assert!(heap.memory_usage() > 0);

        heap.clear();
        assert_eq!(heap.stored_len(), 0);
        assert_eq!(heap.len(), 0);
        assert_eq!(heap.idx(), 0);
        assert!(!heap.is_initialized());
        assert!(!heap.is_row_ptrs_init());
    }

    #[test]
    fn go_heap_pop_drains_in_max_first_order() {
        let mut data = vec![3_i32, 1, 4, 1, 5, 9, 2, 6];
        let mut less = |a: &i32, b: &i32| a > b;
        go_heap::init(&mut data, &mut less);
        let mut out = Vec::new();
        while let Some(v) = go_heap::pop(&mut data, &mut less) {
            out.push(v);
        }
        assert_eq!(out, vec![9, 6, 5, 4, 3, 2, 1, 1]);
    }

    #[test]
    fn go_heap_remove_takes_an_interior_element_and_keeps_the_invariant() {
        let mut data = vec![5_i32, 4, 3, 2, 1];
        let mut less = |a: &i32, b: &i32| a < b;
        go_heap::init(&mut data, &mut less);
        assert_eq!(data[0], 1);
        let removed = go_heap::remove(&mut data, 1, &mut less);
        assert!(removed.is_some());
        let mut out = Vec::new();
        while let Some(v) = go_heap::pop(&mut data, &mut less) {
            out.push(v);
        }
        assert_eq!(out.len(), 4);
        let mut sorted = out.clone();
        sorted.sort_unstable();
        assert_eq!(out, sorted, "pop order must be ascending");
    }
}
