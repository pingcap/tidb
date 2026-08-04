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

//! `pkg/executor/sortexec` `TopNExec`: the `ORDER BY ... LIMIT` operator.
//!
//! Go's optimizer fuses a `Limit` over a `Sort` into one `TopN`
//! (`pkg/planner/core/rule_topn_push_down.go` driving
//! `LogicalLimit.PushDownTopN` -> `LogicalSort.PushDownTopN`), and the
//! executor answers it with a BOUNDED max-heap of `offset + count` rows
//! instead of materializing and sorting the whole input.
//!
//! The three phases are Go's, in Go's order (`topn.go`):
//!
//! 1. `loadChunksUntilTotalLimit`: pull child chunks until the store holds at
//!    least `offset + count` rows (or the child is exhausted), then take a
//!    row pointer per stored row.
//! 2. `executeTopN`: `heap.Init` over those pointers as a MAX-heap ordered by
//!    [`TopNExec::greater`], `heap.Pop` down to exactly `offset + count`,
//!    then stream the rest of the child through
//!    [`topNChunkHeap.processChk`](https://github.com/pingcap/tidb): a row
//!    strictly smaller than the heap's max replaces it and the heap is fixed.
//!    Go's `doCompaction` rebuilds the store when it grew past
//!    `topNCompactionFactor` (4) times the retained row count.
//! 3. `generateTopNResults`: sort the surviving pointers ASCENDING and emit
//!    from index `offset`.
//!
//! # Why the heap is Go's, algorithm for algorithm
//!
//! [`TopNExec::greater`] returns FALSE for equal keys, so a tie never evicts
//! the incumbent. Which of several tied rows survives at the `count` boundary
//! is therefore decided by arrival order AND by the shape of the heap, so the
//! sift rules are reproduced from Go's `container/heap` (`down`/`up`) rather
//! than delegated to `BinaryHeap`, whose sift order differs. A query with
//! ties at the boundary returns the rows Go returns.
//!
//! # Divergences, deliberate and named
//!
//! * **Keys are materialized.** Go's planner guarantees a `TopN`'s by-items
//!   are plain child columns (`buildKeyColumns` unwraps `*expression.Column`),
//!   so Go compares by re-reading the chunk cell. This tier's driver hands the
//!   operator arbitrary by-item EXPRESSIONS, so each stored row carries its
//!   evaluated key -- the same choice [`crate::sort::SortExec`] makes, and the
//!   same over-count against the memory quota, which the tracker reports
//!   honestly.
//! * **The final sort is stable** where Go's `slices.SortFunc` is not; only
//!   the order of exactly-tying rows can differ, which Go does not guarantee.
//! * **No spill, no parallel workers, no `RankInfo`.** Go's spill helper,
//!   worker pool and `ROW_NUMBER`-style rank truncation are deferred; on a
//!   quota breach this operator raises the quota error exactly as
//!   [`crate::sort::SortExec`] does.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_util::memory::Tracker;

use crate::mem_quota::StatementMemory;
use crate::sort::SortByItem;

/// Go `topNCompactionFactor`: rebuild the row store once it holds more than
/// this many times the retained row count.
const TOP_N_COMPACTION_FACTOR: usize = 4;

/// Go `chunk.RowPtr`: where a retained row lives in the store.
type RowPtr = (usize, usize);

/// Go `sortexec.TopNExec` (unparallel, in memory).
pub struct TopNExec<C: Columns> {
    meta: ExecutorMeta,
    /// Go `ByItems`.
    by_items: Vec<SortByItem>,
    child: Box<dyn Executor>,
    ctx: C,
    /// Go `Limit.Offset`: how many of the ordered rows to drop.
    offset: u64,
    /// Go `chkHeap.totalLimit` = `Limit.Offset + Limit.Count`, saturated the
    /// way [`crate::limit::LimitExec`] saturates its `end` (Go's planner
    /// clamps the count so the sum cannot wrap).
    total_limit: u64,
    /// Go `fetched`: whether the whole child has been consumed.
    fetched: bool,
    /// Go `chkHeap.rowChunks`: the retained rows, as a chunk list.
    chunks: Vec<Chunk>,
    /// The evaluated by-item keys, indexed exactly as `chunks` is, so a
    /// `RowPtr` addresses a row and its key with the same pair.
    keys: Vec<Vec<Vec<Datum>>>,
    /// Go `chkHeap.rowPtrs`.
    row_ptrs: Vec<RowPtr>,
    /// Go `chkHeap.idx`, initialized to `Limit.Offset`: the emit cursor.
    idx: usize,
    /// The first comparison error seen while sifting or sorting. Go's
    /// `keyCmpFuncs` reject unorderable key types up front, so an error here
    /// likewise invalidates the whole result, and `next` returns it.
    cmp_err: Option<ExecError>,
    memory: StatementMemory,
    /// Go `TopNExec.memTracker`.
    tracker: Arc<Tracker>,
}

impl<C: Columns> TopNExec<C> {
    /// Builds a `TopN` over `child` keeping `count` rows from `offset` in
    /// `by_items` order.
    ///
    /// `memory` is required for the same reason [`crate::sort::SortExec::new`]
    /// requires it: a call site must not be able to build an UNACCOUNTED
    /// row store by omitting it.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        by_items: Vec<SortByItem>,
        child: Box<dyn Executor>,
        ctx: C,
        offset: u64,
        count: u64,
        memory: StatementMemory,
    ) -> Self {
        let count = count.min(u64::MAX - offset);
        let tracker = memory.operator_tracker(meta.id());
        TopNExec {
            meta,
            by_items,
            child,
            ctx,
            offset,
            total_limit: offset + count,
            fetched: false,
            chunks: Vec::new(),
            keys: Vec::new(),
            row_ptrs: Vec::new(),
            idx: 0,
            cmp_err: None,
            memory,
            tracker,
        }
    }

    /// How many rows the store holds (Go `chunk.List.Len`).
    fn stored_len(&self) -> usize {
        self.chunks.iter().map(Chunk::num_rows).sum()
    }

    /// Evaluates the by-item keys for one row.
    fn eval_key(&self, row: tidb_chunk::row::Row<'_>) -> Result<Vec<Datum>, ExecError> {
        let mut key = Vec::with_capacity(self.by_items.len());
        for item in &self.by_items {
            key.push(item.expr.eval(&self.ctx, row)?);
        }
        Ok(key)
    }

    /// Go `chunk.List.Add`: takes a whole child chunk into the store, with the
    /// keys for its rows.
    fn add_chunk(&mut self, chunk: Chunk) -> Result<(), ExecError> {
        let mut chunk_keys = Vec::with_capacity(chunk.num_rows());
        for r in 0..chunk.num_rows() {
            chunk_keys.push(self.eval_key(chunk.get_row(r))?);
        }
        self.chunks.push(chunk);
        self.keys.push(chunk_keys);
        Ok(())
    }

    /// Go `chunk.List.AppendRow`: appends into the last chunk while it has
    /// capacity, else allocates one, and returns the row's pointer.
    fn append_row(&mut self, row: tidb_chunk::row::Row<'_>, key: Vec<Datum>) -> RowPtr {
        let need_new = match self.chunks.last() {
            None => true,
            Some(last) => last.num_rows() >= last.capacity(),
        };
        if need_new {
            self.chunks.push(Chunk::new(
                self.child.ret_field_types(),
                self.child.init_cap(),
                self.child.max_chunk_size(),
            ));
            self.keys.push(Vec::new());
        }
        let chk_idx = self.chunks.len() - 1;
        let row_idx = self.chunks[chk_idx].num_rows();
        self.chunks[chk_idx].append_row(row);
        self.keys[chk_idx].push(key);
        (chk_idx, row_idx)
    }

    /// Go `lessRow` for one key pair: the first non-equal by-item decides and
    /// `Desc` negates it. Each key compares under its own derived collation,
    /// exactly as [`crate::sort::SortExec`] does.
    ///
    /// A comparison error is captured (the caller checks [`Self::cmp_err`])
    /// and reported as `Equal`, so the sift/sort routines stay total.
    fn compare_keys(&mut self, a: &[Datum], b: &[Datum]) -> Ordering {
        for (i, item) in self.by_items.iter().enumerate() {
            let mut cmp = match tidb_expr::compare_datums_with_collation(
                &a[i],
                &b[i],
                tidb_expr::collation_derive::collation_of_node(&item.expr),
            ) {
                Ok(cmp) => cmp,
                Err(err) => {
                    if self.cmp_err.is_none() {
                        self.cmp_err = Some(err.into());
                    }
                    return Ordering::Equal;
                }
            };
            if item.desc {
                cmp = cmp.reverse();
            }
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }

    /// Go `TopNExec.greaterRow`: strictly greater in by-item order. EQUAL is
    /// FALSE, which is what keeps a tie from evicting the incumbent.
    fn greater_keys(&mut self, a: &[Datum], b: &[Datum]) -> bool {
        self.compare_keys(a, b) == Ordering::Greater
    }

    /// `Less` of Go's `topNChunkHeap`: the heap is a MAX-heap, so "less"
    /// means "greater row".
    fn heap_less(&mut self, i: usize, j: usize) -> bool {
        let a = self.key_at(self.row_ptrs[i]);
        let b = self.key_at(self.row_ptrs[j]);
        self.greater_keys(&a, &b)
    }

    /// A copy of the key at `ptr`. Copying keeps the borrow checker out of the
    /// sift routines; keys are short (one datum per `ORDER BY` item).
    fn key_at(&self, ptr: RowPtr) -> Vec<Datum> {
        self.keys[ptr.0][ptr.1].clone()
    }

    /// Go `container/heap.down`, verbatim in structure.
    fn heap_down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && self.heap_less(j2, j1) {
                j = j2;
            }
            if !self.heap_less(j, i) {
                break;
            }
            self.row_ptrs.swap(i, j);
            i = j;
        }
        i > i0
    }

    /// Go `container/heap.up`, verbatim in structure.
    fn heap_up(&mut self, j0: usize) {
        let mut j = j0;
        loop {
            // Go's `i := (j - 1) / 2` on a signed int gives 0 for j == 0
            // (division truncates toward zero), which is what ends the walk at
            // the root; an unsigned `j - 1` would wrap instead.
            let i = if j == 0 { 0 } else { (j - 1) / 2 };
            if i == j || !self.heap_less(j, i) {
                break;
            }
            self.row_ptrs.swap(i, j);
            j = i;
        }
    }

    /// Go `container/heap.Init`.
    fn heap_init(&mut self) {
        let n = self.row_ptrs.len();
        for i in (0..n / 2).rev() {
            self.heap_down(i, n);
        }
    }

    /// Go `container/heap.Pop`: swap the max to the end, sift, truncate.
    fn heap_pop(&mut self) {
        let n = self.row_ptrs.len() - 1;
        self.row_ptrs.swap(0, n);
        self.heap_down(0, n);
        self.row_ptrs.truncate(n);
    }

    /// Go `container/heap.Fix(h, 0)`.
    fn heap_fix_root(&mut self) {
        let n = self.row_ptrs.len();
        if !self.heap_down(0, n) {
            self.heap_up(0);
        }
    }

    /// Go `chkHeap.doCompaction`: rebuild the store from the retained rows
    /// only, so a long scan that keeps evicting does not grow without bound.
    fn do_compaction(&mut self) {
        let old_chunks = std::mem::take(&mut self.chunks);
        let old_keys = std::mem::take(&mut self.keys);
        let old_ptrs = std::mem::take(&mut self.row_ptrs);
        let fields = self.child.ret_field_types().to_vec();
        let init_cap = self.child.init_cap();
        let max_chunk_size = self.child.max_chunk_size();
        let mut new_ptrs = Vec::with_capacity(old_ptrs.len());
        for (c, r) in old_ptrs {
            let need_new = match self.chunks.last() {
                None => true,
                Some(last) => last.num_rows() >= last.capacity(),
            };
            if need_new {
                self.chunks
                    .push(Chunk::new(&fields, init_cap, max_chunk_size));
                self.keys.push(Vec::new());
            }
            let chk_idx = self.chunks.len() - 1;
            let row_idx = self.chunks[chk_idx].num_rows();
            self.chunks[chk_idx].append_row(old_chunks[c].get_row(r));
            self.keys[chk_idx].push(old_keys[c][r].clone());
            new_ptrs.push((chk_idx, row_idx));
        }
        self.row_ptrs = new_ptrs;
    }

    /// What the store currently holds, as bytes.
    ///
    /// Go tracks this incrementally through `chunk.List`'s own tracker and
    /// `ReplaceChild` on compaction; recomputing the total and REPLACING the
    /// tracker's value has the same effect at the only points where the total
    /// moves, and cannot drift.
    fn stored_bytes(&self) -> i64 {
        let mut bytes: i64 = 0;
        for chunk in &self.chunks {
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

    /// Reports the store's current size to the statement budget and checks the
    /// quota, which is where Go's `Consume` fires the OOM action.
    fn account(&mut self) -> Result<(), ExecError> {
        let bytes = self.stored_bytes();
        self.tracker.replace_bytes_used(bytes);
        self.memory.check()
    }

    /// Go `loadChunksUntilTotalLimit` + `executeTopNWhenNoSpillTriggered` +
    /// `executeTopN` + the ascending sort of `generateTopNResults`.
    fn fetch_and_select(&mut self) -> Result<(), ExecError> {
        // Go's `AttachChild` turns a zero-count TopN into a dual, so this
        // operator is never asked for zero rows in Go. Answering it without
        // touching the child keeps the heap's `rowPtrs[0]` in bounds.
        if self.total_limit == 0 {
            return Ok(());
        }

        // Phase 1: fill the store to `totalLimit` rows.
        while (self.stored_len() as u64) < self.total_limit {
            let mut chunk = self.child.new_chunk();
            self.child.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                break;
            }
            self.add_chunk(chunk)?;
            self.account()?;
        }
        self.row_ptrs = (0..self.chunks.len())
            .flat_map(|c| (0..self.chunks[c].num_rows()).map(move |r| (c, r)))
            .collect();
        self.account()?;

        // Phase 2: heapify, trim to `totalLimit`, then stream the rest of the
        // child through the heap.
        self.heap_init();
        while self.row_ptrs.len() as u64 > self.total_limit {
            self.heap_pop();
        }
        self.take_cmp_err()?;

        let mut child_chunk = self.child.new_chunk();
        loop {
            self.child.next(&mut child_chunk)?;
            if child_chunk.num_rows() == 0 {
                break;
            }
            // Go `processChk`: one row at a time against the heap's max.
            for r in 0..child_chunk.num_rows() {
                let row = child_chunk.get_row(r);
                let new_key = self.eval_key(row)?;
                let max_key = self.key_at(self.row_ptrs[0]);
                if self.greater_keys(&max_key, &new_key) {
                    // Go `update`: the evicted max's slot takes the new row.
                    let ptr = self.append_row(child_chunk.get_row(r), new_key);
                    self.row_ptrs[0] = ptr;
                    self.heap_fix_root();
                }
            }
            self.take_cmp_err()?;
            if self.stored_len() > self.row_ptrs.len() * TOP_N_COMPACTION_FACTOR {
                self.do_compaction();
            }
            self.account()?;
        }

        // Phase 3: ascending order over the survivors, then emit from
        // `offset` (Go seeds `chkHeap.idx` with it).
        let mut ptrs = std::mem::take(&mut self.row_ptrs);
        let mut order: Vec<usize> = (0..ptrs.len()).collect();
        // `sort_by` cannot borrow `self` mutably, so the keys are lifted out
        // first and the comparison error is captured as in `SortExec`.
        let keys: Vec<Vec<Datum>> = ptrs.iter().map(|&ptr| self.key_at(ptr)).collect();
        let by_items = &self.by_items;
        let mut sort_err: Option<ExecError> = None;
        order.sort_by(|&a, &b| {
            for (i, item) in by_items.iter().enumerate() {
                let mut cmp = match tidb_expr::compare_datums_with_collation(
                    &keys[a][i],
                    &keys[b][i],
                    tidb_expr::collation_derive::collation_of_node(&item.expr),
                ) {
                    Ok(cmp) => cmp,
                    Err(err) => {
                        if sort_err.is_none() {
                            sort_err = Some(err.into());
                        }
                        return Ordering::Equal;
                    }
                };
                if item.desc {
                    cmp = cmp.reverse();
                }
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });
        if let Some(err) = sort_err {
            return Err(err);
        }
        ptrs = order.into_iter().map(|i| ptrs[i]).collect();
        self.row_ptrs = ptrs;
        self.idx = usize::try_from(self.offset).unwrap_or(usize::MAX);
        Ok(())
    }

    /// Surfaces the first comparison error a sift or sort captured.
    fn take_cmp_err(&mut self) -> Result<(), ExecError> {
        match self.cmp_err.take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl<C: Columns> Executor for TopNExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.fetched = false;
        self.chunks.clear();
        self.keys.clear();
        self.row_ptrs.clear();
        self.idx = 0;
        self.cmp_err = None;
        // Go `TopNExec.Open`: an operator re-opened by an Apply's inner side
        // must not keep charging for the rows it just dropped.
        self.tracker.replace_bytes_used(0);
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !self.fetched {
            self.fetch_and_select()?;
            self.fetched = true;
        }
        let remaining = self.row_ptrs.len().saturating_sub(self.idx);
        let batch = self.meta.max_chunk_size().min(remaining);
        for _ in 0..batch {
            let (c, r) = self.row_ptrs[self.idx];
            req.append_row(self.chunks[c].get_row(r));
            self.idx += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.chunks.clear();
        self.keys.clear();
        self.row_ptrs.clear();
        self.tracker.replace_bytes_used(0);
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limit::LimitExec;
    use crate::mem_quota::OomAction;
    use crate::sort::SortExec;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;
    use tidb_expr::expression::Expression;
    use tidb_expr::NoColumns;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// A test-only source that emits its rows `batch` at a time, so the
    /// operator's phase-2 loop (Go `processChk`) actually runs -- a one-chunk
    /// source would let phase 1 swallow everything and leave the heap untested.
    struct ChunkedSource {
        meta: ExecutorMeta,
        rows: Vec<Vec<Option<i64>>>,
        cursor: usize,
        batch: usize,
    }

    impl Executor for ChunkedSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.cursor = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let end = (self.cursor + self.batch).min(self.rows.len());
            for row in &self.rows[self.cursor..end] {
                for (c, v) in row.iter().enumerate() {
                    match v {
                        Some(v) => req.append_int64(c, *v),
                        None => req.append_null(c),
                    }
                }
            }
            self.cursor = end;
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn schema(&self) -> &Schema {
            self.meta.schema()
        }
        fn ret_field_types(&self) -> &[FieldType] {
            self.meta.ret_field_types()
        }
        fn init_cap(&self) -> usize {
            self.meta.init_cap()
        }
        fn max_chunk_size(&self) -> usize {
            self.meta.max_chunk_size()
        }
        fn new_chunk(&self) -> Chunk {
            self.meta.new_chunk()
        }
    }

    fn schema_of(n_cols: usize) -> Schema {
        let cols = (0..n_cols)
            .map(|i| {
                let mut c = Column::new(i as i64 + 1, long());
                c.index = i as i64;
                c
            })
            .collect();
        Schema::new(cols)
    }

    fn col_expr(idx: usize) -> Expression {
        let mut c = Column::new(idx as i64 + 1, long());
        c.index = idx as i64;
        Expression::Column(c)
    }

    fn source(rows: &[Vec<Option<i64>>], n_cols: usize, batch: usize) -> Box<dyn Executor> {
        Box::new(ChunkedSource {
            meta: ExecutorMeta::new(schema_of(n_cols), 0, 4, 32),
            rows: rows.to_vec(),
            cursor: 0,
            batch,
        })
    }

    fn by(items: &[(usize, bool)]) -> Vec<SortByItem> {
        items
            .iter()
            .map(|&(idx, desc)| SortByItem {
                expr: col_expr(idx),
                desc,
            })
            .collect()
    }

    fn topn_over(
        rows: &[Vec<Option<i64>>],
        n_cols: usize,
        batch: usize,
        items: &[(usize, bool)],
        offset: u64,
        count: u64,
        memory: StatementMemory,
    ) -> TopNExec<NoColumns> {
        TopNExec::new(
            ExecutorMeta::new(schema_of(n_cols), 1, 4, 32),
            by(items),
            source(rows, n_cols, batch),
            NoColumns,
            offset,
            count,
            memory,
        )
    }

    /// The INDEPENDENT oracle: the `Sort` + `Limit` pair this operator
    /// replaces, built from the same rows. It is a separately written
    /// implementation (`sort.rs` materializes and sorts everything), so
    /// agreeing with it is real evidence and not a round trip.
    fn sort_then_limit(
        rows: &[Vec<Option<i64>>],
        n_cols: usize,
        batch: usize,
        items: &[(usize, bool)],
        offset: u64,
        count: u64,
    ) -> LimitExec {
        let sort = SortExec::new(
            ExecutorMeta::new(schema_of(n_cols), 1, 4, 32),
            by(items),
            source(rows, n_cols, batch),
            NoColumns,
            StatementMemory::default(),
        );
        LimitExec::new(
            ExecutorMeta::new(schema_of(n_cols), 2, 4, 32),
            offset,
            count,
            Box::new(sort),
        )
    }

    fn drain(exec: &mut dyn Executor) -> Vec<Vec<Option<i64>>> {
        exec.open().unwrap();
        let n_cols = exec.ret_field_types().len();
        let mut out = Vec::new();
        let mut req = exec.new_chunk();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                let row = req.get_row(r);
                out.push(
                    (0..n_cols)
                        .map(|c| {
                            if row.is_null(c) {
                                None
                            } else {
                                Some(row.get_int64(c))
                            }
                        })
                        .collect(),
                );
            }
        }
        exec.close().unwrap();
        out
    }

    /// A deterministic pseudo-random generator, so a failure is reproducible
    /// from the seed alone.
    fn lcg(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state >> 33
    }

    #[test]
    fn topn_returns_what_sort_then_limit_returns() {
        // Keys are drawn from a small domain so ties are the common case, not
        // the exception -- ties are exactly where a bounded heap can diverge
        // from a full sort.
        let mut seed = 0x5eed_1234_u64;
        for n_rows in [0_usize, 1, 3, 7, 40, 100] {
            for &(offset, count) in &[(0, 1), (0, 3), (0, 100), (2, 3), (5, 2), (0, 0), (7, 0)] {
                for items in [
                    vec![(0_usize, false)],
                    vec![(0, true)],
                    vec![(0, false), (1, true)],
                ] {
                    let rows: Vec<Vec<Option<i64>>> = (0..n_rows)
                        .map(|_| {
                            vec![
                                match lcg(&mut seed) % 7 {
                                    0 => None,
                                    v => Some(v as i64 % 5),
                                },
                                Some((lcg(&mut seed) % 5) as i64),
                            ]
                        })
                        .collect();
                    for batch in [1_usize, 3, 1000] {
                        let mut top = topn_over(
                            &rows,
                            2,
                            batch,
                            &items,
                            offset,
                            count,
                            StatementMemory::default(),
                        );
                        let got = drain(&mut top);
                        let mut oracle = sort_then_limit(&rows, 2, batch, &items, offset, count);
                        let want = drain(&mut oracle);
                        // Only the KEY columns are guaranteed to agree: among
                        // tying rows either implementation may keep a
                        // different one, which is an order MySQL does not
                        // define either.
                        let key_of = |out: &Vec<Vec<Option<i64>>>| -> Vec<Vec<Option<i64>>> {
                            out.iter()
                                .map(|row| items.iter().map(|&(i, _)| row[i]).collect())
                                .collect()
                        };
                        assert_eq!(
                            key_of(&got),
                            key_of(&want),
                            "n_rows={n_rows} offset={offset} count={count} \
                             items={items:?} batch={batch}\nrows={rows:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn nulls_order_below_every_value_and_desc_negates_that() {
        let rows = vec![
            vec![Some(2), Some(0)],
            vec![None, Some(0)],
            vec![Some(1), Some(0)],
        ];
        let mut asc = topn_over(&rows, 2, 1, &[(0, false)], 0, 2, StatementMemory::default());
        assert_eq!(
            drain(&mut asc)
                .into_iter()
                .map(|r| r[0])
                .collect::<Vec<_>>(),
            vec![None, Some(1)]
        );
        let mut desc = topn_over(&rows, 2, 1, &[(0, true)], 0, 2, StatementMemory::default());
        assert_eq!(
            drain(&mut desc)
                .into_iter()
                .map(|r| r[0])
                .collect::<Vec<_>>(),
            vec![Some(2), Some(1)]
        );
    }

    #[test]
    fn an_offset_past_the_end_returns_nothing() {
        let rows: Vec<Vec<Option<i64>>> = (0..5).map(|v| vec![Some(v), Some(0)]).collect();
        let mut exec = topn_over(
            &rows,
            2,
            2,
            &[(0, false)],
            10,
            3,
            StatementMemory::default(),
        );
        assert!(drain(&mut exec).is_empty());
    }

    /// A zero count never reaches the child: Go's `LogicalTopN.AttachChild`
    /// replaces the whole operator with a dual, so the heap's `rowPtrs[0]` is
    /// never indexed on an empty store.
    #[test]
    fn a_zero_count_returns_nothing_without_draining_the_child() {
        let rows: Vec<Vec<Option<i64>>> = (0..5).map(|v| vec![Some(v), Some(0)]).collect();
        let mut exec = topn_over(&rows, 2, 2, &[(0, false)], 0, 0, StatementMemory::default());
        assert!(drain(&mut exec).is_empty());
    }

    /// The point of the operator: a `TopN` over many rows holds `offset +
    /// count` of them, so it completes under a quota the equivalent `Sort`
    /// cannot survive.
    #[test]
    fn topn_stays_within_a_quota_the_equivalent_sort_breaches() {
        let rows: Vec<Vec<Option<i64>>> = (0..4096).rev().map(|v| vec![Some(v), Some(0)]).collect();
        // Big enough for several child chunks -- so the bound being tested
        // is the SIZE OF THE STORE, not the size of one incoming chunk, which
        // both operators must be able to hold.
        let quota = 200_000;

        // Spilling OFF for the sort: the contrast this test draws is between
        // an operator that must HOLD every row and one that holds only `n`.
        // A spilling sort survives the same quota by writing rows out, which
        // is a different (and now covered) behavior -- see
        // `sort::tests::a_sort_over_the_quota_spills_to_disk_and_returns_every_row`.
        // TopN spill itself is not ported.
        let sort_memory =
            StatementMemory::new(quota, OomAction::Cancel, 42).with_tmp_storage_on_oom(false);
        let mut sort = SortExec::new(
            ExecutorMeta::new(schema_of(2), 1, 4, 32),
            by(&[(0, false)]),
            source(&rows, 2, 32),
            NoColumns,
            sort_memory,
        );
        sort.open().unwrap();
        let mut req = sort.new_chunk();
        assert!(
            matches!(
                sort.next(&mut req),
                Err(ExecError::MemoryExceedForQuery { .. })
            ),
            "the sort was expected to breach the quota this TopN survives"
        );

        let memory = StatementMemory::new(quota, OomAction::Cancel, 42);
        let mut exec = topn_over(&rows, 2, 32, &[(0, false)], 0, 3, memory.clone());
        let out = drain(&mut exec);
        assert_eq!(
            out.into_iter().map(|r| r[0]).collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(2)]
        );
        // Go's `Close` releases the store; a session must not leak its quota
        // statement by statement.
        assert_eq!(memory.bytes_consumed(), 0);
    }

    /// Compaction is what bounds the store while the heap keeps evicting: a
    /// descending input with an ascending TopN evicts on nearly every row.
    #[test]
    fn compaction_bounds_the_store_on_a_worst_case_input() {
        let rows: Vec<Vec<Option<i64>>> = (0..4096).rev().map(|v| vec![Some(v), Some(0)]).collect();
        let memory = StatementMemory::default();
        let mut exec = topn_over(&rows, 2, 32, &[(0, false)], 0, 3, memory.clone());
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        exec.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 3);
        // Without `doCompaction` this store would hold all 4096 rows.
        assert!(
            exec.stored_len() <= 3 * TOP_N_COMPACTION_FACTOR,
            "store held {} rows for a count of 3",
            exec.stored_len()
        );
    }

    /// Re-opening (an Apply's inner side runs the operator once per outer row)
    /// must start from an empty store, not from the last run's rows.
    #[test]
    fn reopening_starts_from_an_empty_store() {
        let rows: Vec<Vec<Option<i64>>> = (0..10).map(|v| vec![Some(v), Some(0)]).collect();
        let mut exec = topn_over(&rows, 2, 3, &[(0, false)], 0, 2, StatementMemory::default());
        let first = drain(&mut exec);
        let second = drain(&mut exec);
        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
    }

    /// Which rows a bounded max-heap RETAINS is not implied by the ordering:
    /// among tying keys it is decided by arrival order and by the heap's sift
    /// rules. This fixture is the answer Go gives -- produced by replaying
    /// `pkg/executor/sortexec`'s `topNChunkHeap` through Go's own
    /// `container/heap` over the same generated rows (see the probe recorded
    /// in the commit message) -- so agreeing with it is evidence that the
    /// ported `heap_down`/`heap_up` are Go's and not merely "a heap".
    ///
    /// Each case is `(row count, child batch size, offset + count, desc,
    /// surviving input row indexes ascending)`. The port is driven with
    /// `offset = 0` and `count = offset + count` because the retained SET
    /// depends only on that total, and with a zero offset every retained row
    /// is emitted and can be checked.
    const GO_HEAP_SURVIVORS: &[(usize, usize, u64, bool, &[i64])] = &[
        (1, 1, 1, false, &[0]),
        (1, 1, 1, true, &[0]),
        (1, 1, 3, false, &[0]),
        (1, 1, 3, true, &[0]),
        (1, 1, 5, false, &[0]),
        (1, 1, 5, true, &[0]),
        (1, 1, 7, false, &[0]),
        (1, 1, 7, true, &[0]),
        (1, 3, 1, false, &[0]),
        (1, 3, 1, true, &[0]),
        (1, 3, 3, false, &[0]),
        (1, 3, 3, true, &[0]),
        (1, 3, 5, false, &[0]),
        (1, 3, 5, true, &[0]),
        (1, 3, 7, false, &[0]),
        (1, 3, 7, true, &[0]),
        (1, 32, 1, false, &[0]),
        (1, 32, 1, true, &[0]),
        (1, 32, 3, false, &[0]),
        (1, 32, 3, true, &[0]),
        (1, 32, 5, false, &[0]),
        (1, 32, 5, true, &[0]),
        (1, 32, 7, false, &[0]),
        (1, 32, 7, true, &[0]),
        (3, 1, 1, false, &[0]),
        (3, 1, 1, true, &[1]),
        (3, 1, 3, false, &[0, 1, 2]),
        (3, 1, 3, true, &[0, 1, 2]),
        (3, 1, 5, false, &[0, 1, 2]),
        (3, 1, 5, true, &[0, 1, 2]),
        (3, 1, 7, false, &[0, 1, 2]),
        (3, 1, 7, true, &[0, 1, 2]),
        (3, 3, 1, false, &[0]),
        (3, 3, 1, true, &[1]),
        (3, 3, 3, false, &[0, 1, 2]),
        (3, 3, 3, true, &[0, 1, 2]),
        (3, 3, 5, false, &[0, 1, 2]),
        (3, 3, 5, true, &[0, 1, 2]),
        (3, 3, 7, false, &[0, 1, 2]),
        (3, 3, 7, true, &[0, 1, 2]),
        (3, 32, 1, false, &[0]),
        (3, 32, 1, true, &[1]),
        (3, 32, 3, false, &[0, 1, 2]),
        (3, 32, 3, true, &[0, 1, 2]),
        (3, 32, 5, false, &[0, 1, 2]),
        (3, 32, 5, true, &[0, 1, 2]),
        (3, 32, 7, false, &[0, 1, 2]),
        (3, 32, 7, true, &[0, 1, 2]),
        (7, 1, 1, false, &[1]),
        (7, 1, 1, true, &[2]),
        (7, 1, 3, false, &[1, 4, 5]),
        (7, 1, 3, true, &[0, 2, 3]),
        (7, 1, 5, false, &[0, 1, 4, 5, 6]),
        (7, 1, 5, true, &[0, 2, 3, 5, 6]),
        (7, 1, 7, false, &[0, 1, 2, 3, 4, 5, 6]),
        (7, 1, 7, true, &[0, 1, 2, 3, 4, 5, 6]),
        (7, 3, 1, false, &[1]),
        (7, 3, 1, true, &[2]),
        (7, 3, 3, false, &[1, 4, 5]),
        (7, 3, 3, true, &[0, 2, 3]),
        (7, 3, 5, false, &[0, 1, 4, 5, 6]),
        (7, 3, 5, true, &[0, 2, 3, 5, 6]),
        (7, 3, 7, false, &[0, 1, 2, 3, 4, 5, 6]),
        (7, 3, 7, true, &[0, 1, 2, 3, 4, 5, 6]),
        (7, 32, 1, false, &[1]),
        (7, 32, 1, true, &[2]),
        (7, 32, 3, false, &[1, 4, 5]),
        (7, 32, 3, true, &[0, 2, 3]),
        (7, 32, 5, false, &[0, 1, 4, 5, 6]),
        (7, 32, 5, true, &[0, 2, 3, 5, 6]),
        (7, 32, 7, false, &[0, 1, 2, 3, 4, 5, 6]),
        (7, 32, 7, true, &[0, 1, 2, 3, 4, 5, 6]),
        (40, 1, 1, false, &[8]),
        (40, 1, 1, true, &[2]),
        (40, 1, 3, false, &[8, 9, 12]),
        (40, 1, 3, true, &[2, 7, 19]),
        (40, 1, 5, false, &[8, 9, 12, 27, 28]),
        (40, 1, 5, true, &[2, 7, 19, 20, 22]),
        (40, 1, 7, false, &[8, 9, 12, 27, 28, 29, 30]),
        (40, 1, 7, true, &[2, 7, 19, 20, 22, 26, 38]),
        (40, 3, 1, false, &[8]),
        (40, 3, 1, true, &[2]),
        (40, 3, 3, false, &[8, 9, 12]),
        (40, 3, 3, true, &[2, 7, 19]),
        (40, 3, 5, false, &[8, 9, 12, 27, 28]),
        (40, 3, 5, true, &[2, 7, 19, 20, 22]),
        (40, 3, 7, false, &[8, 9, 12, 27, 28, 29, 30]),
        (40, 3, 7, true, &[2, 7, 19, 20, 22, 26, 38]),
        (40, 32, 1, false, &[12]),
        (40, 32, 1, true, &[19]),
        (40, 32, 3, false, &[12, 29, 30]),
        (40, 32, 3, true, &[2, 19, 20]),
        (40, 32, 5, false, &[9, 12, 28, 29, 30]),
        (40, 32, 5, true, &[2, 7, 19, 20, 22]),
        (40, 32, 7, false, &[8, 9, 12, 27, 28, 29, 30]),
        (40, 32, 7, true, &[2, 7, 19, 20, 22, 26, 38]),
        (100, 1, 1, false, &[2]),
        (100, 1, 1, true, &[8]),
        (100, 1, 3, false, &[2, 12, 28]),
        (100, 1, 3, true, &[8, 19, 25]),
        (100, 1, 5, false, &[2, 12, 28, 33, 34]),
        (100, 1, 5, true, &[8, 19, 25, 66, 73]),
        (100, 1, 7, false, &[2, 12, 28, 33, 34, 37, 46]),
        (100, 1, 7, true, &[8, 19, 25, 66, 73, 96, 98]),
        (100, 3, 1, false, &[2]),
        (100, 3, 1, true, &[8]),
        (100, 3, 3, false, &[2, 12, 28]),
        (100, 3, 3, true, &[8, 19, 25]),
        (100, 3, 5, false, &[2, 12, 28, 33, 34]),
        (100, 3, 5, true, &[8, 19, 25, 66, 73]),
        (100, 3, 7, false, &[2, 12, 28, 33, 34, 37, 46]),
        (100, 3, 7, true, &[8, 19, 25, 66, 73, 96, 98]),
        (100, 32, 1, false, &[2]),
        (100, 32, 1, true, &[19]),
        (100, 32, 3, false, &[2, 12, 28]),
        (100, 32, 3, true, &[8, 19, 25]),
        (100, 32, 5, false, &[2, 12, 28, 33, 34]),
        (100, 32, 5, true, &[8, 19, 25, 66, 73]),
        (100, 32, 7, false, &[2, 12, 28, 33, 34, 37, 46]),
        (100, 32, 7, true, &[8, 19, 25, 66, 73, 96, 98]),
    ];

    #[test]
    fn the_heap_retains_the_rows_gos_heap_retains() {
        // The Go probe drew every row set from ONE running generator, in this
        // order, so the row sets are rebuilt the same way rather than
        // re-seeded per case.
        let mut seed = 0xabcd_1234_u64;
        let mut inputs: Vec<(usize, Vec<Vec<Option<i64>>>)> = Vec::new();
        for n_rows in [1_usize, 3, 7, 40, 100] {
            let rows: Vec<Vec<Option<i64>>> = (0..n_rows)
                .map(|i| {
                    let v = lcg(&mut seed) % 7;
                    let key = if v == 0 { None } else { Some((v % 5) as i64) };
                    // Column 1 is the row's input index, so the emitted rows
                    // name exactly which inputs survived.
                    vec![key, Some(i as i64)]
                })
                .collect();
            inputs.push((n_rows, rows));
        }
        for &(n_rows, batch, total, desc, want) in GO_HEAP_SURVIVORS {
            let rows = &inputs
                .iter()
                .find(|(n, _)| *n == n_rows)
                .expect("every fixture row count is generated above")
                .1;
            let mut exec = topn_over(
                rows,
                2,
                batch,
                &[(0, desc)],
                0,
                total,
                StatementMemory::default(),
            );
            let mut got: Vec<i64> = drain(&mut exec)
                .into_iter()
                .map(|r| r[1].expect("the payload column is never NULL"))
                .collect();
            got.sort_unstable();
            assert_eq!(
                got, want,
                "n_rows={n_rows} batch={batch} total={total} desc={desc}"
            );
        }
    }
}
