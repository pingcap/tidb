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

//! `pkg/executor/sortexec` `SortExec`: the `ORDER BY` operator.
//!
//! Serial in-memory semantics of Go's unparallel single-partition path: the
//! first `Next` drains the child, materializes every row, evaluates the
//! by-item keys once per row (Go builds `keyColumns`/`keyCmpFuncs` the same
//! way), sorts, then emits the rows in order chunk by chunk.
//!
//! Null ordering matches Go `chunk.cmpNull`: NULL compares below every
//! non-NULL value, and a descending by-item negates the whole comparison --
//! so NULLs come first ascending and last descending.
//!
//! DIVERGENCE (documented): Go's in-memory partition sorts with `sort.Slice`
//! (`sort_partition.go`, unstable); this port uses Rust's stable `sort_by`,
//! so only the order of exactly-tying rows can differ -- an order Go does not
//! guarantee either.
//!
//! DEFERRED (documented): spill-to-disk partitions and the multi-way merger,
//! the parallel sort workers/fetcher/generator pipeline, memory and disk
//! trackers, the SQL killer, and the failpoints.
//!
//! Row comparison is `tidb_expr::compare_datums` — the shared,
//! collation-aware datum comparator (Go `types/datum.go` `Datum.Compare`
//! via `pkg/util/chunk/compare.go` `GetCompareFunc`). A comparison error
//! (an unorderable key kind) is captured during the sort and returned from
//! `Next`, as Go returns it from `Next`.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_util::memory::Tracker;

use crate::mem_quota::StatementMemory;

/// Go `planner/util.ByItems`: one `ORDER BY` item -- the key expression and
/// its direction.
pub struct SortByItem {
    /// Go `ByItems.Expr`.
    pub expr: Expression,
    /// Go `ByItems.Desc`.
    pub desc: bool,
}

/// Go `SortExec` (unparallel, single in-memory partition).
pub struct SortExec<C: Columns> {
    meta: ExecutorMeta,
    /// Go `ByItems`.
    by_items: Vec<SortByItem>,
    child: Box<dyn Executor>,
    ctx: C,
    /// Go `fetched`: whether the child has been drained and sorted.
    fetched: bool,
    /// The materialized child chunks (Go keeps rows in the sort partition's
    /// row container).
    child_chunks: Vec<Chunk>,
    /// Sorted row locations: `(chunk index, row index)` per output row (Go
    /// sorts the row pointers themselves).
    order: Vec<(usize, usize)>,
    /// Go `Unparallel.Idx` in spirit: how many sorted rows were emitted.
    cursor: usize,
    /// The statement's memory budget, which this operator's tracker hangs off
    /// and whose quota it checks after each `Consume`.
    memory: StatementMemory,
    /// Go `SortExec.memTracker` = `memory.NewTracker(e.ID(), -1)` attached to
    /// `StmtCtx.MemTracker`: this operator's own node in the tracker tree, so
    /// `SHOW`-style tree dumps attribute the bytes to the sort.
    tracker: Arc<Tracker>,
}

impl<C: Columns> SortExec<C> {
    /// Builds a sort of `child`'s rows by `by_items`, evaluated with `ctx`.
    /// `memory` is the statement's budget (Go: the `StmtCtx.MemTracker` the
    /// operator attaches to). It is a required argument rather than an
    /// optional one so a new call site cannot produce an UNACCOUNTED sort by
    /// omitting it.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        by_items: Vec<SortByItem>,
        child: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Self {
        let tracker = memory.operator_tracker(meta.id());
        SortExec {
            meta,
            by_items,
            child,
            ctx,
            fetched: false,
            child_chunks: Vec::new(),
            order: Vec::new(),
            cursor: 0,
            memory,
            tracker,
        }
    }

    /// Go `fetchChunks` + the single-partition sort: drains the child,
    /// evaluates each by-item key once per row, and stably sorts the row
    /// locations.
    fn fetch_and_sort(&mut self) -> Result<(), ExecError> {
        // Drain the child into materialized chunks, accounting each one as Go
        // `sortPartition.add` does -- `chunk.RowSize*rowNum + chk.MemoryUsage()`
        // -- and checking the quota right after, which is where Go's `Consume`
        // fires the OOM action. Accounting INSIDE the loop is what makes a
        // query over a large table stop early instead of first materializing
        // everything and only then noticing.
        loop {
            let mut chunk = self.child.new_chunk();
            self.child.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                break;
            }
            let rows = i64::try_from(chunk.num_rows()).unwrap_or(i64::MAX);
            self.tracker
                .consume(chunk.memory_usage() + tidb_chunk::row::ROW_SIZE * rows);
            self.memory.check()?;
            self.child_chunks.push(chunk);
        }

        // Evaluate the sort keys once per row (Go's keyColumns equivalent).
        let mut keys: Vec<Vec<Datum>> = Vec::new();
        self.order.clear();
        for (ci, chunk) in self.child_chunks.iter().enumerate() {
            for ri in 0..chunk.num_rows() {
                let row = chunk.get_row(ri);
                let mut key = Vec::with_capacity(self.by_items.len());
                let mut key_bytes = i64::try_from(size_of::<Vec<Datum>>()).unwrap_or(i64::MAX);
                for item in &self.by_items {
                    let datum = item.expr.eval(&self.ctx, row)?;
                    key_bytes += i64::try_from(datum.estimated_mem_usage()).unwrap_or(i64::MAX);
                    key.push(datum);
                }
                // OVER-COUNT vs Go, deliberately: Go's sort keeps no
                // materialized key at all (`keyCmpFuncs` re-reads the chunk
                // cell on every comparison), so `keys` is memory THIS port
                // holds and Go does not. The tracker reports what this process
                // actually took, because a tracker that matched Go's number
                // while the process held more would fail to protect. It makes
                // a sort here cross a given quota sooner than Go's does.
                self.tracker.consume(key_bytes);
                self.memory.check()?;
                keys.push(key);
                self.order.push((ci, ri));
            }
        }

        // Go `lessRow`: first non-equal key decides; `Desc` negates it.
        // Stable sort where Go's `sort.Slice` is unstable (see module doc).
        // `sort_by` cannot return an error, so the first comparison error is
        // captured and the whole sort fails afterwards -- Go's `keyCmpFuncs`
        // reject unorderable types up front, so an error here likewise means
        // the sort's output must not be used, and `Next` returns it.
        let by_items = &self.by_items;
        let mut sort_err: Option<ExecError> = None;
        let mut indices: Vec<usize> = (0..self.order.len()).collect();
        indices.sort_by(|&a, &b| {
            for (i, item) in by_items.iter().enumerate() {
                // Each key compares under ITS OWN derived collation (Go builds
                // `keyCmpFuncs` from the by-item's `RetType`): `ORDER BY
                // ci_col` orders `a, A, b, B`, not the byte order `A, B, a, b`.
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
        self.order = indices.iter().map(|&i| self.order[i]).collect();
        Ok(())
    }
}

impl<C: Columns> Executor for SortExec<C> {
    /// Go `Open`: resets the fetched state and opens the child (the parallel
    /// machinery is deferred).
    fn open(&mut self) -> Result<(), ExecError> {
        self.fetched = false;
        self.child_chunks.clear();
        self.order.clear();
        self.cursor = 0;
        // Go `SortExec.Open`: `e.memTracker.ReplaceBytesUsed(0)` -- a re-opened
        // sort (an Apply's inner side re-runs per outer row) must not keep
        // charging for rows it has just dropped.
        self.tracker.replace_bytes_used(0);
        self.child.open()
    }

    /// Go `Next`: first call drains and sorts; every call then appends sorted
    /// rows until the chunk-size bound (Go `req.IsFull()`) or exhaustion.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !self.fetched {
            self.fetch_and_sort()?;
            self.fetched = true;
        }
        let batch = self
            .meta
            .max_chunk_size()
            .min(self.order.len() - self.cursor);
        for _ in 0..batch {
            let (ci, ri) = self.order[self.cursor];
            req.append_row(self.child_chunks[ci].get_row(ri));
            self.cursor += 1;
        }
        Ok(())
    }

    /// Go `Close` (minus the spill/parallel teardown, deferred): releases the
    /// materialized rows, gives their bytes back to the statement's budget,
    /// and closes the child.
    fn close(&mut self) -> Result<(), ExecError> {
        self.child_chunks.clear();
        self.order.clear();
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
    use crate::mem_quota::OomAction;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::NoColumns;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// A test-only source that emits one prebuilt chunk, then EOF (same
    /// helper pattern as the limit/selection tests).
    struct OneChunkSource {
        meta: ExecutorMeta,
        data: Option<Chunk>,
    }

    impl Executor for OneChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(data) = self.data.take() {
                for r in 0..data.num_rows() {
                    req.append_row(data.get_row(r));
                }
            }
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

    /// Builds a sort over one chunk whose rows are given per column as
    /// `Option<i64>` (None = NULL).
    fn sort_over(rows: &[Vec<Option<i64>>], by: Vec<SortByItem>) -> SortExec<NoColumns> {
        let n_cols = rows.first().map_or(1, Vec::len);
        let fields: Vec<FieldType> = (0..n_cols).map(|_| long()).collect();
        let mut data = Chunk::new_with_capacity(&fields, rows.len().max(1));
        for row in rows {
            for (c, v) in row.iter().enumerate() {
                match v {
                    Some(v) => data.append_int64(c, *v),
                    None => data.append_null(c),
                }
            }
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(schema_of(n_cols), 0, 4, 1024),
            data: Some(data),
        };
        SortExec::new(
            ExecutorMeta::new(schema_of(n_cols), 1, 4, 1024),
            by,
            Box::new(source),
            NoColumns,
            StatementMemory::default(),
        )
    }

    /// Same as [`sorter`] but with a caller-chosen budget, so a test can pick
    /// a quota the sort must cross.
    fn sorter_with_memory(
        n_cols: usize,
        rows: &[Vec<Option<i64>>],
        by: Vec<SortByItem>,
        memory: StatementMemory,
    ) -> SortExec<NoColumns> {
        let fields: Vec<FieldType> = (0..n_cols).map(|_| long()).collect();
        let mut data = Chunk::new_with_capacity(&fields, rows.len().max(1));
        for row in rows {
            for (c, v) in row.iter().enumerate() {
                match v {
                    Some(v) => data.append_int64(c, *v),
                    None => data.append_null(c),
                }
            }
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(schema_of(n_cols), 0, 4, 1024),
            data: Some(data),
        };
        SortExec::new(
            ExecutorMeta::new(schema_of(n_cols), 1, 4, 1024),
            by,
            Box::new(source),
            NoColumns,
            memory,
        )
    }

    fn one_col_rows(n: i64) -> Vec<Vec<Option<i64>>> {
        (0..n).rev().map(|v| vec![Some(v)]).collect()
    }

    #[test]
    fn a_sort_accounts_its_materialized_rows_against_the_statement() {
        let memory = StatementMemory::default();
        let mut exec = sorter_with_memory(
            1,
            &one_col_rows(64),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
            memory.clone(),
        );
        assert_eq!(memory.bytes_consumed(), 0, "nothing before the fetch");
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        exec.next(&mut req).unwrap();
        let held = memory.bytes_consumed();
        // At least the retained chunk bytes plus one row cursor per row; the
        // exact total also carries the materialized keys.
        assert!(
            held > tidb_chunk::row::ROW_SIZE * 64,
            "accounted only {held} bytes for 64 retained rows"
        );
        // Go `Close` releases the partition: the statement's budget must come
        // back down, or a session would leak its quota statement by statement.
        exec.close().unwrap();
        assert_eq!(memory.bytes_consumed(), 0);
    }

    #[test]
    fn crossing_the_quota_fails_the_sort_with_8175_under_cancel() {
        // A quota far below what 4096 retained rows need.
        let memory = StatementMemory::new(2048, OomAction::Cancel, 42);
        let mut exec = sorter_with_memory(
            1,
            &one_col_rows(4096),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
            memory.clone(),
        );
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        match exec.next(&mut req) {
            Err(ExecError::MemoryExceedForQuery { conn_id }) => assert_eq!(conn_id, 42),
            other => panic!("expected the quota to be enforced, got {other:?}"),
        }
    }

    #[test]
    fn the_same_sort_completes_under_log_however_far_it_overruns() {
        let memory = StatementMemory::new(2048, OomAction::Log, 42);
        let mut exec = sorter_with_memory(
            1,
            &one_col_rows(4096),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
            memory.clone(),
        );
        let out = collect(&mut exec);
        assert_eq!(out.len(), 4096);
        assert_eq!(out[0], vec![Some(0)]);
        assert_eq!(out[4095], vec![Some(4095)]);
    }

    fn collect(exec: &mut SortExec<NoColumns>) -> Vec<Vec<Option<i64>>> {
        exec.open().unwrap();
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
                    (0..exec.ret_field_types().len())
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

    fn rows1(vals: &[Option<i64>]) -> Vec<Vec<Option<i64>>> {
        vals.iter().map(|v| vec![*v]).collect()
    }

    #[test]
    fn ascending_int_sort() {
        let mut e = sort_over(
            &rows1(&[Some(3), Some(1), Some(2)]),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
        );
        assert_eq!(collect(&mut e), rows1(&[Some(1), Some(2), Some(3)]));
    }

    #[test]
    fn descending_int_sort() {
        let mut e = sort_over(
            &rows1(&[Some(3), Some(1), Some(2)]),
            vec![SortByItem {
                expr: col_expr(0),
                desc: true,
            }],
        );
        assert_eq!(collect(&mut e), rows1(&[Some(3), Some(2), Some(1)]));
    }

    #[test]
    fn multi_key_ties_broken_by_second_key() {
        // (col0 asc, col1 desc): col0 ties resolved by larger col1 first.
        let mut e = sort_over(
            &[
                vec![Some(2), Some(1)],
                vec![Some(1), Some(5)],
                vec![Some(2), Some(9)],
                vec![Some(1), Some(7)],
            ],
            vec![
                SortByItem {
                    expr: col_expr(0),
                    desc: false,
                },
                SortByItem {
                    expr: col_expr(1),
                    desc: true,
                },
            ],
        );
        assert_eq!(
            collect(&mut e),
            vec![
                vec![Some(1), Some(7)],
                vec![Some(1), Some(5)],
                vec![Some(2), Some(9)],
                vec![Some(2), Some(1)],
            ]
        );
    }

    #[test]
    fn nulls_first_ascending() {
        // Go chunk.cmpNull: NULL is below every value.
        let mut e = sort_over(
            &rows1(&[Some(2), None, Some(1)]),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
        );
        assert_eq!(collect(&mut e), rows1(&[None, Some(1), Some(2)]));
    }

    #[test]
    fn nulls_last_descending() {
        // Desc negates the whole comparison, so NULLs move to the end.
        let mut e = sort_over(
            &rows1(&[Some(2), None, Some(1)]),
            vec![SortByItem {
                expr: col_expr(0),
                desc: true,
            }],
        );
        assert_eq!(collect(&mut e), rows1(&[Some(2), Some(1), None]));
    }

    #[test]
    fn eof_after_emission() {
        let mut e = sort_over(
            &rows1(&[Some(2), Some(1)]),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
        );
        e.open().unwrap();
        let mut req = e.new_chunk();
        e.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 2);
        e.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        e.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        e.close().unwrap();
    }

    #[test]
    fn empty_child_is_empty() {
        let mut e = sort_over(
            &[],
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
        );
        assert_eq!(collect(&mut e), Vec::<Vec<Option<i64>>>::new());
    }
}
