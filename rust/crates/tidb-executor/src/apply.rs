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

//! Apply: the operator a CORRELATED subquery becomes.
//!
//! Go's `NestedLoopApplyExec` re-runs the inner plan once per outer row, after
//! writing that row's values into the correlated columns the inner plan
//! references (`for _, col := range e.OuterSchema { *col.Data = ... }`). This
//! is that loop: the outer child streams rows, each row is handed to a
//! `run_inner` callback that produces the inner result for those bindings, and
//! the output row is the outer row plus one appended column carrying it.
//!
//! Appending exactly one column is what lets the outer query keep referring to
//! the subquery by an ordinary column reference, which is how Go's plan reads
//! after `handleScalarSubquery` builds an Apply: the subquery expression is
//! replaced by the Apply's last schema column.
//!
//! # Chunking and memory
//!
//! Both operators here return as soon as `req` holds `max_chunk_size` rows
//! and resume from their own outer cursor on the next call, which is what
//! Go's `NestedLoopApplyExec.Next` does with `req.IsFull()` and
//! `outerChunkCursor` (`pkg/executor/join/hash_join_v1.go:1347-1408`). The
//! `LATERAL` shape additionally keeps ONE outer row's inner relation across
//! calls, because that relation alone can outrun a chunk -- Go's `innerIter`.
//!
//! Each inner relation is charged against [`StatementMemory`] and checked
//! before it is emitted, so `tidb_mem_quota_query` cancels an apply whose
//! inner side explodes. Go attaches the `InnerList`'s own tracker to the
//! apply's tracker (`hash_join_v1.go:1225`) for the same reason. The OUTPUT
//! is not charged: the caller drains `req` between calls, so nothing
//! accumulates -- the same line `crate::join` draws for its hash path.
//!
//! NOT MODELLED (documented): Go's apply cache (`applycache`), which skips
//! re-running the inner plan when consecutive outer rows share correlated
//! values, its parallel variant, and the decorrelation rewrites the optimizer
//! applies before falling back to Apply. Those change cost, not results. The
//! cache would want to live beside the other memory-bounded containers rather
//! than here; it is left where it is because moving it needs a home for the
//! LRU that neither `crate::mem_quota` nor `tidb-planner` currently has.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::mem_quota::StatementMemory;
use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;
use tidb_util::memory::Tracker;

/// The outer side's position, shared by both apply operators.
///
/// Go `NestedLoopApplyExec` keeps `OuterChunk` and `outerChunkCursor` across
/// `Next` calls precisely so it can return a FULL `req` and resume where it
/// stopped (`hash_join_v1.go:1272-1408`). Draining the whole outer child in
/// one call instead emits the entire result as a single chunk, which ignores
/// `tidb_max_chunk_size` and holds the whole result live at once.
struct OuterCursor {
    chunk: Option<Chunk>,
    cursor: usize,
    done: bool,
}

impl OuterCursor {
    const fn new() -> Self {
        Self {
            chunk: None,
            cursor: 0,
            done: false,
        }
    }

    fn reset(&mut self) {
        self.chunk = None;
        self.cursor = 0;
        self.done = false;
    }

    /// The next outer row's cells, or `None` at the outer child's end.
    fn next_row(
        &mut self,
        outer: &mut dyn Executor,
        types: &[FieldType],
    ) -> Result<Option<Vec<Datum>>, ExecError> {
        Ok(self.next_row_marked(outer, types)?.map(|(row, _)| row))
    }

    /// The next outer row's cells, plus Go's `outerSelected` bit for it.
    ///
    /// The bit is `false` for exactly the one row Go's
    /// `fetchSelectedOuterRow` deselects, and `true` for every other row --
    /// this cursor has no outer filter to answer for otherwise.
    fn next_row_marked(
        &mut self,
        outer: &mut dyn Executor,
        types: &[FieldType],
    ) -> Result<Option<(Vec<Datum>, bool)>, ExecError> {
        loop {
            if let Some(chunk) = self.chunk.as_ref() {
                if self.cursor < chunk.num_rows() {
                    let row = chunk.get_row(self.cursor);
                    // Go: `e.outerChunkCursor == 0 && e.OuterChunk.NumRows()
                    // == 1 && e.outerSelected[0] && aggExecutorTreeInputEmpty
                    // (e.OuterExec)`, evaluated on the chunk it just fetched.
                    let selected = !(self.cursor == 0
                        && chunk.num_rows() == 1
                        && outer.agg_tree_input_empty());
                    self.cursor += 1;
                    return Ok(Some((row.get_datum_row(types), selected)));
                }
            }
            if self.done {
                return Ok(None);
            }
            let mut chunk = self.chunk.take().unwrap_or_else(|| outer.new_chunk());
            outer.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                self.done = true;
                return Ok(None);
            }
            self.cursor = 0;
            self.chunk = Some(chunk);
        }
    }
}

/// Produces the inner result for one outer row's bindings.
///
/// The values are the outer row's cells, in outer-schema order; the callback
/// binds the correlated columns from them and runs the inner query.
pub type InnerRunner = Box<dyn FnMut(&[Datum]) -> Result<Datum, ExecError>>;

/// Produces one outer row's whole inner relation, row by row.
///
/// This is the multi-row, multi-column counterpart of [`InnerRunner`]: a
/// `LATERAL` derived table is a relation per outer row, not a value per outer
/// row. Each returned row must have the inner relation's fixed width.
pub type LateralRunner = Box<dyn FnMut(&[Datum]) -> Result<Vec<Vec<Datum>>, ExecError>>;

/// Go `LogicalApply` with `InnerJoin` -- what `buildLateralJoin`
/// (`pkg/planner/core/logical_plan_builder.go`) builds for a `LATERAL`
/// derived table.
///
/// The inner query runs once per outer row with that row's columns bound, and
/// every inner row it yields is concatenated onto the outer row. An outer row
/// whose inner relation is EMPTY produces nothing, because the join type is
/// inner: Go rejects `LEFT`/`RIGHT JOIN LATERAL` outright (`ErrInvalidLateralJoin`,
/// 3809), so inner is the only shape that reaches execution.
pub struct LateralApplyExec {
    meta: ExecutorMeta,
    outer: Box<dyn Executor>,
    run_inner: LateralRunner,
    position: OuterCursor,
    /// The current outer row and the inner relation it produced, held across
    /// `Next` calls because one outer row's relation can outrun a chunk.
    /// This is Go's `outerRow` + `innerIter`.
    pending: Option<(Vec<Datum>, Vec<Vec<Datum>>, usize)>,
    memory: StatementMemory,
    tracker: Arc<Tracker>,
}

impl LateralApplyExec {
    /// Builds a lateral apply over `outer`, whose schema must be the outer
    /// columns followed by the inner relation's columns.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        outer: Box<dyn Executor>,
        run_inner: LateralRunner,
        memory: StatementMemory,
    ) -> Self {
        let tracker = memory.operator_tracker(meta.id());
        LateralApplyExec {
            meta,
            outer,
            run_inner,
            position: OuterCursor::new(),
            pending: None,
            memory,
            tracker,
        }
    }
}

impl Executor for LateralApplyExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.outer.open()?;
        self.position.reset();
        self.pending = None;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let max_rows = self.meta.max_chunk_size();
        let outer_types: Vec<FieldType> = self.outer.ret_field_types().to_vec();
        while req.num_rows() < max_rows {
            if self.pending.is_none() {
                let Some(values) = self.position.next_row(self.outer.as_mut(), &outer_types)?
                else {
                    break;
                };
                let inner = (self.run_inner)(&values)?;
                // Go attaches the `InnerList`'s own tracker to the apply's
                // (`hash_join_v1.go:1225`), so one outer row's whole inner
                // relation is what the statement quota sees.
                self.tracker.consume(inner_relation_bytes(&inner));
                self.memory.check()?;
                self.pending = Some((values, inner, 0));
            }
            let Some((values, inner, at)) = self.pending.as_mut() else {
                break;
            };
            while *at < inner.len() && req.num_rows() < max_rows {
                for (c, value) in values.iter().enumerate() {
                    req.append_datum(c, value);
                }
                for (c, value) in inner[*at].iter().enumerate() {
                    req.append_datum(values.len() + c, value);
                }
                *at += 1;
            }
            if *at >= inner.len() {
                self.tracker.consume(-inner_relation_bytes(inner));
                self.pending = None;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.outer.close()
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

/// Go `NestedLoopApplyExec`, restricted to the one-appended-column shape a
/// scalar or `EXISTS` correlated subquery needs.
pub struct ApplyExec {
    meta: ExecutorMeta,
    outer: Box<dyn Executor>,
    run_inner: InnerRunner,
    position: OuterCursor,
    memory: StatementMemory,
    tracker: Arc<Tracker>,
    /// What the appended column holds for an outer row Go's apply loop marks
    /// as NOT selected -- its `Joiner.OnMissMatch` value -- or `None` when
    /// this apply has no such row to answer for. See [`ApplyExec::new`].
    miss_match: Option<Datum>,
}

impl ApplyExec {
    /// Builds an apply over `outer`, appending the column `run_inner` yields.
    ///
    /// The callback owns whatever it reads, because an executor is a `'static`
    /// trait object here; the driver therefore hands it an owned catalog
    /// snapshot. That copy is the price of this seed's ownership shape, not a
    /// semantic choice -- the inner plan only reads, and Go likewise runs it
    /// against one fixed snapshot for the whole statement.
    ///
    /// `miss_match` is the ONE thing this operator needs from the join type
    /// Go's apply carries. Go `NestedLoopApplyExec.fetchSelectedOuterRow`:
    ///
    /// ```text
    /// // For cases like `select count(1), (select count(1) from s where s.a > t.a) as sub from t where t.a = 1`,
    /// // if outer child has no row satisfying `t.a = 1`, `sub` should be `null` instead of `0` theoretically; however, the
    /// // outer `count(1)` produces one row <0, null> over the empty input, we should specially mark this outer row
    /// // as not selected, to trigger the mismatch join procedure.
    /// ```
    ///
    /// A SCALAR subquery's apply is a left outer join, so its mismatch pads
    /// the appended column with NULL: pass `Some(Datum::Null)`. Every other
    /// correlated shape passes `None`, and for a reason that is measured
    /// rather than assumed -- Go decorrelates `EXISTS`/`IN` into a semi HASH
    /// join, which has no such rule, so their inner side really does run over
    /// the aggregation's default row (captured: `select count(1), exists
    /// (select 1 from t2 where t2.a > t1.a or t1.a is null) from t1 where
    /// t1.a = 100` answers `0, 1`, while the scalar `select count(1),
    /// (select 1 from t2 where t1.a is null limit 1) ...` answers `0, NULL`).
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        outer: Box<dyn Executor>,
        run_inner: InnerRunner,
        memory: StatementMemory,
        miss_match: Option<Datum>,
    ) -> Self {
        let tracker = memory.operator_tracker(meta.id());
        ApplyExec {
            meta,
            outer,
            run_inner,
            position: OuterCursor::new(),
            memory,
            tracker,
            miss_match,
        }
    }
}

impl Executor for ApplyExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.outer.open()?;
        self.position.reset();
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let max_rows = self.meta.max_chunk_size();
        let outer_types: Vec<FieldType> = self.outer.ret_field_types().to_vec();
        // Go returns the moment `req.IsFull()` and resumes from its outer
        // cursor on the next call; one outer row yields at most one row here,
        // so the row count is the whole test.
        while req.num_rows() < max_rows {
            let Some((values, selected)) = self
                .position
                .next_row_marked(self.outer.as_mut(), &outer_types)?
            else {
                break;
            };
            // Go's mismatch procedure: the deselected outer row never reaches
            // the inner plan at all, and `Joiner.OnMissMatch` writes the pad.
            if let Some(pad) = self.miss_match.as_ref().filter(|_| !selected) {
                for (c, value) in values.iter().enumerate() {
                    req.append_datum(c, value);
                }
                req.append_datum(values.len(), pad);
                continue;
            }
            // One inner run per outer row, as Go's apply loop does.
            let inner = (self.run_inner)(&values)?;
            // Go's apply tracker sees the inner result for the row it is
            // holding; a scalar apply holds one value at a time, so this is
            // the whole of what it can accumulate.
            self.tracker.consume(datum_bytes(&inner));
            self.memory.check()?;
            for (c, value) in values.iter().enumerate() {
                req.append_datum(c, value);
            }
            req.append_datum(values.len(), &inner);
            self.tracker.consume(-datum_bytes(&inner));
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.outer.close()
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

/// One inner relation's retained size, for the statement memory tracker.
fn inner_relation_bytes(rows: &[Vec<Datum>]) -> i64 {
    rows.iter()
        .map(|row| tidb_chunk::row::ROW_SIZE + row.iter().map(datum_bytes).sum::<i64>())
        .sum()
}

/// One value's retained size, the same measure the join's drain charges.
fn datum_bytes(value: &Datum) -> i64 {
    crate::join::row_bytes(std::slice::from_ref(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn schema_of(width: usize) -> Schema {
        Schema::new(
            (0..width)
                .map(|i| {
                    let mut column = Column::new(i as i64 + 1, long());
                    column.index = i as i64;
                    column
                })
                .collect(),
        )
    }

    /// A one-column source that hands out `batch` rows per `next`, so the
    /// apply above it really is pulled incrementally.
    struct Counter {
        meta: ExecutorMeta,
        remaining: usize,
        batch: usize,
        next_value: i64,
    }

    impl Counter {
        fn new(rows: usize, batch: usize) -> Self {
            Counter {
                meta: ExecutorMeta::new(schema_of(1), 0, batch, batch),
                remaining: rows,
                batch,
                next_value: 0,
            }
        }
    }

    impl Executor for Counter {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            for _ in 0..self.batch.min(self.remaining) {
                req.append_datum(0, &Datum::Int(self.next_value));
                self.next_value += 1;
                self.remaining -= 1;
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

    /// Go `NestedLoopApplyExec.Next` returns as soon as `req.IsFull()` and
    /// resumes from its own outer cursor (`hash_join_v1.go:1347-1408`).
    ///
    /// Emitting the whole result in ONE chunk instead -- which this operator
    /// used to do -- ignores `tidb_max_chunk_size` entirely and holds every
    /// output row live at once. The outer child here hands out 7 rows per
    /// call while the apply's own chunk is 10, so this also pins that the
    /// apply keeps pulling ACROSS outer chunks to fill one of its own.
    #[test]
    fn an_apply_fills_one_chunk_at_a_time_and_resumes_where_it_stopped() {
        let mut apply = ApplyExec::new(
            ExecutorMeta::new(schema_of(2), 1, 10, 10),
            Box::new(Counter::new(25, 7)),
            Box::new(|values| match values[0] {
                Datum::Int(value) => Ok(Datum::Int(value * 2)),
                _ => unreachable!("the counter only produces Int"),
            }),
            StatementMemory::default(),
            Some(Datum::Null),
        );
        apply.open().unwrap();
        let mut chunk = apply.new_chunk();
        let mut seen: Vec<(i64, i64)> = Vec::new();
        let mut widths = Vec::new();
        loop {
            apply.next(&mut chunk).unwrap();
            let rows = chunk.num_rows();
            if rows == 0 {
                break;
            }
            widths.push(rows);
            for r in 0..rows {
                let row = chunk.get_row(r);
                match (row.get_datum(0, &long()), row.get_datum(1, &long())) {
                    (Datum::Int(outer), Datum::Int(inner)) => seen.push((outer, inner)),
                    other => panic!("unexpected row {other:?}"),
                }
            }
        }
        apply.close().unwrap();
        assert_eq!(widths, vec![10, 10, 5], "25 rows through a 10-row chunk");
        assert_eq!(seen.len(), 25);
        assert!(seen
            .iter()
            .enumerate()
            .all(|(i, (outer, inner))| { *outer == i as i64 && *inner == 2 * i as i64 }));
    }

    /// The same for the `LATERAL` shape, whose extra difficulty is that ONE
    /// outer row's inner relation can outrun the chunk: Go keeps `innerIter`
    /// across calls for exactly that, and so does this.
    #[test]
    fn a_lateral_apply_resumes_inside_one_outer_rows_inner_relation() {
        let mut apply = LateralApplyExec::new(
            ExecutorMeta::new(schema_of(2), 1, 4, 4),
            Box::new(Counter::new(3, 7)),
            Box::new(|values| match values[0] {
                // Six inner rows per outer row against a four-row chunk, so
                // every outer row spans a chunk boundary.
                Datum::Int(value) => Ok((0..6).map(|k| vec![Datum::Int(value * 10 + k)]).collect()),
                _ => unreachable!("the counter only produces Int"),
            }),
            StatementMemory::default(),
        );
        apply.open().unwrap();
        let mut chunk = apply.new_chunk();
        let mut seen: Vec<(i64, i64)> = Vec::new();
        let mut widths = Vec::new();
        loop {
            apply.next(&mut chunk).unwrap();
            let rows = chunk.num_rows();
            if rows == 0 {
                break;
            }
            widths.push(rows);
            for r in 0..rows {
                let row = chunk.get_row(r);
                match (row.get_datum(0, &long()), row.get_datum(1, &long())) {
                    (Datum::Int(outer), Datum::Int(inner)) => seen.push((outer, inner)),
                    other => panic!("unexpected row {other:?}"),
                }
            }
        }
        apply.close().unwrap();
        assert_eq!(
            widths,
            vec![4, 4, 4, 4, 2],
            "18 rows through a four-row chunk"
        );
        let want: Vec<(i64, i64)> = (0..3)
            .flat_map(|outer| (0..6).map(move |k| (outer, outer * 10 + k)))
            .collect();
        assert_eq!(seen, want);
    }

    /// The inner relation is charged against the statement's budget and
    /// checked inside the loop, so a `LATERAL` apply whose inner relation
    /// outgrows `tidb_mem_quota_query` is cancelled rather than run to
    /// completion. Go attaches the `InnerList`'s tracker to the apply's
    /// (`hash_join_v1.go:1225`) for the same reason.
    #[test]
    fn an_inner_relation_past_the_quota_is_cancelled() {
        let memory = StatementMemory::new(1, crate::mem_quota::OomAction::Cancel, 0);
        let mut apply = LateralApplyExec::new(
            ExecutorMeta::new(schema_of(2), 1, 4, 4),
            Box::new(Counter::new(3, 7)),
            Box::new(|values| match values[0] {
                Datum::Int(value) => Ok((0..6).map(|k| vec![Datum::Int(value * 10 + k)]).collect()),
                _ => unreachable!("the counter only produces Int"),
            }),
            memory,
        );
        apply.open().unwrap();
        let mut chunk = apply.new_chunk();
        let error = apply
            .next(&mut chunk)
            .expect_err("a one-byte quota must cancel");
        assert!(
            format!("{error:?}").contains("Memory"),
            "expected the quota error, got {error:?}"
        );
    }
}
