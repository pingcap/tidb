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
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::Arc;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_util::memory::{ArcAction, Tracker};

use crate::mem_quota::StatementMemory;
use crate::sort_partition::{spill_action, SortPartition, SPILL_CHUNK_SIZE};

/// Go `planner/util.ByItems`: one `ORDER BY` item -- the key expression and
/// its direction.
pub struct SortByItem {
    /// Go `ByItems.Expr`.
    pub expr: Expression,
    /// Go `ByItems.Desc`.
    pub desc: bool,
}

/// Evaluates every by-item against `row`, producing the row's sort key.
///
/// Go keeps no materialized key (`keyCmpFuncs` re-reads the chunk cell on
/// every comparison); this port materializes one so a spilled run can be
/// compared after its chunk is gone. See `SortPartition::add` for the memory
/// that costs and why it is counted.
pub fn eval_sort_key<C: Columns>(
    by_items: &[SortByItem],
    ctx: &C,
    row: Row<'_>,
) -> Result<Vec<Datum>, ExecError> {
    let mut key = Vec::with_capacity(by_items.len());
    for item in by_items {
        key.push(item.expr.eval(ctx, row)?);
    }
    Ok(key)
}

/// Go `lessRow`: the first non-equal by-item decides, and `Desc` negates it.
///
/// Each key compares under ITS OWN derived collation (Go builds `keyCmpFuncs`
/// from the by-item's `RetType`): `ORDER BY ci_col` orders `a, A, b, B`, not
/// the byte order `A, B, a, b`.
pub fn less_by_items(
    by_items: &[SortByItem],
    a: &[Datum],
    b: &[Datum],
) -> Result<Ordering, ExecError> {
    for (i, item) in by_items.iter().enumerate() {
        let mut cmp = tidb_expr::compare_datums_with_collation(
            &a[i],
            &b[i],
            tidb_expr::collation_derive::collation_of_node(&item.expr),
        )?;
        if item.desc {
            cmp = cmp.reverse();
        }
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

/// Go `SortExec` (unparallel, external): one or more sorted runs, merged.
pub struct SortExec<C: Columns> {
    meta: ExecutorMeta,
    /// Go `ByItems`.
    by_items: Vec<SortByItem>,
    child: Box<dyn Executor>,
    ctx: C,
    /// Go `fetched`: whether the child has been drained and sorted.
    fetched: bool,
    /// Go `Unparallel.sortPartitions`: the sorted runs, in creation order.
    /// One entry, unspilled, is the common in-memory case.
    partitions: Vec<SortPartition>,
    /// The statement's memory budget, which this operator's tracker hangs off
    /// and whose quota it checks after each `Consume`.
    memory: StatementMemory,
    /// Go `SortExec.memTracker` = `memory.NewTracker(e.ID(), -1)` attached to
    /// `StmtCtx.MemTracker`: this operator's own node in the tracker tree, so
    /// `SHOW`-style tree dumps attribute the bytes to the sort.
    tracker: Arc<Tracker>,
    /// Go `SortExec.diskTracker`.
    disk_tracker: Arc<tidb_util::disk::Tracker>,
    /// Go `enableTmpStorageOnOOM` = `vardef.EnableTmpStorageOnOOM.Load()`:
    /// `tidb_enable_tmp_storage_on_oom`. With it OFF the sort registers no
    /// spill action, so an overrun goes straight to the 8175 cancellation --
    /// which is exactly what this executor did before spilling existed.
    enable_tmp_storage_on_oom: bool,
    /// Go `spillLimit` = `MemTracker.GetBytesLimit() / 10`.
    spill_limit: i64,
    /// Raised by the current partition's spill action; see
    /// `crate::sort_partition`'s module doc for why a flag stands in for
    /// Go's spill goroutine.
    need_spill: Arc<AtomicBool>,
    /// The action currently registered on the session tracker, kept so
    /// `close` can unbind it.
    registered_action: Option<ArcAction>,
    /// Go `spillChunkSize` (a package var so tests can shrink it).
    spill_chunk_size: usize,
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
        let disk_tracker = memory.operator_disk_tracker(meta.id());
        let spill_limit = memory.quota() / 10;
        let enable_tmp_storage_on_oom = memory.tmp_storage_on_oom();
        SortExec {
            meta,
            by_items,
            child,
            ctx,
            fetched: false,
            partitions: Vec::new(),
            memory,
            tracker,
            disk_tracker,
            enable_tmp_storage_on_oom,
            spill_limit,
            need_spill: Arc::new(AtomicBool::new(false)),
            registered_action: None,
            spill_chunk_size: SPILL_CHUNK_SIZE,
        }
    }

    /// Go `SetSmallSpillChunkSizeForTest`: shrink the spill chunk so a test
    /// can produce many spilled chunks without a large data set.
    pub fn set_spill_chunk_size_for_test(&mut self, size: usize) {
        self.spill_chunk_size = size;
    }

    /// Bytes this sort has written to spill files (Go `SortExec.diskTracker`).
    #[must_use]
    pub fn bytes_in_disk(&self) -> i64 {
        self.disk_tracker.bytes_consumed()
    }

    /// How many sorted runs the sort produced; more than one means the sort
    /// spilled. For tests and diagnostics.
    #[must_use]
    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    /// Runs that actually hold rows. A spill that fires while the child is
    /// already exhausted leaves a trailing EMPTY run, so `num_partitions`
    /// alone does not prove the merge had anything to merge.
    #[must_use]
    pub fn num_non_empty_partitions(&self) -> usize {
        self.partitions
            .iter()
            .filter(|partition| partition.num_rows() > 0)
            .count()
    }

    /// Go `switchToNewSortPartition`: start a fresh run and point the spill
    /// action at it.
    fn new_partition(&mut self, fields: &[FieldType]) -> SortPartition {
        let mut partition =
            SortPartition::new(fields.to_vec(), &self.tracker, self.memory.spill_storage());
        partition.set_spill_chunk_size(self.spill_chunk_size);
        if self.enable_tmp_storage_on_oom {
            partition.disk_tracker().attach_to(&self.disk_tracker);
            let (action, need_spill) = spill_action(&partition, self.spill_limit);
            self.need_spill = need_spill;
            let action: ArcAction = action;
            self.memory
                .session_tracker()
                .fallback_old_and_set_new_action(Arc::clone(&action));
            self.registered_action = Some(action);
        }
        partition
    }

    /// Go `fetchChunksUnparallel` + `storeChunk`: drain the child into sorted
    /// runs, spilling whenever the memory action says to.
    fn fetch_and_sort(&mut self) -> Result<(), ExecError> {
        let fields: Vec<FieldType> = self.meta.ret_field_types().to_vec();
        let mut current = self.new_partition(&fields);

        loop {
            let mut chunk = self.child.new_chunk();
            self.child.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                break;
            }
            // Accounting happens INSIDE the loop, which is what makes a query
            // over a large table spill (or stop) early instead of first
            // materializing everything and only then noticing.
            current.add(chunk, &self.by_items, &self.ctx)?;

            if self.need_spill.swap(false, SeqCst) {
                current.spill_to_disk(&self.by_items)?;
                self.partitions.push(current);
                current = self.new_partition(&fields);
            }
            // With tmp storage off (or with a partition too small to be worth
            // a file), the action fell through to the cancellation, and this
            // is where the statement stops with 8175.
            self.memory.check()?;
        }

        current.sort(&self.by_items)?;
        self.partitions.push(current);
        Ok(())
    }
}

impl<C: Columns> Executor for SortExec<C> {
    /// Go `Open`: resets the fetched state and opens the child.
    fn open(&mut self) -> Result<(), ExecError> {
        self.fetched = false;
        for partition in &mut self.partitions {
            partition.close();
        }
        self.partitions.clear();
        // Go `SortExec.Open`: `e.memTracker.ReplaceBytesUsed(0)` -- a re-opened
        // sort (an Apply's inner side re-runs per outer row) must not keep
        // charging for rows it has just dropped.
        self.tracker.replace_bytes_used(0);
        self.need_spill.store(false, SeqCst);
        self.child.open()
    }

    /// Go `Next`: the first call drains and sorts; every call then appends
    /// sorted rows until the chunk-size bound or exhaustion.
    ///
    /// With one run this is Go's `onePartitionSorting`; with several it is
    /// `externalSorting`, the multi-way merge over the runs.
    ///
    /// FAITHFUL ADAPTATION: Go's merger is a heap
    /// (`multi_way_merge.go`); this picks the minimum by scanning the runs,
    /// which is the same output for `k` runs and, at the handful of runs an
    /// external sort produces, the same work. Ties resolve to the earlier run
    /// in both, and tie order is not a guaranteed property of either.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !self.fetched {
            self.fetch_and_sort()?;
            self.fetched = true;
        }

        let batch = self.meta.max_chunk_size();
        let mut partitions = std::mem::take(&mut self.partitions);
        let result = (|| -> Result<(), ExecError> {
            while req.num_rows() < batch {
                for partition in &mut partitions {
                    partition.load_head(&self.by_items, &self.ctx)?;
                }
                let mut best: Option<usize> = None;
                for (i, partition) in partitions.iter().enumerate() {
                    let Some(key) = partition.head_key() else {
                        continue;
                    };
                    match best {
                        None => best = Some(i),
                        Some(b) => {
                            let other = partitions[b].head_key().expect("a loaded head");
                            if less_by_items(&self.by_items, key, other)? == Ordering::Less {
                                best = Some(i);
                            }
                        }
                    }
                }
                match best {
                    None => break,
                    Some(i) => partitions[i].take_head_into(req),
                }
            }
            Ok(())
        })();
        self.partitions = partitions;
        result
    }

    /// Go `Close`: drops the runs and their spill files, unbinds the spill
    /// action, and gives the bytes back to the statement's budget.
    fn close(&mut self) -> Result<(), ExecError> {
        for partition in &mut self.partitions {
            partition.close();
        }
        self.partitions.clear();
        if let Some(action) = self.registered_action.take() {
            self.memory
                .session_tracker()
                .unbind_action_from_hard_limit(&action);
        }
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

    /// A test-only source that emits `rows` in chunks of `chunk_size`, so a
    /// sort sees several child chunks -- which is what lets a spill produce
    /// more than one NON-EMPTY sorted run. A single-chunk source cannot: the
    /// spill fires after the only chunk is in, leaving one full run and one
    /// empty one, and a merge over that is not a merge at all.
    struct ManyChunkSource {
        meta: ExecutorMeta,
        rows: Vec<Vec<Option<i64>>>,
        emitted: usize,
        chunk_size: usize,
    }

    impl Executor for ManyChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.emitted = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let end = (self.emitted + self.chunk_size).min(self.rows.len());
            for row in &self.rows[self.emitted..end] {
                for (c, v) in row.iter().enumerate() {
                    match v {
                        Some(v) => req.append_int64(c, *v),
                        None => req.append_null(c),
                    }
                }
            }
            self.emitted = end;
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

    /// A sort over a MULTI-CHUNK source, which is what a spilling sort needs.
    fn multi_chunk_sorter(
        rows: &[Vec<Option<i64>>],
        by: Vec<SortByItem>,
        chunk_size: usize,
        memory: StatementMemory,
    ) -> SortExec<NoColumns> {
        let n_cols = rows.first().map_or(1, Vec::len);
        let source = ManyChunkSource {
            meta: ExecutorMeta::new(schema_of(n_cols), 0, 4, chunk_size),
            rows: rows.to_vec(),
            emitted: 0,
            chunk_size,
        };
        SortExec::new(
            ExecutorMeta::new(schema_of(n_cols), 1, 4, 1024),
            by,
            Box::new(source),
            NoColumns,
            memory,
        )
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
        // A quota far below what 4096 retained rows need, with spilling OFF
        // (`tidb_enable_tmp_storage_on_oom = 0`). With it ON the same sort
        // spills and completes -- see
        // `a_sort_over_the_quota_spills_to_disk_and_returns_every_row`.
        let memory =
            StatementMemory::new(2048, OomAction::Cancel, 42).with_tmp_storage_on_oom(false);
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
        let memory = StatementMemory::new(2048, OomAction::Log, 42).with_tmp_storage_on_oom(false);
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

    /// `tmp-storage-path` is process-global, so the tests that redirect it
    /// must not run at the same time inside one test binary -- and that
    /// includes the aggregation's and the TopN's spill tests, which is why the
    /// lock is the CRATE's rather than this module's.
    use crate::test_temp_storage::{scratch_dir as scratch_temp_dir, storage as test_storage};

    fn spill_files_in(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .map(|entry| entry.path())
                    .filter(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.contains("ChunkDataInDiskByChunks"))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drains an executor into a flat list of first-column values.
    fn drain_first_col(exec: &mut SortExec<NoColumns>) -> Vec<i64> {
        let mut out = Vec::new();
        loop {
            let mut req = exec.new_chunk();
            exec.next(&mut req).expect("sort must not fail");
            if req.num_rows() == 0 {
                return out;
            }
            for r in 0..req.num_rows() {
                out.push(req.get_row(r).get_int64(0));
            }
        }
    }

    fn asc() -> Vec<SortByItem> {
        vec![SortByItem {
            expr: col_expr(0),
            desc: false,
        }]
    }

    /// THE SPILL TEST. A quota the sort cannot hold its rows within, with
    /// `tidb_enable_tmp_storage_on_oom` ON: the sort must spill (proved by a
    /// spill file existing on disk while it runs, and by the disk tracker),
    /// must produce SEVERAL NON-EMPTY sorted runs, and must return every row
    /// in order -- the same rows the same sort returns unspilled.
    ///
    /// The input values are shuffled by a stride so that consecutive runs
    /// cover OVERLAPPING ranges. That is what makes the multi-way merge load
    /// bearing: a merge that drained run 0 and then run 1 would emit an
    /// unsorted sequence, and so would one that picked the wrong end.
    #[test]
    fn a_sort_over_the_quota_spills_to_disk_and_returns_every_row() {
        let dir = scratch_temp_dir("sortexec");

        let n = 8192i64;
        let rows: Vec<Vec<Option<i64>>> = (0..n).map(|i| vec![Some((i * 7919) % n)]).collect();
        let mut expected: Vec<i64> = rows.iter().map(|r| r[0].expect("no nulls")).collect();
        expected.sort_unstable();

        // The unspilled reference: a quota this sort fits inside.
        let mut reference = multi_chunk_sorter(&rows, asc(), 256, StatementMemory::default());
        reference.open().unwrap();
        assert_eq!(drain_first_col(&mut reference), expected);
        assert_eq!(reference.num_partitions(), 1);
        assert_eq!(reference.bytes_in_disk(), 0, "the reference must not spill");
        reference.close().unwrap();

        // Now the same sort under a quota it cannot hold, spilling enabled.
        let memory = StatementMemory::new(1 << 16, OomAction::Cancel, 42)
            .with_spill_storage(test_storage(&dir));
        let mut exec = multi_chunk_sorter(&rows, asc(), 256, memory);
        // Small spill chunks so each run becomes many spilled chunks, the
        // shape Go's `SetSmallSpillChunkSizeForTest` produces.
        exec.set_spill_chunk_size_for_test(64);
        exec.open().unwrap();

        let mut got = Vec::new();
        let mut saw_spill_file = false;
        loop {
            let mut req = exec.new_chunk();
            exec.next(&mut req).expect("a spilling sort must not fail");
            if req.num_rows() == 0 {
                break;
            }
            // DISK WAS ACTUALLY USED: a spill file exists while the sort is
            // still producing rows.
            saw_spill_file |= !spill_files_in(&dir).is_empty();
            for r in 0..req.num_rows() {
                got.push(req.get_row(r).get_int64(0));
            }
        }

        assert!(
            saw_spill_file,
            "no spill file was ever created -- this test proved nothing"
        );
        assert!(
            exec.bytes_in_disk() > 0,
            "the disk tracker must have counted the spilled bytes"
        );
        assert!(
            exec.num_non_empty_partitions() > 1,
            "a spilling sort must produce more than one NON-EMPTY sorted run, got {}",
            exec.num_non_empty_partitions()
        );
        assert_eq!(got, expected, "spilled sort must return the same rows");

        exec.close().unwrap();
        assert!(
            spill_files_in(&dir).is_empty(),
            "close must remove every spill file"
        );
        drop(exec);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A DESCENDING spilled sort, so the merge's direction is exercised in
    /// both directions rather than only the one the ascending test pins.
    #[test]
    fn a_spilled_descending_sort_returns_every_row_in_order() {
        let dir = scratch_temp_dir("sortdesc");

        let n = 8192i64;
        let rows: Vec<Vec<Option<i64>>> = (0..n).map(|i| vec![Some((i * 7919) % n)]).collect();
        let mut expected: Vec<i64> = rows.iter().map(|r| r[0].expect("no nulls")).collect();
        expected.sort_unstable_by(|a, b| b.cmp(a));

        let memory = StatementMemory::new(1 << 16, OomAction::Cancel, 42)
            .with_spill_storage(test_storage(&dir));
        let mut exec = multi_chunk_sorter(
            &rows,
            vec![SortByItem {
                expr: col_expr(0),
                desc: true,
            }],
            256,
            memory,
        );
        exec.set_spill_chunk_size_for_test(64);
        exec.open().unwrap();
        let got = drain_first_col(&mut exec);
        assert!(exec.num_non_empty_partitions() > 1, "this test needs runs");
        assert_eq!(got, expected);
        exec.close().unwrap();
        drop(exec);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The gate: with `tidb_enable_tmp_storage_on_oom = 0` the SAME sort under
    /// the SAME quota raises 8175 instead of spilling, and leaves no file.
    #[test]
    fn the_same_sort_raises_8175_when_tmp_storage_is_disabled() {
        let dir = scratch_temp_dir("sortgate");

        let memory = StatementMemory::new(1 << 15, OomAction::Cancel, 42)
            .with_spill_storage(test_storage(&dir))
            .with_tmp_storage_on_oom(false);
        let mut exec = sorter_with_memory(
            1,
            &one_col_rows(4096),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
            memory,
        );
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        match exec.next(&mut req) {
            Err(ExecError::MemoryExceedForQuery { conn_id }) => assert_eq!(conn_id, 42),
            other => panic!("expected 8175 with tmp storage disabled, got {other:?}"),
        }
        assert!(spill_files_in(&dir).is_empty(), "no file may be written");
        drop(exec);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
