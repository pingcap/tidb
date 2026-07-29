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

//! The narrowed access paths as streaming source executors: Go's
//! `PointGet`/`Batch_Point_Get` and `IndexRangeScan` + row lookup.
//!
//! # Why these are executors and not a `Vec`
//!
//! Each of these paths reads a *subset* of a table rather than all of it, but
//! a subset is not a bound: `WHERE b > 4` over a ten-million-row index is an
//! index range, and materializing it into a `Vec<Vec<Datum>>` to hand to a
//! `MemTableSourceExec` costs the whole relation in decoded form before the
//! first row leaves the source. These executors hold the cursor instead
//! ([`KvTable::row_cursor`], [`KvTable::index_range_cursor`]) and decode one
//! row per pull, so the decoded rows alive at once are one chunk's worth
//! regardless of how many rows the range covers, and a pushed `LIMIT` never
//! decodes or looks up a row past its cap.
//!
//! # How far down the streaming reaches today
//!
//! The executors are fully pull-based, and so is the row decoding above the
//! [`TableStorage`](crate::storage::TableStorage) seam. Below it, neither
//! backend's `iter` is lazy yet: `MemStorage::iter` copies the range's
//! key/value bytes into a `Vec`, and `ClusterTableStorage::iter` scans the
//! snapshot range and merges the staged buffer into another one. So a scan
//! today holds the range's *packed bytes*, not its decoded rows -- which is
//! the smaller of the two by a wide margin (no per-row `Vec<Datum>`, no
//! per-value allocation), and on the index path the row lookups are avoided
//! outright: a capped scan performs `cap` point reads instead of one per
//! entry in the range.
//!
//! This is the seam, not a workaround: the executors pull one row at a time
//! through `StorageIterator`, so the day a backend's `iter` returns a real
//! lazy cursor (a TiKV region iterator, or a borrowing `MemIterator`) the
//! whole path streams end to end with no change above the seam.
//!
//! # `actRows`
//!
//! Both sources expose a live produced-row counter, which the plan trace
//! reads for the access operator's `actRows`. Without a pushed limit that
//! count is exactly the row count a materializing path would have reported;
//! with one it reports the truncation, as Go's does.

use std::cell::Cell;
use std::rc::Rc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::kv_table::{IndexRange, IndexRangeCursor, KvTable, TableHandle};

/// Reads rows for an already-known handle list, one per pull: the source
/// behind Go's `PointGet` (one handle) and `Batch_Point_Get` (several).
///
/// The handle list is bounded by the statement text (`a IN (1, 2, 3)`), so it
/// is materialized; the *rows* are not. Go likewise leaves a `Limit` at the
/// root above a `Batch_Point_Get` rather than pushing into it (captured:
/// `Limit_10 | root` over `Batch_Point_Get_12`), which is why this source
/// takes no row cap.
pub struct HandleSourceExec {
    meta: ExecutorMeta,
    table: KvTable,
    handles: Vec<TableHandle>,
    /// The next handle to read.
    cursor: usize,
    /// Rows produced so far, which the trace reads as this node's `actRows`.
    produced: Rc<Cell<u64>>,
}

impl HandleSourceExec {
    /// Builds a source over `handles`, in the order the plan lists them.
    #[must_use]
    pub fn new(meta: ExecutorMeta, table: KvTable, handles: Vec<TableHandle>) -> Self {
        HandleSourceExec {
            meta,
            table,
            handles,
            cursor: 0,
            produced: Rc::new(Cell::new(0)),
        }
    }

    /// The live count of rows this source produced.
    #[must_use]
    pub fn produced_rows(&self) -> Rc<Cell<u64>> {
        Rc::clone(&self.produced)
    }
}

impl Executor for HandleSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.cursor = 0;
        self.produced.set(0);
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let cap = self.meta.max_chunk_size();
        while req.num_rows() < cap {
            let Some(handle) = self.handles.get(self.cursor) else {
                return Ok(());
            };
            self.cursor += 1;
            // A handle with no row is Go's point get that finds nothing: the
            // plan is right, the row is simply absent.
            let row = self
                .table
                .get_row_by_handle(handle)
                .map_err(|_| ExecError::Unsupported("table bytes failed to decode"))?;
            if let Some(row) = row {
                for (c, value) in row.iter().enumerate() {
                    req.append_datum(c, value);
                }
                self.produced.set(self.produced.get() + 1);
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

/// Walks a set of index ranges in index order, reading each row it finds:
/// Go's `IndexRangeScan` with the table-row lookup above it, collapsed into
/// one operator because this tier prints one node for the pair.
///
/// Rows leave in index-key order within a range, and the ranges are walked in
/// the order the plan lists them -- the property a `LIMIT` under an `ORDER BY`
/// on the index columns relies on (Go's `keep order:true`).
pub struct IndexRangeSourceExec {
    meta: ExecutorMeta,
    table: KvTable,
    index_id: i64,
    ranges: Vec<IndexRange>,
    /// The next range to open a cursor over.
    next_range: usize,
    /// The open cursor over `ranges[next_range - 1]`.
    cursor: Option<IndexRangeCursor>,
    /// Rows produced so far, which the trace reads as this node's `actRows`
    /// when no filter was pushed into it.
    produced: Rc<Cell<u64>>,
    /// Rows read from the range before any pushed filter -- the `actRows` the
    /// access operator reports once it filters internally.
    scanned: Rc<Cell<u64>>,
    /// Conjuncts this source took over from the `Selection` above it.
    filter: Option<crate::scan_pushdown::ScanFilterProbe>,
    /// A pushed row cap (`offset + count`); see [`Executor::accept_scan_limit`].
    limit: Option<u64>,
}

impl IndexRangeSourceExec {
    /// Builds a source over `ranges` of the index `index_id`.
    #[must_use]
    pub fn new(meta: ExecutorMeta, table: KvTable, index_id: i64, ranges: Vec<IndexRange>) -> Self {
        IndexRangeSourceExec {
            meta,
            table,
            index_id,
            ranges,
            next_range: 0,
            cursor: None,
            produced: Rc::new(Cell::new(0)),
            scanned: Rc::new(Cell::new(0)),
            filter: None,
            limit: None,
        }
    }

    /// The live count of rows this source produced.
    #[must_use]
    pub fn produced_rows(&self) -> Rc<Cell<u64>> {
        Rc::clone(&self.produced)
    }

    /// The next handle in index order across all ranges, opening the next
    /// range's cursor when the current one runs out.
    fn next_handle(&mut self) -> Result<Option<TableHandle>, ExecError> {
        loop {
            if let Some(cursor) = self.cursor.as_mut() {
                let handle = cursor
                    .next_handle()
                    .map_err(|_| ExecError::Unsupported("index bytes failed to decode"))?;
                if let Some(handle) = handle {
                    return Ok(Some(handle));
                }
                self.cursor = None;
            }
            let Some(range) = self.ranges.get(self.next_range).cloned() else {
                return Ok(None);
            };
            self.next_range += 1;
            self.cursor = Some(
                self.table
                    .index_range_cursor(self.index_id, &range)
                    .map_err(|_| ExecError::Unsupported("index range is not scannable"))?,
            );
        }
    }
}

impl Executor for IndexRangeSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.next_range = 0;
        self.cursor = None;
        self.produced.set(0);
        self.scanned.set(0);
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let cap = self.meta.max_chunk_size();
        while req.num_rows() < cap {
            if self.limit.is_some_and(|limit| self.produced.get() >= limit) {
                // Early stop: the cursor is dropped, so no entry past the cap
                // is read and no row past it is looked up.
                self.cursor = None;
                self.next_range = self.ranges.len();
                return Ok(());
            }
            let Some(handle) = self.next_handle()? else {
                return Ok(());
            };
            let row = self
                .table
                .get_row_by_handle(&handle)
                .map_err(|_| ExecError::Unsupported("table bytes failed to decode"))?;
            // An index entry whose row is gone is not a row: the same
            // `if let Some(row)` the materializing path had.
            if let Some(row) = row {
                self.scanned.set(self.scanned.get() + 1);
                if let Some(filter) = self.filter.as_mut() {
                    if !filter.admits(&row)? {
                        continue;
                    }
                }
                for (c, value) in row.iter().enumerate() {
                    req.append_datum(c, value);
                }
                self.produced.set(self.produced.get() + 1);
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.cursor = None;
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

    /// The ranges are walked in plan order and each one in index order, so a
    /// cap truncates the same prefix the `LimitExec` above would have kept.
    /// The driver only offers one when nothing above this source filters the
    /// rows (see `run_select_traced`).
    fn accept_scan_limit(&mut self, cap: u64) -> bool {
        self.limit = Some(cap);
        true
    }

    /// The rows this source emits are read through the same storage seam a
    /// full scan reads -- the snapshot with the session's staged mutation
    /// buffer merged in, for both the index entries and the row lookups --
    /// and every one of them is tested here, so the driver may drop these
    /// conjuncts from the `Selection` above (see [`crate::scan_pushdown`]).
    fn accept_scan_filter(
        &mut self,
        filter: &crate::scan_pushdown::PushedScanFilter,
        ctx: &crate::StmtContext,
    ) -> bool {
        if filter.is_empty() {
            return false;
        }
        self.filter = Some(crate::scan_pushdown::ScanFilterProbe::new(
            filter.clone(),
            ctx.clone(),
            self.meta.new_chunk(),
        ));
        true
    }

    fn scanned_rows_counter(&self) -> Option<Rc<Cell<u64>>> {
        Some(Rc::clone(&self.scanned))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tidb_datatype::{Datum, FieldTypeCode};
    use tidb_txnkv::Key;

    use super::*;
    use crate::driver::{run_select_on, Catalog};
    use crate::explain::ExplainFormat;
    use crate::kv_table::{KvColumn, KvTable};
    use crate::storage::{MemTableStorage, StorageError, StorageIterator, TableStorage};

    /// A backend that counts the work a scan actually does: every entry an
    /// iterator advances past, and every point read.
    ///
    /// This is the early-stop and memory proof. A source that materializes
    /// its range walks every entry in it before returning a row, so these
    /// counters are the difference between "read the relation" and "read what
    /// the query needs" -- observable without guessing at allocator numbers.
    #[derive(Debug, Clone, Default)]
    struct CountingStorage {
        inner: MemTableStorage,
        entries: Arc<AtomicUsize>,
        gets: Arc<AtomicUsize>,
    }

    struct CountingIterator {
        inner: Box<dyn StorageIterator>,
        entries: Arc<AtomicUsize>,
    }

    impl StorageIterator for CountingIterator {
        fn valid(&self) -> bool {
            self.inner.valid()
        }
        fn key(&self) -> &Key {
            self.inner.key()
        }
        fn value(&self) -> &[u8] {
            self.inner.value()
        }
        fn next(&mut self) -> Result<(), StorageError> {
            self.entries.fetch_add(1, Ordering::Relaxed);
            self.inner.next()
        }
        fn close(&mut self) {
            self.inner.close();
        }
    }

    impl TableStorage for CountingStorage {
        fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get(key)
        }
        fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), StorageError> {
            self.inner.set(key, value)
        }
        fn delete(&mut self, key: Key) -> Result<(), StorageError> {
            self.inner.delete(key)
        }
        fn iter(
            &mut self,
            start: Option<&Key>,
            upper_bound: Option<&Key>,
        ) -> Result<Box<dyn StorageIterator>, StorageError> {
            Ok(Box::new(CountingIterator {
                inner: self.inner.iter(start, upper_bound)?,
                entries: Arc::clone(&self.entries),
            }))
        }
        fn key_count(&self) -> usize {
            self.inner.key_count()
        }
        fn clear(&mut self) {
            self.inner.clear();
        }
        fn clone_box(&self) -> Box<dyn TableStorage> {
            Box::new(self.clone())
        }
    }

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: long(),
            default_value: None,
            origin_default: None,
        }
    }

    /// The rows counters and a catalog holding `t(a, b, c)` with `n` rows and
    /// an index on `b`. Row `i` is `(i, i, n - i)`, so `b` is unique and
    /// ascending while `c` descends -- the two orders the push-down rule has
    /// to tell apart.
    fn table_of(n: i64, indexed: bool) -> (Catalog, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let store = CountingStorage::default();
        let entries = Arc::clone(&store.entries);
        let gets = Arc::clone(&store.gets);
        let mut table = KvTable::with_storage(
            77,
            vec![column("a", 1), column("b", 2), column("c", 3)],
            Box::new(store),
        );
        if indexed {
            table
                .create_index(crate::kv_table::KvIndex {
                    id: 1,
                    name: "ib".to_owned(),
                    unique: false,
                    column_offsets: vec![1],
                })
                .unwrap();
        }
        for i in 1..=n {
            table
                .insert_row(&[Datum::Int(i), Datum::Int(i), Datum::Int(n - i)])
                .unwrap();
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        // Only the reads the query performs are interesting.
        entries.store(0, Ordering::Relaxed);
        gets.store(0, Ordering::Relaxed);
        (catalog, entries, gets)
    }

    fn first_column(rows: &[Vec<Datum>]) -> Vec<i64> {
        rows.iter()
            .map(|row| match row[0] {
                Datum::Int(v) => v,
                ref other => panic!("expected an integer, got {other:?}"),
            })
            .collect()
    }

    const ROWS: i64 = 5000;

    /// A full scan under a `LIMIT` stops at the cap: the rows past it are
    /// never advanced past, let alone decoded into memory.
    #[test]
    fn a_limit_stops_the_full_scan_instead_of_reading_the_relation() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows = run_select_on("SELECT a FROM t LIMIT 10", &catalog, &ctx).unwrap();
        assert_eq!(first_column(&rows), (1..=10).collect::<Vec<_>>());
        assert_eq!(
            entries.load(Ordering::Relaxed),
            10,
            "the scan read exactly the capped rows, not the {ROWS}-row relation"
        );

        // The same query without the LIMIT is the control: the counter really
        // does climb to the whole relation when nothing caps it.
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows = run_select_on("SELECT a FROM t", &catalog, &ctx).unwrap();
        assert_eq!(rows.len(), ROWS as usize);
        assert_eq!(entries.load(Ordering::Relaxed), ROWS as usize);
    }

    /// `LIMIT offset, count` reads `offset + count` rows -- Go's cop-side
    /// `Limit` for `limit 2, 3` is `offset:0, count:5`.
    #[test]
    fn an_offset_is_added_to_the_pushed_cap() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows = run_select_on("SELECT a FROM t LIMIT 2, 3", &catalog, &ctx).unwrap();
        assert_eq!(first_column(&rows), vec![3, 4, 5]);
        assert_eq!(entries.load(Ordering::Relaxed), 5);
    }

    /// A scan with no cap still hands out one chunk at a time: after a single
    /// `next` only a chunk's worth of rows has been read, so the source's
    /// live memory is a chunk and not the relation.
    #[test]
    fn a_scan_reads_one_chunk_per_pull_not_the_whole_range() {
        let store = CountingStorage::default();
        let entries = Arc::clone(&store.entries);
        let mut table = KvTable::with_storage(
            78,
            vec![column("a", 1)],
            Box::new(store) as Box<dyn TableStorage>,
        );
        for i in 1..=ROWS {
            table.insert_row(&[Datum::Int(i)]).unwrap();
        }
        entries.store(0, Ordering::Relaxed);

        let mut schema_column = tidb_expr::column::Column::new(1, long());
        schema_column.index = 0;
        let meta = ExecutorMeta::new(
            tidb_expr::schema::Schema::new(vec![schema_column]),
            0,
            1,
            1024,
        );
        let mut scan = crate::kv_table::TableScanExec::new(meta, table);
        scan.open().unwrap();
        let mut req = scan.new_chunk();
        scan.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 1024, "a full chunk, and no more");
        assert_eq!(
            entries.load(Ordering::Relaxed),
            1024,
            "the cursor advanced one chunk: the other {} rows are still \
             undecoded, so the source's live rows are a chunk, not the relation",
            ROWS - 1024
        );
        scan.close().unwrap();
    }

    /// An `ORDER BY` the index range already produces lets the cap through:
    /// the scan stops at `count` rows instead of reading the range and
    /// sorting it.
    #[test]
    fn an_order_by_the_index_satisfies_pushes_the_limit() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, gets) = table_of(ROWS, true);
        let rows = run_select_on(
            "SELECT a FROM t WHERE b > 0 ORDER BY b LIMIT 5",
            &catalog,
            &ctx,
        )
        .unwrap();
        assert_eq!(first_column(&rows), vec![1, 2, 3, 4, 5]);
        assert_eq!(
            entries.load(Ordering::Relaxed),
            5,
            "only five index entries were walked"
        );
        assert_eq!(
            gets.load(Ordering::Relaxed),
            5,
            "and only five rows were looked up"
        );
    }

    /// An `ORDER BY` on a column no index orders must NOT push: a sort has to
    /// see every row before it can name the first one. Go turns this case
    /// into a `TopN` over a scan that still reports all the rows (captured:
    /// `TopN_8` over `TableFullScan_16 | 20.00`).
    #[test]
    fn an_order_by_the_access_path_does_not_satisfy_reads_everything() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, true);
        let rows = run_select_on("SELECT a FROM t ORDER BY c LIMIT 5", &catalog, &ctx).unwrap();
        // c = ROWS - a descends, so the smallest c values are the last rows.
        assert_eq!(
            first_column(&rows),
            vec![ROWS, ROWS - 1, ROWS - 2, ROWS - 3, ROWS - 4]
        );
        assert_eq!(
            entries.load(Ordering::Relaxed),
            ROWS as usize,
            "the sort saw the whole relation, as it must"
        );
    }

    /// Nor may a DESC order by an indexed column push: the cursor is
    /// forward-only, so the index's ascending walk is the wrong end.
    #[test]
    fn a_descending_order_by_does_not_push() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, true);
        let rows = run_select_on(
            "SELECT a FROM t WHERE b > 0 ORDER BY b DESC LIMIT 3",
            &catalog,
            &ctx,
        )
        .unwrap();
        assert_eq!(first_column(&rows), vec![ROWS, ROWS - 1, ROWS - 2]);
        assert_eq!(entries.load(Ordering::Relaxed), ROWS as usize);
    }

    /// A conjunct the source could not take stays in a `Selection` above it,
    /// which can drop rows the cap already counted -- so nothing is pushed.
    #[test]
    fn a_residual_predicate_blocks_the_cap() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows =
            run_select_on("SELECT a FROM t WHERE c + 1 > 1 LIMIT 3", &catalog, &ctx).unwrap();
        assert_eq!(first_column(&rows), vec![1, 2, 3]);
        assert_eq!(
            entries.load(Ordering::Relaxed),
            ROWS as usize,
            "the arithmetic conjunct is residual, so the scan may not stop early"
        );
    }

    /// A predicate the scan DID take is applied below the cap, so the cap
    /// counts rows that passed it -- and the answer is the unfiltered one.
    #[test]
    fn a_pushed_predicate_is_applied_below_the_cap() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows = run_select_on("SELECT a FROM t WHERE a > 100 LIMIT 3", &catalog, &ctx).unwrap();
        assert_eq!(first_column(&rows), vec![101, 102, 103]);
        assert_eq!(
            entries.load(Ordering::Relaxed),
            103,
            "the scan read past the 100 rejected rows and stopped at the third kept one"
        );
    }

    /// `DISTINCT` can collapse rows above the source, so the cap would count
    /// rows the user never sees.
    #[test]
    fn distinct_blocks_the_cap() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows = run_select_on("SELECT DISTINCT a FROM t LIMIT 3", &catalog, &ctx).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(entries.load(Ordering::Relaxed), ROWS as usize);
    }

    /// `EXPLAIN ANALYZE`'s `actRows` for the scan reports the truncation: the
    /// counter the trace reads is the source's own, so it cannot drift from
    /// what the scan really did.
    #[test]
    fn explain_analyze_act_rows_reflect_the_truncation() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, _, _) = table_of(ROWS, false);
        let stmt = tidb_parser::parse("SELECT a FROM t WHERE a > 0 LIMIT 4").unwrap();
        let tidb_ast::Stmt::Query(query) = &stmt else {
            panic!("the test statement must parse as a query");
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            panic!("the test statement must parse as a SELECT");
        };
        let (_, rows) = crate::explain::explain_analyze_select_stmt(
            select,
            &catalog,
            "test",
            &ctx,
            ExplainFormat::Row,
        )
        .unwrap();
        // EXPLAIN cells are the wire's text bytes.
        let text = |cell: &Datum| match cell {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        };
        let scan = rows
            .iter()
            .find(|row| text(&row[0]).contains("TableFullScan"))
            .unwrap_or_else(|| {
                panic!(
                    "the plan has a full scan, got {:?}",
                    rows.iter().map(|r| text(&r[0])).collect::<Vec<_>>()
                )
            });
        assert_eq!(
            text(&scan[2]),
            "4",
            "the scan reports the four rows it read, not {ROWS}"
        );
    }
}
