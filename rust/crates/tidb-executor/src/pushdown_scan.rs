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

//! The seam a storage backend uses to serve a base-table scan *remotely*:
//! Go's coprocessor request, described here without naming distsql, tipb, or
//! a transport.
//!
//! # What this seam is for
//!
//! [`crate::storage::TableStorage`] speaks keys and bytes, so a scan through
//! it always moves the range's packed bytes to the client and decodes them
//! there. A cluster backend can do better: TiKV evaluates the predicate, the
//! row cap and the column projection at the region, and only the surviving
//! rows cross the network. That is not expressible as a key/value iterator --
//! the answer is *rows*, not pairs -- so it is a second, optional method of
//! the storage seam rather than a widening of `iter`.
//!
//! A backend that does not have a coprocessor (the in-process store) returns
//! `None` and the caller keeps its existing byte-level cursor. Nothing above
//! the seam changes shape.
//!
//! # The staged-buffer rule, made structural
//!
//! A coprocessor answers from the **snapshot** only. Inside an explicit
//! transaction the session's staged mutations are client-side (Go's
//! `MemBuffer` in front of `kv.Snapshot`, and Go's `UnionScan` on top of a
//! coprocessor reader), so a remote scan that returned only what TiKV saw
//! would lose every uncommitted row -- and would wrongly keep rows the
//! transaction has already deleted or changed out of the predicate.
//!
//! This seam therefore cannot hand back a row stream alone. [`PushdownScan`]
//! carries the stream *and* the session's staged writes for the same range,
//! so a caller physically cannot consume the remote rows without being handed
//! the overlay it has to merge. The caller re-applies the full pushed
//! predicate to the staged rows, exactly as Go's `UnionScan` filters its
//! membuffer rows through the same conditions.
//!
//! # Why the pushed predicate is best-effort and the answer is still exact
//!
//! The conjuncts in [`PushdownScanRequest::comparisons`] are a *request*. A
//! backend may lower all of them, some of them, or none -- whatever its
//! coprocessor lowering accepts -- because the caller keeps evaluating every
//! pushed conjunct itself on every row it emits, remote or staged. The remote
//! filter can therefore only ever return a superset of the answer, which the
//! local test narrows. The same holds for [`PushdownScanRequest::limit`]: it
//! is an early-stop hint, and the caller still enforces the cap.

use std::fmt;

use tidb_datatype::{Datum, FieldType};
use tidb_txnkv::Key;

use crate::scan_pushdown::ScanComparison;
use crate::storage::StorageError;

/// One column a remote scan must return, in the order the caller wants it.
#[derive(Clone, Debug, PartialEq)]
pub struct PushdownScanColumn {
    /// The table column's stable id, or [`EXTRA_HANDLE_COLUMN_ID`] for the
    /// synthetic handle column of a table whose handle is no column of its
    /// own.
    pub id: i64,
    /// The column's declared type, which decides how its bytes decode.
    pub field_type: FieldType,
    /// Whether the row handle *is* this column's value, so the backend reads
    /// it from the record key rather than from the row value (Go's
    /// `ColumnInfo.PkHandle`).
    pub is_handle: bool,
}

/// Go `model.ExtraHandleID`: the column id of the implicit `_tidb_rowid`
/// handle a table without an integer primary key carries.
pub const EXTRA_HANDLE_COLUMN_ID: i64 = -1;

/// One base-table scan a backend may serve remotely.
#[derive(Clone, Debug)]
pub struct PushdownScanRequest {
    /// The table whose record range is scanned.
    pub table_id: i64,
    /// The columns to return, in output order.
    pub columns: Vec<PushdownScanColumn>,
    /// Which of `columns` carries the row handle. The caller needs a handle
    /// for every remote row to merge the staged overlay by key; when the
    /// projection already contains the table's integer primary key this
    /// points at it, and otherwise the handle column was appended last and
    /// the caller drops it again before the row is emitted.
    pub handle_index: usize,
    /// The conjuncts the caller would like evaluated remotely. Best-effort:
    /// see the module doc.
    pub comparisons: Vec<ScanComparison>,
    /// A row cap the backend may stop at. Best-effort: see the module doc.
    pub limit: Option<u64>,
    /// Start of the scanned record range, inclusive.
    pub start: Key,
    /// End of the scanned record range, exclusive.
    pub end: Key,
}

/// A lazily pulled stream of snapshot rows a backend served remotely.
pub trait PushdownRowStream: Send {
    /// The next row in record-key order, as the requested columns, or `None`
    /// at the end of the answer.
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError>;

    /// How many rows have crossed the network so far. This is the wire
    /// receipt: with a lowered predicate it is smaller than the table holds.
    fn rows_returned(&self) -> u64;

    /// Releases the request, which an abandoned stream (an early-stopping
    /// `LIMIT`) must still do.
    fn close(&mut self);
}

/// A remote scan plus the client-side overlay it must be merged with.
pub struct PushdownScan {
    /// The snapshot rows, filtered and capped at the backend.
    pub stream: Box<dyn PushdownRowStream>,
    /// The session's staged writes inside the scanned range, in key order and
    /// still encoded: the caller owns decoding them, because row layout is
    /// not this seam's business. `None` is a staged delete.
    pub staged: Vec<(Key, Option<Vec<u8>>)>,
}

impl fmt::Debug for PushdownScan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PushdownScan")
            .field("rows_returned", &self.stream.rows_returned())
            .field("staged", &self.staged.len())
            .finish()
    }
}

/// The capability a cluster backend is given so it can serve
/// [`PushdownScanRequest`]s: one coprocessor round trip per open scan.
///
/// It is a separate trait from the storage itself because the storage lives
/// in this crate while the transport does not: the production implementation
/// is injected from the crate that owns distsql.
pub trait PushdownScanner: fmt::Debug + Send + Sync {
    /// Opens one remote scan. An `Err` is a backend failure, not a refusal:
    /// a backend that cannot serve this request shape must say so by
    /// returning [`PushdownScannerError::Unsupported`], which makes the caller
    /// fall back to the byte-level cursor with no change in answer.
    fn open(
        &self,
        request: &PushdownScanRequest,
    ) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError>;
}

/// Why a remote scan did not open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PushdownScannerError {
    /// The backend declines this request shape; the caller must use the
    /// byte-level cursor instead. Never a wrong answer, only a slower one.
    Unsupported(String),
    /// The backend tried and failed.
    Backend(StorageError),
}

impl fmt::Display for PushdownScannerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(reason) => write!(formatter, "remote scan is unsupported: {reason}"),
            Self::Backend(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for PushdownScannerError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use tidb_datatype::FieldTypeCode;
    use tidb_txnkv::Key;

    use super::*;
    use crate::cluster_storage::{
        ClusterSnapshot, ClusterTableStorage, MutationBuffer, SnapshotPairs,
    };
    use crate::driver::{run_select_on, Catalog};
    use crate::kv_table::{KvColumn, KvTable, TableHandle};
    use crate::scan_pushdown::ScanComparisonOp;
    use crate::storage::{MemTableStorage, TableStorage};

    /// The committed half of a cluster read, shared by the snapshot the
    /// session reads through and by the coprocessor below it.
    #[derive(Debug, Default)]
    struct MockSnapshot {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl ClusterSnapshot for MockSnapshot {
        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(key.as_bytes()).cloned())
        }

        fn scan(&mut self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    /// A coprocessor standing in for TiKV: it reads the committed half only,
    /// evaluates the requested comparisons and cap there, and returns rows.
    ///
    /// It is deliberately blind to the session's staged writes -- that is
    /// precisely the property the merge has to repair, and a fake that quietly
    /// saw the buffer would prove nothing.
    #[derive(Debug)]
    struct FakeCoprocessor {
        snapshot: Arc<Mutex<MockSnapshot>>,
        columns: Vec<KvColumn>,
        /// Rows that crossed the wire, across every scan.
        returned: Arc<AtomicU64>,
        /// Rows the coprocessor read before its own filter.
        scanned: Arc<AtomicU64>,
    }

    impl PushdownScanner for FakeCoprocessor {
        fn open(
            &self,
            request: &PushdownScanRequest,
        ) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError> {
            // The region's committed bytes for the requested key range.
            let mut store = MemTableStorage::new();
            {
                let mut snapshot = self.snapshot.lock().unwrap();
                for (key, value) in snapshot.scan(&request.start, &request.end).unwrap() {
                    store.set(Key::from_bytes(key), value).unwrap();
                }
            }
            let mut table =
                KvTable::with_storage(request.table_id, self.columns.clone(), Box::new(store));
            // Every requested column that is one of the table's own; the
            // appended handle column is not, and is filled from the key.
            let appended_handle =
                request.columns[request.handle_index].id == EXTRA_HANDLE_COLUMN_ID;
            let projected = if appended_handle {
                &request.columns[..request.columns.len() - 1]
            } else {
                &request.columns[..]
            };
            let keep: Vec<usize> = projected
                .iter()
                .map(|column| {
                    self.columns
                        .iter()
                        .position(|candidate| candidate.id == column.id)
                        .expect("a requested column belongs to the table")
                })
                .collect();
            let mut cursor = table.row_cursor_projected(Some(&keep)).unwrap();
            let mut rows = Vec::new();
            while let Some((handle, mut row)) = cursor.next_row().unwrap() {
                self.scanned.fetch_add(1, Ordering::Relaxed);
                if !request
                    .comparisons
                    .iter()
                    .all(|comparison| admits(comparison, &row))
                {
                    continue;
                }
                if appended_handle {
                    row.push(Datum::Int(handle.int_value().unwrap()));
                }
                rows.push(row);
                if request.limit.is_some_and(|cap| rows.len() as u64 >= cap) {
                    break;
                }
            }
            self.returned
                .fetch_add(rows.len() as u64, Ordering::Relaxed);
            Ok(Box::new(FakeStream {
                rows: rows.into_iter(),
                returned: 0,
            }))
        }
    }

    /// Evaluates one comparison the way the coprocessor would, over the
    /// integer domain the lowering accepts.
    fn admits(comparison: &ScanComparison, row: &[Datum]) -> bool {
        let (Some(Datum::Int(value)), Datum::Int(literal)) = (
            row.get(comparison.column_offset as usize),
            comparison.literal.clone(),
        ) else {
            return true;
        };
        let (left, right) = if comparison.column_on_left {
            (*value, literal)
        } else {
            (literal, *value)
        };
        match comparison.op {
            ScanComparisonOp::Eq => left == right,
            ScanComparisonOp::Ne => left != right,
            ScanComparisonOp::Lt => left < right,
            ScanComparisonOp::Le => left <= right,
            ScanComparisonOp::Gt => left > right,
            ScanComparisonOp::Ge => left >= right,
        }
    }

    struct FakeStream {
        rows: std::vec::IntoIter<Vec<Datum>>,
        returned: u64,
    }

    impl PushdownRowStream for FakeStream {
        fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
            let row = self.rows.next();
            if row.is_some() {
                self.returned += 1;
            }
            Ok(row)
        }

        fn rows_returned(&self) -> u64 {
            self.returned
        }

        fn close(&mut self) {}
    }

    fn column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            default_value: None,
            origin_default: None,
        }
    }

    fn commit(buffer: &MutationBuffer, snapshot: &Arc<Mutex<MockSnapshot>>) {
        let mut snapshot = snapshot.lock().unwrap();
        for (key, value) in buffer.staged() {
            match value {
                Some(value) => snapshot.data.insert(key.as_bytes().to_vec(), value),
                None => snapshot.data.remove(key.as_bytes()),
            };
        }
        buffer.reset();
    }

    struct Fixture {
        table: KvTable,
        buffer: MutationBuffer,
        snapshot: Arc<Mutex<MockSnapshot>>,
        returned: Arc<AtomicU64>,
        scanned: Arc<AtomicU64>,
    }

    /// A cluster-backed `t(a, b)` whose scans go through the coprocessor.
    fn fixture() -> Fixture {
        let snapshot = Arc::new(Mutex::new(MockSnapshot::default()));
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        let buffer = MutationBuffer::new();
        let columns = vec![column("a", 1), column("b", 2)];
        let returned = Arc::new(AtomicU64::new(0));
        let scanned = Arc::new(AtomicU64::new(0));
        let scanner = Arc::new(FakeCoprocessor {
            snapshot: Arc::clone(&snapshot),
            columns: columns.clone(),
            returned: Arc::clone(&returned),
            scanned: Arc::clone(&scanned),
        });
        let storage = ClusterTableStorage::new(buffer.clone(), handle)
            .with_pushdown_scanner(scanner as Arc<dyn PushdownScanner>);
        Fixture {
            table: KvTable::with_storage(91, columns, Box::new(storage)),
            buffer,
            snapshot,
            returned,
            scanned,
        }
    }

    fn catalog_of(table: KvTable) -> Catalog {
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        catalog
    }

    /// The wire win: with a predicate lowered into the request, the rows that
    /// cross the network are the qualifying ones and not the relation.
    #[test]
    fn a_pushed_predicate_keeps_the_rejected_rows_off_the_wire() {
        let mut fixture = fixture();
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)])
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.returned.store(0, Ordering::Relaxed);
        fixture.scanned.store(0, Ordering::Relaxed);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let rows = run_select_on("SELECT a FROM t WHERE a > 97", &catalog, &ctx).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(98)],
                vec![Datum::Int(99)],
                vec![Datum::Int(100)]
            ]
        );
        assert_eq!(
            fixture.scanned.load(Ordering::Relaxed),
            100,
            "the coprocessor read the relation, as a full scan must"
        );
        assert_eq!(
            fixture.returned.load(Ordering::Relaxed),
            3,
            "but only the qualifying rows crossed the network"
        );
    }

    /// A cap travels with the request when nothing is staged, so the
    /// coprocessor stops reading instead of returning the relation.
    #[test]
    fn a_pushed_limit_stops_the_remote_scan() {
        let mut fixture = fixture();
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)])
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.scanned.store(0, Ordering::Relaxed);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let rows = run_select_on("SELECT a FROM t LIMIT 4", &catalog, &ctx).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(
            fixture.scanned.load(Ordering::Relaxed),
            4,
            "the cap reached the coprocessor, which stopped there"
        );
    }

    /// The correctness core. A coprocessor answers from the snapshot, so the
    /// transaction's own staged writes must be merged back in and filtered by
    /// the same predicate: this is the remote twin of the byte-level test in
    /// `crate::scan_pushdown`, and it must produce the identical answer.
    #[test]
    fn staged_rows_survive_the_remote_scan_and_are_filtered_by_the_same_predicate() {
        let mut fixture = fixture();
        let committed_low = fixture
            .table
            .insert_row(&[Datum::Int(1), Datum::Int(10)])
            .unwrap();
        fixture
            .table
            .insert_row(&[Datum::Int(9), Datum::Int(90)])
            .unwrap();
        let committed_moved = fixture
            .table
            .insert_row(&[Datum::Int(2), Datum::Int(20)])
            .unwrap();
        commit(&fixture.buffer, &fixture.snapshot);

        // One open transaction stages all four shapes.
        fixture
            .table
            .insert_row(&[Datum::Int(7), Datum::Int(70)])
            .unwrap();
        fixture
            .table
            .insert_row(&[Datum::Int(3), Datum::Int(30)])
            .unwrap();
        fixture
            .table
            .update_row(&committed_moved, &[Datum::Int(8), Datum::Int(80)])
            .unwrap();
        fixture.table.delete_row(&committed_low).unwrap();
        assert!(!fixture.buffer.is_empty(), "the writes are staged");

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        assert_eq!(
            run_select_on("SELECT a, b FROM t WHERE a > 5 ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(7), Datum::Int(70)],
                vec![Datum::Int(8), Datum::Int(80)],
                vec![Datum::Int(9), Datum::Int(90)],
            ],
            "a staged INSERT and a staged UPDATE that satisfy the predicate are \
             kept, and the staged row that does not is dropped"
        );
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE a < 5 ORDER BY a", &catalog, &ctx).unwrap(),
            vec![vec![Datum::Int(3)]],
            "the staged DELETE hid the committed row the coprocessor still \
             returns, and the updated row's old value went with it"
        );
        assert_eq!(
            run_select_on("SELECT a FROM t ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(7)],
                vec![Datum::Int(8)],
                vec![Datum::Int(9)],
            ],
            "and the merged relation itself is the union-scan answer"
        );
    }

    /// A cap must not travel while writes are staged: the coprocessor's first
    /// `n` snapshot rows are the wrong prefix once a staged delete uncovers a
    /// row past them.
    #[test]
    fn a_cap_does_not_travel_while_writes_are_staged() {
        let mut fixture = fixture();
        for a in 1..=6 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)])
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        // A cap of three applied at the coprocessor would have returned rows
        // 1..3, of which only one survives the overlay.
        fixture.table.delete_row(&TableHandle::Int(1)).unwrap();
        fixture.table.delete_row(&TableHandle::Int(2)).unwrap();

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        assert_eq!(
            run_select_on("SELECT a FROM t LIMIT 3", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(4)],
                vec![Datum::Int(5)]
            ],
            "the cap is enforced on the merged stream, not on the snapshot's prefix"
        );
    }

    /// The remote path may only ever narrow: with the coprocessor lowering
    /// nothing of the predicate, the local test still answers exactly.
    #[test]
    fn an_unlowered_predicate_is_still_answered_exactly() {
        let mut fixture = fixture();
        for a in 1..=20 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)])
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE b + 1 > 190", &catalog, &ctx).unwrap(),
            vec![vec![Datum::Int(19)], vec![Datum::Int(20)]],
            "the arithmetic conjunct is residual, so it runs above the scan"
        );
    }
}
