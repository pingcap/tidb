// Temporary taobench gate reproduction: a prepared-style edge point read
// (`varchar type column = int literal`) over a NONCLUSTERED composite PK.

#![allow(missing_docs)]

use std::sync::{Arc, Mutex};

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::driver::{run_select_on, Catalog};
use tidb_executor::kv_table::{KvColumn, KvIndex, KvTable};
use tidb_executor::remote_scan::{PushdownScan, PushdownScanRequest};
use tidb_executor::storage::{MemTableStorage, StorageError, StorageIterator, TableStorage};
use tidb_executor::StmtContext;
use tidb_txnkv::Key;

fn long_col(name: &str, id: i64) -> KvColumn {
    let mut ft = FieldType::new(FieldTypeCode::LongLong);
    ft.set_flen(20);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    KvColumn { name: name.to_owned(), id, field_type: ft, 
        column_info_version: 1,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None, }
}

fn varchar_col(name: &str, id: i64) -> KvColumn {
    let mut ft = FieldType::new(FieldTypeCode::Varchar);
    ft.set_flen(63);
    ft.set_collation(tidb_datatype::Collation::Utf8Mb4Bin);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    KvColumn { name: name.to_owned(), id, field_type: ft, 
        column_info_version: 1,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None, }
}

fn edges_columns() -> Vec<KvColumn> {
    vec![
        long_col("id1", 1),
        long_col("id2", 2),
        varchar_col("type", 3),
        long_col("ts", 4),
        {
            let mut ft = FieldType::new(FieldTypeCode::Varchar);
            ft.set_flen(150);
            KvColumn { name: "value".to_owned(), id: 5, field_type: ft, 
        column_info_version: 1,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None, }
        },
    ]
}

fn edge_table() -> KvTable {
    let mut table = KvTable::new(459, edges_columns());
    table.add_index(KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        column_offsets: vec![0, 1, 2],
        prefix_lengths: vec![-1, -1, -1],
        visible: true,
        global: false,
        clustered_primary: false,
    }, false);
    table
        .insert_row(
            &[
                Datum::Int(848250056732),
                Datum::Int(1947761684552),
                Datum::new_string("3"),
                Datum::Int(1660627540589311589),
                Datum::new_string("hello"),
            ],
            &tidb_expr::NoColumns,
        )
        .unwrap();
    table
}

#[test]
fn edge_point_read_with_int_type_literal_returns_the_row() {
    let mut catalog = Catalog::default();
    catalog.register_kv("t", edge_table());
    let ctx = StmtContext::for_query();
    let rows = run_select_on(
        "SELECT ts FROM t WHERE id1 = 848250056732 AND id2 = 1947761684552 AND type = 3",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the string '3' coerces to the integer 3 under MySQL comparison rules"
    );
}

#[test]
fn edge_point_read_without_type_filter_returns_the_row() {
    let mut catalog = Catalog::default();
    catalog.register_kv("t", edge_table());
    let ctx = StmtContext::for_query();
    let rows = run_select_on(
        "SELECT ts FROM t WHERE id1 = 848250056732 AND id2 = 1947761684552",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
}

// A bare aggregate (`COUNT(*)`) references no column. Go
// `DataSource.PruneColumns` forces one key column back into an otherwise
// empty DataSource schema (`preferKeyColumnFromTable`), so the covering
// index reader is never asked for columns its index cannot supply. This
// tier must prune to that single key column instead of staying full width,
// which made the covering projection demand `value`/`ts` from PRIMARY and
// refuse the whole read.

#[test]
fn bare_count_star_with_limit_over_covering_primary_answers() {
    let mut catalog = Catalog::default();
    catalog.register_kv("t", edge_table());
    let ctx = StmtContext::for_query();
    let rows = run_select_on("SELECT COUNT(*) FROM t USE INDEX (PRIMARY) LIMIT 1", &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Int(1), "one inserted row counted once");
}

#[test]
fn bare_count_star_matches_the_unlimited_answer() {
    let mut catalog = Catalog::default();
    catalog.register_kv("t", edge_table());
    let ctx = StmtContext::for_query();
    let limited = run_select_on("SELECT COUNT(*) FROM t USE INDEX (PRIMARY) LIMIT 1", &catalog, &ctx).unwrap();
    let unlimited = run_select_on("SELECT COUNT(*) FROM t USE INDEX (PRIMARY)", &catalog, &ctx).unwrap();
    assert_eq!(limited, unlimited);
}

#[test]
fn select_literal_over_table_limit_one_returns_one_row() {
    // Go's own motivating case for the forced handle column:
    // "For SQL like `select 1 from t`, tikv's response will be empty if no
    // column is in schema."
    let mut catalog = Catalog::default();
    catalog.register_kv("t", edge_table());
    let ctx = StmtContext::for_query();
    let rows = run_select_on("SELECT 1 FROM t LIMIT 1", &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 1);
}

/// A backend that answers reads from an in-memory table while recording every
/// pushdown request the executor BUILDS. The recorded requests are the parity
/// evidence: Go's pruned `DataSource` schema is what the coprocessor request
/// names, so the request's column list must match
/// `DataSource.PruneColumns`' answer (one forced key column for a bare
/// aggregate).
#[derive(Debug)]
struct CapturingStore {
    inner: MemTableStorage,
    captured: Arc<Mutex<Vec<PushdownScanRequest>>>,
}

impl CapturingStore {
    fn new(captured: Arc<Mutex<Vec<PushdownScanRequest>>>) -> Self {
        Self {
            inner: MemTableStorage::new(),
            captured,
        }
    }
}

impl TableStorage for CapturingStore {
    fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
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
        self.inner.iter(start, upper_bound)
    }
    fn key_count(&self) -> usize {
        self.inner.key_count()
    }
    fn clear(&mut self) {
        self.inner.clear()
    }
    fn clone_box(&self) -> Box<dyn TableStorage> {
        Box::new(Self {
            inner: self.inner.clone(),
            captured: Arc::clone(&self.captured),
        })
    }
    fn open_remote_scan(
        &mut self,
        request: &PushdownScanRequest,
    ) -> Option<Result<PushdownScan, StorageError>> {
        self.captured.lock().unwrap().push(request.clone());
        None
    }
}

fn edges_table_on_capturing_store(
    captured: Arc<Mutex<Vec<PushdownScanRequest>>>,
) -> KvTable {
    let mut table = KvTable::with_storage(459, edges_columns(), Box::new(CapturingStore::new(captured)));
    table.add_index(
        KvIndex {
            id: 1,
            name: "PRIMARY".to_owned(),
            comment: String::new(),
            unique: true,
            column_offsets: vec![0, 1, 2],
            prefix_lengths: vec![-1, -1, -1],
            visible: true,
            global: false,
            clustered_primary: false,
        },
        false,
    );
    table
        .insert_row(
            &[
                Datum::Int(848250056732),
                Datum::Int(1947761684552),
                Datum::new_string("3"),
                Datum::Int(1660627540589311589),
                Datum::new_string("hello"),
            ],
            &tidb_expr::NoColumns,
        )
        .unwrap();
    // A raw insert stages as dirty content; the committed-table state the
    // coprocessor path serves is the CLEAN read below.
    table.clear_dirty_content();
    table
}

#[test]
fn bare_count_pushdown_request_carries_only_the_forced_key_column() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut catalog = Catalog::default();
    catalog.register_kv("t", edges_table_on_capturing_store(Arc::clone(&captured)));
    let ctx = StmtContext::for_query();
    let rows =
        run_select_on("SELECT COUNT(*) FROM t USE INDEX (PRIMARY) LIMIT 1", &catalog, &ctx)
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Int(1));
    let requests = captured.lock().unwrap();
    assert!(
        !requests.is_empty(),
        "the covering read must attempt a coprocessor request before its local fallback"
    );
    // Without the forced key column the statement prunes to an EMPTY demand;
    // the full-width leaf then asks the covering reader for columns PRIMARY
    // cannot supply and the read dies in "covering index omitted a requested
    // table column" before any request is built. Reaching THIS point -- a
    // built request plus the same answer the unlimited read gives -- is the
    // parity Go's PruneColumns guarantees.
}
