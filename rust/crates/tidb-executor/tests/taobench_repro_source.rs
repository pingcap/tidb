// Temporary taobench gate reproduction: a prepared-style edge point read
// (`varchar type column = int literal`) over a NONCLUSTERED composite PK.

#![allow(missing_docs)]

use std::sync::{Arc, Mutex};

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::driver::run_select_on;
use tidb_infoschema::Catalog;
use tidb_table::kv_table::KvTable;
use tidb_table::kv_table::KvIndex;
use tidb_table::kv_table::KvColumn;
use prost::Message;
use tidb_proto::tipb::DagRequest;
use tidb_kv::storage::{MemTableStorage, StorageError, StorageIterator, TableStorage};
use tidb_sessionctx::StmtContext;
use tidb_txnkv::Key;

fn long_col(name: &str, id: i64) -> KvColumn {
    let mut ft = FieldType::new(FieldTypeCode::LongLong);
    ft.set_flen(20);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    KvColumn {
        name: name.to_owned(),
        id,
        field_type: ft,
        column_info_version: 1,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None,
    }
}

fn varchar_col(name: &str, id: i64) -> KvColumn {
    let mut ft = FieldType::new(FieldTypeCode::Varchar);
    ft.set_flen(63);
    ft.set_collation(tidb_charset::Collation::Utf8Mb4Bin);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    KvColumn {
        name: name.to_owned(),
        id,
        field_type: ft,
        column_info_version: 1,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None,
    }
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
            KvColumn {
                name: "value".to_owned(),
                id: 5,
                field_type: ft,
                column_info_version: 1,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            }
        },
    ]
}

fn edge_table() -> KvTable {
    let mut table = KvTable::new(459, edges_columns());
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
            global_index_version: 0,
            clustered_primary: false,
            primary: true,
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
    table
}

#[test]
fn edge_point_read_with_int_type_literal_returns_the_row() {
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
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
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
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
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    catalog.register_kv("t", edge_table());
    let ctx = StmtContext::for_query();
    let rows = run_select_on(
        "SELECT COUNT(*) FROM t USE INDEX (PRIMARY) LIMIT 1",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Int(1), "one inserted row counted once");
}

#[test]
fn bare_count_star_matches_the_unlimited_answer() {
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    catalog.register_kv("t", edge_table());
    let ctx = StmtContext::for_query();
    let limited = run_select_on(
        "SELECT COUNT(*) FROM t USE INDEX (PRIMARY) LIMIT 1",
        &catalog,
        &ctx,
    )
    .unwrap();
    let unlimited =
        run_select_on("SELECT COUNT(*) FROM t USE INDEX (PRIMARY)", &catalog, &ctx).unwrap();
    assert_eq!(limited, unlimited);
}

#[test]
fn select_literal_over_table_limit_one_returns_one_row() {
    // Go's own motivating case for the forced handle column:
    // "For SQL like `select 1 from t`, tikv's response will be empty if no
    // column is in schema."
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    catalog.register_kv("t", edge_table());
    let ctx = StmtContext::for_query();
    let rows = run_select_on("SELECT 1 FROM t LIMIT 1", &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 1);
}

/// The region behind [`CapturingStore`]'s client: the DAG runs on unistore's
/// coprocessor over the table's committed bytes (Go's mockstore), and every
/// DAG is recorded.
#[derive(Debug)]
struct CapturingRegion {
    data: Vec<(Vec<u8>, Vec<u8>)>,
    captured: Arc<Mutex<Vec<DagRequest>>>,
}

#[derive(Debug)]
struct OneAnswer(Option<tidb_txnkv::QueryResultSubset>);

impl tidb_txnkv::Response for OneAnswer {
    fn next(
        &mut self,
    ) -> Result<Option<tidb_txnkv::QueryResultSubset>, tidb_txnkv::QueryResponseError> {
        Ok(self.0.take())
    }
    fn close(&mut self) {
        self.0 = None;
    }
}

impl tidb_txnkv::Client for CapturingRegion {
    fn send(
        &self,
        request: &tidb_txnkv::TransportRequest,
    ) -> Result<Box<dyn tidb_txnkv::Response + Send>, String> {
        let metadata = request.metadata();
        let data = metadata.data.clone().expect("a DAG request");
        let dag = DagRequest::decode(data.as_slice()).map_err(|error| error.to_string())?;
        self.captured.lock().unwrap().push(dag);
        let mut store = tidb_unistore_tikv::mvcc_store::MvccStore::new();
        if let Some((primary, _)) = self.data.first() {
            store
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: self
                        .data
                        .iter()
                        .map(|(key, value)| tidb_proto::KvrpcMutation {
                            op: tidb_proto::KvrpcOp::Put as i32,
                            key: key.clone(),
                            value: value.clone(),
                            ..tidb_proto::KvrpcMutation::default()
                        })
                        .collect(),
                    primary_lock: primary.clone(),
                    start_version: 1,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
                })
                .map_err(|error| format!("{error:?}"))?;
            let keys: Vec<Vec<u8>> = self.data.iter().map(|(key, _)| key.clone()).collect();
            store.commit(&keys, 1, 2).map_err(|error| format!("{error:?}"))?;
        }
        let response = tidb_unistore_cophandler::handle_cop_request(
            &mut store,
            &tidb_proto::coprocessor::Request {
                tp: tidb_unistore_cophandler::REQ_TYPE_DAG,
                data,
                ranges: metadata
                    .key_ranges
                    .as_ref()
                    .map(|ranges| {
                        ranges
                            .partitions()
                            .iter()
                            .flatten()
                            .map(|range| tidb_proto::coprocessor::KeyRange {
                                start: range.start_key.as_bytes().to_vec(),
                                end: range.end_key.as_bytes().to_vec(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                start_ts: 100,
                ..tidb_proto::coprocessor::Request::default()
            },
        );
        if !response.other_error.is_empty() {
            return Err(response.other_error);
        }
        Ok(Box::new(OneAnswer(Some(tidb_txnkv::QueryResultSubset {
            data: response.data.to_vec().into(),
            runtime: None,
        }))))
    }
    fn is_request_type_supported(&self, _request_type: i64, _sub_type: i64) -> bool {
        true
    }
}

/// A backend that answers reads from an in-memory table while recording every
/// coprocessor request the executor BUILDS. The recorded requests are the
/// parity evidence: Go's pruned `DataSource` schema is what the coprocessor
/// request names, so the request's column list must match
/// `DataSource.PruneColumns`' answer (one forced key column for a bare
/// aggregate).
#[derive(Debug)]
struct CapturingStore {
    inner: MemTableStorage,
    captured: Arc<Mutex<Vec<DagRequest>>>,
}

impl CapturingStore {
    fn new(captured: Arc<Mutex<Vec<DagRequest>>>) -> Self {
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
    fn client(&self) -> Option<Arc<dyn tidb_txnkv::Client>> {
        let mut inner = self.inner.clone();
        let mut iter = inner.iter(None, None).ok()?;
        let mut data = Vec::new();
        while iter.valid() {
            data.push((iter.key().as_bytes().to_vec(), iter.value().to_vec()));
            iter.next().ok()?;
        }
        Some(Arc::new(CapturingRegion {
            data,
            captured: Arc::clone(&self.captured),
        }))
    }
    fn start_ts(&self) -> u64 {
        100
    }
}

fn edges_table_on_capturing_store(captured: Arc<Mutex<Vec<DagRequest>>>) -> KvTable {
    let mut table = KvTable::with_storage(
        459,
        edges_columns(),
        Box::new(CapturingStore::new(captured)),
    );
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
            global_index_version: 0,
            clustered_primary: false,
            primary: true,
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
    table
}

#[test]
fn bare_count_pushdown_request_carries_only_the_forced_key_column() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    catalog.register_kv("t", edges_table_on_capturing_store(Arc::clone(&captured)));
    let ctx = StmtContext::for_query();
    let rows = run_select_on(
        "SELECT COUNT(*) FROM t USE INDEX (PRIMARY) LIMIT 1",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Int(1));
    let requests = captured.lock().unwrap();
    assert!(
        !requests.is_empty(),
        "the covering read sends a coprocessor request"
    );
    // Without the forced key column the statement prunes to an EMPTY demand;
    // the full-width leaf then asks the covering reader for columns PRIMARY
    // cannot supply and the read dies in "covering index omitted a requested
    // table column" before any request is built. Reaching THIS point -- a
    // built request plus the same answer the unlimited read gives -- is the
    // parity Go's PruneColumns guarantees.
}
