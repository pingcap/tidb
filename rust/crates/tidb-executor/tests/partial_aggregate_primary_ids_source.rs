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

//! The partial-aggregate pushdown request carries the clustered primary's
//! column ids.
//!
//! Go `BuildTableScanFromInfos` fills `PrimaryColumnIds` from the table
//! whenever it IS common-handle -- whether or not `TableInfo.Indices` stores
//! a PRIMARY entry. TiKV recovers those columns from the record KEY through
//! this list; a clustered row VALUE does not carry them. A request without
//! the ids makes TiKV answer `missing data for NOT NULL column` for every
//! aggregate whose input is a primary column (`count(a)`, grouped reads),
//! which live Snapchat traffic hits on its `binary(16)` clustered keys.

#![allow(missing_docs)]

use tidb_executor::KvTableReader;
use std::sync::{Arc, Mutex};

use tidb_datatype::{FieldType, FieldTypeCode, SessionTimeZone};
use tidb_datatype::UNSPECIFIED_LENGTH;
use prost::Message;
use tidb_distsql::remote_scan::PushdownStatementContext;
use tidb_distsql::remote_scan::PushdownPartialAggregate;
use tidb_proto::tipb::DagRequest;
use tidb_kv::storage::{StorageError, StorageIterator, TableStorage};
use tidb_ranger::types::Range;
use tidb_table::kv_table::KvColumn;
use tidb_table::kv_table::KvIndex;
use tidb_table::kv_table::KvTable;
use tidb_txnkv::Key;

const TABLE_ID: i64 = 42;

#[derive(Debug)]
struct EmptyIterator;

impl StorageIterator for EmptyIterator {
    fn valid(&self) -> bool {
        false
    }
    fn key(&self) -> &Key {
        unreachable!("an empty iterator is never positioned");
    }
    fn value(&self) -> &[u8] {
        unreachable!("an empty iterator is never positioned");
    }
    fn next(&mut self) -> Result<(), StorageError> {
        Err(StorageError::NotFound)
    }
    fn close(&mut self) {}
}

/// The request a table built, as its region received it.
#[derive(Clone, Debug)]
struct Recorded {
    dag: DagRequest,
    desc: bool,
    keep_order: bool,
}

/// A region that records the request it was sent and answers no rows.
#[derive(Debug)]
struct RecordingRegion {
    captured: Arc<Mutex<Option<Recorded>>>,
}

#[derive(Debug)]
struct NoRows;

impl tidb_txnkv::Response for NoRows {
    fn next(
        &mut self,
    ) -> Result<Option<tidb_txnkv::QueryResultSubset>, tidb_txnkv::QueryResponseError> {
        Ok(None)
    }
    fn close(&mut self) {}
}

impl tidb_txnkv::Client for RecordingRegion {
    fn send(
        &self,
        request: &tidb_txnkv::TransportRequest,
    ) -> Result<Box<dyn tidb_txnkv::Response + Send>, String> {
        let metadata = request.metadata();
        let dag = DagRequest::decode(metadata.data.as_deref().expect("a DAG request"))
            .expect("the request is a TiDB DAG");
        *self.captured.lock().unwrap() = Some(Recorded {
            dag,
            desc: metadata.desc,
            keep_order: metadata.keep_order,
        });
        Ok(Box::new(NoRows))
    }
    fn is_request_type_supported(&self, _request_type: i64, _sub_type: i64) -> bool {
        true
    }
}

/// A store whose coprocessor client records the request the table BUILT.
#[derive(Debug)]
struct RecordingStore {
    captured: Arc<Mutex<Option<Recorded>>>,
}

impl TableStorage for RecordingStore {
    fn get(&mut self, _key: &Key) -> Result<Vec<u8>, StorageError> {
        Err(StorageError::NotFound)
    }
    fn set(&mut self, _key: Key, _value: Vec<u8>) -> Result<(), StorageError> {
        Ok(())
    }
    fn delete(&mut self, _key: Key) -> Result<(), StorageError> {
        Ok(())
    }
    fn iter(
        &mut self,
        _start: Option<&Key>,
        _upper_bound: Option<&Key>,
    ) -> Result<Box<dyn StorageIterator>, StorageError> {
        Ok(Box::new(EmptyIterator))
    }
    fn key_count(&self) -> usize {
        0
    }
    fn clear(&mut self) {}
    fn clone_box(&self) -> Box<dyn TableStorage> {
        Box::new(RecordingStore {
            captured: Arc::clone(&self.captured),
        })
    }
    fn client(&self) -> Option<Arc<dyn tidb_txnkv::Client>> {
        Some(Arc::new(RecordingRegion {
            captured: Arc::clone(&self.captured),
        }))
    }
    fn start_ts(&self) -> u64 {
        100
    }
}

fn binary_pk_column() -> KvColumn {
    // `binary(16)`'s declared type: MySQL TYPE_STRING under the binary
    // collation. The id matches what this tier's DDL allocates for the first
    // column of a fresh table.
    let mut field_type = FieldType::new(FieldTypeCode::String);
    field_type.set_flen(16);
    KvColumn {
        name: "a".to_owned(),
        id: 2,
        field_type,
        column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None,
    }
}

fn plain_column() -> KvColumn {
    KvColumn {
        name: "b".to_owned(),
        id: 4,
        field_type: FieldType::new(FieldTypeCode::LongLong),
        column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None,
    }
}

fn count_over_primary() -> PushdownPartialAggregate {
    let mut output_type = FieldType::new(FieldTypeCode::LongLong);
    output_type.set_flen(21);
    output_type.set_decimal(0);
    // Go's `COUNT(a)` argument is the column itself, with `a`'s type.
    let mut input = tidb_expr::column::Column::new(1, binary_pk_column().field_type);
    input.index = 0;
    PushdownPartialAggregate::Global {
        functions: vec![tidb_distsql::remote_scan::PushdownAggregateFunction {
            kind: tidb_distsql::remote_scan::PushdownAggregateKind::Count,
            input: Some(tidb_expr::expression::Expression::Column(input)),
            output_type,
        }],
        streamed: false,
    }
}

fn capture(table: &mut KvTable, captured: Arc<Mutex<Option<Recorded>>>) {
    let opened = table.pushdown_partial_aggregate_cursor(
        &[0],
        Vec::<tidb_expr::expression::Expression>::new().as_slice(),
        Option::<&[Range]>::None,
        &count_over_primary(),
        &tidb_sessionctx::StmtContext::default(),
        &SessionTimeZone::default(),
        &PushdownStatementContext::default(),
        false, // desc
        false, // keep_order
    );
    assert!(
        opened.unwrap().is_some(),
        "the recording region serves the aggregate"
    );
    assert!(
        captured.lock().unwrap().is_some(),
        "the request reached the region"
    );
}

fn recorded(captured: &Arc<Mutex<Option<Recorded>>>) -> Recorded {
    captured
        .lock()
        .unwrap()
        .as_ref()
        .cloned()
        .expect("a request was recorded")
}

/// This tier's CREATE TABLE stores no PRIMARY `KvIndex` for a clustered key:
/// the record key itself enforces it. The aggregate pushdown must still name
/// the primary columns, as Go does from the table info alone.
#[test]
fn a_clustered_table_without_a_stored_primary_index_sends_its_primary_ids() {
    let captured = Arc::new(Mutex::new(None));
    let mut table = KvTable::with_storage(
        TABLE_ID,
        vec![binary_pk_column(), plain_column()],
        Box::new(RecordingStore {
            captured: Arc::clone(&captured),
        }),
    );
    table.set_common_handle_offsets(vec![0]);

    capture(&mut table, Arc::clone(&captured));
    let request = recorded(&captured);
    assert!(!request.desc);
    assert!(!request.keep_order);
    let scan = request.dag.executors[0]
        .tbl_scan
        .as_ref()
        .expect("a table scan");
    assert_eq!(scan.primary_column_ids, vec![2]);
    // Whole-column key parts: nothing travels as a prefix.
    assert!(scan.primary_prefix_column_ids.is_empty());
    // The scanned column list itself stays the projection the caller asked for.
    assert_eq!(scan.columns.len(), 1);
    assert_eq!(scan.columns[0].column_id, Some(2));
}

/// A table that DOES store a PRIMARY `KvIndex` keeps the same ids -- the two
/// metadata shapes must not disagree about one wire field.
#[test]
fn a_stored_primary_index_yields_the_same_ids() {
    let captured = Arc::new(Mutex::new(None));
    let mut table = KvTable::with_storage(
        TABLE_ID,
        vec![binary_pk_column(), plain_column()],
        Box::new(RecordingStore {
            captured: Arc::clone(&captured),
        }),
    );
    table.set_common_handle_offsets(vec![0]);
    table.add_index(KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        column_offsets: vec![0],
        prefix_lengths: vec![UNSPECIFIED_LENGTH],
        visible: true,
        global: false,
        global_index_version: 0,
        clustered_primary: true,
        primary: true,
    }, true);

    capture(&mut table, Arc::clone(&captured));
    let request = recorded(&captured);
    assert!(!request.desc);
    assert!(!request.keep_order);
    let scan = request.dag.executors[0]
        .tbl_scan
        .as_ref()
        .expect("a table scan");
    assert_eq!(scan.primary_column_ids, vec![2]);
    assert!(scan.primary_prefix_column_ids.is_empty());
}
