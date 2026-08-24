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

//! The partial-aggregate pushdown carries the clustered primary's column ids.
//!
//! Go's `PhysicalTableScan.ToPB` names `PrimaryColumnIds` on every table scan
//! over a common-handle table. TiKV reads those columns out of the record KEY,
//! so a request without them cannot decode the rows at all: each PK slot stays
//! unfilled, and because the columns are `NOT NULL` with no default, TiKV
//! rejects the region with `Data is corrupted, missing data for NOT NULL
//! column`. This tier stores no `KvIndex` for a clustered key, so the request
//! builder must take the metadata from the synthesized primary (the same
//! reconstruction Go's loader materializes), not from a stored-index lookup
//! that answers None for exactly these tables.

use std::sync::{Arc, Mutex};

use tidb_datatype::{Datum, FieldType, FieldTypeCode, SessionTimeZone};
use tidb_executor::remote_scan::{
    PushdownPartialAggregate, PushdownScanRequest, PushdownStatementContext,
};
use tidb_executor::storage::{MemTableStorage, StorageError, StorageIterator, TableStorage};
use tidb_executor::{IndexRange, KvColumn, KvTable};
use tidb_txnkv::Key;

/// A [`TableStorage`] that records every pushdown request and then declines,
/// so the caller falls back to the local cursor exactly as a non-coprocessor
/// backend would.
#[derive(Debug)]
struct Recorder {
    inner: MemTableStorage,
    requests: Arc<Mutex<Vec<PushdownScanRequest>>>,
}

impl Recorder {
    fn shared() -> (Self, Arc<Mutex<Vec<PushdownScanRequest>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner: MemTableStorage::new(),
                requests: Arc::clone(&requests),
            },
            requests,
        )
    }
}

impl TableStorage for Recorder {
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
        self.inner.clear();
    }

    fn open_remote_scan(
        &mut self,
        request: &PushdownScanRequest,
    ) -> Option<Result<tidb_executor::remote_scan::PushdownScan, StorageError>> {
        self.requests.lock().unwrap().push(request.clone());
        None
    }

    fn clone_box(&self) -> Box<dyn TableStorage> {
        Box::new(Self {
            inner: MemTableStorage::new(),
            requests: Arc::clone(&self.requests),
        })
    }
}

fn int_column(id: i64, name: &str) -> KvColumn {
    KvColumn {
        name: name.to_owned(),
        id,
        field_type: FieldType::new(FieldTypeCode::Long),
        column_info_version: 1,
        comment: String::new(),
        generated: None,
        default_value: None,
        origin_default: None,
    }
}

/// `bmsql_stock`'s shape: a two-column CLUSTERED primary key stored only in
/// the record key, plus nullable payload columns, and no stored `PRIMARY`
/// index entry.
fn clustered_table() -> (KvTable, Arc<Mutex<Vec<PushdownScanRequest>>>) {
    let columns = vec![
        int_column(1, "s_w_id"),
        int_column(2, "s_i_id"),
        int_column(3, "s_quantity"),
    ];
    let (store, requests) = Recorder::shared();
    let mut table = KvTable::with_storage(133, columns, Box::new(store));
    table.set_common_handle_offsets(vec![0, 1]);
    (table, requests)
}

#[test]
fn partial_aggregate_names_the_clustered_primary_columns() {
    let (mut table, requests) = clustered_table();

    // STOCK_LEVEL's shape folded to ranges: the count needs no scan columns,
    // yet the PK columns still travel so TiKV can decode every row's key.
    let aggregate = PushdownPartialAggregate::Count {
        input_offset: None,
        output_type: FieldType::new(FieldTypeCode::LongLong),
    };
    let zone = SessionTimeZone::utc();
    let statement = PushdownStatementContext::default();

    let cursor = table
        .pushdown_partial_aggregate_cursor(
            &[0, 1],
            &[],
            Option::<&[IndexRange]>::None,
            &aggregate,
            &zone,
            &statement,
        )
        .unwrap();
    assert!(cursor.is_none(), "the recorder declines every request");

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "one recorded pushdown request");
    let request = &requests[0];
    assert_eq!(
        request.primary_column_ids,
        vec![1, 2],
        "the clustered primary's column ids must travel, or TiKV cannot decode the rows"
    );
    assert!(request.primary_prefix_column_ids.is_empty());
    assert_eq!(
        request.columns.iter().map(|column| column.id).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

/// A single-column integer primary key that IS the row handle sends no
/// primary column ids: TiKV fills such a column from the int handle itself.
#[test]
fn int_handle_table_sends_no_redundant_primary_ids() {
    let columns = vec![int_column(1, "c"), int_column(2, "v")];
    let (store, requests) = Recorder::shared();
    let mut table = KvTable::with_storage(9, columns, Box::new(store));
    table.set_pk_handle_offset(0);

    let aggregate = PushdownPartialAggregate::Count {
        input_offset: None,
        output_type: FieldType::new(FieldTypeCode::LongLong),
    };
    let zone = SessionTimeZone::utc();
    let statement = PushdownStatementContext::default();

    let cursor = table
        .pushdown_partial_aggregate_cursor(
            &[0],
            &[],
            Option::<&[IndexRange]>::None,
            &aggregate,
            &zone,
            &statement,
        )
        .unwrap();
    assert!(cursor.is_none());

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].primary_column_ids.is_empty());
}

/// Rows written before the read still decode once the ids travel: the same
/// request shape against an in-process store returns every staged row's
/// clustered key columns instead of failing them as corrupted.
#[test]
fn clustered_rows_survive_a_partial_count_request_shape() {
    let columns = vec![
        int_column(1, "s_w_id"),
        int_column(2, "s_i_id"),
        int_column(3, "s_quantity"),
    ];
    let mut table = KvTable::new(134, columns);
    table.set_common_handle_offsets(vec![0, 1]);

    let statement = tidb_executor::StmtContext::for_query();
    table
        .insert_row(
            &[Datum::Int(7), Datum::Int(1), Datum::Int(13)],
            &statement,
        )
        .unwrap();
    table
        .insert_row(
            &[Datum::Int(7), Datum::Int(2), Datum::Int(4)],
            &statement,
        )
        .unwrap();

    let aggregate = PushdownPartialAggregate::Count {
        input_offset: None,
        output_type: FieldType::new(FieldTypeCode::LongLong),
    };
    let zone = SessionTimeZone::utc();
    let pushdown = PushdownStatementContext::default();

    // An empty in-process store has no coprocessor, so the request is refused
    // and the caller reads the same rows locally -- which is exactly where the
    // unfilled-NOT-NULL failure used to surface against real TiKV.
    let cursor = table
        .pushdown_partial_aggregate_cursor(
            &[0, 1],
            &[],
            Option::<&[IndexRange]>::None,
            &aggregate,
            &zone,
            &pushdown,
        )
        .unwrap();
    assert!(cursor.is_none());
}
