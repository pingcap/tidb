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

use std::sync::{Arc, Mutex};

use tidb_datatype::{FieldType, FieldTypeCode, SessionTimeZone};
use tidb_executor::ddl::index_prefix::UNSPECIFIED_LENGTH;
use tidb_executor::remote_scan::{
    PushdownPartialAggregate, PushdownScan, PushdownScanRequest, PushdownStatementContext,
};
use tidb_executor::storage::{StorageError, StorageIterator, TableStorage};
use tidb_executor::{IndexRange, KvColumn, KvIndex, KvTable, ScanPredicate};
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

/// A store with no coprocessor: it records the request the table BUILT, then
/// refuses, exactly where the caller falls back to the local cursor.
#[derive(Debug)]
struct RecordingStore {
    captured: Arc<Mutex<Option<PushdownScanRequest>>>,
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
    fn open_remote_scan(
        &mut self,
        request: &PushdownScanRequest,
    ) -> Option<Result<PushdownScan, StorageError>> {
        *self.captured.lock().unwrap() = Some(request.clone());
        None
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
    PushdownPartialAggregate::Count {
        input_offset: Some(0),
        output_type,
    }
}

fn capture(table: &mut KvTable, captured: Arc<Mutex<Option<PushdownScanRequest>>>) {
    let opened = table.pushdown_partial_aggregate_cursor(
        &[0],
        Vec::<ScanPredicate>::new().as_slice(),
        Option::<&[IndexRange]>::None,
        &count_over_primary(),
        &SessionTimeZone::default(),
        &PushdownStatementContext::default(),
    );
    assert!(
        opened.unwrap().is_none(),
        "the recording store has no coprocessor"
    );
    assert!(
        captured.lock().unwrap().is_some(),
        "the request must be recorded before the refusal"
    );
}

fn recorded(captured: &Arc<Mutex<Option<PushdownScanRequest>>>) -> PushdownScanRequest {
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
    assert_eq!(request.primary_column_ids, vec![2]);
    // Whole-column key parts: nothing travels as a prefix.
    assert!(request.primary_prefix_column_ids.is_empty());
    // The scanned column list itself stays the projection the caller asked for.
    assert_eq!(request.columns.len(), 1);
    assert_eq!(request.columns[0].id, 2);
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
    });

    capture(&mut table, Arc::clone(&captured));
    let request = recorded(&captured);
    assert_eq!(request.primary_column_ids, vec![2]);
    assert!(request.primary_prefix_column_ids.is_empty());
}
