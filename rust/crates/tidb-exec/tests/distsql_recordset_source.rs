// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_distsql::{ResponseChannel, WarningCollector};
use tidb_exec::distsql_recordset::{DistSqlRecordSet, DistSqlRecordSetError};
use tidb_protocol::{ColumnInfo, TYPE_LONG};

fn column() -> ColumnInfo {
    ColumnInfo {
        schema: "test".to_owned(),
        table: "t".to_owned(),
        org_table: "t".to_owned(),
        name: "a".to_owned(),
        org_name: "a".to_owned(),
        column_length: 11,
        charset: 63,
        flag: 0,
        decimal: 0,
        type_code: TYPE_LONG,
        default_value: None,
    }
}

fn recordset(values: &[u8]) -> DistSqlRecordSet {
    let mut rows_data = Vec::with_capacity(values.len() * 2);
    for value in values {
        // TiDB value codec's varint tag followed by the positive zig-zag byte.
        rows_data.extend_from_slice(&[8, *value << 1]);
    }
    // tipb.SelectResponse.chunks (field 3) containing one Chunk.rows_data
    // (field 3). Omitted encode_type is protobuf's TypeDefault zero value.
    let mut chunk = vec![0x1a, rows_data.len() as u8];
    chunk.extend_from_slice(&rows_data);
    let mut response = vec![0x1a, chunk.len() as u8];
    response.extend_from_slice(&chunk);
    let mut source = ResponseChannel::new();
    source.push_result(response).unwrap();
    source.finish().unwrap();
    let iter = source.into_select_iter(
        vec![FieldType::new(FieldTypeCode::Long)],
        Vec::new(),
        WarningCollector::new(),
    );
    DistSqlRecordSet::new(iter, vec![column()])
}

#[test]
fn select_response_rows_are_pulled_in_bounded_batches() {
    let mut recordset = recordset(&[1, 2, 3]);
    assert_eq!(
        recordset.next_batch(2).unwrap(),
        vec![
            vec![tidb_datatype::Datum::Int(1)],
            vec![tidb_datatype::Datum::Int(2)]
        ]
    );
    assert_eq!(
        recordset.next_batch(2).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(3)]]
    );
    assert!(recordset.next_batch(2).unwrap().is_empty());
    assert!(recordset.lifecycle().has_advanced());
}

#[test]
fn finish_and_close_are_idempotent_and_metadata_survives_close() {
    let mut recordset = recordset(&[]);
    recordset.finish().unwrap();
    recordset.finish().unwrap();
    recordset.close().unwrap();
    recordset.close().unwrap();
    assert!(recordset.lifecycle().is_finished());
    assert!(recordset.lifecycle().is_closed());
    assert_eq!(recordset.columns(), &[column()]);
}

#[test]
fn close_without_explicit_finish_still_runs_finish_cleanup() {
    let mut recordset = recordset(&[]);
    recordset.close().unwrap();
    assert!(recordset.lifecycle().is_finished());
    assert!(recordset.lifecycle().is_closed());
}

#[test]
fn first_source_error_is_preserved_without_fabricating_rows() {
    let mut source = ResponseChannel::new();
    source.fail("first next failed").unwrap();
    let iter = source.into_select_iter(Vec::new(), Vec::new(), WarningCollector::new());
    let mut recordset = DistSqlRecordSet::new(iter, Vec::new());
    assert_eq!(
        recordset.next_batch(32),
        Err(DistSqlRecordSetError::Source(
            "first next failed".to_owned()
        ))
    );
    assert!(recordset.lifecycle().has_advanced());
}
