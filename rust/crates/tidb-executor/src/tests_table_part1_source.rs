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

//! Source-backed inventory for manifest batch `b150`, `pkg/table`.
//!
//! This is the deterministic items 1--60 of the repository-wide `pkg/table`
//! test enumeration. The module keeps one Rust carrier for every upstream
//! `Test*` or `Benchmark*` function. Runnable carriers use the executor's
//! public SQL, key-codec, and metadata seams; Go-only transaction-buffer,
//! failpoint, cache-lease, and session/information-schema behavior remains an
//! explicit ignored carrier rather than an invented approximation.

#![cfg(test)]



/// `pkg/table/tables/index_test.go:601::TestExtractColumnsFromCondition`.
#[test]
fn test_extract_columns_from_condition() {
    let column = |name: &str, offset, generated: &str, stored| tidb_model::ColumnInfo {
        name: tidb_ast::CiString::new(name),
        offset,
        state: tidb_model::SchemaState::PUBLIC,
        generated_expr_string: generated.to_owned(),
        generated_stored: stored,
        ..Default::default()
    };
    let table = tidb_model::TableInfo {
        name: tidb_ast::CiString::new("test_table"),
        columns: vec![
            column("c1", 0, "", false),
            column("c2", 1, "", false),
            column("c3", 2, "c1 + c2", false),
            column("c4", 3, "c1 + c2", true),
        ]
        .into(),
        ..Default::default()
    };
    let index = tidb_model::IndexInfo {
        condition_expr_string: "c3 > 50".to_owned(),
        ..Default::default()
    };
    let columns = tidb_table::tables::extract_columns_from_condition(&index, &table, true)
        .expect("condition columns");
    let names = columns
        .iter()
        .map(|column| column.read().name.original().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, ["c1", "c2", "c3"]);
}

/// `pkg/table/tables/index_test.go:661::TestDedupIndexColumns4Test`.
#[test]
fn test_dedup_index_columns4_test() {
    let columns = (0..4)
        .map(|offset| {
            tidb_model::GoShared::new(tidb_model::IndexColumn {
                name: tidb_ast::CiString::new(format!("c{offset}")),
                offset,
                ..Default::default()
            })
        })
        .collect::<Vec<_>>();
    let input = vec![
        columns[0].clone(),
        columns[1].clone(),
        columns[0].clone(),
        columns[2].clone(),
        columns[1].clone(),
        columns[3].clone(),
    ];
    let output = tidb_table::tables::dedup_index_columns(input);
    assert_eq!(output.len(), 4);
    for (actual, expected) in output.iter().zip(columns) {
        assert!(actual.ptr_eq(&expected));
    }
}

/// `pkg/table/tables/tables_test.go:271::TestRowKeyCodec`.
#[test]
fn test_row_key_codec() {
    use tidb_tablecodec::table_key::{
        decode_record_key, decode_row_key, encode_row_key_with_handle, RecordHandle,
    };

    for (table_id, handle) in [(1, 1_234_567_890), (2, 1), (3, -1), (4, -1)] {
        let key = encode_row_key_with_handle(table_id, &RecordHandle::Int(handle));
        assert_eq!(
            decode_record_key(&key),
            Ok((table_id, RecordHandle::Int(handle)))
        );
        assert_eq!(decode_row_key(&key), Ok(RecordHandle::Int(handle)));
    }
    for invalid in [
        "",
        "x",
        "t1",
        "t12345678",
        "t12345678_i",
        "t12345678_r1",
        "t12345678_r1234567",
    ] {
        assert!(
            decode_row_key(invalid.as_bytes()).is_err(),
            "invalid: {invalid:?}"
        );
    }
}
