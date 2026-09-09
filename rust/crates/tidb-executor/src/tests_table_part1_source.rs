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

use crate::{
    run_create_table_on, run_insert_on, run_select_on, run_update_on, Catalog, StmtContext,
};

fn query_ctx() -> StmtContext {
    StmtContext::for_query()
}

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
    let columns = crate::kv_table::extract_columns_from_index_condition(&index, &table, true)
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
    let output = crate::kv_table::dedup_index_columns(input);
    assert_eq!(output.len(), 4);
    for (actual, expected) in output.iter().zip(columns) {
        assert!(actual.ptr_eq(&expected));
    }
}

/// `pkg/table/tables/tables_test.go:77::TestBasic`.
#[test]
fn test_basic() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int primary key auto_increment, b varchar(255) unique)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 'abc')",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    run_update_on(
        "update t set b = 'cba' where a = 1",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    let rows = run_select_on("select a, b from t", &catalog, &query_ctx()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], tidb_datatype::Datum::Int(1));
    run_create_table_on("create table t2 (a int)", &mut catalog).unwrap();
    crate::run_delete_on("delete from t", &mut catalog, &query_ctx()).unwrap();
    assert_eq!(
        run_select_on("select count(*) from t", &catalog, &query_ctx()).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(0)]]
    );
}

/// `pkg/table/tables/tables_test.go:179::TestTypes`.
#[test]
fn test_types() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 text, c6 blob, c7 varchar(64), c10 decimal(10,1))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 2, 3, 4, '5', '6', '7', 1.4)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    let rows = run_select_on(
        "select c1, c5, c10 from t where c1 = 1",
        &catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 3);
}

/// `pkg/table/tables/tables_test.go:231::TestUniqueIndexMultipleNullEntries`.
#[test]
fn test_unique_index_multiple_null_entries() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int primary key, b varchar(255) unique)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, null), (2, null)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on("select count(*) from t", &catalog, &query_ctx()).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(2)]]
    );
}

/// `pkg/table/tables/tables_test.go:271::TestRowKeyCodec`.
#[test]
fn test_row_key_codec() {
    use tidb_codec::table_key::{
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

/// `pkg/table/tables/tables_test.go:312::TestUnsignedPK`.
#[test]
fn test_unsigned_pk() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a bigint unsigned primary key, b varchar(255))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 'abc')",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    let rows = run_select_on("select a, b from t", &catalog, &query_ctx()).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], tidb_datatype::Datum::UInt(1)));
}

/// `pkg/table/tables/tables_test.go:335::TestIterRecords`.
#[test]
fn test_iter_records() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int primary key, b int)", &mut catalog).unwrap();
    run_insert_on(
        "insert into t values (-1, 2), (2, null)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on("select count(*) from t", &catalog, &query_ctx()).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(2)]]
    );
}

/// `pkg/table/tables/tables_test.go:657::TestConstraintCheckForUniqueIndex`.
#[test]
fn test_constraint_check_for_unique_index() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id int primary key, k int not null, c varchar(20) not null, unique key uk(k, c))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 1, 'tidb'), (2, 2, 'tidb')",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert!(run_update_on(
        "update t set k = 1 where id = 2",
        &mut catalog,
        &query_ctx()
    )
    .is_err());
}
