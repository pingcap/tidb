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

//! Runnable final-state behavior derived from pinned Go
//! `pkg/ddl/db_integration_test.go` tests and exercised through the catalog
//! and DDL runners.

use tidb_datatype::Datum;
use tidb_executor::{ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Datum::Null => "<nil>".to_owned(),
                    Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
                    Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
                    Datum::Int(value) => value.to_string(),
                    Datum::UInt(value) => value.to_string(),
                    Datum::Float32(value) => value.to_string(),
                    Datum::Real(value) => value.to_string(),
                    Datum::Enum(value, _) => value.name().to_string(),
                    Datum::Set(value, _) => value.name().to_string(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

// --- TestAlterColumn (pkg/ddl/db_integration_test.go:1143) ---

/// The default-changing and origin-row half of Go's `TestAlterColumn`.
#[test]
fn alter_column_set_default_changes_new_rows() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_alter_column (a int default 111, b varchar(8), c varchar(8) not null)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    run_insert_on(
        "insert into b103_alter_column set b = 'a', c = 'aa'",
        &mut catalog,
        &context,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table b103_alter_column alter column a set default 222",
        &mut catalog,
        "test",
        &context,
    )
    .unwrap();
    run_insert_on(
        "insert into b103_alter_column set b = 'b', c = 'bb'",
        &mut catalog,
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(
            &run_select_on(
                "select a from b103_alter_column order by a",
                &catalog,
                &context,
            )
            .unwrap()
        ),
        vec![vec!["111".to_owned()], vec!["222".to_owned()]],
    );
}

// --- TestSqlFunctionsInGeneratedColumns (pkg/ddl/db_integration_test.go:1817) ---

/// A small positive generated-column contract from the larger Go matrix.
#[test]
fn deterministic_sql_function_in_generated_column_is_readable() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_generated (a int, b int generated always as (abs(a)) virtual)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    run_insert_on(
        "insert into b103_generated values (-1, default)",
        &mut catalog,
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(&run_select_on("select * from b103_generated", &catalog, &context).unwrap()),
        vec![vec!["-1".to_owned(), "1".to_owned()]],
    );
}

// --- TestDropColumnWithIndex (pkg/ddl/db_integration_test.go:2017) ---

/// Dropping a covered ordinary index column removes the index and leaves the
/// remaining table usable.  This is the synchronous portion of the Go test.
#[test]
fn drop_column_with_index_removes_index_and_column() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_drop_index (a int, b int, c int)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    ddl::run_alter_table_in(
        "alter table b103_drop_index add index idx(b)",
        &mut catalog,
        "test",
        &context,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table b103_drop_index drop column b",
        &mut catalog,
        "test",
        &context,
    )
    .unwrap();
    run_insert_on(
        "insert into b103_drop_index values (1, 3)",
        &mut catalog,
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(&run_select_on("select * from b103_drop_index", &catalog, &context).unwrap()),
        vec![vec!["1".to_owned(), "3".to_owned()]],
    );
}

// --- TestDropColumnWithMultiIndex (pkg/ddl/db_integration_test.go:2047) ---

/// Multiple ordinary indexes covering one dropped column are removed by the
/// synchronous ALTER implementation.
#[test]
fn drop_column_with_multiple_indexes_removes_all_covering_indexes() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_drop_multi (a int, b int, c int)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    for sql in [
        "alter table b103_drop_multi add index idx_1(b)",
        "alter table b103_drop_multi add index idx_2(b)",
    ] {
        ddl::run_alter_table_in(sql, &mut catalog, "test", &context).unwrap();
    }
    ddl::run_alter_table_in(
        "alter table b103_drop_multi drop column b",
        &mut catalog,
        "test",
        &context,
    )
    .unwrap();
    assert_eq!(
        rows_text(&run_select_on("select * from b103_drop_multi", &catalog, &context).unwrap()),
        Vec::<Vec<String>>::new(),
    );
}

// --- TestDropColumnsWithMultiIndex (pkg/ddl/db_integration_test.go:2061) ---

/// Dropping two indexed columns in one ALTER leaves the unindexed column.
#[test]
fn drop_columns_with_multiple_indexes_removes_all_covering_indexes() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table b103_drop_both (a int, b int, c int)",
        &mut catalog,
    )
    .unwrap();
    let context = ctx();
    for sql in [
        "alter table b103_drop_both add index idx_1(b)",
        "alter table b103_drop_both add index idx_2(b)",
        "alter table b103_drop_both add index idx_3(c)",
        "alter table b103_drop_both drop column b, drop column c",
    ] {
        ddl::run_alter_table_in(sql, &mut catalog, "test", &context).unwrap();
    }
    assert_eq!(
        rows_text(&run_select_on("select * from b103_drop_both", &catalog, &context).unwrap()),
        Vec::<Vec<String>>::new(),
    );
}
