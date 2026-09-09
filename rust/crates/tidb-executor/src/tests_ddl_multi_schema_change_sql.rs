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

//! Running ports of the `pkg/ddl/multi_schema_change_test.go` halves whose
//! observable contract is this tier's synchronous, in-order application of
//! one ALTER TABLE's action list (`ddl/alter_table.rs::run_alter_table_in`):
//! the success paths and the per-statement outcomes the Go tests assert
//! without online-DDL machinery. The job-queue contracts—cancellation,
//! failpoint hooks, sub-job schema states, and racing submissions—remain
//! unimplemented and therefore are not represented as Rust tests.
//!
//! Go's job-build-time combination refusals (`checkOperateSameColAndIdx`,
//! `pkg/ddl/multi_schema_change.go:350`, error template
//! `pkg/util/dbterror/ddl_terror.go:45`) answer
//! `[ddl:8200] Unsupported ... operate same column/index` for the whole
//! statement, atomically. The Rust dispatcher now performs the same
//! preflight, so conflicting action lists fail before any metadata mutation.

use crate::{
    run_alter_table_in, run_create_table_on, run_insert_on, run_rename_table_in, run_select_on,
    run_truncate_table_in, Catalog, StmtContext,
};
use tidb_datatype::Datum;

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn alter(catalog: &mut Catalog, sql: &str) -> Result<(), crate::DriverError> {
    run_alter_table_in(sql, catalog, "test", &ctx())
}

// Same, over a caller-owned session, so the warnings the statements file can
// be taken from it afterwards.
fn alter_on(
    catalog: &mut Catalog,
    session: &StmtContext,
    sql: &str,
) -> Result<(), crate::DriverError> {
    run_alter_table_in(sql, catalog, "test", session)
}

fn code_of(error: &crate::DriverError) -> u16 {
    error.clone().to_mysql_error().code
}

fn message_of(error: &crate::DriverError) -> String {
    error.clone().to_mysql_error().message
}

// The rows of a `SELECT` rendered as strings, the way Go's
// `testkit.Rows` prints them.
fn text_rows(catalog: &Catalog, sql: &str) -> Vec<Vec<String>> {
    run_select_on(sql, catalog, &ctx())
        .expect("select succeeds")
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|datum| match &datum {
                    Datum::Int(value) => value.to_string(),
                    Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    Datum::Null => "NULL".to_owned(),
                    other => panic!("unexpected datum {other:?}"),
                })
                .collect()
        })
        .collect()
}

fn index_names(catalog: &Catalog, table: &str) -> Vec<String> {
    let Some(crate::TableEntry::Kv(kv)) = catalog.table_in("test", table) else {
        panic!("table {table} missing");
    };
    kv.indexes()
        .iter()
        .map(|index| index.name.clone())
        .collect()
}

/// Go `multi_schema_change_test.go:166::TestMultiSchemaChangeRenameColumns`
/// success half: `rename column b to c, add column e int default 3` over
/// `t(a, b, index t(a, b))` applies both actions; the renamed column reads
/// its old value under the new name and the row reads `1 2 3`.
#[test]
fn multi_schema_change_rename_columns_applies_both_actions() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2, index t(a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("insert into t values ()", &mut catalog, &ctx()).unwrap();

    alter(
        &mut catalog,
        "alter table t rename column b to c, add column e int default 3",
    )
    .expect("Go: both actions apply");
    assert_eq!(text_rows(&catalog, "select c from t"), vec![["2"]]);
    assert_eq!(
        text_rows(&catalog, "select * from t"),
        vec![["1", "2", "3"]]
    );
}

/// Go `multi_schema_change_test.go:170-197::TestMultiSchemaChangeRenameColumns`
/// unsupported-combination block. Go refuses all four statements at job
/// build with 8200 (`multi_schema_change.go:350` +
/// `dbterror/ddl_terror.go:45`), atomically. The Rust preflight now reports
/// the same first conflicting column and leaves the original table intact.
#[test]
fn multi_schema_change_rename_columns_combinations_refuse_conflicting_operations() {
    // add and rename to the same column name
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("insert into t values ()", &mut catalog, &ctx()).unwrap();
    let error = alter(
        &mut catalog,
        "alter table t rename column b to c, add column c int",
    )
    .expect_err("Go: 8200 Unsupported operate same column 'c'");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same column 'c'"
    );
    assert_eq!(column_order(&catalog, "t"), vec!["a", "b"]);

    // add a column positioned AFTER the renamed-away column
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2)",
        &mut catalog,
    )
    .unwrap();
    let error = alter(
        &mut catalog,
        "alter table t rename column b to c, add column e int after b",
    )
    .expect_err("Go: 8200 Unsupported operate same column");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same column 'b'"
    );
    assert_eq!(column_order(&catalog, "t"), vec!["a", "b"]);

    // drop and rename the same column
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2)",
        &mut catalog,
    )
    .unwrap();
    let error = alter(
        &mut catalog,
        "alter table t drop column b, rename column b to c",
    )
    .expect_err("Go: 8200 Unsupported operate same column 'b'");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same column 'b'"
    );
    assert_eq!(column_order(&catalog, "t"), vec!["a", "b"]);

    // add an index over a column the same statement renames away
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2, index t(a, b))",
        &mut catalog,
    )
    .unwrap();
    let error = alter(
        &mut catalog,
        "alter table t rename column b to c, add index t1(a, b)",
    )
    .expect_err("Go: 8200 Unsupported operate same column");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same column 'b'"
    );
    assert_eq!(column_order(&catalog, "t"), vec!["a", "b"]);
}

/// Go `multi_schema_change_test.go:273-279::TestMultiSchemaChangeAlterColumns`
/// success half: `rename column a to c, alter column b set default 3` applies
/// both actions; after TRUNCATE the fresh row takes the new default (`1 3`).
#[test]
fn multi_schema_change_alter_columns_applies_rename_and_default() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2, index t(a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("insert into t values ()", &mut catalog, &ctx()).unwrap();
    assert_eq!(text_rows(&catalog, "select * from t"), vec![["1", "2"]]);

    alter(
        &mut catalog,
        "alter table t rename column a to c, alter column b set default 3",
    )
    .expect("Go: both actions apply");
    run_truncate_table_in(
        "truncate table t",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
    )
    .unwrap();
    run_insert_on("insert into t values ()", &mut catalog, &ctx()).unwrap();
    assert_eq!(text_rows(&catalog, "select * from t"), vec![["1", "3"]]);
}

/// Go `multi_schema_change_test.go:245-266::TestMultiSchemaChangeAlterColumns`
/// unsupported-combination block: Go refuses alter+drop, alter+rename and
/// alter+modify naming the same column with 8200
/// (`multi_schema_change.go:350`). The Rust preflight now rejects all three
/// before the default or type change can mutate the table.
#[test]
fn multi_schema_change_alter_columns_combinations_refuse_conflicts() {
    for sql in [
        "alter table t alter column b set default 3, drop column b",
        "alter table t alter column b set default 3, rename column b to c",
        "alter table t alter column b set default 3, modify column b double",
    ] {
        let mut catalog = Catalog::default();
        run_create_table_on(
            "create table t (a int default 1, b int default 2)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on("insert into t values ()", &mut catalog, &ctx()).unwrap();
        let error = alter(&mut catalog, sql).expect_err("Go: 8200 Unsupported operate same column");
        assert_eq!(code_of(&error), 8200);
        assert!(message_of(&error).contains("operate same column 'b'"));
        assert_eq!(
            text_rows(&catalog, "select * from t"),
            vec![vec!["1".to_owned(), "2".to_owned()]]
        );
    }
}

/// Go `multi_schema_change_test.go:331-337::TestMultiSchemaChangeChangeColumns`
/// success half: `rename column b to c, change column a e bigint default 3`
/// applies both actions; the row reads `1 2` under the new names, and after
/// TRUNCATE the fresh row takes the new default (`3 2`).
#[test]
fn multi_schema_change_change_columns_applies_rename_and_change() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2, index t(a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("insert into t values ()", &mut catalog, &ctx()).unwrap();

    alter(
        &mut catalog,
        "alter table t rename column b to c, change column a e bigint default 3",
    )
    .expect("Go: both actions apply");
    assert_eq!(text_rows(&catalog, "select e, c from t"), vec![["1", "2"]]);
    run_truncate_table_in(
        "truncate table t",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
    )
    .unwrap();
    run_insert_on("insert into t values ()", &mut catalog, &ctx()).unwrap();
    assert_eq!(text_rows(&catalog, "select e, c from t"), vec![["3", "2"]]);
}

/// Go `multi_schema_change_test.go:295-315::TestMultiSchemaChangeChangeColumns`
/// unsupported-combination block. Go refuses all three with 8200
/// (`multi_schema_change.go:350`); the Rust preflight now reports the same
/// first conflicting column before any CHANGE action runs.
#[test]
fn multi_schema_change_change_columns_combinations_refuse_conflicts() {
    // change and drop the same column
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2)",
        &mut catalog,
    )
    .unwrap();
    let error = alter(
        &mut catalog,
        "alter table t change column b c double, drop column b",
    )
    .expect_err("Go: 8200 Unsupported operate same column 'b'");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same column 'b'"
    );

    // change and add the same column name
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2)",
        &mut catalog,
    )
    .unwrap();
    let error = alter(
        &mut catalog,
        "alter table t change column b c double, add column c int",
    )
    .expect_err("Go: 8200 Unsupported operate same column 'c'");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same column 'c'"
    );

    // change a column and add an index over its old name
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2, index t(a, b))",
        &mut catalog,
    )
    .unwrap();
    let error = alter(
        &mut catalog,
        "alter table t change column b c double, add index t1(a, b)",
    )
    .expect_err("Go: 8200 Unsupported operate same column");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same column 'b'"
    );
}

/// Go `multi_schema_change_test.go:148::TestMultiSchemaChangeDropColumnsParallel`.
/// Go submits `drop column if exists b, drop column if exists c` twice
/// (`putTheSameDDLJobTwice`): both runs succeed and the loser files one
/// `Note 1091` per guarded column -- Go's text `column b doesn't exist`
/// (`column.go:260` wraps `ErrCantDropFieldOrKey`). Serialized here: the
/// second run files the notes, with THIS engine's 1091 spelling
/// `Can't DROP 'b'; check that column/key exists` (the MySQL canonical text
/// `drop_column_action` suppresses) -- same code, different message.
/// Without `IF EXISTS`, Go's second submission is the plain
/// `ErrCantDropFieldOrKey` 1091; the serialized second run errors 1091 too.
#[test]
fn multi_schema_change_drop_columns_parallel_second_run_reports_1091() {
    let mut catalog = Catalog::default();
    let session = ctx();
    run_create_table_on("create table t (a int, b int, c int)", &mut catalog).unwrap();
    for _ in 0..2 {
        alter_on(
            &mut catalog,
            &session,
            "alter table t drop column if exists b, drop column if exists c",
        )
        .expect("Go: both submissions succeed");
    }
    let warnings = session.take_warnings();
    let notes: Vec<_> = warnings
        .iter()
        .filter(|(_, code, _)| *code == 1091)
        .collect();
    assert_eq!(notes.len(), 2, "one note per guarded column: {warnings:?}");

    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int, c int)", &mut catalog).unwrap();
    alter(&mut catalog, "alter table t drop column b, drop column c").expect("first submission");
    let error = alter(&mut catalog, "alter table t drop column b, drop column c")
        .expect_err("Go: second submission is 1091 ErrCantDropFieldOrKey");
    assert_eq!(code_of(&error), 1091);
    assert_eq!(
        message_of(&error),
        "Can't DROP 'b'; check that column/key exists"
    );
}

/// Go `multi_schema_change_test.go:483::TestMultiSchemaChangeDropIndexesParallel`.
/// Same double-submission shape for indexes: `drop index if exists b, drop
/// index if exists c` twice succeeds, the loser filing
/// `Note 1091 index b doesn't exist` (`index.go:2262`) -- and THIS engine's
/// suppressed note text matches Go exactly (`indexes.rs`
/// `drop_index_from_table`). Without `IF EXISTS` the second submission is
/// 1091 `index b doesn't exist`, likewise identical.
#[test]
fn multi_schema_change_drop_indexes_parallel_matches_go_notes() {
    let mut catalog = Catalog::default();
    let session = ctx();
    run_create_table_on(
        "create table t (a int, b int, c int, index(a), index(b), index(c))",
        &mut catalog,
    )
    .unwrap();
    for _ in 0..2 {
        alter_on(
            &mut catalog,
            &session,
            "alter table t drop index if exists b, drop index if exists c",
        )
        .expect("Go: both submissions succeed");
    }
    let warnings = session.take_warnings();
    assert_eq!(
        warnings
            .iter()
            .filter(|(_, code, _)| *code == 1091)
            .map(|(_, _, message)| message.as_str())
            .collect::<Vec<_>>(),
        vec![
            "index b doesn't exist".to_string(),
            "index c doesn't exist".to_string()
        ],
        "Go's exact note texts: {warnings:?}"
    );

    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int, c int, index (a), index(b), index(c))",
        &mut catalog,
    )
    .unwrap();
    alter(&mut catalog, "alter table t drop index b, drop index a").expect("first submission");
    let error = alter(&mut catalog, "alter table t drop index b, drop index a")
        .expect_err("Go: second submission is 1091 ErrCantDropFieldOrKey");
    assert_eq!(code_of(&error), 1091);
    assert_eq!(message_of(&error), "index b doesn't exist");
}

/// Go `multi_schema_change_test.go:394-429::TestMultiSchemaChangeAddIndexesCancelled`
/// success-path half (the cancellation itself is the job-queue gap): one
/// ALTER adding four indexes `t(a, b), t1(a), t2(a), t3(a, b)` applies all
/// four; the data survives and reads through all of them.
#[test]
fn multi_schema_change_add_indexes_applies_all_four() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int, c int)", &mut catalog).unwrap();
    run_insert_on("insert into t values (1, 2, 3)", &mut catalog, &ctx()).unwrap();

    alter(
        &mut catalog,
        "alter table t add index t(a, b), add index t1(a), add index t2(a), add index t3(a, b)",
    )
    .expect("Go: the statement succeeds when not cancelled");
    let mut names = index_names(&catalog, "t");
    names.sort();
    assert_eq!(names, vec!["t", "t1", "t2", "t3"]);
    assert_eq!(
        text_rows(&catalog, "select * from t use index(t, t1, t2, t3)"),
        vec![["1", "2", "3"]]
    );
}

/// Go `multi_schema_change_test.go:442-480::TestMultiSchemaChangeDropIndexesCancelled`
/// success-path half: `drop index a, drop index b, drop index idx` over
/// `t(a, b, index(a), unique index(b), index idx(a, b))` removes all three;
/// afterwards `USE INDEX` on any of them answers Go's 1176
/// `ErrKeyDoesNotExist` while the rows themselves stay readable.
#[test]
fn multi_schema_change_drop_indexes_removes_all_and_use_index_answers_1176() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int, index(a), unique index(b), index idx(a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("insert into t values (1, 2)", &mut catalog, &ctx()).unwrap();

    alter(
        &mut catalog,
        "alter table t drop index a, drop index b, drop index idx",
    )
    .expect("Go: the drop succeeds (MustCancelFailed -- already non-revertible)");
    assert!(
        index_names(&catalog, "t").is_empty(),
        "all three indexes dropped"
    );
    for missing in ["a", "b", "idx"] {
        let error = run_select_on(
            &format!("select * from t use index ({missing})"),
            &catalog,
            &ctx(),
        )
        .expect_err("Go: 1176 ErrKeyDoesNotExist");
        assert_eq!(code_of(&error), 1176, "use index ({missing})");
    }
    assert_eq!(text_rows(&catalog, "select * from t"), vec![["1", "2"]]);
}

/// Go `multi_schema_change_test.go:501-549::TestMultiSchemaChangeRenameIndexes`.
/// `rename index t to x, rename index t1 to x1` applies both; the old names
/// stop resolving (1176) and the new ones serve reads. The combination arms
/// Go refuses with 8200 are rejected by the Rust preflight before either
/// index mutation. The drop-column/rename-index arm remains a separate
/// no-op-vs-missing-index compatibility boundary.
#[test]
fn multi_schema_change_rename_indexes_applies_and_combinations_measured() {
    // rename index
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int, c int, index t(a), index t1(b))",
        &mut catalog,
    )
    .unwrap();
    alter(
        &mut catalog,
        "alter table t rename index t to x, rename index t1 to x1",
    )
    .expect("Go: both renames apply");
    run_select_on("select * from t use index (x)", &catalog, &ctx()).expect("new name serves");
    run_select_on("select * from t use index (x1)", &catalog, &ctx()).expect("new name serves");
    for gone in ["t", "t1"] {
        let error = run_select_on(
            &format!("select * from t use index ({gone})"),
            &catalog,
            &ctx(),
        )
        .expect_err("Go: 1176 ErrKeyDoesNotExist");
        assert_eq!(code_of(&error), 1176);
    }

    // drop and rename the same index
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int, c int, index t(a))",
        &mut catalog,
    )
    .unwrap();
    let error = alter(
        &mut catalog,
        "alter table t drop index t, rename index t to t1",
    )
    .expect_err("Go: 8200 Unsupported operate same index 't'");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same index 't'"
    );

    // add and rename to the same index name
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int, c int, index t(a))",
        &mut catalog,
    )
    .unwrap();
    let error = alter(
        &mut catalog,
        "alter table t add index t1(b), rename index t to t1",
    )
    .expect_err("Go: 8200 Unsupported operate same index");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same index 't1'"
    );

    // drop a column with its covering index and rename that index
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2, c int default 3, index t(a))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("insert into t values ()", &mut catalog, &ctx()).unwrap();
    let error = alter(
        &mut catalog,
        "alter table t drop column a, rename index t to x",
    )
    .expect_err("Go: the rename silently no-ops (index already gone with the column)");
    assert_eq!(
        code_of(&error),
        1176,
        "sequential: 't' went with the column"
    );
    assert_eq!(text_rows(&catalog, "select * from t"), vec![["2", "3"]]);
}

/// Go `multi_schema_change_test.go:579-638::TestMultiSchemaChangeAlterIndex`
/// mixed-with-modify half (the failpoint-interleaved read is the gap): one
/// statement `alter index i1 invisible, modify column a tinyint, alter index
/// i2 invisible` applies all three; both indexes stop serving (1176) and the
/// row survives the tinyint rewrite (`1 2`).
#[test]
fn multi_schema_change_alter_index_mixed_with_modify_applies() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int, index i1(a, b), index i2(b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("insert into t values (1, 2)", &mut catalog, &ctx()).unwrap();

    alter(
        &mut catalog,
        "alter table t alter index i1 invisible, modify column a tinyint, alter index i2 invisible",
    )
    .expect("Go: the statement succeeds");
    for missing in ["i1", "i2"] {
        let error = run_select_on(
            &format!("select * from t use index ({missing})"),
            &catalog,
            &ctx(),
        )
        .expect_err("Go: 1176 ErrKeyDoesNotExist");
        assert_eq!(code_of(&error), 1176, "use index ({missing})");
    }
    assert_eq!(text_rows(&catalog, "select * from t"), vec![["1", "2"]]);
}

/// Go `multi_schema_change_test.go:584-601::TestMultiSchemaChangeAlterIndex`
/// unsupported-combination block. Go refuses all three alter/drop/add
/// combinations naming the same index with 8200. The Rust preflight now
/// rejects them before visibility or index metadata changes.
#[test]
fn multi_schema_change_alter_index_combinations_refuse_conflicts() {
    // alter the same index twice
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int, index idx(a, b))",
        &mut catalog,
    )
    .unwrap();
    let error = alter(
        &mut catalog,
        "alter table t alter index idx visible, alter index idx invisible",
    )
    .expect_err("Go: 8200 Unsupported operate same index 'idx'");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same index 'idx'"
    );

    // drop and alter the same index
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int, index idx(a, b))",
        &mut catalog,
    )
    .unwrap();
    let error = alter(
        &mut catalog,
        "alter table t drop index idx, alter index idx visible",
    )
    .expect_err("Go: 8200 Unsupported operate same index");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same index 'idx'"
    );

    // add and alter the same index
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int)", &mut catalog).unwrap();
    let error = alter(
        &mut catalog,
        "alter table t add index idx(a, b), alter index idx invisible",
    )
    .expect_err("Go: 8200 Unsupported operate same index 'idx'");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same index 'idx'"
    );
}

/// Go `multi_schema_change_test.go:849-868::TestMultiSchemaChangeModifyColumnOrderByStates`:
/// five statements mixing MODIFY COLUMN (including positional moves and a
/// two-column position swap) with ADD and DROP COLUMN all apply in one
/// statement each -- the order the sub-jobs run in never matters for the
/// final metadata this tier writes.
#[test]
fn multi_schema_change_modify_column_order_by_states_all_orders_apply() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t values (1, 1)", &mut catalog, &ctx()).unwrap();
    alter(
        &mut catalog,
        "alter table t modify column b smallint, add column d int",
    )
    .expect("modify then add");

    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t values (1, 1)", &mut catalog, &ctx()).unwrap();
    alter(
        &mut catalog,
        "alter table t modify column a smallint, add column c int, modify column b smallint",
    )
    .expect("modify, add, modify");

    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int, c char(10))", &mut catalog).unwrap();
    run_insert_on("insert into t values (1, 1, '1')", &mut catalog, &ctx()).unwrap();
    alter(
        &mut catalog,
        "alter table t modify column c int after a, add column d int, add column e int, modify column b smallint",
    )
    .expect("positional modify, adds, modify");

    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id bigint, c1 bigint, c2 bigint)",
        &mut catalog,
    )
    .unwrap();
    alter(
        &mut catalog,
        "alter table t modify column c2 int after id, modify column id int after c2",
    )
    .expect("two-column position swap");
    // Sequential move semantics: `c2 after id` turns [id, c1, c2] into
    // [id, c2, c1], then `id after c2` turns that into [c2, id, c1]. Go's
    // sub-job order is re-planned by the owner; this test pins only that the
    // statement applies.
    assert_eq!(
        column_order(&catalog, "t"),
        vec!["c2".to_string(), "id".to_string(), "c1".to_string()],
        "measured sequential order"
    );

    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t1 (id bigint, c1 bigint, c2 bigint)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("insert into t1 values (1, 1, 1)", &mut catalog, &ctx()).unwrap();
    alter(
        &mut catalog,
        "alter table t1 modify column c2 int, drop column id",
    )
    .expect("modify with drop");
}

fn column_order(catalog: &Catalog, table: &str) -> Vec<String> {
    let Some(crate::TableEntry::Kv(kv)) = catalog.table_in("test", table) else {
        panic!("table {table} missing");
    };
    kv.columns
        .iter()
        .map(|column| column.name.clone())
        .collect()
}

// Go `multi_schema_change_test.go:738-748::TestMultiSchemaChangeNoSubJobs`:
// the `add column if not exists` notes path remains unimplemented: this
// tier's `add_column_action` matches the parsed `IF NOT EXISTS` flag away at
// `alter_table.rs:150` and answers 1060 instead of two Note 1060s.

/// Go `multi_schema_change_test.go:364-392::TestMultiSchemaChangeRenameTable`
/// tail half (the racing rename-under-failpoint is the gap): after the table
/// is renamed to `t1`, the rename+change ALTER applies there and the data is
/// intact -- verified through the `admin check table` equivalent
/// (`admin_check::check_table`, Go `tk2.MustExec("admin check table t1")`).
#[test]
fn multi_schema_change_rename_table_then_alter_leaves_consistent_table() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int default 1, b int default 2, index t(a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("insert into t values (1, 2)", &mut catalog, &ctx()).unwrap();

    run_rename_table_in(
        "alter table t rename to t1",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
    )
    .expect("Go: rename to t1");
    assert_eq!(text_rows(&catalog, "select * from t1"), vec![["1", "2"]]);

    alter(
        &mut catalog,
        "alter table t1 rename column b to c, change column a e bigint default 3",
    )
    .expect("Go: the ALTER applies on the renamed table");
    assert_eq!(text_rows(&catalog, "select * from t1"), vec![["1", "2"]]);
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t1") else {
        panic!("table t1 missing");
    };
    let session = ctx();
    let context = crate::kv_table::RowDecodeContext::for_query(&session);
    crate::admin_check::check_table(table, None, &context)
        .expect("Go: admin check table t1 passes");
}

/// Go `multi_schema_change_test.go:699-737::TestMultiSchemaChangeWithExpressionIndex`.
/// The refusals and the duplicate-detection arm, minus the failpoint:
/// expression-index dependencies now participate in the same preflight as
/// ordinary index columns. Go refuses the two dependency conflicts at job
/// build with 8200, atomically; the duplicate-entry arm remains a later
/// 1062 backfill error and keeps its successful earlier column/index changes.
#[test]
fn multi_schema_change_expression_index_combinations_measured() {
    // drop column a, add unique index idx((a + b))
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t values (1, 2), (2, 1)", &mut catalog, &ctx()).unwrap();
    let error = alter(
        &mut catalog,
        "alter table t drop column a, add unique index idx((a + b))",
    )
    .expect_err("Go: 8200 Unsupported operate same column");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same column 'a'"
    );

    // add column c, change column a d bigint, add index idx((a + a))
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t values (1, 2), (2, 1)", &mut catalog, &ctx()).unwrap();
    let error = alter(
        &mut catalog,
        "alter table t add column c int, change column a d bigint, add index idx((a + a))",
    )
    .expect_err("Go: 8200 Unsupported operate same column");
    assert_eq!(code_of(&error), 8200);
    assert_eq!(
        message_of(&error),
        "Unsupported modify column: operate same column 'a'"
    );

    // add column c default 10, add index idx1((a + b)), add unique index idx2((a + b))
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t values (1, 2), (2, 1)", &mut catalog, &ctx()).unwrap();
    let error = alter(
        &mut catalog,
        "alter table t add column c int default 10, add index idx1((a + b)), add unique index idx2((a + b))",
    )
    .expect_err("Go: 1062 ErrDupEntry");
    assert_eq!(
        code_of(&error),
        1062,
        "unique index backfill collides on a+b = 3"
    );
    assert_eq!(
        text_rows(&catalog, "select * from t"),
        vec![["1", "2", "10"], ["2", "1", "10"]]
    );
}

/// Go `multi_schema_change_test.go:725-737::TestMultiSchemaChangeWithExpressionIndex`
/// success arm (the failpoint-interleaved UPDATE is the gap): one ALTER adds
/// column `c`, a non-unique expression index and a unique expression index
/// over a DIFFERENT expression; the rows read through both indexes as
/// `1 2 10` / `2 1 10`.
#[test]
fn multi_schema_change_expression_index_success_applies() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t values (1, 2), (2, 1)", &mut catalog, &ctx()).unwrap();

    alter(
        &mut catalog,
        "alter table t add column c int default 10, add index idx1((a + b)), add unique index idx2((a*10 + b))",
    )
    .expect("Go: the ALTER succeeds");
    assert_eq!(
        text_rows(&catalog, "select * from t use index(idx1, idx2)"),
        vec![["1", "2", "10"], ["2", "1", "10"]]
    );
}
