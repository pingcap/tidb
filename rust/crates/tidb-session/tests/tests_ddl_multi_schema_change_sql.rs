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
//! observable contract is one ALTER TABLE's action list applied as a single
//! multi-schema-change job: the success paths and the per-statement outcomes
//! the Go tests assert without online-DDL machinery. The job-queue
//! contracts -- cancellation, failpoint hooks, sub-job schema states, and
//! racing submissions -- remain unimplemented and therefore are not
//! represented as Rust tests.
//!
//! Go's job-build-time combination refusals (`checkOperateSameColAndIdx`,
//! `pkg/ddl/multi_schema_change.go`, error template
//! `pkg/util/dbterror/ddl_terror.go`) answer
//! `[ddl:8200] Unsupported ... operate same column/index` for the whole
//! statement, atomically, before any metadata mutation.
//!
//! The tests without a Go owner pin the job's admission order; they observe
//! the table the way Go's tests do (`show create table`, `show table
//! next_row_id`, `admin check table`, `show warnings`) and read `TableInfo`
//! through `tk.get_table_by_name` (Go `external.GetTableByName`).

use crate::support::testkit::{enable_failpoint, TestKit};
use tidb_executor::TableEntry;
use tidb_table::kv_table::KvTable;
use tidb_util::context::WarningLevel;

/// Go `external.GetTableByName(t, tk, "test", table).Meta()`.
fn table_meta(tk: &TestKit, table: &str) -> std::sync::Arc<KvTable> {
    let TableEntry::Kv(kv) = tk.get_table_by_name("test", table) else {
        panic!("table {table} is not a KV table");
    };
    kv
}

fn index_names(tk: &TestKit, table: &str) -> Vec<String> {
    table_meta(tk, table)
        .indexes()
        .iter()
        .map(|index| index.name.clone())
        .collect()
}

fn column_order(tk: &TestKit, table: &str) -> Vec<String> {
    table_meta(tk, table)
        .columns()
        .iter()
        .map(|column| column.name.clone())
        .collect()
}

/// The last statement's notes and warnings, without the `Error` row a failed
/// statement files beside them (Go's testkit reads `StmtCtx` straight from
/// `session.ExecuteStmt`, before any `AppendError`).
fn notes(tk: &mut TestKit) -> Vec<(u16, String)> {
    tk.session()
        .warnings()
        .iter()
        .filter(|warning| warning.warning_level() != WarningLevel::Error)
        .map(|warning| (warning.code(), warning.message().to_owned()))
        .collect()
}

/// A table as Go's tests observe it: its definition (`show create table`:
/// columns, indexes, AUTO_INCREMENT / AUTO_RANDOM_BASE), its allocators
/// (`show table next_row_id`), its rows, and the domain's schema version
/// (`dom.InfoSchema().SchemaMetaVersion()`); `admin check table` verifies
/// the index entries still match the rows.
#[derive(Debug, PartialEq)]
struct TableImage {
    create: Vec<Vec<String>>,
    next_row_id: Vec<Vec<String>>,
    rows: Vec<Vec<String>>,
    schema_version: u64,
}

fn table_image(tk: &mut TestKit, table: &str) -> TableImage {
    let schema_version = tk.schema_version();
    tk.must_exec(&format!("admin check table {table}"));
    TableImage {
        create: tk.must_query(&format!("show create table {table}")).rows().to_vec(),
        next_row_id: tk
            .must_query(&format!("show table {table} next_row_id"))
            .rows()
            .to_vec(),
        rows: tk.must_query(&format!("select * from {table}")).rows().to_vec(),
        schema_version,
    }
}

#[track_caller]
fn assert_unchanged_table(tk: &mut TestKit, original: &TableImage, table: &str) {
    assert_eq!(&table_image(tk, table), original, "{table}");
}

/// A statement whose job failed while running and rolled back: Go's rollback
/// walks the job's states back, each publishing a schema version, so the
/// version moves while the table's definition, allocators and rows do not.
#[track_caller]
fn assert_rolled_back_table(tk: &mut TestKit, original: &TableImage, table: &str) {
    let after = table_image(tk, table);
    assert!(after.schema_version > original.schema_version, "{table}");
    assert_eq!(
        TableImage {
            schema_version: original.schema_version,
            ..after
        },
        *original,
        "{table}"
    );
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeRenameColumns`
/// success half: `rename column b to c, add column e int default 3` over
/// `t(a, b, index t(a, b))` applies both actions; the renamed column reads
/// its old value under the new name and the row reads `1 2 3`.
#[test]
fn multi_schema_change_rename_columns_applies_both_actions() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2, index t(a, b))");
    tk.must_exec("insert into t values ()");

    tk.must_exec("alter table t rename column b to c, add column e int default 3");
    tk.must_query("select c from t").check(["2"]);
    tk.must_query("select * from t").check(["1 2 3"]);
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeRenameColumns`
/// unsupported-combination block. Go refuses all four statements at job
/// build with 8200, atomically: the original table is intact.
#[test]
fn multi_schema_change_rename_columns_combinations_refuse_conflicting_operations() {
    // add and rename to the same column name
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2)");
    tk.must_exec("insert into t values ()");
    let error = tk.must_get_err_code("alter table t rename column b to c, add column c int", 8200);
    assert_eq!(error.message, "Unsupported operate same column 'c'");
    assert_eq!(column_order(&tk, "t"), vec!["a", "b"]);

    // add a column positioned AFTER the renamed-away column
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2)");
    let error = tk.must_get_err_code("alter table t rename column b to c, add column e int after b", 8200);
    assert_eq!(error.message, "Unsupported operate same column 'b'");
    assert_eq!(column_order(&tk, "t"), vec!["a", "b"]);

    // drop and rename the same column
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2)");
    let error = tk.must_get_err_code("alter table t drop column b, rename column b to c", 8200);
    assert_eq!(error.message, "Unsupported operate same column 'b'");
    assert_eq!(column_order(&tk, "t"), vec!["a", "b"]);

    // add an index over a column the same statement renames away
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2, index t(a, b))");
    let error = tk.must_get_err_code("alter table t rename column b to c, add index t1(a, b)", 8200);
    assert_eq!(error.message, "Unsupported operate same column 'b'");
    assert_eq!(column_order(&tk, "t"), vec!["a", "b"]);
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeAlterColumns`
/// success half: `rename column a to c, alter column b set default 3` applies
/// both actions; after TRUNCATE the fresh row takes the new default (`1 3`).
#[test]
fn multi_schema_change_alter_columns_applies_rename_and_default() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2, index t(a, b))");
    tk.must_exec("insert into t values ()");
    tk.must_query("select * from t").check(["1 2"]);

    tk.must_exec("alter table t rename column a to c, alter column b set default 3");
    tk.must_exec("truncate table t");
    tk.must_exec("insert into t values ()");
    tk.must_query("select * from t").check(["1 3"]);
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeAlterColumns`
/// unsupported-combination block: Go refuses alter+drop, alter+rename and
/// alter+modify naming the same column with 8200, before the default or type
/// change can mutate the table.
#[test]
fn multi_schema_change_alter_columns_combinations_refuse_conflicts() {
    for sql in [
        "alter table t alter column b set default 3, drop column b",
        "alter table t alter column b set default 3, rename column b to c",
        "alter table t alter column b set default 3, modify column b double",
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int default 1, b int default 2)");
        tk.must_exec("insert into t values ()");
        let error = tk.must_get_err_code(sql, 8200);
        assert!(error.message.contains("operate same column 'b'"), "{sql}");
        tk.must_query("select * from t").check(["1 2"]);
    }
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeChangeColumns`
/// success half: `rename column b to c, change column a e bigint default 3`
/// applies both actions; the row reads `1 2` under the new names, and after
/// TRUNCATE the fresh row takes the new default (`3 2`).
#[test]
fn multi_schema_change_change_columns_applies_rename_and_change() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2, index t(a, b))");
    tk.must_exec("insert into t values ()");

    tk.must_exec("alter table t rename column b to c, change column a e bigint default 3");
    tk.must_query("select e, c from t").check(["1 2"]);
    tk.must_exec("truncate table t");
    tk.must_exec("insert into t values ()");
    tk.must_query("select e, c from t").check(["3 2"]);
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeChangeColumns`
/// unsupported-combination block. Go refuses all three with 8200, naming the
/// first conflicting column, before any CHANGE action runs.
#[test]
fn multi_schema_change_change_columns_combinations_refuse_conflicts() {
    // change and drop the same column
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2)");
    let error = tk.must_get_err_code("alter table t change column b c double, drop column b", 8200);
    assert_eq!(error.message, "Unsupported operate same column 'b'");

    // change and add the same column name
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2)");
    let error = tk.must_get_err_code("alter table t change column b c double, add column c int", 8200);
    assert_eq!(error.message, "Unsupported operate same column 'c'");

    // change a column and add an index over its old name
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2, index t(a, b))");
    let error = tk.must_get_err_code("alter table t change column b c double, add index t1(a, b)", 8200);
    assert_eq!(error.message, "Unsupported operate same column 'b'");
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeDropColumnsParallel`.
/// Go submits `drop column if exists b, drop column if exists c` twice
/// (`putTheSameDDLJobTwice`): both runs succeed and the loser files one
/// `Note 1091` per guarded column -- Go's text `column b doesn't exist`
/// (`column.go` wraps `ErrCantDropFieldOrKey`). Serialized here: the
/// second run files the notes, with THIS engine's 1091 spelling
/// `Can't DROP 'b'; check that column/key exists` (the MySQL canonical text
/// `drop_column_action` suppresses) -- same code, different message.
/// Without `IF EXISTS`, Go's second submission is the plain
/// `ErrCantDropFieldOrKey` 1091; the serialized second run errors 1091 too.
#[test]
fn multi_schema_change_drop_columns_parallel_second_run_reports_1091() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c int)");
    for _ in 0..2 {
        tk.must_exec("alter table t drop column if exists b, drop column if exists c");
    }
    let warnings = tk.must_query("show warnings");
    assert_eq!(
        warnings.rows().iter().filter(|row| row[1] == "1091").count(),
        2,
        "one note per guarded column: {:?}",
        warnings.rows()
    );

    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c int)");
    tk.must_exec("alter table t drop column b, drop column c");
    let error = tk.must_get_err_code("alter table t drop column b, drop column c", 1091);
    assert_eq!(error.message, "Can't DROP 'b'; check that column/key exists");
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeDropIndexesParallel`.
/// Same double-submission shape for indexes: `drop index if exists b, drop
/// index if exists c` twice succeeds, the loser filing
/// `Note 1091 index b doesn't exist` (`index.go`) -- and THIS engine's
/// suppressed note text matches Go exactly. Without `IF EXISTS` the second
/// submission is 1091 `index b doesn't exist`, likewise identical.
#[test]
fn multi_schema_change_drop_indexes_parallel_matches_go_notes() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c int, index(a), index(b), index(c))");
    for _ in 0..2 {
        tk.must_exec("alter table t drop index if exists b, drop index if exists c");
    }
    let warnings = tk.must_query("show warnings");
    assert_eq!(
        warnings
            .rows()
            .iter()
            .filter(|row| row[1] == "1091")
            .map(|row| row[2].as_str())
            .collect::<Vec<_>>(),
        vec!["index b doesn't exist", "index c doesn't exist"],
        "Go's exact note texts: {:?}",
        warnings.rows()
    );

    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c int, index (a), index(b), index(c))");
    tk.must_exec("alter table t drop index b, drop index a");
    let error = tk.must_get_err_code("alter table t drop index b, drop index a", 1091);
    assert_eq!(error.message, "index b doesn't exist");
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeAddIndexesCancelled`
/// success-path half (the cancellation itself is the job-queue gap): one
/// ALTER adding four indexes `t(a, b), t1(a), t2(a), t3(a, b)` applies all
/// four; the data survives and reads through all of them.
#[test]
fn multi_schema_change_add_indexes_applies_all_four() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c int)");
    tk.must_exec("insert into t values (1, 2, 3)");

    tk.must_exec("alter table t add index t(a, b), add index t1(a), add index t2(a), add index t3(a, b)");
    let mut names = index_names(&tk, "t");
    names.sort();
    assert_eq!(names, vec!["t", "t1", "t2", "t3"]);
    tk.must_query("select * from t use index(t, t1, t2, t3)").check(["1 2 3"]);
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeDropIndexesCancelled`
/// success-path half: `drop index a, drop index b, drop index idx` over
/// `t(a, b, index(a), unique index(b), index idx(a, b))` removes all three;
/// afterwards `USE INDEX` on any of them answers Go's 1176
/// `ErrKeyDoesNotExist` while the rows themselves stay readable.
#[test]
fn multi_schema_change_drop_indexes_removes_all_and_use_index_answers_1176() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, index(a), unique index(b), index idx(a, b))");
    tk.must_exec("insert into t values (1, 2)");

    // Go: the drop succeeds (MustCancelFailed -- already non-revertible).
    tk.must_exec("alter table t drop index a, drop index b, drop index idx");
    assert!(index_names(&tk, "t").is_empty(), "all three indexes dropped");
    for missing in ["a", "b", "idx"] {
        tk.must_get_err_code(&format!("select * from t use index ({missing})"), 1176);
    }
    tk.must_query("select * from t").check(["1 2"]);
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeRenameIndexes`.
/// `rename index t to x, rename index t1 to x1` applies both; the old names
/// stop resolving (1176) and the new ones serve reads. The combination arms
/// Go refuses with 8200 are rejected before either index mutation. Dropping
/// a covering column ultimately removes its renamed index too, after the
/// rename has been admitted against the original table.
#[test]
fn multi_schema_change_rename_indexes_applies_and_combinations_measured() {
    // rename index
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c int, index t(a), index t1(b))");
    tk.must_exec("alter table t rename index t to x, rename index t1 to x1");
    tk.must_query("select * from t use index (x)");
    tk.must_query("select * from t use index (x1)");
    for gone in ["t", "t1"] {
        tk.must_get_err_code(&format!("select * from t use index ({gone})"), 1176);
    }

    // drop and rename the same index
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c int, index t(a))");
    let error = tk.must_get_err_code("alter table t drop index t, rename index t to t1", 8200);
    assert_eq!(error.message, "Unsupported operate same index 't'");

    // add and rename to the same index name
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c int, index t(a))");
    let error = tk.must_get_err_code("alter table t add index t1(b), rename index t to t1", 8200);
    assert_eq!(error.message, "Unsupported operate same index 't1'");

    // drop a column with its covering index and rename that index: Go
    // retains the index through rename, then completes its column drop.
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2, c int default 3, index t(a))");
    tk.must_exec("insert into t values ()");
    tk.must_exec("alter table t drop column a, rename index t to x");
    assert!(index_names(&tk, "t").is_empty());
    tk.must_query("select * from t").check(["2 3"]);
}

#[test]
fn multi_schema_change_index_metadata_uses_original_names() {
    for (sql, code) in [
        (
            "alter table t add index fresh(b), rename index fresh to renamed",
            1176,
        ),
        ("alter table t drop column a, rename index i to j", 1061),
        ("alter table t drop index j, rename index i to j", 1061),
        ("alter table t rename index missing to missing", 1176),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int, b int, index i(a), index j(b))");
        tk.must_exec("insert into t values (1,2)");
        let original = table_image(&mut tk, "t");
        tk.must_get_err_code(sql, code);
        assert_unchanged_table(&mut tk, &original, "t");
    }
}

#[test]
fn multi_schema_change_index_rename_noops_do_not_create_conflicts() {
    for (sql, remaining) in [
        ("alter table t rename index i to i", vec!["i"]),
        (
            "alter table t rename index i to i, rename index i to j",
            vec!["j"],
        ),
        ("alter table t drop index i, rename index i to i", vec![]),
        ("alter table t drop column a, rename index i to i", vec![]),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int, b int, index i(a))");
        tk.must_exec(sql);
        assert_eq!(index_names(&tk, "t"), remaining, "{sql}");
    }
}

#[test]
fn multi_schema_change_expression_index_rename_preserves_identity() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, index expr_i((a+1)))");
    tk.must_exec("insert into t values (1)");
    let original = table_meta(&tk, "t");
    let index_id = original.indexes()[0].id;
    let hidden_id = original.columns()[1].id;
    tk.must_exec("alter table t rename index expr_i to expr_j");
    let table = table_meta(&tk, "t");
    assert_eq!(table.columns()[1].name, "_V$_expr_j_0");
    assert_eq!(table.columns()[1].id, hidden_id);
    assert_eq!(table.indexes()[0].id, index_id);
    // The renamed index's entries still match the rows.
    tk.must_exec("admin check table t");
    tk.must_query("select a from t use index (expr_j) where a+1=2").check(["1"]);
    // Go can reuse the old expression-index name after its hidden columns move.
    tk.must_exec("alter table t add index expr_i((a+2))");
    assert_eq!(index_names(&tk, "t"), vec!["expr_j", "expr_i"]);
}

#[test]
fn multi_schema_change_index_jobs_exclude_conditional_noops() {
    for (sql, expected) in [
        (
            "alter table t add index if not exists i(missing), drop index i",
            vec![],
        ),
        (
            "alter table t drop index if exists j, add index j(b)",
            vec!["i", "j"],
        ),
        (
            "alter table t add column (c int, index if not exists i(a)), drop index i",
            vec![],
        ),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int, b int, index i(a))");
        tk.must_exec(sql);
        // Go `tk.Session().GetSessionVars().StmtCtx.WarningCount()`.
        assert_eq!(tk.session().warnings().len(), 1, "one admission note: {sql}");
        assert_eq!(index_names(&tk, "t"), expected, "{sql}");
    }
}

#[test]
fn multi_schema_change_index_preparation_precedes_backfill() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int)");
    tk.must_exec("insert into t values (1),(1)");
    let original = table_image(&mut tk, "t");
    // Job admission precedes the unique backfill.
    tk.must_get_err_code("alter table t add unique index u(a), rename index missing to j", 1176);
    assert_unchanged_table(&mut tk, &original, "t");
}

#[test]
fn multi_schema_change_index_jobs_use_resolved_anonymous_names() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, index a(a))");
    let error = tk.must_get_err_code("alter table t add index(a), add index(a)", 8200);
    assert_eq!(error.message, "Unsupported operate same index 'a_2'");
    // The original duplicate is reported before job conflicts.
    tk.must_get_err_code("alter table t drop index a, add index a(a)", 1061);
}

#[test]
fn multi_schema_change_index_preparation_preserves_action_error_order() {
    for (sql, code) in [
        (
            "alter table t add column a int, rename index missing to j",
            1060,
        ),
        (
            "alter table t rename index missing to j, add column a int",
            1176,
        ),
        ("alter table t drop column missing, add index i(a)", 1091),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int, b int, index i(a))");
        let original = table_image(&mut tk, "t");
        tk.must_get_err_code(sql, code);
        assert_unchanged_table(&mut tk, &original, "t");
    }
}

#[test]
fn multi_schema_change_index_single_statement_and_noop_admission() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, index i(a))");
    tk.must_exec("alter table t rename index i to I");
    assert_eq!(index_names(&tk, "t"), vec!["I"]);
    let original = table_image(&mut tk, "t");
    tk.must_exec("alter table t alter index I visible");
    assert_unchanged_table(&mut tk, &original, "t");
    // A case-only rename is still a conflicting multi-schema job.
    tk.must_get_err_code("alter table t rename index I to i, add column b int", 8200);
    assert_unchanged_table(&mut tk, &original, "t");
}

#[test]
fn multi_schema_change_index_notes_follow_action_admission_order() {
    for (sql, expected_notes) in [
        (
            "alter table t add column a int, drop index if exists missing",
            0,
        ),
        (
            "alter table t drop index if exists missing, add column a int",
            1,
        ),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int)");
        tk.must_get_err_code(sql, 1060);
        assert_eq!(notes(&mut tk).len(), expected_notes, "{sql}");
    }
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeAlterIndex`
/// mixed-with-modify half (the failpoint-interleaved read is the gap): one
/// statement `alter index i1 invisible, modify column a tinyint, alter index
/// i2 invisible` applies all three; both indexes stop serving (1176) and the
/// row survives the tinyint rewrite (`1 2`).
#[test]
fn multi_schema_change_alter_index_mixed_with_modify_applies() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, index i1(a, b), index i2(b))");
    tk.must_exec("insert into t values (1, 2)");

    tk.must_exec("alter table t alter index i1 invisible, modify column a tinyint, alter index i2 invisible");
    for missing in ["i1", "i2"] {
        tk.must_get_err_code(&format!("select * from t use index ({missing})"), 1176);
    }
    tk.must_query("select * from t").check(["1 2"]);
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeAlterIndex`
/// unsupported-combination block. Go refuses all three alter/drop/add
/// combinations naming the same index with 8200, before visibility or index
/// metadata changes.
#[test]
fn multi_schema_change_alter_index_combinations_refuse_conflicts() {
    // alter the same index twice
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, index idx(a, b))");
    let error = tk.must_get_err_code("alter table t alter index idx visible, alter index idx invisible", 8200);
    assert_eq!(error.message, "Unsupported operate same index 'idx'");

    // drop and alter the same index
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, index idx(a, b))");
    let error = tk.must_get_err_code("alter table t drop index idx, alter index idx visible", 8200);
    assert_eq!(error.message, "Unsupported operate same index 'idx'");

    // Go validates the visibility source before collecting index conflicts:
    // a sibling ADD does not supply the original index.
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_get_err_code("alter table t add index idx(a, b), alter index idx invisible", 1176);
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeModifyColumnOrderByStates`:
/// five statements mixing MODIFY COLUMN (including positional moves and a
/// two-column position swap) with ADD and DROP COLUMN all apply in one
/// statement each -- the order the sub-jobs run in never matters for the
/// final metadata.
#[test]
fn multi_schema_change_modify_column_order_by_states_all_orders_apply() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("insert into t values (1, 1)");
    tk.must_exec("alter table t modify column b smallint, add column d int");

    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("insert into t values (1, 1)");
    tk.must_exec("alter table t modify column a smallint, add column c int, modify column b smallint");

    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c char(10))");
    tk.must_exec("insert into t values (1, 1, '1')");
    tk.must_exec(
        "alter table t modify column c int after a, add column d int, add column e int, modify column b smallint",
    );

    let mut tk = TestKit::new();
    tk.must_exec("create table t (id bigint, c1 bigint, c2 bigint)");
    tk.must_exec("alter table t modify column c2 int after id, modify column id int after c2");
    // Sequential move semantics: `c2 after id` turns [id, c1, c2] into
    // [id, c2, c1], then `id after c2` turns that into [c2, id, c1]. Go's
    // sub-job order is re-planned by the owner; this test pins only that the
    // statement applies.
    assert_eq!(
        column_order(&tk, "t"),
        vec!["c2", "id", "c1"],
        "measured sequential order"
    );

    let mut tk = TestKit::new();
    tk.must_exec("create table t1 (id bigint, c1 bigint, c2 bigint)");
    tk.must_exec("insert into t1 values (1, 1, 1)");
    tk.must_exec("alter table t1 modify column c2 int, drop column id");
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeRenameTable` tail
/// half (the racing rename-under-failpoint is the gap): after the table is
/// renamed to `t1`, the rename+change ALTER applies there and the data is
/// intact (`admin check table t1`).
#[test]
fn multi_schema_change_rename_table_then_alter_leaves_consistent_table() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int default 1, b int default 2, index t(a, b))");
    tk.must_exec("insert into t values (1, 2)");

    tk.must_exec("alter table t rename to t1");
    tk.must_query("select * from t1").check(["1 2"]);

    tk.must_exec("alter table t1 rename column b to c, change column a e bigint default 3");
    tk.must_query("select * from t1").check(["1 2"]);
    tk.must_exec("admin check table t1");
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeWithExpressionIndex`.
/// The refusals and the duplicate-detection arm, minus the failpoint:
/// expression-index dependencies participate in the same preflight as
/// ordinary index columns. Go refuses the two dependency conflicts at job
/// build with 8200, atomically; the duplicate-entry arm remains a later
/// 1062 backfill error and rolls back the earlier column/index changes.
#[test]
fn multi_schema_change_expression_index_combinations_measured() {
    // drop column a, add unique index idx((a + b))
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("insert into t values (1, 2), (2, 1)");
    let error = tk.must_get_err_code("alter table t drop column a, add unique index idx((a + b))", 8200);
    assert_eq!(error.message, "Unsupported operate same column 'a'");

    // add column c, change column a d bigint, add index idx((a + a))
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("insert into t values (1, 2), (2, 1)");
    let error = tk.must_get_err_code(
        "alter table t add column c int, change column a d bigint, add index idx((a + a))",
        8200,
    );
    assert_eq!(error.message, "Unsupported operate same column 'a'");

    // add column c default 10, add index idx1((a + b)), add unique index idx2((a + b)):
    // the unique index backfill collides on a+b = 3.
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("insert into t values (1, 2), (2, 1)");
    let original = table_image(&mut tk, "t");
    tk.must_get_err_code(
        "alter table t add column c int default 10, add index idx1((a + b)), add unique index idx2((a + b))",
        1062,
    );
    tk.must_query("select * from t").check(["1 2", "2 1"]);
    assert_rolled_back_table(&mut tk, &original, "t");
}

fn assert_failed_alter_keeps_allocators(definition: &str, sql: &str, code: u16) {
    assert_failed_alter_keeps_allocators_with(definition, sql, code, assert_unchanged_table);
}

/// [`assert_failed_alter_keeps_allocators`] for a statement whose job fails
/// while running (a backfill duplicate, a forced rebase the allocator
/// refuses), which Go rolls back.
fn assert_rolled_back_alter_keeps_allocators(definition: &str, sql: &str, code: u16) {
    assert_failed_alter_keeps_allocators_with(definition, sql, code, assert_rolled_back_table);
}

fn assert_failed_alter_keeps_allocators_with(
    definition: &str,
    sql: &str,
    code: u16,
    check: fn(&mut TestKit, &TableImage, &str),
) {
    let mut tk = TestKit::new();
    tk.must_exec(definition);
    tk.must_exec("insert into t(v) values (1), (1)");
    // The image carries the allocators: `show table next_row_id` and the
    // AUTO_INCREMENT / AUTO_RANDOM_BASE of `show create table`.
    let original = table_image(&mut tk, "t");
    tk.must_get_err_code(sql, code);
    check(&mut tk, &original, "t");
}

#[test]
fn multi_schema_change_failed_alter_keeps_auto_increment_rebases() {
    for sql in [
        "alter table t auto_increment=1000000, add column v int",
        "alter table t force auto_increment=1000000, add column v int",
    ] {
        assert_failed_alter_keeps_allocators(
            "create table t (id bigint auto_increment primary key, v int)",
            sql,
            1060,
        );
    }
}

#[test]
fn multi_schema_change_refuses_auto_random_rebases_without_consuming_ids() {
    for sql in [
        "alter table t auto_random_base=1000000, add column v int",
        "alter table t force auto_random_base=1000000, add column v int",
    ] {
        assert_failed_alter_keeps_allocators(
            "create table t (id bigint auto_random(3) primary key, v int)",
            sql,
            8200,
        );
    }
}

#[test]
fn multi_schema_change_failed_alter_keeps_auto_random_layout_counter() {
    assert_failed_alter_keeps_allocators(
        "create table t (id bigint auto_random(3) primary key, v int)",
        "alter table t modify id bigint auto_random(4), add column v int",
        1060,
    );
}

#[test]
fn multi_schema_change_failed_alter_keeps_auto_increment_conversion_counter() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (id bigint auto_increment primary key, v int)");
    tk.must_exec("insert into t(v) values (1), (1)");
    // Go converts AUTO_INCREMENT to AUTO_RANDOM only under
    // `@@tidb_allow_remove_auto_inc` (`GetModifiableColumnJob`); without it
    // the MODIFY is refused before ADD COLUMN is reached.
    tk.must_exec("set @@tidb_allow_remove_auto_inc = 1");
    let original = table_image(&mut tk, "t");
    tk.must_get_err_code("alter table t modify id bigint auto_random(3), add column v int", 1060);
    assert_unchanged_table(&mut tk, &original, "t");
}

#[test]
fn multi_schema_change_failed_alter_keeps_rebases_after_backfill_error() {
    assert_rolled_back_alter_keeps_allocators(
        "create table t (id bigint auto_increment primary key, v int)",
        "alter table t auto_increment=1000000, add unique index idx(v)",
        1062,
    );
}

#[test]
fn multi_schema_change_failed_alter_keeps_rebases_after_invalid_force_base() {
    assert_rolled_back_alter_keeps_allocators(
        "create table t (id bigint auto_increment primary key, v int)",
        "alter table t auto_increment=1000000, force auto_increment=0",
        1467,
    );
}

#[test]
fn multi_schema_change_refuses_random_job_before_layout_execution() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (id bigint auto_random(3) primary key, v int)");
    tk.must_exec(&format!("alter table t auto_random_base = {}", 1_i64 << 50));
    let original = table_image(&mut tk, "t");
    // Go refuses the AUTO_RANDOM job before either the counter or layout changes.
    let error = tk.must_get_err_code(
        "alter table t force auto_random_base=1000000, modify id bigint auto_random(15)",
        8200,
    );
    assert_eq!(error.message, "Unsupported multi schema change for rebase auto_random ID");
    assert_unchanged_table(&mut tk, &original, "t");
}

#[test]
fn multi_schema_change_executes_prepared_rebases_on_success() {
    for sql in [
        "alter table t auto_increment=1000000, add column c int default 3",
        "alter table t force auto_increment=1000000, add column c int default 3",
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (id bigint auto_increment primary key, v int)");
        tk.must_exec("insert into t(v) values (1)");
        tk.must_exec(sql);
        tk.must_exec("insert into t(v) values (2)");
        tk.must_query("select id,v,c from t where v=2").check(["1000000 2 3"]);
    }
    for option in ["auto_random_base=1000000", "force auto_random_base=1000000"] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (id bigint auto_random(3) primary key, v int)");
        tk.must_exec(&format!("alter table t {option}"));
        // `NEXT_GLOBAL_ROW_ID` of the AUTO_RANDOM allocator.
        let next_row_id = tk.must_query("show table t next_row_id");
        assert_eq!(next_row_id.rows()[0][3], "1000000", "{option}");
    }
}

#[test]
fn multi_schema_change_failed_column_preserves_statement_image() {
    for sql in [
        "alter table t add column added int default 3, add column a int",
        "alter table t add column (added int default 3, a int)",
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int, b int, index b_idx(b))");
        tk.must_exec("insert into t values (1, 2)");
        let original = table_image(&mut tk, "t");
        tk.must_get_err_code(sql, 1060);
        assert_unchanged_table(&mut tk, &original, "t");
        tk.must_query("select * from t").check(["1 2"]);
    }
}

#[test]
fn multi_schema_change_failed_action_restores_dropped_index_entries() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, index b_idx(b))");
    tk.must_exec("insert into t values (1, 2)");
    let original = table_image(&mut tk, "t");
    tk.must_get_err_code("alter table t drop index b_idx, add column a int", 1060);
    assert_unchanged_table(&mut tk, &original, "t");
    tk.must_query("select * from t force index(b_idx) where b = 2").check(["1 2"]);
}

#[test]
fn multi_schema_change_failed_action_restores_referencing_table_metadata() {
    let mut tk = TestKit::new();
    tk.must_exec("create table parent (a int primary key, b int)");
    tk.must_exec("create table child (a int, foreign key (a) references parent(a))");
    tk.must_exec("insert into parent values (1, 2)");
    tk.must_exec("insert into child values (1)");
    let parent = table_image(&mut tk, "parent");
    let child = table_image(&mut tk, "child");
    tk.must_get_err_code("alter table parent rename column a to renamed, add column b int", 1060);
    assert_unchanged_table(&mut tk, &parent, "parent");
    assert_unchanged_table(&mut tk, &child, "child");
    assert_eq!(table_meta(&tk, "child").foreign_keys()[0].ref_cols, vec!["a"]);
    tk.must_exec("insert into child values (1)");
}

/// Go `multi_schema_change_test.go::TestMultiSchemaChangeWithExpressionIndex`
/// success arm (the failpoint-interleaved UPDATE is the gap): one ALTER adds
/// column `c`, a non-unique expression index and a unique expression index
/// over a DIFFERENT expression; the rows read through both indexes as
/// `1 2 10` / `2 1 10`.
#[test]
fn multi_schema_change_expression_index_success_applies() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("insert into t values (1, 2), (2, 1)");

    tk.must_exec(
        "alter table t add column c int default 10, add index idx1((a + b)), add unique index idx2((a*10 + b))",
    );
    tk.must_query("select * from t use index(idx1, idx2)").check(["1 2 10", "2 1 10"]);
}

#[test]
fn multi_schema_change_index_identity_survives_sibling_replacement() {
    for action in ["rename index i to j", "drop index i"] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int, b int, index i(b))");
        tk.must_exec("insert into t values (1,2)");
        tk.must_exec(&format!("alter table t drop column b, add index fresh(a), {action}"));
        assert_eq!(index_names(&tk, "t"), vec!["fresh"], "{action}");
        tk.must_query("select * from t force index(fresh) where a=1").check(["1"]);
    }
}

#[test]
fn multi_schema_change_retains_metadata_id_high_water_marks() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, index i(b))");
    tk.must_exec("alter table t drop column b");
    tk.must_exec("create table copy like t");
    for name in ["t", "copy"] {
        tk.must_exec(&format!("alter table {name} add column c int default 7, add index j(a)"));
        let table = table_meta(&tk, name);
        assert_eq!(table.columns()[1].id, 3, "{name}");
        assert_eq!(table.indexes()[0].id, 2, "{name}");
    }
}

#[test]
fn multi_schema_change_column_jobs_exclude_conditional_noops() {
    for (sql, expected, note) in [
        (
            "alter table t add column if not exists a int, drop column a",
            vec!["b"],
            "1060",
        ),
        (
            "alter table t drop column if exists c, add column c int",
            vec!["a", "b", "c"],
            "1091",
        ),
        (
            "alter table t modify column if exists c bigint, add column c int",
            vec!["a", "b", "c"],
            "1054",
        ),
        (
            "alter table t change column if exists missing c bigint, add column c int",
            vec!["a", "b", "c"],
            "1054",
        ),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int, b int)");
        tk.must_exec(sql);
        let warnings = tk.must_query("show warnings");
        assert_eq!(warnings.rows().len(), 1, "{sql}");
        assert_eq!(warnings.rows()[0][1], note, "{sql}");
        assert_eq!(column_order(&tk, "t"), expected, "{sql}");
    }
}

#[test]
fn multi_schema_change_column_admission_uses_original_schema() {
    for (sql, code) in [
        ("alter table t add column c int, rename column c to d", 1054),
        (
            "alter table t add column c int, modify column c bigint",
            1054,
        ),
        (
            "alter table t add column c int, alter column c set default 7",
            1054,
        ),
        ("alter table t add column c int, drop column c", 1091),
        ("alter table t drop column b, rename column a to b", 1060),
        ("alter table t drop column b, change column a b int", 1060),
        ("alter table t drop column b, add column b int", 1060),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int, b int)");
        let original = table_image(&mut tk, "t");
        tk.must_get_err_code(sql, code);
        assert_unchanged_table(&mut tk, &original, "t");
    }
}

#[test]
fn multi_schema_change_column_admission_precedes_row_rewrite() {
    for (sql, code) in [
        (
            "alter table t modify column a tinyint, drop column missing",
            1091,
        ),
        (
            "alter table t change column a c tinyint, rename column missing to d",
            1054,
        ),
        (
            "alter table t add unique index u(b), drop column missing",
            1091,
        ),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (a int, b int)");
        tk.must_exec("insert into t values (300,1),(301,1)");
        let original = table_image(&mut tk, "t");
        tk.must_get_err_code(sql, code);
        assert_unchanged_table(&mut tk, &original, "t");
    }
}

#[test]
fn multi_schema_change_column_notes_precede_conflict_validation() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_get_err_code(
        "alter table t add column if not exists a int, modify column b bigint, drop column b",
        8200,
    );
    let notes = notes(&mut tk);
    assert_eq!(notes.len(), 1, "{notes:?}");
    assert_eq!(notes[0].0, 1060);
}

#[test]
fn multi_schema_change_column_dependencies_use_original_schema() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, index expr_i((a+1)))");
    let original = table_image(&mut tk, "t");
    tk.must_get_err_code("alter table t drop index expr_i, modify column a bigint", 3106);
    assert_unchanged_table(&mut tk, &original, "t");
}

#[test]
fn multi_schema_change_prepared_columns_resolve_current_positions() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c int)");
    tk.must_exec("insert into t values (1,2,3)");
    tk.must_exec(
        "alter table t add column x int default 7 first, modify column c bigint after a, add column g int as (b+10)",
    );
    assert_eq!(column_order(&tk, "t"), vec!["x", "a", "c", "b", "g"]);
    tk.must_query("select * from t").check(["7 1 3 2 12"]);
}

#[test]
fn multi_schema_change_prepared_random_column_tracks_current_offset() {
    for (definition, sql, expected) in [
        (
            "create table t (id bigint auto_random(3) primary key, v int)",
            "alter table t add column c int default 7 first",
            1,
        ),
        (
            "create table t (id bigint auto_random(3) primary key, v int)",
            "alter table t modify column id bigint auto_random(4) after v",
            1,
        ),
        (
            "create table t (c int, id bigint auto_random(3) primary key, v int)",
            "alter table t drop column c",
            0,
        ),
        (
            "create table t (id bigint auto_random(3) primary key, v int)",
            "alter table t add column c int default 7 first, modify column id bigint auto_random(4)",
            1,
        ),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec(definition);
        tk.must_exec(sql);
        assert_eq!(table_meta(&tk, "t").auto_random().unwrap().offset, expected, "{sql}");
        tk.must_exec("insert into t(v) values (5)");
        tk.must_query("select v from t").check(["5"]);
    }
}

#[test]
fn multi_schema_change_preparation_publishes_notes_before_execution() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("insert into t values (300,1)");
    // Go `getModifyColumnType`: INT to TINYINT in strict mode with no index
    // is `ModifyTypeNoReorgWithCheck`, whose `checkModifyColumnData` finds
    // the out-of-range row: "Data truncated for column 'a', value is '300'".
    tk.must_get_err_code(
        "alter table t modify column a tinyint, add column if not exists b int",
        1265,
    );
    let notes = notes(&mut tk);
    assert_eq!(notes.len(), 1, "{notes:?}");
    assert_eq!(notes[0].0, 1060);
}

/// Go admits metadata jobs before any sibling index backfill scans rows.
#[test]
fn multi_schema_change_metadata_admission_precedes_backfill() {
    for (tail, code) in [
        ("rename to occupied", 1050),
        ("charset=utf8 collate=latin1_bin", 1253),
        ("ttl = missing + interval 1 day", 1054),
        ("auto_id_cache=1", 1105),
        ("compression='lz4'", 8200),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (id int, v int)");
        tk.must_exec("create table occupied (id int)");
        tk.must_exec("insert into t values (1,2),(2,2)");
        let original = table_image(&mut tk, "t");
        tk.must_get_err_code(&format!("alter table t add unique index idx_v(v), {tail}"), code);
        assert_unchanged_table(&mut tk, &original, "t");
    }
}

#[test]
fn multi_schema_change_metadata_jobs_follow_go_allowlist() {
    for (spec, job) in [
        ("rename to moved", "rename table"),
        ("auto_id_cache=100", "modify auto id cache"),
        ("ttl = created + interval 1 day", "alter table ttl"),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (id int, created timestamp)");
        let original = table_image(&mut tk, "t");
        let error = tk.must_get_err_code(&format!("alter table t {spec}, add column c int"), 8200);
        assert_eq!(error.message, format!("Unsupported multi schema change for {job}"));
        assert_unchanged_table(&mut tk, &original, "t");
    }
}

#[test]
fn multi_schema_change_metadata_noops_do_not_create_jobs() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (id int)");
    // Go's ALTER TABLE has no arm for COMPRESSION (its option loop answers
    // `ErrUnsupportedAlterTableOption`), so it is not one of the no-ops.
    tk.must_exec(
        "alter table t remove ttl, rename to t, engine=innodb, row_format=compact, add column c int",
    );
    assert_eq!(column_order(&tk, "t"), vec!["id", "c"]);
}

#[test]
fn multi_schema_change_charset_options_share_go_resolution() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (id int) charset=utf8mb4");
    tk.must_exec("alter table t charset=utf8mb4 charset=utf8mb4 collate=utf8mb4_bin, add column c int");
    assert_eq!(column_order(&tk, "t"), vec!["id", "c"]);
    tk.must_get_err_code("alter table t collate=utf8_bin charset=utf8mb4", 1302);
}

#[test]
fn multi_schema_change_table_options_run_in_order() {
    // Go `AlterTable`'s option loop runs AUTO_ID_CACHE's executor before it
    // reaches COMPRESSION's `ErrUnsupportedAlterTableOption` (gorun).
    let mut tk = TestKit::new();
    tk.must_exec("create table t (id int)");
    let error = tk.must_get_err_code("alter table t auto_id_cache=1 compression='lz4'", 1105);
    assert_eq!(
        error.message,
        "Can't Alter AUTO_ID_CACHE between 1 and non-1, the underlying implementation is different"
    );
    let error = tk.must_get_err_code("alter table t compression='lz4' auto_id_cache=1", 8200);
    assert_eq!(error.message, "This type of ALTER TABLE is currently unsupported");
}

#[test]
fn multi_schema_change_metadata_uses_original_columns_and_publishes_notes() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (id int, v int)");
    tk.must_get_err_code(
        "alter table t add column created timestamp, ttl=created + interval 1 day",
        1054,
    );
    assert_eq!(column_order(&tk, "t"), vec!["id", "v"]);
    tk.must_exec("insert into t values (1,2),(2,2)");
    tk.must_get_err_code("alter table t add unique index idx_v(v), with validation", 1062);
    let notes = notes(&mut tk);
    assert_eq!(notes.len(), 1, "{notes:?}");
    assert_eq!(notes[0].0, 8200);
}

#[test]
fn multi_schema_change_metadata_single_jobs_and_siblings_keep_values() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (id int, created timestamp)");
    tk.must_exec("alter table t ttl=created + interval 1 day");
    let original = table_image(&mut tk, "t");
    let error = tk.must_exec_to_err("alter table t remove ttl, add column c int");
    assert_eq!(error.message, "Unsupported multi schema change for alter table no_ttl");
    assert_unchanged_table(&mut tk, &original, "t");
    tk.must_exec("alter table t remove ttl");
    tk.must_exec("alter table t auto_id_cache=100");
    tk.must_exec("alter table t comment='kept', add column c int default 3");
    tk.must_exec("alter table t rename to moved");
    let table = table_meta(&tk, "moved");
    assert_eq!(table.auto_id_cache(), 100);
    assert!(table.ttl_info().is_none());
    assert_eq!(table.comment(), "kept");
    assert_eq!(column_order(&tk, "moved"), vec!["id", "created", "c"]);
}

#[test]
fn multi_schema_change_rebase_uses_heap_allocator_without_auto_column() {
    for option in ["auto_increment=1000000", "force auto_increment=1000000"] {
        let mut tk = TestKit::new();
        tk.must_exec("create table t (v int)");
        tk.must_exec("insert into t values (1)");
        tk.must_exec(&format!("alter table t {option}, add column c int default 3"));
        tk.must_exec("insert into t(v) values (2)");
        tk.must_query("select _tidb_rowid,v,c from t where v=2").check(["1000000 2 3"]);
    }
}



// Go `ResolveAlterTableSpec` + `ResolveAlterAlgorithm`: the last ALGORITHM
// is resolved against every valid spec before any of them runs. ADD
// CONSTRAINT runs INPLACE, everything else INSTANT; a cheaper request is an
// error-level 1846 warning per spec, a dearer one fails the statement.
// Answers captured with gorun.
#[test]
fn alter_table_algorithm_resolves_per_spec_like_go() {
    let inplace_for_instant = "Error 1846 ALGORITHM=INPLACE is not supported. Reason: Cannot alter table by INPLACE. Try ALGORITHM=INSTANT.";
    let copy_for_instant = "Error 1846 ALGORITHM=COPY is not supported. Reason: Cannot alter table by COPY. Try ALGORITHM=INSTANT.";
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_get_err_msg(
        "alter table t add index i(a), algorithm=instant",
        "[ddl:1846]ALGORITHM=INSTANT is not supported. Reason: Cannot alter table by INSTANT. Try ALGORITHM=INPLACE.",
    );
    tk.must_exec("alter table t add index i(a), algorithm=copy");
    tk.must_query("show warnings").check([
        "Error 1846 ALGORITHM=COPY is not supported. Reason: Cannot alter table by COPY. Try ALGORITHM=INPLACE.",
    ]);
    tk.must_exec("alter table t add column c int, algorithm=inplace");
    tk.must_query("show warnings").check([inplace_for_instant]);
    tk.must_exec("alter table t add index j(b), algorithm=inplace");
    tk.must_query("show warnings").check_empty();
    tk.must_exec("alter table t algorithm=inplace, lock=none");
    tk.must_query("show warnings").check_empty();
    tk.must_exec("alter table t drop index i, add index k(b), algorithm=inplace");
    tk.must_query("show warnings").check([inplace_for_instant]);
    // A grouped ADD COLUMN is split first: its index is an ADD CONSTRAINT.
    tk.must_get_err_code("alter table t add column (d int, index d1(d)), algorithm=instant", 1846);
    tk.must_get_err_code("alter table nosuch add index i(a), algorithm=instant", 1146);
    tk.must_get_err_code("alter table nosuch algorithm=inplace", 1146);
    tk.must_exec("alter table t add column (e int, f int), algorithm=copy");
    tk.must_query("show warnings").check([copy_for_instant, copy_for_instant]);
    assert_eq!(index_names(&tk, "t"), vec!["j", "k"]);
    tk.must_query("select column_name from information_schema.columns where table_schema = 'test' and table_name = 't' order by ordinal_position")
        .check(["a", "b", "c", "e", "f"]);
}

// Go `AlterTable`: RENAME TO beside another spec runs `renameTable`, whose
// job `fillMultiSchemaInfo` has no sub-job for, so `DoDDLJob` refuses it and
// nothing changes (gorun: `[ddl:8200]Unsupported multi schema change for
// rename table`).
#[test]
fn multi_schema_change_refuses_a_rename_beside_another_spec() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (v int)");
    let error = tk.must_get_err_code("alter table t add column w bigint, rename to t2", 8200);
    assert_eq!(error.message, "Unsupported multi schema change for rename table");
    tk.must_query("select column_name from information_schema.columns where table_schema = 'test' and table_name = 't'")
        .check(["v"]);
}

// Go `AlterTable`'s `AlterTableSetTiFlashReplica` arm runs for a HYPO replica
// beside other specs: `setHypoTiFlashReplica` writes the session's
// `HypoTiFlashReplicas` and the other specs run as usual (gorun: the column
// lands and EXPLAIN reads `mpp[tiflash]` until the replica is reset).
#[test]
fn hypo_tiflash_replica_beside_another_spec_is_session_state() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("alter table t set hypo tiflash replica 1, add column c int");
    tk.must_query("select column_name from information_schema.columns where table_schema = 'test' and table_name = 't' order by ordinal_position")
        .check(["a", "b", "c"]);
    tk.must_query("explain format='brief' select a from t").check_contain("mpp[tiflash]");
    tk.must_exec("alter table t set hypo tiflash replica 0, drop column c");
    tk.must_query("explain format='brief' select a from t").check_not_contain("mpp[tiflash]");
    tk.must_query("select column_name from information_schema.columns where table_schema = 'test' and table_name = 't' order by ordinal_position")
        .check(["a", "b"]);
    // Another session has none of it.
    tk.must_exec("alter table t set hypo tiflash replica 1");
    let mut other = tk.new_tk();
    other.must_exec("use test");
    other.must_query("explain format='brief' select a from t").check_not_contain("mpp[tiflash]");
}

// Go `DDLExec.Next` has no hypothetical exemption: `NewTxnInStmt` commits the
// open transaction before `CREATE INDEX ... TYPE HYPO`, `DROP HYPO INDEX`
// and `SET HYPO TIFLASH REPLICA` alike (gorun: each ROLLBACK keeps the row).
#[test]
fn hypothetical_ddl_commits_the_open_transaction_like_go() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    for (row, ddl, count) in [
        (1, "create index h on t(b) type hypo", "1"),
        (2, "alter table t set hypo tiflash replica 1", "2"),
        (3, "drop hypo index h on t", "3"),
    ] {
        tk.must_exec("begin");
        tk.must_exec(&format!("insert into t values ({row}, {row})"));
        tk.must_exec(ddl);
        tk.must_exec("rollback");
        tk.must_query("select count(*) from t").check([count]);
    }
}

// Go `AlterTableSetTiFlashReplica` runs `isTableTiFlashSupported` before it
// builds a job or records a hypothetical replica (gorun).
#[test]
fn set_tiflash_replica_admission_is_gos() {
    let mut tk = TestKit::new();
    tk.must_exec("create table g (a varchar(10)) charset gbk");
    for sql in ["alter table g set tiflash replica 1", "alter table g set hypo tiflash replica 1"] {
        let error = tk.must_get_err_code(sql, 8200);
        assert_eq!(
            error.message,
            "Unsupported `set TiFlash replica` settings for table contains gbk charset",
            "{sql}"
        );
    }
    tk.must_exec("create global temporary table tt (a int) on commit delete rows");
    let error = tk.must_get_err_code("alter table tt set tiflash replica 1", 8006);
    assert_eq!(error.message, "`set TiFlash replica` is unsupported on temporary tables.");
    tk.must_exec("create view v as select 1");
    let error = tk.must_get_err_code("alter table v set tiflash replica 1", 1347);
    assert_eq!(error.message, "'test.v' is not BASE TABLE");
    let error = tk.must_get_err_code("alter table mysql.user set tiflash replica 1", 8200);
    assert_eq!(
        error.message,
        "Unsupported `set TiFlash replica` settings for system table and memory table"
    );
}

// Go `checkTiFlashReplicaCount`: without TiFlash servers a replica count
// above zero is refused, for the table (`AlterTableSetTiFlashReplica`, after
// the hypo branch) and for every table of a database
// (`ModifySchemaSetTiFlashReplica`, after its database checks) (gorun).
// With Go's `mockTiFlashStoreCount` the database's tables get the replica.
#[test]
fn tiflash_replica_count_is_checked_against_tiflash_servers_like_go() {
    let count_error = "the tiflash replica count: 1 should be less than the total tiflash server count: 0";
    let mut tk = TestKit::new();
    tk.must_exec("create database d1");
    tk.must_exec("create database d2");
    tk.must_exec("create table d2.t (a int)");
    let error = tk.must_get_err_code("alter table d2.t set tiflash replica 1", 1105);
    assert_eq!(error.message, count_error);
    tk.must_exec("alter table d2.t set tiflash replica 0");
    tk.must_exec("alter table d2.t set hypo tiflash replica 1");
    let error = tk.must_get_err_code("alter database d1 set tiflash replica 1", 1049);
    assert_eq!(error.message, "Empty database 'd1'");
    let error = tk.must_get_err_code("alter database mysql set tiflash replica 1", 8200);
    assert_eq!(
        error.message,
        "Unsupported `set TiFlash replica` settings for system table and memory table"
    );
    for virtual_schema in ["information_schema", "metrics_schema"] {
        let error = tk.must_get_err_code(
            &format!("alter database {virtual_schema} set tiflash replica 1"),
            8200,
        );
        assert_eq!(
            error.message,
            "Unsupported `set TiFlash replica` settings for system table and memory table"
        );
    }
    let error = tk.must_get_err_code("alter database nod set tiflash replica 1", 1049);
    assert_eq!(error.message, "Unknown database 'nod'");
    let error = tk.must_get_err_code("alter database d2 set tiflash replica 1", 1105);
    assert_eq!(error.message, count_error);
    tk.must_exec("alter database d2 set tiflash replica 0");

    let _store_count = enable_failpoint("mockTiFlashStoreCount", "return(true)");
    tk.must_exec("alter database d2 set tiflash replica 2");
    tk.must_query(
        "select table_name, replica_count from information_schema.tiflash_replica \
         where table_schema = 'd2'",
    )
    .check(["t 2"]);
}

// Go `createIndex` / `dropIndex` with a hypothetical index: every check up to
// the hypo branch, then `addHypoIndexIntoCtx` / `dropHypoIndexFromCtx` on the
// session, the ALTER TABLE spelling included (gorun).
#[test]
fn hypothetical_indexes_are_the_ddl_executors_like_go() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a int, b int)");
    tk.must_exec("create index h on t(b) type hypo");
    for sql in [
        "create index h on t(b) type hypo",
        "create index if not exists h on t(b) type hypo",
    ] {
        let error = tk.must_get_err_code(sql, 1105);
        assert_eq!(error.message, "conflict hypo index name h", "{sql}");
    }
    tk.must_exec("drop hypo index h on t");
    let error = tk.must_get_err_code("drop hypo index h on t", 1091);
    assert_eq!(error.message, "index h doesn't exist");
    tk.must_exec("drop hypo index if exists h on t");
    tk.must_get_err_code("drop hypo index if exists h on nosuch", 1146);
    tk.must_get_err_code("create index h on nosuch(b) type hypo", 1146);
    // The first spec's index is in the session when the second conflicts.
    let error = tk.must_get_err_code("alter table t add index h(b) type hypo, add index h(a) type hypo", 1105);
    assert_eq!(error.message, "conflict hypo index name h");
    tk.must_query("explain format='brief' select b from t where b = 1").check_contain("index:h(b)");
    tk.must_exec("create index ra on t(a)");
    tk.must_get_err_code("create index ra on t(a) type hypo", 1061);
    tk.must_exec("alter table t add index h2(a) type hypo, add column c int");
    tk.must_query("select column_name from information_schema.columns where table_schema = 'test' and table_name = 't' order by ordinal_position")
        .check(["a", "b", "c"]);
    // Only the real index is stored.
    tk.must_query("select index_name from information_schema.statistics where table_schema = 'test' and table_name = 't'")
        .check(["ra"]);
}
