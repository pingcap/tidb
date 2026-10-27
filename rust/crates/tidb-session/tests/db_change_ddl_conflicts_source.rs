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

//! Behavioral tests retained from Go. Removed documentary entries are
//! indexed in rust/docs/parity/current-audit/comment-test-cleanup-validation.json.

use crate::support::testkit::TestKit;

/// Go's `testControlParallelExecSQL` fixture table (`db_change_test.go:1452`):
/// `t(a int, b int, c double default null, d int auto_increment, e int,
/// index idx1(d), index idx2(d,e))` plus the range-partitioned `t_part`
/// every parallel test pre-creates beside it.
fn create_fixture_tables(tk: &mut TestKit) {
    tk.must_exec(
        "create table t(a int, b int, c double default null, d int auto_increment, \
         e int, index idx1(d), index idx2(d,e))",
    );
    tk.must_exec("insert into t values(1, 2, 3.1234, 4, 5)");
    tk.must_exec(
        "create table t_part (a int key) partition by range(a) (\
         partition p0 values less than (10), partition p1 values less than (20))",
    );
}

/// The `show warnings` rows of the last statement: every one is a Note.
fn assert_only_notes(tk: &mut TestKit, sql: &str) {
    let warnings = tk.must_query("show warnings");
    assert!(
        warnings.rows().iter().all(|row| row[0] == "Note"),
        "{sql}: every IF EXISTS suppression is a note: {:?}",
        warnings.rows()
    );
}

// --- TestParallelDropColumn (pkg/ddl/db_change_test.go:1329) ---
//
// Go drops `c` from two racing sessions and requires exactly one loser with
// `[ddl:1091]column c doesn't exist`. The serialized form below pins the
// loser's contract: after the column is gone, naming it again is 1091.
#[test]
fn parallel_drop_column_twice_second_1091() {
    let mut tk = TestKit::new();
    create_fixture_tables(&mut tk);
    tk.must_exec("ALTER TABLE t drop COLUMN c");
    let error = tk.must_get_err_code("ALTER TABLE t drop COLUMN c", 1091);
    assert_eq!(error.message, "Can't DROP 'c'; check that column/key exists");
}

// --- TestParallelDropColumns (pkg/ddl/db_change_test.go:1341) ---
//
// Same race with a two-column drop; the loser reports the FIRST missing
// name, `b`, still 1091.
#[test]
fn parallel_drop_columns_twice_second_1091_names_first() {
    let mut tk = TestKit::new();
    create_fixture_tables(&mut tk);
    tk.must_exec("ALTER TABLE t drop COLUMN b, drop COLUMN c");
    let error = tk.must_get_err_code("ALTER TABLE t drop COLUMN b, drop COLUMN c", 1091);
    assert_eq!(error.message, "Can't DROP 'b'; check that column/key exists");
}

// --- TestParallelDropIfExistsColumns (pkg/ddl/db_change_test.go:1353) ---
//
// Go runs `drop COLUMN if exists b, drop COLUMN if exists c` in two racing
// sessions and requires BOTH to succeed — the loser files notes, not errors.
// Running the same statement twice reproduces both outcomes on one session;
// the second run leaves one Note 1091 per guarded column, Go's
// `AppendNote`-swallowed `ErrCantDropFieldOrKey` text (captured in
// `drop_column_action`).
#[test]
fn parallel_drop_if_exists_columns_succeed_with_1091_notes() {
    let mut tk = TestKit::new();
    create_fixture_tables(&mut tk);
    for _ in 0..2 {
        tk.must_exec("ALTER TABLE t drop COLUMN if exists b, drop COLUMN if exists c");
    }
    // The second run's notes, as `show warnings` lists them.
    let warnings = tk.must_query("show warnings");
    assert_eq!(
        warnings.rows().iter().filter(|row| row[1] == "1091").count(),
        2,
        "each guarded column files exactly one note: {:?}",
        warnings.rows()
    );
}

// --- TestParallelDropIndex (pkg/ddl/db_change_test.go:1365) ---
//
// Two racing sessions drop two DIFFERENT indexes; both must succeed.
#[test]
fn parallel_drop_index_both_succeed() {
    let mut tk = TestKit::new();
    create_fixture_tables(&mut tk);
    tk.must_exec("alter table t drop index idx1");
    tk.must_exec("alter table t drop index idx2");
}

// --- TestParallelAlterAddIndex (pkg/ddl/db_change_test.go:1215) ---
//
// `add index index_b(b)` races `CREATE INDEX index_b ON t (c)`; Go requires
// the loser to fail with `[ddl:1061]index already exist index_b`. The
// serialized form pins the loser's error for whichever spelling runs second.
#[test]
fn parallel_alter_add_index_duplicate_1061() {
    let mut tk = TestKit::new();
    create_fixture_tables(&mut tk);
    tk.must_exec("ALTER TABLE t add index index_b(b)");
    let error = tk.must_get_err_code("CREATE INDEX index_b ON t (c)", 1061);
    assert_eq!(error.message, "Duplicate key name 'index_b'");
}

// --- TestParallelAlterAddExpressionIndex (pkg/ddl/db_change_test.go:1285) ---
//
// The same duplicate-name race over EXPRESSION indexes: `add index
// expr_index_b((b+1))` races `CREATE INDEX expr_index_b ON t ((c+1))`, and
// the loser must report 1061.
#[test]
fn parallel_alter_add_expression_index_duplicate_1061() {
    let mut tk = TestKit::new();
    create_fixture_tables(&mut tk);
    tk.must_exec("ALTER TABLE t add index expr_index_b((b+1))");
    let error = tk.must_get_err_code("CREATE INDEX expr_index_b ON t ((c+1))", 1061);
    assert_eq!(error.message, "Duplicate key name 'expr_index_b'");
}

// --- TestParallelAddColumAndSetDefaultValue (pkg/ddl/db_change_test.go:1172) ---
//
// Go proves `add column cx int after c1` and `alter table tx alter c2 set
// default 'N'` can both commit while racing. The serialized form pins the
// compatibility they jointly need: both statements succeed on the same
// table, and the row the test deletes afterwards is still addressable.
#[test]
fn parallel_add_column_and_set_default_value_both_commit() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table tx (c1 varchar(64), c2 enum('N','Y') not null default 'N', \
         primary key idx2 (c2, c1))",
    );
    tk.must_exec("insert into tx values('a', 'N')");
    tk.must_exec("alter table tx add column cx int after c1");
    tk.must_exec("alter table tx alter c2 set default 'N'");
    // The row stays addressable through both ALTERs.
    tk.must_exec("delete from tx where c1='a'");
}

// --- TestParallelChangeColumnName (pkg/ddl/db_change_test.go:1194) ---
//
// `CHANGE a aa int` races `CHANGE b aa int`; Go requires EXACTLY ONE loser
// with `[schema:1060]Duplicate column name 'aa'`. Serialized, the second
// rename must lose with 1060.
#[test]
fn parallel_change_column_name_duplicate_1060_exactly_once() {
    let mut tk = TestKit::new();
    create_fixture_tables(&mut tk);
    tk.must_exec("ALTER TABLE t CHANGE a aa int");
    let error = tk.must_get_err_code("ALTER TABLE t CHANGE b aa int", 1060);
    assert_eq!(error.message, "Duplicate column name 'aa'");
}

// --- TestParallelAlterAddPartition (pkg/ddl/db_change_test.go:1312) ---
//
// `add partition p2 less than (30)` races `add partition p3 less than (30)`
// on the fixture `t_part` (bounds 10, 20); Go requires the loser to fail
// with 1493 VALUES LESS THAN must be strictly increasing.
#[test]
fn parallel_alter_add_partition_less_than_strictly_increasing_1493() {
    let mut tk = TestKit::new();
    create_fixture_tables(&mut tk);
    tk.must_exec("alter table t_part add partition (partition p2 values less than (30))");
    let error = tk.must_get_err_code(
        "alter table t_part add partition (partition p3 values less than (30))",
        1493,
    );
    assert_eq!(
        error.message,
        "VALUES LESS THAN value must be strictly increasing for each partition"
    );
}

// --- TestParallelCreateAndRename (pkg/ddl/db_change_test.go:1391) ---
//
// `create table t_exists(c int)` races `rename t to t_exists`; the rename
// loses with `[schema:1050]Table 't_exists' already exists`.
#[test]
fn parallel_create_and_rename_collision_1050() {
    let mut tk = TestKit::new();
    create_fixture_tables(&mut tk);
    tk.must_exec("create table t_exists(c int)");
    let error = tk.must_get_err_code("alter table t rename to t_exists", 1050);
    assert_eq!(error.message, "Table 'test.t_exists' already exists");
}

// --- TestCreateTableIfNotExists (pkg/ddl/db_change_test.go:1540) ---
//
// Go runs `create table if not exists test_not_exists(a int)` from two
// sessions and expects NO error from either. Serialized, the second run is
// the loser: it succeeds with Go's `Note 1050` instead of an error (the
// note the parallel loser would carry), while the first run creates the
// table without one.
#[test]
fn create_table_if_not_exists_parallel_no_error() {
    let mut tk = TestKit::new();
    tk.must_exec("create table if not exists test_not_exists(a int)");
    tk.must_query("show warnings").check(Vec::<&str>::new());
    tk.must_exec("create table if not exists test_not_exists(a int)");
    tk.must_query("show warnings")
        .check(["Note 1050 Table 'test.test_not_exists' already exists"]);
}

// --- TestDDLIfExists (pkg/ddl/db_change_test.go:1573) ---
//
// Go runs every `IF EXISTS` DDL from two racing sessions and expects no
// error from either. Serialized: the same statement must succeed TWICE —
// the second run finds nothing and files notes instead of errors. This is
// exactly the contract the parallel loser depends on.
#[test]
fn ddl_if_exists_each_statement_survives_a_second_run() {
    let mut tk = TestKit::new();
    tk.must_exec("create table test_exists (a int key, b int)");
    tk.must_exec(
        "create table test_exists_2 (a int key) partition by range(a) (\
         partition p0 values less than (10), partition p1 values less than (20), \
         partition p2 values less than (30))",
    );
    fn run_twice(tk: &mut TestKit, sql: &str) {
        for _ in 0..2 {
            tk.must_exec(sql);
            assert_only_notes(tk, sql);
        }
    }
    for sql in [
        "alter table test_exists drop column if exists c, drop column if exists d",
        "alter table test_exists drop column if exists b",
        "alter table test_exists change column if exists a c int",
        "alter table test_exists modify column if exists a bigint",
    ] {
        run_twice(&mut tk, sql);
    }
    // Go's DROP INDEX sub-case first adds `idx_c (c)` (the column the CHANGE
    // above produced), then races two guarded drops.
    tk.must_exec("alter table test_exists add index idx_c (c)");
    run_twice(&mut tk, "alter table test_exists drop index if exists idx_c");
    // Go's final sub-case drops a partition twice under IF EXISTS. The
    // preceding `add index idx_c (c)` from the Go flow is skipped here
    // because this statement list already consumed `c`'s table above.
    run_twice(&mut tk, "alter table test_exists_2 drop partition if exists p1");
}

// --- TestCreateExpressionIndex (pkg/ddl/db_change_test.go:1715) ---
//
// The Go test hooks `afterWaitSchemaSynced` to run a DML matrix inside each
// schema state of `alter table t add index idx((b+1))` and then admin-checks
// the table; the issue-39784 tail adds `create index idx on
// test.t((lower(test.t.name)))` over mixed-case names. The state matrix
// needs the failpoint machinery (documented below); what this tier CAN pin
// is the index mechanics both halves share: an expression index over a
// populated table is built, is consistent, and the collation-aware
// `lower()` expression index of issue 39784 builds over mixed-case rows.
#[test]
fn create_expression_index_builds_and_admin_checks_clean() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a int default 0, b int default 0)");
    tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)");
    tk.must_exec("alter table t add index idx((b+1))");
    // The expression index is consistent with its rows.
    tk.must_exec("admin check table t");

    // Issue 39784 (db_change_test.go:1711): an expression index over
    // `lower(name)` on a recreated `t`.
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(name varchar(20))");
    tk.must_exec("insert into t values ('Abc'), ('Bcd'), ('abc')");
    tk.must_exec("create index idx on test.t((lower(test.t.name)))");
    tk.must_exec("admin check table t");
}

// --- TestCreateUniqueExpressionIndex (pkg/ddl/db_change_test.go:1784) ---
//
// Same shape as above with `add unique index idx((a*b+1))`. The serialized
// port pins the index build over the populated fixture; the ODKU/update
// state matrix stays a gap (it needs the schema-state hooks).
#[test]
fn create_unique_expression_index_builds_and_admin_checks_clean() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a int default 0, b int default 0)");
    tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)");
    tk.must_exec("alter table t add unique index idx((a*b+1))");
    tk.must_exec("admin check table t");
    tk.must_query("select * from t order by a, b")
        .check(["1 1", "2 2", "3 3", "4 4"]);
}

// --- TestDropExpressionIndex (pkg/ddl/db_change_test.go:1887) ---
//
// The Go test drops `idx((b+1))` under schema-state hooks and checks the
// surviving rows. Serialized port: the declared expression index drops
// cleanly and the table stays consistent and readable.
#[test]
fn drop_expression_index_roundtrip_keeps_rows_readable() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a int default 0, b int default 0, key idx((b+1)))");
    tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)");
    tk.must_exec("alter table t drop index idx");
    tk.must_exec("admin check table t");
    tk.must_query("select * from t order by a")
        .check(["1 1", "2 2", "3 3", "4 4"]);
}

// --- TestConcurrentSetDefaultValue (pkg/ddl/db_change_test.go:2048) ---
//
// Go interleaves a concurrent `ALTER COLUMN SET DEFAULT` into a
// `MODIFY COLUMN` job via `beforeRunOneJobStep`, then checks the surviving
// column type and that fresh rows take the settled default. The default
// half is portable: after `modify column a MEDIUMINT NULL DEFAULT
// '-8145111'` a row written with no columns reads -8145111, and after the
// TIMESTAMP spelling an empty insert succeeds (Go then checks SHOW CREATE
// TABLE, unported here).
#[test]
fn concurrent_set_default_value_new_rows_take_modified_default() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a YEAR NULL DEFAULT '2029')");
    tk.must_exec("alter table t modify column a MEDIUMINT NULL DEFAULT '-8145111'");
    tk.must_exec("insert into t value()");
    tk.must_query("select a from t").check(["-8145111"]);

    // Go rebuilds the table as `t(a int default 2)` (db_change_test.go:2025)
    // and reruns the modification with a TIMESTAMP default; the empty insert
    // takes the settled TIMESTAMP default.
    tk.must_exec("drop table t");
    tk.must_exec("create table t(a int default 2)");
    tk.must_exec("alter table t modify column a TIMESTAMP NULL DEFAULT '2017-08-06 10:47:11'");
    tk.must_exec("insert into t value()");
}
