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
use tidb_executor::TableEntry;

/// The view `db.name`'s column names, read off the table info as Go's
/// `dom.InfoSchema().TableByName` reads it.
fn view_columns(tk: &TestKit, db: &str, name: &str) -> Vec<String> {
    match tk.get_table_by_name(db, name) {
        TableEntry::View(view) => view.columns.iter().map(|(name, _)| name.clone()).collect(),
        other => panic!("expected a view, got {other:?}"),
    }
}

// --- TestTable (pkg/ddl/table_test.go:213) ---
//
// Go creates `test_table.t` (3 columns) as a job, re-submits the same
// CREATE TABLE and requires the job to fail, writes 2000 rows, drops the
// table as a job, re-creates it as `tt`, truncates it (new table id),
// renames it across schemas, locks it, and toggles CACHE/NOCACHE -- each leg
// ending with `testCheckJobDone` on the history job and a state check on the
// meta.
//
// The port runs the same statement sequence through a session. Go's
// history-job assertions (checkJobWithHistory / testCheckJobDone) have no
// carrier -- there is no job history here -- and the LOCK TABLE leg needs the
// session table-lock registry, so those two legs are split into gap tests.
#[test]
fn table_create_duplicate_drop_truncate_rename_round_trip() {
    let mut tk = TestKit::new();
    tk.must_exec("create database test_table");
    tk.must_exec("use test_table");
    tk.must_exec("create table t (c1 int, c2 int, c3 int)");

    // Go: create an existing table -- the second job fails with
    // `infoschema.ErrTableExists` ([schema:1050]).
    let error = tk.must_get_err_code("create table t (c1 int, c2 int, c3 int)", 1050);
    assert_eq!(error.message, "Table 'test_table.t' already exists");

    // Go: 2000 single-row `AddRecord`s through the table handle.
    for start in (1..=2000).step_by(500) {
        let values: Vec<String> = (start..start + 500)
            .map(|i| format!("({i}, {i}, {i})"))
            .collect();
        tk.must_exec(&format!("insert into test_table.t values {}", values.join(", ")));
    }
    tk.must_query("select count(*) from test_table.t").check(["2000"]);

    // Go: testDropTable -- the table is gone afterwards.
    tk.must_exec("drop table test_table.t");

    // Go: re-create as `tt` and truncate it -- Go truncates to a NEW table id
    // (testTruncateTable pre-allocates one) and the rows are gone.
    tk.must_exec("create table tt (c1 int, c2 int, c3 int)");
    tk.must_exec("insert into test_table.tt values (1, 1, 1)");
    tk.must_exec("truncate table test_table.tt");
    tk.must_query("select count(*) from test_table.tt").check(["0"]);

    // Go: rename across schemas (testRenameTable) -- the table moves with its
    // definition, empty after the truncate.
    tk.must_exec("create database test_rename_table");
    tk.must_exec("rename table test_table.tt to test_rename_table.tt");
    let schema = tk.info_schema();
    assert!(schema.contains_in("test_rename_table", "tt"));
    assert!(!schema.contains_in("test_table", "tt"));
    tk.must_query("select count(*) from test_rename_table.tt").check(["0"]);

    // Go: ALTER CACHE / NO CACHE toggles the meta's cache status
    // (checkTableCacheTest / checkTableNoCacheTest read
    // TableCacheStatusEnable / Disable). The table info carries the same
    // state behind `KvTable::is_cache_table`.
    tk.must_exec("alter table test_rename_table.tt cache");
    assert!(matches!(
        tk.get_table_by_name("test_rename_table", "tt"),
        TableEntry::Kv(table) if table.is_cache_table()
    ));
    tk.must_exec("alter table test_rename_table.tt nocache");
    assert!(matches!(
        tk.get_table_by_name("test_rename_table", "tt"),
        TableEntry::Kv(table) if !table.is_cache_table()
    ));
}

// --- TestCreateView (pkg/ddl/table_test.go:288) ---
//
// Go submits `ActionCreateView` for `v` over table `t`, then replaces `v`
// with `OnExistReplace: true` (OldViewTblID = the first view's id), then
// replaces AGAIN carrying the now-stale `OldViewTblID` and still requires
// success -- "the non-existing table id in job args will not be considered
// anymore" (Go `pkg/ddl/table_test.go:372-381`). Each ported half asserts
// the same statement-level outcome here; the stale-id leg exists only as a
// job-args concept, which a statement cannot express, and is noted where it
// applied.
#[test]
fn create_view_then_replace_round_trip() {
    let mut tk = TestKit::new();
    tk.must_exec("create database test_table");
    tk.must_exec("use test_table");
    tk.must_exec("create table t (c1 int, c2 int, c3 int)");

    // Leg 1: create view v as select c1, c2 from t.
    tk.must_exec("create view v as select c1, c2 from test_table.t");
    assert_eq!(view_columns(&tk, "test_table", "v"), vec!["c1", "c2"]);

    // Leg 2 (Go `:332-358`): the same CREATE VIEW without OR REPLACE fails --
    // the name is taken.
    tk.must_get_err_code("create view v as select c1, c2 from test_table.t", 1050);

    // Legs 2+3 (Go `:332-381`): `OR REPLACE` overwrites the view whatever
    // the OLD view's table id was -- Go proves the stale id is ignored by
    // passing a long-gone one; here the replace simply succeeds and the new
    // body is what resolves.
    tk.must_exec("create or replace view v as select c1, c2, c3 from test_table.t");
    assert!(tk.info_schema().is_view_in("test_table", "v"));
    assert_eq!(view_columns(&tk, "test_table", "v"), vec!["c1", "c2", "c3"]);
}

// --- TestRenameTables (pkg/ddl/table_test.go:445) ---
//
// Go creates t1/t2 in one schema, submits one `ActionRenameTables` job
// moving t1->tt1 and t2->tt2, then reads the HISTORY job back
// (`ddl.GetHistoryJobByID`) and requires `BinlogInfo.MultipleTableInfos` to
// carry the NEW names tt1/tt2 in order.
//
// The port runs the same two-pair RENAME statement and asserts the schema
// outcome; Go's `MultipleTableInfos` history assertion has no carrier here
// -- there is no job history -- and is recorded in the comment.
#[test]
fn rename_tables_moves_both_pairs_in_one_statement() {
    let mut tk = TestKit::new();
    tk.must_exec("create database test_table");
    tk.must_exec("use test_table");
    for name in ["t1", "t2"] {
        tk.must_exec(&format!("create table {name} (c1 int, c2 int, c3 int)"));
    }
    tk.must_exec("rename table test_table.t1 to test_table.tt1, test_table.t2 to test_table.tt2");
    let schema = tk.info_schema();
    assert!(schema.contains_in("test_table", "tt1"));
    assert!(schema.contains_in("test_table", "tt2"));
    assert!(!schema.contains_in("test_table", "t1"));
    assert!(!schema.contains_in("test_table", "t2"));
    // Go (pkg/ddl/table_test.go:472-477): the history job's
    // MultipleTableInfos[0].Name.L == "tt1" and [1].Name.L == "tt2" -- the
    // meta records the new names in pair order. No job history exists here;
    // the schema contains exactly the new names, in the order it tracks.
    assert_eq!(schema.table_names("test_table").unwrap(), vec!["tt1", "tt2"]);
}

// --- TestAlterTTL (pkg/ddl/table_test.go:537), create half ---
//
// Go builds `t` with two DATETIME columns and `TTLInfo{ColumnName: c0,
// IntervalExprStr: "5", IntervalTimeUnit: DAY}` on the meta, creates it as a
// job, then submits `ActionAlterTTLInfo` (move the TTL to column 1 with
// `1 YEAR`) and `ActionAlterTTLRemove`, reading the HISTORY job's
// `BinlogInfo.TableInfo.TTLInfo` after each.
//
// The port pins the create half: CREATE TABLE lowers the TTL option into
// the table info's TTL exactly as Go's `buildTableInfo` does (Go
// `pkg/ddl/table.go` `getTTLInfoInOptions`). The ALTER legs need the
// job/history machinery and are the gap test.
#[test]
fn create_table_stores_the_ttl_options_on_the_meta() {
    let mut tk = TestKit::new();
    tk.must_exec("create database test_table");
    tk.must_exec("use test_table");
    tk.must_exec("create table t (d1 datetime, d2 datetime) TTL=`d1` + INTERVAL 5 DAY");
    let TableEntry::Kv(table) = tk.get_table_by_name("test_table", "t") else {
        panic!("expected a storage-backed table");
    };
    let ttl = table.ttl_info().expect("Go: the meta carries TTLInfo");
    assert_eq!(ttl.column_name.to_string(), "d1");
    assert_eq!(ttl.interval_expr_str, "5");
    assert_eq!(
        ttl.interval_time_unit,
        tidb_model::time_unit_type_from_keyword("DAY").unwrap(),
        "Go: ast.TimeUnitDay on the created meta"
    );
    assert!(ttl.enable, "Go defaults TTL_ENABLE to ON at create");
}

// --- TestCreateSameTableOrDBOnOwnerChange (pkg/ddl/table_test.go:679) ---
//
// Go runs a TWO-NODE cluster, flips the DDL owner every 50ms, submits three
// racing `create table test.t` (then three `create database aaa`) with
// submission paused at the `waitJobSubmitted` failpoint, and requires the
// first to succeed and BOTH losers to report
// `infoschema.ErrTableExists`/`ErrDatabaseExists`.
//
// The port pins the contract those races depend on -- the SAME name is
// creatable exactly once; the owner change, the submit gate and the
// concurrent sessions have no carrier here.
#[test]
fn same_table_or_database_is_creatable_exactly_once() {
    let mut tk = TestKit::new();
    tk.must_exec("create table test.t (a int)");
    tk.must_get_err_code("create table test.t (a int)", 1050);

    // The first create wins; every loser is Go's
    // `infoschema.ErrDatabaseExists` (1007).
    tk.must_exec("create database aaa");
    tk.must_get_err_code("create database aaa", 1007);
}

// --- TestCreateViewTwice (pkg/ddl/table_test.go:786) ---
//
// Go holds the first `create view v` in the `beforeDeliveryJob` failpoint
// while a SECOND session's `create view v ... where id > 666` must fail
// (MustExecToErr) -- two in-flight CREATE VIEWs of one name collide even
// before the first is delivered.
//
// The port pins the collision contract on Go's two sessions: the name `v`
// is creatable exactly once, and the loser gets ErrTableExists; the
// delivery-gate race itself has no carrier here.
#[test]
fn a_second_create_view_of_one_name_collides() {
    let mut tk = TestKit::new();
    let mut tk2 = tk.new_tk();
    tk.must_exec("create table t_raw (id int)");
    tk2.must_exec("use test");

    tk.must_exec("create view v as select * from t_raw");
    tk2.must_get_err_code("create view v as select * from t_raw where id > 666", 1050);
}
