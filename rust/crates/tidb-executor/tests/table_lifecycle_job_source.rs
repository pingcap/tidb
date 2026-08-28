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

//! Ports of `pkg/ddl/table_test.go` (part12 items 666-676 of the package's
//! `func Test*`/`func Benchmark*` declarations sorted by file and line),
//! read from `origin/master`.
//!
//! Go drives these lifecycles by submitting raw DDL jobs through
//! `ddl.ExecutorForTest::DoDDLJobWrapper` against a bootstrapped mock store,
//! asserting job history (`checkJobWithHistory`), table state
//! (`testCheckTableState`) and the persisted meta after each job. This tier
//! has no job queue, no job history and no schema states: each port runs the
//! same statement through the tier's serialized DDL runners
//! (`run_create_table_in`, `run_drop_table_in`, `run_truncate_table_in`,
//! `run_rename_table_in`, `run_alter_table_in`, `run_create_view_in`) and
//! reads the resulting storage-backed meta. The job-history halves are named
//! in each test's comment; where they are the test's only observable the
//! test is an explicit gap. Nothing is approximated.

use tidb_executor::ddl::{self, CreateTableSettings};
use tidb_executor::{run_insert_on, run_select_on, StmtContext};

use tidb_datatype::Datum;
use tidb_executor::Catalog;

/// The text of a datum, however the codec chose to represent it.
fn datum_text(value: &Datum) -> String {
    match value {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Int(i) => i.to_string(),
        Datum::UInt(u) => u.to_string(),
        other => panic!("unexpected datum {other:?}"),
    }
}

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(datum_text).collect())
        .collect()
}

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

// --- TestTable (pkg/ddl/table_test.go:213) ---
//
// Go creates `test_table.t` (3 columns) as a job, re-submits the same
// CREATE TABLE and requires the job to fail, writes 2000 rows, drops the
// table as a job, re-creates it as `tt`, truncates it (new table id),
// renames it across schemas, locks it, and toggles CACHE/NOCACHE — each leg
// ending with `testCheckJobDone` on the history job and a state check on the
// meta.
//
// The serialized port runs the same statement sequence through the tier's
// runners. Go's history-job assertions (checkJobWithHistory /
// testCheckJobDone) have no carrier — there is no job history here — and the
// LOCK TABLE leg needs the session table-lock registry, so those two legs
// are split into the gap tests below.
#[test]
fn table_create_duplicate_drop_truncate_rename_round_trip() {
    let mut catalog = Catalog::default();
    catalog.create_database("test_table");
    let ctx = ctx();
    ddl::run_create_table_in(
        "create table t (c1 int, c2 int, c3 int)",
        &mut catalog,
        "test_table",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    // Go: create an existing table — the second job fails with
    // `infoschema.ErrTableExists` ([schema:1050]).
    let error = ddl::run_create_table_in(
        "create table t (c1 int, c2 int, c3 int)",
        &mut catalog,
        "test_table",
        CreateTableSettings::default(),
        &ctx,
    )
    .expect_err("Go: [schema:1050]Table 'test_table.t' already exists");
    let mysql = error.clone().to_mysql_error();
    assert_eq!(mysql.code, 1050);
    assert_eq!(mysql.message, "Table 'test_table.t' already exists");

    // Go: 2000 single-row `AddRecord`s through the table handle.
    for start in (1..=2000).step_by(500) {
        let values: Vec<String> = (start..start + 500)
            .map(|i| format!("({i}, {i}, {i})"))
            .collect();
        run_insert_on(
            &format!("insert into test_table.t values {}", values.join(", ")),
            &mut catalog,
            &ctx,
        )
        .unwrap();
    }
    let rows = run_select_on("select count(*) from test_table.t", &mut catalog, &ctx).unwrap();
    assert_eq!(rows_text(&rows), vec![vec!["2000".to_owned()]]);

    // Go: testDropTable — the table is gone afterwards.
    ddl::run_drop_table_in(
        "drop table test_table.t",
        &mut catalog,
        "test_table",
        ctx.sql_mode(),
        ctx.foreign_key_checks(),
    )
    .unwrap();

    // Go: re-create as `tt` and truncate it — Go truncates to a NEW table id
    // (testTruncateTable pre-allocates one) and the rows are gone.
    ddl::run_create_table_in(
        "create table tt (c1 int, c2 int, c3 int)",
        &mut catalog,
        "test_table",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    run_insert_on("insert into test_table.tt values (1, 1, 1)", &mut catalog, &ctx).unwrap();
    ddl::run_truncate_table_in(
        "truncate table test_table.tt",
        &mut catalog,
        "test_table",
        ctx.sql_mode(),
    )
    .unwrap();
    let rows = run_select_on("select count(*) from test_table.tt", &mut catalog, &ctx).unwrap();
    assert_eq!(rows_text(&rows), vec![vec!["0".to_owned()]]);

    // Go: rename across schemas (testRenameTable) — the table moves with its
    // definition, empty after the truncate.
    catalog.create_database("test_rename_table");
    ddl::run_rename_table_in(
        "rename table test_table.tt to test_rename_table.tt",
        &mut catalog,
        "test_table",
        ctx.sql_mode(),
    )
    .unwrap();
    assert!(catalog.contains_in("test_rename_table", "tt"));
    assert!(!catalog.contains_in("test_table", "tt"));
    let rows =
        run_select_on("select count(*) from test_rename_table.tt", &mut catalog, &ctx).unwrap();
    assert_eq!(rows_text(&rows), vec![vec!["0".to_owned()]]);

    // Go: ALTER CACHE / NO CACHE toggles the meta's cache status
    // (checkTableCacheTest / checkTableNoCacheTest read
    // TableCacheStatusEnable / Disable). The tier's observable is the same
    // state behind `KvTable::is_cache_table`.
    ddl::run_alter_table_in(
        "alter table test_rename_table.tt cache",
        &mut catalog,
        "test_table",
        &ctx,
    )
    .unwrap();
    assert!(matches!(
        catalog.table_in("test_rename_table", "tt"),
        Some(tidb_executor::TableEntry::Kv(table)) if table.is_cache_table()
    ));
    ddl::run_alter_table_in(
        "alter table test_rename_table.tt nocache",
        &mut catalog,
        "test_table",
        &ctx,
    )
    .unwrap();
    assert!(matches!(
        catalog.table_in("test_rename_table", "tt"),
        Some(tidb_executor::TableEntry::Kv(table)) if !table.is_cache_table()
    ));
}

// The LOCK TABLE leg of Go's TestTable (pkg/ddl/table_test.go:233-254, the
// `testLockTable` helper plus `checkTableLockedTest` at `:256`): the job
// stores `model.TableLockInfo` on the table meta — one session entry
// (server id + connection id), the lock type `TableLockWrite`, state
// `TableLockStatePublic` — read straight back from a fresh meta txn.
//
// go-parity-gap: this tier's ALTER/driver treats LOCK specs as no-ops
// (`ddl/alter_table.rs:331` strips them before dispatch, mirroring Go's
// spec-removal, but no lock registry or `TableLockInfo` write exists), so
// the persisted lock meta is not reproducible.
#[test]
#[ignore = "go-parity-gap: table-lock meta (TableLockInfo) is never persisted in this tier"]
fn lock_table_persists_the_session_lock_on_the_meta() {
    // Contract (pkg/ddl/table_test.go:256-272): after the LOCK TABLE job,
    // meta.Lock has one session {serverID, connectionID}, Tp
    // TableLockWrite, state TableLockStatePublic.
}

// --- TestCreateView (pkg/ddl/table_test.go:288) ---
//
// Go submits `ActionCreateView` for `v` over table `t`, then replaces `v`
// with `OnExistReplace: true` (OldViewTblID = the first view's id), then
// replaces AGAIN carrying the now-stale `OldViewTblID` and still requires
// success — "the non-existing table id in job args will not be considered
// anymore" (Go `pkg/ddl/table_test.go:372-381`). Each ported half asserts
// the same statement-level outcome here; the stale-id leg exists only as a
// job-args concept, which this tier does not model, and is noted where it
// applied.
#[test]
fn create_view_then_replace_round_trip() {
    let mut catalog = Catalog::default();
    catalog.create_database("test_table");
    let ctx = ctx();
    ddl::run_create_table_in(
        "create table t (c1 int, c2 int, c3 int)",
        &mut catalog,
        "test_table",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    // Leg 1: create view v as select c1, c2 from t.
    let stmt = tidb_parser::parse_with_sql_mode(
        "create view v as select c1, c2 from test_table.t",
        ctx.sql_mode(),
    )
    .unwrap();
    let tidb_ast::Stmt::Ddl(ddl_stmt) = &stmt else {
        panic!("expected a DDL statement");
    };
    let tidb_ast::DdlStmt::CreateView(create) = &**ddl_stmt else {
        panic!("expected CREATE VIEW");
    };
    tidb_executor::run_create_view_in(create, &mut catalog, "test_table", &ctx).unwrap();
    let view_of = |catalog: &Catalog| match catalog.table_in("test_table", "v") {
        Some(tidb_executor::TableEntry::View(view)) => view.clone(),
        other => panic!("expected a view, got {other:?}"),
    };
    let view = view_of(&catalog);
    assert_eq!(
        view.columns.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>(),
        vec!["c1".to_owned(), "c2".to_owned()]
    );

    // Leg 2 (Go `:332-358`): the same CREATE VIEW without OR REPLACE fails —
    // the name is taken.
    let error = tidb_executor::run_create_view_in(create, &mut catalog, "test_table", &ctx)
        .expect_err("Go: the replace job's non-replace sibling is ErrTableExists");
    assert_eq!(error.clone().to_mysql_error().code, 1050);

    // Legs 2+3 (Go `:332-381`): `OR REPLACE` overwrites the view whatever
    // the OLD view's table id was — Go proves the stale id is ignored by
    // passing a long-gone one; here the replace simply succeeds and the new
    // body is what resolves.
    let stmt = tidb_parser::parse_with_sql_mode(
        "create or replace view v as select c1, c2, c3 from test_table.t",
        ctx.sql_mode(),
    )
    .unwrap();
    let tidb_ast::Stmt::Ddl(ddl_stmt) = &stmt else {
        panic!("expected a DDL statement");
    };
    let tidb_ast::DdlStmt::CreateView(replace) = &**ddl_stmt else {
        panic!("expected CREATE OR REPLACE VIEW");
    };
    tidb_executor::run_create_view_in(replace, &mut catalog, "test_table", &ctx).unwrap();
    assert!(catalog.is_view_in("test_table", "v"));
    let view = view_of(&catalog);
    assert_eq!(
        view.columns.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>(),
        vec!["c1".to_owned(), "c2".to_owned(), "c3".to_owned()]
    );
}

// --- TestRenameTables (pkg/ddl/table_test.go:445) ---
//
// Go creates t1/t2 in one schema, submits one `ActionRenameTables` job
// moving t1→tt1 and t2→tt2, then reads the HISTORY job back
// (`ddl.GetHistoryJobByID`) and requires `BinlogInfo.MultipleTableInfos` to
// carry the NEW names tt1/tt2 in order.
//
// The serialized port runs the same two-pair RENAME statement (the tier's
// `run_rename_table_in` stages every pair before moving any) and asserts the
// catalog outcome; Go's `MultipleTableInfos` history assertion has no
// carrier here — there is no job history — and is recorded in the comment.
#[test]
fn rename_tables_moves_both_pairs_in_one_statement() {
    let mut catalog = Catalog::default();
    catalog.create_database("test_table");
    let ctx = ctx();
    for name in ["t1", "t2"] {
        ddl::run_create_table_in(
            &format!("create table {name} (c1 int, c2 int, c3 int)"),
            &mut catalog,
            "test_table",
            CreateTableSettings::default(),
            &ctx,
        )
        .unwrap();
    }
    ddl::run_rename_table_in(
        "rename table test_table.t1 to test_table.tt1, test_table.t2 to test_table.tt2",
        &mut catalog,
        "test_table",
        ctx.sql_mode(),
    )
    .unwrap();
    assert!(catalog.contains_in("test_table", "tt1"));
    assert!(catalog.contains_in("test_table", "tt2"));
    assert!(!catalog.contains_in("test_table", "t1"));
    assert!(!catalog.contains_in("test_table", "t2"));
    // Go (pkg/ddl/table_test.go:472-477): the history job's
    // MultipleTableInfos[0].Name.L == "tt1" and [1].Name.L == "tt2" — the
    // meta records the new names in pair order. No job history exists in
    // this tier; the catalog contains exactly the new names, in the order
    // the schema tracks.
    assert_eq!(catalog.table_names("test_table").unwrap(), vec!["tt1", "tt2"]);
}

// --- TestCreateTables (pkg/ddl/table_test.go:487) ---
//
// Go pre-allocates 3 global ids, builds one `ActionCreateTables`
// (`model.BatchCreateTableArgs`) job carrying s1/s2/s3, submits it, and —
// with the `mockGetJobByIDFail` failpoint armed once — requires the job to
// succeed and all three tables to resolve.
//
// go-parity-gap: batch CREATE TABLE is a JOB-level action (the job queue
// merges fast-create-table submissions into it); neither the job nor
// `ddl.GetAllDDLJobs`-style inspection exists in this tier, and the runner
// accepts a single CREATE TABLE statement.
#[test]
#[ignore = "go-parity-gap: ActionCreateTables batch jobs and job-history failpoints are not transcreated"]
fn batch_create_tables_lands_all_three_tables() {
    // Contract (pkg/ddl/table_test.go:487-535): one batch job creates s1,
    // s2 and s3; the injected get-job-by-id failure (first call only) does
    // not fail the batch; all three tables resolve from the infoschema.
}

// --- TestAlterTTL (pkg/ddl/table_test.go:537), create half ---
//
// Go builds `t` with two DATETIME columns and `TTLInfo{ColumnName: c0,
// IntervalExprStr: "5", IntervalTimeUnit: DAY}` on the meta, creates it as a
// job, then submits `ActionAlterTTLInfo` (move the TTL to column 1 with
// `1 YEAR`) and `ActionAlterTTLRemove`, reading the HISTORY job's
// `BinlogInfo.TableInfo.TTLInfo` after each.
//
// The serialized port pins the create half: the tier's CREATE TABLE lowers
// the TTL option into `KvTable::ttl_info` exactly as Go's `buildTableInfo`
// does (`rust/crates/tidb-executor/src/ddl.rs:488`
// `ttl_info_from_options`, Go `pkg/ddl/table.go` `getTTLInfoInOptions`).
// The ALTER legs need the job/history machinery and are the gap test below.
#[test]
fn create_table_stores_the_ttl_options_on_the_meta() {
    let mut catalog = Catalog::default();
    catalog.create_database("test_table");
    let ctx = ctx();
    ddl::run_create_table_in(
        "create table t (d1 datetime, d2 datetime) TTL=`d1` + INTERVAL 5 DAY",
        &mut catalog,
        "test_table",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    let Some(tidb_executor::TableEntry::Kv(table)) = catalog.table_in("test_table", "t") else {
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

// The ALTER half of Go's TestAlterTTL (pkg/ddl/table_test.go:569-617):
// `ActionAlterTTLInfo` moves the TTL to column `d2` with `INTERVAL 1 YEAR`
// and the history job's TableInfo reflects it; `ActionAlterTTLRemove` then
// empties `historyJob.BinlogInfo.TableInfo.TTLInfo` entirely.
//
// go-parity-gap: neither ALTER action is lowered by this tier's runner
// (`ddl/alter_table.rs` refuses them in the catch-all arm), and the
// assertions Go makes read the JOB HISTORY, which does not exist here.
#[test]
#[ignore = "go-parity-gap: ALTER TTL INFO/REMOVE actions and history-job assertions are not transcreated"]
fn alter_ttl_moves_and_then_removes_the_ttl_info() {
    // Contract (pkg/ddl/table_test.go:569-617): after the first job,
    // historyJob.BinlogInfo.TableInfo.TTLInfo == {d2, "1", YEAR}; after the
    // second, it is empty.
}

// --- TestRenameTableIntermediateState (pkg/ddl/table_test.go:621) ---
//
// Go renames db1.t through four round trips (within db1, then across to
// db2), and on the `afterWaitSchemaSynced` failpoint — parked at
// StateWriteReorganization→public — probes DML from a second session: at
// the intermediate state the OLD name is already invisible
// (`[schema:1146]Table 'db1.t' doesn't exist`) while the NEW name accepts
// the insert, and the final `select` shows the row under the new name only.
//
// go-parity-gap: schema states and the job queue that walks them do not
// exist in this tier — a rename here is atomic, so the
// old-name-invisible/new-name-visible window cannot be reproduced.
#[test]
#[ignore = "go-parity-gap: the rename's intermediate schema state needs the DDL job queue"]
fn rename_table_intermediate_state_hides_the_old_name() {
    // Contract (pkg/ddl/table_test.go:621-677): at StateWriteReorganization
    // public, insert into the old name reports
    // "[schema:1146]Table 'db1.t' doesn't exist", insert into the new name
    // succeeds, and the final select reads the row only under the new name.
}

// --- TestCreateSameTableOrDBOnOwnerChange (pkg/ddl/table_test.go:679) ---
//
// Go runs a TWO-NODE cluster, flips the DDL owner every 50ms, submits three
// racing `create table test.t` (then three `create database aaa`) with
// submission paused at the `waitJobSubmitted` failpoint, and requires the
// first to succeed and BOTH losers to report
// `infoschema.ErrTableExists`/`ErrDatabaseExists`.
//
// The serialized port pins the contract those races depend on — the SAME
// name is creatable exactly once — through the tier's runners; the owner
// change, the submit gate and the concurrent sessions have no carrier here.
#[test]
fn same_table_or_database_is_creatable_exactly_once() {
    let mut catalog = Catalog::default();
    let ctx = ctx();
    ddl::run_create_table_in(
        "create table test.t (a int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    let error = ddl::run_create_table_in(
        "create table test.t (a int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .expect_err("Go: infoschema.ErrTableExists for every loser");
    assert_eq!(error.clone().to_mysql_error().code, 1050);

    assert!(catalog.create_database("aaa"), "the first create wins");
    assert!(
        !catalog.create_database("aaa"),
        "Go: infoschema.ErrDatabaseExists (1007) for every loser; the tier's \
         create_database reports the collision as false"
    );
}

// --- TestDropTableAccessibleInInfoSchema (pkg/ddl/table_test.go:758) ---
//
// Go drops `t` and, on the `beforeRunOneJobStep` failpoint at
// StateDeleteOnly and StateWriteOnly, resolves `test.t` from the live
// infoschema — the dropped table stays accessible until its state reaches
// `StateNone` (both probes collect NoError, and errs is non-empty).
//
// go-parity-gap: schema states do not exist in this tier; a drop here
// removes the name atomically, so the still-accessible window cannot be
// reproduced.
#[test]
#[ignore = "go-parity-gap: the drop's DeleteOnly/WriteOnly visibility window needs schema states"]
fn dropped_table_stays_accessible_until_state_none() {
    // Contract (pkg/ddl/table_test.go:758-784): during both early states,
    // infoschema.TableByName("test", "t") succeeds; the probes fire at least
    // once.
}

// --- TestCreateViewTwice (pkg/ddl/table_test.go:786) ---
//
// Go holds the first `create view v` in the `beforeDeliveryJob` failpoint
// while a SECOND session's `create view v ... where id > 666` must fail
// (MustExecToErr) — two in-flight CREATE VIEWs of one name collide even
// before the first is delivered.
//
// The serialized port pins the collision contract: the name `v` is creatable
// exactly once, and the loser gets ErrTableExists; the delivery-gate race
// itself has no carrier here.
#[test]
fn a_second_create_view_of_one_name_collides() {
    let mut catalog = Catalog::default();
    let ctx = ctx();
    ddl::run_create_table_in(
        "create table t_raw (id int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();

    let view_parse = |sql: &str, ctx: &StmtContext| -> tidb_ast::CreateViewStmt {
        let stmt = tidb_parser::parse_with_sql_mode(sql, ctx.sql_mode()).unwrap();
        match &stmt {
            tidb_ast::Stmt::Ddl(ddl_stmt) => match &**ddl_stmt {
                tidb_ast::DdlStmt::CreateView(create) => (**create).clone(),
                _ => panic!("expected CREATE VIEW"),
            },
            _ => panic!("expected CREATE VIEW"),
        }
    };
    tidb_executor::run_create_view_in(
        &view_parse("create view v as select * from t_raw", &ctx),
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let error = tidb_executor::run_create_view_in(
        &view_parse("create view v as select * from t_raw where id > 666", &ctx),
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("Go: the second session's create view fails while the first is in flight");
    assert_eq!(error.clone().to_mysql_error().code, 1050);
}

// --- TestIssue59238 (pkg/ddl/table_test.go:810) ---
//
// Go creates a range-partitioned table with an index, reads
// `select distinct create_time from information_schema.partitions`, then
// TRUNCATEs partition p1 and EXCHANGEs partition p1 with t1 — the distinct
// create_time set must NOT change across either operation (partition
// create-times are preserved).
//
// go-parity-gap: `information_schema.partitions` is not served by this tier
// (`driver/infoschema_meta.rs` has no partitions table) and EXCHANGE
// PARTITION is not lowered by the ALTER runner, so the create_time
// invariants are not observable.
#[test]
#[ignore = "go-parity-gap: no information_schema.partitions surface and no EXCHANGE PARTITION"]
fn partition_create_times_survive_truncate_and_exchange() {
    // Contract (pkg/ddl/table_test.go:810-830): the distinct create_time
    // rows are identical after `alter table t truncate partition p1` and
    // again after `alter table t exchange partition p1 with table t1`.
}

// --- TestRefreshMetaBasic (pkg/ddl/table_test.go:832) ---
//
// Go creates two placement policies and database test1 under p1, rewrites
// t1's name to t2 directly in the META KV (bypassing infoschema), requires
// the infoschema to still 404 t2, then `testutil.RefreshMeta` refreshes it:
// the schema version moves +1 and t2 resolves.
//
// go-parity-gap: the refresh seam (`testutil.RefreshMeta` /
// `ddl.RefreshMeta`), the placement-policy-placed database bootstrapping and
// the raw meta-KV mutation helper (`testutil.GetTableInfoByTxn`) are not
// transcreated; this tier's catalog has no infoschema/meta split to refresh
// across.
#[test]
#[ignore = "go-parity-gap: no meta-KV-vs-infoschema refresh seam in this tier"]
fn refresh_meta_publishes_out_of_band_meta_rewrites() {
    // Contract (pkg/ddl/table_test.go:832-878): after the out-of-band rename,
    // TableByName("test1", "t2") fails "Table 'test1.t2' doesn't exist";
    // after RefreshMeta the version is old+1 and t2 resolves.
}
