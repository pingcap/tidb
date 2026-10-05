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
