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

//! Statement-result cases from Go `pkg/executor/test/executor/executor_test.go`:
//! UNION, fields, scalar subqueries, decimal division, defaults and snapshots.
//! Broader session, memory and kill obligations remain in the parity audit.

use crate::{
    run_create_table_on, run_insert_on, run_select_meta_in, run_select_on, run_update_on, SessionExtendedInfoSchema,
    StmtContext,
};
use tidb_datatype::Datum;

fn ctx() -> StmtContext {
    StmtContext::for_query().with_txn_store(Box::new(crate::test_store::store()))
}

fn create(catalog: &mut SessionExtendedInfoSchema, sql: &str) {
    run_create_table_on(sql, catalog).unwrap_or_else(|error| panic!("create {sql:?}: {error:?}"));
}

fn insert(catalog: &mut SessionExtendedInfoSchema, sql: &str) {
    run_insert_on(sql, catalog, &ctx()).unwrap_or_else(|error| panic!("insert {sql:?}: {error:?}"));
}

fn select(catalog: &SessionExtendedInfoSchema, sql: &str) -> Vec<Vec<Datum>> {
    run_select_on(sql, catalog, &ctx()).unwrap_or_else(|error| panic!("select {sql:?}: {error:?}"))
}

fn select_err(catalog: &SessionExtendedInfoSchema, sql: &str) -> crate::DriverError {
    run_select_on(sql, catalog, &ctx()).expect_err(&format!("select {sql:?} must fail"))
}

/// Go `testkit.Rows` compares `fmt.Sprintf("%v")` per cell, so NULL prints
/// as `<nil>` and a decimal as its plain rendering; this mirrors that view.
fn render(datum: &Datum) -> String {
    match datum {
        Datum::Null => "<nil>".to_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) => format!("{value}"),
        Datum::Decimal(value) => value.to_string(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::Time(time) => time.to_string(),
        other => format!("{other:?}"),
    }
}

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(render).collect())
        .collect()
}

fn assert_rows(catalog: &SessionExtendedInfoSchema, sql: &str, expected: &[&str]) {
    let actual = rows_text(&select(catalog, sql));
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.split(' ').map(str::to_owned).collect())
        .collect();
    assert_eq!(actual, expected, "sql: {sql}");
}

/// Go `(*testkit.Result).Sort()`: the same comparison with both sides sorted.
///
/// A `UNION` (not `UNION ALL`) is deduplicated by an aggregation, and Go's
/// parallel HashAgg emits its groups in map order, so the statement has NO
/// defined output order. Go's `TestUnion2` calls `r.Sort()` on exactly these
/// arms; comparing the rows positionally would pin an order the engine never
/// promised.
fn assert_rows_sorted(catalog: &SessionExtendedInfoSchema, sql: &str, expected: &[&str]) {
    let mut actual = rows_text(&select(catalog, sql));
    actual.sort();
    let mut expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.split(' ').map(str::to_owned).collect())
        .collect();
    expected.sort();
    assert_eq!(actual, expected, "sql: {sql}");
}

/// Go `executor_test.go:1439::TestAdapterStatement`: the compiled statement
/// keeps its ORIGIN text (`select 1`, the create-table text) and the GBK
/// client-charset statement renders `select '表1'` as text while keeping the
/// raw GBK bytes as origin.
///
/// The origin-text halves are pinned here through the AST the driver parses
/// (`tidb_ast`'s `NodeText::original_text`); the GBK client-charset arm
/// needs the charset handshake and stays a gap.
#[test]
fn adapter_statement_keeps_origin_text() {
    let stmt = ctx().parse("select 1").expect("parse");
    assert_eq!(stmt.node_text().original_text(), b"select 1");
    let stmt = ctx().parse("create table test.t (a int)").expect("parse");
    assert_eq!(
        stmt.node_text().original_text(),
        b"create table test.t (a int)"
    );
}

/// The session's `ONLY_FULL_GROUP_BY` flag reaches the planner. Go calls
/// `checkOnlyFullGroupBy` only when `sql_mode` has the flag
/// (`logical_plan_builder.go:3731`), so a non-aggregated select field beside
/// an aggregate is accepted when the mode omits it and rejected (8123) when
/// the mode has it. The executor bridge forwarded only
/// `tidb_enable_new_only_full_group_by_check`, leaving the builder's default
/// of `true`, so the mode was ignored.
#[test]
fn the_session_only_full_group_by_flag_reaches_the_planner() {
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    create(&mut catalog, "create table t (c int, d int)");
    let names = |ctx: &StmtContext| -> Vec<String> {
        let (columns, _) =
            run_select_meta_in("select 1 + c, count(*) from t", &catalog, "test", ctx)
                .unwrap_or_else(|error| panic!("{error:?}"));
        columns.into_iter().map(|(name, _)| name).collect()
    };
    assert_eq!(
        names(&ctx().with_only_full_group_by(false)),
        vec!["1 + c".to_owned(), "count(*)".to_owned()]
    );
    let error = run_select_meta_in(
        "select 1 + c, count(*) from t",
        &catalog,
        "test",
        &ctx().with_only_full_group_by(true),
    )
    .expect_err("ONLY_FULL_GROUP_BY must reject a non-aggregated select field");
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 8123);
    assert!(mysql.message.contains("only_full_group_by"), "{mysql:?}");
}

/// Go `executor_test.go:1534::TestColumnName`: result-field names. An
/// expression keeps its written text (`1 + c`), aggregates print `count(*)`,
/// aliases replace names in both directions (`select c d, d c`), a plain
/// column resolves to its own name, `hour(1)` aliased is `a`, and
/// parenthesized/unary-plus-wrapped columns keep the bare column name — all
/// matching Go's `fields[i].Column.Name.L` / `ColumnAsName.L`.
///
/// The table/database name metadata Go also asserts (`fields[0].Table.Name`,
/// `DBName`) is not carried by this tier's `SelectMeta` — that slice is the
/// `column_name_table_metadata` gap below.
#[test]
fn column_name_resolution() {
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    create(&mut catalog, "create table t (c int, d int)");
    insert(&mut catalog, "insert t values(1,1)");

    // Go: `set sql_mode='STRICT_TRANS_TABLES'` first.
    let ctx = ctx().with_ddl_sql_mode(tidb_mysql::ModeStrictTransTables.0);
    let names = |sql: &str| -> Vec<String> {
        let (columns, _) = run_select_meta_in(sql, &catalog, "test", &ctx)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        columns.into_iter().map(|(name, _)| name).collect()
    };

    assert_eq!(
        names("select 1 + c, count(*) from t"),
        vec!["1 + c", "count(*)"]
    );
    assert_eq!(
        names("select (c) > all (select c from t) from t"),
        vec!["(c) > all (select c from t)"]
    );
    assert_eq!(names("select c d, d c from t"), vec!["d", "c"]);
    assert_eq!(names("select c as a from t as t2"), vec!["a"]);
    assert_eq!(names("select hour(1) as a from t as t2"), vec!["a"]);
    assert_eq!(
        names("select (c), (+c), +(c), +(+(c)), ++c from t"),
        vec!["c", "c", "c", "c", "c"]
    );
    assert_eq!(names("select if(1,c,c) from t"), vec!["if(1,c,c)"]);
    // Issue 9639: window function next to an expression.
    assert_eq!(
        names("select 1+1, row_number() over() num from t"),
        vec!["1+1", "num"]
    );
}

/// A quantified comparison, IN, and EXISTS in the SELECT LIST are lowered by
/// the projection path, not only in a filter. Go's `expressionRewriter`
/// handles all four subquery forms wherever they appear; the Rust projection
/// path lowered only a bare `Expr::Subquery`, so
/// `select (c) > all (select c from t) from t` failed with "expression form is
/// not yet supported by the rewriter".
#[test]
fn a_select_field_quantified_subquery_is_lowered() {
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    create(&mut catalog, "create table t (c int, d int)");
    insert(&mut catalog, "insert t values(1,1)");
    // `> ALL` is planned, named by its written text, and evaluated.
    let (columns, _) = run_select_meta_in(
        "select (c) > all (select c from t) from t",
        &catalog,
        "test",
        &ctx(),
    )
    .unwrap();
    assert_eq!(columns[0].0, "(c) > all (select c from t)");
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select (c) > all (select c from t) from t"
        )),
        vec![vec!["0".to_owned()]]
    );
    // IN and EXISTS evaluate in the select list too.
    assert_eq!(
        rows_text(&select(&catalog, "select c in (select c from t) from t")),
        vec![vec!["1".to_owned()]]
    );
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select exists (select 1 from t where t.c = t2.c) from t t2"
        )),
        vec![vec!["1".to_owned()]]
    );
}

/// Go `executor_test.go:2157::TestMaxOneRow`: a scalar subquery returning
/// more than one row fails with `[executor:1242]Subquery returns more than 1
/// row` at read time.
#[test]
fn max_one_row_scalar_subquery_is_1242() {
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    create(&mut catalog, "create table t1(a double, b double)");
    create(&mut catalog, "create table t2(a double, b double)");
    insert(&mut catalog, "insert into t1 values(1, 1), (2, 2), (3, 3)");
    insert(&mut catalog, "insert into t2 values(0, 0)");
    let error = select_err(
        &catalog,
        "select (select t1.a from t1 where t1.a > t2.a) as a from t2",
    );
    let sql_error = error.clone().to_mysql_error();
    assert_eq!(sql_error.code, 1242, "{error:?}");
    assert_eq!(sql_error.message, "Subquery returns more than 1 row");
}

/// Go `executor_test.go:2275::TestSessionRootTrackerDetach`, ported at the
/// session/ statement tracker boundary (`SessionMemory`/`StatementMemory`,
/// Go's `ResetContextOfStmt` shape): while a statement is live the session
/// root's action chain carries a fallback (Go's `GetFallbackForTest` is
/// non-nil) and after the statement finishes — Go's `rs.Close()` — the
/// fallback is detached, so a fresh statement starts clean.
#[test]
fn session_root_tracker_fallback_detaches_after_the_statement() {
    // Go's quota=10 arm: an over-quota statement errors 8175 (pinned for the
    // write path by driver::tests::mem_quota; the fallback lifecycle is what
    // is pinned here).
    let session = crate::SessionMemory::new(10_000_000, crate::OomAction::Cancel, 1);
    let statement = session.statement();
    assert!(
        statement
            .session_tracker()
            .get_fallback_for_test(false)
            .is_some(),
        "Go: MemTracker.GetFallbackForTest(false) is non-nil while the result set is open"
    );
    statement.finish_statement();
    assert!(
        statement
            .session_tracker()
            .get_fallback_for_test(false)
            .is_none(),
        "Go: after rs.Close() GetFallbackForTest(false) is nil"
    );
}

