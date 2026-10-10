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

//! Statements Go's planner refuses, names or resolves in a way
//! `planner/core/integration.test` and `executor/executor.test` record.

use crate::tests_support::row_text;
use crate::Session;

fn code(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_mysql_error();
    (error.code, error.message)
}

/// Go `CheckUpdateList`: a table updated through two aliases may not have
/// its primary key changed through either.
#[test]
fn a_primary_key_updated_through_two_aliases_is_refused() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int)")
        .unwrap();
    session.run("insert into t values (1, 2)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "update t m, t n set m.a = m.a + 10, n.a = n.a + 10"
        ),
        (
            1706,
            "Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "update t m, t n, t q set m.b = m.b + 1, n.b = n.b + 10, q.a = q.a - 10"
        )
        .0,
        1706
    );
    session
        .run("update t m, t n set m.b = m.b + 1, n.b = n.b + 10")
        .unwrap();
    assert_eq!(
        row_text(session.run("select * from t")),
        vec![vec!["1", "12"]]
    );
}

/// Go's preprocessor `checkNonUniqTableAlias`, and `buildSelection`, which
/// rewrites every conjunct before it folds a constant one.
#[test]
fn from_aliases_and_where_columns_are_checked_before_folding() {
    let mut session = Session::new();
    session.run("create table t (a int, b int)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "select a, b from (select 1 a) `x`, (select 2 b) `x`"
        ),
        (1066, "Not unique table/alias: 'x'".to_owned())
    );
    assert_eq!(
        code(&mut session, "select * from t where 0 and c = 10").0,
        1054
    );
}

/// Go `checkOnlyFullGroupByWithOutGroupClause` walks HAVING and ORDER BY too
/// once the field list names a column, and the last ORDER BY aggregate is
/// the one 3029 names.
#[test]
fn only_full_group_by_sees_aggregates_in_having_and_order_by() {
    let mut session = Session::new();
    session.run("create table t (v1 int, v2 int)").unwrap();
    assert_eq!(
        code(&mut session, "select v1 from t having count(v2)").0,
        8123
    );
    assert_eq!(
        code(&mut session, "select v1 from t order by count(v2), count(v1)"),
        (
            3029,
            "Expression #2 of ORDER BY contains aggregate function and applies to the result of a non-aggregated query".to_owned()
        )
    );
}

/// Go `checkGeneratedColumn`: a generated column may not read the
/// AUTO_INCREMENT column unless `tidb_enable_auto_increment_in_generated`.
#[test]
fn a_generated_column_reading_auto_increment_is_refused_at_create() {
    let mut session = Session::new();
    assert_eq!(
        code(
            &mut session,
            "create table t (a int primary key auto_increment, b int, c int as (a + 8) virtual)"
        ),
        (
            3109,
            "Generated column 'c' cannot refer to auto-increment column.".to_owned()
        )
    );
    session
        .run("set tidb_enable_auto_increment_in_generated = on")
        .unwrap();
    session
        .run("create table t (a int primary key auto_increment, b int, c int as (a + 8) virtual)")
        .unwrap();
}

/// Go `buildSelection` folds a constant conjunct with `EvalBool` in the
/// statement context: a strict UPDATE refuses the truncated empty string.
#[test]
fn a_truncated_constant_condition_fails_a_strict_update() {
    let mut session = Session::new();
    session.run("create table t (a varchar(10))").unwrap();
    session.run("insert into t values ('x')").unwrap();
    assert_eq!(
        code(&mut session, "update t set a = 'def' where from_base64('')"),
        (1292, "Truncated incorrect DOUBLE value: ''".to_owned())
    );
}

/// Go refuses a VALUES subquery whose plan keeps a correlated column
/// (`Insert's SET operation or VALUES_LIST doesn't support complex
/// subqueries now`), and evaluates an uncorrelated one.
#[test]
fn a_complex_values_subquery_is_refused() {
    let mut session = Session::new();
    session.run("create table t (a int, b int)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "insert into t values (81, (select (select '1' as c0 where '1' >= subq_0.c0) as c1 from (select '1' as c0) as subq_0))"
        ),
        (
            1105,
            "Insert's SET operation or VALUES_LIST doesn't support complex subqueries now".to_owned()
        )
    );
    session
        .run("insert into t values (82, (select 1))")
        .unwrap();
    assert_eq!(
        row_text(session.run("select * from t")),
        vec![vec!["82", "1"]]
    );
}

/// Go's parser builds `_cs'str'` as a string literal, named by its content,
/// and `_cs 0x..` as a binary literal, named by its text.
#[test]
fn an_introduced_literal_is_named_as_go_names_it() {
    let mut session = Session::new();
    let crate::StmtOutput::Rows { columns, .. } = session
        .run_with_columns("select _utf8\"string\", _utf8 0x41")
        .unwrap()
    else {
        panic!("expected rows");
    };
    let names: Vec<_> = columns.into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, vec!["string", "_utf8 0x41"]);
}

/// Go `temptable.DetachLocalTemporaryTableInfoSchema`: a view keeps reading
/// the permanent table a local temporary one shadows.
#[test]
fn a_view_reads_around_a_local_temporary_table() {
    let mut session = Session::new();
    session.run("create table t1 (a int, b int)").unwrap();
    session
        .run("create view v1 as select * from t1 order by a limit 5")
        .unwrap();
    session.run("insert into t1 values (1, 2), (3, 4)").unwrap();
    session
        .run("create temporary table t1 (a int, b int)")
        .unwrap();
    assert_eq!(
        row_text(session.run("select * from t1")),
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        row_text(session.run("select * from v1")),
        vec![vec!["1", "2"], vec!["3", "4"]]
    );
}
