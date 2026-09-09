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

//! Source-mapped ports of Go `pkg/executor/test/issuetest` items 967–992.
//!
//! The running tests below pin SQL-result contracts that are available from
//! the in-process executor catalog. Failpoint, session, transaction, DDL-job,
//! EXPLAIN ANALYZE, and memory-manager arms remain explicit gaps.

use crate::{run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};
use tidb_datatype::Datum;

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn create(catalog: &mut Catalog, sql: &str) {
    run_create_table_on(sql, catalog)
        .unwrap_or_else(|error| panic!("create {sql:?} failed: {error:?}"));
}

fn insert(catalog: &mut Catalog, sql: &str) {
    run_insert_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("insert {sql:?} failed: {error:?}"));
}

fn select(catalog: &Catalog, sql: &str) -> Vec<Vec<Datum>> {
    run_select_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("select {sql:?} failed: {error:?}"))
}

fn cell(datum: &Datum) -> String {
    match datum {
        Datum::Null => "<nil>".to_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) => format!("{}", value),
        Datum::Time(value) => value.to_string(),
        Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        other => format!("{:?}", other),
    }
}

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(cell).collect())
        .collect()
}

/// Go `executor_issue_test.go:73::TestUnionIssue`, data-level arms: UNION
/// preserves the source type conversions and NULL rows in the cases that do
/// not require prepared-protocol metadata or a pessimistic transaction.
#[test]
fn union_issue_data_type_and_null_arms() {
    let mut catalog = Catalog::default();
    assert_eq!(
        rows_text(&select(
            &catalog,
            "(select cast('abcdefghijklmnopqrstuvwxyz' as char) as c1) union all (select 1 where false)",
        )),
        vec![vec!["abcdefghijklmnopqrstuvwxyz".to_owned()]],
    );

    create(&mut catalog, "create table tbl_3 (col_15 bit(20))");
    create(&mut catalog, "create table tbl_23 (col_15 bit(15))");
    insert(&mut catalog, "insert into tbl_3 values (0xFFFF), (0xFF)");
    insert(&mut catalog, "insert into tbl_23 values (0xF)");
    let rows = select(
        &catalog,
        "select col_15 from tbl_23 union all select col_15 from tbl_3 order by col_15",
    );
    assert_eq!(rows.len(), 3, "Go Issue25506 expects three BIT rows");

    let mut greatest_rows = rows_text(&select(
        &catalog,
        "select greatest(cast('2020-01-01 01:01:01' as datetime), cast('2019-01-01 01:01:01' as datetime)) union select null",
    ));
    greatest_rows.sort();
    assert_eq!(
        greatest_rows,
        vec![
            vec!["2020-01-01 01:01:01".to_owned()],
            vec!["<nil>".to_owned()],
        ],
    );
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select quote(cast('abc' as char)) union all select '1' order by 1",
        )),
        vec![vec!["'abc'".to_owned()], vec!["1".to_owned()]],
    );
}

/// Go `executor_issue_test.go:640::TestIssue50393`: a blob containing the
/// prefix bytes is found by a LIKE pattern built from another blob.
#[test]
fn issue50393_blob_like_prefix() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t1 (a blob)");
    create(&mut catalog, "create table t2 (a blob)");
    insert(&mut catalog, "insert into t1 values (0xC2A0)");
    insert(&mut catalog, "insert into t2 values (0xC2)");
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select count(*) from t1, t2 where t1.a like concat('%', t2.a, '%')",
        )),
        vec![vec!["1".to_owned()]],
    );
}

/// Go `executor_issue_test.go:682::TestIssue52978`: TRUNCATE with a DOUBLE
/// precision argument keeps the constant result through MIN.
#[test]
fn issue52978_truncate_double_constant() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t (a int)");
    insert(
        &mut catalog,
        "insert into t values (-1790816583), (2049821819), (-1366665321), (536581933), (-1613686445)",
    );
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select min(truncate(cast(-26340 as double), t.a)) from t",
        )),
        vec![vec!["-26340".to_owned()]],
    );
}
