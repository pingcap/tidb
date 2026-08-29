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

//! Source-mapped ports of Go `pkg/executor/test/jointest/join_test.go` items
//! 1015–1020. The running rows cover deterministic join semantics; session
//! variables, worker cleanup, failpoints, memory quotas, and prepared
//! statements are retained as explicit gaps.

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
        Datum::Decimal(value) => value.to_string(),
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

/// Go `join_test.go:32::TestJoin2`: basic outer and duplicate-key joins from
/// the large mixed strategy test.
#[test]
fn join2_outer_and_duplicate_rows() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t (c1 int, c2 int)");
    create(&mut catalog, "create table t1 (c1 int, c2 int)");
    insert(&mut catalog, "insert into t values (1,1),(2,2)");
    insert(&mut catalog, "insert into t1 values (2,3),(4,4)");
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select * from t left join t1 on t.c1=t1.c1 order by t.c1",
        )),
        vec![
            vec![
                "1".to_owned(),
                "1".to_owned(),
                "<nil>".to_owned(),
                "<nil>".to_owned(),
            ],
            vec![
                "2".to_owned(),
                "2".to_owned(),
                "2".to_owned(),
                "3".to_owned(),
            ],
        ],
    );
    create(&mut catalog, "create table dup (c1 int)");
    insert(&mut catalog, "insert into dup values (1),(1),(1)");
    assert_eq!(
        select(&catalog, "select * from dup a join dup b on a.c1=b.c1").len(),
        9
    );
}
