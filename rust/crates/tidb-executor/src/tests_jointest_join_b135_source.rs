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

use crate::{Catalog, StmtContext, run_create_table_on, run_insert_on, run_select_on};
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

/// The session-variable, hint-warning, user-variable, derived-table, and
/// plan-order portions of Go `TestJoin2` are not represented by the catalog
/// driver.
#[test]
#[ignore = "go-parity-gap: join strategy/session variables, warning state, user variables, and plan-order assertions are unported"]
fn join2_session_and_strategy_arms() {}

/// Go `join_test.go:290::TestJoinLeak`: close a partially consumed concurrent
/// hash join without leaking workers.
#[test]
#[ignore = "go-parity-gap: result-set lifecycle, concurrent hash-join workers, and transaction setup are unported"]
fn join_leak_on_partial_close() {}

/// Go `join_test.go:313::TestNullEmptyAwareSemiJoin`: NULL/empty-aware
/// anti-semi semantics are already exercised by the data-level correlated
/// apply carriers; this source case additionally requires five forced join
/// strategies and the full hint/session surface.
#[test]
#[ignore = "go-parity-gap: five forced join strategies, session hints, and NULL-aware semi-join planner paths are unported here"]
fn null_empty_aware_semi_join() {}

/// Go `join_test.go:708::TestIssue18070`: query-memory cancellation for index
/// hash and index merge joins.
#[test]
#[ignore = "go-parity-gap: session memory quota, OOM action, and index-join OOM failpoint are unported"]
fn issue18070_index_join_oom() {}

/// Go `join_test.go:732::TestIssue20779`: injected inner lookup error is
/// returned while consuming an index-hash-join result.
#[test]
#[ignore = "go-parity-gap: IndexHashJoin failpoint and session result-consumption surface are unported"]
fn issue20779_index_hash_join_error() {}

/// Go `join_test.go:753::TestIssue30211`: injected index-join panic, plan
/// cache execution, and memory cancellation.
#[test]
#[ignore = "go-parity-gap: index-join failpoints, PREPARE/EXECUTE plan cache, and session OOM action are unported"]
fn issue30211_index_join_panic_and_plan_cache() {}
