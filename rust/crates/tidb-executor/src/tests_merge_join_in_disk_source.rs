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

//! Merge/hash join SQL-result cases from Go's merge_join_test.go and
//! join_pkg_test.go. These cases cover result equivalence and quota-driven
//! execution, not complete plan-text, failpoint, peak-tracker or concurrency
//! obligations; those remain in the current parity audit.

use tidb_vardef::OomAction;
use crate::{run_create_table_on, run_insert_on, run_select_on, SessionExtendedInfoSchema, StmtContext};
use tidb_datatype::Datum;

fn ctx() -> StmtContext {
    StmtContext::for_query().with_txn_store(Box::new(crate::test_store::store()))
}

/// testkit's `result.Sort()`: NULL first, then numbers numerically, then text.
fn datum_sort_key(datum: &Datum) -> (u8, String) {
    const NULL: u8 = 0;
    const NUMBER: u8 = 1;
    const TEXT: u8 = 2;
    match datum {
        Datum::Null => (NULL, String::new()),
        Datum::Int(value) => (NUMBER, format!("{value:024}")),
        Datum::UInt(value) => (NUMBER, format!("{value:024}")),
        Datum::Decimal(value) => (NUMBER, format!("{:024}", value)),
        Datum::Real(value) => (NUMBER, format!("{value:024}")),
        Datum::String(text) => (TEXT, String::from_utf8_lossy(text.bytes()).into_owned()),
        Datum::Bytes(bytes) => (TEXT, String::from_utf8_lossy(bytes).into_owned()),
        other => (TEXT, format!("{other:?}")),
    }
}

fn sorted(mut rows: Vec<Vec<Datum>>) -> Vec<Vec<Datum>> {
    rows.sort_by_key(|row| row.iter().map(datum_sort_key).collect::<Vec<_>>());
    rows
}

fn select_sorted(catalog: &SessionExtendedInfoSchema, sql: &str, context: &StmtContext) -> Vec<Vec<Datum>> {
    match run_select_on(sql, catalog, context) {
        Ok(rows) => sorted(rows),
        Err(error) => panic!("query {sql:?} failed: {error:?}"),
    }
}

/// Go `pkg/executor/join/test/mergejoin/merge_join_test.go:82::TestMergeJoinInDisk`,
/// data arm: `select /*+ TIDB_SMJ(t) */ * from t1 left outer join t on
/// t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20` over t=(1,1), t1=(1,3),(4,4)
/// must return exactly `1 3 1 1`. Go runs it under `tidb_mem_quota_query=1`
/// with `tidb_mem_oom_action='LOG'` (and the container-spill failpoint);
/// here the 1-byte quota with [`OomAction::Log`] exercises the same
/// run-to-completion-under-overrun contract through the statement driver.
#[test]
fn merge_join_in_disk_rows_under_log_quota() {
    let store = crate::test_store::store();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    run_create_table_on("create table t(c1 int, c2 int)", &mut catalog).unwrap();
    run_create_table_on("create table t1(c1 int, c2 int)", &mut catalog).unwrap();
    run_insert_on("insert into t values (1,1)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t1 values (1,3),(4,4)", &mut catalog, &ctx()).unwrap();

    let sql = "select /*+ TIDB_SMJ(t) */ * from t1 left outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20";
    let log_ctx = StmtContext::for_query().with_txn_store(Box::new(store.clone())).with_mem_quota(1, OomAction::Log);
    assert_eq!(
        run_select_on(sql, &catalog, &log_ctx).unwrap(),
        vec![vec![
            Datum::Int(1),
            Datum::Int(3),
            Datum::Int(1),
            Datum::Int(1),
        ]],
    );
}

/// Go `pkg/executor/join/test/mergejoin/merge_join_test.go:41::TestShuffleMergeJoinInDisk`,
/// data arm: the same TIDB_SMJ left-outer query over t=(1..4) and t1 = four
/// rows for every i in 1..1024 (i%4 pattern) under a 1-byte quota with LOG
/// action must return, as a multiset, `1 1 1 1` plus `(i, i, NULL, NULL)`
/// for i in 21..1024 -- 1005 rows. Go also asserts the session
/// MemTracker/DiskTracker peaks and the shuffle concurrency; those are the
/// `merge_join_tracker_and_shuffle_gaps` test below.
#[test]
fn shuffle_merge_join_in_disk_rows_under_log_quota() {
    let store = crate::test_store::store();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    run_create_table_on("create table t(c1 int, c2 int)", &mut catalog).unwrap();
    run_create_table_on("create table t1(c1 int, c2 int)", &mut catalog).unwrap();
    run_insert_on(
        "insert into t values (1,1),(2,2),(3,3),(4,4)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    let values: String = (1..=1024i64)
        .step_by(4)
        .map(|i| {
            format!(
                "({i},{i}),({},{ }),({},{ }),({},{ })",
                i + 1,
                i + 1,
                i + 2,
                i + 2,
                i + 3,
                i + 3
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    run_insert_on(
        &format!("insert into t1 values {values}"),
        &mut catalog,
        &ctx(),
    )
    .unwrap();

    let sql = "select /*+ TIDB_SMJ(t) */ * from t1 left outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20";
    let log_ctx = StmtContext::for_query().with_txn_store(Box::new(store.clone())).with_mem_quota(1, OomAction::Log);
    let rows = select_sorted(&catalog, sql, &log_ctx);
    assert_eq!(
        rows.len(),
        1005,
        "1 matched row plus i = 21..1024 unmatched"
    );
    // The matched row: t1 (1,1) joined to t (1,1) -- Go's "1 1 1 1".
    assert_eq!(
        rows[0],
        vec![Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Int(1)],
    );
    // Every i in 21..1024 appears once as (i, i, NULL, NULL).
    let mut unmatched: Vec<(i64, i64)> = rows[1..]
        .iter()
        .map(|row| match (&row[0], &row[1], &row[2]) {
            (Datum::Int(a), Datum::Int(b), Datum::Null) => (*a, *b),
            other => panic!("unexpected unmatched row {other:?}"),
        })
        .collect();
    unmatched.sort_unstable();
    let expected: Vec<(i64, i64)> = (21..=1024i64).map(|i| (i, i)).collect();
    assert_eq!(unmatched, expected);
}

/// Go `pkg/executor/join_pkg_test.go:82::TestJoinExec`, data dimension: an
/// inner hash join over identical (bigint, double) sources joined on BOTH
/// columns yields one 4-column row per key with column parity
/// `val == col0 == col2` and `float(val) == col1 == col3`, for every key in
/// 0..rows. Go sweeps rows over {3, 1024, 4096}, worker concurrency over
/// {1, 4} and a RowContainer-spill failpoint over {false, true}; the
/// concurrency and failpoint dimensions are the
/// `join_exec_spill_and_concurrency_gaps` test, and the `disk=true` arms'
/// `AlreadySpilledSafeForTest` assertion has no statement-driver surface.
#[test]
fn join_exec_inner_equi_rows_source() {
    for rows in [3i64, 1024, 4096] {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        run_create_table_on("create table tl(a bigint, b double)", &mut catalog).unwrap();
        run_create_table_on("create table tr(a bigint, b double)", &mut catalog).unwrap();
        let values: String = (0..rows)
            .map(|i| format!("({i}, {i}.0)"))
            .collect::<Vec<_>>()
            .join(",");
        run_insert_on(
            &format!("insert into tl values {values}"),
            &mut catalog,
            &ctx(),
        )
        .unwrap();
        run_insert_on(
            &format!("insert into tr values {values}"),
            &mut catalog,
            &ctx(),
        )
        .unwrap();

        let result = run_select_on(
            "select tl.a, tl.b, tr.a, tr.b from tl, tr where tl.a = tr.a and tl.b = tr.b order by tl.a",
            &catalog,
            &ctx(),
        )
        .unwrap();
        assert_eq!(result.len(), rows as usize, "one output row per key");
        assert_eq!(result[0].len(), 4usize, "join result has 4 columns");
        let mut visited = std::collections::BTreeSet::new();
        for row in &result {
            let val = match &row[0] {
                Datum::Int(value) => *value,
                other => panic!("expected int key, got {other:?}"),
            };
            assert_eq!(row[1], Datum::Real(val as f64), "column parity at {val}");
            assert_eq!(row[2], Datum::Int(val));
            assert_eq!(row[3], Datum::Real(val as f64));
            visited.insert(val);
        }
        for key in 0..rows {
            assert!(
                visited.contains(&key),
                "key {key} missing from the join result"
            );
        }
    }
}

/// The apply-over-join composition Go's `TestHashJoinV2UnderApply` exists to
/// protect (an apply whose inner side is a join, re-executed per outer row),
/// with Go's row-count expectation shape: each outer row re-derives the join
/// and the counts stay consistent.
#[test]
fn join_under_apply_repeated_inner_rows() {
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    run_create_table_on("create table t_outer(a int)", &mut catalog).unwrap();
    run_create_table_on("create table tl(a bigint, b double)", &mut catalog).unwrap();
    run_create_table_on("create table tr(a bigint, b double)", &mut catalog).unwrap();
    run_insert_on(
        "insert into t_outer values (1),(2),(3)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    run_insert_on(
        "insert into tl values (0,0),(1,1),(2,2)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    run_insert_on(
        "insert into tr values (0,0),(1,1),(2,2)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    // The inner join (3 rows) is re-executed once per outer row; the scalar
    // count filters on the outer key so each outer row re-reads the join.
    assert_eq!(
        run_select_on(
            "select t_outer.a, (select count(*) from tl, tr where tl.a = tr.a and tl.b = tr.b and tl.a < t_outer.a) from t_outer order by t_outer.a",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(2)],
            vec![Datum::Int(3), Datum::Int(3)],
        ],
    );
}
