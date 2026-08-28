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

//! Data-level ports of Go `pkg/executor/test/jointest/join_test.go`'s
//! issue regressions (items 1021–1024 of the batch enumeration): the
//! BIGINT UNSIGNED ↔ BIT(64) join-equality pins of issues 11895/11896 and
//! the row-count contract of the single-task incremental index hash join.
//!
//! SCOPE NOTE. Go drives these through a suite that first runs
//! `set @@tidb_max_chunk_size=32`, `set @@tidb_index_lookup_join_concurrency=1`
//! and `set @@tidb_index_join_batch_size=32` and forces the index-hash-join
//! strategy with `/*+ INL_HASH_JOIN(...) */`. This tier's driver has no SET
//! statement or join-concurrency surface, and strategy selection belongs to
//! the shared physical planner, so the pins here are the ROWS the suite
//! checks (`testkit.Rows` assertions), not the physical operator that
//! produced them. The hint text is kept in the SQL so the parsed-hint path
//! is exercised too (`plan_hints.rs:958` recognizes `inl_hash_join`).

use tidb_datatype::Datum;

use crate::{run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn text_of(datum: &Datum) -> String {
    match datum {
        Datum::Null => "<nil>".to_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) => format!("{value}"),
        Datum::Decimal(value) => value.to_string(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    }
}

fn rows_text(rows: &[Vec<Datum>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(text_of).collect())
        .collect()
}

/// Go `pkg/executor/test/jointest/join_test.go:917::TestIssue11895`: a
/// BIGINT UNSIGNED and a BIT(64) holding the same 64-bit pattern join on
/// equality, and `hex()` renders the BIT side as `FFFFFFFFFFFFFFFF`.
#[test]
fn issue11895_bigint_unsigned_matches_bit64_max_pattern() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t(c1 bigint unsigned)", &mut catalog).unwrap();
    run_create_table_on("create table t1(c1 bit(64))", &mut catalog).unwrap();
    run_insert_on("insert into t value(18446744073709551615)", &mut catalog, &ctx()).unwrap();
    // Go inserts -1; BIT(64) stores the two's-complement pattern, all ones.
    run_insert_on("insert into t1 value(-1)", &mut catalog, &ctx()).unwrap();

    let rows = run_select_on(
        "select t.c1, hex(t1.c1) from t, t1 where t.c1 = t1.c1",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        rows_text(&rows),
        vec![vec![
            "18446744073709551615".to_owned(),
            "FFFFFFFFFFFFFFFF".to_owned()
        ]],
        "Go: tk.MustQuery(...).Check(testkit.Rows(\"18446744073709551615 FFFFFFFFFFFFFFFF\"))"
    );
}

/// Go `pkg/executor/test/jointest/join_test.go:931::TestIssue11896`, both
/// arms: a BIGINT and a BIT(64) holding the small value 1 join to one row
/// rendered `1`/`1`, while the signed/unsigned mismatch arm (`-1` vs the
/// stored all-ones pattern) must produce NO rows -- the issue was the crash
/// and wrong match this comparison used to yield.
#[test]
fn issue11896_bigint_bit64_equality_sign_arms() {
    let mut catalog = Catalog::default();

    // Arm 1: 1 == 1.
    run_create_table_on("create table t(c1 bigint)", &mut catalog).unwrap();
    run_create_table_on("create table t1(c1 bit(64))", &mut catalog).unwrap();
    run_insert_on("insert into t value(1)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t1 value(1)", &mut catalog, &ctx()).unwrap();
    let rows = run_select_on(
        "select t.c1, hex(t1.c1) from t, t1 where t.c1 = t1.c1",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(rows_text(&rows), vec![vec!["1".to_owned(), "1".to_owned()]]);

    // Arm 2: -1 (signed) vs 18446744073709551615 stored in BIT(64) must not
    // match; Go checks the empty result with `.Check(nil)`.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t(c1 bigint)", &mut catalog).unwrap();
    run_create_table_on("create table t1(c1 bit(64))", &mut catalog).unwrap();
    run_insert_on("insert into t value(-1)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t1 value(18446744073709551615)", &mut catalog, &ctx()).unwrap();
    let rows = run_select_on(
        "select * from t, t1 where t.c1 = t1.c1",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert!(rows.is_empty(), "Go: .Check(nil), got {rows:?}");
}

/// Go `pkg/executor/test/jointest/join_test.go:954
/// ::TestSingleTaskIncrementalIndexHashJoin`: over `t1` (9 rows, a=2..10)
/// and `t2` (9000 rows, b=i/1000 so b=0..8 have 1000 rows each and b=9 has
/// one row), the inner join counts 7001, both outer variants 7002 (the
/// unmatched a=10 row), and the star projections materialize without
/// asserting shape. The final correlated `NOT IN` arm is a measured gap.
#[test]
fn single_task_incremental_index_hash_join_row_counts() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int primary key)", &mut catalog).unwrap();
    run_create_table_on(
        "create table t2(b int not null, c varchar(100), index idx_b(b))",
        &mut catalog,
    )
    .unwrap();

    let mut sql1 = String::from("insert into t1 values ");
    for i in 2..=10 {
        if i > 2 {
            sql1.push(',');
        }
        sql1.push_str(&format!("({i})"));
    }
    run_insert_on(&sql1, &mut catalog, &ctx()).unwrap();

    let mut sql2 = String::from("insert into t2 values ");
    for i in 1..=9000 {
        if i > 1 {
            sql2.push(',');
        }
        sql2.push_str(&format!("({}, 'abc')", i / 1000));
    }
    assert_eq!(run_insert_on(&sql2, &mut catalog, &ctx()).unwrap(), 9000);

    let count =
        |sql: &str| -> i64 {
            let rows = run_select_on(sql, &catalog, &ctx()).unwrap();
            assert_eq!(rows.len(), 1);
            match rows[0][0] {
                Datum::Int(value) => value,
                ref other => panic!("count returned {other:?}"),
            }
        };

    // Go's MustQuery star arms run the full materialization; the COUNT arms
    // carry the checked literals.
    let star_inner = run_select_on(
        "select /*+ inl_hash_join(t1,t2) */ * from t1 inner join t2 on t1.a = t2.b",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(star_inner.len(), 7001);
    let star_left = run_select_on(
        "select /*+ inl_hash_join(t1,t2) */ * from t1 left join t2 on t1.a = t2.b",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(star_left.len(), 7002);
    let star_right = run_select_on(
        "select /*+ inl_hash_join(t2,t1) */ * from t2 right join t1 on t1.a = t2.b",
        &catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(star_right.len(), 7002);

    assert_eq!(
        count("select /*+ inl_hash_join(t1,t2) */ count(*) from t1 inner join t2 on t1.a = t2.b"),
        7001,
        "Go: .Check(testkit.Rows(\"7001\"))"
    );
    assert_eq!(
        count("select /*+ inl_hash_join(t1,t2) */ count(*) from t1 left join t2 on t1.a = t2.b"),
        7002,
        "Go: .Check(testkit.Rows(\"7002\"))"
    );
    assert_eq!(
        count("select /*+ inl_hash_join(t2,t1) */ count(*) from t2 right join t1 on t1.a = t2.b"),
        7002,
        "Go: .Check(testkit.Rows(\"7002\"))"
    );
}

/// Go `pkg/executor/test/jointest/join_test.go:954
/// ::TestSingleTaskIncrementalIndexHashJoin`'s final arm: the correlated
/// `NOT IN` anti-join over the same fixture must count 1 (the unmatched
/// a=10 row). Measured on this tier: `UnknownColumnInClause { column:
/// "t1.a", clause: "where clause" }` both with and without the hint -- the
/// outer column reference inside the IN subquery's WHERE is not resolved.
#[test]
#[ignore = "go-parity-gap: measured — the correlated `t1.a` reference inside the NOT IN subquery fails UnknownColumnInClause on this tier; decorrelation of that shape is unported"]
fn single_task_incremental_index_hash_join_not_in_correlated_arm() {}

/// Go `pkg/executor/test/jointest/join_test.go:880::TestIssue49033`'s final
/// arms: with failpoint
/// `github.com/pingcap/tidb/pkg/executor/testIssue49033` returning, reading
/// the ordered (and unordered) index-hash-join result must fail with exactly
/// `testIssue49033` from `session.GetRows4Test`, after which `rs.Close()`
/// still succeeds. The failpoint hook and the built-tree read surface have
/// no Rust equivalent, so the injected-error contract is not pinnable here.
#[test]
#[ignore = "go-parity-gap: the testIssue49033 failpoint and the session.GetRows4Test/built-tree seam are unported; measured: this tier has no failpoint surface"]
fn issue49033_injected_error_cancels_index_hash_join_reads() {}

/// Go `pkg/executor/test/jointest/main_test.go:26::TestMain` only sets
/// autoid step, suite config (slow threshold, async-commit windows,
/// expression-index switch) and goleak hooks -- bootstrap with no statement
/// behavior to pin on this tier.
#[test]
#[ignore = "go-parity-gap: jointest TestMain is goleak/config suite bootstrap (autoid.SetStep(5000), config.UpdateGlobal, tikv.EnableFailpoints); no statement behavior"]
fn jointest_main_is_bootstrap_only() {}
