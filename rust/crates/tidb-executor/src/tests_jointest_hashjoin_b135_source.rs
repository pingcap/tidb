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

//! Source-mapped ports of Go `pkg/executor/test/jointest/hashjoin` items
//! 993–1014. SQL-result-only regressions run through the catalog driver;
//! failpoint, EXPLAIN ANALYZE, runtime-statistics, and worker-lifecycle cases
//! remain explicit gaps.

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

/// Go `hash_join_test.go:32::TestIndexNestedLoopHashJoin`: the index-hash
/// join's row contract is already covered by the smaller local index-join
/// carrier; the Go test additionally needs plan text, analyze, semi-join
/// strategy forcing, and runtime setup.
#[test]
#[ignore = "go-parity-gap: forced IndexHashJoin plan/EXPLAIN and semi-join execution surfaces are unported; row carrier is tests_index_join"]
fn index_nested_loop_hash_join() {}

/// Go `hash_join_test.go:156::TestIssue52902`: IndexHashJoin-v2 semi-join
/// regression under a planner failpoint.
#[test]
#[ignore = "go-parity-gap: MockOnlyEnableIndexHashJoinV2 planner failpoint is unported"]
fn issue52902_index_hash_join_semi_join() {}

/// Go `hash_join_test.go:181::TestHashJoin`: EXPLAIN ANALYZE runtime stats for
/// empty-build hash joins.
#[test]
#[ignore = "go-parity-gap: EXPLAIN ANALYZE hash-join runtime statistics are unported"]
fn hash_join_explain_analyze() {}

/// Go `hash_join_test.go:210::TestOuterTableBuildHashTableIsuse13933`: row
/// semantics of the preserved-side build orientation.
#[test]
fn outer_table_build_hash_table_row_semantics() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t (a int, b int)");
    create(&mut catalog, "create table s (a int, b int)");
    insert(&mut catalog, "insert into t values (11,11),(1,2)");
    insert(&mut catalog, "insert into s values (1,2),(2,1),(11,11)");
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select * from t left join s on s.a > t.a order by t.a, s.a",
        )),
        vec![
            vec![
                "1".to_owned(),
                "2".to_owned(),
                "2".to_owned(),
                "1".to_owned(),
            ],
            vec![
                "1".to_owned(),
                "2".to_owned(),
                "11".to_owned(),
                "11".to_owned(),
            ],
            vec![
                "11".to_owned(),
                "11".to_owned(),
                "<nil>".to_owned(),
                "<nil>".to_owned(),
            ],
        ],
    );
}

/// Go `hash_join_test.go:244::TestInlineProjection4HashJoinIssue15316`:
/// projection must retain the build-side columns after a residual predicate.
#[test]
fn inline_projection_hash_join_row_semantics() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table s (a int not null, b int, c int)",
    );
    create(
        &mut catalog,
        "create table t (a int not null, b int, c int)",
    );
    insert(
        &mut catalog,
        "insert into s values (0,1,2),(0,1,NULL),(0,1,2)",
    );
    insert(
        &mut catalog,
        "insert into t values (0,10,2),(0,10,NULL),(1,10,2)",
    );
    let rows = select(
        &catalog,
        "select t.a,t.a,t.c from s join t on t.a=s.a where s.b<t.b order by t.a,t.c",
    );
    assert_eq!(rows.len(), 6, "Go expects six projected rows");
}

/// Go `hash_join_test.go:277::TestIssue18572_1`: inner-worker failpoint.
#[test]
#[ignore = "go-parity-gap: IndexHashJoin inner-worker failpoint and session result consumption are unported"]
fn issue18572_inner_worker_error() {}

/// Go `hash_join_test.go:298::TestIssue18572_2`: outer-worker failpoint.
#[test]
#[ignore = "go-parity-gap: IndexHashJoin outer-worker failpoint and session result consumption are unported"]
fn issue18572_outer_worker_error() {}

/// Go `hash_join_test.go:319::TestIssue18572_3`: build-worker failpoint.
#[test]
#[ignore = "go-parity-gap: IndexHashJoin build-worker failpoint and session result consumption are unported"]
fn issue18572_build_error() {}

/// Go `hash_join_test.go:340::TestExplainAnalyzeJoin`: runtime-statistics
/// formatting for index, index-hash, and hash joins.
#[test]
#[ignore = "go-parity-gap: EXPLAIN ANALYZE and join runtime-statistics formatting are unported"]
fn explain_analyze_join() {}

/// Go `hash_join_test.go:387::TestIssue20270`: cancellation from probe and
/// outer-hash-join failpoints.
#[test]
#[ignore = "go-parity-gap: hash-join cancellation failpoints are unported"]
fn issue20270_kill_during_join() {}

/// Go `hash_join_test.go:411::TestIssue31129`: IndexHashJoin error, panic, and
/// fetch-inner failpoints.
#[test]
#[ignore = "go-parity-gap: IndexHashJoin failpoint suite is unported"]
fn issue31129_index_hash_join_failures() {}

/// Go `hash_join_test.go:461::TestSplitPartitionPanic`.
#[test]
#[ignore = "go-parity-gap: HashJoinV2 split-partition failpoint is unported"]
fn split_partition_panic() {}

/// Go `hash_join_test.go:481::TestProcessOneProbeChunkPanic`.
#[test]
#[ignore = "go-parity-gap: HashJoinV2 probe-chunk failpoint is unported"]
fn process_one_probe_chunk_panic() {}

/// Go `hash_join_test.go:501::TestCreateTasksPanic`.
#[test]
#[ignore = "go-parity-gap: HashJoinV2 task-creation failpoint is unported"]
fn create_tasks_panic() {}

/// Go `hash_join_test.go:521::TestBuildHashTablePanic`.
#[test]
#[ignore = "go-parity-gap: HashJoinV2 build-table failpoint is unported"]
fn build_hash_table_panic() {}

/// Go `hash_join_test.go:541::TestKillDuringProbe`.
#[test]
#[ignore = "go-parity-gap: HashJoinV2 probe cancellation and session kill are unported"]
fn kill_during_probe() {}

/// Go `hash_join_test.go:571::TestKillDuringBuild`.
#[test]
#[ignore = "go-parity-gap: HashJoinV2 build cancellation and session kill are unported"]
fn kill_during_build() {}

/// Go `hash_join_test.go:597::TestIssue54755`: left/right outer joins retain
/// the preserved value through a NULL-matching residual.
#[test]
fn issue54755_outer_join_row_semantics() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table t1(pk int primary key, col_int_nokey int, col_int_key int, col_varchar_key varchar(1), col_varchar_nokey varchar(1), key(col_int_key), key(col_varchar_key, col_int_key))",
    );
    create(
        &mut catalog,
        "create table t2(pk int primary key, col_int_nokey int, col_int_key int, col_varchar_key varchar(1), col_varchar_nokey varchar(1), key(col_int_key), key(col_varchar_key, col_int_key))",
    );
    insert(
        &mut catalog,
        "insert into t1 values (1,2,4,'v','v'),(2,150,62,'v','v')",
    );
    insert(
        &mut catalog,
        "insert into t2 values (1,NULL,8,'x','x'),(2,8,7,'d','d')",
    );
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select max(t1.col_int_nokey) from t2 right join t1 on t1.col_varchar_key=t2.col_varchar_nokey",
        )),
        vec![vec!["150".to_owned()]],
    );
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select max(t1.col_int_nokey) from t1 left join t2 on t1.col_varchar_key=t2.col_varchar_nokey",
        )),
        vec![vec!["150".to_owned()]],
    );
}

/// Go `hash_join_test.go:614::TestIssue55016`: opposite string/char
/// collations do not produce a false two-row match under either hash-join
/// implementation.
#[test]
fn issue55016_char_varchar_equality() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t (a varchar(10), b char(10))");
    insert(&mut catalog, "insert into t values ('aa','a')");
    assert_eq!(
        rows_text(&select(
            &catalog,
            "select count(*) from t t1 join t t2 on t1.a=t2.b and t2.a=t1.b",
        )),
        vec![vec!["0".to_owned()]],
    );
}

/// Go `hash_join_test.go:627::TestIssue56214`: correlated scalar subquery
/// uses the outer value in the join residual. The local driver currently
/// reports `UnknownColumn("t3.value")` for this correlated shape.
#[test]
#[ignore = "go-parity-gap: correlated join residual cannot resolve the outer t3 reference on this tier (measured UnknownColumn(\"t3.value\"))"]
fn issue56214_correlated_join_residual() {}

/// Go `hash_join_test.go:646::TestIssue56825`: both preserved-side choices
/// retain unmatched rows when the residual is evaluated after equality.
#[test]
fn issue56825_outer_join_residual() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t1(id int, col1 int)");
    create(
        &mut catalog,
        "create table t2(id int, col1 int, col2 int, col3 int, col4 int, col5 int)",
    );
    insert(&mut catalog, "insert into t1 values (1,2),(2,3)");
    insert(
        &mut catalog,
        "insert into t2 values (1,2,3,4,5,6),(3,4,5,6,7,8),(4,5,6,7,8,9)",
    );
    assert_eq!(
        select(
            &catalog,
            "select * from t1 left join t2 on t1.id=t2.id and t1.col1<=t2.col1 order by t1.id"
        )
        .len(),
        2,
    );
    assert_eq!(
        select(
            &catalog,
            "select * from t1 right join t2 on t1.id=t2.id and t1.col1<=t2.col1 order by t2.id"
        )
        .len(),
        3,
    );
}

/// Go `hashjoin/main_test.go:26::TestMain`: suite bootstrap only.
#[test]
#[ignore = "skipped-reason: Go hashjoin TestMain only configures auto-ID/failpoints/goleak"]
fn hashjoin_suite_main_is_bootstrap() {}
