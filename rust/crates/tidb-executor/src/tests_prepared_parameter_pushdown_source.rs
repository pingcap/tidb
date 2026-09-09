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

//! Port of the runnable half of Go
//! `pkg/executor/prepared_test.go:116::TestParameterPushDown`: parameters of
//! a prepared SELECT are pushed into the access path / operators, and
//! re-executes with different values reuse the cached plan
//! (`@@last_plan_from_cache`) while returning value-correct rows. The
//! golden fixture is `pkg/executor/testdata/prepare_suite_in.json`
//! (`TestParameterPushDown`) over
//! `create table t (a int, b int, c int, key(a)); insert ... (1,1,1)..(6,6,6)`
//! (`pkg/executor/prepared_test.go:116-131`).
//!
//! The row-level test below pins the `select * from t where b>?` executes:
//! Go records `[2..6]` for `@x1 = 1` (cache `0`) and `[6]` for `@x5 = 5`
//! (cache `1`) — `pkg/executor/testdata/prepare_suite_out.json`,
//! `TestParameterPushDown` case 2. The other executable statement shapes are
//! covered through the public prepared-statement path in
//! `tidb-session::tests_prepared_plan_cache`.

use tidb_datatype::Datum;

use crate::{
    build_prepared_select_plan, run_create_table_on, run_insert_on, run_prepared_select_for_test,
    Catalog, PreparedPlanCacheEnvironment, StmtContext, DEFAULT_DATABASE,
};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn go_t_catalog() -> Catalog {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int, c int, key(a))", &mut catalog)
        .expect("t creates");
    run_insert_on(
        "insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6)",
        &mut catalog,
        &ctx(),
    )
    .expect("t rows insert");
    catalog
}

/// Go
/// `pkg/executor/prepared_test.go:116::TestParameterPushDown`, fixture case
/// `select * from t where b>?`: `@x1 = 1` returns rows a=2..6 (cache miss,
/// `FromCache = 0`), `@x5 = 5` returns only `6 6 6` (cache hit, `FromCache =
/// 1`) — the parameter reaches the scan predicate without invalidating the
/// cached physical tree
/// (`pkg/executor/testdata/prepare_suite_out.json`, the `select * from t where b>?` execute pair).
#[test]
fn parameter_predicate_pushes_into_the_scan_and_keeps_the_cached_plan() {
    let catalog = go_t_catalog();
    let ctx = ctx();
    let environment = PreparedPlanCacheEnvironment::default();
    let stmt = tidb_parser::parse("select * from t where b>?").expect("parses");
    let plan = std::sync::Arc::new(
        build_prepared_select_plan(&stmt, 1, &catalog, DEFAULT_DATABASE, &ctx)
            .expect("the parameter predicate is cacheable"),
    );

    let execution = plan
        .bind(
            &[Datum::Int(1)],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("binds");
    assert!(!execution.cache_hit(), "first execute misses the cache");
    let mut catalog = catalog;
    let (_, rows) =
        run_prepared_select_for_test(&execution, &catalog, DEFAULT_DATABASE, &ctx).expect("runs");
    let firsts: Vec<i64> = rows
        .iter()
        .map(|row| match &row[0] {
            Datum::Int(value) => *value,
            other => panic!("unexpected datum {other:?}"),
        })
        .collect();
    assert_eq!(
        firsts,
        vec![2, 3, 4, 5, 6],
        "b > 1 answers Go's recorded rows"
    );

    let execution = plan
        .bind(
            &[Datum::Int(5)],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("binds");
    assert!(execution.cache_hit(), "same-type re-execute hits the cache");
    let (_, rows) =
        run_prepared_select_for_test(&execution, &catalog, DEFAULT_DATABASE, &ctx).expect("runs");
    assert_eq!(
        rows,
        vec![vec![Datum::Int(6), Datum::Int(6), Datum::Int(6)]]
    );
}
