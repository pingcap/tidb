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
//! The row-level halves below pin the `select * from t where b>?` executes:
//! Go records `[2..6]` for `@x1 = 1` (cache `0`) and `[6]` for `@x5 = 5`
//! (cache `1`) — `pkg/executor/testdata/prepare_suite_out.json`,
//! `TestParameterPushDown` case 2. Every plan-text column and the remaining
//! statement shapes are recorded as `#[ignore]` gap tests.

use tidb_datatype::Datum;

use crate::{
    build_prepared_select_plan, run_create_table_on, run_insert_on, run_prepared_select, Catalog,
    PreparedPlanCacheEnvironment, StmtContext, DEFAULT_DATABASE,
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
        .bind(&[Datum::Int(1)], &catalog, DEFAULT_DATABASE, &ctx, &environment)
        .expect("binds");
    assert!(!execution.cache_hit(), "first execute misses the cache");
    let mut catalog = catalog;
    let (_, rows) = run_prepared_select(&execution, &mut catalog, DEFAULT_DATABASE, &ctx)
        .expect("runs")
        .expect("schema unchanged");
    let firsts: Vec<i64> = rows
        .iter()
        .map(|row| match &row[0] {
            Datum::Int(value) => *value,
            other => panic!("unexpected datum {other:?}"),
        })
        .collect();
    assert_eq!(firsts, vec![2, 3, 4, 5, 6], "b > 1 answers Go's recorded rows");

    let execution = plan
        .bind(&[Datum::Int(5)], &catalog, DEFAULT_DATABASE, &ctx, &environment)
        .expect("binds");
    assert!(execution.cache_hit(), "same-type re-execute hits the cache");
    let (_, rows) = run_prepared_select(&execution, &mut catalog, DEFAULT_DATABASE, &ctx)
        .expect("runs")
        .expect("schema unchanged");
    assert_eq!(rows, vec![vec![Datum::Int(6), Datum::Int(6), Datum::Int(6)]]);
}

/// Go's fixture case `select a from t use index(a) where a+0>?` (IndexReader
/// + pushed-down Selection with parameters, results `[2..6]` / `[6]`): the
/// parameter sits INSIDE an arithmetic expression, which this tier evaluates
/// by constant folding — not ported (`tidb-expr/src/constant.rs:145`) — so
/// the execute cannot bind.
#[test]
#[ignore = "go-parity-gap: a parameter inside an expression (a+0>?) needs deferred/parameter constant evaluation (tidb-expr/src/constant.rs:145); Go pins results [2..6]/[6] for @x1/@x5 (prepare_suite_out.json, the `a+0>?` execute pair)"]
fn parameter_inside_an_expression_pushes_down_as_a_selection() {}

/// Go's fixture case `select * from t use index(a) where a+0>? and b>?`
/// (IndexLookUp + pushed Selections on both leaves, results `[2..6]` /
/// `[6]`): same expression-parameter blocker on the `a+0` side.
#[test]
#[ignore = "go-parity-gap: the a+0>? conjunct needs deferred/parameter constant evaluation (tidb-expr/src/constant.rs:145); Go pins results [2..6]/[6] for @x1,@x1/@x5,@x5 (prepare_suite_out.json, the `a+0>? and b>?` execute pair)"]
fn parameter_pair_pushes_to_both_index_lookup_leaves() {}

/// Go's fixture case `select * from t limit ?` (pushed-down Limit with
/// parameters, `@x10`/`@x20` both answer all six rows): the tier's `bind`
/// collects LIMIT parameter orders, but execution still evaluates the bound
/// AST's LIMIT node, which carries a parameter marker rather than an integer
/// literal (`crate::driver::eval_limit_bound`,
/// rust/crates/tidb-executor/src/driver.rs:3475).
#[test]
#[ignore = "go-parity-gap: executing a parameterized LIMIT fails at eval_limit_bound ('LIMIT bound must be an integer literal', driver.rs:3477); Go pins the 6-row answer for @x10/@x20 (prepare_suite_out.json, the `limit ?` execute pair)"]
fn parameterized_limit_answers_each_bound_value() {}

/// Go's fixture case `select * from t order by b limit ?` (pushed-down TopN
/// with parameters, results `[1 1 1]` for `@x1` / `[1..5]` for `@x5`): same
/// parameterized-LIMIT blocker.
#[test]
#[ignore = "go-parity-gap: the parameterized TopN LIMIT fails at eval_limit_bound (driver.rs:3477); Go pins results [1 1 1]/[1..5] for @x1/@x5 (prepare_suite_out.json, the `order by b limit ?` execute pair)"]
fn parameterized_top_n_limit_answers_each_bound_value() {}

/// Go's fixture case `select b, sum(c+?) from t group by b` (pushed-down Agg
/// with parameters, results `1 2..6 7` for `@x1` / `1 6..6 11` for `@x5`):
/// the parameter sits inside the aggregate's argument expression, so the
/// same constant-evaluation blocker applies.
#[test]
#[ignore = "go-parity-gap: a parameter inside sum(c+?) needs deferred/parameter constant evaluation (tidb-expr/src/constant.rs:145); Go pins results '1 2',...,'6 7' and '1 6',...,'6 11' (prepare_suite_out.json, the `sum(c+?)` execute pair)"]
fn parameter_inside_an_aggregate_argument_pushes_down() {}

/// Go's recorded `Plan` columns for this test (IndexReader/TableReader/
/// IndexLookUp/Limit/TopN trees with `gt(plus(test.t.a, 0), 1)`-style pushed
/// conditions): Go pins each physical tree in explain text. This tier has no
/// prepared-statement EXPLAIN; `explain_select_stmt` plans concrete
/// statements only.
#[test]
#[ignore = "go-parity-gap: EXPLAIN for a prepared statement's bound execution is not available (crate::explain::explain_select_stmt takes a concrete SelectStmt); Go's golden plan trees (prepare_suite_out.json, TestParameterPushDown) stay unrecorded"]
fn parameter_pushdown_golden_plan_text_is_recorded() {}
