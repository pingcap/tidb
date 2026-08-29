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
//! `pkg/executor/prepared_test.go:36::TestPlanCacheWithDifferentVariableTypes`:
//! one prepared SELECT executed repeatedly with different parameter VALUES
//! and TYPES. Go's plan cache keys physical plans by parameter TYPE
//! (`PlanCacheKey.ParamTypes`, pkg/planner/core/plan_cache_utils.go:546,
//! hashed by `newPlanCacheKeyWithMatchedBinding` :318), so same-type
//! re-executes hit the cache (`@@last_plan_from_cache = 1`) while a type
//! change misses. This tier mirrors that keying with
//! `PreparedParameterType` (`driver/access.rs`), and `bind` reports the
//! hit/miss through [`crate::PreparedSelectExecution::cache_hit`], the local
//! counterpart of Go's `@@last_plan_from_cache`.
//!
//! The golden fixture is `pkg/executor/testdata/prepare_suite_in.json` /
//! `prepare_suite_out.json` (`TestPlanCacheWithDifferentVariableTypes`,
//! case `select a from t1 where t1.b = ?`). The rows below are that case's
//! executes, with Go's `Result` and `LastPlanUseCache` columns pinned.
//! FROM-less parameter projections and decimal casts deliberately remain
//! ordinary, uncached `TableDual` plans, matching Go's physical cache
//! refusal for a parameterized `PhysicalTableDual`.

use tidb_datatype::{Collation, Datum, StringDatum};

use crate::{
    build_prepared_select_plan, run_create_table_on, run_insert_on, run_prepared_select_for_test,
    Catalog, PreparedPlanCacheEnvironment, StmtContext, DEFAULT_DATABASE,
};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn go_t1_t2_catalog() -> Catalog {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t1(a varchar(20), b int, c float, key(b, a))",
        &mut catalog,
    )
    .expect("t1 creates");
    run_insert_on(
        "insert into t1 values('1',1,1.1),('2',2,222),('3',3,333)",
        &mut catalog,
        &ctx(),
    )
    .expect("t1 rows insert");
    run_create_table_on(
        "create table t2(a varchar(20), b int, c float, key(b, a))",
        &mut catalog,
    )
    .expect("t2 creates");
    run_insert_on(
        "insert into t2 values('3',3,3.3),('2',2,222),('3',3,333)",
        &mut catalog,
        &ctx(),
    )
    .expect("t2 rows insert");
    catalog
}

/// Go
/// `pkg/executor/prepared_test.go:36::TestPlanCacheWithDifferentVariableTypes`,
/// fixture case 1 (`prepare stmt from "select a from t1 where t1.b = ?"`),
/// Int-typed executes. Go's recorded rows
/// (`pkg/executor/testdata/prepare_suite_out.json`):
/// `@v1 = 3` -> result `3`, `last_plan_from_cache = 0`;
/// `@v1 = 2` -> result `2`, `last_plan_from_cache = 1`;
/// `@v1 = -200` -> empty result, `last_plan_from_cache = 1` (same Int type
/// reuses the cached plan, so the second distinct Int value is already a
/// hit). The third Int execute is followed by a repeat of `-200` here to pin
/// that the single Int entry keeps serving further same-type executes.
#[test]
fn plan_cache_keyed_by_int_parameter_type_serves_each_value() {
    let catalog = go_t1_t2_catalog();
    let ctx = ctx();
    let environment = PreparedPlanCacheEnvironment::default();
    let stmt = tidb_parser::parse("select a from t1 where t1.b = ?").expect("parses");
    let plan = std::sync::Arc::new(
        build_prepared_select_plan(&stmt, 1, &catalog, DEFAULT_DATABASE, &ctx)
            .expect("the point shape is cacheable"),
    );

    let execution = plan
        .bind(
            &[Datum::Int(3)],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("Int bind plans");
    assert!(
        !execution.cache_hit(),
        "first Int execute must miss the cache"
    );
    let mut catalog = catalog;
    let (_, rows) =
        run_prepared_select_for_test(&execution, &catalog, DEFAULT_DATABASE, &ctx).expect("runs");
    assert_eq!(
        rows,
        vec![vec![Datum::String(StringDatum::new(
            "3",
            Collation::Utf8Mb4Bin
        ))]]
    );

    let execution = plan
        .bind(
            &[Datum::Int(2)],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("Int bind plans");
    assert!(
        execution.cache_hit(),
        "second Int execute must hit the cache"
    );
    let (_, rows) =
        run_prepared_select_for_test(&execution, &catalog, DEFAULT_DATABASE, &ctx).expect("runs");
    assert_eq!(
        rows,
        vec![vec![Datum::String(StringDatum::new(
            "2",
            Collation::Utf8Mb4Bin
        ))]]
    );

    let execution = plan
        .bind(
            &[Datum::Int(-200)],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("Int bind plans");
    assert!(execution.cache_hit(), "third Int execute stays a cache hit");
    let (_, rows) =
        run_prepared_select_for_test(&execution, &catalog, DEFAULT_DATABASE, &ctx).expect("runs");
    assert!(rows.is_empty(), "b = -200 matches no row, got {rows:?}");
}

/// Go's same fixture, schema-stability half: Go re-prepares nothing but
/// requires a DDL between executes to invalidate the cached plan (the
/// `schema_version` component of `PlanCacheKey`,
/// pkg/planner/core/plan_cache_utils.go:314). This tier's `bind` compares
/// `catalog.metadata_version()` the same way (`driver/access.rs`), so a
/// catalog change must turn the next same-type execute back into a miss.
#[test]
fn plan_cache_entry_invalidates_when_the_catalog_version_moves() {
    let mut catalog = go_t1_t2_catalog();
    let ctx = ctx();
    let environment = PreparedPlanCacheEnvironment::default();
    let stmt = tidb_parser::parse("select a from t1 where t1.b = ?").expect("parses");
    let plan = std::sync::Arc::new(
        build_prepared_select_plan(&stmt, 1, &catalog, DEFAULT_DATABASE, &ctx)
            .expect("the point shape is cacheable"),
    );

    let execution = plan
        .bind(
            &[Datum::Int(3)],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("Int bind plans");
    assert!(!execution.cache_hit());
    let (_, rows) =
        run_prepared_select_for_test(&execution, &catalog, DEFAULT_DATABASE, &ctx).expect("runs");
    assert_eq!(rows.len(), 1);
    drop(execution);

    // Any catalog mutation bumps the metadata version, standing in for Go's
    // `reload`/DDL invalidation path.
    run_create_table_on("create table t3 (a int)", &mut catalog).expect("t3 creates");

    let execution = plan
        .bind(
            &[Datum::Int(3)],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("Int bind plans");
    assert!(
        !execution.cache_hit(),
        "after the schema version moved, the same-type execute must re-plan"
    );
}

/// Go
/// `pkg/executor/prepared_test.go:36::TestPlanCacheWithDifferentVariableTypes`,
/// fixture case 1, String-typed execute (`@v1 = "abc"`): it receives a
/// separate typed physical entry, casts to the integer comparison domain, and
/// returns Go's recorded empty result with a cache miss.
#[test]
fn plan_cache_keys_string_typed_parameters_as_their_own_entry() {
    let catalog = go_t1_t2_catalog();
    let ctx = ctx();
    let environment = PreparedPlanCacheEnvironment::default();
    let stmt = tidb_parser::parse("select a from t1 where t1.b = ?").expect("parses");
    let plan = std::sync::Arc::new(
        build_prepared_select_plan(&stmt, 1, &catalog, DEFAULT_DATABASE, &ctx)
            .expect("the statement is cacheable"),
    );
    let execution = plan
        .bind(
            &[Datum::String(StringDatum::new(
                "abc",
                Collation::Utf8Mb4Bin,
            ))],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("String bind plans");
    assert!(!execution.cache_hit());
    let (_, rows) =
        run_prepared_select_for_test(&execution, &catalog, DEFAULT_DATABASE, &ctx).expect("runs");
    assert!(rows.is_empty());
}

/// Go
/// `pkg/executor/prepared_test.go:36::TestPlanCacheWithDifferentVariableTypes`,
/// fixture case 2 (`select t1.c, t2.c from t1 join t2 on t1.b = t2.b and
/// t1.a = t2.a where t1.b = ?`, recorded results `''`/`222 222`/`''` and
/// cache `0`/`1`/`0`): the retained general physical tree includes the JOIN
/// and recursively rebuilds its parameterized access ranges.
#[test]
fn plan_cache_serves_a_parameterized_join() {
    let catalog = go_t1_t2_catalog();
    let ctx = ctx();
    let environment = PreparedPlanCacheEnvironment::default();
    let stmt = tidb_parser::parse(
        "select t1.c, t2.c from t1 join t2 on t1.b = t2.b and t1.a = t2.a where t1.b = ?",
    )
    .expect("parses");
    let plan = std::sync::Arc::new(
        build_prepared_select_plan(&stmt, 1, &catalog, DEFAULT_DATABASE, &ctx)
            .expect("the join is cacheable"),
    );

    let first = plan
        .bind(
            &[Datum::Int(1)],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("first bind");
    assert!(!first.cache_hit());
    let (_, rows) =
        run_prepared_select_for_test(&first, &catalog, DEFAULT_DATABASE, &ctx).expect("runs");
    assert!(rows.is_empty());

    let second = plan
        .bind(
            &[Datum::Int(2)],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("second bind");
    assert!(second.cache_hit());
    let (_, rows) =
        run_prepared_select_for_test(&second, &catalog, DEFAULT_DATABASE, &ctx).expect("runs");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![Datum::Float32(222.0), Datum::Float32(222.0)]);

    let string = plan
        .bind(
            &[Datum::String(StringDatum::new(
                "abc",
                Collation::Utf8Mb4Bin,
            ))],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &environment,
        )
        .expect("String bind");
    assert!(!string.cache_hit());
    let (_, rows) =
        run_prepared_select_for_test(&string, &catalog, DEFAULT_DATABASE, &ctx).expect("runs");
    assert!(rows.is_empty());
}
