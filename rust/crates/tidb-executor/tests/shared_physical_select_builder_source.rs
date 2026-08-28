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

//! Regression guard for Go's single `executorBuilder.build` execution path.

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_executor::{
    build_prepared_select_plan, run_create_table_on, run_insert_on, run_prepared_select,
    run_select_meta_on, Catalog, PreparedPlanCacheEnvironment, DEFAULT_DATABASE,
};

#[test]
fn fresh_and_cached_selects_call_one_physical_executor_entrypoint() {
    let driver = include_str!("../src/driver.rs");
    let access = include_str!("../src/driver/access.rs");
    let physical_builder = include_str!("../src/driver/physical_builder.rs");
    let ordinary_entry = driver
        .split_once("fn run_select_stmt(")
        .expect("ordinary SELECT entrypoint")
        .1
        .split_once("/// [`run_select_stmt`]")
        .expect("end of ordinary SELECT entrypoint")
        .0;

    assert!(
        ordinary_entry.contains("physical_builder::execute_select("),
        "ordinary SELECT execution must use the common physical executor entrypoint"
    );
    assert!(
        !ordinary_entry.contains("run_select_traced("),
        "ordinary SELECT execution must not fall back to the legacy AST executor builder"
    );
    assert!(
        access.contains("physical_builder::execute_select("),
        "prepared cache execution must use the common physical executor entrypoint"
    );
    assert!(
        !physical_builder.contains("fn run_cached_select("),
        "the physical executor must not expose a cache-specific runner"
    );
    assert!(
        !physical_builder.contains("fn direct_reader_shape(")
            && !physical_builder.contains("fn supports(plan: &PhysicalPlan)"),
        "fresh and cached execution must not be gated by Rust-only plan subsets"
    );
}

#[test]
fn fresh_and_cached_physical_execution_returns_the_same_rows_and_metadata() {
    let mut catalog = Catalog::default();
    let ctx = tidb_executor::StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE shared_physical_builder (\
            id BIGINT PRIMARY KEY, value BIGINT NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO shared_physical_builder VALUES (1,10),(2,20),(3,30)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let ordinary = run_select_meta_on(
        "SELECT value FROM shared_physical_builder \
         WHERE id BETWEEN 1 AND 2 ORDER BY id",
        &catalog,
        &ctx,
    )
    .unwrap();
    let (_, count_rows) = run_select_meta_on(
        "SELECT COUNT(*) FROM shared_physical_builder",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(count_rows, vec![vec![Datum::Int(3)]]);
    let statement = tidb_parser::parse(
        "SELECT value FROM shared_physical_builder \
         WHERE id BETWEEN ? AND ? ORDER BY id",
    )
    .unwrap();
    let plan = Arc::new(
        build_prepared_select_plan(&statement, 2, &catalog, DEFAULT_DATABASE, &ctx)
            .expect("the range statement is cacheable"),
    );
    let execution = plan
        .bind(
            &[Datum::Int(1), Datum::Int(2)],
            &catalog,
            DEFAULT_DATABASE,
            &ctx,
            &PreparedPlanCacheEnvironment::default(),
        )
        .expect("the prepared statement builds the same physical tree");
    let cached = run_prepared_select(&execution, &mut catalog, DEFAULT_DATABASE, &ctx)
        .unwrap()
        .expect("the schema is unchanged");

    assert_eq!(ordinary, cached);
}
