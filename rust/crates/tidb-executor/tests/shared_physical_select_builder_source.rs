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
    build_prepared_select_plan, run_create_table_on, run_insert_on, run_select_meta_on,
    run_select_meta_stmt_with_physical, Catalog, PreparedPlanCacheEnvironment, DEFAULT_DATABASE,
};

fn has_index_join_compare_filters(plan: &tidb_planner::physical::PhysicalPlan) -> bool {
    matches!(
        plan,
        tidb_planner::physical::PhysicalPlan::IndexJoin(join)
            if join.compare_filters.is_some()
    ) || plan.children().iter().any(has_index_join_compare_filters)
}

#[test]
fn fresh_and_cached_selects_call_one_physical_executor_entrypoint() {
    let driver = include_str!("../src/driver.rs");
    let access = include_str!("../src/driver/access.rs");
    let physical_builder = include_str!("../src/driver/physical_builder.rs");
    let ordinary_entry = driver
        .split_once("pub(crate) fn run_query_stmt(")
        .expect("ordinary query entrypoint")
        .1
        .split_once("/// Go `planner.optimize`")
        .expect("end of ordinary query entrypoint")
        .0;

    assert!(
        ordinary_entry.contains("physical_builder::execute_query("),
        "ordinary query execution must use the common physical executor entrypoint"
    );
    assert!(
        driver.contains("Some(physical) => physical_builder::execute_query(")
            && driver.contains("None => run_query_stmt("),
        "fresh and retained plans must converge on the same physical query executor"
    );
    assert!(
        driver.contains("run_query_meta_stmt_with_physical("),
        "fresh and retained plans must share the ordinary query executor seam"
    );
    assert!(
        !access.contains("pub fn run_prepared_select("),
        "the cache must not expose a private SELECT execution pipeline"
    );
    assert!(
        !physical_builder.contains("fn run_cached_select(")
            && !physical_builder.contains("fn execute_select("),
        "the physical executor must not expose a cache-specific runner"
    );
    assert!(
        !physical_builder.contains("fn direct_reader_shape(")
            && !physical_builder.contains("fn supports(plan: &PhysicalPlan)")
            && !physical_builder.contains("fn collect_reader_conditions("),
        "fresh and cached execution must not be gated by Rust-only plan subsets"
    );

    let session = include_str!("../../tidb-session/src/dispatch.rs");
    let server = include_str!("../../tidb-server/src/cluster_session_node/mod.rs");
    assert!(
        !session.contains("cache was invalidated") && !server.contains("cache was invalidated"),
        "plan-cache invalidation must remain a typed cache miss"
    );
}

#[test]
fn merge_join_keeps_the_planner_supplied_outer_default_row() {
    let physical = include_str!("../../tidb-planner/src/physical/mod.rs");
    let builder = include_str!("../src/driver/physical_builder.rs");
    assert!(
        physical.contains("pub struct PhysicalMergeJoin")
            && physical.contains("pub default_values: Vec<tidb_datatype::Datum>"),
        "PhysicalMergeJoin must retain BasePhysicalJoin.DefaultValues"
    );
    let merge_arm = builder
        .split_once("PhysicalPlan::MergeJoin(join) => {")
        .expect("physical merge-join builder arm")
        .1
        .split_once("PhysicalPlan::IndexJoin(join)")
        .expect("end of physical merge-join builder arm")
        .0;
    assert!(
        merge_arm.contains("set_default_values(join.default_values.clone())"),
        "Go buildMergeJoin passes the retained default row to its joiner"
    );
}

#[test]
fn index_join_compare_filters_intersect_on_go_one_column_range() {
    let mut catalog = Catalog::default();
    let ctx = tidb_executor::StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE ij_outer (a BIGINT, lo1 BIGINT, lo2 BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_create_table_on(
        "CREATE TABLE ij_inner (a BIGINT, b BIGINT, KEY ab(a,b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("INSERT INTO ij_outer VALUES (1,10,12)", &mut catalog, &ctx).unwrap();
    run_insert_on(
        "INSERT INTO ij_inner VALUES (1,9),(1,10),(1,11),(1,12),(1,13),(2,99)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let (_, rows) = run_select_meta_on(
        "SELECT /*+ TIDB_INLJ(i) */ i.b \
         FROM ij_outer o JOIN ij_inner i \
           ON i.a=o.a AND i.b>o.lo1 AND i.b>=o.lo2 \
         ORDER BY i.b",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(rows, vec![vec![Datum::Int(12)], vec![Datum::Int(13)]]);
}

#[test]
fn index_join_compare_filters_use_go_column_ranger() {
    let access = include_str!("../src/access_path.rs");
    let range_builder = access
        .split_once("fn probe_index_ranges(")
        .expect("the index-join per-row range builder")
        .1
        .split_once("/// The next probe's converted key tuple")
        .expect("the end of the per-row range builder")
        .0;
    assert!(
        range_builder.contains("ranger::ranger::build_column_range("),
        "Go BuildRangesByRow intersects all comparisons through BuildColumnRange"
    );
    assert!(
        !range_builder.contains("low.push(value.clone())")
            && !range_builder.contains("high.push(value.clone())"),
        "multiple comparisons target one key column, not multiple appended columns"
    );
}

#[test]
fn index_join_cuts_equality_probe_to_go_prefix_index_key() {
    let mut catalog = Catalog::default();
    let ctx = tidb_executor::StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE pij_outer (s VARCHAR(20))",
        &mut catalog,
    )
    .unwrap();
    run_create_table_on(
        "CREATE TABLE pij_inner (s VARCHAR(20), KEY s3(s(3)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("INSERT INTO pij_outer VALUES ('abcdef')", &mut catalog, &ctx).unwrap();
    run_insert_on(
        "INSERT INTO pij_inner VALUES ('abcdef'),('abcxyz'),('zzz')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let (_, rows) = run_select_meta_on(
        "SELECT /*+ TIDB_INLJ(i) */ i.s \
         FROM pij_outer o JOIN pij_inner i ON i.s=o.s",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![vec![Datum::new_collation_string(
            b"abcdef".to_vec(),
            tidb_datatype::Collation::Utf8Mb4Bin,
        )]]
    );
}

#[test]
fn index_join_deduplicates_go_prefix_lookup_keys_before_reading() {
    let mut catalog = Catalog::default();
    let ctx = tidb_executor::StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE pdij_outer (s VARCHAR(20))",
        &mut catalog,
    )
    .unwrap();
    run_create_table_on(
        "CREATE TABLE pdij_inner (s VARCHAR(20), KEY s3(s(3)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO pdij_outer VALUES ('abcdef'),('abcxyz')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO pdij_inner VALUES ('abcdef'),('abcxyz')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let (_, rows) = run_select_meta_on(
        "SELECT /*+ TIDB_INLJ(i) */ i.s \
         FROM pdij_outer o JOIN pdij_inner i ON i.s=o.s \
         ORDER BY i.s",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![
            vec![Datum::new_collation_string(
                b"abcdef".to_vec(),
                tidb_datatype::Collation::Utf8Mb4Bin,
            )],
            vec![Datum::new_collation_string(
                b"abcxyz".to_vec(),
                tidb_datatype::Collation::Utf8Mb4Bin,
            )],
        ],
        "Go cuts dLookUpKey before sortAndDedupLookUpContents, so one prefix range is read once",
    );
}

#[test]
fn cached_index_join_compare_filter_rebinds_its_parameter() {
    let mut catalog = Catalog::default();
    let ctx = tidb_executor::StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE cij_outer (a BIGINT, lo BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_create_table_on(
        "CREATE TABLE cij_inner (a BIGINT, b BIGINT, KEY ab(a,b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("INSERT INTO cij_outer VALUES (1,10)", &mut catalog, &ctx).unwrap();
    run_insert_on(
        "INSERT INTO cij_inner VALUES (1,10),(1,11),(1,12),(1,13)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let statement = tidb_parser::parse(
        "SELECT /*+ TIDB_INLJ(i) */ i.b \
         FROM cij_outer o JOIN cij_inner i \
           ON i.a=o.a AND i.b>o.lo+? \
         ORDER BY i.b",
    )
    .unwrap();
    let plan = Arc::new(
        build_prepared_select_plan(&statement, 1, &catalog, DEFAULT_DATABASE, &ctx)
            .expect("the parameterized index join is cacheable"),
    );
    let execute = |parameter: i64| {
        let execution = plan
            .bind(
                &[Datum::Int(parameter)],
                &catalog,
                DEFAULT_DATABASE,
                &ctx,
                &PreparedPlanCacheEnvironment::default(),
            )
            .expect("the retained index join rebuilds");
        execution
            .with_plan(|statement, physical| {
                assert!(
                    has_index_join_compare_filters(physical),
                    "the completed physical index path must retain Go CompareFilters"
                );
                let tidb_ast::Stmt::Query(query) = statement else {
                    panic!("retained SELECT owns a query statement")
                };
                let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
                    panic!("retained SELECT owns a SELECT query")
                };
                run_select_meta_stmt_with_physical(
                    select,
                    Some(physical),
                    &catalog,
                    DEFAULT_DATABASE,
                    &ctx,
                )
            })
            .expect("the cache generation is current")
            .unwrap()
            .1
    };

    assert_eq!(
        execute(0),
        vec![
            vec![Datum::Int(11)],
            vec![Datum::Int(12)],
            vec![Datum::Int(13)]
        ]
    );
    assert_eq!(execute(2), vec![vec![Datum::Int(13)]]);
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
    let cached = execution
        .with_plan(|statement, physical| {
            let tidb_ast::Stmt::Query(query) = statement else {
                panic!("retained SELECT owns a query statement")
            };
            let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
                panic!("retained SELECT owns a SELECT query")
            };
            run_select_meta_stmt_with_physical(
                select,
                Some(physical),
                &catalog,
                DEFAULT_DATABASE,
                &ctx,
            )
        })
        .expect("the generation is current")
        .unwrap();

    assert_eq!(ordinary, cached);
}
