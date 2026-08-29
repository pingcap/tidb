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

//! Ports of the deterministic `pkg/executor.part19` slice: Go test items
//! 1081–1140. The runnable tests use the executor's retained physical-plan
//! boundary and ordinary DML executors. Session account state, plan-replayer files, DDL history,
//! transactions, remote requests, and goroutine/failpoint observations stay
//! explicit parity gaps rather than being approximated at this crate boundary.

use std::sync::Arc;

use tidb_datatype::Datum;

use crate::{
    build_prepared_dml_plan, build_prepared_select_plan, run_create_table_on,
    run_delete_stmt_with_physical, run_insert_on, run_insert_stmt_with_physical,
    run_prepared_select_for_test, run_select_on, run_update_stmt_with_physical, Catalog,
    PreparedPlanCacheEnvironment, StmtContext, DEFAULT_DATABASE,
};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn prepared_catalog() -> Catalog {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE prepared_part19 (id INT PRIMARY KEY, v INT NOT NULL)",
        &mut catalog,
    )
    .expect("prepared_part19 creates");
    run_insert_on(
        "INSERT INTO prepared_part19 VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &ctx(),
    )
    .expect("prepared_part19 rows insert");
    catalog
}

fn prepared_select_plan(
    plan_sql: &str,
    parameter_count: usize,
    catalog: &Catalog,
) -> Arc<crate::PreparedSelectPlan> {
    let statement = tidb_parser::parse(plan_sql).expect("prepared statement parses");
    Arc::new(
        build_prepared_select_plan(
            &statement,
            parameter_count,
            catalog,
            DEFAULT_DATABASE,
            &ctx(),
        )
        .expect("prepared statement is cacheable"),
    )
}

fn cached_select_rows(
    plan: &Arc<crate::PreparedSelectPlan>,
    values: &[Datum],
    catalog: &mut Catalog,
) -> (bool, Vec<Vec<Datum>>) {
    let execution = plan
        .bind(
            values,
            catalog,
            DEFAULT_DATABASE,
            &ctx(),
            &PreparedPlanCacheEnvironment::default(),
        )
        .expect("prepared values bind");
    let cache_hit = execution.cache_hit();
    let (_, rows) = run_prepared_select_for_test(&execution, catalog, DEFAULT_DATABASE, &ctx())
        .expect("prepared statement runs");
    (cache_hit, rows)
}

fn execute_prepared_dml(sql: &str, values: &[Datum], catalog: &mut Catalog) -> (bool, u64) {
    let statement = tidb_parser::parse(sql).expect("prepared DML parses");
    let plan = Arc::new(
        build_prepared_dml_plan(&statement, values.len(), catalog, DEFAULT_DATABASE)
            .expect("prepared DML definition builds")
            .expect("prepared DML is cacheable"),
    );
    let execution = plan
        .bind(
            values,
            catalog,
            DEFAULT_DATABASE,
            &PreparedPlanCacheEnvironment::default(),
        )
        .expect("prepared DML physical root builds");
    let cache_hit = execution.cache_hit();
    let affected = execution
        .with_plan(|statement, physical| {
            let tidb_planner::physical::PhysicalPlan::Dml(_) = physical else {
                panic!("prepared DML cache owns a DML root")
            };
            let tidb_ast::Stmt::Dml(dml) = statement else {
                panic!("prepared DML cache owns a DML statement")
            };
            match dml.as_ref() {
                tidb_ast::DmlStmt::Insert(insert) => run_insert_stmt_with_physical(
                    insert,
                    catalog,
                    DEFAULT_DATABASE,
                    &ctx(),
                    Some(physical),
                )
                .map(|(affected, _)| affected),
                tidb_ast::DmlStmt::Update(update) => run_update_stmt_with_physical(
                    update,
                    catalog,
                    DEFAULT_DATABASE,
                    &ctx(),
                    Some(physical),
                ),
                tidb_ast::DmlStmt::Delete(delete) => run_delete_stmt_with_physical(
                    delete,
                    catalog,
                    DEFAULT_DATABASE,
                    &ctx(),
                    Some(physical),
                ),
                _ => panic!("prepared DML root owns INSERT, UPDATE, or DELETE"),
            }
        })
        .expect("the execution generation stays pinned")
        .expect("ordinary DML executor runs");
    (cache_hit, affected)
}

/// `pkg/executor/test/plancache/plan_cache_test.go:36::TestPointGetPreparedPlan`.
#[test]
fn point_get_prepared_plan_rebinds_values_and_reports_hits() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan("SELECT v FROM prepared_part19 WHERE id = ?", 1, &catalog);
    let (first_hit, first_rows) = cached_select_rows(&plan, &[Datum::Int(1)], &mut catalog);
    assert!(!first_hit);
    assert_eq!(first_rows, vec![vec![Datum::Int(10)]]);

    let (second_hit, second_rows) = cached_select_rows(&plan, &[Datum::Int(3)], &mut catalog);
    assert!(second_hit);
    assert_eq!(second_rows, vec![vec![Datum::Int(30)]]);
}

/// `plan_cache_test.go:263::TestPointUpdatePreparedPlan`.
#[test]
fn point_update_prepared_plan_reuses_the_ordinary_cached_update_root() {
    let mut catalog = prepared_catalog();
    let (cache_hit, changed) = execute_prepared_dml(
        "UPDATE prepared_part19 SET v = v + ? WHERE id = ?",
        &[Datum::Int(5), Datum::Int(2)],
        &mut catalog,
    );
    assert!(!cache_hit);
    assert_eq!(changed, 1);
    assert_eq!(
        run_select_on(
            "SELECT v FROM prepared_part19 WHERE id = 2",
            &catalog,
            &ctx(),
        )
        .expect("updated row reads"),
        vec![vec![Datum::Int(25)]]
    );
}

/// `plan_cache_test.go:452::TestPreparedPlanCachePlanSelectionRegressions`.
#[test]
fn prepared_plan_cache_selection_rebinds_a_range_without_replanning_the_shape() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan(
        "SELECT v FROM prepared_part19 WHERE id BETWEEN ? AND ? ORDER BY id",
        2,
        &catalog,
    );
    let (first_hit, first_rows) =
        cached_select_rows(&plan, &[Datum::Int(1), Datum::Int(2)], &mut catalog);
    assert!(!first_hit);
    assert_eq!(first_rows, vec![vec![Datum::Int(10)], vec![Datum::Int(20)]]);

    let (second_hit, second_rows) =
        cached_select_rows(&plan, &[Datum::Int(2), Datum::Int(3)], &mut catalog);
    assert!(second_hit);
    assert_eq!(
        second_rows,
        vec![vec![Datum::Int(20)], vec![Datum::Int(30)]]
    );
}

/// `plan_cache_test.go:574::TestPreparedPlanCacheOperators`.
#[test]
fn prepared_plan_cache_reuses_a_parameterized_operator_tree() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan(
        "SELECT v FROM prepared_part19 WHERE id > ? ORDER BY id",
        1,
        &catalog,
    );
    let (first_hit, first_rows) = cached_select_rows(&plan, &[Datum::Int(1)], &mut catalog);
    assert!(!first_hit);
    assert_eq!(first_rows, vec![vec![Datum::Int(20)], vec![Datum::Int(30)]]);

    let (second_hit, second_rows) = cached_select_rows(&plan, &[Datum::Int(2)], &mut catalog);
    assert!(second_hit);
    assert_eq!(second_rows, vec![vec![Datum::Int(30)]]);
}

/// `pkg/executor/test/seqtest/prepared_test.go:39::TestPrepared`.
#[test]
fn prepared_statement_select_reuses_the_executor_plan() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan(
        "SELECT id, v FROM prepared_part19 WHERE id = ?",
        1,
        &catalog,
    );
    let (first_hit, rows) = cached_select_rows(&plan, &[Datum::Int(1)], &mut catalog);
    assert!(!first_hit);
    assert_eq!(rows, vec![vec![Datum::Int(1), Datum::Int(10)]]);
    let (second_hit, rows) = cached_select_rows(&plan, &[Datum::Int(2)], &mut catalog);
    assert!(second_hit);
    assert_eq!(rows, vec![vec![Datum::Int(2), Datum::Int(20)]]);
}

/// `prepared_test.go:268::TestPreparedLimitOffset`.
#[test]
fn prepared_limit_offset_binds_integer_parameters() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan(
        "SELECT id FROM prepared_part19 ORDER BY id LIMIT ? OFFSET ?",
        2,
        &catalog,
    );
    let (hit, rows) = cached_select_rows(&plan, &[Datum::Int(1), Datum::Int(1)], &mut catalog);
    assert!(!hit);
    assert_eq!(rows, vec![vec![Datum::Int(2)]]);
}

/// `prepared_test.go:300::TestPrepareWithAggregation`.
#[test]
fn prepared_aggregation_rebinds_its_filter_parameter() {
    let mut catalog = prepared_catalog();
    let plan = prepared_select_plan(
        "SELECT SUM(v) FROM prepared_part19 WHERE id > ?",
        1,
        &catalog,
    );
    let (first_hit, rows) = cached_select_rows(&plan, &[Datum::Int(1)], &mut catalog);
    assert!(!first_hit);
    assert_eq!(
        rows,
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(50))]]
    );
    let (second_hit, rows) = cached_select_rows(&plan, &[Datum::Int(2)], &mut catalog);
    assert!(second_hit);
    assert_eq!(
        rows,
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(30))]]
    );
}

/// `prepared_test.go:328::TestPreparedInsert`.
#[test]
fn prepared_insert_writes_bound_values() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE prepared_insert_part19 (id VARCHAR(16) PRIMARY KEY, v INT)",
        &mut catalog,
    )
    .expect("prepared insert table creates");
    let (cache_hit, affected) = execute_prepared_dml(
        "INSERT INTO prepared_insert_part19 (id, v) VALUES (?, ?)",
        &[Datum::Bytes(b"k1".to_vec()), Datum::Int(42)],
        &mut catalog,
    );
    assert!(!cache_hit);
    assert_eq!(affected, 1);
    assert_eq!(
        run_select_on(
            "SELECT v FROM prepared_insert_part19 WHERE id = 'k1'",
            &catalog,
            &ctx(),
        )
        .expect("inserted row reads"),
        vec![vec![Datum::Int(42)]]
    );
}

/// `prepared_test.go:405::TestPreparedUpdate`.
#[test]
fn prepared_update_rebinds_assignment_and_handle_parameters() {
    let mut catalog = prepared_catalog();
    let (cache_hit, changed) = execute_prepared_dml(
        "UPDATE prepared_part19 SET v = v + ? WHERE id = ?",
        &[Datum::Int(7), Datum::Int(3)],
        &mut catalog,
    );
    assert!(!cache_hit);
    assert_eq!(changed, 1);
    assert_eq!(
        run_select_on(
            "SELECT v FROM prepared_part19 WHERE id = 3",
            &catalog,
            &ctx(),
        )
        .expect("updated row reads"),
        vec![vec![Datum::Int(37)]]
    );
}

/// `prepared_test.go:478::TestPreparedDelete`.
#[test]
fn prepared_delete_rebinds_the_handle_parameter() {
    let mut catalog = prepared_catalog();
    let (cache_hit, changed) = execute_prepared_dml(
        "DELETE FROM prepared_part19 WHERE id = ?",
        &[Datum::Int(2)],
        &mut catalog,
    );
    assert!(!cache_hit);
    assert_eq!(changed, 1);
    assert!(run_select_on(
        "SELECT v FROM prepared_part19 WHERE id = 2",
        &catalog,
        &ctx(),
    )
    .expect("deleted row reads")
    .is_empty());
}
