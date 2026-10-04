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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use tidb_ast::{QueryStmt, Stmt};
use tidb_datatype::FieldTypeCode;
use tidb_expr::{SessionTimeZone, ZonedNoColumns};
use tidb_planner::expression_rewriter::ColumnIdAllocator;
use tidb_planner::logical::LogicalPlan;
use tidb_planner::plan_base::PlanIdAllocator;
use tidb_planner::plan_builder::PlanBuilder;
use tidb_planner::plan_builder::catalog::{SourceColumn, SourceTable, TableSource};

/// Go `point_get_plan_test.go:274`: `create table t (c1 int primary key, c2 int)`.
struct PointGetCatalog {
    t: SourceTable,
}

impl TableSource for PointGetCatalog {
    fn current_database(&self) -> &str {
        "test"
    }

    fn find_table(&self, db_name: &str, table_name: &str) -> Option<&SourceTable> {
        if db_name.eq_ignore_ascii_case("test")
            && table_name.eq_ignore_ascii_case(&self.t.table_name)
        {
            Some(&self.t)
        } else {
            None
        }
    }

    fn database_exists(&self, db_name: &str) -> bool {
        db_name.eq_ignore_ascii_case("test")
    }
}

fn point_get_column(offset: usize, name: &str, primary: bool) -> SourceColumn {
    let mut ret_type = tidb_datatype::FieldType::new(FieldTypeCode::Long);
    ret_type.set_flen(11);
    ret_type.set_decimal(0);
    SourceColumn {
        id: (offset + 1) as i64,
        name: name.to_owned(),
        is_primary_key: primary,
        offset,
        ret_type,
        is_public: true,
        is_hidden: false,
        is_generated: false,
        is_virtual_generated: false,
        generated_expr: None,
    }
}

/// `t (c1 int primary key, c2 int)` as declared at `point_get_plan_test.go:273`.
fn point_get_catalog() -> PointGetCatalog {
    let mut t = SourceTable::default();
    t.table_id = 201;
    t.db_name = "test".to_owned();
    t.table_name = "t".to_owned();
    t.physical_table_id = 201;
    t.columns = vec![
        point_get_column(0, "c1", true),
        point_get_column(1, "c2", false),
    ];
    PointGetCatalog { t }
}

/// Go `point_get_plan_test.go:275`.
const POINT_GET_QUERY: &str = "select c2 from t where c1 = 1";

fn parse_point_get_query(sql: &str) -> tidb_ast::SelectStmt {
    match tidb_parser::parse(sql).expect("the point-get SQL parses") {
        Stmt::Query(query) => match query.into_inner() {
            QueryStmt::Select(select) => *select,
            other => panic!("expected a SELECT, got {other:?}"),
        },
        other => panic!("expected a SELECT, got {other:?}"),
    }
}

/// Every plan id in the subtree, pre-order.
fn collect_plan_ids(plan: &LogicalPlan, out: &mut Vec<i32>) {
    out.push(plan.id());
    for child in plan.base().children() {
        collect_plan_ids(child, out);
    }
}

/// Builds `select c2 from t where c1 = 1` and returns its sorted plan-id set.
///
/// One call == one Go optimize pass: the caller decides whether the allocator
/// is fresh (`optimize.go:904` reset) or shared with earlier passes.
fn build_once(catalog: &PointGetCatalog) -> Vec<i32> {
    let ctx = ZonedNoColumns(SessionTimeZone::utc());
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let mut builder = PlanBuilder::new(
        catalog,
        &ctx,
        &plan_ids,
        &column_ids,
        SessionTimeZone::utc(),
    );
    let select = parse_point_get_query(POINT_GET_QUERY);
    let (plan, _) = builder
        .build_select(&select)
        .expect("the point-get query builds");
    let mut ids = Vec::new();
    collect_plan_ids(&plan, &mut ids);
    ids.sort_unstable();
    ids
}

/// Rust side of `pkg/planner/core/tests/pointget/point_get_plan_test.go:277
/// TestPointGetId` — the plan-id counter restarts from 1 for every top-level
/// statement build, never inside one.
#[test]
fn point_get_id_fresh_statement_build_reallocates_ids_from_one() {
    let catalog = point_get_catalog();

    // Two passes, each mirroring one Optimize: `buildLogicalPlan` stores 0 into
    // PlanID before Build (optimize.go:904) and `TryFastPlan` repeats it before
    // the point-get conversion (point_get_plan.go:97). A fresh allocator is
    // this crate's model of that reset.
    let mut first = build_once(&catalog);
    let second = build_once(&catalog);

    assert!(
        !first.is_empty(),
        "the built statement carries at least one plan id"
    );
    assert_eq!(first.first(), Some(&1), "ids start at 1 after the reset");
    assert_eq!(first, second, "each pass reallocates the identical id set");
    let len = first.len();
    first.dedup();
    assert_eq!(first.len(), len, "ids are unique inside one statement");

    // Contrast arm: WITHOUT the reset nothing restarts — a shared counter keeps
    // counting past the first pass. This is the exact failure mode the Go test
    // guards against; pass two would break if reset were dropped upstream.
    let ctx = ZonedNoColumns(SessionTimeZone::utc());
    let shared_plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let select = parse_point_get_query(POINT_GET_QUERY);
    let build_shared = |catalog: &'_ PointGetCatalog| {
        let mut builder = PlanBuilder::new(
            catalog,
            &ctx,
            &shared_plan_ids,
            &column_ids,
            SessionTimeZone::utc(),
        );
        builder
            .build_select(&select)
            .expect("the point-get query builds")
            .0
            .id()
    };
    let _first_root = build_shared(&catalog);
    let second_root_shared = build_shared(&catalog);
    assert!(
        second_root_shared > 1,
        "a shared allocator keeps allocating: root of pass two is id {second_root_shared}, not 1"
    );
}
