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

use tidb_ast::Stmt;
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::{SessionTimeZone, ZonedNoColumns};
use tidb_planner::expression_rewriter::ColumnIdAllocator;
use tidb_planner::logical::LogicalPlan;
use tidb_planner::plan_base::PlanIdAllocator;
use tidb_planner::plan_builder::PlanBuilder;
use tidb_planner::plan_builder::catalog::{SourceColumn, SourceTable, TableSource};

/// The catalog of `TestLogicalPlanTypeRegression`: t1/t2/t3 as created by the
/// test's `CREATE TABLE`s (`logical_plan_builder_test.go:63-68`).
struct TypeRegressionCatalog {
    tables: Vec<SourceTable>,
}

impl TableSource for TypeRegressionCatalog {
    fn current_database(&self) -> &str {
        "test"
    }

    fn find_table(&self, db_name: &str, table_name: &str) -> Option<&SourceTable> {
        self.tables.iter().find(|table| {
            table.db_name.eq_ignore_ascii_case(db_name)
                && table.table_name.eq_ignore_ascii_case(table_name)
        })
    }

    fn database_exists(&self, db_name: &str) -> bool {
        self.tables
            .iter()
            .any(|table| table.db_name.eq_ignore_ascii_case(db_name))
    }
}

fn integer_column(code: FieldTypeCode, unsigned: bool) -> SourceColumn {
    let mut ret_type = FieldType::new(code);
    ret_type.set_flen(20);
    ret_type.set_decimal(0);
    if unsigned {
        ret_type.add_flags(FieldTypeFlags::UNSIGNED);
    }
    SourceColumn {
        id: 1,
        name: "c1".to_owned(),
        is_primary_key: false,
        offset: 0,
        ret_type,
        is_public: true,
        is_hidden: false,
        is_generated: false,
        is_virtual_generated: false,
        generated_expr: None,
    }
}

/// `CREATE TABLE t1 (c1 int)` / `CREATE TABLE t2 (c1 int unsigned)` /
/// `CREATE TABLE t3 (c1 bigint unsigned)` (`logical_plan_builder_test.go:64-66`).
fn type_regression_catalog() -> TypeRegressionCatalog {
    let mut t1 = SourceTable::default();
    t1.table_id = 101;
    t1.db_name = "test".to_owned();
    t1.table_name = "t1".to_owned();
    t1.physical_table_id = 101;
    t1.columns = vec![integer_column(FieldTypeCode::Long, false)];
    let mut t2 = SourceTable::default();
    t2.table_id = 102;
    t2.db_name = "test".to_owned();
    t2.table_name = "t2".to_owned();
    t2.physical_table_id = 102;
    t2.columns = vec![integer_column(FieldTypeCode::Long, true)];
    let mut t3 = SourceTable::default();
    t3.table_id = 103;
    t3.db_name = "test".to_owned();
    t3.table_name = "t3".to_owned();
    t3.physical_table_id = 103;
    t3.columns = vec![integer_column(FieldTypeCode::LongLong, true)];
    TypeRegressionCatalog {
        tables: vec![t1, t2, t3],
    }
}

/// Builds one query against the regression catalog and returns the field type
/// of the first column of the produced plan's schema — the Rust-side answer to
/// Go's `rs.Fields()[0].Column.FieldType.GetType()`.
fn union_first_output_type(sql: &str) -> FieldType {
    let catalog = type_regression_catalog();
    let ctx = ZonedNoColumns(SessionTimeZone::utc());
    let plan_ids = PlanIdAllocator::default();
    let column_ids = ColumnIdAllocator::new();
    let mut builder = PlanBuilder::new(
        &catalog,
        &ctx,
        &plan_ids,
        &column_ids,
        SessionTimeZone::utc(),
    );
    let query = match tidb_parser::parse(sql).expect("the union SQL parses") {
        Stmt::Query(query) => query.into_inner(),
        other => panic!("expected a query statement, got {other:?}"),
    };
    let plan = builder
        .build_query_stmt(&query, false)
        .unwrap_or_else(|error| panic!("{sql} should build: {}", error.message()));
    match &plan {
        LogicalPlan::UnionAll(_) => {}
        other => panic!("{sql} should build a UnionAll, got {}", other.tp()),
    }
    plan.schema()
        .expect("a UnionAll produces its own schema")
        .columns
        .first()
        .map(|column| {
            column
                .ret_type
                .clone()
                .expect("a union output column carries its result type")
        })
        .expect("the union has one output column")
}

/// GO PORT of `pkg/planner/core/casetest/logicalplan/
/// logical_plan_builder_test.go:57 TestLogicalPlanTypeRegression`,
/// issue:52472 arm (lines 74-79).
///
/// Re-derived contract: uniting an INT branch with an INT UNSIGNED branch must
/// report `mysql.TypeLonglong` (8). `unionJoinFieldType`
/// (`pkg/planner/core/logical_plan_builder.go:2001`) folds both into
/// `AggFieldType` (`pkg/types/field_type.go:63`): `mergeFieldType(Long, Long)`
/// keeps TypeLong, the branches differ in sign and one is exactly TypeLong, so
/// AggFieldType's mixed-sign integral promotion bumps it to TypeLonglong.
#[test]
fn logical_plan_type_regression_union_int_with_unsigned_int_promotes_to_longlong() {
    let field_type = union_first_output_type("SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t2");
    assert_eq!(field_type.code(), FieldTypeCode::LongLong);
}

/// GO PORT of `pkg/planner/core/casetest/logicalplan/
/// logical_plan_builder_test.go:57 TestLogicalPlanTypeRegression`,
/// issue:52472 second arm (lines 80-85).
///
/// Re-derived contract: `SELECT 0` gives a signed TypeLonglong constant
/// (`types.DefaultTypeForValue`, `pkg/expression/util.go`), and uniting it
/// with a BIGINT UNSIGNED column must report `mysql.TypeNewDecimal`:
/// mixed-sign TypeLonglong with an unsigned TypeLonglong member bumps to
/// TypeNewDecimal in `AggFieldType` (`pkg/types/field_type.go:63`) before
/// `unionJoinFieldType` applies the decimal-sign rule. The literal branch
/// still flows through the builder here: Go's `SELECT 0` builds a projection
/// above `buildTableDual` and `build_projection4_union` reads the child's
/// schema column, which carries the constant's `DefaultTypeForValue` type.
#[test]
fn logical_plan_type_regression_union_signed_literal_with_unsigned_bigint_promotes_to_decimal() {
    let field_type = union_first_output_type("SELECT 0 UNION ALL SELECT c1 FROM t3");
    assert_eq!(field_type.code(), FieldTypeCode::NewDecimal);
}
