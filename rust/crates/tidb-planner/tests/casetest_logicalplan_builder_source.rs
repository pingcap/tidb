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

//! Ports for `pkg/planner/core/casetest/logicalplan` (`pkg/planner.part6`
//! items 302–303 on `origin/master`; family bootstrap is
//! `logicalplan/main_test.go:24 TestMain`, skipped-reason in the receipt).
//!
//! `TestLogicalPlanTypeRegression` (`logical_plan_builder_test.go:57`) pins
//! three behaviors; two of them are exactly what this crate's transcreated
//! `buildSetOpr` type-merging pipeline owns, so they run as real assertions:
//!
//! 1. `SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t2` over `c1 INT` /
//!    `c1 INT UNSIGNED` reports `mysql.TypeLonglong`
//!    ("union int and unsigned int will be promoted to long long").
//! 2. `SELECT 0 UNION ALL SELECT c1 FROM t3` over `c1 BIGINT UNSIGNED`
//!    reports `mysql.TypeNewDecimal` ("union int (even literal) and unsigned
//!    bigint will be promoted to decimal").
//!
//! In Go both statements run through `PlanBuilder.buildProjection4Union`
//! (`pkg/planner/core/logical_plan_builder.go:2053`) whose per-column result
//! type is `unionJoinFieldType` (:2001) = `types.AggFieldType`
//! (`pkg/types/field_type.go:63`, mixed-sign integral promotion bumping
//! TypeLong → TypeLonglong and TypeLonglong → TypeNewDecimal) followed by the
//! decimal-only sign rule and the flen arithmetic. The Rust owner of that path
//! is `tidb_planner::plan_builder::set_opr::{build_projection4_union,
//! union_join_field_type}` (same line-for-line sources), so the port asserts
//! the same observable: the field type of column 0 on the built
//! `LogicalUnionAll`'s schema — which is what Go's `rs.Fields()[0]` prints
//! after execution.

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

/// GO PORT of `pkg/planner/core/casetest/logicalplan/
/// logical_plan_builder_test.go:60-62`, issue:50235 arm of
/// `TestLogicalPlanTypeRegression`.
///
/// Re-derived contract: a YEAR(4) primary-key column compared with the
/// out-of-int64-range constant `16212511333665770580` still returns the stored
/// row `2016`. In Go the ranger's range detacher converts the reference to the
/// column's type without truncation errors and clamps the huge bound, leaving
/// a full-table year range; correctness then needs a mock store, insert and
/// executor round-trip.
#[test]
#[ignore = "go-parity-gap: needs executor/mock-store data round trip plus the session-backed YEAR range comparison from buildDataSource+ranger -- none exists in tidb-planner"]
fn logical_plan_type_regression_year_upper_bound_still_matches_row() {}

/// GO PORT of `pkg/planner/core/casetest/logicalplan/
/// logical_plan_builder_test.go:25 TestGroupBySchema`.
///
/// Re-derived contract: with cascades off/on the EXPLAIN plan_tree of the
/// scalar-subquery query (`EXISTS (... NATURAL RIGHT JOIN ... GROUP BY ...)`)
/// must open with `TableDual root rows:0` and carry a Null-aware anti semi
/// join under a ScalarSubQuery with seven ScalarQueryCol outputs, per the
/// inline golden. Pinning it needs RunTestUnderCascades' live session plus the
/// full optimize-and-print pipeline (`planner.Optimize` + explain printer),
/// which tidb-planner deliberately does not have.
#[test]
#[ignore = "go-parity-gap: needs RunTestUnderCascades live session and whole-plan explain printing of the scalar-subquery/NATURAL-RIGHT-JOIN pipeline"]
fn group_by_schema_explain_golden_with_scalar_subquery() {}
