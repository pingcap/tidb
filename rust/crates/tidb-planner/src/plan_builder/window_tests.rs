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

//! 6e's seam tests, for [`super::window`].
//!
//! WRITTEN, not transcreated, with the exceptions named below. Go's coverage
//! for the window stage is `pkg/planner/core/casetest/windows`,
//! `logical_plan_test.go`'s `TestWindowFunction` and
//! `tests/integrationtest/t/window_function.test` — every one of them needs a
//! live session, a `testkit` cluster and a golden-plan file, none of which is
//! reachable from this crate. The exceptions are:
//!
//! * [`test_default_frame_matches_mysqls_documented_rule`] and
//!   [`test_row_number_takes_the_pipelined_default_frame`], whose expectations
//!   are TRANSCREATED from `handleDefaultFrame`'s own comment (`:7241-7246`)
//!   and `aggregation.UseDefaultFrame` (`window_func.go:97`);
//! * [`test_cmp_type_for_a_datetime_order_key_is_real_like_gos`], transcreated
//!   from `getBaseCmpType` (`builtin_compare.go:1470`) by case analysis.
//!
//! What each group proves is named on the group.

use tidb_ast::{
    Expr, FrameBound as AstFrameBound, FrameKind, OrderItem, QueryStmt, Stmt, WindowDef,
    WindowFrame as AstWindowFrame, WindowSpec,
};
use tidb_datatype::{EvalType, FieldType, FieldTypeCode, SessionTimeZone, UNSPECIFIED_LENGTH};
use tidb_expr::column::Column;
use tidb_expr::ZonedNoColumns;

use super::catalog::{SourceColumn, SourceTable, TableSource};
use super::window::{
    all_by_items, bound_type_of, cmp_func_token, cmp_type_for_same_field_type, compare_items,
    is_unbounded, merge_window_spec, spec_equal, window_name, NamedWindowSpec,
};
use super::PlanBuilder;
use crate::expression_rewriter::ColumnIdAllocator;
use crate::logical::window::{BoundType, FrameType, LogicalWindow};
use crate::logical::LogicalPlan;
use crate::plan_base::{PlanError, PlanIdAllocator};

// ***** the harness *****

struct TestCatalog {
    current_database: String,
    tables: Vec<SourceTable>,
}

impl TableSource for TestCatalog {
    fn current_database(&self) -> &str {
        &self.current_database
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

fn column(offset: usize, name: &str, ret_type: FieldType) -> SourceColumn {
    SourceColumn {
        id: offset as i64 + 1,
        name: name.to_owned(),
        is_primary_key: false,
        offset,
        ret_type,
        is_public: true,
        is_hidden: false,
        is_virtual_generated: false,
    }
}

fn bigint() -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::LongLong);
    ft.set_flen(20);
    ft.set_decimal(0);
    ft
}

fn varchar(flen: i64) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::Varchar);
    ft.set_flen(flen);
    ft.set_decimal(UNSPECIFIED_LENGTH);
    ft.set_charset_name("utf8mb4");
    ft.set_collation_name("utf8mb4_bin");
    ft
}

fn datetime() -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::Datetime);
    ft.set_flen(19);
    ft.set_decimal(0);
    ft
}

/// `CREATE TABLE test.t (a BIGINT, b BIGINT, v VARCHAR(10), d DATETIME)`.
fn catalog() -> TestCatalog {
    TestCatalog {
        current_database: "test".to_owned(),
        tables: vec![SourceTable {
            table_id: 100,
            table_name: "t".to_owned(),
            db_name: "test".to_owned(),
            physical_table_id: 100,
            columns: vec![
                column(0, "a", bigint()),
                column(1, "b", bigint()),
                column(2, "v", varchar(10)),
                column(3, "d", datetime()),
            ],
            ..SourceTable::default()
        }],
    }
}

struct Harness {
    catalog: TestCatalog,
    ctx: ZonedNoColumns,
    plan_ids: PlanIdAllocator,
    column_ids: ColumnIdAllocator,
}

impl Harness {
    fn new() -> Self {
        Self {
            catalog: catalog(),
            ctx: ZonedNoColumns(SessionTimeZone::utc()),
            plan_ids: PlanIdAllocator::default(),
            column_ids: ColumnIdAllocator::new(),
        }
    }

    fn builder(&self) -> PlanBuilder<'_, TestCatalog, ZonedNoColumns> {
        PlanBuilder::new(
            &self.catalog,
            &self.ctx,
            &self.plan_ids,
            &self.column_ids,
            SessionTimeZone::utc(),
        )
    }
}

fn parse_query(sql: &str) -> QueryStmt {
    match tidb_parser::parse(sql).expect("the seam's SQL parses") {
        Stmt::Query(query) => query.into_inner(),
        other => panic!("expected a query, got {other:?}"),
    }
}

fn try_build(sql: &str) -> Result<LogicalPlan, PlanError> {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let query = parse_query(sql);
    builder.build_query_stmt(&query, false)
}

fn build(sql: &str) -> LogicalPlan {
    try_build(sql).unwrap_or_else(|error| panic!("{sql} should build: {}", error.message()))
}

fn build_err(sql: &str) -> String {
    match try_build(sql) {
        Ok(_) => panic!("{sql} should have been refused"),
        Err(error) => error.message().to_owned(),
    }
}

fn operator_names(plan: &LogicalPlan) -> Vec<String> {
    let mut names = vec![plan.tp().to_owned()];
    for child in plan.children() {
        names.extend(operator_names(child));
    }
    names
}

/// Every `LogicalWindow` on the way down, root first.
fn windows(plan: &LogicalPlan) -> Vec<&LogicalWindow> {
    let mut found = Vec::new();
    collect_windows(plan, &mut found);
    found
}

fn collect_windows<'a>(plan: &'a LogicalPlan, out: &mut Vec<&'a LogicalWindow>) {
    if let LogicalPlan::Window(window) = plan {
        out.push(window);
    }
    for child in plan.children() {
        collect_windows(child, out);
    }
}

/// Every `LogicalProjection` in the plan, root first.
fn projections(plan: &LogicalPlan) -> Vec<&crate::logical::projection::LogicalProjection> {
    let mut found = Vec::new();
    collect_projections(plan, &mut found);
    found
}

fn collect_projections<'a>(
    plan: &'a LogicalPlan,
    out: &mut Vec<&'a crate::logical::projection::LogicalProjection>,
) {
    if let LogicalPlan::Projection(projection) = plan {
        out.push(projection);
    }
    for child in plan.children() {
        collect_projections(child, out);
    }
}

/// Whether some projection reads the column with `unique_id` directly.
fn projection_reads(plan: &LogicalPlan, unique_id: i64) -> bool {
    projections(plan).iter().any(|projection| {
        projection.exprs.iter().any(|expr| {
            matches!(expr, tidb_expr::expression::Expression::Column(column)
                if column.unique_id == unique_id)
        })
    })
}

fn only_window(sql: &str) -> LogicalWindow {
    let plan = build(sql);
    let found = windows(&plan);
    assert_eq!(
        found.len(),
        1,
        "{sql} should build exactly one window, got {:?}",
        operator_names(&plan)
    );
    found[0].clone()
}

// ***** PARTITION BY / ORDER BY shape *****

#[test]
fn test_a_plain_window_builds_one_window_over_a_projection() {
    // `buildWindowFunctions` (`:7064`) stacks a `LogicalWindow` on the
    // projection `buildProjectionForWindow` (`:6728`) puts under it.
    let plan = build("SELECT a, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) FROM t");
    let names = operator_names(&plan);
    assert!(names.iter().any(|tp| tp == "Window"), "{names:?}");
    let window = only_window("SELECT a, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) FROM t");
    assert_eq!(window.partition_by.len(), 1);
    assert_eq!(window.order_by.len(), 1);
    assert!(!window.order_by[0].desc);
    assert_eq!(window.window_func_descs.len(), 1);
    // The window's own output column is APPENDED past the child's schema.
    let child_len = window.base.children()[0]
        .schema()
        .map_or(0, |schema| schema.columns.len());
    assert_eq!(
        window
            .base
            .base
            .schema()
            .map_or(0, |schema| schema.columns.len()),
        child_len + 1
    );
}

#[test]
fn test_order_by_direction_reaches_the_window() {
    let window = only_window("SELECT SUM(a) OVER (ORDER BY b DESC) FROM t");
    assert_eq!(window.order_by.len(), 1);
    assert!(window.order_by[0].desc);
    assert!(window.partition_by.is_empty());
}

#[test]
fn test_an_empty_over_clause_has_neither_partition_nor_order() {
    let window = only_window("SELECT ROW_NUMBER() OVER () FROM t");
    assert!(window.partition_by.is_empty());
    assert!(window.order_by.is_empty());
}

#[test]
fn test_a_non_column_by_item_is_projected_into_a_new_column() {
    // `buildByItemsForWindow` (`:6826`)'s non-column arm appends the
    // expression to the projection and sorts on the fresh column.
    let window = only_window("SELECT SUM(a) OVER (PARTITION BY a + b) FROM t");
    assert_eq!(window.partition_by.len(), 1);
    let child = &window.base.children()[0];
    assert_eq!(child.tp(), "Projection");
    // Two source columns are projected through plus the computed key; the
    // exact count depends on the select list, so the assertion is that the
    // partition column IS in the child's schema.
    let schema = child.schema().expect("a projection has a schema");
    assert!(schema.contains(&window.partition_by[0].col));
}

#[test]
fn test_two_window_functions_over_the_same_named_spec_share_one_operator() {
    // Section 1: `b.windowSpecs[w]` is ONE pointer, so both group together.
    // Both functions must leave `handleDefaultFrame` UNCHANGED for the shared
    // pointer to survive: RANK and DENSE_RANK are both frame-less and neither
    // has a `UseDefaultFrame` entry. ROW_NUMBER would NOT share, because rule
    // 4 rewrites its spec into a private copy — which
    // [`test_row_number_takes_the_pipelined_default_frame`] covers.
    let plan = build(
        "SELECT RANK() OVER w, DENSE_RANK() OVER w FROM t WINDOW w AS (PARTITION BY a ORDER BY b)",
    );
    let found = windows(&plan);
    assert_eq!(found.len(), 1, "{:?}", operator_names(&plan));
    assert_eq!(found[0].window_func_descs.len(), 2);
}

#[test]
fn test_two_textually_equal_inline_specs_do_not_share_an_operator() {
    // Section 1, the other direction: `spec := &windowFunc.Spec` is a FRESH
    // address per window function, so Go builds two groups here even though
    // the two specs restore to the same text.
    let plan = build("SELECT RANK() OVER (ORDER BY a), DENSE_RANK() OVER (ORDER BY a) FROM t");
    let found = windows(&plan);
    assert_eq!(found.len(), 2, "{:?}", operator_names(&plan));
    assert!(found.iter().all(|w| w.window_func_descs.len() == 1));
}

// ***** named window spec resolution and inheritance *****

#[test]
fn test_a_named_window_inherits_partition_and_order_from_its_base() {
    // `mergeWindowSpec` (`:7410`) folds the base's clauses into the extension.
    let window = only_window(
        "SELECT ROW_NUMBER() OVER w2 FROM t WINDOW w1 AS (PARTITION BY a ORDER BY b), w2 AS (w1)",
    );
    assert_eq!(window.partition_by.len(), 1);
    assert_eq!(window.order_by.len(), 1);
}

#[test]
fn test_an_over_clause_may_reference_a_named_window_inline() {
    // `groupWindowFuncs`' `spec.Ref.L != ""` arm (`:7322`).
    let window =
        only_window("SELECT ROW_NUMBER() OVER (w ORDER BY b) FROM t WINDOW w AS (PARTITION BY a)");
    assert_eq!(window.partition_by.len(), 1);
    assert_eq!(window.order_by.len(), 1);
}

#[test]
fn test_a_named_window_may_reference_one_declared_later() {
    let window =
        only_window("SELECT ROW_NUMBER() OVER w1 FROM t WINDOW w1 AS (w2), w2 AS (PARTITION BY a)");
    assert_eq!(window.partition_by.len(), 1);
}

#[test]
fn test_an_unused_named_window_is_still_validated() {
    // `:7373` "Unused window specs should also be checked in
    // b.buildWindowFunctions". A window clause with no window function at all
    // must still report an undefined reference.
    let message = build_err("SELECT a FROM t WINDOW w AS (nosuch ORDER BY b)");
    assert!(message.contains("is not defined"), "{message}");
}

#[test]
fn test_an_unused_named_window_leaves_the_plan_alone() {
    // `:4564` "In such case plan `p` is not changed, so we don't have to build
    // another projection" — and `buildWindowFunctions` DISCARDS the projection
    // it built for the unused spec.
    let plan = build("SELECT a FROM t WINDOW w AS (PARTITION BY a ORDER BY b)");
    assert!(windows(&plan).is_empty(), "{:?}", operator_names(&plan));
}

#[test]
fn test_a_duplicate_window_name_is_refused() {
    let message = build_err("SELECT a FROM t WINDOW w AS (PARTITION BY a), w AS (ORDER BY b)");
    assert!(message.contains("defined twice"), "{message}");
}

#[test]
fn test_an_undefined_window_name_is_refused() {
    let message = build_err("SELECT ROW_NUMBER() OVER nosuch FROM t");
    assert!(message.contains("is not defined"), "{message}");
}

#[test]
fn test_a_circular_window_reference_is_refused() {
    let message = build_err("SELECT ROW_NUMBER() OVER w1 FROM t WINDOW w1 AS (w2), w2 AS (w1)");
    assert!(message.contains("circularity"), "{message}");
}

#[test]
fn test_inheriting_from_a_window_with_a_frame_is_refused() {
    // `mergeWindowSpec`'s first guard: `ErrWindowNoInherentFrame`.
    let message = build_err(
        "SELECT SUM(a) OVER (w) FROM t WINDOW w AS (ORDER BY b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
    );
    assert!(message.contains("frame definition"), "{message}");
}

#[test]
fn test_redefining_the_bases_order_by_is_refused() {
    // `ErrWindowNoRedefineOrderBy`.
    let message =
        build_err("SELECT ROW_NUMBER() OVER (w ORDER BY a) FROM t WINDOW w AS (ORDER BY b)");
    assert!(message.contains("ORDER BY"), "{message}");
}

#[test]
fn test_an_extension_may_not_add_its_own_partitioning() {
    // `ErrWindowNoChildPartitioning`. Exercised directly, because the parser
    // is where the SQL form of this is otherwise rejected.
    let base = NamedWindowSpec::new(
        "w".to_owned(),
        WindowDef {
            base: None,
            spec: WindowSpec {
                partition_by: vec![Expr::Column(vec!["a".to_owned()])],
                ..WindowSpec::default()
            },
        },
    );
    let mut extension = WindowDef {
        base: Some("w".to_owned()),
        spec: WindowSpec {
            partition_by: vec![Expr::Column(vec!["b".to_owned()])],
            ..WindowSpec::default()
        },
    };
    let error = merge_window_spec(&mut extension, "", &base).expect_err("must be refused");
    assert!(error.message().contains("partitioning"), "{error:?}");
}

#[test]
fn test_merge_inherits_partitioning_and_clears_the_reference() {
    let base = NamedWindowSpec::new(
        "w".to_owned(),
        WindowDef {
            base: None,
            spec: WindowSpec {
                partition_by: vec![Expr::Column(vec!["a".to_owned()])],
                order_by: vec![OrderItem {
                    expr: Expr::Column(vec!["b".to_owned()]),
                    desc: true,
                }],
                frame: None,
            },
        },
    );
    let mut extension = WindowDef {
        base: Some("w".to_owned()),
        spec: WindowSpec::default(),
    };
    merge_window_spec(&mut extension, "", &base).expect("a clean extension merges");
    assert_eq!(extension.base, None);
    assert_eq!(extension.spec.partition_by.len(), 1);
    assert_eq!(extension.spec.order_by.len(), 1);
    assert!(extension.spec.order_by[0].desc);
}

// ***** the default frame *****

#[test]
fn test_default_frame_matches_mysqls_documented_rule() {
    // TRANSCREATED from `handleDefaultFrame`'s comment (`:7241`): "With order
    // by, the default frame is equivalent to RANGE BETWEEN UNBOUNDED
    // PRECEDING AND CURRENT ROW".
    let window = only_window("SELECT SUM(a) OVER (ORDER BY b) FROM t");
    let frame = window.frame.as_ref().expect("a default frame is built");
    assert_eq!(frame.frame_type, FrameType::Ranges);
    let start = frame.start.as_ref().expect("a start bound");
    assert_eq!(start.bound_type, BoundType::Preceding);
    assert!(start.unbounded);
    let end = frame.end.as_ref().expect("an end bound");
    assert_eq!(end.bound_type, BoundType::CurrentRow);
    assert!(!end.unbounded);
}

#[test]
fn test_no_order_by_means_no_frame_at_all() {
    // The comment's case (2): without ORDER BY the default frame is the whole
    // partition, "which is the same as an empty frame" — a NIL `Frame`, which
    // rule 3 keeps as `None` and not as an empty `WindowFrame`.
    let window = only_window("SELECT SUM(a) OVER (PARTITION BY b) FROM t");
    assert!(window.frame.is_none());
}

#[test]
fn test_a_doubly_unbounded_frame_is_erased() {
    // `handleDefaultFrame`'s second rule: "RANGE/ROWS BETWEEN UNBOUNDED
    // PRECEDING AND UNBOUNDED FOLLOWING is equivalent to empty frame."
    let window = only_window(
        "SELECT SUM(a) OVER (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
    );
    assert!(window.frame.is_none());
}

#[test]
fn test_row_number_takes_the_pipelined_default_frame() {
    // TRANSCREATED from `aggregation.UseDefaultFrame` (`window_func.go:97`):
    // ROW_NUMBER is fixed to `ROWS BETWEEN CURRENT ROW AND CURRENT ROW`, and
    // `handleDefaultFrame`'s rule 3 has already erased whatever was written.
    let window = only_window("SELECT ROW_NUMBER() OVER (ORDER BY b) FROM t");
    let frame = window.frame.as_ref().expect("the pipelined default frame");
    assert_eq!(frame.frame_type, FrameType::Rows);
    assert_eq!(
        frame.start.as_ref().map(|bound| bound.bound_type),
        Some(BoundType::CurrentRow)
    );
    assert_eq!(
        frame.end.as_ref().map(|bound| bound.bound_type),
        Some(BoundType::CurrentRow)
    );
}

#[test]
fn test_a_non_frame_function_without_the_pipelined_variable_loses_its_frame() {
    // Rule 3 alone, with rule 4 switched off: RANK never keeps a frame.
    let harness = Harness::new();
    let mut builder = harness.builder();
    builder.enable_pipelined_window_exec = false;
    let query = parse_query(
        "SELECT RANK() OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
    );
    let plan = builder
        .build_query_stmt(&query, false)
        .expect("RANK with an ignored frame builds");
    let found = windows(&plan);
    assert_eq!(found.len(), 1);
    assert!(found[0].frame.is_none());
}

// ***** frame bound types *****

#[test]
fn test_rows_frame_bounds_carry_their_row_counts() {
    // `buildWindowFunctionFrameBound` (`:6873`)'s ROWS arm: `Num` is
    // `getUintFromNode`'s value and no calc function is built.
    let window = only_window(
        "SELECT SUM(a) OVER (ORDER BY b ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING) FROM t",
    );
    let frame = window.frame.as_ref().expect("an explicit frame");
    assert_eq!(frame.frame_type, FrameType::Rows);
    let start = frame.start.as_ref().expect("a start bound");
    assert_eq!(start.bound_type, BoundType::Preceding);
    assert_eq!(start.num, 2);
    assert!(start.calc_funcs.is_empty());
    assert!(!start.is_explicit_range);
    let end = frame.end.as_ref().expect("an end bound");
    assert_eq!(end.bound_type, BoundType::Following);
    assert_eq!(end.num, 3);
}

#[test]
fn test_unbounded_preceding_to_current_row_under_rows() {
    let window = only_window(
        "SELECT SUM(a) OVER (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
    );
    let frame = window.frame.as_ref().expect("an explicit frame");
    let start = frame.start.as_ref().expect("a start bound");
    assert!(start.unbounded);
    assert_eq!(start.bound_type, BoundType::Preceding);
    assert_eq!(start.num, 0);
    let end = frame.end.as_ref().expect("an end bound");
    assert_eq!(end.bound_type, BoundType::CurrentRow);
    assert!(end.calc_funcs.is_empty());
}

#[test]
fn test_a_range_current_row_bound_gets_one_calc_func_per_order_item() {
    // The RANGE arm's `CURRENT ROW` case: `CalcFuncs[i] = col` and
    // `CmpFuncs[i] = GetCmpFunction(col, col)`.
    let window = only_window("SELECT SUM(a) OVER (ORDER BY b) FROM t");
    let frame = window.frame.as_ref().expect("the default RANGE frame");
    let end = frame.end.as_ref().expect("an end bound");
    assert_eq!(end.bound_type, BoundType::CurrentRow);
    assert_eq!(end.calc_funcs.len(), 1);
    assert_eq!(end.cmp_func_tokens, vec!["CompareInt".to_owned()]);
    assert!(!end.is_explicit_range);
    // The UNBOUNDED start returns before any of that is filled in.
    let start = frame.start.as_ref().expect("a start bound");
    assert!(start.calc_funcs.is_empty());
}

#[test]
fn test_an_explicit_range_bound_builds_a_calc_func_and_is_marked_explicit() {
    let window = only_window(
        "SELECT SUM(a) OVER (ORDER BY b RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
    );
    let frame = window.frame.as_ref().expect("an explicit frame");
    assert_eq!(frame.frame_type, FrameType::Ranges);
    let start = frame.start.as_ref().expect("a start bound");
    assert!(start.is_explicit_range);
    assert_eq!(start.calc_funcs.len(), 1);
    assert_eq!(start.cmp_func_tokens, vec!["CompareInt".to_owned()]);
    let end = frame.end.as_ref().expect("an end bound");
    assert!(end.is_explicit_range);
    assert_eq!(end.calc_funcs.len(), 1);
}

#[test]
fn test_a_descending_order_flips_the_explicit_range_arithmetic() {
    // "When the order is desc, `+` becomes `-` and vice-versa" (`:6923`).
    let ascending = only_window(
        "SELECT SUM(a) OVER (ORDER BY b RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
    );
    let descending = only_window(
        "SELECT SUM(a) OVER (ORDER BY b DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
    );
    let ascending_start = ascending.frame.unwrap().start.unwrap();
    let descending_start = descending.frame.unwrap().start.unwrap();
    let name_of = |expr: &tidb_expr::expression::Expression| match expr {
        tidb_expr::expression::Expression::ScalarFunction(function) => {
            function.func_name.to_string()
        }
        other => panic!("expected a scalar calc function, got {other:?}"),
    };
    assert_eq!(name_of(&ascending_start.calc_funcs[0]), "minus");
    assert_eq!(name_of(&descending_start.calc_funcs[0]), "plus");
}

// ***** spec validation refusals *****

#[test]
fn test_unbounded_following_as_a_start_is_refused() {
    let message = build_err(
        "SELECT SUM(a) OVER (ORDER BY b ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) FROM t",
    );
    assert!(message.contains("illegal frame start"), "{message}");
}

#[test]
fn test_unbounded_preceding_as_an_end_is_refused() {
    let message = build_err(
        "SELECT SUM(a) OVER (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) FROM t",
    );
    assert!(message.contains("illegal frame end"), "{message}");
}

#[test]
fn test_a_start_after_its_end_is_refused() {
    // `checkOriginWindowSpec`'s "(start FOLLOWING or CURRENT ROW) and end
    // PRECEDING" arm.
    let message = build_err(
        "SELECT SUM(a) OVER (ORDER BY b ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM t",
    );
    assert!(message.contains("illegal frame definition"), "{message}");
}

#[test]
fn test_a_following_start_with_a_current_row_end_is_refused() {
    let message = build_err(
        "SELECT SUM(a) OVER (ORDER BY b ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM t",
    );
    assert!(message.contains("illegal frame definition"), "{message}");
}

#[test]
fn test_a_range_frame_needs_exactly_one_order_by_item() {
    let message = build_err(
        "SELECT SUM(a) OVER (ORDER BY a, b RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
    );
    assert!(message.contains("exactly one ORDER BY"), "{message}");
}

#[test]
fn test_a_range_frame_over_a_string_order_key_is_refused() {
    let message = build_err(
        "SELECT SUM(a) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
    );
    assert!(message.contains("numeric or temporal"), "{message}");
}

#[test]
fn test_a_numeric_bound_over_a_datetime_order_key_is_refused() {
    // `ErrWindowRangeFrameTemporalType`: "Only INTERVAL bound value allowed".
    let message = build_err(
        "SELECT SUM(a) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
    );
    assert!(
        message.contains("INTERVAL bound value allowed"),
        "{message}"
    );
}

#[test]
fn test_group_concat_as_a_window_function_is_refused() {
    let message = build_err("SELECT GROUP_CONCAT(v) OVER (ORDER BY a) FROM t");
    assert!(message.contains("group_concat"), "{message}");
}

#[test]
fn test_a_distinct_window_function_is_refused() {
    let message = build_err("SELECT SUM(DISTINCT a) OVER (ORDER BY b) FROM t");
    assert!(message.contains("DISTINCT"), "{message}");
}

#[test]
fn test_an_aggregate_inside_a_window_argument_names_its_blocking_symbol() {
    // Section 3's `resolveWindowFunction` boundary.
    let message = build_err("SELECT ROW_NUMBER() OVER (ORDER BY SUM(a)) FROM t GROUP BY b");
    assert!(message.contains("resolveWindowFunction"), "{message}");
}

#[test]
fn test_a_positional_window_by_item_names_its_blocking_symbol() {
    // Section 3's `itemTransformer` boundary.
    let message = build_err("SELECT a, ROW_NUMBER() OVER (ORDER BY 1) FROM t");
    assert!(message.contains("itemTransformer"), "{message}");
}

#[test]
fn test_a_non_constant_range_bound_names_its_blocking_symbol() {
    let message = build_err(
        "SELECT SUM(a) OVER (ORDER BY b RANGE BETWEEN a PRECEDING AND CURRENT ROW) FROM t",
    );
    assert!(
        message.contains("evalAstExprWithPlanCtx") || message.contains("non-constant"),
        "{message}"
    );
}

#[test]
fn test_a_window_in_a_recursive_cte_block_is_refused() {
    let message = build_err(
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT ROW_NUMBER() OVER () FROM c) SELECT n FROM c",
    );
    assert!(
        message.contains("neither aggregation nor window functions"),
        "{message}"
    );
}

// ***** the marker binding *****

#[test]
fn test_the_window_column_reaches_the_final_projection() {
    // Section 2: the `#win#k` marker resolves to the `LogicalWindow`'s own
    // output column, and `buildProjection(considerWindow = true)` projects it.
    let plan = build("SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t");
    let found = windows(&plan);
    let window_column = found[0]
        .base
        .base
        .schema()
        .expect("the window has a schema")
        .columns
        .last()
        .cloned()
        .expect("the window appends its own column");
    // The root is `buildSelect`'s `:4620` trailing projection, which trims the
    // auxiliary `a` the window's ORDER BY needed; the marker binding is one
    // level below it.
    assert!(
        projection_reads(&plan, window_column.unique_id),
        "no projection reads the window column, plan is {:?}",
        operator_names(&plan)
    );
}

#[test]
fn test_a_window_inside_a_larger_expression_still_resolves() {
    let plan = build("SELECT ROW_NUMBER() OVER (ORDER BY a) + 1 FROM t");
    assert!(!windows(&plan).is_empty());
    // Some projection computes the `+`, over the window's own column rather
    // than over the zero placeholder the FIRST projection left.
    assert!(
        projections(&plan).iter().any(|projection| projection
            .exprs
            .iter()
            .any(|expr| matches!(expr, tidb_expr::expression::Expression::ScalarFunction(_)))),
        "{:?}",
        operator_names(&plan)
    );
}

#[test]
fn test_a_non_window_field_is_passed_through_by_index() {
    // `buildProjection`'s `considerWindow && !isWindowFuncField` arm: the
    // field becomes the child's column at the SAME index, not a re-rewrite.
    let plan = build("SELECT a + b, ROW_NUMBER() OVER (ORDER BY a) FROM t");
    // The projection built ABOVE the window reads `a + b` as a COLUMN — the
    // one the first projection already computed — and never rebuilds the sum.
    let above = projections(&plan)
        .into_iter()
        .find(|projection| {
            projection
                .base
                .children()
                .first()
                .is_some_and(|child| child.tp() == "Window")
        })
        .expect("a projection sits directly on the window");
    assert!(matches!(
        above.exprs[0],
        tidb_expr::expression::Expression::Column(_)
    ));
}

// ***** the unit-level helpers *****

#[test]
fn test_window_name_reports_an_unnamed_window() {
    assert_eq!(window_name(""), "<unnamed window>");
    assert_eq!(window_name("w"), "w");
}

#[test]
fn test_bound_predicates_classify_every_ast_bound() {
    let one = || Box::new(Expr::Int("1".to_owned()));
    assert!(is_unbounded(&AstFrameBound::UnboundedPreceding));
    assert!(is_unbounded(&AstFrameBound::UnboundedFollowing));
    assert!(!is_unbounded(&AstFrameBound::CurrentRow));
    assert!(!is_unbounded(&AstFrameBound::Preceding(one())));
    assert_eq!(
        bound_type_of(&AstFrameBound::UnboundedPreceding),
        BoundType::Preceding
    );
    assert_eq!(
        bound_type_of(&AstFrameBound::Preceding(one())),
        BoundType::Preceding
    );
    assert_eq!(
        bound_type_of(&AstFrameBound::CurrentRow),
        BoundType::CurrentRow
    );
    assert_eq!(
        bound_type_of(&AstFrameBound::Following(one())),
        BoundType::Following
    );
    assert_eq!(
        bound_type_of(&AstFrameBound::UnboundedFollowing),
        BoundType::Following
    );
}

#[test]
fn test_cmp_type_for_a_datetime_order_key_is_real_like_gos() {
    // TRANSCREATED by case analysis over `getBaseCmpType`
    // (`builtin_compare.go:1470`): a DATETIME/DATETIME pair matches none of
    // the string, int, decimal or year-vs-date arms and falls through to
    // `return types.ETReal`, and no override in `GetAccurateCmpType` rescues
    // it because every override needs a CONSTANT on one side. Reproduced, not
    // repaired.
    assert_eq!(cmp_type_for_same_field_type(&datetime()), EvalType::Real);
    assert_eq!(cmp_func_token(EvalType::Real), "CompareReal");

    assert_eq!(cmp_type_for_same_field_type(&bigint()), EvalType::Int);
    assert_eq!(cmp_type_for_same_field_type(&varchar(10)), EvalType::String);
    assert_eq!(
        cmp_type_for_same_field_type(&FieldType::new(FieldTypeCode::NewDecimal)),
        EvalType::Decimal
    );
    assert_eq!(
        cmp_type_for_same_field_type(&FieldType::new(FieldTypeCode::Duration)),
        EvalType::Duration
    );
    assert_eq!(
        cmp_type_for_same_field_type(&FieldType::new(FieldTypeCode::Double)),
        EvalType::Real
    );
    // ENUM's own `EvalType` is `ETString`, and `getBaseCmpType`'s FIRST arm
    // (`lhs.IsStringKind() && rhs.IsStringKind()`) fires before the
    // `lft.Hybrid()` arm can — so an ENUM pair compares as a STRING, not as
    // the integer its storage suggests. BIT, whose `EvalType` is `ETInt`,
    // does take the hybrid arm.
    assert_eq!(
        cmp_type_for_same_field_type(&FieldType::new(FieldTypeCode::Enum)),
        EvalType::String
    );
    assert_eq!(
        cmp_type_for_same_field_type(&FieldType::new(FieldTypeCode::Bit)),
        EvalType::Int
    );
    // Both `TypeUnspecified` -> `ETString`, the first arm of `getBaseCmpType`.
    assert_eq!(
        cmp_type_for_same_field_type(&FieldType::new(FieldTypeCode::Unspecified)),
        EvalType::String
    );
}

#[test]
fn test_spec_equal_matches_gos_nil_arms() {
    let spec = WindowDef::default();
    let other = WindowDef {
        base: None,
        spec: WindowSpec {
            partition_by: vec![Expr::Column(vec!["a".to_owned()])],
            ..WindowSpec::default()
        },
    };
    assert!(spec_equal(None, None));
    assert!(!spec_equal(Some(&spec), None));
    assert!(!spec_equal(None, Some(&spec)));
    assert!(spec_equal(Some(&spec), Some(&spec.clone())));
    assert!(!spec_equal(Some(&spec), Some(&other)));
}

#[test]
fn test_all_by_items_is_partition_then_order() {
    let spec = WindowSpec {
        partition_by: vec![Expr::Column(vec!["a".to_owned()])],
        order_by: vec![OrderItem {
            expr: Expr::Column(vec!["b".to_owned()]),
            desc: true,
        }],
        frame: Some(AstWindowFrame {
            kind: FrameKind::Rows,
            start: AstFrameBound::UnboundedPreceding,
            end: AstFrameBound::CurrentRow,
        }),
    };
    let items = all_by_items(&spec);
    assert_eq!(items.len(), 2);
    assert!(!items[0].desc, "a PARTITION BY item is never descending");
    assert!(items[1].desc);
}

#[test]
fn test_compare_items_is_a_strict_weak_ordering_over_length_and_direction() {
    let item = |name: &str, desc: bool| OrderItem {
        expr: Expr::Column(vec![name.to_owned()]),
        desc,
    };
    // A shorter prefix sorts first.
    assert!(compare_items(
        &[item("a", false)],
        &[item("a", false), item("b", false)]
    ));
    assert!(!compare_items(
        &[item("a", false), item("b", false)],
        &[item("a", false)]
    ));
    // ASC sorts before DESC on an otherwise equal item.
    assert!(compare_items(&[item("a", false)], &[item("a", true)]));
    assert!(!compare_items(&[item("a", true)], &[item("a", false)]));
    // Equal lists are not less than one another.
    assert!(!compare_items(&[item("a", false)], &[item("a", false)]));
}

#[test]
fn test_a_window_column_is_not_reused_across_two_functions() {
    // The marker binding must be DENSE and per-call: two calls in one group
    // get two distinct output columns.
    let plan = build("SELECT ROW_NUMBER() OVER w, RANK() OVER w FROM t WINDOW w AS (ORDER BY a)");
    let found = windows(&plan);
    let schema = found[0].base.base.schema().expect("a schema");
    let ids: Vec<i64> = schema
        .columns
        .iter()
        .rev()
        .take(2)
        .map(|column: &Column| column.unique_id)
        .collect();
    assert_ne!(ids[0], ids[1]);
}
