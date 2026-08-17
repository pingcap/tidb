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

//! Tests for [`super::rewrite::recursive_derive_stats`], Go's
//! `RecursiveDeriveStats` driver over the per-operator `DeriveStats` bodies.
//!
//! All WRITTEN: Go's coverage of stats derivation is testkit- and
//! golden-file-bound (`cardinality/selectivity_test.go`, planner casetests),
//! none of it reachable from this crate. Each expectation cites the Go rule
//! it checks instead.

use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;

use crate::cost_factors::SELECTION_FACTOR;
use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PlanIdAllocator;
use crate::stats_info::StatsInfo;

use super::data_source::DataSource;
use super::join::LogicalJoin;
use super::projection::LogicalProjection;
use super::sort::LogicalSort;
use super::topn::LogicalTopN;
use super::{BaseLogicalPlan, LogicalPlan};

fn column(id: i64) -> Column {
    let mut col = Column::default();
    col.unique_id = id;
    col.ret_type = Some(FieldType::new(FieldTypeCode::Long));
    col
}

fn schema_of(ids: &[i64]) -> Schema {
    Schema::new(ids.iter().map(|id| column(*id)).collect())
}

fn base(allocator: &PlanIdAllocator, tp: &str, schema: Option<Schema>) -> BaseLogicalPlan {
    let mut base = BaseLogicalPlan::new(allocator, tp, 0);
    base.base.set_schema(schema);
    base
}

/// A `DataSource` whose `table_stats` is already attached, as Go's
/// `initStats` guarantees before any `DeriveStats` runs.
fn stated_source(
    allocator: &PlanIdAllocator,
    ids: &[i64],
    row_count: f64,
    ndvs: &[(i64, f64)],
) -> LogicalPlan {
    let mut source = DataSource::new(base(allocator, "DataSource", Some(schema_of(ids))), 1, "t");
    source.table_stats = Some(StatsInfo::new(row_count, ndvs.iter().copied()));
    LogicalPlan::DataSource(source)
}

fn derive(plan: &mut LogicalPlan) -> Result<(StatsInfo, bool), crate::plan_base::PlanError> {
    plan.recursive_derive_stats(&[])
}

/// `col(left) = col(right)` as the `ScalarFunction` an `EqualCondition`
/// holds.
fn eq_condition(left: i64, right: i64) -> ScalarFunction {
    ScalarFunction::new(
        CiString::new("eq"),
        FieldType::new(FieldTypeCode::Long),
        vec![
            Expression::Column(column(left)),
            Expression::Column(column(right)),
        ],
    )
}

#[test]
fn a_selection_scales_its_source_by_gos_flat_factor() {
    // `LogicalSelection.DeriveStats` (`logical_selection.go`) is a flat
    // `Scale(SelectionFactor)` over the child, and the DataSource's own
    // profile is its (pre-scaled) table stats.
    let allocator = PlanIdAllocator::new();
    let source = stated_source(&allocator, &[1, 2], 100.0, &[(1, 10.0), (2, 50.0)]);
    let mut selection = LogicalPlan::Selection(super::selection::LogicalSelection::new(
        base(&allocator, "Selection", None),
        Vec::new(),
    ));
    selection.set_children(vec![source]);

    let (stats, _) = derive(&mut selection).expect("a stated chain derives");
    assert!(
        (stats.row_count() - 100.0 * SELECTION_FACTOR).abs() < f64::EPSILON,
        "Go scales the child's count by SelectionFactor, got {}",
        stats.row_count()
    );
    // The profile was WRITTEN onto both nodes, as every Go body's SetStats
    // does.
    assert!(selection.stats_info().is_some());
    assert!(selection.children()[0].stats_info().is_some());
}

#[test]
fn the_base_body_adopts_the_single_child_for_a_sort() {
    // `logical_sort.go` declares no DeriveStats, so Go runs
    // `BaseLogicalPlan.DeriveStats`'s one-child arm: adopt the child profile
    // unchanged.
    let allocator = PlanIdAllocator::new();
    let source = stated_source(&allocator, &[1], 42.0, &[(1, 7.0)]);
    let mut sort = LogicalPlan::Sort(LogicalSort::new(base(&allocator, "Sort", None), Vec::new()));
    sort.set_children(vec![source]);

    let (stats, _) = derive(&mut sort).expect("the base body derives");
    assert!((stats.row_count() - 42.0).abs() < f64::EPSILON);
    assert_eq!(stats.col_ndvs().get(&1).copied(), Some(7.0));
}

#[test]
fn a_topn_clamps_to_its_count_like_gos_derive_limit_stats() {
    // `logical_top_n.go:134`: `DeriveLimitStats(childStats[0], Count)` —
    // the row count caps at the limit and every NDV caps at that count.
    let allocator = PlanIdAllocator::new();
    let source = stated_source(&allocator, &[1], 100.0, &[(1, 50.0)]);
    let mut topn = LogicalPlan::TopN(LogicalTopN::new(
        base(&allocator, "TopN", None),
        Vec::new(),
        0,
        5,
    ));
    topn.set_children(vec![source]);

    let (stats, _) = derive(&mut topn).expect("a top-n derives");
    assert!((stats.row_count() - 5.0).abs() < f64::EPSILON);
    assert_eq!(
        stats.col_ndvs().get(&1).copied(),
        Some(5.0),
        "NDV clamps to the limited count"
    );
}

#[test]
fn an_unported_override_still_refuses_by_its_go_name() {
    // The scan overrides remain refused: they bottom out in
    // `deriveStatsByFilter` + the ranger, which is access-path work.
    let allocator = PlanIdAllocator::new();
    let mut scan = LogicalPlan::TableScan(super::table_scan::LogicalTableScan::new(base(
        &allocator,
        "TableScan",
        None,
    )));
    let error = derive(&mut scan).expect_err("an unported override must refuse");
    let rendered = format!("{error:?}");
    assert!(
        rendered.contains("deriveStats4LogicalTableScan"),
        "the error names the Go symbol, got {rendered}"
    );
}

#[test]
fn a_join_reproduces_gos_equal_cond_out_cnt() {
    // `logical_join.go` DeriveStats: for an inner join the row count is
    // `EstimateFullJoinRowCount = leftRows * rightRows / max(leftKeyNDV,
    // rightKeyNDV)` (`cardinality/join.go`, threshold 0). 100 * 200 / max(10,
    // 20) = 1000.
    let allocator = PlanIdAllocator::new();
    let left = stated_source(&allocator, &[1], 100.0, &[(1, 10.0)]);
    let right = stated_source(&allocator, &[11], 200.0, &[(11, 20.0)]);
    let mut join = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 11]))),
        LogicalJoinType::Inner,
    );
    join.equal_conditions = vec![eq_condition(1, 11)];
    let mut join = LogicalPlan::Join(join);
    join.set_children(vec![left, right]);

    let (stats, _) = derive(&mut join).expect("a keyed inner join derives");
    assert!(
        (stats.row_count() - 1000.0).abs() < f64::EPSILON,
        "Go's full-join estimate is 1000, got {}",
        stats.row_count()
    );
}

#[test]
fn a_cartesian_join_multiplies_the_children() {
    // Go: `is_cartesian = (0 == len(p.EqualConditions))`, and
    // `EstimateFullJoinRowCount` returns leftRows * rightRows for it.
    let allocator = PlanIdAllocator::new();
    let left = stated_source(&allocator, &[1], 30.0, &[(1, 3.0)]);
    let right = stated_source(&allocator, &[11], 40.0, &[(11, 4.0)]);
    let join = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 11]))),
        LogicalJoinType::Inner,
    );
    let mut join = LogicalPlan::Join(join);
    join.set_children(vec![left, right]);

    let (stats, _) = derive(&mut join).expect("a cartesian join derives");
    assert!((stats.row_count() - 1200.0).abs() < f64::EPSILON);
}

#[test]
fn a_semi_join_takes_the_left_count_under_the_selection_factor() {
    // `logical_join.go`: SemiJoin's count is `leftRows * SelectionFactor`,
    // with every LEFT NDV scaled the same way — the right side contributes
    // no columns.
    let allocator = PlanIdAllocator::new();
    let left = stated_source(&allocator, &[1], 100.0, &[(1, 10.0)]);
    let right = stated_source(&allocator, &[11], 999.0, &[(11, 999.0)]);
    let mut join = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1]))),
        LogicalJoinType::Semi,
    );
    join.equal_conditions = vec![eq_condition(1, 11)];
    let mut join = LogicalPlan::Join(join);
    join.set_children(vec![left, right]);

    let (stats, _) = derive(&mut join).expect("a semi join derives");
    assert!((stats.row_count() - 100.0 * SELECTION_FACTOR).abs() < f64::EPSILON);
    assert_eq!(
        stats.col_ndvs().get(&11),
        None,
        "the semi join keeps only the left side's columns"
    );
}

#[test]
fn a_projection_passes_the_count_through() {
    // `logical_projection.go` DeriveStats: row count unchanged, one NDV per
    // output expression.
    let allocator = PlanIdAllocator::new();
    let source = stated_source(&allocator, &[1, 2], 64.0, &[(1, 8.0), (2, 16.0)]);
    let mut projection = LogicalPlan::Projection(LogicalProjection::new(
        base(&allocator, "Projection", Some(schema_of(&[1]))),
        vec![Expression::Column(column(1))],
    ));
    projection.set_children(vec![source]);

    let (stats, _) = derive(&mut projection).expect("a projection derives");
    assert!((stats.row_count() - 64.0).abs() < f64::EPSILON);
}

#[test]
fn depth_costs_no_host_stack() {
    // The driver rides `fold_owned`; a chain that would overflow a recursive
    // walk must derive. Sorts adopt the child profile, so the root's answer
    // is the source's, sixty thousand levels down.
    let allocator = PlanIdAllocator::new();
    let mut plan = stated_source(&allocator, &[1], 42.0, &[(1, 7.0)]);
    for _ in 0..60_000 {
        let mut sort =
            LogicalPlan::Sort(LogicalSort::new(base(&allocator, "Sort", None), Vec::new()));
        sort.set_children(vec![plan]);
        plan = sort;
    }

    let (stats, _) = derive(&mut plan).expect("depth alone must not fail");
    assert!((stats.row_count() - 42.0).abs() < f64::EPSILON);

    // Owned drop of a 60k chain is itself recursive if left to `Drop`;
    // detach one level at a time so each drop is shallow.
    let mut cursor = plan;
    loop {
        let dummy = LogicalPlan::TableDual(super::table_dual::LogicalTableDual::default());
        match cursor.set_child(0, dummy) {
            Some(child) => cursor = child,
            None => break,
        }
    }
}

#[test]
fn a_source_without_table_stats_refuses_rather_than_guessing() {
    // Go's `initStats` always attaches at least the pseudo table before
    // DeriveStats runs; a bare source here means the builder skipped that,
    // and inventing 10000 rows for it would hide the gap.
    let allocator = PlanIdAllocator::new();
    let mut source = LogicalPlan::DataSource(DataSource::new(
        base(&allocator, "DataSource", Some(schema_of(&[1]))),
        1,
        "t",
    ));

    let error = derive(&mut source).expect_err("a stat-less source refuses");
    assert!(format!("{error:?}").contains("table_stats"));
}

#[test]
fn a_pushed_equality_charges_gos_pseudo_rate() {
    // `deriveStatsByFilter` over an unanalyzed table takes
    // `pseudoSelectivity` (`selectivity.go:69`): one equality charges
    // `1/pseudoEqualRate = 1/1000`, so 10000 pseudo rows estimate 10 — the
    // number real TiDB prints for `a = 7` on an unanalyzed table.
    let allocator = PlanIdAllocator::new();
    let mut source = stated_source(&allocator, &[1], 10_000.0, &[(1, 8_000.0)]);
    let LogicalPlan::DataSource(op) = &mut source else {
        unreachable!()
    };
    op.columns = vec![super::data_source::DataSourceColumn {
        id: 1,
        name: "a".to_owned(),
        is_primary_key: false,
    }];
    op.pushed_down_conds = vec![Expression::ScalarFunction(eq_condition_to_constant(1, 7))];

    let (stats, _) = derive(&mut source).expect("an unanalyzed equality derives");
    assert!(
        (stats.row_count() - 10.0).abs() < f64::EPSILON,
        "10000 / pseudoEqualRate, got {}",
        stats.row_count()
    );
}

#[test]
fn no_conditions_still_answer_the_full_table() {
    // `Selectivity` with no conditions is 100% (`selectivity.go:61`).
    let allocator = PlanIdAllocator::new();
    let mut source = stated_source(&allocator, &[1], 500.0, &[(1, 5.0)]);
    let (stats, _) = derive(&mut source).expect("derives");
    assert!((stats.row_count() - 500.0).abs() < f64::EPSILON);
}

/// `col = const`, the shape `getConstantColumnID` resolves.
fn eq_condition_to_constant(col: i64, value: i64) -> ScalarFunction {
    ScalarFunction::new(
        CiString::new("eq"),
        FieldType::new(FieldTypeCode::Long),
        vec![
            Expression::Column(column(col)),
            Expression::Constant(tidb_expr::constant::Constant::new(
                tidb_datatype::Datum::new_int(value),
                FieldType::new(FieldTypeCode::Long),
            )),
        ],
    )
}
