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

//! The DataSource derivation (`stats::CoreDataSourceStats`) under the
//! statement's prepared parameters.

use tidb_planner_core_operator_logicalop::rewrite::*;
use tidb_planner_core_operator_logicalop::*;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_planner_property::stats_info::StatsInfo;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::constant::{Constant, ParamMarker};
use tidb_expr::scalar_function::ScalarFunction;

struct Parameters([i64; 2]);

impl tidb_exprctx::Columns for Parameters {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn param_value(&self, order: usize) -> Result<Datum, tidb_exprctx::EvalError> {
        self.0
            .get(order)
            .copied()
            .map(Datum::Int)
            .ok_or(tidb_exprctx::EvalError::Unsupported(
                "unbound prepared parameter",
            ))
    }
}

#[test]
fn logical_range_derivation_uses_current_statement_parameters() {
    let allocator = tidb_planner_core_base::PlanIdAllocator::new();
    let mut ty = FieldType::new(FieldTypeCode::LongLong);
    ty.add_flags(tidb_datatype::FieldTypeFlags::PRI_KEY);
    let mut column = tidb_expr::column::Column::new(1, ty.clone());
    column.id = 1;
    column.ret_type = Some(ty.clone());
    let bound = |name: &str, order: i64| {
        let mut value = Constant::new(Datum::Int(99), ty.clone());
        value.param_marker = Some(ParamMarker { order });
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new(name),
            ty.clone(),
            vec![
                Expression::Column(column.clone()),
                Expression::Constant(value),
            ],
        ))
    };
    let mut source = tidb_planner_core_operator_logicalop::data_source::DataSource {
        handle_is_int: true,
        handle_cols: vec![column.clone()],
        table_columns: vec![column.clone()],
        // Go `initStats` attaches `PseudoTable`'s collection, whose
        // handle column is `IsHandle`.
        table_stats: Some(StatsInfo::new(10_000.0, [(1, 10_000.0)]).with_hist_coll({
            let mut coll = tidb_planner_property::stats_info::HistColl::new_with_cols_and_idxs(
                0,
                10_000,
                0,
                [(
                    1,
                    tidb_stats::column::Column {
                        info: Some(tidb_model::GoShared::new(tidb_model::ColumnInfo {
                            id: 1,
                            name: tidb_ast::CiString::new("id"),
                            ..tidb_model::ColumnInfo::default()
                        })),
                        is_handle: true,
                        ..tidb_stats::column::Column::default()
                    },
                )],
                [],
            );
            coll.pseudo = true;
            std::sync::Arc::new(coll)
        })),
        pushed_down_conds: vec![bound("ge", 0), bound("le", 1)],
        ..Default::default()
    };
    source.table_scan_penalty.pseudo_stats = true;
    source.base.base.set_schema(Some(Schema::new(vec![column])));
    // Go pseudoGetRowCountByIntRanges caps a non-point estimate at
    // high-low; Selectivity also retains its one-row floor for empty
    // ranges. These are estimates, not the inclusive row counts.
    for (values, expected) in [([3, 5], 2.0), ([3, 8], 5.0), ([8, 7], 1.0)] {
        let parameters = Parameters(values);
        let mut context = crate::rule::rule_tests::test_context(&allocator);
        context.eval_context = &parameters;
        let (_, result) = recursive_derive_stats_with_context(
            LogicalPlan::DataSource(source.clone()),
            vec![],
            &context,
        );
        assert_eq!(
            result.unwrap().0.row_count(),
            expected,
            "parameters {values:?}"
        );
    }
}

#[test]
fn analyzed_derivation_evaluates_parameters_like_literal_predicates() {
    let allocator = tidb_planner_core_base::PlanIdAllocator::new();
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let columns = [1, 2].map(|id| {
        let mut column = tidb_expr::column::Column::new(id, ty.clone());
        column.id = id;
        column
    });
    let histogram = std::sync::Arc::new(tidb_stats::column::Column {
        histogram: tidb_stats::Histogram {
            ndv: 10,
            buckets: vec![tidb_stats::Bucket {
                count: 100,
                repeat: 10,
                ndv: 10,
                lower_bound: Datum::Int(1),
                upper_bound: Datum::Int(10),
            }],
            ..Default::default()
        },
        top_n: None,
        cmsketch: None,
        stats_version: 2,
        ..Default::default()
    });
    let profile = StatsInfo::new(100.0, [(1, 10.0), (2, 10.0)]).with_hist_coll(
        tidb_planner_property::stats_info::loaded_hist_coll_for_test(false, 100, 0, [(1, std::sync::Arc::unwrap_or_clone(histogram.clone())), (2, std::sync::Arc::unwrap_or_clone(histogram))], []),
    );
    let scalar = |name: &str, args| {
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new(name),
            ty.clone(),
            args,
        ))
    };
    let predicates = |mode: u8| {
        let value = |order: i64| {
            let mut value = Constant::new(
                if mode == 0 {
                    Datum::Int(if order == 0 { 3 } else { 5 })
                } else {
                    Datum::Null
                },
                ty.clone(),
            );
            if mode != 0 {
                value.param_marker = Some(ParamMarker { order });
            }
            if mode == 2 {
                let mut deferred = Constant::new(Datum::Null, ty.clone());
                deferred.deferred_expr = Some(Box::new(Expression::Constant(value)));
                Expression::Constant(deferred)
            } else {
                Expression::Constant(value)
            }
        };
        let eq = scalar("eq", vec![Expression::Column(columns[0].clone()), value(0)]);
        let other = scalar("eq", vec![Expression::Column(columns[1].clone()), value(1)]);
        vec![
            vec![eq.clone()],
            vec![scalar(
                "in",
                vec![Expression::Column(columns[0].clone()), value(0), value(1)],
            )],
            vec![
                scalar("ge", vec![Expression::Column(columns[0].clone()), value(0)]),
                scalar("le", vec![Expression::Column(columns[0].clone()), value(1)]),
            ],
            vec![scalar("or", vec![eq, other])],
        ]
    };
    let parameters = Parameters([3, 5]);
    let mut context = crate::rule::rule_tests::test_context(&allocator);
    context.eval_context = &parameters;
    let estimate = |predicates| {
        let mut source = tidb_planner_core_operator_logicalop::data_source::DataSource {
            table_columns: columns.to_vec(),
            table_stats: Some(profile.clone()),
            pushed_down_conds: predicates,
            ..Default::default()
        };
        source
            .base
            .base
            .set_schema(Some(Schema::new(columns.to_vec())));
        recursive_derive_stats_with_context(LogicalPlan::DataSource(source), vec![], &context)
            .1
            .unwrap()
            .0
            .row_count()
    };
    for mode in [1, 2] {
        for (case, (parameterized, literal)) in
            predicates(mode).into_iter().zip(predicates(0)).enumerate()
        {
            assert_eq!(
                estimate(parameterized),
                estimate(literal),
                "analyzed predicate mode {mode}, case {case}"
            );
        }
    }
    // A comparison whose constant cannot be evaluated builds no point
    // (`points.go:340`), so that DNF item is an empty range charged the
    // one-row floor, combined under the independence assumption.
    let valid = predicates(0)[0][0].clone();
    let mut missing = Constant::new(Datum::Null, ty.clone());
    missing.param_marker = Some(ParamMarker { order: 2 });
    let invalid = scalar(
        "eq",
        vec![
            Expression::Column(columns[1].clone()),
            Expression::Constant(missing),
        ],
    );
    let valid_selectivity = estimate(vec![valid.clone()]) / 100.0;
    let floor = 1.0 / 100.0;
    let dnf_selectivity = valid_selectivity + floor - valid_selectivity * floor;
    assert_eq!(
        estimate(vec![valid.clone(), scalar("or", vec![valid, invalid])]),
        100.0 * valid_selectivity * dnf_selectivity
    );
}
