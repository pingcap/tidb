// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `partidx`'s tests over the planner's `DataSource`.

use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::constant::Constant;
use tidb_expr::rewriter::NoResolver;

use tidb_planner_core_partidx::*;
use tidb_datatype::Datum;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_planner_util::path::PossiblePath;
use tidb_planner_core_operator_logicalop::{BaseLogicalPlan, DataSource};
use tidb_planner_core_base::PlanIdAllocator;

fn integer_column(id: i64) -> Expression {
    Expression::Column(Column::new(id, FieldType::new(FieldTypeCode::LongLong)))
}

fn integer(value: i64) -> Expression {
    Expression::Constant(Constant::new(
        Datum::Int(value),
        FieldType::new(FieldTypeCode::LongLong),
    ))
}

fn call(name: &str, arguments: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new(name),
        FieldType::new(FieldTypeCode::Tiny),
        arguments,
    ))
}

fn compare(name: &str, left: Expression, right: Expression) -> Expression {
    call(name, vec![left, right])
}

fn not_is_null(column: Expression) -> Expression {
    call("not", vec![call("isnull", vec![column])])
}

#[test]
fn exact_match_is_a_multiset_and_empty_constraints_always_match() {
    let a = compare("gt", integer_column(1), integer(0));
    let b = compare("lt", integer_column(1), integer(10));
    assert!(check_constraints(true, &[], &[]));
    assert!(exact_match(&[a.clone(), b.clone()], &[b, a]));
    let duplicate = compare("eq", integer_column(1), integer(1));
    assert!(!exact_match(
        &[duplicate.clone(), duplicate.clone()],
        &[duplicate]
    ));
}

#[test]
fn comparison_and_not_null_implication_match_ranger_ranges() {
    let column = integer_column(1);
    let wider = compare("gt", column.clone(), integer(1));
    let narrower = compare("gt", column.clone(), integer(5));
    assert!(check_constraints(
        true,
        std::slice::from_ref(&wider),
        std::slice::from_ref(&narrower)
    ));
    assert!(!check_constraints(true, &[narrower], &[wider]));

    let pre = not_is_null(column.clone());
    assert!(check_constraints(
        true,
        std::slice::from_ref(&pre),
        &[compare("ge", column.clone(), integer(0))]
    ));
    assert!(!check_constraints(
        true,
        &[pre],
        &[call("isnull", vec![column])]
    ));
}

#[test]
fn unsupported_predicates_only_match_exactly() {
    let pre = call("sin", vec![integer_column(1)]);
    assert!(check_constraints(
        true,
        std::slice::from_ref(&pre),
        std::slice::from_ref(&pre)
    ));
    assert!(!check_constraints(
        true,
        &[pre],
        &[compare("gt", integer_column(1), integer(0))]
    ));
}

#[test]
fn plan_cache_not_null_proof_has_the_source_and_or_shape() {
    let target = integer_column(1);
    let other = integer_column(2);
    let pre = not_is_null(target.clone());
    let target_gt = compare("gt", target.clone(), integer(0));
    let other_gt = compare("gt", other.clone(), integer(0));

    assert!(always_meet_constraints(
        std::slice::from_ref(&pre),
        &[call("and", vec![other_gt.clone(), target_gt.clone()])]
    ));
    assert!(!always_meet_constraints(
        std::slice::from_ref(&pre),
        &[call("or", vec![other_gt, target_gt.clone()])]
    ));
    assert!(always_meet_constraints(
        std::slice::from_ref(&pre),
        &[call(
            "or",
            vec![target_gt.clone(), compare("lt", integer(0), target.clone())]
        )]
    ));
    assert!(!always_meet_constraints(
        std::slice::from_ref(&pre),
        &[compare("nulleq", target.clone(), integer(0))]
    ));
    assert!(!always_meet_constraints(
        &[call("isnull", vec![target.clone()])],
        &[target_gt]
    ));
    assert!(!always_meet_constraints(
        &[pre],
        &[call("isnull", vec![target])]
    ));
}

#[test]
fn datasource_prunes_partial_paths_and_marks_the_ordinary_cached_plan() {
    let allocator = PlanIdAllocator::new();
    let column = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
    let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
    base.base
        .set_schema(Some(tidb_expr::schema::Schema::new(vec![column.clone()])));
    let mut source = DataSource::new(base, 1, "t");
    source.columns = vec![tidb_model::GoShared::new({
            let info = tidb_model::ColumnInfo {
                id: 1,
                name: tidb_ast::CiString::new("a"),
                state: tidb_model::SchemaState::PUBLIC,
                ..tidb_model::ColumnInfo::default()
            };
            info
        })];
    source.indexes = vec![
        tidb_model::GoShared::new(tidb_model::IndexInfo {
            id: 10,
            name: tidb_ast::CiString::new("partial"),
            condition_expr_string: "a > 0".to_owned(),
            ..tidb_model::IndexInfo::default()
        }),
        tidb_model::GoShared::new(tidb_model::IndexInfo {
            id: 20,
            name: tidb_ast::CiString::new("ordinary"),
            ..tidb_model::IndexInfo::default()
        }),
    ];
    source.enumerated_paths = vec![
        PossiblePath::Table {
            is_int_handle: true,
            primary_index: None,
        },
        PossiblePath::Index { index: 0 },
        PossiblePath::Index { index: 1 },
    ];
    source.pushed_down_conds = vec![compare("lt", integer_column(1), integer(0))];
    source.check_partial_indexes(&NoResolver, false, true);
    assert_eq!(
        source.enumerated_paths,
        vec![
            PossiblePath::Table {
                is_int_handle: true,
                primary_index: None,
            },
            PossiblePath::Index { index: 1 },
        ]
    );

    source
        .enumerated_paths
        .insert(1, PossiblePath::Index { index: 0 });
    source.pushed_down_conds = vec![compare("gt", integer_column(1), integer(5))];
    source.forced_index_ids.insert(10);
    source.check_partial_indexes(&NoResolver, true, true);
    assert_eq!(
        source.enumerated_paths,
        vec![PossiblePath::Index { index: 0 }]
    );
    assert!(source.partial_index_noncacheable_ids.contains(&10));
}

