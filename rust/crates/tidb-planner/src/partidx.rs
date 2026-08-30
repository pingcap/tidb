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

//! Go `pkg/planner/core/partidx/check_constraint.go`.

use tidb_datatype::Datum;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;

use crate::ranger::checker::UNSPECIFIED_LENGTH;
use crate::ranger::detacher::extract_access_conditions_for_column;
use crate::ranger::ranger::{build_column_range, union_ranges};

/// Go `CheckConstraints`: whether `filters` imply the partial index's stored
/// predicate.
///
/// `opt_prefix_index_single_scan` is the sole `RangerContext` field read by
/// the source package. It is the session value of
/// `tidb_opt_prefix_index_single_scan`.
#[must_use]
pub fn check_constraints(
    opt_prefix_index_single_scan: bool,
    pre_predicates: &[Expression],
    filters: &[Expression],
) -> bool {
    if pre_predicates.is_empty() {
        return true;
    }
    debug_assert_eq!(pre_predicates.len(), 1);
    if exact_match(pre_predicates, filters) {
        return true;
    }
    can_be_implied_from_exprs(opt_prefix_index_single_scan, &pre_predicates[0], filters)
}

fn exact_match(pre_predicates: &[Expression], filters: &[Expression]) -> bool {
    let mut matched = vec![false; filters.len()];
    for pre in pre_predicates {
        let Some(index) = filters
            .iter()
            .enumerate()
            .position(|(index, filter)| !matched[index] && pre.equal(filter))
        else {
            return false;
        };
        matched[index] = true;
    }
    true
}

fn can_be_implied_from_exprs(
    opt_prefix_index_single_scan: bool,
    pre: &Expression,
    filters: &[Expression],
) -> bool {
    let Expression::ScalarFunction(function) = pre else {
        panic!("partial-index predicates are scalar functions")
    };
    if function.func_name.lowercase() == "not" {
        let [Expression::ScalarFunction(is_null)] = function.get_args() else {
            return false;
        };
        if is_null.func_name.lowercase() != "isnull" {
            return false;
        }
        let [Expression::Column(column)] = is_null.get_args() else {
            return false;
        };
        return impl_is_not_null(opt_prefix_index_single_scan, column, filters);
    }
    if !is_compare_operator(function.func_name.lowercase()) {
        return false;
    }
    impl_compare_expr(opt_prefix_index_single_scan, function, filters)
}

fn is_compare_operator(name: &str) -> bool {
    matches!(
        name,
        "lt" | "ge" | "gt" | "le" | "eq" | "ne" | "nulleq" | "in"
    )
}

fn impl_compare_expr(
    opt_prefix_index_single_scan: bool,
    pre: &ScalarFunction,
    filters: &[Expression],
) -> bool {
    let Some(column) = pre
        .get_args()
        .first()
        .and_then(as_column)
        .or_else(|| pre.get_args().get(1).and_then(as_column))
    else {
        return false;
    };
    let Some(field_type) = column.get_static_type() else {
        return false;
    };
    let pre_expression = Expression::ScalarFunction(pre.clone());
    let Ok(pre_result) = build_column_range(
        std::slice::from_ref(&pre_expression),
        field_type,
        UNSPECIFIED_LENGTH,
        0,
    ) else {
        return false;
    };
    if pre_result.ranges.is_empty() {
        return false;
    }
    let column_conditions =
        extract_access_conditions_for_column(filters, column, opt_prefix_index_single_scan);
    if column_conditions.is_empty() {
        return false;
    }
    let Ok(filter_result) =
        build_column_range(&column_conditions, field_type, UNSPECIFIED_LENGTH, 0)
    else {
        return false;
    };
    if filter_result.ranges.is_empty() {
        return false;
    }
    let mut ranges = filter_result.ranges;
    ranges.extend(pre_result.ranges.iter().cloned());
    let Ok(unioned) = union_ranges(ranges, false) else {
        return false;
    };
    unioned.len() == pre_result.ranges.len()
        && unioned
            .iter()
            .zip(&pre_result.ranges)
            .all(|(left, right)| left.equal(right))
}

fn impl_is_not_null(
    opt_prefix_index_single_scan: bool,
    target_column: &Column,
    filters: &[Expression],
) -> bool {
    let column_conditions =
        extract_access_conditions_for_column(filters, target_column, opt_prefix_index_single_scan);
    if column_conditions.is_empty() {
        return false;
    }
    let Some(field_type) = target_column.get_static_type() else {
        return false;
    };
    let Ok(result) = build_column_range(&column_conditions, field_type, UNSPECIFIED_LENGTH, 0)
    else {
        return false;
    };
    !result.ranges.is_empty()
        && result
            .ranges
            .iter()
            .all(|range| !matches!(range.low_val.first(), Some(Datum::Null)) || range.low_exclude)
}

fn as_column(expression: &Expression) -> Option<&Column> {
    let Expression::Column(column) = expression else {
        return None;
    };
    Some(column)
}

/// Go `AlwaysMeetConstraints`: the deliberately narrow plan-cache proof for
/// one stored `NOT(ISNULL(column))` predicate.
#[must_use]
pub fn always_meet_constraints(pre_predicates: &[Expression], filters: &[Expression]) -> bool {
    let [Expression::ScalarFunction(not)] = pre_predicates else {
        return false;
    };
    if not.func_name.lowercase() != "not" {
        return false;
    }
    let [Expression::ScalarFunction(is_null)] = not.get_args() else {
        return false;
    };
    if is_null.func_name.lowercase() != "isnull" {
        return false;
    }
    let [Expression::Column(column)] = is_null.get_args() else {
        return false;
    };
    filters.iter().any(|filter| {
        let Expression::ScalarFunction(function) = filter else {
            return false;
        };
        check_is_null_rejected(column, function)
    })
}

fn check_is_null_rejected(target_column: &Column, filter: &ScalarFunction) -> bool {
    match filter.func_name.lowercase() {
        "or" => filter.get_args().iter().all(|argument| {
            let Expression::ScalarFunction(function) = argument else {
                return false;
            };
            check_is_null_rejected(target_column, function)
        }),
        "and" => filter.get_args().iter().any(|argument| {
            let Expression::ScalarFunction(function) = argument else {
                return false;
            };
            check_is_null_rejected(target_column, function)
        }),
        "isnull" => false,
        "nulleq" => false,
        name if is_compare_operator(name) => filter
            .get_args()
            .first()
            .and_then(as_column)
            .or_else(|| filter.get_args().get(1).and_then(as_column))
            .is_some_and(|column| {
                Expression::Column(column.clone()).equal(&Expression::Column(target_column.clone()))
            }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::constant::Constant;
    use tidb_expr::rewriter::NoResolver;

    use super::*;
    use crate::access_path::PossiblePath;
    use crate::logical::data_source::DataSourceColumn;
    use crate::logical::{BaseLogicalPlan, DataSource};
    use crate::plan_base::PlanIdAllocator;
    use crate::plan_builder::catalog::SourceIndex;

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
        source.columns = vec![DataSourceColumn {
            id: 1,
            name: "a".to_owned(),
            ..DataSourceColumn::default()
        }];
        source.indexes = vec![
            SourceIndex {
                id: 10,
                name: "partial".to_owned(),
                condition_expr_string: "a > 0".to_owned(),
                ..SourceIndex::default()
            },
            SourceIndex {
                id: 20,
                name: "ordinary".to_owned(),
                ..SourceIndex::default()
            },
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
}
