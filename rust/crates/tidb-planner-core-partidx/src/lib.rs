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

use tidb_ranger::checker::UNSPECIFIED_LENGTH;
use tidb_ranger::detacher::extract_access_conditions_for_column;
use tidb_ranger::ranger::{build_column_range, union_ranges};

/// Go `CheckConstraints`: whether `filters` imply the partial index's stored
/// predicate.
///
/// `opt_prefix_index_single_scan` is the sole `RangerContext` field read by
/// the source package. It is the session value of
/// `tidb_opt_prefix_index_single_scan`.
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

/// Go `exactMatch`: whether every partial-index predicate equals a distinct filter.
pub fn exact_match(pre_predicates: &[Expression], filters: &[Expression]) -> bool {
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
