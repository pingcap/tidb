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

//! Go `pkg/planner/core/rule/rule_predicate_simplification.go`.

use std::collections::BTreeSet;

use tidb_datatype::{compatible_collate, Datum, EvalType, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expr_util::normal_form::{split_cnf_items, split_dnf_items};
use tidb_expr::expr_util::predicates::{
    is_mutable_effects_expr, maybe_over_optimized_4_plan_cache,
};
use tidb_expr::expr_util::push_not::push_down_not;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{compose_cnf_condition, compose_dnf_condition};

use super::rule::{LogicalOptRule, RuleContext};
use super::LogicalPlan;
use crate::plan_base::PlanError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PredicateType {
    In,
    NotEqual,
    Equal,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,
    IsNull,
    Or,
    And,
    Scalar,
    False,
    True,
    Other,
}

fn logical_constant(ctx: &RuleContext<'_>, constant: &Constant) -> PredicateType {
    if maybe_over_optimized_4_plan_cache(
        ctx.use_plan_cache,
        &[Expression::Constant(constant.clone())],
    ) {
        return PredicateType::Other;
    }
    if matches!(constant.value, Datum::Null) {
        return PredicateType::False;
    }
    constant
        .value
        .to_bool()
        .map_or(PredicateType::Other, |value| {
            if value.value == 0 {
                PredicateType::False
            } else {
                PredicateType::True
            }
        })
}

fn predicate_type<'a>(
    ctx: &RuleContext<'_>,
    expression: &'a Expression,
) -> (Option<&'a Column>, PredicateType) {
    match expression {
        Expression::Constant(constant) => (None, logical_constant(ctx, constant)),
        Expression::ScalarFunction(function) => {
            let name = function.func_name.lowercase();
            if name == "or" {
                return (None, PredicateType::Or);
            }
            if name == "and" {
                return (None, PredicateType::And);
            }
            let Some(Expression::Column(column)) = function.get_args().first() else {
                return (None, PredicateType::Other);
            };
            if name == "isnull" {
                return (Some(column), PredicateType::IsNull);
            }
            if function
                .get_args()
                .get(1)
                .is_some_and(|argument| !matches!(argument, Expression::Constant(_)))
            {
                return (None, PredicateType::Other);
            }
            let predicate_type = match name {
                "ne" => PredicateType::NotEqual,
                "eq" => PredicateType::Equal,
                "lt" => PredicateType::Less,
                "gt" => PredicateType::Greater,
                "le" => PredicateType::LessEqual,
                "ge" => PredicateType::GreaterEqual,
                "in" if function
                    .get_args()
                    .iter()
                    .skip(1)
                    .all(|argument| matches!(argument, Expression::Constant(_))) =>
                {
                    PredicateType::In
                }
                _ => PredicateType::Other,
            };
            (Some(column), predicate_type)
        }
        _ => (None, PredicateType::Other),
    }
}

fn mark_skip(ctx: &RuleContext<'_>, reason: &'static str, expressions: &[Expression]) {
    if maybe_over_optimized_4_plan_cache(ctx.use_plan_cache, expressions) {
        if let Some(marker) = ctx.plan_cache_marker {
            marker.set_skip_plan_cache(reason);
        }
    }
}

fn rebuild(ctx: &RuleContext<'_>, original: &Expression, args: Vec<Expression>) -> Expression {
    let Expression::ScalarFunction(function) = original else {
        return original.clone();
    };
    ctx.builder
        .new_function(
            function.func_name.lowercase(),
            function.ret_type.clone(),
            args,
        )
        .unwrap_or_else(|_| original.clone())
}

fn process_logical(
    ctx: &RuleContext<'_>,
    expression: Expression,
) -> (Expression, PredicateType, bool) {
    let (_, kind) = predicate_type(ctx, &expression);
    if !matches!(kind, PredicateType::Or | PredicateType::And) {
        return (expression, kind, false);
    }
    let Expression::ScalarFunction(function) = &expression else {
        unreachable!()
    };
    let [left, right] = function.get_args() else {
        return (expression, PredicateType::Other, false);
    };
    let (left, left_type, left_changed) = process_logical(ctx, left.clone());
    let (right, right_type, right_changed) = process_logical(ctx, right.clone());
    let or_case = kind == PredicateType::Or;
    let selected = match (left_type, right_type, or_case) {
        (PredicateType::True, _, true) => Some(left.clone()),
        (_, PredicateType::True, true) => Some(right.clone()),
        (PredicateType::False, _, true) => Some(right.clone()),
        (_, PredicateType::False, true) => Some(left.clone()),
        (PredicateType::True, _, false) => Some(right.clone()),
        (_, PredicateType::True, false) => Some(left.clone()),
        (PredicateType::False, _, false) => Some(left.clone()),
        (_, PredicateType::False, false) => Some(right.clone()),
        _ => None,
    };
    let changed = selected.is_some() || left_changed || right_changed;
    let result = selected.unwrap_or_else(|| rebuild(ctx, &expression, vec![left, right]));
    let (_, result_type) = predicate_type(ctx, &result);
    if changed {
        mark_skip(
            ctx,
            "True/False predicate simplification is triggered",
            std::slice::from_ref(&expression),
        );
    }
    (result, result_type, changed)
}

fn short_circuit(ctx: &RuleContext<'_>, predicates: Vec<Expression>) -> Vec<Expression> {
    let mut result = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        let (predicate, predicate_type, _) = process_logical(ctx, predicate);
        match predicate_type {
            PredicateType::False => return vec![predicate],
            PredicateType::True => {}
            _ => result.push(predicate),
        }
    }
    result
}

fn update_in(in_predicate: &Expression, not_equal: &Expression) -> (Expression, bool) {
    let (Expression::ScalarFunction(in_function), Expression::ScalarFunction(ne_function)) =
        (in_predicate, not_equal)
    else {
        return (in_predicate.clone(), false);
    };
    let Some(Expression::Constant(not_equal_value)) = ne_function.get_args().get(1) else {
        return (in_predicate.clone(), false);
    };
    if matches!(not_equal_value.value, Datum::Null) {
        return (in_predicate.clone(), true);
    }
    let mut values = Vec::with_capacity(in_function.get_args().len());
    let mut last_constant = None;
    for element in in_function.get_args() {
        let redundant = matches!(element, Expression::Constant(value) if Expression::Constant(value.clone()).equal(&Expression::Constant(not_equal_value.clone())));
        if !redundant {
            values.push(element.clone());
        }
        if let Expression::Constant(value) = element {
            last_constant = Some(Expression::Constant(value.clone()));
        }
    }
    let special = values.len() < 2;
    if special {
        if let Some(last) = last_constant {
            values.push(last);
        }
    }
    (
        Expression::ScalarFunction(tidb_expr::scalar_function::ScalarFunction::new(
            in_function.func_name.clone(),
            in_function
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::Tiny)),
            values,
        )),
        special,
    )
}

fn merge_in_and_not_equal(
    ctx: &RuleContext<'_>,
    mut predicates: Vec<Expression>,
) -> Vec<Expression> {
    if predicates.len() <= 1 {
        return predicates;
    }
    let mut remove = BTreeSet::new();
    for left in 0..predicates.len() {
        for right in left + 1..predicates.len() {
            let (left_column, left_type) = predicate_type(ctx, &predicates[left]);
            let (right_column, right_type) = predicate_type(ctx, &predicates[right]);
            if left_column.map(|column| column.unique_id)
                != right_column.map(|column| column.unique_id)
            {
                continue;
            }
            let (in_offset, ne_offset) = match (left_type, right_type) {
                (PredicateType::NotEqual, PredicateType::In) => (right, left),
                (PredicateType::In, PredicateType::NotEqual) => (left, right),
                _ => continue,
            };
            let (updated, special) = update_in(&predicates[in_offset], &predicates[ne_offset]);
            mark_skip(
                ctx,
                "NE/INList simplification is triggered",
                &[predicates[in_offset].clone(), predicates[ne_offset].clone()],
            );
            predicates[in_offset] = updated;
            if !special {
                remove.insert(ne_offset);
            }
        }
    }
    predicates
        .into_iter()
        .enumerate()
        .filter_map(|(offset, predicate)| (!remove.contains(&offset)).then_some(predicate))
        .collect()
}

fn remove_redundant_or(ctx: &RuleContext<'_>, expression: Expression) -> Expression {
    let (_, expression_type) = predicate_type(ctx, &expression);
    if expression_type != PredicateType::Or {
        return expression;
    }
    let mut seen = BTreeSet::new();
    let mut items = Vec::new();
    for item in split_dnf_items(&expression) {
        let (_, item_type) = predicate_type(ctx, &item);
        let item = if item_type == PredicateType::And {
            compose_cnf_condition(
                split_cnf_items(&item)
                    .into_iter()
                    .map(|condition| remove_redundant_or(ctx, condition))
                    .collect(),
            )
            .unwrap_or(item)
        } else {
            item
        };
        let key = item.clone().hash_code().to_vec();
        if seen.insert(key) || is_mutable_effects_expr(&item) {
            items.push(item);
        }
    }
    compose_dnf_condition(items).unwrap_or(expression)
}

fn is_unsatisfiable_expression(ctx: &RuleContext<'_>, expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::Constant(constant)
            if logical_constant(ctx, constant) == PredicateType::False
    )
}

fn is_binary_comparison(kind: PredicateType) -> bool {
    matches!(
        kind,
        PredicateType::Equal
            | PredicateType::NotEqual
            | PredicateType::Less
            | PredicateType::Greater
            | PredicateType::LessEqual
            | PredicateType::GreaterEqual
    )
}

fn comparison_predicate(kind: PredicateType) -> PredicateType {
    if matches!(
        kind,
        PredicateType::Equal
            | PredicateType::Less
            | PredicateType::Greater
            | PredicateType::LessEqual
            | PredicateType::GreaterEqual
            | PredicateType::IsNull
    ) {
        PredicateType::Scalar
    } else {
        kind
    }
}

fn is_prunable_or_branch(kind: PredicateType) -> bool {
    comparison_predicate(kind) == PredicateType::Scalar || kind == PredicateType::In
}

fn is_null_in_contradiction(
    ctx: &RuleContext<'_>,
    left: &Expression,
    left_type: PredicateType,
    right: &Expression,
    right_type: PredicateType,
) -> bool {
    let in_predicate = match (left_type, right_type) {
        (PredicateType::IsNull, PredicateType::In) => right,
        (PredicateType::In, PredicateType::IsNull) => left,
        _ => return false,
    };
    if maybe_over_optimized_4_plan_cache(ctx.use_plan_cache, std::slice::from_ref(in_predicate)) {
        return false;
    }
    let Expression::ScalarFunction(function) = in_predicate else {
        return false;
    };
    function.get_args().iter().skip(1).all(|argument| {
        matches!(argument, Expression::Constant(constant) if !matches!(constant.value, Datum::Null))
    })
}

fn is_unsatisfiable(ctx: &RuleContext<'_>, left: &Expression, right: &Expression) -> bool {
    let (left_column, left_type) = predicate_type(ctx, left);
    let (right_column, right_type) = predicate_type(ctx, right);
    let (Some(left_column), Some(right_column)) = (left_column, right_column) else {
        return false;
    };
    if left_column.unique_id != right_column.unique_id {
        return false;
    }
    if is_null_in_contradiction(ctx, left, left_type, right, right_type) {
        return true;
    }
    let (equal, other, other_type) = if left_type == PredicateType::Equal {
        (left, right, right_type)
    } else if right_type == PredicateType::Equal {
        (right, left, left_type)
    } else {
        return false;
    };
    if !is_binary_comparison(other_type) {
        return false;
    }
    let (Expression::ScalarFunction(equal), Expression::ScalarFunction(other)) = (equal, other)
    else {
        return false;
    };
    let (Some(Expression::Constant(equal_constant)), Some(Expression::Constant(other_constant))) =
        (equal.get_args().get(1), other.get_args().get(1))
    else {
        return false;
    };
    let Some(column_type) = left_column.get_static_type() else {
        return false;
    };
    for constant in [equal_constant, other_constant] {
        let Some(constant_type) = constant.get_static_type() else {
            return false;
        };
        if constant_type.eval_type() == EvalType::String
            && !compatible_collate(constant_type.collation_name(), column_type.collation_name())
        {
            return false;
        }
    }
    let Ok(comparison) = ctx.builder.new_function(
        other.func_name.lowercase(),
        other.ret_type.clone(),
        vec![
            Expression::Constant(equal_constant.clone()),
            Expression::Constant(other_constant.clone()),
        ],
    ) else {
        return false;
    };
    let outcome = tidb_expr::constant_propagation::propagate_constant(
        ctx.builder,
        ctx.use_plan_cache,
        vec![comparison],
        None,
    );
    outcome
        .conditions
        .first()
        .is_some_and(|condition| is_unsatisfiable_expression(ctx, condition))
}

fn update_or_predicate(
    ctx: &RuleContext<'_>,
    expression: Expression,
    scalar: &Expression,
) -> (Expression, bool) {
    if predicate_type(ctx, &expression).1 != PredicateType::Or
        || comparison_predicate(predicate_type(ctx, scalar).1) != PredicateType::Scalar
    {
        return (expression, false);
    }
    let Expression::ScalarFunction(function) = &expression else {
        return (expression, false);
    };
    let [first, second] = function.get_args() else {
        return (expression, false);
    };
    let (mut first, mut second) = (first.clone(), second.clone());
    let (mut first_changed, mut second_changed) = (false, false);
    let first_type = predicate_type(ctx, &first).1;
    let second_type = predicate_type(ctx, &second).1;
    let mut first_empty = if is_prunable_or_branch(first_type) {
        is_unsatisfiable(ctx, &first, scalar)
    } else {
        false
    };
    let mut second_empty = if is_prunable_or_branch(second_type) {
        is_unsatisfiable(ctx, &second, scalar)
    } else {
        false
    };
    if first_type == PredicateType::Or {
        (first, first_changed) = update_or_predicate(ctx, first, scalar);
    }
    if second_type == PredicateType::Or {
        (second, second_changed) = update_or_predicate(ctx, second, scalar);
    }
    first_empty |= is_unsatisfiable_expression(ctx, &first);
    second_empty |= is_unsatisfiable_expression(ctx, &second);
    match (first_empty, second_empty) {
        (true, false) => (second, true),
        (false, true) => (first, true),
        (true, true) => (
            Expression::Constant(Constant::new(
                Datum::Int(0),
                FieldType::new(FieldTypeCode::Tiny),
            )),
            true,
        ),
        (false, false) => {
            let rebuilt = rebuild(ctx, &expression, vec![first, second]);
            (rebuilt, first_changed || second_changed)
        }
    }
}

fn prune_empty_or_branches(
    ctx: &RuleContext<'_>,
    mut predicates: Vec<Expression>,
) -> Vec<Expression> {
    if predicates.len() <= 1 {
        return predicates;
    }
    for left in 0..predicates.len() {
        for right in left + 1..predicates.len() {
            let left_type = comparison_predicate(predicate_type(ctx, &predicates[left]).1);
            let right_type = comparison_predicate(predicate_type(ctx, &predicates[right]).1);
            let target = match (left_type, right_type) {
                (PredicateType::Scalar, PredicateType::Or) => Some((right, left)),
                (PredicateType::Or, PredicateType::Scalar) => Some((left, right)),
                _ => None,
            };
            let Some((or_offset, scalar_offset)) = target else {
                continue;
            };
            let original = predicates[or_offset].clone();
            let (updated, changed) =
                update_or_predicate(ctx, original.clone(), &predicates[scalar_offset]);
            if changed {
                mark_skip(
                    ctx,
                    "OR predicate simplification is triggered",
                    std::slice::from_ref(&original),
                );
            }
            predicates[or_offset] = updated;
            if is_unsatisfiable_expression(ctx, &predicates[or_offset]) {
                return vec![predicates[or_offset].clone()];
            }
        }
    }
    predicates
}

/// Go `applyPredicateSimplification`.
#[must_use]
pub fn apply_predicate_simplification(
    ctx: &RuleContext<'_>,
    predicates: Vec<Expression>,
    propagate_constant: bool,
) -> Vec<Expression> {
    if predicates.is_empty() {
        return predicates;
    }
    let mut predicates = predicates
        .iter()
        .map(|predicate| push_down_not(predicate, ctx.builder))
        .collect::<Vec<_>>();
    if propagate_constant {
        let outcome = tidb_expr::constant_propagation::propagate_constant(
            ctx.builder,
            ctx.use_plan_cache,
            predicates,
            None,
        );
        if let (Some(marker), Some(reason)) =
            (ctx.plan_cache_marker, outcome.skip_plan_cache_reason)
        {
            marker.set_skip_plan_cache(reason);
        }
        predicates = outcome.conditions;
    } else {
        let outcome = tidb_expr::constant_propagation::propagate_constant(
            ctx.builder,
            ctx.use_plan_cache,
            predicates.clone(),
            None,
        );
        if outcome.conditions.len() == 1 {
            predicates = outcome.conditions;
        }
    }
    predicates = short_circuit(ctx, predicates);
    predicates = merge_in_and_not_equal(ctx, predicates);
    predicates = predicates
        .into_iter()
        .map(|predicate| remove_redundant_or(ctx, predicate))
        .collect();
    predicates = prune_empty_or_branches(ctx, predicates);
    predicates
        .into_iter()
        .filter(|predicate| predicate_type(ctx, predicate).1 != PredicateType::True)
        .collect()
}

/// Go `applyPredicateSimplificationForJoin`.
#[must_use]
pub fn apply_predicate_simplification_for_join(
    ctx: &RuleContext<'_>,
    predicates: Vec<Expression>,
    left_schema: &Schema,
    right_schema: &Schema,
    propagate_constant: bool,
) -> Vec<Expression> {
    if predicates.is_empty() {
        return predicates;
    }
    let mut predicates = predicates
        .iter()
        .map(|predicate| push_down_not(predicate, ctx.builder))
        .collect::<Vec<_>>();
    if propagate_constant {
        let outcome = tidb_expr::constant_propagation::propagate_constant_for_join(
            ctx.builder,
            ctx.use_plan_cache,
            ctx.always_keep_join_key,
            left_schema,
            right_schema,
            predicates,
            None,
        );
        if let (Some(marker), Some(reason)) =
            (ctx.plan_cache_marker, outcome.skip_plan_cache_reason)
        {
            marker.set_skip_plan_cache(reason);
        }
        predicates = outcome.conditions;
    } else {
        let outcome = tidb_expr::constant_propagation::propagate_constant(
            ctx.builder,
            ctx.use_plan_cache,
            predicates.clone(),
            None,
        );
        if outcome.conditions.len() == 1 {
            predicates = outcome.conditions;
        }
    }
    predicates = short_circuit(ctx, predicates);
    predicates = merge_in_and_not_equal(ctx, predicates);
    predicates = predicates
        .into_iter()
        .map(|predicate| remove_redundant_or(ctx, predicate))
        .collect();
    predicates = prune_empty_or_branches(ctx, predicates);
    predicates
        .into_iter()
        .filter(|predicate| predicate_type(ctx, predicate).1 != PredicateType::True)
        .collect()
}

/// Go `PredicateSimplification` rule. Only DataSource overrides the recursive
/// base body; LogicalSelection's override is an assertion and returns the base walk.
#[derive(Debug)]
pub struct PredicateSimplification;

impl LogicalOptRule for PredicateSimplification {
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        Ok((predicate_simplification(ctx, plan), false))
    }

    fn name(&self) -> &'static str {
        "predicate_simplification"
    }
}

/// Go's recursive `PredicateSimplification` plan-interface walk.
#[must_use]
pub fn predicate_simplification(ctx: &RuleContext<'_>, mut plan: LogicalPlan) -> LogicalPlan {
    let children = plan.base_mut().take_children();
    plan.set_children(
        children
            .into_iter()
            .map(|child| predicate_simplification(ctx, child))
            .collect(),
    );
    if let LogicalPlan::DataSource(source) = &mut plan {
        source.pushed_down_conds = apply_predicate_simplification(
            ctx,
            std::mem::take(&mut source.pushed_down_conds),
            true,
        );
        source.all_conds =
            apply_predicate_simplification(ctx, std::mem::take(&mut source.all_conds), true);
    }
    plan
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_expr::expr_util::builder::{FunctionBuilder, RealFunctionBuilder};
    use tidb_expr::scalar_function::ScalarFunction;
    use tidb_expr::NoColumns;

    use super::*;
    use crate::logical::rule_tests::test_context;
    use crate::plan_base::PlanIdAllocator;

    fn integer_type() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn column(id: i64) -> Expression {
        Expression::Column(Column::new(id, integer_type()))
    }

    fn integer(value: i64) -> Expression {
        Expression::Constant(Constant::new(Datum::Int(value), integer_type()))
    }

    fn function(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new(name),
            FieldType::new(FieldTypeCode::Tiny),
            args,
        ))
    }

    #[test]
    fn logical_constants_and_in_not_equal_follow_go_order() {
        let allocator = PlanIdAllocator::new();
        let ctx = test_context(&allocator);
        let comparison = function("eq", vec![column(1), integer(7)]);
        let conditions = apply_predicate_simplification(
            &ctx,
            vec![function(
                "and",
                vec![comparison, Expression::Constant(Constant::new_zero())],
            )],
            false,
        );
        assert_eq!(conditions.len(), 1);
        assert!(is_unsatisfiable_expression(&ctx, &conditions[0]));

        let conditions = apply_predicate_simplification(
            &ctx,
            vec![
                function("in", vec![column(1), integer(1), integer(2), integer(3)]),
                function("ne", vec![column(1), integer(2)]),
            ],
            false,
        );
        assert_eq!(conditions.len(), 1);
        let Expression::ScalarFunction(in_function) = &conditions[0] else {
            panic!("IN must remain after subtracting NE")
        };
        assert_eq!(in_function.func_name.lowercase(), "in");
        assert_eq!(in_function.get_args().len(), 3);
        assert!(matches!(
            in_function.get_args(),
            [Expression::Column(_), Expression::Constant(one), Expression::Constant(three)]
                if matches!(one.value, Datum::Int(1)) && matches!(three.value, Datum::Int(3))
        ));
    }

    #[test]
    fn scalar_predicate_prunes_only_unsatisfiable_or_branch() {
        let allocator = PlanIdAllocator::new();
        let builder = RealFunctionBuilder::new(&NoColumns);
        let mut ctx = test_context(&allocator);
        ctx.builder = &builder;
        let build = |name, args| builder.new_function(name, None, args).unwrap();
        let equal_one = build("eq", vec![column(1), integer(1)]);
        let greater_two = build("gt", vec![column(1), integer(2)]);
        let less_three = build("lt", vec![column(1), integer(3)]);
        let disjunction = build("or", vec![greater_two, less_three.clone()]);
        let result = prune_empty_or_branches(&ctx, vec![equal_one, disjunction]);
        assert_eq!(result.len(), 2);
        assert!(result[1].equal(&less_three));
    }
}
