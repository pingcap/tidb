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

//! Go `pkg/expression/constant_propagation.go`.

use std::collections::BTreeMap;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use crate::column::Column;
use crate::constant::Constant;
use crate::expr_util::builder::FunctionBuilder;
use crate::expr_util::normal_form::{split_cnf_items, split_dnf_items};
use crate::expr_util::predicates::{
    is_mutable_effects_expr, maybe_over_optimized_4_plan_cache, remove_dup_exprs,
};
use crate::expr_util::substitute::{column_substitute_impl, SubstituteOptions};
use crate::expression::Expression;
use crate::schema::Schema;
use crate::simple_expr::{compose_cnf_condition, compose_dnf_condition, extract_columns};

/// Go `MaxPropagateColsCnt`.
pub const MAX_PROPAGATE_COLUMNS: usize = 100;

/// Go's constant-propagation result plus its `SetSkipPlanCache` side effect.
#[derive(Clone, Debug)]
pub struct PropagationOutcome {
    /// Rewritten CNF conditions.
    pub conditions: Vec<Expression>,
    /// The first Go skip-plan-cache reason, when propagation used mutable values.
    pub skip_plan_cache_reason: Option<&'static str>,
}

fn bool_constant(value: bool) -> Expression {
    Expression::Constant(Constant::new(
        Datum::Int(i64::from(value)),
        FieldType::new(FieldTypeCode::Tiny),
    ))
}

fn false_constant() -> Expression {
    bool_constant(false)
}

fn is_false_constant(expression: &Expression) -> bool {
    let Expression::Constant(constant) = expression else {
        return false;
    };
    if constant.param_marker.is_some() || constant.deferred_expr.is_some() {
        return false;
    }
    matches!(constant.value, Datum::Null | Datum::Int(0) | Datum::UInt(0))
}

fn equal_column_constant(expression: &Expression) -> Option<(&Column, &Constant)> {
    let Expression::ScalarFunction(function) = expression else {
        return None;
    };
    if function.func_name.lowercase() != "eq" {
        return None;
    }
    match function.get_args() {
        [Expression::Column(column), Expression::Constant(constant)]
        | [Expression::Constant(constant), Expression::Column(column)]
            if column.get_static_type()?.collation_name()
                == constant.get_static_type()?.collation_name() =>
        {
            Some((column, constant))
        }
        _ => None,
    }
}

fn column_equality(expression: &Expression) -> Option<(&Column, &Column)> {
    let Expression::ScalarFunction(function) = expression else {
        return None;
    };
    if function.func_name.lowercase() != "eq" {
        return None;
    }
    match function.get_args() {
        [Expression::Column(left), Expression::Column(right)]
            if left.get_static_type()?.collation_name()
                == right.get_static_type()?.collation_name()
                && !left.get_static_type()?.is_hybrid()
                && !right.get_static_type()?.is_hybrid() =>
        {
            Some((left, right))
        }
        _ => None,
    }
}

fn root(parent: &mut [usize], mut node: usize) -> usize {
    while parent[node] != node {
        parent[node] = parent[parent[node]];
        node = parent[node];
    }
    node
}

fn substitute(
    expression: &Expression,
    columns: Vec<Column>,
    replacements: Vec<Expression>,
    builder: &dyn FunctionBuilder,
) -> Expression {
    let options = SubstituteOptions {
        builder,
        constant_propagate_check: true,
        new_collation_enabled: true,
    };
    column_substitute_impl(
        expression,
        &Schema::new(columns),
        &replacements,
        false,
        &options,
    )
    .expr
}

fn propagate_constant_equalities(
    builder: &dyn FunctionBuilder,
    use_plan_cache: bool,
    columns: &BTreeMap<i64, Column>,
    conditions: &mut Vec<Expression>,
) -> Option<&'static str> {
    let mut known = BTreeMap::<i64, Constant>::new();
    let mut visited = vec![false; conditions.len()];
    let mut skip_reason = None;
    for _ in 0..MAX_PROPAGATE_COLUMNS {
        let mut fresh = Vec::<(Column, Constant)>::new();
        for offset in 0..conditions.len() {
            if visited.get(offset).copied().unwrap_or(false) {
                continue;
            }
            if is_false_constant(&conditions[offset]) {
                if maybe_over_optimized_4_plan_cache(use_plan_cache, conditions) {
                    skip_reason =
                        Some("some parameters may be overwritten when constant propagation");
                }
                *conditions = vec![false_constant()];
                return skip_reason;
            }
            let Some((column, constant)) = equal_column_constant(&conditions[offset]) else {
                continue;
            };
            if column.get_static_type().is_some_and(FieldType::is_hybrid) {
                continue;
            }
            visited[offset] = true;
            match known.get(&column.unique_id) {
                Some(previous)
                    if !Expression::Constant(previous.clone())
                        .equal(&Expression::Constant(constant.clone())) =>
                {
                    if maybe_over_optimized_4_plan_cache(use_plan_cache, conditions) {
                        skip_reason =
                            Some("some parameters may be overwritten when constant propagation");
                    }
                    *conditions = vec![false_constant()];
                    return skip_reason;
                }
                Some(_) => {}
                None => {
                    known.insert(column.unique_id, constant.clone());
                    fresh.push((column.clone(), constant.clone()));
                }
            }
        }
        if fresh.is_empty() {
            break;
        }
        let source_columns: Vec<Column> = fresh.iter().map(|(column, _)| column.clone()).collect();
        let replacements: Vec<Expression> = fresh
            .into_iter()
            .map(|(_, constant)| Expression::Constant(constant))
            .collect();
        for (offset, condition) in conditions.iter_mut().enumerate() {
            if !visited.get(offset).copied().unwrap_or(false) {
                *condition = substitute(
                    condition,
                    source_columns.clone(),
                    replacements.clone(),
                    builder,
                );
            }
        }
    }
    let _ = columns;
    skip_reason
}

fn propagate_column_equalities(
    builder: &dyn FunctionBuilder,
    columns: &BTreeMap<i64, Column>,
    conditions: &mut Vec<Expression>,
    valid: Option<&dyn Fn(&Expression) -> bool>,
) {
    let ids = columns.keys().copied().collect::<Vec<_>>();
    let offsets = ids
        .iter()
        .enumerate()
        .map(|(offset, id)| (*id, offset))
        .collect::<BTreeMap<_, _>>();
    let mut parent = (0..ids.len()).collect::<Vec<_>>();
    let mut equality = vec![false; conditions.len()];
    for (offset, condition) in conditions.iter().enumerate() {
        let Some((left, right)) = column_equality(condition) else {
            continue;
        };
        equality[offset] = true;
        let left = offsets[&left.unique_id];
        let right = offsets[&right.unique_id];
        let left_root = root(&mut parent, left);
        let right_root = root(&mut parent, right);
        if left_root != right_root {
            parent[right_root] = left_root;
        }
    }
    let original_len = conditions.len();
    for left_offset in 0..ids.len() {
        for right_offset in left_offset + 1..ids.len() {
            if root(&mut parent, left_offset) != root(&mut parent, right_offset) {
                continue;
            }
            let left = columns[&ids[left_offset]].clone();
            let right = columns[&ids[right_offset]].clone();
            if left.get_static_type().map(FieldType::code)
                != right.get_static_type().map(FieldType::code)
            {
                continue;
            }
            for condition_offset in 0..original_len {
                if equality[condition_offset]
                    || is_mutable_effects_expr(&conditions[condition_offset])
                {
                    continue;
                }
                for (source, target) in [(&left, &right), (&right, &left)] {
                    let outcome = substitute(
                        &conditions[condition_offset],
                        vec![source.clone()],
                        vec![Expression::Column(target.clone())],
                        builder,
                    );
                    if !outcome.equal(&conditions[condition_offset])
                        && (matches!(outcome, Expression::Constant(_))
                            || valid.is_none_or(|filter| filter(&outcome)))
                    {
                        conditions.push(outcome);
                    }
                }
            }
        }
    }
}

fn propagate_dnf(
    builder: &dyn FunctionBuilder,
    use_plan_cache: bool,
    conditions: &mut [Expression],
    valid: Option<&dyn Fn(&Expression) -> bool>,
) -> Option<&'static str> {
    let mut skip_reason = None;
    for condition in conditions {
        let Expression::ScalarFunction(function) = condition else {
            continue;
        };
        if function.func_name.lowercase() != "or" {
            continue;
        }
        let mut items = Vec::new();
        for item in split_dnf_items(condition) {
            let outcome =
                propagate_constant(builder, use_plan_cache, split_cnf_items(&item), valid);
            skip_reason = skip_reason.or(outcome.skip_plan_cache_reason);
            if let Some(composed) = compose_cnf_condition(outcome.conditions) {
                items.push(composed);
            }
        }
        if let Some(composed) = compose_dnf_condition(items) {
            *condition = composed;
        }
    }
    skip_reason
}

fn clone_join_keys(
    conditions: &[Expression],
    left: Option<&Schema>,
    right: Option<&Schema>,
) -> Vec<Expression> {
    let (Some(left), Some(right)) = (left, right) else {
        return Vec::new();
    };
    conditions
        .iter()
        .filter(|condition| {
            column_equality(condition).is_some_and(|(first, second)| {
                (left.contains(first) && right.contains(second))
                    || (left.contains(second) && right.contains(first))
            })
        })
        .cloned()
        .collect()
}

fn solve(
    builder: &dyn FunctionBuilder,
    use_plan_cache: bool,
    mut conditions: Vec<Expression>,
    keep_join_keys: bool,
    left: Option<&Schema>,
    right: Option<&Schema>,
    valid: Option<&dyn Fn(&Expression) -> bool>,
) -> PropagationOutcome {
    conditions = conditions.iter().flat_map(split_cnf_items).collect();
    let mut columns = BTreeMap::new();
    for condition in &conditions {
        for column in extract_columns(condition) {
            columns.entry(column.unique_id).or_insert(column);
        }
    }
    if columns.len() > MAX_PROPAGATE_COLUMNS {
        return PropagationOutcome {
            conditions,
            skip_plan_cache_reason: None,
        };
    }
    let join_keys = keep_join_keys.then(|| clone_join_keys(&conditions, left, right));
    let mut skip_reason =
        propagate_constant_equalities(builder, use_plan_cache, &columns, &mut conditions);
    if !conditions.iter().any(is_false_constant) {
        propagate_column_equalities(builder, &columns, &mut conditions, valid);
        skip_reason = skip_reason.or(propagate_dnf(
            builder,
            use_plan_cache,
            &mut conditions,
            valid,
        ));
        if let Some(join_keys) = join_keys {
            conditions.extend(join_keys);
        }
        conditions = remove_dup_exprs(conditions);
    }
    PropagationOutcome {
        conditions,
        skip_plan_cache_reason: skip_reason,
    }
}

/// Go `PropagateConstant`.
#[must_use]
pub fn propagate_constant(
    builder: &dyn FunctionBuilder,
    use_plan_cache: bool,
    conditions: Vec<Expression>,
    valid: Option<&dyn Fn(&Expression) -> bool>,
) -> PropagationOutcome {
    solve(
        builder,
        use_plan_cache,
        conditions,
        false,
        None,
        None,
        valid,
    )
}

/// Go `PropagateConstantForJoin`.
#[must_use]
pub fn propagate_constant_for_join(
    builder: &dyn FunctionBuilder,
    use_plan_cache: bool,
    keep_join_keys: bool,
    left: &Schema,
    right: &Schema,
    conditions: Vec<Expression>,
    valid: Option<&dyn Fn(&Expression) -> bool>,
) -> PropagationOutcome {
    solve(
        builder,
        use_plan_cache,
        conditions,
        keep_join_keys,
        Some(left),
        Some(right),
        valid,
    )
}
