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

use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};

use crate::column::Column;
use crate::constant::Constant;
use crate::expr_util::builder::FunctionBuilder;
use crate::expr_util::normal_form::{split_cnf_items, split_dnf_items};
use crate::expr_util::predicates::{maybe_over_optimized_4_plan_cache, remove_dup_exprs};
use crate::expr_util::substitute::{
    build_not_null_expr, column_substitute_impl, SubstituteOptions,
};
use crate::expression::Expression;
use crate::scalar_function::is_unfoldable_function;
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

/// Go `PropConstForOuterJoin`'s two condition lists plus its plan-cache side effect.
#[derive(Clone, Debug)]
pub struct OuterJoinPropagationOutcome {
    /// Rewritten ON conditions.
    pub join_conditions: Vec<Expression>,
    /// Rewritten predicates above the join.
    pub filter_conditions: Vec<Expression>,
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
    let (left, right) = raw_column_equality(expression)?;
    (left.get_static_type()?.collation_name() == right.get_static_type()?.collation_name()
        && !left.get_static_type()?.is_hybrid()
        && !right.get_static_type()?.is_hybrid())
    .then_some((left, right))
}

fn raw_column_equality(expression: &Expression) -> Option<(&Column, &Column)> {
    let Expression::ScalarFunction(function) = expression else {
        return None;
    };
    if function.func_name.lowercase() != "eq" {
        return None;
    }
    match function.get_args() {
        [Expression::Column(left), Expression::Column(right)] => Some((left, right)),
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
    for offset in 0..conditions.len() {
        let condition = &conditions[offset];
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
        } else if left != right {
            conditions[offset] = bool_constant(true);
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
                if equality[condition_offset] {
                    continue;
                }
                conditions[condition_offset] = replace_eq_condition_with_true(
                    builder,
                    &conditions[condition_offset],
                    &left,
                    &right,
                );
                for (source, target) in [(&left, &right), (&right, &left)] {
                    let (replaced, nondeterministic, outcome) = try_replace_column(
                        builder,
                        &conditions[condition_offset],
                        source,
                        target,
                        false,
                    );
                    if replaced
                        && !nondeterministic
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

fn replace_eq_condition_with_true(
    builder: &dyn FunctionBuilder,
    condition: &Expression,
    source: &Column,
    target: &Column,
) -> Expression {
    if source.get_static_type().map(FieldType::code)
        != target.get_static_type().map(FieldType::code)
    {
        return condition.clone();
    }
    let Expression::ScalarFunction(function) = condition else {
        return condition.clone();
    };
    let args = function.get_args();
    let is_column = |expression: &Expression, column: &Column| matches!(expression, Expression::Column(candidate) if candidate.unique_id == column.unique_id);
    match function.func_name.lowercase() {
        "in" if source.get_static_type().is_some_and(|field_type| {
            field_type.eval_type() != tidb_datatype::EvalType::String
        }) && target.get_static_type().is_some_and(|field_type| {
            field_type.eval_type() != tidb_datatype::EvalType::String
        }) =>
        {
            let matched = args.first().is_some_and(|first| {
                (is_column(first, source)
                    && args[1..].iter().any(|argument| is_column(argument, target)))
                    || (is_column(first, target)
                        && args[1..].iter().any(|argument| is_column(argument, source)))
            });
            if matched {
                return bool_constant(true);
            }
        }
        "eq" if args.len() == 2 => {
            if (is_column(&args[0], source) && is_column(&args[1], target))
                || (is_column(&args[0], target) && is_column(&args[1], source))
            {
                return bool_constant(true);
            }
        }
        "or" | "and" => {
            let rewritten = args
                .iter()
                .map(|argument| replace_eq_condition_with_true(builder, argument, source, target))
                .collect::<Vec<_>>();
            if rewritten
                .iter()
                .zip(args)
                .any(|(rewritten, original)| !rewritten.equal(original))
            {
                if let Ok(expression) = builder.new_function(
                    function.func_name.lowercase(),
                    function.ret_type.clone(),
                    rewritten,
                ) {
                    return expression;
                }
            }
        }
        _ => {}
    }
    condition.clone()
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
            raw_column_equality(condition).is_some_and(|(first, second)| {
                (left.contains(first) && right.contains(second))
                    || (left.contains(second) && right.contains(first))
            })
        })
        .cloned()
        .collect()
}

fn expression_from_schema(expression: &Expression, schema: &Schema) -> bool {
    extract_columns(expression)
        .iter()
        .all(|column| schema.contains(column))
}

fn outer_inner_columns<'a>(
    left: &'a Column,
    right: &'a Column,
    outer: &Schema,
    inner: &Schema,
) -> Option<(&'a Column, &'a Column)> {
    if outer.contains(left) && inner.contains(right) {
        Some((left, right))
    } else if outer.contains(right) && inner.contains(left) {
        Some((right, left))
    } else {
        None
    }
}

fn outer_join_key<'a>(
    expression: &'a Expression,
    outer: &Schema,
    inner: &Schema,
) -> Option<(&'a Column, &'a Column)> {
    let (left, right) = raw_column_equality(expression)?;
    (left.get_static_type()?.collation_name() == right.get_static_type()?.collation_name())
        .then(|| outer_inner_columns(left, right, outer, inner))?
}

fn replace_outer_column(
    builder: &dyn FunctionBuilder,
    condition: &Expression,
    outer: &Column,
    inner: &Column,
) -> Option<Expression> {
    if outer.get_static_type().map(FieldType::code) != inner.get_static_type().map(FieldType::code)
    {
        return None;
    }
    let (replaced, nondeterministic, expression) =
        try_replace_column(builder, condition, outer, inner, true);
    (replaced && !nondeterministic).then_some(expression)
}

fn try_replace_column(
    builder: &dyn FunctionBuilder,
    condition: &Expression,
    source: &Column,
    target: &Column,
    null_aware: bool,
) -> (bool, bool, Expression) {
    let Expression::ScalarFunction(function) = condition else {
        return (false, false, condition.clone());
    };
    let name = function.func_name.lowercase();
    if is_unfoldable_function(name) || name == "isnull" {
        return (false, true, condition.clone());
    }
    if null_aware && matches!(name, "ifnull" | "if" | "case" | "nulleq") {
        return (false, true, condition.clone());
    }
    let mut replaced = false;
    let mut args = function.args.clone();
    for (offset, argument) in function.args.iter().enumerate() {
        if matches!(argument, Expression::Column(column) if column.unique_id == source.unique_id) {
            if target.get_static_type().map(FieldType::collation_name)
                != Some(function.derived_collation().name())
            {
                continue;
            }
            args[offset] = Expression::Column(target.clone());
            replaced = true;
            continue;
        }
        let (sub_replaced, nondeterministic, rewritten) =
            try_replace_column(builder, argument, source, target, null_aware);
        if nondeterministic {
            return (false, true, condition.clone());
        }
        if sub_replaced {
            args[offset] = rewritten;
            replaced = true;
        }
    }
    if !replaced {
        return (false, false, condition.clone());
    }
    match builder.new_function(name, function.ret_type.clone(), args) {
        Ok(expression) => (true, false, expression),
        Err(_) => (false, false, condition.clone()),
    }
}

fn pick_outer_constants(
    conditions: &[Expression],
    visited: &mut [bool],
    outer: &Schema,
    known: &mut BTreeMap<i64, Constant>,
    fresh: &mut Vec<(Column, Constant)>,
) -> Result<(), Option<Constant>> {
    for (offset, condition) in conditions.iter().enumerate() {
        if visited[offset] {
            continue;
        }
        if is_false_constant(condition) {
            return Err(None);
        }
        let Some((column, constant)) = equal_column_constant(condition) else {
            continue;
        };
        if !outer.contains(column) || column.get_static_type().is_some_and(FieldType::is_hybrid) {
            continue;
        }
        visited[offset] = true;
        match known.get(&column.unique_id) {
            Some(previous)
                if !Expression::Constant(previous.clone())
                    .equal(&Expression::Constant(constant.clone())) =>
            {
                return Err(Some(constant.clone()));
            }
            Some(_) => {}
            None => {
                known.insert(column.unique_id, constant.clone());
                fresh.push((column.clone(), constant.clone()));
            }
        }
    }
    Ok(())
}

fn propagate_outer_constants(
    builder: &dyn FunctionBuilder,
    use_plan_cache: bool,
    outer: &Schema,
    join_conditions: &mut Vec<Expression>,
    filter_conditions: &mut Vec<Expression>,
) -> Option<&'static str> {
    let mut known = BTreeMap::<i64, Constant>::new();
    let mut join_visited = vec![false; join_conditions.len()];
    let mut filter_visited = vec![false; filter_conditions.len()];
    let mut skip_reason = None;
    for _ in 0..MAX_PROPAGATE_COLUMNS {
        let mut fresh = Vec::new();
        if let Err(conflict) = pick_outer_constants(
            filter_conditions,
            &mut filter_visited,
            outer,
            &mut known,
            &mut fresh,
        ) {
            *join_conditions = vec![false_constant()];
            *filter_conditions = vec![false_constant()];
            if conflict.is_some_and(|constant| {
                maybe_over_optimized_4_plan_cache(use_plan_cache, &[Expression::Constant(constant)])
            }) {
                skip_reason = Some("some parameters may be overwritten when constant propagation");
            }
            return skip_reason;
        }
        if let Err(conflict) = pick_outer_constants(
            join_conditions,
            &mut join_visited,
            outer,
            &mut known,
            &mut fresh,
        ) {
            *join_conditions = vec![false_constant()];
            if conflict.is_some_and(|constant| {
                maybe_over_optimized_4_plan_cache(use_plan_cache, &[Expression::Constant(constant)])
            }) {
                skip_reason = Some("some parameters may be overwritten when constant propagation");
            }
            return skip_reason;
        }
        if fresh.is_empty() {
            break;
        }
        let columns: Vec<Column> = fresh.iter().map(|(column, _)| column.clone()).collect();
        let constants: Vec<Expression> = fresh
            .into_iter()
            .map(|(_, constant)| Expression::Constant(constant))
            .collect();
        for (offset, condition) in join_conditions.iter_mut().enumerate() {
            if !join_visited.get(offset).copied().unwrap_or(false) {
                *condition = substitute(condition, columns.clone(), constants.clone(), builder);
            }
        }
    }
    skip_reason
}

fn propagate_outer_columns(
    builder: &dyn FunctionBuilder,
    outer: &Schema,
    inner: &Schema,
    null_sensitive: bool,
    join_conditions: &mut Vec<Expression>,
    filter_conditions: &[Expression],
    valid: Option<&dyn Fn(&Expression) -> bool>,
) {
    if null_sensitive {
        return;
    }
    let original_join_len = join_conditions.len();
    let direct_keys = join_conditions[..original_join_len]
        .iter()
        .filter_map(|condition| {
            outer_join_key(condition, outer, inner)
                .map(|(outer_column, inner_column)| (outer_column.clone(), inner_column.clone()))
        })
        .collect::<Vec<_>>();
    let opts = SubstituteOptions::new(builder);
    for (_, inner_column) in &direct_keys {
        if !inner_column
            .get_static_type()
            .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL))
        {
            if let Ok(condition) =
                build_not_null_expr(Expression::Column(inner_column.clone()), &opts)
            {
                join_conditions.push(condition);
            }
        }
    }

    let mut columns = BTreeMap::new();
    for condition in join_conditions.iter().chain(filter_conditions) {
        for column in extract_columns(condition) {
            columns.entry(column.unique_id).or_insert(column);
        }
    }
    let ids = columns.keys().copied().collect::<Vec<_>>();
    let offsets = ids
        .iter()
        .enumerate()
        .map(|(offset, id)| (*id, offset))
        .collect::<BTreeMap<_, _>>();
    let mut parent = (0..ids.len()).collect::<Vec<_>>();
    for (outer_column, inner_column) in &direct_keys {
        let outer_offset = offsets[&outer_column.unique_id];
        let inner_offset = offsets[&inner_column.unique_id];
        let outer_root = root(&mut parent, outer_offset);
        let inner_root = root(&mut parent, inner_offset);
        if outer_root != inner_root {
            parent[inner_root] = outer_root;
        }
    }

    let mut merged_columns = outer.columns.clone();
    merged_columns.extend(inner.columns.iter().cloned());
    let merged = Schema::new(merged_columns);
    let original_join_conditions = join_conditions[..original_join_len].to_vec();
    for left_offset in 0..ids.len() {
        for right_offset in left_offset + 1..ids.len() {
            if root(&mut parent, left_offset) != root(&mut parent, right_offset) {
                continue;
            }
            let Some((outer_column, inner_column)) = outer_inner_columns(
                &columns[&ids[left_offset]],
                &columns[&ids[right_offset]],
                outer,
                inner,
            ) else {
                continue;
            };
            for condition in &original_join_conditions {
                if outer_join_key(condition, outer, inner).is_some()
                    || !expression_from_schema(condition, &merged)
                {
                    continue;
                }
                if let Some(derived) =
                    replace_outer_column(builder, condition, outer_column, inner_column)
                {
                    if matches!(derived, Expression::Constant(_))
                        || valid.is_none_or(|filter| filter(&derived))
                    {
                        join_conditions.push(derived);
                    }
                }
            }
            for condition in filter_conditions {
                if !expression_from_schema(condition, outer) {
                    continue;
                }
                if let Some(derived) =
                    replace_outer_column(builder, condition, outer_column, inner_column)
                {
                    if matches!(derived, Expression::Constant(_))
                        || valid.is_none_or(|filter| filter(&derived))
                    {
                        join_conditions.push(derived);
                    }
                }
            }
        }
    }
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

/// Go `PropConstForOuterJoin`.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn propagate_constant_for_outer_join(
    builder: &dyn FunctionBuilder,
    use_plan_cache: bool,
    mut join_conditions: Vec<Expression>,
    mut filter_conditions: Vec<Expression>,
    outer: &Schema,
    inner: &Schema,
    keep_join_keys: bool,
    null_sensitive: bool,
    valid: Option<&dyn Fn(&Expression) -> bool>,
) -> OuterJoinPropagationOutcome {
    join_conditions = join_conditions.iter().flat_map(split_cnf_items).collect();
    filter_conditions = filter_conditions.iter().flat_map(split_cnf_items).collect();
    let mut columns = BTreeMap::new();
    for condition in join_conditions.iter().chain(&filter_conditions) {
        for column in extract_columns(condition) {
            columns.entry(column.unique_id).or_insert(column);
        }
    }
    if columns.len() > MAX_PROPAGATE_COLUMNS {
        return OuterJoinPropagationOutcome {
            join_conditions,
            filter_conditions,
            skip_plan_cache_reason: None,
        };
    }
    let join_keys =
        keep_join_keys.then(|| clone_join_keys(&join_conditions, Some(outer), Some(inner)));
    let mut skip_plan_cache_reason = propagate_outer_constants(
        builder,
        use_plan_cache,
        outer,
        &mut join_conditions,
        &mut filter_conditions,
    );
    propagate_outer_columns(
        builder,
        outer,
        inner,
        null_sensitive,
        &mut join_conditions,
        &filter_conditions,
        valid,
    );
    skip_plan_cache_reason = skip_plan_cache_reason.or(propagate_dnf(
        builder,
        use_plan_cache,
        &mut join_conditions,
        valid,
    ));
    skip_plan_cache_reason = skip_plan_cache_reason.or(propagate_dnf(
        builder,
        use_plan_cache,
        &mut filter_conditions,
        valid,
    ));
    if let Some(join_keys) = join_keys {
        join_conditions.extend(join_keys);
    }
    join_conditions = remove_dup_exprs(join_conditions);
    OuterJoinPropagationOutcome {
        join_conditions,
        filter_conditions,
        skip_plan_cache_reason,
    }
}
