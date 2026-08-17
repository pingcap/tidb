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

//! NOT push-down and redundant-cast elimination.
//!
//! Go sources: `pkg/expression/util.go:915`-`:926` (`pushNotAcrossArgs`),
//! `:927`-`:956` (`noPrecisionLossCastCompatible`), `:957`-`:992`
//! (`unwrapCast`), `:993`-`:1077` (`eliminateCastFunction`), `:1078`-`:1128`
//! (`pushNotAcrossExpr`), `:1142` (`PushDownNot`), `:1151`
//! (`EliminateNoPrecisionLossCast`).
//!
//! Both rewrites exist to make a predicate RANGE-BUILDABLE: an index can serve
//! `a > 1` but not `NOT (a <= 1)`, and not `CAST(a AS CHAR(100)) = 'x'` either.

use super::builder::{tiny_int_type, FunctionBuilder};
use super::normal_form::{flatten_cnf_conditions, flatten_dnf_conditions};
use super::traits::opposite_op;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use crate::simple_expr::{compose_cnf_condition, compose_dnf_condition};
use tidb_datatype::FieldType;

/// Go `pushNotAcrossArgs` (`util.go:915`).
fn push_not_across_args(
    exprs: &[Expression],
    not: bool,
    builder: &dyn FunctionBuilder,
) -> (Vec<Expression>, bool) {
    let mut new_exprs = Vec::with_capacity(exprs.len());
    let mut flag = false;
    for expr in exprs {
        let (new_expr, changed) = push_not_across_expr(expr, not, builder);
        flag = changed || flag;
        new_exprs.push(new_expr);
    }
    (new_exprs, flag)
}

/// Go `pushNotAcrossExpr` (`util.go:1078`): eliminates `NOT` by pushing it
/// into the expression tree.
///
/// `not` says whether a `NOT` from the parent still has to be applied here.
/// Logical connectives cancel a double `NOT` and flip through De Morgan;
/// anything else gets wrapped in `istrue_with_null` so three-valued logic
/// survives -- `NOT (NULL)` is NULL, not TRUE, and dropping the wrapper would
/// lose that.
///
/// The returned flag is Go's `changed`: whether the output differs from the
/// input BECAUSE of the pushed-down not.
fn push_not_across_expr(
    expr: &Expression,
    not: bool,
    builder: &dyn FunctionBuilder,
) -> (Expression, bool) {
    if let Expression::ScalarFunction(function) = expr {
        let name = function.func_name.lowercase();
        match name {
            "not" => {
                let Some(arg0) = function.get_args().first() else {
                    return (expr.clone(), false);
                };
                let Ok(child) = builder.wrap_with_is_true(arg0.clone()) else {
                    return (expr.clone(), false);
                };
                let (child_expr, changed) = push_not_across_expr(&child, !not, builder);
                if !changed && !not {
                    // The double negation cancelled and nothing below moved, so
                    // the original is already in its simplest form.
                    return (expr.clone(), false);
                }
                return (child_expr, true);
            }
            "lt" | "ge" | "gt" | "le" | "eq" | "ne" => {
                if not {
                    let opposite =
                        opposite_op(name).expect("every comparison above is in the table");
                    let built = builder.new_function(
                        opposite,
                        function.ret_type.clone(),
                        function.get_args().to_vec(),
                    );
                    return match built {
                        Ok(built) => (built, true),
                        Err(_) => (expr.clone(), false),
                    };
                }
                let (new_args, changed) = push_not_across_args(function.get_args(), false, builder);
                if !changed {
                    return (expr.clone(), false);
                }
                let built = builder.new_function(name, function.ret_type.clone(), new_args);
                return match built {
                    Ok(built) => (built, true),
                    Err(_) => (expr.clone(), false),
                };
            }
            "and" | "or" => {
                let (new_args, changed, func_name) = if not {
                    // De Morgan: the connective flips and the NOT descends into
                    // both arguments.
                    let (new_args, _) = push_not_across_args(function.get_args(), true, builder);
                    let flipped = opposite_op(name).expect("and/or are in the table");
                    (new_args, true, flipped)
                } else {
                    let (new_args, changed) =
                        push_not_across_args(function.get_args(), false, builder);
                    (new_args, changed, name)
                };
                if !changed {
                    return (expr.clone(), false);
                }
                let built = builder.new_function(func_name, function.ret_type.clone(), new_args);
                return match built {
                    Ok(built) => (built, true),
                    Err(_) => (expr.clone(), false),
                };
            }
            _ => {}
        }
    }
    // A leaf, or a function with no negation rule: re-apply the NOT literally.
    if not {
        let built = builder.new_function("not", Some(tiny_int_type()), vec![expr.clone()]);
        return match built {
            Ok(built) => (built, true),
            Err(_) => (expr.clone(), false),
        };
    }
    (expr.clone(), false)
}

/// Go `PushDownNot` (`util.go:1142`): pushes every `NOT` down to the leaves.
#[must_use]
pub fn push_down_not(expr: &Expression, builder: &dyn FunctionBuilder) -> Expression {
    push_not_across_expr(expr, false, builder).0
}

/// Go `noPrecisionLossCastCompatible` (`util.go:927`): whether casting
/// `arg_col` to `cast` loses nothing.
///
/// Only two families qualify. VARCHAR and integer are the cases where the
/// storage encoding is effectively the same, so removing the cast cannot
/// change a comparison. CHAR is deliberately excluded -- its padding makes the
/// stored form differ.
#[must_use]
pub fn no_precision_loss_cast_compatible(cast: &FieldType, arg_col: &FieldType) -> bool {
    let both_varchar = cast.code().is_type_varchar() && arg_col.code().is_type_varchar();
    let both_integer = tidb_mysql::util::is_integer_type(cast.code().mysql_type())
        && tidb_mysql::util::is_integer_type(arg_col.code().mysql_type());
    if !both_varchar && !both_integer {
        return false;
    }
    if cast.code().is_type_varchar() {
        // A varchar cast may only EXTEND the length.
        if cast.flen() < arg_col.flen() {
            return false;
        }
        tidb_datatype::compatible_collate(cast.collation_name(), arg_col.collation_name())
    } else {
        // For integers `flen` is only a display width, so compare the types'
        // DEFAULT lengths instead, and require the signedness to match.
        let (cast_flen, _) =
            tidb_mysql::util::default_field_length_and_decimal(cast.code().mysql_type());
        let (origin_flen, _) =
            tidb_mysql::util::default_field_length_and_decimal(arg_col.code().mysql_type());
        if cast_flen < origin_flen {
            return false;
        }
        cast.is_unsigned() == arg_col.is_unsigned()
    }
}

/// Go `unwrapCast` (`util.go:957`): removes a `CAST` from one side of a
/// comparison when the other side is a constant and the cast is lossless.
///
/// `cast_offset` selects which argument to unwrap, so a caller tries both.
fn unwrap_cast(
    parent: &ScalarFunction,
    cast_offset: usize,
    builder: &dyn FunctionBuilder,
) -> Option<Expression> {
    let (_, collation) = parent.collation.charset_and_collation();
    let collation = collation.to_owned();
    let args = parent.get_args();
    if args.len() != 2 {
        return None;
    }
    let Expression::ScalarFunction(cast) = &args[cast_offset] else {
        return None;
    };
    if cast.func_name.lowercase() != "cast" {
        return None;
    }
    let cast_type = cast.ret_type.as_ref()?;
    // An incompatible collation means the condition cannot build a range even
    // once the cast is gone, so removing it buys nothing and risks changing
    // the comparison.
    if cast_type.eval_type() == tidb_datatype::EvalType::String
        && !tidb_datatype::compatible_collate(cast_type.collation_name(), &collation)
    {
        return None;
    }
    // The other side has to be a constant.
    if !matches!(args[1 - cast_offset], Expression::Constant(_)) {
        return None;
    }
    // And the cast has to sit directly on a column -- a deeper cast is out of
    // scope, as Go's doc comment on `EliminateNoPrecisionLossCast` states.
    let Some(Expression::Column(column)) = cast.get_args().first() else {
        return None;
    };
    let column_type = column.ret_type.as_ref()?;
    if !no_precision_loss_cast_compatible(cast_type, column_type) {
        return None;
    }

    let new_args = if cast_offset == 0 {
        vec![Expression::Column(column.clone()), args[1].clone()]
    } else {
        vec![args[0].clone(), Expression::Column(column.clone())]
    };
    builder
        .new_function(
            parent.func_name.lowercase(),
            parent.ret_type.clone(),
            new_args,
        )
        .ok()
}

/// Go `eliminateCastFunction` (`util.go:993`).
fn eliminate_cast_function(expr: &Expression, builder: &dyn FunctionBuilder) -> (Expression, bool) {
    let Expression::ScalarFunction(function) = expr else {
        return (expr.clone(), false);
    };
    let (_, collation) = function.collation.charset_and_collation();
    let collation = collation.to_owned();

    match function.func_name.lowercase() {
        "or" => {
            let dnf_items = flatten_dnf_conditions(function);
            let mut rm_cast = false;
            let mut rm_cast_items = Vec::with_capacity(dnf_items.len());
            for dnf_item in &dnf_items {
                let (new_expr, cur) = eliminate_cast_function(dnf_item, builder);
                rm_cast_items.push(new_expr);
                rm_cast = rm_cast || cur;
            }
            if rm_cast {
                if let Some(composed) = compose_dnf_condition(rm_cast_items) {
                    return (composed, true);
                }
            }
            (expr.clone(), false)
        }
        "and" => {
            let cnf_items = flatten_cnf_conditions(function);
            let mut rm_cast = false;
            let mut rm_cast_items = Vec::with_capacity(cnf_items.len());
            for cnf_item in &cnf_items {
                let (new_expr, cur) = eliminate_cast_function(cnf_item, builder);
                rm_cast_items.push(new_expr);
                rm_cast = rm_cast || cur;
            }
            if rm_cast {
                if let Some(composed) = compose_cnf_condition(rm_cast_items) {
                    return (composed, true);
                }
            }
            (expr.clone(), false)
        }
        "eq" | "nulleq" | "le" | "ge" | "lt" | "gt" => {
            // `eq(cast(t2.a, varchar(100)), 'aaaaa')` -- once `t2.a` is covered
            // by an index, deconstruct the cast out. Try both sides.
            if let Some(new_func) = unwrap_cast(function, 0, builder) {
                return (new_func, true);
            }
            if let Some(new_func) = unwrap_cast(function, 1, builder) {
                return (new_func, true);
            }
            (expr.clone(), false)
        }
        "in" => {
            // `cast(a AS bigint) IN (1, 2, 3)`: `a` comes out directly.
            let args = function.get_args();
            let Some(Expression::ScalarFunction(cast)) = args.first() else {
                return (expr.clone(), false);
            };
            if cast.func_name.lowercase() != "cast" {
                return (expr.clone(), false);
            }
            let Some(cast_type) = cast.ret_type.as_ref() else {
                return (expr.clone(), false);
            };
            if cast_type.eval_type() == tidb_datatype::EvalType::String
                && !tidb_datatype::compatible_collate(cast_type.collation_name(), &collation)
            {
                return (expr.clone(), false);
            }
            if !args[1..]
                .iter()
                .all(|arg| matches!(arg, Expression::Constant(_)))
            {
                return (expr.clone(), false);
            }
            let Some(Expression::Column(column)) = cast.get_args().first() else {
                return (expr.clone(), false);
            };
            let Some(column_type) = column.ret_type.as_ref() else {
                return (expr.clone(), false);
            };
            if !no_precision_loss_cast_compatible(cast_type, column_type) {
                return (expr.clone(), false);
            }
            let mut new_args = vec![Expression::Column(column.clone())];
            new_args.extend_from_slice(&args[1..]);
            match builder.new_function("in", function.ret_type.clone(), new_args) {
                Ok(built) => (built, true),
                Err(_) => (expr.clone(), false),
            }
        }
        _ => (expr.clone(), false),
    }
}

/// Go `EliminateNoPrecisionLossCast` (`util.go:1151`): drops a redundant
/// `CAST` so a range can be built.
///
/// The three constraints Go's doc comment states, all preserved:
///
/// 1. a cast nested deeper inside a complicated function is NOT considered;
/// 2. the cast's arguments must be one base column against one constant;
/// 3. collation compatibility and precision loss are both checked before the
///    cast is removed.
#[must_use]
pub fn eliminate_no_precision_loss_cast(
    expr: &Expression,
    builder: &dyn FunctionBuilder,
) -> Expression {
    eliminate_cast_function(expr, builder).0
}
