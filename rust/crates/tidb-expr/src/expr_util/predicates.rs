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

//! The tree predicates and small accessors from `pkg/expression/util.go` that
//! guard a rewrite: `:1129`-`:1204`, `:1331`-`:1357`, `:1417`-`:1427`,
//! `:1483`-`:1802`, `:2066`-`:2116`, `:2357`-`:2369`.
//!
//! Everything here is a pure read over a built tree; nothing rebuilds a node,
//! so none of it touches the [`super::builder`] boundary.

use super::traits::is_mutable_effects_function;
use crate::column::Column;
use crate::context::{Columns, EvalError};
use crate::expression::{ConstLevel, Expression};
use crate::scalar_function::is_unfoldable_function;
use std::collections::BTreeSet;
use tidb_datatype::{Datum, FieldTypeFlags};

/// Go `GetExprInsideIsTruth` (`util.go:1129`): unwraps the `istrue` /
/// `istrue_with_null` wrappers.
///
/// NOT push-down and `!` handling add those wrappers to preserve three-valued
/// logic (see [`super::push_not`]); a rule that wants to look at the real
/// predicate has to see past them.
#[must_use]
pub fn get_expr_inside_is_truth(expr: &Expression) -> &Expression {
    if let Expression::ScalarFunction(function) = expr {
        let name = function.func_name.lowercase();
        if name == "istrue_with_null" || name == "istrue" {
            if let Some(arg0) = function.get_args().first() {
                return get_expr_inside_is_truth(arg0);
            }
        }
    }
    expr
}

/// Go `containOuterNot` (`util.go:1168`).
fn contain_outer_not_inner(expr: &Expression, not: bool) -> bool {
    let Expression::ScalarFunction(function) = expr else {
        return false;
    };
    let args = function.get_args();
    match function.func_name.lowercase() {
        "not" => args
            .first()
            .is_some_and(|arg| contain_outer_not_inner(arg, true)),
        // These two are transparent: they do not themselves negate, so an
        // enclosing NOT keeps applying through them.
        "istrue_with_null" | "isnull" => args
            .first()
            .is_some_and(|arg| contain_outer_not_inner(arg, not)),
        _ => {
            if not {
                return true;
            }
            args.iter().any(|arg| contain_outer_not_inner(arg, not))
        }
    }
}

/// Go `ContainOuterNot` (`util.go:1157`): whether a `NOT` encloses anything
/// other than another logical connective.
///
/// `not(0+(t.a = 1 and t.b = 2))` is true; `not(t.a) and not(t.b)` is false --
/// the second has NOTs, but each one sits directly on a leaf, which
/// [`super::push_not::push_down_not`] can already handle.
#[must_use]
pub fn contain_outer_not(expr: &Expression) -> bool {
    contain_outer_not_inner(expr, false)
}

/// Go `Contains` (`util.go:1193`): whether `exprs` contains `e`.
///
/// `// boundary:` Go tests pointer identity first and then
/// `expr.Equal(ectx, e)`. `Expression::equal` in this crate is documented as
/// context-free -- columns compare by `UniqueID`, constants and scalar
/// functions conservatively report `false`. This predicate inherits exactly
/// that: a constant or function that Go would find may be missed here.
#[must_use]
pub fn contains(exprs: &[Expression], e: &Expression) -> bool {
    exprs.iter().any(|expr| expr.equal(e))
}

/// Go `GetRowLen` (`util.go:1331`): the arity of a `ROW(...)`, or 1 for
/// anything else.
#[must_use]
pub fn get_row_len(e: &Expression) -> usize {
    match e {
        Expression::ScalarFunction(function) if function.func_name.lowercase() == "row" => {
            function.get_args().len()
        }
        _ => 1,
    }
}

/// Go `CheckArgsNotMultiColumnRow` (`util.go:1339`): rejects an argument that
/// is a multi-column row where a scalar is required.
///
/// # Errors
///
/// Returns the offending argument's index, which is what a caller needs to
/// build Go's `ErrOperandColumns` message.
pub fn check_args_not_multi_column_row(args: &[Expression]) -> Result<(), usize> {
    for (index, arg) in args.iter().enumerate() {
        if get_row_len(arg) != 1 {
            return Err(index);
        }
    }
    Ok(())
}

/// Go `GetFuncArg` (`util.go:1349`): the `idx`-th argument of `e`, or `None`
/// when `e` is not a function.
///
/// Go indexes unguarded and panics on an out-of-range `idx`; `None` covers
/// both that and the non-function case.
#[must_use]
pub fn get_func_arg(e: &Expression, idx: usize) -> Option<&Expression> {
    match e {
        Expression::ScalarFunction(function) => function.get_args().get(idx),
        _ => None,
    }
}

/// Go `DisableParseJSONFlag4Expr` (`util.go:1417`): clears `ParseToJSONFlag`
/// on `expr`, except on a column.
///
/// Columns are skipped for two reasons Go states: the flag is already 0 for a
/// JSON column, and a `Column`'s `RetType` points into the infoschema, where
/// writing it would race another goroutine reading the same table definition.
pub fn disable_parse_json_flag_4_expr(expr: &mut Expression) {
    if matches!(
        expr,
        Expression::Column(_) | Expression::CorrelatedColumn(_)
    ) {
        return;
    }
    let ret_type = match expr {
        Expression::Constant(c) => c.ret_type.as_mut(),
        Expression::ScalarFunction(c) => c.ret_type.as_mut(),
        _ => None,
    };
    if let Some(ret_type) = ret_type {
        ret_type.del_flags(FieldTypeFlags::PARSE_TO_JSON);
    }
}

/// Go `IsRuntimeConstExpr` (`util.go:1483`): whether the EXECUTOR may treat
/// `expr` as a constant.
///
/// Looser than [`Expression::const_level`]: a correlated column counts,
/// because it is fixed for the duration of one execution of the inner plan.
#[must_use]
pub fn is_runtime_const_expr(expr: &Expression) -> bool {
    match expr {
        Expression::ScalarFunction(function) => {
            if is_unfoldable_function(function.func_name.lowercase()) {
                return false;
            }
            function.get_args().iter().all(is_runtime_const_expr)
        }
        Expression::Column(_) => false,
        Expression::Constant(_) | Expression::CorrelatedColumn(_) => true,
    }
}

/// Go `CheckNonDeterministic` (`util.go:1504`): whether `e` contains a
/// non-deterministic call.
#[must_use]
pub fn check_non_deterministic(e: &Expression) -> bool {
    match e {
        Expression::ScalarFunction(function) => {
            is_unfoldable_function(function.func_name.lowercase())
                || function.get_args().iter().any(check_non_deterministic)
        }
        _ => false,
    }
}

/// Go `CheckFuncInExpr` (`util.go:1520`): whether `func_name` appears anywhere
/// in `e`.
#[must_use]
pub fn check_func_in_expr(e: &Expression, func_name: &str) -> bool {
    match e {
        Expression::ScalarFunction(function) => {
            function.func_name.lowercase() == func_name
                || function
                    .get_args()
                    .iter()
                    .any(|arg| check_func_in_expr(arg, func_name))
        }
        _ => false,
    }
}

/// Go `IsMutableEffectsExpr` (`util.go:1538`): whether `expr` contains a
/// function that is mutable or has side effects.
///
/// A `Constant` is inspected too: a DEFERRED constant is a wrapper around the
/// expression that will actually run.
#[must_use]
pub fn is_mutable_effects_expr(expr: &Expression) -> bool {
    match expr {
        Expression::ScalarFunction(function) => {
            is_mutable_effects_function(function.func_name.lowercase())
                || function.get_args().iter().any(is_mutable_effects_expr)
        }
        Expression::Constant(constant) => constant
            .deferred_expr
            .as_deref()
            .is_some_and(is_mutable_effects_expr),
        _ => false,
    }
}

/// Go `IsImmutableFunc` (`util.go:1558`): whether `expr` consists only of
/// foldable, effect-free functions, so a single `Eval` against the empty row
/// gives a result that will not change.
///
/// Note the asymmetry Go builds in: the `default` arm returns TRUE, so a
/// COLUMN is "immutable" here. This predicate answers "does re-evaluating
/// change the answer", not "is this a constant".
#[must_use]
pub fn is_immutable_func(expr: &Expression) -> bool {
    match expr {
        Expression::ScalarFunction(function) => {
            let name = function.func_name.lowercase();
            if is_unfoldable_function(name) || is_mutable_effects_function(name) {
                return false;
            }
            function.get_args().iter().all(is_immutable_func)
        }
        _ => true,
    }
}

/// Go `RemoveDupExprs` (`util.go:1581`): drops later duplicates, keyed by hash
/// code.
///
/// An expression that is mutable or has side effects is NEVER dropped even
/// when it duplicates an earlier one: evaluating `RAND()` twice is not the
/// same as evaluating it once.
#[must_use]
pub fn remove_dup_exprs(exprs: Vec<Expression>) -> Vec<Expression> {
    if exprs.len() <= 1 {
        return exprs;
    }
    let mut exists: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut result = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let key = expr.clone().hash_code().to_vec();
        // Go's condition is `!ok || IsMutableEffectsExpr(expr)`: unseen, or
        // never-deduplicable.
        if !exists.contains(&key) || is_mutable_effects_expr(&expr) {
            exists.insert(key);
            result.push(expr);
        }
    }
    result
}

/// Go `GetUint64FromConstant` (`util.go:1597`): reads a `u64` out of a
/// constant expression.
///
/// Returns `(value, is_null)`, or `None` for "not usable" -- Go's third return
/// value. A negative signed integer is NOT usable, which is the guard that
/// keeps a negative `LIMIT` from wrapping.
///
/// `// narrowing:` Go resolves a `ParamMarker` through
/// `ParamMarker.GetUserVar(ctx)`. That needs the session's bound parameters,
/// which this crate does not carry, so a parameter constant reports `None`
/// (Go's own outcome when that call errors).
#[must_use]
pub fn get_uint64_from_constant(expr: &Expression, ctx: &impl Columns) -> Option<(u64, bool)> {
    let Expression::Constant(constant) = expr else {
        return None;
    };
    let value = if constant.param_marker.is_some() {
        return None;
    } else if let Some(deferred) = constant.deferred_expr.as_deref() {
        super::substitute::eval_once(deferred, ctx).ok()?
    } else {
        constant.value.clone()
    };
    match value {
        Datum::Null => Some((0, true)),
        Datum::Int(v) => {
            if v < 0 {
                None
            } else {
                Some((v as u64, false))
            }
        }
        Datum::UInt(v) => Some((v, false)),
        _ => None,
    }
}

/// Go `ContainVirtualColumn` (`util.go:1635`): whether any expression reads a
/// virtual generated column.
#[must_use]
pub fn contain_virtual_column(exprs: &[Expression]) -> bool {
    exprs.iter().any(|expr| match expr {
        Expression::Column(column) => column.virtual_expr.is_some(),
        Expression::ScalarFunction(function) => contain_virtual_column(function.get_args()),
        _ => false,
    })
}

/// Go `ContainCorrelatedColumn` (`util.go:1652`): whether any expression reads
/// a correlated column.
#[must_use]
pub fn contain_correlated_column(exprs: &[Expression]) -> bool {
    exprs.iter().any(|expr| match expr {
        Expression::CorrelatedColumn(_) => true,
        Expression::ScalarFunction(function) => contain_correlated_column(function.get_args()),
        _ => false,
    })
}

/// Go `jsonUnquoteFunctionBenefitsFromPushedDown` (`util.go:1666`).
///
/// Only the `->>` spelling -- which the parser produces as
/// `JSON_UNQUOTE(CAST(JSON_EXTRACT(...) AS string))` -- can be pushed to TiKV.
fn json_unquote_benefits_from_pushed_down(
    function: &crate::scalar_function::ScalarFunction,
) -> bool {
    let Some(Expression::ScalarFunction(child)) = function.get_args().first() else {
        return false;
    };
    if child.func_name.lowercase() != "cast" {
        return false;
    }
    matches!(
        child.get_args().first(),
        Some(Expression::ScalarFunction(grand)) if grand.func_name.lowercase() == "json_extract"
    )
}

/// Go `ProjectionBenefitsFromPushedDown` (`util.go:1684`): whether pushing
/// this projection to TiKV is a PERFORMANCE win.
///
/// Projections are not pushed down by default, so the test is deliberately
/// strict: only the JSON functions TiKV evaluates well, plus pure column
/// pruning. Virtual columns are not considered -- this asks about speed, not
/// about whether the push-down would be correct.
///
/// `// narrowing:` Go's `forcePushDownTiKV` failpoint, which short-circuits to
/// `true` for debugging, has no failpoint mechanism here.
#[must_use]
pub fn projection_benefits_from_pushed_down(exprs: &[Expression], input_schema_len: usize) -> bool {
    let mut all_col_ref = true;
    let mut col_ref_count = 0usize;
    for expr in exprs {
        match expr {
            Expression::Column(_) => col_ref_count += 1,
            Expression::ScalarFunction(function) => {
                all_col_ref = false;
                match function.func_name.lowercase() {
                    "json_depth" | "json_length" | "json_type" | "json_valid" | "json_contains"
                    | "json_contains_path" | "json_extract" | "json_keys" | "json_search"
                    | "json_memberof" | "json_overlaps" => {}
                    "json_unquote" => {
                        if !json_unquote_benefits_from_pushed_down(function) {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
            _ => return false,
        }
    }
    if all_col_ref {
        // A projection of only columns is worth pushing exactly when it PRUNES
        // some of them.
        return col_ref_count < input_schema_len;
    }
    true
}

/// Go `containMutableConst` (`util.go:1742`): whether any expression holds a
/// lazy constant -- a `?` placeholder or a deferred expression.
#[must_use]
pub fn contain_mutable_const(exprs: &[Expression]) -> bool {
    exprs.iter().any(|expr| match expr {
        Expression::Constant(constant) => {
            constant.param_marker.is_some() || constant.deferred_expr.is_some()
        }
        Expression::ScalarFunction(function) => contain_mutable_const(function.get_args()),
        _ => false,
    })
}

/// Go `MaybeOverOptimized4PlanCache` (`util.go:1733`): whether an optimization
/// might bake a parameter-specific decision into a cached plan.
///
/// `pk >= $a AND pk <= $b` can become a PointGet when `$a == $b`, and that
/// plan is wrong for every later execution where they differ. When plan
/// caching is off, no such sharing happens and every optimization is safe.
#[must_use]
pub fn maybe_over_optimized_4_plan_cache(use_cache: bool, exprs: &[Expression]) -> bool {
    use_cache && contain_mutable_const(exprs)
}

/// Go `RemoveMutableConst` (`util.go:1759`): strips `ParamMarker` and
/// `DeferredExpr` in place, evaluating the deferred expression into the
/// constant's value first so the node becomes fully immutable.
///
/// # Errors
///
/// Returns the evaluation error from a deferred expression.
pub fn remove_mutable_const(exprs: &mut [Expression], ctx: &impl Columns) -> Result<(), EvalError> {
    for expr in exprs {
        match expr {
            Expression::Constant(constant) => {
                constant.param_marker = None;
                if let Some(deferred) = constant.deferred_expr.take() {
                    constant.value = super::substitute::eval_once(&deferred, ctx)?;
                }
            }
            Expression::ScalarFunction(function) => {
                remove_mutable_const(&mut function.args, ctx)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Go `hasColumnWithCondition` (`util.go:2070`).
fn has_column_with_condition_inner(e: &Expression, cond: &dyn Fn(&Column) -> bool) -> bool {
    match e {
        Expression::Column(column) => cond(column),
        Expression::ScalarFunction(function) => function
            .get_args()
            .iter()
            .any(|arg| has_column_with_condition_inner(arg, cond)),
        _ => false,
    }
}

/// Go `HasColumnWithCondition` (`util.go:2066`): whether `e` reads a column
/// satisfying `cond`.
#[must_use]
pub fn has_column_with_condition(e: &Expression, cond: &dyn Fn(&Column) -> bool) -> bool {
    has_column_with_condition_inner(e, cond)
}

/// Go `ConstExprConsiderPlanCache` (`util.go:2088`): whether `expr` may be
/// treated as constant given the plan-cache setting.
///
/// A plan-cached expression is shared across statements and so needs
/// `ConstStrict`; one that is not cached only has to hold for the single
/// statement, where `ConstOnlyInContext` suffices.
#[must_use]
pub fn const_expr_consider_plan_cache(expr: &Expression, in_plan_cache: bool) -> bool {
    match expr.const_level() {
        ConstLevel::STRICT => true,
        ConstLevel::ONLY_IN_CONTEXT => !in_plan_cache,
        _ => false,
    }
}

/// Go `ExprHasSetVarOrSleep` (`util.go:2105`): whether `expr` calls `SET @var`
/// or `SLEEP()`.
#[must_use]
pub fn expr_has_set_var_or_sleep(expr: &Expression) -> bool {
    let Expression::ScalarFunction(function) = expr else {
        return false;
    };
    let name = function.func_name.lowercase();
    if name == "setvar" || name == "sleep" {
        return true;
    }
    function.get_args().iter().any(expr_has_set_var_or_sleep)
}

/// Go `ExprsHasSideEffects` (`util.go:2100`).
#[must_use]
pub fn exprs_has_side_effects(exprs: &[Expression]) -> bool {
    exprs.iter().any(expr_has_set_var_or_sleep)
}

/// Go `IsConstNull` (`util.go:2357`): recognizes `col <op> NULL`, where the
/// comparison can never be TRUE.
///
/// As Go's own comment says, this assumes the first argument is a column and
/// only inspects the second. A DEFERRED null is excluded: it is not yet known
/// to be null.
#[must_use]
pub fn is_const_null(expr: &Expression) -> bool {
    let Expression::ScalarFunction(function) = expr else {
        return false;
    };
    if !matches!(
        function.func_name.lowercase(),
        "lt" | "le" | "gt" | "ge" | "eq" | "ne"
    ) {
        return false;
    }
    matches!(
        function.get_args().get(1),
        Some(Expression::Constant(constant))
            if constant.value.is_null() && constant.deferred_expr.is_none()
    )
}
