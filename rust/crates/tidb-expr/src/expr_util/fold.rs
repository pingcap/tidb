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

//! Go `pkg/expression/constant_fold.go`: the public `FoldConstant` entry point
//! (`:39`), the recursive `foldConstant` walk (`:161`), and the four special
//! handlers -- `isNullHandler` (`:51`), `ifFoldHandler` (`:74`),
//! `ifNullFoldHandler` (`:95`) and `caseWhenHandler` (`:120`).
//!
//! `constant_fold.rs` in this crate already holds the AST-REWRITER tier's view
//! of folding: `derive_constant_null_flag` and `fold_constant_in_mode`, which
//! fold a tree the rewriter has just built, plus the `folds_to_constant` /
//! `folded_value` predicates its dispatch keys on. What it does NOT have is
//! Go's public `FoldConstant(ctx, expr)` over an already-built `Expression`
//! tree, with the special handlers. That is what this module adds; the shared
//! unfoldable-function table is reused, not re-declared.
//!
//! # Narrowings, each named
//!
//! - `// narrowing:` Go's `specialFoldHandler` dispatch is guarded by
//!   `!MaybeOverOptimized4PlanCache(ctx, expr)`. That guard needs the
//!   session's plan-cache flag; [`FoldOptions::use_plan_cache`] carries it, and
//!   the predicate itself is
//!   [`super::predicates::maybe_over_optimized_4_plan_cache`].
//! - `// narrowing:` Go's `extensionFuncSig` check. Extension (plugin)
//!   functions are not in this workspace, so there is no signature to
//!   recognize; the arm is unreachable rather than omitted.
//! - `// narrowing:` Go's `NewFunctionBase` dummy-evaluation branch, used only
//!   when `IsInNullRejectCheck` is set and SOME arguments are non-constant. It
//!   is reproduced through the injected [`FunctionBuilder`], and so inherits
//!   that boundary's deferred type inference.
//! - `// narrowing:` `ParamMarker.GetUserVar(ctx)`. Reading a prepared
//!   statement's bound parameter needs the session's parameter values, which
//!   this crate does not carry. A `ParamMarker` constant therefore folds to
//!   ITSELF and is reported deferred, which is Go's own behaviour on the error
//!   path of that same call.

use super::builder::FunctionBuilder;
use super::substitute::{eval_once, SubstituteError, SubstituteOptions};
use crate::constant::Constant;
use crate::context::Columns;
use crate::expression::Expression;
use crate::scalar_function::{is_unfoldable_function, ScalarFunction};
use tidb_datatype::{Datum, FieldTypeFlags};

/// The construction-context flags `foldConstant` reads.
pub struct FoldOptions<'a> {
    /// `// boundary:` Go `NewFunction` -- see [`super::builder`].
    pub builder: &'a dyn FunctionBuilder,
    /// Go `ctx.IsUseCache()`: whether the statement may be plan-cached.
    pub use_plan_cache: bool,
    /// Go `ctx.IsInNullRejectCheck()`: enables the partial-constant branch
    /// that tries to prove a predicate NULL or FALSE.
    pub in_null_reject_check: bool,
}

impl<'a> FoldOptions<'a> {
    /// The plain folding context: no plan cache, not inside null rejection.
    #[must_use]
    pub fn new(builder: &'a dyn FunctionBuilder) -> Self {
        FoldOptions {
            builder,
            use_plan_cache: false,
            in_null_reject_check: false,
        }
    }
}

impl<'a> From<&SubstituteOptions<'a>> for FoldOptions<'a> {
    fn from(opts: &SubstituteOptions<'a>) -> Self {
        FoldOptions::new(opts.builder)
    }
}

/// Go `FoldConstant` (`constant_fold.go:39`): constant-folds `expr`, excluding
/// deferred subexpressions.
///
/// Go's wrapper does one thing beyond the recursive walk, and it is not
/// cosmetic: it copies the ORIGINAL expression's coercibility, charset,
/// collation and repertoire onto the folded result. Folding must not change
/// how the value compares, only what it is.
#[must_use]
pub fn fold_constant(
    expr: &Expression,
    ctx: &impl Columns,
    opts: &SubstituteOptions<'_>,
) -> Expression {
    fold_constant_with(expr, ctx, &FoldOptions::from(opts))
}

/// [`fold_constant`] against an explicit [`FoldOptions`].
#[must_use]
pub fn fold_constant_with(
    expr: &Expression,
    ctx: &impl Columns,
    opts: &FoldOptions<'_>,
) -> Expression {
    let (mut folded, _) = fold_constant_inner(expr, ctx, opts);

    // Keep the original coercibility, charset, collation and repertoire.
    let coercibility = crate::collation_derive::coercibility_of(expr);
    let (charset, collation) = charset_and_collation_of(expr);
    let repertoire = repertoire_of(expr);
    if let Some(info) = collation_info_of_mut(&mut folded) {
        info.set_coercibility(coercibility);
        info.set_repertoire(repertoire);
    }
    if let Some(ret_type) = static_type_mut(&mut folded) {
        ret_type.set_charset_name(charset);
        ret_type.set_collation_name(collation);
    }
    folded
}

/// The VALUE a fold reduces `expr` to, for callers that want the datum rather
/// than the rewrapped node (Go `SubstituteCorCol2Constant`'s deferred arm).
///
/// # Errors
///
/// Returns [`SubstituteError`] when evaluation fails.
pub(super) fn fold_constant_value(
    expr: &Expression,
    ctx: &impl Columns,
) -> Result<Datum, SubstituteError> {
    if let Expression::Constant(constant) = expr {
        if let Some(deferred) = constant.deferred_expr.as_deref() {
            return Ok(eval_once(deferred, ctx)?);
        }
        return Ok(constant.value.clone());
    }
    Ok(eval_once(expr, ctx)?)
}

/// Go `foldConstant` (`constant_fold.go:161`). The `bool` is Go's
/// `isDeferredConst`.
fn fold_constant_inner(
    expr: &Expression,
    ctx: &impl Columns,
    opts: &FoldOptions<'_>,
) -> (Expression, bool) {
    match expr {
        Expression::ScalarFunction(function) => fold_scalar_function(expr, function, ctx, opts),
        Expression::Constant(constant) => {
            if constant.param_marker.is_some() {
                // `// narrowing:` see the module header -- no session
                // parameter values here, so this takes Go's error path, which
                // returns the expression unchanged and marks it deferred.
                return (expr.clone(), true);
            }
            if let Some(deferred) = constant.deferred_expr.as_deref() {
                let Ok(value) = eval_once(deferred, ctx) else {
                    return (expr.clone(), true);
                };
                let mut folded = constant.clone();
                folded.value = value;
                return (Expression::Constant(folded), true);
            }
            (expr.clone(), false)
        }
        other => (other.clone(), false),
    }
}

fn fold_scalar_function(
    expr: &Expression,
    function: &ScalarFunction,
    ctx: &impl Columns,
    opts: &FoldOptions<'_>,
) -> (Expression, bool) {
    let name = function.func_name.lowercase();
    if is_unfoldable_function(name) {
        return (expr.clone(), false);
    }

    // Go's `specialFoldHandler` map, gated on the plan-cache check: a
    // short-circuiting fold would bake one branch into a cached plan whose
    // parameters may later select the other.
    let over_optimized =
        opts.use_plan_cache && super::predicates::contain_mutable_const(std::slice::from_ref(expr));
    if !over_optimized {
        match name {
            "isnull" => return is_null_handler(expr, function, ctx, opts),
            "if" => return if_fold_handler(expr, function, ctx, opts),
            "ifnull" => return if_null_fold_handler(expr, function, ctx, opts),
            "case" => return case_when_handler(expr, function, ctx, opts),
            _ => {}
        }
    }

    let args = function.get_args();
    let mut arg_is_const = vec![false; args.len()];
    let mut has_null_arg = false;
    let mut all_const_arg = true;
    let mut is_deferred_const = false;
    for (index, arg) in args.iter().enumerate() {
        match arg {
            Expression::Constant(constant) => {
                is_deferred_const = is_deferred_const
                    || constant.deferred_expr.is_some()
                    || constant.param_marker.is_some();
                arg_is_const[index] = true;
                has_null_arg = has_null_arg || constant.value.is_null();
            }
            _ => all_const_arg = false,
        }
    }

    if !all_const_arg {
        return fold_partially_const(
            expr,
            function,
            &arg_is_const,
            has_null_arg,
            is_deferred_const,
            ctx,
            opts,
        );
    }

    let Ok(value) = eval_once(expr, ctx) else {
        // Go logs at DEBUG and returns the expression unfolded, so the error
        // surfaces again at execution time and reaches the client then.
        return (expr.clone(), is_deferred_const);
    };
    let mut ret_type = function.ret_type.clone();
    if !has_null_arg {
        if let Some(ret_type) = ret_type.as_mut() {
            if value.is_null() {
                ret_type.del_flags(FieldTypeFlags::NOT_NULL);
            } else {
                ret_type.add_flags(FieldTypeFlags::NOT_NULL);
            }
        }
    }

    let mut folded = Constant::default();
    folded.value = value;
    folded.ret_type = ret_type;
    if is_deferred_const {
        folded.deferred_expr = Some(Box::new(expr.clone()));
        return (Expression::Constant(folded), true);
    }
    // A folded scalar-query reference keeps its id so EXPLAIN can still name
    // the subquery column the value came from.
    folded.subquery_ref_id = args
        .iter()
        .find_map(|arg| match arg {
            Expression::Constant(c) if c.subquery_ref_id > 0 => Some(c.subquery_ref_id),
            _ => None,
        })
        .unwrap_or(0);
    (Expression::Constant(folded), false)
}

/// Go's `!allConstArg` branch: under null-rejection checking, try to prove the
/// whole call NULL or FALSE from the constant arguments alone.
fn fold_partially_const(
    expr: &Expression,
    function: &ScalarFunction,
    arg_is_const: &[bool],
    has_null_arg: bool,
    is_deferred_const: bool,
    ctx: &impl Columns,
    opts: &FoldOptions<'_>,
) -> (Expression, bool) {
    let name = function.func_name.lowercase();
    // `NULLEQ`, `CONCAT_WS` and `FIELD` are excluded because their result
    // genuinely depends on the non-constant argument's value even when another
    // argument is NULL: `concat_ws(NULL, NULL)` is NULL but
    // `concat_ws(1, NULL)` is `''`, and `FIELD(0, 0.0, NULL)` is 1 while
    // `FIELD(1, 0.0, NULL)` is 0.
    if !has_null_arg
        || !opts.in_null_reject_check
        || name == "nulleq"
        || name == "concat_ws"
        || name == "field"
    {
        return (expr.clone(), is_deferred_const);
    }

    // Stand every non-constant argument up as 1 and see whether the result is
    // forced regardless.
    let const_args: Vec<Expression> = function
        .get_args()
        .iter()
        .enumerate()
        .map(|(index, arg)| {
            if arg_is_const[index] {
                arg.clone()
            } else {
                Expression::Constant(Constant::new_one())
            }
        })
        .collect();

    let Ok(dummy) = opts
        .builder
        .new_function(name, function.ret_type.clone(), const_args)
    else {
        return (expr.clone(), is_deferred_const);
    };
    let Ok(value) = eval_once(&dummy, ctx) else {
        return (expr.clone(), is_deferred_const);
    };

    // The constant built here composes the result of `EvaluateExprWithNull`
    // under null-rejection checking; the caller only asks whether it is NULL or
    // FALSE and then discards it, so leaving `DeferredExpr` unset is safe.
    if value.is_null() {
        return (Expression::Constant(constant_of(value, function)), false);
    }
    if crate::truthy_of(&value).ok() == Some(Some(false)) {
        return (Expression::Constant(constant_of(value, function)), false);
    }
    (expr.clone(), is_deferred_const)
}

fn constant_of(value: Datum, function: &ScalarFunction) -> Constant {
    let mut constant = Constant::default();
    constant.value = value;
    constant.ret_type = function.ret_type.clone();
    constant
}

/// Go `isNullHandler` (`constant_fold.go:51`).
///
/// Beyond folding a constant argument, this is where `ISNULL(x)` collapses to
/// `0` for any `x` the type system already declares NOT NULL -- the one arm
/// that needs no evaluation at all.
fn is_null_handler(
    expr: &Expression,
    function: &ScalarFunction,
    ctx: &impl Columns,
    _opts: &FoldOptions<'_>,
) -> (Expression, bool) {
    let Some(arg0) = function.get_args().first() else {
        return (expr.clone(), false);
    };
    if let Expression::Constant(constant) = arg0 {
        let is_deferred_const = constant.deferred_expr.is_some() || constant.param_marker.is_some();
        let Ok(value) = eval_once(expr, ctx) else {
            return (expr.clone(), is_deferred_const);
        };
        let mut folded = constant_of(value, function);
        if is_deferred_const {
            folded.deferred_expr = Some(Box::new(expr.clone()));
            return (Expression::Constant(folded), true);
        }
        return (Expression::Constant(folded), false);
    }
    if arg0
        .static_type()
        .is_some_and(|t| t.flags() & FieldTypeFlags::NOT_NULL != 0)
    {
        return (Expression::Constant(Constant::new_zero()), false);
    }
    (expr.clone(), false)
}

/// Go `ifFoldHandler` (`constant_fold.go:74`): folds `IF(c, a, b)` to the
/// branch `c` selects, but ONLY when `c` itself folds to a constant.
fn if_fold_handler(
    expr: &Expression,
    function: &ScalarFunction,
    ctx: &impl Columns,
    opts: &FoldOptions<'_>,
) -> (Expression, bool) {
    let args = function.get_args();
    if args.len() != 3 {
        return (expr.clone(), false);
    }
    let (folded_arg0, _) = fold_constant_inner(&args[0], ctx, opts);
    let Expression::Constant(_) = &folded_arg0 else {
        // The condition is not constant, so which branch runs is unknown.
        return (expr.clone(), false);
    };
    let Ok(value) = eval_once(&folded_arg0, ctx) else {
        return (expr.clone(), false);
    };
    // Go's `EvalInt`: NULL and 0 both take the else branch.
    let takes_then = matches!(crate::truthy_of(&value), Ok(Some(true)));
    if takes_then {
        fold_constant_inner(&args[1], ctx, opts)
    } else {
        fold_constant_inner(&args[2], ctx, opts)
    }
}

/// Go `ifNullFoldHandler` (`constant_fold.go:95`).
fn if_null_fold_handler(
    expr: &Expression,
    function: &ScalarFunction,
    ctx: &impl Columns,
    opts: &FoldOptions<'_>,
) -> (Expression, bool) {
    let args = function.get_args();
    if args.len() != 2 {
        return (expr.clone(), false);
    }
    let (folded_arg0, is_deferred) = fold_constant_inner(&args[0], ctx, opts);
    let Expression::Constant(constant) = &folded_arg0 else {
        return (expr.clone(), false);
    };
    // Only the VALUE matters: a deferred argument has already been evaluated
    // into it by the fold above.
    if !constant.value.is_null() {
        return (folded_arg0.clone(), is_deferred);
    }
    let (folded, is_constant) = fold_constant_inner(&args[1], ctx, opts);
    // TiDB issue #51765: when the first argument folds to NULL, IFNULL's
    // collation must become the SECOND argument's, since that is the value
    // that will actually be returned.
    let mut result = folded;
    if let (Some(src), Some(dst)) = (args[1].static_type().cloned(), static_type_mut(&mut result)) {
        dst.set_charset_name(src.charset_name().to_owned());
        dst.set_collation_name(src.collation_name().to_owned());
    }
    (result, is_constant)
}

/// Go `caseWhenHandler` (`constant_fold.go:120`): walks the `WHEN` conditions
/// in order, folding each, and returns the first body whose condition is
/// constant-TRUE.
///
/// A non-constant condition stops the walk immediately -- everything after it
/// is unknown to run.
fn case_when_handler(
    expr: &Expression,
    function: &ScalarFunction,
    ctx: &impl Columns,
    opts: &FoldOptions<'_>,
) -> (Expression, bool) {
    let args = function.get_args();
    let len = args.len();
    let mut is_deferred_const = false;
    let mut index = 0;
    while index + 1 < len {
        let (folded_cond, is_deferred) = fold_constant_inner(&args[index], ctx, opts);
        is_deferred_const = is_deferred_const || is_deferred;
        if !matches!(folded_cond, Expression::Constant(_)) {
            return (expr.clone(), false);
        }
        let Ok(value) = eval_once(&folded_cond, ctx) else {
            return (expr.clone(), false);
        };
        if matches!(crate::truthy_of(&value), Ok(Some(true))) {
            let (mut folded, is_deferred) = fold_constant_inner(&args[index + 1], ctx, opts);
            is_deferred_const = is_deferred_const || is_deferred;
            if matches!(folded, Expression::Constant(_)) {
                // Keep the CASE's own decimal on the body it collapsed to.
                if let (Some(decimal), Some(ret_type)) = (
                    function
                        .ret_type
                        .as_ref()
                        .map(tidb_datatype::FieldType::decimal),
                    static_type_mut(&mut folded),
                ) {
                    ret_type.set_decimal(decimal);
                }
            }
            return (folded, is_deferred_const);
        }
        index += 2;
    }
    // An odd argument count means a trailing ELSE body; every condition was
    // constant-false, so that body is the result.
    if len % 2 == 1 {
        let (mut folded, is_deferred) = fold_constant_inner(&args[len - 1], ctx, opts);
        is_deferred_const = is_deferred_const || is_deferred;
        if matches!(folded, Expression::Constant(_)) {
            if let (Some(decimal), Some(ret_type)) = (
                function
                    .ret_type
                    .as_ref()
                    .map(tidb_datatype::FieldType::decimal),
                static_type_mut(&mut folded),
            ) {
                ret_type.set_decimal(decimal);
            }
        }
        return (folded, is_deferred_const);
    }
    (expr.clone(), is_deferred_const)
}

fn charset_and_collation_of(expr: &Expression) -> (String, String) {
    expr.static_type().map_or_else(
        || (String::new(), String::new()),
        |t| (t.charset_name().to_owned(), t.collation_name().to_owned()),
    )
}

fn repertoire_of(expr: &Expression) -> crate::expr_collation::Repertoire {
    match expr {
        Expression::Column(c) => c.collation.repertoire(),
        Expression::Constant(c) => c.collation.repertoire(),
        Expression::CorrelatedColumn(c) => c.column.collation.repertoire(),
        Expression::ScalarFunction(c) => c.collation.repertoire(),
    }
}

fn collation_info_of_mut(
    expr: &mut Expression,
) -> Option<&mut crate::expr_collation::CollationInfo> {
    match expr {
        Expression::Column(c) => Some(&mut c.collation),
        Expression::Constant(c) => Some(&mut c.collation),
        Expression::CorrelatedColumn(c) => Some(&mut c.column.collation),
        Expression::ScalarFunction(c) => Some(&mut c.collation),
    }
}

fn static_type_mut(expr: &mut Expression) -> Option<&mut tidb_datatype::FieldType> {
    match expr {
        Expression::Column(c) => c.ret_type.as_mut(),
        Expression::Constant(c) => c.ret_type.as_mut(),
        Expression::CorrelatedColumn(c) => c.column.ret_type.as_mut(),
        Expression::ScalarFunction(c) => c.ret_type.as_mut(),
    }
}
