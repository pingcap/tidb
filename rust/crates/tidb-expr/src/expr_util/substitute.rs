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

//! Column and correlated-column substitution.
//!
//! Go sources: `pkg/expression/util.go:546`-`:817` (`ColumnSubstitute`,
//! `ColumnSubstituteAll`, `ColumnSubstituteImpl`, `SubstituteCorCol2Constant`),
//! `pkg/expression/expression.go:953`-`:1058` (`EvaluateExprWithNull` and its
//! two private walks), plus `util.go:1358` `PopRowFirstArg` and `:1476`
//! `BuildNotNullExpr`, which are rewrites of the same kind.
//!
//! Every function here rebuilds nodes and therefore takes a
//! [`FunctionBuilder`]; see [`super::builder`] for what that boundary defers.

use super::builder::{tiny_int_type, FunctionBuildError, FunctionBuilder};
use super::extract::set_expr_column_in_operand;
use super::traits::check_collation_strictness;
use crate::collation_derive::{check_and_derive_collation_from_exprs, coercibility_of};
use crate::constant::Constant;
use crate::context::{Columns, EvalError};
use crate::expression::Expression;
use crate::schema::Schema;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

/// The construction-context flags `ColumnSubstituteImpl` reads off Go's
/// `BuildContext`, plus the builder it needs.
pub struct SubstituteOptions<'a> {
    /// `// boundary:` Go `NewFunction` -- see [`super::builder`].
    pub builder: &'a dyn FunctionBuilder,

    /// Go `ctx.IsConstantPropagateCheck()`: set while constant propagation is
    /// deciding whether a substitution is legal. It gates the `LENGTH()` guard
    /// below and nothing else.
    pub constant_propagate_check: bool,

    /// Go `collate.NewCollationEnabled()`. When false, Go SKIPS the entire
    /// per-argument collation-compatibility check and substitutes freely.
    pub new_collation_enabled: bool,
}

impl<'a> SubstituteOptions<'a> {
    /// The options Go uses outside constant propagation, with new collations
    /// enabled -- the default TiDB configuration.
    #[must_use]
    pub fn new(builder: &'a dyn FunctionBuilder) -> Self {
        SubstituteOptions {
            builder,
            constant_propagate_check: false,
            new_collation_enabled: true,
        }
    }
}

/// Go `ColumnSubstituteImpl`'s three return values (`util.go:566`).
#[derive(Clone, Debug)]
pub struct SubstituteOutcome {
    /// Go's first `bool`: whether `expr` actually changed.
    pub substituted: bool,
    /// Go's second `bool`: whether the expression SHOULD have changed -- it
    /// had a dependency in the schema with a substitute available -- but the
    /// substitution was abandoned for collation compatibility and the original
    /// was kept.
    ///
    /// Projection elimination cannot accept such a fallback, which is why it
    /// is reported separately rather than folded into `substituted`.
    pub has_fail: bool,
    /// The original or the rewritten expression, per `substituted`.
    pub expr: Expression,
}

/// Go `ColumnSubstitute` (`util.go:546`): replaces columns of `schema` in
/// `expr` by the matching expression in `new_exprs`.
///
/// `select * from (select b as a from t) k where a < 10` becomes
/// `select * from (select b as a from t where b < 10) k`.
///
/// As Go's own TODO notes, this form SWALLOWS the failure signal; prefer
/// [`column_substitute_impl`] when the caller can act on it.
#[must_use]
pub fn column_substitute(
    expr: &Expression,
    schema: &Schema,
    new_exprs: &[Expression],
    opts: &SubstituteOptions<'_>,
) -> Expression {
    column_substitute_impl(expr, schema, new_exprs, false, opts).expr
}

/// Go `ColumnSubstituteAll` (`util.go:556`): substitution that refuses to be
/// PARTIAL.
///
/// Either every substitutable column is replaced, or nothing is. The returned
/// flag is Go's `hasFail`: true means the all-or-nothing contract was broken
/// and the caller must not use the result.
#[must_use]
pub fn column_substitute_all(
    expr: &Expression,
    schema: &Schema,
    new_exprs: &[Expression],
    opts: &SubstituteOptions<'_>,
) -> (bool, Expression) {
    let outcome = column_substitute_impl(expr, schema, new_exprs, true, opts);
    (outcome.has_fail, outcome.expr)
}

/// Go `ColumnSubstituteImpl` (`util.go:566`): the full substitution walk.
///
/// `fail1return` is Go's parameter of the same name: when set, the walk
/// ABORTS at the first argument whose substitution had to be abandoned,
/// instead of falling back to the original argument and continuing.
///
/// The three special arms Go carves out, all preserved here:
///
/// 1. `CAST` and `GROUPING` substitute only their FIRST argument and rebuild
///    through a cast/clone rather than `NewFunction`, because both carry
///    result-type or metadata state that re-inference would destroy.
/// 2. Under constant propagation, `LENGTH(col)` is NOT substituted when the
///    column's collation is `PAD SPACE` -- the padded length differs from the
///    literal's. (TiDB issue #53730.)
/// 3. `EQ` is rebuilt with the constant on the RIGHT, so plan output stays
///    stable regardless of which side the substitution landed on.
#[must_use]
pub fn column_substitute_impl(
    expr: &Expression,
    schema: &Schema,
    new_exprs: &[Expression],
    fail1return: bool,
    opts: &SubstituteOptions<'_>,
) -> SubstituteOutcome {
    match expr {
        Expression::Column(column) => {
            let id = schema.column_index(column);
            if id < 0 {
                return unchanged(expr);
            }
            let Some(new_expr) = new_exprs.get(id as usize) else {
                // Go indexes `newExprs[id]` unguarded; a schema and a
                // substitute list of different lengths is a caller bug there
                // and a panic. Reporting "not substituted" keeps the same
                // result for every well-formed caller without the panic.
                return unchanged(expr);
            };
            let new_expr = if column.in_operand {
                set_expr_column_in_operand(new_expr.clone())
            } else {
                new_expr.clone()
            };
            SubstituteOutcome {
                substituted: true,
                has_fail: false,
                expr: new_expr,
            }
        }
        Expression::ScalarFunction(function) => {
            let name = function.func_name.lowercase().to_owned();

            // (1) CAST / GROUPING: first argument only.
            if name == "cast" || name == "grouping" {
                return substitute_cast_or_grouping(expr, schema, new_exprs, fail1return, opts);
            }

            // (2) The PAD SPACE guard on LENGTH() during constant propagation.
            if opts.constant_propagate_check && name == "length" {
                if let Some(Expression::Column(arg0)) = function.get_args().first() {
                    let id = schema.column_index(arg0);
                    if id >= 0 {
                        let is_constant =
                            matches!(new_exprs.get(id as usize), Some(Expression::Constant(_)));
                        let mapped_collate = schema
                            .columns
                            .get(id as usize)
                            .and_then(|c| c.get_static_type())
                            .map(tidb_datatype::FieldType::collation_name);
                        // `utf8mb4_bin` / `utf8_bin` are the PAD SPACE
                        // collations Go names here explicitly.
                        if is_constant && matches!(mapped_collate, Some("utf8mb4_bin" | "utf8_bin"))
                        {
                            return unchanged(expr);
                        }
                    }
                }
            }

            substitute_function_args(expr, schema, new_exprs, fail1return, opts)
        }
        _ => unchanged(expr),
    }
}

fn unchanged(expr: &Expression) -> SubstituteOutcome {
    SubstituteOutcome {
        substituted: false,
        has_fail: false,
        expr: expr.clone(),
    }
}

/// The `ast.Cast` / `ast.Grouping` arm of `ColumnSubstituteImpl`.
fn substitute_cast_or_grouping(
    expr: &Expression,
    schema: &Schema,
    new_exprs: &[Expression],
    fail1return: bool,
    opts: &SubstituteOptions<'_>,
) -> SubstituteOutcome {
    let Expression::ScalarFunction(function) = expr else {
        return unchanged(expr);
    };
    let Some(arg0) = function.get_args().first() else {
        return unchanged(expr);
    };
    let inner = column_substitute_impl(arg0, schema, new_exprs, fail1return, opts);
    if fail1return && inner.has_fail {
        return SubstituteOutcome {
            substituted: inner.substituted,
            has_fail: inner.has_fail,
            expr: expr.clone(),
        };
    }
    if !inner.substituted {
        return unchanged(expr);
    }

    // Go captures the ORIGINAL flag and restores it onto the rebuilt node,
    // because cast construction recomputes flags from the new argument.
    let original_flags = function
        .ret_type
        .as_ref()
        .map(tidb_datatype::FieldType::flags);
    let is_cast = function.func_name.lowercase() == "cast";

    let rebuilt = if is_cast {
        // Go deep-copies the new argument's RetType first: cast construction
        // MUTATES it, and the argument may be shared with another tree. Values
        // here are owned clones already, so the copy is implicit.
        let mut new_arg = inner.expr;
        if let Expression::ScalarFunction(arg_func) = &mut new_arg {
            arg_func.ret_type = arg_func.ret_type.clone();
        }
        opts.builder.build_cast(
            new_arg,
            function.ret_type.clone(),
            function.collation.is_explicit_charset(),
        )
    } else {
        // Grouping is recreated by CLONE, not by construction: it carries
        // grouping metadata that `NewFunction` would not reproduce.
        let mut cloned = function.clone();
        if let Some(slot) = cloned.args.first_mut() {
            *slot = inner.expr;
        }
        // The args changed in place, so the cached hash code is stale.
        cloned.clean_hash_code();
        Ok(Expression::ScalarFunction(cloned))
    };

    let Ok(mut rebuilt) = rebuilt else {
        // Go `terror.Log(err)` then proceeds with a nil `e`; treating a failed
        // rebuild as "no substitution" is the only non-panicking reading.
        return unchanged(expr);
    };

    // Go: `e.SetCoercibility(v.Coercibility())` and
    // `e.GetType(ctx).SetFlag(flag)`.
    let coercibility = coercibility_of(expr);
    if let Some(collation) = collation_info_of_mut(&mut rebuilt) {
        collation.set_coercibility(coercibility);
    }
    if let (Some(flags), Some(ret_type)) = (original_flags, static_type_mut(&mut rebuilt)) {
        ret_type.set_flags(flags);
    }
    SubstituteOutcome {
        substituted: true,
        has_fail: false,
        expr: rebuilt,
    }
}

/// The general arm of `ColumnSubstituteImpl`: substitute every argument, then
/// rebuild if anything changed.
fn substitute_function_args(
    expr: &Expression,
    schema: &Schema,
    new_exprs: &[Expression],
    fail1return: bool,
    opts: &SubstituteOptions<'_>,
) -> SubstituteOutcome {
    let Expression::ScalarFunction(function) = expr else {
        return unchanged(expr);
    };
    let name = function.func_name.lowercase().to_owned();
    let eval_type = function.ret_type.as_ref().map_or(
        tidb_datatype::EvalType::Int,
        tidb_datatype::FieldType::eval_type,
    );

    // Go's `cowExprRef` is copy-on-write purely to avoid allocating an args
    // array when nothing changes; an owned Vec is the same result.
    let mut ref_expr_arr: Vec<Expression> = function.get_args().to_vec();

    let Ok(old_coll) = check_and_derive_collation_from_exprs(&name, eval_type, function.get_args())
    else {
        // Go logs and gives up on the substitution entirely.
        return unchanged(expr);
    };

    let mut substituted = false;
    let mut has_fail = false;
    for (idx, arg) in function.get_args().iter().enumerate() {
        let inner = column_substitute_impl(arg, schema, new_exprs, fail1return, opts);
        if fail1return && inner.has_fail {
            return SubstituteOutcome {
                substituted: inner.substituted,
                has_fail: true,
                expr: expr.clone(),
            };
        }
        let old_changed = inner.substituted;
        let mut changed = inner.substituted;

        if opts.new_collation_enabled && changed {
            // Keep the function's own collation unchanged, and require the
            // replacement's result collation to be no WEAKER than the
            // original's -- otherwise a pushed-down projection compares
            // differently from the one it replaced.
            changed = false;
            let mut probe = ref_expr_arr.clone();
            probe[idx] = inner.expr.clone();
            let Ok(new_coll) = check_and_derive_collation_from_exprs(&name, eval_type, &probe)
            else {
                // The substituted arguments are invalid under collation rules:
                // an unsafe substitution, not merely a skipped one.
                return SubstituteOutcome {
                    substituted: false,
                    has_fail: true,
                    expr: expr.clone(),
                };
            };
            if old_coll.collation == new_coll.collation {
                let new_collate = inner
                    .expr
                    .static_type()
                    .map(tidb_datatype::FieldType::collation_name);
                let arg_collate = arg
                    .static_type()
                    .map(tidb_datatype::FieldType::collation_name);
                if new_collate == arg_collate
                    && coercibility_of(&inner.expr) == coercibility_of(arg)
                {
                    changed = true;
                } else {
                    changed = check_collation_strictness(
                        &old_coll.collation,
                        new_collate.unwrap_or_default(),
                    );
                }
            }
        }

        has_fail = has_fail || inner.has_fail || old_changed != changed;
        if fail1return && old_changed != changed {
            // Reachable only as `old_changed == true && changed == false`: the
            // argument HAD a substitute but the collation check rejected it.
            // A caller that cannot accept a fallback must hear about it.
            return SubstituteOutcome {
                substituted: changed,
                has_fail: true,
                expr: expr.clone(),
            };
        }
        if changed {
            ref_expr_arr[idx] = inner.expr;
            substituted = true;
        }
    }

    if !substituted {
        return unchanged(expr);
    }

    // Keep `col = value` ordering so plan output does not flip with the side
    // the substitution landed on.
    let args = if name == "eq"
        && ref_expr_arr.len() == 2
        && matches!(ref_expr_arr[0], Expression::Constant(_))
    {
        vec![ref_expr_arr[1].clone(), ref_expr_arr[0].clone()]
    } else {
        ref_expr_arr
    };

    match opts
        .builder
        .new_function(&name, function.ret_type.clone(), args)
    {
        Ok(new_func) => SubstituteOutcome {
            substituted: true,
            has_fail,
            expr: new_func,
        },
        Err(_) => SubstituteOutcome {
            substituted: true,
            has_fail: true,
            expr: expr.clone(),
        },
    }
}

/// Go `SubstituteCorCol2Constant` (`util.go:774`): replaces every correlated
/// column by the constant it is currently bound to, folding any subtree that
/// becomes wholly constant.
///
/// This is what turns a correlated subquery's filter into one the outer plan
/// can evaluate directly, once the outer row is known.
///
/// # Errors
///
/// Returns [`SubstituteError`] when evaluating a now-constant subtree fails,
/// or when rebuilding a function does.
pub fn substitute_cor_col_2_constant(
    expr: &Expression,
    ctx: &impl Columns,
    opts: &SubstituteOptions<'_>,
) -> Result<Expression, SubstituteError> {
    match expr {
        Expression::ScalarFunction(function) => {
            let mut all_constant = true;
            let mut new_args = Vec::with_capacity(function.get_args().len());
            for arg in function.get_args() {
                let new_arg = substitute_cor_col_2_constant(arg, ctx, opts)?;
                all_constant = all_constant && matches!(new_arg, Expression::Constant(_));
                new_args.push(new_arg);
            }
            if all_constant {
                // Go evaluates the ORIGINAL `x`, not the rebuilt one: its
                // arguments already answer from their bound values.
                let value = eval_once(expr, ctx)?;
                let mut constant = Constant::new(
                    value,
                    expr.static_type().cloned().unwrap_or_else(tiny_int_type),
                );
                constant.ret_type = expr.static_type().cloned();
                return Ok(Expression::Constant(constant));
            }
            let name = function.func_name.lowercase();
            if name == "cast" {
                return Ok(opts.builder.build_cast(
                    new_args.swap_remove(0),
                    function.ret_type.clone(),
                    function.collation.is_explicit_charset(),
                )?);
            }
            if name == "grouping" {
                // Clone, not rebuild -- grouping metadata again.
                let mut cloned = function.clone();
                if let Some(slot) = cloned.args.first_mut() {
                    *slot = new_args.swap_remove(0);
                }
                cloned.clean_hash_code();
                return Ok(Expression::ScalarFunction(cloned));
            }
            Ok(opts
                .builder
                .new_function(name, function.ret_type.clone(), new_args)?)
        }
        Expression::CorrelatedColumn(cor) => {
            // Go dereferences `*x.Data` unguarded: a correlated column reached
            // here is always bound. An unbound one becomes NULL rather than a
            // panic, which is the value an unbound correlated column reads as.
            let value = cor.data.clone().unwrap_or(Datum::Null);
            let mut constant = Constant::new(
                value,
                cor.column
                    .get_static_type()
                    .cloned()
                    .unwrap_or_else(tiny_int_type),
            );
            constant.ret_type = cor.column.get_static_type().cloned();
            Ok(Expression::Constant(constant))
        }
        Expression::Constant(constant) if constant.deferred_expr.is_some() => {
            // Go folds the deferred expression and REWRAPS the value with the
            // constant's own type, dropping the deferral.
            let value = super::fold::fold_constant_value(expr, ctx)?;
            let mut folded = Constant::new(
                value,
                constant.ret_type.clone().unwrap_or_else(tiny_int_type),
            );
            folded.ret_type = constant.ret_type.clone();
            Ok(Expression::Constant(folded))
        }
        other => Ok(other.clone()),
    }
}

/// Go `EvaluateExprWithNull` (`expression.go:953`): sets every column of
/// `schema` to NULL and reduces `expr` as far as it goes.
///
/// A result that is not a `Constant` means the outcome is unknown. This is the
/// engine of null-rejection analysis: if a join predicate evaluates to
/// FALSE or NULL once the inner side is nulled, the outer join can be
/// rewritten as an inner join.
///
/// `in_null_reject_check` selects Go's second walk
/// (`evaluateExprWithNullInNullRejectCheck`, `expression.go:993`), which
/// tracks WHICH NULLs came from the schema so that `AND`/`OR` do not swallow
/// an unrelated one.
///
/// `// narrowing:` Go also calls `ctx.SetSkipPlanCache(...)` when
/// `skipPlanCacheCheck` is set and the expression holds a mutable constant.
/// That is a side effect on the session's plan-cache decision, not part of the
/// returned expression, and this crate has no plan-cache context to set it on.
/// [`super::predicates::maybe_over_optimized_4_plan_cache`] is the predicate
/// Go tests, and is ported; the caller performs the marking.
///
/// # Errors
///
/// Returns [`SubstituteError`] when a rebuild or a deferred fold fails.
pub fn evaluate_expr_with_null(
    expr: &Expression,
    schema: &Schema,
    in_null_reject_check: bool,
    ctx: &impl Columns,
    opts: &SubstituteOptions<'_>,
) -> Result<Expression, SubstituteError> {
    if in_null_reject_check {
        return Ok(evaluate_expr_with_null_in_null_reject_check(expr, schema, ctx, opts)?.0);
    }
    evaluate_expr_with_null_plain(expr, schema, ctx, opts)
}

/// Go `evaluateExprWithNull` (`expression.go:964`).
fn evaluate_expr_with_null_plain(
    expr: &Expression,
    schema: &Schema,
    ctx: &impl Columns,
    opts: &SubstituteOptions<'_>,
) -> Result<Expression, SubstituteError> {
    match expr {
        Expression::ScalarFunction(function) => {
            let mut args = Vec::with_capacity(function.get_args().len());
            for arg in function.get_args() {
                args.push(evaluate_expr_with_null_plain(arg, schema, ctx, opts)?);
            }
            Ok(opts.builder.new_function(
                function.func_name.lowercase(),
                function.ret_type.clone(),
                args,
            )?)
        }
        Expression::Column(column) => {
            if schema.contains(column) {
                Ok(null_constant())
            } else {
                Ok(expr.clone())
            }
        }
        Expression::Constant(constant) if constant.deferred_expr.is_some() => {
            Ok(super::fold::fold_constant(expr, ctx, opts))
        }
        other => Ok(other.clone()),
    }
}

/// Go `evaluateExprWithNullInNullRejectCheck` (`expression.go:993`).
///
/// The extra `bool` is Go's `nullFromSet`: whether a NULL result came from a
/// column this walk nulled, as opposed to a NULL already in the expression.
fn evaluate_expr_with_null_in_null_reject_check(
    expr: &Expression,
    schema: &Schema,
    ctx: &impl Columns,
    opts: &SubstituteOptions<'_>,
) -> Result<(Expression, bool), SubstituteError> {
    match expr {
        Expression::ScalarFunction(function) => {
            let mut args = Vec::with_capacity(function.get_args().len());
            let mut null_from_sets = Vec::with_capacity(function.get_args().len());
            for arg in function.get_args() {
                let (res, null_from_set) =
                    evaluate_expr_with_null_in_null_reject_check(arg, schema, ctx, opts)?;
                args.push(res);
                null_from_sets.push(null_from_set);
            }
            let all_args_null_from_set =
                !args.iter().zip(&null_from_sets).any(|(arg, from_set)| {
                    matches!(arg, Expression::Constant(c) if c.value.is_null()) && !*from_set
                });

            let name = function.func_name.lowercase().to_owned();
            if name == "and" || name == "or" {
                // A NULL that came from the nulled schema must not decide an
                // AND/OR whose other side is still unknown: the real row might
                // have had any value there. Neutralize it -- 1 for AND, 0 for
                // OR -- so the operator's result stays driven by the argument
                // that is actually known.
                let has_non_constant = args
                    .iter()
                    .any(|arg| !matches!(arg, Expression::Constant(_)));
                if has_non_constant {
                    for (index, arg) in args.iter_mut().enumerate() {
                        let is_schema_null = matches!(arg, Expression::Constant(c) if c.value.is_null())
                            && null_from_sets[index];
                        if is_schema_null {
                            // Go `break`s after the FIRST such argument.
                            *arg = if name == "and" {
                                Expression::Constant(Constant::new_one())
                            } else {
                                Expression::Constant(Constant::new_zero())
                            };
                            break;
                        }
                    }
                }
            }

            let built = opts
                .builder
                .new_function(&name, function.ret_type.clone(), args)?;
            let result_null_from_set = matches!(&built, Expression::Constant(c) if c.value.is_null())
                && all_args_null_from_set;
            Ok((built, result_null_from_set))
        }
        Expression::Column(column) => {
            if schema.contains(column) {
                Ok((null_constant(), true))
            } else {
                Ok((expr.clone(), false))
            }
        }
        Expression::Constant(constant) if constant.deferred_expr.is_some() => {
            Ok((super::fold::fold_constant(expr, ctx, opts), false))
        }
        other => Ok((other.clone(), false)),
    }
}

/// Go `&Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}`.
fn null_constant() -> Expression {
    Expression::Constant(Constant::new(
        Datum::Null,
        FieldType::new(FieldTypeCode::Null),
    ))
}

/// Go `PopRowFirstArg` (`util.go:1358`): drops the first element of a row.
///
/// `(1, 2, 3)` becomes `(2, 3)`; `(1, 2)` becomes the bare `2`, because a
/// one-element row is not a row in MySQL. A non-row argument yields `None`,
/// which is Go's nil return.
///
/// # Errors
///
/// Returns [`SubstituteError`] when rebuilding the shortened row fails.
pub fn pop_row_first_arg(
    e: &Expression,
    opts: &SubstituteOptions<'_>,
) -> Result<Option<Expression>, SubstituteError> {
    let Expression::ScalarFunction(function) = e else {
        return Ok(None);
    };
    if function.func_name.lowercase() != "row" {
        return Ok(None);
    }
    let args = function.get_args();
    if args.len() == 2 {
        return Ok(Some(args[1].clone()));
    }
    let rest = args[1..].to_vec();
    Ok(Some(opts.builder.new_function(
        "row",
        function.ret_type.clone(),
        rest,
    )?))
}

/// Go `BuildNotNullExpr` (`util.go:1476`): wraps `expr` as `NOT(ISNULL(expr))`.
///
/// # Errors
///
/// Returns [`SubstituteError`] when either wrapper cannot be built.
pub fn build_not_null_expr(
    expr: Expression,
    opts: &SubstituteOptions<'_>,
) -> Result<Expression, SubstituteError> {
    let is_null = opts
        .builder
        .new_function("isnull", Some(tiny_int_type()), vec![expr])?;
    Ok(opts
        .builder
        .new_function("not", Some(tiny_int_type()), vec![is_null])?)
}

/// A failure from a substitution walk: either function construction or the
/// evaluation of a subtree that became constant.
#[derive(Clone, Debug)]
pub enum SubstituteError {
    /// Go's `err` from `NewFunction` / `BuildCastFunction`.
    Build(FunctionBuildError),
    /// Go's `err` from `Eval`.
    Eval(EvalError),
}

impl From<FunctionBuildError> for SubstituteError {
    fn from(err: FunctionBuildError) -> Self {
        SubstituteError::Build(err)
    }
}

impl From<EvalError> for SubstituteError {
    fn from(err: EvalError) -> Self {
        SubstituteError::Eval(err)
    }
}

impl std::fmt::Display for SubstituteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubstituteError::Build(err) => write!(f, "{err}"),
            SubstituteError::Eval(err) => write!(f, "{err:?}"),
        }
    }
}

impl std::error::Error for SubstituteError {}

/// Go `expr.Eval(ctx.GetEvalCtx(), chunk.Row{})`: evaluating against the empty
/// row, which is what a wholly constant subtree needs.
pub(super) fn eval_once(expr: &Expression, ctx: &impl Columns) -> Result<Datum, EvalError> {
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
    chunk.set_num_virtual_rows(1);
    expr.eval(ctx, chunk.get_row(0))
}

/// The `collationInfo` of whichever node kind `expr` is, for the
/// `SetCoercibility` calls Go makes on a rebuilt node.
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

/// The mutable result type of any node kind, for the `SetFlag` calls Go makes.
fn static_type_mut(expr: &mut Expression) -> Option<&mut FieldType> {
    match expr {
        Expression::Column(c) => c.ret_type.as_mut(),
        Expression::Constant(c) => c.ret_type.as_mut(),
        Expression::CorrelatedColumn(c) => c.column.ret_type.as_mut(),
        Expression::ScalarFunction(c) => c.ret_type.as_mut(),
    }
}
