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

//! Go `pkg/expression/constant_fold.go`, reduced to the part this tier can
//! observe.

use crate::expression::Expression;
use tidb_datatype::Datum;

/// How Go's expression rewriter constructs functions under the current AST
/// parent (`newFunctionWithInit`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConstantFoldMode {
    /// `expression.NewFunction`: fold the newly built function normally.
    #[default]
    Normal,
    /// `expression.NewFunctionTryFold`: keep the function when folding warns.
    Try,
    /// `expression.NewFunctionBase`: do not fold descendants in this scope.
    Disabled,
}

/// Runs the NOT NULL half of Go's `foldConstant`
/// (`pkg/expression/constant_fold.go`) over a freshly built tree, bottom up.
///
/// Go REPLACES a wholly-constant subtree with a `Constant`, and while doing so
/// stamps `NotNullFlag` on or off according to the value it computed. That
/// flag is the only part of the substitution anything downstream of this
/// rewriter can observe -- an evaluable tree gives the same rows either way --
/// so this walk computes the same fold and keeps only its flag, rather than
/// rewriting nodes this tier still evaluates directly.
///
/// The flag is not decoration: `SHOW COLUMNS` on a view reads it, which is how
/// `CREATE VIEW v AS SELECT CAST('' AS CHAR(32))` reports `NO` for a column no
/// base table declared NOT NULL.
pub fn derive_constant_null_flag(expr: &mut Expression) {
    let _ = fold_value(expr);
}

/// Folds exactly the function just built, under Go's selected construction
/// mode. Its arguments have already been rewritten and folded individually;
/// recursively evaluating them here would duplicate warnings and side effects.
pub fn fold_constant_in_mode(
    expr: &mut Expression,
    ctx: &impl crate::Columns,
    mode: ConstantFoldMode,
) {
    fold_constant_in_mode_inner(expr, ctx, mode, false);
}

/// Folds a planner expression while retaining constant coercions whose
/// evaluation can emit a statement warning. Go builds those casts and
/// charset conversions with the live statement context, so a planner-side
/// `NoColumns` fold would otherwise replace them with a value and permanently
/// lose the warning at execution. The flag is deliberately opt-in: DDL and
/// expression-unit callers that use an actual warning context retain the
/// ordinary Go construction-time fold.
pub fn fold_constant_in_mode_preserving_warning_casts(
    expr: &mut Expression,
    ctx: &impl crate::Columns,
    mode: ConstantFoldMode,
) {
    fold_constant_in_mode_inner(expr, ctx, mode, true);
}

fn fold_constant_in_mode_inner(
    expr: &mut Expression,
    ctx: &impl crate::Columns,
    mode: ConstantFoldMode,
    preserve_warning_casts: bool,
) {
    if mode == ConstantFoldMode::Disabled {
        return;
    }
    let original = (mode == ConstantFoldMode::Try).then(|| expr.clone());
    let warning_bookmark = ctx.warning_count();
    let _ = fold_current_value_in(expr, ctx, preserve_warning_casts);
    if mode == ConstantFoldMode::Try && ctx.warning_count() > warning_bookmark {
        ctx.truncate_warnings(warning_bookmark);
        *expr = original.expect("try-fold mode retained the original expression");
    }
}

fn fold_current_value_in(
    expr: &mut Expression,
    ctx: &impl crate::Columns,
    preserve_warning_casts: bool,
) -> Option<Datum> {
    if preserve_warning_casts && has_runtime_warning_cast(expr) {
        return None;
    }
    // Recursively fold sub-expressions FIRST (Go's `FoldConstant` walks
    // bottom-up): a nested `date_add_month("...", "...")` whose args are all
    // constants becomes a Constant before the parent checks its own args.
    // Lazy short-circuit functions are exempt: their UNTAKEN branches must
    // not evaluate -- `SELECT IF(1, 1, 1/0)` runs without dividing, so
    // plan-time folding inside them would fabricate both warnings and
    // errors the runtime never reaches.
    //
    // Unfoldable functions get the same treatment, for the reason Go's
    // `expression_rewriter.go:662/1912` encodes with `disableFoldCounter`:
    // inside `BENCHMARK`'s scope every function is built with
    // `NewFunctionBase`, which never folds -- not even its own constant
    // sub-expressions. Descending into their arguments here would fold a
    // subtree the source keeps runtime-only.
    if let Expression::ScalarFunction(func) = expr {
        let name_lc = func.func_name.lowercase();
        let lazy = is_lazy_short_circuit(&name_lc) || is_unfoldable(&name_lc);
        if !lazy {
            for arg in &mut func.args {
                fold_current_value_in(arg, ctx, preserve_warning_casts);
            }
        }
    }
    let func = match expr {
        Expression::Constant(constant) => return Some(constant.value.clone()),
        Expression::Column(_) | Expression::CorrelatedColumn(_) => return None,
        Expression::ScalarFunction(func) => func,
    };
    // Go `FoldConstant` copies the original expression's collation state onto
    // the replacement constant.  That state is semantic: metadata functions
    // such as COERCIBILITY and enclosing collation aggregation inspect it
    // after folding.
    let original_collation = func.collation.clone();
    let unfoldable = is_unfoldable(func.func_name.lowercase());
    let mut has_null_arg = false;
    let mut has_deferred_arg = false;
    let mut all_const_arg = true;
    for arg in &func.args {
        match arg {
            Expression::Constant(constant) => {
                has_null_arg |= constant.value.is_null();
                // Go's `foldConstant` carries ParamMarker/DeferredExpr
                // provenance onto the replacement Constant through its
                // `DeferredExpr` field.  Dropping that bit here turns a
                // context-only expression into a strict constant and lets a
                // cached plan freeze a parameter value.
                has_deferred_arg |=
                    constant.deferred_expr.is_some() || constant.param_marker.is_some();
            }
            _ => all_const_arg = false,
        }
    }
    if unfoldable || !all_const_arg {
        return None;
    }
    // Go `expression_rewriter.go:3016-3029`: a deferred function (the clock
    // family) folds into a Constant that carries the function as
    // `DeferredExpr`, so a cached plan re-evaluates it on every execution
    // instead of serving the folding-time value. `UNIX_TIMESTAMP` with
    // arguments is explicitly excluded (it is a normal expression of its
    // argument).
    let deferred_self = is_deferred_function(func.func_name.lowercase(), func.args.len());
    let value = crate::eval_expression_once(expr, ctx).ok()?;
    let mut ret_type = expr.static_type()?.clone();
    if !has_null_arg {
        if value.is_null() {
            ret_type.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        } else {
            ret_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        }
    }
    let mut folded = crate::constant::Constant::new(value.clone(), ret_type);
    folded.collation = original_collation;
    if has_deferred_arg || deferred_self {
        folded.deferred_expr = Some(Box::new(expr.clone()));
    }
    *expr = Expression::Constant(folded);
    Some(value)
}

/// Constant integer casts, builtins that wrap their arguments in Go's
/// `WrapWithCastAsInt`, and `CHAR(... USING charset)` use the statement context
/// to report truncation, out-of-range, or invalid-byte diagnostics. A
/// no-column planner context cannot retain those diagnostics, so callers that
/// are preparing an executable plan keep these nodes unfolded for runtime.
///
/// String/byte inputs always go through Go's prefix scanner and may report
/// truncation.  Real, float32, and decimal inputs are retained too: their
/// range checks report overflow through the same statement context, and
/// deciding whether a particular value is in range here would duplicate the
/// type-specific conversion rules.  Keeping the numeric carriers unfolded is
/// value-preserving and makes the runtime warning owner unambiguous.
fn has_runtime_warning_cast(expr: &Expression) -> bool {
    let Expression::ScalarFunction(function) = expr else {
        return false;
    };
    let name = function.func_name.lowercase();
    let indexes: &[usize] = match name {
        "cast_signed"
        | "cast_unsigned"
        | "cast_unsigned_in_union"
        | "vitess_hash"
        | "tidb_shard" => &[0],
        // `FORMAT(number, decimals)` wraps its second argument to ETInt.
        "format" => &[1],
        // `builtinCharSig.evalString` appends ErrInvalidCharacterString
        // (1300) while decoding the bytes produced by CHAR's numeric
        // arguments. The final constant is NULL for the no-USING form, which
        // uses the binary signature and has no decode warning to preserve.
        "char_func" => {
            return function.args.last().is_some_and(|charset| {
                matches!(
                    charset,
                    Expression::Constant(constant) if !constant.value.is_null()
                )
            });
        }
        _ => return false,
    };
    if !indexes.iter().any(|&index| {
        matches!(
            function.args.get(index),
            Some(Expression::Constant(constant))
                if matches!(
                    constant.value,
                    Datum::String(_)
                        | Datum::Bytes(_)
                        | Datum::Real(_)
                        | Datum::Float32(_)
                        | Datum::Decimal(_)
                )
        )
    }) {
        return false;
    }
    true
}

/// One node of Go's `foldConstant`: folds bottom up, returning the constant
/// value the node reduces to, or `None` when it does not reduce to one.
///
/// `None` is Go's "return the expression unfolded", which is what a parent
/// reads as `allConstArg = false`. An evaluation that FAILS also returns
/// `None` and leaves the flag alone, exactly as Go's `err != nil` arm returns
/// before the folded `Constant` is built.
fn fold_value(expr: &mut Expression) -> Option<Datum> {
    let func = match expr {
        Expression::Constant(constant) => return Some(constant.value.clone()),
        Expression::Column(_) | Expression::CorrelatedColumn(_) => return None,
        Expression::ScalarFunction(func) => func,
    };
    if is_unfoldable(func.func_name.lowercase()) {
        // Go still folds this node's ARGUMENTS -- `foldConstant` recurses
        // through `NewFunction` as each level is built -- it only refuses to
        // fold the unfoldable node itself.
        for arg in &mut func.args {
            let _ = fold_value(arg);
        }
        return None;
    }
    let mut has_null_arg = false;
    let mut all_const_arg = true;
    for arg in &mut func.args {
        match fold_value(arg) {
            Some(Datum::Null) => has_null_arg = true,
            Some(_) => {}
            None => all_const_arg = false,
        }
    }
    if !all_const_arg {
        return None;
    }
    // A constant tree reads no row and no session state, so Go's empty
    // `chunk.Row{}` over a resolver that answers nothing is the whole input.
    let chunk = {
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        chunk
    };
    let value = expr
        .eval(&crate::context::NoColumns, chunk.get_row(0))
        .ok()?;
    if !has_null_arg {
        let Expression::ScalarFunction(func) = expr else {
            unreachable!("matched as a scalar function above")
        };
        if let Some(ret_type) = func.ret_type.as_mut() {
            if matches!(value, Datum::Null) {
                ret_type.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
            } else {
                ret_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
            }
        }
    }
    Some(value)
}

/// Whether Go's `foldConstant` would have REPLACED this subtree with a
/// `*Constant` by the time an enclosing function's `getFunction` runs its
/// `args[i].(*Constant)` type switch.
///
/// Go folds during construction, so the switch never sees the unfolded shape.
/// This rewriter keeps the shape (see [`derive_constant_null_flag`]), so every
/// dispatch that Go keys on constant-ness has to ask this question instead of
/// testing the node kind -- and the two answers differ for exactly the cases
/// that matter: `CONCAT('1:00',':00')` is a `*Constant` in Go and a
/// `ScalarFunction` here, while a column reference is neither.
///
/// The predicate is `foldConstant`'s own gate: not one of the
/// [`is_unfoldable`] names, and every argument folds too. The one arm not
/// reproduced is Go's `err != nil` escape -- a constant subtree whose
/// evaluation FAILS is left unfolded there and reported constant here. That
/// arm only changes which of two error-or-domain choices a doomed expression
/// takes, never a value, so it is not worth evaluating the subtree once per
/// row to reproduce.
pub(crate) fn folds_to_constant(expr: &Expression) -> bool {
    match expr {
        Expression::Constant(_) => true,
        Expression::Column(_) | Expression::CorrelatedColumn(_) => false,
        Expression::ScalarFunction(func) => {
            !is_unfoldable(func.func_name.lowercase()) && func.args.iter().all(folds_to_constant)
        }
    }
}

/// The `*Constant`'s VALUE that Go's dispatch reads off a folded subtree --
/// `unaryMinusFunctionClass.handleIntOverflow` is the one rule here that needs
/// the value and not merely the fact of constancy (`arg.Value.GetInt64()`).
///
/// `None` for anything [`folds_to_constant`] rejects, and for a fold whose
/// evaluation fails -- which is Go's own `err != nil` arm, where the subtree
/// stays unfolded and the type switch therefore misses it. The evaluation runs
/// against no columns because a foldable subtree reads none; that is exactly
/// the `chunk.Row{}` Go's `foldConstant` passes.
pub(crate) fn folded_value(expr: &Expression) -> Option<Datum> {
    if let Expression::Constant(constant) = expr {
        return Some(constant.value.clone());
    }
    if !folds_to_constant(expr) {
        return None;
    }
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
    chunk.set_num_virtual_rows(1);
    expr.eval(&crate::context::NoColumns, chunk.get_row(0)).ok()
}

/// Go `unFoldableFunctions` (`pkg/expression/function_traits.go`): the
/// functions whose result is not a property of their arguments -- a clock, a
/// counter, a random source, a session variable, or a side effect.
pub fn is_unfoldable(name: &str) -> bool {
    // Go's `GetVar` is one name; this rewriter encodes the signature the
    // session's current value picked into the name (`getvar_int`,
    // `getvar_string`, ...), so a bare `"getvar"` arm below would never match
    // anything the rewriter builds. The prefix test is what actually fires.
    if name.starts_with("getvar_") {
        return true;
    }
    matches!(
        name,
        "sysdate"
            | "found_rows"
            | "rand"
            | "uuid"
            | "uuid_v4"
            | "uuid_v7"
            | "sleep"
            | "row"
            | "values"
            | "setvar"
            | "getparam"
            | "benchmark"
            | "dayname"
            // Reads the SESSION transaction context at evaluation: folding
            // it at plan time would freeze the zero the statement had before
            // its first read opened a snapshot.
            | "tidb_current_tso"
            | "nextval"
            | "lastval"
            | "setval"
            | "any_value"
    )
}

/// Go `IsDeferredFunctions` (`pkg/expression/function_traits.go:159-171`),
/// with the caller's `UNIX_TIMESTAMP`-with-arguments exception
/// (`expression_rewriter.go:3021`): these foldable clock functions must be
/// re-evaluated per execution when a plan cache reuses the built tree.
fn is_deferred_function(name: &str, arg_count: usize) -> bool {
    let clock = matches!(
        name,
        "now"
            | "random_bytes"
            | "current_timestamp"
            | "utc_time"
            | "curtime"
            | "current_time"
            | "utc_timestamp"
            | "unix_timestamp"
            | "curdate"
            | "current_date"
            | "utc_date"
    );
    clock && (name != "unix_timestamp" || arg_count == 0)
}

/// Whether the function evaluates only SOME of its arguments at run time.
/// Folding inside one would evaluate branches or operands the executor can
/// skip, changing warnings and errors (`IF(1, 1, 1/0)` divides).
fn is_lazy_short_circuit(name: &str) -> bool {
    matches!(
        name,
        "if" | "ifnull" | "case" | "case_when" | "and" | "or" | "xor" | "nullif" | "coalesce"
            // Go `TryFoldFunctions` (function_traits.go:81) puts `ast.Interval`
            // in the try-fold scope: its arguments are try-folded and any
            // warning keeps the function unfolded. Skipping the arguments in
            // the walk reproduces that "no build-time warning" outcome.
            | "interval"
    )
}

#[cfg(test)]
mod deferred_function_tests {
    use super::super::context::Columns;
    use super::*;
    use crate::context::NoColumns;
    use crate::expression::{Constant, Expression, ScalarFunction};
    use tidb_ast::CiString;
    use tidb_datatype::FieldType;

    /// A context whose statement clock the test pins, so NOW() evaluations
    /// are deterministic and two executions can observe the difference.
    struct WarningCollector(std::cell::RefCell<Vec<(u16, String)>>);

    impl Columns for WarningCollector {
        fn get(&self, _path: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
        fn warning_count(&self) -> usize {
            self.0.borrow().len()
        }
    }

    struct Clock(u64);

    impl Columns for Clock {
        fn get(&self, _path: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            Some((self.0 as i64, 0, 0))
        }
    }

    /// Go `expression_rewriter.go:662/1912`: entering a `BENCHMARK` call
    /// increments `disableFoldCounter`, and functions inside that scope are
    /// built with `NewFunctionBase` -- the fold never touches them, not even
    /// their own constant sub-expressions. The tree-walk mirror is: the
    /// recursion must not descend into an unfoldable function's arguments.
    #[test]
    /// Go `TryFoldFunctions` includes `ast.Interval` (function_traits.go:81):
    /// INTERVAL's arguments are try-folded, and a warning during that fold
    /// keeps the function UNFOLDED with no warning raised at build time. The
    /// walk therefore must not descend into INTERVAL's arguments.
    #[test]
    fn interval_scope_does_not_raise_child_warnings_during_fold() {
        // INTERVAL(1, 0, <cast that warns 1292>): the cast child must not be
        // folded, so the fold itself must not append the 1292 warning.
        let mut expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("interval"),
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            vec![
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(1),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(0),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("intdiv"),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                    vec![
                        Expression::Constant(crate::constant::Constant::new(
                            Datum::Int(1),
                            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        )),
                        Expression::Constant(crate::constant::Constant::new(
                            Datum::Int(0),
                            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        )),
                    ],
                )),
            ],
        ));
        let warnings = WarningCollector(std::cell::RefCell::new(Vec::new()));
        fold_constant_in_mode(&mut expr, &warnings, ConstantFoldMode::Normal);
        assert!(
            warnings.0.borrow().is_empty(),
            "child warnings leaked into the fold: {:?}",
            warnings.0.borrow()
        );
    }

    fn benchmark_scope_keeps_its_subtree_unfolded() {
        // BENCHMARK(1, CAST('x' AS SIGNED)): the inner cast folds only if the
        // walk descends into the unfoldable parent's children.
        let inner = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            vec![
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(1),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(2),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
            ],
        ));
        let mut expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("benchmark"),
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            vec![
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(1),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
                inner,
            ],
        ));
        fold_constant_in_mode(&mut expr, &NoColumns, ConstantFoldMode::Normal);

        // The benchmark's cast argument must still be a scalar function.
        match &expr {
            Expression::ScalarFunction(benchmark) => match &benchmark.get_args()[1] {
                Expression::ScalarFunction(_) => {}
                other => panic!("the cast argument was folded: {other:?}"),
            },
            other => panic!("benchmark itself was folded: {other:?}"),
        }
    }

    #[test]
    fn deferred_function_fold_re_evaluates_per_execution() {
        let now_node = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("now"),
            FieldType::new(tidb_datatype::FieldTypeCode::Datetime),
            vec![],
        ));

        // Fold once under clock 1000.
        let mut expr = now_node.clone();
        fold_constant_in_mode(&mut expr, &Clock(1000), ConstantFoldMode::Try);

        // The folded constant must carry the deferred provenance: evaluating
        // the SAME folded node under a LATER statement clock must serve the
        // fresh value, not the folding-time one (Go `Constant.DeferredExpr`,
        // expression_rewriter.go:3016-3029).
        // Re-executing under a LATER statement clock must re-evaluate: the
        // folding-time value (00:16:40) must not be served again.
        let later_value = expr
            .eval(
                &Clock(5000),
                tidb_chunk::chunk::Chunk::new(&[], 1, 1).get_row(0),
            )
            .unwrap();
        let later = later_value.sql_string().unwrap();
        assert!(
            later.contains("01:23:20"),
            "clock 5000 must render 01:23:20, got {later}"
        );
        // And the first execution's own value stayed at its own clock.
        let first_value = expr
            .eval(
                &Clock(1000),
                tidb_chunk::chunk::Chunk::new(&[], 1, 1).get_row(0),
            )
            .unwrap();
        let first = first_value.sql_string().unwrap();
        assert!(
            first.contains("00:16:40"),
            "clock 1000 must render 00:16:40, got {first}"
        );
    }
}
