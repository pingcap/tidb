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

//! `// boundary:` Go `NewFunction` / `NewFunctionInternal` /
//! `BuildCastFunction` / `BuildCastFunctionWithCheck`.
//!
//! Rewrites use this boundary for argument inference, collation, signature
//! selection and folding. RealFunctionBuilder supplies the live evaluation
//! context; PreservingFunctionBuilder deliberately retains the requested
//! structure for callers that do not perform construction-time evaluation.
//! These native construction paths remain part of the whole expression
//! package audit; this module does not claim package acceptance.

use super::traits::is_logical_op;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode};

/// Go's `err` from `NewFunction`: construction rejected the arguments.
///
/// Go's callers branch on `err != nil` in ways that change the RESULT (for
/// example `ColumnSubstituteImpl` reports a failed substitution rather than
/// propagating), so the failure has to be representable, not panicked on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionBuildError {
    /// The function that could not be built (Go `FuncName.L`).
    pub func_name: String,
    /// Why construction failed.
    pub reason: String,
}

impl std::fmt::Display for FunctionBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot build function {}: {}",
            self.func_name, self.reason
        )
    }
}

impl std::error::Error for FunctionBuildError {}

/// Go `BuildCastFunctionWithCheck`: pick the dedicated `cast_*` signature for
/// `target`.
///
/// The generic name `cast` is one of the four `NewFunction` explicitly
/// refuses, and the executor has no `cast` arm, so a placeholder node left
/// under that name fails at run time with "this scalar function is not yet
/// ported". `crate::simple_expr::build_cast_function` is the port of Go's
/// dedicated builder.
fn dedicated_cast(arg: Expression, target: FieldType) -> Result<Expression, FunctionBuildError> {
    crate::simple_expr::build_cast_function(arg, target, false).map_err(|error| {
        FunctionBuildError {
            func_name: "cast".to_owned(),
            reason: format!("{error:?}"),
        }
    })
}

/// Native cast signatures share Go's single ast.Cast rewrite contract.
pub(super) fn is_cast_name(name: &str) -> bool {
    name == "cast" || name.starts_with("cast_")
}

/// The construction half of Go's `BuildContext`, as the ported `util.go`
/// rewrites use it.
pub trait FunctionBuilder {
    /// Go `NewFunction(ctx, funcName, retType, args...)`.
    ///
    /// `ret_type` is the type Go passes in at the call site; a faithful
    /// implementation is free to override it during inference, exactly as
    /// `NewFunction` does.
    ///
    /// # Errors
    ///
    /// Returns [`FunctionBuildError`] when the arguments are not valid for
    /// `func_name`, mirroring Go's non-nil `err`.
    fn new_function(
        &self,
        func_name: &str,
        ret_type: Option<FieldType>,
        args: Vec<Expression>,
    ) -> Result<Expression, FunctionBuildError>;

    /// Go `BuildCastFunction(ctx, expr, tp)` and
    /// `BuildCastFunctionWithCheck(ctx, expr, tp, inUnion, isExplicitCharset)`.
    ///
    /// The two Go entry points differ only in whether an unsupported cast is
    /// reported or silently logged; both produce the same node when the cast
    /// is supported, so one method serves both call sites here and the
    /// difference is carried by the returned `Result`.
    ///
    /// # Errors
    ///
    /// Returns [`FunctionBuildError`] when the cast is not supported.
    fn build_cast(
        &self,
        arg: Expression,
        ret_type: Option<FieldType>,
        is_explicit_charset: bool,
    ) -> Result<Expression, FunctionBuildError> {
        let _ = is_explicit_charset;
        match ret_type {
            Some(target) => dedicated_cast(arg, target),
            None => self.new_function("cast", None, vec![arg]),
        }
    }

    /// Go `wrapWithIsTrue(ctx, keepNull=true, expr, wrapForInt=true)`, the one
    /// call `pushNotAcrossExpr` makes before descending through a `NOT`.
    ///
    /// Go skips the wrapper for an argument that is ALREADY a truth value:
    /// integer-valued and one of the [`is_logical_op`] functions. Everything
    /// else -- arithmetic, a cast, a bare column -- gets wrapped, and the
    /// wrapper is not cosmetic: it is what preserves three-valued logic once
    /// the `NOT` is pushed away, so `NOT NULL` stays NULL rather than becoming
    /// TRUE.
    ///
    /// # Errors
    ///
    /// Returns [`FunctionBuildError`] when the wrapper cannot be built.
    fn wrap_with_is_true(&self, expr: Expression) -> Result<Expression, FunctionBuildError> {
        // Go's `wrapForInt` is `true` at the one call site ported here
        // (`pushNotAcrossExpr`), so the `!wrapForInt` early return is not
        // reachable and is not reproduced.
        let is_int = expr
            .static_type()
            .is_some_and(|t| t.eval_type() == tidb_datatype::EvalType::Int);
        if is_int {
            if let Expression::ScalarFunction(child) = &expr {
                if is_logical_op(child.func_name.lowercase()) {
                    return Ok(expr);
                }
            }
        }
        // Go's `keepNull` is `true` at that call site, selecting
        // `IsTruthWithNull` over `IsTruthWithoutNull`.
        self.new_function("istrue_with_null", Some(tiny_int_type()), vec![expr])
    }
}

/// Go `types.NewFieldType(mysql.TypeTiny)`, the result type Go hands to
/// `NewFunctionInternal` for `not`, `isnull` and the truth wrappers.
#[must_use]
pub fn tiny_int_type() -> FieldType {
    FieldType::new(FieldTypeCode::Tiny)
}

/// GO `types.NewFieldType(mysql.TypeDouble)` — the cast target
/// `isTrueOrFalseFunctionClass.getFunction` prices a string/time/json
/// argument through (ETReal), rendering `cast(x, double BINARY)`.
#[must_use]
pub fn double_field_type() -> FieldType {
    FieldType::new(FieldTypeCode::Double)
}

/// The narrow default [`FunctionBuilder`]: builds the node Go's rewrite asks
/// for while KEEPING the caller-supplied result type verbatim.
///
/// What it reproduces: the tree SHAPE -- which function name wraps which
/// arguments, in which order. That is the entire content of the `util.go`
/// rewrites and is what a downstream planner rule reads.
///
/// What it does NOT reproduce, and what a real `NewFunction` adds:
///
/// - argument type inference and implicit cast insertion,
/// - collation and coercibility derivation over the new argument list,
/// - the constant folding `NewFunction` performs on the built node,
/// - the `getFunction` signature dispatch, and with it the rejection of
///   argument lists no signature accepts -- so this builder never returns
///   [`FunctionBuildError`], where Go sometimes does.
///
/// Swapping in the real builder upgrades every ported rewrite with no change
/// to the rewrites themselves.
#[derive(Clone, Copy, Debug, Default)]
pub struct PreservingFunctionBuilder;

impl FunctionBuilder for PreservingFunctionBuilder {
    fn new_function(
        &self,
        func_name: &str,
        ret_type: Option<FieldType>,
        args: Vec<Expression>,
    ) -> Result<Expression, FunctionBuildError> {
        let mut func = ScalarFunction::new(
            CiString::new(func_name),
            ret_type.clone().unwrap_or_else(tiny_int_type),
            args,
        );
        // `ScalarFunction::new` cannot express a nil `RetType`; restore it so a
        // caller that deliberately passed `None` gets `None` back.
        func.ret_type = ret_type;
        Ok(Expression::ScalarFunction(func))
    }
}

/// The FAITHFUL [`FunctionBuilder`]: Go's real `NewFunction`, with the
/// evaluation context it needs.
///
/// This is what closes the boundary this module describes. It routes every
/// rebuild through [`crate::new_function`], so a rewrite built with it gets
/// Go's argument type inference, null-type propagation, arity checking,
/// registry dispatch and post-construction constant folding -- the whole
/// contract [`PreservingFunctionBuilder`] defers.
///
/// Prefer this wherever an evaluation context is available.
pub struct RealFunctionBuilder<'a, C: crate::context::Columns> {
    ctx: &'a C,
}

impl<'a, C: crate::context::Columns> RealFunctionBuilder<'a, C> {
    /// Binds Go's `BuildContext` for the rebuilds this builder performs.
    pub fn new(ctx: &'a C) -> Self {
        RealFunctionBuilder { ctx }
    }
}

impl<C: crate::context::Columns> FunctionBuilder for RealFunctionBuilder<'_, C> {
    fn new_function(
        &self,
        func_name: &str,
        ret_type: Option<FieldType>,
        args: Vec<Expression>,
    ) -> Result<Expression, FunctionBuildError> {
        // Go `NewFunction`'s `case ast.Cast: return BuildCastFunction(ctx,
        // args[0], retType)` (`scalar_function.go:208`). This port names the
        // dedicated cast signatures `cast_decimal`, `cast_char`, ... instead
        // of the single `ast.Cast`, and the builtin registry refuses them, so
        // every `cast*` name rebuilds through the cast builder. Without this
        // a substitution that has to rebuild a cast (for example pushing a
        // predicate through a projection) reports `hasFail` and the predicate
        // is not pushed.
        if is_cast_name(func_name) && args.len() == 1 {
            return self.build_cast(
                args.into_iter().next().expect("one argument"),
                ret_type,
                false,
            );
        }
        // A nil `RetType` is Go's "infer it": `new_function_impl` replaces an
        // `Unspecified` type with the inferred one, so that is the right
        // spelling for `None`.
        let ret_type = ret_type.unwrap_or_else(|| FieldType::new(FieldTypeCode::Unspecified));
        crate::new_function::new_function(self.ctx, func_name, ret_type, args).map_err(|err| {
            FunctionBuildError {
                func_name: func_name.to_owned(),
                reason: format!("{err:?}"),
            }
        })
    }

    fn build_cast(
        &self,
        arg: Expression,
        ret_type: Option<FieldType>,
        is_explicit_charset: bool,
    ) -> Result<Expression, FunctionBuildError> {
        // Go routes `cast` to `BuildCastFunctionWithCheck`, a DEDICATED
        // builder that `NewFunction` explicitly refuses. `dedicated_cast` is
        // its port; the generic `cast` name has no executor arm, so a
        // placeholder left under it fails at run time.
        let Some(target) = ret_type else {
            return PreservingFunctionBuilder.new_function("cast", None, vec![arg]);
        };
        let mut expression = dedicated_cast(arg, target)?;
        let Expression::ScalarFunction(function) = &mut expression else {
            unreachable!("the dedicated cast builder produces a function")
        };
        let target = function.ret_type.as_ref().expect("cast target");
        let eval_type = target.eval_type();
        // Go newBaseBuiltinCastFunc4String preserves an explicitly requested
        // charset; ordinary casts derive signature collation from the context.
        let collation = if is_explicit_charset && eval_type == tidb_datatype::EvalType::String {
            crate::expr_collation::ExprCollation {
                coer: crate::expr_collation::Coercibility::EXPLICIT,
                repe: if target.charset_name() == "ascii" {
                    crate::expr_collation::Repertoire::ASCII
                } else {
                    crate::expr_collation::Repertoire::UNICODE
                },
                charset: target.charset_name().to_owned(),
                collation: target.collation_name().to_owned(),
            }
        } else {
            crate::collation_derive::derive_collation_with_connection(
                "cast",
                &function.args,
                eval_type,
                self.ctx.connection_charset_info(),
            )
            .map_err(|error| FunctionBuildError {
                func_name: "cast".to_owned(),
                reason: format!("{error:?}"),
            })?
        };
        function.collation.set_coercibility(collation.coer);
        function.collation.set_repertoire(collation.repe);
        function
            .collation
            .set_charset_and_collation(&collation.charset, &collation.collation);
        function.collation.set_explicit_charset(
            is_explicit_charset && eval_type == tidb_datatype::EvalType::String,
        );
        // BuildCastFunctionWithCheck leaves JSON unfolded because callers may
        // still change its parse flag after construction.
        if eval_type != tidb_datatype::EvalType::Json {
            crate::constant_fold::fold_constant_in_mode(
                &mut expression,
                self.ctx,
                crate::constant_fold::ConstantFoldMode::Normal,
            );
        }
        Ok(expression)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::expression::Expression;
    use tidb_datatype::FieldTypeCode;

    /// Go's `BuildCastFunctionWithCheck` picks the dedicated `cast_*`
    /// signature. The generic `cast` name is one of the four `NewFunction`
    /// refuses and has no executor arm, so a node left under it fails at run
    /// time with "this scalar function is not yet ported".
    #[test]
    fn build_cast_uses_the_dedicated_signature_name() {
        let argument =
            Expression::Column(Column::new(3, FieldType::new(FieldTypeCode::NewDecimal)));
        let mut target = FieldType::new(FieldTypeCode::NewDecimal);
        target.set_flen(34);
        target.set_decimal(2);
        let cast = PreservingFunctionBuilder
            .build_cast(argument, Some(target), false)
            .expect("a decimal cast builds");
        let Expression::ScalarFunction(function) = cast else {
            panic!("a cast is a scalar function");
        };
        assert_eq!(function.func_name.lowercase(), "cast_decimal");
    }

    /// Go `NewFunction`'s `case ast.Cast` (`scalar_function.go:208`): a
    /// `cast` name rebuilds through `BuildCastFunction`. This port names the
    /// dedicated signatures `cast_decimal`, ..., and the builtin registry
    /// refuses them, so `new_function` has to route them itself. Without the
    /// route a substitution that rebuilds a cast reports `hasFail`.
    #[test]
    fn new_function_routes_a_dedicated_cast_name_to_the_cast_builder() {
        let argument =
            Expression::Column(Column::new(3, FieldType::new(FieldTypeCode::NewDecimal)));
        let mut target = FieldType::new(FieldTypeCode::NewDecimal);
        target.set_flen(34);
        target.set_decimal(2);
        let builder = RealFunctionBuilder::new(&crate::NoColumns);
        let rebuilt = builder
            .new_function("cast_decimal", Some(target), vec![argument])
            .expect("a dedicated cast name rebuilds through the cast builder");
        let Expression::ScalarFunction(function) = rebuilt else {
            panic!("a cast is a scalar function");
        };
        assert_eq!(function.func_name.lowercase(), "cast_decimal");
    }
}
