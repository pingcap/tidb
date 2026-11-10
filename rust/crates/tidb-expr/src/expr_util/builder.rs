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
//! context.
//! These native construction paths remain part of the whole expression
//! package audit; this module does not claim package acceptance.

use super::traits::is_logical_op;
use crate::expression::Expression;
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
    /// The typed failure behind `reason`, for a caller that has to report
    /// Go's own error identity (1267 for a collation mix, say).
    pub error: Option<crate::EvalError>,
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

/// Native cast signatures share Go's single ast.Cast rewrite contract.
pub(crate) fn is_cast_name(name: &str) -> bool {
    name == "cast"
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
    ) -> Result<Expression, FunctionBuildError>;

    /// The live evaluation context Go folds built constants against. Rule-side constant folds (projection
    /// elimination's merged-expression fold) must evaluate with it and
    /// re-home the diagnostics like every construction-time fold -- a dry
    /// `NoColumns` fold silently drops go's 1292 rows (oracle:
    /// `SELECT CAST(j AS SIGNED) FROM (SELECT CAST('{}' AS JSON) AS j) t`
    /// warns `Truncated incorrect INTEGER value: '{}'`).
    fn fold_context(&self) -> &dyn tidb_exprctx::context::Columns;

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

/// The [`FunctionBuilder`]: Go's `NewFunction`, with the evaluation context
/// it needs.
///
/// It routes every rebuild through [`crate::new_function`], so a rewrite gets
/// Go's argument type inference, null-type propagation, arity checking,
/// registry dispatch and post-construction constant folding.
pub struct RealFunctionBuilder<'a, C: tidb_exprctx::context::Columns> {
    ctx: &'a C,
}

impl<'a, C: tidb_exprctx::context::Columns> RealFunctionBuilder<'a, C> {
    /// Binds Go's `BuildContext` for the rebuilds this builder performs.
    pub const fn new(ctx: &'a C) -> Self {
        RealFunctionBuilder { ctx }
    }
}

impl<C: tidb_exprctx::context::Columns> FunctionBuilder for RealFunctionBuilder<'_, C> {
    fn fold_context(&self) -> &dyn tidb_exprctx::context::Columns {
        self.ctx
    }

    fn new_function(
        &self,
        func_name: &str,
        ret_type: Option<FieldType>,
        args: Vec<Expression>,
    ) -> Result<Expression, FunctionBuildError> {
        // Go `NewFunction`'s `case ast.Cast: return BuildCastFunction(ctx,
        // args[0], retType)` (`scalar_function.go:208`).
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
                error: Some(err),
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
        // builder that `NewFunction` explicitly refuses. `builtin_cast` is
        // its port; the generic `cast` name has no executor arm, so a
        // placeholder left under it fails at run time.
        let Some(target) = ret_type else {
            return Err(FunctionBuildError {
                func_name: "cast".to_owned(),
                reason: "a cast is built to a target type".to_owned(),
                error: None,
            });
        };
        crate::builtin_cast::build_cast_function_with_check(
            &crate::builtin::ColumnsBuildContext(self.ctx),
            arg,
            target,
            false,
            is_explicit_charset,
        )
        .map_err(|error| FunctionBuildError {
            func_name: "cast".to_owned(),
            reason: format!("{error:?}"),
            error: Some(error),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::expression::Expression;
    use tidb_datatype::FieldTypeCode;

    /// Go's `BuildCastFunctionWithCheck` names its node `ast.Cast`.
    #[test]
    fn build_cast_names_its_node_cast() {
        let argument =
            Expression::Column(Column::new(3, FieldType::new(FieldTypeCode::NewDecimal)));
        let mut target = FieldType::new(FieldTypeCode::NewDecimal);
        target.set_flen(34);
        target.set_decimal(2);
        let cast = RealFunctionBuilder::new(&crate::NoColumns)
            .build_cast(argument, Some(target), false)
            .expect("a decimal cast builds");
        let Expression::ScalarFunction(function) = cast else {
            panic!("a cast is a scalar function");
        };
        assert_eq!(function.func_name.lowercase(), "cast");
    }

    /// Go `NewFunction`'s `case ast.Cast` (`scalar_function.go:208`): a
    /// `cast` name rebuilds through `BuildCastFunction`. Without the route a
    /// substitution that rebuilds a cast reports `hasFail`.
    #[test]
    fn new_function_routes_a_cast_name_to_the_cast_builder() {
        let argument =
            Expression::Column(Column::new(3, FieldType::new(FieldTypeCode::NewDecimal)));
        let mut target = FieldType::new(FieldTypeCode::NewDecimal);
        target.set_flen(34);
        target.set_decimal(2);
        let builder = RealFunctionBuilder::new(&crate::NoColumns);
        let rebuilt = builder
            .new_function("cast", Some(target), vec![argument])
            .expect("a dedicated cast name rebuilds through the cast builder");
        let Expression::ScalarFunction(function) = rebuilt else {
            panic!("a cast is a scalar function");
        };
        assert_eq!(function.func_name.lowercase(), "cast");
    }
}
