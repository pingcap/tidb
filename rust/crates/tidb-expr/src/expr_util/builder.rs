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
//! Faithful function construction -- argument type inference, collation
//! derivation, constant folding and the `getFunction` dispatch that picks a
//! signature -- is its own unit and is not in this crate yet. Every `util.go`
//! symbol that REBUILDS a function calls one of those four Go entry points, so
//! rather than guess at construction, this module names the dependency as a
//! trait the caller supplies.
//!
//! The split matters for honesty about what is ported: the rewrite RULE (which
//! node is replaced by which shape, and under what condition) is fully
//! transcribed from Go; the resulting node's TYPE DERIVATION is whatever the
//! injected builder does.

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
        self.new_function("cast", ret_type, vec![arg])
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
        // `// narrowing:` Go routes `cast` to `BuildCastFunctionWithCheck`, a
        // DEDICATED builder that `NewFunction` explicitly refuses (it is one of
        // the four refused names). That builder is not in this crate yet, so
        // the cast node is constructed directly, preserving the target type --
        // which is the whole of what the substitution rewrites read off it.
        // Routing it through `new_function` here would turn every cast
        // substitution into an error, which is strictly worse.
        let _ = is_explicit_charset;
        PreservingFunctionBuilder.new_function("cast", ret_type, vec![arg])
    }
}
