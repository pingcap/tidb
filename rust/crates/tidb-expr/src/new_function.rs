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

//! SEED of the scalar-function CONSTRUCTION surface of Go
//! `pkg/expression/scalar_function.go`: the `NewFunction` family that every
//! other TiDB package calls to turn a function name plus arguments into an
//! [`Expression`].
//!
//! `pkg/expression` is one of the largest packages in TiDB. This file is a
//! deliberate SLICE of it -- the construction/dispatch path only -- and makes
//! no claim on the package as a whole.
//!
//! # Ported from `scalar_function.go`
//!
//! | Go symbol | line | Rust |
//! | --- | --- | --- |
//! | `typeInferForNull` | 164 | [`type_infer_for_null`] |
//! | `newFunctionImpl` | 203 | [`new_function_impl`] |
//! | `defaultScalarFunctionCheck` | 298 | [`default_scalar_function_check`] |
//! | `ScalarFunctionCallBack` | 295 | [`ScalarFunctionCallBack`] |
//! | `NewFunctionWithInit` | 309 | [`new_function_with_init`] |
//! | `NewFunction` | 314 | [`new_function`] |
//! | `NewFunctionBase` | 319 | [`new_function_base`] |
//! | `NewFunctionTryFold` | 324 | [`new_function_try_fold`] |
//! | `NewFunctionInternal` | 334 | [`new_function_internal`] |
//! | `ScalarFuncs2Exprs` | 341 | [`scalar_funcs_to_exprs`] |
//!
//! The function-class dispatch `newFunctionImpl` performs -- Go's `funcs` map
//! plus `extensionFuncs` -- lives in [`crate::builtin_registry`], which now
//! carries all 309 entries of the Go map literal with each class's arity.
//!
//! # What this file does NOT cover
//!
//! Everything else in `scalar_function.go` (the `ScalarFunction` node itself,
//! evaluation, hashing, `ResolveIndices`, collation accessors) is either
//! already in [`crate::scalar_function`] or still unported; see that module's
//! header. This file adds only the construction entry points.
//!
//! # Narrowings, each named
//!
//! - **`fc.getFunction` is the class's own.** Every name in Go's `funcs` map
//!   has its function class in [`crate::builtin::funcs`]; arity is verified
//!   by [`crate::builtin_registry::verify_args_by_count`], and the class's
//!   `get_function` wraps the arguments, infers the return type and derives
//!   the collation, as Go's does. Only extension functions, registered by
//!   arity alone, have no class yet.
//!
//! - **`ast.Sysdate` -> `ast.Now` IS honored**, through
//!   [`crate::Columns::sysdate_is_now`].
//!
//! - **The `noopFuncs` branch is a no-op**, faithfully: Go's `noopFuncs`
//!   (`function_traits.go:267`) is `map[string]struct{}{}` -- EMPTY -- so the
//!   `GetNoopFuncsMode` check at `scalar_function.go:236-246` cannot fire
//!   upstream today. It is therefore not modelled, and no `noop_funcs_mode`
//!   accessor is added to [`crate::Columns`] for a branch that has no members.
//!
//! - **`defaultScalarFunctionCheck`'s `ast.Grouping` branch is carried by the
//!   node.** [`ScalarFunction::set_grouping_metadata`] is the Rust spelling of
//!   `BuiltinGroupingImplSig.SetMetadata`; the default callback rejects a
//!   grouping node until that metadata is installed, reproducing Go's
//!   "grouping meta data hasn't been initialized" error.
//!
//! - **`typeInferForNull` drops its `EvalContext`.** Go needs it for
//!   `Expression.GetType(ctx)`; this crate's [`Expression::static_type`]
//!   needs none. Go's `nullif` arm in the skip-switch is preserved even
//!   though it is DEAD upstream: `ast.Nullif` is not a key of the `funcs`
//!   map, so `NewFunction(ctx, ast.Nullif, ...)` fails the lookup before ever
//!   reaching that switch.
//!
//! - **No `slices.Clone(args)`.** Go clones because `typeInferForNull`
//!   mutates the slice and the caller's must not change. Rust takes the
//!   `Vec<Expression>` by value, so ownership already guarantees it.

use crate::builtin_registry::verify_args_by_count;
use crate::constant_fold::{fold_constant_in_mode, ConstantFoldMode};
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use crate::{Columns, EvalError};
use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};

/// Go `ScalarFunctionCallBack` (`scalar_function.go:295`): a hook run on the
/// freshly built node before folding, able to reject or replace it.
pub type ScalarFunctionCallBack<'a> =
    &'a dyn Fn(ScalarFunction) -> Result<ScalarFunction, EvalError>;

/// Whether `expr` is Go's `isNull` closure inside `typeInferForNull`
/// (`scalar_function.go:168`): a `Constant` whose declared type is `TypeNull`
/// AND whose value is NULL. Both halves matter -- a NULL-valued constant of a
/// concrete type is not a type-inference candidate.
fn is_untyped_null(expr: &Expression) -> bool {
    let Expression::Constant(constant) = expr else {
        return false;
    };
    constant.value.is_null()
        && constant
            .ret_type
            .as_ref()
            .is_some_and(|ret_type| ret_type.code() == FieldTypeCode::Null)
}

/// Go `typeInferForNull` (`scalar_function.go:164`).
///
/// Gives every untyped NULL literal in an argument list the field type of the
/// LAST non-NULL argument, so that `a = NULL` compares in `a`'s domain rather
/// than the NULL domain. Does nothing unless the list holds both a NULL and a
/// non-NULL argument.
///
/// Only untyped-NULL `Constant`s are ever rewritten, which is why this takes
/// `&mut [Expression]` and needs no general "set an expression's type" hook:
/// Go's `arg.Clone()` + `*newarg.GetType(ctx) = *retFieldTp.Clone()` can only
/// be reached for the constants [`is_untyped_null`] accepts.
pub fn type_infer_for_null(args: &mut [Expression]) {
    if args.len() < 2 {
        return;
    }
    // Go scans from the END, so the inferred type is the LAST non-NULL
    // argument's, and stops as soon as both a NULL and a type have been seen.
    let mut ret_field_type: Option<FieldType> = None;
    let mut has_null_arg = false;
    for arg in args.iter().rev() {
        let is_null_arg = is_untyped_null(arg);
        if !is_null_arg && ret_field_type.is_none() {
            ret_field_type = arg.static_type().cloned();
        }
        has_null_arg = has_null_arg || is_null_arg;
        if has_null_arg && ret_field_type.is_some() {
            break;
        }
    }
    let (true, Some(ret_field_type)) = (has_null_arg, ret_field_type) else {
        return;
    };
    // Go skips an argument already equal to the target type when that target
    // is NOT NULL, because the clone below would strip the flag and change
    // meaning. Anything else gets the target type minus NotNullFlag.
    let target_is_not_null = ret_field_type.has_flag(FieldTypeFlags::NOT_NULL);
    for arg in args.iter_mut() {
        if !is_untyped_null(arg) {
            continue;
        }
        let already_matches = arg
            .static_type()
            .is_some_and(|arg_type| arg_type.equal(&ret_field_type) && target_is_not_null);
        if already_matches {
            continue;
        }
        let Expression::Constant(constant) = arg else {
            continue;
        };
        // Go writes the type through `newarg.GetType(ctx)`, and a parameter's
        // `Constant.GetType` answers with a type of its own on every call:
        // a NULL parameter keeps `TypeNull`.
        if constant.param_marker.is_some() {
            continue;
        }
        let mut inferred = ret_field_type.clone();
        inferred.del_flag(FieldTypeFlags::NOT_NULL);
        constant.ret_type = Some(inferred);
    }
}

/// Go `defaultScalarFunctionCheck` (`scalar_function.go:298`).
///
/// Grouping is the one built-in whose signature carries planner-installed
/// state. The source rejects a fresh node until `SetMetadata` has run; the
/// Rust node carries the same state directly and exposes the same guard.
fn default_scalar_function_check(function: ScalarFunction) -> Result<ScalarFunction, EvalError> {
    if function.func_name.lowercase() == "grouping" && !function.has_grouping_metadata() {
        return Err(EvalError::Unsupported(
            "grouping meta data hasn't been initialized, try use function clone instead",
        ));
    }
    Ok(function)
}

/// Go `newFunctionImpl` (`scalar_function.go:203`), the shared body of the
/// whole `NewFunction` family.
///
/// `fold` is Go's `int` selector spelled as the existing [`ConstantFoldMode`]:
/// `1` is [`ConstantFoldMode::Normal`], `0` is [`ConstantFoldMode::Disabled`],
/// and `-1` is [`ConstantFoldMode::Try`].
///
/// Go's `retType == nil` guard has no counterpart: `ret_type` is a
/// [`FieldType`] by value here, so the error it raises is unrepresentable.
///
/// # Errors
///
/// - [`EvalError::NoDatabaseSelected`] (Go `plannererrors.ErrNoDB`, 1046) for
///   an unregistered name with no current database.
/// - [`EvalError::FunctionNotExists`] (1305) for an unregistered name.
/// - [`EvalError::WrongParameterCount`] (1582) for a bad argument count.
/// - whatever `check_or_init` returns.
pub fn new_function_impl(
    ctx: &dyn Columns,
    fold: ConstantFoldMode,
    func_name: &str,
    ret_type: FieldType,
    check_or_init: Option<ScalarFunctionCallBack<'_>>,
    args: Vec<Expression>,
) -> Result<Expression, EvalError> {
    // Go: `case InternalFuncFromBinary: return BuildFromBinaryFunction(ctx,
    // args[0], retType, false), nil`.
    if func_name == crate::convert_charset::INTERNAL_FUNC_FROM_BINARY {
        let mut args = args;
        return Ok(crate::convert_charset::build_from_binary_function(
            &crate::builtin::ColumnsBuildContext(ctx),
            args.swap_remove(0),
            ret_type,
            false,
        ));
    }
    // Go: `case InternalFuncToBinary: return BuildToBinaryFunction(ctx, args[0]), nil`.
    if func_name == crate::convert_charset::INTERNAL_FUNC_TO_BINARY {
        let mut args = args;
        return Ok(crate::convert_charset::build_to_binary_function(
            &crate::builtin::ColumnsBuildContext(ctx),
            args.swap_remove(0),
        ));
    }
    // Go: `case ast.GetVar: return BuildGetVarFunction(ctx, args[0], retType)`.
    if func_name == "getvar" {
        let mut args = args;
        return crate::builtin_other::build_get_var_function(
            &crate::builtin::ColumnsBuildContext(ctx),
            args.swap_remove(0),
            ret_type,
        );
    }
    // Go: `case ast.Cast: return BuildCastFunction(ctx, args[0], retType), nil`.
    if func_name == "cast" {
        let mut args = args;
        return crate::builtin_cast::build_cast_function(
            &crate::builtin::ColumnsBuildContext(ctx),
            args.swap_remove(0),
            ret_type,
        );
    }
    // Go: `case ast.Sysdate: if ctx.GetSysdateIsNow() { funcName = ast.Now }`.
    let func_name = if func_name == "sysdate" && ctx.sysdate_is_now() {
        "now"
    } else {
        func_name
    };

    // Go's `funcs[funcName]` lookup, falling back to `extensionFuncs`.
    let Some((registered_name, _arity)) = crate::builtin_registry::function_class(func_name) else {
        return Err(match ctx.current_database() {
            Some(database) if !database.is_empty() => {
                EvalError::FunctionNotExists(format!("{database}.{func_name}"))
            }
            _ => EvalError::NoDatabaseSelected,
        });
    };
    // The `noopFuncs` check would sit here; see the module header for why an
    // empty upstream map makes it a no-op.

    let mut func_args = args;
    // Go skips null-type inference for the control functions (they run
    // `InferType4ControlFuncs` instead) and for ROW (whose element types must
    // stay independent until the row comparison is expanded).
    if !matches!(func_name, "if" | "ifnull" | "nullif" | "row") {
        type_infer_for_null(&mut func_args);
    }

    verify_args_by_count(func_name, func_args.len())?;
    // Go: `f, err := fc.getFunction(ctx, funcArgs)`.
    let Some(class) = crate::builtin::funcs_or_extension(func_name) else {
        return Err(crate::builtin_registry::unresolved_error(
            func_name,
            ctx.current_database(),
        ));
    };
    let f = class.get_function(&crate::builtin::ColumnsBuildContext(ctx), func_args)?;
    let function = scalar_function_of(registered_name, ret_type, f);
    check_or_init_and_fold(ctx, fold, check_or_init, function)
}

/// Go `newFunctionImpl` between `getFunction` and `checkOrInit`: the node
/// over the builtin function `f`, typed by `f.getRetTp()` unless that is
/// unspecified and the caller's `ret_type` is not.
pub(crate) fn scalar_function_of(
    registered_name: &str,
    ret_type: FieldType,
    f: crate::builtin::BaseBuiltinFunc,
) -> ScalarFunction {
    let builtin_ret_tp = f.get_ret_tp();
    let ret_type = if builtin_ret_tp.code() != FieldTypeCode::Unspecified
        || ret_type.code() == FieldTypeCode::Unspecified
    {
        builtin_ret_tp
    } else {
        ret_type
    };
    ScalarFunction::from_builtin_func(CiString::new(registered_name), ret_type, f)
}

/// Go `newFunctionImpl`'s tail: `checkOrInit` over the built node, then the
/// fold `fold` selects.
fn check_or_init_and_fold(
    ctx: &dyn Columns,
    fold: ConstantFoldMode,
    check_or_init: Option<ScalarFunctionCallBack<'_>>,
    function: ScalarFunction,
) -> Result<Expression, EvalError> {
    let function = match check_or_init {
        Some(callback) => callback(function)?,
        None => function,
    };
    let mut expr = Expression::ScalarFunction(function);
    fold_constant_in_mode(&mut expr, ctx, fold);
    Ok(expr)
}

/// Go `NewFunction` (`scalar_function.go:314`): builds a scalar function and
/// folds it if it is constant.
///
/// # Errors
///
/// See [`new_function_impl`].
pub fn new_function(
    ctx: &dyn Columns,
    func_name: &str,
    ret_type: FieldType,
    args: Vec<Expression>,
) -> Result<Expression, EvalError> {
    new_function_impl(
        ctx,
        ConstantFoldMode::Normal,
        func_name,
        ret_type,
        Some(&default_scalar_function_check),
        args,
    )
}

/// Go `NewFunctionBase` (`scalar_function.go:319`): disables folding of the
/// outer function; implicit argument casts still fold during construction.
///
/// # Errors
///
/// See [`new_function_impl`].
pub fn new_function_base(
    ctx: &impl Columns,
    func_name: &str,
    ret_type: FieldType,
    args: Vec<Expression>,
) -> Result<Expression, EvalError> {
    new_function_impl(
        ctx,
        ConstantFoldMode::Disabled,
        func_name,
        ret_type,
        Some(&default_scalar_function_check),
        args,
    )
}

/// Go `NewFunctionTryFold` (`scalar_function.go:324`): folds only when doing
/// so raises no warning, otherwise keeps the unfolded function.
///
/// # Errors
///
/// See [`new_function_impl`].
pub fn new_function_try_fold(
    ctx: &impl Columns,
    func_name: &str,
    ret_type: FieldType,
    args: Vec<Expression>,
) -> Result<Expression, EvalError> {
    new_function_impl(
        ctx,
        ConstantFoldMode::Try,
        func_name,
        ret_type,
        Some(&default_scalar_function_check),
        args,
    )
}

/// Go `NewFunctionWithInit` (`scalar_function.go:309`): folds like
/// [`new_function`] but runs a caller-supplied check/init callback on the node
/// before folding.
///
/// # Errors
///
/// See [`new_function_impl`].
pub fn new_function_with_init(
    ctx: &impl Columns,
    func_name: &str,
    ret_type: FieldType,
    init: ScalarFunctionCallBack<'_>,
    args: Vec<Expression>,
) -> Result<Expression, EvalError> {
    new_function_impl(
        ctx,
        ConstantFoldMode::Normal,
        func_name,
        ret_type,
        Some(init),
        args,
    )
}

/// Go `NewFunctionInternal` (`scalar_function.go:334`), which logs the error
/// and returns the (possibly nil) expression.
///
/// Go's own doc comment marks this DEPRECATED because swallowing the error
/// turns an argument-verification failure into a nil dereference far away.
/// The `Option` return keeps that hazard visible in the type instead: Go's
/// implicit nil is this `None`.
#[must_use]
pub fn new_function_internal(
    ctx: &impl Columns,
    func_name: &str,
    ret_type: FieldType,
    args: Vec<Expression>,
) -> Option<Expression> {
    new_function(ctx, func_name, ret_type, args).ok()
}

/// Go `ScalarFuncs2Exprs` (`scalar_function.go:341`).
#[must_use]
pub fn scalar_funcs_2_exprs(funcs: Vec<ScalarFunction>) -> Vec<Expression> {
    funcs.into_iter().map(Expression::ScalarFunction).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        new_function, new_function_base, new_function_internal, new_function_with_init,
        scalar_funcs_2_exprs, type_infer_for_null,
    };
    use crate::column::Column;
    use crate::constant::{Constant, ParamMarker};
    use tidb_exprctx::context::NoColumns;
    use crate::expr_util::{FunctionBuilder, RealFunctionBuilder};
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use crate::{Columns, EvalError};
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};

    /// Go's `types.NewFieldType(mysql.TypeNull)` NULL literal: the untyped
    /// NULL that `typeInferForNull` rewrites.
    fn untyped_null() -> Expression {
        Expression::Constant(Constant::new(
            Datum::Null,
            FieldType::new(FieldTypeCode::Null),
        ))
    }

    fn int_constant(value: i64) -> Expression {
        Expression::Constant(Constant::new(
            Datum::Int(value),
            FieldType::new(FieldTypeCode::LongLong),
        ))
    }

    /// A NOT NULL `DOUBLE` column, Go's `a` in `TestIssue23309`.
    fn not_null_double_column() -> Expression {
        let mut ret_type = FieldType::new(FieldTypeCode::Double);
        ret_type.add_flags(FieldTypeFlags::NOT_NULL);
        Expression::Column(Column::new(1, ret_type))
    }

    /// A context with a current database, so unknown-function lookups take
    /// Go's 1305 arm rather than the `ErrNoDB` one.
    struct WithDatabase;
    impl Columns for WithDatabase {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn current_database(&self) -> Option<String> {
            Some("test".to_owned())
        }
    }

    /// A context whose session has `tidb_sysdate_is_now` on.
    struct SysdateIsNow;
    impl Columns for SysdateIsNow {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn sysdate_is_now(&self) -> bool {
            true
        }
    }

    fn as_func(expr: &Expression) -> &ScalarFunction {
        match expr {
            Expression::ScalarFunction(func) => func,
            other => panic!("expected a scalar function, got {other:?}"),
        }
    }

    /// PORT of Go `TestIssue23309` (`scalar_function_test.go:179`).
    ///
    /// `NE(a NOT NULL DOUBLE column, untyped NULL)` must give the NULL
    /// argument the column's DOUBLE type while STRIPPING the NotNullFlag --
    /// carrying the flag over would claim a NULL constant is not nullable.
    #[test]
    fn issue23309_null_arg_takes_the_column_type_without_the_not_null_flag() {
        let built = new_function_base(
            &NoColumns,
            "ne",
            FieldType::new(FieldTypeCode::LongLong),
            vec![not_null_double_column(), untyped_null()],
        )
        .expect("ne over a column and a null builds");
        let null_arg_type = as_func(&built).get_args()[1]
            .static_type()
            .expect("the inferred null argument has a type");
        assert_eq!(null_arg_type.code(), FieldTypeCode::Double);
        assert!(
            !null_arg_type.has_flag(FieldTypeFlags::NOT_NULL),
            "the NotNullFlag must not survive onto a NULL constant"
        );
        // Go also asserts the argument still evaluates to NULL.
        match &as_func(&built).get_args()[1] {
            Expression::Constant(constant) => assert!(constant.value.is_null()),
            other => panic!("expected a constant, got {other:?}"),
        }
    }

    /// PORT of Go `TestScalarFuncs2Exprs` (`scalar_function_test.go:197`).
    #[test]
    fn scalar_funcs_to_exprs_preserves_each_function() {
        let make = |value: i64| {
            crate::new_function::new_function_base(
                &crate::NoColumns,
                "lt",
                FieldType::new(FieldTypeCode::LongLong),
                vec![not_null_double_column(), int_constant(value)],
            ).unwrap().into_scalar_function().unwrap()
        };
        let funcs = vec![make(0), make(1)];
        let expected = funcs.clone();
        let exprs = scalar_funcs_2_exprs(funcs);
        assert_eq!(exprs.len(), expected.len());
        for (expr, func) in exprs.iter().zip(expected.iter()) {
            assert_eq!(as_func(expr).func_name, func.func_name);
            assert_eq!(as_func(expr).get_args().len(), func.get_args().len());
        }
    }

    /// NEW COVERAGE (Go exercises the fold=1 vs fold=0 split only through
    /// planner testkit output): `NewFunction` folds a wholly-constant call on
    /// construction, `NewFunctionBase` leaves the node standing.
    #[test]
    fn construction_folds_constants_only_in_the_folding_modes() {
        let folded = new_function(
            &NoColumns,
            "plus",
            FieldType::new(FieldTypeCode::LongLong),
            vec![int_constant(1), int_constant(2)],
        )
        .expect("plus over two constants builds");
        match &folded {
            Expression::Constant(constant) => assert_eq!(constant.value, Datum::Int(3)),
            other => panic!("NewFunction must fold a constant call, got {other:?}"),
        }

        let unfolded = new_function_base(
            &NoColumns,
            "plus",
            FieldType::new(FieldTypeCode::LongLong),
            vec![int_constant(1), int_constant(2)],
        )
        .expect("plus over two constants builds");
        assert_eq!(as_func(&unfolded).func_name.lowercase(), "plus");
    }

    #[test]
    fn comparison_refinement_precedes_callback_and_all_folding_modes() {
        use crate::constant_fold::ConstantFoldMode;
        #[derive(Default)]
        struct Warnings(std::cell::RefCell<Vec<String>>);
        impl Columns for Warnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.0.borrow_mut().push(format!("{code} {message}"));
            }
            fn warning_count(&self) -> usize {
                self.0.borrow().len()
            }
            fn truncate_warnings(&self, count: usize) {
                self.0.borrow_mut().truncate(count);
            }
        }
        for mode in [
            ConstantFoldMode::Normal,
            ConstantFoldMode::Disabled,
            ConstantFoldMode::Try,
        ] {
            for kind in ["unsigned", "truncated", "decimal"] {
                let ctx = Warnings::default();
                let mut field = FieldType::new(FieldTypeCode::LongLong);
                let constant = match kind {
                    "unsigned" => {
                        field.add_flags(FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL);
                        int_constant(-1)
                    }
                    _ => Expression::Constant(Constant::new(
                        Datum::new_string_datum(if kind == "truncated" { "10ab" } else { "10.5" }),
                        FieldType::new(FieldTypeCode::VarString),
                    )),
                };
                let callback_called = std::cell::Cell::new(false);
                let callback = |function: ScalarFunction| {
                    callback_called.set(true);
                    let Expression::Constant(right) = &function.args[1] else {
                        panic!("refined constant")
                    };
                    assert_eq!(
                        right.value,
                        Datum::Int(if kind == "unsigned" { 0 } else { 10 }),
                        "{kind} {mode:?}"
                    );
                    assert_eq!(
                        right.ret_type.as_ref().unwrap().eval_type(),
                        tidb_datatype::EvalType::Int
                    );
                    assert_eq!(ctx.warning_count(), if kind == "truncated" { 2 } else { 0 });
                    if kind == "unsigned" {
                        let Expression::Constant(left) = &function.args[0] else {
                            panic!("refined unsigned column")
                        };
                        assert_eq!(left.value, Datum::Int(1));
                    }
                    Ok(function)
                };
                let result = super::new_function_impl(
                    &ctx,
                    mode,
                    "gt",
                    FieldType::new(FieldTypeCode::Tiny),
                    Some(&callback),
                    vec![Expression::Column(Column::new(1, field)), constant],
                )
                .unwrap();
                assert!(callback_called.get());
                assert_eq!(ctx.warning_count(), if kind == "truncated" { 2 } else { 0 });
                if kind == "unsigned" && mode != ConstantFoldMode::Disabled {
                    let Expression::Constant(value) = result else {
                        panic!("comparison must fold")
                    };
                    assert_eq!(value.value, Datum::Int(1));
                } else {
                    assert!(matches!(result, Expression::ScalarFunction(_)));
                }
            }
        }
    }

    #[test]
    fn construction_callback_preserves_replacement_metadata() {
        let mut text = FieldType::new(FieldTypeCode::VarString);
        text.set_charset_name("utf8mb4");
        text.set_collation_name("utf8mb4_bin");
        let replacement = new_function_base(
            &NoColumns,
            "plus",
            FieldType::new(FieldTypeCode::LongLong),
            vec![
                Expression::Column(Column::new(2, FieldType::new(FieldTypeCode::LongLong))),
                int_constant(1),
            ],
        )
        .unwrap();
        let expected = replacement.static_type().unwrap().clone();
        let callback = |function: ScalarFunction| {
            let field = function.ret_type.as_ref().unwrap();
            assert_eq!(field.charset_name(), "utf8mb4");
            assert_eq!(field.collation_name(), "utf8mb4_bin");
            let Expression::ScalarFunction(function) = replacement.clone() else {
                unreachable!()
            };
            Ok(function)
        };
        let actual = new_function_with_init(
            &NoColumns,
            "concat",
            text.clone(),
            &callback,
            vec![Expression::Column(Column::new(1, text))],
        )
        .unwrap();
        assert_eq!(actual.static_type().unwrap(), &expected);
        assert_eq!(as_func(&actual).func_name.lowercase(), "plus");
    }

    /// NEW COVERAGE: a non-constant argument keeps the node unfolded even in
    /// the folding mode, which is what makes folding safe to run on every
    /// construction.
    #[test]
    fn a_non_constant_argument_blocks_folding() {
        let built = new_function(
            &NoColumns,
            "plus",
            FieldType::new(FieldTypeCode::LongLong),
            vec![not_null_double_column(), int_constant(2)],
        )
        .expect("plus over a column builds");
        assert_eq!(as_func(&built).func_name.lowercase(), "plus");
    }

    /// Go's arithmetic function classes infer the result type even when the
    /// caller supplies an unspecified placeholder.  A correctly typed
    /// `plus(Column, constant)` must stay in the integer comparison domain;
    /// leaving it unspecified makes `lt(Column, plus(...))` acquire a
    /// cast to SIGNED Go never builds during comparison refinement.
    #[test]
    fn arithmetic_builder_infers_result_type_before_comparison_refinement() {
        let builder = RealFunctionBuilder::new(&NoColumns);
        let column =
            |id| Expression::Column(Column::new(id, FieldType::new(FieldTypeCode::LongLong)));
        let sum = builder
            .new_function("plus", None, vec![column(1), int_constant(1)])
            .expect("plus over a column builds");
        assert_eq!(
            as_func(&sum)
                .ret_type
                .as_ref()
                .expect("plus has a result type")
                .code(),
            FieldTypeCode::LongLong
        );

        let comparison = builder
            .new_function("lt", None, vec![column(0), sum])
            .expect("lt over the arithmetic expression builds");
        let right = &as_func(&comparison).get_args()[1];
        assert_eq!(
            as_func(right).func_name.lowercase(),
            "plus",
            "comparison refinement must not wrap a typed integer expression"
        );
    }

    /// Go's `FoldConstant` preserves parameter provenance on a folded result.
    /// The value is available for this construction context, but the
    /// expression must remain `ConstOnlyInContext` so a plan cache cannot
    /// treat it as a strict literal.
    #[test]
    fn folding_a_parameter_keeps_context_only_provenance() {
        struct Parameters(Vec<Datum>);
        impl Columns for Parameters {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
                self.0
                    .get(order)
                    .cloned()
                    .ok_or(EvalError::Unsupported("unbound prepared parameter"))
            }
        }
        let mut parameter = Constant::new(Datum::Int(7), FieldType::new(FieldTypeCode::LongLong));
        parameter.param_marker = Some(ParamMarker { order: 0 });
        let folded = new_function(
            &Parameters(vec![Datum::Int(7)]),
            "plus",
            FieldType::new(FieldTypeCode::LongLong),
            vec![int_constant(1), Expression::Constant(parameter)],
        )
        .expect("plus over a parameter builds");
        let Expression::Constant(constant) = &folded else {
            panic!("the constant expression should fold during construction");
        };
        assert!(
            constant.deferred_expr.is_some(),
            "folded parameter must retain deferred provenance"
        );
        assert_eq!(
            folded.const_level(),
            crate::expression::ConstLevel::ONLY_IN_CONTEXT
        );
        assert_eq!(
            crate::eval_expression_once(&folded, &Parameters(vec![Datum::Int(11)])).unwrap(),
            Datum::Int(12)
        );
    }

    /// Go `typeInferForNull` writes the inferred type through
    /// `newarg.GetType(ctx)`, and `Constant.GetType` answers for a parameter
    /// with a type of its own on every call, so a NULL parameter stays
    /// `TypeNull` and `? + 11` is real arithmetic. Live Go PREPARE/EXECUTE
    /// oracle, 2026-11-10: `? + 11` with a NULL parameter is `double`, where
    /// the literal `NULL + 11` is `bigint`.
    #[test]
    fn a_null_parameter_keeps_its_own_type_through_null_inference() {
        let mut parameter = Constant::new(Datum::Null, FieldType::new(FieldTypeCode::Null));
        parameter.param_marker = Some(ParamMarker { order: 0 });
        let literal = Constant::new(Datum::Null, FieldType::new(FieldTypeCode::Null));
        for (null, expected) in [
            (parameter, FieldTypeCode::Double),
            (literal, FieldTypeCode::LongLong),
        ] {
            let sum = new_function_base(
                &NoColumns,
                "plus",
                FieldType::new(FieldTypeCode::Unspecified),
                vec![Expression::Constant(null), int_constant(11)],
            )
            .expect("plus over a NULL builds");
            assert_eq!(as_func(&sum).ret_type.as_ref().unwrap().code(), expected);
        }
    }

    /// NEW COVERAGE of the arity gate, Go `ErrIncorrectParameterCount` (1582).
    /// `ILIKE` is fixed at 3 arguments and `CONCAT` is variadic from 1, so the
    /// pair covers both shapes of the `[minArgs, maxArgs]` check.
    #[test]
    fn wrong_argument_counts_are_rejected_by_name() {
        let too_few = new_function_base(
            &NoColumns,
            "ilike",
            FieldType::new(FieldTypeCode::LongLong),
            vec![int_constant(1), int_constant(2)],
        );
        assert_eq!(
            too_few.unwrap_err(),
            EvalError::WrongParameterCount("ilike")
        );

        let too_many = new_function_base(
            &NoColumns,
            "ifnull",
            FieldType::new(FieldTypeCode::LongLong),
            vec![int_constant(1), int_constant(2), int_constant(3)],
        );
        assert_eq!(
            too_many.unwrap_err(),
            EvalError::WrongParameterCount("ifnull")
        );

        // A variadic class accepts any count at or above its minimum.
        assert!(new_function_base(
            &NoColumns,
            "concat",
            FieldType::new(FieldTypeCode::Varchar),
            vec![int_constant(1)],
        )
        .is_ok());
        assert!(new_function_base(
            &NoColumns,
            "concat",
            FieldType::new(FieldTypeCode::Varchar),
            vec![int_constant(1), int_constant(2), int_constant(3)],
        )
        .is_ok());
    }

    /// NEW COVERAGE: Go raises `plannererrors.ErrNoDB` (1046) BEFORE 1305 when
    /// no database is selected, because the 1305 message embeds the database
    /// name. Both arms are checked.
    #[test]
    fn an_unknown_function_reports_1305_or_no_database() {
        assert_eq!(
            new_function_base(
                &WithDatabase,
                "no_such_fn",
                FieldType::new(FieldTypeCode::LongLong),
                vec![],
            )
            .unwrap_err(),
            EvalError::FunctionNotExists("test.no_such_fn".to_owned())
        );
        assert_eq!(
            new_function_base(
                &NoColumns,
                "no_such_fn",
                FieldType::new(FieldTypeCode::LongLong),
                vec![],
            )
            .unwrap_err(),
            EvalError::NoDatabaseSelected
        );
    }

    /// Go `newFunctionImpl`: `case ast.Cast: return BuildCastFunction(ctx,
    /// args[0], retType), nil`.
    #[test]
    fn cast_is_built_by_build_cast_function() {
        let target = FieldType::new(FieldTypeCode::VarString);
        let built = new_function_base(
            &WithDatabase,
            "cast",
            target.clone(),
            vec![not_null_double_column()],
        )
        .unwrap();
        let Expression::ScalarFunction(function) = &built else {
            panic!("a cast node, got {built:?}");
        };
        assert_eq!(function.func_name.lowercase(), "cast");
        let direct =
            crate::builtin_cast::build_cast_function(
                &crate::builtin::UnfoldedBuildContext,
                not_null_double_column(),
                target,
            )
            .unwrap();
        assert_eq!(format!("{built:?}"), format!("{direct:?}"));
    }

    /// NEW COVERAGE: Go rewrites `SYSDATE` to `NOW` at construction when the
    /// session says so, so the built node's NAME changes.
    #[test]
    fn sysdate_becomes_now_when_the_session_says_so() {
        let rewritten = new_function_base(
            &SysdateIsNow,
            "sysdate",
            FieldType::new(FieldTypeCode::Datetime),
            vec![],
        )
        .expect("sysdate builds");
        assert_eq!(as_func(&rewritten).func_name.lowercase(), "now");

        let kept = new_function_base(
            &NoColumns,
            "sysdate",
            FieldType::new(FieldTypeCode::Datetime),
            vec![],
        )
        .expect("sysdate builds");
        assert_eq!(as_func(&kept).func_name.lowercase(), "sysdate");
    }

    /// NEW COVERAGE: the control functions opt OUT of null type inference
    /// (Go runs `InferType4ControlFuncs` for them instead), so an untyped
    /// NULL argument to IFNULL keeps its NULL type while the same argument to
    /// a non-control function does not.
    #[test]
    fn control_functions_skip_null_type_inference() {
        let control = new_function_base(
            &NoColumns,
            "ifnull",
            FieldType::new(FieldTypeCode::LongLong),
            vec![untyped_null(), not_null_double_column()],
        )
        .expect("ifnull builds");
        // `ifNullFunctionClass` casts the NULL to the DOUBLE it returns; a
        // retyped NULL would carry the column's NOT NULL flag instead.
        let null_type = as_func(&control).get_args()[0]
            .static_type()
            .expect("the cast NULL is typed");
        assert_eq!(null_type.code(), FieldTypeCode::Double);
        assert!(
            null_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL == 0,
            "IFNULL must not have its NULL argument retyped"
        );

        let ordinary = new_function_base(
            &NoColumns,
            "ne",
            FieldType::new(FieldTypeCode::LongLong),
            vec![untyped_null(), not_null_double_column()],
        )
        .expect("ne builds");
        assert_eq!(
            as_func(&ordinary).get_args()[0]
                .static_type()
                .map(FieldType::code),
            Some(FieldTypeCode::Double),
        );
    }

    /// NEW COVERAGE of `typeInferForNull` in isolation, for the two shapes
    /// that must do NOTHING: fewer than two arguments, and no NULL at all.
    #[test]
    fn type_infer_for_null_is_inert_without_both_a_null_and_a_type() {
        let mut single = vec![untyped_null()];
        type_infer_for_null(&mut single);
        assert_eq!(
            single[0].static_type().map(FieldType::code),
            Some(FieldTypeCode::Null)
        );

        // All-NULL: there is no type to infer FROM, so nothing changes.
        let mut all_null = vec![untyped_null(), untyped_null()];
        type_infer_for_null(&mut all_null);
        assert!(all_null
            .iter()
            .all(|arg| arg.static_type().map(FieldType::code) == Some(FieldTypeCode::Null)));

        // No NULL at all: non-NULL arguments are never retyped.
        let mut no_null = vec![int_constant(1), not_null_double_column()];
        type_infer_for_null(&mut no_null);
        assert_eq!(
            no_null[0].static_type().map(FieldType::code),
            Some(FieldTypeCode::LongLong)
        );
    }

    /// NEW COVERAGE: Go scans from the END, so the LAST non-NULL argument
    /// supplies the inferred type.
    #[test]
    fn null_inference_takes_the_last_non_null_argument_type() {
        let mut args = vec![
            int_constant(1),
            untyped_null(),
            Expression::Constant(Constant::new(
                Datum::Bytes(b"x".to_vec()),
                FieldType::new(FieldTypeCode::Varchar),
            )),
        ];
        type_infer_for_null(&mut args);
        assert_eq!(
            args[1].static_type().map(FieldType::code),
            Some(FieldTypeCode::Varchar),
            "the trailing VARCHAR, not the leading BIGINT, is the source"
        );
    }

    /// NEW COVERAGE: `NewFunctionWithInit`'s callback can reject the node, and
    /// the error reaches the caller instead of a half-built expression.
    #[test]
    fn the_init_callback_can_reject_the_built_function() {
        let reject = |_: ScalarFunction| Err(EvalError::Unsupported("rejected by the init hook"));
        let result = new_function_with_init(
            &NoColumns,
            "plus",
            FieldType::new(FieldTypeCode::LongLong),
            &reject,
            vec![int_constant(1), int_constant(2)],
        );
        assert_eq!(
            result.unwrap_err(),
            EvalError::Unsupported("rejected by the init hook")
        );

        // The callback may also replace the node; the replacement is what
        // gets folded and returned.
        let rename = |func: ScalarFunction| {
            Ok(crate::new_function::new_function_base(
                &crate::NoColumns,
                "minus",
                FieldType::new(FieldTypeCode::LongLong),
                func.args,
            ).unwrap().into_scalar_function().unwrap())
        };
        let replaced = new_function_with_init(
            &NoColumns,
            "plus",
            FieldType::new(FieldTypeCode::LongLong),
            &rename,
            vec![int_constant(1), int_constant(2)],
        )
        .expect("the replacement builds");
        match replaced {
            Expression::Constant(constant) => assert_eq!(constant.value, Datum::Int(-1)),
            other => panic!("expected the replaced MINUS to fold, got {other:?}"),
        }
    }

    /// NEW COVERAGE: `NewFunctionInternal` swallows the error. Go returns a
    /// nil `Expression`; this port returns `None`, which is the same hazard
    /// made visible in the type.
    #[test]
    fn new_function_internal_swallows_the_error_as_none() {
        assert!(new_function_internal(
            &NoColumns,
            "no_such_fn",
            FieldType::new(FieldTypeCode::LongLong),
            vec![],
        )
        .is_none());
        assert!(new_function_internal(
            &NoColumns,
            "plus",
            FieldType::new(FieldTypeCode::LongLong),
            vec![int_constant(1), int_constant(2)],
        )
        .is_some());
    }
}
