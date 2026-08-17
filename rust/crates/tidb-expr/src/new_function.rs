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
//! - **`fc.getFunction` is not reachable.** Go's `functionClass.getFunction`
//!   does three things: verify arity, infer the return type, and derive
//!   collation. Only the first is a table lookup; the other two live in
//!   hundreds of per-class Go method bodies keyed to `builtinFunc`
//!   signatures, which this crate does not model (see
//!   [`crate::scalar_function`]'s BRIDGE DECISION). Consequently:
//!   - Arity is verified exactly, via
//!     [`crate::builtin_registry::verify_args_by_count`].
//!   - The return type comes from this crate's own name-keyed inference
//!     table, `rewriter::result_type::builtin_return_type`, which is the
//!     nearest thing to `f.getRetTp()`. When that table has no entry, the
//!     CALLER's `ret_type` is kept. Go's rule is
//!     `if builtinRetTp != TypeUnspecified || retType == TypeUnspecified {
//!     retType = builtinRetTp }`; the Rust rule is the same statement with
//!     "the table produced an answer" standing in for "the signature's
//!     `getRetTp` is not `TypeUnspecified`".
//!   - Collation derivation on the built node does NOT happen here. Go
//!     derives it inside `getFunction` via `deriveCollation`. A node built by
//!     this file carries [`crate::expr_collation::CollationInfo`]'s default
//!     until something sets it. This is the single largest gap in this file
//!     and it is why callers that care about collation still go through
//!     [`crate::rewriter`].
//!
//! - **Four special-cased names are refused, not built.** `newFunctionImpl`
//!   opens with a switch that hands four names to dedicated node builders
//!   rather than the `funcs` map: `ast.Cast` -> `BuildCastFunction`,
//!   `ast.GetVar` -> `BuildGetVarFunction`, `InternalFuncFromBinary` ->
//!   `BuildFromBinaryFunction`, and `InternalFuncToBinary` ->
//!   `BuildToBinaryFunction`. Those four builders live in
//!   `pkg/expression/builtin_cast.go` / `builtin_convert_charset.go` and have
//!   no Rust counterpart that produces a NODE (this crate's `cast.rs` and
//!   `convert_charset.rs` are value-level). They are refused with a named
//!   [`EvalError::Unsupported`] rather than built as plain nodes, because a
//!   plain `cast` node would be silently WRONG: this crate's evaluator spells
//!   casts `cast_<target>` (with the target folded into the name) and user
//!   variables `getvar_<kind>`, so a node named `cast` would never evaluate.
//!   None of these four names is in the `funcs` map, so without the refusal
//!   they would surface as a misleading 1305 "function does not exist".
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
//! - **`defaultScalarFunctionCheck`'s `ast.Grouping` branch is dropped.** It
//!   downcasts `function.Function` to `*BuiltinGroupingImplSig` to assert
//!   `isMetaInited`. The Rust node holds no signature object, so there is
//!   nothing to downcast; [`crate::grouping::GroupingFunction`] owns that
//!   metadata separately and validates it at its own construction. Building a
//!   `grouping` node here therefore does NOT reproduce Go's
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
        let mut inferred = ret_field_type.clone();
        inferred.del_flags(FieldTypeFlags::NOT_NULL);
        constant.ret_type = Some(inferred);
    }
}

/// Go `defaultScalarFunctionCheck` (`scalar_function.go:298`).
///
/// Go's only check is the `ast.Grouping` metadata assertion, which this port
/// cannot express -- see the module header's `defaultScalarFunctionCheck`
/// narrowing. The function is kept so the callback plumbing matches Go's and
/// so future checks have the same place to land.
fn default_scalar_function_check(function: ScalarFunction) -> Result<ScalarFunction, EvalError> {
    Ok(function)
}

/// The four names `newFunctionImpl` routes to dedicated node builders
/// (`scalar_function.go:207-215`), with the Go builder each one needs. See the
/// module header for why these are refused rather than built.
fn dedicated_builder_refusal(func_name: &str) -> Option<EvalError> {
    let reason = match func_name {
        "cast" => "NewFunction(cast) needs BuildCastFunction, which builds a cast_<target> node",
        "getvar" => {
            "NewFunction(getvar) needs BuildGetVarFunction, which builds a getvar_<kind> node"
        }
        "from_binary" => "NewFunction(from_binary) needs BuildFromBinaryFunction",
        "to_binary" => "NewFunction(to_binary) needs BuildToBinaryFunction",
        _ => return None,
    };
    Some(EvalError::Unsupported(reason))
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
/// - [`EvalError::Unsupported`] for the four dedicated-builder names.
/// - [`EvalError::NoDatabaseSelected`] (Go `plannererrors.ErrNoDB`, 1046) for
///   an unregistered name with no current database.
/// - [`EvalError::FunctionNotExists`] (1305) for an unregistered name.
/// - [`EvalError::WrongParameterCount`] (1582) for a bad argument count.
/// - whatever `check_or_init` returns.
pub fn new_function_impl(
    ctx: &impl Columns,
    fold: ConstantFoldMode,
    func_name: &str,
    ret_type: FieldType,
    check_or_init: Option<ScalarFunctionCallBack<'_>>,
    args: Vec<Expression>,
) -> Result<Expression, EvalError> {
    if let Some(refusal) = dedicated_builder_refusal(func_name) {
        return Err(refusal);
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

    // Go: `if builtinRetTp.GetType() != TypeUnspecified || retType.GetType()
    // == TypeUnspecified { retType = builtinRetTp }`. See the module header's
    // `getFunction` narrowing for why the inference table stands in for
    // `f.getRetTp()`.
    let ret_type = crate::rewriter::result_type::builtin_return_type(func_name, &func_args)
        .filter(|inferred| {
            inferred.code() != FieldTypeCode::Unspecified
                || ret_type.code() == FieldTypeCode::Unspecified
        })
        .unwrap_or(ret_type);

    let function = ScalarFunction::new(CiString::new(registered_name), ret_type, func_args);
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
    ctx: &impl Columns,
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

/// Go `NewFunctionBase` (`scalar_function.go:319`): builds with NO constant
/// folding.
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
pub fn scalar_funcs_to_exprs(funcs: Vec<ScalarFunction>) -> Vec<Expression> {
    funcs.into_iter().map(Expression::ScalarFunction).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        new_function, new_function_base, new_function_internal, new_function_with_init,
        scalar_funcs_to_exprs, type_infer_for_null,
    };
    use crate::column::Column;
    use crate::constant::Constant;
    use crate::context::NoColumns;
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
            ScalarFunction::new(
                CiString::new("lt"),
                FieldType::new(FieldTypeCode::LongLong),
                vec![not_null_double_column(), int_constant(value)],
            )
        };
        let funcs = vec![make(0), make(1)];
        let expected = funcs.clone();
        let exprs = scalar_funcs_to_exprs(funcs);
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

    /// NEW COVERAGE for the module header's dedicated-builder narrowing: the
    /// four special-cased names must be REFUSED by name, not misreported as
    /// nonexistent functions and not built as plain nodes that could never
    /// evaluate.
    #[test]
    fn dedicated_builder_names_are_refused_rather_than_built() {
        for name in ["cast", "getvar", "from_binary", "to_binary"] {
            let result = new_function_base(
                &WithDatabase,
                name,
                FieldType::new(FieldTypeCode::LongLong),
                vec![int_constant(1)],
            );
            match result {
                Err(EvalError::Unsupported(reason)) => {
                    assert!(reason.contains(name), "{name} must be named: {reason}");
                }
                other => panic!("{name} must be refused, got {other:?}"),
            }
        }
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
        assert_eq!(
            as_func(&control).get_args()[0]
                .static_type()
                .map(FieldType::code),
            Some(FieldTypeCode::Null),
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
            Ok(ScalarFunction::new(
                CiString::new("minus"),
                FieldType::new(FieldTypeCode::LongLong),
                func.args,
            ))
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
