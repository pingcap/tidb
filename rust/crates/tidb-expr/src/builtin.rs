//! `pkg/expression/builtin.go`: the base builtin function every function
//! class's `getFunction` returns.
//!
//! Go's `functionClass.getFunction(ctx, args)` answers one object, a
//! `builtinFunc`: the arguments after their implicit casts, the return type,
//! the `tipb.ScalarFuncSig` (`PbCode`) and the evaluation. [`FunctionClass`]
//! is that interface and [`BaseBuiltinFunc`] the object; the evaluation is
//! the kernel the signature names
//! (`crate::scalar_function::PbBuiltin`).
//!
//! A class is ported one Go file at a time; [`funcs`] lists the names whose
//! class is ported. Every other name is still built by the name tables in
//! `new_function.rs` and the rewriter.

use crate::collation_derive::derive_collation_with_connection;
use crate::constant::Constant;
use crate::constant_fold::{fold_constant_in_mode, ConstantFoldMode};
use crate::expr_collation::{CollationInfo, ExprCollation};
use crate::expression::{ConstLevel, Expression};
use crate::rewriter::ColumnResolver;
use crate::wrap_cast::{
    build_cast_as_decimal, set_const_decimal_cast_precision, type_of,
    wrap_with_cast_as_duration, wrap_with_cast_as_int,
    wrap_with_cast_as_json, wrap_with_cast_as_real, wrap_with_cast_as_string,
    wrap_with_cast_as_time, wrap_with_cast_as_vector_float32, MAX_BLOB_WIDTH,
};
use crate::{Columns, EvalError};
use tidb_chunk::row::Row;
use tidb_datatype::{
    Datum, EvalType, FieldType, FieldTypeBuilder, FieldTypeCode, FieldTypeFlags, UNSPECIFIED_LENGTH,
};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `mysql.MaxIntWidth`.
const MAX_INT_WIDTH: i64 = 20;
/// Go `mysql.MaxRealWidth`.
const MAX_REAL_WIDTH: i64 = 23;
/// Go `mysql.MaxDatetimeWidthWithFsp`: `YYYY-MM-DD HH:MM:SS.ffffff`.
const MAX_DATETIME_WIDTH_WITH_FSP: i64 = 26;
/// Go `mysql.MaxDurationWidthWithFsp`: `-838:59:59.000000`.
const MAX_DURATION_WIDTH_WITH_FSP: i64 = 17;
/// Go `types.MaxFsp`.
const MAX_FSP: i64 = 6;

/// What Go's `getFunction` reads from its `BuildContext`.
pub(crate) trait BuildContext {
    /// Go `ctx.GetCharsetInfo()`.
    fn connection_charset_info(&self) -> (&str, &str);
    /// Go `ctx.GetEvalCtx().SQLMode().HasNoUnsignedSubtractionMode()`.
    fn no_unsigned_subtraction(&self) -> bool;
    /// Go `ctx.GetEvalCtx().GetDivPrecisionIncrement()`.
    fn div_precision_increment(&self) -> i64;
    /// Go `FoldConstant(ctx, expression)`.
    fn fold_constant(&self, expression: &mut Expression);
    /// Go `expression.Eval(ctx.GetEvalCtx(), chunk.Row{})`.
    fn eval_constant(&self, expression: &Expression) -> Result<Datum, EvalError>;
}

/// The [`BuildContext`] of a function built through `NewFunction`.
pub(crate) struct ColumnsBuildContext<'a>(pub(crate) &'a dyn Columns);

impl BuildContext for ColumnsBuildContext<'_> {
    fn connection_charset_info(&self) -> (&str, &str) {
        self.0.connection_charset_info()
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.0.no_unsigned_subtraction()
    }

    fn div_precision_increment(&self) -> i64 {
        i64::from(self.0.div_precision_increment())
    }

    fn fold_constant(&self, expression: &mut Expression) {
        fold_constant_in_mode(expression, self.0, ConstantFoldMode::Normal);
    }

    fn eval_constant(&self, expression: &Expression) -> Result<Datum, EvalError> {
        expression.eval(self.0, Row::empty())
    }
}

/// The [`BuildContext`] of a function the AST rewriter builds.
pub(crate) struct ResolverBuildContext<'a, R: ColumnResolver>(pub(crate) &'a R);

impl<R: ColumnResolver> BuildContext for ResolverBuildContext<'_, R> {
    fn connection_charset_info(&self) -> (&str, &str) {
        self.0.connection_charset_info()
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.0.no_unsigned_subtraction()
    }

    fn div_precision_increment(&self) -> i64 {
        i64::from(self.0.div_precision_increment())
    }

    fn fold_constant(&self, expression: &mut Expression) {
        self.0.fold_constant(expression, ConstantFoldMode::Normal);
    }

    fn eval_constant(&self, expression: &Expression) -> Result<Datum, EvalError> {
        self.0.eval_constant(expression)
    }
}

/// Go `functionClass`.
pub(crate) trait FunctionClass: Sync {
    /// Go `getFunction`: the builtin function over `args`. The argument
    /// count is verified by the caller (`verify_args_by_count`, Go
    /// `verifyArgs`).
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError>;
}

/// Go `funcs`, for the names whose class is ported.
pub(crate) fn funcs(name: &str) -> Option<&'static dyn FunctionClass> {
    use crate::builtin_arithmetic::{
        ArithmeticDivideFunctionClass, ArithmeticIntDivideFunctionClass,
        ArithmeticMinusFunctionClass, ArithmeticModFunctionClass,
        ArithmeticMultiplyFunctionClass, ArithmeticPlusFunctionClass,
    };
    Some(match name {
        "plus" => &ArithmeticPlusFunctionClass,
        "minus" => &ArithmeticMinusFunctionClass,
        "mul" => &ArithmeticMultiplyFunctionClass,
        "div" => &ArithmeticDivideFunctionClass,
        "intdiv" => &ArithmeticIntDivideFunctionClass,
        "mod" => &ArithmeticModFunctionClass,
        _ => return None,
    })
}

/// Go `baseBuiltinFunc` with the signature's `pbCode`.
#[derive(Debug)]
pub(crate) struct BaseBuiltinFunc {
    /// Go `args`: the arguments after their implicit casts.
    pub(crate) args: Vec<Expression>,
    /// Go `tp`.
    pub(crate) tp: FieldType,
    /// Go `pbCode`; `Unspecified` for a signature that sets none.
    pub(crate) pb_code: ScalarFuncSig,
    /// Go's embedded `collationInfo`.
    pub(crate) collation: CollationInfo,
}

impl BaseBuiltinFunc {
    /// Go `getRetTp`.
    pub(crate) fn get_ret_tp(&self) -> FieldType {
        /// Go's literal `65536`, one past `mysql.MaxBlobSize`.
        const MEDIUM_BLOB_BOUNDARY: i64 = 65536;
        let mut tp = self.tp.clone();
        if tp.eval_type() == EvalType::String {
            if tp.flen() >= MAX_BLOB_WIDTH {
                tp.set_code(FieldTypeCode::LongBlob);
            } else if tp.flen() >= MEDIUM_BLOB_BOUNDARY {
                tp.set_code(FieldTypeCode::MediumBlob);
            }
            if tp.charset_name().is_empty() {
                // Go `charset.GetDefaultCharsetAndCollate`.
                tp.set_charset_name("utf8mb4");
                tp.set_collation_name("utf8mb4_bin");
            }
        }
        tp
    }
}

/// Go `booleanFunctions` (`function_traits.go`).
fn is_boolean_function(func_name: &str) -> bool {
    matches!(
        func_name,
        "not"
            | "eq"
            | "ne"
            | "nulleq"
            | "lt"
            | "le"
            | "gt"
            | "ge"
            | "in"
            | "and"
            | "or"
            | "xor"
            | "istrue_with_null"
            | "istrue"
            | "isfalse"
            | "isnull"
            | "like"
            | "regexp"
            | "is_ipv4"
            | "is_ipv4_compat"
            | "is_ipv4_mapped"
            | "is_ipv6"
            | "json_schema_valid"
            | "json_valid"
            | "regexp_like"
    )
}

/// Go `adjustNullFlagForReturnType`.
fn adjust_null_flag_for_return_type(func_name: &str, args: &[Expression], tp: &mut FieldType) {
    match func_name {
        // `functionSetForReturnTypeAlwaysNotNull`.
        "isnull" | "nulleq" => tp.add_flags(FieldTypeFlags::NOT_NULL),
        // `functionSetForReturnTypeAlwaysNullable`.
        "div" | "intdiv" | "mod" => tp.del_flag(FieldTypeFlags::NOT_NULL),
        // `functionSetForReturnTypeNotNullOnNotNull`.
        "plus" | "minus" | "mul" => {
            let not_null = args
                .iter()
                .all(|arg| type_of(arg).has_flag(FieldTypeFlags::NOT_NULL));
            if not_null {
                tp.add_flags(FieldTypeFlags::NOT_NULL);
            } else {
                tp.del_flag(FieldTypeFlags::NOT_NULL);
            }
        }
        _ => {}
    }
}

/// Go `newReturnFieldTypeForBaseBuiltinFunc`.
fn new_return_field_type_for_base_builtin_func(
    func_name: &str,
    ret_type: EvalType,
    ec: &ExprCollation,
) -> FieldType {
    let binary = |code: FieldTypeCode| {
        FieldTypeBuilder::new()
            .with_code(code)
            .add_flags(FieldTypeFlags::BINARY)
    };
    let mut field_type = match ret_type {
        EvalType::Int => binary(FieldTypeCode::LongLong)
            .flen_set(MAX_INT_WIDTH)
            .build(),
        EvalType::Real => binary(FieldTypeCode::Double)
            .flen_set(MAX_REAL_WIDTH)
            .decimal_set(UNSPECIFIED_LENGTH)
            .build(),
        EvalType::Decimal => binary(FieldTypeCode::NewDecimal).flen_set(11).build(),
        EvalType::String => FieldTypeBuilder::new()
            .with_code(FieldTypeCode::VarString)
            .flen_set(UNSPECIFIED_LENGTH)
            .decimal_set(UNSPECIFIED_LENGTH)
            .charset_set(&ec.charset)
            .collation_set(&ec.collation)
            .build(),
        EvalType::Datetime => binary(FieldTypeCode::Datetime)
            .flen_set(MAX_DATETIME_WIDTH_WITH_FSP)
            .decimal_set(MAX_FSP)
            .build(),
        EvalType::Timestamp => binary(FieldTypeCode::Timestamp)
            .flen_set(MAX_DATETIME_WIDTH_WITH_FSP)
            .decimal_set(MAX_FSP)
            .build(),
        EvalType::Duration => binary(FieldTypeCode::Duration)
            .flen_set(MAX_DURATION_WIDTH_WITH_FSP)
            .decimal_set(MAX_FSP)
            .build(),
        EvalType::Json => binary(FieldTypeCode::Json)
            .flen_set(MAX_BLOB_WIDTH)
            .charset_set("utf8mb4")
            .collation_set("utf8mb4_bin")
            .build(),
        EvalType::VectorFloat32 => binary(FieldTypeCode::VectorFloat32)
            .flen_set(UNSPECIFIED_LENGTH)
            .build(),
    };
    if field_type.has_flag(FieldTypeFlags::BINARY) && field_type.code() != FieldTypeCode::Json {
        field_type.set_charset_name("binary");
        field_type.set_collation_name("binary");
    }
    if is_boolean_function(func_name) {
        field_type.add_flags(FieldTypeFlags::IS_BOOLEAN);
    }
    field_type
}

/// Go `BuildCastFunction` closes the cast it builds with `FoldConstant`. The
/// `wrap_with_cast_as_*` helpers return theirs unfolded, so the fold runs
/// here, on a cast the helper built or was given.
fn fold_built_cast(ctx: &dyn BuildContext, mut wrapped: Expression) -> Expression {
    if crate::rewriter::is_newly_built_cast(&wrapped) {
        ctx.fold_constant(&mut wrapped);
    }
    wrapped
}

/// Go `WrapWithCastAsDecimal` (`builtin_cast.go:2736`): `BuildCastFunction`
/// folds the cast it builds, and a cast that is `ConstStrict` is evaluated
/// in the build context for the precision and scale of its value.
fn wrap_with_cast_as_decimal(
    ctx: &dyn BuildContext,
    expr: Expression,
) -> Result<Expression, EvalError> {
    if type_of(&expr).eval_type() == EvalType::Decimal {
        return Ok(expr);
    }
    let mut cast_expr = fold_built_cast(ctx, build_cast_as_decimal(expr)?);
    if cast_expr.const_level() == ConstLevel::STRICT {
        let value = ctx.eval_constant(&cast_expr);
        set_const_decimal_cast_precision(&mut cast_expr, value);
    }
    Ok(cast_expr)
}

/// Go `newBaseBuiltinFuncWithTp`: the builtin function over `args` cast to
/// `arg_tps`, returning `ret_type`.
///
/// # Panics
///
/// As Go does, when `args` and `arg_tps` differ in length.
pub(crate) fn new_base_builtin_func_with_tp(
    ctx: &dyn BuildContext,
    func_name: &str,
    mut args: Vec<Expression>,
    ret_type: EvalType,
    arg_tps: &[EvalType],
) -> Result<BaseBuiltinFunc, EvalError> {
    assert_eq!(
        args.len(),
        arg_tps.len(),
        "unexpected length of args and argTps"
    );
    // The collation is derived before the implicit casts.
    let ec =
        derive_collation_with_connection(func_name, &args, ret_type, ctx.connection_charset_info())?;
    for (arg, arg_tp) in args.iter_mut().zip(arg_tps) {
        let given = std::mem::replace(arg, Expression::Constant(Constant::new_null()));
        *arg = match arg_tp {
            EvalType::Int => fold_built_cast(ctx, wrap_with_cast_as_int(given, None)?),
            EvalType::Real => fold_built_cast(ctx, wrap_with_cast_as_real(given)?),
            EvalType::Decimal => wrap_with_cast_as_decimal(ctx, given)?,
            EvalType::String => {
                let wrapped = fold_built_cast(
                    ctx,
                    wrap_with_cast_as_string(given, ctx.connection_charset_info())?,
                );
                crate::convert_charset::handle_binary_literal(ctx, wrapped, &ec, func_name)
            }
            EvalType::Datetime => fold_built_cast(
                ctx,
                wrap_with_cast_as_time(given, FieldType::new(FieldTypeCode::Datetime))?,
            ),
            EvalType::Timestamp => fold_built_cast(
                ctx,
                wrap_with_cast_as_time(given, FieldType::new(FieldTypeCode::Timestamp))?,
            ),
            EvalType::Duration => fold_built_cast(ctx, wrap_with_cast_as_duration(given)?),
            EvalType::Json => fold_built_cast(ctx, wrap_with_cast_as_json(given)?),
            EvalType::VectorFloat32 => {
                fold_built_cast(ctx, wrap_with_cast_as_vector_float32(given)?)
            }
        };
    }
    let mut tp = new_return_field_type_for_base_builtin_func(func_name, ret_type, &ec);
    let mut collation = CollationInfo::default();
    collation.set_coercibility(ec.coer);
    collation.set_repertoire(ec.repe);
    collation.set_charset_and_collation(&ec.charset, &ec.collation);
    // The null flag reads the arguments after their casts.
    adjust_null_flag_for_return_type(func_name, &args, &mut tp);
    Ok(BaseBuiltinFunc {
        args,
        tp,
        pb_code: ScalarFuncSig::Unspecified,
        collation,
    })
}
