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

use crate::collation_derive::derive_collation_with_arg_tps;
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
use tidb_terror::TerrorError;

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
    /// Go `ctx.GetEvalCtx().AppendWarning(err)`.
    fn append_warning(&self, error: &TerrorError);
    /// Go `ctx.GetEvalCtx()` itself, for a build that evaluates in the
    /// statement: a context-free resolver has none.
    fn columns(&self) -> Option<&dyn Columns>;
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

    fn append_warning(&self, error: &TerrorError) {
        self.0.append_error_warning(&error.to_sql_error());
    }

    fn columns(&self) -> Option<&dyn Columns> {
        Some(self.0)
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

    /// The statement's warnings are reset once it is planned, so a warning
    /// of the build is kept where the fold's are, to be reported after the
    /// statement ran.
    fn append_warning(&self, error: &TerrorError) {
        let error = error.to_sql_error();
        tidb_context::fold_warnings::record_fold_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            error.code,
            error.message,
            error.identity,
        ));
    }

    fn columns(&self) -> Option<&dyn Columns> {
        self.0.comparison_context()
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
    use crate::builtin_fts::{FtsMatchWordFunctionClass, FtsMysqlMatchAgainstFunctionClass};
    use crate::builtin_grouping::GroupingImplFunctionClass;
    use crate::builtin_ilike::IlikeFunctionClass;
    use crate::builtin_compare::CompareFunctionClass;
    use crate::builtin_control::{CaseWhenFunctionClass, IfFunctionClass, IfNullFunctionClass};
    use crate::builtin_like::LikeFunctionClass;
    use crate::builtin_math as m;
    use crate::builtin_op::{
        BitNegFunctionClass, IsNullFunctionClass, LogicAndFunctionClass, LogicOrFunctionClass,
        LogicXorFunctionClass, UnaryMinusFunctionClass, UnaryNotFunctionClass, BIT_AND, BIT_OR,
        BIT_XOR, IS_FALSITY, IS_TRUTH_WITHOUT_NULL, IS_TRUTH_WITH_NULL, LEFT_SHIFT, RIGHT_SHIFT,
    };
    use crate::builtin_encryption as enc;
    use crate::builtin_info as i;
    use crate::builtin_miscellaneous as misc;
    Some(match name {
        "sleep" => &misc::SleepFunctionClass { func_name: "sleep" },
        "get_lock" => &misc::LockFunctionClass { func_name: "get_lock" },
        "release_lock" => &misc::ReleaseLockFunctionClass { func_name: "release_lock" },
        "inet_aton" => &misc::InetAtonFunctionClass { func_name: "inet_aton" },
        "inet_ntoa" => &misc::InetNtoaFunctionClass { func_name: "inet_ntoa" },
        "inet6_aton" => &misc::Inet6AtonFunctionClass { func_name: "inet6_aton" },
        "inet6_ntoa" => &misc::Inet6NtoaFunctionClass { func_name: "inet6_ntoa" },
        "is_free_lock" => &misc::IsFreeLockFunctionClass { func_name: "is_free_lock" },
        "is_ipv4" => &misc::IsIPv4FunctionClass { func_name: "is_ipv4" },
        "is_ipv4_compat" => &misc::IsIPv4CompatFunctionClass { func_name: "is_ipv4_compat" },
        "is_ipv4_mapped" => &misc::IsIPv4MappedFunctionClass { func_name: "is_ipv4_mapped" },
        "is_ipv6" => &misc::IsIPv6FunctionClass { func_name: "is_ipv6" },
        "is_used_lock" => &misc::IsUsedLockFunctionClass { func_name: "is_used_lock" },
        "is_uuid" => &misc::IsUUIDFunctionClass { func_name: "is_uuid" },
        "release_all_locks" => &misc::ReleaseAllLocksFunctionClass { func_name: "release_all_locks" },
        "uuid" => &misc::UuidFunctionClass { func_name: "uuid" },
        "uuid_v4" => &misc::Uuidv4FunctionClass { func_name: "uuid_v4" },
        "uuid_v7" => &misc::Uuidv7FunctionClass { func_name: "uuid_v7" },
        "uuid_version" => &misc::UuidVersionFunctionClass { func_name: "uuid_version" },
        "uuid_timestamp" => &misc::UuidTimestampFunctionClass { func_name: "uuid_timestamp" },
        "vitess_hash" => &misc::VitessHashFunctionClass { func_name: "vitess_hash" },
        "tidb_shard" => &misc::TidbShardFunctionClass { func_name: "tidb_shard" },
        "any_value" => &misc::AnyValueFunctionClass,
        "name_const" => &misc::NameConstFunctionClass,
        "uuid_to_bin" => &misc::UuidToBinFunctionClass,
        "bin_to_uuid" => &misc::BinToUuidFunctionClass,
        "database" => &i::DatabaseFunctionClass { func_name: "database" },
        "schema" => &i::DatabaseFunctionClass { func_name: "schema" },
        "current_user" => &i::CurrentUserFunctionClass { func_name: "current_user" },
        "current_role" => &i::CurrentRoleFunctionClass { func_name: "current_role" },
        "current_resource_group" => &i::CurrentResourceGroupFunctionClass { func_name: "current_resource_group" },
        "user" => &i::UserFunctionClass { func_name: "user" },
        "session_user" => &i::UserFunctionClass { func_name: "session_user" },
        "system_user" => &i::UserFunctionClass { func_name: "system_user" },
        "version" => &i::VersionFunctionClass { func_name: "version" },
        "found_rows" => &i::FoundRowsFunctionClass { func_name: "found_rows" },
        "connection_id" => &i::ConnectionIdFunctionClass { func_name: "connection_id" },
        "tidb_version" => &i::TidbVersionFunctionClass { func_name: "tidb_version" },
        "tidb_is_ddl_owner" => &i::TidbIsDdlOwnerFunctionClass { func_name: "tidb_is_ddl_owner" },
        "row_count" => &i::RowCountFunctionClass { func_name: "row_count" },
        "tidb_mvcc_info" => &i::TidbMvccInfoFunctionClass { func_name: "tidb_mvcc_info" },
        "tidb_decode_key" => &i::TidbDecodeKeyFunctionClass { func_name: "tidb_decode_key" },
        "tidb_encode_sql_digest" => &i::TidbEncodeSqlDigestFunctionClass { func_name: "tidb_encode_sql_digest" },
        "tidb_decode_plan" => &i::TidbDecodePlanFunctionClass { func_name: "tidb_decode_plan" },
        "tidb_decode_binary_plan" => &i::TidbDecodePlanFunctionClass { func_name: "tidb_decode_binary_plan" },
        "nextval" => &i::NextValFunctionClass { func_name: "nextval" },
        "lastval" => &i::LastValFunctionClass { func_name: "lastval" },
        "setval" => &i::SetValFunctionClass { func_name: "setval" },
        "format_bytes" => &i::FormatBytesFunctionClass { func_name: "format_bytes" },
        "format_nano_time" => &i::FormatNanoTimeFunctionClass { func_name: "format_nano_time" },
        "last_insert_id" => &i::LastInsertIdFunctionClass,
        "benchmark" => &i::BenchmarkFunctionClass,
        "charset" => &i::CharsetFunctionClass,
        "coercibility" => &i::CoercibilityFunctionClass,
        "collation" => &i::CollationFunctionClass,
        "tidb_decode_sql_digests" => &i::TidbDecodeSqlDigestsFunctionClass,
        "aes_decrypt" => &enc::AesDecryptFunctionClass,
        "aes_encrypt" => &enc::AesEncryptFunctionClass,
        "decode" => &enc::DecodeFunctionClass { func_name: "decode" },
        "encode" => &enc::EncodeFunctionClass { func_name: "encode" },
        "password" => &enc::PasswordFunctionClass { func_name: "password" },
        "random_bytes" => &enc::RandomBytesFunctionClass { func_name: "random_bytes" },
        "md5" => &enc::Md5FunctionClass { func_name: "md5" },
        "sha1" => &enc::Sha1FunctionClass { func_name: "sha1" },
        "sha" => &enc::Sha1FunctionClass { func_name: "sha" },
        "sha2" => &enc::Sha2FunctionClass { func_name: "sha2" },
        "sm3" => &enc::Sm3FunctionClass { func_name: "sm3" },
        "compress" => &enc::CompressFunctionClass { func_name: "compress" },
        "uncompress" => &enc::UncompressFunctionClass { func_name: "uncompress" },
        "uncompressed_length" => &enc::UncompressedLengthFunctionClass { func_name: "uncompressed_length" },
        "validate_password_strength" => &enc::ValidatePasswordStrengthFunctionClass { func_name: "validate_password_strength" },
        "lt" => &CompareFunctionClass { func_name: "lt" },
        "le" => &CompareFunctionClass { func_name: "le" },
        "gt" => &CompareFunctionClass { func_name: "gt" },
        "eq" => &CompareFunctionClass { func_name: "eq" },
        "ge" => &CompareFunctionClass { func_name: "ge" },
        "ne" => &CompareFunctionClass { func_name: "ne" },
        "nulleq" => &CompareFunctionClass { func_name: "nulleq" },
        "case" => &CaseWhenFunctionClass,
        "coalesce" => &crate::builtin_compare::CoalesceFunctionClass,
        "greatest" => &crate::builtin_compare::GreatestFunctionClass,
        "least" => &crate::builtin_compare::LeastFunctionClass,
        "interval" => &crate::builtin_compare::IntervalFunctionClass,
        "if" => &IfFunctionClass,
        "ifnull" => &IfNullFunctionClass,
        "like" => &LikeFunctionClass,
        "ilike" => &IlikeFunctionClass,
        "grouping" => &GroupingImplFunctionClass,
        "fts_match_word" => &FtsMatchWordFunctionClass,
        "match_against" => &FtsMysqlMatchAgainstFunctionClass,
        "isnull" => &IsNullFunctionClass,
        "and" => &LogicAndFunctionClass,
        "or" => &LogicOrFunctionClass,
        "xor" => &LogicXorFunctionClass,
        "not" => &UnaryNotFunctionClass,
        "bitneg" => &BitNegFunctionClass,
        "bitand" => &BIT_AND,
        "bitor" => &BIT_OR,
        "bitxor" => &BIT_XOR,
        "leftshift" => &LEFT_SHIFT,
        "rightshift" => &RIGHT_SHIFT,
        "unaryminus" => &UnaryMinusFunctionClass,
        "istrue_with_null" => &IS_TRUTH_WITH_NULL,
        "istrue" => &IS_TRUTH_WITHOUT_NULL,
        "isfalse" => &IS_FALSITY,
        "abs" => &m::AbsFunctionClass,
        "acos" => &m::AcosFunctionClass { func_name: "acos" },
        "asin" => &m::AsinFunctionClass { func_name: "asin" },
        "atan" => &m::AtanFunctionClass { func_name: "atan" },
        "atan2" => &m::AtanFunctionClass { func_name: "atan2" },
        "ceil" => &m::CeilFunctionClass { func_name: "ceil" },
        "ceiling" => &m::CeilFunctionClass { func_name: "ceiling" },
        "conv" => &m::ConvFunctionClass,
        "cos" => &m::CosFunctionClass { func_name: "cos" },
        "cot" => &m::CotFunctionClass { func_name: "cot" },
        "crc32" => &m::Crc32FunctionClass,
        "degrees" => &m::DegreesFunctionClass { func_name: "degrees" },
        "exp" => &m::ExpFunctionClass { func_name: "exp" },
        "floor" => &m::FloorFunctionClass,
        "ln" => &m::LogFunctionClass { func_name: "ln" },
        "log" => &m::LogFunctionClass { func_name: "log" },
        "log2" => &m::Log2FunctionClass { func_name: "log2" },
        "log10" => &m::Log10FunctionClass { func_name: "log10" },
        "pi" => &m::PiFunctionClass,
        "pow" => &m::PowFunctionClass { func_name: "pow" },
        "power" => &m::PowFunctionClass { func_name: "power" },
        "radians" => &m::RadiansFunctionClass { func_name: "radians" },
        "rand" => &m::RandFunctionClass,
        "round" => &m::RoundFunctionClass,
        "sign" => &m::SignFunctionClass { func_name: "sign" },
        "sin" => &m::SinFunctionClass { func_name: "sin" },
        "sqrt" => &m::SqrtFunctionClass { func_name: "sqrt" },
        "tan" => &m::TanFunctionClass { func_name: "tan" },
        "truncate" => &m::TruncateFunctionClass,
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
    let ec = derive_collation_with_arg_tps(
        func_name,
        &args,
        ret_type,
        arg_tps,
        ctx.connection_charset_info(),
    )?;
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
    Ok(base_builtin_func_of(func_name, args, ret_type, &ec))
}

/// Go `newBaseBuiltinFuncWithFieldTypes`: the builtin function over `args`
/// cast to the field types `arg_tps`, returning `ret_type`. A decimal,
/// datetime, timestamp or duration argument is cast to the whole declared
/// type, its length and scale included.
///
/// # Panics
///
/// As Go does, when `args` and `arg_tps` differ in length.
pub(crate) fn new_base_builtin_func_with_field_types(
    ctx: &dyn BuildContext,
    func_name: &str,
    mut args: Vec<Expression>,
    ret_type: EvalType,
    arg_tps: &[FieldType],
) -> Result<BaseBuiltinFunc, EvalError> {
    assert_eq!(
        args.len(),
        arg_tps.len(),
        "unexpected length of args and argTps"
    );
    // The collation is derived before the implicit casts.
    let arg_eval_tps: Vec<EvalType> = arg_tps.iter().map(FieldType::eval_type).collect();
    let ec = derive_collation_with_arg_tps(
        func_name,
        &args,
        ret_type,
        &arg_eval_tps,
        ctx.connection_charset_info(),
    )?;
    for (arg, arg_tp) in args.iter_mut().zip(arg_tps) {
        let given = std::mem::replace(arg, Expression::Constant(Constant::new_null()));
        *arg = match arg_tp.eval_type() {
            EvalType::Int => fold_built_cast(ctx, wrap_with_cast_as_int(given, Some(arg_tp))?),
            EvalType::Real => fold_built_cast(ctx, wrap_with_cast_as_real(given)?),
            EvalType::String => {
                let wrapped = fold_built_cast(
                    ctx,
                    wrap_with_cast_as_string(given, ctx.connection_charset_info())?,
                );
                crate::convert_charset::handle_binary_literal(ctx, wrapped, &ec, func_name)
            }
            EvalType::Json => fold_built_cast(ctx, wrap_with_cast_as_json(given)?),
            EvalType::Decimal | EvalType::Datetime | EvalType::Timestamp | EvalType::Duration => {
                if type_of(&given).equal(arg_tp) {
                    given
                } else {
                    fold_built_cast(
                        ctx,
                        crate::simple_expr::build_cast_function(given, arg_tp.clone(), false)?,
                    )
                }
            }
            EvalType::VectorFloat32 => given,
        };
    }
    Ok(base_builtin_func_of(func_name, args, ret_type, &ec))
}

/// The tail `newBaseBuiltinFuncWithTp` and `newBaseBuiltinFuncWithFieldTypes`
/// share: the return type and collation `ec` gives, the null flag read off
/// the arguments after their casts.
fn base_builtin_func_of(
    func_name: &str,
    args: Vec<Expression>,
    ret_type: EvalType,
    ec: &ExprCollation,
) -> BaseBuiltinFunc {
    let mut tp = new_return_field_type_for_base_builtin_func(func_name, ret_type, ec);
    let mut collation = CollationInfo::default();
    collation.set_coercibility(ec.coer);
    collation.set_repertoire(ec.repe);
    collation.set_charset_and_collation(&ec.charset, &ec.collation);
    adjust_null_flag_for_return_type(func_name, &args, &mut tp);
    BaseBuiltinFunc {
        args,
        tp,
        pb_code: ScalarFuncSig::Unspecified,
        collation,
    }
}
