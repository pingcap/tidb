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

//! `pkg/expression/scalar_function.go`: the `ScalarFunction` expression node.
//!
//! BRIDGE DECISION: Go keeps a `ScalarFunction`'s arguments and evaluation
//! behind a `Function builtinFunc` interface (implemented by hundreds of
//! per-signature structs). This port instead holds `args: Vec<Expression>`
//! directly on the node. Protobuf functions retain a typed implementation
//! selected by `tipb.ScalarFuncSig`; SQL-built functions still use the
//! existing name-based builder and shared evaluation kernels.
//!
//! Ported: the struct and its argument-structural methods, recursive
//! `Decorrelate`, const-level rules, the common `ReHashCode` path (including
//! `Grouping` metadata), structural `Hash64`/`Equals`, and evaluation for
//! operators plus the builtin families owned by the shared dispatch modules.
//! Unknown builtin names fail explicitly. `MemoryUsage` lives in
//! `crate::memory_usage`; the remaining structural gap is per-signature
//! collation.

pub(crate) mod class_kernels;
mod pb_builtin;

pub(crate) use pb_builtin::{greatest_method, least_method, Kernel, PbBuiltin};

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use tidb_exprctx::context::{Columns, EvalError};
use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, SCALAR_FUNCTION_FLAG};
use crate::grouping::{GroupingMetadata, GroupingMetadataError, GroupingMode};
use crate::schema::Schema;
use tidb_ast::{BinaryOp, CiString, UnaryOp};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_codec::{encode_compact_bytes, encode_int};
use tidb_datatype::{Datum, EvalType, FieldType, UNSPECIFIED_LENGTH};

const MAX_ADVISORY_LOCK_TIMEOUT_SECS: i64 = 1_073_741_824;

fn advisory_lock_name(value: Datum) -> Result<String, EvalError> {
    let bytes = crate::arg_eval_type::eval_string(&value)?;
    let Some(bytes) = bytes else {
        return Err(EvalError::AdvisoryLock {
            code: 3057,
            message: "Incorrect user-level lock name 'NULL'.".to_owned(),
        });
    };
    let text = tidb_goruntime::GoString::from_bytes(bytes).to_utf8_lossy_go();
    if text.is_empty() || text.chars().count() > 64 {
        return Err(EvalError::AdvisoryLock {
            code: 3057,
            message: format!("Incorrect user-level lock name '{text}'."),
        });
    }
    let normalized = tidb_mysql::to_lowercase(&text);
    if normalized.chars().count() > 64 {
        return Err(EvalError::IncorrectArguments(
            "Incorrect arguments to get_lock".to_owned(),
        ));
    }
    Ok(normalized)
}

fn binary_op_for_name(name: &str) -> Option<BinaryOp> {
    Some(match name {
        "plus" => BinaryOp::Plus,
        "minus" => BinaryOp::Minus,
        "mul" => BinaryOp::Mul,
        "div" => BinaryOp::Div,
        "intdiv" => BinaryOp::IntDiv,
        "mod" => BinaryOp::Mod,
        "bitand" => BinaryOp::BitAnd,
        "bitor" => BinaryOp::BitOr,
        "bitxor" => BinaryOp::BitXor,
        "leftshift" => BinaryOp::LeftShift,
        "rightshift" => BinaryOp::RightShift,
        "eq" => BinaryOp::Eq,
        "nulleq" => BinaryOp::NullEq,
        "ne" => BinaryOp::Ne,
        "lt" => BinaryOp::Lt,
        "le" => BinaryOp::Le,
        "gt" => BinaryOp::Gt,
        "ge" => BinaryOp::Ge,
        "and" => BinaryOp::LogicAnd,
        "or" => BinaryOp::LogicOr,
        "xor" => BinaryOp::LogicXor,
        _ => return None,
    })
}

/// The eval-type family a [`Datum`] belongs to -- the inverse of the switch
/// Go's `ScalarFunction.Eval` performs on `RetType.EvalType()`. `None` is a
/// kind no eval type names (NULL, the range sentinels, an undecoded `Raw`).
///
/// `Datum::Time` is the value domain of BOTH Go's `ETDatetime` and
/// `ETTimestamp`, so it reports `Datetime` and a timestamp result type is
/// normalized onto it by [`same_eval_family`].
fn datum_eval_type(value: &Datum) -> Option<tidb_datatype::EvalType> {
    use tidb_datatype::EvalType;
    Some(match value {
        Datum::Int(_) | Datum::UInt(_) => EvalType::Int,
        Datum::Real(_) | Datum::Float32(_) => EvalType::Real,
        Datum::Decimal(_) => EvalType::Decimal,
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::BinaryLiteral(_)
        | Datum::Bit(_)
        | Datum::Enum(..)
        | Datum::Set(..) => EvalType::String,
        Datum::Time(_) => EvalType::Datetime,
        Datum::Duration(_) => EvalType::Duration,
        Datum::Json(_) => EvalType::Json,
        Datum::VectorFloat32(_) => EvalType::VectorFloat32,
        Datum::Null | Datum::MinNotNull | Datum::MaxValue | Datum::Raw(_) => return None,
    })
}

/// Whether `value` is already the family `ret_type` declares.
///
/// A HYBRID result type (`BIT`/`ENUM`/`SET`, Go `FieldType.Hybrid`) always
/// answers yes: its eval type is the domain it COMPARES in (`BIT` compares as
/// an integer), while `getFixedLen`'s default arm gives it a variable-length
/// cell that only the hybrid datum itself fits. Converting such a value onto
/// its eval type would put an integer in a var-length column -- the very
/// disagreement this check exists to prevent.
fn same_eval_family(value: &Datum, ret_type: &tidb_datatype::FieldType) -> bool {
    use tidb_datatype::EvalType;
    if ret_type.is_hybrid() {
        return true;
    }
    // Go's `EvalString` reads an ENUM/SET by its name, so under a plain
    // string result the hybrid datum becomes that string: `IF(c, e, e)`
    // over an ENUM column is VARCHAR and orders by name, not by ordinal.
    if matches!(value, Datum::Enum(..) | Datum::Set(..)) {
        return false;
    }
    // The REAL family is the one place where sharing an eval type is not
    // enough. Go's `ScalarFunction.Eval` returns `EvalReal`'s float64 for both
    // `FLOAT` and `DOUBLE` (`cast(1.1 as float) = 1.1` is true), so a
    // 4-byte `KindFloat32` datum is only ever a `FLOAT` COLUMN's own cell,
    // never an expression's result: a `Float32` value under a `DOUBLE`
    // result type widens, while a float64 under `FLOAT` stays one until a
    // chunk's 4-byte cell stores it.
    if ret_type.eval_type() == EvalType::Real {
        return match value {
            Datum::Float32(_) => ret_type.code() == tidb_datatype::FieldTypeCode::Float,
            Datum::Real(_) => true,
            _ => false,
        };
    }
    let normalize = |t| match t {
        EvalType::Timestamp => EvalType::Datetime,
        other => other,
    };
    datum_eval_type(value).is_some_and(|got| normalize(got) == normalize(ret_type.eval_type()))
}

/// Go `unFoldableFunctions`: calls whose result cannot be frozen during
/// constant folding even when every argument is a strict literal.
#[must_use]
pub fn is_unfoldable_function(name: &str) -> bool {
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
                | "getvar"
                | "getparam"
                | "benchmark"
                | "dayname"
                // These information functions read session properties in
                // Go (`CurrentDB`, `CurrentUserPropReader`, or
                // `SessionVarsPropReader`). Rust's planner fold has only
                // `NoColumns`, so keep them runtime-bound instead of
                // replacing a missing property with a frozen NULL.
                | "database"
                | "schema"
                | "current_user"
                | "current_role"
                | "current_resource_group"
                | "user"
                | "session_user"
                | "system_user"
                | "connection_id"
                | "row_count"
                | "version"
                | "tidb_version"
                | "tidb_decode_sql_digests"
                | "nextval"
                | "lastval"
                | "setval"
                | "any_value"
        )
}

/// The Go scalar-function name for a binary operator (inverse of
/// [`binary_op_for_name`]); used when building a [`ScalarFunction`] from an AST
/// operator.
#[must_use]
pub fn binary_op_name(op: BinaryOp) -> &'static str {
    op.opcode().name()
}

/// The Go scalar-function name for a unary operator (inverse of
/// [`unary_op_for_name`]). `Not`/`NotKeyword` share the `not` function.
#[must_use]
pub fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Plus => "unaryplus",
        UnaryOp::Minus => "unaryminus",
        UnaryOp::BitNeg => "bitneg",
        UnaryOp::Not | UnaryOp::NotKeyword => "not",
    }
}

fn append_canonical_name(output: &mut Vec<u8>, name: &str) {
    encode_compact_bytes(output, name.as_bytes());
}

fn append_canonical_args(output: &mut Vec<u8>, args: &[Vec<u8>]) {
    for code in args {
        output.extend_from_slice(code);
    }
}

fn append_canonical_args_reversed(output: &mut Vec<u8>, args: &[Vec<u8>]) {
    for code in args.iter().rev() {
        output.extend_from_slice(code);
    }
}

/// The expression a coprocessor rebuilds from `expr`'s protobuf. Go's wire
/// signatures carry no `cannotConvertStringAsWarning`, so a pushed-down
/// `from_binary` errors where the root-side one would warn.
pub fn into_coprocessor_form(expr: &mut Expression) {
    if let Expression::ScalarFunction(function) = expr {
        function.cannot_convert_string_as_warning = false;
        function.args.iter_mut().for_each(into_coprocessor_form);
    }
}

/// Go `ScalarFunction`: the application of a built-in function to arguments.
#[derive(Clone, Debug, Default)]
pub struct ScalarFunction {
    /// Go `FuncName` (an `ast.CIStr`): the function's name.
    pub func_name: CiString,
    /// Go `RetType` (a `*types.FieldType`; `None` mirrors a nil pointer).
    pub ret_type: Option<FieldType>,
    /// The function arguments. In Go these live inside `Function.getArgs()`.
    pub args: Vec<Expression>,
    /// Go Function: a protobuf-selected builtin, independent of FuncName.
    pb_builtin: Option<PbBuiltin>,
    /// Lazily-filled `HashCode` cache (Go `hashcode`).
    hashcode: Vec<u8>,
    /// Go `BuiltinGroupingImplSig` metadata installed by `SetMetadata`.
    ///
    /// Grouping is built through `NewFunctionWithInit`, so carrying the
    /// validated metadata on the node is what lets clones and substitution
    /// preserve the function's grouping-id semantics and hash identity.
    grouping_metadata: Option<GroupingMetadata>,
    /// Go `builtinInternalFromBinarySig.cannotConvertStringAsWarning`: the
    /// `from_binary` an explicit CAST builds warns where an implicit one
    /// errors. The protobuf signature has no field for it, so a copy pushed
    /// to the coprocessor errors either way.
    cannot_convert_string_as_warning: bool,
    /// Go `builtinJSONSumCRC32Sig.tp`: the `ARRAY` type `JSON_SUM_CRC32`
    /// converts each element to, distinct from its `BIGINT` result type.
    json_sum_crc32_type: Option<FieldType>,
    /// Go `builtinGreatest*Sig` / `builtinLeast*Sig`: the signature
    /// `resolveType4Extremum` chose from the arguments before
    /// `newBaseBuiltinFuncWithTp` cast them (a compare-as-time call's
    /// `cmpAsDate` survives its arguments' cast to strings).
    gl_signature: Option<crate::builtin_ext::GlSignature>,
    /// Go `builtinFtsMysqlMatchAgainstSig.modifier`, which the protobuf
    /// signature has no field for: a copy read back from the wire searches
    /// in natural-language mode.
    fts_modifier: tidb_ast::MatchModifier,
    /// The fields of the Go signature struct `getFunction` built.
    sig: crate::builtin::SigFields,

    /// Go embedded collation state (via the `Function`'s `collationInfo`).
    pub collation: CollationInfo,
    /// Go `builtinInStringSig.hashSet`, prepared after collation derivation.
    /// Only strict string/byte literals enter it; every other argument keeps
    /// the ordinary evaluation path so casts, warnings and errors are not
    /// skipped.
    in_string_hash_set: Option<std::collections::HashSet<Vec<u8>>>,
    in_string_non_const_args: Vec<usize>,
    in_string_has_null: bool,
    json_schema_cache: crate::builtin_ext::JsonSchemaCache,
    json_modify_path_cache:
        crate::builtin_ext::BuiltinFuncCache<Option<Vec<tidb_datatype::JSONPathExpression>>>,
    find_in_set_cache: crate::builtin_ext::BuiltinFuncCache<crate::builtin_ext::FindInSetLookup>,
    regexp_cache: crate::builtin_ext::BuiltinFuncCache<crate::regexp::RegexpMemorizedSig>,
    regexp_replace_instruction_cache:
        crate::builtin_ext::BuiltinFuncCache<Vec<crate::builtin_ext::regexp::ReplacementPart>>,
    like_pattern_cache: crate::builtin_ext::BuiltinFuncCache<crate::like::CompiledLikePattern>,
    ilike_pattern_cache: crate::builtin_ext::BuiltinFuncCache<crate::like::CompiledIlikePattern>,
}

fn arithmetic_symbol(op: tidb_ast::BinaryOp) -> Option<&'static str> {
    Some(match op {
        tidb_ast::BinaryOp::Plus => "+",
        tidb_ast::BinaryOp::Minus => "-",
        tidb_ast::BinaryOp::Mul => "*",
        tidb_ast::BinaryOp::Div => "/",
        tidb_ast::BinaryOp::IntDiv => "DIV",
        tidb_ast::BinaryOp::Mod => "%",
        _ => return None,
    })
}

/// Go `Expression.StringWithCtx(ctx, errors.RedactLogDisable)`: the text a
/// signature's error names its arguments with.
pub(crate) fn string_with_ctx(expression: &Expression, ctx: &dyn Columns) -> Option<String> {
    crate::explain::physical_expression_text_with_columns(
        ctx,
        expression,
        &[],
        crate::explain::ExpressionTextStyle::StringWithCtx,
    )
}

/// Go WrapWithCastAsDecimal: retain declared precision/scale for non-integer
/// sources; integer casts use the decimal width of the integer storage type.
fn numeric_decimal_cast_type(field: &FieldType) -> FieldType {
    let mut target = FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal);
    if field.eval_type() == EvalType::Int {
        use tidb_datatype::FieldTypeCode::*;
        target.set_flen(match field.code() {
            Tiny => 3,
            Short => 5,
            Int24 => 8,
            Long => 10,
            LongLong => 20,
            Year => 4,
            _ => 20,
        });
        target.set_decimal(0);
    } else {
        target.set_flen(if field.flen() < 0 {
            65
        } else {
            field.flen().min(65)
        });
        target.set_decimal_under_limit(field.decimal());
    }
    target
}

fn integer_division_overflow(
    left: crate::coerce::Integer,
    right: crate::coerce::Integer,
) -> EvalError {
    use crate::coerce::Integer;
    let text = |value| match value {
        Integer::Signed(value) => value.to_string(),
        Integer::Unsigned(value) => value.to_string(),
    };
    EvalError::DataOutOfRange {
        value: if matches!(left, Integer::Unsigned(_)) || matches!(right, Integer::Unsigned(_)) {
            "BIGINT UNSIGNED"
        } else {
            "BIGINT"
        },
        expression: format!("({}, {})", text(left), text(right)),
    }
}

/// Renders the operand list Go's `StringWithCtx(errors.RedactLogDisable)` uses
/// in an arithmetic overflow. Resolved columns and nested arithmetic
/// functions retain their source names and shape; unknown expression kinds
/// keep the caller's safe fallback instead of inventing a display string.
fn arithmetic_overflow_expression(
    function: &ScalarFunction,
    op: tidb_ast::BinaryOp,
    ctx: &dyn Columns,
) -> Option<String> {
    let symbol = arithmetic_symbol(op)?;
    let [left, right] = function.get_args() else {
        return None;
    };
    Some(format!(
        "({} {symbol} {})",
        string_with_ctx(left, ctx)?,
        string_with_ctx(right, ctx)?
    ))
}

/// Go's 1690 text for one binary integer overflow: the result type's integer
/// class (`BIGINT` / `BIGINT UNSIGNED`, from the function's declared result
/// type) and the rendered operand list.
fn arithmetic_overflow_error(
    function: &ScalarFunction,
    op: tidb_ast::BinaryOp,
    ctx: &dyn Columns,
) -> EvalError {
    let Some(operands) = arithmetic_overflow_expression(function, op, ctx) else {
        return EvalError::IntOverflow;
    };
    let class = match function.get_static_type() {
        Some(field_type) if field_type.is_unsigned() => "BIGINT UNSIGNED",
        _ => "BIGINT",
    };
    EvalError::DataOutOfRange {
        value: class,
        expression: operands,
    }
}

/// Go's 1690 text for one binary REAL overflow. The real signatures use the
/// same operand rendering as integer signatures, but always name `DOUBLE`.
fn real_arithmetic_overflow_error(
    function: &ScalarFunction,
    op: tidb_ast::BinaryOp,
    ctx: &dyn Columns,
) -> EvalError {
    let Some(operands) = arithmetic_overflow_expression(function, op, ctx) else {
        return EvalError::FloatOverflow;
    };
    EvalError::DataOutOfRange {
        value: "DOUBLE",
        expression: operands,
    }
}

/// Go's 1690 text for one binary DECIMAL overflow. The decimal signatures
/// carry the same source-shaped operand expression and name their domain.
fn decimal_arithmetic_overflow_error(
    function: &ScalarFunction,
    op: tidb_ast::BinaryOp,
    ctx: &dyn Columns,
) -> EvalError {
    let Some(operands) = arithmetic_overflow_expression(function, op, ctx) else {
        return EvalError::DecimalOverflow;
    };
    EvalError::DataOutOfRange {
        value: "DECIMAL",
        expression: operands,
    }
}

/// Renders the argument expression used by Go's math overflow signatures.
/// These signatures report their function name and source-shaped arguments,
/// rather than the datum-only `FloatOverflow` carrier returned by the shared
/// math implementation.
fn math_overflow_expression(function: &ScalarFunction, ctx: &dyn Columns) -> Option<String> {
    fn render(expression: &Expression, ctx: &dyn Columns) -> Option<String> {
        match expression {
            Expression::Constant(constant) => match constant.eval_in(ctx).ok()? {
                Datum::Int(value) => Some(value.to_string()),
                Datum::UInt(value) => Some(value.to_string()),
                Datum::Float32(value) => Some(tidb_datatype::format_float_g_shortest(value)),
                Datum::Real(value) => Some(tidb_datatype::format_float_g_shortest(value)),
                Datum::Decimal(value) => Some(value.to_string()),
                Datum::Null => Some("NULL".to_owned()),
                _ => None,
            },
            Expression::Column(column) if !column.orig_name.is_empty() => {
                Some(column.orig_name.clone())
            }
            Expression::CorrelatedColumn(column) if !column.column.orig_name.is_empty() => {
                Some(column.column.orig_name.clone())
            }
            Expression::ScalarFunction(function) => {
                if let Some(op) = binary_op_for_name(function.func_name.lowercase()) {
                    let symbol = arithmetic_symbol(op)?;
                    let [left, right] = function.args.as_slice() else {
                        return None;
                    };
                    return Some(format!(
                        "({} {symbol} {})",
                        render(left, ctx)?,
                        render(right, ctx)?
                    ));
                }
                let args = function
                    .args
                    .iter()
                    .map(|expression| render(expression, ctx))
                    .collect::<Option<Vec<_>>>()?;
                Some(format!(
                    "{}({})",
                    function.func_name.lowercase(),
                    args.join(", ")
                ))
            }
            _ => None,
        }
    }

    let args = function
        .args
        .iter()
        .map(|expression| render(expression, ctx))
        .collect::<Option<Vec<_>>>()?;
    Some(format!(
        "{}({})",
        function.func_name.lowercase(),
        args.join(", ")
    ))
}

/// Converts the datum-only overflow carriers from math builtins into Go's
/// function-specific 1690 diagnostics while the argument expressions remain
/// available on the scalar-function node.
fn math_overflow_error(
    function: &ScalarFunction,
    error: EvalError,
    ctx: &dyn Columns,
) -> EvalError {
    let name = function.func_name.lowercase();
    let value = match (name, &error) {
        ("abs", EvalError::IntOverflow) => "BIGINT",
        ("exp", EvalError::FloatOverflow) => "DOUBLE",
        _ => return error,
    };
    let Some(expression) = math_overflow_expression(function, ctx) else {
        return error;
    };
    EvalError::DataOutOfRange { value, expression }
}

impl ScalarFunction {
    /// Go `cap(sf.hashcode)`, for `MemoryUsage`.
    pub(crate) fn hashcode_capacity(&self) -> usize {
        self.hashcode.capacity()
    }

    /// A node that carries a call's name, type and arguments for type and
    /// collation inference only; it has no signature and is never evaluated.
    #[must_use]
    pub(crate) fn new(func_name: CiString, ret_type: FieldType, args: Vec<Expression>) -> Self {
        ScalarFunction {
            func_name,
            ret_type: Some(ret_type),
            args,
            ..Default::default()
        }
    }

    /// Go `builtinInternalFromBinarySig.cannotConvertStringAsWarning`.
    pub(crate) fn set_cannot_convert_string_as_warning(&mut self, warning: bool) {
        self.cannot_convert_string_as_warning = warning;
    }

    /// Go `BuildJSONSumCrc32FunctionWithCheck`'s node: a `BIGINT` result over
    /// `arg`, whose elements convert to `array_type`'s element type.
    pub(crate) fn new_json_sum_crc32(
        f: crate::builtin::BaseBuiltinFunc,
        ret_type: FieldType,
        array_type: FieldType,
    ) -> Self {
        // Go `builtinJSONSumCRC32Sig.evalInt`, which reads the signature's `tp`.
        let f = f.evaluated_by(Kernel::Method(|function, values, _| {
            let array_type = function
                .json_sum_crc32_type
                .as_ref()
                .expect("a json_sum_crc32 node carries its ARRAY type");
            crate::cast::eval_json_sum_crc32(values[0].clone(), array_type)
        }));
        let mut function = Self::from_builtin_func(CiString::new("json_sum_crc32"), ret_type, f);
        function.json_sum_crc32_type = Some(array_type);
        function
    }

    /// Go `&ScalarFunction{FuncName, RetType, Function: f}`: the node over
    /// the builtin function a class's `getFunction` returned -- its cast
    /// arguments, its signature and its collation.
    pub(crate) fn from_builtin_func(
        func_name: CiString,
        ret_type: FieldType,
        f: crate::builtin::BaseBuiltinFunc,
    ) -> Self {
        Self {
            func_name,
            ret_type: Some(ret_type),
            args: f.args,
            pb_builtin: Some(PbBuiltin::of_builtin_func(f.pb_code, f.kernel)),
            collation: f.collation,
            sig: f.sig,
            ..Default::default()
        }
    }

    /// The node over the builtin function a cast class's `getFunction`
    /// returned, evaluated by its signature.
    pub(crate) fn from_cast_func(f: crate::builtin::BaseBuiltinFunc, in_union: bool) -> Self {
        Self {
            func_name: CiString::new("cast"),
            ret_type: Some(f.tp),
            args: f.args,
            pb_builtin: Some(PbBuiltin::of_cast_func(f.pb_code, f.kernel, in_union)),
            collation: f.collation,
            sig: f.sig,
            ..Default::default()
        }
    }

    /// Go newDistSQLFunctionBySig: retain the wire type and implementation.
    pub(crate) fn from_pb(
        builtin: PbBuiltin,
        ret_type: FieldType,
        args: Vec<Expression>,
    ) -> Result<Self, EvalError> {
        // Go passes the generated wire enum name (not its SQL alias) to
        // deriveCollation, then stores coercibility independently of the wire type.
        let collation = crate::collation_derive::derive_collation(
            builtin.signature().as_str_name(),
            &args,
            ret_type.eval_type(),
        )?;
        let function = Self {
            func_name: CiString::new(format!("sig_{:?}", builtin.signature())),
            ret_type: Some(ret_type),
            args,
            pb_builtin: Some(builtin),
            ..Default::default()
        };
        function.collation.set_coercibility(collation.coer);
        Ok(function)
    }

    /// The signature selected by protobuf decoding, preserved by Clone.
    #[must_use]
    pub fn pb_signature(&self) -> Option<tidb_proto::tipb::ScalarFuncSig> {
        self.pb_builtin.map(PbBuiltin::signature)
    }

    /// Go `FuncName.L == ast.Cast`.
    pub fn is_cast(&self) -> bool {
        self.func_name.lowercase() == "cast"
    }

    /// Go `baseBuiltinCastFunc.inUnion`.
    pub fn cast_in_union(&self) -> bool {
        self.pb_builtin.is_some_and(PbBuiltin::cast_in_union)
    }

    /// A cast to JSON, as opposed to one to a typed array.
    fn is_cast_as_json(&self) -> bool {
        self.is_cast()
            && self
                .get_static_type()
                .is_some_and(|field| field.code() == tidb_datatype::FieldTypeCode::Json && !field.is_array())
    }

    /// Go `builtinValues*Sig.offset`.
    fn values_offset(&self) -> Option<usize> {
        match self.sig {
            crate::builtin::SigFields::Values { offset } => Some(offset),
            _ => None,
        }
    }

    /// Invalidates memoized state derived from this function's arguments.
    ///
    /// Cached physical-plan rebuild replaces parameter constants inside the
    /// argument tree.  The expression node itself is retained, so every
    /// argument-derived cache must be discarded before the rebound tree is
    /// used by ranger or executor code.
    pub fn invalidate_cached_arguments(&mut self) {
        self.hashcode.clear();
        self.in_string_hash_set = None;
        self.in_string_non_const_args.clear();
        self.in_string_has_null = false;
        self.json_schema_cache = Default::default();
        self.json_modify_path_cache = Default::default();
        self.find_in_set_cache = Default::default();
        self.regexp_cache = Default::default();
        self.regexp_replace_instruction_cache = Default::default();
        self.like_pattern_cache = Default::default();
        self.ilike_pattern_cache = Default::default();
    }

    /// Go `BuiltinGroupingImplSig.SetMetadata`: install validated grouping
    /// mode/mark metadata on a `grouping` scalar function and invalidate its
    /// cached hash code. A failed replacement leaves the metadata
    /// uninitialized, matching the source signature's `isMetaInited` flag.
    pub fn set_grouping_metadata(
        &mut self,
        mode: GroupingMode,
        grouping_marks: Vec<BTreeSet<u64>>,
    ) -> Result<(), GroupingMetadataError> {
        self.clean_hash_code();
        self.grouping_metadata = None;
        let metadata = GroupingMetadata::new(mode, grouping_marks)?;
        self.grouping_metadata = Some(metadata);
        Ok(())
    }

    /// Returns the validated grouping metadata installed on this function.
    pub fn grouping_metadata(&self) -> Result<&GroupingMetadata, GroupingMetadataError> {
        self.grouping_metadata
            .as_ref()
            .ok_or(GroupingMetadataError::Uninitialized)
    }

    /// Whether this function has completed the source `SetMetadata` step.
    #[must_use]
    pub fn has_grouping_metadata(&self) -> bool {
        self.grouping_metadata.is_some()
    }

    /// Go `ScalarFunction.Decorrelate`: recursively decorrelate every
    /// argument and invalidate hashes/caches derived from the old tree.
    ///
    /// Expressions are owned values in Rust, so this method rebuilds a clone
    /// rather than mutating an aliased node as Go does.
    #[must_use]
    pub fn decorrelate(&self, schema: Option<&Schema>) -> Self {
        let mut decorrelated = self.clone();
        decorrelated.args = self
            .args
            .iter()
            .map(|argument| argument.decorrelate(schema))
            .collect();
        decorrelated.invalidate_cached_arguments();
        decorrelated
    }

    /// Go `GetStaticType` / `GetType` (which ignores its `EvalContext`).
    #[must_use]
    pub fn get_static_type(&self) -> Option<&FieldType> {
        self.ret_type.as_ref()
    }

    /// Go `GetArgs`.
    #[must_use]
    pub fn get_args(&self) -> &[Expression] {
        &self.args
    }

    /// Go `IsCorrelated`: correlated iff any argument is correlated.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        self.args.iter().any(Expression::is_correlated)
    }

    /// Go `HashCode` (`ReHashCode`), cached on first call:
    /// `[scalarFunctionFlag, EncodeCompactBytes(FuncName.L), arg.HashCode()...]`,
    /// plus, for `cast`, a trailing byte for the target `EvalType`, or, for
    /// `grouping`, the mode, mark count, each mark's size, and sorted keys.
    pub fn hash_code(&mut self) -> &[u8] {
        if !self.hashcode.is_empty() {
            return &self.hashcode;
        }
        self.hashcode.push(SCALAR_FUNCTION_FLAG);
        encode_compact_bytes(&mut self.hashcode, self.func_name.lowercase().as_bytes());
        // Collect the args' hash codes first to avoid overlapping borrows.
        let arg_codes: Vec<Vec<u8>> = self
            .args
            .iter_mut()
            .map(|a| a.hash_code().to_vec())
            .collect();
        for code in arg_codes {
            self.hashcode.extend_from_slice(&code);
        }
        let name = self.func_name.lowercase();
        if name == "values" {
            let offset = self
                .values_offset()
                .map_or(-1, |offset| i64::try_from(offset).unwrap_or(i64::MAX));
            encode_int(&mut self.hashcode, offset);
        }
        // Cast is special: its result type is effectively an argument.
        if name == "cast" {
            if let Some(rt) = &self.ret_type {
                self.hashcode.push(rt.eval_type() as u8);
            }
        }
        if name == "grouping" {
            let metadata = self
                .grouping_metadata
                .as_ref()
                .expect("grouping metadata is not initialized");
            encode_int(&mut self.hashcode, metadata.mode() as u8 as i64);
            encode_int(
                &mut self.hashcode,
                i64::try_from(metadata.grouping_marks().len()).expect("grouping mark count fits"),
            );
            for mark in metadata.grouping_marks() {
                encode_int(
                    &mut self.hashcode,
                    i64::try_from(mark.len()).expect("grouping mark size fits"),
                );
                for key in mark {
                    // Go casts the uint64 key to int64 before EncodeInt;
                    // preserve the same two's-complement bit pattern.
                    encode_int(&mut self.hashcode, *key as i64);
                }
            }
        }
        &self.hashcode
    }

    /// Go `ScalarFunction.CanonicalHashCode` and
    /// `simpleCanonicalizedHashCode`: normalize commutative operators and
    /// equivalent directed comparisons before concatenating child hashes.
    /// The bytes are freshly owned so a rewrite cannot mutate a returned
    /// canonical key through an alias.
    #[must_use]
    pub fn canonical_hash_code(&self) -> Vec<u8> {
        let arg_codes: Vec<Vec<u8>> = self
            .args
            .iter()
            .map(Expression::canonical_hash_code)
            .collect();
        let name = self.func_name.lowercase();
        let mut canonical = vec![SCALAR_FUNCTION_FLAG];

        match name {
            "plus" | "mul" | "eq" | "in" | "or" | "and" => {
                append_canonical_name(&mut canonical, name);
                let mut sorted = arg_codes;
                sorted.sort();
                append_canonical_args(&mut canonical, &sorted);
            }
            "ge" | "le" => {
                append_canonical_name(&mut canonical, "ge");
                if name == "ge" {
                    append_canonical_args(&mut canonical, &arg_codes);
                } else {
                    append_canonical_args_reversed(&mut canonical, &arg_codes);
                }
            }
            "gt" | "lt" => {
                append_canonical_name(&mut canonical, "gt");
                if name == "gt" {
                    append_canonical_args(&mut canonical, &arg_codes);
                } else {
                    append_canonical_args_reversed(&mut canonical, &arg_codes);
                }
            }
            "not" => {
                if let Some(Expression::ScalarFunction(child)) = self.args.first() {
                    let child_args: Vec<Vec<u8>> = child
                        .args
                        .iter()
                        .map(Expression::canonical_hash_code)
                        .collect();
                    match child.func_name.lowercase() {
                        "gt" => {
                            append_canonical_name(&mut canonical, "ge");
                            append_canonical_args_reversed(&mut canonical, &child_args);
                        }
                        "lt" => {
                            append_canonical_name(&mut canonical, "ge");
                            append_canonical_args(&mut canonical, &child_args);
                        }
                        "ge" => {
                            append_canonical_name(&mut canonical, "gt");
                            append_canonical_args_reversed(&mut canonical, &child_args);
                        }
                        "le" => {
                            append_canonical_name(&mut canonical, "gt");
                            append_canonical_args(&mut canonical, &child_args);
                        }
                        // Go's inner switch has no default arm. Preserve its
                        // exact canonical bytes for a scalar child whose
                        // name is not one of the four comparison operators.
                        _ => {}
                    }
                } else {
                    append_canonical_name(&mut canonical, name);
                    append_canonical_args(&mut canonical, &arg_codes);
                }
            }
            _ => {
                append_canonical_name(&mut canonical, name);
                append_canonical_args(&mut canonical, &arg_codes);
                if name == "values" {
                    encode_int(
                        &mut canonical,
                        self.values_offset()
                            .map_or(-1, |offset| i64::try_from(offset).unwrap_or(i64::MAX)),
                    );
                }
                if name == "cast" {
                    if let Some(ret_type) = &self.ret_type {
                        canonical.push(ret_type.eval_type() as u8);
                    }
                }
            }
        }
        canonical
    }

    /// Go `ScalarFunction.Hash64`: hash the function tag, lower-case name,
    /// nullable return type, argument count, and each argument recursively.
    /// This is the structural plan-key hash and intentionally differs from
    /// [`Self::canonical_hash_code`], which normalizes commutative operators.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = crate::column::Fnv64::default();
        SCALAR_FUNCTION_FLAG.hash(&mut hasher);
        self.func_name.lowercase().hash(&mut hasher);
        self.values_offset().hash(&mut hasher);
        match &self.ret_type {
            Some(ret_type) => {
                1_u8.hash(&mut hasher);
                ret_type.hash(&mut hasher);
            }
            None => 0_u8.hash(&mut hasher),
        }
        self.args.len().hash(&mut hasher);
        for argument in &self.args {
            crate::column::expression_hash64(argument).hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Go `ScalarFunction.Equals`: structural equality over name, nullable
    /// return type, and ordered argument trees. Hash caches and collation
    /// metadata are not part of the source method's contract.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self.func_name.lowercase() == other.func_name.lowercase()
            && self.values_offset() == other.values_offset()
            && self.ret_type == other.ret_type
            && self.args.len() == other.args.len()
            && self
                .args
                .iter()
                .zip(&other.args)
                .all(|(left, right)| crate::column::expression_equals(left, right))
    }

    /// Go `ScalarFunction.CleanHashCode` (`scalar_function.go:604`): drops the
    /// cached hash code so the next [`Self::hash_code`] recomputes it.
    ///
    /// Required by every rewrite that mutates [`Self::args`] in place --
    /// `SetExprColumnInOperand` and `ColumnSubstituteImpl`'s grouping arm both
    /// call it in Go. Canonical bytes are derived on demand, so there is no
    /// second cache to clear here.
    pub fn clean_hash_code(&mut self) {
        self.hashcode.clear();
    }

    /// Go `ConstLevel`.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        if is_unfoldable_function(self.func_name.lowercase()) {
            return ConstLevel::NONE;
        }
        self.args
            .iter()
            .map(Expression::const_level)
            .min()
            .unwrap_or(ConstLevel::STRICT)
    }

    /// THE guarantee Go's `ScalarFunction.Eval` provides and this tier must
    /// reproduce: a function's evaluated value is in the EVAL-TYPE FAMILY of
    /// its own declared result type.
    ///
    /// Go gets this by construction. `ScalarFunction.Eval`
    /// (`pkg/expression/scalar_function.go`) does not ask the signature for
    /// "a value" -- it switches on `sf.GetType().EvalType()` and calls
    /// `EvalInt`/`EvalReal`/`EvalDecimal`/`EvalString`/`EvalTime`/
    /// `EvalDuration`/`EvalJSON` accordingly, so the returned datum's kind is
    /// DERIVED from the result type and cannot disagree with it. Downstream,
    /// `chunk.AppendDatum` dispatches on the datum kind while the column's
    /// cell width came from the field type, and only that construction keeps
    /// the two in step.
    ///
    /// This tier evaluates on Datums and dispatches on operand kinds, so the
    /// guarantee is restored here instead, once, for every function -- rather
    /// than at the handful of call sites whose mismatch had been noticed
    /// (`IF`/`CASE`), which left the same defect reachable from every other
    /// one. A value already in the right family is returned untouched, so the
    /// result type's LENGTH constraints are applied only where Go's own
    /// argument cast would have applied them.
    fn coerce_to_ret_type(&self, value: Datum) -> Result<Datum, EvalError> {
        if value.is_null() {
            return Ok(value);
        }
        let Some(ret_type) = self.get_static_type() else {
            return Ok(value);
        };
        // Go's `builtinCoalesceTimeSig`/`builtinCoalesceDurationSig` stamps
        // every selected temporal value with the merged result FSP after
        // evaluating it. `newBaseBuiltinFuncWithTp` does not wrap temporal
        // arguments, so the selected value can still carry the first
        // argument's original precision. Preserve the value's instant and
        // update only that metadata here; a general `ConvertTo` would also
        // round the instant, which is not what Coalesce's `SetFsp` does.
        if self.func_name.lowercase() == "coalesce" {
            let target_fsp = if ret_type.decimal() == tidb_datatype::UNSPECIFIED_LENGTH {
                0
            } else {
                ret_type.decimal()
            };
            match (&value, ret_type.code()) {
                (
                    Datum::Time(time),
                    tidb_datatype::FieldTypeCode::Date
                    | tidb_datatype::FieldTypeCode::Datetime
                    | tidb_datatype::FieldTypeCode::Timestamp,
                ) => {
                    let mut time = *time;
                    let _ = time.set_fsp(target_fsp);
                    return Ok(Datum::Time(time));
                }
                (Datum::Duration(duration), tidb_datatype::FieldTypeCode::Duration) => {
                    return Ok(Datum::Duration(
                        tidb_datatype::MySqlDuration::from_raw_parts(
                            duration.nanoseconds(),
                            target_fsp,
                        ),
                    ));
                }
                _ => {}
            }
        }
        if same_eval_family(&value, ret_type) {
            // `mysql.TypeBit`'s eval type is `ETInt`, so the family check
            // would keep an integer here. A BIT value's canonical carrier is
            // instead the zero-padded byte string (`Datum::Bit`), which is
            // what Go's `chunk.AppendDatum` stores for `KindMysqlBit` and
            // what a var-length BIT chunk column can hold. A cast to BIT is
            // the scalar function that produces such an integer, so convert
            // it here; a value already carrying the bytes is left alone.
            if ret_type.code() == tidb_datatype::FieldTypeCode::Bit {
                let width = (ret_type.flen() > 0)
                    .then(|| u8::try_from((ret_type.flen() + 7) / 8).ok())
                    .flatten()
                    .and_then(|bytes| tidb_datatype::BinaryLiteralWidth::try_from(bytes).ok());
                match value {
                    Datum::Int(v) => {
                        return Ok(Datum::Bit(tidb_datatype::BinaryLiteral::from_uint(
                            v as u64, width,
                        )));
                    }
                    Datum::UInt(v) => {
                        return Ok(Datum::Bit(tidb_datatype::BinaryLiteral::from_uint(
                            v, width,
                        )));
                    }
                    _ => {}
                }
            }
            return Ok(value);
        }
        // COALESCE is built with `newBaseBuiltinFuncWithTp`, so its ARGUMENTS
        // are cast only to the merged eval family -- but `getFunction` then
        // assigns `bf.tp = resultFieldType`, and that merged type is what the
        // selected value is presented as. `select coalesce(1, 2.55, 3)`
        // answers `1.00`, not `1`: the integer branch is widened to the
        // merged scale, exactly as IF/IFNULL/CASE widen theirs. Handled by
        // the shared conversion below rather than a COALESCE-only rule.
        match value.convert_to_in_context(
            ret_type,
            &tidb_datatype::ConversionContext::default_statement_no_warning(),
            &tidb_datatype::SessionTimeZone::utc(),
        ) {
            Ok(converted) if converted.error.is_none() => Ok(converted.value),
            _ => Ok(value),
        }
    }

    /// Whether Go evaluates this call through its `vecEval*` body. A call
    /// whose arguments are all CONSTANT is what Go folds, and folding runs
    /// the ROW body; anything reading a column runs the vectorized one.
    fn runs_vectorized_body(&self) -> bool {
        !self
            .args
            .iter()
            .all(|arg| matches!(arg, Expression::Constant(_)))
    }

    /// The collation this function's own evaluation runs under (Go's
    /// `baseBuiltinFunc.collator`, set from `ExprCollation.Collation`): the
    /// derivation's collation, which a non-string result does not carry on
    /// its type -- `b < 'x'` over a gbk column compares under
    /// `gbk_chinese_ci` and is a binary-charset integer.
    ///
    /// A node built outside the rewriter carries no derived collation; it
    /// reads its result type, and otherwise falls back to the connection
    /// collation the rest of the tier uses.
    #[must_use]
    pub fn derived_collation(&self) -> tidb_charset::Collation {
        let (_, derived) = self.collation.charset_and_collation();
        tidb_charset::Collation::from_name(derived)
            .or_else(|| {
                self.ret_type
                    .as_ref()
                    .and_then(|ft| tidb_charset::Collation::from_name(ft.collation_name()))
            })
            .unwrap_or(crate::ops::DERIVATION_FREE_COLLATION)
    }

    /// Builds Go `builtinInStringSig.buildHashMapForConstArgs` for the strict
    /// string literals this expression model can evaluate without a statement
    /// context. Other constant families remain in the row path because their
    /// implicit casts can raise warnings or errors.
    pub(crate) fn prepare_in_string_hash_set(&mut self) {
        if self.func_name.lowercase() != "in"
            || self.args.len() < 2
            || self
                .args
                .first()
                .and_then(Expression::static_type)
                .is_none_or(|field_type| field_type.eval_type() != tidb_datatype::EvalType::String)
        {
            return;
        }
        let collator = tidb_datatype::get_collator(self.derived_collation().name());
        // Go's `pkg/expression/builtin_other.go::builtinInStringSig` keeps
        // these immutable literal keys in a per-function map and probes it
        // for every row. Reserve the complete
        // literal-list capacity up front, matching the source map's intended
        // read-mostly shape without changing its collision-resistant hasher or
        // any membership/collation semantics.
        let mut hash_set =
            std::collections::HashSet::with_capacity(self.args.len().saturating_sub(1));
        let mut non_const_args = Vec::new();
        let mut has_null = false;
        for (index, argument) in self.args.iter().enumerate().skip(1) {
            let Expression::Constant(constant) = argument else {
                non_const_args.push(index);
                continue;
            };
            if constant.const_level() != crate::expression::ConstLevel::STRICT {
                non_const_args.push(index);
                continue;
            }
            match &constant.value {
                Datum::String(value) => {
                    hash_set.insert(collator.key(value.bytes()));
                }
                Datum::Bytes(value) => {
                    hash_set.insert(collator.key(value));
                }
                Datum::Null => has_null = true,
                _ => non_const_args.push(index),
            }
        }
        self.in_string_hash_set = Some(hash_set);
        self.in_string_non_const_args = non_const_args;
        self.in_string_has_null = has_null;
    }

    /// The user-variable name a `getvar`/`setvar` call carries in its first
    /// argument, which the rewriter built as a constant because the name is a
    /// build-time token rather than a value.
    fn uservar_name(&self) -> Option<String> {
        let Expression::Constant(name) = self.args.first()? else {
            return None;
        };
        name.value.sql_string().ok()
    }

    /// Go `ScalarFunction.Eval`: evaluate the function against one row, and
    /// return a value in the eval-type family of the function's own result
    /// type -- see [`Self::coerce_to_ret_type`] for why that is the whole
    /// point of Go's `Eval` and not an afterthought.
    pub fn eval(&self, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
        if let Some(builtin) = self.pb_builtin {
            let value = builtin.eval(self, ctx, row)?;
            // Go ScalarFunction.Eval interprets an evalInt carrier through
            // the wire result type, independently of the argument flags.
            let value = match (value, self.get_static_type()) {
                (Datum::UInt(value), Some(field)) if !field.is_unsigned() => {
                    Datum::Int(value as i64)
                }
                (Datum::Int(value), Some(field)) if field.is_unsigned() => {
                    Datum::UInt(value as u64)
                }
                (value, _) => value,
            };
            return self.coerce_to_ret_type(value);
        }
        // Go has no function without the signature its class returned.
        Err(EvalError::Unsupported("this scalar function has no signature"))
    }

    fn eval_like(
        &self,
        ctx: &dyn Columns,
        row: Row<'_>,
        case_insensitive: bool,
    ) -> Result<Datum, EvalError> {
        if self.args.len() != 3 {
            return Err(EvalError::Unsupported("LIKE arity"));
        }
        let value = self.args[0].eval(ctx, row)?;
        if value.is_null() {
            return Ok(Datum::Null);
        }
        let text = value
            .sql_bytes()
            .map_err(|_| EvalError::Unsupported("invalid LIKE operand scalar domain"))?;
        let pattern = self.args[1].eval(ctx, row)?;
        if pattern.is_null() {
            return Ok(Datum::Null);
        }
        let pattern = pattern
            .sql_bytes()
            .map_err(|_| EvalError::Unsupported("invalid LIKE pattern scalar domain"))?;
        let raw_escape = self.args[2].eval(ctx, row)?;
        let int_escape =
            crate::cast::cast_arg_as_int(&raw_escape, self.args[2].static_type(), ctx)?;
        let Some(escape) = crate::arg_eval_type::eval_int(&int_escape)? else {
            return Ok(Datum::Null);
        };
        let escape = escape as u8;
        let cache_pattern = self.args[1].const_level() >= ConstLevel::ONLY_IN_CONTEXT;
        let cache_escape = self.args[2].const_level() >= ConstLevel::ONLY_IN_CONTEXT;
        let matched = if case_insensitive {
            if cache_pattern && cache_escape {
                let cached =
                    self.ilike_pattern_cache
                        .get_or_init_cache(ctx.context_id(), || {
                            Ok::<_, EvalError>(crate::like::CompiledIlikePattern::new(
                                &pattern,
                                escape,
                                self.derived_collation(),
                            ))
                        })?;
                cached.is_match(&text)
            } else {
                crate::like::ilike_match_with_collation(
                    text,
                    pattern,
                    escape,
                    self.derived_collation(),
                )
            }
        } else {
            if cache_pattern && cache_escape {
                let cached = self
                    .like_pattern_cache
                    .get_or_init_cache(ctx.context_id(), || {
                        Ok::<_, EvalError>(crate::like::CompiledLikePattern::new(
                            &pattern,
                            escape,
                            self.derived_collation(),
                        ))
                    })?;
                cached.is_match(&text)
            } else {
                crate::like_match_with_collation(
                    text,
                    pattern,
                    Some(escape),
                    self.derived_collation(),
                )
            }
        };
        return Ok(Datum::Int(i64::from(matched)));
    }

    fn eval_date_arithmetic(
        &self,
        unit: &str,
        subtract: bool,
        ctx: &dyn Columns,
        row: Row<'_>,
    ) -> Result<Datum, EvalError> {
        let result_fsp = crate::time_fn::calendar::date_add_result_fsp(
            unit,
            self.args[0].static_type(),
            self.args[1].static_type(),
        );
        let mut date = self.args[0].eval(ctx, row)?;
        if date.is_null() {
            return Ok(Datum::Null);
        }
        let mut amount = self.args[1].eval(ctx, row)?;
        if amount.is_null() {
            return Ok(Datum::Null);
        }
        // Go `getIntervalFromReal`: a REAL amount is formatted with its
        // argument type's scale before the unit parses it
        // (`strconv.FormatFloat(v, 'f', decimal, 64)`), so `INTERVAL b
        // MINUTE_SECOND` over a DOUBLE(6,3) 1.1 reads "1.100".
        if let (Datum::Real(value) | Datum::Float32(value), true) =
            (&amount, crate::time_fn::calendar::is_composite_unit(unit))
        {
            let decimal = self.args[1].static_type().map_or(
                tidb_datatype::UNSPECIFIED_LENGTH,
                tidb_datatype::FieldType::decimal,
            );
            let prec = i32::try_from(decimal).unwrap_or(-1);
            amount = Datum::new_string_datum(tidb_goruntime::go_strconv::format_float(
                *value, b'f', prec, 64,
            ));
        }
        let result_type = self.get_static_type();
        if result_type
            .is_some_and(|field_type| field_type.code() == tidb_datatype::FieldTypeCode::Duration)
        {
            return crate::time_fn::add_sub::date_add_duration(
                ctx,
                unit,
                &date,
                &amount,
                self.args[1].static_type(),
                if subtract { -1 } else { 1 },
                i64::from(result_fsp.unwrap_or(0)),
            );
        }
        if result_type
            .is_some_and(|field_type| field_type.code() == tidb_datatype::FieldTypeCode::Datetime)
            && self.args[0].static_type().is_some_and(|field_type| {
                field_type.code() == tidb_datatype::FieldTypeCode::Duration
            })
        {
            date = crate::cast::cast_arg_as_datetime(&date, self.args[0].static_type(), ctx)?;
        }
        let result = crate::time_fn::calendar::date_add_with_result_fsp(
            unit,
            &date,
            &amount,
            if subtract { -1 } else { 1 },
            result_fsp,
            ctx,
        )?;
        let Some(result_type) = result_type else {
            return Ok(result);
        };
        return match result_type.code() {
            tidb_datatype::FieldTypeCode::Date => crate::cast::parse_computed_time(
                &result,
                ctx,
                tidb_datatype::TimeType::Date,
                Some(0),
            ),
            tidb_datatype::FieldTypeCode::Datetime => crate::cast::parse_computed_time(
                &result,
                ctx,
                tidb_datatype::TimeType::DateTime,
                Some(result_type.decimal()),
            ),
            _ => Ok(result),
        };
    }

    /// The argument evaluation domain selected by Go's arithmetic builder.
    /// DIV returns Int but consumes Decimal unless both arguments are Int.
    fn numeric_operand_domain(&self) -> Option<EvalType> {
        self.pb_builtin?.numeric_domain()
    }

    fn eval_binary_values(
        &self,
        op: BinaryOp,
        lhs: Datum,
        rhs: Datum,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        let signed = [0, 1].map(|index| {
            self.args[index]
                .static_type()
                .is_some_and(|field| !field.is_unsigned())
        });
        let (lhs, rhs) = crate::binary_literal::cast_signed_literal_operands(op, lhs, rhs, signed);
        if op == BinaryOp::IntDiv {
            if let (Datum::Decimal(left), Datum::Decimal(right)) = (&lhs, &rhs) {
                let unsigned = self
                    .args
                    .iter()
                    .any(|arg| arg.static_type().is_some_and(FieldType::is_unsigned));
                return crate::ops::decimal_integer_division(left, right, unsigned, ctx).map_err(
                    |error| {
                        if !matches!(error, EvalError::IntOverflow) {
                            return error;
                        }
                        match arithmetic_overflow_expression(self, op, ctx) {
                            Some(expression) => EvalError::DataOutOfRange {
                                value: if unsigned {
                                    "BIGINT UNSIGNED"
                                } else {
                                    "BIGINT"
                                },
                                expression,
                            },
                            None => error,
                        }
                    },
                );
            }
        }
        let integer_pair =
            if op == BinaryOp::IntDiv && self.numeric_operand_domain() == Some(EvalType::Int) {
                let integer = |value: &Datum, signed| match value {
                    Datum::Int(value) if signed => Some(crate::coerce::Integer::Signed(*value)),
                    Datum::Int(value) => Some(crate::coerce::Integer::Unsigned(*value as u64)),
                    Datum::UInt(value) => Some(crate::coerce::Integer::Unsigned(*value)),
                    _ => None,
                };
                integer(&lhs, signed[0]).zip(integer(&rhs, signed[1]))
            } else {
                None
            };
        let result = crate::ops::eval_binary_full(
            op,
            lhs,
            rhs,
            ctx.div_precision_increment(),
            self.derived_collation(),
            crate::ops::Operands::of(&self.args[0], &self.args[1]),
            ctx,
        )
        .map_err(|error| match error {
            EvalError::IntOverflow => integer_pair.map_or_else(
                || arithmetic_overflow_error(self, op, ctx),
                |(left, right)| integer_division_overflow(left, right),
            ),
            EvalError::FloatOverflow => real_arithmetic_overflow_error(self, op, ctx),
            EvalError::DecimalOverflow => decimal_arithmetic_overflow_error(self, op, ctx),
            other => other,
        })?;
        Ok(self.round_quotient_to_result_scale(op, result))
    }

    /// Go `builtinArithmeticDivideDecimalSig`: a quotient storing fewer
    /// fraction digits than the result type declares is rounded up to it,
    /// which is how a scale `PropagateType` widened (a DECIMAL division under
    /// a cast to DOUBLE) keeps every stored digit.
    fn round_quotient_to_result_scale(&self, op: BinaryOp, result: Datum) -> Datum {
        if op != BinaryOp::Div {
            return result;
        }
        let Datum::Decimal(quotient) = &result else {
            return result;
        };
        let Some(target) = self
            .get_static_type()
            .map(FieldType::decimal)
            .filter(|&decimal| decimal != tidb_datatype::UNSPECIFIED_LENGTH)
        else {
            return result;
        };
        let (_, fraction) = quotient.precision_and_frac();
        if i64::from(fraction) < target {
            return Datum::Decimal(quotient.round_to_scale(target as i32));
        }
        result
    }

    /// Go `regexpBaseFuncSig`'s positional family (`REGEXP_SUBSTR`,
    /// `REGEXP_INSTR`, `REGEXP_REPLACE`) over already-evaluated operands. The
    /// derived collation seeds the match type (`_ci` compiles case
    /// insensitively unless a later `c` flag says otherwise), and a pattern or
    /// replacement constant for the statement is compiled once.
    pub(crate) fn eval_regexp_family(
        &self,
        upper: &str,
        vals: &[Datum],
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        let cache_pattern = self
            .args
            .get(1)
            .is_some_and(|argument| argument.const_level() >= ConstLevel::ONLY_IN_CONTEXT);
        let match_type_index = match upper {
            "REGEXP_SUBSTR" if self.args.len() == 5 => Some(4),
            "REGEXP_INSTR" if self.args.len() == 6 => Some(5),
            "REGEXP_REPLACE" if self.args.len() == 6 => Some(5),
            _ => None,
        };
        let cache_match_type = match_type_index
            .is_none_or(|index| self.args[index].const_level() >= ConstLevel::ONLY_IN_CONTEXT);
        let cache_replacement = upper == "REGEXP_REPLACE"
            && self
                .args
                .get(2)
                .is_some_and(|argument| argument.const_level() >= ConstLevel::ONLY_IN_CONTEXT);
        crate::builtin_ext::regexp::dispatch_with_cache(
            upper,
            vals,
            ctx.context_id(),
            cache_pattern && cache_match_type,
            cache_replacement,
            self.derived_collation(),
            &self.regexp_cache,
            &self.regexp_replace_instruction_cache,
        )
        .expect("the native regexp family is registered")
    }

    fn eval_regexp_like(&self, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
        if !matches!(self.args.len(), 2 | 3) {
            return Err(EvalError::WrongParameterCount("regexp_like"));
        }
        let string_arg = |index: usize| -> Result<Option<String>, EvalError> {
            let value = self.args[index].eval(ctx, row)?;
            let value =
                crate::cast::cast_arg_as_string(&value, self.args[index].static_type(), ctx)?;
            if value.is_null() {
                return Ok(None);
            }
            let bytes = value
                .sql_bytes()
                .map_err(|_| EvalError::Unsupported("invalid REGEXP_LIKE argument"))?;
            if index == 0 {
                Ok(Some(
                    tidb_goruntime::GoString::from(bytes).to_utf8_lossy_go(),
                ))
            } else {
                // Go hands the raw bytes to `regexp.Compile`, which refuses
                // invalid UTF-8 in a pattern; a match type outside `cimsu`
                // (any invalid byte is) is `Invalid match type`.
                String::from_utf8(bytes).map(Some).map_err(|error| {
                    if index == 1 {
                        crate::regexp::err_regexp(&format!(
                            "error parsing regexp: invalid UTF-8: `{}`",
                            String::from_utf8_lossy(error.as_bytes())
                        ))
                    } else {
                        crate::regexp::err_regexp("Invalid match type")
                    }
                })
            }
        };
        let Some(text) = string_arg(0)? else {
            return Ok(Datum::Null);
        };
        let Some(pattern) = string_arg(1)? else {
            return Ok(Datum::Null);
        };
        let match_type = if self.args.len() == 3 {
            let Some(match_type) = string_arg(2)? else {
                return Ok(Datum::Null);
            };
            match_type
        } else {
            String::new()
        };
        let match_type =
            crate::regexp::regexp_match_type_with_collation(&match_type, self.derived_collation());
        let cache_pattern = self.args[1].const_level() >= ConstLevel::ONLY_IN_CONTEXT;
        let cache_match_type =
            self.args.len() < 3 || self.args[2].const_level() >= ConstLevel::ONLY_IN_CONTEXT;
        let regex = crate::regexp::get_cached_regexp(
            &self.regexp_cache,
            ctx.context_id(),
            cache_pattern && cache_match_type,
            &pattern,
            &match_type,
        )?;
        let matched = regex.is_match(&text);
        return Ok(Datum::Int(i64::from(matched)));
    }

    /// The per-signature evaluation itself.
    ///
    /// The binary operators, the lazy control forms (`CASE`/`IF`), `LIKE`,
    /// the collation-aware string builtins, the casts, the session-state
    /// functions and the date/time family are each handled by name below;
    /// everything else evaluates its arguments eagerly and reuses the shared
    /// Datum-level implementations. Each returns whatever its operand kinds
    /// produced; reconciling that with the declared result type is
    /// [`Self::eval`]'s job, done once for all of them.
    /// Go `builtinGreatest*Sig` / `builtinLeast*Sig` over this row's
    /// argument values, under the signature the arguments select.
    fn extremum_of(
        &self,
        greatest: bool,
        vals: &[Datum],
        collation: tidb_charset::Collation,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        let want = if greatest {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        };
        // Go casts every argument to DECIMAL at that argument's
        // OWN decimal (integers at 0), returns the winner raw,
        // and folds a fully-constant call to the return type's
        // max-argument scale. The per-argument decimals and the
        // constness are what the chunk evaluator knows that the
        // values alone cannot say.
        let arg_decimals: Vec<i64> = self
            .args
            .iter()
            .map(|a| {
                a.static_type()
                    .map_or(tidb_datatype::UNSPECIFIED_LENGTH, |ft| {
                        // Go `WrapWithCastAsDecimal` pins integer
                        // arguments to scale 0 regardless of the
                        // field type's own (often unspecified) value.
                        if ft.eval_type() == tidb_datatype::EvalType::Int {
                            0
                        } else {
                            ft.decimal()
                        }
                    })
            })
            .collect();
        let all_constant = self
            .args
            .iter()
            .all(|a| a.const_level() == crate::expression::ConstLevel::STRICT);
        crate::builtin_ext::extremum_with_signature(
            vals,
            want,
            self.extremum_signature(),
            &arg_decimals,
            all_constant,
            collation,
            ctx,
        )
    }

    /// Go `SetFTSMysqlMatchAgainstModifier`: the planner's call right after
    /// it builds the internal `MATCH ... AGAINST` function.
    pub fn set_fts_mysql_match_against_modifier(
        &mut self,
        modifier: tidb_ast::MatchModifier,
    ) -> Result<(), EvalError> {
        let signature = self.pb_signature();
        if signature != Some(tidb_proto::tipb::ScalarFuncSig::FtsMatchExpression) {
            return Err(EvalError::Raw(format!(
                "unexpected builtin signature for match_against: {signature:?}"
            )));
        }
        self.fts_modifier = modifier;
        Ok(())
    }

    /// Go `builtinFtsMysqlMatchAgainstSig.modifier`, or `None` for a node of
    /// another signature.
    #[must_use]
    pub fn fts_mysql_match_against_modifier(&self) -> Option<tidb_ast::MatchModifier> {
        (self.pb_signature() == Some(tidb_proto::tipb::ScalarFuncSig::FtsMatchExpression))
            .then_some(self.fts_modifier)
    }

    /// The GREATEST/LEAST signature: the one chosen at construction, else
    /// the one the arguments select.
    pub(crate) fn extremum_signature(&self) -> Option<crate::builtin_ext::GlSignature> {
        self.gl_signature
            .or_else(|| self.pb_builtin.as_ref().and_then(|builtin| crate::builtin_ext::GlSignature::of_signature(builtin.signature())))
            .or_else(|| crate::rewriter::result_type::gl_signature(&self.args))
    }

    /// Go's string `GREATEST`/`LEAST` `vecEvalString` for one row: the
    /// call's value, the byte length of each argument's string (`None` for
    /// NULL), which an argument's `VecEvalString` appends into `result`, and
    /// of the running extremum after each argument (`None` once one is
    /// NULL), which the ping-pong appends at its even steps. The arguments
    /// are evaluated once, as Go's kernel evaluates each column once.
    pub(crate) fn eval_string_extremum_steps(
        &self,
        ctx: &dyn Columns,
        row: Row<'_>,
    ) -> Result<(Datum, Vec<Option<usize>>, Vec<Option<usize>>), EvalError> {
        let vals: Vec<Datum> = self
            .args
            .iter()
            .map(|a| a.eval(ctx, row))
            .collect::<Result<_, _>>()?;
        let collation = self.derived_collation();
        let greatest = self.func_name.lowercase() == "greatest";
        let want = if greatest {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        };
        let lengths = crate::builtin_ext::compare2::string_lengths(&vals)?;
        let steps = crate::builtin_ext::compare2::string_extremum_steps(&vals, want, collation)?;
        let value = self.extremum_of(greatest, &vals, collation, ctx)?;
        Ok((self.coerce_to_ret_type(value)?, lengths, steps))
    }

}

/// Go typed GETVAR evaluation reads the live value through its already selected
/// signature. Integer signatures read the carrier; decimal/real/string signatures
/// apply their source conversions independently of declared display width.
fn uservar_as_kind(kind: &str, value: Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_ast::CastType;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    // Go builtinGetIntVarSig returns Datum.GetInt64 verbatim; unsignedness
    // belongs to the compiled result type, independently of the runtime kind.
    if kind == "int" {
        return Ok(Datum::Int(value.get_int64()));
    }
    if kind == "uint" {
        return Ok(Datum::UInt(value.get_int64() as u64));
    }
    let target = match (kind, &value) {
        ("real", Datum::Real(_)) => return Ok(value),
        ("decimal", Datum::Decimal(_)) => return Ok(value),
        ("time", Datum::Time(_)) => return Ok(value),
        ("string", Datum::String(_) | Datum::Bytes(_)) => return Ok(value),
        ("real", _) => CastType::Double,
        ("decimal", _) => {
            // Go GetDecimalVar uses Datum.ToDecimal, without CAST's width/scale rounding.
            let warnings = crate::constant::ConversionWarnings(ctx);
            let zone = ctx.time_zone();
            let context = tidb_datatype::ConversionContext::new(
                ctx.type_flags(),
                tidb_datatype::ConversionLocation::from_time_zone(&zone),
                &warnings,
            );
            let (decimal, error) = value
                .to_decimal_with_context(&context)
                .map_err(|_| EvalError::Unsupported("user variable decimal conversion"))?;
            if let Some(error) = error {
                return Err(EvalError::Conversion(error));
            }
            return Ok(Datum::Decimal(decimal));
        }
        ("time", _) => return crate::cast::cast_arg_as_datetime(&value, None, ctx),
        ("string", _) => CastType::Char {
            len: None,
            charset: None,
        },
        _ => return Err(EvalError::Unsupported("unknown user-variable kind")),
    };
    // No session resolver is threaded here on purpose: every target above is
    // numeric or string, so no arm reads the date modes or raises a warning.
    // Every target above is numeric or string, so no arm reads the
    // source type.
    crate::cast::eval_cast(&target, value, None, ctx)
}

/// One numeric value of a column-wise comparison, in the comparison's
/// evaluation family (Go `getBaseCmpType`: `ETInt` when both sides are
/// integers, `ETDecimal` when a decimal meets an integer or a decimal).
#[derive(Clone, Copy)]
enum CompareValue {
    Int(crate::coerce::Integer),
    Decimal(tidb_datatype::MyDecimal),
}

impl CompareValue {
    fn cmp(self, other: Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => crate::coerce::integer_cmp(a, b),
            (a, b) => a.to_my_decimal().compare(&b.to_my_decimal()),
        }
    }

    fn to_my_decimal(self) -> tidb_datatype::MyDecimal {
        match self {
            Self::Int(crate::coerce::Integer::Signed(value)) => {
                tidb_datatype::MyDecimal::from_int(value)
            }
            Self::Int(crate::coerce::Integer::Unsigned(value)) => {
                tidb_datatype::MyDecimal::from_uint(value)
            }
            Self::Decimal(value) => value,
        }
    }
}

/// One argument of a numeric comparison as Go's `VecEvalInt`/`VecEvalDecimal`
/// reads it: a chunk column's 8-byte integer or 40-byte decimal cells, or
/// one strict constant.
enum CompareOperand<'a> {
    IntColumn {
        column: tidb_chunk::ColumnRead<'a>,
        signed: bool,
    },
    DecimalColumn(tidb_chunk::ColumnRead<'a>),
    Constant(Option<CompareValue>),
}

impl<'a> CompareOperand<'a> {
    /// Returns the operand and whether it is in the decimal family.
    fn of(expression: &Expression, input: &'a Chunk) -> Option<(Self, bool)> {
        use tidb_datatype::FieldTypeCode;
        let field_type = expression.static_type()?;
        // Only the integer codes store the value as the 8-byte cell
        // `GetInt64` reads; BIT and the ENUM/SET-as-int forms (Go's hybrid
        // types) keep the row path.
        let is_int = matches!(
            field_type.code(),
            FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
                | FieldTypeCode::Year
        );
        let is_decimal = field_type.code() == FieldTypeCode::NewDecimal;
        if !is_int && !is_decimal {
            return None;
        }
        // Go `mysql.HasUnsignedFlag(args[i].GetType(ctx).GetFlag())`: the
        // argument's field type decides how its int64 bits are read.
        let signed = !field_type.is_unsigned();
        match expression {
            Expression::Column(column) => {
                let index = usize::try_from(column.index).ok()?;
                if index >= input.num_cols() {
                    return None;
                }
                let column = input.column(index);
                if is_int && column.type_size() == 8 {
                    Some((Self::IntColumn { column, signed }, false))
                } else if is_decimal
                    && column.type_size() == tidb_chunk::column::MY_DECIMAL_STRUCT_SIZE
                {
                    Some((Self::DecimalColumn(column), true))
                } else {
                    None
                }
            }
            Expression::Constant(constant) => {
                let value = match constant.literal_value()? {
                    Datum::Null => None,
                    Datum::Int(value) if is_int => Some(CompareValue::Int(if signed {
                        crate::coerce::Integer::Signed(*value)
                    } else {
                        crate::coerce::Integer::Unsigned(*value as u64)
                    })),
                    Datum::UInt(value) if is_int => {
                        Some(CompareValue::Int(crate::coerce::Integer::Unsigned(*value)))
                    }
                    Datum::Decimal(value) if is_decimal => {
                        Some(CompareValue::Decimal(value.to_my_decimal().ok()?))
                    }
                    _ => return None,
                };
                Some((Self::Constant(value), is_decimal))
            }
            _ => None,
        }
    }

    fn get(&self, physical: usize) -> Option<CompareValue> {
        match self {
            Self::IntColumn { column, signed } => {
                if column.is_null(physical) {
                    return None;
                }
                let bits = column.get_int64(physical);
                Some(CompareValue::Int(if *signed {
                    crate::coerce::Integer::Signed(bits)
                } else {
                    crate::coerce::Integer::Unsigned(bits as u64)
                }))
            }
            Self::DecimalColumn(column) => {
                if column.is_null(physical) {
                    return None;
                }
                Some(CompareValue::Decimal(column.get_my_decimal(physical)))
            }
            Self::Constant(value) => *value,
        }
    }
}

impl ScalarFunction {
    /// Go `builtin{EQ,NE,LT,LE,GT,GE}{Int,Decimal}Sig.vecEvalInt`: a numeric
    /// comparison over the live physical rows `sel` of `input`, column-wise.
    ///
    /// `is_zero` receives one entry per row of `sel` in Go's `VecEvalBool`
    /// encoding (`-1` NULL, `0` false, `1` true). Returns `Ok(false)`, with
    /// `is_zero` untouched, for every shape this kernel does not cover -- an
    /// argument that is neither an integer/decimal column nor a strict
    /// integer/decimal constant, or a result type the row evaluator would
    /// still convert -- so the caller keeps the row evaluator as the
    /// behavior contract there. The covered shapes produce exactly what
    /// [`Self::eval`] produces row by row: NULL when either side is NULL,
    /// the signedness of each integer side read from its argument's field
    /// type (`ops::integer_binary_typed`), and an integer meeting a decimal
    /// compared exactly in the decimal domain (`ops::decimal_binary`).
    pub(crate) fn vec_eval_numeric_compare(
        &self,
        input: &Chunk,
        sel: &[usize],
        is_zero: &mut Vec<i8>,
    ) -> Result<bool, EvalError> {
        if self.args.len() != 2 {
            return Ok(false);
        }
        let op = match binary_op_for_name(self.func_name.lowercase()) {
            Some(
                op @ (BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge),
            ) => op,
            _ => return Ok(false),
        };
        // `coerce_to_ret_type` leaves an integer alone only for an integer
        // result type that is not BIT.
        let Some(ret_type) = self.get_static_type() else {
            return Ok(false);
        };
        if ret_type.eval_type() != EvalType::Int
            || ret_type.code() == tidb_datatype::FieldTypeCode::Bit
        {
            return Ok(false);
        }
        let (Some((lhs, _)), Some((rhs, _))) = (
            CompareOperand::of(&self.args[0], input),
            CompareOperand::of(&self.args[1], input),
        ) else {
            return Ok(false);
        };
        is_zero.clear();
        is_zero.reserve(sel.len());
        for &physical in sel {
            let (Some(a), Some(b)) = (lhs.get(physical), rhs.get(physical)) else {
                is_zero.push(-1);
                continue;
            };
            let ordering = a.cmp(b);
            let truth = match op {
                BinaryOp::Eq => ordering.is_eq(),
                BinaryOp::Ne => !ordering.is_eq(),
                BinaryOp::Lt => ordering.is_lt(),
                BinaryOp::Le => ordering.is_le(),
                BinaryOp::Gt => ordering.is_gt(),
                _ => ordering.is_ge(),
            };
            is_zero.push(i8::from(truth));
        }
        Ok(true)
    }

    /// Go `VecEvalBool`'s column-wise leg over one filter node: the numeric
    /// comparisons of [`Self::vec_eval_numeric_compare`], `NOT` over a node
    /// this kernel covers (`builtinUnaryNotIntSig.vecEvalInt`: NULL stays
    /// NULL, zero becomes one, anything else zero) and `IS NULL` over a
    /// chunk column (`builtin*IsNullSig.vecEvalInt`: the column's null
    /// bitmap, never NULL itself). `is_zero` and the `Ok(false)` contract are
    /// those of the comparison kernel; the covered shapes produce what
    /// [`Self::eval`] produces row by row (`ops::eval_unary`'s three-valued
    /// NOT and the `ISNULL` value dispatch).
    pub(crate) fn vec_eval_bool(
        &self,
        input: &Chunk,
        sel: &[usize],
        is_zero: &mut Vec<i8>,
    ) -> Result<bool, EvalError> {
        let name = self.func_name.lowercase();
        if name != "not" && name != "isnull" {
            return self.vec_eval_numeric_compare(input, sel, is_zero);
        }
        // `coerce_to_ret_type` leaves an integer alone only for an integer
        // result type that is not BIT.
        let Some(ret_type) = self.get_static_type() else {
            return Ok(false);
        };
        if ret_type.eval_type() != EvalType::Int
            || ret_type.code() == tidb_datatype::FieldTypeCode::Bit
        {
            return Ok(false);
        }
        match (name, self.args.as_slice()) {
            ("not", [Expression::ScalarFunction(argument)]) => {
                if !argument.vec_eval_bool(input, sel, is_zero)? {
                    return Ok(false);
                }
                for code in is_zero.iter_mut() {
                    if *code >= 0 {
                        *code = i8::from(*code == 0);
                    }
                }
                Ok(true)
            }
            ("isnull", [Expression::Column(column)]) => {
                let Some(index) = usize::try_from(column.index)
                    .ok()
                    .filter(|&index| index < input.num_cols())
                else {
                    return Ok(false);
                };
                let column = input.column(index);
                is_zero.clear();
                is_zero.extend(
                    sel.iter()
                        .map(|&physical| i8::from(column.is_null(physical))),
                );
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// One decimal value of a column-wise arithmetic evaluation, in the shape
/// the value-layer `Decimal` fast paths compute in: a signed `i128`
/// coefficient at `storage_scale` fraction digits, of which `scale` are the
/// SQL-visible result scale.
#[derive(Clone, Copy)]
struct DecimalValue {
    coefficient: i128,
    storage_scale: u32,
    scale: u32,
}

impl DecimalValue {
    fn from_my_decimal(value: &tidb_datatype::MyDecimal) -> Option<Self> {
        // `Decimal::from_my_decimal`: the stored fraction digits are padded
        // up to the result scale when the cell declares more of them.
        let (coefficient, digits_frac) = value.to_i128_scaled()?;
        let result_frac = u32::try_from(value.result_frac()).ok()?;
        let storage_scale = digits_frac.max(result_frac);
        let coefficient = if storage_scale > digits_frac {
            coefficient.checked_mul(10i128.checked_pow(storage_scale - digits_frac)?)?
        } else {
            coefficient
        };
        Some(Self {
            coefficient,
            storage_scale,
            scale: result_frac,
        })
    }

    fn from_integer(value: crate::coerce::Integer) -> Self {
        let coefficient = match value {
            crate::coerce::Integer::Signed(value) => i128::from(value),
            crate::coerce::Integer::Unsigned(value) => i128::from(value),
        };
        Self {
            coefficient,
            storage_scale: 0,
            scale: 0,
        }
    }

    /// `Decimal::add` (`try_add_fast`): operands aligned to the wider storage
    /// scale, the visible scale the wider of the two.
    fn add(self, other: Self) -> Option<Self> {
        let storage_scale = self.storage_scale.max(other.storage_scale);
        let left = self.aligned(storage_scale)?;
        let right = other.aligned(storage_scale)?;
        Some(Self {
            coefficient: left.checked_add(right)?,
            storage_scale,
            scale: self.scale.max(other.scale),
        })
    }

    fn sub(self, other: Self) -> Option<Self> {
        self.add(Self {
            coefficient: other.coefficient.checked_neg()?,
            ..other
        })
    }

    /// `Decimal::mul_mysql` (`try_mul_mysql_fast`): scales add; a result
    /// scale past MySQL's 30 takes the general path.
    fn mul(self, other: Self) -> Option<Self> {
        let scale = self.scale.checked_add(other.scale)?;
        if scale > 30 {
            return None;
        }
        Some(Self {
            coefficient: self.coefficient.checked_mul(other.coefficient)?,
            storage_scale: self.storage_scale.checked_add(other.storage_scale)?,
            scale,
        })
    }

    fn aligned(self, storage_scale: u32) -> Option<i128> {
        if storage_scale == self.storage_scale {
            Some(self.coefficient)
        } else {
            self.coefficient
                .checked_mul(10i128.checked_pow(storage_scale - self.storage_scale)?)
        }
    }

    /// `Decimal::to_chunk_my_decimal`: the cell the row path appends.
    fn to_my_decimal(self) -> Option<tidb_datatype::MyDecimal> {
        tidb_datatype::MyDecimal::from_scaled_i128(self.coefficient, self.storage_scale, self.scale)
    }
}

/// Evaluates the Go numeric typed entrypoint without changing generic Eval.
pub fn eval_numeric_row(
    expression: &Expression,
    ctx: &dyn Columns,
    row: Row<'_>,
    target: EvalType,
) -> Result<Datum, EvalError> {
    let value = match expression {
        Expression::Constant(constant) => constant.eval_typed_as_on_row(ctx, row, target)?,
        Expression::CorrelatedColumn(column) => column.eval_typed(ctx, target)?,
        _ => {
            let value = expression.eval(ctx, row)?;
            if target == EvalType::Int
                && !value.is_null()
                && expression.static_type().is_some_and(FieldType::is_hybrid)
            {
                if let Datum::Bit(value) = value {
                    let warnings = crate::constant::ConversionWarnings(ctx);
                    let zone = ctx.time_zone();
                    let context = tidb_datatype::ConversionContext::new(
                        ctx.type_flags(),
                        tidb_datatype::ConversionLocation::from_time_zone(&zone),
                        &warnings,
                    );
                    let (integer, error) = value.to_int_with_context(&context);
                    if let Some(error) = error {
                        return Err(EvalError::Conversion(error));
                    }
                    Datum::Int(integer as i64)
                } else {
                    convert_numeric_datum(
                        value,
                        &FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        ctx,
                    )?
                }
            } else {
                value
            }
        }
    };
    Ok(match (target, value) {
        (EvalType::Int, Datum::UInt(value)) => Datum::Int(value as i64),
        (EvalType::Real, Datum::Float32(value)) => Datum::Real(value),
        (_, value) => value,
    })
}

/// Go `castAs{Int,Real,Decimal}FunctionClass.getFunction`: the eval type the
/// cast signature reads its argument in. A hybrid type is read as `ETInt`,
/// and a binary literal by the same-type signature (`CastIntAsInt`,
/// `CastRealAsReal`, `CastDecimalAsDecimal`), whose `Constant.Eval*` reads
/// the literal's unsigned value.
fn numeric_argument_source(expression: &Expression, target: EvalType) -> Option<EvalType> {
    let field = expression.static_type()?;
    Some(if is_numeric_binary_literal(expression) {
        target
    } else if field.is_hybrid() {
        EvalType::Int
    } else {
        field.eval_type()
    })
}

fn is_numeric_binary_literal(expression: &Expression) -> bool {
    matches!(expression, Expression::Constant(c) if matches!(c.value, Datum::BinaryLiteral(_)))
}

// Go's builder inserts numeric argument casts. Native arithmetic retains
// those source FieldTypes, so apply the same conversion before the operator.
/// Go `CanImplicitEvalInt` / `CanImplicitEvalReal` (`builtin_cast.go`):
/// `DAYNAME()` is a string function MySQL also evaluates as a number --
/// its weekday index, Monday 0 (`builtinDayNameSig.evalInt`/`evalReal`) --
/// wherever its value is cast to an integer or a real, and in a truth test
/// (`EvalBool`), rather than as the number its name would parse to.
/// `Some` is that number for a `DAYNAME()` value; `None` leaves the value
/// to the ordinary cast.
pub(crate) fn can_implicit_eval_int(
    expression: &Expression,
    value: &Datum,
    target: EvalType,
) -> Option<Datum> {
    const WEEKDAYS: [&str; 7] = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ];
    let Expression::ScalarFunction(function) = expression else {
        return None;
    };
    if function.func_name.lowercase() != "dayname" {
        return None;
    }
    let Datum::String(name) = value else {
        return None;
    };
    let index = WEEKDAYS
        .iter()
        .position(|day| day.as_bytes() == name.bytes())?;
    match target {
        EvalType::Int => Some(Datum::Int(index as i64)),
        EvalType::Real => Some(Datum::Real(index as f64)),
        _ => None,
    }
}

pub(crate) fn cast_numeric_argument(
    expression: &Expression,
    value: Datum,
    target: EvalType,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    cast_numeric_argument_in_mode(expression, value, target, ctx, false)
}

fn cast_numeric_argument_in_mode(
    expression: &Expression,
    value: Datum,
    target: EvalType,
    ctx: &dyn Columns,
    vectorized: bool,
) -> Result<Datum, EvalError> {
    let field = expression
        .static_type()
        .expect("numeric argument has a type");
    if value.is_null() || field.eval_type() == target {
        return Ok(value);
    }
    if let Some(number) = can_implicit_eval_int(expression, &value, target) {
        return Ok(number);
    }
    let value = match value {
        Datum::Int(bits) if field.is_unsigned() => Datum::UInt(bits as u64),
        Datum::Float32(value) => Datum::Real(f64::from(value as f32)),
        value => value,
    };
    let code = match target {
        EvalType::Int => tidb_datatype::FieldTypeCode::LongLong,
        EvalType::Real => tidb_datatype::FieldTypeCode::Double,
        EvalType::Decimal => tidb_datatype::FieldTypeCode::NewDecimal,
        _ => return Err(EvalError::Unsupported("numeric argument cast domain")),
    };
    if field.eval_type() == EvalType::String && !field.is_hybrid() {
        let bytes = match &value {
            Datum::String(value) => value.bytes(),
            Datum::Bytes(value) => value.as_slice(),
            _ => return Err(EvalError::Unsupported("string arithmetic argument domain")),
        };
        return cast_string_numeric_argument(field, bytes, target, ctx, vectorized);
    }
    if matches!(value, Datum::Json(_)) {
        // Go arithmetic wraps JSON operands with the same typed cast
        // signatures as explicit CAST; do not parse the serialized document.
        let target_field = if target == EvalType::Decimal {
            numeric_decimal_cast_type(field)
        } else {
            FieldType::new(code)
        };
        return crate::cast::eval_numeric_cast_with_type(
            value,
            EvalType::Json,
            None,
            Some(field),
            &target_field,
            false,
            ctx,
        );
    }
    let value =
        if target == EvalType::Decimal && matches!(value, Datum::Time(_) | Datum::Duration(_)) {
            // Time/Duration.ToNumber precedes target precision fitting.
            let warnings = crate::constant::ConversionWarnings(ctx);
            let zone = ctx.time_zone();
            let context = tidb_datatype::ConversionContext::new(
                ctx.type_flags(),
                tidb_datatype::ConversionLocation::from_time_zone(&zone),
                &warnings,
            );
            let (decimal, error) = value.to_decimal_with_context(&context).map_err(|_| {
                EvalError::Unsupported("numeric decimal argument conversion failed")
            })?;
            if let Some(error) = error {
                return Err(EvalError::Conversion(error));
            }
            Datum::Decimal(decimal)
        } else {
            value
        };
    let value = if let (EvalType::Decimal, Datum::Real(real)) = (target, &value) {
        // Every integral double in [-2^53, 2^53] has the same exact integer
        // decimal as Go's shortest-float formatting followed by FromString.
        // Negative zero keeps the source parser's sign behavior below.
        if real.abs() <= 9_007_199_254_740_992.0
            && real.fract() == 0.0
            && (*real != 0.0 || !real.is_sign_negative())
        {
            let value = Datum::Decimal(tidb_datatype::Decimal::from_int(*real as i64));
            let target = numeric_decimal_cast_type(field);
            return if target.decimal() < 0 {
                Ok(value)
            } else {
                convert_numeric_datum(value, &target, ctx)
            };
        }
        let (decimal, error) = tidb_datatype::MyDecimal::from_float64(*real);
        match error {
            Some(tidb_datatype::DecimalError::Overflow) => {
                if ctx.truncate_level() == tidb_exprctx::context::ErrorLevel::Error {
                    return Err(EvalError::Conversion(tidb_datatype::types_errors::ERR_OVERFLOW.clone()));
                }
                crate::expression_errors::handle_truncate(ctx, &format!(
                    "Truncated incorrect DECIMAL value: '{}'",
                    string_with_ctx(expression, ctx)
                        .unwrap_or_else(|| tidb_datatype::format_float_g_shortest(*real))
                ))?;
            }
            Some(tidb_datatype::DecimalError::Truncated) | None => {}
            Some(_) => return Err(EvalError::Conversion(tidb_datatype::types_errors::ERR_BAD_NUMBER.clone())),
        }
        Datum::Decimal(tidb_datatype::Decimal::from_my_decimal(&decimal))
    } else {
        value
    };
    let target_field = if target == EvalType::Decimal {
        numeric_decimal_cast_type(field)
    } else {
        FieldType::new(code)
    };
    // IntAsDecimal constructs the decimal before applying source metadata.
    // ProduceDecWithSpecifiedTp is a no-op when scale is unspecified.
    if target == EvalType::Decimal && target_field.decimal() < 0 {
        return Ok(match value {
            Datum::Int(value) => Datum::Decimal(tidb_datatype::Decimal::from_int(value)),
            Datum::UInt(value) => Datum::Decimal(tidb_datatype::Decimal::from_uint(value)),
            value => value,
        });
    }
    convert_numeric_datum(value, &target_field, ctx)
}

fn cast_string_numeric_argument(
    field: &FieldType,
    bytes: &[u8],
    target: EvalType,
    ctx: &dyn Columns,
    vectorized: bool,
) -> Result<Datum, EvalError> {
    if target == EvalType::Real {
        return crate::ops::bytes_to_f64(bytes, ctx).map(Datum::Real);
    }
    cast_string_as_decimal(bytes, &numeric_decimal_cast_type(field), ctx, vectorized)
}

/// Go `builtinCastStringAsDecimalSig`: `evalDecimal`, or `vecEvalDecimal`
/// when `vectorized`, producing the cast's own type `tp`.
fn cast_string_as_decimal(
    bytes: &[u8],
    tp: &FieldType,
    ctx: &dyn Columns,
    vectorized: bool,
) -> Result<Datum, EvalError> {
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    let (decimal, error) = tidb_datatype::MyDecimal::from_string(text.as_bytes());
    if let Some(error) = error {
        use tidb_datatype::DecimalError;
        // Go's scalar string cast names the truncated value; its vector
        // cast passes FromString's raw ErrTruncated through HandleTruncate.
        if error == DecimalError::TruncatedWrongValue
            || (error == DecimalError::Truncated && !vectorized)
        {
            crate::expression_errors::handle_truncate(
                ctx,
                &format!("Truncated incorrect DECIMAL value: '{text}'"),
            )?;
        } else {
            let error = match error {
                DecimalError::Truncated => tidb_datatype::types_errors::ERR_TRUNCATED.clone(),
                DecimalError::Overflow => tidb_datatype::types_errors::ERR_OVERFLOW.clone(),
                _ => tidb_datatype::types_errors::ERR_BAD_NUMBER.clone(),
            };
            match ctx.truncate_level() {
                crate::ErrorLevel::Error => return Err(EvalError::Conversion(error)),
                crate::ErrorLevel::Warn => {
                    let warning = error.to_sql_error();
                    ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
                        tidb_context::WarningLevel::Warning,
                        warning.code,
                        warning.message.clone(),
                        warning.identity,
                    ));
                }
                crate::ErrorLevel::Ignore => {}
            }
        }
    }
    let value = Datum::Decimal(tidb_datatype::Decimal::from_my_decimal(&decimal));
    if tp.decimal() < 0 {
        Ok(value)
    } else {
        convert_numeric_datum(value, tp, ctx)
    }
}

fn convert_numeric_datum(
    value: Datum,
    target: &FieldType,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let warnings = crate::constant::ConversionWarnings(ctx);
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        ctx.type_flags(),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    let converted = value
        .convert_to_in_context(target, &context, &zone)
        .map_err(|_| EvalError::Unsupported("numeric argument conversion failed"))?;
    if let Some(error) = converted.error {
        return Err(EvalError::Conversion(error));
    }
    Ok(converted.value)
}

fn eval_numeric_operand_row(
    expression: &Expression,
    ctx: &dyn Columns,
    row: Row<'_>,
    target: EvalType,
) -> Result<Datum, EvalError> {
    let source = numeric_argument_source(expression, target).expect("numeric argument has a type");
    if is_numeric_binary_literal(expression) {
        return eval_numeric_row(expression, ctx, row, target);
    }
    cast_numeric_argument(
        expression,
        eval_numeric_row(expression, ctx, row, source)?,
        target,
        ctx,
    )
}

fn eval_numeric_operand_batch(
    expression: &Expression,
    ctx: &dyn Columns,
    input: &Chunk,
    target: EvalType,
) -> Result<Vec<Datum>, EvalError> {
    let field = expression
        .static_type()
        .expect("numeric argument has a type");
    let source = numeric_argument_source(expression, target).expect("numeric argument has a type");
    // Go casts its borrowed string column buffer directly. Preserve the full
    // operand pass while avoiding a String/Datum allocation for every cell.
    if source == EvalType::String {
        if let Expression::Column(column) = expression {
            let index = usize::try_from(column.index)
                .map_err(|_| EvalError::Unsupported("column index is negative"))?;
            if index >= input.num_cols() {
                return Err(EvalError::Unsupported(
                    "column index is outside the input row",
                ));
            }
            let column = input.column(index);
            return (0..input.num_rows())
                .map(|row| {
                    let row = input.get_row(row).idx();
                    if column.is_null(row) {
                        Ok(Datum::Null)
                    } else {
                        cast_string_numeric_argument(
                            field,
                            column.get_bytes(row).as_ref(),
                            target,
                            ctx,
                            true,
                        )
                    }
                })
                .collect();
        }
    }
    let values = eval_numeric_batch_values(expression, ctx, input, source)?;
    if source == target {
        return Ok(values);
    }
    values
        .into_iter()
        .map(|value| cast_numeric_argument_in_mode(expression, value, target, ctx, true))
        .collect()
}

/// Go JSON cast signatures consume their source EvalType, including hybrid
/// string names and signed integer carriers, before constructing binary JSON.
fn cast_json_argument_value(function: &ScalarFunction, value: Datum) -> Result<Datum, EvalError> {
    let field = function.args[0]
        .static_type()
        .ok_or(EvalError::Unsupported("a JSON cast with no source type"))?;
    let value = match value {
        Datum::Int(value)
            if field.is_unsigned() || field.code() == tidb_datatype::FieldTypeCode::Year =>
        {
            Datum::UInt(value as u64)
        }
        value @ (Datum::Enum(..) | Datum::Set(..)) if field.eval_type() == EvalType::String => {
            Datum::new_bytes_datum(
                value
                    .sql_bytes()
                    .map_err(|_| EvalError::Unsupported("hybrid JSON string conversion"))?,
            )
        }
        value => value,
    };
    if function
        .get_static_type()
        .is_some_and(|field| field.has_flag(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON))
    {
        crate::builtin_ext::cast_as_json_typed(&value, Some(field))
    } else {
        crate::builtin_ext::cast_as_json_value_typed(&value, Some(field))
    }
}

// These source-selected JSON signatures share scalar conversion with the
// vector path, after the whole JSON input batch has been evaluated.
/// A numeric cast of one argument that Go's typed `vecEval*` cast signature
/// evaluates argument-first: the whole argument column, then the conversion
/// row by row (so a string column's truncation warnings come in row order,
/// before the next operand's). Hybrid and vector sources keep the row path.
fn numeric_cast_target(function: &ScalarFunction) -> Option<&FieldType> {
    use tidb_datatype::FieldTypeCode;
    if !function.is_cast() || function.cast_in_union() || function.args.len() != 1 {
        return None;
    }
    let field = function.get_static_type()?;
    if matches!(field.code(), FieldTypeCode::Year | FieldTypeCode::Float) || field.is_array() {
        return None;
    }
    let expected = field.eval_type();
    if !matches!(expected, EvalType::Int | EvalType::Real | EvalType::Decimal) {
        return None;
    }
    if numeric_argument_source(&function.args[0], expected)? == EvalType::VectorFloat32 {
        return None;
    }
    Some(field)
}

fn numeric_batch_supported(expression: &Expression, target: EvalType) -> bool {
    match expression {
        Expression::Constant(constant) => constant
            .deferred_expr
            .as_deref()
            .is_none_or(|child| numeric_batch_supported(child, target)),
        Expression::CorrelatedColumn(_) => true,
        // Go `ScalarSubQueryExpr.VecEval*` refuses; the row path answers.
        Expression::ScalarSubQuery(_) => false,
        Expression::Column(column) => column.get_static_type().is_some_and(|field| {
            if field.is_hybrid() {
                target == EvalType::Int
                    || (target == EvalType::String && field.eval_type() == target)
            } else {
                field.eval_type() == target
            }
        }),
        Expression::ScalarFunction(function) => {
            if let Some(field) = numeric_cast_target(function) {
                return field.eval_type() == target
                    && numeric_argument_source(&function.args[0], target)
                        .is_some_and(|source| numeric_batch_supported(&function.args[0], source));
            }

            if function.is_cast_as_json() && target == EvalType::Json {
                return function
                    .get_static_type()
                    .is_some_and(|field| field.eval_type() == target)
                    && function.args.len() == 1
                    && function.args[0].static_type().is_some_and(|field| {
                        field.eval_type() != EvalType::VectorFloat32
                            && numeric_batch_supported(&function.args[0], field.eval_type())
                    });
            }
            function
                .get_static_type()
                .is_some_and(|field| field.eval_type() == target)
                && function.numeric_operand_domain().is_some()
                && function.args.iter().all(|arg| {
                    numeric_batch_supported(
                        arg,
                        numeric_argument_source(
                            arg,
                            function.numeric_operand_domain().unwrap_or(EvalType::Int),
                        )
                        .expect("numeric argument has a type"),
                    )
                })
        }
    }
}

// Integer batches carry the same i64 bit patterns as Go chunk.Int64s. This
// avoids cloning large Datum enums for every intermediate arithmetic node.
fn eval_integer_batch(
    expression: &Expression,
    ctx: &dyn Columns,
    input: &Chunk,
) -> Result<Vec<Option<i64>>, EvalError> {
    let bits = |value: Datum| -> Result<Option<i64>, EvalError> {
        match value {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(value)),
            Datum::UInt(value) => Ok(Some(value as i64)),
            _ => Err(EvalError::Unsupported(
                "integer batch value is outside its typed domain",
            )),
        }
    };
    match expression {
        Expression::Constant(constant) => {
            if let Some(child) = constant.deferred_expr.as_deref() {
                return eval_integer_batch(child, ctx, input);
            }
            Ok(vec![
                bits(constant.eval_typed_as_on_row(
                    ctx,
                    input.get_row(0),
                    EvalType::Int
                )?)?;
                input.num_rows()
            ])
        }
        Expression::CorrelatedColumn(column) => {
            Ok(vec![
                bits(column.eval_typed(ctx, EvalType::Int)?)?;
                input.num_rows()
            ])
        }
        Expression::Column(column) => {
            if column.get_static_type().is_some_and(FieldType::is_hybrid) {
                return (0..input.num_rows())
                    .map(|row| {
                        bits(eval_numeric_row(
                            expression,
                            ctx,
                            input.get_row(row),
                            EvalType::Int,
                        )?)
                    })
                    .collect();
            }
            let index = usize::try_from(column.index)
                .map_err(|_| EvalError::Unsupported("invalid integer column index"))?;
            if index >= input.num_cols() {
                return Err(EvalError::Unsupported("invalid integer column index"));
            }
            let column = input.column(index);
            Ok((0..input.num_rows())
                .map(|row| {
                    let row = input.get_row(row).idx();
                    (!column.is_null(row)).then(|| column.get_int64(row))
                })
                .collect())
        }
        Expression::ScalarFunction(function) => {
            if numeric_cast_target(function).is_some() {
                return eval_numeric_batch_values(expression, ctx, input, EvalType::Int)?
                    .into_iter()
                    .map(bits)
                    .collect();
            }

            if function.numeric_operand_domain() != Some(EvalType::Int)
                || function
                    .args
                    .iter()
                    .any(|arg| numeric_argument_source(arg, EvalType::Int) != Some(EvalType::Int))
            {
                return eval_arithmetic_batch(function, ctx, input)?
                    .into_iter()
                    .map(bits)
                    .collect();
            }
            let mut left = eval_integer_batch(&function.args[0], ctx, input)?;
            let right = eval_integer_batch(&function.args[1], ctx, input)?;
            let op = binary_op_for_name(function.func_name.lowercase())
                .expect("preflight checked arithmetic");
            let unsigned = [0, 1].map(|i| {
                function.args[i]
                    .static_type()
                    .expect("typed argument")
                    .is_unsigned()
            });
            let integer = |bits: i64, unsigned: bool| {
                if unsigned {
                    crate::coerce::Integer::Unsigned(bits as u64)
                } else {
                    crate::coerce::Integer::Signed(bits)
                }
            };
            for (left, right) in left.iter_mut().zip(right) {
                *left = match (*left, right) {
                    (Some(a), Some(b)) => bits(
                        crate::ops::integer_binary(
                            op,
                            integer(a, unsigned[0]),
                            integer(b, unsigned[1]),
                            ctx,
                        )
                        .map_err(|error| match error {
                            EvalError::IntOverflow if op == BinaryOp::IntDiv => {
                                integer_division_overflow(
                                    integer(a, unsigned[0]),
                                    integer(b, unsigned[1]),
                                )
                            }
                            EvalError::IntOverflow => arithmetic_overflow_error(function, op, ctx),
                            other => other,
                        })?,
                    )?,
                    _ => None,
                };
            }
            Ok(left)
        }
        Expression::ScalarSubQuery(_) => Err(crate::scalar_subquery::vec_eval_error()),
    }
}

fn eval_numeric_batch_values(
    expression: &Expression,
    ctx: &dyn Columns,
    input: &Chunk,
    target: EvalType,
) -> Result<Vec<Datum>, EvalError> {
    if let Expression::ScalarFunction(function) = expression {
        if let Some(field) = numeric_cast_target(function) {
            let source = numeric_argument_source(&function.args[0], field.eval_type())
                .expect("numeric cast source has a type");
            let values = eval_numeric_batch_values(&function.args[0], ctx, input, source)?;
            if source == EvalType::String && field.eval_type() == EvalType::Decimal {
                return values
                    .into_iter()
                    .map(|value| match crate::arg_eval_type::eval_string(&value)? {
                        None => Ok(Datum::Null),
                        Some(bytes) => cast_string_as_decimal(&bytes, field, ctx, true),
                    })
                    .collect();
            }
            return values
                .into_iter()
                .map(|value| {
                    if value.is_null() {
                        Ok(value)
                    } else {
                        crate::cast::eval_numeric_cast_with_type(
                            value,
                            source,
                            Some(&function.args[0]),
                            function.args[0].static_type(),
                            field,
                            false,
                            ctx,
                        )
                    }
                })
                .collect();
        }
    }
    if target == EvalType::Int {
        let unsigned = expression
            .static_type()
            .is_some_and(|field| field.is_unsigned());
        return Ok(eval_integer_batch(expression, ctx, input)?
            .into_iter()
            .map(|value| match value {
                None => Datum::Null,
                Some(value) if unsigned => Datum::UInt(value as u64),
                Some(value) => Datum::Int(value),
            })
            .collect());
    }
    match expression {
        Expression::Constant(constant) => {
            if let Some(child) = constant.deferred_expr.as_deref() {
                return eval_numeric_batch_values(child, ctx, input, target);
            }
            let value = constant.eval_typed_as_on_row(ctx, input.get_row(0), target)?;
            Ok(vec![value; input.num_rows()])
        }
        Expression::CorrelatedColumn(column) => {
            Ok(vec![column.eval_typed(ctx, target)?; input.num_rows()])
        }
        Expression::Column(_) => (0..input.num_rows())
            .map(|index| eval_numeric_row(expression, ctx, input.get_row(index), target))
            .collect(),
        Expression::ScalarFunction(function) if function.is_cast_as_json() => {
            // Go completes the source-typed batch before JSON conversion.
            let source = function.args[0]
                .static_type()
                .expect("preflight checked JSON source")
                .eval_type();
            let values = eval_numeric_batch_values(&function.args[0], ctx, input, source)?;
            values
                .into_iter()
                .map(|value| cast_json_argument_value(function, value))
                .collect()
        }
        Expression::ScalarFunction(function) => eval_arithmetic_batch(function, ctx, input),
        Expression::ScalarSubQuery(_) => Err(crate::scalar_subquery::vec_eval_error()),
    }
}

fn eval_arithmetic_batch(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
) -> Result<Vec<Datum>, EvalError> {
    // Go evaluates the entire left batch, then the entire right batch, then
    // arithmetic. A NULL left row does not suppress right operand errors.
    let domain = function
        .numeric_operand_domain()
        .expect("preflight checked arithmetic");
    let mut left = eval_numeric_operand_batch(&function.args[0], ctx, input, domain)?;
    let right = eval_numeric_operand_batch(&function.args[1], ctx, input, domain)?;
    let op =
        binary_op_for_name(function.func_name.lowercase()).expect("preflight checked arithmetic");
    for (left, right) in left.iter_mut().zip(right) {
        let value = std::mem::replace(left, Datum::Null);
        let result = if op == BinaryOp::IntDiv
            && domain == EvalType::Decimal
            && !value.is_null()
            && !right.is_null()
        {
            let (Datum::Decimal(a), Datum::Decimal(b)) = (&value, &right) else {
                return Err(EvalError::Unsupported("decimal DIV argument domain"));
            };
            let unsigned = function
                .args
                .iter()
                .any(|arg| arg.static_type().is_some_and(|field| field.is_unsigned()));
            crate::ops::decimal_integer_division(a, b, unsigned, ctx).map_err(
                |error| match error {
                    EvalError::IntOverflow => EvalError::DataOutOfRange {
                        value: if unsigned {
                            "BIGINT UNSIGNED"
                        } else {
                            "BIGINT"
                        },
                        expression: format!("({a} DIV {b})"),
                    },
                    other => other,
                },
            )?
        } else {
            function.eval_binary_values(op, value, right, ctx)?
        };
        *left = function.coerce_to_ret_type(result)?;
    }
    Ok(left)
}

/// Go numeric arithmetic batch evaluation for typed expression trees. A full
/// eligibility pass precedes evaluation, so declining a shape never emits a
/// warning or evaluates an operand. Other signatures retain their existing path.
/// Deferred wrappers forward the requested domain and use child metadata.
pub fn try_eval_numeric_batch(
    expression: &Expression,
    ctx: &dyn Columns,
    input: &Chunk,
) -> Result<Option<Vec<Datum>>, EvalError> {
    let mut source = expression;
    while let Expression::Constant(constant) = source {
        let Some(child) = constant.deferred_expr.as_deref() else {
            return Ok(None);
        };
        source = child;
    }
    if !matches!(source, Expression::ScalarFunction(_)) {
        return Ok(None);
    }
    let target = match expression {
        Expression::Constant(constant) => constant.get_type(ctx)?.eval_type(),
        _ => match expression.static_type() {
            Some(field) => field.eval_type(),
            None => return Ok(None),
        },
    };
    if !numeric_batch_supported(expression, target) {
        return Ok(None);
    }
    if input.num_rows() == 0 {
        return Ok(Some(Vec::new()));
    }
    eval_numeric_batch_values(expression, ctx, input, target).map(Some)
}

/// Go `VecEvalDecimal` over one expression node, restricted to the shapes
/// whose row evaluation is `ops::decimal_binary`'s exact fast arithmetic:
/// integer and decimal columns, strict integer and decimal constants, and
/// `+`/`-`/`*` nodes with a decimal result type over them where at least
/// one argument is a decimal (an integer-only node is integer arithmetic).
/// `None` means the shape is not covered or a value left the `i128` fast
/// path; the caller then evaluates the whole column row by row.
fn vec_eval_decimal(
    expression: &Expression,
    input: &Chunk,
    physical: &[usize],
) -> Option<Vec<Option<DecimalValue>>> {
    use tidb_datatype::FieldTypeCode;
    let field_type = expression.static_type()?;
    let is_int = matches!(
        field_type.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year
    );
    let is_decimal = field_type.code() == FieldTypeCode::NewDecimal;
    if !is_int && !is_decimal {
        return None;
    }
    let signed = !field_type.is_unsigned();
    match expression {
        Expression::Column(column) => {
            let index = usize::try_from(column.index).ok()?;
            if index >= input.num_cols() {
                return None;
            }
            let column = input.column(index);
            if is_int && column.type_size() == 8 {
                Some(
                    physical
                        .iter()
                        .map(|&row| {
                            (!column.is_null(row)).then(|| {
                                let bits = column.get_int64(row);
                                DecimalValue::from_integer(if signed {
                                    crate::coerce::Integer::Signed(bits)
                                } else {
                                    crate::coerce::Integer::Unsigned(bits as u64)
                                })
                            })
                        })
                        .collect(),
                )
            } else if is_decimal && column.type_size() == tidb_chunk::column::MY_DECIMAL_STRUCT_SIZE
            {
                physical
                    .iter()
                    .map(|&row| {
                        if column.is_null(row) {
                            Some(None)
                        } else {
                            DecimalValue::from_my_decimal(&column.get_my_decimal(row)).map(Some)
                        }
                    })
                    .collect()
            } else {
                None
            }
        }
        Expression::Constant(constant) => {
            let value = match constant.literal_value()? {
                Datum::Null => None,
                Datum::Int(value) if is_int => Some(DecimalValue::from_integer(if signed {
                    crate::coerce::Integer::Signed(*value)
                } else {
                    crate::coerce::Integer::Unsigned(*value as u64)
                })),
                Datum::UInt(value) if is_int => Some(DecimalValue::from_integer(
                    crate::coerce::Integer::Unsigned(*value),
                )),
                Datum::Decimal(value) if is_decimal => {
                    Some(DecimalValue::from_my_decimal(&value.to_my_decimal().ok()?)?)
                }
                _ => return None,
            };
            Some(vec![value; physical.len()])
        }
        Expression::ScalarFunction(function) => {
            if !is_decimal {
                return None;
            }
            vec_eval_decimal_function(function, input, physical)
        }
        Expression::CorrelatedColumn(_) | Expression::ScalarSubQuery(_) => None,
    }
}

/// The `+`/`-`/`*` node of [`vec_eval_decimal`].
fn vec_eval_decimal_function(
    function: &ScalarFunction,
    input: &Chunk,
    physical: &[usize],
) -> Option<Vec<Option<DecimalValue>>> {
    use tidb_datatype::FieldTypeCode;
    if function.is_cast()
        && !function.cast_in_union()
        && function
            .get_static_type()
            .is_some_and(|field| field.code() == FieldTypeCode::NewDecimal)
    {
        return vec_eval_cast_int_as_decimal(function, input, physical);
    }
    {
        {
            if function.args.len() != 2 {
                return None;
            }
            let op = match binary_op_for_name(function.func_name.lowercase()) {
                Some(op @ (BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Mul)) => op,
                _ => return None,
            };
            // `eval_binary_full` reaches `decimal_binary` only when an
            // operand is a decimal; two integers are integer arithmetic.
            let decimal_argument = function.args.iter().any(|argument| {
                argument
                    .static_type()
                    .is_some_and(|ty| ty.code() == FieldTypeCode::NewDecimal)
            });
            if !decimal_argument {
                return None;
            }
            let lhs = vec_eval_decimal(&function.args[0], input, physical)?;
            let rhs = vec_eval_decimal(&function.args[1], input, physical)?;
            lhs.into_iter()
                .zip(rhs)
                .map(|(left, right)| match (left, right) {
                    (Some(left), Some(right)) => match op {
                        BinaryOp::Plus => left.add(right),
                        BinaryOp::Minus => left.sub(right),
                        _ => left.mul(right),
                    }
                    .map(Some),
                    _ => Some(None),
                })
                .collect()
        }
    }
}

/// Go `builtinCastIntAsDecimalSig.vecEvalDecimal`, restricted to the shape
/// whose row evaluation (`cast::eval_cast`'s DECIMAL arm over an integer,
/// `Decimal::from_int` then `cast_to_precision`) is the integer itself: an
/// integer column cast to a `DECIMAL(flen, scale)` whose integer digits hold
/// every value the column's type can take (19 for a signed BIGINT), so
/// `ProduceDecWithSpecifiedTp` neither clamps nor warns and the cell is the
/// integer padded to `scale` fraction digits. An unspecified target shape
/// keeps the row path (it returns the source unchanged), as does a value
/// whose padding leaves `i128`.
fn vec_eval_cast_int_as_decimal(
    function: &ScalarFunction,
    input: &Chunk,
    physical: &[usize],
) -> Option<Vec<Option<DecimalValue>>> {
    use tidb_datatype::FieldTypeCode;
    let [Expression::Column(column)] = function.args.as_slice() else {
        return None;
    };
    let source = column.get_static_type()?;
    let signed = !source.is_unsigned();
    // The decimal digits of the widest value the column's type can store.
    let digits = match source.code() {
        FieldTypeCode::Tiny => 3,
        FieldTypeCode::Year => 4,
        FieldTypeCode::Short => 5,
        FieldTypeCode::Int24 if signed => 7,
        FieldTypeCode::Int24 => 8,
        FieldTypeCode::Long => 10,
        FieldTypeCode::LongLong if signed => 19,
        FieldTypeCode::LongLong => 20,
        _ => return None,
    };
    let target = function.get_static_type()?;
    if target.decimal() == UNSPECIFIED_LENGTH {
        return None;
    }
    let flen = u32::try_from(target.flen()).ok()?;
    let scale = u32::try_from(target.decimal()).ok()?;
    if flen == 0 || flen < scale.checked_add(digits)? {
        return None;
    }
    let factor = 10i128.checked_pow(scale)?;
    let index = usize::try_from(column.index).ok()?;
    if index >= input.num_cols() {
        return None;
    }
    let column = input.column(index);
    if column.type_size() != 8 {
        return None;
    }
    physical
        .iter()
        .map(|&row| {
            if column.is_null(row) {
                return Some(None);
            }
            let bits = column.get_int64(row);
            let value = if signed {
                i128::from(bits)
            } else {
                i128::from(bits as u64)
            };
            Some(Some(DecimalValue {
                coefficient: value.checked_mul(factor)?,
                storage_scale: scale,
                scale,
            }))
        })
        .collect()
}

impl ScalarFunction {
    /// Go `builtinArithmetic{Plus,Minus,Multiply}DecimalSig.vecEvalDecimal`:
    /// appends this expression's value for every row of `input` to column
    /// `output_index` of `output`, column-wise. Returns `Ok(false)` with
    /// nothing appended when the shape is not covered (see
    /// [`vec_eval_decimal`]); the appended cells are the ones the row
    /// evaluator would append through `Chunk::append_datum`.
    pub(crate) fn vec_eval_decimal_arithmetic(
        &self,
        input: &Chunk,
        output: &mut Chunk,
        output_index: usize,
    ) -> Result<bool, EvalError> {
        if !self
            .get_static_type()
            .is_some_and(|ty| ty.code() == tidb_datatype::FieldTypeCode::NewDecimal)
        {
            return Ok(false);
        }
        let rows = input.num_rows();
        let physical: Vec<usize> = (0..rows).map(|row| input.get_row(row).idx()).collect();
        let Some(values) = vec_eval_decimal_function(self, input, &physical) else {
            return Ok(false);
        };
        let mut cells = Vec::with_capacity(rows);
        for value in values {
            match value {
                Some(value) => match value.to_my_decimal() {
                    Some(cell) => cells.push(Some(cell)),
                    None => return Ok(false),
                },
                None => cells.push(None),
            }
        }
        for cell in &cells {
            match cell {
                Some(cell) => output.append_my_decimal(output_index, cell),
                None => output.append_null(output_index),
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::column::Column;
    use crate::constant::Constant;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    #[test]
    fn string_division_preserves_go_decimal_parser_diagnostics() {
        struct Context {
            level: crate::ErrorLevel,
            warnings: std::cell::RefCell<Vec<(u16, String)>>,
        }
        impl crate::Columns for Context {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> crate::ErrorLevel {
                self.level
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        use crate::ErrorLevel::{Error, Ignore, Warn};
        for (case, text) in [
            "",
            "abc",
            " 1x ",
            "1e999",
            "1e-999",
            "1e18446744073709551620",
            &"9".repeat(82),
        ]
        .into_iter()
        .enumerate()
        {
            for level in [Error, Warn, Ignore] {
                for vectorized in [false, true] {
                    let ctx = Context {
                        level,
                        warnings: Default::default(),
                    };
                    let field = FieldType::new(FieldTypeCode::VarString);
                    let args = (0..2)
                        .map(|index| {
                            let mut column =
                                crate::expression::Column::new(index + 1, field.clone());
                            column.index = index;
                            Expression::Column(column)
                        })
                        .collect::<Vec<_>>();
                    let expression = crate::new_function::new_function(
                        &ctx,
                        "intdiv",
                        FieldType::new(FieldTypeCode::LongLong),
                        args,
                    )
                    .unwrap();
                    let mut chunk = Chunk::new_chunk_with_capacity(&[field.clone(), field], 1);
                    chunk.append_datum(0, &Datum::new_string_datum(text));
                    chunk.append_datum(1, &Datum::new_string_datum("1"));
                    let result = if vectorized {
                        super::try_eval_numeric_batch(&expression, &ctx, &chunk)
                            .map(|batch| batch.unwrap().remove(0))
                    } else {
                        expression.eval(&ctx, chunk.get_row(0))
                    };
                    let error = match case {
                        3 | 6 => EvalError::Conversion(tidb_datatype::types_errors::ERR_OVERFLOW.clone()),
                        5 => EvalError::Conversion(tidb_datatype::types_errors::ERR_BAD_NUMBER.clone()),
                        2 | 4 if vectorized => {
                            EvalError::Conversion(tidb_datatype::types_errors::ERR_TRUNCATED.clone())
                        }
                        _ => EvalError::TruncatedWrongValue(format!(
                            "Truncated incorrect DECIMAL value: '{}'",
                            text.trim()
                        )),
                    };
                    let warning = match &error {
                        EvalError::Conversion(error) => {
                            let e = error.to_sql_error();
                            (e.code, e.message)
                        }
                        EvalError::TruncatedWrongValue(message) => (1292, message.clone()),
                        _ => unreachable!(),
                    };
                    let mut warnings = if level == Warn { vec![warning] } else { vec![] };
                    let expected = if level == Error {
                        Err(error)
                    } else if matches!(case, 3 | 6) {
                        if level == Warn {
                            warnings.push((
                                1292,
                                format!("Truncated incorrect DECIMAL value: '{}'", "9".repeat(81)),
                            ));
                        }
                        Err(EvalError::DataOutOfRange {
                            value: "BIGINT",
                            expression: if vectorized {
                                format!("({} DIV 1)", "9".repeat(81))
                            } else {
                                "(cast(Column#1, decimal(65,0) BINARY) DIV cast(Column#2, decimal(65,0) BINARY))".to_owned()
                            },
                        })
                    } else {
                        Ok(Datum::Int(if case == 2 { 1 } else { 0 }))
                    };
                    // The SQL error a client reads: its code and message.
                    let sql = |result: Result<Datum, EvalError>| {
                        result.map_err(|error| {
                            let error = crate::EvalErrorSql::to_sql_error(&error);
                            (error.code, error.message)
                        })
                    };
                    assert_eq!(
                        sql(result),
                        sql(expected),
                        "{text:?}/{level:?}/{vectorized}"
                    );
                    assert_eq!(
                        *ctx.warnings.borrow(),
                        warnings,
                        "{text:?}/{level:?}/{vectorized}"
                    );
                }
            }
        }
    }

    #[test]
    fn string_arithmetic_preserves_go_cast_and_batch_order() {
        #[derive(Default)]
        struct Context {
            errors: bool,
            warnings: std::cell::RefCell<Vec<(u16, String)>>,
        }
        impl crate::Columns for Context {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> crate::ErrorLevel {
                if self.errors {
                    crate::ErrorLevel::Error
                } else {
                    crate::ErrorLevel::Warn
                }
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        let mut mismatches = Vec::new();
        for name in ["plus", "minus", "mul", "div", "intdiv", "mod"] {
            for mode in ["warnings", "nulls", "error"] {
                for (vectorized, selected) in
                    [(false, false), (false, true), (true, false), (true, true)]
                {
                    let field = FieldType::new(FieldTypeCode::VarString);
                    let args = (0..2)
                        .map(|index| {
                            let mut column =
                                crate::expression::Column::new(index + 1, field.clone());
                            column.index = index;
                            Expression::Column(column)
                        })
                        .collect::<Vec<_>>();
                    let output =
                        crate::builtin_arithmetic::infer_arithmetic_type(name, &args[0], &args[1])
                            .unwrap();
                    let expression = crate::new_function::new_function_base(
                        &crate::NoColumns,
                        name,
                        output,
                        args,
                    ).unwrap();
                    let mut chunk = Chunk::new_chunk_with_capacity(&[field.clone(), field], 2);
                    let rows = match mode {
                        "warnings" => [[Some("1x"), Some("3z")], [Some("2y"), Some("4w")]],
                        "nulls" => [[None, Some("3z")], [Some("2y"), None]],
                        _ => [[Some("1"), Some("3z")], [Some("2y"), Some("4")]],
                    };
                    for row in rows {
                        for (i, value) in row.into_iter().enumerate() {
                            chunk.append_datum(
                                i,
                                &value.map(Datum::new_string_datum).unwrap_or(Datum::Null),
                            );
                        }
                    }
                    if selected {
                        chunk.set_sel(Some(vec![1, 0]));
                    }
                    let ctx = Context {
                        errors: mode == "error",
                        ..Context::default()
                    };
                    let scalar = || {
                        (0..2)
                            .map(|i| expression.eval(&ctx, chunk.get_row(i)))
                            .collect::<Result<Vec<_>, _>>()
                    };
                    let actual = if vectorized {
                        super::try_eval_numeric_batch(&expression, &ctx, &chunk)
                            .and_then(|batch| batch.map(Ok).unwrap_or_else(scalar))
                    } else {
                        scalar()
                    };
                    // The decimal cast reports 1292 as the `types` terror.
                    let actual = actual.map_err(|error| match error {
                        EvalError::Conversion(terror) if terror.code() == tidb_terror::TerrorCode::new(1292) => {
                            EvalError::TruncatedWrongValue(terror.message().to_owned())
                        }
                        error => error,
                    });
                    let class = if name == "intdiv" {
                        "DECIMAL"
                    } else {
                        "DOUBLE"
                    };
                    let message = |value| format!("Truncated incorrect {class} value: '{value}'");
                    let mut expected = if mode == "error" {
                        if name == "intdiv" && vectorized {
                            Err(EvalError::Conversion(tidb_datatype::types_errors::ERR_TRUNCATED.clone()))
                        } else {
                            Err(EvalError::TruncatedWrongValue(message(
                                if vectorized || selected { "2y" } else { "3z" },
                            )))
                        }
                    } else if mode == "nulls" {
                        Ok(vec![Datum::Null; 2])
                    } else {
                        Ok(match name {
                            "plus" => vec![Datum::Real(4.0), Datum::Real(6.0)],
                            "minus" => vec![Datum::Real(-2.0); 2],
                            "mul" => vec![Datum::Real(3.0), Datum::Real(8.0)],
                            "div" => vec![Datum::Real(1.0 / 3.0), Datum::Real(0.5)],
                            "intdiv" => vec![Datum::Int(0); 2],
                            _ => vec![Datum::Real(1.0), Datum::Real(2.0)],
                        })
                    };
                    if selected {
                        if let Ok(values) = &mut expected {
                            values.reverse();
                        }
                    }
                    let warning_inputs: &[&str] = match (mode, vectorized) {
                        ("warnings", false) if selected => &["2y", "4w", "1x", "3z"],
                        ("warnings", true) if selected => &["2y", "1x", "4w", "3z"],
                        ("nulls", true) if selected => &["2y", "3z"],
                        ("nulls", false) if selected && matches!(name, "plus" | "mod") => {
                            &["2y", "3z"]
                        }
                        ("warnings", false) => &["1x", "3z", "2y", "4w"],
                        ("warnings", true) => &["1x", "2y", "3z", "4w"],
                        ("nulls", true) => &["2y", "3z"],
                        ("nulls", false) if matches!(name, "plus" | "mod") => &["3z", "2y"],
                        ("nulls", false) => &["2y"],
                        _ => &[],
                    };
                    let warnings = warning_inputs
                        .iter()
                        .map(|v| {
                            if name == "intdiv" && vectorized {
                                (1265, "Data truncated for column '%s' at row %d".to_owned())
                            } else {
                                (1292, message(v))
                            }
                        })
                        .collect::<Vec<_>>();
                    if actual != expected || *ctx.warnings.borrow() != warnings {
                        mismatches.push(format!("{name}/{mode}/{vectorized}/{selected}: {actual:?}, warnings {:?}; expected {expected:?}, {warnings:?}", ctx.warnings.borrow()));
                    }
                }
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    fn ft() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// A hand-built node must be given the result type its function actually
    /// reports, exactly as the rewriter's `builtin_return_type` would: since
    /// [`ScalarFunction::eval`] reconciles the evaluated value with the
    /// declared type (Go's own `Eval` does the same by construction), a node
    /// declared `Long` for a string- or real-valued builtin is a node Go
    /// cannot build, and asserting on one would assert nothing.
    fn text_ft() -> FieldType {
        FieldType::new(FieldTypeCode::VarString)
    }

    fn real_ft() -> FieldType {
        FieldType::new(FieldTypeCode::Double)
    }

    #[test]
    fn numeric_real_cast_fast_path_preserves_decimal_value_and_scale() {
        for value in [
            -9_007_199_254_740_994.0,
            -9_007_199_254_740_992.0,
            -1.0,
            -0.0,
            0.0,
            1.0,
            9_007_199_254_740_992.0,
            9_007_199_254_740_994.0,
            0.3,
            0.39,
            f64::MIN_POSITIVE,
        ] {
            let expression = Expression::Constant(Constant::new(Datum::Real(value), real_ft()));
            let actual = cast_numeric_argument(
                &expression,
                Datum::Real(value),
                EvalType::Decimal,
                &crate::NoColumns,
            )
            .unwrap();
            let Datum::Decimal(actual) = actual else {
                panic!("expected decimal");
            };
            let (expected, error) = tidb_datatype::MyDecimal::from_float64(value);
            assert!(error.is_none() || error == Some(tidb_datatype::DecimalError::Truncated));
            let expected = tidb_datatype::Decimal::from_my_decimal(&expected);
            assert_eq!(
                (actual.storage_string(), actual.scale()),
                (expected.storage_string(), expected.scale()),
                "{value:?}"
            );
        }
    }

    #[test]
    fn binary_operator_names_delegate_to_the_opcode_authority() {
        for operator in [
            BinaryOp::Plus,
            BinaryOp::Minus,
            BinaryOp::Mul,
            BinaryOp::Div,
            BinaryOp::IntDiv,
            BinaryOp::Mod,
            BinaryOp::BitAnd,
            BinaryOp::BitOr,
            BinaryOp::BitXor,
            BinaryOp::LeftShift,
            BinaryOp::RightShift,
            BinaryOp::Eq,
            BinaryOp::NullEq,
            BinaryOp::Ne,
            BinaryOp::Lt,
            BinaryOp::Le,
            BinaryOp::Gt,
            BinaryOp::Ge,
            BinaryOp::LogicAnd,
            BinaryOp::LogicOr,
            BinaryOp::LogicXor,
        ] {
            assert_eq!(binary_op_name(operator), operator.opcode().name());
        }
    }

    struct PacketColumns {
        limit: u64,
        warnings: RefCell<Vec<(u16, String)>>,
    }

    #[derive(Default)]
    struct WarningColumns {
        warnings: RefCell<Vec<(u16, String)>>,
    }

    impl Columns for WarningColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }

        fn warning_count(&self) -> usize {
            self.warnings.borrow().len()
        }

        fn truncate_warnings(&self, bookmark: usize) {
            self.warnings.borrow_mut().truncate(bookmark);
        }
    }

    #[test]
    fn logical_and_interval_skip_unreachable_warning_arguments() {
        let ctx = WarningColumns::default();
        let integer = |value| Expression::Constant(Constant::new(Datum::Int(value), ft()));
        let division = || {
            let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
            decimal.set_flen(15);
            decimal.set_decimal(4);
            crate::new_function::new_function_base(
                &crate::NoColumns,
                "div",
                decimal,
                vec![integer(1), integer(0)],
            ).unwrap()
        };

        for (name, first, expected) in [("and", 0, 0), ("or", 1, 1)] {
            let function =
                crate::new_function::new_function_base(&crate::NoColumns, name, ft(), vec![integer(first), division()]).unwrap().into_scalar_function().unwrap();
            assert_eq!(
                function.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
                Datum::Int(expected)
            );
            assert!(ctx.warnings.borrow().is_empty());
        }

        let interval = crate::new_function::new_function_base(
            &crate::NoColumns,
            "interval",
            ft(),
            vec![integer(1), integer(0), integer(1), integer(2), division()],
        ).unwrap().into_scalar_function().unwrap();
        assert_eq!(
            interval.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
            Datum::Int(2)
        );
        assert!(ctx.warnings.borrow().is_empty());

        let mut not_null_int = ft();
        not_null_int.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        let integer =
            |value| Expression::Constant(Constant::new(Datum::Int(value), not_null_int.clone()));
        let unreachable = crate::new_function::new_function_base(
            &crate::NoColumns,
            "plus",
            not_null_int.clone(),
            vec![
                Expression::Constant(Constant::new(Datum::Int(i64::MAX), not_null_int.clone())),
                Expression::Constant(Constant::new(Datum::Int(1), not_null_int.clone())),
            ],
        ).unwrap();
        let interval = crate::new_function::new_function_base(
            &crate::NoColumns,
            "interval",
            ft(),
            vec![integer(1), integer(0), integer(1), integer(2), unreachable],
        ).unwrap().into_scalar_function().unwrap();
        assert_eq!(
            interval.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
            Datum::Int(2)
        );
    }

    impl Columns for PacketColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn max_allowed_packet(&self) -> u64 {
            self.limit
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    #[test]
    fn concat_family_stops_at_the_packet_limit_before_later_arguments() {
        let ctx = PacketColumns {
            limit: 3,
            warnings: RefCell::new(Vec::new()),
        };
        let string =
            |value: &[u8]| Expression::Constant(Constant::new(Datum::new_string_datum(value), text_ft()));
        let unsupported = crate::new_function::new_function_base(
            &crate::NoColumns,
            "plus",
            text_ft(),
            vec![
                Expression::Constant(Constant::new(Datum::Int(i64::MAX), text_ft())),
                Expression::Constant(Constant::new(Datum::Int(1), text_ft())),
            ],
        ).unwrap();
        let concat = crate::new_function::new_function_base(
            &crate::NoColumns,
            "concat",
            text_ft(),
            vec![string(b"abcd"), unsupported],
        ).unwrap().into_scalar_function().unwrap();
        assert_eq!(
            concat.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.warnings.borrow()[0].0, 1301);
        assert!(ctx.warnings.borrow()[0].1.contains("concat()"));

        ctx.warnings.borrow_mut().clear();
        let concat_ws = crate::new_function::new_function_base(
            &crate::NoColumns,
            "concat_ws",
            text_ft(),
            vec![string(b"--"), string(b"a"), string(b"b")],
        ).unwrap().into_scalar_function().unwrap();
        assert_eq!(
            concat_ws.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.warnings.borrow()[0].0, 1301);
        assert!(ctx.warnings.borrow()[0].1.contains("concat_ws()"));

        ctx.warnings.borrow_mut().clear();
        assert_eq!(
            crate::tests::eval_builtin_func(
                "CONCAT",
                &[Datum::new_string_datum("abcd"), Datum::new_string_datum("x")],
                &ctx,
            )
            .unwrap()
            .unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.warnings.borrow()[0].0, 1301);
    }

    #[test]
    fn decimal_round_and_truncate_cap_dynamic_scale_by_the_result_type() {
        let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
        decimal_type.set_flen(10);
        decimal_type.set_decimal(2);
        let decimal = || {
            Expression::Constant(Constant::new(
                Datum::Decimal(tidb_datatype::Decimal::from_literal("1.23")),
                decimal_type.clone(),
            ))
        };
        let scale = || Expression::Constant(Constant::new(Datum::Int(5), ft()));

        for name in ["round", "truncate"] {
            let function = crate::new_function::new_function_base(
                &crate::NoColumns,
                name,
                decimal_type.clone(),
                vec![decimal(), scale()],
            ).unwrap().into_scalar_function().unwrap();
            let value = function
                .eval(&tidb_exprctx::context::NoColumns, tidb_chunk::row::Row::empty())
                .expect("decimal ROUND/TRUNCATE must evaluate");
            let Datum::Decimal(value) = value else {
                panic!("{name} returned a non-decimal value")
            };
            // Go `calculateDecimal4RoundAndTruncate`: a constant scale is the
            // result's scale.
            assert_eq!(value.to_string(), "1.23000", "{name}");
        }
    }

    fn plus(args: Vec<Expression>) -> ScalarFunction {
        crate::new_function::new_function_base(&crate::NoColumns, "plus", ft(), args).unwrap().into_scalar_function().unwrap()
    }

    #[test]
    fn args_and_static_type() {
        let sf = plus(vec![
            Expression::Constant(Constant::new(Datum::Int(1), ft())),
            Expression::Constant(Constant::new(Datum::Int(2), ft())),
        ]);
        assert_eq!(sf.get_args().len(), 2);
        assert!(sf.get_static_type().is_some());
        assert_eq!(sf.const_level(), ConstLevel::STRICT);
    }

    #[test]
    fn const_level_matches_the_source_function_and_argument_rules() {
        let literal = || Expression::Constant(Constant::new(Datum::Int(1), ft()));
        let parameter = || {
            let mut constant = Constant::new(Datum::Int(1), ft());
            constant.param_marker = Some(crate::constant::ParamMarker { order: 0 });
            Expression::Constant(constant)
        };
        let call = |name: &str, args| crate::new_function::new_function_base(&crate::NoColumns, name, ft(), args).unwrap().into_scalar_function().unwrap();

        assert_eq!(
            call("abs", vec![literal()]).const_level(),
            ConstLevel::STRICT
        );
        assert_eq!(
            plus(vec![literal(), parameter()]).const_level(),
            ConstLevel::ONLY_IN_CONTEXT
        );
        assert_eq!(
            plus(vec![literal(), Expression::Column(Column::new(1, ft()))]).const_level(),
            ConstLevel::NONE
        );
        let name_literal =
            || Expression::Constant(Constant::new(Datum::new_string_datum("v"), text_ft()));
        for (name, args) in [
            ("sysdate", vec![]),
            ("found_rows", vec![]),
            ("rand", vec![]),
            ("uuid", vec![]),
            ("uuid_v4", vec![]),
            ("uuid_v7", vec![]),
            ("sleep", vec![literal()]),
            ("row", vec![literal(), literal()]),
            ("setvar", vec![name_literal(), literal()]),
            ("getvar", vec![name_literal()]),
            ("getparam", vec![literal()]),
            ("benchmark", vec![literal(), literal()]),
            ("dayname", vec![literal()]),
            ("nextval", vec![name_literal()]),
            ("lastval", vec![name_literal()]),
            ("setval", vec![name_literal(), literal()]),
            ("any_value", vec![literal()]),
        ] {
            assert_eq!(call(name, args).const_level(), ConstLevel::NONE, "{name}");
        }
        assert_eq!(
            crate::builtin_other::new_values_func(&crate::builtin::ColumnsBuildContext(&crate::NoColumns), 0, ft())
                .unwrap()
                .const_level(),
            ConstLevel::NONE
        );
    }

    #[test]
    fn is_correlated_follows_args() {
        let literal = || Expression::Constant(Constant::new(Datum::Int(1), ft()));
        let plain = plus(vec![Expression::Column(Column::new(1, ft())), literal()]);
        assert!(!plain.is_correlated());

        let corr = plus(vec![
            Expression::CorrelatedColumn(crate::column::CorrelatedColumn {
                column: Column::new(1, ft()),
                data: Default::default(),
            }),
            literal(),
        ]);
        assert!(corr.is_correlated());
    }

    #[test]
    fn hash_code_is_flag_name_and_arg_codes() {
        let mut c1 = Column::new(1, ft());
        let mut c2 = Column::new(2, ft());
        let mut sf = plus(vec![
            Expression::Column(Column::new(1, ft())),
            Expression::Column(Column::new(2, ft())),
        ]);

        let mut expected = vec![SCALAR_FUNCTION_FLAG];
        encode_compact_bytes(&mut expected, b"plus");
        expected.extend_from_slice(c1.hash_code());
        expected.extend_from_slice(c2.hash_code());
        assert_eq!(sf.hash_code(), expected.as_slice());
        // Cached.
        assert_eq!(sf.hash_code(), expected.as_slice());
    }

    #[test]
    fn cast_hash_code_includes_eval_type_byte() {
        let mut sf = crate::builtin_cast::cast_function_for_test(Expression::Column(Column::new(1, ft())), FieldType::new(FieldTypeCode::Long));
        let hc = sf.hash_code().to_vec();
        // Ends with the target EvalType byte (Long -> Int == 0).
        assert_eq!(*hc.last().unwrap(), tidb_datatype::EvalType::Int as u8);
    }

    #[test]
    fn eval_bridges_values_only_builtins() {
        use tidb_exprctx::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&ft()), 1);
        let mut chk = chk;
        chk.append_int64(0, 0);
        let row = chk.get_row(0);
        let konst = |d: Datum| Expression::Constant(Constant::new(d, ft()));

        // ABS(-5) = 5 via math_fn's shared values-only dispatch.
        let abs = crate::new_function::new_function_base(&crate::NoColumns, "abs", ft(), vec![konst(Datum::Int(-5))]).unwrap().into_scalar_function().unwrap();
        assert_eq!(abs.eval(&NoColumns, row).unwrap(), Datum::Int(5));

        // CONCAT('a', 'b') = 'ab' via the shared string arm.
        let concat = crate::new_function::new_function_base(
            &crate::NoColumns,
            "concat",
            text_ft(),
            vec![
                Expression::Constant(Constant::new(Datum::new_string_datum("a"), text_ft())),
                Expression::Constant(Constant::new(Datum::new_string_datum("b"), text_ft())),
            ],
        ).unwrap().into_scalar_function().unwrap();
        assert_eq!(
            concat.eval(&NoColumns, row).unwrap(),
            Datum::new_string_datum("ab")
        );

        // COALESCE(NULL, 7) = 7 (eager over values, matching the old
        // AST-level evaluator).
        let coalesce = crate::new_function::new_function_base(
            &crate::NoColumns,
            "coalesce",
            ft(),
            vec![konst(Datum::Null), konst(Datum::Int(7))],
        ).unwrap().into_scalar_function().unwrap();
        assert_eq!(coalesce.eval(&NoColumns, row).unwrap(), Datum::Int(7));

        // `InferType4ControlFuncs` widens the result metadata to the widest
        // branch scale, and `bf.tp = resultFieldType` makes that merged type
        // the one the selected argument is presented as: recorded TiDB
        // answers `select coalesce(1, 2.55, 3)` with `1.00`.
        let mut decimal_four = FieldType::new(FieldTypeCode::NewDecimal);
        decimal_four.set_flen(15);
        decimal_four.set_decimal(4);
        let decimal_coalesce = crate::new_function::new_function_base(
            &crate::NoColumns,
            "coalesce",
            decimal_four.clone(),
            vec![
                konst(Datum::Int(1)),
                Expression::Constant(Constant::new(Datum::Null, decimal_four.clone())),
            ],
        ).unwrap().into_scalar_function().unwrap();
        assert_eq!(
            decimal_coalesce.eval(&NoColumns, row).unwrap(),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("1.0000"))
        );

        // A column argument feeds the builtin from the chunk row: ABS(col).
        let mut col = Column::new(1, ft());
        col.index = 0;
        let mut chk2 = Chunk::new_chunk_with_capacity(std::slice::from_ref(&ft()), 1);
        chk2.append_int64(0, -9);
        let abs_col =
            crate::new_function::new_function_base(&crate::NoColumns, "abs", ft(), vec![Expression::Column(col)]).unwrap().into_scalar_function().unwrap();
        assert_eq!(
            abs_col.eval(&NoColumns, chk2.get_row(0)).unwrap(),
            Datum::Int(9)
        );

        // `IF` is a lazy control form now, so it evaluates here: the
        // condition picks the branch (this used to be the example of a
        // function outside the values-only entry).
        let iff = crate::new_function::new_function_base(
            &crate::NoColumns,
            "if",
            ft(),
            vec![
                konst(Datum::Int(1)),
                konst(Datum::Int(2)),
                konst(Datum::Int(3)),
            ],
        ).unwrap().into_scalar_function().unwrap();
        assert_eq!(iff.eval(&NoColumns, row).unwrap(), Datum::Int(2));
    }

    /// `JSON_ARRAY`/`CAST(... AS JSON)` over a column whose static `FieldType`
    /// is BINARY-charset render the JSON `Opaque` value real TiDB produces,
    /// not a plain JSON string -- the chunk path threads
    /// `Expression::static_type()` into `builtin_ext::json::dispatch_typed`/
    /// `cast_as_json_typed`. Captured: `SELECT JSON_ARRAY(vb), CAST(vb AS
    /// JSON) FROM t` where `vb varbinary(8)` holds `'ab'`
    /// (`zz_dump_frozjson_test.go`, `TestZZDumpFrozJSONBinaryOpaque`).
    #[test]
    fn eval_renders_binary_charset_column_as_json_opaque() {
        use tidb_exprctx::context::NoColumns;
        use tidb_chunk::chunk::Chunk;
        use tidb_datatype::FieldTypeCode;
use tidb_charset::Collation;

        let varbinary_type =
            FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        let json_text_type = text_ft();
        let mut chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&varbinary_type), 1);
        chk.append_bytes(0, b"ab");
        let row = chk.get_row(0);

        let mut col = Column::new(1, varbinary_type);
        col.index = 0;

        let json_array = crate::new_function::new_function_base(
            &crate::NoColumns,
            "json_array",
            json_text_type,
            vec![Expression::Column(col.clone())],
        ).unwrap().into_scalar_function().unwrap();
        let Datum::Json(json) = json_array.eval(&NoColumns, row).unwrap() else {
            panic!("JSON_ARRAY returns JSON")
        };
        assert_eq!(json.to_string(), r#"["base64:type15:YWI="]"#);

        let cast_json = crate::builtin_cast::cast_function_for_test(Expression::Column(col), FieldType::new(FieldTypeCode::Json));
        let Datum::Json(got) = cast_json.eval(&NoColumns, row).unwrap() else {
            panic!("CAST AS JSON did not retain the JSON domain")
        };
        let opaque = got.opaque().expect("opaque binary JSON");
        assert_eq!(opaque.type_code, 15);
        assert_eq!(opaque.bytes, b"ab");
    }

    #[test]
    fn rand_reuses_one_generator_per_node_for_a_constant_seed() {
        use std::cell::Cell;
        use tidb_chunk::chunk::Chunk;

        // A minimal session whose `rand_seeded_next` records the key it was
        // called with and returns a fixed value, so this asserts the SAME
        // key reaches it across repeated evaluations of one `ScalarFunction`
        // node.
        struct RandColumns {
            keys: Cell<Vec<usize>>,
        }
        impl Columns for RandColumns {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn rand_seeded_next(&self, key: usize, _seed: i64) -> Option<f64> {
                let mut keys = self.keys.take();
                keys.push(key);
                self.keys.set(keys);
                Some(0.5)
            }
        }

        let chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&ft()), 1);
        let row = chk.get_row(0);
        let konst = |d: Datum| Expression::Constant(Constant::new(d, ft()));
        let rand_five =
            crate::new_function::new_function_base(&crate::NoColumns, "rand", real_ft(), vec![konst(Datum::Int(5))]).unwrap().into_scalar_function().unwrap();
        let columns = RandColumns {
            keys: Cell::new(Vec::new()),
        };

        assert_eq!(rand_five.eval(&columns, row).unwrap(), Datum::Real(0.5));
        assert_eq!(rand_five.eval(&columns, row).unwrap(), Datum::Real(0.5));
        let keys = columns.keys.into_inner();
        assert_eq!(keys.len(), 2);
        // The SAME node produced the same key both times.
        assert_eq!(keys[0], keys[1]);

        // A DIFFERENT node (a different RAND(5) call site) gets a different
        // key, because each owns its own generator.
        let rand_five_again =
            crate::new_function::new_function_base(&crate::NoColumns, "rand", real_ft(), vec![konst(Datum::Int(5))]).unwrap().into_scalar_function().unwrap();
        let columns2 = RandColumns {
            keys: Cell::new(Vec::new()),
        };
        rand_five_again.eval(&columns2, row).unwrap();
        assert_ne!(columns2.keys.into_inner()[0], keys[0]);
    }

    #[test]
    fn rand_with_no_args_reads_the_running_generator() {
        use tidb_chunk::chunk::Chunk;

        struct SeqColumns {
            next: std::cell::Cell<f64>,
        }
        impl Columns for SeqColumns {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn rand_next(&self) -> Option<f64> {
                Some(self.next.get())
            }
        }

        let chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&ft()), 1);
        let row = chk.get_row(0);
        let rand = crate::new_function::new_function_base(&crate::NoColumns, "rand", real_ft(), vec![]).unwrap().into_scalar_function().unwrap();
        let columns = SeqColumns {
            next: std::cell::Cell::new(0.75),
        };
        assert_eq!(rand.eval(&columns, row).unwrap(), Datum::Real(0.75));
    }

    #[test]
    fn different_functions_hash_differently() {
        let column = || Expression::Column(Column::new(1, ft()));
        let mut a = plus(vec![column(), column()]);
        let mut b = crate::new_function::new_function_base(
            &crate::NoColumns,
            "minus",
            ft(),
            vec![column(), column()],
        ).unwrap().into_scalar_function().unwrap();
        assert_ne!(a.hash_code(), b.hash_code());
    }

    #[test]
    fn string_in_prepares_collation_keys_for_strict_literals() {
        use tidb_chunk::chunk::Chunk;

        let result_type = ft();
        let mut string_type = text_ft();
        string_type.set_charset_name("utf8mb4");
        string_type.set_collation_name("utf8mb4_general_ci");
        let string = |value: &str| {
            Expression::Constant(Constant::new(Datum::new_string_datum(value), string_type.clone()))
        };
        let mut function = crate::new_function::new_function_base(
            &crate::NoColumns,
            "in",
            result_type,
            vec![string("a"), string("A"), string("b")],
        ).unwrap().into_scalar_function().unwrap();
        function.prepare_in_string_hash_set();

        assert_eq!(function.in_string_hash_set.as_ref().unwrap().len(), 2);
        assert!(function.in_string_non_const_args.is_empty());
        let chunk = Chunk::new_chunk_with_capacity(&[], 1);
        assert_eq!(
            function.eval(&crate::NoColumns, chunk.get_row(0)).unwrap(),
            Datum::Int(1)
        );
    }

    #[test]
    fn string_in_raw_key_probe_preserves_go_collation_semantics() {
        use tidb_chunk::chunk::Chunk;
        use tidb_charset::Collation;

        let typed = |value: &str, field: &FieldType| {
            Expression::Constant(Constant::new(Datum::new_string_datum(value), field.clone()))
        };
        let string = |value: &str| typed(value, &text_ft());
        let chunk = Chunk::new_chunk_with_capacity(&[], 1);

        // Go's `builtinInStringSig.buildHashMapForConstArgs` stores the
        // `collator.Key` result and `evalInt` probes the same key. Binary and
        // derived-binary collators return the source bytes directly, so the
        // Rust probe may borrow them without changing membership behavior.
        let binary_type = text_ft().with_collation(Collation::Binary);
        let mut binary = crate::new_function::new_function_base(
            &crate::NoColumns,
            "in",
            ft(),
            vec![typed("a", &binary_type), typed("a", &binary_type)],
        ).unwrap().into_scalar_function().unwrap();
        binary.prepare_in_string_hash_set();
        assert_eq!(
            binary.eval(&crate::NoColumns, chunk.get_row(0)).unwrap(),
            Datum::Int(1)
        );

        // PAD SPACE collations still allocate a trimmed sort key. This is
        // the source behavior for `utf8mb4_bin`: `a` and `a ` compare equal.
        let pad_type = text_ft().with_collation(Collation::Utf8Mb4Bin);
        let mut pad = crate::new_function::new_function_base(
            &crate::NoColumns,
            "in",
            ft(),
            vec![typed("a ", &pad_type), typed("a", &pad_type)],
        ).unwrap().into_scalar_function().unwrap();
        pad.prepare_in_string_hash_set();
        assert_eq!(
            pad.eval(&crate::NoColumns, chunk.get_row(0)).unwrap(),
            Datum::Int(1)
        );

        // The Go map is built once for the immutable literal list. Reserving
        // that list's complete size avoids growth/rehash when a query has a
        // large `IN (...)` predicate (the hbx swap query has 1000 literals).
        let mut many = Vec::with_capacity(1001);
        many.push(string("probe"));
        for value in 0..1000 {
            many.push(string(&format!("maker-{value}")));
        }
        let mut large = crate::new_function::new_function_base(&crate::NoColumns, "in", ft(), many).unwrap().into_scalar_function().unwrap();
        large.prepare_in_string_hash_set();
        assert!(large.in_string_hash_set.as_ref().unwrap().capacity() >= 1000);
    }
    /// The stock-level shape: `ge(ol_o_id, minus(d_next_o_id, 20))` over a
    /// joined chunk row -- the exact residual predicate class jTPCC's
    /// stock-level statement evaluates once per hash-join candidate, mixing
    /// PROBE-side and BUILD-side columns. Pins that the typed fast path
    /// answers the composed tree.
    #[test]
    fn fast_integer_path_answers_the_join_residual_shape() {
        use tidb_exprctx::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let probe_col = || {
            let mut col = Column::new(1, ft());
            col.index = 0;
            Expression::Column(col)
        };
        let build_col = || {
            let mut col = Column::new(2, ft());
            col.index = 1;
            Expression::Column(col)
        };
        let konst = |d: Datum| Expression::Constant(Constant::new(d, ft()));

        // Joined row: ol_o_id = 100, d_next_o_id = 105 -> minus(...) = 85 ->
        // 100 >= 85 is true. One below the join boundary: ol_o_id = 84 ->
        // 84 >= 85 is false.
        let inner = crate::new_function::new_function_base(
            &crate::NoColumns,
            "minus",
            ft(),
            vec![build_col(), konst(Datum::Int(20))],
        ).unwrap().into_scalar_function().unwrap();
        let ge = crate::new_function::new_function_base(
            &crate::NoColumns,
            "ge",
            ft(),
            vec![probe_col(), Expression::ScalarFunction(inner)],
        ).unwrap().into_scalar_function().unwrap();

        let mut above = Chunk::new_chunk_with_capacity(&[ft(), ft()], 1);
        above.append_int64(0, 100);
        above.append_int64(1, 105);
        assert_eq!(
            ge.eval(&NoColumns, above.get_row(0)).unwrap(),
            Datum::Int(1)
        );

        let mut below = Chunk::new_chunk_with_capacity(&[ft(), ft()], 1);
        below.append_int64(0, 84);
        below.append_int64(1, 105);
        assert_eq!(
            ge.eval(&NoColumns, below.get_row(0)).unwrap(),
            Datum::Int(0)
        );
    }

    /// NULL propagation on the fast path matches the ladder's answer for
    /// every covered operator family: arithmetic stays NULL, comparisons
    /// stay NULL (never false).
    #[test]
    fn fast_integer_path_propagates_null_like_the_ladder() {
        use tidb_exprctx::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&ft()), 1);
        let mut chk = chk;
        chk.append_int64(0, 7);
        let row = chk.get_row(0);
        let konst = |d: Datum, t: &FieldType| Expression::Constant(Constant::new(d, t.clone()));

        for name in ["plus", "minus", "mul", "eq", "ne", "lt", "le", "gt", "ge"] {
            let function = crate::new_function::new_function_base(
                &crate::NoColumns,
                name,
                ft(),
                vec![konst(Datum::Null, &ft()), konst(Datum::Int(3), &ft())],
            ).unwrap().into_scalar_function().unwrap();
            assert_eq!(
                function.eval(&NoColumns, row).unwrap(),
                Datum::Null,
                "{name} NULL lhs"
            );
            let function = crate::new_function::new_function_base(
                &crate::NoColumns,
                name,
                ft(),
                vec![konst(Datum::Int(3), &ft()), konst(Datum::Null, &ft())],
            ).unwrap().into_scalar_function().unwrap();
            assert_eq!(
                function.eval(&NoColumns, row).unwrap(),
                Datum::Null,
                "{name} NULL rhs"
            );
        }
    }

    /// A mixed signed/unsigned comparison orders by VALUE, not by bit
    /// pattern: `-1 < 1u` because Go's compare treats the signed side as
    /// negative against an unsigned partner. The fast path reinterprets each
    /// operand through its own argument field type before comparing.
    #[test]
    fn fast_integer_path_orders_mixed_signedness_by_value() {
        use tidb_exprctx::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let signed_type = ft();
        let unsigned_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
        assert!(unsigned_type.is_unsigned());

        let mut signed_chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&signed_type), 1);
        signed_chk.append_int64(0, -1);
        let signed_row = signed_chk.get_row(0);

        let unsigned_chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&unsigned_type), 1);
        let mut unsigned_chk = unsigned_chk;
        unsigned_chk.append_uint64(0, 1);
        let unsigned_row = unsigned_chk.get_row(0);

        let signed_col = {
            let mut col = Column::new(1, signed_type.clone());
            col.index = 0;
            move || Expression::Column(col.clone())
        };
        let unsigned_col = {
            let mut col = Column::new(2, unsigned_type.clone());
            col.index = 0;
            move || Expression::Column(col.clone())
        };

        // The joined rows are assembled left-then-right in one scratch chunk:
        // column 0 reads the SIGNED value, column 1 the UNSIGNED one. Build
        // that exact two-column row and compare across it.
        let mut joined = Chunk::new_chunk_with_capacity(&[signed_type, unsigned_type], 1);
        joined.append_int64(0, -1);
        joined.append_uint64(1, 1);
        let joined_row = joined.get_row(0);
        let _ = (signed_row, unsigned_row);

        for (name, op_name, want) in [
            ("lt", "lt", 1u8),
            ("le", "le", 1),
            ("gt", "gt", 0),
            ("ge", "ge", 0),
            ("eq", "eq", 0),
            ("ne", "ne", 1),
        ] {
            let function = crate::new_function::new_function_base(
                &crate::NoColumns,
                op_name,
                ft(),
                vec![signed_col(), unsigned_col()],
            ).unwrap().into_scalar_function().unwrap();
            assert_eq!(
                function.eval(&NoColumns, joined_row).unwrap(),
                Datum::Int(want as i64),
                "{name}(-1, 1u)"
            );
        }
    }

    /// Unsigned subtraction past zero is `ErrOverflow` with Go's BIGINT
    /// UNSIGNED wording (1690), not a wrapped value: `1u - 2` overflows.
    #[test]
    fn fast_integer_path_reports_unsigned_subtraction_overflow() {
        use tidb_exprctx::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let unsigned_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);

        let mut chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&unsigned_type), 1);
        chk.append_uint64(0, 1);
        let row = chk.get_row(0);
        let mut col = Column::new(1, unsigned_type.clone());
        col.index = 0;
        let konst = |d: Datum, t: &FieldType| Expression::Constant(Constant::new(d, t.clone()));

        let function = crate::new_function::new_function_base(
            &crate::NoColumns,
            "minus",
            unsigned_type.clone(),
            vec![
                Expression::Column(col),
                konst(Datum::Int(2), &unsigned_type),
            ],
        ).unwrap().into_scalar_function().unwrap();
        let error = function
            .eval(&NoColumns, row)
            .expect_err("1u - 2 must overflow");
        assert_eq!(
            error,
            EvalError::DataOutOfRange {
                value: "BIGINT UNSIGNED",
                expression: "(Column#1 - 2)".to_owned(),
            }
        );
    }

    /// A non-ETInt argument keeps the generic ladder: string comparison
    /// semantics are untouched by the gate.
    #[test]
    fn fast_integer_gate_leaves_string_operands_on_the_ladder() {
        use tidb_exprctx::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let string_type = text_ft();
        let chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&string_type), 1);
        let mut chk = chk;
        chk.append_string(0, b"a".as_slice());
        let row = chk.get_row(0);
        let konst = |d: Datum| Expression::Constant(Constant::new(d, string_type.clone()));

        let function = crate::new_function::new_function_base(
            &crate::NoColumns,
            "ge",
            ft(),
            vec![konst(Datum::new_string_datum("a")), konst(Datum::new_string_datum("b"))],
        ).unwrap().into_scalar_function().unwrap();
        assert_eq!(function.eval(&NoColumns, row).unwrap(), Datum::Int(0));
    }

    /// TiDB capture over `create table g (i int, d decimal(10,3))` holding
    /// `(-5, 2.500)`: `select least(i, d)` is `-5` with datum frac 0 -- the
    /// winner keeps the winning ARGUMENT's own decimal (Go casts integers to
    /// `SetDecimal(0)`), not the aggregated max (3), and `select least(1, d)`
    /// is `1` the same way. A fully-constant call is the one exception: the
    /// planner folds it to the RETURN type's scale, so `select least(1, 2.5)`
    /// is `1.0` (max argument decimal).
    #[test]
    fn extremum_over_typed_columns_keeps_the_winner_own_scale() {
        let int_ft = FieldType::new(FieldTypeCode::LongLong);
        let mut dec_ft = FieldType::new(FieldTypeCode::NewDecimal);
        dec_ft.set_flen(10);
        dec_ft.set_decimal(3);
        let mut ret_ft = FieldType::new(FieldTypeCode::NewDecimal);
        ret_ft.set_flen(11);
        ret_ft.set_decimal(3);

        let mut col_i = crate::column::Column::new(1, int_ft.clone());
        col_i.index = 0;
        let mut col_d = crate::column::Column::new(2, dec_ft.clone());
        col_d.index = 1;
        let least = crate::new_function::new_function_base(
            &crate::NoColumns,
            "least",
            ret_ft.clone(),
            vec![Expression::Column(col_i), Expression::Column(col_d)],
        ).unwrap();

        let mut chunk =
            tidb_chunk::chunk::Chunk::new_chunk_with_capacity(&[int_ft.clone(), dec_ft.clone()], 1);
        chunk.append_datum(0, &Datum::Int(-5));
        chunk.append_datum(
            1,
            &Datum::Decimal(tidb_datatype::Decimal::from_literal("2.500")),
        );
        let datum = least
            .eval(&tidb_exprctx::context::NoColumns, chunk.get_row(0))
            .unwrap();
        let Datum::Decimal(dec) = &datum else {
            panic!("least over a decimal aggregate answers a decimal");
        };
        assert_eq!(dec.scale(), 0, "the integer winner keeps its own scale 0");
        assert_eq!(dec.to_string(), "-5");

        // Mixed constant + column: `least(1, d)` is `1` (frac 0), not `1.000`.
        let mut col_d = crate::column::Column::new(2, dec_ft.clone());
        col_d.index = 0;
        let least_mixed = crate::new_function::new_function_base(
            &crate::NoColumns,
            "least",
            ret_ft,
            vec![
                Expression::Constant(Constant::new(Datum::Int(1), int_ft.clone())),
                Expression::Column(col_d),
            ],
        ).unwrap();
        let mut chunk = tidb_chunk::chunk::Chunk::new_chunk_with_capacity(&[dec_ft], 1);
        chunk.append_datum(
            0,
            &Datum::Decimal(tidb_datatype::Decimal::from_literal("2.500")),
        );
        let datum = least_mixed
            .eval(&tidb_exprctx::context::NoColumns, chunk.get_row(0))
            .unwrap();
        let Datum::Decimal(dec) = &datum else {
            panic!("least over a decimal aggregate answers a decimal");
        };
        assert_eq!(
            dec.scale(),
            0,
            "the constant winner keeps its own scale 0 too"
        );
        assert_eq!(dec.to_string(), "1");
    }

    fn empty_row() -> tidb_chunk::row::Row<'static> {
        let chunk = Box::leak(Box::new(tidb_chunk::chunk::Chunk::new_empty_chunk(&[])));
        chunk.get_row(0)
    }

    /// Go `builtinTiDBIsDDLOwnerSig.evalInt` (`builtin_info.go:627`) reads
    /// the `DDLOwnerInfo` optional eval prop and answers 1/0, never NULL.
    /// A context without the provider fails with Go's `getPropProvider`
    /// error -- NOT the generic not-yet-ported fallback -- and an extra
    /// argument is 1582, the same refusal the other zero-arg builtins raise.
    #[test]
    fn tidb_is_ddl_owner_reports_the_missing_provider_like_go() {
        let function = crate::new_function::new_function_base(
            &crate::NoColumns,
            "tidb_is_ddl_owner",
            FieldType::new(FieldTypeCode::LongLong),
            vec![],
        ).unwrap();
        let error = function
            .eval(&tidb_exprctx::context::NoColumns, empty_row())
            .unwrap_err();
        assert!(
            matches!(
                &error,
                EvalError::Unsupported(message)
                    if message.contains(
                        "optional property: 'OptPropDDLOwnerInfo' not exists in EvalContext"
                    )
            ),
            "{error:?}"
        );

        let function = crate::new_function::new_function_base(
            &crate::NoColumns,
            "tidb_is_ddl_owner",
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Constant(Constant::new(
                Datum::Int(1),
                FieldType::new(FieldTypeCode::LongLong),
            ))],
        );
        assert_eq!(
            function.err(),
            Some(EvalError::WrongParameterCount("tidb_is_ddl_owner"))
        );
    }
}

/// Go `builtinConvSig.evalString`'s argument test, which
/// `scalarExprSupportedByTiKV` repeats to keep such a CONV in TiDB: when the
/// digit-string argument is a cast to a string over a hybrid (BIT, ENUM, SET)
/// value or a binary literal, CONV reads the value's bits rather than its
/// text.
///
/// Go wraps an argument that is not already a string in that cast
/// (`WrapWithCastAsString`); this tier imposes the cast while evaluating
/// instead, so for such an argument the argument itself is the cast's
/// operand. A string-typed argument is tested as Go tests it: a cast whose
/// operand is hybrid or a binary literal. An explicit `CAST(b AS CHAR)` of a
/// BIT is not one -- `castAsStringFunctionClass` casts the BIT to binary
/// first.
#[must_use]
pub fn conv_reads_bits(argument: &Expression) -> bool {
    fn hybrid_or_binary_literal(operand: &Expression) -> bool {
        operand.static_type().is_some_and(FieldType::is_hybrid)
            || matches!(operand, Expression::Constant(constant)
                if matches!(constant.value, Datum::BinaryLiteral(_)))
    }
    let string_typed = argument
        .static_type()
        .is_some_and(|field_type| field_type.eval_type() == tidb_datatype::EvalType::String);
    if !string_typed {
        return hybrid_or_binary_literal(argument);
    }
    let Expression::ScalarFunction(function) = argument else {
        return false;
    };
    crate::expr_util::builder::is_cast_name(&function.func_name.lowercase())
        && function.args.first().is_some_and(hybrid_or_binary_literal)
}
