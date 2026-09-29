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

//! Go `getSignatureByPB`: select an implementation from the wire enum.
//! SQL function names are diagnostic metadata, never an overload selector here.

use super::{cast_json_argument_value, eval_numeric_operand_row, eval_numeric_row, ScalarFunction};
use crate::context::{Columns, EvalError};
use crate::expression::ConstLevel;
use tidb_ast::BinaryOp;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, EvalType, FieldType};
use tidb_proto::tipb::ScalarFuncSig;

type ValuesKernel = fn(&[Datum], &dyn Columns) -> Result<Datum, EvalError>;

#[derive(Clone, Copy, Debug)]
enum Kernel {
    Binary(BinaryOp, EvalType),
    /// Go comparisons evaluate both typed operands before applying NULL semantics.
    Compare(BinaryOp, EvalType),
    In(EvalType),
    Like,
    Conv,
    DateArithmetic {
        subtract: bool,
    },
    Logic(BinaryOp),
    IntegerMod {
        unsigned: [bool; 2],
    },
    IntegerMultiply {
        unsigned: bool,
    },
    IsNull(EvalType),
    Truth {
        negate: bool,
        domain: EvalType,
    },
    Case(EvalType),
    If(EvalType),
    IfNull(EvalType),
    Cast {
        source: EvalType,
        target: EvalType,
    },
    String {
        operation: StringOp,
        binary: bool,
    },
    Round,
    FromUnixTime,
    Regexp,
    Json,
    Values(ValuesKernel),
}

#[derive(Clone, Copy, Debug)]
enum StringOp {
    Length,
    Upper,
    Lower,
    Substring,
}

/// The selected implementation survives cloning independently of FuncName.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PbBuiltin {
    signature: ScalarFuncSig,
    kernel: Kernel,
}

impl PbBuiltin {
    pub(crate) fn signature(self) -> ScalarFuncSig {
        self.signature
    }

    pub(crate) fn new(signature: ScalarFuncSig) -> Option<Self> {
        use ScalarFuncSig::*;
        let kernel = match signature {
            PlusInt => Kernel::Binary(BinaryOp::Plus, EvalType::Int),
            PlusReal => Kernel::Binary(BinaryOp::Plus, EvalType::Real),
            MinusInt => Kernel::Binary(BinaryOp::Minus, EvalType::Int),
            MinusReal => Kernel::Binary(BinaryOp::Minus, EvalType::Real),
            MultiplyInt => Kernel::IntegerMultiply { unsigned: false },
            MultiplyIntUnsigned => Kernel::IntegerMultiply { unsigned: true },
            MultiplyReal => Kernel::Binary(BinaryOp::Mul, EvalType::Real),
            IntDivideInt => Kernel::Binary(BinaryOp::IntDiv, EvalType::Int),
            IntDivideDecimal => Kernel::Binary(BinaryOp::IntDiv, EvalType::Decimal),
            PlusDecimal => Kernel::Binary(BinaryOp::Plus, EvalType::Decimal),
            MinusDecimal => Kernel::Binary(BinaryOp::Minus, EvalType::Decimal),
            MultiplyDecimal => Kernel::Binary(BinaryOp::Mul, EvalType::Decimal),
            DivideReal => Kernel::Binary(BinaryOp::Div, EvalType::Real),
            DivideDecimal => Kernel::Binary(BinaryOp::Div, EvalType::Decimal),
            ModReal => Kernel::Binary(BinaryOp::Mod, EvalType::Real),
            ModDecimal => Kernel::Binary(BinaryOp::Mod, EvalType::Decimal),
            ModIntSignedSigned => Kernel::IntegerMod {
                unsigned: [false, false],
            },
            ModIntSignedUnsigned => Kernel::IntegerMod {
                unsigned: [false, true],
            },
            ModIntUnsignedSigned => Kernel::IntegerMod {
                unsigned: [true, false],
            },
            ModIntUnsignedUnsigned => Kernel::IntegerMod {
                unsigned: [true, true],
            },
            LtInt => Kernel::Compare(BinaryOp::Lt, EvalType::Int),
            LeInt => Kernel::Compare(BinaryOp::Le, EvalType::Int),
            GtInt => Kernel::Compare(BinaryOp::Gt, EvalType::Int),
            GeInt => Kernel::Compare(BinaryOp::Ge, EvalType::Int),
            EqInt => Kernel::Compare(BinaryOp::Eq, EvalType::Int),
            NeInt => Kernel::Compare(BinaryOp::Ne, EvalType::Int),
            LtReal => Kernel::Compare(BinaryOp::Lt, EvalType::Real),
            LeReal => Kernel::Compare(BinaryOp::Le, EvalType::Real),
            GtReal => Kernel::Compare(BinaryOp::Gt, EvalType::Real),
            GeReal => Kernel::Compare(BinaryOp::Ge, EvalType::Real),
            EqReal => Kernel::Compare(BinaryOp::Eq, EvalType::Real),
            NeReal => Kernel::Compare(BinaryOp::Ne, EvalType::Real),
            LtDecimal => Kernel::Compare(BinaryOp::Lt, EvalType::Decimal),
            LeDecimal => Kernel::Compare(BinaryOp::Le, EvalType::Decimal),
            GtDecimal => Kernel::Compare(BinaryOp::Gt, EvalType::Decimal),
            GeDecimal => Kernel::Compare(BinaryOp::Ge, EvalType::Decimal),
            EqDecimal => Kernel::Compare(BinaryOp::Eq, EvalType::Decimal),
            NeDecimal => Kernel::Compare(BinaryOp::Ne, EvalType::Decimal),
            LtString => Kernel::Compare(BinaryOp::Lt, EvalType::String),
            LeString => Kernel::Compare(BinaryOp::Le, EvalType::String),
            GtString => Kernel::Compare(BinaryOp::Gt, EvalType::String),
            GeString => Kernel::Compare(BinaryOp::Ge, EvalType::String),
            EqString => Kernel::Compare(BinaryOp::Eq, EvalType::String),
            NeString => Kernel::Compare(BinaryOp::Ne, EvalType::String),
            LtTime => Kernel::Compare(BinaryOp::Lt, EvalType::Datetime),
            LeTime => Kernel::Compare(BinaryOp::Le, EvalType::Datetime),
            GtTime => Kernel::Compare(BinaryOp::Gt, EvalType::Datetime),
            GeTime => Kernel::Compare(BinaryOp::Ge, EvalType::Datetime),
            EqTime => Kernel::Compare(BinaryOp::Eq, EvalType::Datetime),
            NeTime => Kernel::Compare(BinaryOp::Ne, EvalType::Datetime),
            InInt => Kernel::In(EvalType::Int),
            InString => Kernel::In(EvalType::String),
            LikeSig => Kernel::Like,
            LogicalAnd => Kernel::Logic(BinaryOp::LogicAnd),
            LogicalOr => Kernel::Logic(BinaryOp::LogicOr),
            IntIsNull => Kernel::IsNull(EvalType::Int),
            RealIsNull => Kernel::IsNull(EvalType::Real),
            DecimalIsNull => Kernel::IsNull(EvalType::Decimal),
            StringIsNull => Kernel::IsNull(EvalType::String),
            TimeIsNull => Kernel::IsNull(EvalType::Datetime),
            DurationIsNull => Kernel::IsNull(EvalType::Duration),
            VectorFloat32IsNull => Kernel::IsNull(EvalType::VectorFloat32),
            UnaryNotInt => Kernel::Truth {
                negate: true,
                domain: EvalType::Int,
            },
            IntIsTrueWithNull => Kernel::Truth {
                negate: false,
                domain: EvalType::Int,
            },
            RealIsTrueWithNull => Kernel::Truth {
                negate: false,
                domain: EvalType::Real,
            },
            DecimalIsTrueWithNull => Kernel::Truth {
                negate: false,
                domain: EvalType::Decimal,
            },
            CaseWhenString => Kernel::Case(EvalType::String),
            CaseWhenInt => Kernel::Case(EvalType::Int),
            CaseWhenReal => Kernel::Case(EvalType::Real),
            CaseWhenDecimal => Kernel::Case(EvalType::Decimal),
            CaseWhenTime => Kernel::Case(EvalType::Datetime),
            CaseWhenDuration => Kernel::Case(EvalType::Duration),
            CaseWhenJson => Kernel::Case(EvalType::Json),
            IfString => Kernel::If(EvalType::String),
            IfInt => Kernel::If(EvalType::Int),
            IfReal => Kernel::If(EvalType::Real),
            IfDecimal => Kernel::If(EvalType::Decimal),
            IfTime => Kernel::If(EvalType::Datetime),
            IfDuration => Kernel::If(EvalType::Duration),
            IfJson => Kernel::If(EvalType::Json),
            IfNullInt => Kernel::IfNull(EvalType::Int),
            IfNullReal => Kernel::IfNull(EvalType::Real),
            IfNullDecimal => Kernel::IfNull(EvalType::Decimal),
            IfNullString => Kernel::IfNull(EvalType::String),
            IfNullTime => Kernel::IfNull(EvalType::Datetime),
            IfNullDuration => Kernel::IfNull(EvalType::Duration),
            IfNullJson => Kernel::IfNull(EvalType::Json),
            CharLength => Kernel::String {
                operation: StringOp::Length,
                binary: true,
            },
            CharLengthUtf8 => Kernel::String {
                operation: StringOp::Length,
                binary: false,
            },
            Upper => Kernel::String {
                operation: StringOp::Upper,
                binary: true,
            },
            UpperUtf8 => Kernel::String {
                operation: StringOp::Upper,
                binary: false,
            },
            Lower => Kernel::String {
                operation: StringOp::Lower,
                binary: true,
            },
            LowerUtf8 => Kernel::String {
                operation: StringOp::Lower,
                binary: false,
            },
            Substring2Args | Substring3Args => Kernel::String {
                operation: StringOp::Substring,
                binary: true,
            },
            Substring2ArgsUtf8 | Substring3ArgsUtf8 => Kernel::String {
                operation: StringOp::Substring,
                binary: false,
            },
            Acos => Kernel::Values(crate::math_fn::acos),
            Asin => Kernel::Values(crate::math_fn::asin),
            Atan1Arg => Kernel::Values(crate::math_fn::atan),
            Atan2Args => Kernel::Values(crate::math_fn::atan2),
            Cos => Kernel::Values(crate::math_fn::cos),
            Cot => Kernel::Values(crate::math_fn::cot),
            Sin => Kernel::Values(crate::math_fn::sin),
            Pow => Kernel::Values(crate::math_fn::pow),
            Pi => Kernel::Values(|values, _| crate::math_fn::pi(values)),
            Conv => Kernel::Conv,
            RoundInt | RoundReal | RoundDec => Kernel::Round,
            Date => Kernel::Values(crate::time_fn::date),
            DateDiff => Kernel::Values(|values, _| crate::time_fn::calendar::date_diff(values)),
            DateFormatSig => Kernel::Values(|values, _| {
                let [date, format] = values else {
                    return Err(EvalError::WrongParameterCount("date_format"));
                };
                crate::time_fn::calendar::date_format(date, format)
            }),
            Hour => Kernel::Values(|values, _| {
                crate::time_fn::calendar::time_part(values, |time| i64::from(time.0))
            }),
            Minute => Kernel::Values(|values, _| {
                crate::time_fn::calendar::time_part(values, |time| i64::from(time.1))
            }),
            Second => Kernel::Values(|values, _| {
                crate::time_fn::calendar::time_part(values, |time| i64::from(time.2))
            }),
            MicroSecond => Kernel::Values(|values, _| crate::time_fn::microsecond(values)),
            Month => Kernel::Values(|values, _| crate::time_fn::month(values)),
            WeekWithoutMode => Kernel::Values(|values, ctx| {
                crate::time_fn::week(values, ctx.default_week_format())
            }),
            TimestampDiff => {
                Kernel::Values(|values, _| crate::time_fn::calendar::timestamp_diff(values))
            }
            UnixTimestampInt | UnixTimestampDec => {
                Kernel::Values(crate::time_fn::session_tz::unix_timestamp)
            }
            FromUnixTime1Arg | FromUnixTime2Arg => Kernel::FromUnixTime,
            RegexpLikeSig => Kernel::Regexp,
            JsonMemberOfSig | JsonReplaceSig | JsonArrayAppendSig | JsonMergePatchSig => {
                Kernel::Json
            }
            AddDateDatetimeDecimal
            | AddDateDatetimeInt
            | AddDateDatetimeReal
            | AddDateDatetimeString
            | AddDateDecimalDecimal
            | AddDateDecimalInt
            | AddDateDecimalReal
            | AddDateDecimalString
            | AddDateDurationDecimal
            | AddDateDurationDecimalDatetime
            | AddDateDurationInt
            | AddDateDurationIntDatetime
            | AddDateDurationReal
            | AddDateDurationRealDatetime
            | AddDateDurationString
            | AddDateDurationStringDatetime
            | AddDateIntDecimal
            | AddDateIntInt
            | AddDateIntReal
            | AddDateIntString
            | AddDateRealDecimal
            | AddDateRealInt
            | AddDateRealReal
            | AddDateRealString
            | AddDateStringDecimal
            | AddDateStringInt
            | AddDateStringReal
            | AddDateStringString => Kernel::DateArithmetic { subtract: false },
            SubDateDatetimeDecimal
            | SubDateDatetimeInt
            | SubDateDatetimeReal
            | SubDateDatetimeString
            | SubDateDecimalDecimal
            | SubDateDecimalInt
            | SubDateDecimalReal
            | SubDateDecimalString
            | SubDateDurationDecimal
            | SubDateDurationDecimalDatetime
            | SubDateDurationInt
            | SubDateDurationIntDatetime
            | SubDateDurationReal
            | SubDateDurationRealDatetime
            | SubDateDurationString
            | SubDateDurationStringDatetime
            | SubDateIntDecimal
            | SubDateIntInt
            | SubDateIntReal
            | SubDateIntString
            | SubDateRealDecimal
            | SubDateRealInt
            | SubDateRealReal
            | SubDateRealString
            | SubDateStringDecimal
            | SubDateStringInt
            | SubDateStringReal
            | SubDateStringString => Kernel::DateArithmetic { subtract: true },
            _ => {
                let (source, target) = cast_types(signature)?;
                Kernel::Cast { source, target }
            }
        };
        Some(Self { signature, kernel })
    }

    pub(super) fn numeric_domain(self) -> Option<EvalType> {
        match self.kernel {
            Kernel::Binary(_, domain) => Some(domain),
            Kernel::IntegerMod { .. } | Kernel::IntegerMultiply { .. } => Some(EvalType::Int),
            _ => None,
        }
    }

    pub(crate) fn is_date_arithmetic(self) -> bool {
        matches!(self.kernel, Kernel::DateArithmetic { .. })
    }

    pub(super) fn eval(
        self,
        function: &ScalarFunction,
        ctx: &dyn Columns,
        row: Row<'_>,
    ) -> Result<Datum, EvalError> {
        let args = &function.args;
        let argument = |index: usize| {
            args.get(index)
                .ok_or(EvalError::Unsupported("missing protobuf builtin argument"))?
                .eval(ctx, row)
        };
        let typed_argument = |index: usize, domain: EvalType| {
            let arg = args
                .get(index)
                .ok_or(EvalError::Unsupported("missing protobuf builtin argument"))?;
            eval_numeric_row(arg, ctx, row, domain)
        };
        let condition = |index: usize| -> Result<Option<bool>, EvalError> {
            let value = typed_argument(index, EvalType::Int)?;
            Ok(crate::arg_eval_type::eval_int(&value)?.map(|value| value != 0))
        };
        match self.kernel {
            Kernel::Case(domain) => {
                let (pairs, remainder) = args.as_chunks::<2>();
                for (index, pair) in pairs.iter().enumerate() {
                    if condition(index * 2)? == Some(true) {
                        return eval_numeric_row(&pair[1], ctx, row, domain);
                    }
                }
                remainder.first().map_or(Ok(Datum::Null), |value| {
                    eval_numeric_row(value, ctx, row, domain)
                })
            }
            Kernel::If(domain) => {
                let branch = if condition(0)? == Some(true) { 1 } else { 2 };
                typed_argument(branch, domain)
            }
            Kernel::IfNull(domain) => {
                let value = typed_argument(0, domain)?;
                if value.is_null() {
                    typed_argument(1, domain)
                } else {
                    Ok(value)
                }
            }
            Kernel::IsNull(domain) => {
                Ok(Datum::Int(i64::from(typed_argument(0, domain)?.is_null())))
            }
            Kernel::Truth { negate, domain } => {
                let value = typed_argument(0, domain)?;
                Ok(crate::truthy_of(&value)?
                    .map_or(Datum::Null, |value| Datum::Int(i64::from(value ^ negate))))
            }
            Kernel::Logic(op) => {
                let left = condition(0)?;
                if (op == BinaryOp::LogicAnd && left == Some(false))
                    || (op == BinaryOp::LogicOr && left == Some(true))
                {
                    return Ok(Datum::Int(i64::from(left.unwrap())));
                }
                let right = condition(1)?;
                Ok(match (op, left, right) {
                    (BinaryOp::LogicAnd, _, Some(false)) => Datum::Int(0),
                    (BinaryOp::LogicOr, _, Some(true)) => Datum::Int(1),
                    (_, None, _) | (_, _, None) => Datum::Null,
                    (_, _, Some(value)) => Datum::Int(i64::from(value)),
                })
            }
            Kernel::IntegerMultiply { unsigned } => {
                if args.len() != 2 {
                    return Err(EvalError::Unsupported("protobuf multiply arity"));
                }
                let left = eval_numeric_row(&args[0], ctx, row, EvalType::Int)?;
                let Some(left) = crate::arg_eval_type::eval_int(&left)? else {
                    return Ok(Datum::Null);
                };
                let right = eval_numeric_row(&args[1], ctx, row, EvalType::Int)?;
                let Some(right) = crate::arg_eval_type::eval_int(&right)? else {
                    return Ok(Datum::Null);
                };
                // These Go signatures bake signedness into the implementation.
                let result = if unsigned {
                    (left as u64).checked_mul(right as u64).map(Datum::UInt)
                } else {
                    left.checked_mul(right).map(Datum::Int)
                };
                result.ok_or_else(|| {
                    let text = |index| {
                        super::numeric_argument_text(function, &args[index], false, ctx, None)
                    };
                    match text(0).zip(text(1)) {
                        Some((left, right)) => EvalError::DataOutOfRange {
                            value: if unsigned {
                                "BIGINT UNSIGNED"
                            } else {
                                "BIGINT"
                            },
                            expression: format!("({left} * {right})"),
                        },
                        None => EvalError::IntOverflow,
                    }
                })
            }
            Kernel::IntegerMod { unsigned } => {
                // The four Go MOD signatures bake signedness into the builtin,
                // rather than reselecting it from each row or the SQL name.
                let operand = |index: usize| -> Result<Datum, EvalError> {
                    let value = argument(index)?;
                    Ok(match crate::arg_eval_type::eval_int(&value)? {
                        None => Datum::Null,
                        Some(value) if unsigned[index] => Datum::UInt(value as u64),
                        Some(value) => Datum::Int(value),
                    })
                };
                let left = operand(0)?;
                let right = operand(1)?;
                crate::ops::eval_binary_full(
                    BinaryOp::Mod,
                    left,
                    right,
                    ctx.div_precision_increment(),
                    function.derived_collation(),
                    crate::ops::Operands::LITERALS,
                    ctx,
                )
            }
            Kernel::Binary(op, domain) => {
                if args.len() != 2 {
                    return Err(EvalError::Unsupported("protobuf binary builtin arity"));
                }
                let left = eval_numeric_operand_row(&args[0], ctx, row, domain)?;
                if left.is_null() && !(op == BinaryOp::Mod && domain != EvalType::Decimal) {
                    return Ok(Datum::Null);
                }
                let right = eval_numeric_operand_row(&args[1], ctx, row, domain)?;
                function.eval_binary_values(op, left, right, ctx)
            }
            Kernel::Compare(op, domain) => {
                if args.len() != 2 {
                    return Err(EvalError::Unsupported("protobuf comparison arity"));
                }
                let left = eval_numeric_row(&args[0], ctx, row, domain)?;
                let right = eval_numeric_row(&args[1], ctx, row, domain)?;
                function.eval_binary_values(op, left, right, ctx)
            }
            Kernel::In(domain) => {
                let Some(first) = args.first() else {
                    return Err(EvalError::Unsupported("protobuf IN arity"));
                };
                let left = eval_numeric_row(first, ctx, row, domain)?;
                if left.is_null() {
                    return Ok(Datum::Null);
                }
                let mut has_null = false;
                for candidate in &args[1..] {
                    let right = eval_numeric_row(candidate, ctx, row, domain)?;
                    if right.is_null() {
                        has_null = true;
                        continue;
                    }
                    let equal = crate::ops::eval_binary_full(
                        BinaryOp::Eq,
                        left.clone(),
                        right,
                        ctx.div_precision_increment(),
                        function.derived_collation(),
                        crate::ops::Operands::of(first, candidate),
                        ctx,
                    )?;
                    if equal == Datum::Int(1) {
                        return Ok(equal);
                    }
                }
                Ok(if has_null { Datum::Null } else { Datum::Int(0) })
            }
            Kernel::Conv => {
                if args.len() != 3 {
                    return Err(EvalError::Unsupported("protobuf CONV arity"));
                }
                // Go evaluates the two bases before the ordinary text argument.
                let from = typed_argument(1, EvalType::Int)?;
                if from.is_null() {
                    return Ok(Datum::Null);
                }
                let to = typed_argument(2, EvalType::Int)?;
                if to.is_null() {
                    return Ok(Datum::Null);
                }
                let text = typed_argument(0, EvalType::String)?;
                crate::math_fn::conv(&[text, from, to])
            }
            Kernel::Like => function.eval_like(ctx, row, false),
            Kernel::DateArithmetic { subtract } => {
                if args.len() != 3 {
                    return Err(EvalError::Unsupported("protobuf date arithmetic arity"));
                }
                // Go evaluates the unit before the date and interval.
                let unit = eval_numeric_row(&args[2], ctx, row, EvalType::String)?;
                let Some(unit) = crate::arg_eval_type::eval_string(&unit)? else {
                    return Ok(Datum::Null);
                };
                let unit = std::str::from_utf8(&unit)
                    .map_err(|_| EvalError::Unsupported("invalid date interval unit"))?;
                function.eval_date_arithmetic(unit, subtract, ctx, row)
            }
            Kernel::Cast { source, target } => {
                if args.len() != 1 {
                    return Err(EvalError::Unsupported("protobuf cast arity"));
                }
                let value = eval_numeric_row(&args[0], ctx, row, source)?;
                if value.is_null() {
                    return Ok(Datum::Null);
                }
                if target == EvalType::Json {
                    return cast_json_argument_value(function, value);
                }
                let field = function
                    .get_static_type()
                    .ok_or(EvalError::Unsupported("protobuf cast result type"))?;
                if matches!(target, EvalType::Int | EvalType::Real | EvalType::Decimal) {
                    return crate::cast::eval_numeric_cast_with_type(
                        value,
                        source,
                        args[0].static_type(),
                        field,
                        ctx,
                    );
                }
                if target == EvalType::String {
                    return crate::cast::eval_string_cast_with_type(
                        value,
                        args[0].static_type(),
                        field,
                        ctx,
                    );
                }
                use tidb_ast::CastType;
                let len = u32::try_from(field.flen()).ok();
                let fsp = u32::try_from(field.decimal()).ok();
                let cast = match target {
                    EvalType::Datetime | EvalType::Timestamp
                        if field.code() == tidb_datatype::FieldTypeCode::Date =>
                    {
                        CastType::Date
                    }
                    EvalType::Datetime | EvalType::Timestamp => CastType::DateTime { fsp },
                    EvalType::Duration => CastType::Time { fsp },
                    EvalType::VectorFloat32 => CastType::Vector { dimensions: len },
                    _ => return Err(EvalError::Unsupported("protobuf cast target")),
                };
                crate::cast::eval_cast(&cast, value, args[0].static_type(), ctx)
            }
            Kernel::Regexp => function.eval_regexp_like(ctx, row),
            kernel => {
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    let value = arg.eval(ctx, row)?;
                    if value.is_null() && !matches!(kernel, Kernel::Json) {
                        return Ok(Datum::Null);
                    }
                    values.push(value);
                }
                match kernel {
                    Kernel::Values(eval) => eval(&values, ctx),
                    Kernel::Round => crate::math_fn::round_or_truncate_with_result_decimal(
                        &values,
                        true,
                        function.ret_type.as_ref().map(FieldType::decimal),
                        ctx,
                    ),
                    Kernel::String { operation, binary } => {
                        let Some(value) = values.first_mut() else {
                            return Err(EvalError::Unsupported("protobuf string builtin arity"));
                        };
                        if !value.is_null() {
                            let bytes = crate::arg_eval_type::eval_string(value)?
                                .ok_or(EvalError::Unsupported("protobuf string argument"))?;
                            *value = if binary {
                                Datum::Bytes(bytes)
                            } else {
                                Datum::new_string(bytes)
                            };
                        }
                        match operation {
                            StringOp::Length => Ok(match &values[0] {
                                Datum::Null => Datum::Null,
                                Datum::Bytes(bytes) => Datum::Int(bytes.len() as i64),
                                value => Datum::Int(
                                    crate::string_signature::StrUnits::of_with_signature(
                                        value, false,
                                    )?
                                    .expect("non-NULL string")
                                    .len() as i64,
                                ),
                            }),
                            StringOp::Upper => crate::string_fn::case_convert(&values, true),
                            StringOp::Lower => crate::string_fn::case_convert(&values, false),
                            StringOp::Substring => crate::string_fn::substring(&values, ctx),
                        }
                    }
                    Kernel::FromUnixTime => {
                        let result = crate::time_fn::session_tz::from_unixtime(&values, ctx)?;
                        if self.signature == ScalarFuncSig::FromUnixTime1Arg {
                            crate::cast::parse_computed_time(
                                &result,
                                ctx,
                                tidb_datatype::TimeType::DateTime,
                                function.get_static_type().map(FieldType::decimal),
                            )
                        } else {
                            Ok(result)
                        }
                    }
                    Kernel::Json => {
                        let types = args
                            .iter()
                            .map(|arg| arg.static_type().cloned())
                            .collect::<Vec<_>>();
                        let cache_paths = args.get(1..).is_some_and(|arguments| {
                            !arguments.is_empty()
                                && arguments
                                    .iter()
                                    .step_by(2)
                                    .all(|arg| arg.const_level() >= ConstLevel::ONLY_IN_CONTEXT)
                        });
                        crate::builtin_ext::json::eval_pb(
                            self.signature,
                            &values,
                            &types,
                            ctx,
                            cache_paths.then_some(&function.json_modify_path_cache),
                        )
                    }
                    _ => unreachable!("lazy kernels were handled before evaluating arguments"),
                }
            }
        }
    }
}

fn cast_types(sig: ScalarFuncSig) -> Option<(EvalType, EvalType)> {
    use ScalarFuncSig::*;
    Some(match sig {
        CastIntAsInt => (EvalType::Int, EvalType::Int),
        CastRealAsReal => (EvalType::Real, EvalType::Real),
        CastDecimalAsDecimal => (EvalType::Decimal, EvalType::Decimal),
        CastStringAsString => (EvalType::String, EvalType::String),
        CastDurationAsDuration => (EvalType::Duration, EvalType::Duration),
        CastJsonAsJson => (EvalType::Json, EvalType::Json),
        CastDecimalAsDuration => (EvalType::Decimal, EvalType::Duration),
        CastDecimalAsInt => (EvalType::Decimal, EvalType::Int),
        CastDecimalAsJson => (EvalType::Decimal, EvalType::Json),
        CastDecimalAsReal => (EvalType::Decimal, EvalType::Real),
        CastDecimalAsString => (EvalType::Decimal, EvalType::String),
        CastDecimalAsTime => (EvalType::Decimal, EvalType::Datetime),
        CastDurationAsDecimal => (EvalType::Duration, EvalType::Decimal),
        CastDurationAsInt => (EvalType::Duration, EvalType::Int),
        CastDurationAsJson => (EvalType::Duration, EvalType::Json),
        CastDurationAsReal => (EvalType::Duration, EvalType::Real),
        CastDurationAsString => (EvalType::Duration, EvalType::String),
        CastDurationAsTime => (EvalType::Duration, EvalType::Datetime),
        CastIntAsDecimal => (EvalType::Int, EvalType::Decimal),
        CastIntAsDuration => (EvalType::Int, EvalType::Duration),
        CastIntAsJson => (EvalType::Int, EvalType::Json),
        CastIntAsReal => (EvalType::Int, EvalType::Real),
        CastIntAsString => (EvalType::Int, EvalType::String),
        CastIntAsTime => (EvalType::Int, EvalType::Datetime),
        CastJsonAsDecimal => (EvalType::Json, EvalType::Decimal),
        CastJsonAsDuration => (EvalType::Json, EvalType::Duration),
        CastJsonAsInt => (EvalType::Json, EvalType::Int),
        CastJsonAsReal => (EvalType::Json, EvalType::Real),
        CastJsonAsString => (EvalType::Json, EvalType::String),
        CastJsonAsTime => (EvalType::Json, EvalType::Datetime),
        CastRealAsDecimal => (EvalType::Real, EvalType::Decimal),
        CastRealAsDuration => (EvalType::Real, EvalType::Duration),
        CastRealAsInt => (EvalType::Real, EvalType::Int),
        CastRealAsJson => (EvalType::Real, EvalType::Json),
        CastRealAsString => (EvalType::Real, EvalType::String),
        CastRealAsTime => (EvalType::Real, EvalType::Datetime),
        CastStringAsDecimal => (EvalType::String, EvalType::Decimal),
        CastStringAsDuration => (EvalType::String, EvalType::Duration),
        CastStringAsInt => (EvalType::String, EvalType::Int),
        CastStringAsJson => (EvalType::String, EvalType::Json),
        CastStringAsReal => (EvalType::String, EvalType::Real),
        CastStringAsTime => (EvalType::String, EvalType::Datetime),
        CastTimeAsDecimal => (EvalType::Datetime, EvalType::Decimal),
        CastTimeAsDuration => (EvalType::Datetime, EvalType::Duration),
        CastTimeAsInt => (EvalType::Datetime, EvalType::Int),
        CastTimeAsJson => (EvalType::Datetime, EvalType::Json),
        CastTimeAsReal => (EvalType::Datetime, EvalType::Real),
        CastTimeAsString => (EvalType::Datetime, EvalType::String),
        CastTimeAsTime => (EvalType::Datetime, EvalType::Datetime),
        _ => return None,
    })
}
