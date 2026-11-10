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

//! `pkg/expression/builtin_time.go`: the time function classes.
//!
//! Go reads `args[i].GetType()` after `newBaseBuiltinFuncWithTp` cast the
//! slice in place, so every class here reads `bf.args[i]` unless Go reads
//! the type before the build.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass, SigFields,
};
use crate::builtin_miscellaneous::{function_class, set_connection_charset};
use crate::builtin_string::string_class;
use crate::constant::Constant;
use crate::expression::Expression;
use crate::wrap_cast::{
    ret_type_mut, type_of, wrap_with_cast_as_string, wrap_with_cast_as_time,
};
use crate::EvalError;
use tidb_datatype::{
    date_fsp, get_format_type, get_fsp, is_clock_unit, is_date_unit, is_microsecond_unit, Datum,
    EvalType, FieldType, FieldTypeCode, FieldTypeFlags, MAX_DECIMAL_WIDTH, UNSPECIFIED_LENGTH,
};
use tidb_chunk::row::Row;
use tidb_proto::tipb::ScalarFuncSig;
use tidb_terror::{TerrorCode, TerrorError};

/// Go `types.MinFsp`.
const MIN_FSP: i64 = 0;
/// Go `types.MaxFsp`.
const MAX_FSP: i64 = 6;
/// Go `types.DefaultFsp`.
const DEFAULT_FSP: i64 = 0;
/// Go `mysql.MaxDateWidth`.
const MAX_DATE_WIDTH: i64 = 10;
/// Go `mysql.MaxDatetimeWidthNoFsp`.
const MAX_DATETIME_WIDTH_NO_FSP: i64 = 19;
/// Go `mysql.MaxDatetimeWidthWithFsp`.
const MAX_DATETIME_WIDTH_WITH_FSP: i64 = 26;
/// Go `mysql.MaxDatetimeFullWidth`.
const MAX_DATETIME_FULL_WIDTH: i64 = 29;

/// Go `Constant.EvalString` on an empty row: `None` is NULL.
fn eval_string(ctx: &dyn BuildContext, arg: &Expression) -> Result<Option<String>, EvalError> {
    match ctx.eval_constant(arg)? {
        Datum::Null => Ok(None),
        value => value
            .sql_string()
            .map(Some)
            .map_err(|_| EvalError::Unsupported("the constant has no string form")),
    }
}

/// Go `getExpressionFsp`.
fn get_expression_fsp(ctx: &dyn BuildContext, expression: &Expression) -> Result<i64, EvalError> {
    if matches!(expression, Expression::Constant(_)) {
        return Ok(eval_string(ctx, expression)?.map_or(0, |value| i64::from(get_fsp(&value))));
    }
    let wrapped =
        wrap_with_cast_as_time(ctx, expression.clone(), FieldType::new(FieldTypeCode::Datetime))?;
    Ok(type_of(&wrapped).decimal().min(MAX_FSP))
}

/// Go `Constant.EvalInt` on an empty row: `None` is NULL.
fn eval_int(ctx: &dyn BuildContext, constant: &Constant) -> Result<Option<i64>, EvalError> {
    let value = match ctx.columns() {
        Some(columns) => constant.eval_typed_as_on_row(columns, Row::empty(), EvalType::Int)?,
        None => ctx.eval_constant(&Expression::Constant(constant.clone()))?,
    };
    match value {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(value)),
        Datum::UInt(value) => Ok(Some(value as i64)),
        _ => Err(EvalError::Unsupported(
            "the constant's integer needs the statement's context",
        )),
    }
}

/// Go `getFspByIntArg`.
fn get_fsp_by_int_arg(
    ctx: &dyn BuildContext,
    exps: &[Expression],
    func_name: &str,
) -> Result<i64, EvalError> {
    let exp = match exps {
        [] => return Ok(0),
        [exp] => exp,
        _ => {
            return Err(EvalError::Conversion(TerrorError::compatible(
                TerrorCode::new(1105),
                format!(
                    "Should not happen, the num of argument should be 1, but got {}",
                    exps.len()
                ),
            )))
        }
    };
    // Should no happen. But our tests may generate non-constant input.
    let Expression::Constant(constant) = exp else {
        return Ok(0);
    };
    // If isNULL, it may be a bug of parser. Return 0 to be compatible with old version.
    let Some(fsp) = eval_int(ctx, constant)? else {
        return Ok(0);
    };
    if fsp > i64::from(i32::MAX) || fsp < 0 {
        return Err(crate::time_fn::fsp_syntax_error());
    }
    if fsp > MAX_FSP {
        return Err(EvalError::TooBigFsp {
            fsp,
            function: crate::time_fn::clock_function_name(func_name),
        });
    }
    Ok(fsp)
}

/// Go `fieldString`.
fn field_string(field_type: FieldTypeCode) -> bool {
    matches!(
        field_type,
        FieldTypeCode::String
            | FieldTypeCode::Varchar
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::Blob
    )
}

function_class!(
    /// Go `dateFunctionClass`.
    DateFunctionClass, Datetime, [Datetime], Date, |bf, _ctx| {
        bf.set_decimal_and_flen_for_date();
    }
);

function_class!(
    /// Go `dateDiffFunctionClass`.
    DateDiffFunctionClass, Int, [Datetime, Datetime], DateDiff
);

/// Go `timeDiffFunctionClass.getArgEvalTp`.
fn time_diff_arg_eval_tp(field_tp: &FieldType) -> EvalType {
    match field_tp.eval_type() {
        tp @ (EvalType::Duration | EvalType::Datetime | EvalType::Timestamp) => tp,
        _ => EvalType::String,
    }
}

string_class!(
    /// Go `timeDiffFunctionClass`.
    TimeDiffFunctionClass, |func_name, ctx, args| {
        let arg0_tp = time_diff_arg_eval_tp(&type_of(&args[0]));
        let arg1_tp = time_diff_arg_eval_tp(&type_of(&args[1]));
        let arg0_dec = get_expression_fsp(ctx, &args[0])?;
        let arg1_dec = get_expression_fsp(ctx, &args[1])?;
        let fsp = arg0_dec.max(arg1_dec);
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::Duration,
            &[arg0_tp, arg1_tp],
        )?;
        bf.set_decimal_and_flen_for_time(fsp);
        // arg0 and arg1 must be the same time type(compatible), or timediff will return NULL.
        let is_time = |tp| matches!(tp, EvalType::Datetime | EvalType::Timestamp);
        bf.pb_code = match (arg0_tp, arg1_tp) {
            (EvalType::Duration, EvalType::Duration) => ScalarFuncSig::DurationDurationTimeDiff,
            (EvalType::Duration, tp) if is_time(tp) => ScalarFuncSig::NullTimeDiff,
            (EvalType::Duration, _) => ScalarFuncSig::DurationStringTimeDiff,
            (tp, EvalType::Duration) if is_time(tp) => ScalarFuncSig::NullTimeDiff,
            (tp0, tp1) if is_time(tp0) && is_time(tp1) => ScalarFuncSig::TimeTimeTimeDiff,
            (tp, _) if is_time(tp) => ScalarFuncSig::TimeStringTimeDiff,
            (_, EvalType::Duration) => ScalarFuncSig::StringDurationTimeDiff,
            (_, tp) if is_time(tp) => ScalarFuncSig::StringTimeTimeDiff,
            _ => ScalarFuncSig::StringStringTimeDiff,
        };
        Ok(bf)
    }
);

function_class!(
    /// Go `dateFormatFunctionClass`.
    DateFormatFunctionClass, String, [Datetime, String], DateFormatSig, |bf, _ctx| {
        // worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
        let flen = (type_of(&bf.args[1]).flen() + 1) / 2 * 11;
        bf.tp.set_flen(flen);
    }
);

function_class!(
    /// Go `fromDaysFunctionClass`.
    FromDaysFunctionClass, Datetime, [Int], FromDays, |bf, _ctx| {
        bf.set_decimal_and_flen_for_date();
    }
);

function_class!(
    /// Go `hourFunctionClass`.
    HourFunctionClass, Int, [Duration], Hour, |bf, _ctx| {
        bf.tp.set_flen(3);
        bf.tp.set_decimal(0);
    }
);

function_class!(
    /// Go `minuteFunctionClass`.
    MinuteFunctionClass, Int, [Duration], Minute, |bf, _ctx| {
        bf.tp.set_flen(2);
        bf.tp.set_decimal(0);
    }
);

function_class!(
    /// Go `secondFunctionClass`.
    SecondFunctionClass, Int, [Duration], Second, |bf, _ctx| {
        bf.tp.set_flen(2);
        bf.tp.set_decimal(0);
    }
);

function_class!(
    /// Go `microSecondFunctionClass`.
    MicroSecondFunctionClass, Int, [Duration], MicroSecond, |bf, _ctx| {
        bf.tp.set_flen(6);
        bf.tp.set_decimal(0);
    }
);

function_class!(
    /// Go `monthFunctionClass`.
    MonthFunctionClass, Int, [Datetime], Month, |bf, _ctx| {
        bf.tp.set_flen(2);
        bf.tp.set_decimal(0);
    }
);

function_class!(
    /// Go `monthNameFunctionClass`.
    MonthNameFunctionClass, String, [Datetime], MonthName, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(10);
    }
);

function_class!(
    /// Go `dayNameFunctionClass`.
    DayNameFunctionClass, String, [Datetime], DayName, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(10);
    }
);

function_class!(
    /// Go `dayOfMonthFunctionClass`.
    DayOfMonthFunctionClass, Int, [Datetime], DayOfMonth, |bf, _ctx| {
        bf.tp.set_flen(2);
    }
);

function_class!(
    /// Go `dayOfWeekFunctionClass`.
    DayOfWeekFunctionClass, Int, [Datetime], DayOfWeek, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

function_class!(
    /// Go `dayOfYearFunctionClass`.
    DayOfYearFunctionClass, Int, [Datetime], DayOfYear, |bf, _ctx| {
        bf.tp.set_flen(3);
    }
);

/// Go `weekFunctionClass.getFunction` and `yearWeekFunctionClass
/// .getFunction`: a datetime and an optional integer mode.
fn week_function(
    ctx: &dyn BuildContext,
    func_name: &str,
    args: Vec<Expression>,
    flen: i64,
    with_mode: ScalarFuncSig,
    without_mode: ScalarFuncSig,
) -> Result<BaseBuiltinFunc, EvalError> {
    let arg_tps = [EvalType::Datetime, EvalType::Int];
    let arg_tps = &arg_tps[..args.len()];
    let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::Int, arg_tps)?;
    bf.tp.set_flen(flen);
    bf.tp.set_decimal(0);
    bf.pb_code = if bf.args.len() == 2 {
        with_mode
    } else {
        without_mode
    };
    Ok(bf)
}

string_class!(
    /// Go `weekFunctionClass`.
    WeekFunctionClass, |func_name, ctx, args| {
        week_function(
            ctx,
            func_name,
            args,
            2,
            ScalarFuncSig::WeekWithMode,
            ScalarFuncSig::WeekWithoutMode,
        )
    }
);

function_class!(
    /// Go `weekDayFunctionClass`.
    WeekDayFunctionClass, Int, [Datetime], WeekDay, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

function_class!(
    /// Go `weekOfYearFunctionClass`.
    WeekOfYearFunctionClass, Int, [Datetime], WeekOfYear, |bf, _ctx| {
        bf.tp.set_flen(2);
        bf.tp.set_decimal(0);
    }
);

function_class!(
    /// Go `yearFunctionClass`.
    YearFunctionClass, Int, [Datetime], Year, |bf, _ctx| {
        bf.tp.set_flen(4);
        bf.tp.set_decimal(0);
    }
);

string_class!(
    /// Go `yearWeekFunctionClass`.
    YearWeekFunctionClass, |func_name, ctx, args| {
        week_function(
            ctx,
            func_name,
            args,
            6,
            ScalarFuncSig::YearWeekWithMode,
            ScalarFuncSig::YearWeekWithoutMode,
        )
    }
);

string_class!(
    /// Go `fromUnixTimeFunctionClass`.
    FromUnixTimeFunctionClass, |func_name, ctx, args| {
        let (ret_tp, arg_tps): (EvalType, &[EvalType]) = if args.len() == 2 {
            (EvalType::String, &[EvalType::Decimal, EvalType::String])
        } else {
            (EvalType::Datetime, &[EvalType::Decimal])
        };
        let arg0_tp = type_of(&args[0]);
        let is_arg0_str = arg0_tp.eval_type() == EvalType::String;
        let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, ret_tp, arg_tps)?;

        if field_string(arg0_tp.code()) {
            // Improve string cast Unix Time precision
            if let Expression::ScalarFunction(x) = &mut bf.args[0] {
                // used to adjust FromUnixTime precision #Fixbug35184
                if x.is_cast() {
                    if let Some(ret_type) = x.ret_type.as_mut() {
                        if ret_type.decimal() == 0 && ret_type.code() == FieldTypeCode::NewDecimal
                        {
                            ret_type.set_decimal(6);
                            let field_len = (ret_type.flen() + 6).min(MAX_DECIMAL_WIDTH);
                            ret_type.set_flen(field_len);
                        }
                    }
                }
            }
        }

        if bf.args.len() > 1 {
            bf.pb_code = ScalarFuncSig::FromUnixTime2Arg;
            return Ok(bf);
        }

        // Calculate the time fsp.
        let mut fsp = MAX_FSP;
        if !is_arg0_str && arg0_tp.decimal() != UNSPECIFIED_LENGTH {
            fsp = bf.tp.decimal().min(arg0_tp.decimal());
        }
        bf.set_decimal_and_flen_for_datetime(fsp);
        bf.pb_code = ScalarFuncSig::FromUnixTime1Arg;
        Ok(bf)
    }
);

function_class!(
    /// Go `getFormatFunctionClass`.
    GetFormatFunctionClass, String, [String, String], GetFormat, |bf, _ctx| {
        bf.tp.set_flen(17);
    }
);

/// Go `strToDateFunctionClass.getRetTp`.
fn str_to_date_ret_tp(ctx: &dyn BuildContext, arg: &Expression) -> (FieldTypeCode, i64) {
    let tp = FieldTypeCode::Datetime;
    if !matches!(arg, Expression::Constant(_)) {
        return (tp, MAX_FSP);
    }
    let format = wrap_with_cast_as_string(ctx, arg.clone())
        .and_then(|str_arg| eval_string(ctx, &str_arg));
    let Ok(Some(format)) = format else {
        return (tp, 0);
    };
    let tp = match get_format_type(&format) {
        (true, false) => FieldTypeCode::Duration,
        (false, true) => FieldTypeCode::Date,
        _ => tp,
    };
    let fsp = if format.contains("%f") { MAX_FSP } else { 0 };
    (tp, fsp)
}

string_class!(
    /// Go `strToDateFunctionClass`.
    StrToDateFunctionClass, |func_name, ctx, args| {
        let (ret_tp, fsp) = str_to_date_ret_tp(ctx, &args[1]);
        let arg_tps = [EvalType::String, EvalType::String];
        if ret_tp == FieldTypeCode::Duration {
            let mut bf =
                new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::Duration, &arg_tps)?;
            bf.set_decimal_and_flen_for_time(fsp);
            bf.pb_code = ScalarFuncSig::StrToDateDuration;
            return Ok(bf);
        }
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::Datetime, &arg_tps)?;
        if ret_tp == FieldTypeCode::Date {
            bf.set_decimal_and_flen_for_date();
            bf.pb_code = ScalarFuncSig::StrToDateDate;
        } else {
            bf.set_decimal_and_flen_for_datetime(fsp);
            bf.pb_code = ScalarFuncSig::StrToDateDatetime;
        }
        Ok(bf)
    }
);

/// The evaluation types of a clock function's optional precision.
fn fsp_arg_tps(args: &[Expression]) -> &'static [EvalType] {
    if args.len() == 1 {
        &[EvalType::Int]
    } else {
        &[]
    }
}

string_class!(
    /// Go `sysDateFunctionClass`.
    SysDateFunctionClass, |func_name, ctx, args| {
        let fsp = get_fsp_by_int_arg(ctx, &args, func_name)?;
        let arg_tps = fsp_arg_tps(&args);
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::Datetime, arg_tps)?;
        bf.set_decimal_and_flen_for_datetime(fsp);
        // Illegal parameters have been filtered out in the parser, so the result is always not null.
        bf.tp.add_flags(FieldTypeFlags::NOT_NULL);
        bf.pb_code = if bf.args.len() == 1 {
            ScalarFuncSig::SysDateWithFsp
        } else {
            ScalarFuncSig::SysDateWithoutFsp
        };
        Ok(bf)
    }
);

function_class!(
    /// Go `currentDateFunctionClass`.
    CurrentDateFunctionClass, Datetime, [], Unspecified => crate::scalar_function::Kernel::Method(crate::scalar_function::class_kernels::current_date), |bf, _ctx| {
        bf.set_decimal_and_flen_for_date();
    }
);

string_class!(
    /// Go `currentTimeFunctionClass`.
    CurrentTimeFunctionClass, |func_name, ctx, args| {
        let fsp = get_fsp_by_int_arg(ctx, &args, func_name)?;
        let arg_tps = fsp_arg_tps(&args);
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::Duration, arg_tps)?;
        bf.set_decimal_and_flen_for_time(fsp);
        // 1. no sign.
        // 2. hour is in the 2-digit range.
        bf.tp.set_flen(bf.tp.flen() - 2);
        bf.pb_code = if bf.args.is_empty() {
            ScalarFuncSig::CurrentTime0Arg
        } else {
            ScalarFuncSig::CurrentTime1Arg
        };
        Ok(bf)
    }
);

string_class!(
    /// Go `timeFunctionClass`.
    TimeFunctionClass, |func_name, ctx, args| {
        let fsp = get_expression_fsp(ctx, &args[0])?;
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::Duration,
            &[EvalType::String],
        )?;
        bf.set_decimal_and_flen_for_time(fsp);
        bf.pb_code = ScalarFuncSig::Time;
        Ok(bf)
    }
);

function_class!(
    /// Go `utcDateFunctionClass`.
    UTCDateFunctionClass, Datetime, [], Unspecified => crate::scalar_function::Kernel::Method(crate::scalar_function::class_kernels::utc_date), |bf, _ctx| {
        bf.set_decimal_and_flen_for_date();
    }
);

/// Go `utcTimestampFunctionClass.getFunction` and `nowFunctionClass
/// .getFunction`: a datetime of the precision the built argument names.
fn timestamp_clock_function(
    ctx: &dyn BuildContext,
    func_name: &str,
    args: Vec<Expression>,
    with_arg: ScalarFuncSig,
    without_arg: ScalarFuncSig,
) -> Result<BaseBuiltinFunc, EvalError> {
    let arg_tps = fsp_arg_tps(&args);
    let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::Datetime, arg_tps)?;
    let fsp = get_fsp_by_int_arg(ctx, &bf.args, func_name)?;
    bf.set_decimal_and_flen_for_datetime(fsp);
    bf.pb_code = if bf.args.len() == 1 {
        with_arg
    } else {
        without_arg
    };
    Ok(bf)
}

string_class!(
    /// Go `utcTimestampFunctionClass`.
    UTCTimestampFunctionClass, |func_name, ctx, args| {
        timestamp_clock_function(
            ctx,
            func_name,
            args,
            ScalarFuncSig::UtcTimestampWithArg,
            ScalarFuncSig::UtcTimestampWithoutArg,
        )
    }
);

string_class!(
    /// Go `nowFunctionClass`.
    NowFunctionClass, |func_name, ctx, args| {
        timestamp_clock_function(
            ctx,
            func_name,
            args,
            ScalarFuncSig::NowWithArg,
            ScalarFuncSig::NowWithoutArg,
        )
    }
);

string_class!(
    /// Go `extractFunctionClass`.
    ExtractFunctionClass, |func_name, ctx, args| {
        let mut args = args.into_iter();
        let unit_arg = args.next().expect("EXTRACT takes two arguments");
        let unit_arg = wrap_with_cast_as_string(ctx, unit_arg)?;
        let args: Vec<Expression> = std::iter::once(unit_arg).chain(args).collect();
        let unit = eval_string(ctx, &args[0])?.unwrap_or_default();
        let is_clock_unit = is_clock_unit(&unit);
        let is_date_unit = is_date_unit(&unit);
        let datetime = |args| {
            let mut bf = new_base_builtin_func_with_tp(
                ctx,
                func_name,
                args,
                EvalType::Int,
                &[EvalType::String, EvalType::Datetime],
            )?;
            bf.pb_code = ScalarFuncSig::ExtractDatetime;
            Ok(bf)
        };
        let duration = |args| {
            let mut bf = new_base_builtin_func_with_tp(
                ctx,
                func_name,
                args,
                EvalType::Int,
                &[EvalType::String, EvalType::Duration],
            )?;
            bf.pb_code = ScalarFuncSig::ExtractDuration;
            Ok(bf)
        };
        if is_clock_unit && is_date_unit {
            // For unit DAY_MICROSECOND/DAY_SECOND/DAY_MINUTE/DAY_HOUR, the interpretation of the second argument depends on its evaluation type:
            // 1. Datetime/timestamp are interpreted as datetime.
            // 2. Time is interpreted as is.
            // 3. Otherwise, string/int/float are interpreted as arbitrarily either datetime or time, depending on which fits.
            match type_of(&args[1]).eval_type() {
                EvalType::Datetime | EvalType::Timestamp => datetime(args),
                EvalType::Duration => duration(args),
                _ => {
                    let mut bf = new_base_builtin_func_with_tp(
                        ctx,
                        func_name,
                        args,
                        EvalType::Int,
                        &[EvalType::String, EvalType::String],
                    )?;
                    if let Some(tp) = ret_type_mut(&mut bf.args[1]) {
                        tp.set_decimal(MAX_FSP);
                    }
                    bf.pb_code = ScalarFuncSig::ExtractDatetimeFromString;
                    Ok(bf)
                }
            }
        } else if is_clock_unit {
            // Clock units interpret the second argument as time.
            duration(args)
        } else {
            // Date units interpret the second argument as datetime.
            datetime(args)
        }
    }
);

/// Go `addSubDateFunctionClass.setPbCodeOp` for `ADDDATE`/`DATE_ADD`.
pub(crate) fn set_add(add_op: ScalarFuncSig, _sub_op: ScalarFuncSig) -> ScalarFuncSig {
    add_op
}

/// Go `addSubDateFunctionClass.setPbCodeOp` for `SUBDATE`/`DATE_SUB`.
pub(crate) fn set_sub(_add_op: ScalarFuncSig, sub_op: ScalarFuncSig) -> ScalarFuncSig {
    sub_op
}

/// Go `addSubDateFunctionClass`.
pub(crate) struct AddSubDateFunctionClass {
    pub(crate) func_name: &'static str,
    pub(crate) set_pb_code_op: fn(ScalarFuncSig, ScalarFuncSig) -> ScalarFuncSig,
}

impl FunctionClass for AddSubDateFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        use EvalType::{Datetime, Decimal, Duration, Int, Json, Real, String, Timestamp};
        use ScalarFuncSig as Sig;

        let arg0_tp = type_of(&args[0]);
        // Some special evaluation type treatment.
        // Note that it could be more elegant if we always evaluate datetime for int, real, decimal and string, by leveraging existing implicit casts.
        // However, MySQL has a weird behavior for date_add(string, ...), whose result depends on the content of the first argument.
        // This requires a customized parsing of the content of the first argument, by recognizing if it is a pure date format or contains HMS part.
        // So implicit casts are not viable here.
        let date_eval_tp = match arg0_tp.eval_type() {
            Timestamp => Datetime,
            Json => String,
            tp => tp,
        };
        let interval_eval_tp = match type_of(&args[1]).eval_type() {
            Json => String,
            tp @ (String | Decimal | Real) => tp,
            _ => Int,
        };
        let unit = eval_string(ctx, &args[2])?.unwrap_or_default();

        let (result_tp, result_eval_tp) = if arg0_tp.code() == FieldTypeCode::Date {
            if !is_clock_unit(&unit) {
                // First arg is date and unit contains no HMS, return date.
                (FieldTypeCode::Date, Datetime)
            } else {
                // First arg is date and unit contains HMS, return datetime.
                (FieldTypeCode::Datetime, Datetime)
            }
        } else if date_eval_tp == Duration {
            if is_date_unit(&unit) && unit != "DAY_MICROSECOND" {
                // First arg is time and unit contains YMD (except DAY_MICROSECOND), return datetime.
                (FieldTypeCode::Datetime, Datetime)
            } else {
                // First arg is time and unit contains no YMD or is DAY_MICROSECOND, return time.
                (FieldTypeCode::Duration, Duration)
            }
        } else if date_eval_tp == Datetime {
            // First arg is datetime or timestamp, return datetime.
            (FieldTypeCode::Datetime, Datetime)
        } else {
            (FieldTypeCode::VarString, String)
        };

        let arg_tps = [date_eval_tp, interval_eval_tp, String];
        let mut bf =
            new_base_builtin_func_with_tp(ctx, self.func_name, args, result_eval_tp, &arg_tps)?;
        bf.tp.set_code(result_tp);

        let result_fsp = if is_microsecond_unit(&unit) {
            MAX_FSP
        } else {
            let mut interval_fsp = MIN_FSP;
            if unit == "SECOND" {
                interval_fsp = if matches!(interval_eval_tp, String | Real) {
                    MAX_FSP
                } else {
                    MAX_FSP.min(type_of(&bf.args[1]).decimal())
                };
            }
            MAX_FSP.min(type_of(&bf.args[0]).decimal().max(interval_fsp))
        };
        match result_tp {
            FieldTypeCode::Date => bf.set_decimal_and_flen_for_date(),
            FieldTypeCode::Duration => bf.set_decimal_and_flen_for_time(result_fsp),
            FieldTypeCode::Datetime => bf.set_decimal_and_flen_for_datetime(result_fsp),
            _ => {
                bf.tp.set_flen(MAX_DATETIME_FULL_WIDTH);
                bf.tp.set_decimal(MIN_FSP);
            }
        }

        let duration_result = result_tp == FieldTypeCode::Duration;
        let (add_op, sub_op) = match (date_eval_tp, interval_eval_tp) {
            (String, String) => (Sig::AddDateStringString, Sig::SubDateStringString),
            (String, Int) => (Sig::AddDateStringInt, Sig::SubDateStringInt),
            (String, Real) => (Sig::AddDateStringReal, Sig::SubDateStringReal),
            (String, Decimal) => (Sig::AddDateStringDecimal, Sig::SubDateStringDecimal),
            (Int, String) => (Sig::AddDateIntString, Sig::SubDateIntString),
            (Int, Int) => (Sig::AddDateIntInt, Sig::SubDateIntInt),
            (Int, Real) => (Sig::AddDateIntReal, Sig::SubDateIntReal),
            (Int, Decimal) => (Sig::AddDateIntDecimal, Sig::SubDateIntDecimal),
            (Real, String) => (Sig::AddDateRealString, Sig::SubDateRealString),
            (Real, Int) => (Sig::AddDateRealInt, Sig::SubDateRealInt),
            (Real, Real) => (Sig::AddDateRealReal, Sig::SubDateRealReal),
            (Real, Decimal) => (Sig::AddDateRealDecimal, Sig::SubDateRealDecimal),
            (Decimal, String) => (Sig::AddDateDecimalString, Sig::SubDateDecimalString),
            (Decimal, Int) => (Sig::AddDateDecimalInt, Sig::SubDateDecimalInt),
            (Decimal, Real) => (Sig::AddDateDecimalReal, Sig::SubDateDecimalReal),
            (Decimal, Decimal) => (Sig::AddDateDecimalDecimal, Sig::SubDateDecimalDecimal),
            (Datetime, String) => (Sig::AddDateDatetimeString, Sig::SubDateDatetimeString),
            (Datetime, Int) => (Sig::AddDateDatetimeInt, Sig::SubDateDatetimeInt),
            (Datetime, Real) => (Sig::AddDateDatetimeReal, Sig::SubDateDatetimeReal),
            (Datetime, Decimal) => (Sig::AddDateDatetimeDecimal, Sig::SubDateDatetimeDecimal),
            (Duration, String) if duration_result => {
                (Sig::AddDateDurationString, Sig::SubDateDurationString)
            }
            (Duration, Int) if duration_result => {
                (Sig::AddDateDurationInt, Sig::SubDateDurationInt)
            }
            (Duration, Real) if duration_result => {
                (Sig::AddDateDurationReal, Sig::SubDateDurationReal)
            }
            (Duration, Decimal) if duration_result => {
                (Sig::AddDateDurationDecimal, Sig::SubDateDurationDecimal)
            }
            (Duration, String) => (
                Sig::AddDateDurationStringDatetime,
                Sig::SubDateDurationStringDatetime,
            ),
            (Duration, Int) => (
                Sig::AddDateDurationIntDatetime,
                Sig::SubDateDurationIntDatetime,
            ),
            (Duration, Real) => (
                Sig::AddDateDurationRealDatetime,
                Sig::SubDateDurationRealDatetime,
            ),
            (Duration, Decimal) => (
                Sig::AddDateDurationDecimalDatetime,
                Sig::SubDateDurationDecimalDatetime,
            ),
            // Go's switch has no arm here and returns a nil signature.
            _ => return Err(EvalError::Unsupported("ADDDATE has no signature for these types")),
        };
        bf.pb_code = (self.set_pb_code_op)(add_op, sub_op);
        Ok(bf)
    }
}

string_class!(
    /// Go `timestampFunctionClass`.
    TimestampFunctionClass, |func_name, ctx, args| {
        let eval_tps = [EvalType::String; 2];
        let mut fsp = get_expression_fsp(ctx, &args[0])?;
        if args.len() == 2 {
            fsp = fsp.max(get_expression_fsp(ctx, &args[1])?);
        }
        let is_float = matches!(
            type_of(&args[0]).code(),
            FieldTypeCode::Float
                | FieldTypeCode::Double
                | FieldTypeCode::NewDecimal
                | FieldTypeCode::LongLong
        );
        let arg_len = args.len();
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::Datetime,
            &eval_tps[..arg_len],
        )?;
        bf.set_decimal_and_flen_for_datetime(fsp);
        bf.sig = SigFields::Timestamp { is_float };
        bf.pb_code = if arg_len == 2 {
            ScalarFuncSig::Timestamp2Args
        } else {
            ScalarFuncSig::Timestamp1Arg
        };
        Ok(bf)
    }
);

function_class!(
    /// Go `timestampDiffFunctionClass`.
    TimestampDiffFunctionClass, Int, [String, Datetime, Datetime], TimestampDiff
);

string_class!(
    /// Go `unixTimestampFunctionClass`.
    UnixTimestampFunctionClass, |func_name, ctx, args| {
        let mut arg_tps: &[EvalType] = &[];
        let ret_tp;
        let mut ret_decimal;
        if args.is_empty() {
            ret_tp = EvalType::Int;
            ret_decimal = 0;
        } else {
            arg_tps = &[EvalType::Datetime];
            let arg_type = type_of(&args[0]);
            if arg_type.eval_type() == EvalType::String {
                // Treat types.ETString as unspecified decimal.
                ret_decimal = UNSPECIFIED_LENGTH;
                if matches!(args[0], Expression::Constant(_)) {
                    let tmp_str = eval_string(ctx, &args[0])?.unwrap_or_default();
                    ret_decimal = tmp_str
                        .rfind('.')
                        .map_or(0, |dot_idx| (tmp_str.len() - dot_idx - 1) as i64);
                }
            } else {
                ret_decimal = arg_type.decimal();
            }
            if ret_decimal > 6 || ret_decimal == UNSPECIFIED_LENGTH {
                ret_decimal = 6;
            }
            ret_tp = if ret_decimal == 0 {
                EvalType::Int
            } else {
                EvalType::Decimal
            };
        }
        let ret_flen = if ret_tp == EvalType::Int {
            11
        } else {
            12 + ret_decimal
        };

        let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, ret_tp, arg_tps)?;
        bf.tp.set_flen_under_limit(ret_flen);
        bf.tp.set_decimal_under_limit(ret_decimal);

        bf.pb_code = if bf.args.is_empty() {
            ScalarFuncSig::UnixTimestampCurrent
        } else if ret_tp == EvalType::Int {
            ScalarFuncSig::UnixTimestampInt
        } else {
            ScalarFuncSig::UnixTimestampDec
        };
        Ok(bf)
    }
);

/// Go `getBf4TimeAddSub`: the base function with the two argument types Go
/// read before the build.
fn get_bf_4_time_add_sub(
    ctx: &dyn BuildContext,
    func_name: &str,
    args: Vec<Expression>,
) -> Result<(FieldType, FieldType, BaseBuiltinFunc), EvalError> {
    let (tp1, tp2) = (type_of(&args[0]), type_of(&args[1]));
    let (arg_tp1, ret_tp) = match tp1.code() {
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
            (EvalType::Datetime, EvalType::Datetime)
        }
        FieldTypeCode::Duration => (EvalType::Duration, EvalType::Duration),
        FieldTypeCode::Date => (EvalType::Datetime, EvalType::String),
        _ => (EvalType::String, EvalType::String),
    };
    let arg_tp2 = match tp2.code() {
        FieldTypeCode::Datetime | FieldTypeCode::Duration => EvalType::Duration,
        _ => EvalType::String,
    };
    let arg0_dec = get_expression_fsp(ctx, &args[0])?;
    let arg1_dec = get_expression_fsp(ctx, &args[1])?;

    let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, ret_tp, &[arg_tp1, arg_tp2])?;
    match ret_tp {
        EvalType::Datetime => {
            bf.set_decimal_and_flen_for_datetime(arg0_dec.max(arg1_dec).min(MAX_FSP));
        }
        EvalType::Duration => {
            bf.set_decimal_and_flen_for_time(arg0_dec.max(arg1_dec).min(MAX_FSP));
        }
        _ => {
            bf.tp.set_code(FieldTypeCode::String);
            bf.tp.set_flen(MAX_DATETIME_WIDTH_WITH_FSP);
            bf.tp.set_decimal(UNSPECIFIED_LENGTH);
        }
    }
    Ok((tp1, tp2, bf))
}

/// Go `addTimeFunctionClass.getFunction` and `subTimeFunctionClass
/// .getFunction`: `sigs` is the signatures in Go's switch order, `StringNull` once.
fn time_add_sub_function(
    ctx: &dyn BuildContext,
    func_name: &str,
    args: Vec<Expression>,
    sigs: [ScalarFuncSig; 11],
) -> Result<BaseBuiltinFunc, EvalError> {
    let [datetime_duration, datetime_null, datetime_string, date_duration, string_null, date_string, duration_duration, duration_null, duration_string, string_duration, string_string] =
        sigs;
    let (tp1, tp2, mut bf) = get_bf_4_time_add_sub(ctx, func_name, args)?;
    let by_tp2 = |duration, time, string| match tp2.code() {
        FieldTypeCode::Duration => duration,
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp => time,
        _ => string,
    };
    bf.pb_code = match tp1.code() {
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
            by_tp2(datetime_duration, datetime_null, datetime_string)
        }
        FieldTypeCode::Date => {
            set_connection_charset(&mut bf, ctx);
            by_tp2(date_duration, string_null, date_string)
        }
        FieldTypeCode::Duration => by_tp2(duration_duration, duration_null, duration_string),
        _ => by_tp2(string_duration, string_null, string_string),
    };
    Ok(bf)
}

string_class!(
    /// Go `addTimeFunctionClass`.
    AddTimeFunctionClass, |func_name, ctx, args| {
        time_add_sub_function(
            ctx,
            func_name,
            args,
            [
                ScalarFuncSig::AddDatetimeAndDuration,
                ScalarFuncSig::AddTimeDateTimeNull,
                ScalarFuncSig::AddDatetimeAndString,
                ScalarFuncSig::AddDateAndDuration,
                ScalarFuncSig::AddTimeStringNull,
                ScalarFuncSig::AddDateAndString,
                ScalarFuncSig::AddDurationAndDuration,
                ScalarFuncSig::AddTimeDurationNull,
                ScalarFuncSig::AddDurationAndString,
                ScalarFuncSig::AddStringAndDuration,
                ScalarFuncSig::AddStringAndString,
            ],
        )
    }
);

/// Go `convertTzFunctionClass.getDecimal`.
fn convert_tz_decimal(ctx: &dyn BuildContext, arg: &Expression) -> i64 {
    let mut decimal = MAX_FSP;
    if matches!(arg, Expression::Constant(_)) {
        let arg_tp = type_of(arg);
        match arg_tp.eval_type() {
            EvalType::Int => decimal = 0,
            EvalType::Real | EvalType::Decimal => decimal = arg_tp.decimal(),
            EvalType::String => {
                if let Ok(Some(str)) = eval_string(ctx, arg) {
                    decimal = date_fsp(&str) as i64;
                }
            }
            _ => {}
        }
    }
    decimal.clamp(MIN_FSP, MAX_FSP)
}

string_class!(
    /// Go `convertTzFunctionClass`.
    ConvertTzFunctionClass, |func_name, ctx, args| {
        let decimal = convert_tz_decimal(ctx, &args[0]);
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::Datetime,
            &[EvalType::Datetime, EvalType::String, EvalType::String],
        )?;
        bf.set_decimal_and_flen_for_datetime(decimal);
        bf.pb_code = ScalarFuncSig::ConvertTz;
        Ok(bf)
    }
);

function_class!(
    /// Go `makeDateFunctionClass`.
    MakeDateFunctionClass, Datetime, [Int, Int], MakeDate, |bf, _ctx| {
        bf.set_decimal_and_flen_for_date();
    }
);

string_class!(
    /// Go `makeTimeFunctionClass`.
    MakeTimeFunctionClass, |func_name, ctx, args| {
        let arg2_tp = type_of(&args[2]);
        let decimal = match arg2_tp.eval_type() {
            EvalType::Int => 0,
            EvalType::Real | EvalType::Decimal => {
                let decimal = arg2_tp.decimal();
                if decimal > 6 || decimal == UNSPECIFIED_LENGTH {
                    6
                } else {
                    decimal
                }
            }
            _ => 6,
        };
        // MySQL will cast the first and second arguments to INT, and the third argument to DECIMAL.
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::Duration,
            &[EvalType::Int, EvalType::Int, EvalType::Real],
        )?;
        bf.set_decimal_and_flen_for_time(decimal);
        bf.pb_code = ScalarFuncSig::MakeTime;
        Ok(bf)
    }
);

function_class!(
    /// Go `periodAddFunctionClass`.
    PeriodAddFunctionClass, Int, [Int, Int], Unspecified => crate::scalar_function::Kernel::Values(crate::scalar_function::class_kernels::period_add), |bf, _ctx| {
        bf.tp.set_flen(6);
    }
);

function_class!(
    /// Go `periodDiffFunctionClass`.
    PeriodDiffFunctionClass, Int, [Int, Int], Unspecified => crate::scalar_function::Kernel::Values(crate::scalar_function::class_kernels::period_diff), |bf, _ctx| {
        bf.tp.set_flen(6);
    }
);

function_class!(
    /// Go `quarterFunctionClass`.
    QuarterFunctionClass, Int, [Datetime], Quarter, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

string_class!(
    /// Go `secToTimeFunctionClass`.
    SecToTimeFunctionClass, |func_name, ctx, args| {
        let arg_type = type_of(&args[0]);
        // to match MySQL behavior more check issue #59428
        let mut ret_fsp = if crate::builtin::is_binary_literal(&args[0]) {
            MIN_FSP
        } else if arg_type.eval_type() == EvalType::String {
            UNSPECIFIED_LENGTH
        } else {
            arg_type.decimal()
        };
        if ret_fsp > MAX_FSP || ret_fsp == UNSPECIFIED_LENGTH {
            ret_fsp = MAX_FSP;
        } else if ret_fsp < MIN_FSP {
            ret_fsp = MIN_FSP;
        }
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::Duration,
            &[EvalType::Real],
        )?;
        bf.set_decimal_and_flen_for_time(ret_fsp);
        bf.pb_code = ScalarFuncSig::SecToTime;
        Ok(bf)
    }
);

string_class!(
    /// Go `subTimeFunctionClass`.
    SubTimeFunctionClass, |func_name, ctx, args| {
        time_add_sub_function(
            ctx,
            func_name,
            args,
            [
                ScalarFuncSig::SubDatetimeAndDuration,
                ScalarFuncSig::SubTimeDateTimeNull,
                ScalarFuncSig::SubDatetimeAndString,
                ScalarFuncSig::SubDateAndDuration,
                ScalarFuncSig::SubTimeStringNull,
                ScalarFuncSig::SubDateAndString,
                ScalarFuncSig::SubDurationAndDuration,
                ScalarFuncSig::SubTimeDurationNull,
                ScalarFuncSig::SubDurationAndString,
                ScalarFuncSig::SubStringAndDuration,
                ScalarFuncSig::SubStringAndString,
            ],
        )
    }
);

function_class!(
    /// Go `timeFormatFunctionClass`.
    TimeFormatFunctionClass, String, [Duration, String], TimeFormat, |bf, _ctx| {
        // worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
        let flen = (type_of(&bf.args[1]).flen() + 1) / 2 * 11;
        bf.tp.set_flen(flen);
    }
);

function_class!(
    /// Go `timeToSecFunctionClass`.
    TimeToSecFunctionClass, Int, [Duration], TimeToSec, |bf, _ctx| {
        bf.tp.set_flen(10);
    }
);

string_class!(
    /// Go `timestampAddFunctionClass`.
    TimestampAddFunctionClass, |func_name, ctx, args| {
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::String,
            &[EvalType::String, EvalType::Real, EvalType::Datetime],
        )?;
        let should_not_happen = || EvalError::Unsupported("should not happened");
        if !matches!(bf.args[0], Expression::Constant(_)) {
            return Err(should_not_happen());
        }
        let unit = match eval_string(ctx, &bf.args[0]) {
            Ok(Some(unit)) => unit,
            _ => return Err(should_not_happen()),
        };
        let flen = if unit == "MICROSECOND" {
            MAX_DATETIME_WIDTH_WITH_FSP
        } else {
            MAX_DATETIME_WIDTH_NO_FSP
        };
        bf.tp.set_flen(flen);
        bf.pb_code = ScalarFuncSig::TimestampAdd;
        Ok(bf)
    }
);

function_class!(
    /// Go `toDaysFunctionClass`.
    ToDaysFunctionClass, Int, [Datetime], ToDays
);

function_class!(
    /// Go `toSecondsFunctionClass`.
    ToSecondsFunctionClass, Int, [Datetime], ToSeconds
);

string_class!(
    /// Go `utcTimeFunctionClass`.
    UTCTimeFunctionClass, |func_name, ctx, args| {
        let arg_tps = fsp_arg_tps(&args);
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::Duration, arg_tps)?;
        let fsp = get_fsp_by_int_arg(ctx, &bf.args, func_name)?;
        bf.set_decimal_and_flen_for_time(fsp);
        // 1. no sign.
        // 2. hour is in the 2-digit range.
        bf.tp.set_flen(bf.tp.flen() - 2);
        bf.pb_code = if bf.args.len() == 1 {
            ScalarFuncSig::UtcTimeWithArg
        } else {
            ScalarFuncSig::UtcTimeWithoutArg
        };
        Ok(bf)
    }
);

function_class!(
    /// Go `lastDayFunctionClass`.
    LastDayFunctionClass, Datetime, [Datetime], LastDay, |bf, _ctx| {
        bf.set_decimal_and_flen_for_date();
    }
);

string_class!(
    /// Go `tidbParseTsoFunctionClass`.
    TidbParseTsoFunctionClass, |func_name, ctx, args| {
        let arg_tp = type_of(&args[0]).eval_type();
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, arg_tp, &[EvalType::Int])?;
        bf.tp.set_code(FieldTypeCode::Datetime);
        bf.tp.set_flen(MAX_DATE_WIDTH);
        bf.tp.set_decimal(DEFAULT_FSP);
        Ok(bf.evaluated_by(crate::scalar_function::Kernel::Values(crate::time_fn::session_tz::tidb_parse_tso)))
    }
);

function_class!(
    /// Go `tidbParseTsoLogicalFunctionClass`.
    TidbParseTsoLogicalFunctionClass, Int, [Int], Unspecified => crate::scalar_function::Kernel::Values(crate::scalar_function::class_kernels::tidb_parse_tso_logical)
);

function_class!(
    /// Go `tidbBoundedStalenessFunctionClass`.
    TidbBoundedStalenessFunctionClass, Datetime, [Datetime, Datetime], Unspecified => crate::scalar_function::Kernel::Values(crate::time_fn::tidb_bounded_staleness), |bf, _ctx| {
        bf.set_decimal_and_flen_for_datetime(3);
    }
);

function_class!(
    /// Go `tidbCurrentTsoFunctionClass`.
    TidbCurrentTsoFunctionClass, Int, [], Unspecified => crate::scalar_function::Kernel::Values(crate::time_fn::current_tso)
);
