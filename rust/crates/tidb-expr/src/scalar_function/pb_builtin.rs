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

use super::{class_kernels, cast_json_argument_value, eval_numeric_operand_row, eval_numeric_row, ScalarFunction};
use tidb_exprctx::context::{Columns, EvalError};
use crate::expression::{ConstLevel, Expression};
use tidb_ast::BinaryOp;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, EvalType, FieldType, VectorFloat32};
use tidb_proto::tipb::ScalarFuncSig;

pub(crate) type ValuesKernel = fn(&[Datum], &dyn Columns) -> Result<Datum, EvalError>;
/// A signature that reads its node's build state -- Go `baseBuiltinFunc`'s
/// collator or an argument's field type -- beside the argument values.
pub(crate) type MethodKernel = fn(&ScalarFunction, &[Datum], &dyn Columns) -> Result<Datum, EvalError>;

/// Go `builtinStrcmpSig`: the comparison under `b.collation`.
fn strcmp_method(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::strcmp_with_collation(values, function.derived_collation())
}

/// Go `builtinFieldStringSig` compares under `b.collation`; the int and real
/// signatures read arguments their class already cast.
fn field_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::field_with_collation(values, function.derived_collation(), ctx)
}

/// Go `builtinHexStrArgSig` / `builtinHexIntArgSig` over the argument's type.
fn hex_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::hex_with_type(values, function.args.first().and_then(Expression::static_type), ctx)
}

/// Go `builtinFromDaysSig.evalTime`: a DATE.
fn from_days_values(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let date = crate::time_fn::calendar::from_days(values)?;
    crate::cast::parse_computed_time(&date, ctx, tidb_datatype::TimeType::Date, Some(0))
}

/// Go `builtinMakeDateSig.evalTime`: a DATE.
fn make_date_values(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let date = crate::time_fn::makedate(values)?;
    crate::cast::parse_computed_time(&date, ctx, tidb_datatype::TimeType::Date, Some(0))
}

/// Go `builtinMakeTimeSig.evalDuration`: a duration of the fsp its class
/// derived into the result type.
fn make_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let time = crate::time_fn::maketime(values, ctx)?;
    crate::cast::parse_computed_duration(&time, ctx, function.get_static_type().map(FieldType::decimal))
}

/// Go `builtinSysDateWithFspSig` / `builtinSysDateWithoutFspSig`: a DATETIME
/// of the result type's fsp.
fn sys_date_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let now = crate::time_fn::add_sub::sysdate(values, ctx)?;
    crate::cast::parse_computed_time(
        &now,
        ctx,
        tidb_datatype::TimeType::DateTime,
        function.get_static_type().map(FieldType::decimal),
    )
}

/// A signature that reads its arguments itself, or not at all.
pub(crate) type RowKernel = fn(&ScalarFunction, &dyn Columns, Row<'_>) -> Result<Datum, EvalError>;

/// Go `builtinFtsMatchWordSig.evalReal`: only a fulltext index computes a
/// relevance score.
fn fts_match_word_row(_: &ScalarFunction, _: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    Err(EvalError::Raw("cannot use 'FTS_MATCH_WORD()' outside of fulltext index".to_owned()))
}

/// Go `builtinFtsMysqlMatchAgainstSig.evalReal`: a NULL search constant is
/// NULL; anything else needs a fulltext index.
fn fts_match_against_row(function: &ScalarFunction, _: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    if let Some(Expression::Constant(against)) = function.args.first() {
        if against.value.is_null() {
            return Ok(Datum::Null);
        }
    }
    Err(EvalError::Raw("cannot use 'MATCH ... AGAINST' outside of fulltext index".to_owned()))
}

/// Go `builtinRandWithSeedFirstGenSig.evalReal`: a nonconstant seed starts a
/// fresh generator every row; this node's address is the occurrence's
/// identity.
fn rand_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let values = function
        .args
        .iter()
        .map(|arg| arg.eval(ctx, row))
        .collect::<Result<Vec<_>, _>>()?;
    crate::math_fn::eval_rand_values(&values, ctx, Some(std::ptr::from_ref(function) as usize), false)
}

/// Go `builtinRandSig.evalReal`: the generator the class seeded, one per
/// occurrence.
fn rand_const_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let values = function
        .args
        .iter()
        .map(|arg| arg.eval(ctx, row))
        .collect::<Result<Vec<_>, _>>()?;
    crate::math_fn::eval_rand_values(&values, ctx, Some(std::ptr::from_ref(function) as usize), true)
}

/// Go `builtinTruncate{Int,Uint,Real,Decimal}Sig`: the decimal signature caps
/// the scale by the result type's.
fn truncate_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::math_fn::round_or_truncate_with_result_decimal(
        values,
        false,
        function.ret_type.as_ref().map(FieldType::decimal),
        ctx,
    )
}

/// Go `builtinUnaryNotJSONSig`.
fn unary_not_json_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let value = function.args[0].eval(ctx, row)?;
    crate::ops::eval_unary(
        tidb_ast::UnaryOp::Not,
        value,
        crate::ops::Operand::Expr(&function.args[0]),
        ctx,
    )
}

/// The value a temporal signature computed, in the function's return type:
/// the chunk column Go writes it to carries that type's decimal.
pub(super) fn temporal_of_ret_type(function: &ScalarFunction, result: Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_datatype::{FieldTypeCode, TimeType};
    let ret_type = function.get_static_type();
    let decimal = ret_type.map(FieldType::decimal);
    match ret_type.map(FieldType::code) {
        Some(FieldTypeCode::Datetime) => {
            crate::cast::parse_computed_time(&result, ctx, TimeType::DateTime, decimal)
        }
        Some(FieldTypeCode::Date) => {
            crate::cast::parse_computed_time(&result, ctx, TimeType::Date, Some(0))
        }
        Some(FieldTypeCode::Duration) => crate::cast::parse_computed_duration(&result, ctx, decimal),
        _ => Ok(result),
    }
}

/// Go `builtin*TimeDiffSig`: the eight signatures `timeDiffFunctionClass`
/// picks from its arguments' eval types.
fn time_diff_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::time_fn::time_diff::TimeDiffArg::{Duration, String, Time};
    let kinds = match function.pb_code() {
        ScalarFuncSig::DurationDurationTimeDiff => [Duration, Duration],
        ScalarFuncSig::DurationStringTimeDiff => [Duration, String],
        ScalarFuncSig::StringDurationTimeDiff => [String, Duration],
        ScalarFuncSig::StringStringTimeDiff => [String, String],
        ScalarFuncSig::StringTimeTimeDiff => [String, Time],
        ScalarFuncSig::TimeStringTimeDiff => [Time, String],
        ScalarFuncSig::TimeTimeTimeDiff => [Time, Time],
        // `builtinNullTimeDiffSig` answers NULL without reading either side.
        _ => return Ok(Datum::Null),
    };
    let fsp = function
        .get_static_type()
        .map_or(tidb_datatype::MAX_FSP, FieldType::decimal);
    crate::time_fn::time_diff::time_diff(values, kinds, fsp, ctx)
}

/// Go `builtinAdd*Sig` / `builtinSub*Sig` of `ADDTIME` / `SUBTIME`: the
/// signature carries the two kinds its class switched on.
fn add_sub_time_of(function: &ScalarFunction, values: &[Datum], sign: i64, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_datatype::{FieldTypeCode, TimeType};
    let kinds = crate::time_fn::add_sub::kinds_of_signature(function.pb_code())
        .ok_or(EvalError::Unsupported("not an ADDTIME/SUBTIME signature"))?;
    let row_path = !function.runs_vectorized_body();
    let result = crate::time_fn::add_sub::add_sub_time(values, kinds, sign, row_path, ctx)?;
    match function.get_static_type().map(FieldType::code) {
        Some(FieldTypeCode::Datetime) => {
            crate::cast::parse_computed_time(&result, ctx, TimeType::DateTime, None)
        }
        Some(FieldTypeCode::Duration) => crate::cast::parse_computed_duration(&result, ctx, None),
        _ => Ok(result),
    }
}

fn add_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    add_sub_time_of(function, values, 1, ctx)
}

fn sub_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    add_sub_time_of(function, values, -1, ctx)
}

/// Go `builtinTimestamp1ArgSig` / `builtinTimestamp2ArgsSig`.
fn timestamp_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let is_float = function.sig == crate::builtin::SigFields::Timestamp { is_float: true };
    let result = crate::time_fn::add_sub::timestamp(values, is_float, ctx)?;
    crate::cast::parse_computed_time(&result, ctx, tidb_datatype::TimeType::DateTime, None)
}

/// Go `builtinNowWithArgSig` / `builtinNowWithoutArgSig`.
fn now_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    temporal_of_ret_type(function, crate::time_fn::now(values, ctx)?, ctx)
}

/// Go `builtinUTCTimestampWithArgSig` / `builtinUTCTimestampWithoutArgSig`.
fn utc_timestamp_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    temporal_of_ret_type(function, crate::time_fn::utc_timestamp(values, ctx)?, ctx)
}

/// Go `builtinCurrentTime0ArgSig` / `builtinCurrentTime1ArgSig`; the
/// function's own name is the one an fsp error reports.
fn current_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let name = if function.func_name.lowercase() == "curtime" {
        "curtime"
    } else {
        "current_time"
    };
    temporal_of_ret_type(function, crate::time_fn::current_time(values, name, ctx)?, ctx)
}

/// Go `builtinUTCTimeWithArgSig` / `builtinUTCTimeWithoutArgSig`.
fn utc_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    temporal_of_ret_type(function, crate::time_fn::utc_time(values, ctx)?, ctx)
}

/// Go `builtinSecToTimeSig`.
fn sec_to_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    temporal_of_ret_type(function, crate::time_fn::sec_to_time(values, ctx)?, ctx)
}

/// Go `builtinStrToDateDateSig` / `builtinStrToDateDatetimeSig` /
/// `builtinStrToDateDurationSig`: a time-only parse under the datetime
/// signature is the zero date with that time, which NO_ZERO_DATE refuses.
fn str_to_date_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let mut result = crate::time_fn::calendar::str_to_date(values, ctx)?;
    if function.pb_code() == ScalarFuncSig::StrToDateDatetime {
        if let Ok(text) = result.sql_string() {
            if text.contains(':') && !text.contains('-') {
                if ctx.date_modes().no_zero_date {
                    return Ok(Datum::Null);
                }
                result = Datum::new_string_datum(format!("0000-00-00 {text}"));
            }
        }
    }
    temporal_of_ret_type(function, result, ctx)
}

/// Go `builtinConvertTzSig.evalTime` reads its arguments in turn and answers
/// NULL at the first that is NULL or fails, the error dropped: a strict
/// write stores NULL where other builtins fail.
fn convert_tz_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let mut values = Vec::with_capacity(function.args.len());
    for argument in &function.args {
        match argument.eval(ctx, row) {
            Ok(Datum::Null) | Err(_) => return Ok(Datum::Null),
            Ok(value) => values.push(value),
        }
    }
    temporal_of_ret_type(function, crate::time_fn::convert_tz::convert_tz(&values)?, ctx)
}

/// Go `builtinGreatest*Sig`: the string signature compares under the
/// function's collation.
pub(crate) fn greatest_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    function.extremum_of(true, values, function.derived_collation(), ctx)
}

/// Go `builtinLeast*Sig`.
pub(crate) fn least_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    function.extremum_of(false, values, function.derived_collation(), ctx)
}

/// Go `builtinIntervalIntSig` / `builtinIntervalRealSig`.
fn interval_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let arg_types = function
        .args
        .iter()
        .map(|argument| argument.static_type().cloned())
        .collect::<Vec<_>>();
    crate::builtin_ext::interval_lazy(&arg_types, |index| function.args[index].eval(ctx, row), ctx)
}

/// Go `builtinAesEncryptSig.evalString` / `builtinAesDecryptSig.evalString`.
fn aes_row(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    row: Row<'_>,
    name: &'static str,
) -> Result<Datum, EvalError> {
    let mode = function.aes_mode().ok_or(EvalError::Unsupported("not an AES signature"))?;
    crate::builtin_ext::crypto::aes(name, mode, function.args.len(), |index| function.args[index].eval(ctx, row), ctx)
}

/// Go `builtinAesEncryptIVSig.evalString` / `builtinAesDecryptIVSig.evalString`.
fn aes_iv_row(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    row: Row<'_>,
    name: &'static str,
) -> Result<Datum, EvalError> {
    let mode = function.aes_mode().ok_or(EvalError::Unsupported("not an AES signature"))?;
    crate::builtin_ext::crypto::aes_iv(name, mode, |index| function.args[index].eval(ctx, row))
}

/// Go `builtinLastInsertIDSig.evalInt`: the session's previous last insert
/// id, in the unsigned domain.
fn last_insert_id_values(_: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    Ok(match ctx.last_insert_id() {
        Some(id) => Datum::UInt(id),
        None => Datum::Null,
    })
}

/// Go `builtinLastInsertIDWithIDSig.evalInt`: records the argument as the
/// session's last insert id and returns it; NULL for a NULL argument.
fn last_insert_id_with_id_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    if values[0].is_null() {
        return Ok(Datum::Null);
    }
    let cast = crate::cast::cast_arg_as_int(&values[0], function.args[0].static_type(), ctx)?;
    let recorded = match cast {
        Datum::Int(i) => i as u64,
        Datum::UInt(u) => u,
        _ => return Err(EvalError::Unsupported("LAST_INSERT_ID argument")),
    };
    ctx.set_last_insert_id(recorded);
    Ok(Datum::Int(recorded as i64))
}

/// Go `BuiltinGroupingImplSig.evalInt`: the planner installs the grouping
/// metadata before execution; a NULL grouping id is NULL.
fn grouping_method(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    let Some(grouping_id) = crate::arg_eval_type::eval_int(&values[0])? else {
        return Ok(Datum::Null);
    };
    let metadata = function
        .grouping_metadata
        .as_ref()
        .ok_or(EvalError::Unsupported("Meta data is not initialized"))?;
    Ok(Datum::UInt(metadata.eval(grouping_id as u64)))
}

fn ltrim_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::string2::ltrim(&values[0])
}

fn rtrim_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::string2::rtrim(&values[0])
}

fn left_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::str_take(values, true)
}

fn lpad_values(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_packet::pad(values, true, ctx)
}

fn rpad_values(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_packet::pad(values, false, ctx)
}

fn unhex_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::unhex(values)
}

fn oct_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::oct(values)
}

fn export_set_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::export_set(values)
}

fn make_set_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::make_set(values)
}

/// Go `builtinLocate2ArgsSig` / `builtinLocate2ArgsUTF8Sig`: the search runs
/// under the function's collation.
fn locate_2_args_method(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::locate(&values[0], &values[1], function.derived_collation())
}

/// Go `builtinLocate3ArgsSig` / `builtinLocate3ArgsUTF8Sig`.
fn locate_3_args_method(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::locate_with_position(values, function.derived_collation())
}

/// Go `builtinInstrSig` / `builtinInstrUTF8Sig`.
fn instr_method(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::instr(&values[0], &values[1], function.derived_collation())
}

/// Go `builtinTrim1ArgSig` / `builtinTrim2ArgsSig` / `builtinTrim3ArgsSig`:
/// the optional second argument is the string to remove (a space by default)
/// and the optional third an `ast.TrimDirectionType` integer.
fn trim_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let value = function.args[0].eval(ctx, row)?;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let remstr = match function.args.get(1) {
        Some(argument) => argument.eval(ctx, row)?,
        None => Datum::new_string_datum(" "),
    };
    if remstr.is_null() {
        return Ok(Datum::Null);
    }
    let direction = match function.args.get(2) {
        Some(argument) => {
            let direction = argument.eval(ctx, row)?;
            if direction.is_null() {
                return Ok(Datum::Null);
            }
            match direction.get_int64() {
                2 => tidb_ast::TrimDirection::Leading,
                3 => tidb_ast::TrimDirection::Trailing,
                _ => tidb_ast::TrimDirection::Both,
            }
        }
        None => tidb_ast::TrimDirection::Both,
    };
    let binary = matches!(value, Datum::Bytes(_));
    let text = crate::coerce::coerce_str_bytes(&value)?;
    let remove = crate::coerce::coerce_str_bytes(&remstr)?;
    Ok(crate::string_fn::trim_value(text, remove, direction, binary))
}

/// Go `builtinConvertSig.evalString`.
fn convert_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let value = function.args[0].eval(ctx, row)?;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let target = function.args[1].eval(ctx, row)?;
    let target = crate::coerce::coerce_str_bytes(&target)?.unwrap_or_default();
    let target = String::from_utf8_lossy(&target).into_owned();
    let arg_type = function.args[0]
        .static_type()
        .cloned()
        .unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::VarString));
    let value = crate::cast::cast_arg_as_string(&value, Some(&arg_type), ctx)?;
    let explicit_collation = crate::collation_derive::coercibility_of(&function.args[0])
        == crate::expr_collation::Coercibility::EXPLICIT;
    let string_type = crate::cast::cast_arg_as_string_type(
        &arg_type,
        explicit_collation,
        ctx.connection_charset_info(),
    );
    crate::convert_charset::convert_using(&value, &string_type, &target)
}

fn right_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::str_take(values, false)
}

/// Go `builtinCeil*Sig` / `builtinFloor*Sig`: an `*IntToInt` or `*Real`
/// signature keeps its argument's domain; a `*DecTo*` one answers the
/// signature's own result domain.
fn ceil_values(values: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::math_fn::ceil_floor_with_result_domain(values, true, None, ctx)
}

fn floor_values(values: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::math_fn::ceil_floor_with_result_domain(values, false, None, ctx)
}

fn ceil_dec_to_int(values: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::math_fn::ceil_floor_with_result_domain(values, true, Some(false), ctx)
}

fn ceil_dec_to_dec(values: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::math_fn::ceil_floor_with_result_domain(values, true, Some(true), ctx)
}

fn floor_dec_to_int(values: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::math_fn::ceil_floor_with_result_domain(values, false, Some(false), ctx)
}

fn floor_dec_to_dec(values: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::math_fn::ceil_floor_with_result_domain(values, false, Some(true), ctx)
}

/// Go `IsBinaryLiteral`.
fn is_binary_literal(expr: &Expression) -> bool {
    matches!(expr, Expression::Constant(constant) if matches!(constant.value, Datum::BinaryLiteral(_)))
}

/// Go `builtinConvSig.evalString`'s first switch: a binary literal, or a
/// cast over a hybrid-typed or binary-literal argument, is read as its bits.
fn conv_bit_literal(
    arg: &Expression,
    ctx: &dyn Columns,
    row: Row<'_>,
) -> Result<Option<Datum>, EvalError> {
    match arg {
        Expression::Constant(constant) => Ok(is_binary_literal(arg).then(|| constant.value.clone())),
        Expression::ScalarFunction(cast) if cast.is_cast() => {
            let Some(arg0) = cast.args.first() else {
                return Ok(None);
            };
            let hybrid = arg0.static_type().is_some_and(FieldType::is_hybrid);
            if !hybrid && !is_binary_literal(arg0) {
                return Ok(None);
            }
            // Go `Column.EvalString` of a hybrid column is `Datum.ToString`:
            // a BIT value is its bytes.
            Ok(Some(match eval_numeric_row(arg0, ctx, row, EvalType::String)? {
                Datum::Bit(bits) => Datum::BinaryLiteral(bits),
                text => match crate::arg_eval_type::eval_string(&text)? {
                    Some(bytes) => Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(bytes)),
                    None => Datum::Null,
                },
            }))
        }
        _ => Ok(None),
    }
}

fn md5_values(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::crypto::dispatch("MD5", values, ctx)
        .unwrap_or(Err(EvalError::Unsupported("protobuf MD5 arity")))
}

fn crc32_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::math_fn::crc32(values)
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Kernel {
    Binary(BinaryOp, EvalType),
    /// Go comparisons evaluate both typed operands before applying NULL semantics.
    Compare(BinaryOp, EvalType),
    In(EvalType),
    Like,
    Ilike,
    Conv,
    DateArithmetic {
        subtract: bool,
    },
    Logic(BinaryOp),
    /// Go `builtinBitNegSig` / `builtinUnaryMinus*Sig` over their one
    /// argument.
    Unary(tidb_ast::UnaryOp),
    IntegerMod {
        unsigned: [bool; 2],
    },
    IntegerMultiply {
        unsigned: bool,
    },
    IsNull(EvalType),
    /// `negate` is `UnaryNot`; `keep_null` separates Go's `*IsTrueWithNull`
    /// (NULL stays NULL) from `*IsTrue` / `*IsFalse` (NULL is 0, Go
    /// `builtinIntIsTrueSig{keepNull: false}`); `falsity` is `*IsFalse`.
    Truth {
        negate: bool,
        domain: EvalType,
        keep_null: bool,
        falsity: bool,
    },
    /// Go `builtinCoalesce*Sig`: the first non-NULL argument, each read in
    /// the result's evaluation type.
    Coalesce(EvalType),
    Case(EvalType),
    If(EvalType),
    IfNull(EvalType),
    Cast {
        source: EvalType,
        target: EvalType,
        /// Go `baseBuiltinCastFunc.inUnion`.
        in_union: bool,
    },
    String {
        operation: StringOp,
        binary: bool,
    },
    Round,
    /// Go `builtinInternalToBinarySig` / `builtinInternalFromBinarySig`. The
    /// wire signature carries no `cannotConvertStringAsWarning`, so a
    /// decoded `from_binary` errors on bytes its charset cannot hold.
    ConvertCharset {
        from_binary: bool,
    },
    FromUnixTime,
    Regexp,
    /// The positional regexp builtins, evaluated by the same method the
    /// root tier uses so the derived collation seeds the match type.
    RegexpFamily(&'static str),
    Json,
    Values(ValuesKernel),
    Method(MethodKernel),
    Row(RowKernel),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StringOp {
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
            LtDuration => Kernel::Compare(BinaryOp::Lt, EvalType::Duration),
            LeDuration => Kernel::Compare(BinaryOp::Le, EvalType::Duration),
            GtDuration => Kernel::Compare(BinaryOp::Gt, EvalType::Duration),
            GeDuration => Kernel::Compare(BinaryOp::Ge, EvalType::Duration),
            EqDuration => Kernel::Compare(BinaryOp::Eq, EvalType::Duration),
            NeDuration => Kernel::Compare(BinaryOp::Ne, EvalType::Duration),
            NullEqDuration => Kernel::Compare(BinaryOp::NullEq, EvalType::Duration),
            LtJson => Kernel::Compare(BinaryOp::Lt, EvalType::Json),
            LeJson => Kernel::Compare(BinaryOp::Le, EvalType::Json),
            GtJson => Kernel::Compare(BinaryOp::Gt, EvalType::Json),
            GeJson => Kernel::Compare(BinaryOp::Ge, EvalType::Json),
            EqJson => Kernel::Compare(BinaryOp::Eq, EvalType::Json),
            NeJson => Kernel::Compare(BinaryOp::Ne, EvalType::Json),
            NullEqJson => Kernel::Compare(BinaryOp::NullEq, EvalType::Json),
            LtVectorFloat32 => Kernel::Compare(BinaryOp::Lt, EvalType::VectorFloat32),
            LeVectorFloat32 => Kernel::Compare(BinaryOp::Le, EvalType::VectorFloat32),
            GtVectorFloat32 => Kernel::Compare(BinaryOp::Gt, EvalType::VectorFloat32),
            GeVectorFloat32 => Kernel::Compare(BinaryOp::Ge, EvalType::VectorFloat32),
            EqVectorFloat32 => Kernel::Compare(BinaryOp::Eq, EvalType::VectorFloat32),
            NeVectorFloat32 => Kernel::Compare(BinaryOp::Ne, EvalType::VectorFloat32),
            NullEqVectorFloat32 => Kernel::Compare(BinaryOp::NullEq, EvalType::VectorFloat32),
            InReal => Kernel::In(EvalType::Real),
            InDecimal => Kernel::In(EvalType::Decimal),
            InTime => Kernel::In(EvalType::Datetime),
            InDuration => Kernel::In(EvalType::Duration),
            InJson => Kernel::In(EvalType::Json),
            LikeSig => Kernel::Like,
            IlikeSig => Kernel::Ilike,
            FtsMatchWord => Kernel::Row(fts_match_word_row),
            FtsMatchExpression => Kernel::Row(fts_match_against_row),
            LogicalAnd => Kernel::Logic(BinaryOp::LogicAnd),
            LogicalOr => Kernel::Logic(BinaryOp::LogicOr),
            LogicalXor => Kernel::Logic(BinaryOp::LogicXor),
            NullEqInt => Kernel::Compare(BinaryOp::NullEq, EvalType::Int),
            NullEqReal => Kernel::Compare(BinaryOp::NullEq, EvalType::Real),
            NullEqDecimal => Kernel::Compare(BinaryOp::NullEq, EvalType::Decimal),
            NullEqString => Kernel::Compare(BinaryOp::NullEq, EvalType::String),
            NullEqTime => Kernel::Compare(BinaryOp::NullEq, EvalType::Datetime),
            BitAndSig => Kernel::Binary(BinaryOp::BitAnd, EvalType::Int),
            BitOrSig => Kernel::Binary(BinaryOp::BitOr, EvalType::Int),
            BitXorSig => Kernel::Binary(BinaryOp::BitXor, EvalType::Int),
            LeftShift => Kernel::Binary(BinaryOp::LeftShift, EvalType::Int),
            RightShift => Kernel::Binary(BinaryOp::RightShift, EvalType::Int),
            BitNegSig => Kernel::Unary(tidb_ast::UnaryOp::BitNeg),
            UnaryMinusInt | UnaryMinusDecimal | UnaryMinusReal => Kernel::Unary(tidb_ast::UnaryOp::Minus),
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
                keep_null: true,
                falsity: false,
            },
            UnaryNotReal => Kernel::Truth {
                negate: true,
                domain: EvalType::Real,
                keep_null: true,
                falsity: false,
            },
            UnaryNotDecimal => Kernel::Truth {
                negate: true,
                domain: EvalType::Decimal,
                keep_null: true,
                falsity: false,
            },
            IntIsTrueWithNull => Kernel::Truth {
                negate: false,
                domain: EvalType::Int,
                keep_null: true,
                falsity: false,
            },
            RealIsTrueWithNull => Kernel::Truth {
                negate: false,
                domain: EvalType::Real,
                keep_null: true,
                falsity: false,
            },
            DecimalIsTrueWithNull => Kernel::Truth {
                negate: false,
                domain: EvalType::Decimal,
                keep_null: true,
                falsity: false,
            },
            IntIsTrue => Kernel::Truth {
                negate: false,
                domain: EvalType::Int,
                keep_null: false,
                falsity: false,
            },
            RealIsTrue => Kernel::Truth {
                negate: false,
                domain: EvalType::Real,
                keep_null: false,
                falsity: false,
            },
            DecimalIsTrue => Kernel::Truth {
                negate: false,
                domain: EvalType::Decimal,
                keep_null: false,
                falsity: false,
            },
            IntIsFalse => Kernel::Truth {
                negate: false,
                domain: EvalType::Int,
                keep_null: false,
                falsity: true,
            },
            RealIsFalse => Kernel::Truth {
                negate: false,
                domain: EvalType::Real,
                keep_null: false,
                falsity: true,
            },
            DecimalIsFalse => Kernel::Truth {
                negate: false,
                domain: EvalType::Decimal,
                keep_null: false,
                falsity: true,
            },
            IntIsFalseWithNull => Kernel::Truth {
                negate: false,
                domain: EvalType::Int,
                keep_null: true,
                falsity: true,
            },
            RealIsFalseWithNull => Kernel::Truth {
                negate: false,
                domain: EvalType::Real,
                keep_null: true,
                falsity: true,
            },
            DecimalIsFalseWithNull => Kernel::Truth {
                negate: false,
                domain: EvalType::Decimal,
                keep_null: true,
                falsity: true,
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
            CoalesceInt => Kernel::Coalesce(EvalType::Int),
            CoalesceReal => Kernel::Coalesce(EvalType::Real),
            CoalesceDecimal => Kernel::Coalesce(EvalType::Decimal),
            CoalesceString => Kernel::Coalesce(EvalType::String),
            CoalesceTime => Kernel::Coalesce(EvalType::Datetime),
            CoalesceDuration => Kernel::Coalesce(EvalType::Duration),
            CoalesceJson => Kernel::Coalesce(EvalType::Json),
            // Go `builtinLengthSig`: the argument's byte length, whatever its
            // collation -- `CharLength`'s binary spelling.
            Length => Kernel::String {
                operation: StringOp::Length,
                binary: true,
            },
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
            ToBinary => Kernel::ConvertCharset { from_binary: false },
            FromBinary => Kernel::ConvertCharset { from_binary: true },
            // Go `absFunctionClass`: one signature per argument evaluation
            // type; the planner has already cast the argument to it.
            AbsInt | AbsUInt | AbsReal | AbsDecimal => Kernel::Values(crate::math_fn::abs),
            // Go `builtinMD5Sig`, `builtinCRC32Sig`, `builtinConcatWSSig`:
            // the root tier's evaluators over the argument values.
            Sqrt => Kernel::Values(crate::math_fn::sqrt),
            Exp => Kernel::Values(crate::math_fn::exp),
            Log1Arg | Log2Args => Kernel::Values(crate::math_fn::log),
            Log2 => Kernel::Values(crate::math_fn::log2),
            Log10 => Kernel::Values(crate::math_fn::log10),
            Radians => Kernel::Values(crate::math_fn::radians),
            Degrees => Kernel::Values(crate::math_fn::degrees),
            Sign => Kernel::Values(crate::math_fn::sign),
            CeilIntToInt | CeilReal => Kernel::Values(ceil_values),
            FloorIntToInt | FloorReal => Kernel::Values(floor_values),
            CeilDecToInt => Kernel::Values(ceil_dec_to_int),
            CeilDecToDec => Kernel::Values(ceil_dec_to_dec),
            FloorDecToInt => Kernel::Values(floor_dec_to_int),
            FloorDecToDec => Kernel::Values(floor_dec_to_dec),
            VecDimsSig => Kernel::Values(|values, _| crate::builtin_ext::vec::dims(&values[0])),
            VecL2NormSig => Kernel::Values(|values, _| crate::builtin_ext::vec::l2_norm(&values[0])),
            VecAsTextSig => Kernel::Values(|values, _| crate::builtin_ext::vec::as_text(&values[0])),
            VecL1DistanceSig => Kernel::Values(|values, _| {
                crate::builtin_ext::vec::distance(&values[0], &values[1], VectorFloat32::l1_distance)
            }),
            VecL2DistanceSig => Kernel::Values(|values, _| {
                crate::builtin_ext::vec::distance(&values[0], &values[1], VectorFloat32::l2_distance)
            }),
            VecNegativeInnerProductSig => Kernel::Values(|values, _| {
                crate::builtin_ext::vec::distance(&values[0], &values[1], VectorFloat32::negative_inner_product)
            }),
            VecCosineDistanceSig => Kernel::Values(|values, _| {
                crate::builtin_ext::vec::distance(&values[0], &values[1], VectorFloat32::cosine_distance)
            }),
            Uuid => Kernel::Values(|_, _| crate::builtin_ext::misc::uuid_v1()),
            UuidTimestamp => Kernel::Values(|values, _| crate::builtin_ext::misc::uuid_timestamp(&values[0])),
            UuidVersion => Kernel::Values(|values, _| crate::builtin_ext::misc::uuid_version(&values[0])),
            SysDateWithoutFsp | SysDateWithFsp => Kernel::Method(sys_date_method),
            RandWithSeedFirstGen => Kernel::Row(rand_row),
            Sha1 => Kernel::Values(|values, _| crate::builtin_ext::crypto::hash_unary::<sha1::Sha1>(&values[0])),
            Sha2 => Kernel::Values(|values, _| crate::builtin_ext::crypto::sha2_hash(&values[0], &values[1])),
            UncompressedLength => Kernel::Values(|values, ctx| {
                crate::builtin_ext::crypto::uncompressed_length(&values[0], ctx)
            }),
            JsonTypeSig | JsonUnquoteSig | JsonLengthSig | JsonObjectSig | JsonArraySig
            | JsonMergeSig | JsonSetSig | JsonInsertSig | JsonRemoveSig | JsonContainsSig
            | JsonValidJsonSig | JsonValidStringSig | JsonValidOthersSig => Kernel::Json,
            DayOfMonth => Kernel::Values(|values, _| crate::time_fn::day_of_month(values)),
            DayOfWeek => Kernel::Values(crate::time_fn::day_of_week),
            DayOfYear => Kernel::Values(crate::time_fn::day_of_year),
            WeekOfYear => Kernel::Values(crate::time_fn::week_of_year_builtin),
            Year => Kernel::Values(|values, _| crate::time_fn::calendar::date_part(values, |date| date.0)),
            MonthName => Kernel::Values(crate::time_fn::monthname),
            FromDays => Kernel::Values(from_days_values),
            MakeDate => Kernel::Values(make_date_values),
            MakeTime => Kernel::Method(make_time_method),
            TimeToSec => Kernel::Values(|values, _| crate::time_fn::time_to_sec(values)),
            Ascii => Kernel::Values(|values, _| crate::string_fn::ascii(values)),
            BitLength => Kernel::Values(|values, _| crate::string_fn::bit_length(values)),
            Reverse | ReverseUtf8 => Kernel::Values(|values, _| crate::string_fn::reverse(values)),
            Replace => Kernel::Values(|values, _| crate::string_fn::replace(values)),
            Strcmp => Kernel::Method(strcmp_method),
            Elt => Kernel::Values(|values, _| crate::string_fn::elt(values)),
            FieldInt | FieldReal | FieldString => Kernel::Method(field_method),
            HexStrArg | HexIntArg => Kernel::Method(hex_method),
            LTrim => Kernel::Values(ltrim_values),
            RTrim => Kernel::Values(rtrim_values),
            Right | RightUtf8 => Kernel::Values(right_values),
            Space => Kernel::Values(crate::string_packet::space),
            Md5 => Kernel::Values(md5_values),
            Crc32 => Kernel::Values(crc32_values),
            Concat => Kernel::Values(crate::string_fn::concat_with_context),
            ConcatWs => Kernel::Values(crate::string_fn::concat_ws_with_context),
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
            DateFormatSig => Kernel::Values(|values, cols| {
                let [date, format] = values else {
                    return Err(EvalError::WrongParameterCount("date_format"));
                };
                crate::time_fn::calendar::date_format_in(date, format, Some(cols))
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
            WeekWithMode | WeekWithoutMode => Kernel::Values(|values, ctx| {
                crate::time_fn::week(values, ctx.default_week_format(), ctx)
            }),
            TimestampDiff => {
                Kernel::Values(|values, _| crate::time_fn::calendar::timestamp_diff(values))
            }
            ExtractDatetimeFromString => Kernel::Values(|values, ctx| {
                crate::time_fn::extract::extract_values(
                    values,
                    ctx,
                    crate::time_fn::extract::extract_datetime_from_string,
                )
            }),
            ExtractDatetime => Kernel::Values(|values, ctx| {
                crate::time_fn::extract::extract_values(
                    values,
                    ctx,
                    crate::time_fn::extract::extract_datetime,
                )
            }),
            ExtractDuration => Kernel::Values(|values, ctx| {
                crate::time_fn::extract::extract_values(
                    values,
                    ctx,
                    crate::time_fn::extract::extract_duration,
                )
            }),
            UnixTimestampInt | UnixTimestampDec => {
                Kernel::Method(crate::time_fn::session_tz::unix_timestamp)
            }
            FromUnixTime1Arg | FromUnixTime2Arg => Kernel::FromUnixTime,
            RegexpLikeSig => Kernel::Regexp,
            RegexpSubstrSig => Kernel::RegexpFamily("REGEXP_SUBSTR"),
            RegexpInStrSig => Kernel::RegexpFamily("REGEXP_INSTR"),
            RegexpReplaceSig => Kernel::RegexpFamily("REGEXP_REPLACE"),
            JsonMemberOfSig | JsonReplaceSig | JsonArrayAppendSig | JsonMergePatchSig
            | JsonExtractSig => {
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
            // builtin_math.go
            Tan => Kernel::Method(class_kernels::tan),
            // builtin_string.go
            Bin => Kernel::Values(crate::string_fn::bin),
            SubstringIndex => Kernel::Values(|values, _| crate::string_fn::substring_index(values)),
            ToBase64 => Kernel::Values(crate::string_packet::to_base64),
            FromBase64 => Kernel::Values(|values, ctx| {
                crate::string_fn::from_base64_with_packet_limit(values, Some(ctx))
            }),
            Quote => Kernel::Method(class_kernels::quote),
            Ord => Kernel::Method(class_kernels::ord),
            Repeat => Kernel::Method(class_kernels::repeat),
            FindInSet => Kernel::Row(class_kernels::find_in_set),
            // builtin_time.go
            WeekDay => Kernel::Values(crate::time_fn::weekday),
            Quarter => Kernel::Values(|values, _| crate::time_fn::quarter(values)),
            GetFormat => Kernel::Values(|values, _| crate::time_fn::get_format_value(values)),
            DayName => Kernel::Values(crate::time_fn::dayname),
            TimeFormat => Kernel::Values(|values, _| crate::time_fn::time_format(values)),
            ToDays => Kernel::Values(crate::time_fn::calendar::to_days),
            ToSeconds => Kernel::Values(crate::time_fn::calendar::to_seconds),
            LastDay => Kernel::Method(class_kernels::last_day),
            // builtin_miscellaneous.go
            InetAton => Kernel::Values(|values, _| crate::builtin_ext::compare2::inet_aton(&values[0])),
            InetNtoa => Kernel::Values(|values, ctx| crate::builtin_ext::compare2::inet_ntoa(&values[0], ctx)),
            Inet6Aton => Kernel::Values(|values, _| crate::builtin_ext::compare2::inet6_aton(&values[0])),
            Inet6Ntoa => Kernel::Values(|values, _| crate::builtin_ext::compare2::inet6_ntoa(&values[0])),
            IsIPv4 => Kernel::Method(|_, values, _| crate::builtin_ext::compare2::is_ipv4_value(&values[0])),
            IsIPv4Compat => {
                Kernel::Method(|_, values, _| crate::builtin_ext::compare2::is_ipv4_compat_value(&values[0]))
            }
            IsIPv4Mapped => {
                Kernel::Method(|_, values, _| crate::builtin_ext::compare2::is_ipv4_mapped_value(&values[0]))
            }
            IsIPv6 => Kernel::Method(|_, values, _| crate::builtin_ext::compare2::is_ipv6_value(&values[0])),
            UuiDv4 => Kernel::Method(|_, _, _| crate::builtin_ext::misc::uuid_v4()),
            UuiDv7 => Kernel::Method(|_, _, _| crate::builtin_ext::misc::uuid_v7()),
            DecimalAnyValue | DurationAnyValue | IntAnyValue | JsonAnyValue | RealAnyValue
            | StringAnyValue | TimeAnyValue | VectorFloat32AnyValue => {
                Kernel::Values(|values, _| Ok(values[0].clone()))
            }
            // builtin_encryption.go
            Compress => Kernel::Values(|values, _| crate::builtin_ext::crypto::compress(&values[0])),
            Uncompress => Kernel::Values(|values, ctx| crate::builtin_ext::crypto::uncompress(&values[0], ctx)),
            Password => Kernel::Method(class_kernels::password),
            // builtin_info.go
            RowCount => Kernel::Method(|_, values, ctx| class_kernels::row_count(values, ctx)),
            _ => {
                let (source, target) = cast_types(signature)?;
                Kernel::Cast { source, target, in_union: false }
            }
        };
        Some(Self { signature, kernel })
    }

    /// Go `builtinFunc.PbCode()` of the function a class's `getFunction`
    /// returned: the signature the class set, decodable or not.
    pub(crate) fn of_builtin_func(signature: ScalarFuncSig, kernel: Option<Kernel>) -> Self {
        let kernel = kernel
            .or_else(|| Self::new(signature).map(|builtin| builtin.kernel))
            .or_else(|| Self::class_only(signature))
            .unwrap_or(Kernel::Values(class_kernels::not_yet_ported));
        Self { signature, kernel }
    }

    /// The evaluation of a signature a class sets and `getSignatureByPB`
    /// does not decode.
    fn class_only(signature: ScalarFuncSig) -> Option<Kernel> {
        use ScalarFuncSig::*;
        Some(match signature {
            // builtin_miscellaneous.go
            IsUuid => Kernel::Values(|values, _| crate::builtin_ext::misc::is_uuid(&values[0])),
            TiDbShard => Kernel::Values(|values, ctx| crate::builtin_ext::misc::tidb_shard(&values[0], ctx)),
            VitessHash => Kernel::Values(|values, ctx| crate::builtin_ext::misc::vitess_hash(&values[0], ctx)),
            // builtin_encryption.go
            Encode => Kernel::Values(|values, _| crate::builtin_ext::crypto::sql_encode(&values[0], &values[1])),
            Decode => Kernel::Values(|values, _| crate::builtin_ext::crypto::sql_decode(&values[0], &values[1])),
            // builtin_string.go
            Left | LeftUtf8 => Kernel::Values(left_values),
            Lpad | LpadUtf8 => Kernel::Values(lpad_values),
            Rpad | RpadUtf8 => Kernel::Values(rpad_values),
            Locate2Args | Locate2ArgsUtf8 => Kernel::Method(locate_2_args_method),
            Locate3Args | Locate3ArgsUtf8 => Kernel::Method(locate_3_args_method),
            Instr | InstrUtf8 => Kernel::Method(instr_method),
            UnHex => Kernel::Values(unhex_values),
            OctInt | OctString => Kernel::Values(oct_values),
            ExportSet3Arg | ExportSet4Arg | ExportSet5Arg => Kernel::Values(export_set_values),
            MakeSet => Kernel::Values(make_set_values),
            Format => Kernel::Values(crate::string_fn::format_num),
            FormatWithLocale => Kernel::Values(crate::builtin_ext::string2::format_with_locale),
            Insert | InsertUtf8 => Kernel::Values(crate::string_fn::insert_with_context),
            Char => Kernel::Values(crate::string_fn::char_func_with_context),
            Trim1Arg | Trim2Args | Trim3Args => Kernel::Row(trim_row),
            Convert => Kernel::Row(convert_row),
            // builtin_math.go
            Rand => Kernel::Row(rand_const_row),
            RoundWithFracInt | RoundWithFracReal | RoundWithFracDec => Kernel::Round,
            TruncateInt | TruncateUint | TruncateReal | TruncateDecimal => {
                Kernel::Method(truncate_method)
            }
            // builtin_json.go
            JsonMergePreserveSig | JsonArrayInsertSig | JsonContainsPathSig | JsonDepthSig
            | JsonKeysSig | JsonKeys2ArgsSig | JsonPrettySig | JsonQuoteSig | JsonSearchSig
            | JsonStorageFreeSig | JsonStorageSizeSig => Kernel::Json,
            // builtin_compare.go
            GreatestInt | GreatestReal | GreatestDecimal | GreatestString | GreatestTime
            | GreatestDate | GreatestDuration | GreatestCmpStringAsTime
            | GreatestCmpStringAsDate => Kernel::Method(greatest_method),
            LeastInt | LeastReal | LeastDecimal | LeastString | LeastTime | LeastDate
            | LeastDuration | LeastCmpStringAsTime | LeastCmpStringAsDate => {
                Kernel::Method(least_method)
            }
            IntervalInt | IntervalReal => Kernel::Row(interval_row),
            // builtin_encryption.go
            AesEncrypt => Kernel::Row(|function, ctx, row| aes_row(function, ctx, row, "aes_encrypt")),
            AesEncryptIv => Kernel::Row(|function, ctx, row| aes_iv_row(function, ctx, row, "aes_encrypt")),
            AesDecrypt => Kernel::Row(|function, ctx, row| aes_row(function, ctx, row, "aes_decrypt")),
            AesDecryptIv => Kernel::Row(|function, ctx, row| aes_iv_row(function, ctx, row, "aes_decrypt")),
            // builtin_info.go
            LastInsertId => Kernel::Values(last_insert_id_values),
            LastInsertIdWithId => Kernel::Method(last_insert_id_with_id_method),
            // builtin_grouping.go
            GroupingSig => Kernel::Method(grouping_method),
            // builtin_op.go
            UnaryNotJson => Kernel::Row(unary_not_json_row),
            // builtin_time.go
            DurationDurationTimeDiff | DurationStringTimeDiff | StringDurationTimeDiff
            | StringStringTimeDiff | StringTimeTimeDiff | TimeStringTimeDiff | TimeTimeTimeDiff
            | NullTimeDiff => Kernel::Method(time_diff_method),
            AddDatetimeAndDuration | AddTimeDateTimeNull | AddDatetimeAndString
            | AddDateAndDuration | AddTimeStringNull | AddDateAndString | AddDurationAndDuration
            | AddTimeDurationNull | AddDurationAndString | AddStringAndDuration
            | AddStringAndString => Kernel::Method(add_time_method),
            SubDatetimeAndDuration | SubTimeDateTimeNull | SubDatetimeAndString
            | SubDateAndDuration | SubTimeStringNull | SubDateAndString | SubDurationAndDuration
            | SubTimeDurationNull | SubDurationAndString | SubStringAndDuration
            | SubStringAndString => Kernel::Method(sub_time_method),
            Timestamp1Arg | Timestamp2Args => Kernel::Method(timestamp_method),
            NowWithArg | NowWithoutArg => Kernel::Method(now_method),
            UtcTimestampWithArg | UtcTimestampWithoutArg => Kernel::Method(utc_timestamp_method),
            CurrentTime0Arg | CurrentTime1Arg => Kernel::Method(current_time_method),
            UtcTimeWithArg | UtcTimeWithoutArg => Kernel::Method(utc_time_method),
            SecToTime => Kernel::Method(sec_to_time_method),
            StrToDateDate | StrToDateDatetime | StrToDateDuration => {
                Kernel::Method(str_to_date_method)
            }
            ConvertTz => Kernel::Row(convert_tz_row),
            Time => Kernel::Values(crate::time_fn::time),
            YearWeekWithMode | YearWeekWithoutMode => Kernel::Values(crate::time_fn::yearweek),
            UnixTimestampCurrent => Kernel::Values(crate::time_fn::session_tz::unix_timestamp_current),
            TimestampAdd => Kernel::Values(crate::time_fn::add_sub::timestamp_add),
            _ => return None,
        })
    }

    /// Go `newBaseBuiltinCastFunc(bf, inUnion)` of the signature a cast
    /// class's `getFunction` set.
    pub(crate) fn of_cast_func(signature: ScalarFuncSig, kernel: Option<Kernel>, in_union: bool) -> Self {
        let mut builtin = Self::of_builtin_func(signature, kernel);
        if let Kernel::Cast { source, target, .. } = builtin.kernel {
            builtin.kernel = Kernel::Cast { source, target, in_union };
        }
        builtin
    }

    pub(super) fn cast_in_union(self) -> bool {
        matches!(self.kernel, Kernel::Cast { in_union: true, .. })
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
            Kernel::Truth {
                negate,
                domain,
                keep_null,
                falsity,
            } => {
                let value = typed_argument(0, domain)?;
                Ok(match crate::truthy_of(&value)? {
                    None if keep_null => Datum::Null,
                    None => Datum::Int(0),
                    Some(value) => Datum::Int(i64::from(value ^ negate ^ falsity)),
                })
            }
            Kernel::Coalesce(domain) => {
                for index in 0..args.len() {
                    let value = typed_argument(index, domain)?;
                    if !value.is_null() {
                        return Ok(value);
                    }
                }
                Ok(Datum::Null)
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
                    // Go `builtinLogicXorSig`: true when exactly one is.
                    (BinaryOp::LogicXor, Some(left), Some(right)) => Datum::Int(i64::from(left != right)),
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
                    match super::arithmetic_overflow_expression(function, BinaryOp::Mul, ctx) {
                        Some(expression) => EvalError::DataOutOfRange {
                            value: if unsigned {
                                "BIGINT UNSIGNED"
                            } else {
                                "BIGINT"
                            },
                            expression,
                        },
                        None => EvalError::IntOverflow,
                    }
                })
            }
            Kernel::IntegerMod { unsigned } => {
                // The four Go MOD signatures bake signedness into the builtin,
                // rather than reselecting it from each row or the SQL name.
                let operand = |index: usize| -> Result<Datum, EvalError> {
                    let value = typed_argument(index, EvalType::Int)?;
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
                // Go `builtinArithmeticPlusRealSig` and `builtinArithmeticModRealSig`
                // read both operands before the NULL check; the other
                // signatures return on a NULL left.
                let reads_both =
                    domain == EvalType::Real && matches!(op, BinaryOp::Plus | BinaryOp::Mod);
                if left.is_null() && !reads_both {
                    return Ok(Datum::Null);
                }
                let right = eval_numeric_operand_row(&args[1], ctx, row, domain)?;
                function.eval_binary_values(op, left, right, ctx)
            }
            Kernel::Unary(op) => {
                let [arg] = args.as_slice() else {
                    return Err(EvalError::Unsupported("protobuf unary builtin arity"));
                };
                let value = arg.eval(ctx, row)?;
                crate::ops::eval_unary(op, value, crate::ops::Operand::Expr(arg), ctx)
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
                let text = match conv_bit_literal(&args[0], ctx, row)? {
                    Some(literal) => literal,
                    None => typed_argument(0, EvalType::String)?,
                };
                crate::math_fn::conv(&[text, from, to])
            }
            Kernel::Like => function.eval_like(ctx, row, false),
            Kernel::Ilike => function.eval_like(ctx, row, true),
            Kernel::Row(eval) => eval(function, ctx, row),
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
            Kernel::Cast { source, target, in_union } => {
                if args.len() != 1 {
                    return Err(EvalError::Unsupported("protobuf cast arity"));
                }
                if source == EvalType::String
                    && matches!(target, EvalType::Real | EvalType::Decimal)
                    && super::is_numeric_binary_literal(&args[0])
                {
                    return eval_numeric_row(&args[0], ctx, row, target);
                }
                // Go `builtinCastStringAsStringSig.evalString`: `EvalString`
                // of an ENUM or SET is its name, whatever `EnumSetAsIntFlag`.
                let value = if function.pb_code() == ScalarFuncSig::CastStringAsString {
                    args[0].eval_string(ctx, row)?
                } else {
                    eval_numeric_row(&args[0], ctx, row, source)?
                };
                if value.is_null() {
                    return Ok(Datum::Null);
                }
                // Go `builtinCastStringAs{Int,Real}Sig`: "Take the implicit
                // evalInt path if possible."
                if let Some(number) = super::can_implicit_eval_int(&args[0], &value, target) {
                    return Ok(number);
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
                        Some(&args[0]),
                        args[0].static_type(),
                        field,
                        in_union,
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
                let fsp = field.decimal().max(0);
                match target {
                    EvalType::Datetime | EvalType::Timestamp => {
                        let (tp, fsp) = match field.code() {
                            tidb_datatype::FieldTypeCode::Date => (tidb_datatype::TimeType::Date, 0),
                            _ => (tidb_datatype::TimeType::DateTime, fsp),
                        };
                        crate::cast::cast_to_time(&value, args[0].static_type(), ctx, tp, fsp)
                    }
                    EvalType::Duration => {
                        crate::cast::cast_to_duration(&value, args[0].static_type(), ctx, fsp)
                    }
                    EvalType::VectorFloat32 => crate::cast::cast_as_vector_float32(value, field),
                    _ => Err(EvalError::Unsupported("protobuf cast target")),
                }
            }
            Kernel::Regexp => function.eval_regexp_like(ctx, row),
            kernel => {
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    let value = arg.eval(ctx, row)?;
                    // The root tier's evaluators read a NULL argument
                    // themselves: Go `builtinConcatWSSig` skips a NULL member
                    // and `builtinWeekWithModeSig` reads a NULL mode as 0.
                    if value.is_null() && !matches!(kernel, Kernel::Json | Kernel::Values(_) | Kernel::Method(_)) {
                        return Ok(Datum::Null);
                    }
                    values.push(value);
                }
                match kernel {
                    Kernel::Values(eval) => eval(&values, ctx),
                    Kernel::Method(eval) => eval(function, &values, ctx),
                    Kernel::RegexpFamily(name) => function.eval_regexp_family(name, &values, ctx),
                    Kernel::ConvertCharset { from_binary } => {
                        let [value] = values.as_slice() else {
                            return Err(EvalError::Unsupported("protobuf charset builtin arity"));
                        };
                        if from_binary {
                            let charset = function
                                .get_static_type()
                                .map_or("binary", FieldType::charset_name);
                            crate::convert_charset::from_binary(
                                value,
                                charset,
                                function.cannot_convert_string_as_warning,
                                ctx,
                            )
                        } else {
                            let charset = args[0]
                                .static_type()
                                .map_or("binary", FieldType::charset_name);
                            crate::convert_charset::to_binary(value, charset)
                        }
                    }
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
                                Datum::new_string_datum(bytes)
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
                            StringOp::Upper => crate::string_fn::case_convert_with_type(
                                &values,
                                true,
                                args[0].static_type(),
                                binary,
                            ),
                            StringOp::Lower => crate::string_fn::case_convert_with_type(
                                &values,
                                false,
                                args[0].static_type(),
                                binary,
                            ),
                            StringOp::Substring => crate::string_fn::substring(&values, ctx),
                        }
                    }
                    Kernel::FromUnixTime => {
                        let result = crate::time_fn::session_tz::from_unixtime_with_precision(
                            &values,
                            ctx,
                            function.get_static_type().map(FieldType::decimal),
                        )?;
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
        CastVectorFloat32AsString => (EvalType::VectorFloat32, EvalType::String),
        CastVectorFloat32AsVectorFloat32 => (EvalType::VectorFloat32, EvalType::VectorFloat32),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constant::Constant;
    use crate::expression::Expression;
    use tidb_datatype::FieldTypeCode;
    use tidb_proto::tipb;

    fn literal(domain: EvalType) -> tipb::Expr {
        let (value, code) = match domain {
            EvalType::Int => (Datum::Int(1), FieldTypeCode::LongLong),
            EvalType::Real => (Datum::Real(1.0), FieldTypeCode::Double),
            EvalType::Decimal => (
                Datum::Decimal(tidb_datatype::Decimal::from_literal("1")),
                FieldTypeCode::NewDecimal,
            ),
            EvalType::String => (Datum::new_string_datum("DAY"), FieldTypeCode::VarString),
            EvalType::Datetime => (
                Datum::Time(
                    tidb_datatype::parse_datetime("2008-02-20", &chrono::Utc, true, false)
                        .unwrap()
                        .time,
                ),
                FieldTypeCode::Datetime,
            ),
            EvalType::Duration => (
                Datum::Duration(
                    tidb_datatype::MySqlDuration::from_nanoseconds(3_600_000_000_000, 0).unwrap(),
                ),
                FieldTypeCode::Duration,
            ),
            EvalType::VectorFloat32 => (
                Datum::VectorFloat32(VectorFloat32::parse("[1,2]").unwrap()),
                FieldTypeCode::VectorFloat32,
            ),
            domain => panic!("no literal for {domain:?}"),
        };
        let constant = Expression::Constant(Constant::new(value, FieldType::new(code)));
        crate::expr_to_pb::PbConverter::new(&crate::NoColumns, false, &tidb_datatype::SessionTimeZone::utc())
            .expr_to_pb(&constant)
            .unwrap()
    }

    fn eval(signature: ScalarFuncSig, children: Vec<tipb::Expr>) -> Datum {
        let pb = tipb::Expr {
            tp: Some(tipb::ExprType::ScalarFunc as i32),
            sig: Some(signature as i32),
            children,
            field_type: Some(tipb::FieldType {
                tp: Some(8),
                ..Default::default()
            }),
            ..Default::default()
        };
        let row = tidb_chunk::mutrow::MutRow::mut_row_from_datums(&[]);
        crate::distsql_builtin::pb_to_expr(&pb, &[])
            .unwrap_or_else(|error| panic!("{signature:?}: {error}"))
            .eval(&crate::NoColumns, row.to_row())
            .unwrap_or_else(|error| panic!("{signature:?}: {error:?}"))
    }

    /// The signatures evaluated by the root tier's evaluators see their NULL
    /// arguments. Go answers NULL for a NULL argument of every one of them
    /// but two: `builtinConcatWSSig` skips a NULL member after the separator
    /// (`TestConcatWS`), and `builtinWeekWithModeSig` reads a NULL mode as 0
    /// (`TestWeek`).
    #[test]
    fn a_null_argument_is_read_by_the_signature_as_go_does() {
        let null = tipb::Expr {
            tp: Some(tipb::ExprType::Null as i32),
            ..Default::default()
        };
        let text = |value: &str| {
            let constant = Expression::Constant(Constant::new(
                Datum::new_string_datum(value),
                FieldType::new(FieldTypeCode::VarString),
            ));
            crate::expr_to_pb::PbConverter::new(&crate::NoColumns, false, &tidb_datatype::SessionTimeZone::utc())
            .expr_to_pb(&constant)
            .unwrap()
        };
        let mut swept = std::collections::BTreeSet::new();
        // `ceil` and `floor` resolve outside the catalog's rows.
        let rounding = {
            use ScalarFuncSig::*;
            [
                (CeilIntToInt, &[EvalType::Int]),
                (CeilReal, &[EvalType::Real]),
                (CeilDecToInt, &[EvalType::Decimal]),
                (CeilDecToDec, &[EvalType::Decimal]),
                (FloorIntToInt, &[EvalType::Int]),
                (FloorReal, &[EvalType::Real]),
                (FloorDecToInt, &[EvalType::Decimal]),
                (FloorDecToDec, &[EvalType::Decimal]),
            ]
        };
        let built = crate::tests::pb_code_matrix_source::signature_arg_types();
        let rows = built
            .iter()
            .map(|(sig, domains)| (*sig, &domains[..]))
            .chain(rounding.iter().map(|(sig, domains)| (*sig, &domains[..])));
        for (sig, arg_types) in rows {
            let Some(Kernel::Values(_)) = PbBuiltin::new(sig).map(|builtin| builtin.kernel) else {
                continue;
            };
            swept.insert(sig);
            // A NULL member of CONCAT_WS is skipped; its rows are below.
            let nullable = if sig == ScalarFuncSig::ConcatWs { 1 } else { arg_types.len() };
            for position in 0..nullable {
                let children = arg_types
                    .iter()
                    .enumerate()
                    .map(|(index, domain)| {
                        if index == position {
                            null.clone()
                        } else {
                            literal(*domain)
                        }
                    })
                    .collect();
                let expect = if (sig, position) == (ScalarFuncSig::WeekWithMode, 1) {
                    Datum::Int(7)
                } else {
                    Datum::Null
                };
                assert_eq!(eval(sig, children), expect, "{sig:?} {position}");
            }
        }
        assert!(swept.len() > 40, "{swept:?}");

        use ScalarFuncSig::{Concat, ConcatWs};
        let bytes = |value: Datum| value.sql_bytes().unwrap();
        assert_eq!(
            eval(Concat, vec![text("a"), null.clone(), text("b")]),
            Datum::Null
        );
        assert_eq!(eval(ConcatWs, vec![null.clone(), null.clone()]), Datum::Null);
        assert_eq!(
            eval(ConcatWs, vec![null.clone(), text("a"), text("b")]),
            Datum::Null
        );
        assert_eq!(
            bytes(eval(
                ConcatWs,
                vec![text("|"), text("a"), null.clone(), text("b"), text("c")]
            )),
            b"a|b|c"
        );
        assert_eq!(
            bytes(eval(
                ConcatWs,
                vec![text(","), text("a"), text(","), text("b"), text("c")]
            )),
            b"a,,,b,c"
        );
    }
}
