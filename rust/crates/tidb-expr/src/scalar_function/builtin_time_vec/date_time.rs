// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{arithmetic::shift_datetime, text, vec_eval, ScalarFunction};
use crate::expression::Expression;
use crate::scalar_function::Kernel;
use crate::time_fn::duration_parse;
use crate::{Columns, EvalError, EvalErrorSql};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{
    types_errors, ConversionFlags, CoreTime, EvalType, FieldTypeCode, MySqlDuration,
    SessionTimeZone, Time, TimeError, TimeType,
};
use tidb_proto::tipb::ScalarFuncSig;

pub(super) fn arithmetic(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::{AddDateAndDuration, SubDateAndDuration, SubDateAndString};
    let sig = function.pb_code();
    let is_duration = matches!(sig, AddDateAndDuration | SubDateAndDuration);
    let subtract = matches!(sig, SubDateAndDuration | SubDateAndString);
    let left = vec_eval::read_argument(&function.args[0], EvalType::Datetime, ctx, input)?;
    let right = vec_eval::read_argument(
        &function.args[1],
        if is_duration {
            EvalType::Duration
        } else {
            EvalType::String
        },
        ctx,
        input,
    )?;
    let (left, right) = (left.cells(), right.cells());
    let left = left.fixed::<8>();
    let fsp = function.args[1]
        .static_type()
        .expect("typed DATE arithmetic argument")
        .decimal();
    let rows = input.num_rows();
    let mut scratch = String::new();
    result.reserve_string(rows);
    result.try_append_byte_rows(rows, |row, output| {
        let (Some(&packed), Some(right)) = (left(row), right.bytes(row)) else {
            return Ok(false);
        };
        let date = Time::from_go_raw_like_go(u64::from_ne_bytes(packed));
        if date.is_zero() {
            return Ok(false);
        }
        let delta = if is_duration {
            MySqlDuration::from_raw_parts(
                i64::from_ne_bytes(right.try_into().expect("TIME cell width")),
                fsp,
            )
        } else {
            if !duration_parse::is_duration_bytes(right) {
                return Ok(false);
            }
            let right = std::str::from_utf8(right).expect("duration pattern is ASCII");
            let fsp = i64::from(duration_parse::get_fsp_4_time_add_sub(right));
            let Some(delta) = text::parse_text_duration(right, fsp, ctx) else {
                return Ok(false);
            };
            delta
        };
        let nanos = if subtract {
            delta.nanoseconds().wrapping_neg()
        } else {
            delta.nanoseconds()
        };
        let date = Time::new(date.core_time(), TimeType::DateTime, 0).expect("zero FSP is valid");
        match shift_datetime(date, MySqlDuration::from_raw_parts(nanos, delta.fsp()), ctx) {
            Ok(time) => {
                scratch.clear();
                text::append_datetime(time, time.fsp(), &mut scratch);
                output.append(scratch.as_bytes());
                Ok(true)
            }
            Err(error) => {
                ctx.append_error_warning(&error.to_sql_error());
                Ok(false)
            }
        }
    })
}

pub(super) fn timestamp(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let is_float = function.sig == crate::builtin::SigFields::Timestamp { is_float: true };
    let (arg, domain) = timestamp_source(&function.args[0], is_float);
    let first = vec_eval::read_argument(arg, domain, ctx, input)?;
    let second = function
        .args
        .get(1)
        .map(|arg| vec_eval::read_argument(arg, EvalType::String, ctx, input))
        .transpose()?;
    let first = first.cells();
    let second = second.as_ref().map(|arg| arg.cells());
    let zone = ctx.time_zone();
    let flags = ctx.type_flags();
    result.resize_time(input.num_rows(), false);
    result.with_fixed_cells_mut(|output, bitmap| {
        for (row, output) in output.iter_mut().enumerate() {
            let convert = || -> Result<Option<Time>, EvalError> {
                let Some(first) = first.bytes(row) else {
                    return Ok(None);
                };
                let delta = match &second {
                    Some(second) => match second.bytes(row) {
                        Some(second) => Some(second),
                        None => return Ok(None),
                    },
                    None => None,
                };
                let Some(time) = parse_timestamp(first, domain, is_float, flags, &zone, ctx)?
                else {
                    return Ok(None);
                };
                let Some(delta) = delta else {
                    return Ok(Some(time));
                };
                if time.core_time().year() == 0 || !duration_parse::is_duration_bytes(delta) {
                    return Ok(None);
                }
                let text = std::str::from_utf8(delta).expect("duration pattern is ASCII");
                let fsp = i64::from(tidb_datatype::get_fsp(text));
                let Some(delta) = text::read_duration_text(text, fsp, ctx) else {
                    ctx.handle_truncate_error(text::duration_parse_error(text))?;
                    return Ok(None);
                };
                shift_datetime(time, delta, ctx).map(Some)
            };
            if let Some(time) = convert()? {
                *output = time.go_raw().to_ne_bytes();
            } else {
                bitmap[row / 8] &= !(1 << (row % 8));
            }
        }
        Ok(())
    })
}

/// Skip only casts whose full source representation fits in the string.
fn timestamp_source(arg: &Expression, is_float: bool) -> (&Expression, EvalType) {
    if let Expression::ScalarFunction(cast) = arg {
        if let Kernel::Cast {
            source,
            target: EvalType::String,
            in_union: false,
        } = cast.pb_builtin.kernel
        {
            let input = &cast.args[0];
            let field = input.static_type().expect("typed TIMESTAMP cast input");
            let target = cast.get_static_type().expect("typed TIMESTAMP cast");
            if target.code() == FieldTypeCode::String && target.is_binary_string() {
                return (arg, EvalType::String);
            }
            let width = match (source, field.code()) {
                (EvalType::Datetime, FieldTypeCode::Datetime) if !is_float => 26,
                (EvalType::Int, FieldTypeCode::LongLong) if is_float && !field.is_unsigned() => 20,
                _ => return (arg, EvalType::String),
            };
            let length = target.flen();
            if length < 0 || length >= width {
                return (input, source);
            }
        }
    }
    (arg, EvalType::String)
}

fn parse_timestamp(
    bytes: &[u8],
    domain: EvalType,
    is_float: bool,
    flags: ConversionFlags,
    zone: &SessionTimeZone,
    ctx: &dyn Columns,
) -> Result<Option<Time>, EvalError> {
    let text = match domain {
        EvalType::Datetime => {
            let time = Time::from_go_raw_like_go(u64::from_ne_bytes(
                bytes.try_into().expect("DATETIME cell width"),
            ));
            if time.kind() == TimeType::DateTime
                && time.core_time().microsecond() <= 999_999
                && time
                    .validate(
                        flags.ignore_zero_in_date_err(),
                        flags.ignore_invalid_date_err(),
                        zone,
                    )
                    .is_ok()
            {
                let core = time.core_time();
                let precision = 10_u32.pow(6 - u32::from(time.fsp()));
                let micros = core.microsecond() / precision * precision;
                if micros == core.microsecond() {
                    return Ok(Some(time));
                }
                let core = CoreTime::from_date(
                    core.year() as u16,
                    core.month(),
                    core.day(),
                    core.hour(),
                    core.minute(),
                    core.second(),
                    micros,
                );
                return Ok(Time::new(core, TimeType::DateTime, i64::from(time.fsp())).ok());
            }
            std::borrow::Cow::Owned(time.to_string())
        }
        EvalType::Int => {
            let value = i64::from_ne_bytes(bytes.try_into().expect("BIGINT cell width"));
            if let Ok(parsed) = tidb_datatype::parse_time_from_num(
                value,
                TimeType::DateTime,
                0,
                flags.ignore_zero_in_date_err(),
                flags.ignore_invalid_date_err(),
                flags.ignore_zero_date_err(),
                zone,
            ) {
                return Ok(Some(parsed.time));
            }
            std::borrow::Cow::Owned(value.to_string())
        }
        _ => String::from_utf8_lossy(bytes),
    };
    let fsp = i64::from(tidb_datatype::get_fsp(&text));
    match tidb_datatype::parse_time_with_flags(
        &text,
        TimeType::DateTime,
        fsp,
        is_float,
        flags,
        zone,
    ) {
        Ok(parsed) => {
            if parsed.truncated {
                ctx.append_go_warning(
                    &types_errors::ERR_TRUNCATED_WRONG_VAL,
                    &format!("Truncated incorrect datetime value: '{text}'"),
                );
            }
            Ok(Some(parsed.time))
        }
        Err(TimeError::Syntax(error)) => Err(EvalError::Raw(error)),
        Err(error) => {
            ctx.handle_truncate_error(text::datetime_parse_error(
                &text, error, fsp, is_float, zone,
            ))?;
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        column::Column as ExpressionColumn, evaluator::EvaluatorSuite,
        new_function::new_function_base, NoColumns,
    };
    use tidb_datatype::FieldType;

    fn run(name: &str, fields: &[FieldType], input: &mut Chunk, output_type: FieldType) -> Chunk {
        let args = fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let mut column = ExpressionColumn::new(index as i64 + 1, field.clone());
                column.index = index as i64;
                Expression::Column(column)
            })
            .collect();
        let function = new_function_base(&NoColumns, name, output_type.clone(), args).unwrap();
        let mut output = Chunk::new_chunk_with_capacity(&[output_type], input.num_rows());
        EvaluatorSuite::new(vec![function], false)
            .run(&NoColumns, input, &mut output)
            .unwrap();
        output
    }

    #[test]
    fn date_arithmetic_keeps_go_zero_calendar_with_nonzero_clock() {
        let mut duration = FieldType::new(FieldTypeCode::Duration);
        duration.set_decimal(6);
        let fields = [FieldType::new(FieldTypeCode::Date), duration];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
        let date = CoreTime::from_date(1, 1, 1, 0, 0, 0, 0);
        input.append_time(0, Time::new(date, TimeType::Date, 0).unwrap());
        input.append_duration(1, MySqlDuration::from_raw_parts(-1_000, 6));
        let output = run(
            "addtime",
            &fields,
            &mut input,
            FieldType::new(FieldTypeCode::VarString),
        );
        assert_eq!(output.column(0).get_raw(0), b"0000-00-00 23:59:59.999999");
    }

    #[test]
    fn timestamp_accepts_numeric_zero_without_losing_packed_metadata() {
        let fields = [FieldType::new(FieldTypeCode::LongLong)];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 2);
        input.append_int64(0, 0);
        input.append_null(0);
        input.set_sel(Some(vec![1, 0, 0]));
        let output = run(
            "timestamp",
            &fields,
            &mut input,
            FieldType::new(FieldTypeCode::Datetime),
        );
        let result = output.column(0);
        assert!(result.is_null(0));
        assert!(!result.is_null(1));
        assert_eq!(result.get_raw(1), [0; 8]);
        assert_eq!(result.get_raw(2), [0; 8]);
    }

    #[test]
    fn timestamp_keeps_the_go_numeric_zero_policy_in_strict_mode() {
        struct Strict;
        impl Columns for Strict {
            fn get(&self, _: &[String]) -> Option<tidb_datatype::Datum> {
                None
            }

            fn truncate_level(&self) -> crate::ErrorLevel {
                crate::ErrorLevel::Error
            }
        }
        let error = parse_timestamp(
            &0_i64.to_ne_bytes(),
            EvalType::Int,
            true,
            tidb_datatype::STRICT_FLAGS,
            &SessionTimeZone::utc(),
            &Strict,
        )
        .unwrap_err()
        .to_sql_error();
        assert_eq!(error.code, 1292);
        assert_eq!(error.message, "Incorrect datetime value: '0'");
    }

    #[test]
    fn timestamp_preserves_explicit_string_casts_with_distinct_conversion_rules() {
        let column = |code| Expression::Column(ExpressionColumn::new(1, FieldType::new(code)));
        let mut binary = FieldType::new(FieldTypeCode::String);
        binary.set_charset_name("binary");
        binary.set_collation_name("binary");
        binary.set_flen(32);
        let padded = crate::builtin_cast::build_cast_function(
            &crate::builtin::UnfoldedBuildContext,
            column(FieldTypeCode::Datetime),
            binary,
        )
        .unwrap();
        let mut string = FieldType::new(FieldTypeCode::VarString);
        string.set_flen(32);
        let explicit_text = crate::builtin_cast::build_cast_function(
            &crate::builtin::UnfoldedBuildContext,
            column(FieldTypeCode::LongLong),
            string,
        )
        .unwrap();
        assert_eq!(timestamp_source(&padded, false).1, EvalType::String);
        assert_eq!(timestamp_source(&explicit_text, false).1, EvalType::String);
    }

    #[test]
    fn timestamp_of_datetime_matches_the_source_string_precision() {
        let mut datetime = FieldType::new(FieldTypeCode::Datetime);
        datetime.set_decimal(2);
        let fields = [datetime.clone()];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
        let core = CoreTime::from_date(2024, 2, 29, 0, 0, 0, 123_456);
        input.append_time(0, Time::new(core, TimeType::DateTime, 2).unwrap());
        let output = run("timestamp", &fields, &mut input, datetime);
        let time = output.column(0).get_time(0);
        assert_eq!(time.fsp(), 2);
        assert_eq!(time.core_time().microsecond(), 120_000);
    }
}
