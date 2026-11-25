// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{
    arithmetic::{add_to_datetime, shift_datetime},
    vec_eval, ScalarFunction,
};
use crate::{
    time_fn::{duration_parse, time_diff, time_text},
    Columns, EvalError,
};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{
    types_errors, DurationParseError, EvalType, FieldTypeFlags, MySqlDuration, SessionTimeZone,
    Time, TimeType,
};
use tidb_proto::tipb::ScalarFuncSig;

pub(super) fn time(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let text = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let text = text.cells();
    result.resize_go_duration(input.num_rows(), false);
    result.with_fixed_cells_mut(|output, bitmap| {
        for (row, output) in output.iter_mut().enumerate() {
            if let Some(text) = text.bytes(row) {
                *output = time_text(&String::from_utf8_lossy(text), ctx)?
                    .nanoseconds()
                    .to_ne_bytes();
            } else {
                bitmap[row / 8] &= !(1 << (row % 8));
            }
        }
        Ok(())
    })
}

pub(super) fn arithmetic(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::{AddDatetimeAndString, SubDatetimeAndString, SubDurationAndString};
    let sig = function.pb_code();
    let datetime = matches!(sig, AddDatetimeAndString | SubDatetimeAndString);
    let subtract = matches!(sig, SubDatetimeAndString | SubDurationAndString);
    let domain = if datetime {
        EvalType::Datetime
    } else {
        EvalType::Duration
    };
    vec_eval::eval_into(&function.args[0], domain, ctx, input, result)?;
    let right = vec_eval::read_argument(&function.args[1], EvalType::String, ctx, input)?;
    let right = right.cells();
    result.with_fixed_cells_mut(|output, bitmap| {
        for (row, output) in output.iter_mut().enumerate() {
            let bit = 1 << (row & 7);
            let presence = &mut bitmap[row >> 3];
            if *presence & bit == 0 {
                continue;
            }
            if let Some(right) = right.bytes(row) {
                let raw = i64::from_ne_bytes(*output);
                let time = Time::from_go_raw_like_go(raw as u64);
                if let Some(duration) = (!datetime || !time.is_zero())
                    .then(|| parse_operand(right, ctx))
                    .flatten()
                {
                    let nanos = duration.nanoseconds();
                    let value = if datetime {
                        let delta = if subtract {
                            MySqlDuration::from_raw_parts(-nanos, duration.fsp())
                        } else {
                            duration
                        };
                        add_to_datetime(time, delta, ctx)?
                    } else {
                        let nanos = if subtract {
                            time_diff::subtract_duration(raw, nanos)?
                        } else {
                            raw.checked_add(nanos)
                                .ok_or_else(|| time_diff::duration_overflow(raw, nanos))?
                        };
                        nanos.to_ne_bytes()
                    };
                    *output = value;
                    continue;
                }
            }
            *presence &= !bit;
        }
        Ok(())
    })
}

/// The generated ADDTIME/SUBTIME signatures warn even in strict statements
/// after the duration-pattern check accepts text that ParseDuration rejects.
fn parse_operand(text: &[u8], ctx: &dyn Columns) -> Option<MySqlDuration> {
    if !duration_parse::is_duration_bytes(text) {
        return None;
    }
    let text = std::str::from_utf8(text).expect("duration pattern is ASCII");
    let fsp = i64::from(duration_parse::get_fsp(text));
    parse_text_duration(text, fsp, ctx)
}

pub(super) fn parse_text_duration(
    text: &str,
    fsp: i64,
    ctx: &dyn Columns,
) -> Option<MySqlDuration> {
    if let Some(duration) = read_duration_text(text, fsp, ctx) {
        return Some(duration);
    }
    ctx.append_error_warning(&duration_parse_error(text).to_sql_error());
    None
}

pub(super) fn read_duration_text(text: &str, fsp: i64, ctx: &dyn Columns) -> Option<MySqlDuration> {
    match tidb_datatype::parse_duration(text.trim().as_bytes(), fsp) {
        Ok(parsed) if parsed.event().is_none() => {
            return Some(MySqlDuration::from_raw_parts(
                parsed.nanoseconds(),
                parsed.fsp(),
            ));
        }
        Err(DurationParseError::DateTimeFallback(_)) => {
            if let Ok(parsed) = crate::cast::parse_duration(ctx, text, fsp) {
                if parsed.event.is_none() {
                    return Some(parsed.value);
                }
            }
        }
        _ => {}
    }
    None
}

#[cold]
pub(super) fn duration_parse_error(text: &str) -> tidb_terror::TerrorError {
    types_errors::ERR_TRUNCATED_WRONG_VAL.generate(format!(
        "Truncated incorrect time value: '{}'",
        tidb_datatype::warning_subject_byte_cap(text),
    ))
}

pub(super) fn string_arithmetic(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::{AddStringAndDuration, SubStringAndDuration, SubStringAndString};
    let sig = function.pb_code();
    let typed_duration = matches!(sig, AddStringAndDuration | SubStringAndDuration);
    let subtract = matches!(sig, SubStringAndDuration | SubStringAndString);
    let left = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let rows = input.num_rows();
    if !typed_duration
        && function.args[1]
            .static_type()
            .is_some_and(|field| field.flags() & FieldTypeFlags::BINARY != 0)
    {
        result.reserve_string(rows);
        return result.try_append_byte_rows(rows, |_, _| Ok(false));
    }
    let target = if typed_duration {
        EvalType::Duration
    } else {
        EvalType::String
    };
    let right = vec_eval::read_argument(&function.args[1], target, ctx, input)?;
    let (left, right) = (left.cells(), right.cells());
    let zone = ctx.time_zone();
    let mut scratch = String::new();
    result.reserve_string(rows);
    result.try_append_byte_rows(rows, |row, output| {
        let (Some(left), Some(right)) = (left.bytes(row), right.bytes(row)) else {
            return Ok(false);
        };
        let delta = if typed_duration {
            i64::from_ne_bytes(right.try_into().expect("TIME cell width"))
        } else {
            let text = String::from_utf8_lossy(right);
            let fsp = i64::from(duration_parse::get_fsp_4_time_add_sub(&text));
            let Some(duration) = parse_text_duration(&text, fsp, ctx) else {
                return Ok(false);
            };
            duration.nanoseconds()
        };
        scratch.clear();
        if duration_parse::is_duration_bytes(left) {
            let text = std::str::from_utf8(left).expect("duration pattern is ASCII");
            let Some(duration) = parse_text_duration(text, 6, ctx) else {
                return Ok(false);
            };
            let base = duration.nanoseconds();
            let nanos = if subtract {
                time_diff::subtract_duration(base, delta)?
            } else {
                base.checked_add(delta)
                    .ok_or_else(|| time_diff::duration_overflow(base, delta))?
            };
            let duration = MySqlDuration::from_raw_parts(nanos, 6);
            if nanos < 0 {
                scratch.push('-');
            }
            let pattern = if duration.microsecond() == 0 {
                "%T"
            } else {
                "%T.%f"
            };
            duration.append_duration_format(pattern, &mut scratch);
        } else {
            let text = String::from_utf8_lossy(left);
            let Some(time) = parse_arithmetic_date(&text, &zone, ctx) else {
                return Ok(false);
            };
            let delta = if subtract {
                delta.wrapping_neg()
            } else {
                delta
            };
            let time = shift_datetime(time, MySqlDuration::from_raw_parts(delta, 6), ctx)?;
            let fsp = if time.core_time().microsecond() == 0 {
                0
            } else {
                6
            };
            append_datetime(time, fsp, &mut scratch);
        }
        output.append(scratch.as_bytes());
        Ok(true)
    })
}

pub(super) fn append_datetime(time: Time, fsp: u8, output: &mut String) {
    let pattern = if fsp == 0 {
        "%Y-%m-%d %T"
    } else {
        "%Y-%m-%d %T.%f"
    };
    time.append_date_format(pattern, output)
        .expect("normalized calendar fields");
    if fsp != 0 && fsp < 6 {
        output.truncate(output.len() - (6 - usize::from(fsp)));
    }
}

fn parse_arithmetic_date(text: &str, zone: &SessionTimeZone, ctx: &dyn Columns) -> Option<Time> {
    let flags = ctx.type_flags();
    match tidb_datatype::parse_time(
        text,
        TimeType::DateTime,
        6,
        false,
        flags.ignore_zero_in_date_err(),
        flags.ignore_invalid_date_err(),
        zone,
    ) {
        Ok(parsed) => {
            if parsed.truncated {
                ctx.append_go_warning(
                    &types_errors::ERR_TRUNCATED_WRONG_VAL,
                    &format!("Truncated incorrect datetime value: '{text}'"),
                );
            }
            Some(parsed.time)
        }
        Err(error) => {
            let error = datetime_parse_error(text, error, 6, false, zone);
            ctx.append_error_warning(&error.to_sql_error());
            None
        }
    }
}

#[cold]
pub(super) fn datetime_parse_error(
    text: &str,
    error: tidb_datatype::TimeError,
    fsp: i64,
    is_float: bool,
    zone: &SessionTimeZone,
) -> tidb_terror::TerrorError {
    if error == tidb_datatype::TimeError::InvalidDate {
        // checkMonthDay reports the parsed calendar fields without padding.
        if let Ok(parsed) =
            tidb_datatype::parse_time(text, TimeType::DateTime, fsp, is_float, true, true, zone)
        {
            let core = parsed.time.core_time();
            return types_errors::ERR_WRONG_VALUE.generate(format!(
                "Incorrect datetime value: '{}-{}-{}'",
                core.year(),
                core.month(),
                core.day(),
            ));
        }
    }
    let trimmed = text.trim();
    let numeric_error = !is_float
        && !trimmed.is_empty()
        && trimmed.bytes().all(|byte| byte.is_ascii_digit())
        && trimmed
            .parse::<i64>()
            .map_or(true, |value| value > 99_991_231);
    let kind = if numeric_error { "time" } else { "datetime" };
    types_errors::ERR_WRONG_VALUE.generate(format!(
        "Incorrect {kind} value: '{}'",
        tidb_datatype::warning_subject_byte_cap(text),
    ))
}

pub(super) fn difference(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use EvalType::{Datetime, Duration, String};
    use ScalarFuncSig::{
        DurationStringTimeDiff, StringDurationTimeDiff, StringTimeTimeDiff, TimeStringTimeDiff,
    };
    let [left_type, right_type] = match function.pb_code() {
        DurationStringTimeDiff => [Duration, String],
        StringDurationTimeDiff => [String, Duration],
        StringTimeTimeDiff => [String, Datetime],
        TimeStringTimeDiff => [Datetime, String],
        _ => [String, String],
    };
    let left = vec_eval::read_argument(&function.args[0], left_type, ctx, input)?;
    let right = vec_eval::read_argument(&function.args[1], right_type, ctx, input)?;
    let (left, right) = (left.cells(), right.cells());
    let fsp = function.get_static_type().expect("TIMEDIFF type").decimal();
    let zone = ctx.time_zone();
    let read = |bytes: &[u8], kind| match kind {
        String => time_diff::convert_string_to_duration(
            &std::string::String::from_utf8_lossy(bytes),
            fsp,
            &zone,
            ctx,
        ),
        Duration => Ok(time_diff::Operand::Duration(MySqlDuration::from_raw_parts(
            i64::from_ne_bytes(bytes.try_into().expect("TIME cell width")),
            fsp,
        ))),
        Datetime => Ok(time_diff::Operand::Time(Time::from_go_raw_like_go(
            u64::from_ne_bytes(bytes.try_into().expect("DATETIME cell width")),
        ))),
        _ => unreachable!("TIMEDIFF argument type"),
    };
    result.resize_go_duration(input.num_rows(), false);
    result.with_fixed_cells_mut(|output, bitmap| {
        for (row, output) in output.iter_mut().enumerate() {
            if let (Some(a), Some(b)) = (left.bytes(row), right.bytes(row)) {
                if let Some(nanos) =
                    time_diff::difference(read(a, left_type)?, read(b, right_type)?, ctx)?
                {
                    *output = nanos.nanoseconds().to_ne_bytes();
                    continue;
                }
            }
            bitmap[row / 8] &= !(1 << (row % 8));
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        column::Column as ExpressionColumn, evaluator::EvaluatorSuite, expression::Expression,
        new_function::new_function_base, NoColumns,
    };
    use tidb_datatype::{FieldType, FieldTypeCode, MAX_TIME_NANOS};

    fn suite(name: &str, input: &[FieldType], output: &FieldType) -> EvaluatorSuite {
        let args = input
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let mut column = ExpressionColumn::new(index as i64 + 1, field.clone());
                column.index = index as i64;
                Expression::Column(column)
            })
            .collect();
        let expression = new_function_base(&NoColumns, name, output.clone(), args).unwrap();
        EvaluatorSuite::new(vec![expression], false)
    }

    #[test]
    fn string_left_addtime_preserves_zero_date_and_invalid_date_warning() {
        use std::cell::RefCell;
        #[derive(Default)]
        struct Strict(RefCell<Vec<(u16, String)>>);
        impl Columns for Strict {
            fn get(&self, _: &[String]) -> Option<tidb_datatype::Datum> {
                None
            }
            fn truncate_level(&self) -> crate::ErrorLevel {
                crate::ErrorLevel::Error
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.0.borrow_mut().push((code, message.to_owned()));
            }
        }
        let text = FieldType::new(FieldTypeCode::VarString);
        let fields = [text.clone(), text.clone()];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 2);
        for left in ["0000-00-00 00:00:00", "2024-02-30 12:34:56"] {
            input.append_string(0, left);
            input.append_string(1, "00:00:01.123456");
        }
        input.set_sel(Some(vec![1, 0]));
        let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&text), 2);
        let ctx = Strict::default();
        suite("addtime", &fields, &text)
            .run(&ctx, &mut input, &mut output)
            .unwrap();
        let result = output.column(0);
        assert!(result.is_null(0));
        assert_eq!(result.get_raw(1), b"0000-00-00 00:00:01.123456");
        assert_eq!(
            ctx.0.into_inner(),
            [(1292, "Incorrect datetime value: '2024-2-30'".to_owned())]
        );
    }

    #[test]
    fn string_left_addtime_preserves_submicrosecond_input_until_formatting() {
        let text = FieldType::new(FieldTypeCode::VarString);
        let mut duration = FieldType::new(FieldTypeCode::Duration);
        duration.set_decimal(6);
        let fields = [text.clone(), duration];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
        input.append_string(0, "838:59:59");
        input.append_duration(1, MySqlDuration::from_raw_parts(-1_234_567_890, 6));
        let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&text), 1);
        suite("addtime", &fields, &text)
            .run(&NoColumns, &mut input, &mut output)
            .unwrap();
        assert_eq!(output.column(0).get_raw(0), b"838:59:57.765432");
    }

    #[test]
    fn string_arithmetic_with_binary_right_column_produces_null() {
        let text = FieldType::new(FieldTypeCode::VarString);
        let mut binary = text.clone();
        binary.add_flags(FieldTypeFlags::BINARY);
        let fields = [text.clone(), binary];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
        input.append_string(0, "01:29:59.999999");
        input.append_string(1, "0.000001");
        let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&text), 1);
        for name in ["addtime", "subtime"] {
            suite(name, &fields, &text)
                .run(&NoColumns, &mut input, &mut output)
                .unwrap();
            assert!(output.column(0).is_null(0));
        }
    }

    #[test]
    fn time_format_reads_go_duration_fields_and_discards_a_dangling_percent() {
        let text = FieldType::new(FieldTypeCode::VarString);
        let mut duration = FieldType::new(FieldTypeCode::Duration);
        duration.set_decimal(6);
        let fields = [duration, text.clone()];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 2);
        for mask in [b"%T %p\xff%".as_slice(), b"%".as_slice()] {
            input.append_duration(0, MySqlDuration::from_raw_parts(-93_600_000_000_000, 6));
            input.append_bytes(1, mask);
        }
        input.set_sel(Some(vec![1, 0, 1]));
        let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&text), 3);
        suite("time_format", &fields, &text)
            .run(&NoColumns, &mut input, &mut output)
            .unwrap();
        let result = output.column(0);
        assert_eq!(result.get_raw(0), b"");
        assert!(!result.is_null(0));
        assert_eq!(result.get_raw(1), "26:00:00 AM�".as_bytes());
        assert_eq!(result.get_raw(2), b"");
    }

    #[test]
    fn subtime_typed_duration_with_text_keeps_go_out_of_range_sum() {
        let text = FieldType::new(FieldTypeCode::VarString);
        let duration = FieldType::new(FieldTypeCode::Duration);
        let fields = [duration.clone(), text];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 2);
        for right in ["-838:59:59", "\u{a0}01:02:03\u{a0}"] {
            input.append_duration(0, MySqlDuration::from_raw_parts(1_000_000_000, 0));
            input.append_string(1, right);
        }
        let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&duration), 2);
        suite("subtime", &fields, &duration)
            .run(&NoColumns, &mut input, &mut output)
            .unwrap();
        let result = output.column(0);
        assert_eq!(result.get_int64(0), MAX_TIME_NANOS + 1_000_000_000);
        assert!(result.is_null(1));
    }

    #[test]
    fn time_trims_go_unicode_space_before_parsing() {
        let text = FieldType::new(FieldTypeCode::VarString);
        let duration = FieldType::new(FieldTypeCode::Duration);
        let fields = [text];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
        input.append_string(0, "\u{a0}01:02:03\u{a0}");
        let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&duration), 1);
        suite("time", &fields, &duration)
            .run(&NoColumns, &mut input, &mut output)
            .unwrap();
        assert_eq!(output.column(0).get_int64(0), 3_723_000_000_000);
    }
}
