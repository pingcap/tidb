// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{vec_eval, ScalarFunction};
use crate::{time_fn::time_diff, Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{types_errors, EvalType, MySqlDuration, Time, TimeType};
use tidb_proto::tipb::ScalarFuncSig;

pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::{
        AddDatetimeAndDuration, AddDurationAndDuration, DurationDurationTimeDiff,
        SubDatetimeAndDuration, SubDurationAndDuration, TimeTimeTimeDiff,
    };
    let left = &function.args[0];
    let right = &function.args[1];
    let rows = input.num_rows();
    match function.pb_code() {
        AddDurationAndDuration | SubDurationAndDuration => {
            let subtract = function.pb_code() == SubDurationAndDuration;
            vec_eval::eval_into(left, EvalType::Duration, ctx, input, result)?;
            let right = vec_eval::read_argument(right, EvalType::Duration, ctx, input)?;
            update_left(result, right, |a, b| {
                let nanos = if subtract {
                    time_diff::subtract_duration(a, b)?
                } else {
                    a.checked_add(b).ok_or_else(|| time_diff::duration_overflow(a, b))?
                };
                Ok(Some(nanos.to_ne_bytes()))
            })
        }
        DurationDurationTimeDiff => vec_eval::with_argument_pair(
            left,
            right,
            EvalType::Duration,
            ctx,
            input,
            |left, right| {
                vec_eval::write_fixed_pair(left, right, result, rows, |a, b| {
                    let nanos = time_diff::subtract_duration(a, b)?;
                    Ok(Some(
                        time_diff::truncate_duration_difference(nanos, ctx)?.to_ne_bytes(),
                    ))
                })
            },
        ),
        TimeTimeTimeDiff => vec_eval::with_argument_pair(
            left,
            right,
            EvalType::Datetime,
            ctx,
            input,
            |left, right| {
                vec_eval::write_fixed_pair(left, right, result, rows, |a, b| {
                    let a = Time::from_go_raw_like_go(a as u64);
                    let b = Time::from_go_raw_like_go(b as u64);
                    let nanos = if a.kind() == TimeType::Timestamp
                        && b.kind() == TimeType::Timestamp
                    {
                        a.sub(b, &ctx.time_zone())
                            .map_err(|_| {
                                EvalError::Unsupported("TIMEDIFF over an unconvertible TIMESTAMP")
                            })?
                            .nanoseconds()
                    } else {
                        (a.core_time().calendar_micros() - b.core_time().calendar_micros())
                            .wrapping_mul(1_000)
                    };
                    Ok(Some(
                        time_diff::truncate_duration_difference(nanos, ctx)?.to_ne_bytes(),
                    ))
                })
            },
        ),
        AddDatetimeAndDuration | SubDatetimeAndDuration => {
            let subtract = function.pb_code() == SubDatetimeAndDuration;
            vec_eval::eval_into(left, EvalType::Datetime, ctx, input, result)?;
            let right = vec_eval::read_argument(right, EvalType::Duration, ctx, input)?;
            update_left(result, right, |raw, nanos| {
                let time = Time::from_go_raw_like_go(raw as u64);
                if time.is_zero() {
                    return Ok(None);
                }
                let nanos = if subtract {
                    nanos.wrapping_neg()
                } else {
                    nanos
                };
                add_to_datetime(time, MySqlDuration::from_raw_parts(nanos, -1), ctx).map(Some)
            })
        }
        _ => unreachable!("typed temporal arithmetic signature"),
    }
}

fn update_left(
    result: &mut Column,
    right: vec_eval::ReadArgument<'_>,
    map: impl Fn(i64, i64) -> Result<Option<[u8; 8]>, EvalError>,
) -> Result<(), EvalError> {
    let right = right.cells();
    let right = right.fixed::<8>();
    result.with_fixed_cells_mut(|output, bitmap| {
        for (row, left) in output.iter_mut().enumerate() {
            let bit = 1 << (row & 7);
            let presence = &mut bitmap[row >> 3];
            if *presence & bit == 0 {
                continue;
            }
            if let Some(&right) = right(row) {
                if let Some(value) = map(i64::from_ne_bytes(*left), i64::from_ne_bytes(right))? {
                    *left = value;
                    continue;
                }
            }
            *presence &= !bit;
        }
        Ok(())
    })
}

pub(super) fn add_to_datetime(
    time: Time,
    duration: MySqlDuration,
    ctx: &dyn Columns,
) -> Result<[u8; 8], EvalError> {
    shift_datetime(time, duration, ctx).map(|time| time.go_raw().to_ne_bytes())
}

pub(super) fn shift_datetime(
    time: Time,
    duration: MySqlDuration,
    ctx: &dyn Columns,
) -> Result<Time, EvalError> {
    let time = time
        .add_duration(duration)
        .expect("packed datetime with valid FSP");
    // Calendar arithmetic produced normalized fields. Go rejects
    // results past year 9999 and also validates TIMESTAMP instants.
    if time.core_time().year() > 9999 {
        return Err(wrong_time(time));
    }
    if time.kind() == TimeType::Timestamp && time.validate(false, false, &ctx.time_zone()).is_err()
    {
        return Err(wrong_time(time));
    }
    Ok(time)
}

#[cold]
fn wrong_time(time: Time) -> EvalError {
    EvalError::Conversion(
        types_errors::ERR_WRONG_VALUE
            .generate(format!("Incorrect time value: '{}'", time.core_time(),)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        column::Column as ExpressionColumn,
        evaluator::{EvaluatorError, EvaluatorSuite},
        expression::Expression,
        new_function::new_function_base,
        EvalErrorSql, NoColumns,
    };
    use tidb_datatype::{CoreTime, FieldType, FieldTypeCode, MAX_TIME_NANOS};

    fn field(code: FieldTypeCode, fsp: i64) -> FieldType {
        let mut field = FieldType::new(code);
        field.set_decimal(fsp);
        field
    }

    fn suite(name: &str, types: &[FieldType; 2], result: &FieldType) -> EvaluatorSuite {
        let args = types
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let mut arg = ExpressionColumn::new(index as i64 + 1, field.clone());
                arg.index = index as i64;
                Expression::Column(arg)
            })
            .collect();
        let expr = new_function_base(&NoColumns, name, result.clone(), args).unwrap();
        EvaluatorSuite::new(vec![expr], false)
    }

    #[test]
    fn addtime_preserves_packed_duration_outside_sql_time_range() {
        let duration = field(FieldTypeCode::Duration, 6);
        let fields = [duration.clone(), duration.clone()];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 2);
        input.append_duration(0, MySqlDuration::from_raw_parts(MAX_TIME_NANOS, 6));
        input.append_duration(1, MySqlDuration::from_raw_parts(MAX_TIME_NANOS - 1, 6));
        input.append_null(0);
        input.append_duration(1, MySqlDuration::from_raw_parts(0, 6));
        input.set_sel(Some(vec![1, 0, 0]));
        let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&duration), 3);
        suite("addtime", &fields, &duration)
            .run(&NoColumns, &mut input, &mut output)
            .unwrap();
        let result = output.column(0);
        assert!(result.is_null(0));
        assert_eq!(result.get_int64(1), 2 * MAX_TIME_NANOS - 1);
        assert_eq!(result.get_int64(2), 2 * MAX_TIME_NANOS - 1);
    }

    #[test]
    fn addtime_keeps_the_datetime_fsp_and_the_full_packed_fraction() {
        let datetime = field(FieldTypeCode::Datetime, 2);
        let fields = [datetime.clone(), field(FieldTypeCode::Duration, 6)];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
        let t = Time::new(
            CoreTime::from_date(2024, 2, 29, 23, 59, 59, 990_000),
            TimeType::DateTime,
            2,
        )
        .unwrap();
        input.append_time(0, t);
        input.append_duration(1, MySqlDuration::from_raw_parts(1_234_567_000, 6));
        let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&datetime), 1);
        suite("addtime", &fields, &datetime)
            .run(&NoColumns, &mut input, &mut output)
            .unwrap();
        let actual = output.column(0).get_time(0);
        assert_eq!(actual.fsp(), 2);
        assert_eq!(
            actual.core_time(),
            CoreTime::from_date(2024, 3, 1, 0, 0, 1, 224_567)
        );
    }

    #[test]
    fn timediff_duration_reports_go_bigint_overflow() {
        let duration = field(FieldTypeCode::Duration, 6);
        let fields = [duration.clone(), duration.clone()];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
        input.append_duration(0, MySqlDuration::from_raw_parts(i64::MAX, 6));
        input.append_duration(1, MySqlDuration::from_raw_parts(-1, 6));
        let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&duration), 1);
        let err = suite("timediff", &fields, &duration)
            .run(&NoColumns, &mut input, &mut output)
            .unwrap_err();
        let EvaluatorError::Eval(err) = err else {
            panic!("expected SQL evaluation error, got {err:?}")
        };
        let err = err.to_sql_error();
        assert_eq!(err.code, 1690);
        assert_eq!(
            err.message,
            "BIGINT value is out of range in '(9223372036854775807, -1)'"
        );
    }
}
