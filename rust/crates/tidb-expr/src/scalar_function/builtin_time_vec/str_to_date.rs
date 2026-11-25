// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{types_errors, EvalType, Time, TimeType};
use tidb_proto::tipb::ScalarFuncSig;

pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let date = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let format = vec_eval::read_argument(&function.args[1], EvalType::String, ctx, input)?;
    let (date, format) = (date.cells(), format.cells());
    let sig = function.pb_code();
    let duration = sig == ScalarFuncSig::StrToDateDuration;
    let date_only = sig == ScalarFuncSig::StrToDateDate;
    let no_zero_date = !duration && ctx.date_modes().no_zero_date;
    let flags = ctx.type_flags();
    let zone = ctx.time_zone();
    let fsp = function
        .get_static_type()
        .expect("STR_TO_DATE type")
        .decimal();
    if duration {
        result.resize_go_duration(input.num_rows(), false);
    } else {
        result.resize_time(input.num_rows(), false);
    }

    result.with_fixed_cells_mut(|output, bitmap| {
        for (row, output) in output.iter_mut().enumerate() {
            if let (Some(date), Some(format)) = (date.bytes(row), format.bytes(row)) {
                match Time::str_to_date_bytes(
                    date,
                    format,
                    flags.ignore_zero_in_date_err(),
                    flags.ignore_invalid_date_err(),
                    &zone,
                ) {
                    Ok((mut time, truncated)) => {
                        if truncated {
                            warn_trailing_input(ctx, date);
                        }
                        let core = time.core_time();
                        if no_zero_date
                            && (core.year() == 0 || core.month() == 0 || core.day() == 0)
                        {
                            invalid_time(ctx, time)?;
                        } else {
                            if date_only {
                                time.set_kind(TimeType::Date);
                            } else {
                                time.set_fsp(fsp).expect("valid STR_TO_DATE precision");
                            }
                            *output = if duration {
                                time.to_duration()
                                    .expect("valid clock")
                                    .nanoseconds()
                                    .to_ne_bytes()
                            } else {
                                time.go_raw().to_ne_bytes()
                            };
                            continue;
                        }
                    }
                    Err((time, _)) => invalid_time(ctx, time)?,
                }
            }
            bitmap[row / 8] &= !(1 << (row % 8));
        }
        Ok(())
    })
}

#[cold]
fn invalid_time(ctx: &dyn Columns, time: Time) -> Result<(), EvalError> {
    ctx.handle_truncate_error(
        types_errors::ERR_WRONG_VALUE.generate(format!("Incorrect datetime value: '{time}'",)),
    )
}

#[cold]
fn warn_trailing_input(ctx: &dyn Columns, date: &[u8]) {
    let date = String::from_utf8_lossy(date);
    let end = date.char_indices().nth(128).map_or(date.len(), |(i, _)| i);
    ctx.append_go_warning(
        &types_errors::ERR_TRUNCATED_WRONG_VAL,
        &format!("Truncated incorrect datetime value: '{}'", &date[..end]),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        column::Column as ExprColumn, constant::Constant, evaluator::EvaluatorSuite,
        expression::Expression, new_function::new_function_base,
    };
    use std::cell::RefCell;
    use tidb_datatype::{CoreTime, DateModes, Datum, FieldType, FieldTypeCode};

    #[derive(Default)]
    struct Context {
        warnings: RefCell<Vec<(u16, String)>>,
    }

    impl Columns for Context {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn date_modes(&self) -> DateModes {
            DateModes {
                no_zero_date: false,
                ..DateModes::TIDB_DEFAULT_SQL_MODE
            }
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    fn run(ctx: &Context, rows: &[(&[u8], &[u8])], constant_format: bool) -> Chunk {
        let field = FieldType::new(FieldTypeCode::VarString);
        let mut input = Chunk::new_chunk_with_capacity(&[field.clone(), field.clone()], rows.len());
        for (date, format) in rows {
            input.append_bytes(0, date);
            input.append_bytes(1, format);
        }
        let column = |index: usize| {
            let mut column = ExprColumn::new(index as i64 + 1, field.clone());
            column.index = index as i64;
            Expression::Column(column)
        };
        let format = if constant_format {
            Expression::Constant(Constant::new(
                Datum::new_string_datum(rows[0].1),
                field.clone(),
            ))
        } else {
            column(1)
        };
        let expr = new_function_base(
            ctx,
            "str_to_date",
            FieldType::new(FieldTypeCode::Datetime),
            vec![column(0), format],
        )
        .unwrap();
        let mut output =
            Chunk::new_chunk_with_capacity(&[expr.static_type().unwrap().clone()], rows.len());
        EvaluatorSuite::new(vec![expr], false)
            .run(ctx, &mut input, &mut output)
            .unwrap();
        output
    }

    #[test]
    fn dynamic_formats_keep_datetime_precision_and_warn_about_trailing_input() {
        let ctx = Context::default();
        let output = run(
            &ctx,
            &[
                (b"29 February 2024 suffix", b"%d %M %Y"),
                (b"12:34:56.012", b"%T.%f"),
            ],
            false,
        );
        let column = output.column(0);
        let date = column.get_time(0);
        assert_eq!(
            date,
            Time::new(
                CoreTime::from_date(2024, 2, 29, 0, 0, 0, 0),
                TimeType::DateTime,
                6
            )
            .unwrap()
        );
        assert_eq!(
            column.get_time(1),
            Time::new(
                CoreTime::from_date(0, 0, 0, 12, 34, 56, 12_000),
                TimeType::DateTime,
                6
            )
            .unwrap()
        );
        assert_eq!(
            *ctx.warnings.borrow(),
            [(
                1292,
                "Truncated incorrect datetime value: '29 February 2024 suffix'".into()
            )]
        );
    }

    #[test]
    fn constant_format_uses_the_original_bytes_for_date_type_and_matching() {
        let ctx = Context::default();
        let output = run(&ctx, &[(b"2024\xff02\xff29", b"%Y\xff%m\xff%d")], true);
        let time = output.column(0).get_time(0);
        assert_eq!(time.kind(), TimeType::Date);
        assert_eq!(
            time.core_time(),
            CoreTime::from_date(2024, 2, 29, 0, 0, 0, 0)
        );
        assert!(ctx.warnings.borrow().is_empty());
    }
}
