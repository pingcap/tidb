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

//! Go `sum4Decimal`, `avgOriginal4Decimal` and floating-point window states.

use super::*;

pub(super) struct NumericWindowState {
    sum: Datum,
    count: i64,
    previous: Option<(usize, usize)>,
}

impl NumericWindowState {
    pub(super) fn new(output_type: &FieldType) -> Self {
        Self {
            sum: if output_type.eval_type() == EvalType::Decimal {
                Datum::Decimal(Decimal::from_int(0))
            } else {
                Datum::Real(0.0)
            },
            count: 0,
            previous: None,
        }
    }

    #[cfg(test)]
    pub(super) fn value<C: Columns>(
        &mut self,
        func: &AggFunc,
        ctx: &C,
        chunk: &impl crate::window::FrameRows,
        start: usize,
        end: usize,
        output_type: &FieldType,
    ) -> Result<Datum, ExecError> {
        self.update_frame(func, ctx, chunk, start, end, output_type)?;
        self.finish(func, ctx, output_type)
    }

    pub(super) fn update_frame<C: Columns>(
        &mut self,
        func: &AggFunc,
        ctx: &C,
        chunk: &impl crate::window::FrameRows,
        start: usize,
        end: usize,
        output_type: &FieldType,
    ) -> Result<(), ExecError> {
        let sliding = matches!(self.sum, Datum::Decimal(_)) || !ctx.windowing_use_high_precision();
        if let Some((last_start, last_end)) = self.previous.filter(|_| sliding) {
            // Unlike COUNT, Go SUM/AVG add arrivals BEFORE subtracting departures.
            // Changing this order changes float rounding and decimal overflow.
            for row in last_end..chunk.sliding_end(last_end, end) {
                self.update(func, ctx, chunk.get_row(row), false)?;
            }
            chunk.check_sliding_end(last_end, end)?;
            for row in last_start..chunk.sliding_end(last_start, start) {
                self.update(func, ctx, chunk.get_row(row), true)?;
            }
            chunk.check_sliding_end(last_start, start)?;
        } else {
            *self = Self::new(output_type);
            for row in start..end {
                self.update(func, ctx, chunk.get_row(row), false)?;
            }
        }
        if start < end || self.previous.is_some() {
            self.previous = Some((start, end));
        }
        Ok(())
    }

    pub(super) fn finish<C: Columns>(
        &mut self,
        func: &AggFunc,
        ctx: &C,
        output_type: &FieldType,
    ) -> Result<Datum, ExecError> {
        if self.count == 0 {
            return Ok(Datum::Null);
        }
        let average = matches!(func.kind, AggKind::Avg);
        match &mut self.sum {
            Datum::Real(sum) => Ok(Datum::Real(if average {
                *sum / self.count as f64
            } else {
                *sum
            })),
            Datum::Decimal(sum) => {
                let scale = if output_type.decimal() == UNSPECIFIED_LENGTH {
                    MAX_DECIMAL_SCALE
                } else {
                    output_type.decimal()
                } as i32;
                if average {
                    let (value, warning) = sum
                        .div_mysql_with_warning(
                            &Decimal::from_int(self.count),
                            ctx.div_precision_increment(),
                        )
                        .ok_or_else(|| ExecError::Eval(tidb_expr::EvalError::DivisionByZero))?;
                    check_decimal(warning)?;
                    Ok(Datum::Decimal(value.round_to_scale(scale)))
                } else {
                    // Go rounds SUM's partial result in place before sliding it.
                    *sum = sum.round_to_scale(scale);
                    Ok(Datum::Decimal(sum.clone()))
                }
            }
            _ => unreachable!("numeric window state must be decimal or real"),
        }
    }

    fn update<C: Columns>(
        &mut self,
        func: &AggFunc,
        ctx: &C,
        row: tidb_chunk::row::Row<'_>,
        remove: bool,
    ) -> Result<(), ExecError> {
        let input = func
            .arg
            .as_ref()
            .ok_or_else(|| ExecError::internal("window SUM/AVG needs an argument"))?
            .eval(ctx, row)?;
        if input.is_null() {
            return Ok(());
        }
        match &mut self.sum {
            Datum::Real(sum) => {
                let value = real_aggregate_value(
                    &input,
                    if matches!(func.kind, AggKind::Avg) {
                        "AVG"
                    } else {
                        "SUM"
                    },
                )?;
                if remove {
                    *sum -= value;
                } else {
                    *sum += value;
                }
            }
            Datum::Decimal(sum) => {
                let value = match input {
                    Datum::Decimal(value) => value,
                    Datum::Int(value) => Decimal::from_int(value),
                    Datum::UInt(value) => Decimal::from_uint(value),
                    _ => {
                        return Err(ExecError::internal(
                            "decimal window SUM/AVG argument was not cast to decimal",
                        ));
                    }
                };
                if !remove && self.count == 0 && matches!(func.kind, AggKind::Sum) {
                    *sum = value;
                } else {
                    let (value, warning) = if remove {
                        sum.sub_mysql(&value)
                    } else {
                        sum.add_mysql(&value)
                    };
                    check_decimal(warning)?;
                    *sum = value;
                }
            }
            _ => unreachable!("numeric window state must be decimal or real"),
        }
        self.count = if remove {
            self.count.wrapping_sub(1)
        } else {
            self.count.wrapping_add(1)
        };
        Ok(())
    }
}

fn check_decimal(warning: Option<tidb_datatype::DecimalCodecWarning>) -> Result<(), ExecError> {
    use tidb_datatype::DecimalCodecWarning;
    let error = match warning {
        None => return Ok(()),
        Some(DecimalCodecWarning::Overflow) => (*tidb_datatype::ERR_OVERFLOW).clone(),
        Some(DecimalCodecWarning::Truncated) => (*tidb_datatype::ERR_TRUNCATED).clone(),
    };
    Err(ExecError::Eval(tidb_expr::EvalError::Conversion(error)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use tidb_expr::column::Column;
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::{EvalError, NoColumns};

    fn decimal_type(scale: i64) -> FieldType {
        let mut field_type = FieldType::new(FieldTypeCode::NewDecimal);
        field_type.set_decimal(scale);
        field_type
    }

    #[test]
    fn decimal_sum_avg_slide_nulls_and_empty_frames() {
        let ty = decimal_type(2);
        let values = [None, Some("1.25"), Some("-0.25"), None, None, Some("2.50")];
        let mut chunk = Chunk::new_with_capacity(&[ty.clone()], values.len());
        for value in values {
            chunk.append_datum(
                0,
                &value.map_or(Datum::Null, |v| Datum::Decimal(Decimal::from_literal(v))),
            );
        }
        let mut column = Column::new(1, ty.clone());
        column.index = 0;
        for kind in [AggKind::Sum, AggKind::Avg] {
            let func = AggFunc::new(kind.clone(), Some(Expression::Column(column.clone())));
            let mut state = NumericWindowState::new(&ty);
            let expected = if kind == AggKind::Sum {
                [
                    None,
                    Some("1.25"),
                    Some("1.00"),
                    Some("-0.25"),
                    None,
                    Some("2.50"),
                    Some("2.50"),
                    None,
                ]
            } else {
                [
                    None,
                    Some("1.25"),
                    Some("0.50"),
                    Some("-0.25"),
                    None,
                    Some("2.50"),
                    Some("2.50"),
                    None,
                ]
            };
            for (i, expected) in expected.into_iter().enumerate() {
                let actual = state
                    .value(
                        &func,
                        &NoColumns,
                        &chunk,
                        i.saturating_sub(1).min(6),
                        (i + 1).min(6),
                        &ty,
                    )
                    .unwrap();
                assert_eq!(
                    actual,
                    expected.map_or(Datum::Null, |v| Datum::Decimal(Decimal::from_literal(v))),
                    "{kind:?} frame {i}"
                );
            }
        }
    }

    #[test]
    fn decimal_sliding_evaluates_only_frame_changes() {
        struct Parameters(Cell<usize>);
        impl Columns for Parameters {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn param_value(&self, _: usize) -> Result<Datum, EvalError> {
                self.0.set(self.0.get() + 1);
                Ok(Datum::Decimal(Decimal::from_literal("1.25")))
            }
        }
        let ty = decimal_type(2);
        let mut input = Constant::new(Datum::Null, ty.clone());
        input.param_marker = Some(ParamMarker { order: 0 });
        let mut chunk = Chunk::new_with_capacity(&[ty.clone()], 100);
        for _ in 0..100 {
            chunk.append_null(0);
        }
        for kind in [AggKind::Sum, AggKind::Avg] {
            let func = AggFunc::new(kind.clone(), Some(Expression::Constant(input.clone())));
            let ctx = Parameters(Cell::new(0));
            let mut state = NumericWindowState::new(&ty);
            for i in 0_usize..100 {
                let value = state
                    .value(&func, &ctx, &chunk, i.saturating_sub(9), i + 1, &ty)
                    .unwrap();
                let expected = if kind == AggKind::Avg {
                    125
                } else {
                    125 * (i + 1).min(10)
                };
                assert_eq!(
                    value,
                    Datum::Decimal(Decimal::from_scaled_i128(expected as i128, 2))
                );
            }
            assert_eq!(ctx.0.get(), 190, "{kind:?}");
        }
    }

    #[test]
    fn decimal_sum_rounds_the_retained_partial_result() {
        let ty = decimal_type(2);
        let mut chunk = Chunk::new_with_capacity(&[ty.clone()], 2);
        for value in ["0.14", "0.16"] {
            chunk.append_datum(0, &Datum::Decimal(Decimal::from_literal(value)));
        }
        let mut column = Column::new(1, ty);
        column.index = 0;
        let func = AggFunc::new(AggKind::Sum, Some(Expression::Column(column)));
        let output = decimal_type(1);
        let mut state = NumericWindowState::new(&output);
        for i in 0..2 {
            assert_eq!(
                state
                    .value(&func, &NoColumns, &chunk, i, i + 1, &output)
                    .unwrap(),
                Datum::Decimal(Decimal::from_literal("0.1"))
            );
        }
    }

    #[test]
    fn decimal_slide_reports_arrival_overflow_before_departure() {
        let ty = decimal_type(0);
        let mut chunk = Chunk::new_with_capacity(&[ty.clone()], 2);
        // Each value fits; adding the arrival before removing the old value overflows.
        for _ in 0..2 {
            chunk.append_datum(
                0,
                &Datum::Decimal(Decimal::from_literal(&format!("5{}", "0".repeat(80)))),
            );
        }
        let mut column = Column::new(1, ty.clone());
        column.index = 0;
        // SUM can retain all nine integer words. AVG's division would already
        // report truncation while finalizing the first maximum-width frame.
        let func = AggFunc::new(AggKind::Sum, Some(Expression::Column(column)));
        let mut state = NumericWindowState::new(&ty);
        state.value(&func, &NoColumns, &chunk, 0, 1, &ty).unwrap();
        assert!(matches!(
            state.value(&func, &NoColumns, &chunk, 1, 2, &ty),
            Err(ExecError::Eval(EvalError::Conversion(_)))
        ));
    }
}
