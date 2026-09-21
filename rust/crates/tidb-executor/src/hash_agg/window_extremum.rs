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

//! Go's typed `maxMin*Sliding` partial results and monotonic deque.

use super::*;
use std::collections::VecDeque;

/// Go buildMaxMinInWindowFunction leaves ENUM, SET, JSON and vector
/// aggregates on the non-sliding implementations.
pub(super) fn supports(output_type: &FieldType) -> bool {
    !matches!(output_type.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
        && !matches!(
            output_type.eval_type(),
            EvalType::Json | EvalType::VectorFloat32
        )
}

#[derive(Default)]
pub(super) struct ExtremumWindowState {
    // VecDeque gives Go's slice-front removal cost without moving retained rows.
    items: VecDeque<(usize, Datum)>,
    previous: Option<(usize, usize)>,
}

impl ExtremumWindowState {
    pub(super) fn update_frame<C: Columns>(
        &mut self,
        func: &AggFunc,
        ctx: &C,
        chunk: &impl crate::window::FrameRows,
        start: usize,
        end: usize,
        output_type: &FieldType,
    ) -> Result<(), ExecError> {
        let first_new = if let Some((_, last_end)) = self.previous {
            last_end
        } else {
            self.items.clear();
            start
        };
        let arg = func
            .arg
            .as_ref()
            .ok_or_else(|| ExecError::internal("window MIN/MAX needs an argument"))?;
        let collation = tidb_expr::collation_derive::collation_of_node(arg);
        for index in first_new..if self.previous.is_some() {
            chunk.sliding_end(first_new, end)
        } else {
            end
        } {
            let mut value = arg.eval(ctx, chunk.get_row(index))?;
            if value.is_null() {
                continue;
            }
            if output_type.code() == FieldTypeCode::Float {
                value = Datum::Float32(real_aggregate_value(&value, "MIN/MAX")? as f32 as f64);
            } else if output_type.code() == FieldTypeCode::Bit
                || output_type.eval_type() == EvalType::String
            {
                value = Datum::Bytes(value.sql_bytes().map_err(|_| {
                    ExecError::internal("window string extremum cannot be read as bytes")
                })?);
            } else if output_type.eval_type() == EvalType::Int {
                value = match (value, output_type.is_unsigned()) {
                    (Datum::Int(v), true) => Datum::UInt(v as u64),
                    (Datum::UInt(v), false) => Datum::Int(v as i64),
                    (v, _) => v,
                };
            }
            while let Some((_, back)) = self.items.back() {
                let order = match (&value, back) {
                    // cmp.Compare orders NaNs before all non-NaNs, and treats
                    // two NaNs and signed zero pairs as equal.
                    (Datum::Real(a) | Datum::Float32(a), Datum::Real(b) | Datum::Float32(b)) => {
                        if a < b || (a.is_nan() && !b.is_nan()) {
                            Ordering::Less
                        } else if a > b || (!a.is_nan() && b.is_nan()) {
                            Ordering::Greater
                        } else {
                            Ordering::Equal
                        }
                    }
                    _ => tidb_expr::compare_datums_with_collation(&value, back, collation)?,
                };
                let remove = if matches!(func.kind, AggKind::Max) {
                    order != Ordering::Less
                } else {
                    order != Ordering::Greater
                };
                if !remove {
                    break;
                }
                self.items.pop_back();
            }
            self.items.push_back((index, value));
        }
        if self.previous.is_some() {
            chunk.check_sliding_end(first_new, end)?;
        }
        // Go adds arriving rows before expiring rows before the new start.
        while self.items.front().is_some_and(|(index, _)| *index < start) {
            self.items.pop_front();
        }
        if start < end || self.previous.is_some() {
            self.previous = Some((start, end));
        }
        Ok(())
    }

    pub(super) fn finish(&self, output_type: &FieldType) -> Result<Datum, ExecError> {
        let value = self
            .items
            .front()
            .map_or(Datum::Null, |(_, value)| value.clone());
        if let Datum::Decimal(value) = value {
            let scale = if output_type.decimal() == UNSPECIFIED_LENGTH {
                MAX_DECIMAL_SCALE
            } else {
                output_type.decimal()
            };
            // Go copies the front into p.val before rounding; the deque keeps
            // the original input for subsequent comparisons.
            Ok(Datum::Decimal(value.round_to_scale(scale as i32)))
        } else {
            Ok(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use tidb_expr::column::Column;
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::{EvalError, NoColumns};

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    #[test]
    fn sliding_extrema_and_xor_evaluate_only_changed_rows() {
        struct Parameters(Cell<usize>);
        impl Columns for Parameters {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn param_value(&self, _: usize) -> Result<Datum, EvalError> {
                self.0.set(self.0.get() + 1);
                Ok(Datum::Int(1))
            }
        }
        let mut parameter = Constant::new(Datum::Null, long());
        parameter.param_marker = Some(ParamMarker { order: 0 });
        let mut chunk = Chunk::new_with_capacity(&[long()], 100);
        for _ in 0..100 {
            chunk.append_null(0);
        }
        for kind in [AggKind::Min, AggKind::Max, AggKind::Bit(BitOp::Xor)] {
            let ctx = Parameters(Cell::new(0));
            let func = AggFunc::new(kind.clone(), Some(Expression::Constant(parameter.clone())));
            let mut state = WindowAggState::new(&func);
            for index in 0_usize..100 {
                let value = state
                    .value(
                        &func,
                        &ctx,
                        &chunk,
                        index.saturating_sub(9),
                        index + 1,
                        &long(),
                    )
                    .unwrap();
                assert_eq!(
                    value,
                    if matches!(kind, AggKind::Bit(_)) {
                        Datum::UInt((index + 1).min(10) as u64 % 2)
                    } else {
                        Datum::Int(1)
                    }
                );
            }
            assert_eq!(
                ctx.0.get(),
                if matches!(kind, AggKind::Bit(_)) {
                    190
                } else {
                    100
                }
            );
        }
    }

    #[test]
    fn extrema_expire_nulls_disjoint_and_empty_frames() {
        let mut chunk = Chunk::new_with_capacity(&[long()], 6);
        for value in [Some(3), None, Some(2), Some(5), None, Some(1)] {
            chunk.append_datum(0, &value.map_or(Datum::Null, Datum::Int));
        }
        let mut column = Column::new(1, long());
        column.index = 0;
        for (kind, expected) in [
            (AggKind::Min, [3, 3, 2, 2, 5, 1]),
            (AggKind::Max, [3, 3, 2, 5, 5, 1]),
        ] {
            let func = AggFunc::new(kind, Some(Expression::Column(column.clone())));
            let mut state = WindowAggState::new(&func);
            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(
                    state
                        .value(
                            &func,
                            &NoColumns,
                            &chunk,
                            index.saturating_sub(1),
                            index + 1,
                            &long()
                        )
                        .unwrap(),
                    Datum::Int(expected)
                );
            }
            assert_eq!(
                state
                    .value(&func, &NoColumns, &chunk, 6, 6, &long())
                    .unwrap(),
                Datum::Null
            );
            state.reset(&func);
            assert_eq!(
                state
                    .value(&func, &NoColumns, &chunk, 0, 1, &long())
                    .unwrap(),
                Datum::Int(3)
            );
            assert_eq!(
                state
                    .value(&func, &NoColumns, &chunk, 3, 4, &long())
                    .unwrap(),
                Datum::Int(5)
            );
        }
    }

    #[test]
    fn extrema_use_go_nan_and_signed_zero_order() {
        let ty = FieldType::new(FieldTypeCode::Double);
        let mut chunk = Chunk::new_with_capacity(&[ty.clone()], 4);
        for value in [f64::NAN, 1.0, 0.0, -0.0] {
            chunk.append_datum(0, &Datum::Real(value));
        }
        let mut column = Column::new(1, ty.clone());
        column.index = 0;
        for kind in [AggKind::Min, AggKind::Max] {
            let func = AggFunc::new(kind.clone(), Some(Expression::Column(column.clone())));
            let mut state = WindowAggState::new(&func);
            let Datum::Real(value) = state.value(&func, &NoColumns, &chunk, 0, 2, &ty).unwrap()
            else {
                panic!("real result");
            };
            if kind == AggKind::Min {
                assert!(value.is_nan());
            } else {
                assert_eq!(value, 1.0);
            }
            let Datum::Real(value) = state.value(&func, &NoColumns, &chunk, 2, 4, &ty).unwrap()
            else {
                panic!("real result");
            };
            assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
        }
    }
}
