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

//! Typed inputs selected as Go's `aggfuncs/builder.go` selects aggregate
//! implementations. Bind column addresses once per chunk, then update in
//! row/function order as `HashAggPartialWorker.updatePartialResult` does.

use super::*;
use std::sync::Arc;
use tidb_chunk::ColumnRead;
use tidb_codec::JoinKeyColumn;
use tidb_datatype::MYDECIMAL_STRUCT_SIZE;

type DecimalBatch = Arc<[Option<(i128, u32)>]>;
type DecimalCache = smallvec::SmallVec<[Option<DecimalBatch>; 4]>;
type IntegerBatch = Arc<[Option<i64>]>;
type IntegerCache = smallvec::SmallVec<[Option<IntegerBatch>; 4]>;
type RealBatch = Arc<[Option<f64>]>;
type RealCache = smallvec::SmallVec<[Option<RealBatch>; 4]>;

#[derive(Clone, Copy)]
pub(super) enum IntegerAggOp {
    Sum,
    MinMax { is_max: bool },
    Avg,
}

#[derive(Clone, Copy)]
pub(super) enum AggInputMode<T = usize> {
    Expression,
    FirstRow,
    CountAll,
    Count(T),
    CountDistinctInt(T),
    FinalCount {
        column: T,
        unsigned: bool,
    },
    Integer {
        column: T,
        unsigned: bool,
        op: IntegerAggOp,
    },
    Real {
        column: T,
        float32: bool,
        op: IntegerAggOp,
    },
    Decimal(T),
    AvgDecimal {
        sum: T,
        count: Option<(T, bool)>,
    },
}

impl AggInputMode {
    pub(super) fn new(func: &AggFunc) -> Self {
        Self::typed(func).unwrap_or(Self::Expression)
    }

    fn typed(func: &AggFunc) -> Option<Self> {
        if matches!(func.kind, AggKind::FirstRow) {
            return Some(Self::FirstRow);
        }
        let column = |expr: &Expression| {
            let col = expr.as_column()?;
            usize::try_from(col.index).ok()
        };
        let integer = |expr: &Expression| {
            let index = column(expr)?;
            let ty = expr.static_type()?;
            matches!(
                ty.code(),
                FieldTypeCode::Tiny
                    | FieldTypeCode::Short
                    | FieldTypeCode::Int24
                    | FieldTypeCode::Long
                    | FieldTypeCode::LongLong
            )
            .then_some((index, ty.is_unsigned()))
        };
        let decimal = |expr: &Expression| {
            let index = column(expr)?;
            (expr.static_type()?.code() == FieldTypeCode::NewDecimal).then_some(index)
        };
        let real = |expr: &Expression| {
            let index = column(expr)?;
            let field_type = expr.static_type()?;
            (field_type.eval_type() == EvalType::Real)
                .then_some((index, field_type.code() == FieldTypeCode::Float))
        };
        if count_distinct_int(func) {
            return Some(Self::CountDistinctInt(column(func.arg.as_ref()?)?));
        }
        if func.distinct || !func.order_by.is_empty() {
            return None;
        }
        match (&func.kind, func.extra_args.as_slice()) {
            (AggKind::Count, []) => Some(match &func.arg {
                None => Self::CountAll,
                Some(expr) => Self::Count(column(expr)?),
            }),
            (AggKind::FinalCount, []) => {
                let (column, unsigned) = integer(func.arg.as_ref()?)?;
                Some(Self::FinalCount { column, unsigned })
            }
            (AggKind::Sum, []) => {
                if let Some((index, unsigned)) = integer(func.arg.as_ref()?) {
                    Some(Self::Integer {
                        column: index,
                        unsigned,
                        op: IntegerAggOp::Sum,
                    })
                } else if let Some((index, float32)) = real(func.arg.as_ref()?) {
                    Some(Self::Real {
                        column: index,
                        float32,
                        op: IntegerAggOp::Sum,
                    })
                } else {
                    Some(Self::Decimal(decimal(func.arg.as_ref()?)?))
                }
            }
            (AggKind::Min | AggKind::Max, []) => {
                if let Some((index, unsigned)) = integer(func.arg.as_ref()?) {
                    Some(Self::Integer {
                        column: index,
                        unsigned,
                        op: IntegerAggOp::MinMax {
                            is_max: matches!(func.kind, AggKind::Max),
                        },
                    })
                } else if let Some((index, float32)) = real(func.arg.as_ref()?) {
                    Some(Self::Real {
                        column: index,
                        float32,
                        op: IntegerAggOp::MinMax {
                            is_max: matches!(func.kind, AggKind::Max),
                        },
                    })
                } else {
                    Some(Self::Decimal(decimal(func.arg.as_ref()?)?))
                }
            }
            (AggKind::Avg, []) => {
                if let Some((index, unsigned)) = integer(func.arg.as_ref()?) {
                    Some(Self::Integer {
                        column: index,
                        unsigned,
                        op: IntegerAggOp::Avg,
                    })
                } else if let Some((index, float32)) = real(func.arg.as_ref()?) {
                    Some(Self::Real {
                        column: index,
                        float32,
                        op: IntegerAggOp::Avg,
                    })
                } else {
                    Some(Self::AvgDecimal {
                        sum: decimal(func.arg.as_ref()?)?,
                        count: None,
                    })
                }
            }
            (AggKind::Avg, [sum]) => Some(Self::AvgDecimal {
                sum: decimal(sum)?,
                count: Some(integer(func.arg.as_ref()?)?),
            }),
            _ => None,
        }
    }

    pub(super) fn bind<'a>(&self, chunk: &'a Chunk) -> AggInputMode<ColumnRead<'a>> {
        // Type/offset checks belong to the plan, and both slot indexing and
        // the immutable column read view belong to this chunk boundary.
        // Holding the view matches Go's direct column pointer and avoids
        // reacquiring a shared read handle for every row.
        match *self {
            Self::Expression => AggInputMode::Expression,
            Self::FirstRow => AggInputMode::FirstRow,
            Self::CountAll => AggInputMode::CountAll,
            Self::Count(index) => AggInputMode::Count(chunk.column(index)),
            Self::CountDistinctInt(index) => AggInputMode::CountDistinctInt(chunk.column(index)),
            Self::FinalCount { column, unsigned } => AggInputMode::FinalCount {
                column: chunk.column(column),
                unsigned,
            },
            Self::Integer {
                column,
                unsigned,
                op,
            } => AggInputMode::Integer {
                column: chunk.column(column),
                unsigned,
                op,
            },
            Self::Real {
                column,
                float32,
                op,
            } => AggInputMode::Real {
                column: chunk.column(column),
                float32,
                op,
            },
            Self::Decimal(index) => AggInputMode::Decimal(chunk.column(index)),
            Self::AvgDecimal { sum, count } => AggInputMode::AvgDecimal {
                sum: chunk.column(sum),
                count: count.map(|(index, unsigned)| (chunk.column(index), unsigned)),
            },
        }
    }
}

impl AggInputMode<ColumnRead<'_>> {
    /// `None` leaves the exact expression path in charge: non-column input,
    /// decimal scale/width changes, or a count outside the scalar domain.
    fn update_cell(
        &self,
        state: &mut AggState,
        row: usize,
        decimal_data: Option<&[Option<(i128, u32)>]>,
        integer_data: Option<&[Option<i64>]>,
        real_data: Option<&[Option<f64>]>,
    ) -> Option<i64> {
        match self {
            Self::Expression => None,
            Self::FirstRow => state.has_first_row().then_some(0),
            Self::CountAll => state.update_count_fast(true).then_some(0),
            Self::Count(column) => state.update_count_fast(!column.is_null(row)).then_some(0),
            Self::CountDistinctInt(column) => {
                let value = integer_data
                    .map(|values| values[row])
                    .unwrap_or_else(|| (!column.is_null(row)).then(|| column.get_int64(row)));
                state.update_count_distinct_int_fast(value)
            }
            Self::FinalCount { column, unsigned } => {
                let value = integer_data
                    .map(|values| read_count_value(values[row], *unsigned))
                    .unwrap_or_else(|| read_count(column, row, *unsigned));
                match value? {
                    None => Some(0),
                    Some(value) => state.update_final_count_fast(value).then_some(0),
                }
            }
            Self::Integer {
                column,
                unsigned,
                op,
            } => {
                let value = integer_data
                    .map(|values| values[row])
                    .unwrap_or_else(|| (!column.is_null(row)).then(|| column.get_int64(row)));
                let Some(value) = value else {
                    return Some(0);
                };
                match op {
                    IntegerAggOp::Sum => state
                        .partial_update_with_coefficient(integer_coefficient(value, *unsigned), 0)
                        .then_some(0),
                    IntegerAggOp::Avg => state
                        .update_avg_decimal_fast(integer_coefficient(value, *unsigned), 0, 1)
                        .then_some(0),
                    IntegerAggOp::MinMax { is_max } => state
                        .update_integer_fast(value, *unsigned, *is_max)
                        .then_some(0),
                }
            }
            Self::Real {
                column,
                float32,
                op,
            } => {
                let value = real_data.map(|values| values[row]).unwrap_or_else(|| {
                    (!column.is_null(row)).then(|| {
                        if *float32 {
                            f64::from(column.get_float32(row))
                        } else {
                            column.get_float64(row)
                        }
                    })
                });
                let Some(value) = value else {
                    return Some(0);
                };
                match op {
                    IntegerAggOp::Sum => state.update_sum_real_fast(value).then_some(0),
                    IntegerAggOp::Avg => state.update_avg_real_fast(value).then_some(0),
                    IntegerAggOp::MinMax { is_max } => state
                        .update_real_fast(value, *float32, *is_max)
                        .then_some(0),
                }
            }
            Self::Decimal(column) => {
                if column.is_null(row) {
                    return Some(0);
                }
                let (coefficient, scale) = match decimal_data {
                    Some(values) => values.get(row).copied().flatten()?,
                    None => column.get_my_decimal_i128_scaled(row)?,
                };
                state
                    .partial_update_with_coefficient(coefficient, scale)
                    .then_some(0)
            }
            Self::AvgDecimal { sum, count } => {
                // Go avgPartial4Decimal evaluates sum before count. Both
                // immutable views were bound at the chunk boundary, so an
                // aliased sum/count column remains safe to read in that order.
                if sum.is_null(row) {
                    return Some(0);
                }
                let coefficient = match decimal_data {
                    Some(values) => values.get(row).copied().flatten()?,
                    None => sum.get_my_decimal_i128_scaled(row)?,
                };
                let count = match count {
                    None => 1,
                    Some((column, unsigned)) => {
                        let value = integer_data
                            .map(|values| read_count_value(values[row], *unsigned))
                            .unwrap_or_else(|| read_count(column, row, *unsigned));
                        match value? {
                            None => return Some(0),
                            Some(count) if count >= 0 => count,
                            Some(_) => return None,
                        }
                    }
                };
                if state.update_avg_decimal_fast(coefficient.0, coefficient.1, count) {
                    Some(0)
                } else {
                    state.partial.materialize_avg_fast();
                    None
                }
            }
        }
    }

    pub(super) fn update<C: Columns>(
        &self,
        func: &AggFunc,
        ctx: &C,
        state: &mut AggState,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<i64, ExecError> {
        self.update_with_decimal_data(func, ctx, state, row, None, None, None)
    }

    fn update_with_decimal_data<C: Columns>(
        &self,
        func: &AggFunc,
        ctx: &C,
        state: &mut AggState,
        row: tidb_chunk::row::Row<'_>,
        decimal_data: Option<&[Option<(i128, u32)>]>,
        integer_data: Option<&[Option<i64>]>,
        real_data: Option<&[Option<f64>]>,
    ) -> Result<i64, ExecError> {
        if let Some(delta) =
            self.update_cell(state, row.idx(), decimal_data, integer_data, real_data)
        {
            return Ok(delta);
        }
        let mut extra_values = Vec::new();
        let input = eval_agg_input(func, ctx, row, &mut extra_values)?;
        let mut sort_key = Vec::with_capacity(func.order_by.len());
        for (expr, _) in &func.order_by {
            sort_key.push(expr.eval(ctx, row)?);
        }
        state.update(input.value, &extra_values, sort_key, input.distinct_key)
    }
}

fn read_count(column: &ColumnRead<'_>, row: usize, unsigned: bool) -> Option<Option<i64>> {
    if column.is_null(row) {
        return Some(None);
    }
    read_count_value(Some(column.get_int64(row)), unsigned)
}

fn read_count_value(value: Option<i64>, unsigned: bool) -> Option<Option<i64>> {
    let value = value?;
    if unsigned {
        i64::try_from(value as u64).ok().map(Some)
    } else {
        Some(Some(value))
    }
}

pub(super) fn bind_inputs<'a>(
    modes: &[AggInputMode],
    chunk: &'a Chunk,
) -> smallvec::SmallVec<[AggInputMode<ColumnRead<'a>>; 4]> {
    modes.iter().map(|mode| mode.bind(chunk)).collect()
}

/// Decode each DECIMAL input once for the complete physical chunk. Go binds a
/// typed column once before `UpdatePartialResult` receives its row slice; the
/// old Rust row accessor reacquired the backing byte view for every row. The
/// cache is aligned with `modes`, and repeated DECIMAL columns share one
/// allocation so `SUM(x), MIN(x), MAX(x)` does not decode `x` three times.
pub(super) fn prepare_decimal_cache(modes: &[AggInputMode], chunk: &Chunk) -> DecimalCache {
    let mut batches: smallvec::SmallVec<[(usize, DecimalBatch); 4]> = smallvec::SmallVec::new();
    modes
        .iter()
        .map(|mode| {
            let index = match mode {
                AggInputMode::Decimal(index) => Some(*index),
                AggInputMode::AvgDecimal { sum, .. } => Some(*sum),
                _ => None,
            }?;
            if let Some((_, batch)) = batches.iter().find(|(cached, _)| *cached == index) {
                return Some(Arc::clone(batch));
            }
            let column = chunk.column(index);
            let rows = column.rows();
            let mut values = Vec::with_capacity(rows);
            column.with_my_decimal_data(|data| {
                for row in 0..rows {
                    if column.is_null(row) {
                        values.push(None);
                        continue;
                    }
                    let start = row * MYDECIMAL_STRUCT_SIZE;
                    values.push(
                        data.get(start..start + MYDECIMAL_STRUCT_SIZE)
                            .and_then(tidb_datatype::MyDecimal::i128_scaled_from_raw_bytes),
                    );
                }
            });
            let batch: DecimalBatch = values.into();
            batches.push((index, Arc::clone(&batch)));
            Some(batch)
        })
        .collect()
}

/// Decode the fixed-width integer inputs used by the typed aggregate modes
/// once per physical chunk. Go's `Int64s`/`Uint64s` accessors return a view of
/// the whole column; retaining the values here avoids reopening the Rust
/// backing lock for every row while preserving the same signed storage bits
/// for unsigned SQL columns.
pub(super) fn prepare_integer_cache(modes: &[AggInputMode], chunk: &Chunk) -> IntegerCache {
    let mut batches: smallvec::SmallVec<[(usize, IntegerBatch); 4]> = smallvec::SmallVec::new();
    modes
        .iter()
        .map(|mode| {
            let index = match mode {
                AggInputMode::CountDistinctInt(index) => Some(*index),
                AggInputMode::FinalCount { column, .. } => Some(*column),
                AggInputMode::Integer { column, .. } => Some(*column),
                AggInputMode::AvgDecimal {
                    count: Some((index, _)),
                    ..
                } => Some(*index),
                _ => None,
            }?;
            if let Some((_, batch)) = batches.iter().find(|(cached, _)| *cached == index) {
                return Some(Arc::clone(batch));
            }
            let column = chunk.column(index);
            let rows = column.rows();
            let mut values = Vec::with_capacity(rows);
            column.with_raw(|raw| {
                for row in 0..rows {
                    if column.is_null(row) {
                        values.push(None);
                    } else {
                        values.push(Some(i64::from_ne_bytes(
                            raw.row(row)
                                .try_into()
                                .expect("integer aggregate cell is 8 bytes"),
                        )));
                    }
                }
            });
            let batch: IntegerBatch = values.into();
            batches.push((index, Arc::clone(&batch)));
            Some(batch)
        })
        .collect()
}

/// Decode direct REAL inputs once per physical chunk. Go's `Float32s`/
/// `Float64s` accessors expose the typed column to `sum4Float64`,
/// `avgOriginal4Float64`, and `maxMin4Float*`; keeping the values here avoids
/// reopening the Rust backing byte view for every row and aggregate.
pub(super) fn prepare_real_cache(modes: &[AggInputMode], chunk: &Chunk) -> RealCache {
    let mut batches: smallvec::SmallVec<[(usize, RealBatch); 4]> = smallvec::SmallVec::new();
    modes
        .iter()
        .map(|mode| {
            let index = match mode {
                AggInputMode::Real { column, .. } => Some(*column),
                _ => None,
            }?;
            if let Some((_, batch)) = batches.iter().find(|(cached, _)| *cached == index) {
                return Some(Arc::clone(batch));
            }
            let column = chunk.column(index);
            let rows = column.rows();
            let mut values = Vec::with_capacity(rows);
            column.with_raw(|raw| {
                for row in 0..rows {
                    if column.is_null(row) {
                        values.push(None);
                    } else {
                        let cell = raw.row(row);
                        let value = match cell.len() {
                            4 => f64::from(f32::from_ne_bytes(
                                cell.try_into().expect("Float cell is 4 bytes"),
                            )),
                            8 => {
                                f64::from_ne_bytes(cell.try_into().expect("Double cell is 8 bytes"))
                            }
                            _ => panic!("real aggregate cell is 4 or 8 bytes"),
                        };
                        values.push(Some(value));
                    }
                }
            });
            let batch: RealBatch = values.into();
            batches.push((index, Arc::clone(&batch)));
            Some(batch)
        })
        .collect()
}

fn integer_coefficient(value: i64, unsigned: bool) -> i128 {
    if unsigned {
        i128::from(value as u64)
    } else {
        i128::from(value)
    }
}

pub(super) fn update_row<C: Columns>(
    modes: &[AggInputMode<ColumnRead<'_>>],
    funcs: &[AggFunc],
    ctx: &C,
    states: &mut [AggState],
    row: tidb_chunk::row::Row<'_>,
) -> Result<i64, ExecError> {
    let mut delta = 0;
    for ((mode, func), state) in modes.iter().zip(funcs).zip(states) {
        delta += mode.update(func, ctx, state, row)?;
    }
    Ok(delta)
}

pub(super) fn update_row_with_decimal_cache<C: Columns>(
    modes: &[AggInputMode<ColumnRead<'_>>],
    funcs: &[AggFunc],
    ctx: &C,
    states: &mut [AggState],
    row: tidb_chunk::row::Row<'_>,
    decimal_cache: &DecimalCache,
    integer_cache: &IntegerCache,
    real_cache: &RealCache,
) -> Result<i64, ExecError> {
    let mut delta = 0;
    for (mode_index, ((mode, func), state)) in modes.iter().zip(funcs).zip(states).enumerate() {
        let decimal_data = decimal_cache.get(mode_index).and_then(Option::as_deref);
        let integer_data = integer_cache.get(mode_index).and_then(Option::as_deref);
        let real_data = real_cache.get(mode_index).and_then(Option::as_deref);
        delta += mode.update_with_decimal_data(
            func,
            ctx,
            state,
            row,
            decimal_data,
            integer_data,
            real_data,
        )?;
    }
    Ok(delta)
}
