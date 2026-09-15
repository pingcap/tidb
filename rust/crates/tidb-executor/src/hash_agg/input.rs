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
use tidb_chunk::{ColumnRead, ColumnRef};

#[derive(Clone, Copy)]
pub(super) enum AggInputMode<T = usize> {
    Expression,
    FirstRow,
    CountAll,
    Count(T),
    CountDistinctInt(T),
    FinalCount { column: T, unsigned: bool },
    Decimal(T),
    AvgDecimal { sum: T, count: Option<(T, bool)> },
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
            (AggKind::Sum | AggKind::Min | AggKind::Max, []) => {
                Some(Self::Decimal(decimal(func.arg.as_ref()?)?))
            }
            (AggKind::Avg, []) => Some(Self::AvgDecimal {
                sum: decimal(func.arg.as_ref()?)?,
                count: None,
            }),
            (AggKind::Avg, [sum]) => Some(Self::AvgDecimal {
                sum: decimal(sum)?,
                count: Some(integer(func.arg.as_ref()?)?),
            }),
            _ => None,
        }
    }

    pub(super) fn bind<'a>(&self, chunk: &'a Chunk) -> AggInputMode<ColumnRef<'a>> {
        // Type/offset checks belong to the plan, and slot indexing belongs to
        // this chunk boundary. Neither repeats for each row or group.
        match *self {
            Self::Expression => AggInputMode::Expression,
            Self::FirstRow => AggInputMode::FirstRow,
            Self::CountAll => AggInputMode::CountAll,
            Self::Count(index) => AggInputMode::Count(chunk.column_ref(index)),
            Self::CountDistinctInt(index) => {
                AggInputMode::CountDistinctInt(chunk.column_ref(index))
            }
            Self::FinalCount { column, unsigned } => AggInputMode::FinalCount {
                column: chunk.column_ref(column),
                unsigned,
            },
            Self::Decimal(index) => AggInputMode::Decimal(chunk.column_ref(index)),
            Self::AvgDecimal { sum, count } => AggInputMode::AvgDecimal {
                sum: chunk.column_ref(sum),
                count: count.map(|(index, unsigned)| (chunk.column_ref(index), unsigned)),
            },
        }
    }
}

impl AggInputMode<ColumnRef<'_>> {
    /// `None` leaves the exact expression path in charge: non-column input,
    /// decimal scale/width changes, or a count outside the scalar domain.
    fn update_cell(&self, state: &mut AggState, row: usize) -> Option<i64> {
        match self {
            Self::Expression => None,
            Self::FirstRow => state.has_first_row().then_some(0),
            Self::CountAll => state.update_count_fast(true).then_some(0),
            Self::Count(column) => state
                .update_count_fast(!column.read().is_null(row))
                .then_some(0),
            Self::CountDistinctInt(column) => {
                let column = column.read();
                let value = (!column.is_null(row)).then(|| column.get_int64(row));
                state.update_count_distinct_int_fast(value)
            }
            Self::FinalCount { column, unsigned } => {
                match read_count(&column.read(), row, *unsigned)? {
                    None => Some(0),
                    Some(value) => state.update_final_count_fast(value).then_some(0),
                }
            }
            Self::Decimal(column) => {
                let column = column.read();
                if column.is_null(row) {
                    return Some(0);
                }
                let (coefficient, scale) = column.get_my_decimal_i128_scaled(row)?;
                state
                    .partial_update_with_coefficient(coefficient, scale)
                    .then_some(0)
            }
            Self::AvgDecimal { sum, count } => {
                // Go avgPartial4Decimal evaluates sum before count. Release
                // each read before acquiring another: the columns may alias.
                let coefficient = {
                    let sum = sum.read();
                    if sum.is_null(row) {
                        return Some(0);
                    }
                    sum.get_my_decimal_i128_scaled(row)?
                };
                let count = match count {
                    None => 1,
                    Some((column, unsigned)) => match read_count(&column.read(), row, *unsigned)? {
                        None => return Some(0),
                        Some(count) if count >= 0 => count,
                        Some(_) => return None,
                    },
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
        if let Some(delta) = self.update_cell(state, row.idx()) {
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
    let value = column.get_int64(row);
    if unsigned {
        i64::try_from(value as u64).ok().map(Some)
    } else {
        Some(Some(value))
    }
}

pub(super) fn bind_inputs<'a>(
    modes: &[AggInputMode],
    chunk: &'a Chunk,
) -> smallvec::SmallVec<[AggInputMode<ColumnRef<'a>>; 4]> {
    modes.iter().map(|mode| mode.bind(chunk)).collect()
}

pub(super) fn update_row<C: Columns>(
    modes: &[AggInputMode<ColumnRef<'_>>],
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
