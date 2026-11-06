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


//! Go `pkg/executor/aggfuncs/func_percentile.go`: `APPROX_PERCENTILE`,
//! the value at ordinal rank `ceil(N * percent / 100)` of the group's
//! values, found by Go's introselect.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, MyDecimal, MySqlDuration, Time};
use tidb_driver_error::ExecError;
use tidb_selection::Selectable;

use crate::agg_func::{partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};

/// Go `DefSliceSize`.
const DEF_SLICE_SIZE: i64 = 24;

/// Go `percentile`: the index of the value at rank
/// `k = min(ceil(N * percent / 100), N)`, which `selection.Select` moves
/// into place.
fn percentile<T: Selectable + ?Sized>(data: &mut T, percent: i64) -> usize {
    let len = data.len();
    let k = ((len as f64 * (percent as f64 / 100.0)).ceil() as usize).min(len);
    tidb_selection::select(data, k as isize) as usize
}

/// Go's `sort.Interface` over a partial result's values.
struct PercentileData<'a, T> {
    values: &'a mut [T],
    less: fn(&T, &T) -> bool,
}

impl<T> Selectable for PercentileData<'_, T> {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn less(&self, i: usize, j: usize) -> bool {
        (self.less)(&self.values[i], &self.values[j])
    }

    fn swap(&mut self, i: usize, j: usize) {
        self.values.swap(i, j);
    }
}

/// Go `basePercentile`.
#[derive(Clone, Debug)]
pub struct BasePercentile {
    /// Go `percent`.
    pub percent: i64,
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

/// Go `basePercentile` itself, built for an argument type no percentile
/// orders: it keeps nothing and answers NULL.
#[derive(Clone, Debug)]
pub struct PercentileNull(pub BasePercentile);

impl AggFunc for PercentileNull {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(()), 0)
    }

    fn reset_partial_result(&self, _pr: &mut PartialResult) {}

    fn update_partial_result(&self, _sctx: &AggFuncUpdateContext, _rows_in_group: &[Row<'_>], _pr: &mut PartialResult) -> Result<i64, ExecError> {
        Ok(0)
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, _pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        chk.append_null(self.0.base.ordinal);
        Ok(())
    }
}

/// The Go `percentileOriginal4X` types over `partialResult4PercentileX`.
macro_rules! percentile_original {
    ($name:ident, $val:ty, elem: $elem:expr, eval: |$d:ident| $eval:expr, less: $less:expr, append: |$chk:ident, $ordinal:ident, $v:ident| $append:expr) => {
        #[doc = concat!("Go `", stringify!($name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $name(pub BasePercentile);

        impl $name {
            /// Go `MemSize`: the slice header and its values (by length).
            fn mem_size(p: &[$val]) -> i64 {
                DEF_SLICE_SIZE + p.len() as i64 * $elem
            }
        }

        impl AggFunc for $name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                (Box::new(Vec::<$val>::new()), DEF_SLICE_SIZE)
            }

            fn reset_partial_result(&self, pr: &mut PartialResult) {
                *partial::<Vec<$val>>(pr) = Vec::new();
            }

            fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
                let p = partial::<Vec<$val>>(pr);
                let start_mem = Self::mem_size(p);
                for row in rows_in_group {
                    let $d = self.0.base.args[0].eval(sctx, *row).map_err(ExecError::Eval)?;
                    if $d.is_null() {
                        continue;
                    }
                    p.push($eval);
                }
                Ok(Self::mem_size(p) - start_mem)
            }

            fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
                let p1 = std::mem::take(partial::<Vec<$val>>(src));
                partial::<Vec<$val>>(dst).extend(p1);
                Ok(0)
            }

            fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                let p = partial::<Vec<$val>>(pr);
                if p.is_empty() {
                    chk.append_null(self.0.base.ordinal);
                    return Ok(());
                }
                let index = percentile(&mut PercentileData { values: p, less: $less }, self.0.percent);
                let ($chk, $ordinal, $v) = (chk, self.0.base.ordinal, &p[index]);
                $append;
                Ok(())
            }
        }
    };
}

fn unexpected(what: &str) -> ExecError {
    ExecError::internal(format!("APPROX_PERCENTILE read a non-{what} argument"))
}

percentile_original!(PercentileOriginal4Int, i64, elem: 8,
    // Go `EvalInt`: an unsigned value's bit pattern, ordered as `int64`.
    eval: |d| match d { Datum::Int(v) => v, Datum::UInt(v) => v as i64, _ => return Err(unexpected("integer")) },
    less: |a, b| a < b,
    append: |chk, ordinal, v| chk.append_int64(ordinal, *v));
percentile_original!(PercentileOriginal4Real, f64, elem: 8,
    eval: |d| match d { Datum::Real(v) | Datum::Float32(v) => v, _ => return Err(unexpected("real")) },
    less: |a, b| a < b,
    append: |chk, ordinal, v| chk.append_float64(ordinal, *v));
percentile_original!(PercentileOriginal4Decimal, MyDecimal, elem: 40,
    eval: |d| match d {
        Datum::Decimal(v) => crate::func_sum::my_decimal(&v)?,
        _ => return Err(unexpected("decimal")),
    },
    less: |a, b| a.compare(b).is_lt(),
    append: |chk, ordinal, v| chk.append_my_decimal(ordinal, v));
percentile_original!(PercentileOriginal4Time, Time, elem: 8,
    eval: |d| match d { Datum::Time(v) => v, _ => return Err(unexpected("time")) },
    less: |a, b| a.compare(*b).is_lt(),
    append: |chk, ordinal, v| chk.append_time(ordinal, *v));
percentile_original!(PercentileOriginal4Duration, MySqlDuration, elem: 8,
    // Go `Duration.Compare`: the nanoseconds.
    eval: |d| match d { Datum::Duration(v) => v, _ => return Err(unexpected("duration")) },
    less: |a, b| a.nanoseconds() < b.nanoseconds(),
    append: |chk, ordinal, v| chk.append_duration(ordinal, *v));
