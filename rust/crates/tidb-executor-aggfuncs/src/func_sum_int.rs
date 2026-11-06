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

//! Go `pkg/executor/aggfuncs/func_sum_int.go`: `SUM_INT`, the integer sum
//! the planner uses where an integer result cannot overflow into a
//! decimal. A sum that leaves the integer domain is Go's `ErrOverflow`.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{add_int64, add_uint64, sub_int64, sub_uint64, OverflowError};
use tidb_datatype::Datum;
use tidb_driver_error::ExecError;
use tidb_set::Int64SetWithMemoryUsage;

use crate::agg_func::{
    deserialize_partial_result_common, partial, AggFunc, AggFuncUpdateContext, BaseAggFunc,
    PartialResult, SlidingWindowAggFunc,
};
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4SumInt64Size`.
pub const DEF_PARTIAL_RESULT_4_SUM_INT64_SIZE: i64 = 16;
/// Go `DefPartialResult4SumDistinctInt64Size`.
pub const DEF_PARTIAL_RESULT_4_SUM_DISTINCT_INT64_SIZE: i64 = 32;
/// Go `DefPartialResult4SumUint64Size`.
pub const DEF_PARTIAL_RESULT_4_SUM_UINT64_SIZE: i64 = 16;
/// Go `DefPartialResult4SumDistinctUint64Size`.
pub const DEF_PARTIAL_RESULT_4_SUM_DISTINCT_UINT64_SIZE: i64 = 32;

/// Go `partialResult4SumInt64`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartialResult4SumInt64 {
    pub(crate) val: i64,
    pub(crate) not_null_row_count: i64,
}

/// Go `partialResult4SumUint64`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartialResult4SumUint64 {
    pub(crate) val: u64,
    pub(crate) not_null_row_count: i64,
}

/// Go `partialResult4SumDistinctInt64` / `...Uint64`: the unsigned one keys
/// the set by the value's bit pattern.
pub struct PartialResult4SumDistinctInt64 {
    val_set: Int64SetWithMemoryUsage,
}

/// Go `baseSumIntAggFunc`.
#[derive(Clone, Debug)]
pub struct BaseSumIntAggFunc {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseSumIntAggFunc {
    /// Go `EvalInt`: the argument's bit pattern, `None` for NULL.
    fn eval_int(&self, sctx: &AggFuncUpdateContext, row: Row<'_>) -> Result<Option<i64>, ExecError> {
        Ok(match self.base.args[0].eval(sctx, row).map_err(ExecError::Eval)? {
            Datum::Null => None,
            Datum::Int(value) => Some(value),
            Datum::UInt(value) => Some(value as i64),
            _ => return Err(ExecError::internal("SUM_INT read a non-integer argument")),
        })
    }
}

/// Go `types.AddInt64`'s `ErrOverflow`.
fn overflow(error: OverflowError) -> ExecError {
    ExecError::from(tidb_datatype::types_errors::ERR_OVERFLOW.generate(error.to_string()))
}

/// Go `sumInt` and `sumUint`: the same sum over `i64` and `u64`.
macro_rules! sum_int {
    (
        $name:ident, $pr:ident, $size:expr, $val:ty,
        from: |$input:ident| $from:expr,
        add: $add:path, sub: $sub:path,
        append: $append:ident,
        serialize: $serialize:ident, deserialize: $deserialize:ident $(,)?
    ) => {
        #[doc = concat!("Go `", stringify!($name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $name(pub BaseSumIntAggFunc);

        impl $name {
            fn add_input(p: &mut $pr, value: $val) -> Result<(), ExecError> {
                if p.not_null_row_count == 0 {
                    p.val = value;
                    p.not_null_row_count = 1;
                    return Ok(());
                }
                p.val = $add(p.val, value).map_err(overflow)?;
                p.not_null_row_count += 1;
                Ok(())
            }

            fn input(&self, sctx: &AggFuncUpdateContext, row: Row<'_>) -> Result<Option<$val>, ExecError> {
                Ok(self.0.eval_int(sctx, row)?.map(|$input| $from))
            }
        }

        impl AggFunc for $name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                (Box::new($pr::default()), $size)
            }

            fn reset_partial_result(&self, pr: &mut PartialResult) {
                *partial::<$pr>(pr) = $pr::default();
            }

            fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                let p = *partial::<$pr>(pr);
                if p.not_null_row_count == 0 {
                    chk.append_null(self.0.base.ordinal);
                    return Ok(());
                }
                chk.$append(self.0.base.ordinal, p.val);
                Ok(())
            }

            fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
                let p = partial::<$pr>(pr);
                for row in rows_in_group {
                    if let Some(value) = self.input(sctx, *row)? {
                        Self::add_input(p, value)?;
                    }
                }
                Ok(0)
            }

            fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
                let p1 = *partial::<$pr>(src);
                let p2 = partial::<$pr>(dst);
                if p1.not_null_row_count == 0 {
                    return Ok(0);
                }
                if p2.not_null_row_count == 0 {
                    *p2 = p1;
                    return Ok(0);
                }
                p2.val = $add(p2.val, p1.val).map_err(overflow)?;
                p2.not_null_row_count += p1.not_null_row_count;
                Ok(0)
            }

            fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
                let pr = *partial::<$pr>(partial_result);
                let res_buf = spill_helper.$serialize(pr.val, pr.not_null_row_count);
                chk.append_bytes(self.0.base.ordinal, res_buf);
            }

            fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
                deserialize_partial_result_common(src, self.0.base.ordinal, |helper| {
                    let (mut pr, mem_delta) = self.alloc_partial_result();
                    let (val, not_null_row_count) = helper.$deserialize()?;
                    *partial::<$pr>(&mut pr) = $pr { val, not_null_row_count };
                    Some((pr, mem_delta))
                })
            }

            fn as_sliding_window(&self) -> Option<&dyn SlidingWindowAggFunc> {
                Some(self)
            }
        }

        impl SlidingWindowAggFunc for $name {
            fn slide<'a>(&self, sctx: &AggFuncUpdateContext, get_row: &dyn Fn(u64) -> Row<'a>, last_start: u64, last_end: u64, shift_start: u64, shift_end: u64, pr: &mut PartialResult) -> Result<(), ExecError> {
                let p = partial::<$pr>(pr);
                for i in 0..shift_start {
                    let Some(value) = self.input(sctx, get_row(last_start + i))? else {
                        continue;
                    };
                    p.val = $sub(p.val, value).map_err(overflow)?;
                    p.not_null_row_count -= 1;
                }
                for i in 0..shift_end {
                    let Some(value) = self.input(sctx, get_row(last_end + i))? else {
                        continue;
                    };
                    p.val = $add(p.val, value).map_err(overflow)?;
                    p.not_null_row_count += 1;
                }
                Ok(())
            }
        }
    };
}

sum_int!(
    SumInt, PartialResult4SumInt64, DEF_PARTIAL_RESULT_4_SUM_INT64_SIZE, i64,
    from: |input| input,
    add: add_int64, sub: sub_int64,
    append: append_int64,
    serialize: serialize_partial_result_4_sum_int64, deserialize: deserialize_partial_result_4_sum_int64,
);

sum_int!(
    SumUint, PartialResult4SumUint64, DEF_PARTIAL_RESULT_4_SUM_UINT64_SIZE, u64,
    from: |input| input as u64,
    add: add_uint64, sub: sub_uint64,
    append: append_uint64,
    serialize: serialize_partial_result_4_sum_uint64, deserialize: deserialize_partial_result_4_sum_uint64,
);

/// Go `sumDistinctInt64` and `sumDistinctUint64`: a set of the values'
/// bit patterns, summed in their own domain at the end.
macro_rules! sum_distinct_int {
    ($name:ident, $size:expr, $val:ty, add: $add:path, append: $append:ident $(,)?) => {
        #[doc = concat!("Go `", stringify!($name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $name(pub BaseSumIntAggFunc);

        impl AggFunc for $name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                let (val_set, set_size) = Int64SetWithMemoryUsage::new(std::iter::empty());
                (Box::new(PartialResult4SumDistinctInt64 { val_set }), $size + set_size)
            }

            fn reset_partial_result(&self, pr: &mut PartialResult) {
                partial::<PartialResult4SumDistinctInt64>(pr).val_set =
                    Int64SetWithMemoryUsage::new(std::iter::empty()).0;
            }

            fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
                let p = partial::<PartialResult4SumDistinctInt64>(pr);
                let mut mem_delta = 0;
                for row in rows_in_group {
                    let Some(key) = self.0.eval_int(sctx, *row)? else {
                        continue;
                    };
                    if p.val_set.contains(key) {
                        continue;
                    }
                    mem_delta += p.val_set.insert(key);
                }
                Ok(mem_delta)
            }

            fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                let p = partial::<PartialResult4SumDistinctInt64>(pr);
                if p.val_set.is_empty() {
                    chk.append_null(self.0.base.ordinal);
                    return Ok(());
                }
                let mut sum: $val = 0;
                for key in p.val_set.iter() {
                    sum = $add(sum, *key as $val).map_err(overflow)?;
                }
                chk.$append(self.0.base.ordinal, sum);
                Ok(())
            }

            fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
                let source: Vec<i64> = partial::<PartialResult4SumDistinctInt64>(src).val_set.iter().copied().collect();
                let d = partial::<PartialResult4SumDistinctInt64>(dst);
                let mut mem_delta = 0;
                for key in source {
                    if d.val_set.contains(key) {
                        continue;
                    }
                    mem_delta += d.val_set.insert(key);
                }
                Ok(mem_delta)
            }
        }
    };
}

sum_distinct_int!(SumDistinctInt64, DEF_PARTIAL_RESULT_4_SUM_DISTINCT_INT64_SIZE, i64, add: add_int64, append: append_int64);
sum_distinct_int!(SumDistinctUint64, DEF_PARTIAL_RESULT_4_SUM_DISTINCT_UINT64_SIZE, u64, add: add_uint64, append: append_uint64);
