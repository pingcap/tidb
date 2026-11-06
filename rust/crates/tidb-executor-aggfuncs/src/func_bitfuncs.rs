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


//! Go `pkg/executor/aggfuncs/func_bitfuncs.go`: `BIT_OR`, `BIT_XOR` and
//! `BIT_AND` over the argument's `uint64` bit pattern.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::Datum;
use tidb_driver_error::ExecError;

use crate::agg_func::{
    deserialize_partial_result_common, partial, AggFunc, AggFuncUpdateContext, BaseAggFunc,
    PartialResult, SlidingWindowAggFunc,
};
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4BitFuncSize`.
pub const DEF_PARTIAL_RESULT_4_BIT_FUNC_SIZE: i64 = 8;

/// Go `partialResult4BitFunc`.
pub type PartialResult4BitFunc = u64;

/// Go `baseBitAggFunc`.
#[derive(Clone, Debug)]
pub struct BaseBitAggFunc {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseBitAggFunc {
    /// Go `EvalInt`: the argument's bit pattern, `None` for NULL.
    fn eval_int(&self, sctx: &AggFuncUpdateContext, row: Row<'_>) -> Result<Option<u64>, ExecError> {
        Ok(match self.base.args[0].eval(sctx, row).map_err(ExecError::Eval)? {
            Datum::Null => None,
            Datum::Int(value) => Some(value as u64),
            Datum::UInt(value) => Some(value),
            _ => return Err(ExecError::internal("a bit function read a non-integer argument")),
        })
    }

    fn append_final_result_2_chunk(&self, pr: &mut PartialResult, chk: &mut Chunk) {
        chk.append_uint64(self.base.ordinal, *partial::<PartialResult4BitFunc>(pr));
    }

    fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
        let pr = *partial::<PartialResult4BitFunc>(partial_result);
        let res_buf = spill_helper.serialize_partial_result_4_bit_func(pr);
        chk.append_bytes(self.base.ordinal, res_buf);
    }

    fn deserialize_partial_result(&self, src: &Chunk, alloc: impl Fn() -> (PartialResult, i64)) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            let (mut pr, mem_delta) = alloc();
            *partial::<PartialResult4BitFunc>(&mut pr) = helper.deserialize_partial_result_4_bit_func()?;
            Some((pr, mem_delta))
        })
    }

    /// Folds every non-NULL row of the group into `p` with `op`.
    fn update(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult, op: fn(u64, u64) -> u64) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4BitFunc>(pr);
        for row in rows_in_group {
            if let Some(input_value) = self.eval_int(sctx, *row)? {
                *p = op(*p, input_value);
            }
        }
        Ok(0)
    }
}

/// The Go `bitXUint64` types: one fold over [`BaseBitAggFunc`] and its
/// starting value.
macro_rules! bit_func {
    ($name:ident, init: $init:expr, op: |$a:ident, $b:ident| $op:expr) => {
        #[doc = concat!("Go `", stringify!($name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $name(pub BaseBitAggFunc);

        impl $name {
            fn op($a: u64, $b: u64) -> u64 {
                $op
            }
        }

        impl AggFunc for $name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                (Box::new($init as PartialResult4BitFunc), DEF_PARTIAL_RESULT_4_BIT_FUNC_SIZE)
            }

            fn reset_partial_result(&self, pr: &mut PartialResult) {
                *partial::<PartialResult4BitFunc>(pr) = $init;
            }

            fn update_partial_result(&self, sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
                self.0.update(sctx, rows_in_group, pr, Self::op)
            }

            fn merge_partial_result(&self, _sctx: &AggFuncUpdateContext, src: &mut PartialResult, dst: &mut PartialResult) -> Result<i64, ExecError> {
                let p1 = *partial::<PartialResult4BitFunc>(src);
                let p2 = partial::<PartialResult4BitFunc>(dst);
                *p2 = Self::op(*p2, p1);
                Ok(0)
            }

            fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                self.0.append_final_result_2_chunk(pr, chk);
                Ok(())
            }

            fn serialize_partial_result(&self, partial_result: &mut PartialResult, chk: &mut Chunk, spill_helper: &mut SerializeHelper) {
                self.0.serialize_partial_result(partial_result, chk, spill_helper);
            }

            fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
                self.0.deserialize_partial_result(src, || self.alloc_partial_result())
            }

            bit_func!(@sliding $name);
        }
    };
    (@sliding BitXorUint64) => {
        fn as_sliding_window(&self) -> Option<&dyn SlidingWindowAggFunc> {
            Some(self)
        }
    };
    (@sliding $name:ident) => {};
}

bit_func!(BitOrUint64, init: 0, op: |a, b| a | b);
bit_func!(BitXorUint64, init: 0, op: |a, b| a ^ b);
bit_func!(BitAndUint64, init: u64::MAX, op: |a, b| a & b);

impl SlidingWindowAggFunc for BitXorUint64 {
    fn slide<'a>(&self, sctx: &AggFuncUpdateContext, get_row: &dyn Fn(u64) -> Row<'a>, last_start: u64, last_end: u64, shift_start: u64, shift_end: u64, pr: &mut PartialResult) -> Result<(), ExecError> {
        let rows = (0..shift_start).map(|i| last_start + i).chain((0..shift_end).map(|i| last_end + i));
        let p = partial::<PartialResult4BitFunc>(pr);
        for row in rows {
            if let Some(input_value) = self.0.eval_int(sctx, get_row(row))? {
                *p ^= input_value;
            }
        }
        Ok(())
    }
}
