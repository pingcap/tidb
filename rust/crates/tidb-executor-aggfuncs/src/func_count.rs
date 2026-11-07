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

//! Go `pkg/executor/aggfuncs/func_count.go`: `COUNT` without `DISTINCT`.
//!
//! Go keeps one struct per argument eval type (`countOriginal4Int`, ...)
//! because each calls its typed `EvalX`. This tier's expressions evaluate
//! to a `Datum`, so the types share one body: a non-NULL value counts.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::Datum;
use tidb_driver_error::ExecError;

use crate::agg_func::{
    deserialize_partial_result_common, partial, AggFunc, AggFuncUpdateContext, BaseAggFunc,
    PartialResult, SlidingWindowAggFunc,
};
use crate::spill_deserialize_helper::DeserializeHelper;
use crate::spill_serialize_helper::SerializeHelper;

/// Go `DefPartialResult4CountSize`.
pub const DEF_PARTIAL_RESULT_4_COUNT_SIZE: i64 = 8;

/// Go `partialResult4Count`.
pub type PartialResult4Count = i64;

/// Go `baseCount`.
#[derive(Clone, Debug)]
pub struct BaseCount {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl BaseCount {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(0 as PartialResult4Count), DEF_PARTIAL_RESULT_4_COUNT_SIZE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        *partial::<PartialResult4Count>(pr) = 0;
    }

    fn append_final_result_2_chunk(&self, pr: &mut PartialResult, chk: &mut Chunk) {
        chk.append_int64(self.base.ordinal, *partial::<PartialResult4Count>(pr));
    }

    fn serialize_partial_result(
        &self,
        partial_result: &mut PartialResult,
        chk: &mut Chunk,
        spill_helper: &mut SerializeHelper,
    ) {
        let pr = *partial::<PartialResult4Count>(partial_result);
        let res_buf = spill_helper.serialize_partial_result_4_count(pr);
        chk.append_bytes(self.base.ordinal, res_buf);
    }

    fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
        deserialize_partial_result_common(src, self.base.ordinal, |helper| {
            self.deserialize_for_spill(helper)
        })
    }

    /// Go `deserializeForSpill`.
    fn deserialize_for_spill(&self, helper: &mut DeserializeHelper<'_>) -> Option<(PartialResult, i64)> {
        let (mut pr, mem_delta) = self.alloc_partial_result();
        let value = helper.deserialize_partial_result_4_count()?;
        *partial::<PartialResult4Count>(&mut pr) = value;
        Some((pr, mem_delta))
    }

    /// Whether `args[0]` is NULL on `row` (Go's `_, isNull, err := EvalX`).
    fn arg_is_null(&self, sctx: &AggFuncUpdateContext, row: Row<'_>) -> Result<bool, ExecError> {
        Ok(self.base.args[0].eval(sctx, row).map_err(ExecError::Eval)?.is_null())
    }
}

/// The `AggFunc` methods every `COUNT` shares, delegated to its `BaseCount`.
macro_rules! base_count_methods {
    () => {
        fn alloc_partial_result(&self) -> (PartialResult, i64) {
            self.0.alloc_partial_result()
        }

        fn reset_partial_result(&self, pr: &mut PartialResult) {
            self.0.reset_partial_result(pr);
        }

        fn append_final_result_2_chunk(
            &self,
            _sctx: &AggFuncUpdateContext,
            pr: &mut PartialResult,
            chk: &mut Chunk,
        ) -> Result<(), ExecError> {
            self.0.append_final_result_2_chunk(pr, chk);
            Ok(())
        }

        fn serialize_partial_result(
            &self,
            partial_result: &mut PartialResult,
            chk: &mut Chunk,
            spill_helper: &mut SerializeHelper,
        ) {
            self.0.serialize_partial_result(partial_result, chk, spill_helper);
        }

        fn deserialize_partial_result(&self, src: &Chunk) -> (Vec<PartialResult>, i64) {
            self.0.deserialize_partial_result(src)
        }
    };
}

/// Go's `countOriginal4X` for each argument eval type: counts the rows
/// whose argument is not NULL, and slides by the rows that leave and enter.
macro_rules! count_original {
    ($($name:ident),* $(,)?) => {$(
        #[doc = concat!("Go `", stringify!($name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $name(pub BaseCount);

        impl AggFunc for $name {
            base_count_methods!();

            fn update_partial_result(
                &self,
                sctx: &AggFuncUpdateContext,
                rows_in_group: &[Row<'_>],
                pr: &mut PartialResult,
            ) -> Result<i64, ExecError> {
                let p = partial::<PartialResult4Count>(pr);
                for row in rows_in_group {
                    if self.0.arg_is_null(sctx, *row)? {
                        continue;
                    }
                    *p += 1;
                }
                Ok(0)
            }

            fn as_sliding_window(&self) -> Option<&dyn SlidingWindowAggFunc> {
                Some(self)
            }
        }

        impl SlidingWindowAggFunc for $name {
            fn slide<'a>(
                &self,
                sctx: &AggFuncUpdateContext,
                get_row: &dyn Fn(u64) -> Result<Row<'a>, ExecError>,
                last_start: u64,
                last_end: u64,
                shift_start: u64,
                shift_end: u64,
                pr: &mut PartialResult,
            ) -> Result<(), ExecError> {
                let p = partial::<PartialResult4Count>(pr);
                for i in 0..shift_start {
                    if self.0.arg_is_null(sctx, get_row(last_start.wrapping_add(i))?)? {
                        continue;
                    }
                    *p -= 1;
                }
                for i in 0..shift_end {
                    if self.0.arg_is_null(sctx, get_row(last_end.wrapping_add(i))?)? {
                        continue;
                    }
                    *p += 1;
                }
                Ok(())
            }
        }
    )*};
}

count_original!(
    CountOriginal4Int,
    CountOriginal4Real,
    CountOriginal4Decimal,
    CountOriginal4Time,
    CountOriginal4Duration,
    CountOriginal4JSON,
    CountOriginal4VectorFloat32,
    CountOriginal4String,
);

/// Go `countPartial`: sums the partial counts its input carries.
#[derive(Clone, Debug)]
pub struct CountPartial(pub BaseCount);

impl AggFunc for CountPartial {
    base_count_methods!();

    fn update_partial_result(
        &self,
        sctx: &AggFuncUpdateContext,
        rows_in_group: &[Row<'_>],
        pr: &mut PartialResult,
    ) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4Count>(pr);
        for row in rows_in_group {
            let input = match self.0.base.args[0].eval(sctx, *row).map_err(ExecError::Eval)? {
                Datum::Null => continue,
                Datum::Int(value) => value,
                Datum::UInt(value) => value as i64,
                _ => return Err(ExecError::internal("a partial COUNT's input is not an integer")),
            };
            *p += input;
        }
        Ok(0)
    }

    fn merge_partial_result(
        &self,
        _sctx: &AggFuncUpdateContext,
        src: &mut PartialResult,
        dst: &mut PartialResult,
    ) -> Result<i64, ExecError> {
        let p1 = *partial::<PartialResult4Count>(src);
        *partial::<PartialResult4Count>(dst) += p1;
        Ok(0)
    }
}
