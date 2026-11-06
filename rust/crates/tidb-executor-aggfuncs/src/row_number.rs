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

//! Go `pkg/executor/aggfuncs/row_number.go`: `ROW_NUMBER()`.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_driver_error::ExecError;

use crate::agg_func::{partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult, SlidingWindowAggFunc};

/// Go `DefPartialResult4RowNumberSize`.
pub const DEF_PARTIAL_RESULT_4_ROW_NUMBER_SIZE: i64 = 8;

/// Go `partialResult4RowNumber`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartialResult4RowNumber {
    cur_idx: i64,
}

/// Go `rowNumber`.
#[derive(Clone, Debug)]
pub struct RowNumber {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl AggFunc for RowNumber {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4RowNumber::default()), DEF_PARTIAL_RESULT_4_ROW_NUMBER_SIZE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        partial::<PartialResult4RowNumber>(pr).cur_idx = 0;
    }

    fn update_partial_result(&self, _sctx: &AggFuncUpdateContext, _rows_in_group: &[Row<'_>], _pr: &mut PartialResult) -> Result<i64, ExecError> {
        Ok(0)
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4RowNumber>(pr);
        p.cur_idx += 1;
        chk.append_int64(self.base.ordinal, p.cur_idx);
        Ok(())
    }

    fn as_sliding_window(&self) -> Option<&dyn SlidingWindowAggFunc> {
        Some(self)
    }
}

impl SlidingWindowAggFunc for RowNumber {
    fn slide<'a>(&self, _sctx: &AggFuncUpdateContext, _get_row: &dyn Fn(u64) -> Row<'a>, _last_start: u64, _last_end: u64, _shift_start: u64, _shift_end: u64, _pr: &mut PartialResult) -> Result<(), ExecError> {
        Ok(())
    }
}
