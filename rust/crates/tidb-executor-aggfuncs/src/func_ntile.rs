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

//! Go `pkg/executor/aggfuncs/func_ntile.go`: `NTILE(n)`, which divides the
//! partition into `n` ranked groups -- 11 rows into 3 make groups of 4, 4
//! and 3.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_driver_error::ExecError;

use crate::agg_func::{partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};

/// Go `DefPartialResult4Ntile`.
pub const DEF_PARTIAL_RESULT_4_NTILE: i64 = 40;

/// Go `partialResult4Ntile`.
#[derive(Clone, Copy, Debug)]
pub struct PartialResult4Ntile {
    cur_idx: u64,
    cur_group_idx: u64,
    remainder: u64,
    quotient: u64,
    num_rows: u64,
}

/// Go `ntile`.
#[derive(Clone, Debug)]
pub struct Ntile {
    /// Go `n`: zero where the argument is NULL.
    pub n: u64,
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
}

impl AggFunc for Ntile {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        let p = PartialResult4Ntile { cur_idx: 0, cur_group_idx: 1, remainder: 0, quotient: 0, num_rows: 0 };
        (Box::new(p), DEF_PARTIAL_RESULT_4_NTILE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        let p = partial::<PartialResult4Ntile>(pr);
        p.cur_idx = 0;
        p.cur_group_idx = 1;
        p.num_rows = 0;
    }

    fn update_partial_result(&self, _sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        let p = partial::<PartialResult4Ntile>(pr);
        p.num_rows += rows_in_group.len() as u64;
        // Update the quotient and remainder.
        if self.n != 0 {
            p.quotient = p.num_rows / self.n;
            p.remainder = p.num_rows % self.n;
        }
        Ok(0)
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4Ntile>(pr);
        // A zero divisor is a NULL argument: NULL.
        if self.n == 0 {
            chk.append_null(self.base.ordinal);
            return Ok(());
        }
        chk.append_uint64(self.base.ordinal, p.cur_group_idx);
        p.cur_idx += 1;
        let mut cur_max_idx = p.quotient;
        if p.cur_group_idx <= p.remainder {
            cur_max_idx += 1;
        }
        if p.cur_idx == cur_max_idx {
            p.cur_idx = 0;
            p.cur_group_idx += 1;
        }
        Ok(())
    }
}
