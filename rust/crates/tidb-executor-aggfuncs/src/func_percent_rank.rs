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

//! Go `pkg/executor/aggfuncs/func_percent_rank.go`: `PERCENT_RANK()`,
//! `(rank - 1) / (rows - 1)`.

use std::cmp::Ordering;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_driver_error::ExecError;

use crate::agg_func::{partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};
use crate::func_rank::{keep_rows, PartialResult4Rank, RowComparer, DEF_PARTIAL_RESULT_4_RANK_SIZE};

/// Go `percentRank`.
#[derive(Debug)]
pub struct PercentRank {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
    /// Go's embedded `rowComparer`.
    pub row_comparer: RowComparer,
}

impl AggFunc for PercentRank {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4Rank::default()), DEF_PARTIAL_RESULT_4_RANK_SIZE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        let p = partial::<PartialResult4Rank>(pr);
        p.cur_idx = 0;
        p.last_rank = 0;
        p.rows.clear();
    }

    fn update_partial_result(&self, _sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        Ok(keep_rows(&mut partial::<PartialResult4Rank>(pr).rows, rows_in_group))
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4Rank>(pr);
        let num_rows = p.rows.len() as i64;
        p.cur_idx += 1;
        if p.cur_idx == 1 {
            p.last_rank = 1;
            chk.append_float64(self.base.ordinal, 0.0);
            return Ok(());
        }
        let (prev, curr) = (&p.rows[(p.cur_idx - 2) as usize], &p.rows[(p.cur_idx - 1) as usize]);
        if self.row_comparer.compare_rows(prev, curr) != Ordering::Equal {
            p.last_rank = p.cur_idx;
        }
        chk.append_float64(self.base.ordinal, (p.last_rank - 1) as f64 / (num_rows - 1) as f64);
        Ok(())
    }
}
