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

//! Go `pkg/executor/aggfuncs/func_cume_dist.go`: `CUME_DIST()`, the share
//! of the partition's rows up to the current row's last peer.

use std::cmp::Ordering;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::{OwnedRow, Row};
use tidb_driver_error::ExecError;

use crate::agg_func::{partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};
use crate::func_rank::{keep_rows, RowComparer};

/// Go `DefPartialResult4CumeDistSize`.
pub const DEF_PARTIAL_RESULT_4_CUME_DIST_SIZE: i64 = 40;

/// Go `partialResult4CumeDist`.
#[derive(Clone, Debug, Default)]
pub struct PartialResult4CumeDist {
    cur_idx: usize,
    last_rank: usize,
    rows: Vec<OwnedRow>,
}

/// Go `cumeDist`.
#[derive(Debug)]
pub struct CumeDist {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
    /// Go's embedded `rowComparer`.
    pub row_comparer: RowComparer,
}

impl AggFunc for CumeDist {
    fn alloc_partial_result(&self) -> (PartialResult, i64) {
        (Box::new(PartialResult4CumeDist::default()), DEF_PARTIAL_RESULT_4_CUME_DIST_SIZE)
    }

    fn reset_partial_result(&self, pr: &mut PartialResult) {
        let p = partial::<PartialResult4CumeDist>(pr);
        p.cur_idx = 0;
        p.last_rank = 0;
        p.rows.clear();
    }

    fn update_partial_result(&self, _sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
        Ok(keep_rows(&mut partial::<PartialResult4CumeDist>(pr).rows, rows_in_group))
    }

    fn append_final_result_2_chunk(&self, _sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
        let p = partial::<PartialResult4CumeDist>(pr);
        let num_rows = p.rows.len();
        while p.last_rank < num_rows && self.row_comparer.compare_rows(&p.rows[p.cur_idx], &p.rows[p.last_rank]) == Ordering::Equal {
            p.last_rank += 1;
        }
        p.cur_idx += 1;
        chk.append_float64(self.base.ordinal, p.last_rank as f64 / num_rows as f64);
        Ok(())
    }
}
