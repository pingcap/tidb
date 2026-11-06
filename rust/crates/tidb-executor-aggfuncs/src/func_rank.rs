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

//! Go `pkg/executor/aggfuncs/func_rank.go`: `RANK()` and `DENSE_RANK()`,
//! and the ORDER BY row comparison `PERCENT_RANK()` and `CUME_DIST()`
//! share.
//!
//! Go keeps the partition's `chunk.Row`s, which point into chunks the
//! window executor holds; a Rust partial result owns copies of them
//! (`Row.CopyConstruct`) and charges Go's `DefRowSize` per row.

use std::cmp::Ordering;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::compare::{get_compare_func, CompareFunc};
use tidb_chunk::row::{OwnedRow, Row};
use tidb_driver_error::ExecError;
use tidb_expr::column::Column;

use crate::agg_func::{partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};

/// Go `DefPartialResult4RankSize`.
pub const DEF_PARTIAL_RESULT_4_RANK_SIZE: i64 = 40;
/// Go `DefRowSize`.
pub const DEF_ROW_SIZE: i64 = 16;

/// Go `partialResult4Rank`.
#[derive(Clone, Debug, Default)]
pub struct PartialResult4Rank {
    pub(crate) cur_idx: i64,
    pub(crate) last_rank: i64,
    pub(crate) rows: Vec<OwnedRow>,
}

/// Appends the group's rows (Go `p.rows = append(p.rows, rowsInGroup...)`)
/// and returns Go's charge.
pub(crate) fn keep_rows(rows: &mut Vec<OwnedRow>, rows_in_group: &[Row<'_>]) -> i64 {
    rows.extend(rows_in_group.iter().map(Row::copy_construct));
    rows_in_group.len() as i64 * DEF_ROW_SIZE
}

/// Go `rowComparer`: the ORDER BY columns that have a comparator.
pub struct RowComparer {
    cmp_funcs: Vec<CompareFunc>,
    col_idx: Vec<usize>,
}

impl std::fmt::Debug for RowComparer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowComparer").field("col_idx", &self.col_idx).finish_non_exhaustive()
    }
}

/// Go `buildRowComparer`.
#[must_use]
pub fn build_row_comparer(cols: &[Column]) -> RowComparer {
    let mut rc = RowComparer { cmp_funcs: Vec::with_capacity(cols.len()), col_idx: Vec::with_capacity(cols.len()) };
    for col in cols {
        let Some(cmp_func) = col.get_static_type().and_then(get_compare_func) else {
            continue;
        };
        rc.cmp_funcs.push(cmp_func);
        rc.col_idx.push(col.index as usize);
    }
    rc
}

impl RowComparer {
    /// Go `compareRows`.
    pub(crate) fn compare_rows(&self, prev: &OwnedRow, curr: &OwnedRow) -> Ordering {
        for (i, idx) in self.col_idx.iter().enumerate() {
            let res = (self.cmp_funcs[i])(prev.as_row(), *idx, curr.as_row(), *idx);
            if res != Ordering::Equal {
                return res;
            }
        }
        Ordering::Equal
    }
}

/// Go `rank`.
#[derive(Debug)]
pub struct Rank {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
    /// Go `isDense`: `DENSE_RANK()`.
    pub is_dense: bool,
    /// Go's embedded `rowComparer`.
    pub row_comparer: RowComparer,
}

impl AggFunc for Rank {
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
        p.cur_idx += 1;
        if p.cur_idx == 1 {
            p.last_rank = 1;
            chk.append_int64(self.base.ordinal, p.last_rank);
            return Ok(());
        }
        let (prev, curr) = (&p.rows[(p.cur_idx - 2) as usize], &p.rows[(p.cur_idx - 1) as usize]);
        if self.row_comparer.compare_rows(prev, curr) == Ordering::Equal {
            chk.append_int64(self.base.ordinal, p.last_rank);
            return Ok(());
        }
        if self.is_dense {
            p.last_rank += 1;
        } else {
            p.last_rank = p.cur_idx;
        }
        chk.append_int64(self.base.ordinal, p.last_rank);
        Ok(())
    }
}
