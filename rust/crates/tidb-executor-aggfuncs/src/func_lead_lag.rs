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

//! Go `pkg/executor/aggfuncs/func_lead_lag.go`: `LEAD` and `LAG`, which
//! keep the partition's rows and evaluate the argument (or the default) on
//! the row `offset` ahead or behind.
//!
//! Go keeps one `valueEvaluator` in the function (its TODO: "move it to
//! partial result when parallel execution is supported") and appends right
//! after evaluating, so each final result evaluates into a fresh one.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::{OwnedRow, Row};
use tidb_datatype::FieldType;
use tidb_driver_error::ExecError;
use tidb_expr::expression::Expression;

use crate::agg_func::{partial, AggFunc, AggFuncUpdateContext, BaseAggFunc, PartialResult};
use crate::func_rank::keep_rows;
use crate::func_value::build_value_evaluator;

/// Go `DefPartialResult4LeadLagSize`.
pub const DEF_PARTIAL_RESULT_4_LEAD_LAG_SIZE: i64 = 32;

/// Go `partialResult4LeadLag`.
#[derive(Clone, Debug, Default)]
pub struct PartialResult4LeadLag {
    rows: Vec<OwnedRow>,
    cur_idx: u64,
}

/// Go `baseLeadLag`.
#[derive(Clone, Debug)]
pub struct BaseLeadLag {
    /// Go's embedded `baseAggFunc`.
    pub base: BaseAggFunc,
    /// The result type Go's `valueEvaluator` was built for.
    pub ret_tp: FieldType,
    /// Go `defaultExpr`.
    pub default_expr: Expression,
    /// Go `offset`.
    pub offset: u64,
}

impl BaseLeadLag {
    fn evaluate_and_append(&self, sctx: &AggFuncUpdateContext, expr: &Expression, row: &OwnedRow, chk: &mut Chunk) -> Result<(), ExecError> {
        let (mut evaluator, _) = build_value_evaluator(&self.ret_tp);
        evaluator.evaluate_row(sctx, expr, row.as_row())?;
        evaluator.append_result(chk, self.base.ordinal)
    }
}

macro_rules! lead_lag {
    ($name:ident, |$e:ident, $p:ident| $pick:expr) => {
        #[doc = concat!("Go `", stringify!($name), "`.")]
        #[derive(Clone, Debug)]
        pub struct $name(pub BaseLeadLag);

        impl AggFunc for $name {
            fn alloc_partial_result(&self) -> (PartialResult, i64) {
                (Box::new(PartialResult4LeadLag::default()), DEF_PARTIAL_RESULT_4_LEAD_LAG_SIZE)
            }

            fn reset_partial_result(&self, pr: &mut PartialResult) {
                let p = partial::<PartialResult4LeadLag>(pr);
                p.rows.clear();
                p.cur_idx = 0;
            }

            fn update_partial_result(&self, _sctx: &AggFuncUpdateContext, rows_in_group: &[Row<'_>], pr: &mut PartialResult) -> Result<i64, ExecError> {
                Ok(keep_rows(&mut partial::<PartialResult4LeadLag>(pr).rows, rows_in_group))
            }

            fn append_final_result_2_chunk(&self, sctx: &AggFuncUpdateContext, pr: &mut PartialResult, chk: &mut Chunk) -> Result<(), ExecError> {
                let $p = partial::<PartialResult4LeadLag>(pr);
                let $e = &self.0;
                let (expr, row): (&Expression, &OwnedRow) = $pick;
                $e.evaluate_and_append(sctx, expr, row, chk)?;
                $p.cur_idx += 1;
                Ok(())
            }
        }
    };
}

lead_lag!(Lead, |e, p| if p.cur_idx + e.offset < p.rows.len() as u64 {
    (&e.base.args[0], &p.rows[(p.cur_idx + e.offset) as usize])
} else {
    (&e.default_expr, &p.rows[p.cur_idx as usize])
});

lead_lag!(Lag, |e, p| if p.cur_idx >= e.offset {
    (&e.base.args[0], &p.rows[(p.cur_idx - e.offset) as usize])
} else {
    (&e.default_expr, &p.rows[p.cur_idx as usize])
});
