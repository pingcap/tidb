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

//! Execution-time rebuilding from the scan's retained access expressions.
//! Reader adapters must call this after Apply updates its outer bindings, before opening
//! the storage request. There is no optimizer range-memory limit at execution.
use super::{PhysicalIndexScan, PhysicalTableScan};
use crate::{
    plan_base::PlanError,
    ranger::{detacher, ranger, types::Ranges},
};
use tidb_expr::{
    expr_util::{
        builder::RealFunctionBuilder,
        substitute::{substitute_cor_col_2_constant, SubstituteOptions},
    },
    expression::Expression,
    Columns,
};

fn substitute(access: &[Expression], context: &impl Columns) -> Result<Vec<Expression>, PlanError> {
    let builder = RealFunctionBuilder::new(context);
    let options = SubstituteOptions::new(&builder);
    access
        .iter()
        .map(|expr| {
            substitute_cor_col_2_constant(expr, context, &options).map_err(|error| {
                PlanError::internal(format!("cannot bind scan access condition: {error:?}"))
            })
        })
        .collect()
}
impl PhysicalTableScan {
    /// Go PhysicalTableScan.ResolveCorrelatedColumns, with execution-owned output.
    pub fn rebuild_access_ranges(&self, context: &impl Columns) -> Result<Ranges, PlanError> {
        let evaluate =
            |expression: &Expression| expression.eval(context, tidb_chunk::row::Row::empty());
        if !self.common_handle_cols.is_empty() {
            // Go appends substituted predicates to the original access slice.
            let mut access = self.access_conditions.clone();
            access.extend(substitute(&self.access_conditions, context)?);
            return detacher::detach_cond_and_build_range_for_index_in(
                &access,
                &self.common_handle_cols,
                &self.common_handle_lens,
                0,
                &evaluate,
            )
            .map(|result| result.ranges)
            .map_err(|error| {
                PlanError::internal(format!("cannot rebuild common-handle ranges: {error:?}"))
            });
        }
        let tp = self
            .pk_column
            .as_ref()
            .and_then(|col| col.ret_type.as_ref())
            .ok_or_else(|| {
                PlanError::internal("integer range rebuild requires the primary-key type")
            })?;
        ranger::build_table_range_in(&self.access_conditions, tp, 0, &evaluate)
            .map(|result| result.ranges)
            .map_err(|error| PlanError::internal(format!("cannot rebuild table ranges: {error:?}")))
    }
}
impl PhysicalIndexScan {
    /// Go executor.rebuildIndexRanges. Reserved filters stay in the physical
    /// parent; rebuilding does not delete or reclassify those predicates.
    pub fn rebuild_access_ranges(&self, context: &impl Columns) -> Result<Ranges, PlanError> {
        if self.idx_cols.len() != self.idx_col_lens.len() || self.idx_cols.is_empty() {
            return Err(PlanError::internal(
                "index range rebuild requires index columns and lengths",
            ));
        }
        let access = substitute(&self.access_conditions, context)?;
        detacher::detach_simple_cond_and_build_range_for_index_in(
            &access,
            &self.idx_cols,
            &self.idx_col_lens,
            0,
            &|constant| constant.eval_in(context),
        )
        .map(|(ranges, _, _)| ranges)
        .map_err(|error| PlanError::internal(format!("cannot rebuild index ranges: {error:?}")))
    }
}
