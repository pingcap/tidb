// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `mutableIndexJoinRange`: immutable definitions shared by cache clones,
//! with rebuilt ranges owned by the current execution.

use super::IndexJoinPathRangeBuilder;
use crate::{
    physical_property::IndexJoinRuntimeProp,
    plan_base::PlanError,
    ranger::{points::ExpressionEvaluator, types::Ranges},
};
use tidb_expr::{column::Column, expression::Expression, schema::Schema};

/// The chosen path and `indexJoinPathInfo` retained by Go's mutable range.
/// No evaluation context or transaction is captured. Mutable constant markers
/// are evaluated through the current execution, not their saved literal values.
#[derive(Debug)]
pub struct IndexJoinRangeTemplate {
    columns: Vec<Column>,
    lengths: Vec<i64>,
    lookup: IndexJoinRuntimeProp,
    inner_schema: Schema,
    pushed_conditions: Vec<Expression>,
    regard_null_as_point: bool,
    opt_prefix_index_single_scan: bool,
}

impl IndexJoinRangeTemplate {
    pub(super) fn capture(builder: &IndexJoinPathRangeBuilder<'_>) -> Self {
        Self {
            columns: builder.columns.to_vec(),
            lengths: builder.lengths.to_vec(),
            lookup: builder.lookup.clone(),
            inner_schema: builder.inner_schema.clone(),
            pushed_conditions: builder.pushed_conditions.to_vec(),
            regard_null_as_point: builder.regard_null_as_point,
            opt_prefix_index_single_scan: builder.opt_prefix_index_single_scan,
        }
    }

    /// Go `mutableIndexJoinRange.Rebuild`. A changed shape rejects cache reuse;
    /// it must never silently replace the chosen plan with a different access.
    pub fn rebuild(
        &self,
        previous: &Ranges,
        eval_expression: &ExpressionEvaluator<'_>,
    ) -> Result<Ranges, PlanError> {
        let (result, empty) = IndexJoinPathRangeBuilder {
            columns: &self.columns,
            lengths: &self.lengths,
            lookup: &self.lookup,
            inner_schema: &self.inner_schema,
            pushed_conditions: &self.pushed_conditions,
            eval_expression,
            range_max_size: 0,
            record_range_fallback: &|_| {},
            regard_null_as_point: self.regard_null_as_point,
            opt_prefix_index_single_scan: self.opt_prefix_index_single_scan,
        }
        .build(true)?;
        if empty {
            return Err(PlanError::internal("failed to rebuild range: empty range"));
        }
        let result = result
            .ok_or_else(|| PlanError::internal("failed to rebuild range: range width changed"))?;
        if previous.len() != result.ranges.len()
            || previous.first().map(|range| range.width())
                != result.ranges.first().map(|range| range.width())
        {
            return Err(PlanError::internal(
                "failed to rebuild range: range width changed",
            ));
        }
        Ok(result.ranges)
    }
}
