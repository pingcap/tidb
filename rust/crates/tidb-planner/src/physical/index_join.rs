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

//! Execution-bearing Go `physicalop.PhysicalIndexJoin`. Candidate enumeration,
//! live cache activation and IndexHash/IndexMerge variants remain
//! separate integration work; this is not a whole physicalop package port.

use super::{BasePhysicalJoin, BasePhysicalPlan, PhysicalPlan};
use crate::find_best_task::index_join::path::IndexJoinRangeTemplate;
use crate::ranger::types::Ranges;
use std::{borrow::Cow, sync::Arc};
use tidb_expr::{
    column::Column,
    expression::{Expression, ScalarFunction},
    schema::Schema,
};

/// Go `IndexJoinInfo`: the chosen inner access path feeds this back through
/// CopTask/RootTask to the enclosing IndexJoin. Negative offsets denote
/// equalities that the chosen index cannot use as lookup keys.
#[derive(Clone, Debug, Default)]
pub struct IndexJoinInfo {
    /// Prefix lengths for the chosen index.
    pub idx_col_lens: Vec<i64>,
    /// Original equality offset to chosen index offset, or -1.
    pub key_off_to_idx_off: Vec<i64>,
    /// Chosen template ranges.
    pub ranges: Ranges,
    /// Go mutable range's read-only chosen path and join definitions.
    pub range_template: Option<Arc<IndexJoinRangeTemplate>>,
    /// Outer-dependent final-column bounds.
    pub compare_filters: Option<ColWithCmpFuncManager>,
}

/// Go `ColWithCmpFuncManager`: definitions are retained; row-specific scratch
/// constants and comparison functions belong to each executor build.
#[derive(Clone, Debug)]
pub struct ColWithCmpFuncManager {
    /// The indexed last column.
    pub target_col: Column,
    /// Its index prefix length, or UnspecifiedLength.
    pub col_length: i64,
    /// Comparison names in expression order.
    pub op_types: Vec<String>,
    /// Outer-row expressions supplying each bound.
    pub op_args: Vec<Expression>,
    /// Deduplication keys for rows with equal lookup keys but different bounds.
    pub affected_col_schema: Schema,
}

impl ColWithCmpFuncManager {
    /// Go AppendNewExpr keeps affected columns once, in encounter order.
    pub fn append_new_expr(&mut self, op: String, arg: Expression, affected: &[Column]) {
        self.op_types.push(op);
        self.op_args.push(arg);
        for column in affected {
            if !self.affected_col_schema.contains(column) {
                self.affected_col_schema.columns.push(column.clone());
            }
        }
    }
}

/// Go `PhysicalIndexJoin`. InnerPlan is the optimizer's unattached inner task;
/// execution reads the attached child at BasePhysicalJoin.InnerChildIdx.
#[derive(Clone, Debug, Default)]
pub struct PhysicalIndexJoin {
    /// Logical join kind, child schemas, predicates and defaults.
    pub join: BasePhysicalJoin,
    /// Optimizer-owned unattached inner plan.
    pub inner_plan: Option<Box<PhysicalPlan>>,
    /// Template ranges at plan construction, defining the reusable shape.
    pub ranges: Ranges,
    /// Immutable definitions shared by cached-plan clones, when parameter dependent.
    pub range_template: Option<Arc<IndexJoinRangeTemplate>>,
    /// Join-key offset to index-column offset.
    pub key_off_to_idx_off: Vec<usize>,
    /// Index-column prefix lengths.
    pub idx_col_lens: Vec<i64>,
    /// Outer-row bounds on the final index column.
    pub compare_filters: Option<ColWithCmpFuncManager>,
    /// Outer columns used to construct lookup ranges.
    pub outer_join_keys: Vec<Column>,
    /// Corresponding inner index or handle columns.
    pub inner_join_keys: Vec<Column>,
    /// Outer lookup keys followed by additional hash equalities.
    pub outer_hash_keys: Vec<Column>,
    /// Corresponding inner hash keys.
    pub inner_hash_keys: Vec<Column>,
    /// Original logical equality conditions, retained for planner completion.
    pub equal_conditions: Vec<ScalarFunction>,
    /// Whether decorrelation preserved Apply's original input order.
    pub from_decorrelated_apply: bool,
}

impl PhysicalIndexJoin {
    /// Bind the retained template before constructing this execution's readers.
    /// Go performs this at `rebuildRange` before building fresh executors.
    pub fn ranges_for_execution(
        &self,
        context: &impl tidb_expr::Columns,
    ) -> Result<Cow<'_, Ranges>, crate::plan_base::PlanError> {
        match &self.range_template {
            Some(template) => template
                .rebuild(&self.ranges, &|constant| constant.eval_in(context))
                .map(Cow::Owned),
            None => Ok(Cow::Borrowed(&self.ranges)),
        }
    }

    /// Go `completePhysicalIndexJoin`: consume the inner path feedback after
    /// task selection, then promote eligible residual equalities to hash keys.
    pub fn complete_from_inner(
        &mut self,
        info: IndexJoinInfo,
        inner_schema: &Schema,
        outer_schema: &Schema,
        extract_other_eq: bool,
    ) -> Result<(), crate::plan_base::PlanError> {
        use crate::plan_base::PlanError;
        let count = info.key_off_to_idx_off.len();
        if self.inner_join_keys.len() != count
            || self.outer_join_keys.len() != count
            || self.join.is_null_eq.len() != count
            || self.equal_conditions.len() != count
        {
            return Err(PlanError::internal(
                "IndexJoin feedback equality count mismatch",
            ));
        }
        let mut inner_keys = Vec::with_capacity(count);
        let mut outer_keys = Vec::with_capacity(count);
        let mut null_eq = Vec::with_capacity(count);
        let mut offsets = Vec::with_capacity(count);
        let mut conditions = std::mem::take(&mut self.join.other_conditions);
        for (key, offset) in info.key_off_to_idx_off.into_iter().enumerate() {
            if offset < 0 {
                conditions.push(Expression::ScalarFunction(
                    self.equal_conditions[key].clone(),
                ));
            } else {
                inner_keys.push(self.inner_join_keys[key].clone());
                outer_keys.push(self.outer_join_keys[key].clone());
                null_eq.push(self.join.is_null_eq[key]);
                offsets.push(offset as usize);
            }
        }
        let mut outer_hash = outer_keys.clone();
        let mut inner_hash = inner_keys.clone();
        if extract_other_eq {
            for index in (0..conditions.len()).rev() {
                let Expression::ScalarFunction(function) = &conditions[index] else {
                    continue;
                };
                if function.func_name.lowercase() != "eq" {
                    continue;
                }
                let [Expression::Column(lhs), Expression::Column(rhs)] = function.args.as_slice()
                else {
                    continue;
                };
                // Go #25799: IN operands must retain the joiner's NULL semantics.
                if lhs.in_operand || rhs.in_operand {
                    continue;
                }
                if outer_schema.contains(lhs) && inner_schema.contains(rhs) {
                    outer_hash.push(lhs.clone());
                    inner_hash.push(rhs.clone());
                } else if inner_schema.contains(lhs) && outer_schema.contains(rhs) {
                    outer_hash.push(rhs.clone());
                    inner_hash.push(lhs.clone());
                }
                // The upstream loop removes bare-column EQs here, after its
                // schema-side checks, in reverse residual order.
                conditions.remove(index);
            }
        }
        self.join.is_null_eq = null_eq;
        self.inner_join_keys = inner_keys;
        self.outer_join_keys = outer_keys;
        self.join.other_conditions = conditions;
        self.key_off_to_idx_off = offsets;
        self.ranges = info.ranges;
        self.range_template = info.range_template;
        self.idx_col_lens = info.idx_col_lens;
        self.compare_filters = info.compare_filters;
        self.outer_hash_keys = outer_hash;
        self.inner_hash_keys = inner_hash;
        self.equal_conditions.clear();
        Ok(())
    }

    pub(super) fn clone_with_base(&self, base: BasePhysicalPlan) -> Self {
        Self {
            join: self.join.clone_with_base(base),
            inner_plan: self.inner_plan.clone(),
            ranges: self.ranges.clone(),
            range_template: self.range_template.clone(),
            key_off_to_idx_off: self.key_off_to_idx_off.clone(),
            idx_col_lens: self.idx_col_lens.clone(),
            compare_filters: self.compare_filters.clone(),
            outer_join_keys: self.outer_join_keys.clone(),
            inner_join_keys: self.inner_join_keys.clone(),
            outer_hash_keys: self.outer_hash_keys.clone(),
            inner_hash_keys: self.inner_hash_keys.clone(),
            equal_conditions: self.equal_conditions.clone(),
            from_decorrelated_apply: self.from_decorrelated_apply,
        }
    }
}
