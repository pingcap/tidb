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

//! Go `pkg/planner/core/operator/logicalop/logical_selection.go`:
//! `LogicalSelection`, a `WHERE`/`HAVING` filter over a single child.
//!
//! SEED of `pkg/planner/core`: the operator's own state and its
//! dependency-closed member bodies land here. The bodies that Go writes
//! against the session (`SCtx()`), the rule utilities
//! (`ruleutil.ApplyPredicateSimplification`), or operators this batch does not
//! port (`LogicalWindow`, `LogicalTopN`) are named at their call sites.

use tidb_expr::column::Column;
use tidb_expr::expression::{CorrelatedColumn, Expression};
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{extract_columns_from_expressions, extract_cor_columns};

use crate::logical::BaseLogicalPlan;
use crate::stats_info::StatsInfo;

/// Go `cost.SelectionFactor` (`pkg/planner/core/cost/cost.go`): the fraction
/// of rows a filter with no better estimate is assumed to keep.
pub const SELECTION_FACTOR: f64 = 0.8;

/// Go `logicalop.LogicalSelection` (`logical_selection.go:38`).
#[derive(Clone, Debug, Default)]
pub struct LogicalSelection {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `Conditions`: the CNF conjuncts this operator applies.
    ///
    /// The source comment: the `WHERE`/`ON` condition is parsed as one
    /// expression, then split into a list of `AND` conditions.
    pub conditions: Vec<Expression>,
}

impl LogicalSelection {
    /// Go `LogicalSelection.Init(ctx, qbOffset)` (`logical_selection.go:48`),
    /// whose plan-codec type is `plancodec.TypeSel`.
    #[must_use]
    pub fn new(base: BaseLogicalPlan, conditions: Vec<Expression>) -> Self {
        Self { base, conditions }
    }

    /// Go `plancodec.TypeSel`.
    pub const TYPE: &'static str = "Selection";

    /// Go `LogicalSelection.ExtractCorrelatedCols()`
    /// (`logical_selection.go:253`): every correlated column under every
    /// condition, appended in order and NOT deduplicated.
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        let mut cor_cols = Vec::with_capacity(self.conditions.len());
        for condition in &self.conditions {
            cor_cols.extend(extract_cor_columns(condition));
        }
        cor_cols
    }

    /// Go `LogicalSelection.PullUpConstantPredicates()`
    /// (`logical_selection.go:212`): the conditions that are a comparison
    /// against a constant, which are the ones constant propagation may lift.
    ///
    /// Go's test is `expression.ValidCompareConstantPredicate(evalCtx, pred)`:
    /// a binary comparison with exactly one column and one strict constant.
    /// That predicate is dependency-closed and reproduced by
    /// [`is_valid_compare_constant_predicate`].
    #[must_use]
    pub fn pull_up_constant_predicates(&self) -> Vec<Expression> {
        self.conditions
            .iter()
            .filter(|candidate| is_valid_compare_constant_predicate(candidate))
            .cloned()
            .collect()
    }

    /// Go `splitSetGetVarFunc(filters)` (`logical_selection.go:328`): the
    /// conditions that may cross an operator boundary, and those that may not.
    ///
    /// A `GET_VAR`/`SET_VAR` call is order-sensitive, so it is pinned in place.
    #[must_use]
    pub fn split_set_get_var_func(filters: &[Expression]) -> (Vec<Expression>, Vec<Expression>) {
        let mut can_be_pushed = Vec::with_capacity(filters.len());
        let mut cannot_be_pushed = Vec::with_capacity(filters.len());
        for expr in filters {
            if tidb_expr::evaluator::has_get_set_var_func(expr) {
                cannot_be_pushed.push(expr.clone());
            } else {
                can_be_pushed.push(expr.clone());
            }
        }
        (can_be_pushed, cannot_be_pushed)
    }

    /// Go `LogicalSelection.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_selection.go:127`): the column set the child must still
    /// produce, which is the parent's plus every column this filter reads.
    ///
    /// The recursion into `children[0]` belongs to the enum-level driver; see
    /// [`crate::logical::LogicalPlan::prune_columns`].
    #[must_use]
    pub fn child_used_cols(&self, parent_used_cols: &[Column]) -> Vec<Column> {
        let mut used = parent_used_cols.to_vec();
        used.extend(extract_columns_from_expressions(&self.conditions, None));
        used
    }

    /// Go `LogicalSelection.BuildKeyInfo(selfSchema, childSchema)`
    /// (`logical_selection.go:141`): a filter is at most one row when every
    /// column of some child key is pinned to a constant by an `=` condition.
    ///
    /// Go delegates the final test to `ruleutil.CheckMaxOneRowCond(eqCols,
    /// childSchema[0])`, which asks whether the equal-constant column set
    /// contains a whole `PKOrUK`. That is `Schema::is_unique`, so the body is
    /// dependency-closed and ported whole.
    pub fn build_key_info(&mut self, child_schema: &[Schema]) {
        if self.base.max_one_row() {
            return;
        }
        let Some(child) = child_schema.first() else {
            return;
        };
        let eq_cols = self.equal_constant_columns();
        self.base.set_max_one_row(child.is_unique(true, &eq_cols));
    }

    /// The columns this filter equates to a constant or a correlated column,
    /// which is the `eqCols` set Go builds inside `BuildKeyInfo`.
    #[must_use]
    pub fn equal_constant_columns(&self) -> Vec<Column> {
        let mut eq_cols = Vec::new();
        for condition in &self.conditions {
            let Expression::ScalarFunction(function) = condition else {
                continue;
            };
            if function.func_name.lowercase() != "eq" {
                continue;
            }
            let args = function.get_args();
            if args.len() != 2 {
                continue;
            }
            for (i, arg) in args.iter().enumerate() {
                let Expression::Column(column) = arg else {
                    continue;
                };
                let other = &args[1 - i];
                if matches!(
                    other,
                    Expression::Constant(_) | Expression::CorrelatedColumn(_)
                ) {
                    eq_cols.push(column.clone());
                }
                // Go `break`s at the FIRST column argument, so a `col = col`
                // condition contributes nothing.
                break;
            }
        }
        eq_cols
    }

    /// Go `LogicalSelection.DeriveStats(childStats, _, _, reloads)`
    /// (`logical_selection.go:227`): the child's profile scaled by
    /// [`SELECTION_FACTOR`], with the group NDVs dropped.
    ///
    /// # Narrowing
    ///
    /// Go's `StatsInfo.Scale(sessionVars, factor)` consults
    /// `sessionVars.GetOptimizerFactor` before multiplying; there is no session
    /// here, so the constant factor is applied directly. `GroupNDVs` is not a
    /// field of this port's [`StatsInfo`], so "set to nil" is vacuous.
    pub fn derive_stats(
        &mut self,
        child_stats: &[StatsInfo],
        reloads: &[bool],
    ) -> Option<(StatsInfo, bool)> {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return Some((existing.clone(), false));
            }
        }
        let child = child_stats.first()?;
        let scaled = StatsInfo::new(
            child.row_count() * SELECTION_FACTOR,
            child
                .col_ndvs()
                .iter()
                .map(|(id, ndv)| (*id, ndv * SELECTION_FACTOR)),
        );
        self.base.base.set_stats(Some(scaled.clone()));
        Some((scaled, true))
    }
}

/// Go `expression.ValidCompareConstantPredicate(evalCtx, candidate)`
/// (`pkg/expression/util.go`): a binary comparison between exactly one column
/// and one strict constant.
#[must_use]
pub fn is_valid_compare_constant_predicate(candidate: &Expression) -> bool {
    let Expression::ScalarFunction(function) = candidate else {
        return false;
    };
    if !matches!(
        function.func_name.lowercase(),
        "lt" | "le" | "gt" | "ge" | "eq" | "ne"
    ) {
        return false;
    }
    let args = function.get_args();
    if args.len() != 2 {
        return false;
    }
    let column_count = args
        .iter()
        .filter(|arg| matches!(arg, Expression::Column(_)))
        .count();
    let constant_count = args
        .iter()
        .filter(|arg| matches!(arg, Expression::Constant(_)))
        .count();
    column_count == 1 && constant_count == 1
}

impl LogicalSelection {
    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            conditions: self.conditions.clone(),
        }
    }
}
