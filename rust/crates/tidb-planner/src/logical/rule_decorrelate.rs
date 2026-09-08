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

//! Go `pkg/planner/core/rule_decorrelate.go`, Go rule #5
//! (`DecorrelateSolver`).
//!
//! The rule walks the logical tree top-down and rewrites a `LogicalApply`
//! into a `LogicalJoin` whenever the correlated inner plan can be lifted out.
//! The arms implemented here, in Go's order:
//!
//! * an uncorrelated apply becomes the embedded join directly;
//! * `NoDecorrelate` and every condition-carrying case fall through to the
//!   children walk;
//! * an inner `Selection` has its conditions decorrelated and attached as
//!   join conditions;
//! * an inner `MaxOneRow` over a max-one-row child is peeled;
//! * an inner `Sort` is dropped (it cannot change the subquery's result);
//! * an inner `Limit` is peeled for the semi/anti join family when the apply
//!   carries no conditions and the offset is zero.
//!
//! The aggregation pull-up and projection arms are documented narrowings; see
//! [`DecorrelateSolver::optimize`].

use std::collections::BTreeSet;

use tidb_expr::expr_util::substitute::SubstituteOptions;
use tidb_expr::simple_expr::extract_columns;

use crate::find_best_task::LogicalJoinType;

use super::rule::{LogicalOptRule, RuleContext};
use super::{LogicalPlan, PlanError};

/// Go `DecorrelateSolver`.
pub struct DecorrelateSolver;

impl DecorrelateSolver {
    /// Go `(*DecorrelateSolver).optimize` (`rule_decorrelate.go:253`).
    fn optimize(
        ctx: &RuleContext<'_>,
        mut plan: LogicalPlan,
        group_by_column: &mut BTreeSet<i64>,
    ) -> Result<LogicalPlan, PlanError> {
        if let LogicalPlan::Aggregation(aggregation) = &plan {
            for item in &aggregation.group_by_items {
                for column in extract_columns(item) {
                    group_by_column.insert(column.unique_id);
                }
            }
        }

        if let LogicalPlan::Apply(mut apply) = plan {
            let outer_schema = apply
                .base()
                .children()
                .first()
                .and_then(LogicalPlan::schema)
                .cloned()
                .unwrap_or_default();
            let inner_schema = apply
                .base()
                .children()
                .get(1)
                .and_then(LogicalPlan::schema)
                .cloned()
                .unwrap_or_default();
            // Go `coreusage.ExtractCorColumnsBySchema4LogicalPlan(innerPlan,
            // outerSchema)`: the INNER TREE walk, which resolves the columns
            // this apply's own outer side supplies. `LogicalApply::
            // extract_correlated_cols` is the different question
            // `PruneColumns` asks (which columns reach FURTHER out).
            let mut children = apply.base_mut().take_children().into_iter();
            let outer = children.next();
            let mut inner = children.next();
            match (outer, inner.as_mut()) {
                (Some(outer), Some(inner_plan)) => {
                    apply.cor_cols =
                        super::super::expression_rewriter::extract_cor_columns_by_schema_4_logical_plan(
                            inner_plan,
                            &outer_schema,
                        );
                    let inner = inner.expect("the inner child was just matched");
                    apply.base_mut().set_children(vec![outer, inner]);
                }
                (outer, _) => {
                    let mut restored = Vec::new();
                    if let Some(outer) = outer {
                        restored.push(outer);
                    }
                    if let Some(inner) = inner {
                        restored.push(inner);
                    }
                    apply.base_mut().set_children(restored);
                }
            }
            if apply.cor_cols.is_empty() {
                // Go: "If the inner plan is non-correlated, the apply will be
                // simplified to join."
                //
                // Narrowing: the left-outer-semi family carries a 0/1 marker
                // column that `JoinExec` emits after the OUTER child's columns.
                // The Rust pruner leaves the outer child's unused columns in
                // place, so the converted join's schema and the executor's
                // emission disagree (Go inserts the pruning projection during
                // `LogicalJoin.PruneColumns`). Keep those as Apply until that
                // projection alignment is ported; the `Semi`/`AntiSemi` family
                // has no marker and converts.
                if !matches!(
                    apply.join.join_type,
                    LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi
                ) {
                    // Go assigns `p = join` and falls through to `NoOptimize`,
                    // so the CONVERTED join's children are still visited.
                    return Self::optimize_children(
                        ctx,
                        LogicalPlan::Join(apply.join),
                        group_by_column,
                    );
                }
            }
            if apply.no_decorrelate {
                return Self::optimize_children(ctx, LogicalPlan::Apply(apply), group_by_column);
            }
            let opts = SubstituteOptions::new(ctx.builder);
            let inner = apply.base().children()[1].clone();
            match inner {
                LogicalPlan::Selection(mut selection) => {
                    let conditions: Vec<_> = selection
                        .conditions
                        .iter()
                        .map(|condition| condition.decorrelate(Some(&outer_schema)))
                        .collect();
                    apply
                        .join
                        .attach_on_conds(&conditions, &outer_schema, &inner_schema, &opts);
                    let Some(child) = selection.base.take_children().into_iter().next() else {
                        return Err(PlanError::internal("selection has no child"));
                    };
                    let outer = apply
                        .base_mut()
                        .take_children()
                        .into_iter()
                        .next()
                        .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
                    apply.base_mut().set_children(vec![outer, child]);
                    return Self::optimize(ctx, LogicalPlan::Apply(apply), group_by_column);
                }
                LogicalPlan::MaxOneRow(max_one_row) => {
                    let max_one_row_child = max_one_row
                        .base
                        .children()
                        .first()
                        .filter(|child| child.max_one_row())
                        .cloned();
                    if let Some(child) = max_one_row_child {
                        let outer = apply
                            .base_mut()
                            .take_children()
                            .into_iter()
                            .next()
                            .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
                        apply.base_mut().set_children(vec![outer, child]);
                        return Self::optimize(ctx, LogicalPlan::Apply(apply), group_by_column);
                    }
                }
                LogicalPlan::Sort(mut sort) => {
                    let Some(child) = sort.base.take_children().into_iter().next() else {
                        return Err(PlanError::internal("sort has no child"));
                    };
                    let outer = apply
                        .base_mut()
                        .take_children()
                        .into_iter()
                        .next()
                        .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
                    apply.base_mut().set_children(vec![outer, child]);
                    return Self::optimize(ctx, LogicalPlan::Apply(apply), group_by_column);
                }
                LogicalPlan::Limit(mut limit) => {
                    let semi_family = matches!(
                        apply.join.join_type,
                        LogicalJoinType::Semi
                            | LogicalJoinType::LeftOuterSemi
                            | LogicalJoinType::AntiSemi
                            | LogicalJoinType::AntiLeftOuterSemi
                    );
                    let has_conditions = !apply.join.equal_conditions.is_empty()
                        || !apply.join.left_conditions.is_empty()
                        || !apply.join.right_conditions.is_empty()
                        || !apply.join.other_conditions.is_empty();
                    if semi_family && !has_conditions && limit.offset == 0 {
                        let Some(child) = limit.base.take_children().into_iter().next() else {
                            return Err(PlanError::internal("limit has no child"));
                        };
                        let outer = apply
                            .base_mut()
                            .take_children()
                            .into_iter()
                            .next()
                            .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
                        apply.base_mut().set_children(vec![outer, child]);
                        return Self::optimize(ctx, LogicalPlan::Apply(apply), group_by_column);
                    }
                }
                _ => {}
            }
            return Self::optimize_children(ctx, LogicalPlan::Apply(apply), group_by_column);
        }

        Self::optimize_children(ctx, plan, group_by_column)
    }

    /// Go's `NoOptimize` tail: recurse into every child.
    fn optimize_children(
        ctx: &RuleContext<'_>,
        mut plan: LogicalPlan,
        group_by_column: &mut BTreeSet<i64>,
    ) -> Result<LogicalPlan, PlanError> {
        // CTE's logical optimization is independent.
        if matches!(plan, LogicalPlan::CTE(_)) {
            return Ok(plan);
        }
        let children = plan.base_mut().take_children();
        let mut new_children = Vec::with_capacity(children.len());
        for child in children {
            new_children.push(Self::optimize(ctx, child, group_by_column)?);
        }
        plan.set_children(new_children);
        Ok(plan)
    }
}

impl LogicalOptRule for DecorrelateSolver {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let recovery = plan.clone();
        Self::optimize(ctx, plan, &mut BTreeSet::new())
            .map(|plan| (plan, false))
            .map_err(|error| (recovery, error))
    }

    fn name(&self) -> &'static str {
        "decorrelate"
    }
}
