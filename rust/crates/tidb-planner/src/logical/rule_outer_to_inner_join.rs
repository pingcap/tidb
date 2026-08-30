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

//! Go `pkg/planner/core/rule_outer_to_inner_join.go` and the three logical
//! operator overrides of `ConvertOuterToInnerJoin`.

use tidb_expr::expr_util::substitute::SubstituteOptions;
use tidb_expr::expression::{is_null_rejected, Expression};

use crate::find_best_task::LogicalJoinType;

use super::rule::{LogicalOptRule, RuleContext};
use super::{LogicalJoin, LogicalPlan, PlanError};

/// Go `ConvertOuterToInnerJoin`.
pub struct ConvertOuterToInnerJoin;

fn combined_conditions(join: &LogicalJoin, predicates: &[Expression]) -> Vec<Expression> {
    let mut combined = Vec::with_capacity(
        join.left_conditions.len()
            + join.right_conditions.len()
            + join.equal_conditions.len()
            + join.other_conditions.len()
            + predicates.len(),
    );
    combined.extend(join.left_conditions.iter().cloned());
    combined.extend(join.right_conditions.iter().cloned());
    combined.extend(
        join.equal_conditions
            .iter()
            .cloned()
            .map(Expression::ScalarFunction),
    );
    combined.extend(join.other_conditions.iter().cloned());
    combined.extend(predicates.iter().cloned());
    combined
}

fn convert_join(
    context: &RuleContext<'_>,
    mut join: LogicalJoin,
    predicates: &[Expression],
) -> LogicalJoin {
    let mut children = join.base.take_children();
    if children.len() != 2 {
        join.base.set_children(children);
        return join;
    }

    let mut outer = children.pop().expect("two join children");
    let mut inner = children.pop().expect("two join children");
    let switch_children = join.join_type == LogicalJoinType::LeftOuter;
    if switch_children {
        std::mem::swap(&mut inner, &mut outer);
    }

    if matches!(
        join.join_type,
        LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter
    ) {
        let inner_ids: Vec<i64> = inner
            .schema()
            .into_iter()
            .flat_map(|schema| schema.columns.iter().map(|column| column.unique_id))
            .collect();
        if predicates
            .iter()
            .any(|predicate| is_null_rejected(&inner_ids, predicate))
        {
            join.join_type = LogicalJoinType::Inner;
        }
    }

    let combined = combined_conditions(&join, predicates);
    match join.join_type {
        LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter => {
            inner = convert(context, inner, &combined);
            outer = convert(context, outer, predicates);
        }
        LogicalJoinType::Inner | LogicalJoinType::Semi => {
            inner = convert(context, inner, &combined);
            outer = convert(context, outer, &combined);
        }
        LogicalJoinType::AntiSemi => {
            inner = convert(context, inner, predicates);
            outer = convert(context, outer, &combined);
        }
        _ => {
            inner = convert(context, inner, predicates);
            outer = convert(context, outer, predicates);
        }
    }

    if switch_children {
        join.base.set_children(vec![outer, inner]);
    } else {
        join.base.set_children(vec![inner, outer]);
    }
    join
}

fn convert(
    context: &RuleContext<'_>,
    mut plan: LogicalPlan,
    predicates: &[Expression],
) -> LogicalPlan {
    match plan {
        LogicalPlan::Selection(mut selection) => {
            let mut combined = predicates.to_vec();
            combined.extend(selection.conditions.iter().cloned());
            let children = selection
                .base
                .take_children()
                .into_iter()
                .map(|child| convert(context, child, &combined))
                .collect();
            selection.base.set_children(children);
            LogicalPlan::Selection(selection)
        }
        LogicalPlan::Projection(mut projection) => {
            let schema = projection.base.base.schema().cloned().unwrap_or_default();
            let options = SubstituteOptions::new(context.builder);
            let (pushable, _) = projection.break_down_predicates(predicates, &schema, &options);
            let children = projection
                .base
                .take_children()
                .into_iter()
                .map(|child| convert(context, child, &pushable))
                .collect();
            projection.base.set_children(children);
            LogicalPlan::Projection(projection)
        }
        LogicalPlan::Join(join) => LogicalPlan::Join(convert_join(context, join, predicates)),
        // Go promotes `LogicalJoin.ConvertOuterToInnerJoin` through the
        // embedded join on `LogicalApply`. That method returns its
        // `*LogicalJoin` receiver, so this rule has the same concrete-plan
        // transition when the rule flag reaches an Apply.
        LogicalPlan::Apply(apply) => {
            LogicalPlan::Join(convert_join(context, apply.join, predicates))
        }
        _ => {
            let children = plan
                .base_mut()
                .take_children()
                .into_iter()
                .map(|child| convert(context, child, predicates))
                .collect();
            plan.base_mut().set_children(children);
            plan
        }
    }
}

impl LogicalOptRule for ConvertOuterToInnerJoin {
    fn optimize(
        &self,
        context: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        Ok((convert(context, plan, &[]), false))
    }

    fn name(&self) -> &'static str {
        "convert_outer_to_inner_joins"
    }
}
