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

//! Go `pkg/planner/core/rule/rule_constant_propagation.go`.

use tidb_expr::expression::Expression;

use super::rule::{LogicalOptRule, RuleContext};
use super::{BaseLogicalPlan, LogicalPlan, LogicalSelection};
use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PlanError;

enum Task {
    Enter(LogicalPlan),
    Exit(LogicalPlan, usize, Vec<Expression>),
}

fn candidate_predicates(plan: &LogicalPlan) -> Vec<Expression> {
    let LogicalPlan::Join(join) = plan else {
        return Vec::new();
    };
    let children = join.base.children();
    match join.join_type {
        LogicalJoinType::LeftOuter => children
            .first()
            .map_or_else(Vec::new, LogicalPlan::pull_up_constant_predicates),
        LogicalJoinType::RightOuter => children
            .get(1)
            .map_or_else(Vec::new, LogicalPlan::pull_up_constant_predicates),
        LogicalJoinType::Inner => {
            let mut predicates = children
                .first()
                .map_or_else(Vec::new, LogicalPlan::pull_up_constant_predicates);
            if let Some(right) = children.get(1) {
                predicates.extend(right.pull_up_constant_predicates());
            }
            predicates
        }
        LogicalJoinType::Semi
        | LogicalJoinType::AntiSemi
        | LogicalJoinType::LeftOuterSemi
        | LogicalJoinType::AntiLeftOuterSemi => Vec::new(),
    }
}

/// Go's preorder constant-propagation walk, implemented with an explicit
/// stack so deep logical plans do not consume the host stack.
#[must_use]
pub fn constant_propagation(ctx: &RuleContext<'_>, root: LogicalPlan) -> LogicalPlan {
    let mut work = vec![Task::Enter(root)];
    let mut done = Vec::new();
    while let Some(task) = work.pop() {
        match task {
            Task::Enter(mut plan) => {
                // Go computes this before recursively optimizing children.
                let predicates = candidate_predicates(&plan);
                let children = plan.base_mut().take_children();
                let child_count = children.len();
                work.push(Task::Exit(plan, child_count, predicates));
                for child in children.into_iter().rev() {
                    work.push(Task::Enter(child));
                }
            }
            Task::Exit(mut plan, child_count, predicates) => {
                let children = done.split_off(done.len() - child_count);
                plan.set_children(children);
                if predicates.is_empty() {
                    done.push(plan);
                    continue;
                }
                let query_block_offset = plan.base().base.query_block_offset();
                let mut selection = LogicalPlan::Selection(LogicalSelection::new(
                    BaseLogicalPlan::new(ctx.allocator, LogicalSelection::TYPE, query_block_offset),
                    predicates,
                ));
                selection.set_children(vec![plan]);
                done.push(selection);
            }
        }
    }
    done.pop()
        .unwrap_or_else(|| unreachable!("the constant-propagation walk returns one root"))
}

/// Go `ConstantPropagationSolver` at logical rule position 10.
#[derive(Debug)]
pub struct ConstantPropagationSolver;

impl LogicalOptRule for ConstantPropagationSolver {
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        // Go's `planChanged` is hard-coded false.
        Ok((constant_propagation(ctx, plan), false))
    }

    fn name(&self) -> &'static str {
        "constant_propagation"
    }
}
