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

//! Go `EmptySelectionEliminator`
//! (`pkg/planner/core/rule_eliminate_empty_selection.go`), Go rule #32.
//!
//! A `LogicalSelection` whose condition list is empty filters nothing —
//! predicate pushdown can empty one by moving every condition down — so Go
//! splices it out.
//!
//! # Go eliminates CHILDREN, and that is narrower than "every selection"
//!
//! `recursivePlan(p)` iterates `p.Children()` and only a CHILD is tested. Two
//! consequences are reproduced here exactly rather than repaired:
//!
//! * the ROOT is never eliminated, even when it is an empty selection;
//! * when a child IS a selection, Go recurses into `sel.Children()[0]`, not
//!   into `sel`, so the selection's own child is never TESTED. A chain of two
//!   empty selections therefore loses the upper one and keeps the lower one.
//!
//! Both fall out of one carried bit, [`OwnedRewrite::Down`] here: whether the
//! node was reached as a tested child. The root starts with `false`, a tested
//! selection passes `false` to its child, and every other node passes `true`.
//! The bit's `Default` is `false` — "not tested" — so the padding path of
//! [`fold_owned`] keeps a selection rather than dropping one, which is the
//! direction that keeps a filter in the plan.

use crate::base_arms;
use crate::plan_base::PlanError;

use super::fold::{fold_owned, Descend, OwnedRewrite};
use super::rule::{LogicalOptRule, RuleContext};
use super::LogicalPlan;

/// Go's `child.(*logicalop.LogicalSelection)` with `len(sel.Conditions) == 0`.
fn is_empty_selection(node: &LogicalPlan) -> bool {
    matches!(node, LogicalPlan::Selection(sel) if sel.conditions.is_empty())
}

struct EmptySelection {
    /// Whether the node currently being ascended was a TESTED child.
    stash: Vec<bool>,
}

impl OwnedRewrite for EmptySelection {
    /// Go: whether this node was reached as an element of some
    /// `p.Children()` loop.
    type Down = bool;
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, tested: bool) -> Descend<bool, ()> {
        self.stash.push(tested);
        // A tested selection is the one node whose child Go reaches by
        // `recursivePlan(sel.Children()[0])`, skipping the test on it.
        let children_are_tested = !(tested && matches!(node, LogicalPlan::Selection(_)));
        match node {
            base_arms![
                Selection,
                Projection,
                Join,
                Apply,
                Aggregation,
                Sort,
                Limit,
                TopN,
                UnionAll,
                PartitionUnionAll,
                Window,
                CTE,
                CTETable,
                MaxOneRow,
                Lock,
                Sequence,
                UnionScan,
                TiKVSingleGather,
                TableScan,
                IndexScan,
                DataSource,
                TableDual,
                Expand,
                MemTable,
                Show,
                ShowDDLJobs,
                Todo,
            ] => Descend::Children(
                (0..node.children().len())
                    .map(|_| children_are_tested)
                    .collect(),
            ),
        }
    }

    fn ascend(&mut self, mut node: LogicalPlan, _child_ups: Vec<()>) -> (LogicalPlan, ()) {
        let tested = self
            .stash
            .pop()
            .unwrap_or_else(|| unreachable!("every descend stashes exactly once"));
        if !tested || !is_empty_selection(&node) {
            return (node, ());
        }
        // Go's `p.SetChild(idx, sel.Children()[0])`. A childless selection
        // would make Go panic on that index; it is left in place instead.
        match node.base_mut().take_children().pop() {
            Some(child) => (child, ()),
            None => (node, ()),
        }
    }
}

/// Go `(*EmptySelectionEliminator).recursivePlan(p)`
/// (`rule_eliminate_empty_selection.go:33`), as one [`fold_owned`].
#[must_use]
pub fn eliminate_empty_selection(plan: LogicalPlan) -> LogicalPlan {
    let mut rewrite = EmptySelection { stash: Vec::new() };
    fold_owned(&mut rewrite, plan, false).0
}

/// Go `EmptySelectionEliminator`
/// (`rule_eliminate_empty_selection.go:25`), Go rule #32.
///
/// [`crate::eliminate_empty_selection`] is the crate's trait-shaped seed of the
/// same rule and is KEPT — `difftests/planner-tests` consumes it from outside
/// this crate.
#[derive(Debug)]
pub struct EmptySelectionEliminator;

impl LogicalOptRule for EmptySelectionEliminator {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        _ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        // Go hard-codes `planChanged := false`.
        Ok((eliminate_empty_selection(plan), false))
    }

    fn name(&self) -> &'static str {
        "eliminate_empty_selection"
    }
}
