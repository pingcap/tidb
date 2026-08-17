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

//! Go `PushDownSequenceSolver` (`pkg/planner/core/rule_push_down_sequence.go`),
//! Go rule #30.
//!
//! Go's body is one recursive function, `recursiveOptimize(pushedSequence,
//! lp)`, which carries a `*logicalop.LogicalSequence` DOWN the tree and plants
//! it as deep as it can: through a `DataSource` or `LogicalCTE`, through any
//! unary operator, and no further than a multi-child or childless one. A
//! nested `LogicalSequence` is MERGED into the carried one — its CTE producers
//! are appended after the carried producers, which preserves Go's ordering
//! contract that a producer may only reference producers before it (see
//! [`super::sequence::LogicalSequence`]).
//!
//! # The carried sequence is a Vec, not a node
//!
//! Go carries a real `LogicalSequence` whose LAST child is a placeholder that
//! every terminal overwrites with `SetChild(ChildLen()-1, ...)`. Carrying a
//! half-built node in a by-value IR would mean inventing a placeholder
//! `LogicalPlan` to occupy that slot, so [`PushedSequence`] carries the CTE
//! producers alone and materialises the `LogicalSequence` at the moment Go
//! writes that last slot. The node Go builds and the node built here have the
//! same children in the same order; only the intermediate placeholder is gone.
//!
//! # Why the multi-child case stops descending
//!
//! Go's `default` branch with `len(lp.Children()) != 1` attaches the sequence
//! above `lp` and returns WITHOUT recursing into `lp`. That is preserved
//! exactly — [`super::fold::Descend::Stop`] is that refusal — even though
//! recursing would find more sequences to push. Widening it would push
//! sequences Go leaves in place.

use crate::base_arms;
use crate::plan_base::PlanError;

use super::fold::{fold_owned, Descend, OwnedRewrite};
use super::rule::{LogicalOptRule, RuleContext};
use super::sequence::LogicalSequence;
use super::{BaseLogicalPlan, LogicalPlan};

/// The `pushedSequence` Go threads through `recursiveOptimize`, as its CTE
/// producers plus the base its `Init` allocated.
#[derive(Debug, Default)]
pub struct PushedSequence {
    /// The base `logicalop.LogicalSequence{}.Init(lp.SCtx(),
    /// lp.QueryBlockOffset())` produced.
    base: BaseLogicalPlan,
    /// `pushedSequence.Children()[:ChildLen()-1]`, the CTE producers.
    ctes: Vec<LogicalPlan>,
}

impl PushedSequence {
    /// Go's `logicalop.LogicalSequence{}.Init(...)` with the producers of an
    /// already-carried sequence in front of `ctes`.
    fn init(
        ctx: &RuleContext<'_>,
        query_block_offset: i32,
        carried: Option<Self>,
        ctes: Vec<LogicalPlan>,
    ) -> Self {
        let mut all = carried.map(|carried| carried.ctes).unwrap_or_default();
        all.extend(ctes);
        Self {
            base: BaseLogicalPlan::new(ctx.allocator, LogicalSequence::TYPE, query_block_offset),
            ctes: all,
        }
    }

    /// Go `pushedSequence.SetChild(pushedSequence.ChildLen()-1, main)`
    /// followed by `return pushedSequence`.
    fn emit(self, main_query: LogicalPlan) -> LogicalPlan {
        let mut children = self.ctes;
        children.push(main_query);
        let mut sequence = LogicalPlan::Sequence(LogicalSequence::new(self.base));
        sequence.set_children(children);
        sequence
    }
}

/// What a node's `descend` left for its `ascend`; see [`super::rewrite`]'s
/// stash discipline.
enum Pending {
    /// Go's "return lp": the node keeps whatever its children became.
    Keep,
    /// Go's `LogicalSequence` arm, whose return value is the recursion's, not
    /// the sequence node's — the node disappears into its main query.
    Collapse,
    /// Go's `pushedSequence.SetChild(ChildLen()-1, ...)`; the node becomes the
    /// sequence's main query.
    ///
    /// Boxed because a [`PushedSequence`] carries a whole
    /// [`BaseLogicalPlan`] and the other two variants carry nothing.
    Emit(Box<PushedSequence>),
}

struct PushDownSequence<'a, 'ctx> {
    ctx: &'a RuleContext<'ctx>,
    stash: Vec<Pending>,
}

impl OwnedRewrite for PushDownSequence<'_, '_> {
    type Down = Option<PushedSequence>;
    type Up = ();

    fn descend(
        &mut self,
        node: &mut LogicalPlan,
        pushed: Self::Down,
    ) -> Descend<Self::Down, Self::Up> {
        let child_count = node.children().len();
        let query_block_offset = node.base().base.query_block_offset();

        // Go: `_, ok := lp.(*LogicalSequence); if !ok && pushedSequence == nil`
        // — the plain top-down walk that has not met a sequence yet.
        if pushed.is_none() && !matches!(node, LogicalPlan::Sequence(_)) {
            self.stash.push(Pending::Keep);
            return Descend::Children((0..child_count).map(|_| None).collect());
        }

        match node {
            // Go's `case *logicalop.LogicalSequence`, both halves: with no
            // carried sequence a fresh one is built from this node's children,
            // and with one carried the two producer lists are concatenated.
            // Either way the recursion continues into the MAIN QUERY alone and
            // this node's own identity is discarded.
            LogicalPlan::Sequence(_) => {
                let mut children = node.base_mut().take_children();
                let Some(main_query) = children.pop() else {
                    // Go indexes `Children()[ChildLen()-1]` and would panic on
                    // a childless sequence. Nothing can be pushed into a node
                    // with no main query, so the carried sequence lands here.
                    self.stash.push(match pushed {
                        Some(pushed) => Pending::Emit(Box::new(pushed)),
                        None => Pending::Keep,
                    });
                    return Descend::Children(Vec::new());
                };
                let merged = PushedSequence::init(self.ctx, query_block_offset, pushed, children);
                node.set_children(vec![main_query]);
                self.stash.push(Pending::Collapse);
                Descend::Children(vec![Some(merged)])
            }
            // Go's `case *logicalop.DataSource, *logicalop.LogicalCTE`: the
            // sequence is planted directly above the leaf, and the leaf itself
            // is still walked with NO carried sequence
            // (`recursiveOptimize(nil, lp)`).
            LogicalPlan::DataSource(_) | LogicalPlan::CTE(_) => {
                self.stash.push(match pushed {
                    Some(pushed) => Pending::Emit(Box::new(pushed)),
                    None => Pending::Keep,
                });
                Descend::Children((0..child_count).map(|_| None).collect())
            }
            // Go's `default`. A unary operator lets the sequence through; a
            // multi-child or childless one takes it above itself and STOPS.
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
                CTETable,
                MaxOneRow,
                Lock,
                UnionScan,
                TiKVSingleGather,
                TableScan,
                IndexScan,
                TableDual,
                Expand,
                MemTable,
                Show,
                ShowDDLJobs,
                Todo,
            ] => {
                let Some(pushed) = pushed else {
                    // Unreachable: the `pushed.is_none()` early return above
                    // already answered for every non-sequence node.
                    self.stash.push(Pending::Keep);
                    return Descend::Children((0..child_count).map(|_| None).collect());
                };
                if child_count == 1 {
                    self.stash.push(Pending::Keep);
                    Descend::Children(vec![Some(pushed)])
                } else {
                    let taken =
                        std::mem::replace(node, LogicalPlan::Sequence(LogicalSequence::default()));
                    *node = pushed.emit(taken);
                    Descend::Stop(())
                }
            }
        }
    }

    fn ascend(&mut self, mut node: LogicalPlan, _child_ups: Vec<Self::Up>) -> (LogicalPlan, ()) {
        match self
            .stash
            .pop()
            .unwrap_or_else(|| unreachable!("every descend that recurses stashes exactly once"))
        {
            Pending::Keep => (node, ()),
            Pending::Collapse => {
                let main_query = node.base_mut().take_children().pop().unwrap_or(node);
                (main_query, ())
            }
            Pending::Emit(pushed) => (pushed.emit(node), ()),
        }
    }
}

/// Go `(*PushDownSequenceSolver).recursiveOptimize(nil, lp)`
/// (`rule_push_down_sequence.go:38`), as one [`fold_owned`].
#[must_use]
pub fn push_down_sequence(ctx: &RuleContext<'_>, plan: LogicalPlan) -> LogicalPlan {
    let mut rewrite = PushDownSequence {
        ctx,
        stash: Vec::new(),
    };
    fold_owned(&mut rewrite, plan, None).0
}

/// Go `PushDownSequenceSolver` (`rule_push_down_sequence.go:25`), Go rule #30.
///
/// The toy-tree twin of this traversal is [`crate::push_down_sequence`], which
/// is KEPT — `difftests/planner-tests` consumes it from outside this crate.
#[derive(Debug)]
pub struct PushDownSequenceSolver;

impl LogicalOptRule for PushDownSequenceSolver {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        // Go hard-codes `planChanged := false`.
        Ok((push_down_sequence(ctx, plan), false))
    }

    fn name(&self) -> &'static str {
        "push_down_sequence"
    }
}
