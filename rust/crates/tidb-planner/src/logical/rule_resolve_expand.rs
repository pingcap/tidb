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

//! Go `ResolveExpand` (`pkg/planner/core/rule_resolve_grouping_expand.go`),
//! Go rule #34 — the LAST rule in `optRuleList`.
//!
//! Go's body is `genExpand(p)`: a post-order walk that calls
//! `expand.GenLevelProjections()` on every `LogicalExpand` it finds. The
//! levelled projection list is what a `ROLLUP` expands each input row into, and
//! Go's file header states why it is built here and not at build time — the
//! Expand projects its child's whole output plus the grouping columns, so
//! materialising it earlier would block column pruning underneath.
//!
//! The generation itself is [`super::expand::LogicalExpand::gen_level_projections`];
//! all this rule adds is Go's traversal.

use crate::plan_base::PlanError;

use super::fold::{fold_owned, Descend, OwnedRewrite};
use super::rule::{LogicalOptRule, RuleContext};
use super::LogicalPlan;

struct GenExpand;

impl OwnedRewrite for GenExpand {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): ()) -> Descend<(), ()> {
        Descend::Children((0..node.children().len()).map(|_| ()).collect())
    }

    fn ascend(&mut self, mut node: LogicalPlan, _child_ups: Vec<()>) -> (LogicalPlan, ()) {
        // Go: `if expand, ok := p.(*logicalop.LogicalExpand); ok {
        // expand.GenLevelProjections() }`, AFTER the children are done.
        if matches!(node, LogicalPlan::Expand(_)) {
            // Go reads `expand.Schema()` off the operator itself; the schema is
            // copied out first because the generator takes `&mut self`.
            let schema = node.schema().cloned().unwrap_or_default();
            if let LogicalPlan::Expand(expand) = &mut node {
                expand.gen_level_projections(&schema);
            }
        }
        (node, ())
    }
}

/// Go `genExpand(p)` (`rule_resolve_grouping_expand.go:90`), as one
/// [`fold_owned`].
///
/// Go's signature returns an `error` that only ever comes back out of its own
/// recursion — nothing in the body produces one — so this is infallible.
#[must_use]
pub fn gen_expand(plan: LogicalPlan) -> LogicalPlan {
    fold_owned(&mut GenExpand, plan, ()).0
}

/// Go `ResolveExpand` (`rule_resolve_grouping_expand.go:54`), Go rule #34.
///
/// [`crate::resolve_grouping_expand`] carries the same traversal over a
/// toy tree and is KEPT — `difftests/planner-tests` consumes it from outside
/// this crate.
#[derive(Debug)]
pub struct ResolveExpand;

impl LogicalOptRule for ResolveExpand {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        _ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        // Go hard-codes `planChanged := false`.
        Ok((gen_expand(plan), false))
    }

    fn name(&self) -> &'static str {
        "resolve_expand"
    }
}
