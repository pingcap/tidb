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

//! Go `EliminateUnionAllDualItem`
//! (`pkg/planner/core/rule_eliminate_unionall_dual_item.go`), Go rule #31.
//!
//! A `UNION ALL` branch that can produce no row at all contributes nothing, so
//! Go's `unionAllEliminateDualItem` drops it. Two shapes count as such a
//! branch: a bare zero-row `LogicalTableDual`, and a `LogicalProjection` whose
//! FIRST child is one.
//!
//! # `planChanged` is narrower than "the plan changed"
//!
//! Go's returned flag is the OR of the CHILDREN's flags plus the
//! empty-union-becomes-dual case only. Dropping a branch from a union that
//! still has branches left does NOT set it — see `flag := false` after the
//! union-all block, which starts fresh below the drop. That is preserved
//! exactly here; the toy-tree twin [`crate::eliminate_unionall_dual_item`]
//! reports the drop as a change, which is a divergence in that module, and it
//! is KEPT as it stands because `difftests/planner-tests` consumes it from
//! outside this crate.
//!
//! # Only `LogicalUnionAll`, never `LogicalPartitionUnionAll`
//!
//! Go's `p.(*logicalop.LogicalUnionAll)` is a CONCRETE type assertion, and Go
//! embedding is not subtyping: a `*LogicalPartitionUnionAll` fails it even
//! though it embeds a `LogicalUnionAll`. So [`super::LogicalPlan::UnionAll`]
//! alone is matched.

use crate::plan_base::PlanError;

use super::fold::{fold_owned, Descend, OwnedRewrite};
use super::rule::{LogicalOptRule, RuleContext};
use super::table_dual::LogicalTableDual;
use super::{BaseLogicalPlan, LogicalPlan};

/// Go's `child.(*logicalop.LogicalTableDual)` with `dual.RowCount == 0`.
fn is_zero_row_dual(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::TableDual(dual) if dual.row_count == 0)
}

/// Go's "case 2: indirect projection + table dual item", which reads
/// `proj.Children()[0]` and would panic on a childless projection.
fn is_projection_over_zero_row_dual(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Projection(_))
        && plan.children().first().is_some_and(is_zero_row_dual)
}

struct EliminateUnionAllDual<'a, 'ctx> {
    ctx: &'a RuleContext<'ctx>,
}

impl OwnedRewrite for EliminateUnionAllDual<'_, '_> {
    type Down = ();
    /// Go's `planChanged`.
    type Up = bool;

    fn descend(&mut self, node: &mut LogicalPlan, (): ()) -> Descend<(), bool> {
        // Go filters the union's branches BEFORE recursing into what is left,
        // so the filtering belongs to `descend`.
        if matches!(node, LogicalPlan::UnionAll(_)) {
            let children = node.base_mut().take_children();
            let mut retained = Vec::with_capacity(children.len());
            for child in children {
                if is_zero_row_dual(&child) || is_projection_over_zero_row_dual(&child) {
                    child.dismantle();
                } else {
                    retained.push(child);
                }
            }
            if retained.is_empty() {
                // Go: `dual := LogicalTableDual{}.Init(unionAll.SCtx(), 0)`
                // — query block offset ZERO, not the union's — and
                // `dual.SetSchema(unionAll.Schema())` so the parent's column
                // references still resolve.
                let schema = node.schema().cloned();
                let mut base = BaseLogicalPlan::new(self.ctx.allocator, LogicalTableDual::TYPE, 0);
                base.base.set_schema(schema);
                *node = LogicalPlan::TableDual(LogicalTableDual::new(base, 0));
                return Descend::Stop(true);
            }
            node.set_children(retained);
        }
        Descend::Children((0..node.children().len()).map(|_| ()).collect())
    }

    fn ascend(&mut self, node: LogicalPlan, child_ups: Vec<bool>) -> (LogicalPlan, bool) {
        // Go: `flag := false; ...; flag = flag || changed`.
        let changed = child_ups.into_iter().any(|changed| changed);
        (node, changed)
    }
}

/// Go `unionAllEliminateDualItem(p)`
/// (`rule_eliminate_unionall_dual_item.go:40`), as one [`fold_owned`].
#[must_use]
pub fn union_all_eliminate_dual_item(
    ctx: &RuleContext<'_>,
    plan: LogicalPlan,
) -> (LogicalPlan, bool) {
    let mut rewrite = EliminateUnionAllDual { ctx };
    fold_owned(&mut rewrite, plan, ())
}

/// Go `EliminateUnionAllDualItem`
/// (`rule_eliminate_unionall_dual_item.go:25`), Go rule #31.
#[derive(Debug)]
pub struct EliminateUnionAllDualItem;

impl LogicalOptRule for EliminateUnionAllDualItem {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        Ok(union_all_eliminate_dual_item(ctx, plan))
    }

    fn name(&self) -> &'static str {
        // Go's own `Name()`, which is NOT the string the flag constant is
        // spelled with.
        "union_all_eliminate_dual_item"
    }
}
