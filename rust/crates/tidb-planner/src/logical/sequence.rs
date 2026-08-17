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

//! Go `pkg/planner/core/operator/logicalop/logical_sequence.go`:
//! `LogicalSequence`, which "is used to mark the CTE producer in the main query
//! tree".
//!
//! SEED of `pkg/planner/core`. This operator was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! # The child order is the contract
//!
//! Go's comment, kept because every body below depends on it: the LAST child is
//! the main query and the earlier ones are CTE producers, and a producer may
//! reference only the producers BEFORE it. With four children `c0..c3`, `c3` is
//! the main query, `c2` may reference `c1` and `c0`, and `c0` may reference
//! neither. That ordering is what lets the CTE optimisations be done at all.

use tidb_expr::schema::Schema;

use crate::logical::BaseLogicalPlan;
use crate::plan_base::PossiblePropertiesInfo;
use crate::stats_info::StatsInfo;

/// Go `logicalop.LogicalSequence` (`logical_sequence.go:34`).
///
/// Go's own note: it "doesn't have any other attribute to distinguish, use plan
/// id inside", so the base IS the operator.
#[derive(Clone, Debug, Default)]
pub struct LogicalSequence {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
}

impl LogicalSequence {
    /// Go `plancodec.TypeSequence`.
    pub const TYPE: &'static str = "Sequence";

    /// Go `LogicalSequence.Init(ctx, offset)` (`logical_sequence.go:39`).
    #[must_use]
    pub const fn new(base: BaseLogicalPlan) -> Self {
        Self { base }
    }

    /// Go `LogicalSequence.Schema()` (`logical_sequence.go:47`): the LAST
    /// child's, which is the main query's.
    ///
    /// Go indexes `Children()[ChildLen()-1]` and panics on a childless
    /// sequence; `None` is that refusal without the panic.
    #[must_use]
    pub fn schema(children_schemas: &[Schema]) -> Option<&Schema> {
        children_schemas.last()
    }

    /// Go `LogicalSequence.PredicatePushDown(predicates)`
    /// (`logical_sequence.go:57`): the predicates go to the LAST child only.
    ///
    /// Go's comment: "Currently, we only maintain the main query tree." A
    /// producer is optimised as its own plan, so a filter cannot cross into it.
    /// Returns the index of the child that receives them.
    #[must_use]
    pub const fn predicate_push_down_child(child_len: usize) -> Option<usize> {
        child_len.checked_sub(1)
    }

    /// Go `LogicalSequence.PruneColumns(parentUsedCols)`
    /// (`logical_sequence.go:65`): the same single-child rule as
    /// [`Self::predicate_push_down_child`], and the parent's set reaches the
    /// main query UNCHANGED — a sequence reads no column of its own.
    #[must_use]
    pub const fn prune_columns_child(child_len: usize) -> Option<usize> {
        child_len.checked_sub(1)
    }

    /// Go `LogicalSequence.DeriveStats(childStats, _, _, reloads)`
    /// (`logical_sequence.go:82`): the LAST child's profile, adopted whole.
    ///
    /// Note that this operator does NOT take the memoisation escape every other
    /// operator takes; Go always re-adopts and reports "reloaded" from the LAST
    /// reload flag alone, because "sequence only care about the last child stats
    /// is changed or not".
    pub fn derive_stats(
        &mut self,
        child_stats: &[StatsInfo],
        reloads: &[bool],
    ) -> Option<(StatsInfo, bool)> {
        let stats = child_stats.last()?.clone();
        self.base.base.set_stats(Some(stats.clone()));
        let reload = reloads.last().copied().unwrap_or(true);
        Some((stats, reload))
    }

    /// Go `LogicalSequence.PreparePossibleProperties(_, childrenProperties)`
    /// (`logical_sequence.go:95`): no order survives, and TiFlash needs every
    /// child — the base body's rule.
    pub fn prepare_possible_properties(
        &mut self,
        children_properties: &[Option<PossiblePropertiesInfo>],
    ) -> PossiblePropertiesInfo {
        let mut has_tiflash = !children_properties.is_empty();
        for child in children_properties.iter().flatten() {
            has_tiflash = has_tiflash && child.has_tiflash;
        }
        self.base.set_has_tiflash(has_tiflash);
        PossiblePropertiesInfo {
            orders: Vec::new(),
            has_tiflash,
        }
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
        }
    }
}
