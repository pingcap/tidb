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

//! Go `pkg/planner/core/operator/logicalop/logical_union_all.go`:
//! `LogicalUnionAll`, the N-child concatenation, and
//! `pkg/planner/core/operator/logicalop/logical_partition_union_all.go`:
//! `LogicalPartitionUnionAll`, the same operator over a partitioned table.
//!
//! SEED of `pkg/planner/core`. Both were [`crate::logical::TodoLogicalOp`]
//! before this batch.
//!
//! # Why one file and an embedded struct
//!
//! Go writes `LogicalPartitionUnionAll { LogicalUnionAll }` — a struct
//! EMBEDDING, with its own plan-codec type and two overrides
//! (`PruneColumns`, `PushDownTopN`) whose bodies are the parent's. That
//! embedding is reproduced literally by [`LogicalPartitionUnionAll::union_all`]
//! rather than by a copied struct, so the two can never drift; the enum still
//! carries them as SEPARATE variants, because the plan-codec type is what
//! `EXPLAIN` prints and a partition union is not a plain union.
//!
//! # Narrowings, by name
//!
//! * `PredicatePushDown` calls `logicalop.AddSelection(p, newChild, retCond,
//!   i)` per child; `AddSelection` is `logical_plans_misc.go` and belongs to
//!   the enum-level driver, which owns the children. The LOCAL contract —
//!   every predicate is offered to EVERY child and NOTHING survives to the
//!   parent — is [`LogicalUnionAll::predicate_push_down_local`].
//! * `ExtractFD` needs `pkg/planner/funcdep` (`fd.FindCommonEquivClasses`,
//!   `FDSet.MakeNotNull`), which is not transcreated; see the
//!   [`crate::logical::BaseLogicalPlan`] header.
//! * `PruneColumns`' repair step — inserting a `LogicalProjection` over a child
//!   whose schema grew wider than the union's — needs the child plans, so its
//!   TEST lands here as [`LogicalUnionAll::child_needs_pruning_projection`] and
//!   the insertion belongs to the driver.

use tidb_expr::column::Column;
use tidb_expr::schema::Schema;

use crate::logical::schema_producer;
use crate::logical::topn::LogicalTopN;
use crate::logical::BaseLogicalPlan;
use crate::plan_base::PossiblePropertiesInfo;
use crate::stats_info::StatsInfo;

/// Go `logicalop.LogicalUnionAll` (`logical_union_all.go:30`).
///
/// Go's body is the embedded `LogicalSchemaProducer` and nothing else; the
/// schema it produces lives on [`crate::plan_base::BasePlan`] here, so this
/// struct is its base alone. That is the operator, not a stub.
#[derive(Clone, Debug, Default)]
pub struct LogicalUnionAll {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
}

/// What [`LogicalUnionAll::prune_columns_local`] decided, in the order the
/// driver needs it.
#[derive(Clone, Debug)]
pub struct UnionAllPruning {
    /// The set every child must be pruned with. Go REPLACES the parent's set
    /// with the union's whole schema when the parent used nothing, because a
    /// union with no output column is not a plan.
    pub child_used_cols: Vec<Column>,
    /// The columns dropped from the union's own schema, in Go's back-to-front
    /// order.
    pub pruned_columns: Vec<Column>,
    /// Go's `hasBeenUsed`: whether the parent used at least one column. Only
    /// then does the repair projection step apply.
    pub has_been_used: bool,
}

impl LogicalUnionAll {
    /// Go `plancodec.TypeUnion`.
    pub const TYPE: &'static str = "Union";

    /// Go `LogicalUnionAll.Init(ctx, offset)` (`logical_union_all.go:35`).
    #[must_use]
    pub const fn new(base: BaseLogicalPlan) -> Self {
        Self { base }
    }

    /// Go `LogicalUnionAll.PredicatePushDown(predicates)`'s LOCAL contract
    /// (`logical_union_all.go:45`): every predicate is offered to EVERY child,
    /// each child keeps what it could not push under a `Selection`, and the
    /// union returns NOTHING to its parent.
    ///
    /// A union cannot hold a predicate itself, so `ret` is `nil` in Go: a
    /// condition that no child accepted is re-attached to that child, not
    /// bubbled up. See this module's header for `AddSelection`.
    #[must_use]
    pub const fn predicate_push_down_local() -> Vec<tidb_expr::expression::Expression> {
        Vec::new()
    }

    /// Go `LogicalUnionAll.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_union_all.go:60`).
    ///
    /// Two things happen that a single-child operator's pruning does not:
    /// * when the parent used NO column of the union, the parent's set is
    ///   replaced by the union's entire schema — a union must still produce a
    ///   row shape;
    /// * the surviving set is pushed to every child UNCHANGED, because each
    ///   child produces the same schema positionally.
    pub fn prune_columns_local(
        parent_used_cols: &[Column],
        schema: &mut Schema,
    ) -> UnionAllPruning {
        let mut used = schema_producer::get_used_list(parent_used_cols, schema);
        let has_been_used = used.iter().any(|one| *one);
        let child_used_cols = if has_been_used {
            parent_used_cols.to_vec()
        } else {
            used.iter_mut().for_each(|one| *one = true);
            schema.columns.clone()
        };
        let mut pruned_columns = Vec::new();
        for i in (0..used.len()).rev() {
            if !used[i] {
                pruned_columns.push(schema.columns.remove(i));
            }
        }
        UnionAllPruning {
            child_used_cols,
            pruned_columns,
            has_been_used,
        }
    }

    /// Go's repair test inside `PruneColumns` (`logical_union_all.go:136`):
    /// after pruning, a child may be WIDER than the union — Go names
    /// `(*LogicalAggregation).PruneColumns` as the operator that does this —
    /// and a `LogicalProjection` down to the union's schema has to be inserted
    /// above that child.
    ///
    /// Only applies when the parent actually used something; see
    /// [`UnionAllPruning::has_been_used`].
    #[must_use]
    pub const fn child_needs_pruning_projection(
        self_schema_len: usize,
        child_schema_len: usize,
    ) -> bool {
        self_schema_len < child_schema_len
    }

    /// Go `LogicalUnionAll.PushDownTopN(topN)`'s per-child TopN
    /// (`logical_union_all.go:159`): a copy that keeps `count + offset` rows
    /// and NO offset of its own.
    ///
    /// The offset is folded into the count because each branch must supply
    /// enough rows for the union's own offset to be applied ONCE, above.
    #[must_use]
    pub fn push_down_topn_for_child(topn: &LogicalTopN) -> LogicalTopN {
        let mut base = topn.base.shell();
        base.set_children(Vec::new());
        LogicalTopN {
            base,
            by_items: topn.by_items.clone(),
            partition_by: Vec::new(),
            offset: 0,
            count: topn.count + topn.offset,
            prefer_limit_to_cop: topn.prefer_limit_to_cop,
        }
    }

    /// Go `LogicalUnionAll.DeriveStats(childStats, selfSchema, _, reloads)`
    /// (`logical_union_all.go:187`): row counts ADD, and so do the per-column
    /// NDVs, over the union's OWN schema columns.
    ///
    /// Adding NDVs is an over-estimate whenever the branches overlap; that is
    /// Go's estimate, kept exactly.
    pub fn derive_stats(
        &mut self,
        child_stats: &[StatsInfo],
        self_schema: &Schema,
        reloads: &[bool],
    ) -> (StatsInfo, bool) {
        let reload = reloads.iter().any(|one| *one);
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return (existing.clone(), false);
            }
        }
        let mut row_count = 0.0;
        let mut ndvs: Vec<(i64, f64)> = self_schema
            .columns
            .iter()
            .map(|col| (col.unique_id, 0.0))
            .collect();
        for child in child_stats {
            row_count += child.row_count();
            for (id, ndv) in &mut ndvs {
                // Go indexes a missing key as 0, which `copied().unwrap_or`
                // reproduces without inserting.
                *ndv += child.col_ndvs().get(id).copied().unwrap_or(0.0);
            }
        }
        let stats = StatsInfo::new(row_count, ndvs);
        self.base.base.set_stats(Some(stats.clone()));
        (stats, true)
    }

    /// Go `LogicalUnionAll.PreparePossibleProperties(_, childrenProperties)`
    /// (`logical_union_all.go:206`): a union offers NO order, and is
    /// TiFlash-capable only if it has children and every one of them is.
    ///
    /// This is the base body minus the orders, which a union cannot preserve
    /// because it interleaves its branches.
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

/// Go `logicalop.LogicalPartitionUnionAll` (`logical_partition_union_all.go:25`).
///
/// Go's two overrides, `PruneColumns` (`:38`) and `PushDownTopN` (`:55`), are
/// character-for-character the parent's bodies plus the unwrap that keeps the
/// partition type; there is nothing here that is not
/// [`LogicalUnionAll`]'s, which is why it is reached through the embedded
/// field rather than restated.
#[derive(Clone, Debug, Default)]
pub struct LogicalPartitionUnionAll {
    /// Go's embedded `LogicalUnionAll`.
    pub union_all: LogicalUnionAll,
}

impl LogicalPartitionUnionAll {
    /// Go `plancodec.TypePartitionUnion`.
    pub const TYPE: &'static str = "PartitionUnion";

    /// Go `LogicalPartitionUnionAll.Init(ctx, offset)`
    /// (`logical_partition_union_all.go:30`).
    #[must_use]
    pub const fn new(base: BaseLogicalPlan) -> Self {
        Self {
            union_all: LogicalUnionAll::new(base),
        }
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            union_all: self.union_all.clone_shallow(),
        }
    }
}
