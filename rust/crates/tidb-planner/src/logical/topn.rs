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

//! Go `pkg/planner/core/operator/logicalop/logical_top_n.go`: `LogicalTopN`,
//! the fused `ORDER BY ... LIMIT` operator.
//!
//! SEED of `pkg/planner/core`. `LogicalTopN` was a [`crate::logical::TodoLogicalOp`]
//! before this batch. It shares its `ByItems` handling with
//! [`crate::logical::sort`] and its limit arithmetic with
//! [`crate::logical::limit`]; neither is restated here.
//!
//! # Narrowings, by name
//!
//! * `ExplainInfo` needs `property.ExplainPartitionBy` and
//!   `util.ExplainByItems`, both of which render expressions through an
//!   `EvalContext`; see [`LogicalTopN::explain_info`].
//! * `ReplaceExprColumns` needs `ruleutil.ResolveExprAndReplace`, which is not
//!   transcreated.

use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::expression::CorrelatedColumn;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::extract_cor_columns;

use crate::logical::limit::LogicalLimit;
use crate::logical::sort::{get_possible_property_from_by_items, prune_by_items};
use crate::logical::{schema_producer, BaseLogicalPlan, LogicalPlan};
use crate::physical_property::SortItem;
use crate::plan_base::PossiblePropertiesInfo;
use crate::stats_info::StatsInfo;

/// Go `logicalop.LogicalTopN` (`logical_top_n.go:30`).
#[derive(Clone, Debug, Default)]
pub struct LogicalTopN {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `ByItems []*util.ByItems`.
    pub by_items: Vec<ByItems>,
    /// Go `PartitionBy`: the K-heap partitioning that
    /// `rule_derive_topn_from_window` installs.
    pub partition_by: Vec<SortItem>,
    /// Go `Offset`.
    pub offset: u64,
    /// Go `Count`.
    pub count: u64,
    /// Go `PreferLimitToCop`.
    pub prefer_limit_to_cop: bool,
}

impl LogicalTopN {
    /// Go `plancodec.TypeTopN`.
    pub const TYPE: &'static str = "TopN";

    /// Go `LogicalTopN.Init(ctx, offset)` (`logical_top_n.go:42`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, by_items: Vec<ByItems>, offset: u64, count: u64) -> Self {
        Self {
            base,
            by_items,
            partition_by: Vec::new(),
            offset,
            count,
            prefer_limit_to_cop: false,
        }
    }

    /// Go `LogicalTopN.GetPartitionBy()` (`logical_top_n.go:189`).
    #[must_use]
    pub fn get_partition_by(&self) -> &[SortItem] {
        &self.partition_by
    }

    /// Go `LogicalTopN.IsLimit()` (`logical_top_n.go:194`): a TopN with no
    /// order is a plain limit.
    #[must_use]
    pub const fn is_limit(&self) -> bool {
        self.by_items.is_empty()
    }

    /// Go `LogicalTopN.ExtractCorrelatedCols()` (`logical_top_n.go:160`).
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        let mut cor_cols = Vec::with_capacity(self.by_items.len());
        for item in &self.by_items {
            cor_cols.extend(extract_cor_columns(&item.expr));
        }
        cor_cols
    }

    /// Go `LogicalTopN.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_top_n.go:80`), which is [`crate::logical::LogicalSort`]'s
    /// `ByItems` pruning followed by [`LogicalLimit`]'s schema rebuild.
    ///
    /// Returns the column set the child must produce. Go's own comment on the
    /// rebuild: a TopN "may carry stale hidden sort columns or duplicate column
    /// slots after child pruning", so the schema is dropped and re-derived
    /// before the inline projection, which is fed the SNAPSHOT of the parent's
    /// set rather than the set the `ByItems` widened.
    pub fn prune_columns_local(&mut self, parent_used_cols: &[Column]) -> Vec<Column> {
        let (kept, from_items) = prune_by_items(&self.by_items);
        self.by_items = kept;
        let mut used = parent_used_cols.to_vec();
        used.extend(from_items);
        used
    }

    /// The second half of `LogicalTopN.PruneColumns` (`logical_top_n.go:97`),
    /// run once the driver has pruned the child; see
    /// [`LogicalLimit::prune_columns_local`], whose body this is.
    pub fn rebuild_schema_after_pruning(
        &mut self,
        snapshot_parent_used_cols: &[Column],
        pruned_child_schema: &Schema,
    ) -> Schema {
        let mut schema = pruned_child_schema.clone();
        schema_producer::inline_projection(&mut schema, snapshot_parent_used_cols);
        self.base.base.set_schema(Some(schema.clone()));
        schema
    }

    /// Go `LogicalTopN.BuildKeyInfo(selfSchema, childSchema)`
    /// (`logical_top_n.go:106`).
    pub fn build_key_info(&mut self, self_schema: &mut Schema, child_schema: &[Schema]) {
        schema_producer::propagate_child_keys(self_schema, child_schema);
        if self.count == 1 {
            self.base.set_max_one_row(true);
        }
    }

    /// Go `LogicalTopN.DeriveStats(childStats, _, _, reloads)`
    /// (`logical_top_n.go:126`): identical to [`LogicalLimit::derive_stats`].
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
        let stats = child_stats.first()?.derive_limit_stats(self.count as f64);
        self.base.base.set_stats(Some(stats.clone()));
        Some((stats, true))
    }

    /// Go `LogicalTopN.PreparePossibleProperties(_, infos...)`
    /// (`logical_top_n.go:141`): the same body as
    /// [`crate::logical::LogicalSort::prepare_possible_properties`] — a TopN
    /// establishes its own order and discards the child's.
    pub fn prepare_possible_properties(
        &mut self,
        child: Option<&PossiblePropertiesInfo>,
    ) -> PossiblePropertiesInfo {
        let has_tiflash = child.is_some_and(|info| info.has_tiflash);
        self.base.set_has_tiflash(has_tiflash);
        let prop_cols = get_possible_property_from_by_items(&self.by_items);
        PossiblePropertiesInfo {
            orders: if prop_cols.is_empty() {
                Vec::new()
            } else {
                vec![prop_cols]
            },
            has_tiflash,
        }
    }

    /// Go `LogicalTopN.AttachChild(p)` (`logical_top_n.go:200`): install `p`
    /// below this TopN, COLLAPSING the TopN where it can.
    ///
    /// Three outcomes, exactly Go's:
    /// * over a `LogicalTableDual`, the dual absorbs the window — its row count
    ///   becomes `min(rows - offset, count)`, or zero when the offset already
    ///   skipped past the end — and the TopN disappears;
    /// * with no `ByItems` this is a bare limit, so a [`LogicalLimit`] takes its
    ///   place, carrying `PartitionBy` across (which
    ///   [`LogicalLimit::convert_to_topn`] does NOT do in the other direction);
    /// * otherwise `p` simply becomes this TopN's child.
    ///
    /// Go's comment for why this exists rather than `SetChild`: "AttachChild
    /// will tracer the children change while SetChild doesn't."
    #[must_use]
    pub fn attach_child(self, child: LogicalPlan) -> LogicalPlan {
        if let LogicalPlan::TableDual(mut dual) = child {
            let num_dual_rows = dual.row_count as u64;
            dual.row_count = if num_dual_rows < self.offset {
                0
            } else {
                (num_dual_rows - self.offset).min(self.count) as usize
            };
            return LogicalPlan::TableDual(dual);
        }
        if self.is_limit() {
            let mut base = self.base.shell();
            base.base.set_tp(LogicalLimit::TYPE);
            let mut limit = LogicalLimit {
                base,
                partition_by: self.partition_by,
                offset: self.offset,
                count: self.count,
                prefer_limit_to_cop: self.prefer_limit_to_cop,
                is_partial: false,
            };
            limit.base.set_children(vec![child]);
            return LogicalPlan::Limit(limit);
        }
        let mut topn = self;
        topn.base.set_children(vec![child]);
        LogicalPlan::TopN(topn)
    }

    /// Go `LogicalTopN.ExplainInfo()` (`logical_top_n.go:50`).
    ///
    /// # Blocked
    ///
    /// Both `property.ExplainPartitionBy(evalCtx, ...)` and
    /// `util.ExplainByItems(evalCtx, ...)` render expressions through
    /// `StringWithCtx`. The `offset`/`count` suffix is exact; the two lists are
    /// reported by COUNT so neither is silently missing.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let mut buffer = String::new();
        if !self.partition_by.is_empty() {
            buffer.push_str(&format!("partition by {} cols", self.partition_by.len()));
            if !self.by_items.is_empty() {
                buffer.push_str(" order by ");
            }
        }
        if !self.by_items.is_empty() {
            buffer.push_str(&format!("{} by items", self.by_items.len()));
        }
        buffer.push_str(&format!(", offset:{}, count:{}", self.offset, self.count));
        buffer
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            by_items: self.by_items.clone(),
            partition_by: self.partition_by.clone(),
            offset: self.offset,
            count: self.count,
            prefer_limit_to_cop: self.prefer_limit_to_cop,
        }
    }
}
