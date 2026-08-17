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

//! Go `pkg/planner/core/operator/logicalop/logical_limit.go`: `LogicalLimit`,
//! the `LIMIT offset, count` operator.
//!
//! SEED of `pkg/planner/core`. The operator was previously a SKELETON in
//! [`crate::logical`] carrying only `Offset`/`Count`; this file gives it its
//! real member bodies and the two fields Go carries that the skeleton dropped
//! (`PartitionBy`, `PreferLimitToCop`, `IsPartial`).
//!
//! # Narrowings, by name
//!
//! * `HashCode` (`logical_limit.go:65`) encodes
//!   `plancodec.TypeStringToPhysicalID(p.TP())` in its first four bytes. That
//!   table is `pkg/util/plancodec`, which is not transcreated, so
//!   [`LogicalLimit::hash_code`] takes the physical id from the caller rather
//!   than inventing one; the other three fields are exact.
//! * `ExplainInfo`'s `PartitionBy` prefix is
//!   `property.ExplainPartitionBy(evalCtx, ...)`, which renders each column
//!   through `StringWithCtx`. This crate's [`SortItem`] carries a column's
//!   `UniqueID` and nothing else, so the prefix is reported by COUNT.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::logical::schema_producer;
use crate::logical::topn::LogicalTopN;
use crate::logical::BaseLogicalPlan;
use crate::physical_property::SortItem;
use crate::stats_info::StatsInfo;

/// Go `logicalop.LogicalLimit` (`logical_limit.go:29`).
#[derive(Clone, Debug, Default)]
pub struct LogicalLimit {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `PartitionBy []property.SortItem`, used by the enhanced TopN
    /// optimisation.
    pub partition_by: Vec<SortItem>,
    /// Go `Offset`.
    pub offset: u64,
    /// Go `Count`.
    pub count: u64,
    /// Go `PreferLimitToCop`.
    pub prefer_limit_to_cop: bool,
    /// Go `IsPartial`.
    pub is_partial: bool,
}

impl LogicalLimit {
    /// Go `plancodec.TypeLimit`.
    pub const TYPE: &'static str = "Limit";

    /// Go `LogicalLimit.Init(ctx, offset)` (`logical_limit.go:40`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, offset: u64, count: u64) -> Self {
        Self {
            base,
            partition_by: Vec::new(),
            offset,
            count,
            prefer_limit_to_cop: false,
            is_partial: false,
        }
    }

    /// Go `LogicalLimit.GetPartitionBy()` (`logical_limit.go:168`).
    #[must_use]
    pub fn get_partition_by(&self) -> &[SortItem] {
        &self.partition_by
    }

    /// Go `LogicalLimit.HashCode()` (`logical_limit.go:65`): 24 bytes of
    /// big-endian `plan type | query block offset | Offset | Count`.
    ///
    /// This OVERRIDES [`BaseLogicalPlan::hash_code`], which encodes the plan
    /// id: two limits that differ only in id must hash alike, because they are
    /// the same operator. See this module's header for `physical_id`.
    #[must_use]
    pub fn hash_code(&self, physical_id: u32) -> Vec<u8> {
        let mut result = Vec::with_capacity(24);
        result.extend_from_slice(&physical_id.to_be_bytes());
        result.extend_from_slice(&(self.base.base.query_block_offset() as u32).to_be_bytes());
        result.extend_from_slice(&self.offset.to_be_bytes());
        result.extend_from_slice(&self.count.to_be_bytes());
        result
    }

    /// Go `LogicalLimit.PredicatePushDown(predicates)`
    /// (`logical_limit.go:76`): "Limit forbids any condition to push down."
    ///
    /// Go still calls the base body with `nil`, which recurses into the child
    /// with an empty predicate set; the predicates themselves come straight
    /// back to the parent, which is what this returns.
    #[must_use]
    pub fn predicate_push_down(predicates: Vec<Expression>) -> Vec<Expression> {
        predicates
    }

    /// Go `LogicalLimit.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_limit.go:83`): the child is pruned with the parent's set
    /// UNCHANGED — a limit reads no column of its own — then this operator's
    /// schema is rebuilt from the pruned child and inline-projected down to
    /// what the parent asked for.
    ///
    /// Go's `p.SetSchema(nil)` is what makes the rebuild happen: the embedded
    /// `LogicalSchemaProducer` re-derives from `children[0]` on the next
    /// `Schema()`. Here the child schema is supplied, which is the same answer
    /// without the lazy write.
    pub fn prune_columns_local(
        &mut self,
        parent_used_cols: &[Column],
        pruned_child_schema: &Schema,
    ) -> Schema {
        let mut schema = pruned_child_schema.clone();
        schema_producer::inline_projection(&mut schema, parent_used_cols);
        self.base.base.set_schema(Some(schema.clone()));
        schema
    }

    /// Go `LogicalLimit.BuildKeyInfo(selfSchema, childSchema)`
    /// (`logical_limit.go:98`): the schema producer's key propagation, then
    /// `LIMIT 1` is at most one row.
    pub fn build_key_info(&mut self, self_schema: &mut Schema, child_schema: &[Schema]) {
        schema_producer::propagate_child_keys(self_schema, child_schema);
        if self.count == 1 {
            self.base.set_max_one_row(true);
        }
    }

    /// Go `LogicalLimit.DeriveStats(childStats, _, _, reloads)`
    /// (`logical_limit.go:129`): `property.DeriveLimitStats(childStats[0],
    /// float64(p.Count))`, which caps the row count and every column NDV at
    /// the limit.
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

    /// Go `LogicalLimit.convertToTopN()` (`logical_limit.go:172`): the TopN
    /// this limit becomes on the way down, with NO `ByItems` — so the result
    /// answers `true` to [`LogicalTopN::is_limit`].
    ///
    /// Go allocates a fresh plan through `Init(p.SCtx(), p.QueryBlockOffset())`;
    /// the base shell here carries the same query-block offset, and the caller
    /// re-allocates the id when it has an allocator. Note that Go does NOT
    /// carry `PartitionBy` across, which is why it is absent below.
    #[must_use]
    pub fn convert_to_topn(&self) -> LogicalTopN {
        let mut base = self.base.shell();
        base.base.set_tp(LogicalTopN::TYPE);
        LogicalTopN {
            base,
            by_items: Vec::new(),
            partition_by: Vec::new(),
            offset: self.offset,
            count: self.count,
            prefer_limit_to_cop: self.prefer_limit_to_cop,
        }
    }

    /// Go `LogicalLimit.ExplainInfo()` (`logical_limit.go:48`).
    ///
    /// The no-partition form is exact. See this module's header for why the
    /// `PartitionBy` prefix is a count.
    #[must_use]
    pub fn explain_info(&self) -> String {
        if self.partition_by.is_empty() {
            return format!("offset:{}, count:{}", self.offset, self.count);
        }
        format!(
            "partition by {} cols, offset:{}, count:{}",
            self.partition_by.len(),
            self.offset,
            self.count
        )
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            partition_by: self.partition_by.clone(),
            offset: self.offset,
            count: self.count,
            prefer_limit_to_cop: self.prefer_limit_to_cop,
            is_partial: self.is_partial,
        }
    }
}
