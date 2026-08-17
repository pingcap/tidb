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

//! Go `pkg/planner/core/operator/logicalop/logical_max_one_row.go`:
//! `LogicalMaxOneRow`, which "checks if a query returns no more than one row".
//!
//! SEED of `pkg/planner/core`. This operator was a
//! [`crate::logical::TodoLogicalOp`] before this batch, and it MERGES the
//! crate's former `logical_max_one_row` identity leaf — that leaf modelled the
//! generated `Hash64`/`Equals`, which for this operator is the PLAN ID and
//! nothing else, because Go's own comment says it "doesn't have any other
//! attribute to distinguish, use plan id inside". Go's difftest for the leaf
//! is the reason the leaf survives; see the batch report.

use tidb_datatype::FieldTypeFlags;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::logical::BaseLogicalPlan;
use crate::stats_info::StatsInfo;

/// Go `logicalop.LogicalMaxOneRow` (`logical_max_one_row.go:26`).
///
/// Its Go body is the embedded `BaseLogicalPlan` alone; this struct is that
/// base, which is the whole operator and not a stub.
#[derive(Clone, Debug, Default)]
pub struct LogicalMaxOneRow {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
}

impl LogicalMaxOneRow {
    /// Go `plancodec.TypeMaxOneRow`.
    pub const TYPE: &'static str = "MaxOneRow";

    /// Go `LogicalMaxOneRow.Init(ctx, offset)` (`logical_max_one_row.go:32`).
    #[must_use]
    pub const fn new(base: BaseLogicalPlan) -> Self {
        Self { base }
    }

    /// Go `LogicalMaxOneRow.Schema()` (`logical_max_one_row.go:41`): the
    /// child's schema with EVERY column made nullable.
    ///
    /// That is `util.ResetNotNullFlag(s, 0, s.Len())`, and the reason is the
    /// operator's whole purpose: when the child produces no row at all, this
    /// operator emits one row of `NULL`s, so no column of it can be `NOT NULL`.
    #[must_use]
    pub fn schema(child_schema: &Schema) -> Schema {
        let mut schema = child_schema.clone();
        for column in &mut schema.columns {
            if let Some(ret_type) = column.ret_type.as_mut() {
                ret_type.del_flags(FieldTypeFlags::NOT_NULL);
            }
        }
        schema
    }

    /// Go `LogicalMaxOneRow.PredicatePushDown(predicates)`
    /// (`logical_max_one_row.go:55`): "MaxOneRow forbids any condition to push
    /// down."
    ///
    /// Go still recurses into the child with an EMPTY predicate set; the
    /// predicates themselves come straight back to the parent.
    #[must_use]
    pub fn predicate_push_down(predicates: Vec<Expression>) -> Vec<Expression> {
        predicates
    }

    /// Go `LogicalMaxOneRow.DeriveStats(_, selfSchema, _, reloads)`
    /// (`logical_max_one_row.go:73`): [`singleton_stats`], regardless of what
    /// the child estimated.
    pub fn derive_stats(&mut self, self_schema: &Schema, reloads: &[bool]) -> (StatsInfo, bool) {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return (existing.clone(), false);
            }
        }
        let stats = singleton_stats(self_schema);
        self.base.base.set_stats(Some(stats.clone()));
        (stats, true)
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

/// Go `getSingletonStats(schema)` (`logical_max_one_row.go:117`), whose comment
/// is "Exists and MaxOneRow produce at most one row, so we set the RowCount of
/// stats one": one row, and every column at NDV 1.
#[must_use]
pub fn singleton_stats(schema: &Schema) -> StatsInfo {
    StatsInfo::new(
        1.0,
        schema.columns.iter().map(|col| (col.unique_id, 1.0_f64)),
    )
}
