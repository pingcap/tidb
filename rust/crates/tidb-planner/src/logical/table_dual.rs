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

//! Go `pkg/planner/core/operator/logicalop/logical_table_dual.go`:
//! `LogicalTableDual`, the leaf that emits 0 or 1 rows and no table access.
//!
//! SEED of `pkg/planner/core`. The operator was previously a SKELETON in
//! [`crate::logical`] carrying only `RowCount`; this file gives it its member
//! bodies.
//!
//! The crate's `logical_table_dual` identity leaf is KEPT rather than merged:
//! `difftests/planner-tests/tests/logical_table_dual.rs` consumes its
//! `LogicalTableDualIdentity`/`ColumnIdentity` from OUTSIDE this crate, so
//! deleting it would break a gate this batch does not own.
//!
//! Go's own note on the schema, kept because it is a correctness caveat rather
//! than an implementation detail: a dual is often built with NO schema at all
//! (`buildTableDual()`), which means "outputting 0/1 row with zero column".
//!
//! # Narrowings, by name
//!
//! * `HashCode` (`logical_table_dual.go:61`) begins with
//!   `plancodec.TypeStringToPhysicalID(p.TP())`; `pkg/util/plancodec` is not
//!   transcreated, so [`LogicalTableDual::hash_code`] takes the physical id
//!   from the caller, exactly as [`crate::logical::LogicalLimit`] does.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::logical::{schema_producer, BaseLogicalPlan};
use crate::stats_info::StatsInfo;

/// Go `logicalop.LogicalTableDual` (`logical_table_dual.go:33`).
#[derive(Clone, Debug, Default)]
pub struct LogicalTableDual {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `RowCount`, which "could only be 0 or 1".
    pub row_count: usize,
}

impl LogicalTableDual {
    /// Go `plancodec.TypeDual`.
    pub const TYPE: &'static str = "TableDual";

    /// Go `LogicalTableDual.Init(ctx, offset)` (`logical_table_dual.go:41`).
    #[must_use]
    pub const fn new(base: BaseLogicalPlan, row_count: usize) -> Self {
        Self { base, row_count }
    }

    /// Go `LogicalTableDual.ExplainInfo()` (`logical_table_dual.go:49`).
    #[must_use]
    pub fn explain_info(&self) -> String {
        format!("rowcount:{}", self.row_count)
    }

    /// Go `LogicalTableDual.HashCode()` (`logical_table_dual.go:61`): the plan
    /// TYPE, the query-block offset and the row count — deliberately NOT the
    /// plan id, so two duals with the same row count hash alike.
    ///
    /// See this module's header for `physical_id`.
    #[must_use]
    pub fn hash_code(&self, physical_id: u32) -> Vec<u8> {
        let mut result = Vec::with_capacity(12);
        result.extend_from_slice(&physical_id.to_be_bytes());
        result.extend_from_slice(&(self.base.base.query_block_offset() as u32).to_be_bytes());
        result.extend_from_slice(&(self.row_count as u32).to_be_bytes());
        result
    }

    /// Go `LogicalTableDual.PredicatePushDown(predicates)`
    /// (`logical_table_dual.go:71`): a dual has no child, so every predicate
    /// stays above it.
    #[must_use]
    pub fn predicate_push_down(predicates: Vec<Expression>) -> Vec<Expression> {
        predicates
    }

    /// Go `LogicalTableDual.PruneColumns(parentUsedCols)`
    /// (`logical_table_dual.go:76`): drop every schema column the parent does
    /// not read, walking backwards so the indices stay valid.
    ///
    /// Unlike most operators, Go does NOT delete the matching output names
    /// here; the names are left as they were. Returns the removed positions in
    /// descending order so a caller can see what went.
    pub fn prune_columns(schema: &mut Schema, parent_used_cols: &[Column]) -> Vec<usize> {
        let used = schema_producer::get_used_list(parent_used_cols, schema);
        let mut pruned = Vec::new();
        for i in (0..used.len()).rev() {
            if !used[i] {
                pruned.push(i);
                schema.columns.remove(i);
            }
        }
        pruned
    }

    /// Go `LogicalTableDual.BuildKeyInfo(selfSchema, childSchema)`
    /// (`logical_table_dual.go:89`): the base body, then `maxOneRow` when the
    /// dual emits exactly one row.
    ///
    /// The base half is the child-key propagation the driver already runs; only
    /// the `RowCount == 1` decision is this operator's own, and it is what lets
    /// a scalar subquery over a dual skip its `MaxOneRow` check.
    pub const fn sets_max_one_row(&self) -> bool {
        self.row_count == 1
    }

    /// Go `LogicalTableDual.DeriveStats(_, selfSchema, _, reloads)`
    /// (`logical_table_dual.go:109`): the row count IS `RowCount`, and every
    /// schema column has that same NDV — with 0 or 1 rows there is nothing else
    /// it could be.
    pub fn derive_stats(&mut self, self_schema: &Schema, reloads: &[bool]) -> (StatsInfo, bool) {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return (existing.clone(), false);
            }
        }
        let row_count = self.row_count as f64;
        let profile = StatsInfo::new(
            row_count,
            self_schema
                .columns
                .iter()
                .map(|col| (col.unique_id, row_count)),
        );
        self.base.base.set_stats(Some(profile.clone()));
        (profile, true)
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            row_count: self.row_count,
        }
    }
}
