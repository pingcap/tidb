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

//! Physical TableDual planning metadata from
//! `pkg/planner/core/operator/physicalop/physical_table_dual.go`.
//!
//! The Go operator carries output names/schema, context, statistics, and root
//! task objects. This leaf preserves the dependency-closed Init metadata,
//! ExplainInfo rendering, and the source property gates; schema/name storage,
//! physical task wiring, memory accounting, and the mock-datasource fallback
//! remain external boundaries.

/// The source plan-codec type assigned by `PhysicalTableDual.Init`.
pub const PLAN_TYPE: &str = "Dual";

/// Minimal initialized physical TableDual metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalTableDualPlan {
    row_count: i64,
    query_block_offset: i32,
}

impl PhysicalTableDualPlan {
    /// Initializes source-shaped TableDual metadata.
    #[must_use]
    pub const fn init(row_count: i64, query_block_offset: i32) -> Self {
        Self {
            row_count,
            query_block_offset,
        }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the source TableDual row count.
    #[must_use]
    pub const fn row_count(self) -> i64 {
        self.row_count
    }

    /// Returns the query-block offset assigned by `Init`.
    #[must_use]
    pub const fn query_block_offset(self) -> i32 {
        self.query_block_offset
    }

    /// Returns the source `ExplainInfo` shape: `rows:<RowCount>`.
    #[must_use]
    pub fn explain_info(self) -> String {
        format!("rows:{}", self.row_count)
    }
}

/// Applies `findBestTask4LogicalTableDual`'s pure property gates.
#[must_use]
pub const fn find_best_task(
    row_count: i64,
    query_block_offset: i32,
    has_index_join_prop: bool,
    has_sort_items: bool,
) -> Option<PhysicalTableDualPlan> {
    // Even enforce hints cannot make TableDual satisfy an index-join property.
    if has_index_join_prop {
        return None;
    }
    // A zero/one-row dual can satisfy any required order; larger results cannot.
    if has_sort_items && row_count > 1 {
        return None;
    }
    Some(PhysicalTableDualPlan::init(row_count, query_block_offset))
}

#[cfg(test)]
mod tests {
    use super::{find_best_task, PhysicalTableDualPlan, PLAN_TYPE};

    #[test]
    fn init_preserves_dual_kind_offset_and_row_count() {
        let plan = PhysicalTableDualPlan::init(-2, 7);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Dual");
        assert_eq!(plan.query_block_offset(), 7);
        assert_eq!(plan.row_count(), -2);
    }

    #[test]
    fn explain_info_uses_rows_prefix_and_signed_count() {
        assert_eq!(PhysicalTableDualPlan::init(0, 0).explain_info(), "rows:0");
        assert_eq!(PhysicalTableDualPlan::init(-1, 0).explain_info(), "rows:-1");
    }

    #[test]
    fn index_join_property_always_rejects() {
        for row_count in [0, 1, 2] {
            assert!(find_best_task(row_count, 0, true, false).is_none());
            assert!(find_best_task(row_count, 0, true, true).is_none());
        }
    }

    #[test]
    fn sort_property_is_allowed_for_at_most_one_row() {
        assert_eq!(
            find_best_task(0, 3, false, true),
            Some(PhysicalTableDualPlan::init(0, 3))
        );
        assert_eq!(
            find_best_task(1, 4, false, true),
            Some(PhysicalTableDualPlan::init(1, 4))
        );
        assert!(find_best_task(2, 5, false, true).is_none());
    }
}
