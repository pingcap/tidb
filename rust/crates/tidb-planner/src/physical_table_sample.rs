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

//! PhysicalTableSample initialization metadata from
//! `pkg/planner/core/operator/physicalop/physical_table_sample.go`.
//!
//! The Go plan owns concrete schema, table, sampler, context, and memory
//! objects. This leaf preserves the source plan type, pseudo row-count
//! initialization, query-block offset, physical table identity, and Desc flag
//! over dependency-closed scalar metadata; sampling and execution remain
//! explicit external boundaries.

/// The source plan-codec type assigned by `PhysicalTableSample.Init`.
pub const PLAN_TYPE: &str = "TableSample";

/// Minimal PhysicalTableSample initialization state.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PhysicalTableSamplePlan {
    physical_table_id: i64,
    desc: bool,
    query_block_offset: i32,
    row_count: f64,
}

impl PhysicalTableSamplePlan {
    /// Initializes source-shaped TableSample metadata.
    ///
    /// `PhysicalTableSample.Init` assigns the TableSample plan type and
    /// pseudo statistics with `RowCount: 1`; the table ID and Desc fields are
    /// retained from the plan value.
    #[must_use]
    pub const fn init(physical_table_id: i64, desc: bool, query_block_offset: i32) -> Self {
        Self {
            physical_table_id,
            desc,
            query_block_offset,
            row_count: 1.0,
        }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the source physical table identity.
    #[must_use]
    pub const fn physical_table_id(self) -> i64 {
        self.physical_table_id
    }

    /// Returns the source descending-scan flag.
    #[must_use]
    pub const fn desc(self) -> bool {
        self.desc
    }

    /// Returns the retained query-block offset.
    #[must_use]
    pub const fn query_block_offset(self) -> i32 {
        self.query_block_offset
    }

    /// Returns the pseudo row count assigned during initialization.
    #[must_use]
    pub const fn row_count(self) -> f64 {
        self.row_count
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalTableSamplePlan, PLAN_TYPE};

    #[test]
    fn init_sets_table_sample_type_and_pseudo_row_count() {
        let plan = PhysicalTableSamplePlan::init(11, false, 3);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "TableSample");
        assert_eq!(plan.row_count(), 1.0);
    }

    #[test]
    fn init_preserves_table_identity_direction_and_offset() {
        let plan = PhysicalTableSamplePlan::init(-7, true, -2);
        assert_eq!(plan.physical_table_id(), -7);
        assert!(plan.desc());
        assert_eq!(plan.query_block_offset(), -2);
    }

    #[test]
    fn ascending_and_descending_plans_share_source_stats() {
        let ascending = PhysicalTableSamplePlan::init(1, false, 0);
        let descending = PhysicalTableSamplePlan::init(1, true, 0);
        assert_eq!(ascending.row_count(), descending.row_count());
        assert_ne!(ascending.desc(), descending.desc());
    }
}
