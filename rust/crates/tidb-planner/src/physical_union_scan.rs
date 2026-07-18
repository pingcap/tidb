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

//! PhysicalUnionScan planning gate from
//! `pkg/planner/core/operator/physicalop/physical_union_scan.go`.
//!
//! The Go operator carries concrete physical properties, expressions, handle
//! columns, context, statistics, and task objects. This leaf preserves the
//! source MPP rejection, index-join-admission outcome, and initialization
//! metadata over normalized scalar inputs; property cloning, child/task
//! wiring, transaction-buffer reads, and executor behavior remain external.

/// The source plan-codec type assigned by `PhysicalUnionScan.Init`.
pub const PLAN_TYPE: &str = "UnionScan";

/// Initialization metadata retained by a planned UnionScan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalUnionScanPlan {
    query_block_offset: i32,
    condition_count: usize,
    handle_column_count: usize,
}

impl PhysicalUnionScanPlan {
    /// Creates source-shaped UnionScan initialization metadata.
    #[must_use]
    pub const fn init(
        query_block_offset: i32,
        condition_count: usize,
        handle_column_count: usize,
    ) -> Self {
        Self {
            query_block_offset,
            condition_count,
            handle_column_count,
        }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the query-block offset assigned by Init.
    #[must_use]
    pub const fn query_block_offset(self) -> i32 {
        self.query_block_offset
    }

    /// Returns the number of retained condition expressions.
    #[must_use]
    pub const fn condition_count(self) -> usize {
        self.condition_count
    }

    /// Returns the number of retained handle columns.
    #[must_use]
    pub const fn handle_column_count(self) -> usize {
        self.handle_column_count
    }
}

/// Outcome of `ExhaustPhysicalPlans4LogicalUnionScan`'s pure gates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnionScanExhaustion {
    /// MPP properties reject UnionScan; source returns handled=true/no plans.
    UnsupportedFlash,
    /// Child index-join admission cannot satisfy the property; no plans.
    IncompatibleIndexJoin,
    /// The source emits one physical UnionScan plan.
    Planned(PhysicalUnionScanPlan),
}

/// Applies the source MPP and index-join-admission gates.
#[must_use]
pub const fn exhaust_physical_union_scan(
    is_flash_prop: bool,
    index_join_admitted: bool,
    query_block_offset: i32,
    condition_count: usize,
    handle_column_count: usize,
) -> UnionScanExhaustion {
    if is_flash_prop {
        UnionScanExhaustion::UnsupportedFlash
    } else if !index_join_admitted {
        UnionScanExhaustion::IncompatibleIndexJoin
    } else {
        UnionScanExhaustion::Planned(PhysicalUnionScanPlan::init(
            query_block_offset,
            condition_count,
            handle_column_count,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        exhaust_physical_union_scan, PhysicalUnionScanPlan, UnionScanExhaustion, PLAN_TYPE,
    };

    #[test]
    fn flash_properties_reject_before_index_join_admission() {
        assert_eq!(
            exhaust_physical_union_scan(true, false, 0, 1, 1),
            UnionScanExhaustion::UnsupportedFlash
        );
    }

    #[test]
    fn incompatible_index_join_property_emits_no_plan() {
        assert_eq!(
            exhaust_physical_union_scan(false, false, 0, 1, 1),
            UnionScanExhaustion::IncompatibleIndexJoin
        );
    }

    #[test]
    fn admitted_properties_emit_source_metadata() {
        let outcome = exhaust_physical_union_scan(false, true, -3, 2, 4);
        assert_eq!(
            outcome,
            UnionScanExhaustion::Planned(PhysicalUnionScanPlan::init(-3, 2, 4))
        );
        let UnionScanExhaustion::Planned(plan) = outcome else {
            unreachable!();
        };
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "UnionScan");
        assert_eq!(plan.query_block_offset(), -3);
        assert_eq!(plan.condition_count(), 2);
        assert_eq!(plan.handle_column_count(), 4);
    }

    #[test]
    fn init_metadata_preserves_empty_condition_and_handle_lists() {
        let plan = PhysicalUnionScanPlan::init(7, 0, 0);
        assert_eq!(plan.condition_count(), 0);
        assert_eq!(plan.handle_column_count(), 0);
        assert_eq!(plan.query_block_offset(), 7);
    }
}
