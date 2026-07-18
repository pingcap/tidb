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

//! PhysicalUnionAll planning metadata from
//! `pkg/planner/core/operator/physicalop/physical_union_all.go`.
//!
//! The Go operator carries child schemas/properties, context/statistics, task
//! wiring, costs, expressions, and runtime union execution. This leaf keeps
//! the dependency-closed Init identity/Mpp bit and the source exhaustion gates
//! over normalized property booleans; child property construction, partition
//! union type switching, and execution remain external boundaries.

/// The source plan-codec type assigned by `PhysicalUnionAll.Init`.
pub const PLAN_TYPE: &str = "Union";

/// Minimal initialized physical UnionAll metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalUnionAllPlan {
    mpp: bool,
    query_block_offset: i32,
}

impl PhysicalUnionAllPlan {
    /// Initializes source-shaped UnionAll metadata.
    #[must_use]
    pub const fn init(mpp: bool, query_block_offset: i32) -> Self {
        Self {
            mpp,
            query_block_offset,
        }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns whether the physical UnionAll uses an MPP task.
    #[must_use]
    pub const fn mpp(self) -> bool {
        self.mpp
    }

    /// Returns the caller-owned query-block offset passed to Init.
    #[must_use]
    pub const fn query_block_offset(self) -> i32 {
        self.query_block_offset
    }
}

/// Applies `ExhaustPhysicalPlans4LogicalUnionAll`'s pure property gates.
///
/// The booleans correspond to source properties: MPP task, MPP allowance,
/// required sort, TiFlash property, and an `AnyType` MPP partition. `None`
/// means the source returns handled=true with no physical plans.
#[must_use]
pub fn exhaust_physical_union_all(
    is_mpp_task: bool,
    is_mpp_allowed: bool,
    has_sort_items: bool,
    is_flash_prop: bool,
    mpp_partition_is_any: bool,
    is_root_task: bool,
    query_block_offset: i32,
) -> Option<Vec<PhysicalUnionAllPlan>> {
    if has_sort_items || (is_flash_prop && !is_mpp_task) || (is_mpp_task && !mpp_partition_is_any) {
        return None;
    }

    let can_use_mpp = is_mpp_allowed;
    if is_root_task && can_use_mpp {
        // Source returns the ordinary root candidate first, then an MPP one.
        return Some(vec![
            PhysicalUnionAllPlan::init(false, query_block_offset),
            PhysicalUnionAllPlan::init(true, query_block_offset),
        ]);
    }

    Some(vec![PhysicalUnionAllPlan::init(
        can_use_mpp && is_mpp_task,
        query_block_offset,
    )])
}

#[cfg(test)]
mod tests {
    use super::{exhaust_physical_union_all, PhysicalUnionAllPlan, PLAN_TYPE};

    #[test]
    fn init_preserves_union_kind_offset_and_mpp_bit() {
        let plan = PhysicalUnionAllPlan::init(true, -3);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Union");
        assert_eq!(plan.query_block_offset(), -3);
        assert!(plan.mpp());
    }

    #[test]
    fn source_property_gates_reject_invalid_union_all_requests() {
        assert!(exhaust_physical_union_all(false, true, true, false, true, false, 0).is_none());
        assert!(exhaust_physical_union_all(false, true, false, true, true, false, 0).is_none());
        assert!(exhaust_physical_union_all(true, true, false, false, false, false, 0).is_none());
    }

    #[test]
    fn root_task_with_mpp_allowed_emits_ordinary_then_mpp_candidates() {
        assert_eq!(
            exhaust_physical_union_all(false, true, false, false, true, true, 4),
            Some(vec![
                PhysicalUnionAllPlan::init(false, 4),
                PhysicalUnionAllPlan::init(true, 4),
            ])
        );
    }

    #[test]
    fn non_root_task_emits_single_mpp_candidate_only_when_requested() {
        assert_eq!(
            exhaust_physical_union_all(true, true, false, false, true, false, 2),
            Some(vec![PhysicalUnionAllPlan::init(true, 2)])
        );
        assert_eq!(
            exhaust_physical_union_all(false, true, false, false, true, false, 2),
            Some(vec![PhysicalUnionAllPlan::init(false, 2)])
        );
    }
}
