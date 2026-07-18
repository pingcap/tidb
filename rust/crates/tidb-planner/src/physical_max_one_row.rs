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

//! PhysicalMaxOneRow's dependency-closed planning gate from
//! `pkg/planner/core/operator/physicalop/physical_max_one_row.go`.
//!
//! The Go operator carries physical-plan context, statistics, child
//! properties, and warning/session state. This leaf keeps the source-visible
//! eligibility decision and the fixed expected-count/propagation metadata;
//! physical task construction, warning publication, and executor row-limit
//! behavior remain outside this boundary.

/// The source CTE producer status is opaque to this planner leaf.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CteProducerStatus(u32);

impl CteProducerStatus {
    /// Creates a caller-owned CTE producer status token.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the opaque source status value.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

/// Minimal PhysicalMaxOneRow plan metadata emitted by the source gate.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PhysicalMaxOneRowPlan {
    expected_cnt: u64,
    cte_producer_status: CteProducerStatus,
    no_cop_push_down: bool,
}

impl PhysicalMaxOneRowPlan {
    /// The source operator always requests two rows from its child property.
    pub const EXPECTED_CNT: u64 = 2;

    /// Applies `ExhaustPhysicalPlans4LogicalMaxOneRow`'s source gates.
    ///
    /// A non-empty sort requirement or TiFlash property prevents this
    /// operator from being emitted. Otherwise the source creates a plan with
    /// `ExpectedCnt: 2` and forwards the CTE/no-cop property fields.
    #[must_use]
    pub const fn exhaust(
        sort_items_empty: bool,
        is_flash_prop: bool,
        cte_producer_status: CteProducerStatus,
        no_cop_push_down: bool,
    ) -> Option<Self> {
        if !sort_items_empty || is_flash_prop {
            None
        } else {
            Some(Self {
                expected_cnt: Self::EXPECTED_CNT,
                cte_producer_status,
                no_cop_push_down,
            })
        }
    }

    /// Returns the fixed source expected-count metadata.
    #[must_use]
    pub const fn expected_cnt(self) -> u64 {
        self.expected_cnt
    }

    /// Returns the forwarded CTE producer status.
    #[must_use]
    pub const fn cte_producer_status(self) -> CteProducerStatus {
        self.cte_producer_status
    }

    /// Returns the forwarded no-cop-pushdown flag.
    #[must_use]
    pub const fn no_cop_push_down(self) -> bool {
        self.no_cop_push_down
    }
}

#[cfg(test)]
mod tests {
    use super::{CteProducerStatus, PhysicalMaxOneRowPlan};

    #[test]
    fn source_gates_reject_sort_and_flash_properties() {
        let status = CteProducerStatus::new(7);
        assert!(PhysicalMaxOneRowPlan::exhaust(false, false, status, false).is_none());
        assert!(PhysicalMaxOneRowPlan::exhaust(true, true, status, false).is_none());
        assert!(PhysicalMaxOneRowPlan::exhaust(false, true, status, true).is_none());
    }

    #[test]
    fn supported_plan_preserves_expected_count_and_properties() {
        let status = CteProducerStatus::new(9);
        let plan = PhysicalMaxOneRowPlan::exhaust(true, false, status, true).unwrap();
        assert_eq!(plan.expected_cnt(), 2);
        assert_eq!(plan.expected_cnt(), PhysicalMaxOneRowPlan::EXPECTED_CNT);
        assert_eq!(plan.cte_producer_status().value(), 9);
        assert!(plan.no_cop_push_down());
    }

    #[test]
    fn supported_plan_forwards_false_no_cop_pushdown() {
        let plan =
            PhysicalMaxOneRowPlan::exhaust(true, false, CteProducerStatus::new(u32::MAX), false)
                .unwrap();
        assert_eq!(plan.cte_producer_status().value(), u32::MAX);
        assert!(!plan.no_cop_push_down());
    }
}
