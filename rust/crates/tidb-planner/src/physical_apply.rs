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

//! PhysicalApply metadata from
//! `pkg/planner/core/operator/physicalop/physical_apply.go`.
//!
//! The Go PhysicalApply embeds a physical hash join but deliberately does not
//! implement the physical-join interface. This leaf preserves that exact
//! dispatch boundary plus Init's plan identity/offset; hash-join state,
//! correlated columns, clone/cost/index/task wiring, and subquery execution
//! remain external boundaries.

/// The source plan-codec type assigned by `PhysicalApply.Init`.
pub const PLAN_TYPE: &str = "Apply";

/// Minimal initialized physical Apply metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalApplyPlan {
    query_block_offset: i32,
}

impl PhysicalApplyPlan {
    /// Initializes source-shaped Apply metadata.
    #[must_use]
    pub const fn init(query_block_offset: i32) -> Self {
        Self { query_block_offset }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the caller-owned query-block offset passed to Init.
    #[must_use]
    pub const fn query_block_offset(self) -> i32 {
        self.query_block_offset
    }

    /// Mirrors Go's `PhysicalJoinImplement`: Apply is not a PhysicalJoin.
    #[must_use]
    pub const fn physical_join_implement(self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalApplyPlan, PLAN_TYPE};

    #[test]
    fn init_preserves_apply_kind_and_offset() {
        let plan = PhysicalApplyPlan::init(-4);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Apply");
        assert_eq!(plan.query_block_offset(), -4);
    }

    #[test]
    fn apply_is_not_a_physical_join() {
        assert!(!PhysicalApplyPlan::init(0).physical_join_implement());
    }

    #[test]
    fn join_dispatch_boundary_is_stable_for_any_offset() {
        for offset in [i32::MIN, -1, 0, 1, i32::MAX] {
            assert!(!PhysicalApplyPlan::init(offset).physical_join_implement());
        }
    }
}
