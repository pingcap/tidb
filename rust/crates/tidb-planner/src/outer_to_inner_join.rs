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

//! Outer-to-inner join rule wrapper from
//! `pkg/planner/core/rule_outer_to_inner_join.go`.
//!
//! The Go rule owns the registry name and delegates the actual logical-plan
//! traversal to `LogicalPlan.ConvertOuterToInnerJoin`; it intentionally
//! returns `planChanged == false` because the delegated conversion is handled
//! by the plan itself. This leaf keeps that wrapper contract over a caller-
//! owned plan adapter; join predicates, null-rejection, child traversal, and
//! error/session handling remain external planner boundaries.

/// Minimal plan callback required by the source rule wrapper.
pub trait LogicalPlanAdapter: Sized {
    /// Applies the caller-owned outer-to-inner conversion and returns the plan.
    fn convert_outer_to_inner_join(self) -> Self;
}

/// Source logical optimization rule wrapper.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct ConvertOuterToInnerJoin;

impl ConvertOuterToInnerJoin {
    /// Creates the source rule wrapper.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Returns the source rule registry name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        "convert_outer_to_inner_joins"
    }

    /// Delegates conversion and preserves the source false change flag.
    #[must_use]
    pub fn optimize<P: LogicalPlanAdapter>(self, plan: P) -> (P, bool) {
        (plan.convert_outer_to_inner_join(), false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    struct FakePlan {
        converted: bool,
        conversion_calls: u8,
    }

    impl LogicalPlanAdapter for FakePlan {
        fn convert_outer_to_inner_join(mut self) -> Self {
            self.converted = true;
            self.conversion_calls += 1;
            self
        }
    }

    #[test]
    fn test_rule_name_and_delegation_contract() {
        let rule = ConvertOuterToInnerJoin::new();
        assert_eq!(rule.name(), "convert_outer_to_inner_joins");
        let (plan, changed) = rule.optimize(FakePlan {
            converted: false,
            conversion_calls: 0,
        });
        assert!(plan.converted);
        assert_eq!(plan.conversion_calls, 1);
        assert!(!changed);
    }

    #[test]
    fn test_delegated_noop_still_reports_false_change_flag() {
        let (plan, changed) = ConvertOuterToInnerJoin.optimize(NoopPlan);
        assert_eq!(plan, NoopPlan);
        assert!(!changed);
    }

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    struct NoopPlan;

    impl LogicalPlanAdapter for NoopPlan {
        fn convert_outer_to_inner_join(self) -> Self {
            self
        }
    }
}
