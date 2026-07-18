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

//! Empty-selection elimination rule wrapper from
//! `pkg/planner/core/rule_eliminate_empty_selection.go`.
//!
//! The Go entrypoint recursively walks logical-plan children, removes
//! zero-condition selections, and reports `planChanged=false` with no error.
//! Rust keeps that tree mutation on a caller-owned plan trait; logical
//! selection representation, child replacement, and the full optimizer ring
//! remain external planner owners.

/// Caller-owned plan operation for recursively removing empty selections.
pub trait EmptySelectionPlan {
    /// Resulting logical-plan representation after the recursive walk.
    type Output;

    /// Removes zero-condition selection nodes from this plan tree.
    fn eliminate_empty_selections(self) -> Self::Output;
}

/// Source-shaped logical optimization rule for empty selections.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct EmptySelectionEliminator;

impl EmptySelectionEliminator {
    /// Applies the caller-owned recursive walk and reports no direct
    /// plan-change flag, matching the Go wrapper.
    #[must_use]
    pub fn optimize<P: EmptySelectionPlan>(&self, plan: P) -> (P::Output, bool) {
        (plan.eliminate_empty_selections(), false)
    }

    /// Returns the source rule registry name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        "eliminate_empty_selection"
    }
}
