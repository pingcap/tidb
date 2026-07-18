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

//! Derived TopN-from-window rule wrapper from
//! `pkg/planner/core/rule_derive_topn_from_window.go`.
//!
//! The Go rule delegates all plan semantics to `LogicalPlan.DeriveTopN()` and
//! reports `planChanged=false` with no error. Rust keeps that callback on a
//! caller-owned plan trait; window operators, TopN construction, and the full
//! optimizer/testdata ring remain external planner owners.

/// Plan operation used by the derived TopN rule.
pub trait DeriveTopNPlan {
    /// Resulting logical-plan representation after the callback.
    type Output;

    /// Derives an implicit TopN from a row-number window plan.
    fn derive_top_n(self) -> Self::Output;
}

/// Source-shaped logical optimization rule for deriving TopN from windows.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct DeriveTopNFromWindow;

impl DeriveTopNFromWindow {
    /// Applies the source callback and reports no direct plan-change flag,
    /// matching the Go wrapper.
    #[must_use]
    pub fn optimize<P: DeriveTopNPlan>(&self, plan: P) -> (P::Output, bool) {
        (plan.derive_top_n(), false)
    }

    /// Returns the source rule registry name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        "derive_topn_from_window"
    }
}
