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

//! TopN push-down rule wrapper from `pkg/planner/core/rule_topn_push_down.go`.
//!
//! The Go rule delegates all plan semantics to `LogicalPlan.PushDownTopN(nil)`
//! and reports `planChanged=false` with no error. Rust keeps that delegation as
//! a caller-owned plan trait; logical operators, TopN construction, and the
//! full optimizer/testdata ring remain external planner owners.

/// Plan operation used by the TopN push-down rule.
pub trait TopNPushDownPlan {
    /// Resulting logical-plan representation after the callback.
    type Output;

    /// Pushes a TopN/limit into this plan, receiving the optional parent plan.
    fn push_down_top_n(self, top_n: Option<Self::Output>) -> Self::Output;
}

/// Source-shaped logical optimization rule for TopN push-down.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct PushDownTopNOptimizer;

impl PushDownTopNOptimizer {
    /// Applies the source callback with a nil/absent parent and reports no
    /// direct plan-change flag, matching the Go wrapper.
    #[must_use]
    pub fn optimize<P: TopNPushDownPlan>(&self, plan: P) -> (P::Output, bool) {
        (plan.push_down_top_n(None), false)
    }

    /// Returns the source rule registry name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        "topn_push_down"
    }
}
