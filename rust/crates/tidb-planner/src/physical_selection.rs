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

//! PhysicalSelection metadata from
//! `pkg/planner/core/operator/physicalop/physical_selection.go`.
//!
//! The Go selection owns typed expressions, session/context, property and
//! statistics propagation, cost, clone, index resolution, protobuf encoding,
//! and task attachment. This leaf preserves only Init plan identity/offset and
//! the source ExplainInfo suffix over caller-supplied sorted expression text;
//! expression sorting/evaluation and execution remain external boundaries.

/// The source plan-codec type assigned by `PhysicalSelection.Init`.
pub const PLAN_TYPE: &str = "Selection";

/// Minimal initialized physical Selection metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalSelectionPlan {
    condition_explain: String,
    query_block_offset: i32,
    stream_count: u64,
}

impl PhysicalSelectionPlan {
    /// Initializes source-shaped Selection metadata.
    #[must_use]
    pub fn init(
        condition_explain: impl Into<String>,
        query_block_offset: i32,
        stream_count: u64,
    ) -> Self {
        Self {
            condition_explain: condition_explain.into(),
            query_block_offset,
            stream_count,
        }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(&self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the caller-owned query-block offset passed to Init.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.query_block_offset
    }

    /// Returns the caller-supplied sorted condition explain text.
    #[must_use]
    pub fn condition_explain(&self) -> &str {
        &self.condition_explain
    }

    /// Returns TiFlash's fine-grained shuffle stream count.
    #[must_use]
    pub const fn stream_count(&self) -> u64 {
        self.stream_count
    }

    /// Returns source ExplainInfo with the optional stream-count suffix.
    #[must_use]
    pub fn explain_info(&self) -> String {
        if self.stream_count == 0 {
            self.condition_explain.clone()
        } else {
            format!(
                "{}, stream_count: {}",
                self.condition_explain, self.stream_count
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalSelectionPlan, PLAN_TYPE};

    #[test]
    fn init_preserves_selection_kind_offset_and_expression_text() {
        let plan = PhysicalSelectionPlan::init("gt(a, 1)", -2, 0);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Selection");
        assert_eq!(plan.query_block_offset(), -2);
        assert_eq!(plan.condition_explain(), "gt(a, 1)");
    }

    #[test]
    fn zero_stream_count_returns_expression_text_without_suffix() {
        let plan = PhysicalSelectionPlan::init("eq(a, 1)", 0, 0);
        assert_eq!(plan.stream_count(), 0);
        assert_eq!(plan.explain_info(), "eq(a, 1)");
    }

    #[test]
    fn positive_stream_count_matches_source_suffix() {
        let plan = PhysicalSelectionPlan::init("gt(a, 1)", 0, 10);
        assert_eq!(plan.explain_info(), "gt(a, 1), stream_count: 10");
    }

    #[test]
    fn empty_expression_text_keeps_source_separator_behavior() {
        assert_eq!(
            PhysicalSelectionPlan::init("", 0, 3).explain_info(),
            ", stream_count: 3"
        );
    }
}
