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

//! PhysicalProjection metadata from
//! `pkg/planner/core/operator/physicalop/physical_projection.go`.
//!
//! The Go projection owns typed expressions/schema, session/redaction context,
//! pushdown properties and statistics, cost, clone/index resolution, protobuf,
//! and task attachment. This leaf preserves only Init plan identity/offset and
//! the source ExplainInfo stream-count suffix over caller-supplied expression
//! list text; expression rendering and execution remain external boundaries.

/// The source plan-codec type assigned by `PhysicalProjection.Init`.
pub const PLAN_TYPE: &str = "Projection";

/// Minimal initialized physical Projection metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalProjectionPlan {
    expr_explain: String,
    query_block_offset: i32,
    stream_count: u64,
}

impl PhysicalProjectionPlan {
    /// Initializes source-shaped Projection metadata.
    #[must_use]
    pub fn init(
        expr_explain: impl Into<String>,
        query_block_offset: i32,
        stream_count: u64,
    ) -> Self {
        Self {
            expr_explain: expr_explain.into(),
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

    /// Returns the caller-supplied expression-list explain text.
    #[must_use]
    pub fn expr_explain(&self) -> &str {
        &self.expr_explain
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
            self.expr_explain.clone()
        } else {
            format!("{}, stream_count: {}", self.expr_explain, self.stream_count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalProjectionPlan, PLAN_TYPE};

    #[test]
    fn init_preserves_projection_kind_offset_and_expression_text() {
        let plan = PhysicalProjectionPlan::init("Column#1, plus(Column#2, 1)", -2, 0);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Projection");
        assert_eq!(plan.query_block_offset(), -2);
        assert_eq!(plan.expr_explain(), "Column#1, plus(Column#2, 1)");
    }

    #[test]
    fn zero_stream_count_returns_expression_list_without_suffix() {
        let plan = PhysicalProjectionPlan::init("Column#1", 0, 0);
        assert_eq!(plan.stream_count(), 0);
        assert_eq!(plan.explain_info(), "Column#1");
    }

    #[test]
    fn positive_stream_count_matches_source_projection_suffix() {
        let plan = PhysicalProjectionPlan::init("Column#1", 0, 10);
        assert_eq!(plan.explain_info(), "Column#1, stream_count: 10");
    }

    #[test]
    fn empty_expression_list_keeps_source_separator_behavior() {
        assert_eq!(
            PhysicalProjectionPlan::init("", 0, 3).explain_info(),
            ", stream_count: 3"
        );
    }
}
