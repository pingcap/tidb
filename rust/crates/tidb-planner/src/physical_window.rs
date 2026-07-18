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

//! Dependency-closed metadata for the physical window operator.
//!
//! `PhysicalWindow` in Go owns window-function descriptors, partition/order
//! expressions, frames, schema/context state, task attachment, and protobuf
//! encoding. This leaf ports the stable plan identity and the inherited
//! TiFlash fine-grained shuffle stream-count behavior. Window expression and
//! execution state remain caller-owned until those dependencies are ported.

/// The source plan-codec type assigned by `PhysicalWindow.Init`.
pub const PLAN_TYPE: &str = "Window";

/// Minimal initialized physical Window metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalWindowPlan {
    explain_text: String,
    query_block_offset: i32,
    stream_count: u64,
}

impl PhysicalWindowPlan {
    /// Initializes source-shaped Window metadata.
    #[must_use]
    pub fn init(
        explain_text: impl Into<String>,
        query_block_offset: i32,
        stream_count: u64,
    ) -> Self {
        Self {
            explain_text: explain_text.into(),
            query_block_offset,
            stream_count,
        }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(&self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the caller-owned query-block offset passed to `Init`.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.query_block_offset
    }

    /// Returns the caller-supplied window expression text.
    #[must_use]
    pub fn explain_text(&self) -> &str {
        &self.explain_text
    }

    /// Returns TiFlash's inherited fine-grained shuffle stream count.
    #[must_use]
    pub const fn stream_count(&self) -> u64 {
        self.stream_count
    }

    /// Clones plan metadata, preserving inherited fields as Go `Clone` does.
    #[must_use]
    pub fn clone_plan(&self) -> Self {
        self.clone()
    }

    /// Returns source `ExplainInfo` with its optional stream-count suffix.
    #[must_use]
    pub fn explain_info(&self) -> String {
        if self.stream_count == 0 {
            self.explain_text.clone()
        } else {
            format!("{}, stream_count: {}", self.explain_text, self.stream_count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalWindowPlan, PLAN_TYPE};

    #[test]
    fn init_preserves_window_kind_offset_and_expression_text() {
        let plan = PhysicalWindowPlan::init("row_number() over(order by a)", -2, 0);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Window");
        assert_eq!(plan.query_block_offset(), -2);
        assert_eq!(plan.explain_text(), "row_number() over(order by a)");
    }

    #[test]
    fn clone_preserves_zero_inherited_stream_count() {
        let plan = PhysicalWindowPlan::init("window", 0, 0);
        let cloned = plan.clone_plan();
        assert_eq!(cloned.stream_count(), plan.stream_count());
        assert_eq!(cloned, plan);
    }

    #[test]
    fn clone_preserves_positive_inherited_stream_count() {
        let plan = PhysicalWindowPlan::init("window", 0, 8);
        let cloned = plan.clone_plan();
        assert_eq!(cloned.stream_count(), 8);
        assert_eq!(cloned.stream_count(), plan.stream_count());
    }

    #[test]
    fn explain_info_appends_stream_count_only_when_positive() {
        assert_eq!(
            PhysicalWindowPlan::init("row_number() over()", 0, 0).explain_info(),
            "row_number() over()"
        );
        assert_eq!(
            PhysicalWindowPlan::init("row_number() over()", 0, 3).explain_info(),
            "row_number() over(), stream_count: 3"
        );
    }

    #[test]
    fn empty_expression_text_keeps_positive_stream_separator() {
        assert_eq!(
            PhysicalWindowPlan::init("", 0, 3).explain_info(),
            ", stream_count: 3"
        );
    }
}
