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

//! PhysicalLimit metadata from
//! `pkg/planner/core/operator/physicalop/physical_limit.go`.
//!
//! The Go operator owns typed partition/prefix columns, properties, context,
//! statistics, cost, clone, protobuf, index resolution, and task attachment.
//! This leaf preserves Init identity and the source ExplainInfo redaction
//! branches over caller-supplied partition/prefix text; those planner and
//! execution boundaries remain external.

/// The source plan-codec type assigned by `PhysicalLimit.Init`.
pub const PLAN_TYPE: &str = "Limit";

/// Redaction modes compared by the source ExplainInfo implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedactMode {
    /// `errors.RedactLogDisable`: render actual values.
    Disable,
    /// `errors.RedactLogMarker`: wrap values in redaction markers.
    Marker,
    /// `errors.RedactLogEnable`: replace values with `?`.
    Enable,
    /// Any unrecognized mode; source leaves the value portion empty.
    Other,
}

/// Minimal initialized physical Limit metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalLimitPlan {
    offset: u64,
    count: u64,
    query_block_offset: i32,
    partition_explain: Option<String>,
    prefix_col_explain: Option<String>,
    prefix_len: i64,
}

impl PhysicalLimitPlan {
    /// Initializes source-shaped Limit metadata.
    #[must_use]
    pub const fn init(offset: u64, count: u64, query_block_offset: i32) -> Self {
        Self {
            offset,
            count,
            query_block_offset,
            partition_explain: None,
            prefix_col_explain: None,
            prefix_len: 0,
        }
    }

    /// Supplies the already-rendered partition-by prefix from the planner.
    #[must_use]
    pub fn with_partition_explain(mut self, partition_explain: impl Into<String>) -> Self {
        self.partition_explain = Some(partition_explain.into());
        self
    }

    /// Supplies the already-rendered prefix column and byte length.
    #[must_use]
    pub fn with_prefix_col(
        mut self,
        prefix_col_explain: impl Into<String>,
        prefix_len: i64,
    ) -> Self {
        self.prefix_col_explain = Some(prefix_col_explain.into());
        self.prefix_len = prefix_len;
        self
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

    /// Returns the LIMIT offset.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the LIMIT count.
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// Returns source ExplainInfo for the selected redaction mode.
    #[must_use]
    pub fn explain_info(&self, redact: RedactMode) -> String {
        let mut result = String::new();
        if let Some(partition) = &self.partition_explain {
            result.push_str(partition);
            result.push_str(", ");
        }
        match redact {
            RedactMode::Disable => {
                result.push_str(&format!("offset:{}, count:{}", self.offset, self.count));
                if let Some(prefix_col) = &self.prefix_col_explain {
                    result.push_str(&format!(
                        ", prefix_col:{}, prefix_len:{}",
                        prefix_col, self.prefix_len
                    ));
                }
            }
            RedactMode::Marker => {
                result.push_str(&format!("offset:‹{}›, count:‹{}›", self.offset, self.count));
                if let Some(prefix_col) = &self.prefix_col_explain {
                    result.push_str(&format!(
                        ", prefix_col:{}, prefix_len:‹{}›",
                        prefix_col, self.prefix_len
                    ));
                }
            }
            RedactMode::Enable => {
                result.push_str("offset:?, count:?");
                if self.prefix_col_explain.is_some() {
                    result.push_str(", prefix_col:?, prefix_len:?");
                }
            }
            RedactMode::Other => {}
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalLimitPlan, RedactMode, PLAN_TYPE};

    #[test]
    fn init_preserves_limit_kind_offsets_and_counts() {
        let plan = PhysicalLimitPlan::init(3, 9, -2);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Limit");
        assert_eq!(plan.offset(), 3);
        assert_eq!(plan.count(), 9);
        assert_eq!(plan.query_block_offset(), -2);
    }

    #[test]
    fn explain_info_renders_unredacted_values_and_prefix_metadata() {
        let plan = PhysicalLimitPlan::init(3, 9, 0)
            .with_partition_explain("partition by a")
            .with_prefix_col("a", 4);
        assert_eq!(
            plan.explain_info(RedactMode::Disable),
            "partition by a, offset:3, count:9, prefix_col:a, prefix_len:4"
        );
    }

    #[test]
    fn explain_info_preserves_marker_and_enabled_redaction() {
        let plan = PhysicalLimitPlan::init(3, 9, 0).with_prefix_col("a", 4);
        assert_eq!(
            plan.explain_info(RedactMode::Marker),
            "offset:‹3›, count:‹9›, prefix_col:a, prefix_len:‹4›"
        );
        assert_eq!(
            plan.explain_info(RedactMode::Enable),
            "offset:?, count:?, prefix_col:?, prefix_len:?"
        );
    }

    #[test]
    fn unknown_redaction_mode_keeps_source_empty_value_branch() {
        let plan = PhysicalLimitPlan::init(3, 9, 0).with_partition_explain("partition by a");
        assert_eq!(plan.explain_info(RedactMode::Other), "partition by a, ");
    }
}
