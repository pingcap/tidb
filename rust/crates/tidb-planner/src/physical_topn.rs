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

//! Dependency-closed metadata for the physical TopN operator.
//!
//! The Go `PhysicalTopN` owns typed expressions, schema/context and child
//! properties, task attachment, cost, index resolution, and protobuf
//! conversion. This leaf ports the stable plan metadata, source ByItems and
//! PartitionBy rendering, offset/count redaction, prefix-index explain fields,
//! deep clone, and monotonic memory accounting. Optimizer enumeration,
//! expression evaluation, execution, and storage boundaries remain external.

pub use crate::physical_limit::RedactMode;

/// The source plan-codec type assigned by `PhysicalTopN.Init`.
pub const PLAN_TYPE: &str = "TopN";

/// Caller-owned text and direction for one source order item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopNOrderItem {
    explain_text: String,
    normalized_text: String,
    desc: bool,
}

impl TopNOrderItem {
    /// Creates a source-shaped order item from rendered expression text.
    #[must_use]
    pub fn new(explain_text: impl Into<String>, desc: bool) -> Self {
        let explain_text = explain_text.into();
        Self {
            normalized_text: explain_text.clone(),
            explain_text,
            desc,
        }
    }

    /// Supplies normalized expression text when it differs from ExplainInfo.
    #[must_use]
    pub fn with_normalized_text(mut self, normalized_text: impl Into<String>) -> Self {
        self.normalized_text = normalized_text.into();
        self
    }

    /// Returns caller-supplied expression/column text.
    #[must_use]
    pub fn explain_text(&self) -> &str {
        &self.explain_text
    }

    /// Returns caller-supplied normalized expression/column text.
    #[must_use]
    pub fn normalized_explain_text(&self) -> &str {
        &self.normalized_text
    }

    /// Returns whether the source item is descending.
    #[must_use]
    pub const fn is_desc(&self) -> bool {
        self.desc
    }

    /// Replaces expression text while retaining source direction.
    pub fn set_explain_text(&mut self, explain_text: impl Into<String>) {
        self.explain_text = explain_text.into();
    }
}

/// Minimal initialized physical TopN metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTopNPlan {
    by_items: Vec<TopNOrderItem>,
    partition_by: Vec<TopNOrderItem>,
    offset: u64,
    count: u64,
    prefix_col_explain: Option<String>,
    prefix_len: i64,
    query_block_offset: i32,
}

impl PhysicalTopNPlan {
    /// Initializes source-shaped TopN metadata.
    #[must_use]
    pub fn init(
        by_items: Vec<TopNOrderItem>,
        partition_by: Vec<TopNOrderItem>,
        offset: u64,
        count: u64,
        query_block_offset: i32,
    ) -> Self {
        Self {
            by_items,
            partition_by,
            offset,
            count,
            prefix_col_explain: None,
            prefix_len: 0,
            query_block_offset,
        }
    }

    /// Supplies source PrefixCol text and PrefixLen metadata.
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

    /// Returns the caller-owned query-block offset passed to `Init`.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.query_block_offset
    }

    /// Returns TopN offset.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns TopN count.
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// Returns source order-by items.
    #[must_use]
    pub fn by_items(&self) -> &[TopNOrderItem] {
        &self.by_items
    }

    /// Returns mutable order-by items for caller-owned expression state.
    pub fn by_items_mut(&mut self) -> &mut [TopNOrderItem] {
        &mut self.by_items
    }

    /// Returns source partition-by items.
    #[must_use]
    pub fn partition_by(&self) -> &[TopNOrderItem] {
        &self.partition_by
    }

    /// Clones TopN metadata, including deep-owned item and prefix text.
    #[must_use]
    pub fn clone_plan(&self) -> Self {
        self.clone()
    }

    /// Returns source ExplainInfo for the selected redaction mode.
    #[must_use]
    pub fn explain_info(&self, redact: RedactMode) -> String {
        let mut result = String::new();
        if !self.partition_by.is_empty() {
            result.push_str("partition by ");
            for (index, item) in self.partition_by.iter().enumerate() {
                if index > 0 {
                    result.push_str(", ");
                }
                // ExplainPartitionBy intentionally ignores direction.
                result.push_str(item.explain_text());
            }
            result.push(' ');
        }
        if !self.by_items.is_empty() {
            if !self.partition_by.is_empty() {
                result.push_str("order by ");
            }
            for (index, item) in self.by_items.iter().enumerate() {
                if index > 0 {
                    result.push_str(", ");
                }
                result.push_str(item.explain_text());
                if item.is_desc() {
                    result.push_str(":desc");
                }
            }
        }
        match redact {
            RedactMode::Disable => {
                result.push_str(&format!(", offset:{}, count:{}", self.offset, self.count));
                if let Some(prefix_col) = &self.prefix_col_explain {
                    result.push_str(&format!(
                        ", prefix_col:{}, prefix_len:{}",
                        prefix_col, self.prefix_len
                    ));
                }
            }
            RedactMode::Marker => {
                result.push_str(&format!(
                    ", offset:‹{}›, count:‹{}›",
                    self.offset, self.count
                ));
                if let Some(prefix_col) = &self.prefix_col_explain {
                    result.push_str(&format!(
                        ", prefix_col:‹{}›, prefix_len:‹{}›",
                        prefix_col, self.prefix_len
                    ));
                }
            }
            RedactMode::Enable => {
                result.push_str(", offset:?, count:?");
                if self.prefix_col_explain.is_some() {
                    result.push_str(", prefix_col:?, prefix_len:?");
                }
            }
            RedactMode::Other => {}
        }
        result
    }

    /// Returns source normalized ExplainInfo without value redaction.
    #[must_use]
    pub fn explain_normalized_info(&self) -> String {
        let mut result = String::new();
        if !self.partition_by.is_empty() {
            result.push_str("partition by ");
            for (index, item) in self.partition_by.iter().enumerate() {
                if index > 0 {
                    result.push_str(", ");
                }
                result.push_str(item.normalized_explain_text());
            }
            result.push(' ');
        }
        if !self.by_items.is_empty() {
            if !self.partition_by.is_empty() {
                result.push_str("order by ");
            }
            for (index, item) in self.by_items.iter().enumerate() {
                if index > 0 {
                    result.push_str(", ");
                }
                result.push_str(item.normalized_explain_text());
                if item.is_desc() {
                    result.push_str(":desc");
                }
            }
        }
        result
    }

    /// Returns a monotonic metadata-size estimate matching source growth.
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        let item_bytes = self
            .by_items
            .iter()
            .chain(self.partition_by.iter())
            .map(|item| 1 + item.explain_text.len() + item.normalized_text.len())
            .sum::<usize>();
        let prefix_bytes = self
            .prefix_col_explain
            .as_ref()
            .map_or(0, |prefix| 1 + prefix.len());
        1 + item_bytes + prefix_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalTopNPlan, RedactMode, TopNOrderItem, PLAN_TYPE};

    #[test]
    fn init_preserves_topn_kind_offsets_counts_and_query_block() {
        let plan = PhysicalTopNPlan::init(vec![], vec![], 3, 9, -2);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "TopN");
        assert_eq!(plan.offset(), 3);
        assert_eq!(plan.count(), 9);
        assert_eq!(plan.query_block_offset(), -2);
    }

    #[test]
    fn explain_info_matches_source_partition_order_and_prefix_shape() {
        let plan = PhysicalTopNPlan::init(
            vec![
                TopNOrderItem::new("a", false),
                TopNOrderItem::new("b", true),
            ],
            vec![TopNOrderItem::new("p", true)],
            1,
            2,
            0,
        )
        .with_prefix_col("a", 4);
        assert_eq!(
            plan.explain_info(RedactMode::Disable),
            "partition by p order by a, b:desc, offset:1, count:2, prefix_col:a, prefix_len:4"
        );
    }

    #[test]
    fn explain_redaction_and_normalized_info_match_source_branches() {
        let plan = PhysicalTopNPlan::init(
            vec![TopNOrderItem::new("a", true)],
            vec![TopNOrderItem::new("p", false)],
            4,
            8,
            0,
        )
        .with_prefix_col("a", 3);
        assert_eq!(
            plan.explain_info(RedactMode::Marker),
            "partition by p order by a:desc, offset:‹4›, count:‹8›, prefix_col:‹a›, prefix_len:‹3›"
        );
        assert_eq!(
            plan.explain_info(RedactMode::Enable),
            "partition by p order by a:desc, offset:?, count:?, prefix_col:?, prefix_len:?"
        );
        assert_eq!(
            plan.explain_normalized_info(),
            "partition by p order by a:desc"
        );
    }

    #[test]
    fn normalized_item_text_is_independent_from_explain_text() {
        let plan = PhysicalTopNPlan::init(
            vec![TopNOrderItem::new("a", true).with_normalized_text("?")],
            vec![TopNOrderItem::new("p", false).with_normalized_text("?")],
            0,
            1,
            0,
        );
        assert_eq!(plan.by_items()[0].explain_text(), "a");
        assert_eq!(plan.by_items()[0].normalized_explain_text(), "?");
        assert_eq!(
            plan.explain_info(RedactMode::Disable),
            "partition by p order by a:desc, offset:0, count:1"
        );
        assert_eq!(
            plan.explain_normalized_info(),
            "partition by ? order by ?:desc"
        );
    }

    #[test]
    fn clone_deep_copies_order_and_prefix_metadata() {
        let plan = PhysicalTopNPlan::init(
            vec![TopNOrderItem::new("a", false)],
            vec![TopNOrderItem::new("p", false)],
            2333,
            2333,
            0,
        )
        .with_prefix_col("prefix", 5);
        let mut cloned = plan.clone_plan();
        cloned.by_items_mut()[0].set_explain_text("b");
        assert_eq!(plan.by_items()[0].explain_text(), "a");
        assert_eq!(cloned.by_items()[0].explain_text(), "b");
        assert_eq!(cloned.partition_by(), plan.partition_by());
        assert_eq!(cloned.offset(), plan.offset());
        assert_eq!(cloned.count(), plan.count());
    }

    #[test]
    fn memory_usage_grows_for_items_and_prefix() {
        let empty = PhysicalTopNPlan::init(vec![], vec![], 0, 0, 0);
        let with_item = PhysicalTopNPlan::init(
            vec![TopNOrderItem::new("a", false)],
            vec![TopNOrderItem::new("p", false)],
            0,
            0,
            0,
        );
        let with_prefix = with_item.clone().with_prefix_col("a", 3);
        assert!(with_item.memory_usage() > empty.memory_usage());
        assert!(with_prefix.memory_usage() > with_item.memory_usage());
    }
}
