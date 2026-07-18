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

//! PhysicalShuffle metadata from
//! `pkg/planner/core/operator/physicalop/physical_shuffle.go`.
//!
//! The Go shuffle owns physical child/data-source plans, typed partition
//! expressions, task wiring, context/statistics, index resolution, and worker
//! execution. This leaf preserves Init identity/offset, splitter iota values,
//! and ExplainInfo's caller-owned data-source display over normalized inputs;
//! partitioning and runtime behavior remain external boundaries.

/// The source plan-codec type assigned by `PhysicalShuffle.Init`.
pub const PLAN_TYPE: &str = "Shuffle";

/// Source `PartitionSplitterType` values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionSplitterType(i32);

impl PartitionSplitterType {
    /// `PartitionHashSplitterType`.
    pub const HASH: Self = Self(0);
    /// `PartitionRangeSplitterType`.
    pub const RANGE: Self = Self(1);

    /// Creates a source splitter type from a raw discriminant.
    #[must_use]
    pub const fn from_raw(raw: i32) -> Self {
        Self(raw)
    }

    /// Returns the source raw discriminant.
    #[must_use]
    pub const fn raw(self) -> i32 {
        self.0
    }
}

/// Minimal initialized physical Shuffle metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalShufflePlan {
    concurrency: i64,
    query_block_offset: i32,
    data_source_ids: Vec<String>,
    splitter_type: PartitionSplitterType,
}

impl PhysicalShufflePlan {
    /// Initializes source-shaped Shuffle metadata with the default hash splitter.
    #[must_use]
    pub fn init(
        concurrency: i64,
        query_block_offset: i32,
        data_source_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            concurrency,
            query_block_offset,
            data_source_ids: data_source_ids.into_iter().map(Into::into).collect(),
            splitter_type: PartitionSplitterType::HASH,
        }
    }

    /// Overrides the source splitter discriminant.
    #[must_use]
    pub const fn with_splitter_type(mut self, splitter_type: PartitionSplitterType) -> Self {
        self.splitter_type = splitter_type;
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

    /// Returns the configured worker concurrency.
    #[must_use]
    pub const fn concurrency(&self) -> i64 {
        self.concurrency
    }

    /// Returns the splitter discriminant.
    #[must_use]
    pub const fn splitter_type(&self) -> PartitionSplitterType {
        self.splitter_type
    }

    /// Returns source ExplainInfo using Go's `[]fmt.Stringer` spacing.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let data_sources = self.data_source_ids.join(" ");
        format!(
            "execution info: concurrency:{}, data sources:[{}]",
            self.concurrency, data_sources
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{PartitionSplitterType, PhysicalShufflePlan, PLAN_TYPE};

    #[test]
    fn init_preserves_shuffle_kind_offset_and_concurrency() {
        let plan = PhysicalShufflePlan::init(5, -2, ["TableReader"]);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Shuffle");
        assert_eq!(plan.query_block_offset(), -2);
        assert_eq!(plan.concurrency(), 5);
        assert_eq!(plan.splitter_type(), PartitionSplitterType::HASH);
    }

    #[test]
    fn explain_info_matches_source_data_source_list_shape() {
        let plan = PhysicalShufflePlan::init(5, 0, ["TableReader", "IndexReader"]);
        assert_eq!(
            plan.explain_info(),
            "execution info: concurrency:5, data sources:[TableReader IndexReader]"
        );
    }

    #[test]
    fn empty_data_sources_keep_empty_brackets() {
        let plan = PhysicalShufflePlan::init(0, 0, std::iter::empty::<String>());
        assert_eq!(
            plan.explain_info(),
            "execution info: concurrency:0, data sources:[]"
        );
    }

    #[test]
    fn splitter_discriminants_preserve_source_iota_order() {
        assert_eq!(PartitionSplitterType::HASH.raw(), 0);
        assert_eq!(PartitionSplitterType::RANGE.raw(), 1);
        assert_eq!(
            PartitionSplitterType::from_raw(1),
            PartitionSplitterType::RANGE
        );
        assert_eq!(
            PhysicalShufflePlan::init(1, 0, ["x"])
                .with_splitter_type(PartitionSplitterType::RANGE)
                .splitter_type(),
            PartitionSplitterType::RANGE
        );
    }
}
