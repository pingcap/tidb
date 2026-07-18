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

//! PhysicalExchangeReceiver metadata from
//! `pkg/planner/core/operator/physicalop/physical_exchange_receiver.go`.
//!
//! The Go receiver owns MPP task/fragment connections, schema, context,
//! protobuf conversion, clone state, and cost calculation. This leaf preserves
//! only the dependency-closed Init plan identity and source ExplainInfo
//! stream-count rendering; those MPP/runtime boundaries remain external.

/// The source plan-codec type assigned by `PhysicalExchangeReceiver.Init`.
pub const PLAN_TYPE: &str = "ExchangeReceiver";

/// Root query-block offset assigned by the receiver's Init method.
pub const QUERY_BLOCK_OFFSET: i32 = 0;

/// Minimal initialized physical ExchangeReceiver metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalExchangeReceiverPlan {
    stream_count: u64,
}

impl PhysicalExchangeReceiverPlan {
    /// Initializes source-shaped receiver metadata.
    #[must_use]
    pub const fn init(stream_count: u64) -> Self {
        Self { stream_count }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the root query-block offset assigned by Init.
    #[must_use]
    pub const fn query_block_offset(self) -> i32 {
        QUERY_BLOCK_OFFSET
    }

    /// Returns TiFlash's configured fine-grained shuffle stream count.
    #[must_use]
    pub const fn stream_count(self) -> u64 {
        self.stream_count
    }

    /// Returns source `ExplainInfo`: empty at zero, otherwise stream_count text.
    #[must_use]
    pub fn explain_info(self) -> String {
        if self.stream_count == 0 {
            String::new()
        } else {
            format!("stream_count: {}", self.stream_count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalExchangeReceiverPlan, PLAN_TYPE, QUERY_BLOCK_OFFSET};

    #[test]
    fn init_preserves_receiver_kind_and_root_offset() {
        let plan = PhysicalExchangeReceiverPlan::init(8);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "ExchangeReceiver");
        assert_eq!(plan.query_block_offset(), QUERY_BLOCK_OFFSET);
        assert_eq!(plan.query_block_offset(), 0);
    }

    #[test]
    fn zero_stream_count_has_empty_explain_info() {
        let plan = PhysicalExchangeReceiverPlan::init(0);
        assert_eq!(plan.stream_count(), 0);
        assert_eq!(plan.explain_info(), "");
    }

    #[test]
    fn positive_stream_count_matches_source_explain_text() {
        assert_eq!(
            PhysicalExchangeReceiverPlan::init(10).explain_info(),
            "stream_count: 10"
        );
        assert_eq!(
            PhysicalExchangeReceiverPlan::init(u64::MAX).explain_info(),
            format!("stream_count: {}", u64::MAX)
        );
    }

    #[test]
    fn stream_count_is_lossless_uint64_metadata() {
        let plan = PhysicalExchangeReceiverPlan::init(20);
        assert_eq!(plan.stream_count(), 20);
    }
}
