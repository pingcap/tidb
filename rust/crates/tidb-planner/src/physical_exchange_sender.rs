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

//! PhysicalExchangeSender metadata from
//! `pkg/planner/core/operator/physicalop/physical_exchange_sender.go`.
//!
//! The Go sender owns MPP task targets, typed hash columns, context/schema,
//! clone/cost/index-resolution, protobuf conversion, and runtime dispatch.
//! This leaf preserves Init's ExchangeSender/root-offset identity and the
//! source ExplainInfo branches over normalized caller-owned metadata.

/// The source plan-codec type assigned by `PhysicalExchangeSender.Init`.
pub const PLAN_TYPE: &str = "ExchangeSender";

/// Raw `tipb.ExchangeType` value used by ExplainInfo.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeType(i32);

impl ExchangeType {
    /// `tipb.ExchangeType_PassThrough`.
    pub const PASS_THROUGH: Self = Self(0);
    /// `tipb.ExchangeType_Broadcast`.
    pub const BROADCAST: Self = Self(1);
    /// `tipb.ExchangeType_Hash`.
    pub const HASH: Self = Self(2);

    /// Creates an exchange type from its source raw value.
    #[must_use]
    pub const fn from_raw(raw: i32) -> Self {
        Self(raw)
    }

    /// Returns the source raw value.
    #[must_use]
    pub const fn raw(self) -> i32 {
        self.0
    }

    fn explain_label(self) -> &'static str {
        match self.raw() {
            0 => "PassThrough",
            1 => "Broadcast",
            2 => "HashPartition",
            _ => "",
        }
    }
}

/// Raw source exchange-compression mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompressionMode(i32);

impl CompressionMode {
    /// `ExchangeCompressionModeNONE`.
    pub const NONE: Self = Self(0);
    /// `ExchangeCompressionModeFast`.
    pub const FAST: Self = Self(1);
    /// `ExchangeCompressionModeHC`.
    pub const HIGH_COMPRESSION: Self = Self(2);
    /// `ExchangeCompressionModeUnspecified`.
    pub const UNSPECIFIED: Self = Self(3);
    /// Source-recommended mode.
    pub const RECOMMENDED: Self = Self::FAST;

    /// Creates a compression mode from its source raw value.
    #[must_use]
    pub const fn from_raw(raw: i32) -> Self {
        Self(raw)
    }

    /// Returns the source raw value.
    #[must_use]
    pub const fn raw(self) -> i32 {
        self.0
    }

    /// Returns the source variable name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self.raw() {
            0 => "NONE",
            1 => "FAST",
            2 => "HIGH_COMPRESSION",
            3 => "UNSPECIFIED",
            // Go's ToTipbCompressionMode falls back to NONE for unknown values.
            _ => "NONE",
        }
    }

    /// Parses the case-insensitive source variable domain.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("UNSPECIFIED") {
            return Some(Self::UNSPECIFIED);
        }
        if name.eq_ignore_ascii_case("NONE") {
            return Some(Self::NONE);
        }
        if name.eq_ignore_ascii_case("FAST") {
            return Some(Self::FAST);
        }
        if name.eq_ignore_ascii_case("HIGH_COMPRESSION") {
            return Some(Self::HIGH_COMPRESSION);
        }
        None
    }

    /// Returns the raw generated TIPB compression-mode value.
    #[must_use]
    pub const fn to_tipb_raw(self) -> i32 {
        match self.raw() {
            0..=2 => self.raw(),
            _ => 0,
        }
    }
}

/// Minimal initialized physical ExchangeSender metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalExchangeSenderPlan {
    exchange_type: ExchangeType,
    compression_mode: CompressionMode,
    hash_cols_explain: String,
    task_ids: Vec<u64>,
    stream_count: u64,
}

impl PhysicalExchangeSenderPlan {
    /// Initializes source-shaped sender metadata with the fixed root offset.
    #[must_use]
    pub fn init(
        exchange_type: ExchangeType,
        compression_mode: CompressionMode,
        hash_cols_explain: impl Into<String>,
        task_ids: impl IntoIterator<Item = u64>,
        stream_count: u64,
    ) -> Self {
        Self {
            exchange_type,
            compression_mode,
            hash_cols_explain: hash_cols_explain.into(),
            task_ids: task_ids.into_iter().collect(),
            stream_count,
        }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(&self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the fixed root query-block offset assigned by Init.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        0
    }

    /// Returns the source exchange type.
    #[must_use]
    pub const fn exchange_type(&self) -> ExchangeType {
        self.exchange_type
    }

    /// Returns the source compression mode.
    #[must_use]
    pub const fn compression_mode(&self) -> CompressionMode {
        self.compression_mode
    }

    /// Returns source ExplainInfo over caller-owned hash-column text/task IDs.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let mut result = format!("ExchangeType: {}", self.exchange_type.explain_label());
        if self.compression_mode != CompressionMode::NONE {
            result.push_str(", Compression: ");
            result.push_str(self.compression_mode.name());
        }
        if self.exchange_type == ExchangeType::HASH {
            result.push_str(", Hash Cols: ");
            result.push_str(&self.hash_cols_explain);
        }
        if !self.task_ids.is_empty() {
            result.push_str(", tasks: [");
            for (index, task_id) in self.task_ids.iter().enumerate() {
                if index > 0 {
                    result.push_str(", ");
                }
                result.push_str(&task_id.to_string());
            }
            result.push(']');
        }
        if self.stream_count > 0 {
            result.push_str(", stream_count: ");
            result.push_str(&self.stream_count.to_string());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::{CompressionMode, ExchangeType, PhysicalExchangeSenderPlan, PLAN_TYPE};

    #[test]
    fn init_preserves_sender_kind_and_root_offset() {
        let plan = PhysicalExchangeSenderPlan::init(
            ExchangeType::PASS_THROUGH,
            CompressionMode::NONE,
            "",
            [],
            0,
        );
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "ExchangeSender");
        assert_eq!(plan.query_block_offset(), 0);
        assert_eq!(plan.exchange_type(), ExchangeType::PASS_THROUGH);
        assert_eq!(plan.compression_mode(), CompressionMode::NONE);
    }

    #[test]
    fn pass_through_explain_matches_mpp_plan_tree() {
        let plan = PhysicalExchangeSenderPlan::init(
            ExchangeType::PASS_THROUGH,
            CompressionMode::NONE,
            "",
            [],
            0,
        );
        assert_eq!(plan.explain_info(), "ExchangeType: PassThrough");
    }

    #[test]
    fn hash_explain_preserves_compression_columns_tasks_and_streams() {
        let plan = PhysicalExchangeSenderPlan::init(
            ExchangeType::HASH,
            CompressionMode::FAST,
            "[name: a, collate: binary]",
            [3, 7],
            8,
        );
        assert_eq!(
            plan.explain_info(),
            "ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: a, collate: binary], tasks: [3, 7], stream_count: 8"
        );
        let high = PhysicalExchangeSenderPlan::init(
            ExchangeType::HASH,
            CompressionMode::HIGH_COMPRESSION,
            "",
            [],
            0,
        );
        assert_eq!(high.compression_mode(), CompressionMode::HIGH_COMPRESSION);
    }

    #[test]
    fn compression_names_and_unknown_exchange_values_match_source_fallbacks() {
        let unspecified = PhysicalExchangeSenderPlan::init(
            ExchangeType::BROADCAST,
            CompressionMode::UNSPECIFIED,
            "",
            [],
            0,
        );
        assert_eq!(
            unspecified.explain_info(),
            "ExchangeType: Broadcast, Compression: UNSPECIFIED"
        );
        let unknown = PhysicalExchangeSenderPlan::init(
            ExchangeType::from_raw(99),
            CompressionMode::from_raw(99),
            "",
            [],
            0,
        );
        assert_eq!(unknown.explain_info(), "ExchangeType: , Compression: NONE");
    }

    /// Source: `pkg/kv/version_test.go::TestExchangeCompressionMode`.
    #[test]
    #[allow(non_snake_case)]
    fn TestExchangeCompressionMode() {
        for (name, mode) in [
            ("UNSPECIFIED", CompressionMode::UNSPECIFIED),
            ("NONE", CompressionMode::NONE),
            ("FAST", CompressionMode::FAST),
            ("HIGH_COMPRESSION", CompressionMode::HIGH_COMPRESSION),
        ] {
            assert_eq!(mode.name(), name);
            assert_eq!(CompressionMode::parse(name), Some(mode));
            assert_eq!(
                CompressionMode::parse(&name.to_ascii_lowercase()),
                Some(mode)
            );
        }
        assert_eq!(CompressionMode::RECOMMENDED, CompressionMode::FAST);
        assert_eq!(CompressionMode::RECOMMENDED.to_tipb_raw(), 1);
        assert_eq!(CompressionMode::UNSPECIFIED.to_tipb_raw(), 0);
    }
}
