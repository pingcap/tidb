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

//! Dependency-closed physical-property classifications from
//! `pkg/planner/property/physical_property.go`.
//!
//! This leaf ports only the integer classification and boolean matching
//! contracts. The source's expression columns, protobuf exchange enum,
//! functional-dependency sets, and physical-property construction remain
//! owned by future planner layers.

/// MPP exchange partitioning requirement.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MppPartitionType {
    /// No special partitioning requirement.
    Any,
    /// Broadcast rows to every MPP worker.
    Broadcast,
    /// Hash-partition rows by exchange columns.
    Hash,
    /// Send all rows to one worker.
    SinglePartition,
    /// Unknown source integer, retained for compatibility.
    Unknown(i32),
}

/// Exchange wire classification produced by the source mapping.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExchangeKind {
    /// Broadcast exchange.
    Broadcast,
    /// Hash exchange.
    Hash,
    /// Pass-through exchange, including `Any` and single-partition fallback.
    PassThrough,
}

impl MppPartitionType {
    /// Converts source integer values to a typed partition requirement.
    #[must_use]
    pub const fn from_raw(raw: i32) -> Self {
        match raw {
            0 => Self::Any,
            1 => Self::Broadcast,
            2 => Self::Hash,
            3 => Self::SinglePartition,
            other => Self::Unknown(other),
        }
    }

    /// Returns the source integer value.
    #[must_use]
    pub const fn raw(self) -> i32 {
        match self {
            Self::Any => 0,
            Self::Broadcast => 1,
            Self::Hash => 2,
            Self::SinglePartition => 3,
            Self::Unknown(raw) => raw,
        }
    }

    /// Returns the source `ToExchangeType` mapping.
    #[must_use]
    pub const fn exchange_kind(self) -> ExchangeKind {
        match self {
            Self::Broadcast => ExchangeKind::Broadcast,
            Self::Hash => ExchangeKind::Hash,
            Self::Any | Self::SinglePartition | Self::Unknown(_) => ExchangeKind::PassThrough,
        }
    }
}

/// Whether a physical property matched directly or needs a merge sort.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PhysicalPropMatchResult {
    /// Required order cannot be satisfied.
    NotMatched,
    /// Required order is satisfied directly.
    Matched,
    /// Required order is satisfied after a merge sort.
    MatchedNeedMergeSort,
}

impl PhysicalPropMatchResult {
    /// Returns whether the property is considered matched by the source.
    #[must_use]
    pub const fn matched(self) -> bool {
        matches!(self, Self::Matched | Self::MatchedNeedMergeSort)
    }
}

/// Ordering work a source index path would have to perform for a task.
///
/// `findBestTask4LogicalDataSource` permits several ordering forms.  The
/// bounded index-only transition has no `KeepOrder`, partial-order, or
/// range-group merge-sort attachment yet, so callers must describe those
/// requests and receive an explicit invalid task instead of losing them.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum IndexOrderingRequirement {
    /// The parent has no ordering requirement.
    None,
    /// The index scan must preserve full order.
    KeepOrder,
    /// The source partial-order optimization is required.
    PartialOrder,
    /// The source grouped-range merge-sort path is required.
    MergeSort,
}
