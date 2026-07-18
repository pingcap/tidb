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

//! Analyze table/partition identity from `pkg/statistics/analyze.go`.
//!
//! This leaf owns only the deterministic ID selection and formatting rules.
//! Analyze scheduling, storage persistence, partition metadata lookup, and
//! result lifecycle remain outside the Rust value object.

/// Partition ID sentinel used for a non-partitioned table.
pub const NON_PARTITION_TABLE_ID: i64 = -1;

/// The hybrid table/partition identity used while building statistics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyzeTableId {
    /// Logical table ID.
    pub table_id: i64,
    /// Physical partition ID, or [`NON_PARTITION_TABLE_ID`] for a table.
    pub partition_id: i64,
}

impl AnalyzeTableId {
    /// Creates a table/partition identity.
    #[must_use]
    pub const fn new(table_id: i64, partition_id: i64) -> Self {
        Self {
            table_id,
            partition_id,
        }
    }

    /// Chooses the physical ID used to build statistics.
    #[must_use]
    pub const fn statistics_id(self) -> i64 {
        if self.partition_id == NON_PARTITION_TABLE_ID {
            self.table_id
        } else {
            self.partition_id
        }
    }

    /// Returns whether this identity names a partition table.
    #[must_use]
    pub const fn is_partition_table(self) -> bool {
        self.partition_id != NON_PARTITION_TABLE_ID
    }

    /// Formats the source diagnostic representation: `partition => table`.
    #[must_use]
    pub fn display_string(self) -> String {
        format!("{} => {}", self.partition_id, self.table_id)
    }

    /// Compares two concrete identities by both IDs.
    #[must_use]
    pub const fn equals(self, other: Self) -> bool {
        self.table_id == other.table_id && self.partition_id == other.partition_id
    }

    /// Compares optional identities with the source's nil semantics.
    ///
    /// Go's pointer receiver returns true for two nil pointers, false for one
    /// nil pointer, and otherwise compares both fields. Rust callers express
    /// that boundary explicitly with `Option<&AnalyzeTableId>`.
    #[must_use]
    pub fn equals_optional(left: Option<&Self>, right: Option<&Self>) -> bool {
        left == right
    }
}
