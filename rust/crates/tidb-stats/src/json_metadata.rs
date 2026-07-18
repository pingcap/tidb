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

//! Statistics JSON ordering metadata from
//! `pkg/statistics/util/json_objects.go`.
//!
//! The Go JSON owner sorts predicate-column IDs before test serialization and
//! exposes the global-statistics marker. This leaf keeps only that deterministic
//! metadata boundary; tipb JSON structures, encoding, storage blocks, and
//! statistics-handle integration remain outside the crate.

/// Marker used by TiDB for global statistics on partitioned tables.
pub const TIDB_GLOBAL_STATS: &str = "global";

/// Caller-owned predicate-column metadata needed for deterministic ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonPredicateColumn {
    /// Column ID used by the source comparator.
    pub id: i64,
}

impl JsonPredicateColumn {
    /// Creates predicate metadata from a source column ID.
    #[must_use]
    pub const fn new(id: i64) -> Self {
        Self { id }
    }
}

/// Partial JSON-table shape that owns predicate-column ordering.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JsonTable {
    /// Predicate columns emitted by statistics dumping.
    pub predicate_columns: Vec<JsonPredicateColumn>,
}

impl JsonTable {
    /// Sorts predicate columns by source ID ordering for deterministic output.
    pub fn sort(&mut self) {
        self.predicate_columns
            .sort_unstable_by_key(|column| column.id);
    }
}
