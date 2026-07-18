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

//! InfoSchema-V1 partition-to-table cache metadata from
//! `pkg/statistics/handle/util/table_info.go`.
//!
//! The Go owner rebuilds its map when the InfoSchema schema version changes,
//! then uses the cached parent table ID to avoid a full partition scan. This
//! leaf accepts already-extracted `(partition_id, table_id)` pairs and keeps
//! only that versioned lookup policy. InfoSchema V1/V2 detection, table-item
//! resolution, locking, and schema traversal remain external boundaries.

use std::collections::HashMap;

/// Versioned partition-ID to parent-table-ID cache.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PartitionTableIdCache {
    schema_version: i64,
    partition_to_table: HashMap<i64, i64>,
}

impl PartitionTableIdCache {
    /// Create an empty cache with the source's zero schema-version sentinel.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve a partition ID, rebuilding the map when the schema version changes.
    ///
    /// `partition_pairs` should contain the current InfoSchema partition
    /// definitions. Duplicate partition IDs follow the source map assignment
    /// rule: the last pair wins. Callers that share this cache across threads
    /// must provide synchronization, matching the source mutex boundary.
    pub fn lookup(
        &mut self,
        schema_version: i64,
        partition_pairs: &[(i64, i64)],
        partition_id: i64,
    ) -> Option<i64> {
        if schema_version != self.schema_version {
            self.schema_version = schema_version;
            self.partition_to_table.clear();
            self.partition_to_table
                .extend(partition_pairs.iter().copied());
        }
        self.partition_to_table.get(&partition_id).copied()
    }

    /// Return the schema version represented by this cache.
    #[must_use]
    pub const fn schema_version(&self) -> i64 {
        self.schema_version
    }

    /// Return the number of cached partition mappings.
    #[must_use]
    pub fn len(&self) -> usize {
        self.partition_to_table.len()
    }

    /// Return whether no partition mappings are cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.partition_to_table.is_empty()
    }
}
