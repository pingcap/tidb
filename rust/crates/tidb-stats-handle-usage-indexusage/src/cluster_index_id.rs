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

//! Clustered-index identity selection from `indexusage.go`.
//!
//! The Go reporter chooses index ID zero for an integer primary-key handle,
//! chooses the primary index ID for a common handle, and declines reporting
//! for rowid/non-clustered tables. This leaf ports that pure metadata rule;
//! TiDB's full `model.TableInfo`, table wrappers, usage collector, runtime
//! statistics, and statement/session wiring remain external.

/// Minimal index metadata consumed by clustered-index selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexInfo {
    /// Source index identifier.
    pub id: i64,
    /// Whether this index is the table's primary index.
    pub primary: bool,
}

/// Minimal table metadata consumed by clustered-index selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterIndexTableInfo {
    /// True for an integer primary key used directly as the row handle.
    pub pk_is_handle: bool,
    /// True for a clustered common (non-integer) handle.
    pub is_common_handle: bool,
    /// Declared table indexes in source order.
    pub indices: Vec<IndexInfo>,
}

/// Returns the source clustered index ID, or `None` for rowid/non-clustered tables.
#[must_use]
pub fn cluster_index_id(table: &ClusterIndexTableInfo) -> Option<i64> {
    if table.pk_is_handle {
        return Some(0);
    }
    if table.is_common_handle {
        return Some(
            table
                .indices
                .iter()
                .find(|index| index.primary)
                .map_or(0, |index| index.id),
        );
    }
    None
}
