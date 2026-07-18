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

//! Column/index statistics existence metadata from `pkg/statistics/table.go`.
//!
//! The map records whether a column or index is known and whether its stats
//! are analyzed. It deliberately does not trigger loading, inspect a table
//! schema, or connect this metadata to the statistics handle.

use std::collections::HashMap;

/// Metadata map distinguishing known and analyzed columns and indices.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColAndIdxExistenceMap {
    columns: HashMap<i64, bool>,
    indices: HashMap<i64, bool>,
}

impl ColAndIdxExistenceMap {
    /// Creates an empty map with source-compatible capacity hints.
    #[must_use]
    pub fn new(column_capacity: usize, index_capacity: usize) -> Self {
        Self {
            columns: HashMap::with_capacity(column_capacity),
            indices: HashMap::with_capacity(index_capacity),
        }
    }

    /// Creates an empty map using the default Rust map capacity.
    #[must_use]
    pub fn new_without_size() -> Self {
        Self::default()
    }

    /// Removes a column ID from the map.
    pub fn delete_column_not_found(&mut self, id: i64) {
        self.columns.remove(&id);
    }

    /// Removes an index ID from the map.
    pub fn delete_index_not_found(&mut self, id: i64) {
        self.indices.remove(&id);
    }

    /// Returns whether a known column/index also has analyzed statistics.
    #[must_use]
    pub fn has_analyzed(&self, id: i64, is_index: bool) -> bool {
        self.map(is_index).get(&id).copied().unwrap_or(false)
    }

    /// Returns whether a column/index ID is known, regardless of analysis.
    #[must_use]
    pub fn has(&self, id: i64, is_index: bool) -> bool {
        self.map(is_index).contains_key(&id)
    }

    /// Inserts or replaces column metadata.
    pub fn insert_column(&mut self, id: i64, analyzed: bool) {
        self.columns.insert(id, analyzed);
    }

    /// Inserts or replaces index metadata.
    pub fn insert_index(&mut self, id: i64, analyzed: bool) {
        self.indices.insert(id, analyzed);
    }

    /// Returns whether no column or index metadata is present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty() && self.indices.is_empty()
    }

    /// Returns the number of known columns.
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Deep-copies both metadata maps.
    #[must_use]
    pub fn deep_clone(&self) -> Self {
        self.clone()
    }

    /// Compares both metadata maps.
    #[must_use]
    pub fn is_equal(&self, other: &Self) -> bool {
        self == other
    }

    fn map(&self, is_index: bool) -> &HashMap<i64, bool> {
        if is_index {
            &self.indices
        } else {
            &self.columns
        }
    }
}
