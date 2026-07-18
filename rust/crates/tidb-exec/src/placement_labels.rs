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

//! Deterministic store-label aggregation from `pkg/executor/show_placement.go`.
//!
//! TiDB groups store-label values by label key, deduplicates each value, and
//! emits keys and values in lexicographic order. BinaryJSON decoding,
//! placement retrieval, and SQL-row encoding remain outside this value leaf.

use std::collections::{BTreeMap, BTreeSet};

/// A decoded placement label from one store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreLabel {
    /// Label key, such as `zone` or `rack`.
    pub key: String,
    /// Label value, such as `z1` or `r3`.
    pub value: String,
}

impl StoreLabel {
    /// Creates a decoded store label.
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Accumulates and deterministically orders store labels for SHOW PLACEMENT.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlacementLabels {
    values_by_key: BTreeMap<String, BTreeSet<String>>,
}

impl PlacementLabels {
    /// Adds one decoded store's labels. A missing store is a no-op, matching a
    /// nil store-label array in the source test fixture.
    pub fn append_store_labels(&mut self, labels: Option<&[StoreLabel]>) {
        let Some(labels) = labels else {
            return;
        };
        for label in labels {
            self.values_by_key
                .entry(label.key.clone())
                .or_default()
                .insert(label.value.clone());
        }
    }

    /// Returns `(key, sorted_unique_values)` rows in source order.
    #[must_use]
    pub fn build_rows(&self) -> Vec<(String, Vec<String>)> {
        self.values_by_key
            .iter()
            .map(|(key, values)| (key.clone(), values.iter().cloned().collect()))
            .collect()
    }
}
