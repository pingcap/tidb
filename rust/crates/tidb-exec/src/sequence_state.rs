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

//! Dependency-closed session sequence state from TiDB's `SequenceState`.
//!
//! TiDB keeps the latest `NEXTVAL` result per numeric sequence ID so
//! `LASTVAL` and session migration can read it later. This leaf owns only the
//! map and its copy/lookup/update semantics. The SQL sequence catalog,
//! expression evaluation, JSON session-state envelope, and synchronization
//! with a live session remain outside this value owner.

use std::collections::BTreeMap;

/// Session-local latest values returned by sequence `NEXTVAL`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SequenceState {
    latest_values: BTreeMap<i64, i64>,
}

impl SequenceState {
    /// Creates an empty sequence state map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the latest value obtained from a sequence.
    pub fn update_state(&mut self, sequence_id: i64, value: i64) {
        self.latest_values.insert(sequence_id, value);
    }

    /// Returns the cached value, or `None` when the sequence has not been read.
    ///
    /// The Go method returns `(0, true, nil)` for this missing case; `Option`
    /// removes that sentinel while preserving the observable distinction.
    #[must_use]
    pub fn get_last_value(&self, sequence_id: i64) -> Option<i64> {
        self.latest_values.get(&sequence_id).copied()
    }

    /// Returns a copied map suitable for session-state serialization.
    #[must_use]
    pub fn get_all_states(&self) -> BTreeMap<i64, i64> {
        self.latest_values.clone()
    }

    /// Merges serialized state into the current map, preserving unrelated keys.
    ///
    /// This is the source `maps.Copy` behavior rather than replacement.
    pub fn set_all_states(&mut self, states: &BTreeMap<i64, i64>) {
        self.latest_values.extend(states);
    }
}
