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

//! SET_VAR hint restore metadata from `stmtctx.go`.
//!
//! A statement may apply a variable hint more than once while building a
//! plan. TiDB records the first old value for each variable so cleanup can
//! restore the state that existed before hint application. This leaf ports
//! that first-write-wins map only; hint parsing, sysvar mutation, warning
//! publication, and restore timing remain external.

use std::collections::BTreeMap;

/// First-write-wins old values for statement-local `SET_VAR` hints.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SetVarHintRestore {
    values: BTreeMap<String, String>,
}

impl SetVarHintRestore {
    /// Creates an empty restore registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an old value only when `name` has not been seen yet.
    pub fn record(&mut self, name: impl Into<String>, old_value: impl Into<String>) {
        self.values
            .entry(name.into())
            .or_insert_with(|| old_value.into());
    }

    /// Returns the first recorded old value for `name`.
    #[must_use]
    pub fn old_value(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    /// Returns all recorded names and old values in deterministic order.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Clears all restore metadata for a statement boundary.
    pub fn clear(&mut self) {
        self.values.clear();
    }
}
