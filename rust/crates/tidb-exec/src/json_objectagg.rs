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

//! `JSON_OBJECTAGG` partial-result state from
//! `pkg/executor/aggfuncs/func_json_objectagg.go`.
//!
//! The Go implementation keeps a memory-aware map from evaluated string keys
//! to JSON-compatible values. Duplicate keys are replaced by the last value,
//! partial merges apply source entries after destination entries, and final
//! BinaryJSON encoding sorts keys lexicographically. This leaf preserves that
//! state contract over canonical JSON fragments. Typed key/value evaluation,
//! charset checks, BinaryJSON validation, spill encoding, and memory tracker
//! accounting remain external.

use std::collections::BTreeMap;
use std::fmt;
use std::mem::size_of;

/// The source initial map-bucket memory constant for string-to-interface maps.
pub const MAP_BUCKET_MEMORY: usize = 312;

/// Errors from the dependency-closed JSON object key/value boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonObjectError {
    /// Go rejects a NULL key before mutating the partial map.
    NullKey,
    /// Go rejects a binary-charset key before mutating the partial map.
    BinaryKeyCharset,
}

impl fmt::Display for JsonObjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NullKey => f.write_str("JSON document NULL key"),
            Self::BinaryKeyCharset => f.write_str("invalid JSON key charset"),
        }
    }
}

/// The source-shaped `partialResult4JsonObjectAgg` state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JsonObjectAggState {
    entries: BTreeMap<String, String>,
    _map_bytes: u64,
}

impl JsonObjectAggState {
    /// Creates an empty partial result.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            _map_bytes: 0,
        }
    }

    /// Resets entries while keeping the state owner allocated.
    pub fn reset(&mut self) {
        self.entries.clear();
        self._map_bytes = 0;
    }

    /// Inserts a value under a non-NULL, non-binary key.
    ///
    /// Returns whether the key was newly inserted. Existing keys are replaced
    /// exactly like `MemAwareMap.Set`, so the last value wins.
    pub fn insert(&mut self, key: &str, fragment: impl Into<String>) -> bool {
        self.insert_fragment(key, fragment)
    }

    /// Inserts a value while exposing the source NULL/charset checks.
    pub fn insert_optional(
        &mut self,
        key: Option<&str>,
        binary_charset: bool,
        fragment: impl Into<String>,
    ) -> Result<bool, JsonObjectError> {
        let key = key.ok_or(JsonObjectError::NullKey)?;
        if binary_charset {
            return Err(JsonObjectError::BinaryKeyCharset);
        }
        let existed = self
            .entries
            .insert(key.to_owned(), fragment.into())
            .is_some();
        self._map_bytes = self.entries.len() as u64;
        Ok(!existed)
    }

    /// Inserts an already-serialized JSON fragment and returns new-key status.
    pub fn insert_fragment(&mut self, key: &str, fragment: impl Into<String>) -> bool {
        let existed = self
            .entries
            .insert(key.to_owned(), fragment.into())
            .is_some();
        self._map_bytes = self.entries.len() as u64;
        !existed
    }

    /// Merges source entries after destination entries; source duplicates win.
    pub fn merge_from(&mut self, source: &Self) {
        for (key, value) in &source.entries {
            self.entries.insert(key.clone(), value.clone());
        }
        self._map_bytes = self.entries.len() as u64;
    }

    /// Returns SQL NULL for an empty input, otherwise a canonical JSON object.
    #[must_use]
    pub fn finish(&self) -> Option<String> {
        (!self.entries.is_empty()).then(|| {
            let fields = self
                .entries
                .iter()
                .map(|(key, value)| format!("{}:{value}", quote_json_string(key)))
                .collect::<Vec<_>>();
            format!("{{{}}}", fields.join(","))
        })
    }

    /// Returns the number of distinct keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no keys have been aggregated.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the source partial-result allocation size.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<Self>()
    }

    /// Returns source partial state plus the initial map bucket allocation.
    #[must_use]
    pub const fn initial_allocation_size() -> usize {
        size_of::<Self>() + MAP_BUCKET_MEMORY
    }
}

fn quote_json_string(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        match ch {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\u{08}' => quoted.push_str("\\b"),
            '\u{0C}' => quoted.push_str("\\f"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            ch if ch.is_control() => {
                use std::fmt::Write;
                write!(quoted, "\\u{:04x}", ch as u32).expect("String write cannot fail");
            }
            ch => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}
