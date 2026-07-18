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

//! `JSON_ARRAYAGG` partial-result state from
//! `pkg/executor/aggfuncs/func_json_arrayagg.go`.
//!
//! TiDB stores each evaluated value in a source `[]any`, merges partial
//! slices in source order, and turns the values into one binary JSON array at
//! finalization. This leaf accepts already-serialized JSON fragments, keeping
//! value coercion, binary-JSON validation, chunk writes, spill encoding, and
//! aggregate scheduling outside the dependency-closed state owner.

use std::fmt;
use std::mem::size_of;

/// A value that can cross the JSON array aggregation boundary.
#[derive(Clone, Debug, PartialEq)]
pub enum JsonArrayValue {
    /// SQL NULL, represented by JSON `null` in an aggregate array.
    Null,
    /// A JSON boolean.
    Boolean(bool),
    /// A signed JSON integer.
    Signed(i64),
    /// An unsigned JSON integer.
    Unsigned(u64),
    /// A finite JSON number.
    Real(f64),
    /// A string value, escaped as one JSON string.
    String(String),
    /// A caller-validated JSON fragment (for JSON, DATE, or DURATION input).
    JsonFragment(String),
}

/// Errors from the dependency-closed JSON value conversion boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonArrayError {
    /// JSON cannot represent a non-finite IEEE-754 number.
    NonFiniteReal,
}

impl fmt::Display for JsonArrayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonFiniteReal => f.write_str("JSON number must be finite"),
        }
    }
}

impl JsonArrayValue {
    fn into_fragment(self) -> Result<String, JsonArrayError> {
        match self {
            Self::Null => Ok(String::from("null")),
            Self::Boolean(value) => Ok(value.to_string()),
            Self::Signed(value) => Ok(value.to_string()),
            Self::Unsigned(value) => Ok(value.to_string()),
            Self::Real(value) if value.is_finite() => Ok(value.to_string()),
            Self::Real(_) => Err(JsonArrayError::NonFiniteReal),
            Self::String(value) => Ok(quote_json_string(&value)),
            Self::JsonFragment(value) => Ok(value),
        }
    }
}

/// The source-shaped `partialResult4JsonArrayagg` state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JsonArrayAggState {
    entries: Vec<String>,
}

impl JsonArrayAggState {
    /// Creates an empty partial result.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Clears values while retaining the source slice allocation.
    pub fn reset(&mut self) {
        self.entries.clear();
    }

    /// Appends one evaluated value in source row order.
    pub fn append(&mut self, value: JsonArrayValue) -> Result<(), JsonArrayError> {
        self.entries.push(value.into_fragment()?);
        Ok(())
    }

    /// Appends an already-serialized JSON fragment.
    pub fn append_fragment(&mut self, fragment: impl Into<String>) {
        self.entries.push(fragment.into());
    }

    /// Merges another partial result after this one, preserving source order.
    pub fn merge_from(&mut self, source: &Self) {
        self.entries.extend(source.entries.iter().cloned());
    }

    /// Returns SQL NULL for an empty input, otherwise one JSON array.
    #[must_use]
    pub fn finish(&self) -> Option<String> {
        (!self.entries.is_empty()).then(|| format!("[{}]", self.entries.join(",")))
    }

    /// Returns the number of aggregated entries.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no entries have been aggregated.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the source partial-result allocation size.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<Self>()
    }

    /// Returns the source initial state plus empty-slice allocation size.
    #[must_use]
    pub const fn initial_allocation_size() -> usize {
        size_of::<Self>() + size_of::<Vec<String>>()
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
