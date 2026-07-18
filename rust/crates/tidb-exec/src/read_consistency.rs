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

//! Read-consistency value semantics from `session.go`.
//!
//! The source keeps `ReadConsistencyLevel` as a validated string: strict is
//! the default, weak reads are recognized only by the exact lower-case
//! `"weak"` value, and system-variable validation accepts either label
//! case-insensitively. This leaf owns that value/validation boundary only;
//! request isolation, transaction state, and non-transactional DML policy
//! remain external consumers.

/// Canonical strict-read consistency label.
pub const READ_CONSISTENCY_STRICT: &str = "strict";
/// Canonical weak-read consistency label.
pub const READ_CONSISTENCY_WEAK: &str = "weak";

/// Validated or source-preserving read-consistency level.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReadConsistencyLevel(String);

impl Default for ReadConsistencyLevel {
    fn default() -> Self {
        Self::strict()
    }
}

impl ReadConsistencyLevel {
    /// Creates a source-preserving value without validation.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Creates the canonical strict level.
    #[must_use]
    pub fn strict() -> Self {
        Self::new(READ_CONSISTENCY_STRICT)
    }

    /// Creates the canonical weak level.
    #[must_use]
    pub fn weak() -> Self {
        Self::new(READ_CONSISTENCY_WEAK)
    }

    /// Validates and normalizes a system-variable value.
    ///
    /// This mirrors `validateReadConsistencyLevel`: only strict and weak are
    /// accepted, and their stored representation is canonical lower-case.
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            READ_CONSISTENCY_STRICT => Some(Self::strict()),
            READ_CONSISTENCY_WEAK => Some(Self::weak()),
            _ => None,
        }
    }

    /// Returns the source string representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns true only for the exact canonical weak value.
    pub fn is_weak(&self) -> bool {
        self.0 == READ_CONSISTENCY_WEAK
    }
}

impl From<&str> for ReadConsistencyLevel {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}
