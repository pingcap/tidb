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

//! Cascades memo group identifiers from
//! `pkg/planner/cascades/memo/group_id_generator.go`.
//!
//! The source owns a single-threaded monotonically incrementing `uint64`
//! counter. This leaf preserves its one-based sequence and Go-style wrapping
//! at `uint64::MAX` without introducing memo/group dependencies.

/// Stable identifier assigned to a cascades memo group.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct GroupId(u64);

impl GroupId {
    /// Creates an identifier from its source integer representation.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the source integer representation.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Single-threaded generator for memo group identifiers.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GroupIdGenerator {
    id: u64,
}

impl GroupIdGenerator {
    /// Creates a generator whose first identifier is one.
    #[must_use]
    pub const fn new() -> Self {
        Self { id: 0 }
    }

    /// Creates a generator with an explicit current counter.
    ///
    /// The source test adjusts its private counter directly; this constructor
    /// provides the same deterministic setup without exposing mutable state.
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self { id: raw }
    }

    /// Generates the next group identifier, wrapping like a Go `uint64`.
    pub fn next_group_id(&mut self) -> GroupId {
        self.id = self.id.wrapping_add(1);
        GroupId::new(self.id)
    }
}
