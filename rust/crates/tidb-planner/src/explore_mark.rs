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

//! Memo exploration-round bit marks from `pkg/planner/memo/group.go`.
//!
//! The source stores one bit per transformation round on both groups and
//! group expressions. This leaf preserves set/clear/query semantics in a
//! fixed-width, copyable Rust value while leaving memo ownership external.

/// Bitset tracking which memo exploration rounds have completed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExploreMark(u64);

impl ExploreMark {
    /// Creates an unexplored mark.
    #[must_use]
    pub const fn new() -> Self {
        Self(0)
    }

    /// Marks one round as explored.
    pub fn set_explored(&mut self, round: usize) {
        if let Some(mask) = 1_u64.checked_shl(round as u32) {
            self.0 |= mask;
        }
    }

    /// Marks one round as unexplored.
    pub fn set_unexplored(&mut self, round: usize) {
        if let Some(mask) = 1_u64.checked_shl(round as u32) {
            self.0 &= !mask;
        }
    }

    /// Reports whether one round has been marked explored.
    #[must_use]
    pub fn explored(&self, round: usize) -> bool {
        1_u64
            .checked_shl(round as u32)
            .is_some_and(|mask| self.0 & mask != 0)
    }

    /// Returns the raw bit mask for diagnostics and serialization adapters.
    #[must_use]
    pub const fn bits(self) -> u64 {
        self.0
    }
}
