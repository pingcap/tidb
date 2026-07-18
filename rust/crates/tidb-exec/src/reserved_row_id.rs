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

//! Dependency-closed reserved row-ID allocation from TiDB's statement context.
//!
//! `ReservedRowIDAlloc` is deliberately just an inclusive/exclusive counter:
//! the next value is `base + 1` while `base < max`, and exhaustion is
//! represented by `base >= max`. Reservation, auto-ID service calls, table
//! mutation, and statement-context reset remain outside this value owner.

/// A bounded sequence of already-reserved row IDs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReservedRowIdAllocator {
    base: i64,
    max: i64,
}

impl ReservedRowIdAllocator {
    /// Replaces the reservation with the source `(base, max)` pair.
    pub const fn reset(&mut self, base: i64, max: i64) {
        self.base = base;
        self.max = max;
    }

    /// Consumes and returns the next reserved ID, or `None` when exhausted.
    pub fn consume(&mut self) -> Option<i64> {
        if self.base < self.max {
            self.base += 1;
            Some(self.base)
        } else {
            None
        }
    }

    /// Returns whether no reserved ID remains.
    pub const fn is_exhausted(&self) -> bool {
        self.base >= self.max
    }
}
