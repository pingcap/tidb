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

//! Statement chunk-allocation usage state from `stmtctx.go`.
//!
//! A reuse allocator marks the current statement when it has served a chunk
//! from the pool. This leaf ports only the source set/clear/read boolean; the
//! allocator, pool, column reuse, and SessionVars lifecycle remain external.

/// Whether the current statement has used a reusable chunk allocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChunkAllocationStatus {
    used: bool,
}

impl ChunkAllocationStatus {
    /// Creates a status with no allocation use recorded.
    #[must_use]
    pub const fn new() -> Self {
        Self { used: false }
    }

    /// Marks the current statement as having used a reusable chunk.
    pub fn set_used(&mut self) {
        self.used = true;
    }

    /// Clears the current statement's reusable-chunk usage marker.
    pub fn clear(&mut self) {
        self.used = false;
    }

    /// Returns the source usage marker.
    pub const fn is_used(self) -> bool {
        self.used
    }
}
