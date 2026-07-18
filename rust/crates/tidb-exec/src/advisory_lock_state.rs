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

//! Advisory-lock reference state from `pkg/session/advisory_locks.go`.
//!
//! TiDB keeps one private pessimistic transaction per advisory lock and uses a
//! reference count so repeated `GET_LOCK` calls release only after matching
//! `RELEASE_LOCK` calls. This leaf ports the owner identity and reference
//! counter only. SQL transactions, lock-name normalization/validation,
//! timeout handling, rollback, and session cleanup remain external.

/// Pure metadata for one session-owned advisory lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdvisoryLockState {
    reference_count: i64,
    owner: u64,
}

impl AdvisoryLockState {
    /// Creates a lock state with no references for `owner`.
    #[must_use]
    pub const fn new(owner: u64) -> Self {
        Self {
            reference_count: 0,
            owner,
        }
    }

    /// Increments the source reference count after a successful acquisition.
    pub fn incr_references(&mut self) {
        self.reference_count = self.reference_count.wrapping_add(1);
    }

    /// Decrements the source reference count after a release.
    pub fn decr_references(&mut self) {
        self.reference_count = self.reference_count.wrapping_sub(1);
    }

    /// Returns the current source reference count.
    #[must_use]
    pub const fn reference_count(self) -> i64 {
        self.reference_count
    }

    /// Returns the session/connection owner identity.
    #[must_use]
    pub const fn owner(self) -> u64 {
        self.owner
    }
}
