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

//! Session reuse/close state from `pkg/session/syssession/session.go`.
//!
//! A pooled internal session can be marked unsafe to reuse after a panic, but
//! remains usable until its owner closes it. Closing is idempotent and an
//! owner-gated close ignores callers that do not hold the current owner. This
//! leaf preserves those state transitions only; owner hooks, context closing,
//! in-use deferral, mutexes, and operation/transfer sequencing remain external.

/// Dependency-closed reuse and closed-state ring for an internal session.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionReuseState {
    closed: bool,
    avoid_reuse: bool,
}

impl SessionReuseState {
    /// Creates an open session that is eligible for reuse.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            closed: false,
            avoid_reuse: false,
        }
    }

    /// Returns whether the session has been closed.
    #[must_use]
    pub const fn is_closed(self) -> bool {
        self.closed
    }

    /// Returns whether the session should be removed from the reuse pool.
    #[must_use]
    pub const fn is_avoid_reuse(self) -> bool {
        self.avoid_reuse
    }

    /// Marks the session avoid-reuse when the caller is its current owner.
    ///
    /// `caller_is_owner` is the owner identity check performed by the Go
    /// session wrapper. A closed session has no owner and therefore cannot be
    /// newly marked through this operation.
    pub fn owner_mark_avoid_reuse(&mut self, caller_is_owner: bool) {
        if !self.closed && caller_is_owner {
            self.avoid_reuse = true;
        }
    }

    /// Closes the session when the caller is its current owner.
    ///
    /// Repeated closes are intentionally harmless, matching `doCloseWithoutLock`
    /// after the owner has already been cleared.
    pub fn owner_close(&mut self, caller_is_owner: bool) {
        if caller_is_owner {
            self.close();
        }
    }

    /// Closes the session without an owner check.
    pub const fn close(&mut self) {
        self.closed = true;
    }
}
