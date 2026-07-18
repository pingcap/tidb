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

//! Lazy transaction state predicates from `pkg/session/txn.go`.
//!
//! TiDB distinguishes an active valid transaction, a not-yet-materialized
//! transaction future, and an empty lazy wrapper. This leaf preserves only the
//! boolean composition; KV transaction validity, future activation, locking,
//! and commit/rollback lifecycle remain external.

/// The dependency-closed state consumed by TiDB's lazy transaction predicates.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LazyTxnState {
    transaction_exists: bool,
    transaction_valid: bool,
    future_exists: bool,
}

impl LazyTxnState {
    /// Creates a state with explicit transaction/future presence facts.
    #[must_use]
    pub const fn new(
        transaction_exists: bool,
        transaction_valid: bool,
        future_exists: bool,
    ) -> Self {
        Self {
            transaction_exists,
            transaction_valid,
            future_exists,
        }
    }

    /// Mirrors `LazyTxn.Valid`: an allocated transaction must also be valid.
    #[must_use]
    pub const fn valid(self) -> bool {
        self.transaction_exists && self.transaction_valid
    }

    /// Mirrors `LazyTxn.pending`: no transaction yet, but a future exists.
    #[must_use]
    pub const fn pending(self) -> bool {
        !self.transaction_exists && self.future_exists
    }

    /// Mirrors `LazyTxn.validOrPending`: a future or a valid transaction is
    /// sufficient, even if both facts are present during a transition.
    #[must_use]
    pub const fn valid_or_pending(self) -> bool {
        self.future_exists || self.valid()
    }
}
