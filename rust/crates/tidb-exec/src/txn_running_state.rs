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

//! TIDB_TRX transaction-running states from `pkg/session/txninfo/txn_info.go`.
//!
//! This leaf preserves the source integer discriminants and display labels
//! used by the transaction information datasource. It does not observe or
//! mutate live transaction state, collect lock/timing metrics, or publish
//! infoschema rows; those remain session, storage, and infoschema owners.

/// Current execution state of a transaction.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TxnRunningState {
    /// Waiting for the user's next statement.
    Idle = 0,
    /// Executing a statement.
    Running = 1,
    /// Trying to acquire a lock.
    LockAcquiring = 2,
    /// Trying to commit.
    Committing = 3,
    /// Rolling back.
    RollingBack = 4,
}

/// Number of concrete transaction-running states in the source registry.
pub const TXN_STATE_COUNTER: usize = 5;

/// Source display labels for `TxnRunningState` values, in discriminant order.
pub const TXN_RUNNING_STATE_LABELS: [&str; TXN_STATE_COUNTER] = [
    "Idle",
    "Running",
    "LockWaiting",
    "Committing",
    "RollingBack",
];

impl TxnRunningState {
    /// Returns the source display label used by `TIDB_TRX.STATE`.
    #[must_use]
    pub const fn label(self) -> &'static str {
        TXN_RUNNING_STATE_LABELS[self as usize]
    }

    /// Returns the source integer discriminant.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self as i32
    }
}
