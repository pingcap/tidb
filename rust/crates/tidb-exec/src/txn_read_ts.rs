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

//! Dependency-closed metadata for TiDB's `tx_read_ts` session variable.
//!
//! The Go source keeps this value in `SessionVars` as a small two-field
//! object.  A stale-read owner consumes it once when a transaction starts;
//! the session boundary then clears it after that transaction.  This module
//! ports only those value transitions.  It does not parse wall-clock values,
//! obtain timestamps from an oracle, construct a snapshot, or clear a live
//! session's `SnapshotInfoschema` pointer.

/// The pending read timestamp and whether a consumer has used it.
///
/// The Rust owner is always present, matching the normal `SessionVars`
/// initialization (`NewTxnReadTS(0)`).  Callers that model the Go nil pointer
/// should keep this value in an `Option` and skip calls when it is `None`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TxnReadTs {
    read_ts: u64,
    used: bool,
}

impl TxnReadTs {
    /// Creates an unused pending read timestamp.
    pub const fn new(read_ts: u64) -> Self {
        Self {
            read_ts,
            used: false,
        }
    }

    /// Returns the configured timestamp and marks it consumed.
    ///
    /// Consumption is recorded even for timestamp zero, just as Go's
    /// `UseTxnReadTS` sets `used` before returning the stored value.  Cleanup
    /// deliberately ignores a consumed zero value below.
    pub fn use_read_ts(&mut self) -> u64 {
        self.used = true;
        self.read_ts
    }

    /// Replaces the timestamp and makes it available to the next consumer.
    pub const fn set_read_ts(&mut self, read_ts: u64) {
        self.used = false;
        self.read_ts = read_ts;
    }

    /// Returns the timestamp without changing its consumed state.
    pub const fn peek(&self) -> u64 {
        self.read_ts
    }

    /// Returns whether a consumer has called [`Self::use_read_ts`].
    pub const fn is_used(&self) -> bool {
        self.used
    }

    /// Clears a consumed, non-zero timestamp and reports whether it was reset.
    ///
    /// Go's `CleanupTxnReadTSIfUsed` also clears `SessionVars.SnapshotInfoschema`
    /// when this returns true.  That session-owned side effect remains outside
    /// this value leaf; a future session owner can use the boolean to perform
    /// the corresponding reset at the transaction boundary.
    pub fn cleanup_if_used(&mut self) -> bool {
        if self.used && self.read_ts > 0 {
            *self = Self::new(0);
            true
        } else {
            false
        }
    }
}
