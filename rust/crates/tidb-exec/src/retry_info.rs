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

//! Dependency-closed retry metadata from TiDB's `variable.RetryInfo`.
//!
//! A retry reuses auto-increment and auto-random values allocated by the
//! original attempt. This module owns only the deterministic queues and
//! offsets. It does not start a transaction, rebuild a plan, invalidate a
//! session, or delete prepared statements from a plan cache.

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct AutoIdQueue {
    ids: Vec<i64>,
    offset: usize,
}

impl AutoIdQueue {
    fn add(&mut self, id: i64) {
        self.ids.push(id);
    }

    fn reset_offset(&mut self) {
        self.offset = 0;
    }

    fn clean(&mut self) {
        self.offset = 0;
        self.ids.clear();
    }

    fn next(&mut self) -> Option<i64> {
        let id = self.ids.get(self.offset).copied()?;
        self.offset += 1;
        Some(id)
    }
}

/// Session-scoped deterministic metadata consumed while retrying a statement.
///
/// The public fields mirror the source fields used by the session retry loop.
/// Auto-ID queues stay private so callers can only consume values through the
/// source-shaped methods below, preserving the offset invariant.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetryInfo {
    /// Whether the session is currently replaying its statement history.
    pub retrying: bool,
    /// Prepared statement IDs that the session boundary must clean after retry.
    pub dropped_prepared_stmt_ids: Vec<u32>,
    /// The last read timestamp used by retry-aware read-committed execution.
    pub last_rc_read_ts: u64,
    auto_increment_ids: AutoIdQueue,
    auto_random_ids: AutoIdQueue,
}

impl RetryInfo {
    /// Clears replay queues and dropped prepared-statement IDs.
    ///
    /// The source deliberately does not change `Retrying` or `LastRcReadTS`;
    /// those belong to the surrounding retry/session lifecycle.
    pub fn clean(&mut self) {
        self.auto_increment_ids.clean();
        self.auto_random_ids.clean();
        self.dropped_prepared_stmt_ids.clear();
    }

    /// Rewinds both queues for another pass over the statement history.
    pub fn reset_offset(&mut self) {
        self.auto_increment_ids.reset_offset();
        self.auto_random_ids.reset_offset();
    }

    /// Appends an auto-increment value allocated by the original attempt.
    pub fn add_auto_increment_id(&mut self, id: i64) {
        self.auto_increment_ids.add(id);
    }

    /// Returns the next replayed auto-increment value, if one remains.
    pub fn next_auto_increment_id(&mut self) -> Option<i64> {
        self.auto_increment_ids.next()
    }

    /// Appends an auto-random value allocated by the original attempt.
    pub fn add_auto_random_id(&mut self, id: i64) {
        self.auto_random_ids.add(id);
    }

    /// Returns the next replayed auto-random value, if one remains.
    pub fn next_auto_random_id(&mut self) -> Option<i64> {
        self.auto_random_ids.next()
    }
}
