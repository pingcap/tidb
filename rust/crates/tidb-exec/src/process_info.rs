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

//! Shallow process-information metadata from
//! `pkg/session/sessmgr/processinfo.go`.
//!
//! TiDB's `ProcessInfo.Clone` copies the process-info record without cloning
//! the statement context, reference counter, memory tracker, or other live
//! owners. This leaf keeps the same typed boundary for the fields covered by
//! the source contract. Process-list rendering, tracker accounting, command
//! decoding, TLS state, and session-manager ownership remain external.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Opaque marker for a live statement context owned by the session layer.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct StatementContextMarker;

/// Opaque marker for the statement-context reference counter.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct StatementReferenceCounterMarker;

/// Opaque marker for a live memory tracker.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct MemoryTrackerMarker;

/// The source-shaped callback type carried by `ProcessInfo.StatsInfo`.
pub type StatsInfoFn = fn(&dyn Any) -> BTreeMap<String, u64>;

/// Process-list metadata whose live owners are intentionally borrowed by
/// shared pointers when the record is cloned.
#[derive(Clone)]
pub struct ProcessInfo {
    /// Connection identifier.
    pub id: u64,
    /// Authenticated user name.
    pub user: String,
    /// Remote host address.
    pub host: String,
    /// Current database name.
    pub db: String,
    /// Current SQL text.
    pub info: String,
    /// Transaction start timestamp.
    pub cur_txn_start_ts: u64,
    /// Optional runtime statistics callback.
    pub stats_info: Option<StatsInfoFn>,
    /// Shared statement-context owner.
    pub stmt_ctx: Option<Arc<StatementContextMarker>>,
    /// Shared statement-context reference-count owner.
    pub ref_count_of_stmt_ctx: Option<Arc<StatementReferenceCounterMarker>>,
    /// Shared memory-tracker owner.
    pub mem_tracker: Option<Arc<MemoryTrackerMarker>>,
    /// Redaction mode/text carried by the process record.
    pub redact_sql: String,
    /// Session alias carried by the process record.
    pub session_alias: String,
}

impl ProcessInfo {
    /// Returns a shallow record clone, preserving shared live-owner identity.
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        self.clone()
    }
}
