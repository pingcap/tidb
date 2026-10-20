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

//! TiKV client metadata translated from TiDB's driver boundary.
//!
//! This module owns no client: the calls go through client-rust. It provides
//! source-exact enum mappings, trace metadata injection, and
//! already-observed backoff stats.

use std::collections::BTreeMap;

use tidb_proto::{KvrpcContext, KvrpcSourceStmt};

/// TiKV client-go replica-read values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum ClientReplicaReadType {
    /// Read from the leader.
    #[default]
    Leader = 0,
    /// Read from a follower.
    Follower = 1,
    /// Read from leaders, followers, and learners.
    Mixed = 2,
    /// Read from learners.
    Learner = 3,
    /// Prefer the leader and fall back to followers.
    PreferLeader = 4,
}

/// Maps TiDB's `kv.ReplicaReadType` numeric contract to client-go.
///
/// Closest and closest-adaptive both become mixed. An unknown source value
/// returns client-go's zero value, leader, matching Go's switch default.
#[must_use]
pub const fn map_replica_read_type(source: u8) -> ClientReplicaReadType {
    match source {
        1 => ClientReplicaReadType::Follower,
        2..=4 => ClientReplicaReadType::Mixed,
        5 => ClientReplicaReadType::Learner,
        6 => ClientReplicaReadType::PreferLeader,
        _ => ClientReplicaReadType::Leader,
    }
}

/// client-go endpoint values selected by `copr.getEndPointType`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum EndpointType {
    /// TiKV endpoint.
    #[default]
    TiKv = 0,
    /// TiFlash storage endpoint.
    TiFlash = 1,
    /// TiDB memory-backed endpoint.
    TiDb = 2,
    /// Disaggregated TiFlash compute endpoint.
    TiFlashCompute = 3,
}

/// Maps TiDB's `kv.StoreType` numeric contract to a client-go endpoint.
#[must_use]
pub const fn endpoint_type(source: u8, disaggregated_tiflash: bool) -> EndpointType {
    match source {
        1 if disaggregated_tiflash => EndpointType::TiFlashCompute,
        1 => EndpointType::TiFlash,
        2 => EndpointType::TiDb,
        _ => EndpointType::TiKv,
    }
}

/// Trace identity attached by TiDB's `injectTraceClient`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TraceInfo {
    /// Connection identifier from the tracing context.
    pub connection_id: u64,
    /// User-assigned session alias from the tracing context.
    pub session_alias: String,
}

/// Injects trace identity into the request context's SourceStmt.
///
/// A missing trace is a strict no-op. A present trace creates SourceStmt when
/// absent and replaces only connection ID and session alias, preserving its
/// start timestamp and statement ID exactly like the Go wrapper.
pub fn inject_source_stmt(context: &mut KvrpcContext, trace: Option<&TraceInfo>) {
    let Some(trace) = trace else {
        return;
    };
    let source_stmt = context
        .source_stmt
        .get_or_insert_with(KvrpcSourceStmt::default);
    source_stmt.connection_id = trace.connection_id;
    source_stmt.session_alias.clone_from(&trace.session_alias);
}

/// Already-observed backoff metadata, with no sleeping or retry scheduling.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackoffMetadata {
    max_sleep_ms: u64,
    total_sleep_ms: u64,
    times: BTreeMap<String, u64>,
    sleep_ms: BTreeMap<String, u64>,
}

impl BackoffMetadata {
    /// Creates metadata with the source maximum-sleep budget retained.
    #[must_use]
    pub fn new(max_sleep_ms: u64) -> Self {
        Self {
            max_sleep_ms,
            ..Self::default()
        }
    }

    /// Records a sleep that an external backoff owner already performed.
    pub fn observe(&mut self, kind: impl Into<String>, sleep_ms: u64) {
        let kind = kind.into();
        *self.times.entry(kind.clone()).or_default() += 1;
        *self.sleep_ms.entry(kind).or_default() += sleep_ms;
        self.total_sleep_ms += sleep_ms;
    }

    /// Returns the retained maximum-sleep budget.
    #[must_use]
    pub const fn max_sleep_ms(&self) -> u64 {
        self.max_sleep_ms
    }

    /// Returns observed counts by backoff type.
    #[must_use]
    pub const fn times(&self) -> &BTreeMap<String, u64> {
        &self.times
    }

    /// Returns observed sleep milliseconds by backoff type.
    #[must_use]
    pub const fn sleep_ms(&self) -> &BTreeMap<String, u64> {
        &self.sleep_ms
    }

    /// Returns total observed sleep milliseconds.
    #[must_use]
    pub const fn total_sleep_ms(&self) -> u64 {
        self.total_sleep_ms
    }
}
