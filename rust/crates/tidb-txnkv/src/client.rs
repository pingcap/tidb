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
//! This module deliberately owns no socket, gRPC channel, PD client, region
//! cache, lock resolver, retry scheduler, TLS setup, or cancellation. It
//! provides only source-exact enum mappings, immutable driver option
//! projection, trace metadata injection, and already-observed backoff stats.

use std::collections::BTreeMap;
use std::time::Duration;

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

/// Immutable request presented to an address-directed unary TiKV client.
///
/// DistSQL owns construction of the encoded coprocessor body. The KV client
/// owns only dispatch and therefore cannot reach upward into planner or
/// DistSQL types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectUnaryRequest {
    /// Endpoint selected before dispatch. This bounded runtime admits TiKV.
    pub endpoint: EndpointType,
    /// Replica selection copied from client-go's request wrapper.
    pub replica_read_type: ClientReplicaReadType,
    /// Whether follower-capable replica routing is active.
    pub replica_read: bool,
    /// Whether this is a stale-read attempt.
    pub stale_read: bool,
    /// Request source before client-go appends retry metadata.
    pub input_request_source: String,
    /// Predicted read bytes used by resource control.
    pub predicted_read_bytes: u64,
    /// Replica scope retained by the request wrapper.
    pub read_replica_scope: String,
    /// Transaction scope retained by the request wrapper.
    pub txn_scope: String,
    /// Exact decoded context also encoded into the request body.
    pub context: KvrpcContext,
    /// Exact encoded `coprocessor.Request` body.
    pub encoded_request: Vec<u8>,
}

/// Raw successful result from an address-directed unary TiKV client.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DirectUnaryResponse {
    /// Exact encoded `coprocessor.Response` body.
    pub encoded_response: Vec<u8>,
}

/// Pinned client-go `Client.SendRequest` capability projection.
///
/// A concrete implementation may own gRPC channels and connection pools, but
/// this trait owns none. The selected address and timeout stay explicit, the
/// request is immutable for the duration of the call, and transport failure
/// remains separate from a successful raw response.
pub trait DirectUnaryClient {
    /// Sends one unary request to one already-resolved address.
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, String>;
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

/// Driver security values copied without interpreting or opening TLS files.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SecurityConfig {
    /// CA path.
    pub cluster_ssl_ca: String,
    /// Client certificate path.
    pub cluster_ssl_cert: String,
    /// Client private-key path.
    pub cluster_ssl_key: String,
}

/// TiKV client values consumed by PD dial-option construction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TikvClientConfig {
    /// gRPC keepalive interval in seconds.
    pub grpc_keep_alive_time_secs: u64,
    /// gRPC keepalive timeout in seconds.
    pub grpc_keep_alive_timeout_secs: u64,
}

/// PD client values consumed by driver option construction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PdClientConfig {
    /// Custom PD request timeout in seconds.
    pub server_timeout_secs: u64,
}

/// Transaction-local latch values copied by the driver.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TxnLocalLatchesConfig {
    /// Whether local latches are enabled.
    pub enabled: bool,
    /// Latch capacity.
    pub capacity: usize,
}

/// Caller-owned snapshot standing in for TiDB's global config value.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DriverDefaults {
    /// Default security config.
    pub security: SecurityConfig,
    /// Default TiKV client config.
    pub tikv_client: TikvClientConfig,
    /// Default PD client config.
    pub pd_client: PdClientConfig,
    /// Default transaction-local latches.
    pub txn_local_latches: TxnLocalLatchesConfig,
    /// Default PD forwarding option.
    pub enable_forwarding: bool,
}

/// Explicit overrides accepted by the bounded driver projection.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DriverOptions {
    /// Replacement security config.
    pub security: Option<SecurityConfig>,
    /// Replacement TiKV client config.
    pub tikv_client: Option<TikvClientConfig>,
    /// Replacement PD client config.
    pub pd_client: Option<PdClientConfig>,
    /// Replacement transaction-local latches.
    pub txn_local_latches: Option<TxnLocalLatchesConfig>,
}

/// Effective driver config after defaults and explicit replacements.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TikvDriverConfig {
    /// Effective security config.
    pub security: SecurityConfig,
    /// Effective TiKV client config.
    pub tikv_client: TikvClientConfig,
    /// Effective PD client config.
    pub pd_client: PdClientConfig,
    /// Effective transaction-local latches.
    pub txn_local_latches: TxnLocalLatchesConfig,
    /// Effective PD forwarding option.
    pub enable_forwarding: bool,
}

impl TikvDriverConfig {
    /// Clones global defaults, then applies replacement options.
    #[must_use]
    pub fn from_defaults(defaults: &DriverDefaults, options: DriverOptions) -> Self {
        Self {
            security: options
                .security
                .unwrap_or_else(|| defaults.security.clone()),
            tikv_client: options.tikv_client.unwrap_or(defaults.tikv_client),
            pd_client: options.pd_client.unwrap_or(defaults.pd_client),
            txn_local_latches: options
                .txn_local_latches
                .unwrap_or(defaults.txn_local_latches),
            enable_forwarding: defaults.enable_forwarding,
        }
    }

    /// Projects the non-network PD option values used by the Go driver.
    #[must_use]
    pub fn pd_options(&self, metrics_labels: BTreeMap<String, String>) -> PdOptions {
        PdOptions {
            max_receive_message_size: i32::MAX,
            grpc_keep_alive_time_secs: self.tikv_client.grpc_keep_alive_time_secs,
            grpc_keep_alive_timeout_secs: self.tikv_client.grpc_keep_alive_timeout_secs,
            server_timeout_secs: self.pd_client.server_timeout_secs,
            enable_forwarding: self.enable_forwarding,
            metrics_labels,
        }
    }
}

/// Serializable PD dial-option metadata; this does not create a PD client.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PdOptions {
    /// gRPC maximum receive message size.
    pub max_receive_message_size: i32,
    /// gRPC keepalive interval in seconds.
    pub grpc_keep_alive_time_secs: u64,
    /// gRPC keepalive timeout in seconds.
    pub grpc_keep_alive_timeout_secs: u64,
    /// Custom PD request timeout in seconds.
    pub server_timeout_secs: u64,
    /// Whether PD forwarding is enabled.
    pub enable_forwarding: bool,
    /// Constant metrics labels.
    pub metrics_labels: BTreeMap<String, String>,
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
