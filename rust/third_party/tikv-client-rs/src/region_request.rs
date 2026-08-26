// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Source-compatible diagnostics for physical region requests.
//!
//! client-go's region sender records RPC duration in first-seen command
//! order, bounds distinct error labels, and retains a short replica access
//! trace before aggregating overflow by peer. These collectors use a mutex
//! because Rust can execute shards concurrently while sharing one request
//! owner.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use crate::async_util::Cancellation;
use crate::locate::AccessMode;
use crate::proto::errorpb;
use crate::proto::metapb;
use crate::region::RegionVerId;
use crate::store::CommandType;
use crate::store::EndpointType;
use crate::util::format_duration;
use crate::Error;

const MAX_ERROR_TYPES: usize = 16;
const MAX_REPLICA_ACCESS_INFOS: usize = 5;
static SHUTTING_DOWN: AtomicU32 = AtomicU32::new(0);

/// Source-shaped diagnostics for one concrete region RPC route.
///
/// The executable route remains [`crate::store::RegionStore`]. This compact
/// view exists because retry owners need stable, source-compatible context in
/// their error strings without retaining a transport client.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RpcContext {
    pub(crate) region: RegionVerId,
    pub(crate) meta: Option<metapb::Region>,
    pub(crate) peer: Option<metapb::Peer>,
    pub(crate) access_index: usize,
    pub(crate) address: String,
    pub(crate) access_mode: AccessMode,
    pub(crate) endpoint_type: Option<EndpointType>,
    pub(crate) proxy_store_id: Option<u64>,
    pub(crate) proxy_address: Option<String>,
}

impl RpcContext {
    pub(crate) fn from_region_store(store: &crate::store::RegionStore) -> Self {
        let forwarded = !store.forwarded_host.is_empty();
        let proxy_store_id = if forwarded {
            store.physical_store_id
        } else {
            None
        };
        let endpoint_type = if forwarded {
            EndpointType::TiKv
        } else {
            store.physical_endpoint_type
        };
        Self {
            region: store.region_with_leader.ver_id(),
            meta: Some(store.region_with_leader.region.clone()),
            peer: store.target_peer.clone(),
            access_index: store.access_index,
            address: if forwarded {
                store.forwarded_host.clone()
            } else {
                store.target.clone()
            },
            access_mode: if endpoint_type.is_tiflash_related() {
                AccessMode::TiFlashOnly
            } else {
                AccessMode::TiKvOnly
            },
            endpoint_type: Some(endpoint_type),
            proxy_store_id,
            proxy_address: proxy_store_id.map(|_| store.target.clone()),
        }
    }

    pub(crate) fn to_backoff_reason_string(&self) -> String {
        let peer_id = self.peer.as_ref().map_or(0, |peer| peer.id);
        let store_id = self.peer.as_ref().map_or(0, |peer| peer.store_id);
        let mut result = format!(
            "region: {}, peerID: {peer_id}, storeID: {store_id}, addr: {}, idx: {}, reqStoreType: {}, runStoreType: {}",
            self.region,
            self.address,
            self.access_index,
            self.access_mode,
            self.endpoint_type.map_or("", EndpointType::name),
        );
        self.append_proxy(&mut result);
        result
    }

    fn append_proxy(&self, result: &mut String) {
        if let Some(store_id) = self.proxy_store_id {
            result.push_str(&format!(
                ", proxy store id: {store_id}, proxy addr: {}",
                self.proxy_address.as_deref().unwrap_or_default()
            ));
        }
    }
}

impl fmt::Display for RpcContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = format!(
            "region ID: {}, meta: {}, peer: {}, addr: {}, idx: {}, reqStoreType: {}, runStoreType: {}",
            self.region.id,
            format_region_meta(self.meta.as_ref()),
            format_peer(self.peer.as_ref()),
            self.address,
            self.access_index,
            self.access_mode,
            self.endpoint_type.map_or("", EndpointType::name),
        );
        self.append_proxy(&mut result);
        formatter.write_str(&result)
    }
}

fn format_region_meta(region: Option<&metapb::Region>) -> String {
    region.map_or_else(|| "<nil>".to_owned(), crate::error::protobuf_text)
}

fn format_peer(peer: Option<&metapb::Peer>) -> String {
    peer.map_or_else(|| "<nil>".to_owned(), crate::error::protobuf_text)
}

pub(crate) fn rpc_context_backoff_string(context: Option<&RpcContext>) -> String {
    context.map_or_else(|| "<nil>".to_owned(), RpcContext::to_backoff_reason_string)
}

pub(crate) fn backoff_error_with_rpc_context(
    reason: impl fmt::Display,
    context: Option<&RpcContext>,
) -> String {
    format!("{reason}, ctx: {}", rpc_context_backoff_string(context))
}

pub(crate) fn backoff_error_with_rpc_context_and_advice(
    reason: impl fmt::Display,
    context: Option<&RpcContext>,
    advice: impl fmt::Display,
) -> String {
    format!(
        "{reason}, ctx: {}, {advice}",
        rpc_context_backoff_string(context)
    )
}

/// Returns the deepest transport cause, falling back to the wrapper itself.
/// `None` is Rust's native counterpart of client-go's nil error input.
pub(crate) fn request_error_message(error: Option<&Error>) -> String {
    let Some(error) = error else {
        return String::new();
    };
    let mut cause: &(dyn std::error::Error + 'static) = error;
    while let Some(source) = cause.source() {
        cause = source;
    }
    cause.to_string()
}

pub(crate) fn region_request_sender_string(
    rpc_error: Option<&Error>,
    replica_selector: Option<&str>,
) -> String {
    let rpc_error = rpc_error.map_or_else(|| "<nil>".to_owned(), ToString::to_string);
    let replica_selector = replica_selector.unwrap_or("<nil>");
    format!("{{rpcError:{rpc_error}, replicaSelector: {replica_selector}}}")
}

/// Stores whether the embedding TiDB process is shutting down.
///
/// A nonzero value makes a transport failure terminal before region/store
/// cache side effects, matching client-go's `StoreShuttingDown` contract.
pub fn store_shutting_down(value: u32) {
    SHUTTING_DOWN.store(value, Ordering::Release);
}

/// Loads the process-wide TiDB shutdown marker.
pub fn load_shutting_down() -> u32 {
    SHUTTING_DOWN.load(Ordering::Acquire)
}

/// Cancels every current and future request plan attached to this owner.
///
/// Rust futures do not need client-go's integer-keyed cancel-function map: a
/// shared cancellation root propagates to each attached plan, and dropping a
/// cancelled plan aborts its owned physical shard tasks.
#[derive(Clone, Default)]
pub struct RpcCanceller {
    cancellation: Cancellation,
}

impl RpcCanceller {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel_all(&self) {
        self.cancellation.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub(crate) fn cancellation(&self) -> Cancellation {
        self.cancellation.child()
    }
}

/// Runtime totals for one TiKV command kind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcRuntimeStats {
    pub command: CommandType,
    pub count: u32,
    pub consume: Duration,
}

/// Bounded region- and transport-error counts.
///
/// Once sixteen distinct labels exist, client-go sends every later event to
/// `other_error`, including repetitions of an already recorded label. The
/// seemingly unusual boundary is preserved exactly.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestErrorStats {
    errors: BTreeMap<String, usize>,
    other_error_count: usize,
}

impl RequestErrorStats {
    pub fn record(&mut self, error: impl Into<String>) {
        if self.errors.len() < MAX_ERROR_TYPES {
            *self.errors.entry(error.into()).or_default() += 1;
        } else {
            self.other_error_count += 1;
        }
    }

    pub fn error_count(&self, error: &str) -> usize {
        self.errors.get(error).copied().unwrap_or_default()
    }

    pub fn distinct_error_count(&self) -> usize {
        self.errors.len()
    }

    pub fn other_error_count(&self) -> usize {
        self.other_error_count
    }

    fn merge(&mut self, other: &Self) {
        for (error, count) in &other.errors {
            *self.errors.entry(error.clone()).or_default() += count;
        }
        self.other_error_count += other.other_error_count;
    }
}

impl fmt::Display for RequestErrorStats {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.errors.is_empty() {
            return Ok(());
        }
        formatter.write_str("{")?;
        for (index, (error, count)) in self.errors.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{error}:{count}")?;
        }
        if self.other_error_count > 0 {
            write!(formatter, ", other_error:{}", self.other_error_count)?;
        }
        formatter.write_str("}")
    }
}

/// Read mode captured for a failed replica attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestReadType {
    Leader,
    ReplicaRead,
    StaleRead,
}

/// One of the first five failed replica attempts for a request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaAccessInfo {
    pub peer_id: u64,
    pub store_id: u64,
    pub read_type: RequestReadType,
    pub error: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ReplicaAccessStatsInner {
    access_infos: Vec<ReplicaAccessInfo>,
    overflow: BTreeMap<u64, RequestErrorStats>,
}

impl ReplicaAccessStatsInner {
    fn record(
        &mut self,
        stale_read: bool,
        replica_read: bool,
        peer_id: u64,
        store_id: u64,
        error: impl Into<String>,
    ) {
        let error = error.into();
        if self.access_infos.len() < MAX_REPLICA_ACCESS_INFOS {
            let read_type = if replica_read {
                RequestReadType::ReplicaRead
            } else if stale_read {
                RequestReadType::StaleRead
            } else {
                RequestReadType::Leader
            };
            self.access_infos.push(ReplicaAccessInfo {
                peer_id,
                store_id,
                read_type,
                error,
            });
        } else {
            self.overflow.entry(peer_id).or_default().record(error);
        }
    }

    fn merge(&mut self, other: Self) {
        for info in other.access_infos {
            self.record(
                info.read_type == RequestReadType::StaleRead,
                info.read_type == RequestReadType::ReplicaRead,
                info.peer_id,
                info.store_id,
                info.error,
            );
        }
        for (peer_id, errors) in other.overflow {
            self.overflow.entry(peer_id).or_default().merge(&errors);
        }
    }
}

/// Bounded trace of failed replica attempts.
#[derive(Default)]
pub struct ReplicaAccessStats {
    inner: Mutex<ReplicaAccessStatsInner>,
}

impl Clone for ReplicaAccessStats {
    fn clone(&self) -> Self {
        Self {
            inner: Mutex::new(
                self.inner
                    .lock()
                    .expect("replica access stats lock poisoned")
                    .clone(),
            ),
        }
    }
}

impl ReplicaAccessStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(
        &self,
        stale_read: bool,
        replica_read: bool,
        peer_id: u64,
        store_id: u64,
        error: impl Into<String>,
    ) {
        self.inner
            .lock()
            .expect("replica access stats lock poisoned")
            .record(stale_read, replica_read, peer_id, store_id, error);
    }

    pub fn access_infos(&self) -> Vec<ReplicaAccessInfo> {
        self.inner
            .lock()
            .expect("replica access stats lock poisoned")
            .access_infos
            .clone()
    }

    pub fn overflow_error_stats(&self, peer_id: u64) -> Option<RequestErrorStats> {
        self.inner
            .lock()
            .expect("replica access stats lock poisoned")
            .overflow
            .get(&peer_id)
            .cloned()
    }
}

impl fmt::Display for ReplicaAccessStats {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self
            .inner
            .lock()
            .expect("replica access stats lock poisoned");
        for (index, info) in inner.access_infos.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            match info.read_type {
                RequestReadType::Leader => formatter.write_str("{")?,
                RequestReadType::ReplicaRead => formatter.write_str("{replica_read, ")?,
                RequestReadType::StaleRead => formatter.write_str("{stale_read, ")?,
            }
            write!(
                formatter,
                "peer:{}, store:{}, err:{}}}",
                info.peer_id, info.store_id, info.error
            )?;
        }
        if !inner.overflow.is_empty() {
            formatter.write_str(", overflow_count:{")?;
            for (index, (peer_id, errors)) in inner.overflow.iter().enumerate() {
                if index > 0 {
                    formatter.write_str(", ")?;
                }
                write!(formatter, "{{peer:{peer_id}, error_stats:{errors}}}")?;
            }
            formatter.write_str("}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RegionRequestRuntimeStatsInner {
    rpc_stats: Vec<RpcRuntimeStats>,
    errors: RequestErrorStats,
    replica_access: ReplicaAccessStatsInner,
}

/// Runtime statistics for every physical attempt owned by one region request.
#[derive(Default)]
pub struct RegionRequestRuntimeStats {
    inner: Mutex<RegionRequestRuntimeStatsInner>,
}

impl Clone for RegionRequestRuntimeStats {
    fn clone(&self) -> Self {
        Self {
            inner: Mutex::new(
                self.inner
                    .lock()
                    .expect("region request stats lock poisoned")
                    .clone(),
            ),
        }
    }
}

impl RegionRequestRuntimeStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_rpc(&self, command: CommandType, duration: Duration) {
        let mut inner = self
            .inner
            .lock()
            .expect("region request stats lock poisoned");
        if let Some(stats) = inner
            .rpc_stats
            .iter_mut()
            .find(|stats| stats.command == command)
        {
            stats.count += 1;
            stats.consume += duration;
        } else {
            inner.rpc_stats.push(RpcRuntimeStats {
                command,
                count: 1,
                consume: duration,
            });
        }
    }

    pub fn rpc_type_count(&self) -> usize {
        self.inner
            .lock()
            .expect("region request stats lock poisoned")
            .rpc_stats
            .len()
    }

    pub fn command_rpc_count(&self, command: CommandType) -> u32 {
        self.inner
            .lock()
            .expect("region request stats lock poisoned")
            .rpc_stats
            .iter()
            .find(|stats| stats.command == command)
            .map_or(0, |stats| stats.count)
    }

    pub fn command_rpc_duration(&self, command: CommandType) -> Duration {
        self.inner
            .lock()
            .expect("region request stats lock poisoned")
            .rpc_stats
            .iter()
            .find(|stats| stats.command == command)
            .map_or(Duration::ZERO, |stats| stats.consume)
    }

    pub fn rpc_stats(&self) -> Vec<RpcRuntimeStats> {
        self.inner
            .lock()
            .expect("region request stats lock poisoned")
            .rpc_stats
            .clone()
    }

    pub fn record_error(&self, error: impl Into<String>) {
        self.inner
            .lock()
            .expect("region request stats lock poisoned")
            .errors
            .record(error);
    }

    pub fn error_stats(&self) -> RequestErrorStats {
        self.inner
            .lock()
            .expect("region request stats lock poisoned")
            .errors
            .clone()
    }

    pub fn replica_access_stats(&self) -> ReplicaAccessStats {
        let inner = self
            .inner
            .lock()
            .expect("region request stats lock poisoned");
        ReplicaAccessStats {
            inner: Mutex::new(inner.replica_access.clone()),
        }
    }

    pub(crate) fn record_replica_access(
        &self,
        stale_read: bool,
        replica_read: bool,
        peer_id: u64,
        store_id: u64,
        error: impl Into<String>,
    ) {
        self.inner
            .lock()
            .expect("region request stats lock poisoned")
            .replica_access
            .record(stale_read, replica_read, peer_id, store_id, error);
    }

    pub fn merge(&self, other: &Self) {
        let other = other
            .inner
            .lock()
            .expect("region request stats lock poisoned")
            .clone();
        let mut inner = self
            .inner
            .lock()
            .expect("region request stats lock poisoned");
        for other_stats in other.rpc_stats {
            if let Some(stats) = inner
                .rpc_stats
                .iter_mut()
                .find(|stats| stats.command == other_stats.command)
            {
                stats.count += other_stats.count;
                stats.consume += other_stats.consume;
            } else {
                inner.rpc_stats.push(other_stats);
            }
        }
        inner.errors.merge(&other.errors);
        inner.replica_access.merge(other.replica_access);
    }
}

impl fmt::Display for RegionRequestRuntimeStats {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self
            .inner
            .lock()
            .expect("region request stats lock poisoned");
        for (index, stats) in inner.rpc_stats.iter().enumerate() {
            if index > 0 {
                formatter.write_str(",")?;
            }
            write!(
                formatter,
                "{}:{{num_rpc:{}, total_time:{}}}",
                stats.command.name(),
                stats.count,
                format_duration(stats.consume)
            )?;
        }
        if !inner.errors.errors.is_empty() {
            write!(formatter, ", rpc_errors:{}", inner.errors)?;
        }
        Ok(())
    }
}

/// Classify a TiKV region error in the exact field order used by client-go.
pub(crate) fn region_error_label(error: &errorpb::Error) -> &'static str {
    if error.not_leader.is_some() {
        "not_leader"
    } else if error.region_not_found.is_some() {
        "region_not_found"
    } else if error.key_not_in_region.is_some() {
        "key_not_in_region"
    } else if error.epoch_not_match.is_some() {
        "epoch_not_match"
    } else if let Some(busy) = &error.server_is_busy {
        if busy.reason.contains("deadline is exceeded") {
            "deadline_exceeded"
        } else {
            "server_is_busy"
        }
    } else if error.stale_command.is_some() {
        "stale_command"
    } else if error.store_not_match.is_some() {
        "store_not_match"
    } else if error.raft_entry_too_large.is_some() {
        "raft_entry_too_large"
    } else if error.max_timestamp_not_synced.is_some() {
        "max_timestamp_not_synced"
    } else if error.read_index_not_ready.is_some() {
        "read_index_not_ready"
    } else if error.proposal_in_merging_mode.is_some() {
        "proposal_in_merging_mode"
    } else if error.data_is_not_ready.is_some() {
        "data_is_not_ready"
    } else if error.region_not_initialized.is_some() {
        "region_not_initialized"
    } else if error.disk_full.is_some() {
        "disk_full"
    } else if error.recovery_in_progress.is_some() {
        "recovery_in_progress"
    } else if error.flashback_in_progress.is_some() {
        "flashback_in_progress"
    } else if error.flashback_not_prepared.is_some() {
        "flashback_not_prepared"
    } else if error.is_witness.is_some() {
        "peer_is_witness"
    } else if error.message.contains("Deadline is exceeded") {
        "deadline_exceeded"
    } else if error.mismatch_peer_id.is_some() {
        "mismatch_peer_id"
    } else if error.bucket_version_not_match.is_some() {
        "bucket_version_not_match"
    } else if error.message.contains("invalid max_ts update") {
        "invalid_max_ts_update"
    } else if error.undetermined_result.is_some() {
        "undetermined_result"
    } else {
        "unknown"
    }
}

pub(crate) fn region_error_access_message(error: &errorpb::Error, label: &str) -> String {
    match error.not_leader.as_ref() {
        Some(not_leader) => match not_leader.leader.as_ref() {
            Some(leader) => format!("{label}_with_leader_{}", leader.id),
            None => format!("{label}_with_no_leader"),
        },
        None => label.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn source_shutdown_marker_defaults_to_running() {
        store_shutting_down(0);
        assert_eq!(load_shutting_down(), 0);
    }
    use crate::proto::errorpb;
    use crate::proto::metapb;
    use crate::region::RegionWithLeader;
    use crate::store::RegionStore;

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request_TestRegionRequestSenderString() {
        assert_eq!(
            region_request_sender_string(None, None),
            "{rpcError:<nil>, replicaSelector: <nil>}"
        );
        let error = Error::StringError("send failed".to_owned());
        assert_eq!(
            region_request_sender_string(Some(&error), Some("selector")),
            "{rpcError:send failed, replicaSelector: selector}"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request_TestRegionRequestStats() {
        let stats = RegionRequestRuntimeStats::new();
        stats.record_rpc(CommandType::Get, Duration::from_secs(1));
        stats.record_rpc(CommandType::Get, Duration::from_millis(1));
        stats.record_rpc(CommandType::Coprocessor, Duration::from_secs(2));
        stats.record_rpc(CommandType::Coprocessor, Duration::from_millis(200));
        stats.record_error("context canceled");
        stats.record_error("context canceled");
        stats.record_error("region_not_found");

        stats.merge(&RegionRequestRuntimeStats::new());
        let merged = RegionRequestRuntimeStats::new();
        merged.merge(&stats.clone());
        assert_eq!(stats.rpc_type_count(), 2);
        assert_eq!(stats.command_rpc_count(CommandType::Get), 2);
        assert_eq!(
            stats.command_rpc_duration(CommandType::Get),
            Duration::from_millis(1_001)
        );
        assert_eq!(stats.error_stats().error_count("context canceled"), 2);
        assert_eq!(
            merged.to_string(),
            "Get:{num_rpc:2, total_time:1s},Cop:{num_rpc:2, total_time:2.2s}, rpc_errors:{context canceled:2, region_not_found:1}"
        );

        for index in 0..50 {
            stats.record_error(format!("err_{index}"));
        }
        let errors = stats.error_stats();
        assert_eq!(errors.distinct_error_count(), 16);
        assert_eq!(errors.other_error_count(), 36);
        assert!(stats.to_string().contains("other_error:36"));

        let access = RegionRequestRuntimeStats::new();
        access.record_replica_access(true, false, 1, 2, "data_not_ready");
        access.record_replica_access(false, false, 3, 4, "not_leader");
        access.record_replica_access(false, true, 5, 6, "server_is_Busy");
        assert_eq!(
            access.replica_access_stats().to_string(),
            "{stale_read, peer:1, store:2, err:data_not_ready}, {peer:3, store:4, err:not_leader}, {replica_read, peer:5, store:6, err:server_is_Busy}"
        );
        for index in 0..20 {
            access.record_replica_access(false, false, 5 + index % 2, 6, "server_is_Busy");
        }
        let access = access.replica_access_stats();
        assert_eq!(
            access
                .overflow_error_stats(5)
                .unwrap()
                .error_count("server_is_Busy"),
            9
        );
        assert_eq!(
            access
                .overflow_error_stats(6)
                .unwrap()
                .error_count("server_is_Busy"),
            9
        );
        assert_eq!(
            access.to_string(),
            "{stale_read, peer:1, store:2, err:data_not_ready}, {peer:3, store:4, err:not_leader}, {replica_read, peer:5, store:6, err:server_is_Busy}, {peer:5, store:6, err:server_is_Busy}, {peer:6, store:6, err:server_is_Busy}, overflow_count:{{peer:5, error_stats:{server_is_Busy:9}}, {peer:6, error_stats:{server_is_Busy:9}}}"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request_TestGetErrMsg() {
        assert_eq!(request_error_message(None), "");
        let fallback = Error::StringError("no cause err".to_owned());
        assert_eq!(request_error_message(Some(&fallback)), "no cause err");

        let wrapped = Error::Connection {
            source: Box::new(Error::StringError("root cause".to_owned())),
            address: "tikv-1".to_owned(),
            version: 7,
        };
        assert_eq!(request_error_message(Some(&wrapped)), "root cause");
    }

    fn source_rpc_context() -> RpcContext {
        RpcContext {
            region: RegionVerId {
                id: 100,
                conf_ver: 2,
                ver: 3,
            },
            meta: Some(metapb::Region {
                id: 100,
                region_epoch: Some(metapb::RegionEpoch {
                    conf_ver: 2,
                    version: 3,
                }),
                ..Default::default()
            }),
            peer: Some(metapb::Peer {
                id: 101,
                store_id: 1,
                ..Default::default()
            }),
            access_index: 4,
            address: "tikv-1".to_owned(),
            access_mode: AccessMode::TiKvOnly,
            endpoint_type: Some(EndpointType::TiKv),
            proxy_store_id: None,
            proxy_address: None,
        }
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request_TestRPCContextString() {
        assert_eq!(rpc_context_backoff_string(None), "<nil>");

        let context = source_rpc_context();
        assert_eq!(
            context.to_string(),
            "region ID: 100, meta: id:100 region_epoch:<conf_ver:2 version:3 > , peer: id:101 store_id:1 , addr: tikv-1, idx: 4, reqStoreType: TiKvOnly, runStoreType: tikv"
        );
        assert_eq!(
            context.to_backoff_reason_string(),
            "region: { region id: 100, ver: 3, confVer: 2 }, peerID: 101, storeID: 1, addr: tikv-1, idx: 4, reqStoreType: TiKvOnly, runStoreType: tikv"
        );

        let route = RegionStore::new(
            RegionWithLeader {
                region: context.meta.clone().unwrap(),
                leader: context.peer.clone(),
                ..Default::default()
            },
            Arc::new(crate::mock::MockKvClient::default()),
        )
        .with_target("tikv-1")
        .with_physical_store(1, EndpointType::TiKv)
        .with_target_peer(context.peer.clone().unwrap())
        .with_access_index(4);
        assert_eq!(RpcContext::from_region_store(&route), context);

        let with_proxy = RpcContext {
            region: RegionVerId {
                id: 200,
                conf_ver: 5,
                ver: 8,
            },
            meta: None,
            peer: None,
            access_index: 0,
            address: "tikv-1".to_owned(),
            access_mode: AccessMode::TiKvOnly,
            endpoint_type: Some(EndpointType::TiKv),
            proxy_store_id: Some(2),
            proxy_address: Some("tikv-2".to_owned()),
        };
        let suffix = ", proxy store id: 2, proxy addr: tikv-2";
        assert!(with_proxy.to_string().ends_with(suffix));
        assert!(with_proxy.to_backoff_reason_string().ends_with(suffix));

        let routed_proxy = RpcContext::from_region_store(
            &route
                .with_target("tikv-2")
                .with_physical_store(2, EndpointType::TiKv)
                .with_forwarded_host("tikv-1"),
        );
        assert_eq!(routed_proxy.address, "tikv-1");
        assert_eq!(routed_proxy.proxy_store_id, Some(2));
        assert_eq!(routed_proxy.proxy_address.as_deref(), Some("tikv-2"));
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_region_request_TestBackoffErrWithRPCContext() {
        let mut context = source_rpc_context();
        context.region = RegionVerId {
            id: 200,
            conf_ver: 5,
            ver: 8,
        };
        let context_string = context.to_backoff_reason_string();
        assert_eq!(
            backoff_error_with_rpc_context("reason1", Some(&context)),
            format!("reason1, ctx: {context_string}")
        );
        assert_eq!(
            backoff_error_with_rpc_context_and_advice("reason1", Some(&context), "advice1"),
            format!("reason1, ctx: {context_string}, advice1")
        );
    }

    #[test]
    fn source_region_error_labels_follow_protobuf_order_and_logging_suffixes() {
        let cases = [
            (
                errorpb::Error {
                    not_leader: Some(errorpb::NotLeader::default()),
                    ..Default::default()
                },
                "not_leader",
            ),
            (
                errorpb::Error {
                    region_not_found: Some(errorpb::RegionNotFound::default()),
                    ..Default::default()
                },
                "region_not_found",
            ),
            (
                errorpb::Error {
                    key_not_in_region: Some(errorpb::KeyNotInRegion::default()),
                    ..Default::default()
                },
                "key_not_in_region",
            ),
            (
                errorpb::Error {
                    epoch_not_match: Some(errorpb::EpochNotMatch::default()),
                    ..Default::default()
                },
                "epoch_not_match",
            ),
            (
                errorpb::Error {
                    server_is_busy: Some(errorpb::ServerIsBusy::default()),
                    ..Default::default()
                },
                "server_is_busy",
            ),
            (
                errorpb::Error {
                    stale_command: Some(errorpb::StaleCommand::default()),
                    ..Default::default()
                },
                "stale_command",
            ),
            (
                errorpb::Error {
                    store_not_match: Some(errorpb::StoreNotMatch::default()),
                    ..Default::default()
                },
                "store_not_match",
            ),
            (
                errorpb::Error {
                    raft_entry_too_large: Some(errorpb::RaftEntryTooLarge::default()),
                    ..Default::default()
                },
                "raft_entry_too_large",
            ),
            (
                errorpb::Error {
                    max_timestamp_not_synced: Some(errorpb::MaxTimestampNotSynced::default()),
                    ..Default::default()
                },
                "max_timestamp_not_synced",
            ),
            (
                errorpb::Error {
                    read_index_not_ready: Some(errorpb::ReadIndexNotReady::default()),
                    ..Default::default()
                },
                "read_index_not_ready",
            ),
            (
                errorpb::Error {
                    proposal_in_merging_mode: Some(errorpb::ProposalInMergingMode::default()),
                    ..Default::default()
                },
                "proposal_in_merging_mode",
            ),
            (
                errorpb::Error {
                    data_is_not_ready: Some(errorpb::DataIsNotReady::default()),
                    ..Default::default()
                },
                "data_is_not_ready",
            ),
            (
                errorpb::Error {
                    region_not_initialized: Some(errorpb::RegionNotInitialized::default()),
                    ..Default::default()
                },
                "region_not_initialized",
            ),
            (
                errorpb::Error {
                    disk_full: Some(errorpb::DiskFull::default()),
                    ..Default::default()
                },
                "disk_full",
            ),
            (
                errorpb::Error {
                    recovery_in_progress: Some(errorpb::RecoveryInProgress::default()),
                    ..Default::default()
                },
                "recovery_in_progress",
            ),
            (
                errorpb::Error {
                    flashback_in_progress: Some(errorpb::FlashbackInProgress::default()),
                    ..Default::default()
                },
                "flashback_in_progress",
            ),
            (
                errorpb::Error {
                    flashback_not_prepared: Some(errorpb::FlashbackNotPrepared::default()),
                    ..Default::default()
                },
                "flashback_not_prepared",
            ),
            (
                errorpb::Error {
                    is_witness: Some(errorpb::IsWitness::default()),
                    ..Default::default()
                },
                "peer_is_witness",
            ),
            (
                errorpb::Error {
                    mismatch_peer_id: Some(errorpb::MismatchPeerId::default()),
                    ..Default::default()
                },
                "mismatch_peer_id",
            ),
            (
                errorpb::Error {
                    bucket_version_not_match: Some(errorpb::BucketVersionNotMatch::default()),
                    ..Default::default()
                },
                "bucket_version_not_match",
            ),
            (
                errorpb::Error {
                    undetermined_result: Some(errorpb::UndeterminedResult::default()),
                    ..Default::default()
                },
                "undetermined_result",
            ),
            (errorpb::Error::default(), "unknown"),
        ];
        for (error, expected) in cases {
            assert_eq!(region_error_label(&error), expected);
        }

        let busy_deadline = errorpb::Error {
            server_is_busy: Some(errorpb::ServerIsBusy {
                reason: "request deadline is exceeded".to_owned(),
                ..Default::default()
            }),
            mismatch_peer_id: Some(errorpb::MismatchPeerId::default()),
            ..Default::default()
        };
        assert_eq!(region_error_label(&busy_deadline), "deadline_exceeded");
        assert_eq!(
            region_error_label(&errorpb::Error {
                message: "Deadline is exceeded".to_owned(),
                ..Default::default()
            }),
            "deadline_exceeded"
        );
        assert_eq!(
            region_error_label(&errorpb::Error {
                message: "invalid max_ts update".to_owned(),
                ..Default::default()
            }),
            "invalid_max_ts_update"
        );

        let with_leader = errorpb::Error {
            not_leader: Some(errorpb::NotLeader {
                leader: Some(metapb::Peer {
                    id: 42,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            region_error_access_message(&with_leader, "not_leader"),
            "not_leader_with_leader_42"
        );
        assert_eq!(
            region_error_access_message(
                &errorpb::Error {
                    not_leader: Some(errorpb::NotLeader::default()),
                    ..Default::default()
                },
                "not_leader"
            ),
            "not_leader_with_no_leader"
        );
    }
}
