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
use std::sync::Mutex;
use std::time::Duration;

use crate::proto::errorpb;
use crate::store::CommandType;
use crate::util::format_duration;

const MAX_ERROR_TYPES: usize = 16;
const MAX_REPLICA_ACCESS_INFOS: usize = 5;

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
    use super::*;
    use crate::proto::errorpb;
    use crate::proto::metapb;

    #[test]
    fn source_runtime_stats_clone_merge_and_bound_errors() {
        let stats = RegionRequestRuntimeStats::new();
        stats.record_rpc(CommandType::Get, Duration::from_secs(1));
        stats.record_rpc(CommandType::Get, Duration::from_millis(1));
        stats.record_rpc(CommandType::Coprocessor, Duration::from_secs(2));
        stats.record_rpc(CommandType::Coprocessor, Duration::from_millis(200));
        stats.record_error("context canceled");
        stats.record_error("context canceled");
        stats.record_error("region_not_found");

        let clone = stats.clone();
        let merged = RegionRequestRuntimeStats::new();
        merged.merge(&clone);
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

        stats.record_replica_access(false, false, 1, 11, "not_leader");
        merged.merge(&stats);
        assert_eq!(
            merged.replica_access_stats().access_infos(),
            vec![ReplicaAccessInfo {
                peer_id: 1,
                store_id: 11,
                read_type: RequestReadType::Leader,
                error: "not_leader".to_owned(),
            }]
        );

        for index in 0..50 {
            stats.record_error(format!("err_{index}"));
        }
        let errors = stats.error_stats();
        assert_eq!(errors.distinct_error_count(), 16);
        assert_eq!(errors.other_error_count(), 36);
        stats.record_error("context canceled");
        assert_eq!(stats.error_stats().other_error_count(), 37);
    }

    #[test]
    fn source_replica_access_keeps_five_details_then_counts_by_peer() {
        let stats = RegionRequestRuntimeStats::new();
        stats.record_replica_access(true, false, 1, 2, "data_not_ready");
        stats.record_replica_access(false, false, 3, 4, "not_leader");
        stats.record_replica_access(false, true, 5, 6, "server_is_Busy");
        for index in 0..20 {
            stats.record_replica_access(false, false, 5 + index % 2, 6, "server_is_Busy");
        }
        let access = stats.replica_access_stats();
        assert_eq!(access.access_infos().len(), 5);
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
