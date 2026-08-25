// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Runtime statistics collected for snapshot reads.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;

use crate::interceptor::{RpcDispatchResult, RpcInterceptor, RpcNext};
use crate::proto::kvrpcpb;
use crate::store::Request;
use crate::util::format_duration;

/// Snapshot RPC commands that contribute to [`SnapshotRuntimeStats`].
///
/// This is the native counterpart of client-go's `tikvrpc.CmdType` values
/// observed by `SnapshotRuntimeStats.GetCmdRPCCount`.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SnapshotRpcCommand {
    Get,
    BatchGet,
    BufferBatchGet,
    Scan,
}

impl fmt::Display for SnapshotRpcCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Get => "Get",
            Self::BatchGet => "BatchGet",
            Self::BufferBatchGet => "BufferBatchGet",
            Self::Scan => "Scan",
        })
    }
}

#[derive(Clone, Default)]
struct RpcRuntimeStat {
    count: u64,
    duration: Duration,
}

#[derive(Clone, Default)]
struct BackoffRuntimeStat {
    count: u64,
    duration: Duration,
}

#[derive(Clone, Default)]
struct SnapshotRuntimeStatsInner {
    rpc: BTreeMap<SnapshotRpcCommand, RpcRuntimeStat>,
    scan_detail: SnapshotScanDetail,
    time_detail: SnapshotTimeDetail,
    resolve_lock_duration: Duration,
    backoff: BTreeMap<&'static str, BackoffRuntimeStat>,
    read_pool_task_details: SnapshotPoolTaskDetails,
}

/// Aggregated TiKV MVCC/RocksDB scan details returned with snapshot reads.
///
/// Fields retain client-go's `util.ScanDetail` meaning and accumulate across
/// every physical response attached to a snapshot collector.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotScanDetail {
    pub total_keys: u64,
    pub processed_keys: u64,
    pub processed_keys_size: u64,
    pub rocksdb_delete_skipped_count: u64,
    pub rocksdb_key_skipped_count: u64,
    pub rocksdb_block_cache_hit_count: u64,
    pub rocksdb_block_read_count: u64,
    pub rocksdb_block_read_byte: u64,
    pub rocksdb_block_read_duration: Duration,
    pub get_snapshot_duration: Duration,
    pub read_index_propose_wait_duration: Duration,
    pub read_index_confirm_wait_duration: Duration,
    pub read_pool_schedule_wait_duration: Duration,
    pub ia_cache_hit_count: u64,
    pub ia_remote_read_segment_count: u64,
    pub ia_remote_read_segment_bytes: u64,
    pub ia_remote_read_segment_duration: Duration,
}

impl SnapshotScanDetail {
    fn merge_from_pb(&mut self, detail: &kvrpcpb::ScanDetailV2) {
        self.total_keys += detail.total_versions;
        self.processed_keys += detail.processed_versions;
        self.processed_keys_size += detail.processed_versions_size;
        self.rocksdb_delete_skipped_count += detail.rocksdb_delete_skipped_count;
        self.rocksdb_key_skipped_count += detail.rocksdb_key_skipped_count;
        self.rocksdb_block_cache_hit_count += detail.rocksdb_block_cache_hit_count;
        self.rocksdb_block_read_count += detail.rocksdb_block_read_count;
        self.rocksdb_block_read_byte += detail.rocksdb_block_read_byte;
        self.rocksdb_block_read_duration += Duration::from_nanos(detail.rocksdb_block_read_nanos);
        self.get_snapshot_duration += Duration::from_nanos(detail.get_snapshot_nanos);
        self.read_index_propose_wait_duration +=
            Duration::from_nanos(detail.read_index_propose_wait_nanos);
        self.read_index_confirm_wait_duration +=
            Duration::from_nanos(detail.read_index_confirm_wait_nanos);
        self.read_pool_schedule_wait_duration +=
            Duration::from_nanos(detail.read_pool_schedule_wait_nanos);
        self.ia_cache_hit_count += detail.ia_cache_hit_count;
        self.ia_remote_read_segment_count += detail.ia_remote_read_segment_count;
        self.ia_remote_read_segment_bytes += detail.ia_remote_read_segment_bytes;
        self.ia_remote_read_segment_duration +=
            Duration::from_nanos(detail.ia_remote_read_segment_nanos);
    }

    fn merge(&mut self, other: &Self) {
        self.total_keys += other.total_keys;
        self.processed_keys += other.processed_keys;
        self.processed_keys_size += other.processed_keys_size;
        self.rocksdb_delete_skipped_count += other.rocksdb_delete_skipped_count;
        self.rocksdb_key_skipped_count += other.rocksdb_key_skipped_count;
        self.rocksdb_block_cache_hit_count += other.rocksdb_block_cache_hit_count;
        self.rocksdb_block_read_count += other.rocksdb_block_read_count;
        self.rocksdb_block_read_byte += other.rocksdb_block_read_byte;
        self.rocksdb_block_read_duration += other.rocksdb_block_read_duration;
        self.get_snapshot_duration += other.get_snapshot_duration;
        self.read_index_propose_wait_duration += other.read_index_propose_wait_duration;
        self.read_index_confirm_wait_duration += other.read_index_confirm_wait_duration;
        self.read_pool_schedule_wait_duration += other.read_pool_schedule_wait_duration;
        self.ia_cache_hit_count += other.ia_cache_hit_count;
        self.ia_remote_read_segment_count += other.ia_remote_read_segment_count;
        self.ia_remote_read_segment_bytes += other.ia_remote_read_segment_bytes;
        self.ia_remote_read_segment_duration += other.ia_remote_read_segment_duration;
    }
}

/// Aggregated TiKV execution-time details returned with snapshot reads.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotTimeDetail {
    pub process_time: Duration,
    pub suspend_time: Duration,
    pub wait_time: Duration,
    pub kv_read_wall_time: Duration,
    pub kv_grpc_process_time: Duration,
    pub kv_grpc_wait_time: Duration,
    pub total_rpc_wall_time: Duration,
}

impl SnapshotTimeDetail {
    fn merge_from_pb(
        &mut self,
        time_detail_v2: Option<&kvrpcpb::TimeDetailV2>,
        time_detail: Option<&kvrpcpb::TimeDetail>,
    ) {
        if let Some(detail) = time_detail_v2 {
            self.wait_time += Duration::from_nanos(detail.wait_wall_time_ns);
            self.process_time += Duration::from_nanos(detail.process_wall_time_ns);
            self.suspend_time += Duration::from_nanos(detail.process_suspend_wall_time_ns);
            self.kv_read_wall_time += Duration::from_nanos(detail.kv_read_wall_time_ns);
            self.kv_grpc_process_time += Duration::from_nanos(detail.kv_grpc_process_time_ns);
            self.kv_grpc_wait_time += Duration::from_nanos(detail.kv_grpc_wait_time_ns);
            self.total_rpc_wall_time += Duration::from_nanos(detail.total_rpc_wall_time_ns);
        } else if let Some(detail) = time_detail {
            self.wait_time += Duration::from_millis(detail.wait_wall_time_ms);
            self.process_time += Duration::from_millis(detail.process_wall_time_ms);
            self.kv_read_wall_time += Duration::from_millis(detail.kv_read_wall_time_ms);
            self.total_rpc_wall_time += Duration::from_nanos(detail.total_rpc_wall_time_ns);
        }
    }

    fn merge(&mut self, other: &Self) {
        self.process_time += other.process_time;
        self.suspend_time += other.suspend_time;
        self.wait_time += other.wait_time;
        self.kv_read_wall_time += other.kv_read_wall_time;
        self.kv_grpc_process_time += other.kv_grpc_process_time;
        self.kv_grpc_wait_time += other.kv_grpc_wait_time;
        self.total_rpc_wall_time += other.total_rpc_wall_time;
    }
}

/// Aggregated scheduling and execution details reported by TiKV read-pool tasks.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotPoolTaskDetails {
    pub task_count: u64,
    pub poll_count: u64,
    pub max_poll_count: u64,
    pub min_poll_count: u64,
    pub dispatch_count: u64,
    pub max_dispatch_count: u64,
    pub min_dispatch_count: u64,
    pub total_wall_time: Duration,
    pub task_wall_time_sample_count: u64,
    pub max_task_wall_time: Duration,
    pub min_task_wall_time: Duration,
    pub total_queue_wait_time: Duration,
    pub max_queue_wait_time: Duration,
    pub min_queue_wait_time: Duration,
    pub total_wake_wait_time: Duration,
    pub max_wake_wait_time: Duration,
    pub min_wake_wait_time: Duration,
    pub fair_queue_sample_count: u64,
    pub total_fair_queue_waited_task_slices: u64,
    pub max_fair_queue_waited_task_slices: u64,
    pub min_fair_queue_waited_task_slices: u64,
    pub poll_cpu_time: Duration,
    pub max_poll_cpu_time: Duration,
    pub min_poll_cpu_time: Duration,
    pub poll_wall_time: Duration,
    pub min_poll_wall_time: Duration,
    pub max_poll_wall_time: Duration,
}

impl SnapshotPoolTaskDetails {
    fn merge_from_pb(&mut self, detail: &kvrpcpb::PoolTaskDetails) {
        let had_tasks = self.task_count > 0;
        let had_poll_samples = self.poll_count > 0;
        let had_queue_wait_samples = !self.total_queue_wait_time.is_zero();
        let had_wake_wait_samples = !self.total_wake_wait_time.is_zero();
        let had_fair_queue_samples = self.fair_queue_sample_count > 0;
        let had_task_wall_samples = self.task_wall_time_sample_count > 0;

        self.task_count += 1;
        self.poll_count += detail.poll_count;
        self.max_poll_count = self.max_poll_count.max(detail.poll_count);
        merge_min(&mut self.min_poll_count, detail.poll_count, had_tasks);
        self.dispatch_count += detail.dispatch_count;
        self.max_dispatch_count = self.max_dispatch_count.max(detail.dispatch_count);
        merge_min(
            &mut self.min_dispatch_count,
            detail.dispatch_count,
            had_tasks,
        );

        let wall = Duration::from_nanos(detail.total_wall_nanos);
        self.total_wall_time += wall;
        if !wall.is_zero() {
            self.task_wall_time_sample_count += 1;
            self.max_task_wall_time = self.max_task_wall_time.max(wall);
            merge_min(&mut self.min_task_wall_time, wall, had_task_wall_samples);
        }

        let queue_total = Duration::from_nanos(detail.total_queue_wait_nanos);
        self.total_queue_wait_time += queue_total;
        self.max_queue_wait_time = self
            .max_queue_wait_time
            .max(Duration::from_nanos(detail.max_queue_wait_nanos));
        if !queue_total.is_zero() {
            merge_min(
                &mut self.min_queue_wait_time,
                Duration::from_nanos(detail.min_queue_wait_nanos),
                had_queue_wait_samples,
            );
        }

        let wake_total = Duration::from_nanos(detail.total_wake_wait_nanos);
        self.total_wake_wait_time += wake_total;
        self.max_wake_wait_time = self
            .max_wake_wait_time
            .max(Duration::from_nanos(detail.max_wake_wait_nanos));
        if !wake_total.is_zero() {
            merge_min(
                &mut self.min_wake_wait_time,
                Duration::from_nanos(detail.min_wake_wait_nanos),
                had_wake_wait_samples,
            );
        }

        if detail.fair_queue_enabled {
            self.fair_queue_sample_count += detail.dispatch_count;
            self.total_fair_queue_waited_task_slices += detail.total_fair_queue_waited_task_slices;
            self.max_fair_queue_waited_task_slices = self
                .max_fair_queue_waited_task_slices
                .max(detail.max_fair_queue_waited_task_slices);
            merge_min(
                &mut self.min_fair_queue_waited_task_slices,
                detail.min_fair_queue_waited_task_slices,
                had_fair_queue_samples,
            );
        }

        self.poll_cpu_time += Duration::from_nanos(detail.poll_cpu_nanos);
        self.max_poll_cpu_time = self
            .max_poll_cpu_time
            .max(Duration::from_nanos(detail.max_poll_cpu_nanos));
        self.poll_wall_time += Duration::from_nanos(detail.poll_wall_nanos);
        self.max_poll_wall_time = self
            .max_poll_wall_time
            .max(Duration::from_nanos(detail.max_poll_wall_nanos));
        if detail.poll_count > 0 {
            merge_min(
                &mut self.min_poll_cpu_time,
                Duration::from_nanos(detail.min_poll_cpu_nanos),
                had_poll_samples,
            );
            merge_min(
                &mut self.min_poll_wall_time,
                Duration::from_nanos(detail.min_poll_wall_nanos),
                had_poll_samples,
            );
        }
    }

    fn merge(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        let had_tasks = self.task_count > 0;
        let had_poll_samples = self.poll_count > 0;
        let had_queue_wait_samples = !self.total_queue_wait_time.is_zero();
        let had_wake_wait_samples = !self.total_wake_wait_time.is_zero();
        let had_fair_queue_samples = self.fair_queue_sample_count > 0;
        let had_task_wall_samples = self.task_wall_time_sample_count > 0;

        self.task_count += other.task_count;
        self.poll_count += other.poll_count;
        self.max_poll_count = self.max_poll_count.max(other.max_poll_count);
        merge_min(&mut self.min_poll_count, other.min_poll_count, had_tasks);
        self.dispatch_count += other.dispatch_count;
        self.max_dispatch_count = self.max_dispatch_count.max(other.max_dispatch_count);
        merge_min(
            &mut self.min_dispatch_count,
            other.min_dispatch_count,
            had_tasks,
        );
        self.total_wall_time += other.total_wall_time;
        self.task_wall_time_sample_count += other.task_wall_time_sample_count;
        self.max_task_wall_time = self.max_task_wall_time.max(other.max_task_wall_time);
        if !other.total_wall_time.is_zero() {
            merge_min(
                &mut self.min_task_wall_time,
                other.min_task_wall_time,
                had_task_wall_samples,
            );
        }
        self.total_queue_wait_time += other.total_queue_wait_time;
        self.max_queue_wait_time = self.max_queue_wait_time.max(other.max_queue_wait_time);
        if !other.total_queue_wait_time.is_zero() {
            merge_min(
                &mut self.min_queue_wait_time,
                other.min_queue_wait_time,
                had_queue_wait_samples,
            );
        }
        self.total_wake_wait_time += other.total_wake_wait_time;
        self.max_wake_wait_time = self.max_wake_wait_time.max(other.max_wake_wait_time);
        if !other.total_wake_wait_time.is_zero() {
            merge_min(
                &mut self.min_wake_wait_time,
                other.min_wake_wait_time,
                had_wake_wait_samples,
            );
        }
        self.fair_queue_sample_count += other.fair_queue_sample_count;
        self.total_fair_queue_waited_task_slices += other.total_fair_queue_waited_task_slices;
        self.max_fair_queue_waited_task_slices = self
            .max_fair_queue_waited_task_slices
            .max(other.max_fair_queue_waited_task_slices);
        if other.fair_queue_sample_count > 0 {
            merge_min(
                &mut self.min_fair_queue_waited_task_slices,
                other.min_fair_queue_waited_task_slices,
                had_fair_queue_samples,
            );
        }
        self.poll_cpu_time += other.poll_cpu_time;
        self.max_poll_cpu_time = self.max_poll_cpu_time.max(other.max_poll_cpu_time);
        self.poll_wall_time += other.poll_wall_time;
        self.max_poll_wall_time = self.max_poll_wall_time.max(other.max_poll_wall_time);
        if other.poll_count > 0 {
            merge_min(
                &mut self.min_poll_cpu_time,
                other.min_poll_cpu_time,
                had_poll_samples,
            );
            merge_min(
                &mut self.min_poll_wall_time,
                other.min_poll_wall_time,
                had_poll_samples,
            );
        }
    }

    pub fn is_empty(&self) -> bool {
        self.task_count == 0
    }
}

fn merge_min<T: Ord + Copy>(current: &mut T, candidate: T, has_current: bool) {
    if !has_current || candidate < *current {
        *current = candidate;
    }
}

impl fmt::Display for SnapshotPoolTaskDetails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return Ok(());
        }
        let mut output = format!("{{tasks:{}", self.task_count);
        write_pool_count(
            &mut output,
            "poll_count",
            self.poll_count,
            self.task_count,
            self.max_poll_count,
            self.min_poll_count,
        );
        write_pool_count(
            &mut output,
            "dispatch_count",
            self.dispatch_count,
            0,
            self.max_dispatch_count,
            self.min_dispatch_count,
        );
        write_pool_time(
            &mut output,
            "task_wall_time",
            self.total_wall_time,
            self.task_wall_time_sample_count,
            self.max_task_wall_time,
            self.min_task_wall_time,
        );
        write_pool_time(
            &mut output,
            "queue_wait",
            self.total_queue_wait_time,
            self.dispatch_count,
            self.max_queue_wait_time,
            self.min_queue_wait_time,
        );
        let wake_wait_count = self.dispatch_count.saturating_sub(self.task_count);
        write_pool_time(
            &mut output,
            "wake_wait",
            self.total_wake_wait_time,
            wake_wait_count,
            self.max_wake_wait_time,
            self.min_wake_wait_time,
        );
        output.push_str(&format!(
            ", fair_queue:{{enabled:{}, waited_task_slices:{{total:{}",
            self.fair_queue_sample_count > 0,
            self.total_fair_queue_waited_task_slices
        ));
        if self.fair_queue_sample_count > 0 {
            output.push_str(&format!(
                ", avg:{}",
                format_average(
                    self.total_fair_queue_waited_task_slices,
                    self.fair_queue_sample_count
                )
            ));
        }
        output.push_str(&format!(
            ", max:{}, min:{}}}}}",
            self.max_fair_queue_waited_task_slices, self.min_fair_queue_waited_task_slices
        ));
        write_pool_time(
            &mut output,
            "poll_cpu",
            self.poll_cpu_time,
            self.poll_count,
            self.max_poll_cpu_time,
            self.min_poll_cpu_time,
        );
        write_pool_time(
            &mut output,
            "poll_wall",
            self.poll_wall_time,
            self.poll_count,
            self.max_poll_wall_time,
            self.min_poll_wall_time,
        );
        output.push('}');
        formatter.write_str(&output)
    }
}

fn write_pool_count(
    output: &mut String,
    name: &str,
    total: u64,
    average_divisor: u64,
    maximum: u64,
    minimum: u64,
) {
    output.push_str(&format!(", {name}:{{total:{total}"));
    if average_divisor > 0 {
        output.push_str(&format!(", avg:{}", format_average(total, average_divisor)));
    }
    output.push_str(&format!(", max:{maximum}, min:{minimum}}}"));
}

fn format_average(total: u64, count: u64) -> String {
    format!("{:.2}", total as f64 / count as f64)
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}

fn write_pool_time(
    output: &mut String,
    name: &str,
    total: Duration,
    sample_count: u64,
    maximum: Duration,
    minimum: Duration,
) {
    if total.is_zero() {
        return;
    }
    output.push_str(&format!(", {name}:{{total:{}", format_duration(total)));
    if sample_count > 0 {
        let average_nanos = total.as_nanos() / u128::from(sample_count);
        let average = Duration::from_nanos(average_nanos.try_into().unwrap_or(u64::MAX));
        output.push_str(&format!(", avg:{}", format_duration(average)));
    }
    output.push_str(&format!(
        ", max:{}, min:{}}}",
        format_duration(maximum),
        format_duration(minimum)
    ));
}

/// Runtime statistics collected for a snapshot's physical TiKV read RPCs.
///
/// Attach this to a [`crate::Snapshot`] with
/// [`crate::Snapshot::set_runtime_stats`]. The collector is shared so callers
/// can inspect it while a snapshot is active; [`Self::clone`] creates an
/// independent point-in-time copy, matching client-go's runtime-stats clone
/// contract.
#[derive(Default)]
pub struct SnapshotRuntimeStats {
    inner: Mutex<SnapshotRuntimeStatsInner>,
    region_request_stats: Arc<crate::RegionRequestRuntimeStats>,
}

impl Clone for SnapshotRuntimeStats {
    fn clone(&self) -> Self {
        Self {
            inner: Mutex::new(
                self.inner
                    .lock()
                    .expect("snapshot stats lock poisoned")
                    .clone(),
            ),
            region_request_stats: Arc::new((*self.region_request_stats).clone()),
        }
    }
}

impl SnapshotRuntimeStats {
    /// Create an empty snapshot runtime-stat collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of completed physical RPCs for `command`.
    pub fn rpc_count(&self, command: SnapshotRpcCommand) -> u64 {
        self.inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .rpc
            .get(&command)
            .map_or(0, |stat| stat.count)
    }

    /// Return the cumulative transport duration for completed physical RPCs
    /// for `command`.
    pub fn rpc_duration(&self, command: SnapshotRpcCommand) -> Duration {
        self.inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .rpc
            .get(&command)
            .map_or(Duration::ZERO, |stat| stat.duration)
    }

    /// Return a point-in-time copy of the accumulated TiKV scan detail.
    pub fn scan_detail(&self) -> SnapshotScanDetail {
        self.inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .scan_detail
            .clone()
    }

    /// Return a point-in-time copy of the accumulated TiKV execution times.
    pub fn time_detail(&self) -> SnapshotTimeDetail {
        self.inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .time_detail
            .clone()
    }

    /// Return a point-in-time copy of TiKV read-pool task details, if any.
    pub fn read_pool_task_details(&self) -> Option<SnapshotPoolTaskDetails> {
        let detail = self
            .inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .read_pool_task_details
            .clone();
        (!detail.is_empty()).then_some(detail)
    }

    /// Return time spent resolving locks encountered by this snapshot's reads.
    pub fn resolve_lock_duration(&self) -> Duration {
        self.inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .resolve_lock_duration
    }

    /// Return the number of completed snapshot backoff sleeps for a
    /// client-go retry class such as `regionMiss` or `txnLockFast`.
    pub fn backoff_count(&self, retry_type: &str) -> u64 {
        self.inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .backoff
            .get(retry_type)
            .map_or(0, |stat| stat.count)
    }

    /// Return the scheduled sleep accumulated for a client-go retry class.
    pub fn backoff_duration(&self, retry_type: &str) -> Duration {
        self.inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .backoff
            .get(retry_type)
            .map_or(Duration::ZERO, |stat| stat.duration)
    }

    /// Return bounded region- and transport-error counts collected by the
    /// physical region sender.
    pub fn request_error_stats(&self) -> crate::RequestErrorStats {
        self.region_request_stats.error_stats()
    }

    /// Return the first failed replica attempts and their peer-level overflow
    /// counts for this snapshot.
    pub fn replica_access_stats(&self) -> crate::ReplicaAccessStats {
        self.region_request_stats.replica_access_stats()
    }

    /// Merge another collector into this one, matching client-go's
    /// `SnapshotRuntimeStats.Merge` ownership model.
    pub fn merge(&self, other: &Self) {
        let other_region_request_stats = (*other.region_request_stats).clone();
        let other = other
            .inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .clone();
        let mut inner = self.inner.lock().expect("snapshot stats lock poisoned");
        for (command, stat) in other.rpc {
            let merged = inner.rpc.entry(command).or_default();
            merged.count += stat.count;
            merged.duration += stat.duration;
        }
        inner.scan_detail.merge(&other.scan_detail);
        inner.time_detail.merge(&other.time_detail);
        inner
            .read_pool_task_details
            .merge(&other.read_pool_task_details);
        inner.resolve_lock_duration += other.resolve_lock_duration;
        for (retry_type, stat) in other.backoff {
            let merged = inner.backoff.entry(retry_type).or_default();
            merged.count += stat.count;
            merged.duration += stat.duration;
        }
        drop(inner);
        self.region_request_stats.merge(&other_region_request_stats);
    }

    pub(crate) fn region_request_runtime_stats(&self) -> Arc<crate::RegionRequestRuntimeStats> {
        Arc::clone(&self.region_request_stats)
    }

    pub(crate) fn interceptor(self: &Arc<Self>) -> Arc<dyn RpcInterceptor> {
        Arc::new(SnapshotRuntimeStatsInterceptor {
            stats: Arc::clone(self),
        })
    }

    fn record_rpc(&self, command: SnapshotRpcCommand, duration: Duration) {
        let mut inner = self.inner.lock().expect("snapshot stats lock poisoned");
        let stat = inner.rpc.entry(command).or_default();
        stat.count += 1;
        stat.duration += duration;
    }

    fn record_exec_detail(&self, detail: &kvrpcpb::ExecDetailsV2) {
        let mut inner = self.inner.lock().expect("snapshot stats lock poisoned");
        if let Some(scan_detail) = &detail.scan_detail_v2 {
            inner.scan_detail.merge_from_pb(scan_detail);
        }
        inner
            .time_detail
            .merge_from_pb(detail.time_detail_v2.as_ref(), detail.time_detail.as_ref());
        if let Some(detail) = detail.read_pool_task_details.as_ref() {
            inner.read_pool_task_details.merge_from_pb(detail);
        }
    }

    pub(crate) fn record_resolve_lock(&self, duration: Duration) {
        self.inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .resolve_lock_duration += duration;
    }

    pub(crate) fn record_backoff(&self, retry_type: &'static str, duration: Duration) {
        let mut inner = self.inner.lock().expect("snapshot stats lock poisoned");
        let stat = inner.backoff.entry(retry_type).or_default();
        stat.count += 1;
        stat.duration += duration;
    }
}

pub(crate) fn snapshot_read_sli_interceptor() -> Arc<dyn RpcInterceptor> {
    Arc::new(SnapshotReadSliInterceptor)
}

struct SnapshotReadSliInterceptor;

impl RpcInterceptor for SnapshotReadSliInterceptor {
    fn name(&self) -> &str {
        "snapshot-read-sli"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn wrap<'a>(
        &'a self,
        _: &'a str,
        _: &'a dyn Request,
        next: RpcNext<'a>,
    ) -> BoxFuture<'a, RpcDispatchResult> {
        Box::pin(async move {
            let result = next().await;
            if let Ok(response) = &result {
                observe_snapshot_read_sli(response.as_ref());
            }
            result
        })
    }
}

impl fmt::Display for SnapshotRuntimeStats {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.lock().expect("snapshot stats lock poisoned");
        let request_errors = self.region_request_stats.error_stats();
        let mut output = String::new();
        for (index, (command, stat)) in inner.rpc.iter().enumerate() {
            let separator = if index == 0 { "" } else { "," };
            output.push_str(&format!(
                "{separator}{command}:{{num_rpc:{}, total_time:{}}}",
                stat.count,
                format_duration(stat.duration)
            ));
        }
        if request_errors.distinct_error_count() > 0 {
            output.push_str(", rpc_errors:");
            output.push_str(&request_errors.to_string());
        }
        for (retry_type, stat) in &inner.backoff {
            if !output.is_empty() {
                output.push(',');
            }
            output.push_str(&format!(
                "{retry_type}_backoff:{{num:{}, total_time:{}}}",
                stat.count,
                format_duration(stat.duration)
            ));
        }
        if let Some(detail) = format_time_detail(&inner.time_detail) {
            output.push_str(", ");
            output.push_str(&detail);
        }
        if !inner.resolve_lock_duration.is_zero() {
            output.push_str(", ");
            output.push_str("resolve_lock_time:");
            output.push_str(&format_duration(inner.resolve_lock_duration));
        }
        if let Some(detail) = format_scan_detail(&inner.scan_detail) {
            output.push_str(", ");
            output.push_str(&detail);
        }
        if !inner.read_pool_task_details.is_empty() {
            if !output.is_empty() {
                output.push_str(", ");
            }
            output.push_str("read_pool:");
            output.push_str(&inner.read_pool_task_details.to_string());
        }
        formatter.write_str(&output)?;
        Ok(())
    }
}

fn format_time_detail(detail: &SnapshotTimeDetail) -> Option<String> {
    let mut fields = Vec::new();
    for (name, duration) in [
        ("total_process_time", detail.process_time),
        ("total_suspend_time", detail.suspend_time),
        ("total_wait_time", detail.wait_time),
        ("total_kv_read_wall_time", detail.kv_read_wall_time),
        ("tikv_grpc_process_time", detail.kv_grpc_process_time),
        ("tikv_grpc_wait_time", detail.kv_grpc_wait_time),
        ("tikv_wall_time", detail.total_rpc_wall_time),
    ] {
        if !duration.is_zero() {
            fields.push(format!("{name}: {}", format_duration(duration)));
        }
    }
    (!fields.is_empty()).then(|| format!("time_detail: {{{}}}", fields.join(", ")))
}

fn format_scan_detail(detail: &SnapshotScanDetail) -> Option<String> {
    if detail.total_keys == 0
        && detail.processed_keys == 0
        && detail.processed_keys_size == 0
        && detail.rocksdb_delete_skipped_count == 0
        && detail.rocksdb_key_skipped_count == 0
        && detail.rocksdb_block_cache_hit_count == 0
        && detail.rocksdb_block_read_count == 0
        && detail.rocksdb_block_read_byte == 0
        && detail.rocksdb_block_read_duration.is_zero()
        && detail.get_snapshot_duration.is_zero()
        && detail.ia_cache_hit_count == 0
        && detail.ia_remote_read_segment_count == 0
        && detail.ia_remote_read_segment_bytes == 0
        && detail.ia_remote_read_segment_duration.is_zero()
    {
        return None;
    }
    let mut fields = Vec::new();
    if detail.processed_keys > 0 {
        fields.push(format!("total_process_keys: {}", detail.processed_keys));
    }
    if detail.processed_keys_size > 0 {
        fields.push(format!(
            "total_process_keys_size: {}",
            detail.processed_keys_size
        ));
    }
    if detail.total_keys > 0 {
        fields.push(format!("total_keys: {}", detail.total_keys));
    }
    if !detail.get_snapshot_duration.is_zero() {
        fields.push(format!(
            "get_snapshot_time: {}",
            format_duration(detail.get_snapshot_duration)
        ));
    }
    if detail.ia_cache_hit_count > 0
        || detail.ia_remote_read_segment_count > 0
        || detail.ia_remote_read_segment_bytes > 0
        || !detail.ia_remote_read_segment_duration.is_zero()
    {
        let mut ia = Vec::new();
        if detail.ia_cache_hit_count > 0 {
            ia.push(format!("cache_hit_count: {}", detail.ia_cache_hit_count));
        }
        if detail.ia_remote_read_segment_count > 0 {
            ia.push(format!(
                "remote_read_segment_count: {}",
                detail.ia_remote_read_segment_count
            ));
        }
        if detail.ia_remote_read_segment_bytes > 0 {
            ia.push(format!(
                "remote_read_segment_bytes: {}",
                format_bytes(detail.ia_remote_read_segment_bytes)
            ));
        }
        if !detail.ia_remote_read_segment_duration.is_zero() {
            ia.push(format!(
                "remote_read_segment_wait_time: {}",
                format_duration(detail.ia_remote_read_segment_duration)
            ));
        }
        fields.push(format!("ia: {{{}}}", ia.join(", ")));
    }
    let mut rocksdb = Vec::new();
    if detail.rocksdb_delete_skipped_count > 0 {
        rocksdb.push(format!(
            "delete_skipped_count: {}",
            detail.rocksdb_delete_skipped_count
        ));
    }
    if detail.rocksdb_key_skipped_count > 0 {
        rocksdb.push(format!(
            "key_skipped_count: {}",
            detail.rocksdb_key_skipped_count
        ));
    }
    let mut block = Vec::new();
    if detail.rocksdb_block_cache_hit_count > 0 {
        block.push(format!(
            "cache_hit_count: {}",
            detail.rocksdb_block_cache_hit_count
        ));
    }
    if detail.rocksdb_block_read_count > 0 {
        block.push(format!("read_count: {}", detail.rocksdb_block_read_count));
    }
    if detail.rocksdb_block_read_byte > 0 {
        block.push(format!(
            "read_byte: {}",
            format_bytes(detail.rocksdb_block_read_byte)
        ));
    }
    if !detail.rocksdb_block_read_duration.is_zero() {
        block.push(format!(
            "read_time: {}",
            format_duration(detail.rocksdb_block_read_duration)
        ));
    }
    rocksdb.push(format!("block: {{{}}}", block.join(", ")));
    fields.push(format!("rocksdb: {{{}}}", rocksdb.join(", ")));
    Some(format!("scan_detail: {{{}}}", fields.join(", ")))
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1 << 10;
    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;
    if bytes <= KIB {
        return format!("{bytes} Bytes");
    }
    let (unit, suffix) = if bytes > GIB {
        (GIB, "GB")
    } else if bytes > MIB {
        (MIB, "MB")
    } else {
        (KIB, "KB")
    };
    let precision = if bytes.is_multiple_of(unit) {
        0
    } else if bytes < unit * 10 {
        2
    } else {
        1
    };
    if precision == 0 {
        return format!("{} {suffix}", bytes / unit);
    }
    let scale = 10_u64.pow(precision);
    let scaled = (bytes % unit * scale + unit / 2) / unit;
    let whole = bytes / unit + scaled / scale;
    let fraction = scaled % scale;
    if precision == 1 {
        format!("{whole}.{fraction} {suffix}")
    } else {
        format!("{whole}.{fraction:02} {suffix}")
    }
}

struct SnapshotRuntimeStatsInterceptor {
    stats: Arc<SnapshotRuntimeStats>,
}

impl RpcInterceptor for SnapshotRuntimeStatsInterceptor {
    fn name(&self) -> &str {
        "snapshot-runtime-stats"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn wrap<'a>(
        &'a self,
        _: &'a str,
        request: &'a dyn Request,
        next: RpcNext<'a>,
    ) -> BoxFuture<'a, RpcDispatchResult> {
        let command = snapshot_rpc_command(request);
        let stats = Arc::clone(&self.stats);
        Box::pin(async move {
            let started = Instant::now();
            let result = next().await;
            if let Some(command) = command {
                stats.record_rpc(command, started.elapsed());
            }
            if let Ok(response) = &result {
                if let Some(detail) = snapshot_exec_detail(response.as_ref()) {
                    stats.record_exec_detail(detail);
                }
            }
            result
        })
    }
}

fn observe_snapshot_read_sli(response: &dyn Any) {
    let (read_keys, detail) =
        if let Some(response) = response.downcast_ref::<kvrpcpb::GetResponse>() {
            (
                response.value.len() as u64,
                response.exec_details_v2.as_ref(),
            )
        } else if let Some(response) = response.downcast_ref::<kvrpcpb::BatchGetResponse>() {
            (
                response.pairs.len() as u64,
                response.exec_details_v2.as_ref(),
            )
        } else if let Some(response) = response.downcast_ref::<kvrpcpb::BufferBatchGetResponse>() {
            (
                response.pairs.len() as u64,
                response.exec_details_v2.as_ref(),
            )
        } else {
            return;
        };
    let Some(detail) = detail else {
        return;
    };
    let read_time = if let Some(time) = detail.time_detail_v2.as_ref() {
        time.kv_read_wall_time_ns as f64 / 1_000_000_000.0
    } else {
        detail
            .time_detail
            .as_ref()
            .map_or(0.0, |time| time.kv_read_wall_time_ms as f64 / 1_000.0)
    };
    let read_size = detail
        .scan_detail_v2
        .as_ref()
        .map_or(0.0, |scan| scan.processed_versions_size as f64);
    crate::stats::observe_snapshot_read_sli(read_keys, read_time, read_size);
}

fn snapshot_exec_detail(response: &dyn Any) -> Option<&kvrpcpb::ExecDetailsV2> {
    if let Some(response) = response.downcast_ref::<kvrpcpb::GetResponse>() {
        response
            .region_error
            .is_none()
            .then_some(())
            .and(response.exec_details_v2.as_ref())
    } else if let Some(response) = response.downcast_ref::<kvrpcpb::BatchGetResponse>() {
        response
            .region_error
            .is_none()
            .then_some(())
            .and(response.exec_details_v2.as_ref())
    } else if let Some(response) = response.downcast_ref::<kvrpcpb::BufferBatchGetResponse>() {
        response
            .region_error
            .is_none()
            .then_some(())
            .and(response.exec_details_v2.as_ref())
    } else {
        None
    }
}

fn snapshot_rpc_command(request: &dyn Request) -> Option<SnapshotRpcCommand> {
    let request = request.as_any();
    if request.is::<kvrpcpb::GetRequest>() {
        Some(SnapshotRpcCommand::Get)
    } else if request.is::<kvrpcpb::BatchGetRequest>() {
        Some(SnapshotRpcCommand::BatchGet)
    } else if request.is::<kvrpcpb::BufferBatchGetRequest>() {
        Some(SnapshotRpcCommand::BufferBatchGet)
    } else if request.is::<kvrpcpb::ScanRequest>() {
        Some(SnapshotRpcCommand::Scan)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_and_merge_preserve_independent_rpc_totals() {
        let stats = SnapshotRuntimeStats::new();
        stats.record_rpc(SnapshotRpcCommand::Get, Duration::from_millis(3));
        stats.record_exec_detail(&kvrpcpb::ExecDetailsV2 {
            time_detail: Some(kvrpcpb::TimeDetail {
                process_wall_time_ms: 4,
                ..Default::default()
            }),
            scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                processed_versions: 5,
                ..Default::default()
            }),
            ..Default::default()
        });
        stats.record_resolve_lock(Duration::from_millis(6));
        stats.record_backoff("regionMiss", Duration::from_millis(7));
        stats.region_request_stats.record_error("region_not_found");
        let cloned = stats.clone();
        stats.record_rpc(SnapshotRpcCommand::Get, Duration::from_millis(2));

        assert_eq!(cloned.rpc_count(SnapshotRpcCommand::Get), 1);
        assert_eq!(
            cloned.rpc_duration(SnapshotRpcCommand::Get),
            Duration::from_millis(3)
        );
        assert_eq!(cloned.time_detail().process_time, Duration::from_millis(4));
        assert_eq!(cloned.scan_detail().processed_keys, 5);
        assert_eq!(cloned.resolve_lock_duration(), Duration::from_millis(6));
        assert_eq!(cloned.backoff_count("regionMiss"), 1);
        assert_eq!(
            cloned.request_error_stats().error_count("region_not_found"),
            1
        );
        assert_eq!(
            cloned.backoff_duration("regionMiss"),
            Duration::from_millis(7)
        );

        cloned.merge(&stats);
        assert_eq!(cloned.rpc_count(SnapshotRpcCommand::Get), 3);
        assert_eq!(
            cloned.rpc_duration(SnapshotRpcCommand::Get),
            Duration::from_millis(8)
        );
        assert_eq!(cloned.time_detail().process_time, Duration::from_millis(8));
        assert_eq!(cloned.scan_detail().processed_keys, 10);
        assert_eq!(cloned.resolve_lock_duration(), Duration::from_millis(12));
        assert_eq!(cloned.backoff_count("regionMiss"), 2);
        assert_eq!(
            cloned.request_error_stats().error_count("region_not_found"),
            2
        );
        assert_eq!(
            cloned.backoff_duration("regionMiss"),
            Duration::from_millis(14)
        );
    }

    #[test]
    fn display_matches_client_go_runtime_stat_format() {
        let stats = SnapshotRuntimeStats::new();
        stats.record_rpc(SnapshotRpcCommand::Get, Duration::from_nanos(9_412_345));
        stats.region_request_stats.record_error("region_not_found");
        stats.record_backoff("regionMiss", Duration::from_nanos(10_412_345));
        stats.record_resolve_lock(Duration::from_nanos(100_450));
        stats.record_exec_detail(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                process_wall_time_ns: 5_999_000,
                ..Default::default()
            }),
            scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                total_versions: 9,
                processed_versions: 5,
                processed_versions_size: 12,
                get_snapshot_nanos: 1_234_000,
                rocksdb_delete_skipped_count: 1,
                rocksdb_block_cache_hit_count: 2,
                rocksdb_block_read_byte: 1_536,
                rocksdb_block_read_nanos: 12_345,
                ia_cache_hit_count: 3,
                ia_remote_read_segment_bytes: 1_536,
                ia_remote_read_segment_nanos: 11_001,
                read_index_propose_wait_nanos: 99,
                ..Default::default()
            }),
            ..Default::default()
        });

        assert_eq!(
            stats.to_string(),
            "Get:{num_rpc:1, total_time:9.41ms}, rpc_errors:{region_not_found:1},regionMiss_backoff:{num:1, total_time:10.4ms}, time_detail: {total_process_time: 6ms}, resolve_lock_time:100.5µs, scan_detail: {total_process_keys: 5, total_process_keys_size: 12, total_keys: 9, get_snapshot_time: 1.23ms, ia: {cache_hit_count: 3, remote_read_segment_bytes: 1.50 KB, remote_read_segment_wait_time: 11µs}, rocksdb: {delete_skipped_count: 1, block: {cache_hit_count: 2, read_byte: 1.50 KB, read_time: 12.3µs}}}"
        );
    }

    #[test]
    fn read_pool_details_merge_clone_and_format_like_client_go() {
        let stats = SnapshotRuntimeStats::new();
        stats.record_exec_detail(&kvrpcpb::ExecDetailsV2 {
            read_pool_task_details: Some(kvrpcpb::PoolTaskDetails {
                poll_count: 2,
                dispatch_count: 1,
                ..Default::default()
            }),
            ..Default::default()
        });

        assert_eq!(
            stats.to_string(),
            "read_pool:{tasks:1, poll_count:{total:2, avg:2, max:2, min:2}, dispatch_count:{total:1, max:1, min:1}, fair_queue:{enabled:false, waited_task_slices:{total:0, max:0, min:0}}}"
        );
        let details = stats.read_pool_task_details().unwrap();
        assert_eq!(details.task_count, 1);
        assert_eq!(details.poll_count, 2);
        assert_eq!(details.dispatch_count, 1);

        let cloned = stats.clone();
        stats.record_exec_detail(&kvrpcpb::ExecDetailsV2 {
            read_pool_task_details: Some(kvrpcpb::PoolTaskDetails {
                poll_count: 4,
                dispatch_count: 3,
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(cloned.read_pool_task_details().unwrap(), details);
        cloned.merge(&stats);
        let merged = cloned.read_pool_task_details().unwrap();
        assert_eq!(merged.task_count, 3);
        assert_eq!(merged.poll_count, 8);
        assert_eq!(merged.min_poll_count, 2);
        assert_eq!(merged.max_poll_count, 4);
    }

    #[test]
    fn duration_formatting_matches_client_go_precision_rules() {
        for (duration, expected) in [
            (Duration::from_nanos(999), "999ns"),
            (Duration::from_nanos(1_000), "1µs"),
            (Duration::from_nanos(1_001), "1µs"),
            (Duration::from_nanos(9_412_345), "9.41ms"),
            (Duration::from_nanos(10_412_345), "10.4ms"),
            (Duration::from_nanos(5_999_000_000), "6s"),
        ] {
            assert_eq!(format_duration(duration), expected);
        }
    }
}
