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

    /// Merge another collector into this one, matching client-go's
    /// `SnapshotRuntimeStats.Merge` ownership model.
    pub fn merge(&self, other: &Self) {
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
        inner.resolve_lock_duration += other.resolve_lock_duration;
        for (retry_type, stat) in other.backoff {
            let merged = inner.backoff.entry(retry_type).or_default();
            merged.count += stat.count;
            merged.duration += stat.duration;
        }
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

impl fmt::Display for SnapshotRuntimeStats {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.lock().expect("snapshot stats lock poisoned");
        let mut output = String::new();
        for (index, (command, stat)) in inner.rpc.iter().enumerate() {
            let separator = if index == 0 { "" } else { "," };
            output.push_str(&format!(
                "{separator}{command}:{{num_rpc:{}, total_time:{}}}",
                stat.count,
                format_duration(stat.duration)
            ));
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
        formatter.write_str(&output)?;
        Ok(())
    }
}

/// Render a non-negative duration with client-go's `util.FormatDuration`
/// precision policy. This is deliberately separate from Rust's `Debug` and
/// `Display` implementations, whose precision rules differ from Go's.
fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos <= 1_000 {
        return match nanos {
            0 => "0s".to_owned(),
            1_000 => "1µs".to_owned(),
            nanos => format!("{nanos}ns"),
        };
    }

    let (unit, suffix) = if nanos >= 1_000_000_000 {
        (1_000_000_000_u128, "s")
    } else if nanos >= 1_000_000 {
        (1_000_000_u128, "ms")
    } else {
        (1_000_u128, "µs")
    };
    let integer = nanos / unit;
    let precision = if integer < 10 { 100 } else { 10 };
    let scaled = ((nanos % unit) * precision + unit / 2) / unit;
    let rounded = integer * precision + scaled;
    let whole = rounded / precision;
    let fraction = rounded % precision;
    if fraction == 0 {
        format!("{whole}{suffix}")
    } else if precision == 100 && fraction % 10 == 0 {
        format!("{whole}.{}{suffix}", fraction / 10)
    } else if precision == 100 {
        format!("{whole}.{fraction:02}{suffix}")
    } else {
        format!("{whole}.{fraction}{suffix}")
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
    let precision = if bytes % unit == 0 {
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
            cloned.backoff_duration("regionMiss"),
            Duration::from_millis(14)
        );
    }

    #[test]
    fn display_matches_client_go_runtime_stat_format() {
        let stats = SnapshotRuntimeStats::new();
        stats.record_rpc(SnapshotRpcCommand::Get, Duration::from_nanos(9_412_345));
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
            "Get:{num_rpc:1, total_time:9.41ms},regionMiss_backoff:{num:1, total_time:10.4ms}, time_detail: {total_process_time: 6ms}, resolve_lock_time:100.5µs, scan_detail: {total_process_keys: 5, total_process_keys_size: 12, total_keys: 9, get_snapshot_time: 1.23ms, ia: {cache_hit_count: 3, remote_read_segment_bytes: 1.50 KB, remote_read_segment_wait_time: 11µs}, rocksdb: {delete_skipped_count: 1, block: {cache_hit_count: 2, read_byte: 1.50 KB, read_time: 12.3µs}}}"
        );
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
