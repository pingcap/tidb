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

//! SEED of Go `pkg/util/execdetails`, covering `execdetails.go`'s formatting
//! surface: the slow-log field-name `*Str` constants, the value shape of
//! `ExecDetails` and the client-go detail types its `String()` reads
//! (`CommitDetails`, `LockKeysDetails`, `ScanDetail`, `TimeDetail`,
//! `ReqDetailInfo`, `TiKVExecDetails`, `WriteDetail`), the byte-exact
//! `ExecDetails.String()` rendering, and `GetIARemoteReadSegmentStats`.
//!
//! Boundaries:
//! - client-go (`github.com/tikv/client-go/v2/util`) source is not on disk
//!   here; the detail-struct field shapes are derived only from how
//!   `execdetails.go` and `execdetails_test.go` use them. Go's mutex-guarded
//!   `CommitDetails.Mu` / `LockKeysDetails.Mu` fields are flattened into
//!   plain fields: the data arrives at this boundary as an already-collected
//!   snapshot, so no lock is carried.
//! - `format_go_duration` matches Go `time.Duration.String()`, and client-go
//!   `util.FormatDuration` is pinned only at the fixture-exercised points
//!   (`500ms`, `20ms`, `40ms`, `10µs`, `45µs`, `101µs`, `1s`, `2s`, `0s`)
//!   where the two coincide; FormatDuration's rounding of long durations to
//!   three significant digits stays open.
//! - The nested client-go `TiKVExecDetails.String()` spelling is recovered
//!   from the expected literal in Go `TestString`; fields the fixture leaves
//!   at zero (nested `time_detail` beyond `tikv_wall_time`, nested
//!   `scan_detail` beyond the rocksdb block, `write_detail` zero-suppression,
//!   `util.FormatBytes` above the plain-`Bytes` branch) stay open.
//! - The runtime-stats/`CopRuntimeStats` half of the package (`P90Summary`,
//!   `SyncExecDetails`, `CopTasksDetails`, `RuntimeStatsColl`, zap fields)
//!   stays open.

use std::fmt;
use std::time::Duration;

/// Go `CopTimeStr`: the sum of cop-task time spent in TiDB distSQL.
pub const COP_TIME_STR: &str = "Cop_time";
/// Go `ProcessTimeStr`: the sum of process time of all coprocessor tasks.
pub const PROCESS_TIME_STR: &str = "Process_time";
/// Go `WaitTimeStr`: the time of all coprocessor wait.
pub const WAIT_TIME_STR: &str = "Wait_time";
/// Go `BackoffTimeStr`: the time of all back-off.
pub const BACKOFF_TIME_STR: &str = "Backoff_time";
/// Go `LockKeysTimeStr`: the pessimistic lock wait interval.
pub const LOCK_KEYS_TIME_STR: &str = "LockKeys_time";
/// Go `RequestCountStr`: the request count.
pub const REQUEST_COUNT_STR: &str = "Request_count";
/// Go `PreWriteTimeStr`: the time of pre-write.
pub const PRE_WRITE_TIME_STR: &str = "Prewrite_time";
/// Go `WaitPrewriteBinlogTimeStr`: the time waiting for prewrite binlog.
pub const WAIT_PREWRITE_BINLOG_TIME_STR: &str = "Wait_prewrite_binlog_time";
/// Go `CommitTimeStr`: the time of commit.
pub const COMMIT_TIME_STR: &str = "Commit_time";
/// Go `GetCommitTSTimeStr`: the time of getting commit ts.
pub const GET_COMMIT_TS_TIME_STR: &str = "Get_commit_ts_time";
/// Go `GetLatestTsTimeStr`: the time of getting latest ts in async commit
/// and 1pc.
pub const GET_LATEST_TS_TIME_STR: &str = "Get_latest_ts_time";
/// Go `CommitBackoffTimeStr`: the time of commit backoff.
pub const COMMIT_BACKOFF_TIME_STR: &str = "Commit_backoff_time";
/// Go `BackoffTypesStr`: the backoff type.
pub const BACKOFF_TYPES_STR: &str = "Backoff_types";
/// Go `SlowestPrewriteRPCDetailStr`: details of the slowest 2pc prewrite RPC.
pub const SLOWEST_PREWRITE_RPC_DETAIL_STR: &str = "Slowest_prewrite_rpc_detail";
/// Go `CommitPrimaryRPCDetailStr`: details of the slowest 2pc commit RPC.
pub const COMMIT_PRIMARY_RPC_DETAIL_STR: &str = "Commit_primary_rpc_detail";
/// Go `ResolveLockTimeStr`: the time of resolving lock.
pub const RESOLVE_LOCK_TIME_STR: &str = "Resolve_lock_time";
/// Go `LocalLatchWaitTimeStr`: the time waiting in local latch.
pub const LOCAL_LATCH_WAIT_TIME_STR: &str = "Local_latch_wait_time";
/// Go `WriteKeysStr`: the count of keys in the transaction.
pub const WRITE_KEYS_STR: &str = "Write_keys";
/// Go `WriteSizeStr`: the key/value size in the transaction.
pub const WRITE_SIZE_STR: &str = "Write_size";
/// Go `PrewriteRegionStr`: the count of regions during pre-write.
pub const PREWRITE_REGION_STR: &str = "Prewrite_region";
/// Go `TxnRetryStr`: the count of transaction retry.
pub const TXN_RETRY_STR: &str = "Txn_retry";
/// Go `GetSnapshotTimeStr`: the time spent getting an engine snapshot.
pub const GET_SNAPSHOT_TIME_STR: &str = "Get_snapshot_time";
/// Go `RocksdbDeleteSkippedCountStr`: rocksdb delete skipped count.
pub const ROCKSDB_DELETE_SKIPPED_COUNT_STR: &str = "Rocksdb_delete_skipped_count";
/// Go `RocksdbKeySkippedCountStr`: rocksdb key skipped count.
pub const ROCKSDB_KEY_SKIPPED_COUNT_STR: &str = "Rocksdb_key_skipped_count";
/// Go `RocksdbBlockCacheHitCountStr`: rocksdb block cache hit count.
pub const ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR: &str = "Rocksdb_block_cache_hit_count";
/// Go `RocksdbBlockReadCountStr`: rocksdb block read count.
pub const ROCKSDB_BLOCK_READ_COUNT_STR: &str = "Rocksdb_block_read_count";
/// Go `RocksdbBlockReadByteStr`: bytes of rocksdb block read.
pub const ROCKSDB_BLOCK_READ_BYTE_STR: &str = "Rocksdb_block_read_byte";
/// Go `RocksdbBlockReadTimeStr`: time spent on rocksdb block read.
pub const ROCKSDB_BLOCK_READ_TIME_STR: &str = "Rocksdb_block_read_time";
/// Go `ProcessKeysStr`: the total processed keys.
pub const PROCESS_KEYS_STR: &str = "Process_keys";
/// Go `TotalKeysStr`: the total scan keys.
pub const TOTAL_KEYS_STR: &str = "Total_keys";
/// Go `IARemoteReadSegmentCountStr`: the number of IA remote segment reads.
pub const IA_REMOTE_READ_SEGMENT_COUNT_STR: &str = "IA_remote_read_segment_count";
/// Go `IARemoteReadSegmentSizeStr`: bytes returned from IA remote segment
/// reads.
pub const IA_REMOTE_READ_SEGMENT_SIZE_STR: &str = "IA_remote_read_segment_size";
/// Go `IARemoteReadSegmentWaitTimeStr`: total time waiting for IA remote
/// segment reads.
pub const IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR: &str = "IA_remote_read_segment_wait_time";

/// Go client-go `util.TimeDetail` — the fields `execdetails.go` reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimeDetail {
    /// Go `TimeDetail.ProcessTime`.
    pub process_time: Duration,
    /// Go `TimeDetail.WaitTime`.
    pub wait_time: Duration,
    /// Go `TimeDetail.TotalRPCWallTime`, rendered as `tikv_wall_time` inside
    /// the nested `time_detail: {...}` block.
    pub total_rpc_wall_time: Duration,
}

/// Go client-go `util.ScanDetail` — the fields `execdetails.go` reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanDetail {
    /// Go `ScanDetail.ProcessedKeys` (`int64`).
    pub processed_keys: i64,
    /// Go `ScanDetail.TotalKeys` (`int64`).
    pub total_keys: i64,
    /// Go `ScanDetail.GetSnapshotDuration`.
    pub get_snapshot_duration: Duration,
    /// Go `ScanDetail.RocksdbDeleteSkippedCount` (`uint64`).
    pub rocksdb_delete_skipped_count: u64,
    /// Go `ScanDetail.RocksdbKeySkippedCount` (`uint64`).
    pub rocksdb_key_skipped_count: u64,
    /// Go `ScanDetail.RocksdbBlockCacheHitCount` (`uint64`).
    pub rocksdb_block_cache_hit_count: u64,
    /// Go `ScanDetail.RocksdbBlockReadCount` (`uint64`).
    pub rocksdb_block_read_count: u64,
    /// Go `ScanDetail.RocksdbBlockReadByte` (`uint64`).
    pub rocksdb_block_read_byte: u64,
    /// Go `ScanDetail.RocksdbBlockReadDuration`.
    pub rocksdb_block_read_duration: Duration,
    /// Go `ScanDetail.IaRemoteReadSegmentCount` (`uint64`), read by
    /// [`get_ia_remote_read_segment_stats`].
    pub ia_remote_read_segment_count: u64,
    /// Go `ScanDetail.IaRemoteReadSegmentBytes` (`uint64`), read by
    /// [`get_ia_remote_read_segment_stats`].
    pub ia_remote_read_segment_bytes: u64,
    /// Go `ScanDetail.IaRemoteReadSegmentDuration`, read by
    /// [`get_ia_remote_read_segment_stats`].
    pub ia_remote_read_segment_duration: Duration,
}

/// Go client-go `util.WriteDetail` — the fields the Go `TestString` fixture
/// sets, plus the scheduler process slot its expected literal proves exists.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteDetail {
    /// Go `WriteDetail.StoreBatchWaitDuration`.
    pub store_batch_wait_duration: Duration,
    /// Go `WriteDetail.ProposeSendWaitDuration`.
    pub propose_send_wait_duration: Duration,
    /// Go `WriteDetail.PersistLogDuration`.
    pub persist_log_duration: Duration,
    /// Go `WriteDetail.RaftDbWriteLeaderWaitDuration`.
    pub raft_db_write_leader_wait_duration: Duration,
    /// Go `WriteDetail.RaftDbSyncLogDuration`.
    pub raft_db_sync_log_duration: Duration,
    /// Go `WriteDetail.RaftDbWriteMemtableDuration`.
    pub raft_db_write_memtable_duration: Duration,
    /// Go `WriteDetail.CommitLogDuration`.
    pub commit_log_duration: Duration,
    /// Go `WriteDetail.ApplyBatchWaitDuration`.
    pub apply_batch_wait_duration: Duration,
    /// Go `WriteDetail.ApplyLogDuration`.
    pub apply_log_duration: Duration,
    /// Go `WriteDetail.ApplyMutexLockDuration`.
    pub apply_mutex_lock_duration: Duration,
    /// Go `WriteDetail.ApplyWriteLeaderWaitDuration`.
    pub apply_write_leader_wait_duration: Duration,
    /// Go `WriteDetail.ApplyWriteWalDuration`.
    pub apply_write_wal_duration: Duration,
    /// Go `WriteDetail.ApplyWriteMemtableDuration`.
    pub apply_write_memtable_duration: Duration,
    /// The duration rendered as `scheduler: {process: ...}` in the nested
    /// spelling. The Go fixture never sets it (it renders `0s` there), so
    /// its client-go field name is derived from the rendering alone.
    pub scheduler_process_duration: Duration,
}

/// Go client-go `util.TiKVExecDetails`: the per-RPC detail bundle nested
/// inside `ReqDetailInfo` and the lock-keys slowest-request snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TiKVExecDetails {
    /// Go `TiKVExecDetails.TimeDetail` (`*util.TimeDetail`).
    pub time_detail: Option<TimeDetail>,
    /// Go `TiKVExecDetails.ScanDetail` (`*util.ScanDetail`).
    pub scan_detail: Option<ScanDetail>,
    /// Go `TiKVExecDetails.WriteDetail` (`*util.WriteDetail`).
    pub write_detail: Option<WriteDetail>,
}

/// Go client-go `util.ReqDetailInfo`: one slowest-RPC record inside
/// `CommitDetails.Mu`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReqDetailInfo {
    /// Go `ReqDetailInfo.ReqTotalTime`.
    pub req_total_time: Duration,
    /// Go `ReqDetailInfo.Region` (`uint64`).
    pub region: u64,
    /// Go `ReqDetailInfo.StoreAddr`.
    pub store_addr: String,
    /// Go `ReqDetailInfo.ExecDetails`.
    pub exec_details: TiKVExecDetails,
}

/// Go client-go `util.ResolveLockDetail` — the field `execdetails.go` reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResolveLockDetail {
    /// Go `ResolveLockDetail.ResolveLockTime`: nanoseconds as `int64`
    /// (loaded atomically in Go; a plain snapshot here).
    pub resolve_lock_time: i64,
}

/// Go client-go `util.CommitDetails` — the fields `ExecDetails.String()`
/// reads. Go guards `CommitBackoffTime` through `CommitPrimary` behind the
/// embedded `Mu sync.Mutex`; here they are flattened into plain fields
/// because the data arrives at this boundary as an already-collected
/// snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitDetails {
    /// Go `CommitDetails.GetCommitTsTime`.
    pub get_commit_ts_time: Duration,
    /// Go `CommitDetails.GetLatestTsTime`.
    pub get_latest_ts_time: Duration,
    /// Go `CommitDetails.PrewriteTime`.
    pub prewrite_time: Duration,
    /// Go `CommitDetails.WaitPrewriteBinlogTime`.
    pub wait_prewrite_binlog_time: Duration,
    /// Go `CommitDetails.CommitTime`.
    pub commit_time: Duration,
    /// Go `CommitDetails.LocalLatchTime`.
    pub local_latch_time: Duration,
    /// Go `CommitDetails.Mu.CommitBackoffTime`: nanoseconds as `int64`.
    pub commit_backoff_time: i64,
    /// Go `CommitDetails.Mu.PrewriteBackoffTypes`.
    pub prewrite_backoff_types: Vec<String>,
    /// Go `CommitDetails.Mu.CommitBackoffTypes`.
    pub commit_backoff_types: Vec<String>,
    /// Go `CommitDetails.Mu.SlowestPrewrite`.
    pub slowest_prewrite: ReqDetailInfo,
    /// Go `CommitDetails.Mu.CommitPrimary`.
    pub commit_primary: ReqDetailInfo,
    /// Go `CommitDetails.ResolveLock`.
    pub resolve_lock: ResolveLockDetail,
    /// Go `CommitDetails.WriteKeys` (`int`).
    pub write_keys: i64,
    /// Go `CommitDetails.WriteSize` (`int`).
    pub write_size: i64,
    /// Go `CommitDetails.PrewriteRegionNum` (`int32`, loaded atomically in
    /// Go; a plain snapshot here).
    pub prewrite_region_num: i32,
    /// Go `CommitDetails.TxnRetry` (`int`).
    pub txn_retry: i64,
}

/// Go client-go `util.LockKeysDetails` — the fields the Go `TestString`
/// fixture sets. `ExecDetails.String()` reads only `total_time`; the rest
/// keeps the fixture's slowest-request fragment representable. The
/// mutex-guarded `Mu` fields are flattened as in [`CommitDetails`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LockKeysDetails {
    /// Go `LockKeysDetails.TotalTime`.
    pub total_time: Duration,
    /// Go `LockKeysDetails.RegionNum` (`int32`).
    pub region_num: i32,
    /// Go `LockKeysDetails.LockKeys` (`int32`).
    pub lock_keys: i32,
    /// Go `LockKeysDetails.ResolveLock`.
    pub resolve_lock: ResolveLockDetail,
    /// Go `LockKeysDetails.BackoffTime`: nanoseconds as `int64`.
    pub backoff_time: i64,
    /// Go `LockKeysDetails.Mu.BackoffTypes`.
    pub backoff_types: Vec<String>,
    /// Go `LockKeysDetails.Mu.SlowestReqTotalTime`.
    pub slowest_req_total_time: Duration,
    /// Go `LockKeysDetails.Mu.SlowestRegion` (`uint64`).
    pub slowest_region: u64,
    /// Go `LockKeysDetails.Mu.SlowestStoreAddr`.
    pub slowest_store_addr: String,
    /// Go `LockKeysDetails.Mu.SlowestExecDetails`.
    pub slowest_exec_details: TiKVExecDetails,
    /// Go `LockKeysDetails.LockRPCTime` (`int64` nanoseconds).
    pub lock_rpc_time: i64,
    /// Go `LockKeysDetails.LockRPCCount` (`int64`).
    pub lock_rpc_count: i64,
    /// Go `LockKeysDetails.RetryCount` (`int`).
    pub retry_count: i64,
}

/// Go `CopExecDetails`: cop execution detail information.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopExecDetails {
    /// Go `CopExecDetails.ScanDetail` (`*util.ScanDetail`).
    pub scan_detail: Option<ScanDetail>,
    /// Go `CopExecDetails.TimeDetail`.
    pub time_detail: TimeDetail,
    /// Go `CopExecDetails.CalleeAddress`.
    pub callee_address: String,
    /// Go `CopExecDetails.BackoffTime`.
    pub backoff_time: Duration,
}

/// Go `ExecDetails`: execution detail information. Go embeds
/// `CopExecDetails`; here it is a named field read through the same paths.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExecDetails {
    /// Go's embedded `CopExecDetails`.
    pub cop_exec_details: CopExecDetails,
    /// Go `ExecDetails.CommitDetail` (`*util.CommitDetails`).
    pub commit_detail: Option<CommitDetails>,
    /// Go `ExecDetails.LockKeysDetail` (`*util.LockKeysDetails`).
    pub lock_keys_detail: Option<LockKeysDetails>,
    /// Go `ExecDetails.SharedLockKeysDetail` (`*util.LockKeysDetails`).
    pub shared_lock_keys_detail: Option<LockKeysDetails>,
    /// Go `ExecDetails.CopTime`.
    pub cop_time: Duration,
    /// Go `ExecDetails.LockKeysDuration`.
    pub lock_keys_duration: Duration,
    /// Go `ExecDetails.RequestCount` (`int`).
    pub request_count: i64,
}

/// Go `IARemoteReadSegmentStats`: IA remote-read scan statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IaRemoteReadSegmentStats {
    /// Go `IARemoteReadSegmentStats.Count` (`uint64`).
    pub count: u64,
    /// Go `IARemoteReadSegmentStats.Bytes` (`uint64`).
    pub bytes: u64,
    /// Go `IARemoteReadSegmentStats.WaitTime`.
    pub wait_time: Duration,
}

/// Go `GetIARemoteReadSegmentStats`: reads IA remote-read scan statistics
/// from a client-go `ScanDetail`, returning zeros for a nil detail.
#[must_use]
pub fn get_ia_remote_read_segment_stats(
    scan_detail: Option<&ScanDetail>,
) -> IaRemoteReadSegmentStats {
    match scan_detail {
        None => IaRemoteReadSegmentStats::default(),
        Some(detail) => IaRemoteReadSegmentStats {
            count: detail.ia_remote_read_segment_count,
            bytes: detail.ia_remote_read_segment_bytes,
            wait_time: detail.ia_remote_read_segment_duration,
        },
    }
}

/// Renders a duration's seconds the way Go spells
/// `strconv.FormatFloat(d.Seconds(), 'f', -1, 64)`: shortest decimal that
/// round-trips the float64, never exponent notation. Rust's `f64` `Display`
/// has exactly that contract.
#[must_use]
pub fn format_seconds(d: Duration) -> String {
    format!("{}", d.as_secs_f64())
}

/// Renders a duration's seconds the way Go spells
/// `strconv.FormatFloat(d.Seconds(), 'f', 3, 64)`: fixed three decimals.
#[must_use]
pub fn format_seconds_3(d: Duration) -> String {
    format!("{:.3}", d.as_secs_f64())
}

/// Renders a duration the way Go `time.Duration.String()` does for
/// non-negative durations (`0s`, `10µs`, `500ms`, `1s`, `1h2m3.5s`).
/// client-go's `util.FormatDuration` — which additionally rounds long
/// durations to three significant digits — is pinned only where the Go
/// `TestString` fixture exercises it, and at every such point it coincides
/// with this spelling.
#[must_use]
pub fn format_go_duration(d: Duration) -> String {
    let total = d.as_nanos();
    if total == 0 {
        return "0s".to_owned();
    }
    if total < 1_000_000_000 {
        let (scale, prec, unit) = if total < 1_000 {
            (1u128, 0usize, "ns")
        } else if total < 1_000_000 {
            (1_000, 3, "µs")
        } else {
            (1_000_000, 6, "ms")
        };
        let mut out = (total / scale).to_string();
        push_fraction(&mut out, total % scale, prec);
        out.push_str(unit);
        return out;
    }
    let secs = total / 1_000_000_000;
    let mut tail = (secs % 60).to_string();
    push_fraction(&mut tail, total % 1_000_000_000, 9);
    tail.push('s');
    let minutes = secs / 60;
    if minutes == 0 {
        return tail;
    }
    let hours = minutes / 60;
    if hours == 0 {
        return format!("{minutes}m{tail}");
    }
    format!("{hours}h{}m{tail}", minutes % 60)
}

/// Appends Go's trimmed fractional digits (`fmtFrac`): `prec` zero-padded
/// digits with trailing zeros removed, and no dot when nothing remains.
fn push_fraction(out: &mut String, frac: u128, prec: usize) {
    if frac == 0 || prec == 0 {
        return;
    }
    let mut digits = format!("{frac:0prec$}");
    while digits.ends_with('0') {
        digits.pop();
    }
    if !digits.is_empty() {
        out.push('.');
        out.push_str(&digits);
    }
}

/// Renders a byte count the way the fixture pins client-go
/// `util.FormatBytes`: small values spell as `N Bytes`. The larger-unit
/// branches of FormatBytes stay open (source not readable here).
fn format_bytes(n: u64) -> String {
    format!("{n} Bytes")
}

/// Renders a Go `[]string` the way `fmt.Sprintf("%v", s)` does:
/// space-joined inside brackets.
fn format_go_string_slice(items: &[String]) -> String {
    format!("[{}]", items.join(" "))
}

/// The nested spelling of client-go `TiKVExecDetails.String()`, recovered
/// from the expected literal in Go `TestString`. Sections render only for
/// present (non-nil in Go) details, joined by `, `.
impl fmt::Display for TiKVExecDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sections = Vec::with_capacity(3);
        if let Some(time_detail) = &self.time_detail {
            sections.push(format!(
                "time_detail: {{{}}}",
                nested_time_detail(time_detail)
            ));
        }
        if let Some(scan_detail) = &self.scan_detail {
            sections.push(format!(
                "scan_detail: {{{}}}",
                nested_scan_detail(scan_detail)
            ));
        }
        if let Some(write_detail) = &self.write_detail {
            sections.push(format!(
                "write_detail: {{{}}}",
                nested_write_detail(write_detail)
            ));
        }
        f.write_str(&sections.join(", "))
    }
}

/// The body of the nested `time_detail: {...}` block. Only the
/// `tikv_wall_time` arm is pinned by the fixture; the nested spelling of
/// `process_time`/`wait_time` stays open and is not rendered.
fn nested_time_detail(detail: &TimeDetail) -> String {
    if detail.total_rpc_wall_time > Duration::ZERO {
        format!(
            "tikv_wall_time: {}",
            format_go_duration(detail.total_rpc_wall_time)
        )
    } else {
        String::new()
    }
}

/// The body of the nested `scan_detail: {...}` block, exactly as the fixture
/// pins it. The fixture sets every rendered field non-zero, so whether
/// client-go suppresses zero fields here stays open; this renders the pinned
/// fields unconditionally.
fn nested_scan_detail(detail: &ScanDetail) -> String {
    format!(
        "total_process_keys: {}, total_keys: {}, rocksdb: {{delete_skipped_count: {}, \
         key_skipped_count: {}, block: {{cache_hit_count: {}, read_count: {}, read_byte: {}, \
         read_time: {}}}}}",
        detail.processed_keys,
        detail.total_keys,
        detail.rocksdb_delete_skipped_count,
        detail.rocksdb_key_skipped_count,
        detail.rocksdb_block_cache_hit_count,
        detail.rocksdb_block_read_count,
        format_bytes(detail.rocksdb_block_read_byte),
        format_go_duration(detail.rocksdb_block_read_duration),
    )
}

/// The body of the nested `write_detail: {...}` block, exactly as the
/// fixture pins it — including the asymmetric `persist_log: {total: ` versus
/// `apply: {total:` spacing and the unconditional `scheduler: {process: ...}`
/// tail the fixture leaves at `0s`.
fn nested_write_detail(detail: &WriteDetail) -> String {
    format!(
        "store_batch_wait: {}, propose_send_wait: {}, persist_log: {{total: {}, \
         write_leader_wait: {}, sync_log: {}, write_memtable: {}}}, commit_log: {}, \
         apply_batch_wait: {}, apply: {{total:{}, mutex_lock: {}, write_leader_wait: {}, \
         write_wal: {}, write_memtable: {}}}, scheduler: {{process: {}}}",
        format_go_duration(detail.store_batch_wait_duration),
        format_go_duration(detail.propose_send_wait_duration),
        format_go_duration(detail.persist_log_duration),
        format_go_duration(detail.raft_db_write_leader_wait_duration),
        format_go_duration(detail.raft_db_sync_log_duration),
        format_go_duration(detail.raft_db_write_memtable_duration),
        format_go_duration(detail.commit_log_duration),
        format_go_duration(detail.apply_batch_wait_duration),
        format_go_duration(detail.apply_log_duration),
        format_go_duration(detail.apply_mutex_lock_duration),
        format_go_duration(detail.apply_write_leader_wait_duration),
        format_go_duration(detail.apply_write_wal_duration),
        format_go_duration(detail.apply_write_memtable_duration),
        format_go_duration(detail.scheduler_process_duration),
    )
}

/// One slowest-RPC part: Go's
/// `{total:<'f',3>s, region_id: <n>, store: <addr>, <TiKVExecDetails>}`.
fn req_detail_part(label: &str, info: &ReqDetailInfo) -> String {
    format!(
        "{label}: {{total:{}s, region_id: {}, store: {}, {}}}",
        format_seconds_3(info.req_total_time),
        info.region,
        info.store_addr,
        info.exec_details,
    )
}

/// Go `ExecDetails.String()`: the space-joined slow-log rendering, arm for
/// arm and byte for byte.
impl fmt::Display for ExecDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<String> = Vec::with_capacity(8);
        if self.cop_time > Duration::ZERO {
            parts.push(format!("{COP_TIME_STR}: {}", format_seconds(self.cop_time)));
        }
        let time_detail = &self.cop_exec_details.time_detail;
        if time_detail.process_time > Duration::ZERO {
            parts.push(format!(
                "{PROCESS_TIME_STR}: {}",
                format_seconds(time_detail.process_time)
            ));
        }
        if time_detail.wait_time > Duration::ZERO {
            parts.push(format!(
                "{WAIT_TIME_STR}: {}",
                format_seconds(time_detail.wait_time)
            ));
        }
        if self.cop_exec_details.backoff_time > Duration::ZERO {
            parts.push(format!(
                "{BACKOFF_TIME_STR}: {}",
                format_seconds(self.cop_exec_details.backoff_time)
            ));
        }
        if let Some(lock_key_details) = &self.lock_keys_detail {
            if lock_key_details.total_time > Duration::ZERO {
                parts.push(format!(
                    "{LOCK_KEYS_TIME_STR}: {}",
                    format_seconds(lock_key_details.total_time)
                ));
            }
        }
        if self.request_count > 0 {
            parts.push(format!("{REQUEST_COUNT_STR}: {}", self.request_count));
        }
        if let Some(commit) = &self.commit_detail {
            if commit.prewrite_time > Duration::ZERO {
                parts.push(format!(
                    "{PRE_WRITE_TIME_STR}: {}",
                    format_seconds(commit.prewrite_time)
                ));
            }
            if commit.wait_prewrite_binlog_time > Duration::ZERO {
                parts.push(format!(
                    "{WAIT_PREWRITE_BINLOG_TIME_STR}: {}",
                    format_seconds(commit.wait_prewrite_binlog_time)
                ));
            }
            if commit.commit_time > Duration::ZERO {
                parts.push(format!(
                    "{COMMIT_TIME_STR}: {}",
                    format_seconds(commit.commit_time)
                ));
            }
            if commit.get_commit_ts_time > Duration::ZERO {
                parts.push(format!(
                    "{GET_COMMIT_TS_TIME_STR}: {}",
                    format_seconds(commit.get_commit_ts_time)
                ));
            }
            if commit.get_latest_ts_time > Duration::ZERO {
                parts.push(format!(
                    "{GET_LATEST_TS_TIME_STR}: {}",
                    format_seconds(commit.get_latest_ts_time)
                ));
            }
            if commit.commit_backoff_time > 0 {
                parts.push(format!(
                    "{COMMIT_BACKOFF_TIME_STR}: {}",
                    format_seconds(Duration::from_nanos(
                        commit.commit_backoff_time.unsigned_abs()
                    ))
                ));
            }
            if !commit.prewrite_backoff_types.is_empty() {
                parts.push(format!(
                    "Prewrite_{BACKOFF_TYPES_STR}: {}",
                    format_go_string_slice(&commit.prewrite_backoff_types)
                ));
            }
            if !commit.commit_backoff_types.is_empty() {
                parts.push(format!(
                    "Commit_{BACKOFF_TYPES_STR}: {}",
                    format_go_string_slice(&commit.commit_backoff_types)
                ));
            }
            if commit.slowest_prewrite.req_total_time > Duration::ZERO {
                parts.push(req_detail_part(
                    SLOWEST_PREWRITE_RPC_DETAIL_STR,
                    &commit.slowest_prewrite,
                ));
            }
            if commit.commit_primary.req_total_time > Duration::ZERO {
                parts.push(req_detail_part(
                    COMMIT_PRIMARY_RPC_DETAIL_STR,
                    &commit.commit_primary,
                ));
            }
            if commit.resolve_lock.resolve_lock_time > 0 {
                parts.push(format!(
                    "{RESOLVE_LOCK_TIME_STR}: {}",
                    format_seconds(Duration::from_nanos(
                        commit.resolve_lock.resolve_lock_time.unsigned_abs()
                    ))
                ));
            }
            if commit.local_latch_time > Duration::ZERO {
                parts.push(format!(
                    "{LOCAL_LATCH_WAIT_TIME_STR}: {}",
                    format_seconds(commit.local_latch_time)
                ));
            }
            if commit.write_keys > 0 {
                parts.push(format!("{WRITE_KEYS_STR}: {}", commit.write_keys));
            }
            if commit.write_size > 0 {
                parts.push(format!("{WRITE_SIZE_STR}: {}", commit.write_size));
            }
            if commit.prewrite_region_num > 0 {
                parts.push(format!(
                    "{PREWRITE_REGION_STR}: {}",
                    commit.prewrite_region_num
                ));
            }
            if commit.txn_retry > 0 {
                parts.push(format!("{TXN_RETRY_STR}: {}", commit.txn_retry));
            }
        }
        if let Some(scan) = &self.cop_exec_details.scan_detail {
            if scan.processed_keys > 0 {
                parts.push(format!("{PROCESS_KEYS_STR}: {}", scan.processed_keys));
            }
            if scan.total_keys > 0 {
                parts.push(format!("{TOTAL_KEYS_STR}: {}", scan.total_keys));
            }
            if scan.get_snapshot_duration > Duration::ZERO {
                parts.push(format!(
                    "{GET_SNAPSHOT_TIME_STR}: {}",
                    format_seconds_3(scan.get_snapshot_duration)
                ));
            }
            if scan.rocksdb_delete_skipped_count > 0 {
                parts.push(format!(
                    "{ROCKSDB_DELETE_SKIPPED_COUNT_STR}: {}",
                    scan.rocksdb_delete_skipped_count
                ));
            }
            if scan.rocksdb_key_skipped_count > 0 {
                parts.push(format!(
                    "{ROCKSDB_KEY_SKIPPED_COUNT_STR}: {}",
                    scan.rocksdb_key_skipped_count
                ));
            }
            if scan.rocksdb_block_cache_hit_count > 0 {
                parts.push(format!(
                    "{ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR}: {}",
                    scan.rocksdb_block_cache_hit_count
                ));
            }
            if scan.rocksdb_block_read_count > 0 {
                parts.push(format!(
                    "{ROCKSDB_BLOCK_READ_COUNT_STR}: {}",
                    scan.rocksdb_block_read_count
                ));
            }
            if scan.rocksdb_block_read_byte > 0 {
                parts.push(format!(
                    "{ROCKSDB_BLOCK_READ_BYTE_STR}: {}",
                    scan.rocksdb_block_read_byte
                ));
            }
            if scan.rocksdb_block_read_duration > Duration::ZERO {
                parts.push(format!(
                    "{ROCKSDB_BLOCK_READ_TIME_STR}: {}",
                    format_seconds_3(scan.rocksdb_block_read_duration)
                ));
            }
        }
        f.write_str(&parts.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The nested `TiKVExecDetails` fixture shared by the slowest-prewrite
    /// slot and the lock-keys slowest-request fragment in Go `TestString`.
    fn fixture_tikv_exec_details() -> TiKVExecDetails {
        TiKVExecDetails {
            time_detail: Some(TimeDetail {
                total_rpc_wall_time: Duration::from_millis(500),
                ..TimeDetail::default()
            }),
            scan_detail: Some(ScanDetail {
                processed_keys: 10,
                total_keys: 100,
                rocksdb_delete_skipped_count: 1,
                rocksdb_key_skipped_count: 1,
                rocksdb_block_cache_hit_count: 1,
                rocksdb_block_read_count: 1,
                rocksdb_block_read_byte: 100,
                rocksdb_block_read_duration: Duration::from_millis(20),
                ..ScanDetail::default()
            }),
            write_detail: Some(WriteDetail {
                store_batch_wait_duration: Duration::from_micros(10),
                propose_send_wait_duration: Duration::from_micros(20),
                persist_log_duration: Duration::from_micros(30),
                raft_db_write_leader_wait_duration: Duration::from_micros(40),
                raft_db_sync_log_duration: Duration::from_micros(45),
                raft_db_write_memtable_duration: Duration::from_micros(50),
                commit_log_duration: Duration::from_micros(60),
                apply_batch_wait_duration: Duration::from_micros(70),
                apply_log_duration: Duration::from_micros(80),
                apply_mutex_lock_duration: Duration::from_micros(90),
                apply_write_leader_wait_duration: Duration::from_micros(100),
                apply_write_wal_duration: Duration::from_micros(101),
                apply_write_memtable_duration: Duration::from_micros(102),
                scheduler_process_duration: Duration::ZERO,
            }),
        }
    }

    /// Port of Go `TestString` (`execdetails_test.go`), including the
    /// LockKeysDetails slowest-request fragment, with the byte-exact
    /// expected literal, plus the empty-details `""` case.
    #[test]
    fn exec_details_string_matches_go_test_string() {
        let detail = ExecDetails {
            cop_time: Duration::from_secs(1) + Duration::from_millis(3),
            request_count: 1,
            lock_keys_detail: Some(LockKeysDetails {
                total_time: Duration::from_secs(1),
                region_num: 2,
                lock_keys: 10,
                backoff_time: 3_000_000_000,
                backoff_types: vec![
                    "backoff4".to_owned(),
                    "backoff5".to_owned(),
                    "backoff5".to_owned(),
                ],
                slowest_req_total_time: Duration::from_secs(1),
                slowest_region: 1000,
                slowest_store_addr: "tikv-1:20160".to_owned(),
                slowest_exec_details: fixture_tikv_exec_details(),
                lock_rpc_time: 5_000_000_000,
                lock_rpc_count: 50,
                retry_count: 2,
                resolve_lock: ResolveLockDetail {
                    resolve_lock_time: 2_000_000_000,
                },
            }),
            commit_detail: Some(CommitDetails {
                get_commit_ts_time: Duration::from_secs(1),
                get_latest_ts_time: Duration::from_secs(1),
                prewrite_time: Duration::from_secs(1),
                commit_time: Duration::from_secs(1),
                local_latch_time: Duration::from_secs(1),
                commit_backoff_time: 1_000_000_000,
                prewrite_backoff_types: vec!["backoff1".to_owned(), "backoff2".to_owned()],
                commit_backoff_types: vec!["commit1".to_owned(), "commit2".to_owned()],
                slowest_prewrite: ReqDetailInfo {
                    req_total_time: Duration::from_secs(1),
                    region: 1000,
                    store_addr: "tikv-1:20160".to_owned(),
                    exec_details: fixture_tikv_exec_details(),
                },
                commit_primary: ReqDetailInfo {
                    req_total_time: Duration::from_secs(2),
                    region: 2000,
                    store_addr: "tikv-2:20160".to_owned(),
                    exec_details: TiKVExecDetails {
                        time_detail: Some(TimeDetail {
                            total_rpc_wall_time: Duration::from_millis(1000),
                            ..TimeDetail::default()
                        }),
                        scan_detail: Some(ScanDetail {
                            processed_keys: 20,
                            total_keys: 200,
                            rocksdb_delete_skipped_count: 2,
                            rocksdb_key_skipped_count: 2,
                            rocksdb_block_cache_hit_count: 2,
                            rocksdb_block_read_count: 2,
                            rocksdb_block_read_byte: 200,
                            rocksdb_block_read_duration: Duration::from_millis(40),
                            ..ScanDetail::default()
                        }),
                        write_detail: Some(WriteDetail {
                            store_batch_wait_duration: Duration::from_micros(110),
                            propose_send_wait_duration: Duration::from_micros(120),
                            persist_log_duration: Duration::from_micros(130),
                            raft_db_write_leader_wait_duration: Duration::from_micros(140),
                            raft_db_sync_log_duration: Duration::from_micros(145),
                            raft_db_write_memtable_duration: Duration::from_micros(150),
                            commit_log_duration: Duration::from_micros(160),
                            apply_batch_wait_duration: Duration::from_micros(170),
                            apply_log_duration: Duration::from_micros(180),
                            apply_mutex_lock_duration: Duration::from_micros(190),
                            apply_write_leader_wait_duration: Duration::from_micros(200),
                            apply_write_wal_duration: Duration::from_micros(201),
                            apply_write_memtable_duration: Duration::from_micros(202),
                            scheduler_process_duration: Duration::ZERO,
                        }),
                    },
                },
                write_keys: 1,
                write_size: 1,
                prewrite_region_num: 1,
                txn_retry: 1,
                resolve_lock: ResolveLockDetail {
                    // 10^9 ns = 1s, as the Go fixture spells it.
                    resolve_lock_time: 1_000_000_000,
                },
                ..CommitDetails::default()
            }),
            cop_exec_details: CopExecDetails {
                backoff_time: Duration::from_secs(1),
                scan_detail: Some(ScanDetail {
                    processed_keys: 10,
                    total_keys: 100,
                    rocksdb_delete_skipped_count: 1,
                    rocksdb_key_skipped_count: 1,
                    rocksdb_block_cache_hit_count: 1,
                    rocksdb_block_read_count: 1,
                    rocksdb_block_read_byte: 100,
                    rocksdb_block_read_duration: Duration::from_millis(1),
                    ..ScanDetail::default()
                }),
                time_detail: TimeDetail {
                    process_time: Duration::from_secs(2) + Duration::from_millis(5),
                    wait_time: Duration::from_secs(1),
                    ..TimeDetail::default()
                },
                ..CopExecDetails::default()
            },
            ..ExecDetails::default()
        };
        let expected = concat!(
            "Cop_time: 1.003 Process_time: 2.005 Wait_time: 1 Backoff_time: 1 ",
            "LockKeys_time: 1 Request_count: 1 Prewrite_time: 1 Commit_time: ",
            "1 Get_commit_ts_time: 1 Get_latest_ts_time: 1 Commit_backoff_time: 1 ",
            "Prewrite_Backoff_types: [backoff1 backoff2] Commit_Backoff_types: [commit1 commit2] ",
            "Slowest_prewrite_rpc_detail: {total:1.000s, region_id: 1000, ",
            "store: tikv-1:20160, time_detail: {tikv_wall_time: 500ms}, scan_detail: ",
            "{total_process_keys: 10, total_keys: 100, ",
            "rocksdb: {delete_skipped_count: 1, key_skipped_count: 1, block: ",
            "{cache_hit_count: 1, read_count: 1, ",
            "read_byte: 100 Bytes, read_time: 20ms}}}, write_detail: ",
            "{store_batch_wait: 10µs, propose_send_wait: 20µs, ",
            "persist_log: {total: 30µs, write_leader_wait: 40µs, sync_log: 45µs, ",
            "write_memtable: 50µs}, ",
            "commit_log: 60µs, apply_batch_wait: 70µs, apply: {total:80µs, mutex_lock: 90µs, ",
            "write_leader_wait: 100µs, ",
            "write_wal: 101µs, write_memtable: 102µs}, scheduler: {process: 0s}}} ",
            "Commit_primary_rpc_detail: {total:2.000s, region_id: 2000, ",
            "store: tikv-2:20160, time_detail: {tikv_wall_time: 1s}, scan_detail: ",
            "{total_process_keys: 20, total_keys: 200, ",
            "rocksdb: {delete_skipped_count: 2, key_skipped_count: 2, block: ",
            "{cache_hit_count: 2, read_count: 2, ",
            "read_byte: 200 Bytes, read_time: 40ms}}}, write_detail: ",
            "{store_batch_wait: 110µs, propose_send_wait: 120µs, ",
            "persist_log: {total: 130µs, write_leader_wait: 140µs, sync_log: 145µs, ",
            "write_memtable: 150µs}, ",
            "commit_log: 160µs, apply_batch_wait: 170µs, apply: {total:180µs, mutex_lock: 190µs, ",
            "write_leader_wait: 200µs, ",
            "write_wal: 201µs, write_memtable: 202µs}, scheduler: {process: 0s}}} ",
            "Resolve_lock_time: 1 Local_latch_wait_time: 1 Write_keys: 1 Write_size: ",
            "1 Prewrite_region: 1 Txn_retry: 1 Process_keys: 10 Total_keys: 100 ",
            "Rocksdb_delete_skipped_count: 1 Rocksdb_key_skipped_count: ",
            "1 Rocksdb_block_cache_hit_count: 1 Rocksdb_block_read_count: 1 ",
            "Rocksdb_block_read_byte: 100 Rocksdb_block_read_time: 0.001",
        );
        assert_eq!(expected, detail.to_string());
        assert_eq!("", ExecDetails::default().to_string());
    }

    /// Pins `format_seconds` against Go's
    /// `strconv.FormatFloat(seconds, 'f', -1, 64)` at the fixture values.
    #[test]
    fn format_seconds_matches_go_format_float() {
        let cases = [
            (Duration::from_millis(1003), "1.003"),
            (Duration::from_millis(2005), "2.005"),
            (Duration::from_secs(1), "1"),
            (Duration::from_millis(500), "0.5"),
            (Duration::from_secs(3), "3"),
        ];
        for (duration, expected) in cases {
            assert_eq!(expected, format_seconds(duration));
        }
    }

    /// Pins `format_go_duration` at every point the Go `TestString` fixture
    /// exercises client-go's duration rendering.
    #[test]
    fn format_go_duration_fixture_points() {
        let cases = [
            (Duration::ZERO, "0s"),
            (Duration::from_micros(10), "10µs"),
            (Duration::from_micros(45), "45µs"),
            (Duration::from_micros(101), "101µs"),
            (Duration::from_millis(20), "20ms"),
            (Duration::from_millis(40), "40ms"),
            (Duration::from_millis(500), "500ms"),
            (Duration::from_secs(1), "1s"),
            (Duration::from_secs(2), "2s"),
        ];
        for (duration, expected) in cases {
            assert_eq!(expected, format_go_duration(duration));
        }
    }

    /// Go `GetIARemoteReadSegmentStats`: nil detail yields zeros; a present
    /// detail is read field for field.
    #[test]
    fn get_ia_remote_read_segment_stats_reads_scan_detail() {
        assert_eq!(
            IaRemoteReadSegmentStats::default(),
            get_ia_remote_read_segment_stats(None)
        );
        let scan = ScanDetail {
            ia_remote_read_segment_count: 3,
            ia_remote_read_segment_bytes: 4096,
            ia_remote_read_segment_duration: Duration::from_millis(7),
            ..ScanDetail::default()
        };
        assert_eq!(
            IaRemoteReadSegmentStats {
                count: 3,
                bytes: 4096,
                wait_time: Duration::from_millis(7),
            },
            get_ia_remote_read_segment_stats(Some(&scan))
        );
    }
}
