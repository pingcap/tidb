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

//! Go `pkg/ttl/ttlworker/config.go`.
//!
//! Each getter's `failpoint.Inject("<name>", ...)` override is an atomic a
//! test sets through [`set_failpoint`] (zero clears it), as the timer
//! runtime's tunables stand for its package variables.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::Store;

/// Go `jobManagerLoopTickerInterval`.
pub const JOB_MANAGER_LOOP_TICKER_INTERVAL: Duration = Duration::from_secs(10);
/// Go `updateInfoSchemaCacheInterval`.
pub const UPDATE_INFO_SCHEMA_CACHE_INTERVAL: Duration = Duration::from_secs(2 * 60);
/// Go `updateTTLTableStatusCacheInterval`.
pub const UPDATE_TTL_TABLE_STATUS_CACHE_INTERVAL: Duration = Duration::from_secs(2 * 60);
/// Go `ttlInternalSQLTimeout`.
pub const TTL_INTERNAL_SQL_TIMEOUT: Duration = Duration::from_secs(30);
/// Go `resizeWorkersInterval`.
pub const RESIZE_WORKERS_INTERVAL: Duration = Duration::from_secs(30);
/// Go `splitScanCount`.
pub const SPLIT_SCAN_COUNT: usize = 64;
/// Go `ttlJobTimeout`.
pub const TTL_JOB_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);
/// Go `taskManagerLoopTickerInterval`.
pub const TASK_MANAGER_LOOP_TICKER_INTERVAL: Duration = Duration::from_secs(60);
/// Go `ttlTaskHeartBeatTickerInterval`.
pub const TTL_TASK_HEART_BEAT_TICKER_INTERVAL: Duration = Duration::from_secs(60);
/// Go `ttlGCInterval`.
pub const TTL_GC_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// The failpoints `config.go` declares, each overriding one interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failpoint {
    /// `check-job-interval`.
    CheckJobInterval,
    /// `heartbeat-interval`.
    HeartbeatInterval,
    /// `sync-timer`.
    SyncTimer,
    /// `update-info-schema-cache-interval`.
    UpdateInfoSchemaCacheInterval,
    /// `update-status-table-cache-interval`.
    UpdateStatusTableCacheInterval,
    /// `resize-workers-interval`.
    ResizeWorkersInterval,
    /// `check-task-interval`.
    CheckTaskInterval,
    /// `task-manager-loop-interval`.
    TaskManagerLoopInterval,
    /// `task-manager-heartbeat-interval`.
    TaskManagerHeartbeatInterval,
    /// `check-job-triggered-interval`.
    CheckJobTriggeredInterval,
    /// `gc-interval`.
    GcInterval,
}

const FAILPOINT_COUNT: usize = 11;

static FAILPOINTS: [AtomicU64; FAILPOINT_COUNT] = [const { AtomicU64::new(0) }; FAILPOINT_COUNT];

/// Go `failpoint.Enable("github.com/pingcap/tidb/pkg/ttl/ttlworker/<name>",
/// "return(<nanoseconds>)")`; a zero duration is `failpoint.Disable`.
pub fn set_failpoint(failpoint: Failpoint, value: Duration) {
    FAILPOINTS[failpoint as usize].store(value.as_nanos() as u64, Ordering::SeqCst);
}

fn interval(failpoint: Failpoint, default: Duration) -> Duration {
    match FAILPOINTS[failpoint as usize].load(Ordering::SeqCst) {
        0 => default,
        nanoseconds => Duration::from_nanos(nanoseconds),
    }
}

/// Go `getCheckJobInterval`.
pub fn get_check_job_interval() -> Duration {
    interval(Failpoint::CheckJobInterval, JOB_MANAGER_LOOP_TICKER_INTERVAL)
}

/// Go `getHeartbeatInterval`.
pub fn get_heartbeat_interval() -> Duration {
    interval(Failpoint::HeartbeatInterval, JOB_MANAGER_LOOP_TICKER_INTERVAL)
}

/// Go `getJobManagerLoopSyncTimerInterval`.
pub fn get_job_manager_loop_sync_timer_interval() -> Duration {
    interval(Failpoint::SyncTimer, Duration::from_secs(1))
}

/// Go `getUpdateInfoSchemaCacheInterval`.
pub fn get_update_info_schema_cache_interval() -> Duration {
    interval(Failpoint::UpdateInfoSchemaCacheInterval, UPDATE_INFO_SCHEMA_CACHE_INTERVAL)
}

/// Go `getUpdateTTLTableStatusCacheInterval`.
pub fn get_update_ttl_table_status_cache_interval() -> Duration {
    interval(Failpoint::UpdateStatusTableCacheInterval, UPDATE_TTL_TABLE_STATUS_CACHE_INTERVAL)
}

/// Go `getResizeWorkersInterval`.
pub fn get_resize_workers_interval() -> Duration {
    interval(Failpoint::ResizeWorkersInterval, RESIZE_WORKERS_INTERVAL)
}

/// Go `getTaskManagerLoopCheckTaskInterval`.
pub fn get_task_manager_loop_check_task_interval() -> Duration {
    interval(Failpoint::CheckTaskInterval, Duration::from_secs(5))
}

/// Go `getTaskManagerLoopTickerInterval`.
pub fn get_task_manager_loop_ticker_interval() -> Duration {
    interval(Failpoint::TaskManagerLoopInterval, TASK_MANAGER_LOOP_TICKER_INTERVAL)
}

/// Go `getTaskManagerHeartBeatInterval`.
pub fn get_task_manager_heart_beat_interval() -> Duration {
    interval(Failpoint::TaskManagerHeartbeatInterval, TTL_TASK_HEART_BEAT_TICKER_INTERVAL)
}

/// Go `getCheckJobTriggeredInterval`.
pub fn get_check_job_triggered_interval() -> Duration {
    interval(Failpoint::CheckJobTriggeredInterval, Duration::from_secs(2))
}

/// Go `getTTLGCInterval`.
pub fn get_ttl_gc_interval() -> Duration {
    interval(Failpoint::GcInterval, TTL_GC_INTERVAL)
}

/// Go `getScanSplitCnt`: at least `splitScanCount`, or one range per TiKV
/// store when there are more.
pub fn get_scan_split_cnt(store: &Store) -> usize {
    match store.as_ref().and_then(|store| store.tikv_store_count()) {
        Some(tikv_cnt) if tikv_cnt > SPLIT_SCAN_COUNT => tikv_cnt,
        _ => SPLIT_SCAN_COUNT,
    }
}
