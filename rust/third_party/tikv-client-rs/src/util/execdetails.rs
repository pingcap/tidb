// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::proto::kvrpcpb;
use crate::trace::TraceContext;

use super::{format_bytes, format_duration};

struct CommitDetailsContextKey;
struct LockKeysDetailsContextKey;
struct ExecDetailsContextKey;
struct RuDetailsContextKey;

pub type SharedCommitDetails = Arc<Mutex<CommitDetails>>;
pub type SharedLockKeysDetails = Arc<Mutex<LockKeysDetails>>;

pub fn context_with_commit_details(
    context: &TraceContext,
    details: SharedCommitDetails,
) -> TraceContext {
    context.with_value::<CommitDetailsContextKey, _>(details)
}

pub fn commit_details_from_context(context: &TraceContext) -> Option<&SharedCommitDetails> {
    context.value::<CommitDetailsContextKey, SharedCommitDetails>()
}

pub fn context_with_lock_keys_details(
    context: &TraceContext,
    details: SharedLockKeysDetails,
) -> TraceContext {
    context.with_value::<LockKeysDetailsContextKey, _>(details)
}

pub fn lock_keys_details_from_context(context: &TraceContext) -> Option<&SharedLockKeysDetails> {
    context.value::<LockKeysDetailsContextKey, SharedLockKeysDetails>()
}

pub fn context_with_exec_details(
    context: &TraceContext,
    details: Arc<ExecDetails>,
) -> TraceContext {
    context.with_value::<ExecDetailsContextKey, _>(details)
}

pub fn exec_details_from_context(context: &TraceContext) -> Option<&Arc<ExecDetails>> {
    context.value::<ExecDetailsContextKey, Arc<ExecDetails>>()
}

pub fn context_with_ru_details(
    context: &TraceContext,
    details: Arc<super::RuDetails>,
) -> TraceContext {
    context.with_value::<RuDetailsContextKey, _>(details)
}

pub fn ru_details_from_context(context: &TraceContext) -> Option<&Arc<super::RuDetails>> {
    context.value::<RuDetailsContextKey, Arc<super::RuDetails>>()
}

/// Per-operation execution and traffic counters shared by physical RPCs.
#[derive(Default)]
pub struct ExecDetails {
    backoff_count: AtomicI64,
    backoff_duration_ns: AtomicI64,
    wait_kv_response_duration_ns: AtomicI64,
    wait_pd_response_duration_ns: AtomicI64,
    pub traffic: TrafficDetails,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecDetailsSnapshot {
    pub backoff_count: i64,
    pub backoff_duration_ns: i64,
    pub wait_kv_response_duration_ns: i64,
    pub wait_pd_response_duration_ns: i64,
    pub traffic: TrafficDetailsSnapshot,
}

impl ExecDetails {
    pub fn snapshot(&self) -> ExecDetailsSnapshot {
        ExecDetailsSnapshot {
            backoff_count: self.backoff_count.load(Ordering::Relaxed),
            backoff_duration_ns: self.backoff_duration_ns.load(Ordering::Relaxed),
            wait_kv_response_duration_ns: self.wait_kv_response_duration_ns.load(Ordering::Relaxed),
            wait_pd_response_duration_ns: self.wait_pd_response_duration_ns.load(Ordering::Relaxed),
            traffic: self.traffic.snapshot(),
        }
    }

    pub fn add_backoff(&self, duration: Duration) {
        self.backoff_count.fetch_add(1, Ordering::Relaxed);
        self.backoff_duration_ns.fetch_add(
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn add_wait_kv_response(&self, duration: Duration) {
        self.wait_kv_response_duration_ns.fetch_add(
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn add_wait_pd_response(&self, duration: Duration) {
        self.wait_pd_response_duration_ns.fetch_add(
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX),
            Ordering::Relaxed,
        );
    }
}

tokio::task_local! {
    static CURRENT_EXEC_DETAILS: Arc<ExecDetails>;
}

pub async fn with_exec_details<F>(details: Arc<ExecDetails>, future: F) -> F::Output
where
    F: Future,
{
    CURRENT_EXEC_DETAILS.scope(details, future).await
}

pub(crate) fn current_exec_details() -> Option<Arc<ExecDetails>> {
    CURRENT_EXEC_DETAILS.try_with(Arc::clone).ok()
}

#[derive(Default)]
pub struct TrafficDetails {
    sent_kv_total: AtomicI64,
    received_kv_total: AtomicI64,
    sent_kv_cross_zone: AtomicI64,
    received_kv_cross_zone: AtomicI64,
    sent_mpp_total: AtomicI64,
    received_mpp_total: AtomicI64,
    sent_mpp_cross_zone: AtomicI64,
    received_mpp_cross_zone: AtomicI64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TrafficDetailsSnapshot {
    pub sent_kv_total: i64,
    pub received_kv_total: i64,
    pub sent_kv_cross_zone: i64,
    pub received_kv_cross_zone: i64,
    pub sent_mpp_total: i64,
    pub received_mpp_total: i64,
    pub sent_mpp_cross_zone: i64,
    pub received_mpp_cross_zone: i64,
}

impl TrafficDetails {
    pub fn snapshot(&self) -> TrafficDetailsSnapshot {
        TrafficDetailsSnapshot {
            sent_kv_total: self.sent_kv_total.load(Ordering::Relaxed),
            received_kv_total: self.received_kv_total.load(Ordering::Relaxed),
            sent_kv_cross_zone: self.sent_kv_cross_zone.load(Ordering::Relaxed),
            received_kv_cross_zone: self.received_kv_cross_zone.load(Ordering::Relaxed),
            sent_mpp_total: self.sent_mpp_total.load(Ordering::Relaxed),
            received_mpp_total: self.received_mpp_total.load(Ordering::Relaxed),
            sent_mpp_cross_zone: self.sent_mpp_cross_zone.load(Ordering::Relaxed),
            received_mpp_cross_zone: self.received_mpp_cross_zone.load(Ordering::Relaxed),
        }
    }

    pub fn add_request(&self, bytes: i64, mpp: bool, cross_zone: bool) {
        let (total, cross) = if mpp {
            (&self.sent_mpp_total, &self.sent_mpp_cross_zone)
        } else {
            (&self.sent_kv_total, &self.sent_kv_cross_zone)
        };
        total.fetch_add(bytes, Ordering::Relaxed);
        if cross_zone {
            cross.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn add_response(&self, bytes: i64, mpp: bool, cross_zone: bool) {
        let (total, cross) = if mpp {
            (&self.received_mpp_total, &self.received_mpp_cross_zone)
        } else {
            (&self.received_kv_total, &self.received_kv_cross_zone)
        };
        total.fetch_add(bytes, Ordering::Relaxed);
        if cross_zone {
            cross.fetch_add(bytes, Ordering::Relaxed);
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TiKvExecDetails {
    pub time_detail: Option<TimeDetail>,
    pub scan_detail: Option<ScanDetail>,
    pub write_detail: Option<WriteDetail>,
}

impl TiKvExecDetails {
    pub fn new(details: Option<&kvrpcpb::ExecDetailsV2>) -> Self {
        let Some(details) = details else {
            return Self::default();
        };
        let mut time = TimeDetail::default();
        time.merge_from_pb(
            details.time_detail_v2.as_ref(),
            details.time_detail.as_ref(),
        );
        let mut scan = ScanDetail::default();
        scan.merge_from_pb(details.scan_detail_v2.as_ref());
        let mut write = WriteDetail::default();
        write.merge_from_pb(details.write_detail.as_ref());
        Self {
            time_detail: Some(time),
            scan_detail: Some(scan),
            write_detail: Some(write),
        }
    }
}

impl fmt::Display for TiKvExecDetails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts = [
            self.time_detail.as_ref().map(ToString::to_string),
            self.scan_detail.as_ref().map(ToString::to_string),
            self.write_detail.as_ref().map(ToString::to_string),
        ];
        let mut first = true;
        for part in parts.into_iter().flatten().filter(|part| !part.is_empty()) {
            if !first {
                formatter.write_str(", ")?;
            }
            first = false;
            formatter.write_str(&part)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReqDetailInfo {
    pub request_total_time: Duration,
    pub region: u64,
    pub store_address: String,
    pub exec_details: TiKvExecDetails,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CommitTsLagDetails {
    pub wait_time: Duration,
    pub backoff_count: i32,
    pub first_lag_ts: u64,
    pub wait_until_ts: u64,
}

impl CommitTsLagDetails {
    pub fn merge(&mut self, other: &Self) {
        if other.first_lag_ts == 0 {
            return;
        }
        self.wait_time += other.wait_time;
        self.backoff_count += other.backoff_count;
        self.first_lag_ts = other.first_lag_ts;
        self.wait_until_ts = other.wait_until_ts;
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CommitDetailsInner {
    pub commit_backoff_time_ns: i64,
    pub prewrite_backoff_types: Vec<String>,
    pub commit_backoff_types: Vec<String>,
    pub slowest_prewrite: ReqDetailInfo,
    pub commit_primary: ReqDetailInfo,
}

#[derive(Debug, Default, PartialEq)]
pub struct CommitDetails {
    pub get_commit_ts_time: Duration,
    pub get_latest_ts_time: Duration,
    pub lag_details: CommitTsLagDetails,
    pub prewrite_time: Duration,
    pub wait_prewrite_binlog_time: Duration,
    pub commit_time: Duration,
    pub local_latch_time: Duration,
    pub detail: CommitDetailsInner,
    pub write_keys: usize,
    pub write_size: usize,
    pub prewrite_region_num: i32,
    pub transaction_retry: usize,
    pub resolve_lock: ResolveLockDetail,
    pub prewrite_request_num: usize,
}

impl Clone for CommitDetails {
    fn clone(&self) -> Self {
        Self {
            get_commit_ts_time: self.get_commit_ts_time,
            get_latest_ts_time: self.get_latest_ts_time,
            lag_details: self.lag_details,
            prewrite_time: self.prewrite_time,
            wait_prewrite_binlog_time: self.wait_prewrite_binlog_time,
            commit_time: self.commit_time,
            local_latch_time: self.local_latch_time,
            detail: self.detail.clone(),
            write_keys: self.write_keys,
            write_size: self.write_size,
            prewrite_region_num: self.prewrite_region_num,
            transaction_retry: self.transaction_retry,
            resolve_lock: self.resolve_lock,
            prewrite_request_num: 0,
        }
    }
}

impl CommitDetails {
    pub fn merge(&mut self, other: &Self) {
        self.get_commit_ts_time += other.get_commit_ts_time;
        self.get_latest_ts_time += other.get_latest_ts_time;
        self.prewrite_time += other.prewrite_time;
        self.lag_details.merge(&other.lag_details);
        self.wait_prewrite_binlog_time += other.wait_prewrite_binlog_time;
        self.commit_time += other.commit_time;
        self.local_latch_time += other.local_latch_time;
        self.resolve_lock.merge(&other.resolve_lock);
        self.write_keys += other.write_keys;
        self.write_size += other.write_size;
        self.prewrite_region_num += other.prewrite_region_num;
        self.transaction_retry += other.transaction_retry;
        self.detail.commit_backoff_time_ns += other.detail.commit_backoff_time_ns;
        self.detail
            .prewrite_backoff_types
            .extend(other.detail.prewrite_backoff_types.iter().cloned());
        if self.detail.slowest_prewrite.request_total_time
            < other.detail.slowest_prewrite.request_total_time
        {
            self.detail.slowest_prewrite = other.detail.slowest_prewrite.clone();
        }
        self.detail
            .commit_backoff_types
            .extend(other.detail.commit_backoff_types.iter().cloned());
        if self.detail.commit_primary.request_total_time
            < other.detail.commit_primary.request_total_time
        {
            self.detail.commit_primary = other.detail.commit_primary.clone();
        }
    }

    pub fn merge_prewrite_request_details(
        &mut self,
        duration: Duration,
        region: u64,
        address: impl Into<String>,
        details: Option<&kvrpcpb::ExecDetailsV2>,
    ) {
        if duration > self.detail.slowest_prewrite.request_total_time {
            self.detail.slowest_prewrite = ReqDetailInfo {
                request_total_time: duration,
                region,
                store_address: address.into(),
                exec_details: TiKvExecDetails::new(details),
            };
        }
    }

    pub fn merge_commit_request_details(
        &mut self,
        duration: Duration,
        region: u64,
        address: impl Into<String>,
        details: Option<&kvrpcpb::ExecDetailsV2>,
    ) {
        if duration > self.detail.commit_primary.request_total_time {
            self.detail.commit_primary = ReqDetailInfo {
                request_total_time: duration,
                region,
                store_address: address.into(),
                exec_details: TiKvExecDetails::new(details),
            };
        }
    }

    pub fn merge_flush_request_details(
        &mut self,
        _duration: Duration,
        _region: u64,
        _address: &str,
        _details: Option<&kvrpcpb::ExecDetailsV2>,
    ) {
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LockKeysDetailsInner {
    pub backoff_types: Vec<String>,
    pub slowest_request_total_time: Duration,
    pub slowest_region: u64,
    pub slowest_store_address: String,
    pub slowest_exec_details: TiKvExecDetails,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LockKeysDetails {
    pub total_time: Duration,
    pub region_num: i32,
    pub lock_keys: i32,
    pub aggressive_lock_new_count: usize,
    pub aggressive_lock_derived_count: usize,
    pub locked_with_conflict_count: usize,
    pub resolve_lock: ResolveLockDetail,
    pub backoff_time_ns: i64,
    pub detail: LockKeysDetailsInner,
    pub lock_rpc_time_ns: i64,
    pub lock_rpc_count: i64,
    pub retry_count: usize,
}

impl LockKeysDetails {
    pub fn merge(&mut self, other: &Self) {
        self.total_time += other.total_time;
        self.region_num += other.region_num;
        self.lock_keys += other.lock_keys;
        self.aggressive_lock_new_count += other.aggressive_lock_new_count;
        self.aggressive_lock_derived_count += other.aggressive_lock_derived_count;
        self.locked_with_conflict_count += other.locked_with_conflict_count;
        self.resolve_lock.merge(&other.resolve_lock);
        self.backoff_time_ns += other.backoff_time_ns;
        self.lock_rpc_time_ns += other.lock_rpc_time_ns;
        self.lock_rpc_count += other.lock_rpc_count;
        self.detail
            .backoff_types
            .extend(other.detail.backoff_types.iter().cloned());
        self.retry_count += 1;
        if self.detail.slowest_request_total_time < other.detail.slowest_request_total_time {
            self.detail.slowest_request_total_time = other.detail.slowest_request_total_time;
            self.detail.slowest_region = other.detail.slowest_region;
            self.detail.slowest_store_address = other.detail.slowest_store_address.clone();
            self.detail.slowest_exec_details = other.detail.slowest_exec_details.clone();
        }
    }

    pub fn merge_request_details(
        &mut self,
        duration: Duration,
        region: u64,
        address: impl Into<String>,
        details: Option<&kvrpcpb::ExecDetailsV2>,
    ) {
        if duration > self.detail.slowest_request_total_time {
            self.detail.slowest_request_total_time = duration;
            self.detail.slowest_region = region;
            self.detail.slowest_store_address = address.into();
            self.detail.slowest_exec_details = TiKvExecDetails::new(details);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResolveLockDetail {
    pub resolve_lock_time_ns: i64,
}

impl ResolveLockDetail {
    pub fn merge(&mut self, other: &Self) {
        self.resolve_lock_time_ns += other.resolve_lock_time_ns;
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolTaskDetails {
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

impl PoolTaskDetails {
    pub fn merge_from_pb(&mut self, details: Option<&kvrpcpb::PoolTaskDetails>) {
        let Some(details) = details else {
            return;
        };
        let had_poll_samples = self.poll_count > 0;
        let had_queue_samples = !self.total_queue_wait_time.is_zero();
        let had_wake_samples = !self.total_wake_wait_time.is_zero();
        let had_fair_samples = self.fair_queue_sample_count > 0;
        let had_wall_samples = self.task_wall_time_sample_count > 0;
        let had_tasks = self.task_count > 0;

        self.task_count += 1;
        self.poll_count += details.poll_count;
        self.max_poll_count = self.max_poll_count.max(details.poll_count);
        self.min_poll_count = minimum(self.min_poll_count, details.poll_count, had_tasks);
        self.dispatch_count += details.dispatch_count;
        self.max_dispatch_count = self.max_dispatch_count.max(details.dispatch_count);
        self.min_dispatch_count =
            minimum(self.min_dispatch_count, details.dispatch_count, had_tasks);
        let task_wall = Duration::from_nanos(details.total_wall_nanos);
        self.total_wall_time += task_wall;
        if !task_wall.is_zero() {
            self.task_wall_time_sample_count += 1;
            self.max_task_wall_time = self.max_task_wall_time.max(task_wall);
            self.min_task_wall_time = minimum(self.min_task_wall_time, task_wall, had_wall_samples);
        }

        self.total_queue_wait_time += Duration::from_nanos(details.total_queue_wait_nanos);
        self.max_queue_wait_time = self
            .max_queue_wait_time
            .max(Duration::from_nanos(details.max_queue_wait_nanos));
        if details.total_queue_wait_nanos > 0 {
            self.min_queue_wait_time = minimum(
                self.min_queue_wait_time,
                Duration::from_nanos(details.min_queue_wait_nanos),
                had_queue_samples,
            );
        }

        self.total_wake_wait_time += Duration::from_nanos(details.total_wake_wait_nanos);
        self.max_wake_wait_time = self
            .max_wake_wait_time
            .max(Duration::from_nanos(details.max_wake_wait_nanos));
        if details.total_wake_wait_nanos > 0 {
            self.min_wake_wait_time = minimum(
                self.min_wake_wait_time,
                Duration::from_nanos(details.min_wake_wait_nanos),
                had_wake_samples,
            );
        }

        if details.fair_queue_enabled {
            self.fair_queue_sample_count += details.dispatch_count;
            self.total_fair_queue_waited_task_slices += details.total_fair_queue_waited_task_slices;
            self.max_fair_queue_waited_task_slices = self
                .max_fair_queue_waited_task_slices
                .max(details.max_fair_queue_waited_task_slices);
            self.min_fair_queue_waited_task_slices = minimum(
                self.min_fair_queue_waited_task_slices,
                details.min_fair_queue_waited_task_slices,
                had_fair_samples,
            );
        }

        self.poll_cpu_time += Duration::from_nanos(details.poll_cpu_nanos);
        self.max_poll_cpu_time = self
            .max_poll_cpu_time
            .max(Duration::from_nanos(details.max_poll_cpu_nanos));
        self.poll_wall_time += Duration::from_nanos(details.poll_wall_nanos);
        self.max_poll_wall_time = self
            .max_poll_wall_time
            .max(Duration::from_nanos(details.max_poll_wall_nanos));
        if details.poll_count > 0 {
            self.min_poll_cpu_time = minimum(
                self.min_poll_cpu_time,
                Duration::from_nanos(details.min_poll_cpu_nanos),
                had_poll_samples,
            );
            self.min_poll_wall_time = minimum(
                self.min_poll_wall_time,
                Duration::from_nanos(details.min_poll_wall_nanos),
                had_poll_samples,
            );
        }
    }

    pub fn merge(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        let had_poll_samples = self.poll_count > 0;
        let had_queue_samples = !self.total_queue_wait_time.is_zero();
        let had_wake_samples = !self.total_wake_wait_time.is_zero();
        let had_fair_samples = self.fair_queue_sample_count > 0;
        let had_wall_samples = self.task_wall_time_sample_count > 0;
        let had_tasks = self.task_count > 0;

        self.task_count += other.task_count;
        self.poll_count += other.poll_count;
        self.max_poll_count = self.max_poll_count.max(other.max_poll_count);
        self.min_poll_count = minimum(self.min_poll_count, other.min_poll_count, had_tasks);
        self.dispatch_count += other.dispatch_count;
        self.max_dispatch_count = self.max_dispatch_count.max(other.max_dispatch_count);
        self.min_dispatch_count =
            minimum(self.min_dispatch_count, other.min_dispatch_count, had_tasks);
        self.total_wall_time += other.total_wall_time;
        self.task_wall_time_sample_count += other.task_wall_time_sample_count;
        self.max_task_wall_time = self.max_task_wall_time.max(other.max_task_wall_time);
        if !other.total_wall_time.is_zero() {
            self.min_task_wall_time = minimum(
                self.min_task_wall_time,
                other.min_task_wall_time,
                had_wall_samples,
            );
        }
        self.total_queue_wait_time += other.total_queue_wait_time;
        self.max_queue_wait_time = self.max_queue_wait_time.max(other.max_queue_wait_time);
        if !other.total_queue_wait_time.is_zero() {
            self.min_queue_wait_time = minimum(
                self.min_queue_wait_time,
                other.min_queue_wait_time,
                had_queue_samples,
            );
        }
        self.total_wake_wait_time += other.total_wake_wait_time;
        self.max_wake_wait_time = self.max_wake_wait_time.max(other.max_wake_wait_time);
        if !other.total_wake_wait_time.is_zero() {
            self.min_wake_wait_time = minimum(
                self.min_wake_wait_time,
                other.min_wake_wait_time,
                had_wake_samples,
            );
        }
        self.fair_queue_sample_count += other.fair_queue_sample_count;
        self.total_fair_queue_waited_task_slices += other.total_fair_queue_waited_task_slices;
        self.max_fair_queue_waited_task_slices = self
            .max_fair_queue_waited_task_slices
            .max(other.max_fair_queue_waited_task_slices);
        if other.fair_queue_sample_count > 0 {
            self.min_fair_queue_waited_task_slices = minimum(
                self.min_fair_queue_waited_task_slices,
                other.min_fair_queue_waited_task_slices,
                had_fair_samples,
            );
        }
        self.poll_cpu_time += other.poll_cpu_time;
        self.max_poll_cpu_time = self.max_poll_cpu_time.max(other.max_poll_cpu_time);
        self.poll_wall_time += other.poll_wall_time;
        self.max_poll_wall_time = self.max_poll_wall_time.max(other.max_poll_wall_time);
        if other.poll_count > 0 {
            self.min_poll_cpu_time = minimum(
                self.min_poll_cpu_time,
                other.min_poll_cpu_time,
                had_poll_samples,
            );
            self.min_poll_wall_time = minimum(
                self.min_poll_wall_time,
                other.min_poll_wall_time,
                had_poll_samples,
            );
        }
    }

    pub fn is_empty(&self) -> bool {
        self.task_count == 0
    }
}

impl fmt::Display for PoolTaskDetails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return Ok(());
        }
        write!(formatter, "{{tasks:{}", self.task_count)?;
        write_count_stats(
            formatter,
            "poll_count",
            self.poll_count,
            self.task_count,
            self.max_poll_count,
            self.min_poll_count,
        )?;
        write_count_stats(
            formatter,
            "dispatch_count",
            self.dispatch_count,
            0,
            self.max_dispatch_count,
            self.min_dispatch_count,
        )?;
        write_time_stats(
            formatter,
            "task_wall_time",
            self.total_wall_time,
            self.task_wall_time_sample_count,
            self.max_task_wall_time,
            self.min_task_wall_time,
        )?;
        write_time_stats(
            formatter,
            "queue_wait",
            self.total_queue_wait_time,
            self.dispatch_count,
            self.max_queue_wait_time,
            self.min_queue_wait_time,
        )?;
        write_time_stats(
            formatter,
            "wake_wait",
            self.total_wake_wait_time,
            self.dispatch_count.saturating_sub(self.task_count),
            self.max_wake_wait_time,
            self.min_wake_wait_time,
        )?;
        write!(
            formatter,
            ", fair_queue:{{enabled:{}, waited_task_slices:{{total:{}",
            self.fair_queue_sample_count > 0,
            self.total_fair_queue_waited_task_slices
        )?;
        if self.fair_queue_sample_count > 0 {
            write!(
                formatter,
                ", avg:{}",
                format_average(
                    self.total_fair_queue_waited_task_slices,
                    self.fair_queue_sample_count
                )
            )?;
        }
        write!(
            formatter,
            ", max:{}, min:{}}}}}",
            self.max_fair_queue_waited_task_slices, self.min_fair_queue_waited_task_slices
        )?;
        write_time_stats(
            formatter,
            "poll_cpu",
            self.poll_cpu_time,
            self.poll_count,
            self.max_poll_cpu_time,
            self.min_poll_cpu_time,
        )?;
        write_time_stats(
            formatter,
            "poll_wall",
            self.poll_wall_time,
            self.poll_count,
            self.max_poll_wall_time,
            self.min_poll_wall_time,
        )?;
        formatter.write_str("}")
    }
}

fn minimum<T: Copy + Ord>(current: T, candidate: T, has_current: bool) -> T {
    if !has_current || candidate < current {
        candidate
    } else {
        current
    }
}

fn format_average(total: u64, count: u64) -> String {
    let formatted = format!("{:.2}", total as f64 / count as f64);
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}

fn write_count_stats(
    formatter: &mut fmt::Formatter<'_>,
    name: &str,
    total: u64,
    average_divisor: u64,
    maximum: u64,
    minimum: u64,
) -> fmt::Result {
    write!(formatter, ", {name}:{{total:{total}")?;
    if average_divisor > 0 {
        write!(
            formatter,
            ", avg:{}",
            format_average(total, average_divisor)
        )?;
    }
    write!(formatter, ", max:{maximum}, min:{minimum}}}")
}

fn write_time_stats(
    formatter: &mut fmt::Formatter<'_>,
    name: &str,
    total: Duration,
    sample_count: u64,
    maximum: Duration,
    minimum: Duration,
) -> fmt::Result {
    if total.is_zero() {
        return Ok(());
    }
    write!(formatter, ", {name}:{{total:{}", format_duration(total))?;
    if sample_count > 0 {
        write!(
            formatter,
            ", avg:{}",
            format_duration(total / u32::try_from(sample_count).unwrap_or(u32::MAX))
        )?;
    }
    write!(
        formatter,
        ", max:{}, min:{}}}",
        format_duration(maximum),
        format_duration(minimum)
    )
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScanDetail {
    pub total_keys: i64,
    pub processed_keys: i64,
    pub processed_keys_size: i64,
    pub rocksdb_delete_skipped_count: u64,
    pub rocksdb_key_skipped_count: u64,
    pub rocksdb_block_cache_hit_count: u64,
    pub rocksdb_block_read_count: u64,
    pub rocksdb_block_read_bytes: u64,
    pub rocksdb_block_read_duration: Duration,
    pub get_snapshot_duration: Duration,
    pub ia_cache_hit_count: u64,
    pub ia_remote_read_segment_count: u64,
    pub ia_remote_read_segment_bytes: u64,
    pub ia_remote_read_segment_duration: Duration,
}

impl ScanDetail {
    pub fn merge(&mut self, other: &Self) {
        self.total_keys += other.total_keys;
        self.processed_keys += other.processed_keys;
        self.processed_keys_size += other.processed_keys_size;
        self.rocksdb_delete_skipped_count += other.rocksdb_delete_skipped_count;
        self.rocksdb_key_skipped_count += other.rocksdb_key_skipped_count;
        self.rocksdb_block_cache_hit_count += other.rocksdb_block_cache_hit_count;
        self.rocksdb_block_read_count += other.rocksdb_block_read_count;
        self.rocksdb_block_read_bytes += other.rocksdb_block_read_bytes;
        self.rocksdb_block_read_duration += other.rocksdb_block_read_duration;
        self.get_snapshot_duration += other.get_snapshot_duration;
        self.ia_cache_hit_count += other.ia_cache_hit_count;
        self.ia_remote_read_segment_count += other.ia_remote_read_segment_count;
        self.ia_remote_read_segment_bytes += other.ia_remote_read_segment_bytes;
        self.ia_remote_read_segment_duration += other.ia_remote_read_segment_duration;
    }

    pub fn merge_from_pb(&mut self, detail: Option<&kvrpcpb::ScanDetailV2>) {
        let Some(detail) = detail else {
            return;
        };
        self.total_keys += i64::try_from(detail.total_versions).unwrap_or(i64::MAX);
        self.processed_keys += i64::try_from(detail.processed_versions).unwrap_or(i64::MAX);
        self.processed_keys_size +=
            i64::try_from(detail.processed_versions_size).unwrap_or(i64::MAX);
        self.rocksdb_delete_skipped_count += detail.rocksdb_delete_skipped_count;
        self.rocksdb_key_skipped_count += detail.rocksdb_key_skipped_count;
        self.rocksdb_block_cache_hit_count += detail.rocksdb_block_cache_hit_count;
        self.rocksdb_block_read_count += detail.rocksdb_block_read_count;
        self.rocksdb_block_read_bytes += detail.rocksdb_block_read_byte;
        self.rocksdb_block_read_duration += Duration::from_nanos(detail.rocksdb_block_read_nanos);
        self.get_snapshot_duration += Duration::from_nanos(detail.get_snapshot_nanos);
        self.ia_cache_hit_count += detail.ia_cache_hit_count;
        self.ia_remote_read_segment_count += detail.ia_remote_read_segment_count;
        self.ia_remote_read_segment_bytes += detail.ia_remote_read_segment_bytes;
        self.ia_remote_read_segment_duration +=
            Duration::from_nanos(detail.ia_remote_read_segment_nanos);
    }
}

impl fmt::Display for ScanDetail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::default() {
            return Ok(());
        }
        formatter.write_str("scan_detail: {")?;
        let mut prefix = "";
        macro_rules! field {
            ($condition:expr, $name:expr, $value:expr) => {
                if $condition {
                    write!(formatter, "{}{name}: {}", prefix, $value, name = $name)?;
                    prefix = ", ";
                }
            };
        }
        field!(
            self.processed_keys > 0,
            "total_process_keys",
            self.processed_keys
        );
        field!(
            self.processed_keys_size > 0,
            "total_process_keys_size",
            self.processed_keys_size
        );
        field!(self.total_keys > 0, "total_keys", self.total_keys);
        field!(
            !self.get_snapshot_duration.is_zero(),
            "get_snapshot_time",
            format_duration(self.get_snapshot_duration)
        );
        if self.ia_cache_hit_count > 0
            || self.ia_remote_read_segment_count > 0
            || self.ia_remote_read_segment_bytes > 0
            || !self.ia_remote_read_segment_duration.is_zero()
        {
            write!(formatter, "{prefix}ia: {{")?;
            let mut fields = Vec::new();
            if self.ia_cache_hit_count > 0 {
                fields.push(format!("cache_hit_count: {}", self.ia_cache_hit_count));
            }
            if self.ia_remote_read_segment_count > 0 {
                fields.push(format!(
                    "remote_read_segment_count: {}",
                    self.ia_remote_read_segment_count
                ));
            }
            if self.ia_remote_read_segment_bytes > 0 {
                fields.push(format!(
                    "remote_read_segment_bytes: {}",
                    format_bytes(
                        i64::try_from(self.ia_remote_read_segment_bytes).unwrap_or(i64::MAX)
                    )
                ));
            }
            if !self.ia_remote_read_segment_duration.is_zero() {
                fields.push(format!(
                    "remote_read_segment_wait_time: {}",
                    format_duration(self.ia_remote_read_segment_duration)
                ));
            }
            formatter.write_str(&fields.join(", "))?;
            formatter.write_str("}, ")?;
        } else {
            formatter.write_str(prefix)?;
        }
        formatter.write_str("rocksdb: {")?;
        let mut rocks_prefix = "";
        if self.rocksdb_delete_skipped_count > 0 {
            write!(
                formatter,
                "delete_skipped_count: {}",
                self.rocksdb_delete_skipped_count
            )?;
            rocks_prefix = ", ";
        }
        if self.rocksdb_key_skipped_count > 0 {
            write!(
                formatter,
                "{rocks_prefix}key_skipped_count: {}",
                self.rocksdb_key_skipped_count
            )?;
            rocks_prefix = ", ";
        }
        write!(formatter, "{rocks_prefix}block: {{")?;
        let mut block_prefix = "";
        if self.rocksdb_block_cache_hit_count > 0 {
            write!(
                formatter,
                "cache_hit_count: {}",
                self.rocksdb_block_cache_hit_count
            )?;
            block_prefix = ", ";
        }
        if self.rocksdb_block_read_count > 0 {
            write!(
                formatter,
                "{block_prefix}read_count: {}",
                self.rocksdb_block_read_count
            )?;
            block_prefix = ", ";
        }
        if self.rocksdb_block_read_bytes > 0 {
            write!(
                formatter,
                "{block_prefix}read_byte: {}",
                format_bytes(i64::try_from(self.rocksdb_block_read_bytes).unwrap_or(i64::MAX))
            )?;
            block_prefix = ", ";
        }
        if !self.rocksdb_block_read_duration.is_zero() {
            write!(
                formatter,
                "{block_prefix}read_time: {}",
                format_duration(self.rocksdb_block_read_duration)
            )?;
        }
        formatter.write_str("}}}")
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteDetail {
    pub store_batch_wait_duration: Duration,
    pub propose_send_wait_duration: Duration,
    pub persist_log_duration: Duration,
    pub raft_db_write_leader_wait_duration: Duration,
    pub raft_db_sync_log_duration: Duration,
    pub raft_db_write_memtable_duration: Duration,
    pub commit_log_duration: Duration,
    pub apply_batch_wait_duration: Duration,
    pub apply_log_duration: Duration,
    pub apply_mutex_lock_duration: Duration,
    pub apply_write_leader_wait_duration: Duration,
    pub apply_write_wal_duration: Duration,
    pub apply_write_memtable_duration: Duration,
    pub scheduler_latch_wait_duration: Duration,
    pub scheduler_process_duration: Duration,
    pub scheduler_throttle_duration: Duration,
    pub scheduler_pessimistic_lock_wait_duration: Duration,
}

impl WriteDetail {
    pub fn merge_from_pb(&mut self, detail: Option<&kvrpcpb::WriteDetail>) {
        let Some(detail) = detail else {
            return;
        };
        self.store_batch_wait_duration += Duration::from_nanos(detail.store_batch_wait_nanos);
        self.propose_send_wait_duration += Duration::from_nanos(detail.propose_send_wait_nanos);
        self.persist_log_duration += Duration::from_nanos(detail.persist_log_nanos);
        self.raft_db_write_leader_wait_duration +=
            Duration::from_nanos(detail.raft_db_write_leader_wait_nanos);
        self.raft_db_sync_log_duration += Duration::from_nanos(detail.raft_db_sync_log_nanos);
        self.raft_db_write_memtable_duration +=
            Duration::from_nanos(detail.raft_db_write_memtable_nanos);
        self.commit_log_duration += Duration::from_nanos(detail.commit_log_nanos);
        self.apply_batch_wait_duration += Duration::from_nanos(detail.apply_batch_wait_nanos);
        self.apply_log_duration += Duration::from_nanos(detail.apply_log_nanos);
        self.apply_mutex_lock_duration += Duration::from_nanos(detail.apply_mutex_lock_nanos);
        self.apply_write_leader_wait_duration +=
            Duration::from_nanos(detail.apply_write_leader_wait_nanos);
        self.apply_write_wal_duration += Duration::from_nanos(detail.apply_write_wal_nanos);
        self.apply_write_memtable_duration +=
            Duration::from_nanos(detail.apply_write_memtable_nanos);
        self.scheduler_latch_wait_duration += Duration::from_nanos(detail.latch_wait_nanos);
        self.scheduler_process_duration += Duration::from_nanos(detail.process_nanos);
        self.scheduler_throttle_duration += Duration::from_nanos(detail.throttle_nanos);
        self.scheduler_pessimistic_lock_wait_duration +=
            Duration::from_nanos(detail.pessimistic_lock_wait_nanos);
    }

    pub fn merge(&mut self, other: &Self) {
        self.store_batch_wait_duration += other.store_batch_wait_duration;
        self.propose_send_wait_duration += other.propose_send_wait_duration;
        self.persist_log_duration += other.persist_log_duration;
        self.raft_db_write_leader_wait_duration += other.raft_db_write_leader_wait_duration;
        self.raft_db_sync_log_duration += other.raft_db_sync_log_duration;
        self.raft_db_write_memtable_duration += other.raft_db_write_memtable_duration;
        self.commit_log_duration += other.commit_log_duration;
        self.apply_batch_wait_duration += other.apply_batch_wait_duration;
        self.apply_log_duration += other.apply_log_duration;
        self.apply_mutex_lock_duration += other.apply_mutex_lock_duration;
        self.apply_write_leader_wait_duration += other.apply_write_leader_wait_duration;
        self.apply_write_wal_duration += other.apply_write_wal_duration;
        self.apply_write_memtable_duration += other.apply_write_memtable_duration;
        self.scheduler_latch_wait_duration += other.scheduler_latch_wait_duration;
        self.scheduler_process_duration += other.scheduler_process_duration;
        self.scheduler_throttle_duration += other.scheduler_throttle_duration;
        self.scheduler_pessimistic_lock_wait_duration +=
            other.scheduler_pessimistic_lock_wait_duration;
    }
}

impl fmt::Display for WriteDetail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::default() {
            return Ok(());
        }
        write!(
            formatter,
            "write_detail: {{store_batch_wait: {}, propose_send_wait: {}, persist_log: {{total: {}, write_leader_wait: {}, sync_log: {}, write_memtable: {}}}, commit_log: {}, apply_batch_wait: {}, apply: {{total:{}, mutex_lock: {}, write_leader_wait: {}, write_wal: {}, write_memtable: {}}}, scheduler: {{process: {}",
            format_duration(self.store_batch_wait_duration),
            format_duration(self.propose_send_wait_duration),
            format_duration(self.persist_log_duration),
            format_duration(self.raft_db_write_leader_wait_duration),
            format_duration(self.raft_db_sync_log_duration),
            format_duration(self.raft_db_write_memtable_duration),
            format_duration(self.commit_log_duration),
            format_duration(self.apply_batch_wait_duration),
            format_duration(self.apply_log_duration),
            format_duration(self.apply_mutex_lock_duration),
            format_duration(self.apply_write_leader_wait_duration),
            format_duration(self.apply_write_wal_duration),
            format_duration(self.apply_write_memtable_duration),
            format_duration(self.scheduler_process_duration),
        )?;
        if !self.scheduler_latch_wait_duration.is_zero() {
            write!(
                formatter,
                ", latch_wait: {}",
                format_duration(self.scheduler_latch_wait_duration)
            )?;
        }
        if !self.scheduler_pessimistic_lock_wait_duration.is_zero() {
            write!(
                formatter,
                ", pessimistic_lock_wait: {}",
                format_duration(self.scheduler_pessimistic_lock_wait_duration)
            )?;
        }
        if !self.scheduler_throttle_duration.is_zero() {
            write!(
                formatter,
                ", throttle: {}",
                format_duration(self.scheduler_throttle_duration)
            )?;
        }
        formatter.write_str("}}")
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TimeDetail {
    pub process_time: Duration,
    pub suspend_time: Duration,
    pub wait_time: Duration,
    pub kv_read_wall_time: Duration,
    pub kv_grpc_process_time: Duration,
    pub kv_grpc_wait_time: Duration,
    pub total_rpc_wall_time: Duration,
}

impl TimeDetail {
    pub fn merge(&mut self, other: &Self) {
        self.process_time += other.process_time;
        self.suspend_time += other.suspend_time;
        self.wait_time += other.wait_time;
        self.kv_read_wall_time += other.kv_read_wall_time;
        self.kv_grpc_process_time += other.kv_grpc_process_time;
        self.kv_grpc_wait_time += other.kv_grpc_wait_time;
        self.total_rpc_wall_time += other.total_rpc_wall_time;
    }

    pub fn merge_from_pb(
        &mut self,
        v2: Option<&kvrpcpb::TimeDetailV2>,
        legacy: Option<&kvrpcpb::TimeDetail>,
    ) {
        if let Some(detail) = v2 {
            self.wait_time += Duration::from_nanos(detail.wait_wall_time_ns);
            self.process_time += Duration::from_nanos(detail.process_wall_time_ns);
            self.suspend_time += Duration::from_nanos(detail.process_suspend_wall_time_ns);
            self.kv_read_wall_time += Duration::from_nanos(detail.kv_read_wall_time_ns);
            self.kv_grpc_process_time += Duration::from_nanos(detail.kv_grpc_process_time_ns);
            self.kv_grpc_wait_time += Duration::from_nanos(detail.kv_grpc_wait_time_ns);
            self.total_rpc_wall_time += Duration::from_nanos(detail.total_rpc_wall_time_ns);
        } else if let Some(detail) = legacy {
            self.wait_time += Duration::from_millis(detail.wait_wall_time_ms);
            self.process_time += Duration::from_millis(detail.process_wall_time_ms);
            self.kv_read_wall_time += Duration::from_millis(detail.kv_read_wall_time_ms);
            self.total_rpc_wall_time += Duration::from_nanos(detail.total_rpc_wall_time_ns);
        }
    }
}

impl fmt::Display for TimeDetail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields = [
            ("total_process_time", self.process_time),
            ("total_suspend_time", self.suspend_time),
            ("total_wait_time", self.wait_time),
            ("total_kv_read_wall_time", self.kv_read_wall_time),
            ("tikv_grpc_process_time", self.kv_grpc_process_time),
            ("tikv_grpc_wait_time", self.kv_grpc_wait_time),
            ("tikv_wall_time", self.total_rpc_wall_time),
        ];
        let mut present = fields
            .into_iter()
            .filter(|(_, duration)| !duration.is_zero())
            .peekable();
        if present.peek().is_none() {
            return Ok(());
        }
        formatter.write_str("time_detail: {")?;
        for (index, (name, duration)) in present.enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{name}: {}", format_duration(duration))?;
        }
        formatter.write_str("}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::kvrpcpb::{ExecutorInputs, PoolTaskDetails as PbPoolTaskDetails, Ruv2};
    use crate::proto::resource_manager::Consumption;
    use crate::util::RuDetails;

    fn millis(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    #[test]
    fn ru_details_drain_ru_v2() {
        let details = RuDetails::new();
        details.add_ru_v2(Some(&Ruv2 {
            read_rpc_count: 1,
            storage_processed_keys_batch_get: 2,
            executor_inputs: Some(ExecutorInputs {
                tikv_coprocessor_executor_work_total_batch_selection: 3,
                ..Default::default()
            }),
            ..Default::default()
        }));
        details.add_ru_v2(Some(&Ruv2 {
            write_rpc_count: 4,
            storage_processed_keys_get: 5,
            raftstore_store_write_trigger_wb_bytes: 6,
            executor_inputs: Some(ExecutorInputs {
                tikv_coprocessor_executor_work_total_batch_selection: 7,
                ..Default::default()
            }),
            ..Default::default()
        }));
        let drained = details.drain_ru_v2().unwrap();
        assert_eq!(drained.read_rpc_count, 1);
        assert_eq!(drained.write_rpc_count, 4);
        assert_eq!(drained.storage_processed_keys_batch_get, 2);
        assert_eq!(drained.storage_processed_keys_get, 5);
        assert_eq!(drained.raftstore_store_write_trigger_wb_bytes, 6);
        assert_eq!(
            drained
                .executor_inputs
                .unwrap()
                .tikv_coprocessor_executor_work_total_batch_selection,
            10
        );
        assert!(details.drain_ru_v2().is_none());
    }

    #[test]
    fn ru_details_clone_and_merge_raw_ru_v2() {
        let original = RuDetails::new();
        original.add_ru_v2(Some(&Ruv2 {
            read_rpc_count: 1,
            executor_inputs: Some(ExecutorInputs {
                tikv_coprocessor_executor_work_total_batch_index_scan: 2,
                ..Default::default()
            }),
            ..Default::default()
        }));
        let cloned = original.cloned();
        cloned.add_ru_v2(Some(&Ruv2 {
            write_rpc_count: 3,
            ..Default::default()
        }));
        let original_raw = original.drain_ru_v2().unwrap();
        assert_eq!(original_raw.read_rpc_count, 1);
        assert_eq!(original_raw.write_rpc_count, 0);
        assert_eq!(
            original_raw
                .executor_inputs
                .unwrap()
                .tikv_coprocessor_executor_work_total_batch_index_scan,
            2
        );
        let clone_raw = cloned.drain_ru_v2().unwrap();
        assert_eq!(clone_raw.read_rpc_count, 1);
        assert_eq!(clone_raw.write_rpc_count, 3);

        let left = RuDetails::new();
        left.add_ru_v2(Some(&Ruv2 {
            read_rpc_count: 5,
            ..Default::default()
        }));
        let right = RuDetails::new();
        right.add_ru_v2(Some(&Ruv2 {
            write_rpc_count: 7,
            ..Default::default()
        }));
        left.merge(&right);
        let merged = left.drain_ru_v2().unwrap();
        assert_eq!(merged.read_rpc_count, 5);
        assert_eq!(merged.write_rpc_count, 7);
        assert_eq!(right.drain_ru_v2().unwrap().write_rpc_count, 7);
    }

    fn full_pool_details() -> PoolTaskDetails {
        PoolTaskDetails {
            task_count: 2,
            poll_count: 8,
            max_poll_count: 5,
            min_poll_count: 3,
            dispatch_count: 6,
            max_dispatch_count: 4,
            min_dispatch_count: 2,
            total_wall_time: millis(20),
            task_wall_time_sample_count: 2,
            max_task_wall_time: millis(12),
            min_task_wall_time: millis(8),
            total_queue_wait_time: millis(12),
            max_queue_wait_time: millis(4),
            min_queue_wait_time: millis(1),
            total_wake_wait_time: millis(8),
            max_wake_wait_time: millis(3),
            min_wake_wait_time: millis(1),
            fair_queue_sample_count: 6,
            total_fair_queue_waited_task_slices: 18,
            max_fair_queue_waited_task_slices: 5,
            min_fair_queue_waited_task_slices: 2,
            poll_cpu_time: millis(8),
            max_poll_cpu_time: millis(2),
            min_poll_cpu_time: Duration::from_micros(500),
            poll_wall_time: millis(12),
            max_poll_wall_time: millis(3),
            min_poll_wall_time: Duration::from_micros(750),
        }
    }

    #[test]
    fn pool_task_details_string_uses_average_times() {
        assert_eq!(
            full_pool_details().to_string(),
            "{tasks:2, poll_count:{total:8, avg:4, max:5, min:3}, dispatch_count:{total:6, max:4, min:2}, task_wall_time:{total:20ms, avg:10ms, max:12ms, min:8ms}, queue_wait:{total:12ms, avg:2ms, max:4ms, min:1ms}, wake_wait:{total:8ms, avg:2ms, max:3ms, min:1ms}, fair_queue:{enabled:true, waited_task_slices:{total:18, avg:3, max:5, min:2}}, poll_cpu:{total:8ms, avg:1ms, max:2ms, min:500µs}, poll_wall:{total:12ms, avg:1.5ms, max:3ms, min:750µs}}"
        );
    }

    #[test]
    fn pool_task_details_string_omits_zero_times() {
        let details = PoolTaskDetails {
            task_count: 1,
            poll_count: 2,
            max_poll_count: 2,
            min_poll_count: 2,
            dispatch_count: 2,
            max_dispatch_count: 2,
            min_dispatch_count: 2,
            ..Default::default()
        };
        assert_eq!(
            details.to_string(),
            "{tasks:1, poll_count:{total:2, avg:2, max:2, min:2}, dispatch_count:{total:2, max:2, min:2}, fair_queue:{enabled:false, waited_task_slices:{total:0, max:0, min:0}}}"
        );
    }

    #[test]
    fn pool_task_details_string_omits_average_with_no_samples() {
        let details = PoolTaskDetails {
            task_count: 1,
            total_queue_wait_time: millis(2),
            max_queue_wait_time: millis(2),
            min_queue_wait_time: millis(2),
            ..Default::default()
        };
        assert_eq!(
            details.to_string(),
            "{tasks:1, poll_count:{total:0, avg:0, max:0, min:0}, dispatch_count:{total:0, max:0, min:0}, queue_wait:{total:2ms, max:2ms, min:2ms}, fair_queue:{enabled:false, waited_task_slices:{total:0, max:0, min:0}}}"
        );
    }

    fn pool_pb(
        poll_count: u64,
        dispatch_count: u64,
        wall_ms: u64,
        queue: (u64, u64, u64),
        wake: (u64, u64, u64),
        fair: Option<(u64, u64, u64)>,
        cpu: (u64, u64, u64),
        poll_wall: (u64, u64, u64),
    ) -> PbPoolTaskDetails {
        PbPoolTaskDetails {
            poll_count,
            dispatch_count,
            total_wall_nanos: millis(wall_ms).as_nanos() as u64,
            total_queue_wait_nanos: millis(queue.0).as_nanos() as u64,
            max_queue_wait_nanos: millis(queue.1).as_nanos() as u64,
            min_queue_wait_nanos: millis(queue.2).as_nanos() as u64,
            total_wake_wait_nanos: millis(wake.0).as_nanos() as u64,
            max_wake_wait_nanos: millis(wake.1).as_nanos() as u64,
            min_wake_wait_nanos: millis(wake.2).as_nanos() as u64,
            fair_queue_enabled: fair.is_some(),
            total_fair_queue_waited_task_slices: fair.map_or(0, |value| value.0),
            max_fair_queue_waited_task_slices: fair.map_or(0, |value| value.1),
            min_fair_queue_waited_task_slices: fair.map_or(0, |value| value.2),
            poll_cpu_nanos: millis(cpu.0).as_nanos() as u64,
            max_poll_cpu_nanos: millis(cpu.1).as_nanos() as u64,
            min_poll_cpu_nanos: millis(cpu.2).as_nanos() as u64,
            poll_wall_nanos: millis(poll_wall.0).as_nanos() as u64,
            max_poll_wall_nanos: millis(poll_wall.1).as_nanos() as u64,
            min_poll_wall_nanos: millis(poll_wall.2).as_nanos() as u64,
        }
    }

    #[test]
    fn pool_task_details_merge_from_pb_and_merge() {
        let mut first = pool_pb(
            5,
            3,
            10,
            (6, 3, 1),
            (4, 3, 1),
            Some((9, 5, 1)),
            (5, 2, 0),
            (8, 4, 1),
        );
        first.min_poll_cpu_nanos = Duration::from_micros(500).as_nanos() as u64;
        let mut second = pool_pb(2, 1, 0, (2, 2, 2), (0, 0, 0), None, (2, 1, 0), (3, 2, 0));
        second.max_poll_cpu_nanos = Duration::from_micros(1_500).as_nanos() as u64;
        second.min_poll_cpu_nanos = Duration::from_micros(400).as_nanos() as u64;
        second.min_poll_wall_nanos = Duration::from_micros(800).as_nanos() as u64;
        second.total_fair_queue_waited_task_slices = 100;
        second.max_fair_queue_waited_task_slices = 100;
        second.min_fair_queue_waited_task_slices = 100;

        let mut details = PoolTaskDetails::default();
        details.merge_from_pb(Some(&first));
        details.merge_from_pb(Some(&second));
        assert_eq!(
            details,
            PoolTaskDetails {
                task_count: 2,
                poll_count: 7,
                max_poll_count: 5,
                min_poll_count: 2,
                dispatch_count: 4,
                max_dispatch_count: 3,
                min_dispatch_count: 1,
                total_wall_time: millis(10),
                task_wall_time_sample_count: 1,
                max_task_wall_time: millis(10),
                min_task_wall_time: millis(10),
                total_queue_wait_time: millis(8),
                max_queue_wait_time: millis(3),
                min_queue_wait_time: millis(1),
                total_wake_wait_time: millis(4),
                max_wake_wait_time: millis(3),
                min_wake_wait_time: millis(1),
                fair_queue_sample_count: 3,
                total_fair_queue_waited_task_slices: 9,
                max_fair_queue_waited_task_slices: 5,
                min_fair_queue_waited_task_slices: 1,
                poll_cpu_time: millis(7),
                max_poll_cpu_time: millis(2),
                min_poll_cpu_time: Duration::from_micros(400),
                poll_wall_time: millis(11),
                max_poll_wall_time: millis(4),
                min_poll_wall_time: Duration::from_micros(800),
            }
        );

        let mut third = pool_pb(
            8,
            4,
            20,
            (12, 5, 2),
            (6, 4, 2),
            Some((8, 4, 0)),
            (8, 3, 0),
            (10, 5, 0),
        );
        third.min_poll_cpu_nanos = Duration::from_micros(300).as_nanos() as u64;
        third.min_poll_wall_nanos = Duration::from_micros(700).as_nanos() as u64;
        let mut other = PoolTaskDetails::default();
        other.merge_from_pb(Some(&third));
        details.merge(&other);
        assert_eq!(details.task_count, 3);
        assert_eq!(details.poll_count, 15);
        assert_eq!(details.max_poll_count, 8);
        assert_eq!(details.min_poll_count, 2);
        assert_eq!(details.dispatch_count, 8);
        assert_eq!(details.total_wall_time, millis(30));
        assert_eq!(details.task_wall_time_sample_count, 2);
        assert_eq!(details.total_queue_wait_time, millis(20));
        assert_eq!(details.total_wake_wait_time, millis(10));
        assert_eq!(details.fair_queue_sample_count, 7);
        assert_eq!(details.total_fair_queue_waited_task_slices, 17);
        assert_eq!(details.poll_cpu_time, millis(15));
        assert_eq!(details.min_poll_cpu_time, Duration::from_micros(300));
        assert_eq!(details.poll_wall_time, millis(21));
        assert_eq!(details.min_poll_wall_time, Duration::from_micros(700));
    }

    fn merge_sequentially_and_aggregate(
        first: &PbPoolTaskDetails,
        second: &PbPoolTaskDetails,
    ) -> PoolTaskDetails {
        let mut sequential = PoolTaskDetails::default();
        sequential.merge_from_pb(Some(first));
        sequential.merge_from_pb(Some(second));
        let mut left = PoolTaskDetails::default();
        left.merge_from_pb(Some(first));
        let mut right = PoolTaskDetails::default();
        right.merge_from_pb(Some(second));
        left.merge(&right);
        assert_eq!(sequential, left);
        sequential
    }

    #[test]
    fn pool_task_details_merge_minimum_presence() {
        let first = pool_pb(2, 1, 3, (0, 0, 0), (0, 0, 0), None, (1, 1, 0), (2, 2, 0));
        let second = pool_pb(
            3,
            2,
            25,
            (10, 7, 3),
            (2, 2, 2),
            Some((4, 3, 1)),
            (6, 3, 1),
            (9, 4, 2),
        );
        let details = merge_sequentially_and_aggregate(&first, &second);
        assert_eq!(details.min_queue_wait_time, millis(3));
        assert_eq!(details.min_wake_wait_time, millis(2));
        assert_eq!(details.min_fair_queue_waited_task_slices, 1);
        assert_eq!(details.min_poll_cpu_time, Duration::ZERO);
        assert_eq!(details.min_poll_wall_time, Duration::ZERO);

        let first = pool_pb(
            3,
            3,
            10,
            (1, 1, 0),
            (1, 1, 0),
            Some((0, 0, 0)),
            (1, 1, 0),
            (1, 1, 0),
        );
        let second = pool_pb(
            2,
            2,
            10,
            (4, 3, 1),
            (1, 1, 1),
            Some((3, 2, 1)),
            (2, 1, 1),
            (2, 1, 1),
        );
        let details = merge_sequentially_and_aggregate(&first, &second);
        assert_eq!(details.min_queue_wait_time, Duration::ZERO);
        assert_eq!(details.min_wake_wait_time, Duration::ZERO);
        assert_eq!(details.min_fair_queue_waited_task_slices, 0);
        assert_eq!(details.min_poll_cpu_time, Duration::ZERO);
        assert_eq!(details.min_poll_wall_time, Duration::ZERO);
    }

    #[test]
    fn pool_task_details_string_formats_fractional_count_averages() {
        let details = PoolTaskDetails {
            task_count: 3,
            poll_count: 8,
            max_poll_count: 5,
            min_poll_count: 1,
            dispatch_count: 3,
            max_dispatch_count: 1,
            min_dispatch_count: 1,
            fair_queue_sample_count: 3,
            total_fair_queue_waited_task_slices: 7,
            max_fair_queue_waited_task_slices: 4,
            ..Default::default()
        };
        assert_eq!(
            details.to_string(),
            "{tasks:3, poll_count:{total:8, avg:2.67, max:5, min:1}, dispatch_count:{total:3, max:1, min:1}, fair_queue:{enabled:true, waited_task_slices:{total:7, avg:2.33, max:4, min:0}}}"
        );
    }

    #[test]
    fn pool_task_details_string_keeps_zero_fair_queue_wait() {
        let details = PoolTaskDetails {
            task_count: 1,
            poll_count: 2,
            max_poll_count: 2,
            min_poll_count: 2,
            dispatch_count: 2,
            max_dispatch_count: 2,
            min_dispatch_count: 2,
            fair_queue_sample_count: 2,
            ..Default::default()
        };
        assert_eq!(
            details.to_string(),
            "{tasks:1, poll_count:{total:2, avg:2, max:2, min:2}, dispatch_count:{total:2, max:2, min:2}, fair_queue:{enabled:true, waited_task_slices:{total:0, avg:0, max:0, min:0}}}"
        );
    }

    #[test]
    fn pool_task_details_empty_and_clone() {
        let mut details = PoolTaskDetails::default();
        details.merge_from_pb(None);
        assert!(details.is_empty());
        assert_eq!(details.to_string(), "");
        details.merge_from_pb(Some(&PbPoolTaskDetails {
            poll_count: 1,
            dispatch_count: 1,
            ..Default::default()
        }));
        assert!(!details.is_empty());
        let mut cloned = details;
        cloned.poll_count += 1;
        assert_ne!(details.poll_count, cloned.poll_count);
        let before = details;
        details.merge(&PoolTaskDetails::default());
        assert_eq!(details, before);
    }

    #[test]
    fn scan_detail_merge_from_v2_includes_ia_fields() {
        let pb = kvrpcpb::ScanDetailV2 {
            processed_versions: 10,
            processed_versions_size: 20,
            total_versions: 30,
            rocksdb_delete_skipped_count: 4,
            rocksdb_key_skipped_count: 5,
            rocksdb_block_cache_hit_count: 6,
            rocksdb_block_read_count: 7,
            rocksdb_block_read_byte: 8,
            rocksdb_block_read_nanos: Duration::from_micros(9).as_nanos() as u64,
            get_snapshot_nanos: Duration::from_micros(7).as_nanos() as u64,
            ia_cache_hit_count: 2,
            ia_remote_read_segment_count: 3,
            ia_remote_read_segment_bytes: 128,
            ia_remote_read_segment_nanos: Duration::from_micros(5).as_nanos() as u64,
            ..Default::default()
        };
        let mut detail = ScanDetail::default();
        detail.merge_from_pb(Some(&pb));
        assert_eq!(detail.total_keys, 30);
        assert_eq!(detail.processed_keys, 10);
        assert_eq!(detail.processed_keys_size, 20);
        assert_eq!(detail.rocksdb_delete_skipped_count, 4);
        assert_eq!(detail.rocksdb_key_skipped_count, 5);
        assert_eq!(detail.rocksdb_block_cache_hit_count, 6);
        assert_eq!(detail.rocksdb_block_read_count, 7);
        assert_eq!(detail.rocksdb_block_read_bytes, 8);
        assert_eq!(detail.rocksdb_block_read_duration, Duration::from_micros(9));
        assert_eq!(detail.get_snapshot_duration, Duration::from_micros(7));
        assert_eq!(detail.ia_cache_hit_count, 2);
        assert_eq!(detail.ia_remote_read_segment_count, 3);
        assert_eq!(detail.ia_remote_read_segment_bytes, 128);
        assert_eq!(
            detail.ia_remote_read_segment_duration,
            Duration::from_micros(5)
        );
        let text = detail.to_string();
        for expected in [
            "total_process_keys: 10",
            "total_process_keys_size: 20",
            "total_keys: 30",
            "get_snapshot_time",
            "ia: {",
            "cache_hit_count: 2",
            "remote_read_segment_count: 3",
            "remote_read_segment_bytes: 128 Bytes",
            "remote_read_segment_wait_time",
            "rocksdb: {",
            "delete_skipped_count: 4",
            "key_skipped_count: 5",
            "cache_hit_count: 6",
            "read_count: 7",
            "read_byte: 8 Bytes",
            "read_time",
        ] {
            assert!(text.contains(expected), "missing {expected} in {text}");
        }
    }

    #[test]
    fn scan_detail_merge_includes_ia_fields() {
        let mut left = ScanDetail {
            ia_cache_hit_count: 1,
            ia_remote_read_segment_count: 2,
            ia_remote_read_segment_bytes: 64,
            ia_remote_read_segment_duration: Duration::from_micros(3),
            ..Default::default()
        };
        left.merge(&ScanDetail {
            ia_cache_hit_count: 4,
            ia_remote_read_segment_count: 5,
            ia_remote_read_segment_bytes: 256,
            ia_remote_read_segment_duration: Duration::from_micros(7),
            ..Default::default()
        });
        assert_eq!(left.ia_cache_hit_count, 5);
        assert_eq!(left.ia_remote_read_segment_count, 7);
        assert_eq!(left.ia_remote_read_segment_bytes, 320);
        assert_eq!(
            left.ia_remote_read_segment_duration,
            Duration::from_micros(10)
        );
    }

    fn lock_details_a() -> LockKeysDetails {
        LockKeysDetails {
            total_time: millis(10),
            region_num: 2,
            lock_keys: 5,
            aggressive_lock_new_count: 1,
            aggressive_lock_derived_count: 2,
            locked_with_conflict_count: 3,
            resolve_lock: ResolveLockDetail {
                resolve_lock_time_ns: 100,
            },
            backoff_time_ns: 200,
            detail: LockKeysDetailsInner {
                backoff_types: vec!["txnLock".to_owned()],
                slowest_request_total_time: millis(5),
                slowest_region: 10,
                slowest_store_address: "store1".to_owned(),
                ..Default::default()
            },
            lock_rpc_time_ns: 300,
            lock_rpc_count: 4,
            retry_count: 1,
        }
    }

    fn lock_details_b() -> LockKeysDetails {
        LockKeysDetails {
            total_time: millis(20),
            region_num: 3,
            lock_keys: 7,
            aggressive_lock_new_count: 4,
            aggressive_lock_derived_count: 5,
            locked_with_conflict_count: 6,
            resolve_lock: ResolveLockDetail {
                resolve_lock_time_ns: 150,
            },
            backoff_time_ns: 250,
            detail: LockKeysDetailsInner {
                backoff_types: vec!["regionMiss".to_owned()],
                slowest_request_total_time: millis(8),
                slowest_region: 20,
                slowest_store_address: "store2".to_owned(),
                ..Default::default()
            },
            lock_rpc_time_ns: 350,
            lock_rpc_count: 5,
            retry_count: 2,
        }
    }

    #[test]
    fn lock_keys_details_merge() {
        let mut left = lock_details_a();
        left.merge(&lock_details_b());
        assert_eq!(left.total_time, millis(30));
        assert_eq!(left.region_num, 5);
        assert_eq!(left.lock_keys, 12);
        assert_eq!(left.aggressive_lock_new_count, 5);
        assert_eq!(left.aggressive_lock_derived_count, 7);
        assert_eq!(left.locked_with_conflict_count, 9);
        assert_eq!(left.resolve_lock.resolve_lock_time_ns, 250);
        assert_eq!(left.backoff_time_ns, 450);
        assert_eq!(left.lock_rpc_time_ns, 650);
        assert_eq!(left.lock_rpc_count, 9);
        assert_eq!(left.retry_count, 2);
        assert_eq!(left.detail.backoff_types, ["txnLock", "regionMiss"]);
        assert_eq!(left.detail.slowest_request_total_time, millis(8));
        assert_eq!(left.detail.slowest_region, 20);
        assert_eq!(left.detail.slowest_store_address, "store2");
    }

    #[test]
    fn lock_keys_details_merge_slowest_not_replaced() {
        let mut left = LockKeysDetails::default();
        left.detail.slowest_request_total_time = millis(10);
        left.detail.slowest_region = 1;
        left.detail.slowest_store_address = "store1".to_owned();
        let mut right = LockKeysDetails::default();
        right.detail.slowest_request_total_time = millis(5);
        right.detail.slowest_region = 2;
        right.detail.slowest_store_address = "store2".to_owned();
        left.merge(&right);
        assert_eq!(left.detail.slowest_request_total_time, millis(10));
        assert_eq!(left.detail.slowest_region, 1);
        assert_eq!(left.detail.slowest_store_address, "store1");
    }

    #[test]
    fn lock_keys_details_clone_is_deep() {
        let original = lock_details_a();
        let mut cloned = original.clone();
        assert_eq!(original, cloned);
        cloned.detail.backoff_types.push("extra".to_owned());
        cloned.total_time = millis(999);
        assert_eq!(original.detail.backoff_types.len(), 1);
        assert_eq!(original.total_time, millis(10));
    }

    fn commit_details_a() -> CommitDetails {
        CommitDetails {
            get_commit_ts_time: millis(10),
            get_latest_ts_time: millis(5),
            prewrite_time: millis(20),
            wait_prewrite_binlog_time: millis(3),
            commit_time: millis(15),
            local_latch_time: millis(2),
            detail: CommitDetailsInner {
                commit_backoff_time_ns: 100,
                prewrite_backoff_types: vec!["txnLock".to_owned()],
                commit_backoff_types: vec!["regionMiss".to_owned()],
                slowest_prewrite: ReqDetailInfo {
                    request_total_time: millis(5),
                    region: 1,
                    store_address: "s1".to_owned(),
                    ..Default::default()
                },
                commit_primary: ReqDetailInfo {
                    request_total_time: millis(3),
                    region: 2,
                    store_address: "s2".to_owned(),
                    ..Default::default()
                },
            },
            write_keys: 100,
            write_size: 2_000,
            prewrite_region_num: 4,
            transaction_retry: 1,
            resolve_lock: ResolveLockDetail {
                resolve_lock_time_ns: 50,
            },
            ..Default::default()
        }
    }

    fn commit_details_b() -> CommitDetails {
        CommitDetails {
            get_commit_ts_time: millis(12),
            get_latest_ts_time: millis(6),
            prewrite_time: millis(25),
            wait_prewrite_binlog_time: millis(4),
            commit_time: millis(18),
            local_latch_time: millis(3),
            detail: CommitDetailsInner {
                commit_backoff_time_ns: 200,
                prewrite_backoff_types: vec!["tikvRPC".to_owned()],
                commit_backoff_types: vec!["txnLock".to_owned()],
                slowest_prewrite: ReqDetailInfo {
                    request_total_time: millis(8),
                    region: 10,
                    store_address: "s10".to_owned(),
                    ..Default::default()
                },
                commit_primary: ReqDetailInfo {
                    request_total_time: millis(6),
                    region: 20,
                    store_address: "s20".to_owned(),
                    ..Default::default()
                },
            },
            write_keys: 150,
            write_size: 3_000,
            prewrite_region_num: 5,
            transaction_retry: 2,
            resolve_lock: ResolveLockDetail {
                resolve_lock_time_ns: 60,
            },
            ..Default::default()
        }
    }

    #[test]
    fn commit_details_merge() {
        let mut left = commit_details_a();
        left.merge(&commit_details_b());
        assert_eq!(left.get_commit_ts_time, millis(22));
        assert_eq!(left.get_latest_ts_time, millis(11));
        assert_eq!(left.prewrite_time, millis(45));
        assert_eq!(left.wait_prewrite_binlog_time, millis(7));
        assert_eq!(left.commit_time, millis(33));
        assert_eq!(left.local_latch_time, millis(5));
        assert_eq!(left.write_keys, 250);
        assert_eq!(left.write_size, 5_000);
        assert_eq!(left.prewrite_region_num, 9);
        assert_eq!(left.transaction_retry, 3);
        assert_eq!(left.resolve_lock.resolve_lock_time_ns, 110);
        assert_eq!(left.detail.commit_backoff_time_ns, 300);
        assert_eq!(left.detail.prewrite_backoff_types, ["txnLock", "tikvRPC"]);
        assert_eq!(left.detail.commit_backoff_types, ["regionMiss", "txnLock"]);
        assert_eq!(left.detail.slowest_prewrite.request_total_time, millis(8));
        assert_eq!(left.detail.slowest_prewrite.region, 10);
        assert_eq!(left.detail.slowest_prewrite.store_address, "s10");
        assert_eq!(left.detail.commit_primary.request_total_time, millis(6));
        assert_eq!(left.detail.commit_primary.region, 20);
        assert_eq!(left.detail.commit_primary.store_address, "s20");
    }

    #[test]
    fn commit_details_merge_slowest_not_replaced() {
        let mut left = CommitDetails::default();
        left.detail.slowest_prewrite.request_total_time = millis(10);
        left.detail.slowest_prewrite.region = 1;
        left.detail.commit_primary.request_total_time = millis(10);
        left.detail.commit_primary.region = 2;
        let mut right = CommitDetails::default();
        right.detail.slowest_prewrite.request_total_time = millis(5);
        right.detail.slowest_prewrite.region = 3;
        right.detail.commit_primary.request_total_time = millis(5);
        right.detail.commit_primary.region = 4;
        left.merge(&right);
        assert_eq!(left.detail.slowest_prewrite.region, 1);
        assert_eq!(left.detail.commit_primary.region, 2);
    }

    #[test]
    fn commit_details_clone_is_deep() {
        let mut original = commit_details_a();
        original.prewrite_request_num = 9;
        let mut cloned = original.clone();
        assert_eq!(cloned.prewrite_request_num, 0);
        cloned.prewrite_request_num = original.prewrite_request_num;
        assert_eq!(original, cloned);
        cloned
            .detail
            .prewrite_backoff_types
            .push("extra".to_owned());
        cloned.detail.commit_backoff_types.push("extra".to_owned());
        cloned.get_commit_ts_time = millis(999);
        assert_eq!(original.detail.prewrite_backoff_types.len(), 1);
        assert_eq!(original.detail.commit_backoff_types.len(), 1);
        assert_eq!(original.get_commit_ts_time, millis(10));
    }

    #[test]
    fn scan_detail_merge() {
        let mut left = ScanDetail {
            total_keys: 100,
            processed_keys: 50,
            processed_keys_size: 1_000,
            rocksdb_delete_skipped_count: 10,
            rocksdb_key_skipped_count: 20,
            rocksdb_block_cache_hit_count: 30,
            rocksdb_block_read_count: 40,
            rocksdb_block_read_bytes: 5_000,
            rocksdb_block_read_duration: millis(1),
            get_snapshot_duration: millis(2),
            ..Default::default()
        };
        left.merge(&ScanDetail {
            total_keys: 200,
            processed_keys: 80,
            processed_keys_size: 2_000,
            rocksdb_delete_skipped_count: 15,
            rocksdb_key_skipped_count: 25,
            rocksdb_block_cache_hit_count: 35,
            rocksdb_block_read_count: 45,
            rocksdb_block_read_bytes: 6_000,
            rocksdb_block_read_duration: millis(3),
            get_snapshot_duration: millis(4),
            ..Default::default()
        });
        assert_eq!(left.total_keys, 300);
        assert_eq!(left.processed_keys, 130);
        assert_eq!(left.processed_keys_size, 3_000);
        assert_eq!(left.rocksdb_delete_skipped_count, 25);
        assert_eq!(left.rocksdb_key_skipped_count, 45);
        assert_eq!(left.rocksdb_block_cache_hit_count, 65);
        assert_eq!(left.rocksdb_block_read_count, 85);
        assert_eq!(left.rocksdb_block_read_bytes, 11_000);
        assert_eq!(left.rocksdb_block_read_duration, millis(4));
        assert_eq!(left.get_snapshot_duration, millis(6));
    }

    fn full_write_detail(multiplier: u32) -> WriteDetail {
        WriteDetail {
            store_batch_wait_duration: millis(1) * multiplier,
            propose_send_wait_duration: millis(2) * multiplier,
            persist_log_duration: millis(3) * multiplier,
            raft_db_write_leader_wait_duration: millis(4) * multiplier,
            raft_db_sync_log_duration: millis(5) * multiplier,
            raft_db_write_memtable_duration: millis(6) * multiplier,
            commit_log_duration: millis(7) * multiplier,
            apply_batch_wait_duration: millis(8) * multiplier,
            apply_log_duration: millis(9) * multiplier,
            apply_mutex_lock_duration: millis(10) * multiplier,
            apply_write_leader_wait_duration: millis(11) * multiplier,
            apply_write_wal_duration: millis(12) * multiplier,
            apply_write_memtable_duration: millis(13) * multiplier,
            scheduler_latch_wait_duration: millis(14) * multiplier,
            scheduler_process_duration: millis(15) * multiplier,
            scheduler_throttle_duration: millis(16) * multiplier,
            scheduler_pessimistic_lock_wait_duration: millis(17) * multiplier,
        }
    }

    #[test]
    fn write_detail_merge() {
        let mut left = full_write_detail(1);
        left.merge(&full_write_detail(1));
        assert_eq!(left, full_write_detail(2));
    }

    #[test]
    fn time_detail_merge() {
        let mut left = TimeDetail {
            process_time: millis(10),
            suspend_time: millis(2),
            wait_time: millis(5),
            kv_read_wall_time: millis(3),
            kv_grpc_process_time: millis(1),
            kv_grpc_wait_time: millis(4),
            total_rpc_wall_time: millis(20),
        };
        left.merge(&TimeDetail {
            process_time: millis(15),
            suspend_time: millis(3),
            wait_time: millis(7),
            kv_read_wall_time: millis(4),
            kv_grpc_process_time: millis(2),
            kv_grpc_wait_time: millis(5),
            total_rpc_wall_time: millis(30),
        });
        assert_eq!(left.process_time, millis(25));
        assert_eq!(left.suspend_time, millis(5));
        assert_eq!(left.wait_time, millis(12));
        assert_eq!(left.kv_read_wall_time, millis(7));
        assert_eq!(left.kv_grpc_process_time, millis(3));
        assert_eq!(left.kv_grpc_wait_time, millis(9));
        assert_eq!(left.total_rpc_wall_time, millis(50));
    }

    #[test]
    fn time_detail_merge_absence_and_string() {
        let mut detail = TimeDetail {
            kv_read_wall_time: millis(2),
            total_rpc_wall_time: millis(3),
            ..Default::default()
        };
        assert_eq!(
            detail.to_string(),
            "time_detail: {total_kv_read_wall_time: 2ms, tikv_wall_time: 3ms}"
        );
        detail.merge_from_pb(None, None);
        assert_eq!(detail.kv_read_wall_time, millis(2));
        detail = TimeDetail {
            process_time: millis(2),
            suspend_time: millis(3),
            wait_time: millis(4),
            kv_read_wall_time: millis(5),
            kv_grpc_process_time: millis(6),
            kv_grpc_wait_time: millis(7),
            total_rpc_wall_time: millis(8),
        };
        assert_eq!(
            detail.to_string(),
            "time_detail: {total_process_time: 2ms, total_suspend_time: 3ms, total_wait_time: 4ms, total_kv_read_wall_time: 5ms, tikv_grpc_process_time: 6ms, tikv_grpc_wait_time: 7ms, tikv_wall_time: 8ms}"
        );
    }

    #[test]
    fn ru_details_update_tiflash() {
        let details = RuDetails::new();
        details.update(
            &Consumption {
                r_r_u: 1.5,
                w_r_u: 2.5,
                ..Default::default()
            },
            millis(3),
        );
        details.update_tiflash(&Consumption {
            r_r_u: 3.0,
            w_r_u: 4.0,
            ..Default::default()
        });
        assert!((details.read_ru() - 4.5).abs() < 1e-9);
        assert!((details.write_ru() - 6.5).abs() < 1e-9);
        assert!((details.tiflash_ru() - 7.0).abs() < 1e-9);
        assert_eq!(details.ru_wait_duration(), millis(3));
        assert!((details.cloned().tiflash_ru() - 7.0).abs() < 1e-9);
    }

    #[test]
    fn typed_context_keys_do_not_collide() {
        let base = TraceContext::new();
        let commit = Arc::new(Mutex::new(CommitDetails::default()));
        let lock = Arc::new(Mutex::new(LockKeysDetails::default()));
        let exec = Arc::new(ExecDetails::default());
        let ru = Arc::new(RuDetails::new());
        let context = context_with_commit_details(&base, commit.clone());
        let context = context_with_lock_keys_details(&context, lock.clone());
        let context = context_with_exec_details(&context, exec.clone());
        let context = context_with_ru_details(&context, ru.clone());
        assert!(Arc::ptr_eq(
            commit_details_from_context(&context).unwrap(),
            &commit
        ));
        assert!(Arc::ptr_eq(
            lock_keys_details_from_context(&context).unwrap(),
            &lock
        ));
        assert!(Arc::ptr_eq(
            exec_details_from_context(&context).unwrap(),
            &exec
        ));
        assert!(Arc::ptr_eq(ru_details_from_context(&context).unwrap(), &ru));
        assert!(commit_details_from_context(&base).is_none());
    }
}
