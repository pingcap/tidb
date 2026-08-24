// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;
use std::time::Instant;

use prometheus::register_gauge;
use prometheus::register_gauge_vec;
use prometheus::register_histogram;
use prometheus::register_histogram_vec;
use prometheus::register_int_counter;
use prometheus::register_int_counter_vec;
use prometheus::Gauge;
use prometheus::GaugeVec;
use prometheus::Histogram;
use prometheus::HistogramOpts;
use prometheus::HistogramVec;
use prometheus::IntCounter;
use prometheus::IntCounterVec;

use crate::Result;

pub struct RequestStats {
    start: Instant,
    cmd: &'static str,
    duration: &'static HistogramVec,
    failed_duration: &'static HistogramVec,
    failed_counter: &'static IntCounterVec,
}

impl RequestStats {
    pub fn new(
        cmd: &'static str,
        duration: &'static HistogramVec,
        counter: &'static IntCounterVec,
        failed_duration: &'static HistogramVec,
        failed_counter: &'static IntCounterVec,
    ) -> Self {
        counter.with_label_values(&[cmd]).inc();
        RequestStats {
            start: Instant::now(),
            cmd,
            duration,
            failed_duration,
            failed_counter,
        }
    }

    pub fn done<R>(&self, r: Result<R>) -> Result<R> {
        if r.is_ok() {
            self.duration
                .with_label_values(&[self.cmd])
                .observe(duration_to_sec(self.start.elapsed()));
        } else {
            self.failed_duration
                .with_label_values(&[self.cmd])
                .observe(duration_to_sec(self.start.elapsed()));
            self.failed_counter.with_label_values(&[self.cmd]).inc();
        }
        r
    }
}

pub fn tikv_stats(cmd: &'static str) -> RequestStats {
    RequestStats::new(
        cmd,
        &TIKV_REQUEST_DURATION_HISTOGRAM_VEC,
        &TIKV_REQUEST_COUNTER_VEC,
        &TIKV_FAILED_REQUEST_DURATION_HISTOGRAM_VEC,
        &TIKV_FAILED_REQUEST_COUNTER_VEC,
    )
}

pub fn pd_stats(cmd: &'static str) -> RequestStats {
    RequestStats::new(
        cmd,
        &PD_REQUEST_DURATION_HISTOGRAM_VEC,
        &PD_REQUEST_COUNTER_VEC,
        &PD_FAILED_REQUEST_DURATION_HISTOGRAM_VEC,
        &PD_FAILED_REQUEST_COUNTER_VEC,
    )
}

pub(crate) fn increment_write_conflict() {
    TIKV_TXN_WRITE_CONFLICT_COUNTER.inc();
}

pub(crate) fn increment_batch_receive_loop_panic() {
    TIKV_PANIC_COUNTER
        .with_label_values(&["batch-recv-loop"])
        .inc();
}

pub(crate) fn increment_batch_send_loop_panic() {
    TIKV_PANIC_COUNTER
        .with_label_values(&["batch-send-loop"])
        .inc();
}

pub(crate) fn observe_tso_future_wait(duration: Duration) {
    TIKV_TS_FUTURE_WAIT_DURATION.observe(duration_to_sec(duration));
}

pub(crate) fn increment_validate_read_ts_from_pd() {
    TIKV_VALIDATE_READ_TS_FROM_PD_COUNT.inc();
}

pub(crate) fn set_low_resolution_tso_update_interval(interval: Duration) {
    TIKV_LOW_RESOLUTION_TSO_UPDATE_INTERVAL_SECONDS.set(duration_to_sec(interval));
}

/// Records the three observations emitted by client-go's pipelined MemDB
/// worker: the immutable generation's entry count, logical size, and flush
/// execution time. The high-level metrics package owns global registration
/// policy; this helper keeps the unionstore call site behaviorally complete.
pub(crate) fn observe_pipelined_flush(len: usize, size: usize, duration: Duration) {
    TIKV_PIPELINED_FLUSH_LEN_HISTOGRAM.observe(len as f64);
    TIKV_PIPELINED_FLUSH_SIZE_HISTOGRAM.observe(size as f64);
    TIKV_PIPELINED_FLUSH_DURATION_HISTOGRAM.observe(duration_to_sec(duration));
}

pub(crate) fn observe_retry_backoff(kind: &'static str, duration: Duration) {
    TIKV_BACKOFF_HISTOGRAM
        .with_label_values(&[kind])
        .observe(duration_to_sec(duration));
}

/// Mirrors client-go's range-task completed/failed region gauges. Completed
/// work is reset when a runner exits; failed work remains cumulative.
pub(crate) fn reset_range_task_completed(task_type: &'static str) {
    TIKV_RANGE_TASK_STATS
        .with_label_values(&[task_type, "completed-regions"])
        .set(0.0);
}

pub(crate) fn add_range_task_stats(
    task_type: &'static str,
    completed_regions: usize,
    failed_regions: usize,
) {
    TIKV_RANGE_TASK_STATS
        .with_label_values(&[task_type, "completed-regions"])
        .add(completed_regions as f64);
    TIKV_RANGE_TASK_STATS
        .with_label_values(&[task_type, "failed-regions"])
        .add(failed_regions as f64);
}

/// Records client-go's time spent waiting to enqueue a range subtask for a
/// worker. This is deliberately the channel-send duration, not handler time.
pub(crate) fn observe_range_task_push_duration(task_type: &'static str, duration: Duration) {
    TIKV_RANGE_TASK_PUSH_DURATION
        .with_label_values(&[task_type])
        .observe(duration_to_sec(duration));
}

/// Source `LoadRegionCacheHistogramWithRegions` plus the paired ScanRegions
/// result counter. These observations belong to every PD range-scan attempt,
/// including attempts that will be retried by the caller.
pub(crate) fn observe_region_cache_scan(duration: Duration, succeeded: bool) {
    TIKV_LOAD_REGION_CACHE_DURATION
        .with_label_values(&["scan_regions"])
        .observe(duration_to_sec(duration));
    TIKV_REGION_CACHE_OPERATIONS
        .with_label_values(&["scan_regions", if succeeded { "ok" } else { "err" }])
        .inc();
}

/// Source `LoadRegionCacheHistogramWithBatchScanRegions` and paired result
/// counter for each PD BatchScanRegions attempt.
pub(crate) fn observe_region_cache_batch_scan(duration: Duration, succeeded: bool) {
    TIKV_LOAD_REGION_CACHE_DURATION
        .with_label_values(&["batch_scan_regions"])
        .observe(duration_to_sec(duration));
    TIKV_REGION_CACHE_OPERATIONS
        .with_label_values(&["batch_scan_regions", if succeeded { "ok" } else { "err" }])
        .inc();
}

/// Source `TiKVStaleRegionFromPDCounter` for malformed, incomplete, or
/// leaderless PD range-scan results that must be retried.
pub(crate) fn increment_stale_region_from_pd() {
    TIKV_STALE_REGION_FROM_PD.inc();
}

/// Source `TiKVStoreLimitErrorCounter`. The address identifies the logical
/// TiKV store selected for the region request, even when transport forwards
/// through another store.
pub(crate) fn increment_store_limit_error(address: &str, store_id: u64) {
    let store_id = store_id.to_string();
    TIKV_STORE_LIMIT_ERROR_COUNTER
        .with_label_values(&[address, &store_id])
        .inc();
}

pub(crate) fn observe_stale_read_request(size: u64, cross_zone: bool) {
    let zone = if cross_zone { "cross-zone" } else { "local" };
    TIKV_STALE_READ_BYTES
        .with_label_values(&[zone, "out"])
        .inc_by(size);
    TIKV_STALE_READ_REQUESTS.with_label_values(&[zone]).inc();
}

pub(crate) fn observe_stale_read_response(size: u64, cross_zone: bool) {
    let zone = if cross_zone { "cross-zone" } else { "local" };
    TIKV_STALE_READ_BYTES
        .with_label_values(&[zone, "in"])
        .inc_by(size);
}

pub(crate) fn observe_read_request_bytes(
    size: u64,
    follower: bool,
    access_location: crate::kv::AccessLocationType,
) {
    let location = match access_location {
        crate::kv::AccessLocationType::LocalZone => "local",
        crate::kv::AccessLocationType::CrossZone => "cross-zone",
        crate::kv::AccessLocationType::Unknown | crate::kv::AccessLocationType::Other(_) => return,
    };
    TIKV_READ_REQUEST_BYTES
        .with_label_values(&[if follower { "follower" } else { "leader" }, location])
        .observe(size as f64);
}

#[cfg(test)]
pub(crate) fn stale_read_request_count(zone: &str) -> u64 {
    TIKV_STALE_READ_REQUESTS.with_label_values(&[zone]).get()
}

#[cfg(test)]
pub(crate) fn stale_read_bytes(zone: &str, direction: &str) -> u64 {
    TIKV_STALE_READ_BYTES
        .with_label_values(&[zone, direction])
        .get()
}

#[cfg(test)]
pub(crate) fn read_request_bytes_samples(replica: &str, location: &str) -> (u64, f64) {
    let metric = TIKV_READ_REQUEST_BYTES.with_label_values(&[replica, location]);
    (metric.get_sample_count(), metric.get_sample_sum())
}

/// Source `TiKVGRPCConnTransientFailureCounter`, observed immediately before
/// sending on a selected non-batch connection already known to be transient.
pub(crate) fn increment_grpc_connection_transient_failure(address: &str, store_id: u64) {
    let store_id = store_id.to_string();
    TIKV_GRPC_CONNECTION_TRANSIENT_FAILURE_COUNTER
        .with_label_values(&[address, &store_id])
        .inc();
}

#[cfg(test)]
pub(crate) fn grpc_connection_transient_failures(address: &str, store_id: u64) -> u64 {
    let store_id = store_id.to_string();
    TIKV_GRPC_CONNECTION_TRANSIENT_FAILURE_COUNTER
        .with_label_values(&[address, &store_id])
        .get()
}

/// Event-driven native mapping of client-go's one-second grpc-go connection
/// state sampler. Tonic does not expose `ClientConn.GetState`, while all
/// externally observable transitions pass through the Rust pool owner.
pub(crate) fn set_grpc_connection_state(connection_id: &str, store_ip: &str, state: &str) {
    for candidate in [
        "IDLE",
        "CONNECTING",
        "READY",
        "TRANSIENT_FAILURE",
        "SHUTDOWN",
    ] {
        TIKV_GRPC_CONNECTION_STATE
            .with_label_values(&[connection_id, store_ip, candidate])
            .set(if candidate == state { 1.0 } else { 0.0 });
    }
}

/// Removes a connection from client-go's monitor by clearing every retained
/// state label. Prometheus vectors retain the series, matching the source's
/// explicit zeroing in `connMonitor.RemoveConn`.
pub(crate) fn clear_grpc_connection_state(connection_id: &str, store_ip: &str) {
    for state in [
        "IDLE",
        "CONNECTING",
        "READY",
        "TRANSIENT_FAILURE",
        "SHUTDOWN",
    ] {
        TIKV_GRPC_CONNECTION_STATE
            .with_label_values(&[connection_id, store_ip, state])
            .set(0.0);
    }
}

#[cfg(test)]
pub(crate) fn grpc_connection_state(connection_id: &str, store_ip: &str, state: &str) -> f64 {
    TIKV_GRPC_CONNECTION_STATE
        .with_label_values(&[connection_id, store_ip, state])
        .get()
}

/// Records client-go's per-store request histogram, source-dimensional
/// summary, and client-minus-server network latency observation. The Go
/// summary has no configured quantiles, so the native histogram preserves
/// its count/sum observations while also exposing buckets.
pub(crate) fn observe_tikv_store_rpc(
    request: &dyn crate::store::Request,
    response: Option<&dyn std::any::Any>,
    latency: Duration,
) {
    let context = request.tikv_context();
    let store = context
        .and_then(|context| context.peer.as_ref())
        .map_or(0, |peer| peer.store_id)
        .to_string();
    let stale = context
        .is_some_and(|context| context.stale_read)
        .to_string();
    let source = context.map_or("", |context| context.request_source.as_str());
    let internal = source.starts_with("internal");
    let scope = internal.to_string();
    let command = crate::store::CommandType::from_request_label(request.label())
        .map_or("Unknown", crate::store::CommandType::name);
    let seconds = duration_to_sec(latency);

    TIKV_SEND_REQUEST_DURATION
        .with_label_values(&[command, &store, &stale, &scope])
        .observe(seconds);
    TIKV_SEND_REQUEST_BY_SOURCE
        .with_label_values(&[command, &store, &stale, &scope, source])
        .observe(seconds);

    let Some(details) = response.and_then(crate::store::exec_details_v2) else {
        return;
    };
    let total_rpc_wall_time_ns = details
        .time_detail_v2
        .as_ref()
        .map(|time| time.total_rpc_wall_time_ns)
        .or_else(|| {
            details
                .time_detail
                .as_ref()
                .map(|time| time.total_rpc_wall_time_ns)
        })
        .unwrap_or_default();
    if total_rpc_wall_time_ns > 0 {
        TIKV_RPC_NET_LATENCY
            .with_label_values(&[&store, &scope])
            .observe(seconds - total_rpc_wall_time_ns as f64 / 1_000_000_000.0);
    }
}

#[cfg(test)]
pub(crate) fn tikv_store_rpc_samples(
    command: &str,
    store: &str,
    stale: &str,
    scope: &str,
    source: &str,
) -> (u64, u64, u64, f64) {
    (
        TIKV_SEND_REQUEST_DURATION
            .with_label_values(&[command, store, stale, scope])
            .get_sample_count(),
        TIKV_SEND_REQUEST_BY_SOURCE
            .with_label_values(&[command, store, stale, scope, source])
            .get_sample_count(),
        TIKV_RPC_NET_LATENCY
            .with_label_values(&[store, scope])
            .get_sample_count(),
        TIKV_RPC_NET_LATENCY
            .with_label_values(&[store, scope])
            .get_sample_sum(),
    )
}

/// Source `TiKVLockResolverCounter`. The caller supplies the source shortcut
/// label (for example `read_async_resolve_fallback`).
pub(crate) fn increment_lock_resolver_action(action: &'static str) {
    TIKV_LOCK_RESOLVER_ACTIONS
        .with_label_values(&[action])
        .inc();
}

/// Source `TiKVLockResolverAsyncRunningTasks`. Detached resolver tasks keep
/// this gauge balanced on both normal completion and cancellation-safe drop.
pub(crate) fn add_lock_resolver_async_running_tasks(kind: &'static str, delta: i64) {
    TIKV_LOCK_RESOLVER_ASYNC_RUNNING_TASKS
        .with_label_values(&[kind])
        .add(delta as f64);
}

#[cfg(test)]
pub(crate) fn lock_resolver_action_count(action: &'static str) -> u64 {
    TIKV_LOCK_RESOLVER_ACTIONS
        .with_label_values(&[action])
        .get()
}

#[cfg(test)]
pub(crate) fn lock_resolver_async_running_tasks(kind: &'static str) -> f64 {
    TIKV_LOCK_RESOLVER_ASYNC_RUNNING_TASKS
        .with_label_values(&[kind])
        .get()
}

#[cfg(test)]
pub(crate) fn store_limit_error_count(address: &str, store_id: u64) -> u64 {
    let store_id = store_id.to_string();
    TIKV_STORE_LIMIT_ERROR_COUNTER
        .with_label_values(&[address, &store_id])
        .get()
}

#[cfg(test)]
pub(crate) fn range_task_stat(task_type: &'static str, result: &'static str) -> f64 {
    TIKV_RANGE_TASK_STATS
        .with_label_values(&[task_type, result])
        .get()
}

#[cfg(test)]
pub(crate) fn range_task_push_duration_samples(task_type: &'static str) -> u64 {
    TIKV_RANGE_TASK_PUSH_DURATION
        .with_label_values(&[task_type])
        .get_sample_count()
}

/// Records the stage/outcome breakdown emitted by client-go's BatchCommands
/// transport. Store is carried from the request context rather than cached on
/// a connection because a pooled target may serve a replacement store ID.
pub(crate) fn observe_batch_request_stage(
    store: u64,
    stage: &'static str,
    outcome: &'static str,
    duration: Duration,
) {
    TIKV_BATCH_REQUEST_STAGE_DURATION
        .with_label_values(&[&store.to_string(), stage, outcome])
        .observe(duration_to_sec(duration));
}

/// Records the receive-loop timings emitted by client-go's BatchCommands
/// stream metrics. The native Prometheus crate does not expose summary
/// vectors, so the source summary is represented by a histogram with the
/// same metric name and label dimensions.
pub(crate) fn observe_batch_stream_recv_loop(
    target: &str,
    connection_index: usize,
    forwarded: bool,
    step: &'static str,
    duration: Duration,
) {
    let connection_index = connection_index.to_string();
    TIKV_BATCH_STREAM_RECV_LOOP_DURATION
        .with_label_values(&[
            target,
            &connection_index,
            if forwarded { "1" } else { "0" },
            step,
        ])
        .observe(duration_to_sec(duration));
}

/// Records the source BatchCommands receive-loop tail latency classes.
pub(crate) fn observe_batch_stream_tail(
    target: &str,
    connection_index: usize,
    forwarded: bool,
    kind: BatchStreamTailKind,
    duration: Duration,
) {
    let connection_index = connection_index.to_string();
    let labels = [target, &connection_index, if forwarded { "1" } else { "0" }];
    let seconds = duration_to_sec(duration);
    match kind {
        BatchStreamTailKind::Receive => TIKV_BATCH_STREAM_RECV_TAIL_LATENCY
            .with_label_values(&labels)
            .observe(seconds),
        BatchStreamTailKind::TikvSend => TIKV_BATCH_STREAM_TIKV_SEND_TAIL_LATENCY
            .with_label_values(&labels)
            .observe(seconds),
        BatchStreamTailKind::CancelledEntry => TIKV_BATCH_STREAM_CANCELED_ENTRY_TAIL_LATENCY
            .with_label_values(&labels)
            .observe(seconds),
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum BatchStreamTailKind {
    Receive,
    TikvSend,
    CancelledEntry,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum BatchStreamRequestCounter {
    Tracked,
    Retired,
    Completed,
    Outdated,
}

pub(crate) fn increment_batch_stream_request_counter(
    target: &str,
    connection_index: usize,
    forwarded: bool,
    counter: BatchStreamRequestCounter,
    count: usize,
) {
    let connection_index = connection_index.to_string();
    let labels = [target, &connection_index, if forwarded { "1" } else { "0" }];
    let counter = match counter {
        BatchStreamRequestCounter::Tracked => &*TIKV_BATCH_STREAM_TRACKED_REQUEST_COUNT,
        BatchStreamRequestCounter::Retired => &*TIKV_BATCH_STREAM_RETIRED_REQUEST_COUNT,
        BatchStreamRequestCounter::Completed => &*TIKV_BATCH_STREAM_COMPLETED_RESPONSE_COUNT,
        BatchStreamRequestCounter::Outdated => &*TIKV_BATCH_STREAM_OUTDATED_RESPONSE_COUNT,
    };
    counter.with_label_values(&labels).inc_by(count as u64);
}

pub(crate) fn observe_batch_client_unavailable(duration: Duration) {
    TIKV_BATCH_CLIENT_UNAVAILABLE.observe(duration_to_sec(duration));
}

pub(crate) fn observe_batch_client_wait_establish(duration: Duration) {
    TIKV_BATCH_CLIENT_WAIT_ESTABLISH.observe(duration_to_sec(duration));
}

pub(crate) fn observe_batch_client_recycle(duration: Duration) {
    TIKV_BATCH_CLIENT_RECYCLE.observe(duration_to_sec(duration));
}

pub(crate) fn observe_batch_send_loop(target: &str, step: &str, duration: Duration) {
    TIKV_BATCH_SEND_LOOP_DURATION
        .with_label_values(&[target, step])
        .observe(duration_to_sec(duration));
}

pub(crate) fn observe_batch_send_tail(target: &str, duration: Duration) {
    TIKV_BATCH_SEND_TAIL_LATENCY
        .with_label_values(&[target])
        .observe(duration_to_sec(duration));
}

pub(crate) fn observe_batch_pending_requests(target: &str, count: usize) {
    TIKV_BATCH_PENDING_REQUESTS
        .with_label_values(&[target])
        .observe(count as f64);
}

pub(crate) fn observe_batch_requests(target: &str, count: usize) {
    TIKV_BATCH_REQUESTS
        .with_label_values(&[target])
        .observe(count as f64);
}

pub(crate) fn observe_batch_head_arrival_interval(target: &str, interval: Duration) {
    TIKV_BATCH_HEAD_ARRIVAL_INTERVAL
        .with_label_values(&[target])
        .observe(duration_to_sec(interval));
}

pub(crate) fn observe_batch_best_size(target: &str, size: f64) {
    TIKV_BATCH_BEST_SIZE
        .with_label_values(&[target])
        .observe(size);
}

pub(crate) fn observe_batch_more_requests(target: &str, count: usize) {
    TIKV_BATCH_MORE_REQUESTS
        .with_label_values(&[target])
        .observe(count as f64);
}

pub(crate) fn increment_batch_wait_overload() {
    TIKV_BATCH_WAIT_OVERLOAD.inc();
}

pub(crate) fn increment_no_available_batch_connection() {
    TIKV_NO_AVAILABLE_BATCH_CONNECTION_COUNTER.inc();
}

#[cfg(test)]
pub(crate) fn write_conflict_count() -> u64 {
    TIKV_TXN_WRITE_CONFLICT_COUNTER.get()
}

#[cfg(test)]
pub(crate) fn batch_stream_metric_sample_counts(
    target: &str,
    connection_index: usize,
    forwarded: bool,
) -> (u64, u64, u64) {
    let connection_index = connection_index.to_string();
    let labels = [target, &connection_index, if forwarded { "1" } else { "0" }];
    (
        TIKV_BATCH_STREAM_RECV_LOOP_DURATION
            .with_label_values(&[labels[0], labels[1], labels[2], "recv"])
            .get_sample_count(),
        TIKV_BATCH_STREAM_RECV_TAIL_LATENCY
            .with_label_values(&labels)
            .get_sample_count(),
        TIKV_BATCH_STREAM_TIKV_SEND_TAIL_LATENCY
            .with_label_values(&labels)
            .get_sample_count(),
    )
}

#[cfg(test)]
pub(crate) fn batch_stream_request_counter_values(
    target: &str,
    connection_index: usize,
    forwarded: bool,
) -> (u64, u64, u64, u64) {
    let connection_index = connection_index.to_string();
    let labels = [target, &connection_index, if forwarded { "1" } else { "0" }];
    (
        TIKV_BATCH_STREAM_TRACKED_REQUEST_COUNT
            .with_label_values(&labels)
            .get(),
        TIKV_BATCH_STREAM_RETIRED_REQUEST_COUNT
            .with_label_values(&labels)
            .get(),
        TIKV_BATCH_STREAM_COMPLETED_RESPONSE_COUNT
            .with_label_values(&labels)
            .get(),
        TIKV_BATCH_STREAM_OUTDATED_RESPONSE_COUNT
            .with_label_values(&labels)
            .get(),
    )
}

#[cfg(test)]
pub(crate) fn batch_stream_cancelled_entry_tail_samples(
    target: &str,
    connection_index: usize,
    forwarded: bool,
) -> u64 {
    let connection_index = connection_index.to_string();
    TIKV_BATCH_STREAM_CANCELED_ENTRY_TAIL_LATENCY
        .with_label_values(&[target, &connection_index, if forwarded { "1" } else { "0" }])
        .get_sample_count()
}

#[allow(dead_code)]
pub fn observe_tso_batch(batch_size: usize) {
    PD_TSO_BATCH_SIZE_HISTOGRAM.observe(batch_size as f64);
}

lazy_static::lazy_static! {
    static ref TIKV_REQUEST_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_request_duration_seconds",
        "Bucketed histogram of TiKV requests duration",
        &["type"]
    )
    .unwrap();
    static ref TIKV_REQUEST_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_request_total",
        "Total number of requests sent to TiKV",
        &["type"]
    )
    .unwrap();
    static ref TIKV_FAILED_REQUEST_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_failed_request_duration_seconds",
        "Bucketed histogram of failed TiKV requests duration",
        &["type"]
    )
    .unwrap();
    static ref TIKV_FAILED_REQUEST_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_failed_request_total",
        "Total number of failed requests sent to TiKV",
        &["type"]
    )
    .unwrap();
    static ref PD_REQUEST_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "pd_request_duration_seconds",
        "Bucketed histogram of PD requests duration",
        &["type"]
    )
    .unwrap();
    static ref PD_REQUEST_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "pd_request_total",
        "Total number of requests sent to PD",
        &["type"]
    )
    .unwrap();
    static ref PD_FAILED_REQUEST_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "pd_failed_request_duration_seconds",
        "Bucketed histogram of failed PD requests duration",
        &["type"]
    )
    .unwrap();
    static ref PD_FAILED_REQUEST_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "pd_failed_request_total",
        "Total number of failed requests sent to PD",
        &["type"]
    )
    .unwrap();
    static ref PD_TSO_BATCH_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "pd_tso_batch_size",
        "Bucketed histogram of TSO request batch size"
    )
    .unwrap();
    static ref TIKV_TXN_WRITE_CONFLICT_COUNTER: IntCounter = prometheus::register_int_counter!(
        "tikv_txn_write_conflict_total",
        "Total number of write conflicts returned by TiKV"
    )
    .unwrap();
    static ref TIKV_PANIC_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_client_go_panic_total",
        "Counter of panics recovered by client background loops.",
        &["type"]
    )
    .unwrap();
    static ref TIKV_TS_FUTURE_WAIT_DURATION: Histogram = register_histogram!(
        "tikv_ts_future_wait_seconds",
        "Bucketed histogram of seconds cost for waiting timestamp future."
    )
    .unwrap();
    static ref TIKV_VALIDATE_READ_TS_FROM_PD_COUNT: IntCounter = register_int_counter!(
        "tikv_validate_read_ts_from_pd_count",
        "Counter of validating read ts by getting a timestamp from PD"
    )
    .unwrap();
    static ref TIKV_LOW_RESOLUTION_TSO_UPDATE_INTERVAL_SECONDS: Gauge = register_gauge!(
        "tikv_low_resolution_tso_update_interval_seconds",
        "The actual working update interval for the low resolution TSO."
    )
    .unwrap();
    static ref TIKV_PIPELINED_FLUSH_LEN_HISTOGRAM: Histogram = register_histogram!(
        HistogramOpts::new(
            "tikv_client_go_pipelined_flush_len",
            "Bucketed histogram of length of pipelined flushed memdb"
        )
        .buckets(prometheus::exponential_buckets(1_000.0, 2.0, 16).unwrap())
    )
    .unwrap();
    static ref TIKV_PIPELINED_FLUSH_SIZE_HISTOGRAM: Histogram = register_histogram!(
        HistogramOpts::new(
            "tikv_client_go_pipelined_flush_size",
            "Bucketed histogram of size of pipelined flushed memdb"
        )
        .buckets(prometheus::exponential_buckets(16.0 * 1024.0 * 1024.0, 1.2, 13).unwrap())
    )
    .unwrap();
    static ref TIKV_PIPELINED_FLUSH_DURATION_HISTOGRAM: Histogram = register_histogram!(
        HistogramOpts::new(
            "tikv_client_go_pipelined_flush_duration",
            "Flush time of pipelined memdb."
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 28).unwrap())
    )
    .unwrap();
    static ref TIKV_BACKOFF_HISTOGRAM: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_backoff_seconds",
            "total backoff seconds of a single backoffer."
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 29).unwrap()),
        &["type"]
    )
    .unwrap();
    static ref TIKV_RANGE_TASK_STATS: GaugeVec = register_gauge_vec!(
        "tikv_client_go_range_task_stats",
        "stat of range tasks",
        &["type", "result"]
    )
    .unwrap();
    static ref TIKV_RANGE_TASK_PUSH_DURATION: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_range_task_push_duration",
            "duration to push sub tasks to range task workers"
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 20).unwrap()),
        &["type"]
    )
    .unwrap();
    static ref TIKV_REGION_CACHE_OPERATIONS: IntCounterVec = register_int_counter_vec!(
        "tikv_client_go_region_cache_operations_total",
        "Counter of region cache.",
        &["type", "result"]
    )
    .unwrap();
    static ref TIKV_LOAD_REGION_CACHE_DURATION: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_load_region_cache_seconds",
            "Load region information duration"
        )
        .buckets(prometheus::exponential_buckets(0.0001, 2.0, 20).unwrap()),
        &["type"]
    )
    .unwrap();
    static ref TIKV_STALE_REGION_FROM_PD: IntCounter = register_int_counter!(
        "tikv_client_go_stale_region_from_pd",
        "Counter of stale region from PD"
    )
    .unwrap();
    static ref TIKV_STORE_LIMIT_ERROR_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_client_go_get_store_limit_token_error_total",
        "Store token is up to the limit, probably because the store is hot or unavailable",
        &["address", "store"]
    )
    .unwrap();
    static ref TIKV_STALE_READ_REQUESTS: IntCounterVec = register_int_counter_vec!(
        "tikv_client_go_stale_read_req_counter",
        "Counter of stale read requests",
        &["type"]
    )
    .unwrap();
    static ref TIKV_STALE_READ_BYTES: IntCounterVec = register_int_counter_vec!(
        "tikv_client_go_stale_read_bytes",
        "Counter of stale read request bytes",
        &["result", "direction"]
    )
    .unwrap();
    static ref TIKV_READ_REQUEST_BYTES: HistogramVec = register_histogram_vec!(
        "tikv_client_go_read_request_bytes",
        "Summary-compatible read request byte observations",
        &["type", "result"]
    )
    .unwrap();
    static ref TIKV_GRPC_CONNECTION_STATE: GaugeVec = register_gauge_vec!(
        "tikv_client_go_grpc_connection_state",
        "State of gRPC connection",
        &["connection_id", "store_ip", "grpc_state"]
    )
    .unwrap();
    static ref TIKV_GRPC_CONNECTION_TRANSIENT_FAILURE_COUNTER: IntCounterVec =
        register_int_counter_vec!(
            "tikv_client_go_connection_transient_failure_count",
            "Counter of gRPC connection transient failure",
            &["address", "store"]
        )
        .unwrap();
    static ref TIKV_SEND_REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_request_seconds",
            "Bucketed histogram of sending request duration."
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 24).unwrap()),
        &["type", "store", "stale_read", "scope"]
    )
    .unwrap();
    static ref TIKV_SEND_REQUEST_BY_SOURCE: HistogramVec = register_histogram_vec!(
        "tikv_client_go_source_request_seconds",
        "Summary-compatible sending request observations with source dimensions.",
        &["type", "store", "stale_read", "scope", "source"]
    )
    .unwrap();
    static ref TIKV_RPC_NET_LATENCY: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_rpc_net_latency_seconds",
            "Bucketed histogram of time difference between TiDB and TiKV."
        )
        .buckets(prometheus::exponential_buckets(0.0001, 2.0, 20).unwrap()),
        &["store", "scope"]
    )
    .unwrap();
    static ref TIKV_LOCK_RESOLVER_ACTIONS: IntCounterVec = register_int_counter_vec!(
        "tikv_client_go_lock_resolver_actions_total",
        "Counter of lock resolver actions.",
        &["type"]
    )
    .unwrap();
    static ref TIKV_LOCK_RESOLVER_ASYNC_RUNNING_TASKS: GaugeVec = register_gauge_vec!(
        "tikv_client_go_lock_resolver_async_running_tasks",
        "The number of running async resolve lock tasks in lock resolver.",
        &["type"]
    )
    .unwrap();
    static ref TIKV_BATCH_REQUEST_STAGE_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_client_go_batch_request_stage_duration_seconds",
        "Batch request stage duration breakdown by store and outcome",
        &["store", "stage", "result"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_RECV_LOOP_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_client_go_batch_stream_recv_loop_duration_seconds",
        "Batch stream receive loop duration breakdown by steps",
        &["target", "conn", "forward", "step"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_RECV_TAIL_LATENCY: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_batch_stream_recv_tail_latency_seconds",
            "Batch stream receive tail latency"
        )
        .buckets(prometheus::exponential_buckets(0.02, 2.0, 8).unwrap()),
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_TIKV_SEND_TAIL_LATENCY: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_batch_stream_tikv_send_tail_latency_seconds",
            "Tail latency from TiKV sending a batch response until client receipt"
        )
        .buckets(prometheus::exponential_buckets(0.01, 2.0, 8).unwrap()),
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_CANCELED_ENTRY_TAIL_LATENCY: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_batch_stream_canceled_entry_tail_latency_seconds",
            "Tail latency of cancelled entries that later receive responses"
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 8).unwrap()),
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_CLIENT_UNAVAILABLE: Histogram = register_histogram!(
        HistogramOpts::new(
            "tikv_client_go_batch_client_unavailable_seconds",
            "Time a BatchCommands client is unavailable while reconnecting"
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 28).unwrap())
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_TRACKED_REQUEST_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_client_go_batch_stream_tracked_request_count",
        "Count of requests tracked by each batch stream",
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_RETIRED_REQUEST_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_client_go_batch_stream_retired_request_count",
        "Count of requests retired from each batch stream",
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_COMPLETED_RESPONSE_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_client_go_batch_stream_completed_response_count",
        "Count of responses matched to tracked batch requests",
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_OUTDATED_RESPONSE_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_client_go_batch_stream_outdated_response_count",
        "Count of responses for requests no longer tracked by a batch stream",
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_CLIENT_WAIT_ESTABLISH: Histogram = register_histogram!(
        HistogramOpts::new(
            "tikv_client_go_batch_client_wait_connection_establish",
            "Batch client wait for a new connection to establish"
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 28).unwrap())
    )
    .unwrap();
    static ref TIKV_BATCH_CLIENT_RECYCLE: Histogram = register_histogram!(
        HistogramOpts::new(
            "tikv_client_go_batch_client_reset",
            "Batch client recycle connection and reconnect duration"
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 28).unwrap())
    )
    .unwrap();
    static ref TIKV_BATCH_SEND_LOOP_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_client_go_batch_send_loop_duration_seconds",
        "Summary-compatible batch send-loop duration breakdown",
        &["target", "step"]
    )
    .unwrap();
    static ref TIKV_BATCH_SEND_TAIL_LATENCY: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_batch_send_tail_latency_seconds",
            "Batch send tail latency"
        )
        .buckets(prometheus::exponential_buckets(0.02, 2.0, 8).unwrap()),
        &["target"]
    )
    .unwrap();
    static ref TIKV_BATCH_PENDING_REQUESTS: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_batch_pending_requests",
            "Number of requests pending in the batch channel"
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 11).unwrap()),
        &["target"]
    )
    .unwrap();
    static ref TIKV_BATCH_REQUESTS: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_client_go_batch_requests",
            "Number of requests in one batch"
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 11).unwrap()),
        &["target"]
    )
    .unwrap();
    static ref TIKV_BATCH_HEAD_ARRIVAL_INTERVAL: HistogramVec = register_histogram_vec!(
        "tikv_client_go_batch_head_arrival_interval_seconds",
        "Summary-compatible arrival interval of the head request in a batch",
        &["target"]
    )
    .unwrap();
    static ref TIKV_BATCH_BEST_SIZE: HistogramVec = register_histogram_vec!(
        "tikv_client_go_batch_best_size",
        "Summary-compatible best batch size estimated by the batch client",
        &["target"]
    )
    .unwrap();
    static ref TIKV_BATCH_MORE_REQUESTS: HistogramVec = register_histogram_vec!(
        "tikv_client_go_batch_more_requests_total",
        "Summary-compatible number of requests batched by extra fetch",
        &["target"]
    )
    .unwrap();
    static ref TIKV_BATCH_WAIT_OVERLOAD: IntCounter = register_int_counter!(
        "tikv_client_go_batch_wait_overload",
        "Events where TiKV transport-layer overload extended collection"
    )
    .unwrap();
    static ref TIKV_NO_AVAILABLE_BATCH_CONNECTION_COUNTER: IntCounter = register_int_counter!(
        "tikv_client_go_batch_client_no_available_connection_total",
        "Counter of no available batch client"
    )
    .unwrap();
}

/// Convert Duration to seconds.
#[inline]
fn duration_to_sec(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos());
    // In most cases, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{kvrpcpb, metapb};

    #[test]
    fn source_store_rpc_metrics_preserve_dimensions_and_net_latency() {
        let source = "internal_metrics_transport_unique";
        let request = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                peer: Some(metapb::Peer {
                    store_id: 987_654,
                    ..Default::default()
                }),
                stale_read: true,
                request_source: source.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = kvrpcpb::GetResponse {
            exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                    total_rpc_wall_time_ns: 1_000_000,
                    ..Default::default()
                }),
                // V2 has source precedence over this legacy value.
                time_detail: Some(kvrpcpb::TimeDetail {
                    total_rpc_wall_time_ns: 2_000_000,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let before = tikv_store_rpc_samples("Get", "987654", "true", "true", source);

        observe_tikv_store_rpc(&request, Some(&response), Duration::from_millis(4));
        let after_success = tikv_store_rpc_samples("Get", "987654", "true", "true", source);
        assert_eq!(after_success.0, before.0 + 1);
        assert_eq!(after_success.1, before.1 + 1);
        assert_eq!(after_success.2, before.2 + 1);
        assert!((after_success.3 - before.3 - 0.003).abs() < 1e-12);

        observe_tikv_store_rpc(&request, None, Duration::from_millis(2));
        let after_failure = tikv_store_rpc_samples("Get", "987654", "true", "true", source);
        assert_eq!(after_failure.0, after_success.0 + 1);
        assert_eq!(after_failure.1, after_success.1 + 1);
        assert_eq!(after_failure.2, after_success.2);
        assert_eq!(after_failure.3, after_success.3);
    }
}
