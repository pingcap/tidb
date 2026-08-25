// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use prometheus::register_histogram;
use prometheus::register_histogram_vec;
use prometheus::register_int_counter_vec;
use prometheus::Gauge;
use prometheus::GaugeVec;
use prometheus::Histogram;
use prometheus::HistogramVec;
use prometheus::IntCounterVec;
use prometheus::{Counter, CounterVec};

use crate::Result;

struct ClientCounter(&'static str);

impl ClientCounter {
    fn metric(&self) -> Counter {
        crate::metrics::global_metrics()
            .counter(self.0)
            .unwrap_or_else(|| panic!("missing client-go counter {}", self.0))
            .clone()
    }

    fn inc(&self) {
        self.metric().inc();
    }

    #[cfg(test)]
    fn get(&self) -> f64 {
        self.metric().get()
    }
}

struct ClientCounterVec(&'static str);

impl ClientCounterVec {
    fn metric(&self) -> CounterVec {
        crate::metrics::global_metrics()
            .counter_vec(self.0)
            .unwrap_or_else(|| panic!("missing client-go counter vector {}", self.0))
            .clone()
    }

    fn with_label_values(&self, values: &[&str]) -> Counter {
        self.metric().with_label_values(values)
    }
}

struct ClientGauge(&'static str);

impl ClientGauge {
    fn metric(&self) -> Gauge {
        crate::metrics::global_metrics()
            .gauge(self.0)
            .unwrap_or_else(|| panic!("missing client-go gauge {}", self.0))
            .clone()
    }

    fn set(&self, value: f64) {
        self.metric().set(value);
    }
}

struct ClientGaugeVec(&'static str);

impl ClientGaugeVec {
    fn metric(&self) -> GaugeVec {
        crate::metrics::global_metrics()
            .gauge_vec(self.0)
            .unwrap_or_else(|| panic!("missing client-go gauge vector {}", self.0))
            .clone()
    }

    fn with_label_values(&self, values: &[&str]) -> Gauge {
        self.metric().with_label_values(values)
    }
}

struct ClientHistogram(&'static str);

impl ClientHistogram {
    fn metric(&self) -> Histogram {
        crate::metrics::global_metrics()
            .histogram(self.0)
            .unwrap_or_else(|| panic!("missing client-go histogram {}", self.0))
            .clone()
    }

    fn observe(&self, value: f64) {
        self.metric().observe(value);
    }

    #[cfg(test)]
    fn get_sample_count(&self) -> u64 {
        self.metric().get_sample_count()
    }
}

struct ClientObserverVec(&'static str);

impl ClientObserverVec {
    fn with_label_values(&self, values: &[&str]) -> crate::metrics::ClientGoObserver {
        let metrics = crate::metrics::global_metrics();
        if let Some(metric) = metrics.histogram_vec(self.0) {
            return crate::metrics::ClientGoObserver::Histogram(metric.with_label_values(values));
        }
        if let Some(metric) = metrics.summary_vec(self.0) {
            return crate::metrics::ClientGoObserver::Summary(
                metric
                    .with_label_values(values)
                    .unwrap_or_else(|error| panic!("invalid labels for {}: {error}", self.0)),
            );
        }
        panic!("missing client-go observer vector {}", self.0);
    }
}

static TIKV_TXN_WRITE_CONFLICT_COUNTER: ClientCounter =
    ClientCounter("TiKVTxnWriteConflictCounter");
static TIKV_PANIC_COUNTER: ClientCounterVec = ClientCounterVec("TiKVPanicCounter");
static TIKV_TS_FUTURE_WAIT_DURATION: ClientHistogram = ClientHistogram("TiKVTSFutureWaitDuration");
static TIKV_VALIDATE_READ_TS_FROM_PD_COUNT: ClientCounter =
    ClientCounter("TiKVValidateReadTSFromPDCount");
static TIKV_LOW_RESOLUTION_TSO_UPDATE_INTERVAL_SECONDS: ClientGauge =
    ClientGauge("TiKVLowResolutionTSOUpdateIntervalSecondsGauge");
static TIKV_PIPELINED_FLUSH_LEN_HISTOGRAM: ClientHistogram =
    ClientHistogram("TiKVPipelinedFlushLenHistogram");
static TIKV_PIPELINED_FLUSH_SIZE_HISTOGRAM: ClientHistogram =
    ClientHistogram("TiKVPipelinedFlushSizeHistogram");
static TIKV_PIPELINED_FLUSH_DURATION_HISTOGRAM: ClientHistogram =
    ClientHistogram("TiKVPipelinedFlushDuration");
static TIKV_TXN_CMD_DURATION: ClientObserverVec = ClientObserverVec("TiKVTxnCmdHistogram");
static TIKV_TXN_REGIONS_NUM: ClientObserverVec = ClientObserverVec("TiKVTxnRegionsNumHistogram");
static TIKV_RAWKV_CMD_DURATION: ClientObserverVec = ClientObserverVec("TiKVRawkvCmdHistogram");
static TIKV_RAWKV_SIZE: ClientObserverVec = ClientObserverVec("TiKVRawkvSizeHistogram");
static TIKV_ASYNC_BATCH_GET_COUNTER: ClientCounterVec =
    ClientCounterVec("TiKVAsyncBatchGetCounter");
static TIKV_SMALL_READ_DURATION: ClientHistogram = ClientHistogram("TiKVSmallReadDuration");
static TIKV_READ_THROUGHPUT: ClientHistogram = ClientHistogram("TiKVReadThroughput");
static TIKV_BACKOFF_HISTOGRAM: ClientObserverVec = ClientObserverVec("TiKVBackoffHistogram");
static TIKV_RANGE_TASK_STATS: ClientGaugeVec = ClientGaugeVec("TiKVRangeTaskStats");
static TIKV_RANGE_TASK_PUSH_DURATION: ClientObserverVec =
    ClientObserverVec("TiKVRangeTaskPushDuration");
static TIKV_REGION_CACHE_OPERATIONS: ClientCounterVec = ClientCounterVec("TiKVRegionCacheCounter");
static TIKV_LOAD_REGION_CACHE_DURATION: ClientObserverVec =
    ClientObserverVec("TiKVLoadRegionCacheHistogram");
static TIKV_STALE_REGION_FROM_PD: ClientCounter = ClientCounter("TiKVStaleRegionFromPDCounter");
static TIKV_STORE_LIMIT_ERROR_COUNTER: ClientCounterVec =
    ClientCounterVec("TiKVStoreLimitErrorCounter");
static TIKV_REGION_ERROR_COUNTER: ClientCounterVec = ClientCounterVec("TiKVRegionErrorCounter");
static TIKV_PREFER_LEADER_FLOWS: ClientGaugeVec = ClientGaugeVec("TiKVPreferLeaderFlowsGauge");
static TIKV_STORE_LIVENESS: ClientGaugeVec = ClientGaugeVec("TiKVStoreLivenessGauge");
static TIKV_STORE_SLOW_SCORE: ClientGaugeVec = ClientGaugeVec("TiKVStoreSlowScoreGauge");
static TIKV_FEEDBACK_SLOW_SCORE: ClientGaugeVec = ClientGaugeVec("TiKVFeedbackSlowScoreGauge");
static TIKV_HEALTH_FEEDBACK_OPERATIONS: ClientCounterVec =
    ClientCounterVec("TiKVHealthFeedbackOpsCounter");
static TIKV_STALE_READ_REQUESTS: ClientCounterVec = ClientCounterVec("TiKVStaleReadReqCounter");
static TIKV_STALE_READ_BYTES: ClientCounterVec = ClientCounterVec("TiKVStaleReadBytes");
static TIKV_READ_REQUEST_BYTES: ClientObserverVec = ClientObserverVec("TiKVReadRequestBytes");
static TIKV_GRPC_CONNECTION_STATE: ClientGaugeVec = ClientGaugeVec("TiKVGrpcConnectionState");
static TIKV_GRPC_CONNECTION_TRANSIENT_FAILURE_COUNTER: ClientCounterVec =
    ClientCounterVec("TiKVGRPCConnTransientFailureCounter");
static TIKV_SEND_REQUEST_DURATION: ClientObserverVec = ClientObserverVec("TiKVSendReqHistogram");
static TIKV_SEND_REQUEST_BY_SOURCE: ClientObserverVec =
    ClientObserverVec("TiKVSendReqBySourceSummary");
static TIKV_RPC_NET_LATENCY: ClientObserverVec = ClientObserverVec("TiKVRPCNetLatencyHistogram");
static TIKV_LOCK_RESOLVER_ACTIONS: ClientCounterVec = ClientCounterVec("TiKVLockResolverCounter");
static TIKV_LOCK_RESOLVER_ASYNC_RUNNING_TASKS: ClientGaugeVec =
    ClientGaugeVec("TiKVLockResolverAsyncRunningTasks");
static TIKV_BATCH_REQUEST_STAGE_DURATION: ClientObserverVec =
    ClientObserverVec("TiKVBatchRequestStageDuration");
static TIKV_BATCH_STREAM_RECV_LOOP_DURATION: ClientObserverVec =
    ClientObserverVec("TiKVBatchStreamRecvLoopDuration");
static TIKV_BATCH_STREAM_RECV_TAIL_LATENCY: ClientObserverVec =
    ClientObserverVec("TiKVBatchStreamRecvTailLatency");
static TIKV_BATCH_STREAM_TIKV_SEND_TAIL_LATENCY: ClientObserverVec =
    ClientObserverVec("TiKVBatchStreamTiKVSendTailLatency");
static TIKV_BATCH_STREAM_CANCELED_ENTRY_TAIL_LATENCY: ClientObserverVec =
    ClientObserverVec("TiKVBatchStreamCanceledEntryTailLatency");
static TIKV_BATCH_CLIENT_UNAVAILABLE: ClientHistogram =
    ClientHistogram("TiKVBatchClientUnavailable");
static TIKV_BATCH_STREAM_TRACKED_REQUEST_COUNT: ClientCounterVec =
    ClientCounterVec("TiKVBatchStreamTrackedRequestCount");
static TIKV_BATCH_STREAM_RETIRED_REQUEST_COUNT: ClientCounterVec =
    ClientCounterVec("TiKVBatchStreamRetiredRequestCount");
static TIKV_BATCH_STREAM_COMPLETED_RESPONSE_COUNT: ClientCounterVec =
    ClientCounterVec("TiKVBatchStreamCompletedResponseCount");
static TIKV_BATCH_STREAM_OUTDATED_RESPONSE_COUNT: ClientCounterVec =
    ClientCounterVec("TiKVBatchStreamOutdatedResponseCount");
static TIKV_BATCH_CLIENT_WAIT_ESTABLISH: ClientHistogram =
    ClientHistogram("TiKVBatchClientWaitEstablish");
static TIKV_BATCH_CLIENT_RECYCLE: ClientHistogram = ClientHistogram("TiKVBatchClientRecycle");
static TIKV_BATCH_SEND_LOOP_DURATION: ClientObserverVec =
    ClientObserverVec("TiKVBatchSendLoopDuration");
static TIKV_BATCH_SEND_TAIL_LATENCY: ClientObserverVec =
    ClientObserverVec("TiKVBatchSendTailLatency");
static TIKV_BATCH_PENDING_REQUESTS: ClientObserverVec =
    ClientObserverVec("TiKVBatchPendingRequests");
static TIKV_BATCH_REQUESTS: ClientObserverVec = ClientObserverVec("TiKVBatchRequests");
static TIKV_BATCH_HEAD_ARRIVAL_INTERVAL: ClientObserverVec =
    ClientObserverVec("TiKVBatchHeadArrivalInterval");
static TIKV_BATCH_BEST_SIZE: ClientObserverVec = ClientObserverVec("TiKVBatchBestSize");
static TIKV_BATCH_MORE_REQUESTS: ClientObserverVec = ClientObserverVec("TiKVBatchMoreRequests");
static TIKV_BATCH_WAIT_OVERLOAD: ClientCounter = ClientCounter("TiKVBatchWaitOverLoad");
static TIKV_NO_AVAILABLE_BATCH_CONNECTION_COUNTER: ClientCounter =
    ClientCounter("TiKVNoAvailableConnectionCounter");
static TIKV_LOAD_TXN_SAFE_POINT: ClientCounterVec = ClientCounterVec("TiKVLoadTxnSafePointCounter");
static TIKV_SAFE_TS_UPDATE: ClientCounterVec = ClientCounterVec("TiKVSafeTSUpdateCounter");
static TIKV_MIN_SAFE_TS_GAP_SECONDS: ClientGaugeVec = ClientGaugeVec("TiKVMinSafeTSGapSeconds");
static TIKV_UNSAFE_DESTROY_RANGE_FAILURES: ClientCounterVec =
    ClientCounterVec("TiKVUnsafeDestroyRangeFailuresCounterVec");

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

/// Observe one high-level RawKV operation. These labels are owned by
/// client-go's `rawkv` package rather than by the physical RPC dispatcher.
pub(crate) fn observe_rawkv_command(command: &str, duration: Duration) {
    TIKV_RAWKV_CMD_DURATION
        .with_label_values(&[command])
        .observe(duration_to_sec(duration));
}

/// Observe the key/value sizes recorded by client-go's `PutWithTTL` path.
pub(crate) fn observe_rawkv_size(kind: &str, size: usize) {
    TIKV_RAWKV_SIZE
        .with_label_values(&[kind])
        .observe(size as f64);
}

/// Preserve the pinned source's unusual checksum shortcut: it is registered
/// against `TiKVRawkvSizeHistogram`, not `TiKVRawkvCmdHistogram`.
pub(crate) fn observe_rawkv_checksum(duration: Duration) {
    TIKV_RAWKV_SIZE
        .with_label_values(&["raw_checksum"])
        .observe(duration_to_sec(duration));
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

pub(crate) fn increment_load_txn_safe_point(result: &'static str) {
    TIKV_LOAD_TXN_SAFE_POINT.with_label_values(&[result]).inc();
}

pub(crate) fn record_safe_ts_update(result: &'static str, store: &str, safe_ts: u64) {
    TIKV_SAFE_TS_UPDATE
        .with_label_values(&[result, store])
        .inc();
    let safe_time = crate::oracle::get_time_from_timestamp(safe_ts);
    let gap = match std::time::SystemTime::now().duration_since(safe_time) {
        Ok(duration) => duration_to_sec(duration),
        Err(error) => -duration_to_sec(error.duration()),
    };
    TIKV_MIN_SAFE_TS_GAP_SECONDS
        .with_label_values(&[store])
        .set(gap);
}

pub(crate) fn increment_safe_ts_update_failure(store: &str) {
    TIKV_SAFE_TS_UPDATE
        .with_label_values(&["fail", store])
        .inc();
}

pub(crate) fn increment_unsafe_destroy_range_failure(kind: &'static str) {
    TIKV_UNSAFE_DESTROY_RANGE_FAILURES
        .with_label_values(&[kind])
        .inc();
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

pub(crate) struct SnapshotCommandTimer {
    started: Instant,
    command: &'static str,
    scope: &'static str,
}

impl Drop for SnapshotCommandTimer {
    fn drop(&mut self) {
        TIKV_TXN_CMD_DURATION
            .with_label_values(&[self.command, self.scope])
            .observe(duration_to_sec(self.started.elapsed()));
    }
}

pub(crate) fn snapshot_command_timer(
    command: &'static str,
    internal: bool,
) -> SnapshotCommandTimer {
    SnapshotCommandTimer {
        started: Instant::now(),
        command,
        scope: if internal { "internal" } else { "general" },
    }
}

pub(crate) fn observe_snapshot_regions(internal: bool, regions: usize) {
    TIKV_TXN_REGIONS_NUM
        .with_label_values(&["snapshot", if internal { "internal" } else { "general" }])
        .observe(regions as f64);
}

pub(crate) fn increment_async_batch_get(result: &'static str) {
    TIKV_ASYNC_BATCH_GET_COUNTER
        .with_label_values(&[result])
        .inc();
}

#[cfg(test)]
pub(crate) fn async_batch_get_count(result: &'static str) -> u64 {
    TIKV_ASYNC_BATCH_GET_COUNTER
        .with_label_values(&[result])
        .get() as u64
}

pub(crate) fn observe_snapshot_read_sli(read_keys: u64, read_time: f64, read_size: f64) {
    if read_keys == 0 || read_time == 0.0 {
        return;
    }
    if read_keys <= 20 && read_size < 1024.0 * 1024.0 {
        TIKV_SMALL_READ_DURATION.observe(read_time);
    } else {
        TIKV_READ_THROUGHPUT.observe(read_size / read_time);
    }
}

#[cfg(test)]
pub(crate) fn snapshot_read_sli_sample_counts() -> (u64, u64) {
    (
        TIKV_SMALL_READ_DURATION.get_sample_count(),
        TIKV_READ_THROUGHPUT.get_sample_count(),
    )
}

pub(crate) fn observe_retry_backoff(kind: &'static str, duration: Duration) {
    TIKV_BACKOFF_HISTOGRAM
        .with_label_values(&[kind])
        .observe(duration_to_sec(duration));
}

/// Mirrors client-go's range-task completed/failed region gauges. Completed
/// work is reset when a runner exits; failed work remains cumulative.
pub(crate) fn reset_range_task_completed(task_type: &str) {
    TIKV_RANGE_TASK_STATS
        .with_label_values(&[task_type, "completed-regions"])
        .set(0.0);
}

pub(crate) fn add_range_task_stats(
    task_type: &str,
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
pub(crate) fn observe_range_task_push_duration(task_type: &str, duration: Duration) {
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

/// Source `TiKVRegionErrorCounter`, emitted before request-sender retry
/// classification for every region-error response.
pub(crate) fn increment_region_error(error_type: &str, store_id: Option<u64>) {
    let store_id = store_id.map_or_else(|| "nil".to_owned(), |store_id| store_id.to_string());
    TIKV_REGION_ERROR_COUNTER
        .with_label_values(&[error_type, &store_id])
        .inc();
}

#[cfg(test)]
pub(crate) fn region_error_count(error_type: &str, store_id: Option<u64>) -> u64 {
    let store_id = store_id.map_or_else(|| "nil".to_owned(), |store_id| store_id.to_string());
    TIKV_REGION_ERROR_COUNTER
        .with_label_values(&[error_type, &store_id])
        .get() as u64
}

pub(crate) fn set_prefer_leader_flows(destination: &str, store_id: u64, flows: u64) {
    let store_id = store_id.to_string();
    TIKV_PREFER_LEADER_FLOWS
        .with_label_values(&[destination, &store_id])
        .set(flows as f64);
}

pub(crate) fn set_store_liveness(store_id: u64, liveness: u8) {
    let store_id = store_id.to_string();
    TIKV_STORE_LIVENESS
        .with_label_values(&[&store_id])
        .set(f64::from(liveness));
}

pub(crate) fn set_store_slow_scores(store_id: u64, client_side: i64, tikv_side: i64) {
    let store_id = store_id.to_string();
    TIKV_STORE_SLOW_SCORE
        .with_label_values(&[&store_id])
        .set(client_side as f64);
    TIKV_FEEDBACK_SLOW_SCORE
        .with_label_values(&[&store_id])
        .set(tikv_side as f64);
}

pub(crate) fn increment_health_feedback_operation(store_id: u64, operation: &str) {
    let store_id = store_id.to_string();
    TIKV_HEALTH_FEEDBACK_OPERATIONS
        .with_label_values(&[&store_id, operation])
        .inc();
}

pub(crate) fn remove_store_metrics(store_id: u64) {
    let store_id = store_id.to_string();
    for collector in crate::metrics::global_metrics().store_metric_vec_list() {
        collector.delete_partial_match(&[(crate::metrics::labels::STORE, &store_id)]);
    }
}

/// Finds one store represented by the source liveness collector but absent
/// from PD's current non-tombstone store set. Looking at metric labels rather
/// than cache entries also catches labels retained by a replaced cache.
pub(crate) fn find_next_stale_store_id(valid_store_ids: &HashSet<u64>) -> Option<u64> {
    let collector = crate::metrics::global_metrics()
        .metric_vec("TiKVStoreLivenessGauge")
        .expect("store liveness must be an initialized metric vector");
    let store_id = crate::metrics::find_next_stale_store_id(&collector, valid_store_ids);
    (store_id != 0).then_some(store_id)
}

#[cfg(test)]
pub(crate) fn prefer_leader_flows(destination: &str, store_id: u64) -> f64 {
    let store_id = store_id.to_string();
    TIKV_PREFER_LEADER_FLOWS
        .with_label_values(&[destination, &store_id])
        .get()
}

pub(crate) fn observe_stale_read_request(size: u64, cross_zone: bool) {
    let zone = if cross_zone { "cross-zone" } else { "local" };
    TIKV_STALE_READ_BYTES
        .with_label_values(&[zone, "out"])
        .inc_by(size as f64);
    TIKV_STALE_READ_REQUESTS.with_label_values(&[zone]).inc();
}

pub(crate) fn observe_stale_read_response(size: u64, cross_zone: bool) {
    let zone = if cross_zone { "cross-zone" } else { "local" };
    TIKV_STALE_READ_BYTES
        .with_label_values(&[zone, "in"])
        .inc_by(size as f64);
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
    TIKV_STALE_READ_REQUESTS.with_label_values(&[zone]).get() as u64
}

#[cfg(test)]
pub(crate) fn stale_read_bytes(zone: &str, direction: &str) -> u64 {
    TIKV_STALE_READ_BYTES
        .with_label_values(&[zone, direction])
        .get() as u64
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
        .get() as u64
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
/// summary, and client-minus-server network latency observation. The source
/// summary has no configured quantiles; the shared registry preserves its
/// summary count/sum exposition without adding histogram buckets.
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
        .get() as u64
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
        .get() as u64
}

#[cfg(test)]
pub(crate) fn range_task_stat(task_type: &str, result: &str) -> f64 {
    TIKV_RANGE_TASK_STATS
        .with_label_values(&[task_type, result])
        .get()
}

#[cfg(test)]
pub(crate) fn range_task_push_duration_samples(task_type: &str) -> u64 {
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
/// stream summary with exact label dimensions and count/sum exposition.
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
        BatchStreamRequestCounter::Tracked => &TIKV_BATCH_STREAM_TRACKED_REQUEST_COUNT,
        BatchStreamRequestCounter::Retired => &TIKV_BATCH_STREAM_RETIRED_REQUEST_COUNT,
        BatchStreamRequestCounter::Completed => &TIKV_BATCH_STREAM_COMPLETED_RESPONSE_COUNT,
        BatchStreamRequestCounter::Outdated => &TIKV_BATCH_STREAM_OUTDATED_RESPONSE_COUNT,
    };
    counter.with_label_values(&labels).inc_by(count as f64);
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
    TIKV_TXN_WRITE_CONFLICT_COUNTER.get() as u64
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
            .get() as u64,
        TIKV_BATCH_STREAM_RETIRED_REQUEST_COUNT
            .with_label_values(&labels)
            .get() as u64,
        TIKV_BATCH_STREAM_COMPLETED_RESPONSE_COUNT
            .with_label_values(&labels)
            .get() as u64,
        TIKV_BATCH_STREAM_OUTDATED_RESPONSE_COUNT
            .with_label_values(&labels)
            .get() as u64,
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
    fn every_production_adapter_resolves_the_source_collector_type() {
        let metrics = crate::metrics::global_metrics();
        let counters = [
            &TIKV_TXN_WRITE_CONFLICT_COUNTER,
            &TIKV_VALIDATE_READ_TS_FROM_PD_COUNT,
            &TIKV_STALE_REGION_FROM_PD,
            &TIKV_BATCH_WAIT_OVERLOAD,
            &TIKV_NO_AVAILABLE_BATCH_CONNECTION_COUNTER,
        ];
        assert_eq!(counters.len(), 5);
        for metric in counters {
            assert!(metrics.counter(metric.0).is_some(), "{}", metric.0);
        }

        let counter_vecs = [
            &TIKV_PANIC_COUNTER,
            &TIKV_ASYNC_BATCH_GET_COUNTER,
            &TIKV_REGION_CACHE_OPERATIONS,
            &TIKV_STORE_LIMIT_ERROR_COUNTER,
            &TIKV_REGION_ERROR_COUNTER,
            &TIKV_HEALTH_FEEDBACK_OPERATIONS,
            &TIKV_STALE_READ_REQUESTS,
            &TIKV_STALE_READ_BYTES,
            &TIKV_GRPC_CONNECTION_TRANSIENT_FAILURE_COUNTER,
            &TIKV_LOCK_RESOLVER_ACTIONS,
            &TIKV_BATCH_STREAM_TRACKED_REQUEST_COUNT,
            &TIKV_BATCH_STREAM_RETIRED_REQUEST_COUNT,
            &TIKV_BATCH_STREAM_COMPLETED_RESPONSE_COUNT,
            &TIKV_BATCH_STREAM_OUTDATED_RESPONSE_COUNT,
        ];
        assert_eq!(counter_vecs.len(), 14);
        for metric in counter_vecs {
            assert!(metrics.counter_vec(metric.0).is_some(), "{}", metric.0);
        }

        assert!(metrics
            .gauge(TIKV_LOW_RESOLUTION_TSO_UPDATE_INTERVAL_SECONDS.0)
            .is_some());
        let gauge_vecs = [
            &TIKV_RANGE_TASK_STATS,
            &TIKV_PREFER_LEADER_FLOWS,
            &TIKV_STORE_LIVENESS,
            &TIKV_STORE_SLOW_SCORE,
            &TIKV_FEEDBACK_SLOW_SCORE,
            &TIKV_GRPC_CONNECTION_STATE,
            &TIKV_LOCK_RESOLVER_ASYNC_RUNNING_TASKS,
        ];
        assert_eq!(gauge_vecs.len(), 7);
        for metric in gauge_vecs {
            assert!(metrics.gauge_vec(metric.0).is_some(), "{}", metric.0);
        }

        let histograms = [
            &TIKV_TS_FUTURE_WAIT_DURATION,
            &TIKV_PIPELINED_FLUSH_LEN_HISTOGRAM,
            &TIKV_PIPELINED_FLUSH_SIZE_HISTOGRAM,
            &TIKV_PIPELINED_FLUSH_DURATION_HISTOGRAM,
            &TIKV_SMALL_READ_DURATION,
            &TIKV_READ_THROUGHPUT,
            &TIKV_BATCH_CLIENT_UNAVAILABLE,
            &TIKV_BATCH_CLIENT_WAIT_ESTABLISH,
            &TIKV_BATCH_CLIENT_RECYCLE,
        ];
        assert_eq!(histograms.len(), 9);
        for metric in histograms {
            assert!(metrics.histogram(metric.0).is_some(), "{}", metric.0);
        }

        let observers = [
            &TIKV_TXN_CMD_DURATION,
            &TIKV_TXN_REGIONS_NUM,
            &TIKV_BACKOFF_HISTOGRAM,
            &TIKV_RANGE_TASK_PUSH_DURATION,
            &TIKV_LOAD_REGION_CACHE_DURATION,
            &TIKV_READ_REQUEST_BYTES,
            &TIKV_SEND_REQUEST_DURATION,
            &TIKV_SEND_REQUEST_BY_SOURCE,
            &TIKV_RPC_NET_LATENCY,
            &TIKV_BATCH_REQUEST_STAGE_DURATION,
            &TIKV_BATCH_STREAM_RECV_LOOP_DURATION,
            &TIKV_BATCH_STREAM_RECV_TAIL_LATENCY,
            &TIKV_BATCH_STREAM_TIKV_SEND_TAIL_LATENCY,
            &TIKV_BATCH_STREAM_CANCELED_ENTRY_TAIL_LATENCY,
            &TIKV_BATCH_SEND_LOOP_DURATION,
            &TIKV_BATCH_SEND_TAIL_LATENCY,
            &TIKV_BATCH_PENDING_REQUESTS,
            &TIKV_BATCH_REQUESTS,
            &TIKV_BATCH_HEAD_ARRIVAL_INTERVAL,
            &TIKV_BATCH_BEST_SIZE,
            &TIKV_BATCH_MORE_REQUESTS,
        ];
        assert_eq!(observers.len(), 21);
        for metric in observers {
            assert!(
                metrics.histogram_vec(metric.0).is_some()
                    || metrics.summary_vec(metric.0).is_some(),
                "{}",
                metric.0
            );
        }
    }

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
