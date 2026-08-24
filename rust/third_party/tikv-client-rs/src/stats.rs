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

#[cfg(test)]
pub(crate) fn range_task_stat(task_type: &'static str, result: &'static str) -> f64 {
    TIKV_RANGE_TASK_STATS
        .with_label_values(&[task_type, result])
        .get()
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
    static ref TIKV_BATCH_REQUEST_STAGE_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_batch_request_stage_duration",
        "Batch request stage duration breakdown by store and outcome",
        &["store", "stage", "result"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_RECV_LOOP_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_batch_stream_recv_loop_duration_seconds",
        "Batch stream receive loop duration breakdown by steps",
        &["target", "conn", "forward", "step"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_RECV_TAIL_LATENCY: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_batch_stream_recv_tail_latency_seconds",
            "Batch stream receive tail latency"
        )
        .buckets(prometheus::exponential_buckets(0.02, 2.0, 8).unwrap()),
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_TIKV_SEND_TAIL_LATENCY: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_batch_stream_tikv_send_tail_latency_seconds",
            "Tail latency from TiKV sending a batch response until client receipt"
        )
        .buckets(prometheus::exponential_buckets(0.01, 2.0, 8).unwrap()),
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_CANCELED_ENTRY_TAIL_LATENCY: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "tikv_batch_stream_canceled_entry_tail_latency_seconds",
            "Tail latency of cancelled entries that later receive responses"
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 8).unwrap()),
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_CLIENT_UNAVAILABLE: Histogram = register_histogram!(
        HistogramOpts::new(
            "tikv_batch_client_unavailable_seconds",
            "Time a BatchCommands client is unavailable while reconnecting"
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 28).unwrap())
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_TRACKED_REQUEST_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_batch_stream_tracked_request_count",
        "Count of requests tracked by each batch stream",
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_RETIRED_REQUEST_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_batch_stream_retired_request_count",
        "Count of requests retired from each batch stream",
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_COMPLETED_RESPONSE_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_batch_stream_completed_response_count",
        "Count of responses matched to tracked batch requests",
        &["target", "conn", "forward"]
    )
    .unwrap();
    static ref TIKV_BATCH_STREAM_OUTDATED_RESPONSE_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_batch_stream_outdated_response_count",
        "Count of responses for requests no longer tracked by a batch stream",
        &["target", "conn", "forward"]
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
