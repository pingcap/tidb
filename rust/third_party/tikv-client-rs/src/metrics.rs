// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Source-compatible metric definitions for client-go consumers.
//!
//! The registry is an owned value rather than mutable package globals. Creating
//! [`ClientGoMetrics`] corresponds to client-go's `InitMetrics`; calling
//! [`ClientGoMetrics::register_metrics`] corresponds to `RegisterMetrics`.

use prometheus::core::{Collector, Desc};
use prometheus::proto::{LabelPair, Metric, MetricFamily, MetricType, Summary};
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};

mod shortcuts;
pub use shortcuts::{ShortcutKind, ShortcutSpec, CLIENT_GO_SHORTCUT_SPECS};

/// The default namespace used by client-go.
pub const DEFAULT_NAMESPACE: &str = "tikv";
/// The default subsystem used by client-go.
pub const DEFAULT_SUBSYSTEM: &str = "client_go";

/// Source label constants exported by client-go's `metrics` package.
pub mod labels {
    /// Metric operation or event type.
    pub const TYPE: &str = "type";
    /// Operation result.
    pub const RESULT: &str = "result";
    /// TiKV store ID.
    pub const STORE: &str = "store";
    /// Transport target.
    pub const TARGET: &str = "target";
    /// Connection index.
    pub const CONN: &str = "conn";
    /// Forwarding marker.
    pub const FORWARD: &str = "forward";
    /// Commit command.
    pub const COMMIT: &str = "commit";
    /// Abort command.
    pub const ABORT: &str = "abort";
    /// Rollback command.
    pub const ROLLBACK: &str = "rollback";
    /// Batch-get command.
    pub const BATCH_GET: &str = "batch_get";
    /// Get command.
    pub const GET: &str = "get";
    /// Lock-keys command.
    pub const LOCK_KEYS: &str = "lock_keys";
    /// Shared-lock-keys command.
    pub const SHARED_LOCK_KEYS: &str = "shared_lock_keys";
    /// Batch receive-loop label.
    pub const BATCH_RECV_LOOP: &str = "batch-recv-loop";
    /// Batch send-loop label.
    pub const BATCH_SEND_LOOP: &str = "batch-send-loop";
    /// Network address.
    pub const ADDRESS: &str = "address";
    /// Forwarding source store.
    pub const FROM_STORE: &str = "from_store";
    /// Forwarding destination store.
    pub const TO_STORE: &str = "to_store";
    /// Stale-read marker.
    pub const STALE_READ: &str = "stale_read";
    /// Request source.
    pub const SOURCE: &str = "source";
    /// Internal/general scope.
    pub const SCOPE: &str = "scope";
    /// Internal scope value.
    pub const INTERNAL: &str = "internal";
    /// General scope value.
    pub const GENERAL: &str = "general";
    /// Traffic direction.
    pub const DIRECTION: &str = "direction";
    /// Event reason.
    pub const REASON: &str = "reason";
    /// Request stage.
    pub const STAGE: &str = "stage";
}

/// The source Prometheus collector type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricKind {
    /// A scalar counter.
    Counter,
    /// A counter vector.
    CounterVec,
    /// A scalar gauge.
    Gauge,
    /// A gauge vector.
    GaugeVec,
    /// A scalar histogram.
    Histogram,
    /// A histogram vector.
    HistogramVec,
    /// A summary vector without configured quantiles.
    SummaryVec,
}

/// Histogram bucket construction copied from the pinned source.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BucketSpec {
    /// This collector has no histogram buckets.
    None,
    /// Prometheus exponential buckets.
    Exponential {
        /// First bucket upper bound.
        start: f64,
        /// Multiplicative bucket factor.
        factor: f64,
        /// Number of finite buckets.
        count: usize,
    },
    /// Explicit finite bucket upper bounds.
    Explicit(&'static [f64]),
}

impl BucketSpec {
    fn values(self) -> prometheus::Result<Option<Vec<f64>>> {
        match self {
            Self::None => Ok(None),
            Self::Exponential {
                start,
                factor,
                count,
            } => prometheus::exponential_buckets(start, factor, count).map(Some),
            Self::Explicit(values) => Ok(Some(values.to_vec())),
        }
    }
}

/// Selects the subsystem used by one source collector.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MetricSubsystem {
    /// Use the subsystem passed to [`ClientGoMetrics::new`].
    Configured,
    /// Use client-go's fixed `sli` subsystem.
    Sli,
}

/// Exact source metadata for one declared metric global.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetricSpec {
    /// Go global name.
    pub source_name: &'static str,
    /// Source collector type.
    pub kind: MetricKind,
    /// Prometheus metric name before namespace/subsystem qualification.
    pub metric_name: &'static str,
    /// Prometheus help text.
    pub help: &'static str,
    /// Ordered variable-label names.
    pub labels: &'static [&'static str],
    /// Histogram bucket definition.
    pub buckets: BucketSpec,
    /// Configured or fixed-SLI subsystem selection.
    pub subsystem: MetricSubsystem,
    /// Whether the pinned source actually constructs this declared global.
    pub initialized: bool,
    /// Whether this vector participates in stale-store cleanup.
    pub store_scoped: bool,
}

macro_rules! metric_spec {
    ($source:literal, $kind:expr, $name:literal, $help:literal, $labels:expr, $buckets:expr, $subsystem:expr, $store:expr) => {
        MetricSpec {
            source_name: $source,
            kind: $kind,
            metric_name: $name,
            help: $help,
            labels: $labels,
            buckets: $buckets,
            subsystem: $subsystem,
            initialized: true,
            store_scoped: $store,
        }
    };
}

macro_rules! uninitialized_metric_spec {
    ($source:literal, $kind:expr) => {
        MetricSpec {
            source_name: $source,
            kind: $kind,
            metric_name: "",
            help: "",
            labels: &[],
            buckets: BucketSpec::None,
            subsystem: MetricSubsystem::Configured,
            initialized: false,
            store_scoped: false,
        }
    };
}

/// All 98 globals declared by pinned client-go `metrics/metrics.go`, in source order.
pub const CLIENT_GO_METRIC_SPECS: &[MetricSpec] = &[
    metric_spec!("TiKVTxnCmdHistogram", MetricKind::HistogramVec, "txn_cmd_duration_seconds", "Bucketed histogram of processing time of txn cmds.", &["type", "scope"], BucketSpec::Exponential { start: 0.0005, factor: 2.0, count: 29 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBackoffHistogram", MetricKind::HistogramVec, "backoff_seconds", "total backoff seconds of a single backoffer.", &["type"], BucketSpec::Exponential { start: 0.0005, factor: 2.0, count: 29 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVSendReqHistogram", MetricKind::HistogramVec, "request_seconds", "Bucketed histogram of sending request duration.", &["type", "store", "stale_read", "scope"], BucketSpec::Exponential { start: 0.0005, factor: 2.0, count: 24 }, MetricSubsystem::Configured, true),
    metric_spec!("TiKVSendReqBySourceSummary", MetricKind::SummaryVec, "source_request_seconds", "Summary of sending request with multi dimensions.", &["type", "store", "stale_read", "scope", "source"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVRPCNetLatencyHistogram", MetricKind::HistogramVec, "rpc_net_latency_seconds", "Bucketed histogram of time difference between TiDB and TiKV.", &["store", "scope"], BucketSpec::Exponential { start: 0.0001, factor: 2.0, count: 20 }, MetricSubsystem::Configured, true),
    metric_spec!("TiKVLockResolverCounter", MetricKind::CounterVec, "lock_resolver_actions_total", "Counter of lock resolver actions.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVLockResolverAsyncRunningTasks", MetricKind::GaugeVec, "lock_resolver_async_running_tasks", "The number of running async resolve lock tasks in lock resolver.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVRegionErrorCounter", MetricKind::CounterVec, "region_err_total", "Counter of region errors.", &["type", "store"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVRPCErrorCounter", MetricKind::CounterVec, "rpc_err_total", "Counter of rpc errors.", &["type", "store"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVTxnWriteKVCountHistogram", MetricKind::HistogramVec, "txn_write_kv_num", "Count of kv pairs to write in a transaction.", &["scope"], BucketSpec::Exponential { start: 1.0, factor: 4.0, count: 17 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnWriteSizeHistogram", MetricKind::HistogramVec, "txn_write_size_bytes", "Size of kv pairs to write in a transaction.", &["scope"], BucketSpec::Exponential { start: 16.0, factor: 4.0, count: 17 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVRawkvCmdHistogram", MetricKind::HistogramVec, "rawkv_cmd_seconds", "Bucketed histogram of processing time of rawkv cmds.", &["type"], BucketSpec::Exponential { start: 0.0005, factor: 2.0, count: 29 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVRawkvSizeHistogram", MetricKind::HistogramVec, "rawkv_kv_size_bytes", "Size of key/value to put, in bytes.", &["type"], BucketSpec::Exponential { start: 1.0, factor: 2.0, count: 30 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnRegionsNumHistogram", MetricKind::HistogramVec, "txn_regions_num", "Number of regions in a transaction.", &["type", "scope"], BucketSpec::Exponential { start: 1.0, factor: 2.0, count: 25 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVLoadTxnSafePointCounter", MetricKind::CounterVec, "load_safepoint_total", "Counter of load safepoint.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVSecondaryLockCleanupFailureCounter", MetricKind::CounterVec, "lock_cleanup_task_total", "failure statistic of secondary lock cleanup task.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVRegionCacheCounter", MetricKind::CounterVec, "region_cache_operations_total", "Counter of region cache.", &["type", "result"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVLoadRegionCounter", MetricKind::CounterVec, "load_region_total", "Counter of loading region.", &["type", "reason"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVLoadRegionCacheHistogram", MetricKind::HistogramVec, "load_region_cache_seconds", "Load region information duration", &["type"], BucketSpec::Exponential { start: 0.0001, factor: 2.0, count: 20 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVLocalLatchWaitTimeHistogram", MetricKind::Histogram, "local_latch_wait_seconds", "Wait time of a get local latch.", &[], BucketSpec::Exponential { start: 0.0005, factor: 2.0, count: 20 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVStatusDuration", MetricKind::HistogramVec, "kv_status_api_duration", "duration for kv status api.", &["store"], BucketSpec::Exponential { start: 0.0005, factor: 2.0, count: 20 }, MetricSubsystem::Configured, true),
    metric_spec!("TiKVStatusCounter", MetricKind::CounterVec, "kv_status_api_count", "Counter of access kv status api.", &["result"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchSendTailLatency", MetricKind::HistogramVec, "batch_send_tail_latency_seconds", "batch send tail latency", &["target"], BucketSpec::Exponential { start: 0.02, factor: 2.0, count: 8 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchSendLoopDuration", MetricKind::SummaryVec, "batch_send_loop_duration_seconds", "batch send loop duration breakdown by steps", &["target", "step"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchStreamRecvLoopDuration", MetricKind::SummaryVec, "batch_stream_recv_loop_duration_seconds", "batch stream recv loop duration breakdown by steps", &["target", "conn", "forward", "step"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchStreamRecvTailLatency", MetricKind::HistogramVec, "batch_stream_recv_tail_latency_seconds", "batch stream recv tail latency", &["target", "conn", "forward"], BucketSpec::Exponential { start: 0.02, factor: 2.0, count: 8 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchStreamTiKVSendTailLatency", MetricKind::HistogramVec, "batch_stream_tikv_send_tail_latency_seconds", "tail latency from TiKV sending a batch response until the client receives it", &["target", "conn", "forward"], BucketSpec::Exponential { start: 0.01, factor: 2.0, count: 8 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchStreamCanceledEntryTailLatency", MetricKind::HistogramVec, "batch_stream_canceled_entry_tail_latency_seconds", "tail latency of canceled entries that later receive responses on each batch stream", &["target", "conn", "forward"], BucketSpec::Exponential { start: 1.0, factor: 2.0, count: 8 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchStreamTrackedRequestCount", MetricKind::CounterVec, "batch_stream_tracked_request_count", "count of requests tracked by each batch stream", &["target", "conn", "forward"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchStreamRetiredRequestCount", MetricKind::CounterVec, "batch_stream_retired_request_count", "count of requests retired from each batch stream", &["target", "conn", "forward"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchStreamCompletedResponseCount", MetricKind::CounterVec, "batch_stream_completed_response_count", "count of matched responses completed on each batch stream", &["target", "conn", "forward"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchStreamOutdatedResponseCount", MetricKind::CounterVec, "batch_stream_outdated_response_count", "count of outdated responses received on each batch stream", &["target", "conn", "forward"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchHeadArrivalInterval", MetricKind::SummaryVec, "batch_head_arrival_interval_seconds", "arrival interval of the head request in batch", &["target"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchBestSize", MetricKind::SummaryVec, "batch_best_size", "best batch size estimated by the batch client", &["target"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchMoreRequests", MetricKind::SummaryVec, "batch_more_requests_total", "number of requests batched by extra fetch", &["target"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchWaitOverLoad", MetricKind::Counter, "batch_wait_overload", "event of tikv transport layer overload", &[], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchPendingRequests", MetricKind::HistogramVec, "batch_pending_requests", "number of requests pending in the batch channel", &["target"], BucketSpec::Exponential { start: 1.0, factor: 2.0, count: 11 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchRequests", MetricKind::HistogramVec, "batch_requests", "number of requests in one batch", &["target"], BucketSpec::Exponential { start: 1.0, factor: 2.0, count: 11 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchRequestStageDuration", MetricKind::SummaryVec, "batch_request_stage_duration_seconds", "batch request stage duration breakdown by store and outcome", &["store", "stage", "result"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVBatchClientUnavailable", MetricKind::Histogram, "batch_client_unavailable_seconds", "batch client unavailable", &[], BucketSpec::Exponential { start: 0.001, factor: 2.0, count: 28 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchClientWaitEstablish", MetricKind::Histogram, "batch_client_wait_connection_establish", "batch client wait new connection establish", &[], BucketSpec::Exponential { start: 0.001, factor: 2.0, count: 28 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBatchClientRecycle", MetricKind::Histogram, "batch_client_reset", "batch client recycle connection and reconnect duration", &[], BucketSpec::Exponential { start: 0.001, factor: 2.0, count: 28 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVRangeTaskStats", MetricKind::GaugeVec, "range_task_stats", "stat of range tasks", &["type", "result"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVRangeTaskPushDuration", MetricKind::HistogramVec, "range_task_push_duration", "duration to push sub tasks to range task workers", &["type"], BucketSpec::Exponential { start: 0.001, factor: 2.0, count: 20 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTokenWaitDuration", MetricKind::Histogram, "batch_executor_token_wait_duration", "tidb txn token wait duration to process batches", &[], BucketSpec::Exponential { start: 1.0, factor: 2.0, count: 34 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnHeartBeatHistogram", MetricKind::HistogramVec, "txn_heart_beat", "Bucketed histogram of the txn_heartbeat request duration.", &["type"], BucketSpec::Exponential { start: 0.001, factor: 2.0, count: 20 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTTLManagerHistogram", MetricKind::Histogram, "txn_ttl_manager", "Bucketed histogram of the txn ttl manager lifetime duration.", &[], BucketSpec::Exponential { start: 1.0, factor: 2.0, count: 20 }, MetricSubsystem::Configured, false),
    uninitialized_metric_spec!("TiKVPessimisticLockKeysDuration", MetricKind::Histogram),
    metric_spec!("TiKVTTLLifeTimeReachCounter", MetricKind::Counter, "ttl_lifetime_reach_total", "Counter of ttlManager live too long.", &[], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVNoAvailableConnectionCounter", MetricKind::Counter, "batch_client_no_available_connection_total", "Counter of no available batch client.", &[], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTwoPCTxnCounter", MetricKind::CounterVec, "commit_txn_counter", "Counter of 2PC transactions.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVAsyncCommitTxnCounter", MetricKind::CounterVec, "async_commit_txn_counter", "Counter of async commit transactions.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVOnePCTxnCounter", MetricKind::CounterVec, "one_pc_txn_counter", "Counter of 1PC transactions.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVStoreLimitErrorCounter", MetricKind::CounterVec, "get_store_limit_token_error", "store token is up to the limit, probably because one of the stores is the hotspot or unavailable", &["address", "store"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVGRPCConnTransientFailureCounter", MetricKind::CounterVec, "connection_transient_failure_count", "Counter of gRPC connection transient failure", &["address", "store"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVPanicCounter", MetricKind::CounterVec, "panic_total", "Counter of panic.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVForwardRequestCounter", MetricKind::CounterVec, "forward_request_counter", "Counter of tikv request being forwarded through another node", &["from_store", "to_store", "type", "result"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTSFutureWaitDuration", MetricKind::Histogram, "ts_future_wait_seconds", "Bucketed histogram of seconds cost for waiting timestamp future.", &[], BucketSpec::Exponential { start: 0.000005, factor: 2.0, count: 30 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVSafeTSUpdateCounter", MetricKind::CounterVec, "safets_update_counter", "Counter of tikv safe_ts being updated.", &["result", "store"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVMinSafeTSGapSeconds", MetricKind::GaugeVec, "min_safets_gap_seconds", "The minimal (non-zero) SafeTS gap for each store.", &["store"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVReplicaSelectorFailureCounter", MetricKind::CounterVec, "replica_selector_failure_counter", "Counter of the reason why the replica selector cannot yield a potential leader.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVRequestRetryTimesHistogram", MetricKind::Histogram, "request_retry_times", "Bucketed histogram of how many times a region request retries.", &[], BucketSpec::Explicit(&[1.0, 2.0, 3.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0]), MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnCommitBackoffSeconds", MetricKind::Histogram, "txn_commit_backoff_seconds", "Bucketed histogram of the total backoff duration in committing a transaction.", &[], BucketSpec::Exponential { start: 0.001, factor: 2.0, count: 22 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnCommitBackoffCount", MetricKind::Histogram, "txn_commit_backoff_count", "Bucketed histogram of the backoff count in committing a transaction.", &[], BucketSpec::Exponential { start: 1.0, factor: 2.0, count: 12 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVSmallReadDuration", MetricKind::Histogram, "tikv_small_read_duration", "Read time of TiKV small read.", &[], BucketSpec::Exponential { start: 0.0005, factor: 2.0, count: 28 }, MetricSubsystem::Sli, false),
    metric_spec!("TiKVReadThroughput", MetricKind::Histogram, "tikv_read_throughput", "Read throughput of TiKV read in Bytes/s.", &[], BucketSpec::Exponential { start: 1024.0, factor: 2.0, count: 13 }, MetricSubsystem::Sli, false),
    metric_spec!("TiKVUnsafeDestroyRangeFailuresCounterVec", MetricKind::CounterVec, "gc_unsafe_destroy_range_failures", "Counter of unsafe destroyrange failures", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVPrewriteAssertionUsageCounter", MetricKind::CounterVec, "prewrite_assertion_count", "Counter of assertions used in prewrite requests", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVGrpcConnectionState", MetricKind::GaugeVec, "grpc_connection_state", "State of gRPC connection", &["connection_id", "store_ip", "grpc_state"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVAggressiveLockedKeysCounter", MetricKind::CounterVec, "aggressive_locking_count", "Counter of keys locked in aggressive locking mode", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVStoreLivenessGauge", MetricKind::GaugeVec, "store_liveness_state", "Liveness state of each tikv", &["store"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVStoreSlowScoreGauge", MetricKind::GaugeVec, "store_slow_score", "Slow scores of each tikv node based on RPC timecosts", &["store"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVFeedbackSlowScoreGauge", MetricKind::GaugeVec, "feedback_slow_score", "Slow scores of each tikv node that is calculated by TiKV and sent to the client by health feedback", &["store"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVHealthFeedbackOpsCounter", MetricKind::CounterVec, "health_feedback_ops_counter", "Counter of operations about TiKV health feedback", &["scope", "type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVPreferLeaderFlowsGauge", MetricKind::GaugeVec, "prefer_leader_flows_gauge", "Counter of flows under PreferLeader mode.", &["type", "store"], BucketSpec::None, MetricSubsystem::Configured, true),
    metric_spec!("TiKVStaleReadCounter", MetricKind::CounterVec, "stale_read_counter", "Counter of stale read hit/miss", &["result"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVStaleReadReqCounter", MetricKind::CounterVec, "stale_read_req_counter", "Counter of stale read requests", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVStaleReadBytes", MetricKind::CounterVec, "stale_read_bytes", "Counter of stale read requests bytes", &["result", "direction"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVPipelinedFlushLenHistogram", MetricKind::Histogram, "pipelined_flush_len", "Bucketed histogram of length of pipelined flushed memdb", &[], BucketSpec::Exponential { start: 1000.0, factor: 2.0, count: 16 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVPipelinedFlushSizeHistogram", MetricKind::Histogram, "pipelined_flush_size", "Bucketed histogram of size of pipelined flushed memdb", &[], BucketSpec::Exponential { start: 16777216.0, factor: 1.2, count: 13 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVPipelinedFlushDuration", MetricKind::Histogram, "pipelined_flush_duration", "Flush time of pipelined memdb.", &[], BucketSpec::Exponential { start: 0.0005, factor: 2.0, count: 28 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVValidateReadTSFromPDCount", MetricKind::Counter, "validate_read_ts_from_pd_count", "Counter of validating read ts by getting a timestamp from PD", &[], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVLowResolutionTSOUpdateIntervalSecondsGauge", MetricKind::Gauge, "low_resolution_tso_update_interval_seconds", "The actual working update interval for the low resolution TSO. As there are adaptive mechanism internally, this value may differ from the config.", &[], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVStaleRegionFromPDCounter", MetricKind::Counter, "stale_region_from_pd", "Counter of stale region from PD", &[], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVBucketClampedCounter", MetricKind::Counter, "bucket_clamped", "Counter of bucket boundaries clamped to region boundaries", &[], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVStaleBucketFromPDCounter", MetricKind::Counter, "stale_bucket_from_pd", "Counter of stale bucket from PD", &[], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVPipelinedFlushThrottleSecondsHistogram", MetricKind::Histogram, "pipelined_flush_throttle_seconds", "Throttle durations of pipelined flushes.", &[], BucketSpec::Exponential { start: 0.0005, factor: 2.0, count: 28 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnWriteConflictCounter", MetricKind::Counter, "txn_write_conflict_counter", "Counter of txn write conflict", &[], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVAsyncSendReqCounter", MetricKind::CounterVec, "async_send_req_total", "Counter of async send req by region request sender.", &["result"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVAsyncBatchGetCounter", MetricKind::CounterVec, "async_batch_get_total", "Counter of async batch get by txn snapshot.", &["result"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVReadRequestBytes", MetricKind::SummaryVec, "read_request_bytes", "Summary of read requests bytes", &["type", "result"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnLagCommitTSWaitHistogram", MetricKind::HistogramVec, "txn_lag_commit_ts_wait_seconds", "Bucketed histogram of seconds waiting commit TSO lag.", &["result"], BucketSpec::Exponential { start: 0.0005, factor: 2.0, count: 16 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnLagCommitTSAttemptHistogram", MetricKind::HistogramVec, "txn_lag_commit_ts_attempt_count", "Bucketed histogram of attempts to get the lagging TSO in one commit", &["result"], BucketSpec::Exponential { start: 1.0, factor: 2.0, count: 6 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnFileRequestCounter", MetricKind::CounterVec, "txn_file_requests", "Counter of file-based transactions requests.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnFileErrorCounter", MetricKind::CounterVec, "txn_file_errors", "Counter of file-based transaction errors.", &["type"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnFileWriteBytes", MetricKind::CounterVec, "txn_file_write_bytes", "Counter of file-based transactions write bytes.", &["scope"], BucketSpec::None, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnFileMutationSizeHistogram", MetricKind::HistogramVec, "txn_file_mutation_size", "Histogram of file-based transactions mutation bytes.", &["scope"], BucketSpec::Exponential { start: 1048576.0, factor: 2.0, count: 17 }, MetricSubsystem::Configured, false),
    metric_spec!("TiKVTxnFileDuration", MetricKind::HistogramVec, "txn_file_duration", "Duration of executing file-based transactions.", &["scope"], BucketSpec::Exponential { start: 0.001, factor: 2.0, count: 20 }, MetricSubsystem::Configured, false),
];

#[derive(Clone, Copy, Debug, Default)]
struct SummaryValue {
    count: u64,
    sum: f64,
}

#[derive(Debug)]
struct SummaryVecInner {
    desc: Desc,
    children: Mutex<BTreeMap<Vec<String>, Arc<Mutex<SummaryValue>>>>,
}

/// A no-quantile Prometheus summary vector matching client-go's summary use.
///
/// `rust-prometheus` does not provide a summary collector. The client-go
/// package does not configure quantiles, so this implementation retains the
/// exact summary count/sum exposition without inventing histogram buckets.
#[derive(Clone)]
pub struct SummaryVec {
    inner: Arc<SummaryVecInner>,
}

impl fmt::Debug for SummaryVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SummaryVec")
            .field("name", &self.inner.desc.fq_name)
            .field("labels", &self.inner.desc.variable_labels)
            .finish_non_exhaustive()
    }
}

impl SummaryVec {
    fn new(opts: Opts, variable_labels: &[&str]) -> prometheus::Result<Self> {
        let desc = Desc::new(
            opts.fq_name(),
            opts.help,
            variable_labels
                .iter()
                .map(|label| (*label).to_owned())
                .collect(),
            opts.const_labels,
        )?;
        Ok(Self {
            inner: Arc::new(SummaryVecInner {
                desc,
                children: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// Returns the child for the ordered variable-label values.
    pub fn with_label_values(&self, values: &[&str]) -> prometheus::Result<SummaryObserver> {
        if values.len() != self.inner.desc.variable_labels.len() {
            return Err(prometheus::Error::InconsistentCardinality {
                expect: self.inner.desc.variable_labels.len(),
                got: values.len(),
            });
        }
        let key: Vec<String> = values.iter().map(|value| (*value).to_owned()).collect();
        let state = self
            .inner
            .children
            .lock()
            .expect("summary vector lock poisoned")
            .entry(key)
            .or_default()
            .clone();
        Ok(SummaryObserver { state })
    }

    fn remove_label_values(&self, values: &[&str]) -> prometheus::Result<()> {
        if values.len() != self.inner.desc.variable_labels.len() {
            return Err(prometheus::Error::InconsistentCardinality {
                expect: self.inner.desc.variable_labels.len(),
                got: values.len(),
            });
        }
        let key: Vec<String> = values.iter().map(|value| (*value).to_owned()).collect();
        self.inner
            .children
            .lock()
            .expect("summary vector lock poisoned")
            .remove(&key)
            .ok_or_else(|| prometheus::Error::Msg("summary label values not found".to_owned()))?;
        Ok(())
    }
}

impl Collector for SummaryVec {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.inner.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let children = self
            .inner
            .children
            .lock()
            .expect("summary vector lock poisoned");
        if children.is_empty() {
            return Vec::new();
        }

        let mut family = MetricFamily::default();
        family.set_name(self.inner.desc.fq_name.clone());
        family.set_help(self.inner.desc.help.clone());
        family.set_field_type(MetricType::SUMMARY);
        for (values, state) in children.iter() {
            let mut pairs = self.inner.desc.const_label_pairs.clone();
            pairs.extend(
                self.inner
                    .desc
                    .variable_labels
                    .iter()
                    .zip(values)
                    .map(|(name, value)| label_pair(name, value)),
            );
            pairs.sort();

            let state = *state.lock().expect("summary child lock poisoned");
            let mut summary = Summary::default();
            summary.set_sample_count(state.count);
            summary.set_sample_sum(state.sum);
            let mut metric = Metric::default();
            set_metric_labels(&mut metric, pairs);
            metric.set_summary(summary);
            family.mut_metric().push(metric);
        }
        vec![family]
    }
}

fn label_pair(name: &str, value: &str) -> LabelPair {
    let mut pair = LabelPair::default();
    pair.set_name(name.to_owned());
    pair.set_value(value.to_owned());
    pair
}

fn set_metric_labels(metric: &mut Metric, labels: Vec<LabelPair>) {
    metric.set_label(labels.into());
}

/// One bound child of a [`SummaryVec`].
#[derive(Clone, Debug)]
pub struct SummaryObserver {
    state: Arc<Mutex<SummaryValue>>,
}

impl SummaryObserver {
    /// Adds one observation to the summary count and sum.
    pub fn observe(&self, value: f64) {
        let mut state = self.state.lock().expect("summary child lock poisoned");
        state.count = state.count.wrapping_add(1);
        state.sum += value;
    }

    /// Returns the current sample count.
    pub fn sample_count(&self) -> u64 {
        self.state
            .lock()
            .expect("summary child lock poisoned")
            .count
    }

    /// Prometheus-style alias for [`Self::sample_count`].
    pub fn get_sample_count(&self) -> u64 {
        self.sample_count()
    }

    /// Returns the current sample sum.
    pub fn sample_sum(&self) -> f64 {
        self.state.lock().expect("summary child lock poisoned").sum
    }

    /// Prometheus-style alias for [`Self::sample_sum`].
    pub fn get_sample_sum(&self) -> f64 {
        self.sample_sum()
    }
}

/// One initialized collector in a [`ClientGoMetrics`] registry.
#[derive(Clone)]
pub enum ClientGoCollector {
    /// Scalar counter.
    Counter(Counter),
    /// Counter vector.
    CounterVec(CounterVec),
    /// Scalar gauge.
    Gauge(Gauge),
    /// Gauge vector.
    GaugeVec(GaugeVec),
    /// Scalar histogram.
    Histogram(Histogram),
    /// Histogram vector.
    HistogramVec(HistogramVec),
    /// No-quantile summary vector.
    SummaryVec(SummaryVec),
}

impl fmt::Debug for ClientGoCollector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Counter(_) => "Counter",
            Self::CounterVec(_) => "CounterVec",
            Self::Gauge(_) => "Gauge",
            Self::GaugeVec(_) => "GaugeVec",
            Self::Histogram(_) => "Histogram",
            Self::HistogramVec(_) => "HistogramVec",
            Self::SummaryVec(_) => "SummaryVec",
        })
    }
}

impl ClientGoCollector {
    fn boxed_collector(&self) -> Box<dyn Collector> {
        match self {
            Self::Counter(metric) => Box::new(metric.clone()),
            Self::CounterVec(metric) => Box::new(metric.clone()),
            Self::Gauge(metric) => Box::new(metric.clone()),
            Self::GaugeVec(metric) => Box::new(metric.clone()),
            Self::Histogram(metric) => Box::new(metric.clone()),
            Self::HistogramVec(metric) => Box::new(metric.clone()),
            Self::SummaryVec(metric) => Box::new(metric.clone()),
        }
    }

    fn collect(&self) -> Vec<MetricFamily> {
        match self {
            Self::Counter(metric) => metric.collect(),
            Self::CounterVec(metric) => metric.collect(),
            Self::Gauge(metric) => metric.collect(),
            Self::GaugeVec(metric) => metric.collect(),
            Self::Histogram(metric) => metric.collect(),
            Self::HistogramVec(metric) => metric.collect(),
            Self::SummaryVec(metric) => metric.collect(),
        }
    }
}

fn configured_subsystem<'a>(spec: &MetricSpec, subsystem: &'a str) -> &'a str {
    match spec.subsystem {
        MetricSubsystem::Configured => subsystem,
        MetricSubsystem::Sli => "sli",
    }
}

fn collector_opts(
    spec: &MetricSpec,
    namespace: &str,
    subsystem: &str,
    const_labels: &HashMap<String, String>,
) -> Opts {
    Opts::new(spec.metric_name, spec.help)
        .namespace(namespace)
        .subsystem(configured_subsystem(spec, subsystem))
        .const_labels(const_labels.clone())
}

fn histogram_opts(
    spec: &MetricSpec,
    namespace: &str,
    subsystem: &str,
    const_labels: &HashMap<String, String>,
) -> prometheus::Result<HistogramOpts> {
    let mut opts = HistogramOpts::new(spec.metric_name, spec.help)
        .namespace(namespace)
        .subsystem(configured_subsystem(spec, subsystem))
        .const_labels(const_labels.clone());
    if let Some(buckets) = spec.buckets.values()? {
        opts = opts.buckets(buckets);
    }
    Ok(opts)
}

fn create_collector(
    spec: &MetricSpec,
    namespace: &str,
    subsystem: &str,
    const_labels: &HashMap<String, String>,
) -> prometheus::Result<ClientGoCollector> {
    let collector = match spec.kind {
        MetricKind::Counter => ClientGoCollector::Counter(Counter::with_opts(collector_opts(
            spec,
            namespace,
            subsystem,
            const_labels,
        ))?),
        MetricKind::CounterVec => ClientGoCollector::CounterVec(CounterVec::new(
            collector_opts(spec, namespace, subsystem, const_labels),
            spec.labels,
        )?),
        MetricKind::Gauge => ClientGoCollector::Gauge(Gauge::with_opts(collector_opts(
            spec,
            namespace,
            subsystem,
            const_labels,
        ))?),
        MetricKind::GaugeVec => ClientGoCollector::GaugeVec(GaugeVec::new(
            collector_opts(spec, namespace, subsystem, const_labels),
            spec.labels,
        )?),
        MetricKind::Histogram => ClientGoCollector::Histogram(Histogram::with_opts(
            histogram_opts(spec, namespace, subsystem, const_labels)?,
        )?),
        MetricKind::HistogramVec => ClientGoCollector::HistogramVec(HistogramVec::new(
            histogram_opts(spec, namespace, subsystem, const_labels)?,
            spec.labels,
        )?),
        MetricKind::SummaryVec => ClientGoCollector::SummaryVec(SummaryVec::new(
            collector_opts(spec, namespace, subsystem, const_labels),
            spec.labels,
        )?),
    };
    Ok(collector)
}

/// A shortcut observer backed by either a histogram or source-exact summary.
#[derive(Clone, Debug)]
pub enum ClientGoObserver {
    /// Histogram child.
    Histogram(Histogram),
    /// Summary child.
    Summary(SummaryObserver),
}

impl ClientGoObserver {
    /// Records one observation.
    pub fn observe(&self, value: f64) {
        match self {
            Self::Histogram(observer) => observer.observe(value),
            Self::Summary(observer) => observer.observe(value),
        }
    }

    /// Returns the current sample count.
    pub fn sample_count(&self) -> u64 {
        match self {
            Self::Histogram(observer) => observer.get_sample_count(),
            Self::Summary(observer) => observer.sample_count(),
        }
    }

    /// Prometheus-style alias for [`Self::sample_count`].
    pub fn get_sample_count(&self) -> u64 {
        self.sample_count()
    }

    /// Returns the current sample sum.
    pub fn sample_sum(&self) -> f64 {
        match self {
            Self::Histogram(observer) => observer.get_sample_sum(),
            Self::Summary(observer) => observer.sample_sum(),
        }
    }

    /// Prometheus-style alias for [`Self::sample_sum`].
    pub fn get_sample_sum(&self) -> f64 {
        self.sample_sum()
    }
}

/// One initialized pre-bound shortcut from `metrics/shortcuts.go`.
#[derive(Clone, Debug)]
pub enum ClientGoShortcut {
    /// Histogram or summary observer.
    Observer(ClientGoObserver),
    /// Counter child.
    Counter(Counter),
    /// Gauge child.
    Gauge(Gauge),
}

fn shortcut_type_error(spec: &ShortcutSpec) -> prometheus::Error {
    prometheus::Error::Msg(format!(
        "source shortcut {} does not match parent {}",
        spec.source_name,
        spec.metric_source_name.unwrap_or("<nil>")
    ))
}

fn bind_shortcut(
    spec: &ShortcutSpec,
    collectors: &HashMap<&'static str, ClientGoCollector>,
) -> prometheus::Result<Option<ClientGoShortcut>> {
    let Some(metric_source_name) = spec.metric_source_name else {
        return Ok(None);
    };
    let collector = collectors
        .get(metric_source_name)
        .ok_or_else(|| shortcut_type_error(spec))?;
    let shortcut = match (spec.kind, collector) {
        (ShortcutKind::Observer, ClientGoCollector::HistogramVec(metric)) => {
            ClientGoShortcut::Observer(ClientGoObserver::Histogram(
                metric.get_metric_with_label_values(spec.label_values)?,
            ))
        }
        (ShortcutKind::Observer, ClientGoCollector::SummaryVec(metric)) => {
            ClientGoShortcut::Observer(ClientGoObserver::Summary(
                metric.with_label_values(spec.label_values)?,
            ))
        }
        (ShortcutKind::Counter, ClientGoCollector::CounterVec(metric)) => {
            ClientGoShortcut::Counter(metric.get_metric_with_label_values(spec.label_values)?)
        }
        (ShortcutKind::Gauge, ClientGoCollector::GaugeVec(metric)) => {
            ClientGoShortcut::Gauge(metric.get_metric_with_label_values(spec.label_values)?)
        }
        _ => return Err(shortcut_type_error(spec)),
    };
    Ok(Some(shortcut))
}

/// An owned initialization of every metric global and shortcut in the package.
#[derive(Debug)]
pub struct ClientGoMetrics {
    namespace: String,
    subsystem: String,
    const_labels: HashMap<String, String>,
    collectors: HashMap<&'static str, ClientGoCollector>,
    shortcuts: HashMap<&'static str, ClientGoShortcut>,
}

impl ClientGoMetrics {
    /// Constructs all source-initialized collectors and shortcuts.
    ///
    /// The two SLI metrics retain client-go's fixed `sli` subsystem. The
    /// declared-but-uninitialized source globals remain absent from their
    /// corresponding lookup methods.
    pub fn new(
        namespace: impl Into<String>,
        subsystem: impl Into<String>,
        const_labels: HashMap<String, String>,
    ) -> prometheus::Result<Self> {
        let namespace = namespace.into();
        let subsystem = subsystem.into();
        let mut collectors = HashMap::with_capacity(97);
        for spec in CLIENT_GO_METRIC_SPECS {
            if spec.initialized {
                collectors.insert(
                    spec.source_name,
                    create_collector(spec, &namespace, &subsystem, &const_labels)?,
                );
            }
        }

        let mut shortcuts = HashMap::with_capacity(149);
        for spec in CLIENT_GO_SHORTCUT_SPECS {
            if let Some(shortcut) = bind_shortcut(spec, &collectors)? {
                shortcuts.insert(spec.source_name, shortcut);
            }
        }

        Ok(Self {
            namespace,
            subsystem,
            const_labels,
            collectors,
            shortcuts,
        })
    }

    /// Constructs the package's `tikv`/`client_go` default initialization.
    pub fn new_default() -> prometheus::Result<Self> {
        Self::new(DEFAULT_NAMESPACE, DEFAULT_SUBSYSTEM, HashMap::new())
    }

    /// Returns the configured namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the configured non-SLI subsystem.
    pub fn subsystem(&self) -> &str {
        &self.subsystem
    }

    /// Returns the constant labels copied into every initialized collector.
    pub fn const_labels(&self) -> &HashMap<String, String> {
        &self.const_labels
    }

    /// Returns an initialized collector by its Go global name.
    pub fn collector(&self, source_name: &str) -> Option<&ClientGoCollector> {
        self.collectors.get(source_name)
    }

    /// Returns a scalar counter by its Go global name.
    pub fn counter(&self, source_name: &str) -> Option<&Counter> {
        match self.collector(source_name) {
            Some(ClientGoCollector::Counter(metric)) => Some(metric),
            _ => None,
        }
    }

    /// Returns a counter vector by its Go global name.
    pub fn counter_vec(&self, source_name: &str) -> Option<&CounterVec> {
        match self.collector(source_name) {
            Some(ClientGoCollector::CounterVec(metric)) => Some(metric),
            _ => None,
        }
    }

    /// Returns a scalar gauge by its Go global name.
    pub fn gauge(&self, source_name: &str) -> Option<&Gauge> {
        match self.collector(source_name) {
            Some(ClientGoCollector::Gauge(metric)) => Some(metric),
            _ => None,
        }
    }

    /// Returns a gauge vector by its Go global name.
    pub fn gauge_vec(&self, source_name: &str) -> Option<&GaugeVec> {
        match self.collector(source_name) {
            Some(ClientGoCollector::GaugeVec(metric)) => Some(metric),
            _ => None,
        }
    }

    /// Returns a scalar histogram by its Go global name.
    pub fn histogram(&self, source_name: &str) -> Option<&Histogram> {
        match self.collector(source_name) {
            Some(ClientGoCollector::Histogram(metric)) => Some(metric),
            _ => None,
        }
    }

    /// Returns a histogram vector by its Go global name.
    pub fn histogram_vec(&self, source_name: &str) -> Option<&HistogramVec> {
        match self.collector(source_name) {
            Some(ClientGoCollector::HistogramVec(metric)) => Some(metric),
            _ => None,
        }
    }

    /// Returns a summary vector by its Go global name.
    pub fn summary_vec(&self, source_name: &str) -> Option<&SummaryVec> {
        match self.collector(source_name) {
            Some(ClientGoCollector::SummaryVec(metric)) => Some(metric),
            _ => None,
        }
    }

    /// Returns an initialized shortcut by its Go global name.
    pub fn shortcut(&self, source_name: &str) -> Option<&ClientGoShortcut> {
        self.shortcuts.get(source_name)
    }

    /// Registers all 97 source-registered collectors in source order.
    pub fn register_metrics(&self, registry: &Registry) -> prometheus::Result<()> {
        for spec in CLIENT_GO_METRIC_SPECS {
            if let Some(collector) = self.collectors.get(spec.source_name) {
                registry.register(collector.boxed_collector())?;
            }
        }
        Ok(())
    }

    /// Registers all metrics and panics on an invalid or duplicate descriptor.
    pub fn must_register_metrics(&self, registry: &Registry) {
        self.register_metrics(registry)
            .expect("client-go metric registration failed");
    }

    /// Returns one vector handle by its Go global name.
    pub fn metric_vec(&self, source_name: &str) -> Option<MetricVecHandle> {
        let spec = metric_spec_by_source_name(source_name)?;
        let collector = self.collector(source_name)?;
        match collector {
            ClientGoCollector::CounterVec(_)
            | ClientGoCollector::GaugeVec(_)
            | ClientGoCollector::HistogramVec(_)
            | ClientGoCollector::SummaryVec(_) => Some(MetricVecHandle {
                spec,
                collector: collector.clone(),
            }),
            _ => None,
        }
    }

    /// Returns the 15 vectors client-go tracks for stale-store cleanup.
    pub fn store_metric_vec_list(&self) -> Vec<MetricVecHandle> {
        CLIENT_GO_METRIC_SPECS
            .iter()
            .filter(|spec| spec.store_scoped)
            .map(|spec| {
                self.metric_vec(spec.source_name)
                    .expect("store metric spec must describe an initialized vector")
            })
            .collect()
    }

    /// Returns transaction commit counts for 2PC, async commit, and 1PC.
    pub fn get_txn_commit_counter(&self) -> TxnCommitCounter {
        TxnCommitCounter {
            two_pc: self.shortcut_counter_value("TwoPCTxnCounterOk"),
            async_commit: self.shortcut_counter_value("AsyncCommitTxnCounterOk"),
            one_pc: self.shortcut_counter_value("OnePCTxnCounterOk"),
        }
    }

    fn shortcut_counter_value(&self, source_name: &str) -> i64 {
        match self.shortcut(source_name) {
            Some(ClientGoShortcut::Counter(counter)) => counter.get() as i64,
            _ => -1,
        }
    }

    /// Applies client-go's small-read versus throughput SLI classification.
    pub fn observe_read_sli(&self, read_keys: u64, read_time: f64, read_size: f64) {
        if read_keys == 0 || read_time == 0.0 {
            return;
        }
        if read_keys <= 20 && read_size < 1024.0 * 1024.0 {
            self.histogram("TiKVSmallReadDuration")
                .expect("small-read histogram must be initialized")
                .observe(read_time);
        } else {
            self.histogram("TiKVReadThroughput")
                .expect("read-throughput histogram must be initialized")
                .observe(read_size / read_time);
        }
    }
}

impl Default for ClientGoMetrics {
    fn default() -> Self {
        Self::new_default().expect("the pinned default metric definitions are valid")
    }
}

lazy_static::lazy_static! {
    static ref GLOBAL_CLIENT_GO_METRICS: RwLock<Arc<ClientGoMetrics>> =
        RwLock::new(Arc::new(ClientGoMetrics::default()));
}

/// Returns the process-wide metrics initialization used by client operations.
pub fn global_metrics() -> Arc<ClientGoMetrics> {
    GLOBAL_CLIENT_GO_METRICS
        .read()
        .expect("global client-go metrics lock poisoned")
        .clone()
}

/// Reinitializes process-wide metrics with a namespace and subsystem.
pub fn init_metrics(
    namespace: impl Into<String>,
    subsystem: impl Into<String>,
) -> prometheus::Result<()> {
    init_metrics_with_const_labels(namespace, subsystem, HashMap::new())
}

/// Reinitializes process-wide metrics with namespace, subsystem, and constant labels.
pub fn init_metrics_with_const_labels(
    namespace: impl Into<String>,
    subsystem: impl Into<String>,
    const_labels: HashMap<String, String>,
) -> prometheus::Result<()> {
    let metrics = Arc::new(ClientGoMetrics::new(namespace, subsystem, const_labels)?);
    *GLOBAL_CLIENT_GO_METRICS
        .write()
        .expect("global client-go metrics lock poisoned") = metrics;
    Ok(())
}

/// Tries to register the current process-wide metrics in Prometheus's default registry.
pub fn try_register_metrics() -> prometheus::Result<()> {
    global_metrics().register_metrics(prometheus::default_registry())
}

/// Registers the current process-wide metrics and panics on failure.
///
/// This preserves client-go's `RegisterMetrics`/`MustRegister` contract. Use
/// [`try_register_metrics`] when a native Rust caller needs a recoverable error.
pub fn register_metrics() {
    try_register_metrics().expect("client-go metric registration failed");
}

/// Returns the process-wide list of vectors subject to stale-store cleanup.
pub fn get_store_metric_vec_list() -> Vec<MetricVecHandle> {
    global_metrics().store_metric_vec_list()
}

/// Returns the process-wide transaction commit counter snapshot.
pub fn get_txn_commit_counter() -> TxnCommitCounter {
    global_metrics().get_txn_commit_counter()
}

/// Applies client-go's read-SLI classification to the process-wide metrics.
pub fn observe_read_sli(read_keys: u64, read_time: f64, read_size: f64) {
    global_metrics().observe_read_sli(read_keys, read_time, read_size);
}

/// Finds exact metric metadata by Go global name.
pub fn metric_spec_by_source_name(source_name: &str) -> Option<&'static MetricSpec> {
    CLIENT_GO_METRIC_SPECS
        .iter()
        .find(|spec| spec.source_name == source_name)
}

/// Finds exact shortcut metadata by Go global name.
pub fn shortcut_spec_by_source_name(source_name: &str) -> Option<&'static ShortcutSpec> {
    CLIENT_GO_SHORTCUT_SPECS
        .iter()
        .find(|spec| spec.source_name == source_name)
}

/// Source-shaped transaction commit protocol counters.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize,
)]
pub struct TxnCommitCounter {
    /// Successful 2PC transactions.
    #[serde(rename = "twoPC")]
    pub two_pc: i64,
    /// Successful async-commit transactions.
    #[serde(rename = "asyncCommit")]
    pub async_commit: i64,
    /// Successful 1PC transactions.
    #[serde(rename = "onePC")]
    pub one_pc: i64,
}

impl TxnCommitCounter {
    /// Returns the component-wise difference from an earlier counter snapshot.
    pub fn subtract(self, rhs: Self) -> Self {
        Self {
            two_pc: self.two_pc - rhs.two_pc,
            async_commit: self.async_commit - rhs.async_commit,
            one_pc: self.one_pc - rhs.one_pc,
        }
    }
}

/// A cloneable handle for one metric vector.
#[derive(Clone, Debug)]
pub struct MetricVecHandle {
    spec: &'static MetricSpec,
    collector: ClientGoCollector,
}

impl MetricVecHandle {
    /// Returns the source metric global name.
    pub fn source_name(&self) -> &'static str {
        self.spec.source_name
    }

    /// Returns the ordered variable-label names.
    pub fn labels(&self) -> &'static [&'static str] {
        self.spec.labels
    }

    /// Collects this vector's current Prometheus metric families.
    pub fn collect(&self) -> Vec<MetricFamily> {
        self.collector.collect()
    }

    /// Deletes every child whose variable labels contain all given pairs.
    pub fn delete_partial_match(&self, labels: &[(&str, &str)]) -> usize {
        if labels
            .iter()
            .any(|(name, _)| !self.spec.labels.contains(name))
        {
            return 0;
        }

        let mut matches = BTreeSet::new();
        for family in self.collect() {
            for metric in family.get_metric() {
                if labels.iter().all(|(name, value)| {
                    metric
                        .get_label()
                        .iter()
                        .any(|pair| pair.get_name() == *name && pair.get_value() == *value)
                }) {
                    let values = self
                        .spec
                        .labels
                        .iter()
                        .map(|name| {
                            metric
                                .get_label()
                                .iter()
                                .find(|pair| pair.get_name() == *name)
                                .map(|pair| pair.get_value().to_owned())
                        })
                        .collect::<Option<Vec<_>>>();
                    if let Some(values) = values {
                        matches.insert(values);
                    }
                }
            }
        }

        matches
            .into_iter()
            .filter(|values| {
                let borrowed: Vec<&str> = values.iter().map(String::as_str).collect();
                self.remove_label_values(&borrowed).is_ok()
            })
            .count()
    }

    fn remove_label_values(&self, values: &[&str]) -> prometheus::Result<()> {
        match &self.collector {
            ClientGoCollector::CounterVec(metric) => metric.remove_label_values(values),
            ClientGoCollector::GaugeVec(metric) => metric.remove_label_values(values),
            ClientGoCollector::HistogramVec(metric) => metric.remove_label_values(values),
            ClientGoCollector::SummaryVec(metric) => metric.remove_label_values(values),
            _ => Err(prometheus::Error::Msg(
                "scalar collector cannot be used as a metric vector".to_owned(),
            )),
        }
    }
}

/// Finds one nonzero store ID still tracked by a vector but absent from `valid_store_ids`.
pub fn find_next_stale_store_id(
    collector: &MetricVecHandle,
    valid_store_ids: &HashSet<u64>,
) -> u64 {
    for family in collector.collect() {
        for metric in family.get_metric() {
            let Some(store_id) = metric
                .get_label()
                .iter()
                .find(|pair| pair.get_name() == labels::STORE)
                .and_then(|pair| pair.get_value().parse::<u64>().ok())
            else {
                continue;
            };
            if store_id != 0 && !valid_store_ids.contains(&store_id) {
                return store_id;
            }
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn child_values(labels: &[&str], store_id: &str) -> Vec<String> {
        labels
            .iter()
            .enumerate()
            .map(|(index, label)| {
                if *label == labels::STORE {
                    store_id.to_owned()
                } else {
                    format!("value-{index}")
                }
            })
            .collect()
    }

    fn exercise_collector(spec: &MetricSpec, collector: &ClientGoCollector) {
        let values = child_values(spec.labels, "7");
        let values: Vec<&str> = values.iter().map(String::as_str).collect();
        match collector {
            ClientGoCollector::Counter(metric) => metric.inc(),
            ClientGoCollector::CounterVec(metric) => {
                metric.get_metric_with_label_values(&values).unwrap().inc()
            }
            ClientGoCollector::Gauge(metric) => metric.set(3.0),
            ClientGoCollector::GaugeVec(metric) => metric
                .get_metric_with_label_values(&values)
                .unwrap()
                .set(3.0),
            ClientGoCollector::Histogram(metric) => metric.observe(3.0),
            ClientGoCollector::HistogramVec(metric) => metric
                .get_metric_with_label_values(&values)
                .unwrap()
                .observe(3.0),
            ClientGoCollector::SummaryVec(metric) => {
                metric.with_label_values(&values).unwrap().observe(3.0)
            }
        }
    }

    fn expected_metric_type(kind: MetricKind) -> MetricType {
        match kind {
            MetricKind::Counter | MetricKind::CounterVec => MetricType::COUNTER,
            MetricKind::Gauge | MetricKind::GaugeVec => MetricType::GAUGE,
            MetricKind::Histogram | MetricKind::HistogramVec => MetricType::HISTOGRAM,
            MetricKind::SummaryVec => MetricType::SUMMARY,
        }
    }

    fn expected_name(spec: &MetricSpec, namespace: &str, subsystem: &str) -> String {
        let subsystem = match spec.subsystem {
            MetricSubsystem::Configured => subsystem,
            MetricSubsystem::Sli => "sli",
        };
        [namespace, subsystem, spec.metric_name]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("_")
    }

    #[test]
    fn source_inventory_is_atomic_unique_and_preserves_nil_globals() {
        assert_eq!(CLIENT_GO_METRIC_SPECS.len(), 98);
        assert_eq!(
            CLIENT_GO_METRIC_SPECS
                .iter()
                .filter(|spec| spec.initialized)
                .count(),
            97
        );
        assert_eq!(
            CLIENT_GO_METRIC_SPECS
                .iter()
                .filter(|spec| !spec.initialized)
                .map(|spec| spec.source_name)
                .collect::<Vec<_>>(),
            ["TiKVPessimisticLockKeysDuration"]
        );
        assert_eq!(
            CLIENT_GO_METRIC_SPECS
                .iter()
                .filter(|spec| spec.store_scoped)
                .map(|spec| spec.source_name)
                .collect::<Vec<_>>(),
            [
                "TiKVSendReqHistogram",
                "TiKVSendReqBySourceSummary",
                "TiKVRPCNetLatencyHistogram",
                "TiKVRegionErrorCounter",
                "TiKVRPCErrorCounter",
                "TiKVStatusDuration",
                "TiKVBatchRequestStageDuration",
                "TiKVStoreLimitErrorCounter",
                "TiKVGRPCConnTransientFailureCounter",
                "TiKVSafeTSUpdateCounter",
                "TiKVMinSafeTSGapSeconds",
                "TiKVStoreLivenessGauge",
                "TiKVStoreSlowScoreGauge",
                "TiKVFeedbackSlowScoreGauge",
                "TiKVPreferLeaderFlowsGauge",
            ]
        );

        let metric_names: HashSet<_> = CLIENT_GO_METRIC_SPECS
            .iter()
            .map(|spec| spec.source_name)
            .collect();
        assert_eq!(metric_names.len(), CLIENT_GO_METRIC_SPECS.len());
        let registered_names: HashSet<_> = CLIENT_GO_METRIC_SPECS
            .iter()
            .filter(|spec| spec.initialized)
            .map(|spec| (spec.subsystem, spec.metric_name))
            .collect();
        assert_eq!(registered_names.len(), 97);

        assert_eq!(CLIENT_GO_SHORTCUT_SPECS.len(), 151);
        assert_eq!(
            CLIENT_GO_SHORTCUT_SPECS
                .iter()
                .filter(|spec| spec.initialized())
                .count(),
            149
        );
        assert_eq!(
            CLIENT_GO_SHORTCUT_SPECS
                .iter()
                .filter(|spec| !spec.initialized())
                .map(|spec| spec.source_name)
                .collect::<Vec<_>>(),
            ["BatchRecvHistogramOK", "BatchRecvHistogramError"]
        );
        let shortcut_names: HashSet<_> = CLIENT_GO_SHORTCUT_SPECS
            .iter()
            .map(|spec| spec.source_name)
            .collect();
        assert_eq!(shortcut_names.len(), CLIENT_GO_SHORTCUT_SPECS.len());
    }

    #[test]
    fn every_initialized_collector_registers_with_exact_metadata() {
        let mut const_labels = HashMap::new();
        const_labels.insert("cluster".to_owned(), "source-test".to_owned());
        let metrics = ClientGoMetrics::new("custom", "client", const_labels).unwrap();
        assert_eq!(metrics.collectors.len(), 97);
        assert_eq!(metrics.shortcuts.len(), 149);
        assert!(metrics
            .collector("TiKVPessimisticLockKeysDuration")
            .is_none());
        assert!(metrics.shortcut("BatchRecvHistogramOK").is_none());
        assert!(metrics.shortcut("BatchRecvHistogramError").is_none());

        for spec in CLIENT_GO_METRIC_SPECS
            .iter()
            .filter(|spec| spec.initialized)
        {
            let collector = metrics.collector(spec.source_name).unwrap();
            exercise_collector(spec, collector);
        }

        let registry = Registry::new();
        metrics.register_metrics(&registry).unwrap();
        let families = registry.gather();
        assert_eq!(families.len(), 97);

        for spec in CLIENT_GO_METRIC_SPECS
            .iter()
            .filter(|spec| spec.initialized)
        {
            let name = expected_name(spec, "custom", "client");
            let family = families
                .iter()
                .find(|family| family.get_name() == name)
                .unwrap_or_else(|| panic!("missing family {name} for {}", spec.source_name));
            assert_eq!(family.get_help(), spec.help, "{}", spec.source_name);
            assert_eq!(
                family.get_field_type(),
                expected_metric_type(spec.kind),
                "{}",
                spec.source_name
            );
            assert!(!family.get_metric().is_empty(), "{}", spec.source_name);
            for metric in family.get_metric() {
                let label_names: HashSet<_> = metric
                    .get_label()
                    .iter()
                    .map(|pair| pair.get_name())
                    .collect();
                assert!(label_names.contains("cluster"), "{}", spec.source_name);
                for label in spec.labels {
                    assert!(label_names.contains(label), "{}: {label}", spec.source_name);
                }
                match spec.buckets {
                    BucketSpec::None => {}
                    BucketSpec::Exponential { count, .. } => assert_eq!(
                        metric.get_histogram().get_bucket().len(),
                        count,
                        "{}",
                        spec.source_name
                    ),
                    BucketSpec::Explicit(values) => assert_eq!(
                        metric.get_histogram().get_bucket().len(),
                        values.len(),
                        "{}",
                        spec.source_name
                    ),
                }
                if spec.kind == MetricKind::SummaryVec {
                    assert!(metric.get_summary().get_quantile().is_empty());
                }
            }
            if spec.kind == MetricKind::SummaryVec {
                assert!(family
                    .get_metric()
                    .iter()
                    .any(|metric| metric.get_summary().get_sample_count() >= 1));
            }
        }
        assert!(matches!(
            metrics.register_metrics(&registry),
            Err(prometheus::Error::AlreadyReg)
        ));
    }

    #[test]
    fn shortcuts_share_parent_children_and_commit_snapshots() {
        let metrics = ClientGoMetrics::new_default().unwrap();
        let before = metrics.get_txn_commit_counter();
        for name in [
            "TwoPCTxnCounterOk",
            "AsyncCommitTxnCounterOk",
            "OnePCTxnCounterOk",
        ] {
            match metrics.shortcut(name).unwrap() {
                ClientGoShortcut::Counter(counter) => counter.inc(),
                shortcut => panic!("unexpected shortcut {shortcut:?}"),
            }
        }
        let after = metrics.get_txn_commit_counter();
        assert_eq!(
            after.subtract(before),
            TxnCommitCounter {
                two_pc: 1,
                async_commit: 1,
                one_pc: 1,
            }
        );
        assert_eq!(
            serde_json::to_string(&after.subtract(before)).unwrap(),
            r#"{"twoPC":1,"asyncCommit":1,"onePC":1}"#
        );

        let observer = match metrics.shortcut("BackoffHistogramRPC").unwrap() {
            ClientGoShortcut::Observer(observer) => observer,
            shortcut => panic!("unexpected shortcut {shortcut:?}"),
        };
        observer.observe(0.5);
        assert_eq!(observer.sample_count(), 1);
        assert_eq!(observer.sample_sum(), 0.5);

        let summary = match metrics.shortcut("ReadRequestLeaderLocalBytes").unwrap() {
            ClientGoShortcut::Observer(observer) => observer,
            shortcut => panic!("unexpected shortcut {shortcut:?}"),
        };
        summary.observe(1024.0);
        assert_eq!(summary.sample_count(), 1);
        assert_eq!(summary.sample_sum(), 1024.0);

        match metrics
            .shortcut("LockResolverAsyncRunningTasksForReadResolve")
            .unwrap()
        {
            ClientGoShortcut::Gauge(gauge) => {
                gauge.inc();
                assert_eq!(gauge.get(), 1.0);
            }
            shortcut => panic!("unexpected shortcut {shortcut:?}"),
        }
    }

    #[test]
    fn observe_read_sli_matches_source_boundaries() {
        let metrics = ClientGoMetrics::new_default().unwrap();
        let small = metrics.histogram("TiKVSmallReadDuration").unwrap();
        let throughput = metrics.histogram("TiKVReadThroughput").unwrap();
        metrics.observe_read_sli(0, 1.0, 1.0);
        metrics.observe_read_sli(1, 0.0, 1.0);
        assert_eq!(small.get_sample_count(), 0);
        assert_eq!(throughput.get_sample_count(), 0);

        metrics.observe_read_sli(20, 2.0, 1024.0 * 1024.0 - 1.0);
        assert_eq!(small.get_sample_count(), 1);
        assert_eq!(small.get_sample_sum(), 2.0);
        metrics.observe_read_sli(21, 2.0, 10.0);
        metrics.observe_read_sli(1, 4.0, 1024.0 * 1024.0);
        assert_eq!(throughput.get_sample_count(), 2);
        assert_eq!(throughput.get_sample_sum(), 5.0 + (1024.0 * 1024.0) / 4.0);
    }

    #[test]
    fn stale_store_search_and_partial_delete_cover_native_and_summary_vectors() {
        let metrics = ClientGoMetrics::new_default().unwrap();
        assert_eq!(metrics.store_metric_vec_list().len(), 15);
        let regions = metrics.metric_vec("TiKVRegionErrorCounter").unwrap();
        let region_counter = metrics.counter_vec("TiKVRegionErrorCounter").unwrap();
        region_counter.with_label_values(&["not_leader", "0"]).inc();
        region_counter.with_label_values(&["not_leader", "1"]).inc();
        region_counter
            .with_label_values(&["server_busy", "2"])
            .inc();
        let valid = HashSet::from([1]);
        assert_eq!(find_next_stale_store_id(&regions, &valid), 2);
        assert_eq!(regions.delete_partial_match(&[(labels::STORE, "2")]), 1);
        assert_eq!(regions.delete_partial_match(&[(labels::STORE, "2")]), 0);
        assert_eq!(find_next_stale_store_id(&regions, &valid), 0);

        let source_requests = metrics.metric_vec("TiKVSendReqBySourceSummary").unwrap();
        metrics
            .summary_vec("TiKVSendReqBySourceSummary")
            .unwrap()
            .with_label_values(&["get", "3", "false", "general", "external"])
            .unwrap()
            .observe(0.1);
        assert_eq!(find_next_stale_store_id(&source_requests, &valid), 3);
        assert_eq!(
            source_requests.delete_partial_match(&[(labels::STORE, "3")]),
            1
        );
        assert_eq!(find_next_stale_store_id(&source_requests, &valid), 0);
    }

    #[test]
    fn independent_initializations_do_not_share_values() {
        let first = ClientGoMetrics::new("one", "client", HashMap::new()).unwrap();
        let second = ClientGoMetrics::new("two", "client", HashMap::new()).unwrap();
        first.counter("TiKVTxnWriteConflictCounter").unwrap().inc();
        assert_eq!(
            first.counter("TiKVTxnWriteConflictCounter").unwrap().get(),
            1.0
        );
        assert_eq!(
            second.counter("TiKVTxnWriteConflictCounter").unwrap().get(),
            0.0
        );
    }

    #[test]
    fn process_global_initialization_drives_existing_consumers() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "metrics::tests::process_global_initialization_probe",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "global metrics probe failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "runs in an isolated subprocess through the parent integration test"]
    fn process_global_initialization_probe() {
        let mut const_labels = HashMap::new();
        const_labels.insert("cluster".to_owned(), "first".to_owned());
        init_metrics_with_const_labels("probe", "client", const_labels).unwrap();
        let first = global_metrics();
        assert_eq!(get_store_metric_vec_list().len(), 15);
        assert_eq!(get_txn_commit_counter(), TxnCommitCounter::default());
        observe_read_sli(1, 0.5, 128.0);
        assert_eq!(
            first
                .histogram("TiKVSmallReadDuration")
                .unwrap()
                .get_sample_count(),
            1
        );
        crate::stats::increment_write_conflict();
        assert_eq!(
            first.counter("TiKVTxnWriteConflictCounter").unwrap().get(),
            1.0
        );
        let registry = Registry::new();
        first.register_metrics(&registry).unwrap();
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.get_name() == "probe_client_txn_write_conflict_counter")
            .unwrap();
        assert!(family.get_metric()[0]
            .get_label()
            .iter()
            .any(|pair| pair.get_name() == "cluster" && pair.get_value() == "first"));
        register_metrics();
        assert!(matches!(
            try_register_metrics(),
            Err(prometheus::Error::AlreadyReg)
        ));

        first
            .counter_vec("TiKVRegionErrorCounter")
            .unwrap()
            .with_label_values(&["server_busy", "8"])
            .inc();
        first
            .counter_vec("TiKVRegionErrorCounter")
            .unwrap()
            .with_label_values(&["not_leader", "9"])
            .inc();
        first
            .gauge_vec("TiKVStoreLivenessGauge")
            .unwrap()
            .with_label_values(&["9"])
            .set(1.0);
        assert_eq!(
            crate::stats::find_next_stale_store_id(&HashSet::new()),
            Some(9)
        );
        crate::stats::remove_store_metrics(9);
        assert_eq!(
            crate::stats::find_next_stale_store_id(&HashSet::new()),
            None
        );
        assert!(!first
            .metric_vec("TiKVRegionErrorCounter")
            .unwrap()
            .collect()
            .iter()
            .flat_map(|family| family.get_metric())
            .flat_map(|metric| metric.get_label())
            .any(|pair| pair.get_name() == labels::STORE && pair.get_value() == "9"));
        assert_eq!(
            first
                .counter_vec("TiKVRegionErrorCounter")
                .unwrap()
                .with_label_values(&["server_busy", "8"])
                .get(),
            1.0
        );

        init_metrics("replacement", "client").unwrap();
        crate::stats::increment_write_conflict();
        assert_eq!(
            global_metrics()
                .counter("TiKVTxnWriteConflictCounter")
                .unwrap()
                .get(),
            1.0
        );
        assert_eq!(
            first.counter("TiKVTxnWriteConflictCounter").unwrap().get(),
            1.0
        );
    }
}
