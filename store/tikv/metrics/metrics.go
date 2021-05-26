// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Client metrics.
var (
	TiKVTxnCmdHistogram                    *prometheus.HistogramVec
	TiKVBackoffHistogram                   *prometheus.HistogramVec
	TiKVSendReqHistogram                   *prometheus.HistogramVec
	TiKVCoprocessorHistogram               prometheus.Histogram
	TiKVLockResolverCounter                *prometheus.CounterVec
	TiKVRegionErrorCounter                 *prometheus.CounterVec
	TiKVTxnWriteKVCountHistogram           prometheus.Histogram
	TiKVTxnWriteSizeHistogram              prometheus.Histogram
	TiKVRawkvCmdHistogram                  *prometheus.HistogramVec
	TiKVRawkvSizeHistogram                 *prometheus.HistogramVec
	TiKVTxnRegionsNumHistogram             *prometheus.HistogramVec
	TiKVLoadSafepointCounter               *prometheus.CounterVec
	TiKVSecondaryLockCleanupFailureCounter *prometheus.CounterVec
	TiKVRegionCacheCounter                 *prometheus.CounterVec
	TiKVLocalLatchWaitTimeHistogram        prometheus.Histogram
	TiKVStatusDuration                     *prometheus.HistogramVec
	TiKVStatusCounter                      *prometheus.CounterVec
	TiKVBatchWaitDuration                  prometheus.Histogram
	TiKVBatchSendLatency                   prometheus.Histogram
	TiKVBatchWaitOverLoad                  prometheus.Counter
	TiKVBatchPendingRequests               *prometheus.HistogramVec
	TiKVBatchRequests                      *prometheus.HistogramVec
	TiKVBatchClientUnavailable             prometheus.Histogram
	TiKVBatchClientWaitEstablish           prometheus.Histogram
	TiKVRangeTaskStats                     *prometheus.GaugeVec
	TiKVRangeTaskPushDuration              *prometheus.HistogramVec
	TiKVTokenWaitDuration                  prometheus.Histogram
	TiKVTxnHeartBeatHistogram              *prometheus.HistogramVec
	TiKVPessimisticLockKeysDuration        prometheus.Histogram
	TiKVTTLLifeTimeReachCounter            prometheus.Counter
	TiKVNoAvailableConnectionCounter       prometheus.Counter
	TiKVTwoPCTxnCounter                    *prometheus.CounterVec
	TiKVAsyncCommitTxnCounter              *prometheus.CounterVec
	TiKVOnePCTxnCounter                    *prometheus.CounterVec
	TiKVStoreLimitErrorCounter             *prometheus.CounterVec
	TiKVGRPCConnTransientFailureCounter    *prometheus.CounterVec
	TiKVPanicCounter                       *prometheus.CounterVec
	TiKVForwardRequestCounter              *prometheus.CounterVec
	TiKVTSFutureWaitDuration               prometheus.Histogram
	TiKVSafeTSUpdateCounter                *prometheus.CounterVec
	TiKVSafeTSUpdateStats                  *prometheus.GaugeVec
)

// Label constants.
const (
	LblType            = "type"
	LblResult          = "result"
	LblStore           = "store"
	LblCommit          = "commit"
	LblAbort           = "abort"
	LblRollback        = "rollback"
	LblBatchGet        = "batch_get"
	LblGet             = "get"
	LblLockKeys        = "lock_keys"
	LabelBatchRecvLoop = "batch-recv-loop"
	LabelBatchSendLoop = "batch-send-loop"
	LblAddress         = "address"
	LblFromStore       = "from_store"
	LblToStore         = "to_store"
)

func initMetrics(namespace, subsystem string) {
	TiKVTxnCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "txn_cmd_duration_seconds",
			Help:      "Bucketed histogram of processing time of txn cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType})

	TiKVBackoffHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "backoff_seconds",
			Help:      "total backoff seconds of a single backoffer.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType})

	TiKVSendReqHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "request_seconds",
			Help:      "Bucketed histogram of sending request duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType, LblStore})

	TiKVCoprocessorHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cop_duration_seconds",
			Help:      "Run duration of a single coprocessor task, includes backoff time.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		})

	TiKVLockResolverCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "lock_resolver_actions_total",
			Help:      "Counter of lock resolver actions.",
		}, []string{LblType})

	TiKVRegionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "region_err_total",
			Help:      "Counter of region errors.",
		}, []string{LblType})

	TiKVTxnWriteKVCountHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "txn_write_kv_num",
			Help:      "Count of kv pairs to write in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 4, 17), // 1 ~ 4G
		})

	TiKVTxnWriteSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "txn_write_size_bytes",
			Help:      "Size of kv pairs to write in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(16, 4, 17), // 16Bytes ~ 64GB
		})

	TiKVRawkvCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "rawkv_cmd_seconds",
			Help:      "Bucketed histogram of processing time of rawkv cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType})

	TiKVRawkvSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "rawkv_kv_size_bytes",
			Help:      "Size of key/value to put, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1Byte ~ 512MB
		}, []string{LblType})

	TiKVTxnRegionsNumHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "txn_regions_num",
			Help:      "Number of regions in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 25), // 1 ~ 16M
		}, []string{LblType})

	TiKVLoadSafepointCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "load_safepoint_total",
			Help:      "Counter of load safepoint.",
		}, []string{LblType})

	TiKVSecondaryLockCleanupFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "lock_cleanup_task_total",
			Help:      "failure statistic of secondary lock cleanup task.",
		}, []string{LblType})

	TiKVRegionCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "region_cache_operations_total",
			Help:      "Counter of region cache.",
		}, []string{LblType, LblResult})

	TiKVLocalLatchWaitTimeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "local_latch_wait_seconds",
			Help:      "Wait time of a get local latch.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 262s
		})

	TiKVStatusDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "kv_status_api_duration",
			Help:      "duration for kv status api.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 262s
		}, []string{"store"})

	TiKVStatusCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "kv_status_api_count",
			Help:      "Counter of access kv status api.",
		}, []string{LblResult})

	TiKVBatchWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_wait_duration",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 34), // 1ns ~ 8s
			Help:      "batch wait duration",
		})

	TiKVBatchSendLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_send_latency",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 34), // 1ns ~ 8s
			Help:      "batch send latency",
		})

	TiKVBatchWaitOverLoad = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_wait_overload",
			Help:      "event of tikv transport layer overload",
		})

	TiKVBatchPendingRequests = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_pending_requests",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 8),
			Help:      "number of requests pending in the batch channel",
		}, []string{"store"})

	TiKVBatchRequests = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_requests",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 8),
			Help:      "number of requests in one batch",
		}, []string{"store"})

	TiKVBatchClientUnavailable = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_client_unavailable_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
			Help:      "batch client unavailable",
		})

	TiKVBatchClientWaitEstablish = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_client_wait_connection_establish",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
			Help:      "batch client wait new connection establish",
		})

	TiKVRangeTaskStats = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "range_task_stats",
			Help:      "stat of range tasks",
		}, []string{LblType, LblResult})

	TiKVRangeTaskPushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "range_task_push_duration",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
			Help:      "duration to push sub tasks to range task workers",
		}, []string{LblType})

	TiKVTokenWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_executor_token_wait_duration",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 34), // 1ns ~ 8s
			Help:      "tidb txn token wait duration to process batches",
		})

	TiKVTxnHeartBeatHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "txn_heart_beat",
			Help:      "Bucketed histogram of the txn_heartbeat request duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType})

	TiKVPessimisticLockKeysDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "pessimistic_lock_keys_duration",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 24), // 1ms ~ 8389s
			Help:      "tidb txn pessimistic lock keys duration",
		})

	TiKVTTLLifeTimeReachCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "ttl_lifetime_reach_total",
			Help:      "Counter of ttlManager live too long.",
		})

	TiKVNoAvailableConnectionCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_client_no_available_connection_total",
			Help:      "Counter of no available batch client.",
		})

	TiKVTwoPCTxnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "commit_txn_counter",
			Help:      "Counter of 2PC transactions.",
		}, []string{LblType})

	TiKVAsyncCommitTxnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "async_commit_txn_counter",
			Help:      "Counter of async commit transactions.",
		}, []string{LblType})

	TiKVOnePCTxnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "one_pc_txn_counter",
			Help:      "Counter of 1PC transactions.",
		}, []string{LblType})

	TiKVStoreLimitErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "get_store_limit_token_error",
			Help:      "store token is up to the limit, probably because one of the stores is the hotspot or unavailable",
		}, []string{LblAddress, LblStore})

	TiKVGRPCConnTransientFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "connection_transient_failure_count",
			Help:      "Counter of gRPC connection transient failure",
		}, []string{LblAddress, LblStore})

	TiKVPanicCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "panic_total",
			Help:      "Counter of panic.",
		}, []string{LblType})

	TiKVForwardRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "forward_request_counter",
			Help:      "Counter of tikv request being forwarded through another node",
		}, []string{LblFromStore, LblToStore, LblType, LblResult})

	TiKVTSFutureWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "ts_future_wait_seconds",
			Help:      "Bucketed histogram of seconds cost for waiting timestamp future.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 30), // 5us ~ 2560s
		})

	TiKVSafeTSUpdateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "safets_update_counter",
			Help:      "Counter of tikv safe_ts being updated.",
		}, []string{LblResult, LblStore})

	TiKVSafeTSUpdateStats = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "safets_update_stats",
			Help:      "stat of tikv updating safe_ts stats",
		}, []string{LblStore})

	initShortcuts()
}

func init() {
	initMetrics("tikv", "client_go")
}

// InitMetrics initializes metrics variables with given namespace and subsystem name.
func InitMetrics(namespace, subsystem string) {
	initMetrics(namespace, subsystem)
}

// RegisterMetrics registers all metrics variables.
// Note: to change default namespace and subsystem name, call `InitMetrics` before registering.
func RegisterMetrics() {
	prometheus.MustRegister(TiKVTxnCmdHistogram)
	prometheus.MustRegister(TiKVBackoffHistogram)
	prometheus.MustRegister(TiKVSendReqHistogram)
	prometheus.MustRegister(TiKVCoprocessorHistogram)
	prometheus.MustRegister(TiKVLockResolverCounter)
	prometheus.MustRegister(TiKVRegionErrorCounter)
	prometheus.MustRegister(TiKVTxnWriteKVCountHistogram)
	prometheus.MustRegister(TiKVTxnWriteSizeHistogram)
	prometheus.MustRegister(TiKVRawkvCmdHistogram)
	prometheus.MustRegister(TiKVRawkvSizeHistogram)
	prometheus.MustRegister(TiKVTxnRegionsNumHistogram)
	prometheus.MustRegister(TiKVLoadSafepointCounter)
	prometheus.MustRegister(TiKVSecondaryLockCleanupFailureCounter)
	prometheus.MustRegister(TiKVRegionCacheCounter)
	prometheus.MustRegister(TiKVLocalLatchWaitTimeHistogram)
	prometheus.MustRegister(TiKVStatusDuration)
	prometheus.MustRegister(TiKVStatusCounter)
	prometheus.MustRegister(TiKVBatchWaitDuration)
	prometheus.MustRegister(TiKVBatchSendLatency)
	prometheus.MustRegister(TiKVBatchWaitOverLoad)
	prometheus.MustRegister(TiKVBatchPendingRequests)
	prometheus.MustRegister(TiKVBatchRequests)
	prometheus.MustRegister(TiKVBatchClientUnavailable)
	prometheus.MustRegister(TiKVBatchClientWaitEstablish)
	prometheus.MustRegister(TiKVRangeTaskStats)
	prometheus.MustRegister(TiKVRangeTaskPushDuration)
	prometheus.MustRegister(TiKVTokenWaitDuration)
	prometheus.MustRegister(TiKVTxnHeartBeatHistogram)
	prometheus.MustRegister(TiKVPessimisticLockKeysDuration)
	prometheus.MustRegister(TiKVTTLLifeTimeReachCounter)
	prometheus.MustRegister(TiKVNoAvailableConnectionCounter)
	prometheus.MustRegister(TiKVTwoPCTxnCounter)
	prometheus.MustRegister(TiKVAsyncCommitTxnCounter)
	prometheus.MustRegister(TiKVOnePCTxnCounter)
	prometheus.MustRegister(TiKVStoreLimitErrorCounter)
	prometheus.MustRegister(TiKVGRPCConnTransientFailureCounter)
	prometheus.MustRegister(TiKVPanicCounter)
	prometheus.MustRegister(TiKVForwardRequestCounter)
	prometheus.MustRegister(TiKVTSFutureWaitDuration)
	prometheus.MustRegister(TiKVSafeTSUpdateCounter)
	prometheus.MustRegister(TiKVSafeTSUpdateStats)
}

// readCounter reads the value of a prometheus.Counter.
// Returns -1 when failing to read the value.
func readCounter(m prometheus.Counter) int64 {
	// Actually, it's not recommended to read the value of prometheus metric types directly:
	// https://github.com/prometheus/client_golang/issues/486#issuecomment-433345239
	pb := &dto.Metric{}
	// It's impossible to return an error though.
	if err := m.Write(pb); err != nil {
		return -1
	}
	return int64(pb.GetCounter().GetValue())
}

// TxnCommitCounter is the counter of transactions committed with
// different protocols, i.e. 2PC, async-commit, 1PC.
type TxnCommitCounter struct {
	TwoPC       int64 `json:"twoPC"`
	AsyncCommit int64 `json:"asyncCommit"`
	OnePC       int64 `json:"onePC"`
}

// Sub returns the difference of two counters.
func (c TxnCommitCounter) Sub(rhs TxnCommitCounter) TxnCommitCounter {
	new := TxnCommitCounter{}
	new.TwoPC = c.TwoPC - rhs.TwoPC
	new.AsyncCommit = c.AsyncCommit - rhs.AsyncCommit
	new.OnePC = c.OnePC - rhs.OnePC
	return new
}

// GetTxnCommitCounter gets the TxnCommitCounter.
func GetTxnCommitCounter() TxnCommitCounter {
	return TxnCommitCounter{
		TwoPC:       readCounter(TwoPCTxnCounterOk),
		AsyncCommit: readCounter(AsyncCommitTxnCounterOk),
		OnePC:       readCounter(OnePCTxnCounterOk),
	}
}
