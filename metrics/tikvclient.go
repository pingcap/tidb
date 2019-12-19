// Copyright 2018 PingCAP, Inc.
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

import "github.com/prometheus/client_golang/prometheus"

// TiKVClient metrics.
var (
	TiKVTxnCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_total",
			Help:      "Counter of created txns.",
		})

	TiKVSnapshotCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "snapshot_total",
			Help:      "Counter of snapshots.",
		})

	TiKVTxnCmdCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_cmd_total",
			Help:      "Counter of txn commands.",
		}, []string{LblType})

	TiKVTxnCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_cmd_duration_seconds",
			Help:      "Bucketed histogram of processing time of txn cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 524s
		}, []string{LblType})

	TiKVBackoffHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "backoff_seconds",
			Help:      "total backoff seconds of a single backoffer.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 524s
		}, []string{LblType})

	TiKVSendReqHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of sending request duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 524s
		}, []string{LblType, LblStore})

	TiKVCoprocessorHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "cop_duration_seconds",
			Help:      "Run duration of a single coprocessor task, includes backoff time.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 524s
		})

	TiKVLockResolverCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "lock_resolver_actions_total",
			Help:      "Counter of lock resolver actions.",
		}, []string{LblType})

	TiKVRegionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "region_err_total",
			Help:      "Counter of region errors.",
		}, []string{LblType})

	TiKVTxnWriteKVCountHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_write_kv_num",
			Help:      "Count of kv pairs to write in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 21), // 1 ~ 2097152
		})

	TiKVTxnWriteSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_write_size_bytes",
			Help:      "Size of kv pairs to write in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1Byte ~ 1GB
		})

	TiKVRawkvCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "rawkv_cmd_seconds",
			Help:      "Bucketed histogram of processing time of rawkv cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 524s
		}, []string{LblType})

	TiKVRawkvSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "rawkv_kv_size_bytes",
			Help:      "Size of key/value to put, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 23), // 1Byte ~ 8MB
		}, []string{LblType})

	TiKVTxnRegionsNumHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_regions_num",
			Help:      "Number of regions in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1 ~ 1M
		}, []string{LblType})

	TiKVLoadSafepointCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "load_safepoint_total",
			Help:      "Counter of load safepoint.",
		}, []string{LblType})

	TiKVSecondaryLockCleanupFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "lock_cleanup_task_total",
			Help:      "failure statistic of secondary lock cleanup task.",
		}, []string{LblType})

	TiKVRegionCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "region_cache_operations_total",
			Help:      "Counter of region cache.",
		}, []string{LblType, LblResult})

	TiKVLocalLatchWaitTimeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "local_latch_wait_seconds",
			Help:      "Wait time of a get local latch.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 524s
		})

	// TiKVPendingBatchRequests indicates the number of requests pending in the batch channel.
	TiKVPendingBatchRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "pending_batch_requests",
			Help:      "Pending batch requests",
		}, []string{"store"})

	TiKVBatchWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "batch_wait_duration",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1ns ~ 1s
			Help:      "batch wait duration",
		})

	TiKVBatchClientUnavailable = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "batch_client_unavailable_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 1000s
			Help:      "batch client unavailable",
		})

	TiKVRangeTaskStats = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "range_task_stats",
			Help:      "stat of range tasks",
		}, []string{LblType, LblResult})

	TiKVRangeTaskPushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "range_task_push_duration",
			// 1ms ~ 1000s
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 20),
			Help:    "duration to push sub tasks to range task workers",
		}, []string{LblType})
	TiKVTokenWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "batch_executor_token_wait_duration",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1ns ~ 1s
			Help:      "tidb txn token wait duration to process batches",
		})

	TiKVTxnHeartBeatHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_heart_beat",
			Help:      "Bucketed histogram of the txn_heartbeat request duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 18), // 1ms ~ 292s
		}, []string{LblType})
)
