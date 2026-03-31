// Copyright 2025 PingCAP, Inc.
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

package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// RemotePlanForwardCounter records remote plan forwarding requests.
	RemotePlanForwardCounter *prometheus.CounterVec
	// RemotePlanForwardDuration records remote plan forwarding handling duration.
	RemotePlanForwardDuration *prometheus.HistogramVec
	// RemotePlanForwardSendOKCounter records sender-side forwarding successes.
	RemotePlanForwardSendOKCounter prometheus.Counter
	// RemotePlanForwardSendErrCounter records sender-side forwarding errors.
	RemotePlanForwardSendErrCounter prometheus.Counter
	// RemotePlanForwardRecvOKCounter records receiver-side forwarding successes.
	RemotePlanForwardRecvOKCounter prometheus.Counter
	// RemotePlanForwardRecvErrCounter records receiver-side forwarding errors.
	RemotePlanForwardRecvErrCounter prometheus.Counter
	// RemotePlanForwardSendOKDuration records sender-side forwarding success latency.
	RemotePlanForwardSendOKDuration prometheus.Observer
	// RemotePlanForwardSendErrDuration records sender-side forwarding error latency.
	RemotePlanForwardSendErrDuration prometheus.Observer
	// RemotePlanForwardRecvOKDuration records receiver-side forwarding success latency.
	RemotePlanForwardRecvOKDuration prometheus.Observer
	// RemotePlanForwardRecvErrDuration records receiver-side forwarding error latency.
	RemotePlanForwardRecvErrDuration prometheus.Observer
	// RemotePlanExecDuration records remote exec SQL execution duration (compile + execute, excludes streaming).
	RemotePlanExecDuration *prometheus.HistogramVec
	// RemotePlanExecOKDuration records remote exec SQL success latency.
	RemotePlanExecOKDuration prometheus.Observer
	// RemotePlanExecErrDuration records remote exec SQL error latency.
	RemotePlanExecErrDuration prometheus.Observer
	// RemotePlanFirstRespDuration records time to receive first response on control side (excludes client read time).
	RemotePlanFirstRespDuration *prometheus.HistogramVec
	// RemotePlanFirstRespOKDuration records first response success latency.
	RemotePlanFirstRespOKDuration prometheus.Observer
	// RemotePlanFirstRespErrDuration records first response error latency.
	RemotePlanFirstRespErrDuration prometheus.Observer
	// RemotePlanClientConsumeDuration records time for client to consume all data after first response.
	RemotePlanClientConsumeDuration *prometheus.HistogramVec
	// RemotePlanClientConsumeOKDuration records client consume success latency.
	RemotePlanClientConsumeOKDuration prometheus.Observer
	// RemotePlanClientConsumeErrDuration records client consume error latency.
	RemotePlanClientConsumeErrDuration prometheus.Observer
	// RemotePlanCacheCounter records remote exec plan cache hit/miss.
	RemotePlanCacheCounter *prometheus.CounterVec
	// RemotePlanCacheHitCounter records remote exec plan cache hits.
	RemotePlanCacheHitCounter prometheus.Counter
	// RemotePlanCacheMissCounter records remote exec plan cache misses.
	RemotePlanCacheMissCounter prometheus.Counter

	// ============================================================================
	// Pool resource metrics - for diagnosing resource exhaustion issues
	// ============================================================================

	// RemotePlanBatchClientPendingRequests records pending requests waiting for response.
	RemotePlanBatchClientPendingRequests prometheus.Gauge
	// RemotePlanBatchClientActiveStreams records active gRPC streams.
	RemotePlanBatchClientActiveStreams prometheus.Gauge
	// RemotePlanBatchClientBatchSize records batch size distribution.
	RemotePlanBatchClientBatchSize prometheus.Histogram
	// RemotePlanBatchClientSendChFull records send channel full events (indicates backpressure).
	RemotePlanBatchClientSendChFull prometheus.Counter
	// RemotePlanBatchClientRespChFull records response channel full events (indicates slow consumer).
	RemotePlanBatchClientRespChFull prometheus.Counter

	// RemotePlanWorkerPoolWorkers records current worker count.
	RemotePlanWorkerPoolWorkers prometheus.Gauge
	// RemotePlanWorkerPoolSoftMax records current soft max limit.
	RemotePlanWorkerPoolSoftMax prometheus.Gauge
	// RemotePlanWorkerPoolHardMax records hard max limit.
	RemotePlanWorkerPoolHardMax prometheus.Gauge
	// RemotePlanWorkerPoolSubmitWaitDuration records wait time when submitting to worker pool.
	RemotePlanWorkerPoolSubmitWaitDuration prometheus.Histogram
	// RemotePlanWorkerPoolQueueFull records queue full events when no idle workers.
	RemotePlanWorkerPoolQueueFull prometheus.Counter
	// RemotePlanWorkerPoolQueueLen records current queue length (tasks waiting in buffer).
	RemotePlanWorkerPoolQueueLen prometheus.Gauge
	// RemotePlanWorkerPoolBufferFull records buffer full events when tasks must wait for workers.
	RemotePlanWorkerPoolBufferFull prometheus.Counter

	// RemotePlanSessionPoolSize records session pool size.
	RemotePlanSessionPoolSize prometheus.Gauge
	// RemotePlanSessionPoolInUse records sessions currently in use.
	RemotePlanSessionPoolInUse prometheus.Gauge

	// ============================================================================
	// Detailed streaming metrics - for diagnosing where time goes
	// ============================================================================

	// Control side (batch_recordset.go) detailed metrics

	// RemotePlanChannelWaitDuration records time waiting for data from remote channel.
	RemotePlanChannelWaitDuration prometheus.Histogram
	// RemotePlanChunkDecodeDuration records time decoding chunks from remote.
	RemotePlanChunkDecodeDuration prometheus.Histogram
	// RemotePlanRowCopyDuration records time copying rows to output chunk.
	RemotePlanRowCopyDuration prometheus.Histogram
	// RemotePlanFirstResultWaitDuration records time waiting for the first result from remote.
	RemotePlanFirstResultWaitDuration prometheus.Histogram

	// Remote side (service.go) detailed metrics

	// RemotePlanChunkEncodeDuration records time encoding chunks on remote side.
	RemotePlanChunkEncodeDuration prometheus.Histogram
	// RemotePlanStreamSendDuration records time sending chunks via gRPC stream.
	RemotePlanStreamSendDuration prometheus.Histogram
	// RemotePlanChunkRows records number of rows per chunk sent.
	RemotePlanChunkRows prometheus.Histogram
	// RemotePlanChunkBytes records bytes per chunk sent.
	RemotePlanChunkBytes prometheus.Histogram

	// RemotePlanGrpcSendDuration records actual gRPC stream.Send() time in response sender goroutine.
	RemotePlanGrpcSendDuration prometheus.Histogram
	// RemotePlanRespBatchSize records number of responses batched in one gRPC Send.
	RemotePlanRespBatchSize prometheus.Histogram

	// RemotePlanGrpcRecvDuration records actual gRPC stream.Recv() time on the control side.
	RemotePlanGrpcRecvDuration prometheus.Histogram
	// RemotePlanRSNextDuration records time spent in recordSet.Next() on the control side.
	RemotePlanRSNextDuration prometheus.Histogram
	// RemotePlanRSFirstNextDuration records time spent in the first recordSet.Next() call.
	RemotePlanRSFirstNextDuration prometheus.Histogram
	// RemotePlanRespBatchWaitDuration records time waiting for a batch of responses.
	RemotePlanRespBatchWaitDuration prometheus.Histogram
	// RemotePlanRSNextCopCount records number of Cop requests per rs.Next() call.
	RemotePlanRSNextCopCount prometheus.Histogram

	// ============================================================================
	// Additional detailed metrics for RS Next Duration breakdown
	// ============================================================================

	// RemotePlanSessionAcquireDuration records time to acquire a session from pool or create new one.
	RemotePlanSessionAcquireDuration prometheus.Histogram
	// RemotePlanProcessRequestDuration records total processRequest duration (session acquire + exec + streaming).
	RemotePlanProcessRequestDuration prometheus.Histogram
)

const (
	// RemotePlanForwardTypeSend indicates sender-side forwarding.
	RemotePlanForwardTypeSend = "send"
	// RemotePlanForwardTypeRecv indicates receiver-side handling.
	RemotePlanForwardTypeRecv = "recv"
	// RemotePlanCacheHit indicates a plan cache hit on remote exec.
	RemotePlanCacheHit = "hit"
	// RemotePlanCacheMiss indicates a plan cache miss on remote exec.
	RemotePlanCacheMiss = "miss"
)

// InitRemotePlanMetrics initializes remote plan metrics.
func InitRemotePlanMetrics() {
	RemotePlanForwardCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "forward_total",
			Help:      "Counter of remote plan forwarding requests.",
		}, []string{LblType, LblResult})

	RemotePlanForwardDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "forward_duration_seconds",
			Help:      "Bucketed histogram of remote plan forwarding duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType, LblResult})
	RemotePlanForwardSendOKCounter = RemotePlanForwardCounter.WithLabelValues(RemotePlanForwardTypeSend, opSucc)
	RemotePlanForwardSendErrCounter = RemotePlanForwardCounter.WithLabelValues(RemotePlanForwardTypeSend, opFailed)
	RemotePlanForwardRecvOKCounter = RemotePlanForwardCounter.WithLabelValues(RemotePlanForwardTypeRecv, opSucc)
	RemotePlanForwardRecvErrCounter = RemotePlanForwardCounter.WithLabelValues(RemotePlanForwardTypeRecv, opFailed)
	RemotePlanForwardSendOKDuration = RemotePlanForwardDuration.WithLabelValues(RemotePlanForwardTypeSend, opSucc)
	RemotePlanForwardSendErrDuration = RemotePlanForwardDuration.WithLabelValues(RemotePlanForwardTypeSend, opFailed)
	RemotePlanForwardRecvOKDuration = RemotePlanForwardDuration.WithLabelValues(RemotePlanForwardTypeRecv, opSucc)
	RemotePlanForwardRecvErrDuration = RemotePlanForwardDuration.WithLabelValues(RemotePlanForwardTypeRecv, opFailed)

	RemotePlanExecDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "exec_duration_seconds",
			Help:      "Bucketed histogram of remote exec SQL execution duration (compile + execute, excludes streaming).",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblResult})
	RemotePlanExecOKDuration = RemotePlanExecDuration.WithLabelValues(opSucc)
	RemotePlanExecErrDuration = RemotePlanExecDuration.WithLabelValues(opFailed)

	RemotePlanFirstRespDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "first_resp_duration_seconds",
			Help:      "Bucketed histogram of time to receive first response on control side.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblResult})
	RemotePlanFirstRespOKDuration = RemotePlanFirstRespDuration.WithLabelValues(opSucc)
	RemotePlanFirstRespErrDuration = RemotePlanFirstRespDuration.WithLabelValues(opFailed)

	RemotePlanClientConsumeDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "client_consume_duration_seconds",
			Help:      "Bucketed histogram of time for client to consume all data after first response.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblResult})
	RemotePlanClientConsumeOKDuration = RemotePlanClientConsumeDuration.WithLabelValues(opSucc)
	RemotePlanClientConsumeErrDuration = RemotePlanClientConsumeDuration.WithLabelValues(opFailed)

	RemotePlanCacheCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "plan_cache_total",
			Help:      "Counter of remote exec plan cache hits and misses.",
		}, []string{LblResult})
	RemotePlanCacheHitCounter = RemotePlanCacheCounter.WithLabelValues(RemotePlanCacheHit)
	RemotePlanCacheMissCounter = RemotePlanCacheCounter.WithLabelValues(RemotePlanCacheMiss)

	// ============================================================================
	// Pool resource metrics initialization
	// ============================================================================

	// Batch client metrics (sender side)
	RemotePlanBatchClientPendingRequests = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "batch_client_pending_requests",
			Help:      "Current number of pending requests waiting for response from remote server.",
		})
	RemotePlanBatchClientActiveStreams = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "batch_client_active_streams",
			Help:      "Current number of active gRPC streams to remote servers.",
		})
	RemotePlanBatchClientBatchSize = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "batch_client_batch_size",
			Help:      "Distribution of batch sizes when sending requests to remote server.",
			Buckets:   []float64{1, 2, 4, 8, 16, 32, 64, 128},
		})
	RemotePlanBatchClientSendChFull = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "batch_client_send_ch_full_total",
			Help:      "Total count of send channel full events (indicates backpressure from remote server).",
		})
	RemotePlanBatchClientRespChFull = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "batch_client_resp_ch_full_total",
			Help:      "Total count of response channel full events (indicates slow consumer on client side).",
		})

	// Worker pool metrics (receiver side)
	RemotePlanWorkerPoolWorkers = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "worker_pool_workers",
			Help:      "Current number of workers in the remote exec worker pool.",
		})
	RemotePlanWorkerPoolSoftMax = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "worker_pool_soft_max",
			Help:      "Current soft max limit of the worker pool (can grow up to hard max under load).",
		})
	RemotePlanWorkerPoolHardMax = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "worker_pool_hard_max",
			Help:      "Hard max limit of the worker pool.",
		})
	RemotePlanWorkerPoolSubmitWaitDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "worker_pool_submit_wait_duration_seconds",
			Help:      "Time spent waiting to submit task to worker pool (high values indicate pool exhaustion).",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 16), // 0.1ms ~ 3.2s
		})
	RemotePlanWorkerPoolQueueFull = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "worker_pool_queue_full_total",
			Help:      "Total count of queue full events when no idle workers available.",
		})
	RemotePlanWorkerPoolQueueLen = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "worker_pool_queue_len",
			Help:      "Current number of tasks waiting in the worker pool buffer.",
		})
	RemotePlanWorkerPoolBufferFull = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "worker_pool_buffer_full_total",
			Help:      "Total count of buffer full events when tasks must block waiting for workers.",
		})

	// Session pool metrics (receiver side)
	RemotePlanSessionPoolSize = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "session_pool_size",
			Help:      "Current size of the session pool for remote execution.",
		})
	RemotePlanSessionPoolInUse = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "session_pool_in_use",
			Help:      "Number of sessions currently in use from the session pool.",
		})

	// ============================================================================
	// Detailed streaming metrics initialization
	// ============================================================================

	// Control side detailed metrics
	RemotePlanChannelWaitDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "channel_wait_duration_seconds",
			Help:      "Time spent waiting for data from remote channel (high values indicate remote is slow).",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 18),
		})
	RemotePlanChunkDecodeDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "chunk_decode_duration_seconds",
			Help:      "Time spent decoding chunks received from remote.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 16), // 10us ~ 327ms
		})
	RemotePlanRowCopyDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "row_copy_duration_seconds",
			Help:      "Time spent copying rows to output chunk.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 16), // 10us ~ 327ms
		})
	RemotePlanFirstResultWaitDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "first_result_wait_duration_seconds",
			Help:      "Time from first response to first data row/chunk or EOF on control side.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 18),
		})

	// Remote side detailed metrics
	RemotePlanChunkEncodeDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "chunk_encode_duration_seconds",
			Help:      "Time spent encoding chunks on remote side before sending.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 16), // 10us ~ 327ms
		})
	RemotePlanStreamSendDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "stream_send_duration_seconds",
			Help:      "Time spent sending chunks via gRPC stream.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 18), // 10us ~ 1.3s
		})
	RemotePlanChunkRows = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "chunk_rows",
			Help:      "Number of rows per chunk sent from remote side.",
			Buckets:   []float64{1, 10, 50, 100, 256, 512, 1024, 2048},
		})
	RemotePlanChunkBytes = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "chunk_bytes",
			Help:      "Bytes per chunk sent from remote side.",
			Buckets:   prometheus.ExponentialBuckets(256, 2, 14), // 256B ~ 4MB
		})

	// Actual gRPC send duration in response sender goroutine
	RemotePlanGrpcSendDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "grpc_send_duration_seconds",
			Help:      "Actual gRPC stream.Send() time in response sender goroutine. High values indicate network or gRPC blocking.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 18),
		})

	// Response batch size (server side)
	RemotePlanRespBatchSize = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "resp_batch_size",
			Help:      "Number of responses batched in one gRPC Send. Higher values indicate better batching efficiency.",
			Buckets:   []float64{1, 2, 4, 8, 16, 32, 64},
		})

	RemotePlanGrpcRecvDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "grpc_recv_duration_seconds",
			Help:      "Time spent blocked in gRPC stream.Recv() on the control side recv loop.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 18),
		})
	RemotePlanRSNextDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "rs_next_duration_seconds",
			Help:      "Time spent in RecordSet.Next() while streaming results on remote side.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 18),
		})
	RemotePlanRSFirstNextDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "rs_first_next_duration_seconds",
			Help:      "Time spent in the first RecordSet.Next() call while streaming results on remote side.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 18),
		})
	RemotePlanRespBatchWaitDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "resp_batch_wait_duration_seconds",
			Help:      "Time from first response buffered to stream.Send() call in response sender goroutine.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 18),
		})
	RemotePlanRSNextCopCount = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "rs_next_cop_count",
			Help:      "Number of Cop/KV requests triggered per rs.Next() call during streaming. High values indicate many small fetches.",
			Buckets:   []float64{0, 1, 2, 4, 8, 16, 32, 64, 128},
		})

	// Additional detailed metrics for RS Next Duration breakdown
	RemotePlanSessionAcquireDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "session_acquire_duration_seconds",
			Help:      "Time to acquire a session from pool or create new one. High values indicate session pool exhaustion.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 18), // 10us ~ 1.3s
		})
	RemotePlanProcessRequestDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_plan",
			Name:      "process_request_duration_seconds",
			Help:      "Total processRequest duration (session acquire + exec + streaming). This is the full server-side latency.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 18), // 0.1ms ~ 13s
		})
}
