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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ResettablePlanCacheCounterFortTest be used to support reset counter in test.
	ResettablePlanCacheCounterFortTest = false
)

// Metrics
var (
	PacketIOCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "packet_io_bytes",
			Help:      "Counters of packet IO bytes.",
		}, []string{LblType})

	QueryDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "handle_query_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled queries.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblSQLType, LblDb})

	QueryTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "query_total",
			Help:      "Counter of queries.",
		}, []string{LblType, LblResult})

	AffectedRowsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "affected_rows",
			Help:      "Counters of server affected rows.",
		}, []string{LblSQLType})

	ConnGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "connections",
			Help:      "Number of connections.",
		})

	DisconnectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "disconnection_total",
			Help:      "Counter of connections disconnected.",
		}, []string{LblResult})

	PreparedStmtGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "server",
		Name:      "prepared_stmts",
		Help:      "number of prepared statements.",
	})

	ExecuteErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "execute_error_total",
			Help:      "Counter of execute errors.",
		}, []string{LblType, LblDb})

	CriticalErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "critical_error_total",
			Help:      "Counter of critical errors.",
		})

	EventStart        = "start"
	EventGracefulDown = "graceful_shutdown"
	// Eventkill occurs when the server.Kill() function is called.
	EventKill          = "kill"
	EventClose         = "close"
	ServerEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "event_total",
			Help:      "Counter of tidb-server event.",
		}, []string{LblType})

	TimeJumpBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "monitor",
			Name:      "time_jump_back_total",
			Help:      "Counter of system time jumps backward.",
		})

	PlanCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "plan_cache_total",
			Help:      "Counter of query using plan cache.",
		}, []string{LblType})

	PlanCacheMissCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "plan_cache_miss_total",
			Help:      "Counter of plan cache miss.",
		}, []string{LblType})

	PlanCacheInstanceMemoryUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "plan_cache_instance_memory_usage",
			Help:      "Total plan cache memory usage of all sessions in a instance",
		}, []string{LblType})

	PlanCacheInstancePlanNumCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "plan_cache_instance_plan_num_total",
			Help:      "Counter of plan of all prepared plan cache in a instance",
		}, []string{LblType})

	ReadFromTableCacheCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "read_from_tablecache_total",
			Help:      "Counter of query read from table cache.",
		},
	)

	HandShakeErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "handshake_error_total",
			Help:      "Counter of hand shake error.",
		},
	)

	GetTokenDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "get_token_duration_seconds",
			Help:      "Duration (us) for getting token, it should be small until concurrency limit is reached.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1us ~ 528s
		})

	NumOfMultiQueryHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "multi_query_num",
			Help:      "The number of queries contained in a multi-query statement.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1 ~ 1048576
		})

	TotalQueryProcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "slow_query_process_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of of slow queries.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblSQLType})
	TotalCopProcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "slow_query_cop_duration_seconds",
			Help:      "Bucketed histogram of all cop processing time (s) of of slow queries.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblSQLType})
	TotalCopWaitHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "slow_query_wait_duration_seconds",
			Help:      "Bucketed histogram of all cop waiting time (s) of of slow queries.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblSQLType})

	MaxProcs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "maxprocs",
			Help:      "The value of GOMAXPROCS.",
		})

	GOGC = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "gogc",
			Help:      "The value of GOGC",
		})

	ConnIdleDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "conn_idle_duration_seconds",
			Help:      "Bucketed histogram of connection idle time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblInTxn})

	ServerInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "info",
			Help:      "Indicate the tidb server info, and the value is the start timestamp (s).",
		}, []string{LblVersion, LblHash})

	TokenGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "tokens",
			Help:      "The number of concurrent executing session",
		},
	)

	ConfigStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "config",
			Name:      "status",
			Help:      "Status of the TiDB server configurations.",
		}, []string{LblType})

	TiFlashQueryTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "tiflash_query_total",
			Help:      "Counter of TiFlash queries.",
		}, []string{LblType, LblResult})

	TiFlashFailedMPPStoreState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "tiflash_failed_store",
			Help:      "Statues of failed tiflash mpp store,-1 means detector heartbeat,0 means reachable,1 means abnormal.",
		}, []string{LblAddress})

	PDAPIExecutionHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "pd_api_execution_duration_seconds",
			Help:      "Bucketed histogram of all pd api execution time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType})

	PDAPIRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "pd_api_request_total",
			Help:      "Counter of the pd http api requests",
		}, []string{LblType, LblResult})

	CPUProfileCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "cpu_profile_total",
			Help:      "Counter of cpu profiling",
		})

	LoadTableCacheDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "load_table_cache_seconds",
			Help:      "Duration (us) for loading table cache.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1us ~ 528s
		})

	RCCheckTSWriteConfilictCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "rc_check_ts_conflict_total",
			Help:      "Counter of WriteConflict caused by RCCheckTS.",
		}, []string{LblType})
)

// ExecuteErrorToLabel converts an execute error to label.
func ExecuteErrorToLabel(err error) string {
	err = errors.Cause(err)
	switch x := err.(type) {
	case *terror.Error:
		return string(x.RFCCode())
	default:
		return "unknown"
	}
}
