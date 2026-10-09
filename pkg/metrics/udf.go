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

package metrics

import (
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

// UDF metrics labels
const (
	LblUDFName     = "udf_name"
	LblUDFLanguage = "language"
	LblUDFSchema   = "schema"
)

var (
	// UDFExecutionDuration records the duration of UDF executions.
	UDFExecutionDuration *prometheus.HistogramVec

	// UDFExecutionCounter records the total number of UDF executions.
	UDFExecutionCounter *prometheus.CounterVec

	// UDFErrorCounter records the number of UDF execution errors.
	UDFErrorCounter *prometheus.CounterVec

	// UDFActiveGauge records the number of currently executing UDFs.
	UDFActiveGauge *prometheus.GaugeVec

	// UDFCacheHitCounter records UDF cache hits/misses.
	UDFCacheHitCounter *prometheus.CounterVec

	// ProcedureExecutionDuration records the duration of stored procedure executions.
	ProcedureExecutionDuration *prometheus.HistogramVec

	// ProcedureExecutionCounter records the total number of stored procedure executions.
	ProcedureExecutionCounter *prometheus.CounterVec

	// ProcedureErrorCounter records the number of stored procedure execution errors.
	ProcedureErrorCounter *prometheus.CounterVec
)

// InitUDFMetrics initializes UDF metrics.
func InitUDFMetrics() {
	UDFExecutionDuration = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "udf",
			Name:      "execution_duration_seconds",
			Help:      "Bucketed histogram of UDF execution duration in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms ~ 52s
		}, []string{LblUDFName, LblUDFLanguage, LblUDFSchema},
	)

	UDFExecutionCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "udf",
			Name:      "execution_total",
			Help:      "Counter of UDF executions.",
		}, []string{LblUDFName, LblUDFLanguage, LblUDFSchema, LblResult},
	)

	UDFErrorCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "udf",
			Name:      "error_total",
			Help:      "Counter of UDF execution errors.",
		}, []string{LblUDFName, LblUDFLanguage, LblUDFSchema, LblType},
	)

	UDFActiveGauge = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "udf",
			Name:      "active_executions",
			Help:      "Number of currently executing UDFs.",
		}, []string{LblUDFLanguage},
	)

	UDFCacheHitCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "udf",
			Name:      "cache_total",
			Help:      "Counter of UDF cache hits and misses.",
		}, []string{LblType}, // "hit" or "miss"
	)

	ProcedureExecutionDuration = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "procedure",
			Name:      "execution_duration_seconds",
			Help:      "Bucketed histogram of stored procedure execution duration in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms ~ 52s
		}, []string{LblUDFName, LblUDFSchema},
	)

	ProcedureExecutionCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "procedure",
			Name:      "execution_total",
			Help:      "Counter of stored procedure executions.",
		}, []string{LblUDFName, LblUDFSchema, LblResult},
	)

	ProcedureErrorCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "procedure",
			Name:      "error_total",
			Help:      "Counter of stored procedure execution errors.",
		}, []string{LblUDFName, LblUDFSchema, LblType},
	)
}
