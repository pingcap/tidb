// Copyright 2016 PingCAP, Inc.
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

package tidb

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	sessionExecuteParseDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_execute_parse_duration",
			Help:      "Bucketed histogram of processing time (s) in parse SQL.",
			Buckets:   prometheus.LinearBuckets(0.00004, 0.00001, 13),
		})
	sessionExecuteCompileDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_execute_compile_duration",
			Help:      "Bucketed histogram of processing time (s) in query optimize.",
			Buckets:   prometheus.LinearBuckets(0.00004, 0.00001, 13),
		})
	sessionExecuteRunDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_execute_run_duration",
			Help:      "Bucketed histogram of processing time (s) in running executor.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 13),
		})
	schemaLeaseErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "schema_lease_error_counter",
			Help:      "Counter of schema lease error",
		}, []string{"type"})
	sessionRetry = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_retry",
			Help:      "Bucketed histogram of session retry count.",
			Buckets:   prometheus.LinearBuckets(0, 1, 10),
		})
)

func init() {
	prometheus.MustRegister(sessionExecuteParseDuration)
	prometheus.MustRegister(sessionExecuteCompileDuration)
	prometheus.MustRegister(sessionExecuteRunDuration)
	prometheus.MustRegister(schemaLeaseErrorCounter)
}
