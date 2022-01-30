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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import "github.com/prometheus/client_golang/prometheus"

// Top SQL metrics.
var (
	TopSQLIgnoredCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "ignored_total",
			Help:      "Counter of ignored top-sql metrics (register-sql, register-plan, collect-data and report-data), normally it should be 0.",
		}, []string{LblType})

	TopSQLReportDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "report_duration_seconds",
			Help:      "Bucket histogram of reporting time (s) to the top-sql agent",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 24), // 1ms ~ 2.3h
		}, []string{LblType, LblResult})

	TopSQLReportDataHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "report_data_total",
			Help:      "Bucket histogram of reporting records/sql/plan count to the top-sql agent.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1 ~ 524288
		}, []string{LblType})
)
