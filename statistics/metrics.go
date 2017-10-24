// Copyright 2017 PingCAP, Inc.
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

package statistics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	autoAnalyzeHistgram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "auto_analyze_duration",
			Help:      "Bucketed histogram of processing time (s) of auto analyze.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		})

	autoAnalyzeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "auto_analyze_total",
			Help:      "Counter of auto analyze.",
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(autoAnalyzeHistgram)
	prometheus.MustRegister(autoAnalyzeCounter)
}
