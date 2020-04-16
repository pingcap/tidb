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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics for the timestamp oracle.
var (
	TSFutureWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "pdclient",
			Name:      "ts_future_wait_seconds",
			Help:      "Bucketed histogram of seconds cost for waiting timestamp future.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 24), // 5us ~ 40s
		})
)
