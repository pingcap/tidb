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

var (
	// ModelInferenceCounter records model inference attempts by type and result.
	ModelInferenceCounter *prometheus.CounterVec
	// ModelInferenceDuration records model inference latency by type and result.
	ModelInferenceDuration *prometheus.HistogramVec
	// ModelSessionCacheCounter records model session cache events.
	ModelSessionCacheCounter *prometheus.CounterVec
)

// InitModelMetrics initializes model serving metrics.
func InitModelMetrics() {
	ModelInferenceCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "model",
			Name:      "inference_total",
			Help:      "Counter of model inference.",
		}, []string{LblType, LblResult},
	)

	ModelInferenceDuration = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "model",
			Name:      "inference_duration_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
			Help:      "Histogram of model inference duration.",
		}, []string{LblType, LblResult},
	)

	ModelSessionCacheCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "model",
			Name:      "session_cache_total",
			Help:      "Counter of model session cache events.",
		}, []string{LblType},
	)
}
