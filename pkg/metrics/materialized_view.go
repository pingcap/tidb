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

// MVServiceRefreshScheduleDurationHistogram tracks the interval between
// successful scheduled refreshes.
var MVServiceRefreshScheduleDurationHistogram prometheus.Histogram

// InitMVMetrics initializes materialized view refresh metrics.
func InitMVMetrics() {
	MVServiceRefreshScheduleDurationHistogram = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_refresh_schedule_duration_seconds",
			Help:      "Bucketed histogram of the interval between two successful MV service refreshes, excluding the current refresh duration.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 25), // 1s ~ 194d
		})
}
