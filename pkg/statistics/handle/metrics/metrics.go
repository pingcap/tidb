// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// statistics metrics vars
var (
	StatsHealthyGauges []prometheus.Gauge

	DumpHistoricalStatsSuccessCounter prometheus.Counter
	DumpHistoricalStatsFailedCounter  prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init statistics metrics vars.
func InitMetricsVars() {
	StatsHealthyGauges = []prometheus.Gauge{
		metrics.StatsHealthyGauge.WithLabelValues("[0,50)"),
		metrics.StatsHealthyGauge.WithLabelValues("[50,80)"),
		metrics.StatsHealthyGauge.WithLabelValues("[80,100)"),
		metrics.StatsHealthyGauge.WithLabelValues("[100,100]"),
		// [0,100] should always be the last
		metrics.StatsHealthyGauge.WithLabelValues("[0,100]"),
	}

	DumpHistoricalStatsSuccessCounter = metrics.HistoricalStatsCounter.WithLabelValues("dump", "success")
	DumpHistoricalStatsFailedCounter = metrics.HistoricalStatsCounter.WithLabelValues("dump", "fail")
}
