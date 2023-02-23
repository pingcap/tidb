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

package handle

import (
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	missCounter        prometheus.Counter
	hitCounter         prometheus.Counter
	updateCounter      prometheus.Counter
	delCounter         prometheus.Counter
	evictCounter       prometheus.Counter
	costGauge          prometheus.Gauge
	capacityGauge      prometheus.Gauge
	statsHealthyGauges []prometheus.Gauge

	dumpHistoricalStatsSuccessCounter prometheus.Counter
	dumpHistoricalStatsFailedCounter  prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init statistics metrics vars.
func InitMetricsVars() {
	missCounter = metrics.StatsCacheLRUCounter.WithLabelValues("miss")
	hitCounter = metrics.StatsCacheLRUCounter.WithLabelValues("hit")
	updateCounter = metrics.StatsCacheLRUCounter.WithLabelValues("update")
	delCounter = metrics.StatsCacheLRUCounter.WithLabelValues("del")
	evictCounter = metrics.StatsCacheLRUCounter.WithLabelValues("evict")
	costGauge = metrics.StatsCacheLRUGauge.WithLabelValues("track")
	capacityGauge = metrics.StatsCacheLRUGauge.WithLabelValues("capacity")

	statsHealthyGauges = []prometheus.Gauge{
		metrics.StatsHealthyGauge.WithLabelValues("[0,50)"),
		metrics.StatsHealthyGauge.WithLabelValues("[50,80)"),
		metrics.StatsHealthyGauge.WithLabelValues("[80,100)"),
		metrics.StatsHealthyGauge.WithLabelValues("[100,100]"),
		// [0,100] should always be the last
		metrics.StatsHealthyGauge.WithLabelValues("[0,100]"),
	}

	dumpHistoricalStatsSuccessCounter = metrics.HistoricalStatsCounter.WithLabelValues("dump", "success")
	dumpHistoricalStatsFailedCounter = metrics.HistoricalStatsCounter.WithLabelValues("dump", "fail")
}
