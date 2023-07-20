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
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	MissCounter   prometheus.Counter
	HitCounter    prometheus.Counter
	UpdateCounter prometheus.Counter
	DelCounter    prometheus.Counter
	EvictCounter  prometheus.Counter
	CostGauge     prometheus.Gauge
	CapacityGauge prometheus.Gauge
)

func init() {
	initMetricsVars()
}

// initMetricsVars init copr metrics vars.
func initMetricsVars() {
	MissCounter = metrics.StatsCacheLRUCounter.WithLabelValues("miss")
	HitCounter = metrics.StatsCacheLRUCounter.WithLabelValues("hit")
	UpdateCounter = metrics.StatsCacheLRUCounter.WithLabelValues("update")
	DelCounter = metrics.StatsCacheLRUCounter.WithLabelValues("del")
	EvictCounter = metrics.StatsCacheLRUCounter.WithLabelValues("evict")
	CostGauge = metrics.StatsCacheLRUGauge.WithLabelValues("track")
	CapacityGauge = metrics.StatsCacheLRUGauge.WithLabelValues("capacity")
}
