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
	// MissCounter is the counter of missing cache.
	MissCounter prometheus.Counter
	// HitCounter is the counter of hitting cache.
	HitCounter prometheus.Counter
	// UpdateCounter is the counter of updating cache.
	UpdateCounter prometheus.Counter
	// DelCounter is the counter of deleting cache.
	DelCounter prometheus.Counter
	// EvictCounter is the counter of evicting cache.
	EvictCounter prometheus.Counter
	// CostGauge is the gauge of cost time.
	CostGauge prometheus.Gauge
	// CapacityGauge is the gauge of capacity.
	CapacityGauge prometheus.Gauge
)

func init() {
	initMetricsVars()
}

// initMetricsVars init copr metrics vars.
func initMetricsVars() {
	MissCounter = metrics.StatsCacheCounter.WithLabelValues("miss")
	HitCounter = metrics.StatsCacheCounter.WithLabelValues("hit")
	UpdateCounter = metrics.StatsCacheCounter.WithLabelValues("update")
	DelCounter = metrics.StatsCacheCounter.WithLabelValues("del")
	EvictCounter = metrics.StatsCacheCounter.WithLabelValues("evict")
	CostGauge = metrics.StatsCacheGauge.WithLabelValues("track")
	CapacityGauge = metrics.StatsCacheGauge.WithLabelValues("capacity")
}
