// Copyright 2024 PingCAP, Inc.
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
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// InfoSchemaV2CacheCounter records the counter of infoschema v2 cache hit/miss/evict.
	InfoSchemaV2CacheCounter *prometheus.CounterVec
	// InfoSchemaV2CacheMemUsage records the memory size of infoschema v2 cache.
	InfoSchemaV2CacheMemUsage prometheus.Gauge
	// InfoSchemaV2CacheObjCnt records the table count of infoschema v2 cache.
	InfoSchemaV2CacheObjCnt prometheus.Gauge
	// InfoSchemaV2CacheMemLimit records the memory limit of infoschema v2 cache.
	InfoSchemaV2CacheMemLimit prometheus.Gauge
	// TableByNameDuration records the duration of TableByName API for infoschema v2.
	TableByNameDuration *prometheus.HistogramVec
	// TableByNameHitDuration is TableByNameDuration with label type "hit"
	TableByNameHitDuration prometheus.Observer
	// TableByNameMissDuration is TableByNameDuration with label type "miss"
	TableByNameMissDuration prometheus.Observer
)

// InitInfoSchemaV2Metrics intializes infoschema v2 related metrics.
func InitInfoSchemaV2Metrics() {
	InfoSchemaV2CacheCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "infoschema_v2_cache",
			Help:      "infoschema cache v2 hit, evict and miss number",
		}, []string{LblType})
	InfoSchemaV2CacheObjCnt = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "infoschema_v2_cache_count",
			Help:      "infoschema cache v2 table count",
		})
	InfoSchemaV2CacheMemUsage = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "infoschema_v2_cache_size",
			Help:      "infoschema cache v2 size",
		})
	InfoSchemaV2CacheMemLimit = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "infoschema_v2_cache_limit",
			Help:      "infoschema cache v2 limit",
		})

	TableByNameDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "infoschema",
			Name:      "table_by_name_duration_nanoseconds",
			Help:      "infoschema v2 TableByName API duration",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1us ~ 1,000,000us
		}, []string{LblType})

	TableByNameHitDuration = TableByNameDuration.WithLabelValues("hit")
	TableByNameMissDuration = TableByNameDuration.WithLabelValues("miss")
}
