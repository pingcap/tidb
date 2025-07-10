// Copyright 2018 PingCAP, Inc.
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

// Metrics for the domain package.
var (
	// LeaseExpireTime records the lease expire time.
	LeaseExpireTime prometheus.Gauge

	// LoadSchemaCounter records the counter of load schema.
	LoadSchemaCounter *prometheus.CounterVec

	// LoadSchemaDuration records the duration of load schema.
	LoadSchemaDuration *prometheus.HistogramVec

	// InfoCacheCounters are the counters of get/hit.
	InfoCacheCounters *prometheus.CounterVec

	// InfoCacheCounterGet is the total number of getting entry.
	InfoCacheCounterGet = "get"
	// InfoCacheCounterHit is the cache hit numbers for get.
	InfoCacheCounterHit = "hit"

	// LoadPrivilegeCounter records the counter of load privilege.
	LoadPrivilegeCounter *prometheus.CounterVec

	// LoadSysVarCacheCounter records the counter of loading sysvars
	LoadSysVarCacheCounter *prometheus.CounterVec

	SchemaValidatorStop       = "stop"
	SchemaValidatorRestart    = "restart"
	SchemaValidatorReset      = "reset"
	SchemaValidatorCacheEmpty = "cache_empty"
	SchemaValidatorCacheMiss  = "cache_miss"
	// HandleSchemaValidate records the counter of handling schema validate.
	HandleSchemaValidate *prometheus.CounterVec
)

// InitDomainMetrics initializes domain metrics.
func InitDomainMetrics() {
	LeaseExpireTime = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "lease_expire_time",
			Help:      "When the last time the lease is expired, it is in seconds",
		})

	LoadSchemaCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "load_schema_total",
			Help:      "Counter of load schema",
		}, []string{LblType})

	LoadSchemaDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "load_schema_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in load schema.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblAction})

	InfoCacheCounters = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "infocache_counters",
			Help:      "Counters of infoCache: get/hit.",
		}, []string{LblAction, LblType})

	LoadPrivilegeCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "load_privilege_total",
			Help:      "Counter of load privilege",
		}, []string{LblType})

	LoadSysVarCacheCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "load_sysvarcache_total",
			Help:      "Counter of load sysvar cache",
		}, []string{LblType})

	HandleSchemaValidate = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "handle_schema_validate",
			Help:      "Counter of handle schema validate",
		}, []string{LblType})
}
