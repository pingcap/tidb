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
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics for the domain package.
var (
	// LoadSchemaCounter records the counter of load schema.
	LoadSchemaCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "load_schema_total",
			Help:      "Counter of load schema",
		}, []string{LblType})

	// LoadSchemaDuration records the duration of load schema.
	LoadSchemaDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "load_schema_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in load schema.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		})

	// InfoCacheCounters are the counters of get/hit.
	InfoCacheCounters = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "infocache_counters",
			Help:      "Counters of infoCache: get/hit.",
		}, []string{LblType})
	// InfoCacheCounterGet is the total number of getting entry.
	InfoCacheCounterGet = "get"
	// InfoCacheCounterHit is the cache hit numbers for get.
	InfoCacheCounterHit = "hit"

	// LoadPrivilegeCounter records the counter of load privilege.
	LoadPrivilegeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "load_privilege_total",
			Help:      "Counter of load privilege",
		}, []string{LblType})

	// LoadSysVarCacheCounter records the counter of loading sysvars
	LoadSysVarCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "load_sysvarcache_total",
			Help:      "Counter of load sysvar cache",
		}, []string{LblType})

	SchemaValidatorStop       = "stop"
	SchemaValidatorRestart    = "restart"
	SchemaValidatorReset      = "reset"
	SchemaValidatorCacheEmpty = "cache_empty"
	SchemaValidatorCacheMiss  = "cache_miss"
	// HandleSchemaValidate records the counter of handling schema validate.
	HandleSchemaValidate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "handle_schema_validate",
			Help:      "Counter of handle schema validate",
		}, []string{LblType})
)
