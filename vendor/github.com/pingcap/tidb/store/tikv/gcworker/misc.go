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

package gcworker

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	gcWorkerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_worker_actions_total",
			Help:      "Counter of gc worker actions.",
		}, []string{"type"})

	gcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_seconds",
			Help:      "Bucketed histogram of gc duration.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 13),
		}, []string{"stage"})

	gcConfigGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_config",
			Help:      "Gauge of GC configs.",
		}, []string{"type"})

	gcJobFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_failure",
			Help:      "Counter of gc job failure.",
		}, []string{"type"})

	gcActionRegionResultCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_action_result",
			Help:      "Counter of gc action result on region level.",
		}, []string{"type"})

	gcRegionTooManyLocksCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_region_too_many_locks",
			Help:      "Counter of gc scan lock request more than once in the same region.",
		})
)

func init() {
	prometheus.MustRegister(gcWorkerCounter)
	prometheus.MustRegister(gcConfigGauge)
	prometheus.MustRegister(gcHistogram)
	prometheus.MustRegister(gcJobFailureCounter)
	prometheus.MustRegister(gcActionRegionResultCounter)
	prometheus.MustRegister(gcRegionTooManyLocksCounter)
}
