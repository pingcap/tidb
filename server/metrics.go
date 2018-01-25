// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/terror"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	queryHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "handle_query_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled queries.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		})

	queryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "query_total",
			Help:      "Counter of queries.",
		}, []string{"type", "status"})

	connGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "connections",
			Help:      "Number of connections.",
		})

	executeErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "execute_error",
			Help:      "Counter of execute errors.",
		}, []string{"type"})

	criticalErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "critical_error",
			Help:      "Counter of critical errors.",
		})

	// panicCounter measures the count of panics.
	panicCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "panic",
			Help:      "Counter of panic.",
		})
)

func init() {
	prometheus.MustRegister(queryHistogram)
	prometheus.MustRegister(queryCounter)
	prometheus.MustRegister(connGauge)
	prometheus.MustRegister(criticalErrorCounter)
	prometheus.MustRegister(panicCounter)
}

func executeErrorToLabel(err error) string {
	err = errors.Cause(err)
	switch x := err.(type) {
	case *terror.Error:
		return x.Class().String() + ":" + strconv.Itoa(int(x.Code()))
	default:
		return "unknown"
	}
}
