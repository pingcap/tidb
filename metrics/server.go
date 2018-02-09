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
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/terror"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics
var (
	QueryDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "handle_query_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled queries.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		})

	QueryTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "query_total",
			Help:      "Counter of queries.",
		}, []string{"type", "status"})

	ConnGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "connections",
			Help:      "Number of connections.",
		})

	ExecuteErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "execute_error",
			Help:      "Counter of execute errors.",
		}, []string{"type"})

	CriticalErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "critical_error",
			Help:      "Counter of critical errors.",
		})
)

func init() {
	prometheus.MustRegister(QueryDurationHistogram)
	prometheus.MustRegister(QueryTotalCounter)
	prometheus.MustRegister(ConnGauge)
	prometheus.MustRegister(ExecuteErrorCounter)
	prometheus.MustRegister(CriticalErrorCounter)
}

// ExecuteErrorToLabel converts an execute error to label.
func ExecuteErrorToLabel(err error) string {
	err = errors.Cause(err)
	switch x := err.(type) {
	case *terror.Error:
		return x.Class().String() + ":" + strconv.Itoa(int(x.Code()))
	default:
		return "unknown"
	}
}
