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

package domain

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	loadSchemaCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "load_schema_total",
			Help:      "Counter of load schema",
		}, []string{"type"})

	loadSchemaDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "load_schema_duration",
			Help:      "Bucketed histogram of processing time (s) in load schema.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
		})
)

func init() {
	prometheus.MustRegister(loadSchemaDuration)
	prometheus.MustRegister(loadSchemaCounter)
}
