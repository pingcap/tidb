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

package tidb

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	sessionExecuteDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_execute_duration",
			Help:      "Bucketed histogram of processing time (s) in each stage of a SQL executation.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"stage"})

	// Step of each stage in a SQL executation.
	stageParse   = "stage_parse"
	stageCompile = "stage_compile"
	stageRun     = "stage_run"
)

func init() {
	prometheus.MustRegister(sessionExecuteDuration)
}
