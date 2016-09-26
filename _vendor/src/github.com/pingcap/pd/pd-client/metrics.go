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

package pd

import "github.com/prometheus/client_golang/prometheus"

var (
	cmdCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd_client",
			Subsystem: "cmd",
			Name:      "cmds_total",
			Help:      "Counter of cmds.",
		}, []string{"type"})

	cmdFailedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd_client",
			Subsystem: "cmd",
			Name:      "cmds_failed_total",
			Help:      "Counter of failed cmds.",
		}, []string{"type"})

	cmdDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd_client",
			Subsystem: "cmd",
			Name:      "handle_cmds_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled success cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	cmdFailedDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd_client",
			Subsystem: "cmd",
			Name:      "handle_failed_cmds_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of failed handled cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(cmdCounter)
	prometheus.MustRegister(cmdFailedCounter)
	prometheus.MustRegister(cmdDuration)
	prometheus.MustRegister(cmdFailedDuration)
}
