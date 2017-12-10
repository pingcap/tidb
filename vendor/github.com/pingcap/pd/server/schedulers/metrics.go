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

package schedulers

import "github.com/prometheus/client_golang/prometheus"

var schedulerCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "event_count",
		Help:      "Counter of scheduler events.",
	}, []string{"type", "name"})

var schedulerStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "pd",
		Subsystem: "scheduler",
		Name:      "inner_status",
		Help:      "Inner status of the scheduler.",
	}, []string{"type", "name"})

func init() {
	prometheus.MustRegister(schedulerCounter)
	prometheus.MustRegister(schedulerStatus)
}
