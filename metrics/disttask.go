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

var (
	DpDispatchingStatus string = "dispatching"
	DpWaitingStatus     string = "waiting"
	DpRunningStatus     string = "running"
	DpcompletedStatus   string = "completed"
)

// disttask metrics.
var (
	//DistTaskHistogram      *prometheus.HistogramVec
	DistTaskDispatcherGauge         *prometheus.GaugeVec
	DistTaskDispatcherDurationGauge *prometheus.GaugeVec
)

// InitDistTaskMetrics initializes disttask metrics.
func InitDistTaskMetrics() {
	DistTaskDispatcherGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "dispatcher",
			Help:      "Gauge of distdispatcher.",
		}, []string{LblTaskType, LblTaskStatus})

	DistTaskDispatcherDurationGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "dispatcher_duration",
			Help:      "Gauge of duration time (s) of distdispatcher.",
		}, []string{LblTaskType, LblTaskStatus, LblTaskID})
}
