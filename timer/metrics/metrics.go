// Copyright 2023 PingCAP, Inc.
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
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TimerEventCounter is the counter for timer events
	TimerEventCounter *prometheus.CounterVec
)

// InitTimerMetrics initializes timers metrics.
func InitTimerMetrics() {
	TimerEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "timer_event_count",
			Help:      "Counter of timer event.",
		}, []string{"scope", "type"})
}

// TimerHookWorkerCounter creates a counter for a hook's event
func TimerHookWorkerCounter(hookClass string, event string) prometheus.Counter {
	return TimerScopeCounter(fmt.Sprintf("hook.%s", hookClass), event)
}

// TimerScopeCounter creates a counter for a scope
func TimerScopeCounter(scope string, event string) prometheus.Counter {
	return TimerEventCounter.WithLabelValues(scope, event)
}
