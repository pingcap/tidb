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

import "github.com/prometheus/client_golang/prometheus"

// Metrics
// Query duration by query is QueryDurationHistogram in `server.go`.
var (
	RunawayCheckerCounter *prometheus.CounterVec
)

// InitResourceGroupMetrics initializes resource group metrics.
func InitResourceGroupMetrics() {
	RunawayCheckerCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "query_runaway_check",
			Help:      "Counter of query triggering runaway check.",
		}, []string{LblResourceGroup, LblType, LblAction})
}
