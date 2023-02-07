// Copyright 2022 PingCAP, Inc.
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

var (
	// EMACPUUsageGauge means exponential moving average of CPU usage
	EMACPUUsageGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "rm",
		Name:      "ema_cpu_usage",
		Help:      "exponential moving average of CPU usage",
	})
	// PoolConcurrencyCounter means how much concurrency in the pool
	PoolConcurrencyCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "rm",
			Name:      "pool_concurrency",
			Help:      "How many concurrency in the pool",
		}, []string{LblType})
)
