// Copyright 2019 PingCAP, Inc.
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

// bindinfo metrics.
var (
	BindingCacheHitCounter  prometheus.Counter
	BindingCacheMissCounter prometheus.Counter
	BindingCacheMemUsage    prometheus.Gauge
	BindingCacheMemLimit    prometheus.Gauge
	BindingCacheNumBindings prometheus.Gauge
	// BindingDigestEmptyCounter counts slow-logged statements whose
	// Binding_digest stayed empty, labeled by reason: non-dml
	// (no binding semantics, by design), restore-fail (DML-shaped but
	// normalization failed), execute-unresolved (EXECUTE whose memo never
	// resolved — inner statement without binding semantics or prepare name
	// not found). Counted once per statement at slow log flush time, so the
	// scope matches the slow log field itself; empties on statements that
	// are not slow-logged are not counted.
	BindingDigestEmptyCounter *prometheus.CounterVec
)

// Binding-digest empty-reason label values.
const (
	// LblBindingDigestNonDML labels statements without binding semantics.
	LblBindingDigestNonDML = "non-dml"
	// LblBindingDigestRestoreFail labels DML whose binding normalization failed.
	LblBindingDigestRestoreFail = "restore-fail"
	// LblBindingDigestExecuteUnresolved labels EXECUTE statements whose
	// binding digest never resolved — inner statement without binding
	// semantics, unknown prepare name, or a failed inner normalization.
	LblBindingDigestExecuteUnresolved = "execute-unresolved"
)

// InitBindInfoMetrics initializes bindinfo metrics.
func InitBindInfoMetrics() {
	BindingCacheHitCounter = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "binding_cache_hit_total",
			Help:      "Counter of binding cache hit.",
		})
	BindingCacheMissCounter = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "binding_cache_miss_total",
			Help:      "Counter of binding cache miss.",
		})
	BindingCacheMemUsage = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "binding_cache_mem_usage",
			Help:      "Memory usage of binding cache.",
		})
	BindingCacheMemLimit = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "binding_cache_mem_limit",
			Help:      "Memory limit of binding cache.",
		})
	BindingCacheNumBindings = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "binding_cache_num_bindings",
			Help:      "Number of bindings in binding cache.",
		})
	BindingDigestEmptyCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "binding_digest_empty_total",
			Help:      "Counter of slow-logged statements with an empty binding digest by reason: non-dml, restore-fail, execute-unresolved.",
		}, []string{"reason"})
}
