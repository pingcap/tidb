// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// infoschema metrics vars
var (
	GetLatestCounter  prometheus.Counter
	GetTSCounter      prometheus.Counter
	GetVersionCounter prometheus.Counter

	HitLatestCounter  prometheus.Counter
	HitTSCounter      prometheus.Counter
	HitVersionCounter prometheus.Counter

	LoadSchemaCounterSnapshot prometheus.Counter

	LoadSchemaDurationTotal    prometheus.Observer
	LoadSchemaDurationLoadDiff prometheus.Observer
	LoadSchemaDurationLoadAll  prometheus.Observer
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init infoschema metrics vars.
func InitMetricsVars() {
	GetLatestCounter = metrics.InfoCacheCounters.WithLabelValues("get", "latest")
	GetTSCounter = metrics.InfoCacheCounters.WithLabelValues("get", "ts")
	GetVersionCounter = metrics.InfoCacheCounters.WithLabelValues("get", "version")

	HitLatestCounter = metrics.InfoCacheCounters.WithLabelValues("hit", "latest")
	HitTSCounter = metrics.InfoCacheCounters.WithLabelValues("hit", "ts")
	HitVersionCounter = metrics.InfoCacheCounters.WithLabelValues("hit", "version")

	LoadSchemaCounterSnapshot = metrics.LoadSchemaCounter.WithLabelValues("snapshot")

	LoadSchemaDurationTotal = metrics.LoadSchemaDuration.WithLabelValues("total")
	LoadSchemaDurationLoadDiff = metrics.LoadSchemaDuration.WithLabelValues("load-diff")
	LoadSchemaDurationLoadAll = metrics.LoadSchemaDuration.WithLabelValues("load-all")
}
