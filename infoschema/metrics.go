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

package infoschema

import (
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	getLatestCounter  prometheus.Counter
	getTSCounter      prometheus.Counter
	getVersionCounter prometheus.Counter

	hitLatestCounter  prometheus.Counter
	hitTSCounter      prometheus.Counter
	hitVersionCounter prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init infoschema metrics vars.
func InitMetricsVars() {
	getLatestCounter = metrics.InfoCacheCounters.WithLabelValues("get", "latest")
	getTSCounter = metrics.InfoCacheCounters.WithLabelValues("get", "ts")
	getVersionCounter = metrics.InfoCacheCounters.WithLabelValues("get", "version")

	hitLatestCounter = metrics.InfoCacheCounters.WithLabelValues("hit", "latest")
	hitTSCounter = metrics.InfoCacheCounters.WithLabelValues("hit", "ts")
	hitVersionCounter = metrics.InfoCacheCounters.WithLabelValues("hit", "version")
}
