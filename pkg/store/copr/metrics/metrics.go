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

package copr

import (
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// copr metrics vars
var (
	CoprCacheCounterEvict prometheus.Counter
	CoprCacheCounterHit   prometheus.Counter
	CoprCacheCounterMiss  prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init copr metrics vars.
func InitMetricsVars() {
	CoprCacheCounterEvict = metrics.DistSQLCoprCacheCounter.WithLabelValues("evict")
	CoprCacheCounterHit = metrics.DistSQLCoprCacheCounter.WithLabelValues("hit")
	CoprCacheCounterMiss = metrics.DistSQLCoprCacheCounter.WithLabelValues("miss")
}
