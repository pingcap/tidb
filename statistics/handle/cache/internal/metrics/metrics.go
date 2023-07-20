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
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	StatsCacheCounterEvict prometheus.Counter
	StatsCacheCounterHit   prometheus.Counter
	StatsCacheCounterMiss  prometheus.Counter
)

func init() {
	initMetricsVars()
}

// initMetricsVars init copr metrics vars.
func initMetricsVars() {
	StatsCacheCounterEvict = metrics.StatsCacheCounter.WithLabelValues("evict")
	StatsCacheCounterHit = metrics.StatsCacheCounter.WithLabelValues("hit")
	StatsCacheCounterMiss = metrics.StatsCacheCounter.WithLabelValues("miss")
}
