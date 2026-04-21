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

	// EMASendCold counts cop RPCs dispatched while the per-logical-scan EMA
	// was not yet ready; the request carries PredictedReadBytes=0 and PD
	// skips pre-charge. Denominator is all cop RPC sends, so
	// (EMASendCold + EMASendReady) is the authoritative "total read RPCs"
	// basis for pre-charge coverage ratios at the TiDB side.
	EMASendCold prometheus.Counter
	// EMASendReady counts cop RPCs dispatched while the per-logical-scan EMA
	// was already ready; PredictedReadBytes > 0 is shipped as a pre-charge
	// hint to PD.
	EMASendReady prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init copr metrics vars.
func InitMetricsVars() {
	CoprCacheCounterEvict = metrics.DistSQLCoprCacheCounter.WithLabelValues("evict")
	CoprCacheCounterHit = metrics.DistSQLCoprCacheCounter.WithLabelValues("hit")
	CoprCacheCounterMiss = metrics.DistSQLCoprCacheCounter.WithLabelValues("miss")

	EMASendCold = metrics.DistSQLCoprEMASend.WithLabelValues("cold")
	EMASendReady = metrics.DistSQLCoprEMASend.WithLabelValues("ready")
}
