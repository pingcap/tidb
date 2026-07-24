// Copyright 2025 PingCAP, Inc.
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
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

// TiKV client region cache metrics.
var (
	// RegionCacheOperationsCounter counts TiDB-side region cache operations, labeled by:
	// - type: operation kind (batch_locate|split_locations|split_buckets|on_send_fail|build_task|other)
	// - result: ok|err
	RegionCacheOperationsCounter *prometheus.CounterVec
)

// InitTiKVClientRegionCacheMetrics initializes metrics for TiKV client region cache usage in TiDB.
func InitTiKVClientRegionCacheMetrics() {
	RegionCacheOperationsCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: TiKVClient,
			Name:      "region_cache_operations_total",
			Help:      "TiDB region cache operations count.",
		}, []string{LblType, LblResult})
}


