// Copyright 2024 PingCAP, Inc.
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
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// InfoSchemaV2CacheCounter records the counter of infoschema v2 cache hit/miss/evict.
	InfoSchemaV2CacheCounter *prometheus.CounterVec
)

// InitInfoSchemaV2Metrics intializes infoschema v2 related metrics.
func InitInfoSchemaV2Metrics() {
	InfoSchemaV2CacheCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "domain",
			Name:      "infoschema_v2_cache",
			Help:      "infoschema cache v2 hit, evict and miss number",
		}, []string{LblType})
}
