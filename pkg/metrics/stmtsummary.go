// Copyright 2026 PingCAP, Inc.
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

// Statement summary metrics.
var (
	// StmtSummaryWindowRecordCount is a gauge that tracks the number of distinct
	// statement digests in the current statement summary window.
	StmtSummaryWindowRecordCount prometheus.Gauge

	// StmtSummaryWindowEvictedCount is a gauge that tracks the number of LRU
	// evictions that have occurred in the current statement summary window.
	// This value resets to 0 when the window rotates.
	StmtSummaryWindowEvictedCount prometheus.Gauge
)

// InitStmtSummaryMetrics initializes statement summary metrics.
func InitStmtSummaryMetrics() {
	StmtSummaryWindowRecordCount = metricscommon.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "stmt_summary",
			Name:      "window_record_count",
			Help:      "The number of distinct statement digests in the current statement summary window.",
		})

	StmtSummaryWindowEvictedCount = metricscommon.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "stmt_summary",
			Name:      "window_evicted_count",
			Help:      "The number of LRU evictions in the current statement summary window.",
		})
}
