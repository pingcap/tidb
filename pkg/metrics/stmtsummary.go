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
	"sync"

	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

// Statement summary metrics.
const (
	// StmtSummaryTypeV1 marks metrics reported by the legacy statement summary implementation.
	StmtSummaryTypeV1 = "v1"
	// StmtSummaryTypeV2 marks metrics reported by the persistent statement summary implementation.
	StmtSummaryTypeV2 = "v2"
)

var (
	// StmtSummaryWindowRecordCount is a gauge that tracks the number of statement
	// summary records in the current statement summary window.
	StmtSummaryWindowRecordCount *prometheus.GaugeVec

	// StmtSummaryWindowEvictedCount is a gauge that tracks the number of LRU
	// evictions that have occurred in the current statement summary window.
	// This value resets to 0 when the window rotates.
	StmtSummaryWindowEvictedCount *prometheus.GaugeVec

	stmtSummaryWindowRecordCountV1  prometheus.Gauge
	stmtSummaryWindowRecordCountV2  prometheus.Gauge
	stmtSummaryWindowEvictedCountV1 prometheus.Gauge
	stmtSummaryWindowEvictedCountV2 prometheus.Gauge

	stmtSummaryWindowMetricsMu sync.Mutex
)

// InitStmtSummaryMetrics initializes statement summary metrics.
func InitStmtSummaryMetrics() {
	StmtSummaryWindowRecordCount = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "stmt_summary",
			Name:      "window_record_count",
			Help:      "The number of statement summary records currently tracked by statement summary.",
		}, []string{LblType})

	StmtSummaryWindowEvictedCount = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "stmt_summary",
			Name:      "window_evicted_count",
			Help:      "The number of LRU evictions in the current statement summary window.",
		}, []string{LblType})

	stmtSummaryWindowMetricsMu.Lock()
	stmtSummaryWindowRecordCountV1 = nil
	stmtSummaryWindowRecordCountV2 = nil
	stmtSummaryWindowEvictedCountV1 = nil
	stmtSummaryWindowEvictedCountV2 = nil
	stmtSummaryWindowMetricsMu.Unlock()
}

// SetStmtSummaryWindowMetrics reports statement summary window metrics for a given implementation type.
func SetStmtSummaryWindowMetrics(typ string, recordCount, evictedCount float64) {
	switch typ {
	case StmtSummaryTypeV1:
		recordGauge, evictedGauge := getStmtSummaryWindowMetricsLocked(typ)
		recordGauge.Set(recordCount)
		evictedGauge.Set(evictedCount)
	case StmtSummaryTypeV2:
		recordGauge, evictedGauge := getStmtSummaryWindowMetricsLocked(typ)
		recordGauge.Set(recordCount)
		evictedGauge.Set(evictedCount)
	default:
		StmtSummaryWindowRecordCount.WithLabelValues(typ).Set(recordCount)
		StmtSummaryWindowEvictedCount.WithLabelValues(typ).Set(evictedCount)
	}
}

func getStmtSummaryWindowMetricsLocked(typ string) (prometheus.Gauge, prometheus.Gauge) {
	stmtSummaryWindowMetricsMu.Lock()
	defer stmtSummaryWindowMetricsMu.Unlock()

	switch typ {
	case StmtSummaryTypeV1:
		if stmtSummaryWindowRecordCountV1 == nil {
			stmtSummaryWindowRecordCountV1 = StmtSummaryWindowRecordCount.WithLabelValues(typ)
			stmtSummaryWindowEvictedCountV1 = StmtSummaryWindowEvictedCount.WithLabelValues(typ)
		}
		return stmtSummaryWindowRecordCountV1, stmtSummaryWindowEvictedCountV1
	case StmtSummaryTypeV2:
		if stmtSummaryWindowRecordCountV2 == nil {
			stmtSummaryWindowRecordCountV2 = StmtSummaryWindowRecordCount.WithLabelValues(typ)
			stmtSummaryWindowEvictedCountV2 = StmtSummaryWindowEvictedCount.WithLabelValues(typ)
		}
		return stmtSummaryWindowRecordCountV2, stmtSummaryWindowEvictedCountV2
	default:
		return nil, nil
	}
}
