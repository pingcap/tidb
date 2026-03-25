// Copyright 2018 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRetLabel(t *testing.T) {
	require.Equal(t, opSucc, RetLabel(nil))
	require.Equal(t, opFailed, RetLabel(errors.New("test error")))
}

func readGaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, gauge.Write(m))
	return m.GetGauge().GetValue()
}

func countCollectedMetrics(collector prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 16)
	collector.Collect(ch)
	close(ch)

	count := 0
	for range ch {
		count++
	}
	return count
}

func TestStmtSummaryMetricLabels(t *testing.T) {
	InitStmtSummaryMetrics()
	require.Equal(t, 0, countCollectedMetrics(StmtSummaryWindowRecordCount))
	require.Equal(t, 0, countCollectedMetrics(StmtSummaryWindowEvictedCount))

	SetStmtSummaryWindowMetrics(StmtSummaryTypeV1, 3, 1)
	require.Equal(t, 1, countCollectedMetrics(StmtSummaryWindowRecordCount))
	require.Equal(t, 1, countCollectedMetrics(StmtSummaryWindowEvictedCount))
	require.Equal(t, 3.0, readGaugeValue(t, StmtSummaryWindowRecordCount.WithLabelValues(StmtSummaryTypeV1)))
	require.Equal(t, 1.0, readGaugeValue(t, StmtSummaryWindowEvictedCount.WithLabelValues(StmtSummaryTypeV1)))

	SetStmtSummaryWindowMetrics(StmtSummaryTypeV2, 5, 2)
	require.Equal(t, 2, countCollectedMetrics(StmtSummaryWindowRecordCount))
	require.Equal(t, 2, countCollectedMetrics(StmtSummaryWindowEvictedCount))
	require.Equal(t, 5.0, readGaugeValue(t, StmtSummaryWindowRecordCount.WithLabelValues(StmtSummaryTypeV2)))
	require.Equal(t, 2.0, readGaugeValue(t, StmtSummaryWindowEvictedCount.WithLabelValues(StmtSummaryTypeV2)))
}
