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

func TestRUV2ExecutorCounterReturnsCachedKnownLabels(t *testing.T) {
	cases := []struct {
		level    int
		label    string
		expected prometheus.Counter
	}{
		{1, "BatchPointGetExec", ruv2ExecutorL1BatchPointGetExec},
		{1, "PointGetExecutor", ruv2ExecutorL1PointGetExecutor},
		{1, "LimitExec", ruv2ExecutorL1LimitExec},
		{2, "ExpandExec", ruv2ExecutorL2ExpandExec},
		{2, "HashAggExec", ruv2ExecutorL2HashAggExec},
		{2, "HashJoinExec", ruv2ExecutorL2HashJoinExec},
		{2, "HashJoinV1Exec", ruv2ExecutorL2HashJoinV1Exec},
		{2, "HashJoinV2Exec", ruv2ExecutorL2HashJoinV2Exec},
		{2, "IndexLookUpJoin", ruv2ExecutorL2IndexLookUpJoin},
		{2, "IndexLookUpMergeJoin", ruv2ExecutorL2IndexLookUpMergeJoin},
		{2, "IndexNestedLoopHashJoin", ruv2ExecutorL2IndexNestedLoopHashJoin},
		{2, "IndexLookUpExecutor", ruv2ExecutorL2IndexLookUpExec},
		{2, "IndexReaderExecutor", ruv2ExecutorL2IndexReaderExec},
		{2, "MemTableReaderExec", ruv2ExecutorL2MemTableReaderExec},
		{2, "MergeJoinExec", ruv2ExecutorL2MergeJoinExec},
		{2, "ProjectionExec", ruv2ExecutorL2ProjectionExec},
		{2, "SelectionExec", ruv2ExecutorL2SelectionExec},
		{2, "TableDualExec", ruv2ExecutorL2TableDualExec},
		{2, "TableReaderExecutor", ruv2ExecutorL2TableReaderExec},
		{2, "TopNExec", ruv2ExecutorL2TopNExec},
		{2, "UnionScanExec", ruv2ExecutorL2UnionScanExec},
		{2, "SelectLockExec", ruv2ExecutorL2SelectLockExec},
		{2, "WindowExec", ruv2ExecutorL2WindowExec},
		{3, "SortExec", ruv2ExecutorL3SortExec},
		{3, "StreamAggExec", ruv2ExecutorL3StreamAggExec},
	}

	for _, tc := range cases {
		require.NotNil(t, tc.expected, tc.label)
		require.True(t, tc.expected == RUV2ExecutorCounter(tc.level, tc.label), tc.label)
	}
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
