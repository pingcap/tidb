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
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/intest"
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

func readCounterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, counter.Write(m))
	return m.GetCounter().GetValue()
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

func TestExplainRUMetrics(t *testing.T) {
	InitExplainRUMetrics()

	RecordExplainRUStatus("success")
	ObserveExplainRURenderDuration("success", 0.01)
	RecordExplainRUComponentSnapshot("ok")
	ObserveExplainRURow("plan", "", "projection", "read_billing_model", "runtime_chunk_avg", "v2", 1.25, 3.5, 24, 8)
	RecordReadBillingDemoStatement("success", "v2")
	RecordReadBillingDemoOperatorStatus("tidb", "projection_eval", "projection", "ok", "none", "v2")
	AddReadBillingDemoBaseUnits("tidb", "projection_eval", "projection", "input_rows", "runtime_chunk_bytes", "all", "v2", 3)
	ObserveReadBillingDemoRowWidth("tidb", "projection_eval", "projection", "runtime_chunk_avg", "v2", 8)

	require.Equal(t, 1.0, readCounterValue(t, ExplainRUStatementsCounter.WithLabelValues("success")))
	require.Equal(t, 1.0, readCounterValue(t, ExplainRUComponentSnapshotCounter.WithLabelValues("ok")))
	require.Equal(t, 1.25, readCounterValue(t, ExplainRUPreviewRUCounter.WithLabelValues("plan", "", "projection", "read_billing_model", "v2")))
	require.Equal(t, 3.5, readCounterValue(t, ExplainRUWorkRowsCounter.WithLabelValues("plan", "", "projection", "read_billing_model")))
	require.Equal(t, 24.0, readCounterValue(t, ExplainRUWorkBytesCounter.WithLabelValues("plan", "", "projection", "read_billing_model")))
	require.Equal(t, 1.0, readCounterValue(t, ReadBillingDemoStatementsCounter.WithLabelValues("success", "v2")))
	require.Equal(t, 1.0, readCounterValue(t, ReadBillingDemoOperatorStatusCounter.WithLabelValues("tidb", "projection_eval", "projection", "ok", "none", "v2")))
	require.Equal(t, 3.0, readCounterValue(t, ReadBillingDemoBaseUnitsCounter.WithLabelValues("tidb", "projection_eval", "projection", "input_rows", "runtime_chunk_bytes", "all", "v2")))

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(ExplainRUPreviewRUCounter))
	require.NoError(t, registry.Register(ExplainRUWorkRowsCounter))
	require.NoError(t, registry.Register(ExplainRUWorkBytesCounter))
	require.NoError(t, registry.Register(ExplainRURowWidthHistogram))
	require.NoError(t, registry.Register(ExplainRUStatementsCounter))
	require.NoError(t, registry.Register(ExplainRURenderDurationHistogram))
	require.NoError(t, registry.Register(ExplainRUComponentSnapshotCounter))
	require.NoError(t, registry.Register(ReadBillingDemoStatementsCounter))
	require.NoError(t, registry.Register(ReadBillingDemoOperatorStatusCounter))
	require.NoError(t, registry.Register(ReadBillingDemoBaseUnitsCounter))
	require.NoError(t, registry.Register(ReadBillingDemoRowWidthHistogram))
	families, err := registry.Gather()
	require.NoError(t, err)
	previewRUFamily := findMetricFamily(families, "tidb_explain_ru_preview_ru_total")
	require.NotNil(t, previewRUFamily)
	requireMetricFamilyHasLabels(t, previewRUFamily, "section", "component", "operator", "source", "weight_version")
	require.True(t, metricHasLabelValue(previewRUFamily.GetMetric()[0], "weight_version", "v2"))
	require.NotNil(t, findMetricFamily(families, "tidb_explain_ru_work_rows_total"))
	require.NotNil(t, findMetricFamily(families, "tidb_explain_ru_work_bytes_total"))
	rowWidthFamily := findMetricFamily(families, "tidb_explain_ru_row_width_bytes")
	require.NotNil(t, rowWidthFamily)
	requireMetricFamilyHasLabels(t, rowWidthFamily, "component", "operator", "source")
	require.True(t, metricHasLabelValue(rowWidthFamily.GetMetric()[0], "source", "runtime_chunk_avg"))
	statementFamily := findMetricFamily(families, "tidb_explain_ru_statements_total")
	require.NotNil(t, statementFamily)
	requireMetricFamilyHasLabels(t, statementFamily, "status")
	require.NotNil(t, findMetricFamily(families, "tidb_explain_ru_render_duration_seconds"))
	componentSnapshotFamily := findMetricFamily(families, "tidb_explain_ru_component_snapshot_total")
	require.NotNil(t, componentSnapshotFamily)
	requireMetricFamilyHasLabels(t, componentSnapshotFamily, "component_snapshot_status")
	readBillingStatementFamily := findMetricFamily(families, "tidb_read_billing_demo_statements_total")
	require.NotNil(t, readBillingStatementFamily)
	requireMetricFamilyHasLabels(t, readBillingStatementFamily, "status", "model_version")
	readBillingOperatorFamily := findMetricFamily(families, "tidb_read_billing_demo_operator_status_total")
	require.NotNil(t, readBillingOperatorFamily)
	requireMetricFamilyHasLabels(t, readBillingOperatorFamily, "site", "op_class", "operator_kind", "status", "reason", "model_version")
	readBillingBaseUnitFamily := findMetricFamily(families, "tidb_read_billing_demo_base_units_total")
	require.NotNil(t, readBillingBaseUnitFamily)
	requireMetricFamilyHasLabels(t, readBillingBaseUnitFamily, "site", "op_class", "operator_kind", "unit", "input_source", "input_side", "model_version")
	readBillingRowWidthFamily := findMetricFamily(families, "tidb_read_billing_demo_row_width_bytes")
	require.NotNil(t, readBillingRowWidthFamily)
	requireMetricFamilyHasLabels(t, readBillingRowWidthFamily, "site", "op_class", "operator_kind", "row_width_source", "model_version")
}

func TestExplainRUMetricsIgnoreEmptyLabelsAndMissingValues(t *testing.T) {
	InitExplainRUMetrics()

	RecordExplainRUStatus("")
	ObserveExplainRURenderDuration("", 0.01)
	RecordExplainRUComponentSnapshot("")
	ObserveExplainRURow("summary", "total_preview_ru", "", "summary_total", "", "", 1, -1, -1, -1)
	ObserveExplainRURow("summary", "total_preview_ru", "", "summary_total", "", "v2", 0, -1, -1, -1)
	ObserveExplainRURow("plan", "", "", "read_billing_model", "runtime_chunk_avg", "v2", -1, -1, -1, 32)
	RecordReadBillingDemoStatement("", "v2")
	RecordReadBillingDemoOperatorStatus("tidb", "projection_eval", "projection", "", "none", "v2")
	AddReadBillingDemoBaseUnits("tidb", "projection_eval", "projection", "input_rows", "runtime_chunk_bytes", "all", "v2", 0)
	ObserveReadBillingDemoRowWidth("tidb", "projection_eval", "projection", "runtime_chunk_avg", "v2", 0)

	require.Equal(t, 0, countCollectedMetrics(ExplainRUStatementsCounter))
	require.Equal(t, 0, countCollectedMetrics(ExplainRURenderDurationHistogram))
	require.Equal(t, 0, countCollectedMetrics(ExplainRUComponentSnapshotCounter))
	require.Equal(t, 1, countCollectedMetrics(ExplainRUPreviewRUCounter))
	require.Equal(t, 0, countCollectedMetrics(ExplainRUWorkRowsCounter))
	require.Equal(t, 0, countCollectedMetrics(ExplainRUWorkBytesCounter))
	require.Equal(t, 0, countCollectedMetrics(ExplainRURowWidthHistogram))
	require.Equal(t, 0, countCollectedMetrics(ReadBillingDemoStatementsCounter))
	require.Equal(t, 0, countCollectedMetrics(ReadBillingDemoOperatorStatusCounter))
	require.Equal(t, 0, countCollectedMetrics(ReadBillingDemoBaseUnitsCounter))
	require.Equal(t, 0, countCollectedMetrics(ReadBillingDemoRowWidthHistogram))
}

func TestStmtSummaryMetricLabels(t *testing.T) {
	InitStmtSummaryMetrics()
	require.Equal(t, 0, countCollectedMetrics(StmtSummaryWindowRecordCount))
	require.Equal(t, 0, countCollectedMetrics(StmtSummaryWindowEvictedCount))
	require.Equal(t, 0, countCollectedMetrics(StmtSummaryEvictedLogCounter))

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

	StmtSummaryEvictedLogCounter.WithLabelValues(StmtSummaryTypeV2, StmtSummaryEvictedLogResultPersisted).Add(3)
	StmtSummaryEvictedLogCounter.WithLabelValues(StmtSummaryTypeV2, StmtSummaryEvictedLogResultDropped).Inc()
	require.Equal(t, 2, countCollectedMetrics(StmtSummaryEvictedLogCounter))
	require.Equal(t, 3.0, readCounterValue(t, StmtSummaryEvictedLogCounter.WithLabelValues(StmtSummaryTypeV2, StmtSummaryEvictedLogResultPersisted)))
	require.Equal(t, 1.0, readCounterValue(t, StmtSummaryEvictedLogCounter.WithLabelValues(StmtSummaryTypeV2, StmtSummaryEvictedLogResultDropped)))
}

func TestGrpcChannelzCollectorSingleton(t *testing.T) {
	cleanupGrpcChannelzCollectorForTest()
	t.Cleanup(cleanupGrpcChannelzCollectorForTest)

	func() {
		grpcChannelzCollector.mu.Lock()
		defer grpcChannelzCollector.mu.Unlock()

		require.NoError(t, initGrpcChannelzCollectorLocked())
		firstServer := grpcChannelzCollector.server
		firstListener := grpcChannelzCollector.listener
		firstConn := grpcChannelzCollector.conn
		firstCollector := grpcChannelzCollector.collector

		require.NoError(t, initGrpcChannelzCollectorLocked())
		require.Same(t, firstServer, grpcChannelzCollector.server)
		require.Same(t, firstListener, grpcChannelzCollector.listener)
		require.Same(t, firstConn, grpcChannelzCollector.conn)
		require.True(t, firstCollector == grpcChannelzCollector.collector)
	}()

	cleanupGrpcChannelzCollectorForTest()

	func() {
		grpcChannelzCollector.mu.Lock()
		defer grpcChannelzCollector.mu.Unlock()

		require.Nil(t, grpcChannelzCollector.server)
		require.Nil(t, grpcChannelzCollector.listener)
		require.Nil(t, grpcChannelzCollector.conn)
		require.Nil(t, grpcChannelzCollector.collector)
		require.False(t, grpcChannelzCollector.registered)
	}()
}

func TestSetupChannelzCollectorSkippedInTest(t *testing.T) {
	cleanupGrpcChannelzCollectorForTest()
	t.Cleanup(cleanupGrpcChannelzCollectorForTest)
	require.True(t, intest.InTest)

	setupChannelzCollector()

	func() {
		grpcChannelzCollector.mu.Lock()
		defer grpcChannelzCollector.mu.Unlock()

		require.Nil(t, grpcChannelzCollector.collector)
		require.False(t, grpcChannelzCollector.registered)
	}()
}

func TestGrpcChannelzCollectorGather(t *testing.T) {
	cleanupGrpcChannelzCollectorForTest()
	t.Cleanup(cleanupGrpcChannelzCollectorForTest)

	var collector prometheus.Collector
	func() {
		grpcChannelzCollector.mu.Lock()
		defer grpcChannelzCollector.mu.Unlock()

		require.NoError(t, initGrpcChannelzCollectorLocked())
		collector = grpcChannelzCollector.collector
	}()

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(collector))
	families, err := registry.Gather()
	require.NoError(t, err)

	require.NotNil(t, findMetricFamily(families, "tidb_grpc_channelz_fetch_errors_total"))
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			require.False(t, metricHasLabelValue(metric, "target", "bufnet"))
			require.False(t, metricHasLabelValue(metric, "target", "passthrough:///bufnet"))
			if strings.HasPrefix(family.GetName(), "tidb_grpc_channelz_socket_") {
				require.False(t, metricHasLabelValue(metric, "remote", ""))
			}
		}
	}
}

func findMetricFamily(families []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	return nil
}

func metricHasLabelValue(metric *dto.Metric, name string, value string) bool {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name && label.GetValue() == value {
			return true
		}
	}
	return false
}

func requireMetricFamilyHasLabels(t *testing.T, family *dto.MetricFamily, names ...string) {
	t.Helper()
	require.NotEmpty(t, family.GetMetric())
	labels := make(map[string]struct{}, len(family.GetMetric()[0].GetLabel()))
	for _, label := range family.GetMetric()[0].GetLabel() {
		labels[label.GetName()] = struct{}{}
	}
	for _, name := range names {
		_, ok := labels[name]
		require.Truef(t, ok, "metric %s missing label %s", family.GetName(), name)
	}
}
