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

func TestRUV2MetricDefinitions(t *testing.T) {
	require.Equal(t,
		[]string{"ddl", "read", "write", "analyze", "other"},
		[]string{LblSQLTypeDDL, LblSQLTypeRead, LblSQLTypeWrite, LblSQLTypeAnalyze, LblSQLTypeOther},
	)
	require.Equal(t, []string{"tikv", "tiflash"}, []string{LblEngineTiKV, LblEngineTiFlash})

	InitRUV2Metrics()
	RUV2Total.Add(1)
	RUV2TTLTotal.Add(1)
	RUV2BySQLTypeDDL.Add(2)
	RUV2ByEngineTiKV.Add(3)
	RUV2BySQLType.WithLabelValues("select").Add(2)
	AddRUV2Results(3, 4, 5, 12, "select")
	RUV2Unit.WithLabelValues("tikv", "hash_agg", LblRUV2UnitCPUWork).Add(5)
	RUV2Statements.WithLabelValues("success", "incomplete").Inc()

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(RUV2Total))
	require.NoError(t, registry.Register(RUV2TTLTotal))
	require.NoError(t, registry.Register(RUV2BySQLType))
	require.NoError(t, registry.Register(RUV2ByEngine))
	require.NoError(t, registry.Register(RUV2Unit))
	require.NoError(t, registry.Register(RUV2Statements))
	families, err := registry.Gather()
	require.NoError(t, err)

	require.NotNil(t, findMetricFamily(families, "tidb_ruv2_ru_total"))
	require.NotNil(t, findMetricFamily(families, "tidb_ruv2_ttl_ru_total"))
	requireMetricFamilyHasLabel(t, families, "tidb_ruv2_ru_by_sql_type_total", LblSQLType, LblSQLTypeDDL)
	requireMetricFamilyHasLabel(
		t, families, "tidb_ruv2_ru_by_sql_type_total", LblSQLType, "select",
	)
	requireMetricFamilyHasLabel(
		t, families, "tidb_ruv2_ru_by_engine_total", LblEngine, LblEngineTiKV,
	)
	requireMetricFamilyHasLabel(t, families, "tidb_ruv2_ru_by_engine_total", LblEngine, "tidb")
	requireMetricFamilyHasLabel(t, families, "tidb_ruv2_unit_total", LblEngine, "tikv")
	requireMetricFamilyHasLabel(t, families, "tidb_ruv2_unit_total", "opclass", "hash_agg")
	requireMetricFamilyHasLabel(t, families, "tidb_ruv2_unit_total", LblRUV2Unit, LblRUV2UnitCPUWork)
	requireMetricFamilyHasLabel(t, families, "tidb_ruv2_statements_total", "status", "success")
	requireMetricFamilyHasLabel(t, families, "tidb_ruv2_statements_total", "reason", "incomplete")
}

func requireMetricFamilyHasLabel(t *testing.T, families []*dto.MetricFamily, familyName, labelName, labelValue string) {
	t.Helper()
	family := findMetricFamily(families, familyName)
	require.NotNil(t, family)
	for _, metric := range family.GetMetric() {
		if metricHasLabelValue(metric, labelName, labelValue) {
			return
		}
	}
	require.Failf(t, "missing metric label", "metric family %s has no label %s=%s", familyName, labelName, labelValue)
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
