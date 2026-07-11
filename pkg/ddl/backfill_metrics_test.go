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

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func collectTypeLabelsFromMetricVec(collector prometheus.Collector) map[string]struct{} {
	ch := make(chan prometheus.Metric)
	go func() {
		collector.Collect(ch)
		close(ch)
	}()

	typeLabels := make(map[string]struct{})
	for metric := range ch {
		var pb dto.Metric
		if metric.Write(&pb) != nil {
			continue
		}
		for _, labelPair := range pb.GetLabel() {
			if labelPair.GetName() == metrics.LblType {
				typeLabels[labelPair.GetValue()] = struct{}{}
				break
			}
		}
	}
	return typeLabels
}

func requireBackfillSeriesPresent(t *testing.T, typeLabel string) {
	t.Helper()
	gaugeLabels := collectTypeLabelsFromMetricVec(metrics.BackfillProgressGauge)
	counterLabels := collectTypeLabelsFromMetricVec(metrics.BackfillTotalCounter)
	_, inGauge := gaugeLabels[typeLabel]
	_, inCounter := counterLabels[typeLabel]
	require.Truef(t, inGauge || inCounter, "expected type label %q in prometheus vectors", typeLabel)
}

func requireBackfillSeriesAbsent(t *testing.T, typeLabel string) {
	t.Helper()
	gaugeLabels := collectTypeLabelsFromMetricVec(metrics.BackfillProgressGauge)
	counterLabels := collectTypeLabelsFromMetricVec(metrics.BackfillTotalCounter)
	_, inGauge := gaugeLabels[typeLabel]
	_, inCounter := counterLabels[typeLabel]
	require.Falsef(t, inGauge || inCounter, "expected type label %q to be deleted from prometheus vectors", typeLabel)
}

func TestBackfillMetricsCleanupByTableID(t *testing.T) {
	const tableID int64 = 12345

	// Register progress and total metrics for an add-index backfill.
	progressGauge := getBackfillProgressByTableID(tableID, metrics.LblAddIndex, "test_db_1", "test_table_1", "idx1")
	progressGauge.Set(50.0)

	totalCounter := getBackfillTotalByTableID(tableID, metrics.LblAddIdxRate, "test_db_1", "test_table_1", "idx1")
	totalCounter.Add(100.0)

	conflictCounter := getBackfillTotalByTableID(tableID, metrics.LblAddIdxRate+"-conflict", "test_db_1", "test_table_1", "idx1")
	conflictCounter.Add(1.0)

	// Verify the metrics are registered in the registry.
	labels := metrics.GetBackfillLabelsForTest(tableID)
	require.NotEmpty(t, labels, "expected metrics to be registered for tableID %d", tableID)
	registeredTypeLabels := make([]string, 0, len(labels))
	for typeLabel := range labels {
		registeredTypeLabels = append(registeredTypeLabels, typeLabel)
		requireBackfillSeriesPresent(t, typeLabel)
	}

	// Clear metrics using the tableID-based cleanup.
	metrics.DDLClearBackfillMetrics(tableID)

	// After cleanup, the registry should have no entries for this tableID.
	labels = metrics.GetBackfillLabelsForTest(tableID)
	require.Empty(t, labels, "expected no metrics after cleanup for tableID %d, got %v", tableID, labels)
	for _, typeLabel := range registeredTypeLabels {
		requireBackfillSeriesAbsent(t, typeLabel)
	}
}

func TestBackfillMetricsCleanupPartitionedTable(t *testing.T) {
	const logicalTableID int64 = 100
	partIDs := []int64{101, 102, 103}

	// Simulate metrics registered per partition (as add-index does).
	for _, pid := range partIDs {
		getBackfillProgressByTableID(pid, metrics.LblAddIndex, "test_db_2", "test_table_2", "idx1").Set(10.0)
		getBackfillTotalByTableID(pid, metrics.LblAddIdxRate, "test_db_2", "test_table_2", "idx1").Add(50.0)
	}

	registeredTypeLabels := make(map[string]struct{}, len(partIDs)*2)
	// Verify all partition metrics are registered.
	for _, pid := range partIDs {
		labels := metrics.GetBackfillLabelsForTest(pid)
		require.NotEmpty(t, labels, "expected metrics for partition %d", pid)
		for typeLabel := range labels {
			registeredTypeLabels[typeLabel] = struct{}{}
			requireBackfillSeriesPresent(t, typeLabel)
		}
	}

	// Clear each partition individually (as applyCreateTable does).
	for _, pid := range partIDs {
		metrics.DDLClearBackfillMetrics(pid)
	}

	// All partition metrics should be gone.
	for _, pid := range partIDs {
		labels := metrics.GetBackfillLabelsForTest(pid)
		require.Empty(t, labels, "expected no metrics after cleanup for partition %d", pid)
	}

	// Logical table should also have no stale entries.
	labels := metrics.GetBackfillLabelsForTest(logicalTableID)
	require.Empty(t, labels, "expected no metrics for logical table")
	for typeLabel := range registeredTypeLabels {
		requireBackfillSeriesAbsent(t, typeLabel)
	}

	t.Run("partition-reorg-rate-uses-logical-table-id", func(t *testing.T) {
		const (
			reorgTableID   int64 = 200
			droppingPartID int64 = 201
			schemaName           = "test_db_reorg"
			tableName            = "test_table_reorg"
			indexName            = "idx_reorg"
		)
		info := &reorgInfo{
			Job: &model.Job{
				Type:    model.ActionReorganizePartition,
				TableID: reorgTableID,
			},
			PhysicalTableID: droppingPartID,
		}
		metricTableID := backfillMetricsTableID(info, metrics.LblReorgPartitionRate)
		require.Equal(t, reorgTableID, metricTableID)
		require.Equal(t, reorgTableID, backfillMetricsTableID(info, metrics.LblReorgPartitionRate+"-conflict"))

		getBackfillTotalByTableID(metricTableID, metrics.LblReorgPartitionRate, schemaName, tableName, indexName).Add(1.0)
		labels := metrics.GetBackfillLabelsForTest(reorgTableID)
		require.NotEmpty(t, labels)
		require.Empty(t, metrics.GetBackfillLabelsForTest(droppingPartID))

		registered := make([]string, 0, len(labels))
		for typeLabel := range labels {
			registered = append(registered, typeLabel)
			requireBackfillSeriesPresent(t, typeLabel)
		}

		metrics.DDLClearBackfillMetrics(reorgTableID)
		require.Empty(t, metrics.GetBackfillLabelsForTest(reorgTableID))
		require.Empty(t, metrics.GetBackfillLabelsForTest(droppingPartID))
		for _, typeLabel := range registered {
			requireBackfillSeriesAbsent(t, typeLabel)
		}
	})

	t.Run("metric-table-id-selection-audit", func(t *testing.T) {
		const (
			testLogicalTableID  int64 = 300
			testPhysicalTableID int64 = 301
		)
		cases := []struct {
			name        string
			actionType  model.ActionType
			label       string
			expectTable int64
		}{
			{
				name:        "reorg-partition-progress",
				actionType:  model.ActionReorganizePartition,
				label:       metrics.LblReorgPartition,
				expectTable: testLogicalTableID,
			},
			{
				name:        "reorg-partition-rate",
				actionType:  model.ActionReorganizePartition,
				label:       metrics.LblReorgPartitionRate,
				expectTable: testLogicalTableID,
			},
			{
				name:        "reorg-partition-rate-conflict",
				actionType:  model.ActionReorganizePartition,
				label:       metrics.LblReorgPartitionRate + "-conflict",
				expectTable: testLogicalTableID,
			},
			{
				name:        "alter-partitioning-rate",
				actionType:  model.ActionAlterTablePartitioning,
				label:       metrics.LblReorgPartitionRate,
				expectTable: testLogicalTableID,
			},
			{
				name:        "remove-partitioning-progress",
				actionType:  model.ActionRemovePartitioning,
				label:       metrics.LblReorgPartition,
				expectTable: testLogicalTableID,
			},
			{
				name:        "add-index-rate-keeps-physical-id",
				actionType:  model.ActionAddIndex,
				label:       metrics.LblAddIdxRate,
				expectTable: testPhysicalTableID,
			},
			{
				name:        "add-index-progress-keeps-physical-id",
				actionType:  model.ActionAddIndex,
				label:       metrics.LblAddIndex,
				expectTable: testPhysicalTableID,
			},
			{
				name:        "merge-temp-rate-keeps-physical-id",
				actionType:  model.ActionAddIndex,
				label:       metrics.LblMergeTmpIdxRate,
				expectTable: testPhysicalTableID,
			},
			{
				name:        "cleanup-index-rate-keeps-physical-id-for-non-partition-ddl",
				actionType:  model.ActionAddIndex,
				label:       metrics.LblCleanupIdxRate,
				expectTable: testPhysicalTableID,
			},
			{
				name:        "cleanup-index-rate-uses-logical-id-for-drop-partition",
				actionType:  model.ActionDropTablePartition,
				label:       metrics.LblCleanupIdxRate,
				expectTable: testLogicalTableID,
			},
			{
				name:        "cleanup-index-rate-uses-logical-id-for-truncate-partition",
				actionType:  model.ActionTruncateTablePartition,
				label:       metrics.LblCleanupIdxRate,
				expectTable: testLogicalTableID,
			},
			{
				name:        "modify-column-rate-keeps-physical-id",
				actionType:  model.ActionModifyColumn,
				label:       metrics.LblUpdateColRate,
				expectTable: testPhysicalTableID,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				info := &reorgInfo{
					Job: &model.Job{
						Type:    tc.actionType,
						TableID: testLogicalTableID,
					},
					PhysicalTableID: testPhysicalTableID,
				}
				require.Equal(t, tc.expectTable, backfillMetricsTableID(info, tc.label))
			})
		}
	})
}

func TestBackfillMetricsIdempotentCleanup(t *testing.T) {
	const tableID int64 = 99999

	// Cleanup on a table that was never registered should be a no-op (no panic).
	metrics.DDLClearBackfillMetrics(tableID)

	// Register and cleanup twice.
	getBackfillProgressByTableID(tableID, metrics.LblModifyColumn, "test_db_3", "test_table_3", "col_a").Set(75.0)
	labels := metrics.GetBackfillLabelsForTest(tableID)
	require.Len(t, labels, 1)
	registeredTypeLabels := make([]string, 0, len(labels))
	for typeLabel := range labels {
		registeredTypeLabels = append(registeredTypeLabels, typeLabel)
		requireBackfillSeriesPresent(t, typeLabel)
	}

	metrics.DDLClearBackfillMetrics(tableID)
	metrics.DDLClearBackfillMetrics(tableID) // second cleanup should be safe

	labels = metrics.GetBackfillLabelsForTest(tableID)
	require.Empty(t, labels)
	for _, typeLabel := range registeredTypeLabels {
		requireBackfillSeriesAbsent(t, typeLabel)
	}
}
