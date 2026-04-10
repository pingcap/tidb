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

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func TestBackfillMetricsCleanupByTableID(t *testing.T) {
	const tableID int64 = 12345

	// Register progress and total metrics for an add-index backfill.
	progressGauge := getBackfillProgressByTableID(tableID, metrics.LblAddIndex, "test_db", "test_table", "idx1")
	progressGauge.Set(50.0)

	totalCounter := getBackfillTotalByTableID(tableID, metrics.LblAddIdxRate, "test_db", "test_table", "idx1")
	totalCounter.Add(100.0)

	conflictCounter := getBackfillTotalByTableID(tableID, metrics.LblAddIdxRate+"-conflict", "test_db", "test_table", "idx1")
	conflictCounter.Add(1.0)

	// Verify the metrics are registered in the registry.
	labels := metrics.GetBackfillLabelsForTest(tableID)
	require.NotEmpty(t, labels, "expected metrics to be registered for tableID %d", tableID)

	// Clear metrics using the tableID-based cleanup.
	metrics.DDLClearBackfillMetrics(tableID)

	// After cleanup, the registry should have no entries for this tableID.
	labels = metrics.GetBackfillLabelsForTest(tableID)
	require.Empty(t, labels, "expected no metrics after cleanup for tableID %d, got %v", tableID, labels)
}

func TestBackfillMetricsCleanupPartitionedTable(t *testing.T) {
	const logicalTableID int64 = 100
	partIDs := []int64{101, 102, 103}

	// Simulate metrics registered per partition (as add-index does).
	for _, pid := range partIDs {
		getBackfillProgressByTableID(pid, metrics.LblAddIndex, "test_db", "test_table", "idx1").Set(10.0)
		getBackfillTotalByTableID(pid, metrics.LblAddIdxRate, "test_db", "test_table", "idx1").Add(50.0)
	}

	// Verify all partition metrics are registered.
	for _, pid := range partIDs {
		labels := metrics.GetBackfillLabelsForTest(pid)
		require.NotEmpty(t, labels, "expected metrics for partition %d", pid)
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
}

func TestBackfillMetricsIdempotentCleanup(t *testing.T) {
	const tableID int64 = 99999

	// Cleanup on a table that was never registered should be a no-op (no panic).
	metrics.DDLClearBackfillMetrics(tableID)

	// Register and cleanup twice.
	getBackfillProgressByTableID(tableID, metrics.LblModifyColumn, "test_db", "test_table", "col_a").Set(75.0)
	metrics.DDLClearBackfillMetrics(tableID)
	metrics.DDLClearBackfillMetrics(tableID) // second cleanup should be safe

	labels := metrics.GetBackfillLabelsForTest(tableID)
	require.Empty(t, labels)
}
