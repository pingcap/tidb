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

package metrics_test

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	_ "github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/stretchr/testify/require"
)

func TestMetrics(_ *testing.T) {
	// Make sure it doesn't panic.
	metrics.PanicCounter.WithLabelValues(metrics.LabelDomain).Inc()
}

func TestRegisterMetrics(_ *testing.T) {
	// Make sure it doesn't panic.
	metrics.RegisterMetrics()
}

func TestExecuteErrorToLabel(t *testing.T) {
	require.Equal(t, `unknown`, metrics.ExecuteErrorToLabel(errors.New("test")))
	require.Equal(t, `global:2`, metrics.ExecuteErrorToLabel(terror.ErrResultUndetermined))
}

func TestBackfillProgressMetricsCleanup(t *testing.T) {
	// Test that backfill progress metrics are created and tracked.
	gauge1 := metrics.GetBackfillProgressByLabel(metrics.LblAddIndex, "test_db", "test_table", "idx1")
	require.NotNil(t, gauge1)
	gauge1.Set(50.0)

	gauge2 := metrics.GetBackfillProgressByLabel(metrics.LblAddIndex, "test_db", "test_table", "idx2")
	require.NotNil(t, gauge2)
	gauge2.Set(75.0)

	// Verify metrics are in the active map (by checking they can be retrieved).
	gaugeCheck := metrics.GetBackfillProgressByLabel(metrics.LblAddIndex, "test_db", "test_table", "idx1")
	require.NotNil(t, gaugeCheck)

	// Test backfill total counter metrics.
	counter1 := metrics.GetBackfillTotalByLabel(metrics.LblAddIdxRate, "test_db", "test_table", "idx1")
	require.NotNil(t, counter1)
	counter1.Add(100.0)

	counter2 := metrics.GetBackfillTotalByLabel(metrics.LblAddIdxRate, "test_db", "test_table", "idx2")
	require.NotNil(t, counter2)
	counter2.Add(200.0)

	// Clean up metrics for test_db.test_table.
	progressLabels := []string{metrics.LblAddIndex, metrics.LblAddIndexMerge}
	totalLabels := []string{
		metrics.LblAddIdxRate,
		metrics.LblMergeTmpIdxRate,
		metrics.LblAddIdxRate + "-conflict",
		metrics.LblMergeTmpIdxRate + "-conflict",
	}
	metrics.CleanupBackfillByLabelTypes(progressLabels, totalLabels, "test_db", "test_table")

	// After cleanup, creating new metrics with the same labels should work.
	gauge3 := metrics.GetBackfillProgressByLabel(metrics.LblAddIndex, "test_db", "test_table", "idx1")
	require.NotNil(t, gauge3)
	gauge3.Set(25.0)

	counter3 := metrics.GetBackfillTotalByLabel(metrics.LblAddIdxRate, "test_db", "test_table", "idx1")
	require.NotNil(t, counter3)
	counter3.Add(50.0)

	// Clean up metrics for a different table - should not affect test_table.
	metrics.CleanupBackfillByLabelTypes(progressLabels, totalLabels, "other_db", "other_table")

	// Clean up metrics for a different operation type - should not affect add_index metrics.
	metrics.CleanupBackfillByLabelTypes(
		[]string{metrics.LblModifyColumn},
		[]string{metrics.LblUpdateColRate, metrics.LblUpdateColRate + "-conflict"},
		"test_db",
		"test_table",
	)
}

func TestBackfillTotalMetricsCleanup(t *testing.T) {
	// Test that backfill total counter metrics are created and tracked.
	counter1 := metrics.GetBackfillTotalByLabel(metrics.LblAddIdxRate, "test_db", "test_table", "idx1")
	require.NotNil(t, counter1)
	counter1.Add(100.0)

	counter2 := metrics.GetBackfillTotalByLabel(metrics.LblMergeTmpIdxRate, "test_db", "test_table", "idx2")
	require.NotNil(t, counter2)
	counter2.Add(200.0)

	// Test conflict counter (with -conflict suffix).
	conflictCounter := metrics.GetBackfillTotalByLabel(metrics.LblMergeTmpIdxRate+"-conflict", "test_db", "test_table", "idx2")
	require.NotNil(t, conflictCounter)
	conflictCounter.Add(50.0)

	// Clean up only total metrics for test_db.test_table.
	totalLabels := []string{
		metrics.LblAddIdxRate,
		metrics.LblMergeTmpIdxRate,
		metrics.LblAddIdxRate + "-conflict",
		metrics.LblMergeTmpIdxRate + "-conflict",
	}
	metrics.CleanupBackfillTotalByLabelTypes(totalLabels, "test_db", "test_table")

	// After cleanup, creating a new counter with the same labels should work.
	counter3 := metrics.GetBackfillTotalByLabel(metrics.LblAddIdxRate, "test_db", "test_table", "idx1")
	require.NotNil(t, counter3)
	counter3.Add(50.0)

	// The merge tmp idx rate metrics should be cleaned together with add index metrics.
	counter4 := metrics.GetBackfillTotalByLabel(metrics.LblMergeTmpIdxRate, "test_db", "test_table", "idx2")
	require.NotNil(t, counter4)
	counter4.Add(75.0)
}
