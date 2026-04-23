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

	"github.com/pingcap/tidb/metrics"
	"github.com/stretchr/testify/require"
)

func TestBackfillMetricsCleanupByTableID(t *testing.T) {
	const tableID int64 = 12345
	progress := getBackfillProgressByTableID(tableID, metrics.LblAddIndex, "test_db", "test_table")
	progress.Set(50)
	total := getBackfillTotalByTableID(tableID, "add_idx_rate", "test_db", "test_table")
	total.Add(100)

	require.Len(t, metrics.GetBackfillLabelsForTest(tableID), 2)
	metrics.DDLClearBackfillMetrics(tableID)
	require.Empty(t, metrics.GetBackfillLabelsForTest(tableID))
}

func TestBackfillMetricsCleanupPartitionedTable(t *testing.T) {
	for _, tableID := range []int64{101, 102, 103} {
		getBackfillProgressByTableID(tableID, metrics.LblAddIndex, "test_db", "test_table").Set(10)
		require.NotEmpty(t, metrics.GetBackfillLabelsForTest(tableID))
		metrics.DDLClearBackfillMetrics(tableID)
		require.Empty(t, metrics.GetBackfillLabelsForTest(tableID))
	}
}

func TestBackfillMetricsIdempotentCleanup(t *testing.T) {
	const tableID int64 = 99999
	metrics.DDLClearBackfillMetrics(tableID)
	getBackfillProgressByTableID(tableID, metrics.LblModifyColumn, "test_db", "test_table").Set(75)
	metrics.DDLClearBackfillMetrics(tableID)
	metrics.DDLClearBackfillMetrics(tableID)
	require.Empty(t, metrics.GetBackfillLabelsForTest(tableID))
}
