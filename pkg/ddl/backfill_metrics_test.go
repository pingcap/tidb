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
	"github.com/stretchr/testify/require"
)

func TestBackfillMetricsCleanupAddIndex(t *testing.T) {
	progressLabel := generateReorgLabel(metrics.LblAddIndex, "test_db", "test_table", "idx1")
	mergeProgressLabel := generateReorgLabel(metrics.LblAddIndexMerge, "test_db", "test_table", "idx1")
	totalLabel := generateReorgLabel(metrics.LblAddIdxRate, "test_db", "test_table", "idx1")
	mergeTotalLabel := generateReorgLabel(metrics.LblMergeTmpIdxRate, "test_db", "test_table", "idx1")
	conflictLabel := generateReorgLabel(metrics.LblAddIdxRate+"-conflict", "test_db", "test_table", "idx1")

	getBackfillProgressByLabel(metrics.LblAddIndex, "test_db", "test_table", "idx1").Set(50)
	getBackfillProgressByLabel(metrics.LblAddIndexMerge, "test_db", "test_table", "idx1").Set(25)
	getBackfillTotalByLabel(metrics.LblAddIdxRate, "test_db", "test_table", "idx1").Add(100)
	getBackfillTotalByLabel(metrics.LblMergeTmpIdxRate, "test_db", "test_table", "idx1").Add(20)
	getBackfillTotalByLabel(metrics.LblAddIdxRate+"-conflict", "test_db", "test_table", "idx1").Add(1)

	require.True(t, hasActiveBackfillProgressLabel(progressLabel))
	require.True(t, hasActiveBackfillProgressLabel(mergeProgressLabel))
	require.True(t, hasActiveBackfillTotalLabel(totalLabel))
	require.True(t, hasActiveBackfillTotalLabel(mergeTotalLabel))
	require.True(t, hasActiveBackfillTotalLabel(conflictLabel))

	cleanupBackfillMetrics(model.ActionAddIndex, "test_db", "test_table")

	require.False(t, hasActiveBackfillProgressLabel(progressLabel))
	require.False(t, hasActiveBackfillProgressLabel(mergeProgressLabel))
	require.False(t, hasActiveBackfillTotalLabel(totalLabel))
	require.False(t, hasActiveBackfillTotalLabel(mergeTotalLabel))
	require.False(t, hasActiveBackfillTotalLabel(conflictLabel))
}

func TestBackfillMetricsCleanupModifyColumn(t *testing.T) {
	progressLabel := generateReorgLabel(metrics.LblModifyColumn, "test_db", "test_table", "col_a")
	totalLabel := generateReorgLabel(metrics.LblUpdateColRate, "test_db", "test_table", "col_a")

	getBackfillProgressByLabel(metrics.LblModifyColumn, "test_db", "test_table", "col_a").Set(10)
	getBackfillTotalByLabel(metrics.LblUpdateColRate, "test_db", "test_table", "col_a").Add(1)

	require.True(t, hasActiveBackfillProgressLabel(progressLabel))
	require.True(t, hasActiveBackfillTotalLabel(totalLabel))

	cleanupBackfillMetrics(model.ActionModifyColumn, "test_db", "test_table")

	require.False(t, hasActiveBackfillProgressLabel(progressLabel))
	require.False(t, hasActiveBackfillTotalLabel(totalLabel))
}

func hasActiveBackfillProgressLabel(label string) bool {
	backfillLabelsMu.Lock()
	_, ok := activeBackfillProgressLabels[label]
	backfillLabelsMu.Unlock()
	return ok
}

func hasActiveBackfillTotalLabel(label string) bool {
	backfillLabelsMu.Lock()
	_, ok := activeBackfillTotalLabels[label]
	backfillLabelsMu.Unlock()
	return ok
}
