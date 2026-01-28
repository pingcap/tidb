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
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	backfillLabelsMu sync.Mutex
	// activeBackfillProgressLabels tracks all active backfill progress metrics labels
	// for cleanup when DDL jobs are done.
	activeBackfillProgressLabels = make(map[string]struct{}, 256)
	// activeBackfillTotalLabels tracks all active backfill total counter metrics labels
	// for cleanup when DDL jobs are done.
	activeBackfillTotalLabels = make(map[string]struct{}, 256)
)

// generateReorgLabel returns the label with schema name, table name and optional column/index names.
// Multiple columns/indexes can be concatenated with "+".
func generateReorgLabel(label, schemaName, tableName, colOrIdxNames string) string {
	var stringBuilder strings.Builder
	if len(colOrIdxNames) == 0 {
		stringBuilder.Grow(len(label) + len(schemaName) + len(tableName) + 2)
	} else {
		stringBuilder.Grow(len(label) + len(schemaName) + len(tableName) + len(colOrIdxNames) + 3)
	}
	stringBuilder.WriteString(label)
	stringBuilder.WriteString("-")
	stringBuilder.WriteString(schemaName)
	stringBuilder.WriteString("-")
	stringBuilder.WriteString(tableName)
	if len(colOrIdxNames) > 0 {
		stringBuilder.WriteString("-")
		stringBuilder.WriteString(colOrIdxNames)
	}
	return stringBuilder.String()
}

// getBackfillTotalByLabel returns the Counter showing the speed of backfilling for the given type label.
// It also tracks the label for later cleanup.
func getBackfillTotalByLabel(label, schemaName, tableName, optionalColOrIdxName string) prometheus.Counter {
	labelValue := generateReorgLabel(label, schemaName, tableName, optionalColOrIdxName)
	backfillLabelsMu.Lock()
	activeBackfillTotalLabels[labelValue] = struct{}{}
	backfillLabelsMu.Unlock()
	return metrics.BackfillTotalCounter.WithLabelValues(labelValue)
}

// getBackfillProgressByLabel returns the Gauge showing the percentage progress for the given type label.
// It also tracks the label for later cleanup.
func getBackfillProgressByLabel(label, schemaName, tableName, optionalColOrIdxName string) prometheus.Gauge {
	labelValue := generateReorgLabel(label, schemaName, tableName, optionalColOrIdxName)
	backfillLabelsMu.Lock()
	activeBackfillProgressLabels[labelValue] = struct{}{}
	backfillLabelsMu.Unlock()
	return metrics.BackfillProgressGauge.WithLabelValues(labelValue)
}

func backfillProgressLabel(jobType model.ActionType, mergingTmpIdx bool) string {
	switch jobType {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		if mergingTmpIdx {
			return metrics.LblAddIndexMerge
		}
		return metrics.LblAddIndex
	case model.ActionModifyColumn:
		return metrics.LblModifyColumn
	case model.ActionReorganizePartition, model.ActionAlterTablePartitioning, model.ActionRemovePartitioning:
		return metrics.LblReorgPartition
	default:
		return ""
	}
}

func backfillProgressLabelTypes(jobType model.ActionType) []string {
	switch jobType {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		return []string{metrics.LblAddIndex, metrics.LblAddIndexMerge}
	case model.ActionModifyColumn:
		return []string{metrics.LblModifyColumn}
	case model.ActionReorganizePartition, model.ActionAlterTablePartitioning, model.ActionRemovePartitioning:
		return []string{metrics.LblReorgPartition}
	default:
		return nil
	}
}

func backfillTotalLabelTypes(jobType model.ActionType) []string {
	switch jobType {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		return []string{
			metrics.LblAddIdxRate,
			metrics.LblMergeTmpIdxRate,
			metrics.LblAddIdxRate + "-conflict",
			metrics.LblMergeTmpIdxRate + "-conflict",
		}
	case model.ActionModifyColumn:
		return []string{
			metrics.LblUpdateColRate,
			metrics.LblUpdateColRate + "-conflict",
		}
	case model.ActionReorganizePartition, model.ActionAlterTablePartitioning, model.ActionRemovePartitioning:
		return []string{
			metrics.LblReorgPartitionRate,
			metrics.LblReorgPartitionRate + "-conflict",
		}
	case model.ActionDropTablePartition:
		return []string{
			metrics.LblCleanupIdxRate,
			metrics.LblCleanupIdxRate + "-conflict",
		}
	default:
		return nil
	}
}

func cleanupBackfillMetrics(jobType model.ActionType, schemaName, tableName string) {
	cleanupBackfillProgressByLabelTypes(backfillProgressLabelTypes(jobType), schemaName, tableName)
	cleanupBackfillTotalByLabelTypes(backfillTotalLabelTypes(jobType), schemaName, tableName)
}

type labelDeleter interface {
	DeleteLabelValues(lvs ...string) bool
}

func cleanupBackfillProgressByLabelTypes(labelTypes []string, schemaName, tableName string) {
	if len(labelTypes) == 0 {
		return
	}
	cleanupBackfillLabels(activeBackfillProgressLabels, metrics.BackfillProgressGauge, labelTypes, schemaName, tableName)
}

func cleanupBackfillTotalByLabelTypes(labelTypes []string, schemaName, tableName string) {
	if len(labelTypes) == 0 {
		return
	}
	cleanupBackfillLabels(activeBackfillTotalLabels, metrics.BackfillTotalCounter, labelTypes, schemaName, tableName)
}

func cleanupBackfillLabels(active map[string]struct{}, metric labelDeleter, labelTypes []string, schemaName, tableName string) {
	prefixes := make([]string, 0, len(labelTypes))
	for _, label := range labelTypes {
		prefixes = append(prefixes, generateReorgLabel(label, schemaName, tableName, ""))
	}

	backfillLabelsMu.Lock()
	defer backfillLabelsMu.Unlock()
	for labelValue := range active {
		for _, prefix := range prefixes {
			if strings.HasPrefix(labelValue, prefix) {
				metric.DeleteLabelValues(labelValue)
				delete(active, labelValue)
				break
			}
		}
	}
}
