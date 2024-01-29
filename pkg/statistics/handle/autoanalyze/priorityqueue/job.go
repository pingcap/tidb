// Copyright 2024 PingCAP, Inc.
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

package priorityqueue

import (
	"strings"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// TableAnalysisJob defines the structure for table analysis job information.
type TableAnalysisJob struct {
	// Only set when partitions's indexes need to be analyzed.
	PartitionIndexes map[string][]string
	TableSchema      string
	TableName        string
	// Only set when table's indexes need to be analyzed.
	Indexes []string
	// Only set when table's partitions need to be analyzed.
	Partitions       []string
	TableID          int64
	TableStatsVer    int
	ChangePercentage float64
	Weight           float64
}

// Execute executes the analyze statement.
func (j *TableAnalysisJob) Execute(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) error {
	return statsutil.CallWithSCtx(statsHandle.SPool(), func(sctx sessionctx.Context) error {
		j.analyze(sctx, statsHandle, sysProcTracker)
		return nil
	})
}

func (j *TableAnalysisJob) analyze(sctx sessionctx.Context, statsHandle statstypes.StatsHandle, sysProcTracker sessionctx.SysProcTracker) {
	switch {
	case len(j.PartitionIndexes) > 0:
		j.analyzePartitionIndexes(sctx, statsHandle, sysProcTracker)
	case len(j.Partitions) > 0:
		j.analyzePartitions(sctx, statsHandle, sysProcTracker)
	case len(j.Indexes) > 0:
		j.analyzeIndexes(sctx, statsHandle, sysProcTracker)
	default:
		j.analyzeTable(sctx, statsHandle, sysProcTracker)
	}
}

func (j *TableAnalysisJob) analyzeTable(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) {
	sql, params := j.genSQLForAnalyzeTable()
	exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
}

func (j *TableAnalysisJob) analyzeIndexes(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) {
	for _, index := range j.Indexes {
		sql, params := j.genSQLForAnalyzeIndex(index)
		exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
	}
}

// analyzePartitions performs analysis on the specified partitions.
// This function uses a batch mode for efficiency. After analyzing the partitions,
// it's necessary to merge their statistics. By analyzing them in batches,
// we can reduce the overhead of this merging process.
func (j *TableAnalysisJob) analyzePartitions(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) {
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())
	needAnalyzePartitionNames := make([]any, 0, len(j.Partitions))
	for _, partition := range j.Partitions {
		needAnalyzePartitionNames = append(needAnalyzePartitionNames, partition)
	}
	for i := 0; i < len(needAnalyzePartitionNames); i += analyzePartitionBatchSize {
		start := i
		end := start + analyzePartitionBatchSize
		if end >= len(needAnalyzePartitionNames) {
			end = len(needAnalyzePartitionNames)
		}

		sql := getPartitionSQL("analyze table %n.%n partition", "", end-start)
		params := append([]any{j.TableSchema, j.TableName}, needAnalyzePartitionNames[start:end]...)
		exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
	}
}

// analyzePartitionIndexes performs analysis on the specified partition indexes.
func (j *TableAnalysisJob) analyzePartitionIndexes(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) {
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())

	for indexName, partitionNames := range j.PartitionIndexes {
		needAnalyzePartitionNames := make([]any, 0, len(partitionNames))
		for _, partition := range partitionNames {
			needAnalyzePartitionNames = append(needAnalyzePartitionNames, partition)
		}
		for i := 0; i < len(needAnalyzePartitionNames); i += analyzePartitionBatchSize {
			start := i
			end := start + analyzePartitionBatchSize
			if end >= len(needAnalyzePartitionNames) {
				end = len(needAnalyzePartitionNames)
			}

			sql := getPartitionSQL("analyze table %n.%n partition", " index %n", end-start)
			params := append([]any{j.TableSchema, j.TableName}, needAnalyzePartitionNames[start:end]...)
			params = append(params, indexName)
			exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
		}
	}
}

func getPartitionSQL(prefix, suffix string, numPartitions int) string {
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(prefix)
	for i := 0; i < numPartitions; i++ {
		if i != 0 {
			sqlBuilder.WriteString(",")
		}
		sqlBuilder.WriteString(" %n")
	}
	sqlBuilder.WriteString(suffix)
	return sqlBuilder.String()
}

// genSQLForAnalyzeTable generates the SQL for analyzing the specified table.
func (j *TableAnalysisJob) genSQLForAnalyzeTable() (string, []any) {
	sql := "analyze table %n.%n"
	params := []any{j.TableSchema, j.TableName}

	return sql, params
}

// genSQLForAnalyzeIndex generates the SQL for analyzing the specified index.
func (j *TableAnalysisJob) genSQLForAnalyzeIndex(index string) (string, []any) {
	sql := "analyze table %n.%n index %n"
	params := []any{j.TableSchema, j.TableName, index}

	return sql, params
}
