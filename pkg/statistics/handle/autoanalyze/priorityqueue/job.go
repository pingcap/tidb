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
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"go.uber.org/zap"
)

type analyzeType string

const (
	analyzeTable          analyzeType = "analyzeTable"
	analyzeIndex          analyzeType = "analyzeIndex"
	analyzePartition      analyzeType = "analyzePartition"
	analyzePartitionIndex analyzeType = "analyzePartitionIndex"
)

// defaultFailedAnalysisWaitTime is the default wait time for the next analysis after a failed analysis.
// NOTE: this is only used when the average analysis duration is not available.(No successful analysis before)
const defaultFailedAnalysisWaitTime = 30 * time.Minute

// TableAnalysisJob defines the structure for table analysis job information.
type TableAnalysisJob struct {
	// Only set when partitions's indexes need to be analyzed.
	// It looks like: {"indexName": ["partitionName1", "partitionName2"]}
	// This is only for newly added indexes.
	PartitionIndexes map[string][]string
	TableSchema      string
	TableName        string
	// Only set when table's indexes need to be analyzed.
	// This is only for newly added indexes.
	Indexes []string
	// Only set when table's partitions need to be analyzed.
	// This will analyze all indexes and columns of the specified partitions.
	Partitions           []string
	TableID              int64
	TableStatsVer        int
	ChangePercentage     float64
	TableSize            float64
	LastAnalysisDuration time.Duration
	Weight               float64
}

// IsValidToAnalyze checks whether the table is valid to analyze.
// It checks the last failed analysis duration and the average analysis duration.
// If the last failed analysis duration is less than 2 times the average analysis duration,
// we skip this table to avoid too much failed analysis.
func (j *TableAnalysisJob) IsValidToAnalyze(
	sctx sessionctx.Context,
) (bool, string) {
	// No need to analyze this table.
	// TODO: Usually, we should not put this kind of table into the queue.
	if j.Weight == 0 {
		return false, "weight is 0"
	}

	// Check whether the table or partition is valid to analyze.
	if len(j.Partitions) > 0 || len(j.PartitionIndexes) > 0 {
		// Any partition is invalid to analyze, the whole table is invalid to analyze.
		// Because we need to analyze partitions in batch mode.
		partitions := append(j.Partitions, getPartitionNames(j.PartitionIndexes)...)
		if valid, failReason := isValidToAnalyze(
			sctx,
			j.TableSchema,
			j.TableName,
			partitions...,
		); !valid {
			return false, failReason
		}
	} else {
		if valid, failReason := isValidToAnalyze(
			sctx,
			j.TableSchema,
			j.TableName,
		); !valid {
			return false, failReason
		}
	}

	return true, ""
}

func getPartitionNames(partitionIndexes map[string][]string) []string {
	names := make([]string, 0, len(partitionIndexes))
	for _, partitionNames := range partitionIndexes {
		names = append(names, partitionNames...)
	}
	return names
}

func isValidToAnalyze(
	sctx sessionctx.Context,
	schema, table string,
	partitionNames ...string,
) (bool, string) {
	lastFailedAnalysisDuration, err :=
		getLastFailedAnalysisDuration(sctx, schema, table, partitionNames...)
	if err != nil {
		statslogutil.StatsLogger().Warn(
			"Fail to get last failed analysis duration",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Strings("partitions", partitionNames),
			zap.Error(err),
		)
		return false, fmt.Sprintf("fail to get last failed analysis duration: %v", err)
	}

	averageAnalysisDuration, err :=
		getAverageAnalysisDuration(sctx, schema, table, partitionNames...)
	if err != nil {
		statslogutil.StatsLogger().Warn(
			"Fail to get average analysis duration",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Strings("partitions", partitionNames),
			zap.Error(err),
		)
		return false, fmt.Sprintf("fail to get average analysis duration: %v", err)
	}

	// Last analysis just failed, we should not analyze it again.
	if lastFailedAnalysisDuration == justFailed {
		// The last analysis failed, we should not analyze it again.
		statslogutil.StatsLogger().Info(
			"Skip analysis because the last analysis just failed",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Strings("partitions", partitionNames),
		)
		return false, "last analysis just failed"
	}

	// Failed analysis duration is less than 2 times the average analysis duration.
	// Skip this table to avoid too much failed analysis.
	onlyFailedAnalysis := lastFailedAnalysisDuration != noRecord && averageAnalysisDuration == noRecord
	if onlyFailedAnalysis && lastFailedAnalysisDuration < defaultFailedAnalysisWaitTime {
		statslogutil.StatsLogger().Info(
			fmt.Sprintf("Skip analysis because the last failed analysis duration is less than %v", defaultFailedAnalysisWaitTime),
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Strings("partitions", partitionNames),
			zap.Duration("lastFailedAnalysisDuration", lastFailedAnalysisDuration),
			zap.Duration("averageAnalysisDuration", averageAnalysisDuration),
		)
		return false, fmt.Sprintf("last failed analysis duration is less than %v", defaultFailedAnalysisWaitTime)
	}
	// Failed analysis duration is less than 2 times the average analysis duration.
	meetSkipCondition := lastFailedAnalysisDuration != noRecord &&
		lastFailedAnalysisDuration < 2*averageAnalysisDuration
	if meetSkipCondition {
		statslogutil.StatsLogger().Info(
			"Skip analysis because the last failed analysis duration is less than 2 times the average analysis duration",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Strings("partitions", partitionNames),
			zap.Duration("lastFailedAnalysisDuration", lastFailedAnalysisDuration),
			zap.Duration("averageAnalysisDuration", averageAnalysisDuration),
		)
		return false, "last failed analysis duration is less than 2 times the average analysis duration"
	}

	return true, ""
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
	switch j.getAnalyzeType() {
	case analyzeTable:
		j.analyzeTable(sctx, statsHandle, sysProcTracker)
	case analyzeIndex:
		j.analyzeIndexes(sctx, statsHandle, sysProcTracker)
	case analyzePartition:
		j.analyzePartitions(sctx, statsHandle, sysProcTracker)
	case analyzePartitionIndex:
		j.analyzePartitionIndexes(sctx, statsHandle, sysProcTracker)
	}
}

func (j *TableAnalysisJob) getAnalyzeType() analyzeType {
	switch {
	case len(j.PartitionIndexes) > 0:
		return analyzePartitionIndex
	case len(j.Partitions) > 0:
		return analyzePartition
	case len(j.Indexes) > 0:
		return analyzeIndex
	default:
		return analyzeTable
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

func (j *TableAnalysisJob) String() string {
	analyzeType := j.getAnalyzeType()
	switch analyzeType {
	case analyzeTable:
		return fmt.Sprintf(`TableAnalysisJob: {AnalyzeType: table, Schema: %s, Table: %s, TableID: %d, TableStatsVer: %d, ChangePercentage: %.2f, Weight: %.4f}`,
			j.TableSchema, j.TableName, j.TableID, j.TableStatsVer, j.ChangePercentage, j.Weight)
	case analyzeIndex:
		return fmt.Sprintf(`TableAnalysisJob: {AnalyzeType: index, Indexes: %s, Schema: %s, Table: %s, TableID: %d, TableStatsVer: %d, ChangePercentage: %.2f, Weight: %.4f}`,
			strings.Join(j.Indexes, ", "), j.TableSchema, j.TableName, j.TableID, j.TableStatsVer, j.ChangePercentage, j.Weight)
	case analyzePartition:
		return fmt.Sprintf(`TableAnalysisJob: {AnalyzeType: partition, Partitions: %s, Schema: %s, Table: %s, TableID: %d, TableStatsVer: %d, ChangePercentage: %.2f, Weight: %.4f}`,
			strings.Join(j.Partitions, ", "), j.TableSchema, j.TableName, j.TableID, j.TableStatsVer, j.ChangePercentage, j.Weight)
	case analyzePartitionIndex:
		return fmt.Sprintf(`TableAnalysisJob: {AnalyzeType: partitionIndex, PartitionIndexes: %v, Schema: %s, Table: %s, TableID: %d, TableStatsVer: %d, ChangePercentage: %.2f, Weight: %.4f}`,
			j.PartitionIndexes, j.TableSchema, j.TableName, j.TableID, j.TableStatsVer, j.ChangePercentage, j.Weight)
	default:
		return "TableAnalysisJob: {AnalyzeType: unknown}"
	}
}
