// Copyright 2023 PingCAP, Inc.
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

package analyzequeue

import (
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// TableAnalysisJob defines the structure for table analysis job information.
type TableAnalysisJob struct {
	// Only set when partition's indexes need to be analyzed.
	PartitionIndexes map[string][]string

	DBName    string
	TableName string

	// Only set when indexes need to be analyzed.
	Indexes []string
	// Only set when the table is a partitioned table.
	Partitions []string

	TableID int64

	// The version of the table statistics.
	TableStatsVer int

	// We use the following fields to determine whether the table needs to be analyzed.
	ChangePercentage float64
	Weight           float64
}

// IsValidToAnalyze checks whether the table is valid to analyze.
// It checks the last failed analysis duration and the average analysis duration.
// If the last failed analysis duration is less than 2 times the average analysis duration,
// we skip this table to avoid too many failed analysis.
func (j *TableAnalysisJob) IsValidToAnalyze(
	sctx sessionctx.Context,
) bool {
	if j.Weight == 0 {
		return false
	}
	lastFailedAnalysisDuration, err := getLastFailedAnalysisDuration(sctx, j.DBName, j.TableName, "")
	if err != nil {
		return false
	}

	averageAnalysisDuration, err := getAverageAnalysisDuration(sctx, j.DBName, j.TableName, "")
	if err != nil {
		return false
	}

	// Failed analysis duration is less than 2 times the average analysis duration.
	// Skip this table to avoid too many failed analysis.
	if lastFailedAnalysisDuration < 2*averageAnalysisDuration {
		return false
	}

	return true
}

// Execute executes the table analysis SQL.
func (j *TableAnalysisJob) Execute(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) error {
	se, err := statsHandle.SPool().Get()
	if err != nil {
		return err
	}
	defer statsHandle.SPool().Put(se)

	sctx := se.(sessionctx.Context)
	if len(j.PartitionIndexes) > 0 {
		j.analyzePartitionIndexes(sctx, statsHandle, sysProcTracker)
		return nil
	}
	if len(j.Partitions) > 0 {
		j.analyzePartitions(sctx, statsHandle, sysProcTracker)
		return nil
	}
	if len(j.Indexes) > 0 {
		for _, index := range j.Indexes {
			sql, params := j.genSQLForAnalyzeIndex(index)
			execAutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
		}
		return nil
	}
	sql, params := j.genSQLForAnalyzeTable()
	execAutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
	return nil
}

func (j *TableAnalysisJob) analyzePartitions(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) {
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())
	needAnalyzePartitionNames := j.Partitions
	for i := 0; i < len(needAnalyzePartitionNames); i += analyzePartitionBatchSize {
		start := i
		end := start + analyzePartitionBatchSize
		if end >= len(needAnalyzePartitionNames) {
			end = len(needAnalyzePartitionNames)
		}

		sql := getPartitionSQL("analyze table %n.%n partition", "", end-start)
		params := append([]interface{}{j.DBName, j.TableName}, []interface{}{needAnalyzePartitionNames[start:end]}...)
		execAutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
	}
}

func (j *TableAnalysisJob) analyzePartitionIndexes(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) {
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())

	for indexName, partitionNames := range j.PartitionIndexes {
		for i := 0; i < len(partitionNames); i += analyzePartitionBatchSize {
			start := i
			end := start + analyzePartitionBatchSize
			if end >= len(partitionNames) {
				end = len(partitionNames)
			}

			sql := getPartitionSQL("analyze table %n.%n partition", " index %n", end-start)
			params := append([]interface{}{j.DBName, j.TableName}, []interface{}{partitionNames[start:end]}...)
			params = append(params, indexName)
			execAutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
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

func (j *TableAnalysisJob) genSQLForAnalyzeTable() (string, []interface{}) {
	sql := "analyze table %n.%n"
	params := []interface{}{j.DBName, j.TableName}

	return sql, params
}

func (j *TableAnalysisJob) genSQLForAnalyzeIndex(index string) (string, []interface{}) {
	sql := "analyze table %n.%n index %n"
	params := []interface{}{j.DBName, j.TableName, index}

	return sql, params
}

// TODO: copy from the old implementation. We should refactor this to reduce the code duplication.
func execAutoAnalyze(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
	statsVer int,
	sql string,
	params ...interface{},
) {
	startTime := time.Now()
	_, _, err := execAnalyzeStmt(sctx, statsHandle, sysProcTracker, statsVer, sql, params...)
	dur := time.Since(startTime)
	metrics.AutoAnalyzeHistogram.Observe(dur.Seconds())
	if err != nil {
		escaped, err1 := sqlescape.EscapeSQL(sql, params...)
		if err1 != nil {
			escaped = ""
		}
		statslogutil.StatsLogger().Error(
			"auto analyze failed",
			zap.String("sql", escaped),
			zap.Duration("cost_time", dur),
			zap.Error(err),
		)
		metrics.AutoAnalyzeCounter.WithLabelValues("failed").Inc()
	} else {
		metrics.AutoAnalyzeCounter.WithLabelValues("succ").Inc()
	}
}

var execOptionForAnalyze = map[int]sqlexec.OptionFuncAlias{
	statistics.Version0: sqlexec.ExecOptionAnalyzeVer1,
	statistics.Version1: sqlexec.ExecOptionAnalyzeVer1,
	statistics.Version2: sqlexec.ExecOptionAnalyzeVer2,
}

func execAnalyzeStmt(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
	statsVer int,
	sql string,
	params ...interface{},
) ([]chunk.Row, []*ast.ResultField, error) {
	pruneMode := sctx.GetSessionVars().PartitionPruneMode.Load()
	analyzeSnapshot := sctx.GetSessionVars().EnableAnalyzeSnapshot
	optFuncs := []sqlexec.OptionFuncAlias{
		execOptionForAnalyze[statsVer],
		sqlexec.GetAnalyzeSnapshotOption(analyzeSnapshot),
		sqlexec.GetPartitionPruneModeOption(pruneMode),
		sqlexec.ExecOptionUseCurSession,
		sqlexec.ExecOptionWithSysProcTrack(statsHandle.AutoAnalyzeProcID(), sysProcTracker.Track, sysProcTracker.UnTrack),
	}
	return statsutil.ExecWithOpts(sctx, optFuncs, sql, params...)
}
