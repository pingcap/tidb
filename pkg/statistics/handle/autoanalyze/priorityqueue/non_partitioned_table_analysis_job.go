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
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
)

var _ AnalysisJob = &NonPartitionedTableAnalysisJob{}

const (
	analyzeTable analyzeType = "analyzeTable"
	analyzeIndex analyzeType = "analyzeIndex"
)

// NonPartitionedTableAnalysisJob is a TableAnalysisJob for analyzing the physical table.
//
//nolint:fieldalignment
type NonPartitionedTableAnalysisJob struct {
	successHook SuccessJobHook
	failureHook FailureJobHook

	TableID  int64
	IndexIDs map[int64]struct{}

	Indicators
	TableStatsVer int
	Weight        float64

	// Lazy initialized.
	SchemaName string
	TableName  string
	IndexNames []string
}

// NewNonPartitionedTableAnalysisJob creates a new TableAnalysisJob for analyzing the physical table.
func NewNonPartitionedTableAnalysisJob(
	tableID int64,
	indexIDs map[int64]struct{},
	tableStatsVer int,
	changePercentage float64,
	tableSize float64,
	lastAnalysisDuration time.Duration,
) *NonPartitionedTableAnalysisJob {
	return &NonPartitionedTableAnalysisJob{
		TableID:       tableID,
		IndexIDs:      indexIDs,
		TableStatsVer: tableStatsVer,
		Indicators: Indicators{
			ChangePercentage:     changePercentage,
			TableSize:            tableSize,
			LastAnalysisDuration: lastAnalysisDuration,
		},
	}
}

// GetTableID gets the table ID of the job.
func (j *NonPartitionedTableAnalysisJob) GetTableID() int64 {
	return j.TableID
}

// Analyze analyzes the table or indexes.
func (j *NonPartitionedTableAnalysisJob) Analyze(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) error {
	success := true
	defer func() {
		if success {
			if j.successHook != nil {
				j.successHook(j)
			}
		} else {
			if j.failureHook != nil {
				j.failureHook(j, true)
			}
		}
	}()

	return statsutil.CallWithSCtx(statsHandle.SPool(), func(sctx sessionctx.Context) error {
		switch j.getAnalyzeType() {
		case analyzeTable:
			success = j.analyzeTable(sctx, statsHandle, sysProcTracker)
		case analyzeIndex:
			success = j.analyzeIndexes(sctx, statsHandle, sysProcTracker)
		}
		return nil
	})
}

// RegisterSuccessHook registers a successHook function that will be called after the job can be marked as successful.
func (j *NonPartitionedTableAnalysisJob) RegisterSuccessHook(hook SuccessJobHook) {
	j.successHook = hook
}

// RegisterFailureHook registers a failureHook function that will be called after the job can be marked as failed.
func (j *NonPartitionedTableAnalysisJob) RegisterFailureHook(hook FailureJobHook) {
	j.failureHook = hook
}

// HasNewlyAddedIndex checks whether the table has newly added indexes.
func (j *NonPartitionedTableAnalysisJob) HasNewlyAddedIndex() bool {
	return len(j.IndexIDs) > 0
}

// ValidateAndPrepare validates if the analysis job can run and prepares it for execution.
// For non-partitioned tables, it checks:
// - Schema exists
// - Table exists
// - No recent failed analysis to avoid queue blocking
func (j *NonPartitionedTableAnalysisJob) ValidateAndPrepare(
	sctx sessionctx.Context,
) (bool, string) {
	callFailureHook := func(needRetry bool) {
		if j.failureHook != nil {
			j.failureHook(j, needRetry)
		}
	}
	is := sctx.GetDomainInfoSchema()
	tableInfo, ok := is.TableInfoByID(j.TableID)
	if !ok {
		callFailureHook(false)
		return false, tableNotExist
	}
	dbID := tableInfo.DBID
	schema, ok := is.SchemaByID(dbID)
	if !ok {
		callFailureHook(false)
		return false, schemaNotExist
	}
	tableName := tableInfo.Name.O
	indexNames := make([]string, 0, len(j.IndexIDs))
	for _, index := range tableInfo.Indices {
		if _, ok := j.IndexIDs[index.ID]; ok {
			indexNames = append(indexNames, index.Name.O)
		}
	}

	j.SchemaName = schema.Name.O
	j.TableName = tableName
	j.IndexNames = indexNames
	if valid, failReason := isValidToAnalyze(
		sctx,
		j.SchemaName,
		j.TableName,
	); !valid {
		callFailureHook(true)
		return false, failReason
	}

	return true, ""
}

// SetWeight sets the weight of the job.
func (j *NonPartitionedTableAnalysisJob) SetWeight(weight float64) {
	j.Weight = weight
}

// GetWeight gets the weight of the job.
func (j *NonPartitionedTableAnalysisJob) GetWeight() float64 {
	return j.Weight
}

// GetIndicators returns the indicators of the table.
func (j *NonPartitionedTableAnalysisJob) GetIndicators() Indicators {
	return j.Indicators
}

// SetIndicators sets the indicators of the table.
func (j *NonPartitionedTableAnalysisJob) SetIndicators(indicators Indicators) {
	j.Indicators = indicators
}

// String implements fmt.Stringer interface.
func (j *NonPartitionedTableAnalysisJob) String() string {
	return fmt.Sprintf(
		"NonPartitionedTableAnalysisJob:\n"+
			"\tAnalyzeType: %s\n"+
			"\tIndexes: %s\n"+
			"\tSchema: %s\n"+
			"\tTable: %s\n"+
			"\tTableID: %d\n"+
			"\tTableStatsVer: %d\n"+
			"\tChangePercentage: %.6f\n"+
			"\tTableSize: %.2f\n"+
			"\tLastAnalysisDuration: %v\n"+
			"\tWeight: %.6f\n",
		j.getAnalyzeType(),
		strings.Join(j.IndexNames, ", "),
		j.SchemaName, j.TableName, j.TableID, j.TableStatsVer,
		j.ChangePercentage, j.TableSize, j.LastAnalysisDuration, j.Weight,
	)
}
func (j *NonPartitionedTableAnalysisJob) getAnalyzeType() analyzeType {
	if j.HasNewlyAddedIndex() {
		return analyzeIndex
	}
	return analyzeTable
}

func (j *NonPartitionedTableAnalysisJob) analyzeTable(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) bool {
	sql, params := j.GenSQLForAnalyzeTable()
	return exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
}

// GenSQLForAnalyzeTable generates the SQL for analyzing the specified table.
func (j *NonPartitionedTableAnalysisJob) GenSQLForAnalyzeTable() (string, []any) {
	sql := "analyze table %n.%n"
	params := []any{j.SchemaName, j.TableName}

	return sql, params
}

func (j *NonPartitionedTableAnalysisJob) analyzeIndexes(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) bool {
	if len(j.IndexNames) == 0 {
		return true
	}
	// For version 2, analyze one index will analyze all other indexes and columns.
	// For version 1, analyze one index will only analyze the specified index.
	analyzeVersion := sctx.GetSessionVars().AnalyzeVersion
	if analyzeVersion == 1 {
		for _, index := range j.IndexNames {
			sql, params := j.GenSQLForAnalyzeIndex(index)
			if !exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...) {
				return false
			}
		}
		return true
	}
	// Only analyze the first index.
	// This is because analyzing a single index also analyzes all other indexes and columns.
	// Therefore, to avoid redundancy, we prevent multiple analyses of the same table.
	firstIndex := j.IndexNames[0]
	sql, params := j.GenSQLForAnalyzeIndex(firstIndex)
	return exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
}

// GenSQLForAnalyzeIndex generates the SQL for analyzing the specified index.
func (j *NonPartitionedTableAnalysisJob) GenSQLForAnalyzeIndex(index string) (string, []any) {
	sql := "analyze table %n.%n index %n"
	params := []any{j.SchemaName, j.TableName, index}

	return sql, params
}
