// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

var _ AnalysisJob = &StaticPartitionedTableAnalysisJob{}

const (
	analyzeStaticPartition      analyzeType = "analyzeStaticPartition"
	analyzeStaticPartitionIndex analyzeType = "analyzeStaticPartitionIndex"
)

// StaticPartitionedTableAnalysisJob is a job for analyzing a static partitioned table.
//
//nolint:fieldalignment
type StaticPartitionedTableAnalysisJob struct {
	successHook SuccessJobHook
	failureHook FailureJobHook

	GlobalTableID     int64
	StaticPartitionID int64
	IndexIDs          map[int64]struct{}

	Indicators
	TableStatsVer int
	Weight        float64

	// Lazy initialized.
	SchemaName          string
	GlobalTableName     string
	StaticPartitionName string
	IndexNames          []string
}

// NewStaticPartitionTableAnalysisJob creates a job for analyzing a static partitioned table.
func NewStaticPartitionTableAnalysisJob(
	globalTableID int64,
	partitionID int64,
	indexIDs map[int64]struct{},
	tableStatsVer int,
	changePercentage float64,
	tableSize float64,
	lastAnalysisDuration time.Duration,
) *StaticPartitionedTableAnalysisJob {
	return &StaticPartitionedTableAnalysisJob{
		GlobalTableID:     globalTableID,
		StaticPartitionID: partitionID,
		IndexIDs:          indexIDs,
		TableStatsVer:     tableStatsVer,
		Indicators: Indicators{
			ChangePercentage:     changePercentage,
			TableSize:            tableSize,
			LastAnalysisDuration: lastAnalysisDuration,
		},
	}
}

// GetTableID gets the table ID of the job.
func (j *StaticPartitionedTableAnalysisJob) GetTableID() int64 {
	// Because we only analyze the specified static partition, the table ID is the static partition ID.
	return j.StaticPartitionID
}

// Analyze analyzes the specified static partition or indexes.
func (j *StaticPartitionedTableAnalysisJob) Analyze(
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
		case analyzeStaticPartition:
			success = j.analyzeStaticPartition(sctx, statsHandle, sysProcTracker)
		case analyzeStaticPartitionIndex:
			success = j.analyzeStaticPartitionIndexes(sctx, statsHandle, sysProcTracker)
		}
		return nil
	})
}

// RegisterSuccessHook registers a successHook function that will be called after the job can be marked as successful.
func (j *StaticPartitionedTableAnalysisJob) RegisterSuccessHook(hook SuccessJobHook) {
	j.successHook = hook
}

// RegisterFailureHook registers a failureHook function that will be called after the job can be marked as failed.
func (j *StaticPartitionedTableAnalysisJob) RegisterFailureHook(hook FailureJobHook) {
	j.failureHook = hook
}

// GetIndicators implements AnalysisJob.
func (j *StaticPartitionedTableAnalysisJob) GetIndicators() Indicators {
	return j.Indicators
}

// SetIndicators implements AnalysisJob.
func (j *StaticPartitionedTableAnalysisJob) SetIndicators(indicators Indicators) {
	j.Indicators = indicators
}

// HasNewlyAddedIndex implements AnalysisJob.
func (j *StaticPartitionedTableAnalysisJob) HasNewlyAddedIndex() bool {
	return len(j.IndexIDs) > 0
}

// ValidateAndPrepare validates if the analysis job can run and prepares it for execution.
// For static partitioned tables, it checks:
// - Schema exists
// - Table exists and is partitioned
// - Specified partition exists
// - No recent failed analysis to avoid queue blocking
func (j *StaticPartitionedTableAnalysisJob) ValidateAndPrepare(
	sctx sessionctx.Context,
) (bool, string) {
	callFailureHook := func(needRetry bool) {
		if j.failureHook != nil {
			j.failureHook(j, needRetry)
		}
	}
	is := sctx.GetDomainInfoSchema()
	tableInfo, ok := is.TableInfoByID(j.GlobalTableID)
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
	partitionInfo := tableInfo.GetPartitionInfo()
	if partitionInfo == nil {
		callFailureHook(false)
		return false, notPartitionedTable
	}
	partitionName := ""
	for _, partition := range partitionInfo.Definitions {
		if partition.ID == j.StaticPartitionID {
			partitionName = partition.Name.O
			break
		}
	}
	if partitionName == "" {
		callFailureHook(false)
		return false, partitionNotExist
	}
	indexNames := make([]string, 0, len(j.IndexIDs))
	for _, index := range tableInfo.Indices {
		if _, ok := j.IndexIDs[index.ID]; ok {
			indexNames = append(indexNames, index.Name.O)
		}
	}

	j.SchemaName = schema.Name.O
	j.GlobalTableName = tableInfo.Name.O
	j.StaticPartitionName = partitionName
	j.IndexNames = indexNames

	// Check whether the partition is valid to analyze.
	// For static partition table we only need to check the specified static partition.
	if j.StaticPartitionName != "" {
		partitionNames := []string{j.StaticPartitionName}
		if valid, failReason := isValidToAnalyze(
			sctx,
			j.SchemaName,
			j.GlobalTableName,
			partitionNames...,
		); !valid {
			callFailureHook(true)
			return false, failReason
		}
	}

	return true, ""
}

// SetWeight implements AnalysisJob.
func (j *StaticPartitionedTableAnalysisJob) SetWeight(weight float64) {
	j.Weight = weight
}

// GetWeight implements AnalysisJob.
func (j *StaticPartitionedTableAnalysisJob) GetWeight() float64 {
	return j.Weight
}

// String implements fmt.Stringer interface.
func (j *StaticPartitionedTableAnalysisJob) String() string {
	return fmt.Sprintf(
		"StaticPartitionedTableAnalysisJob:\n"+
			"\tAnalyzeType: %s\n"+
			"\tIndexes: %s\n"+
			"\tSchema: %s\n"+
			"\tGlobalTable: %s\n"+
			"\tGlobalTableID: %d\n"+
			"\tStaticPartition: %s\n"+
			"\tStaticPartitionID: %d\n"+
			"\tTableStatsVer: %d\n"+
			"\tChangePercentage: %.6f\n"+
			"\tTableSize: %.2f\n"+
			"\tLastAnalysisDuration: %s\n"+
			"\tWeight: %.6f\n",
		j.getAnalyzeType(),
		strings.Join(j.IndexNames, ", "),
		j.SchemaName, j.GlobalTableName, j.GlobalTableID,
		j.StaticPartitionName, j.StaticPartitionID,
		j.TableStatsVer, j.ChangePercentage, j.TableSize,
		j.LastAnalysisDuration, j.Weight,
	)
}

func (j *StaticPartitionedTableAnalysisJob) getAnalyzeType() analyzeType {
	switch {
	case j.HasNewlyAddedIndex():
		return analyzeStaticPartitionIndex
	default:
		return analyzeStaticPartition
	}
}

func (j *StaticPartitionedTableAnalysisJob) analyzeStaticPartition(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) bool {
	sql, params := j.GenSQLForAnalyzeStaticPartition()
	return exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
}

func (j *StaticPartitionedTableAnalysisJob) analyzeStaticPartitionIndexes(
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
			sql, params := j.GenSQLForAnalyzeStaticPartitionIndex(index)
			if !exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...) {
				return false
			}
		}
		return true
	}
	// Only analyze the first index.
	// This is because analyzing a single index also analyzes all other indexes and columns.
	// Therefore, to avoid redundancy, we prevent multiple analyses of the same partition.
	firstIndex := j.IndexNames[0]
	sql, params := j.GenSQLForAnalyzeStaticPartitionIndex(firstIndex)
	return exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
}

// GenSQLForAnalyzeStaticPartition generates the SQL for analyzing the specified static partition.
func (j *StaticPartitionedTableAnalysisJob) GenSQLForAnalyzeStaticPartition() (string, []any) {
	sql := "analyze table %n.%n partition %n"
	params := []any{j.SchemaName, j.GlobalTableName, j.StaticPartitionName}

	return sql, params
}

// GenSQLForAnalyzeStaticPartitionIndex generates the SQL for analyzing the specified static partition index.
func (j *StaticPartitionedTableAnalysisJob) GenSQLForAnalyzeStaticPartitionIndex(index string) (string, []any) {
	sql := "analyze table %n.%n partition %n index %n"
	params := []any{j.SchemaName, j.GlobalTableName, j.StaticPartitionName, index}

	return sql, params
}

// AsJSON converts the job to a JSON object.
func (j *StaticPartitionedTableAnalysisJob) AsJSON() statstypes.AnalysisJobJSON {
	indexes := make([]int64, 0, len(j.IndexIDs))
	for index := range j.IndexIDs {
		indexes = append(indexes, index)
	}
	return statstypes.AnalysisJobJSON{
		Type:               string(j.getAnalyzeType()),
		TableID:            j.StaticPartitionID,
		IndexIDs:           indexes,
		Weight:             j.Weight,
		Indicators:         asJSONIndicators(j.Indicators),
		HasNewlyAddedIndex: j.HasNewlyAddedIndex(),
	}
}
