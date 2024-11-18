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
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
)

var _ AnalysisJob = &DynamicPartitionedTableAnalysisJob{}

const (
	analyzeDynamicPartition      analyzeType = "analyzeDynamicPartition"
	analyzeDynamicPartitionIndex analyzeType = "analyzeDynamicPartitionIndex"
)

// DynamicPartitionedTableAnalysisJob is a TableAnalysisJob for analyzing dynamic pruned partitioned table.
//
//nolint:fieldalignment
type DynamicPartitionedTableAnalysisJob struct {
	successHook SuccessJobHook
	failureHook FailureJobHook

	GlobalTableID     int64
	PartitionIDs      map[int64]struct{}
	PartitionIndexIDs map[int64][]int64

	// Some indicators to help us decide whether we need to analyze this table.
	Indicators
	// Analyze table with this version of statistics.
	TableStatsVer int
	// Weight is used to calculate the priority of the job.
	Weight float64

	// Lazy initialized.
	SchemaName      string
	GlobalTableName string
	// This will analyze all indexes and columns of the specified partitions.
	PartitionNames []string
	// Only set when partitions's indexes need to be analyzed.
	// It looks like: {"indexName": ["partitionName1", "partitionName2"]}
	// This is only for newly added indexes.
	// The reason why we need to record the partition names is that we need to analyze partitions in batch mode
	// and we don't want to analyze the same partition multiple times.
	// For example, the user may analyze some partitions manually, and we don't want to analyze them again.
	PartitionIndexNames map[string][]string
}

// NewDynamicPartitionedTableAnalysisJob creates a new job for analyzing a dynamic partitioned table's partitions.
func NewDynamicPartitionedTableAnalysisJob(
	tableID int64,
	partitionIDs map[int64]struct{},
	partitionIndexIDs map[int64][]int64,
	tableStatsVer int,
	changePercentage float64,
	tableSize float64,
	lastAnalysisDuration time.Duration,
) *DynamicPartitionedTableAnalysisJob {
	return &DynamicPartitionedTableAnalysisJob{
		GlobalTableID:     tableID,
		PartitionIDs:      partitionIDs,
		PartitionIndexIDs: partitionIndexIDs,
		TableStatsVer:     tableStatsVer,
		Indicators: Indicators{
			ChangePercentage:     changePercentage,
			TableSize:            tableSize,
			LastAnalysisDuration: lastAnalysisDuration,
		},
	}
}

// GetTableID gets the table ID of the job.
func (j *DynamicPartitionedTableAnalysisJob) GetTableID() int64 {
	return j.GlobalTableID
}

// Analyze analyzes the partitions or partition indexes.
func (j *DynamicPartitionedTableAnalysisJob) Analyze(
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
		case analyzeDynamicPartition:
			success = j.analyzePartitions(sctx, statsHandle, sysProcTracker)
		case analyzeDynamicPartitionIndex:
			success = j.analyzePartitionIndexes(sctx, statsHandle, sysProcTracker)
		}
		return nil
	})
}

// RegisterSuccessHook registers a successHook function that will be called after the job can be marked as successful.
func (j *DynamicPartitionedTableAnalysisJob) RegisterSuccessHook(hook SuccessJobHook) {
	j.successHook = hook
}

// RegisterFailureHook registers a failureHook function that will be called after the job can be marked as failed.
func (j *DynamicPartitionedTableAnalysisJob) RegisterFailureHook(hook FailureJobHook) {
	j.failureHook = hook
}

// GetIndicators returns the indicators of the table.
func (j *DynamicPartitionedTableAnalysisJob) GetIndicators() Indicators {
	return j.Indicators
}

// SetIndicators sets the indicators of the table.
func (j *DynamicPartitionedTableAnalysisJob) SetIndicators(indicators Indicators) {
	j.Indicators = indicators
}

// HasNewlyAddedIndex checks whether the job has newly added index.
func (j *DynamicPartitionedTableAnalysisJob) HasNewlyAddedIndex() bool {
	return len(j.PartitionIndexIDs) > 0
}

// ValidateAndPrepare validates if the analysis job can run and prepares it for execution.
// For dynamic partitioned tables, it checks:
// - Schema exists
// - Table exists and is partitioned
// - All specified partitions exist
// - No recent failed analysis for any partition to avoid queue blocking
func (j *DynamicPartitionedTableAnalysisJob) ValidateAndPrepare(
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
	partitionNames := make([]string, 0, len(j.PartitionIDs))
	for _, partition := range partitionInfo.Definitions {
		if _, ok := j.PartitionIDs[partition.ID]; ok {
			partitionNames = append(partitionNames, partition.Name.O)
		}
	}
	partitionIndexNames := make(map[string][]string, len(j.PartitionIndexIDs))
	partitionIDToName := make(map[int64]string, len(partitionInfo.Definitions))
	for _, def := range partitionInfo.Definitions {
		partitionIDToName[def.ID] = def.Name.O
	}
	for _, index := range tableInfo.Indices {
		if partitionIDs, ok := j.PartitionIndexIDs[index.ID]; ok {
			indexPartitions := make([]string, 0, len(partitionIDs))
			for _, partitionID := range partitionIDs {
				if partitionName, ok := partitionIDToName[partitionID]; ok {
					indexPartitions = append(indexPartitions, partitionName)
				}
			}
			if len(indexPartitions) > 0 {
				partitionIndexNames[index.Name.O] = indexPartitions
			}
		}
	}

	j.SchemaName = schema.Name.O
	j.GlobalTableName = tableInfo.Name.O
	j.PartitionNames = partitionNames
	j.PartitionIndexNames = partitionIndexNames

	// Check whether the table or partition is valid to analyze.
	if len(j.PartitionNames) > 0 || len(j.PartitionIndexNames) > 0 {
		// Any partition is invalid to analyze, the whole table is invalid to analyze.
		// Because we need to analyze partitions in batch mode.
		partitions := append(j.PartitionNames, getPartitionNames(j.PartitionIndexNames)...)
		if valid, failReason := isValidToAnalyze(
			sctx,
			j.SchemaName,
			j.GlobalTableName,
			partitions...,
		); !valid {
			callFailureHook(true)
			return false, failReason
		}
	}

	return true, ""
}

// SetWeight sets the weight of the job.
func (j *DynamicPartitionedTableAnalysisJob) SetWeight(weight float64) {
	j.Weight = weight
}

// GetWeight gets the weight of the job.
func (j *DynamicPartitionedTableAnalysisJob) GetWeight() float64 {
	return j.Weight
}

// String implements fmt.Stringer interface.
func (j *DynamicPartitionedTableAnalysisJob) String() string {
	return fmt.Sprintf(
		"DynamicPartitionedTableAnalysisJob:\n"+
			"\tAnalyzeType: %s\n"+
			"\tPartitions: %s\n"+
			"\tPartitionIndexes: %v\n"+
			"\tSchema: %s\n"+
			"\tGlobal Table: %s\n"+
			"\tGlobal TableID: %d\n"+
			"\tTableStatsVer: %d\n"+
			"\tChangePercentage: %.6f\n"+
			"\tTableSize: %.2f\n"+
			"\tLastAnalysisDuration: %s\n"+
			"\tWeight: %.6f\n",
		j.getAnalyzeType(),
		strings.Join(j.PartitionNames, ", "),
		j.PartitionIndexNames,
		j.SchemaName, j.GlobalTableName,
		j.GlobalTableID, j.TableStatsVer, j.ChangePercentage,
		j.TableSize, j.LastAnalysisDuration, j.Weight,
	)
}

// analyzePartitions performs analysis on the specified partitions.
// This function uses a batch mode for efficiency. After analyzing the partitions,
// it's necessary to merge their statistics. By analyzing them in batches,
// we can reduce the overhead of this merging process.
func (j *DynamicPartitionedTableAnalysisJob) analyzePartitions(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) bool {
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())
	needAnalyzePartitionNames := make([]any, 0, len(j.PartitionNames))
	for _, partition := range j.PartitionNames {
		needAnalyzePartitionNames = append(needAnalyzePartitionNames, partition)
	}
	for i := 0; i < len(needAnalyzePartitionNames); i += analyzePartitionBatchSize {
		start := i
		end := start + analyzePartitionBatchSize
		if end >= len(needAnalyzePartitionNames) {
			end = len(needAnalyzePartitionNames)
		}

		sql := getPartitionSQL("analyze table %n.%n partition", "", end-start)
		params := append([]any{j.SchemaName, j.GlobalTableName}, needAnalyzePartitionNames[start:end]...)
		success := exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
		if !success {
			return false
		}
	}
	return true
}

// analyzePartitionIndexes performs analysis on the specified partition indexes.
func (j *DynamicPartitionedTableAnalysisJob) analyzePartitionIndexes(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) (success bool) {
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())
	// For version 2, analyze one index will analyze all other indexes and columns.
	// For version 1, analyze one index will only analyze the specified index.
	analyzeVersion := sctx.GetSessionVars().AnalyzeVersion

	for indexName, partitionNames := range j.PartitionIndexNames {
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
			params := append([]any{j.SchemaName, j.GlobalTableName}, needAnalyzePartitionNames[start:end]...)
			params = append(params, indexName)
			success = exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
			if !success {
				return false
			}
		}
		// For version 1, we need to analyze all indexes.
		if analyzeVersion != 1 {
			// Halt execution after analyzing one index.
			// This is because analyzing a single index also analyzes all other indexes and columns.
			// Therefore, to avoid redundancy, we prevent multiple analyses of the same partition.
			break
		}
	}
	return
}

func (j *DynamicPartitionedTableAnalysisJob) getAnalyzeType() analyzeType {
	switch {
	case j.HasNewlyAddedIndex():
		return analyzeDynamicPartitionIndex
	default:
		return analyzeDynamicPartition
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

func getPartitionNames(partitionIndexes map[string][]string) []string {
	names := make([]string, 0, len(partitionIndexes))
	for _, partitionNames := range partitionIndexes {
		names = append(names, partitionNames...)
	}
	return names
}
