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
type DynamicPartitionedTableAnalysisJob struct {
	// Only set when partitions's indexes need to be analyzed.
	// It looks like: {"indexName": ["partitionName1", "partitionName2"]}
	// This is only for newly added indexes.
	// The reason why we need to record the partition names is that we need to analyze partitions in batch mode
	// and we don't want to analyze the same partition multiple times.
	// For example, the user may analyze some partitions manually, and we don't want to analyze them again.
	PartitionIndexes map[string][]string

	TableSchema     string
	GlobalTableName string
	// This will analyze all indexes and columns of the specified partitions.
	Partitions []string
	// Some indicators to help us decide whether we need to analyze this table.
	Indicators
	GlobalTableID int64

	// Analyze table with this version of statistics.
	TableStatsVer int
	// Weight is used to calculate the priority of the job.
	Weight float64
}

// NewDynamicPartitionedTableAnalysisJob creates a new job for analyzing a dynamic partitioned table's partitions.
func NewDynamicPartitionedTableAnalysisJob(
	schema, tableName string,
	tableID int64,
	partitions []string,
	partitionIndexes map[string][]string,
	tableStatsVer int,
	changePercentage float64,
	tableSize float64,
	lastAnalysisDuration time.Duration,
) *DynamicPartitionedTableAnalysisJob {
	return &DynamicPartitionedTableAnalysisJob{
		GlobalTableID:    tableID,
		TableSchema:      schema,
		GlobalTableName:  tableName,
		Partitions:       partitions,
		PartitionIndexes: partitionIndexes,
		TableStatsVer:    tableStatsVer,
		Indicators: Indicators{
			ChangePercentage:     changePercentage,
			TableSize:            tableSize,
			LastAnalysisDuration: lastAnalysisDuration,
		},
	}
}

// Analyze analyzes the partitions or partition indexes.
func (j *DynamicPartitionedTableAnalysisJob) Analyze(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) error {
	return statsutil.CallWithSCtx(statsHandle.SPool(), func(sctx sessionctx.Context) error {
		switch j.getAnalyzeType() {
		case analyzeDynamicPartition:
			j.analyzePartitions(sctx, statsHandle, sysProcTracker)
		case analyzeDynamicPartitionIndex:
			j.analyzePartitionIndexes(sctx, statsHandle, sysProcTracker)
		}
		return nil
	})
}

// GetIndicators returns the indicators of the table.
func (j *DynamicPartitionedTableAnalysisJob) GetIndicators() Indicators {
	return j.Indicators
}

// HasNewlyAddedIndex checks whether the job has newly added index.
func (j *DynamicPartitionedTableAnalysisJob) HasNewlyAddedIndex() bool {
	return len(j.PartitionIndexes) > 0
}

// IsValidToAnalyze checks whether the table or partition is valid to analyze.
// We need to check each partition to determine whether the table is valid to analyze.
func (j *DynamicPartitionedTableAnalysisJob) IsValidToAnalyze(
	sctx sessionctx.Context,
) (bool, string) {
	// Check whether the table or partition is valid to analyze.
	if len(j.Partitions) > 0 || len(j.PartitionIndexes) > 0 {
		// Any partition is invalid to analyze, the whole table is invalid to analyze.
		// Because we need to analyze partitions in batch mode.
		partitions := append(j.Partitions, getPartitionNames(j.PartitionIndexes)...)
		if valid, failReason := isValidToAnalyze(
			sctx,
			j.TableSchema,
			j.GlobalTableName,
			partitions...,
		); !valid {
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
		strings.Join(j.Partitions, ", "),
		j.PartitionIndexes,
		j.TableSchema, j.GlobalTableName,
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
		params := append([]any{j.TableSchema, j.GlobalTableName}, needAnalyzePartitionNames[start:end]...)
		exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
	}
}

// analyzePartitionIndexes performs analysis on the specified partition indexes.
func (j *DynamicPartitionedTableAnalysisJob) analyzePartitionIndexes(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) {
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())

OnlyPickOneIndex:
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
			params := append([]any{j.TableSchema, j.GlobalTableName}, needAnalyzePartitionNames[start:end]...)
			params = append(params, indexName)
			exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
			// Halt execution after analyzing one index.
			// This is because analyzing a single index also analyzes all other indexes and columns.
			// Therefore, to avoid redundancy, we prevent multiple analyses of the same partition.
			break OnlyPickOneIndex
		}
	}
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
