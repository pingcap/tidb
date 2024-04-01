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
type StaticPartitionedTableAnalysisJob struct {
	TableSchema         string
	GlobalTableName     string
	StaticPartitionName string
	// This is only for newly added indexes.
	Indexes []string

	Indicators
	GlobalTableID     int64
	StaticPartitionID int64

	TableStatsVer int
	Weight        float64
}

// NewStaticPartitionTableAnalysisJob creates a job for analyzing a static partitioned table.
func NewStaticPartitionTableAnalysisJob(
	schema, globalTableName string,
	globalTableID int64,
	partitionName string,
	partitionID int64,
	indexes []string,
	tableStatsVer int,
	changePercentage float64,
	tableSize float64,
	lastAnalysisDuration time.Duration,
) *StaticPartitionedTableAnalysisJob {
	return &StaticPartitionedTableAnalysisJob{
		GlobalTableID:       globalTableID,
		TableSchema:         schema,
		GlobalTableName:     globalTableName,
		StaticPartitionID:   partitionID,
		StaticPartitionName: partitionName,
		Indexes:             indexes,
		TableStatsVer:       tableStatsVer,
		Indicators: Indicators{
			ChangePercentage:     changePercentage,
			TableSize:            tableSize,
			LastAnalysisDuration: lastAnalysisDuration,
		},
	}
}

// Analyze analyzes the specified static partition or indexes.
func (j *StaticPartitionedTableAnalysisJob) Analyze(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) error {
	return statsutil.CallWithSCtx(statsHandle.SPool(), func(sctx sessionctx.Context) error {
		switch j.getAnalyzeType() {
		case analyzeStaticPartition:
			j.analyzeStaticPartition(sctx, statsHandle, sysProcTracker)
		case analyzeStaticPartitionIndex:
			j.analyzeStaticPartitionIndexes(sctx, statsHandle, sysProcTracker)
		}
		return nil
	})
}

// GetIndicators implements AnalysisJob.
func (j *StaticPartitionedTableAnalysisJob) GetIndicators() Indicators {
	return j.Indicators
}

// HasNewlyAddedIndex implements AnalysisJob.
func (j *StaticPartitionedTableAnalysisJob) HasNewlyAddedIndex() bool {
	return len(j.Indexes) > 0
}

// IsValidToAnalyze checks whether the partition is valid to analyze.
// Only the specified static partition is checked.
func (j *StaticPartitionedTableAnalysisJob) IsValidToAnalyze(
	sctx sessionctx.Context,
) (bool, string) {
	// Check whether the partition is valid to analyze.
	// For static partition table we only need to check the specified static partition.
	if j.StaticPartitionName != "" {
		partitionNames := []string{j.StaticPartitionName}
		if valid, failReason := isValidToAnalyze(
			sctx,
			j.TableSchema,
			j.GlobalTableName,
			partitionNames...,
		); !valid {
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
		strings.Join(j.Indexes, ", "),
		j.TableSchema, j.GlobalTableName, j.GlobalTableID,
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
) {
	sql, params := j.GenSQLForAnalyzeStaticPartition()
	exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
}

func (j *StaticPartitionedTableAnalysisJob) analyzeStaticPartitionIndexes(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) {
	if len(j.Indexes) == 0 {
		return
	}
	// Only analyze the first index.
	// This is because analyzing a single index also analyzes all other indexes and columns.
	// Therefore, to avoid redundancy, we prevent multiple analyses of the same partition.
	firstIndex := j.Indexes[0]
	sql, params := j.GenSQLForAnalyzeStaticPartitionIndex(firstIndex)
	exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
}

// GenSQLForAnalyzeStaticPartition generates the SQL for analyzing the specified static partition.
func (j *StaticPartitionedTableAnalysisJob) GenSQLForAnalyzeStaticPartition() (string, []any) {
	sql := "analyze table %n.%n partition %n"
	params := []any{j.TableSchema, j.GlobalTableName, j.StaticPartitionName}

	return sql, params
}

// GenSQLForAnalyzeStaticPartitionIndex generates the SQL for analyzing the specified static partition index.
func (j *StaticPartitionedTableAnalysisJob) GenSQLForAnalyzeStaticPartitionIndex(index string) (string, []any) {
	sql := "analyze table %n.%n partition %n index %n"
	params := []any{j.TableSchema, j.GlobalTableName, j.StaticPartitionName, index}

	return sql, params
}
