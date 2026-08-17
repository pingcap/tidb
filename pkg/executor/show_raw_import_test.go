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

package executor

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/importinto/jobstats"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBuildRawImportJobStats(t *testing.T) {
	loc := time.UTC
	t2024 := types.NewTime(types.FromGoTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)), mysql.TypeTimestamp, 0)
	t2025 := types.NewTime(types.FromGoTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)), mysql.TypeTimestamp, 0)

	info := &importer.JobInfo{
		ID:          1,
		GroupKey:    "g1",
		TableSchema: "test",
		TableName:   "t",
		TableID:     42,
		CreatedBy:   "u@h",
		Parameters: importer.ImportParameters{
			FileLocation: "s3://bucket/prefix",
		},
		SourceFileSize: 123,
		Step:           "importing",
		Status:         importer.JobStatusRunning,
		CreateTime:     t2024,
		UpdateTime:     t2024,
	}
	runInfo := &importinto.RuntimeInfo{
		ImportRows: 10,
		Step:       proto.ImportStepImport,
		Processed:  50,
		Total:      100,
		Speed:      10,
		UpdateTime: t2025,
		ErrorMsg:   "should-not-be-used",
		Status:     proto.TaskStateRunning,
	}

	stats, err := buildRawImportJobStats(loc, info, runInfo)
	require.NoError(t, err)
	require.Equal(t, jobstats.ContractVersion, stats.Version)
	require.Equal(t, int64(1), stats.JobID)
	require.Equal(t, "g1", stats.GroupKey)
	require.Equal(t, "s3://bucket/prefix", stats.DataSource)
	require.Equal(t, utils.EncloseDBAndTable("test", "t"), stats.TargetTable)
	require.Equal(t, int64(42), stats.TableID)
	require.Equal(t, "importing", stats.Phase)
	require.Equal(t, importer.JobStatusRunning, stats.Status)
	require.Equal(t, jobstats.StatusCategoryRunning, stats.StatusCategory)
	require.False(t, stats.Terminal)
	require.Equal(t, int64(123), stats.SourceFileSizeBytes)
	require.Equal(t, "u@h", stats.CreatedBy)
	rawJSON, err := json.Marshal(stats)
	require.NoError(t, err)
	require.NotContains(t, string(rawJSON), "job_id")
	require.NotContains(t, string(rawJSON), "group_key")
	require.NotContains(t, string(rawJSON), "error_message")
	require.Contains(t, string(rawJSON), "job_phase")

	require.NotNil(t, stats.ImportedRows)
	require.Equal(t, int64(10), *stats.ImportedRows)
	require.NotNil(t, stats.CurrentStep)
	require.Equal(t, "import", stats.CurrentStep.Name)
	require.Equal(t, int64(50), stats.CurrentStep.ProcessedBytes)
	require.Equal(t, int64(100), stats.CurrentStep.TotalBytes)
	require.Equal(t, int64(10), stats.CurrentStep.SpeedBytesPerSec)
	require.Zero(t, stats.CurrentStep.ProcessedConflicts)
	require.NotNil(t, stats.CurrentStep.RemainingSeconds)
	require.Equal(t, int64(5), *stats.CurrentStep.RemainingSeconds)

	require.Equal(t, int64(1704067200), stats.CreateTimeUnix)
	require.Equal(t, int64(1735689600), stats.UpdateTimeUnix)

	// The supplied location must be the one used to decode JobInfo timestamps.
	tokyo := time.FixedZone("UTC+9", 9*60*60)
	nonUTCStats, err := buildRawImportJobStats(tokyo, info, runInfo)
	require.NoError(t, err)
	require.Equal(t, int64(1704034800), nonUTCStats.CreateTimeUnix)
	require.Equal(t, int64(1735657200), nonUTCStats.UpdateTimeUnix)

	runInfo.Step = proto.ImportStepConflictResolution
	runInfo.Processed = 3
	runInfo.Total = 5
	runInfo.Speed = 2
	stats, err = buildRawImportJobStats(loc, info, runInfo)
	require.NoError(t, err)
	require.NotNil(t, stats.CurrentStep)
	require.Equal(t, "conflict-resolution", stats.CurrentStep.Name)
	require.Zero(t, stats.CurrentStep.ProcessedBytes)
	require.Equal(t, int64(3), stats.CurrentStep.ProcessedConflicts)
	require.Equal(t, int64(5), stats.CurrentStep.TotalConflicts)
	require.Equal(t, int64(2), stats.CurrentStep.SpeedConflictsPerSec)

	info.Status = importer.JobStatusFinished
	info.Summary = &importer.Summary{
		EncodeSummary: importer.StepSummary{Bytes: 10, RowCnt: 2},
		ImportedRows:  99,
	}
	info.EndTime = t2025
	stats, err = buildRawImportJobStats(loc, info, nil)
	require.NoError(t, err)
	require.NotNil(t, stats.ImportedRows)
	require.Equal(t, int64(99), *stats.ImportedRows)
	require.Equal(t, jobstats.StatusCategoryTerminal, stats.StatusCategory)
	require.True(t, stats.Terminal)
	require.True(t, stats.IsCompleted())
	require.NotNil(t, stats.Summary)
	require.Equal(t, int64(99), stats.Summary.ImportedRows)
	require.Len(t, stats.Summary.Steps, 1)
	require.Equal(t, "encode", stats.Summary.Steps[0].Name)
	require.Equal(t, int64(10), stats.Summary.Steps[0].InputBytes)
	require.Nil(t, stats.CurrentStep)
	require.Equal(t, int64(1735689600), stats.UpdateTimeUnix)

	info.Status = "failed"
	info.ErrorMessage = "load failed"
	stats, err = buildRawImportJobStats(loc, info, nil)
	require.NoError(t, err)
	require.NotNil(t, stats.Error)
	require.Equal(t, "load failed", stats.Error.Message)
}
