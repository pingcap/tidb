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

package jobhistory

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskkey"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
)

// Duration records elapsed time of an IMPORT INTO history job.
type Duration struct {
	// All fields use Go time.Duration string format, for example "40m0s".
	Total            string `json:"total"`
	Encode           string `json:"encode"`
	MergeSort        string `json:"merge_sort"`
	Ingest           string `json:"ingest"`
	CollectConflicts string `json:"collect_conflicts"`
	ResolveConflicts string `json:"resolve_conflicts"`
	PostProcess      string `json:"post_process"`
}

// Info contains detailed information for one IMPORT INTO history job.
type Info struct {
	JobID                  int64    `json:"job_id"`
	Keyspace               string   `json:"keyspace"`
	TaskID                 int64    `json:"task_id"`
	State                  string   `json:"state"`
	Concurrency            int      `json:"concurrency"`
	MaxNodeCount           int      `json:"max_node_count"`
	DistSQLScanConcurrency int      `json:"distsql_scan_concurrency"`
	IndexCount             int      `json:"index_count"`
	ColumnCount            int      `json:"column_count"`
	FileSize               string   `json:"file_size"`
	DataKVSize             string   `json:"data_kv_size"`
	IndexKVSize            string   `json:"index_kv_size"`
	PerCoreSpeed           string   `json:"per_core_speed"`
	OverallSpeed           string   `json:"overall_speed"`
	RowCount               int64    `json:"row_count"`
	RowLength              int64    `json:"row_length"`
	Duration               Duration `json:"duration"`
}

// GetFromHistory returns IMPORT INTO job info from history table only.
// It returns ErrTaskNotFound when no matching history task exists.
func GetFromHistory(
	ctx context.Context,
	mgr *storage.TaskManager,
	keyspace string,
	jobID int64,
) (*Info, error) {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}

	taskKey := taskkey.ForJobInKeyspace(keyspace, jobID)
	// NOTE: mysql.tidb_background_subtask_history.task_key stores task ID, not task_key string.
	rows, err := mgr.ExecuteSQLWithNewSession(ctx, `
		select
			t.id,
			t.state,
			t.concurrency,
			t.max_node_count,
			cast(json_extract(cast(cast(t.meta as char) as json), '$.Plan.DistSQLScanConcurrency') as signed) as distsql_scan_concurrency,
			cast(json_length(json_extract(cast(cast(t.meta as char) as json), '$.Plan.DesiredTableInfo.index_info')) as signed) as index_count,
			cast(json_length(json_extract(cast(cast(t.meta as char) as json), '$.Plan.DesiredTableInfo.cols')) as signed) as column_count,
			cast(json_extract(cast(cast(t.meta as char) as json), '$.Plan.TotalFileSize') as signed) as file_size_bytes,
			cast(json_extract(cast(cast(t.meta as char) as json), '$.Summary."row-count"') as signed) as row_count
		from mysql.tidb_global_task_history t
		where t.task_key = %? and t.type = %?`,
		taskKey, proto.ImportInto)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.Annotatef(storage.ErrTaskNotFound, "import-into job %d in keyspace %s not found in history", jobID, keyspace)
	}

	row := rows[0]
	info := &Info{
		JobID:        jobID,
		Keyspace:     keyspace,
		TaskID:       row.GetInt64(0),
		State:        row.GetString(1),
		Concurrency:  int(row.GetInt64(2)),
		MaxNodeCount: int(row.GetInt64(3)),
	}
	if !row.IsNull(4) {
		info.DistSQLScanConcurrency = int(row.GetInt64(4))
	}
	if !row.IsNull(5) {
		info.IndexCount = int(row.GetInt64(5))
	}
	if !row.IsNull(6) {
		info.ColumnCount = int(row.GetInt64(6))
	}

	var totalFileBytes int64
	if !row.IsNull(7) {
		totalFileBytes = row.GetInt64(7)
		info.FileSize = formatBytes(totalFileBytes)
	}
	if !row.IsNull(8) {
		info.RowCount = row.GetInt64(8)
	}
	if info.RowCount > 0 {
		info.RowLength = int64(math.Round(float64(totalFileBytes) / float64(info.RowCount)))
	}

	stepRows, err := mgr.ExecuteSQLWithNewSession(ctx, `
		select
			step,
			json_unquote(json_extract(cast(meta as char), '$."kv-group"')) as kv_group,
			cast(sum(cast(json_extract(summary, '$.bytes') as signed)) as signed) as bytes,
			min(case when start_time > 0 then start_time else null end) as min_start_time,
			max(case when state_update_time > 0 then state_update_time else null end) as max_state_update_time
		from mysql.tidb_background_subtask_history
		where task_key = %?
		group by step, kv_group`,
		info.TaskID)
	if err != nil {
		return nil, err
	}
	var (
		dataKVSizeBytes  int64
		indexKVSizeBytes int64
		hasDataKVSize    bool
		hasIndexKVSize   bool
		minStartTime     int64
		maxUpdateTime    int64
		hasTotalDuration bool
	)
	stepDurations := make(map[proto.Step][2]int64)
	for _, stepRow := range stepRows {
		step := proto.Step(stepRow.GetInt64(0))
		kvGroup := ""
		if !stepRow.IsNull(1) {
			kvGroup = stepRow.GetString(1)
		}
		if step == proto.ImportStepWriteAndIngest && !stepRow.IsNull(2) {
			if kvGroup == "data" {
				dataKVSizeBytes += stepRow.GetInt64(2)
				hasDataKVSize = true
			} else {
				indexKVSizeBytes += stepRow.GetInt64(2)
				hasIndexKVSize = true
			}
		}

		if stepRow.IsNull(3) || stepRow.IsNull(4) {
			continue
		}
		startTime := stepRow.GetInt64(3)
		updateTime := stepRow.GetInt64(4)
		if !hasTotalDuration {
			minStartTime, maxUpdateTime = startTime, updateTime
			hasTotalDuration = true
		} else {
			minStartTime = min(minStartTime, startTime)
			maxUpdateTime = max(maxUpdateTime, updateTime)
		}

		existing, ok := stepDurations[step]
		if !ok {
			stepDurations[step] = [2]int64{startTime, updateTime}
			continue
		}
		existing[0] = min(existing[0], startTime)
		existing[1] = max(existing[1], updateTime)
		stepDurations[step] = existing
	}

	if hasDataKVSize {
		info.DataKVSize = formatBytes(dataKVSizeBytes)
	}
	if hasIndexKVSize {
		info.IndexKVSize = formatBytes(indexKVSizeBytes)
	}

	var totalDurationSeconds int64
	if hasTotalDuration {
		totalDurationSeconds = max(maxUpdateTime-minStartTime, 0)
		info.Duration.Total = formatDuration(totalDurationSeconds)
	}
	info.PerCoreSpeed = formatBytesPerCoreHour(totalFileBytes, totalDurationSeconds, info.MaxNodeCount, info.Concurrency)
	info.OverallSpeed = formatBytesPerHour(totalFileBytes, totalDurationSeconds)

	for step, bounds := range stepDurations {
		duration := formatDuration(max(bounds[1]-bounds[0], 0))
		switch step {
		case proto.ImportStepEncodeAndSort:
			info.Duration.Encode = duration
		case proto.ImportStepMergeSort:
			info.Duration.MergeSort = duration
		case proto.ImportStepWriteAndIngest:
			info.Duration.Ingest = duration
		case proto.ImportStepPostProcess:
			info.Duration.PostProcess = duration
		}
	}
	return info, nil
}

func formatDuration(seconds int64) string {
	if seconds < 0 {
		return ""
	}
	return (time.Duration(seconds) * time.Second).String()
}

func formatBytes(size int64) string {
	if size < 0 {
		return ""
	}
	return units.BytesSize(float64(size))
}

func formatBytesPerHour(totalBytes int64, durationSeconds int64) string {
	if totalBytes < 0 || durationSeconds <= 0 {
		return ""
	}
	bytesPerHour := float64(totalBytes) * float64(time.Hour/time.Second) / float64(durationSeconds)
	return fmt.Sprintf("%s/hour", units.BytesSize(bytesPerHour))
}

func formatBytesPerCoreHour(totalBytes int64, durationSeconds int64, maxNodeCount int, taskConcurrency int) string {
	if totalBytes < 0 || durationSeconds <= 0 || maxNodeCount <= 0 || taskConcurrency <= 0 {
		return ""
	}
	cores := float64(maxNodeCount * taskConcurrency)
	bytesPerCoreHour := float64(totalBytes) * float64(time.Hour/time.Second) / float64(durationSeconds) / cores
	return fmt.Sprintf("%s/core/hour", units.BytesSize(bytesPerCoreHour))
}
