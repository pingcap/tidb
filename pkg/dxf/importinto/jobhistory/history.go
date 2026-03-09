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
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
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
	ImportID               int64    `json:"import_id"`
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

	taskKey := importinto.TaskKeyForKeyspace(keyspace, jobID)
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
			(
				select cast(sum(cast(json_extract(summary, '$.bytes') as signed)) as signed)
				from mysql.tidb_background_subtask_history
				where task_key = t.id and step = %?
					and json_extract(cast(meta as char), '$."kv-group"') = 'data'
			) as data_kv_size_bytes,
			(
				select cast(sum(cast(json_extract(summary, '$.bytes') as signed)) as signed)
				from mysql.tidb_background_subtask_history
				where task_key = t.id and step = %?
					and json_extract(cast(meta as char), '$."kv-group"') != 'data'
			) as index_kv_size_bytes,
			(
				select TIMESTAMPDIFF(second, FROM_UNIXTIME(min(start_time)), FROM_UNIXTIME(max(state_update_time)))
				from mysql.tidb_background_subtask_history
				where task_key = t.id and start_time > 0 and state_update_time > 0
			) as total_duration_seconds,
			cast(json_extract(cast(cast(t.meta as char) as json), '$.Summary."row-count"') as signed) as row_count
		from mysql.tidb_global_task_history t
		where t.task_key = %? and t.type = %?`,
		proto.ImportStepWriteAndIngest, proto.ImportStepWriteAndIngest, taskKey, proto.ImportInto)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.Annotatef(storage.ErrTaskNotFound, "import-into job %d in keyspace %s not found in history", jobID, keyspace)
	}

	row := rows[0]
	info := &Info{
		ImportID:     jobID,
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
		info.DataKVSize = formatBytes(row.GetInt64(8))
	}
	if !row.IsNull(9) {
		info.IndexKVSize = formatBytes(row.GetInt64(9))
	}

	var totalDurationSeconds int64
	if !row.IsNull(10) {
		totalDurationSeconds = row.GetInt64(10)
		info.Duration.Total = formatDuration(totalDurationSeconds)
	}
	info.PerCoreSpeed = formatBytesPerCoreHour(totalFileBytes, totalDurationSeconds, info.MaxNodeCount, info.Concurrency)
	info.OverallSpeed = formatBytesPerHour(totalFileBytes, totalDurationSeconds)

	if !row.IsNull(11) {
		info.RowCount = row.GetInt64(11)
	}
	if info.RowCount > 0 {
		info.RowLength = int64(math.Round(float64(totalFileBytes) / float64(info.RowCount)))
	}

	stepRows, err := mgr.ExecuteSQLWithNewSession(ctx, `
		select step, TIMESTAMPDIFF(second, FROM_UNIXTIME(min(start_time)), FROM_UNIXTIME(max(state_update_time))) as duration_seconds
		from mysql.tidb_background_subtask_history
		where task_key = %? and start_time > 0 and state_update_time > 0
		group by step`,
		info.TaskID)
	if err != nil {
		return nil, err
	}
	for _, stepRow := range stepRows {
		if stepRow.IsNull(1) {
			continue
		}
		duration := formatDuration(stepRow.GetInt64(1))
		switch proto.Step(stepRow.GetInt64(0)) {
		case proto.ImportStepEncodeAndSort:
			info.Duration.Encode = duration
		case proto.ImportStepMergeSort:
			info.Duration.MergeSort = duration
		case proto.ImportStepWriteAndIngest:
			info.Duration.Ingest = duration
		case proto.ImportStepCollectConflicts:
			info.Duration.CollectConflicts = duration
		case proto.ImportStepConflictResolution:
			info.Duration.ResolveConflicts = duration
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
