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

// Package export implements the DXF task type for distributed single-table
// export. This is a performance-testing prototype: it only supports CSV
// output and skips the user-facing job table, PostProcess and revert steps.
package export

import (
	"github.com/pingcap/tidb/pkg/meta/model"
)

// TaskMeta is the task meta of an export task.
type TaskMeta struct {
	DBName     string           `json:"db_name"`
	TableInfo  *model.TableInfo `json:"table_info"`
	SnapshotTS uint64           `json:"snapshot_ts"`
	// Dest is the destination URI, including credentials in the query part.
	Dest   string `json:"dest"`
	Format string `json:"format"`
	// FileSize is the target size in bytes to cut a new data file.
	FileSize int64 `json:"file_size"`
	// SubtaskRegions is the number of regions per subtask span, 0 means auto.
	SubtaskRegions int `json:"subtask_regions"`
}

const defaultLanesPerEncoder = 2

// totalLanes is the number of per-writer sub-ranges of one subtask given the
// task concurrency (= encoder count).
func (m *TaskMeta) totalLanes(concurrency int) int {
	return max(concurrency, 1) * defaultLanesPerEncoder
}

// SubtaskMeta is the subtask meta of the Dump step. Each subtask owns a
// contiguous key range of one physical table.
type SubtaskMeta struct {
	PhysicalID int64  `json:"physical_id"`
	Start      []byte `json:"start"`
	End        []byte `json:"end"`
	// WriterSplitKeys are the fixed split points dividing [Start, End) into
	// per-writer sub-ranges, decided at schedule time from region boundaries
	// so a subtask retry rewrites exactly the same files.
	WriterSplitKeys [][]byte `json:"writer_split_keys"`
}
