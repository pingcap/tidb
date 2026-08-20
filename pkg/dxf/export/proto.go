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

package export

import "github.com/pingcap/tidb/pkg/dxf/framework/dxfutil"

// DBSpec is a database and the ids of its tables to export.
type DBSpec struct {
	DBID     int64   `json:"db_id"`
	DBName   string  `json:"db_name"`
	TableIDs []int64 `json:"table_ids"`
}

// TaskMeta is the task meta of an export task.
type TaskMeta struct {
	DBs        []DBSpec `json:"dbs"`
	SnapshotTS uint64   `json:"snapshot_ts"`
	// PreparedPlanPath is the external-storage path of the chunks built during prepare.
	PreparedPlanPath string `json:"prepared_plan_path"`
	Dest             string `json:"dest"`
	Format           string `json:"format"`
	// FileSize is the size in bytes at which the executor cuts output files.
	FileSize int64 `json:"file_size"`
}

// tableCount returns the total number of tables across all databases.
func (m *TaskMeta) tableCount() int {
	n := 0
	for i := range m.DBs {
		n += len(m.DBs[i].TableIDs)
	}
	return n
}

// Chunk is a ~chunkSize key range of one physical table. Its table-local Ordinal
// fixes the output file name at split time, so retries produce the same files.
type Chunk struct {
	TableIdx   int    `json:"table_idx"`
	PhysicalID int64  `json:"physical_id"`
	Start      []byte `json:"start"`
	End        []byte `json:"end"`
	Size       int64  `json:"size"`
	Ordinal    int    `json:"ordinal"`
}

// SubtaskMeta is the external representation of a chunk batch. The prepared
// plan uses the same format before its chunks are grouped into subtasks.
type SubtaskMeta struct {
	dxfutil.BaseExternalMeta
	Chunks []Chunk `json:"chunks" external:"true"`
}
