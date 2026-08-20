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

// TableSpec identifies one table in the export task.
type TableSpec struct {
	DBName  string `json:"db_name"`
	TableID int64  `json:"table_id"`
}

// TaskMeta is the task meta of an export task.
type TaskMeta struct {
	Tables     []TableSpec `json:"tables"`
	SnapshotTS uint64      `json:"snapshot_ts"`
	// PhysicalSizes maps a physical table (or partition) id to its estimated size.
	PhysicalSizes map[int64]int64 `json:"physical_sizes"`
	Dest          string          `json:"dest"`
	Format        string          `json:"format"`
	// FileSize is the size in bytes at which the executor cuts output files.
	FileSize int64 `json:"file_size"`
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

// SubtaskMeta is a batch of chunks.
type SubtaskMeta struct {
	dxfutil.BaseExternalMeta
	Chunks []Chunk `json:"chunks" external:"true"`
}
