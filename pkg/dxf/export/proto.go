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

// Package export implements the DXF task type for distributed table-set export.
package export

import "github.com/pingcap/tidb/pkg/meta/model"

// TableSpec identifies one table in the export set.
type TableSpec struct {
	DBName    string           `json:"db_name"`
	TableInfo *model.TableInfo `json:"table_info"`
}

// TaskMeta is the task meta of an export task, table-set-native: Tables holds
// the tables to export.
type TaskMeta struct {
	Tables     []TableSpec `json:"tables"`
	SnapshotTS uint64      `json:"snapshot_ts"`
	// PhysicalSizes maps a physical table (or partition) id to its estimated byte
	// size, computed in the prepare step (where it also feeds resource scaling).
	// The split prefers a fresh estimate and falls back to this only when PD is
	// unavailable.
	PhysicalSizes map[int64]int64 `json:"physical_sizes"`
	// Dest is the destination URI, with credentials in the query part.
	Dest string `json:"dest"`
	// Format is the output file format, e.g. "csv".
	Format string `json:"format"`
	// FileSize is the size in bytes at which the executor cuts output files.
	FileSize int64 `json:"file_size"`
}

// Chunk is the atomic unit of export work: a key range of one physical table
// sized to ~chunkSize. Its table-local Ordinal fixes the output file-name prefix
// at split time, independent of the worker count, so any concurrency or retry
// produces the same files.
type Chunk struct {
	TableIdx   int    `json:"table_idx"`
	PhysicalID int64  `json:"physical_id"`
	Start      []byte `json:"start"`
	End        []byte `json:"end"`
	Size       int64  `json:"size"`
	Ordinal    int    `json:"ordinal"`
}

// SubtaskMeta is the Dump-step subtask meta: a batch of chunks.
type SubtaskMeta struct {
	Chunks []Chunk `json:"chunks"`
}
