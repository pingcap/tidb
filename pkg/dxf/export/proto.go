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
	DestURI          string `json:"dest_uri"`
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

// dbFirstTableIdxs returns the set of flat table indices (matching
// Chunk.TableIdx) that are the first table of their database, in the same
// (DBs, TableIDs) order used to assign those indices. Exactly one table per
// non-empty database is picked, so whichever schema subtask that table lands
// in is the one that also writes the database's CREATE DATABASE file.
func (m *TaskMeta) dbFirstTableIdxs() map[int]struct{} {
	firstIdxs := make(map[int]struct{}, len(m.DBs))
	idx := 0
	for i := range m.DBs {
		if len(m.DBs[i].TableIDs) > 0 {
			firstIdxs[idx] = struct{}{}
		}
		idx += len(m.DBs[i].TableIDs)
	}
	return firstIdxs
}

// Chunk is a ~chunkSize key range of one physical table. Its table-local Ordinal
// fixes the output file name at split time, so retries produce the same files.
type Chunk struct {
	TableIdx   int    `json:"table_idx"`
	PhysicalID int64  `json:"physical_id"`
	Start      []byte `json:"start"`
	End        []byte `json:"end"`
	// Size is PD's estimated byte size of [Start, End), not an exact count.
	Size    int64 `json:"size"`
	Ordinal int   `json:"ordinal"`
}

// SubtaskMeta is the external representation of a chunk batch. The prepared
// plan uses the same format before its chunks are grouped into subtasks.
type SubtaskMeta struct {
	dxfutil.BaseExternalMeta
	Chunks []Chunk `json:"chunks" external:"true"`
	// ChunkCount and TotalSize summarize Chunks and, unlike Chunks, are not
	// external, so they remain readable after the external file is cleaned up.
	ChunkCount int   `json:"chunk_count"`
	TotalSize  int64 `json:"total_size"`
}

// newSubtaskMeta builds a SubtaskMeta from chunks, filling in the summary fields.
func newSubtaskMeta(chunks []Chunk) *SubtaskMeta {
	var total int64
	for _, c := range chunks {
		total += c.Size
	}
	return &SubtaskMeta{Chunks: chunks, ChunkCount: len(chunks), TotalSize: total}
}

// SchemaSubtaskMeta is the external representation of a schema-file batch:
// the flat table indices (matching Chunk.TableIdx) whose CREATE TABLE text
// this subtask renders. Which of those tables also own their database's
// CREATE DATABASE file is a static property of the index (see
// TaskMeta.dbFirstTableIdxs), not tracked here.
type SchemaSubtaskMeta struct {
	dxfutil.BaseExternalMeta
	TableIdxs  []int `json:"table_idxs" external:"true"`
	TableCount int   `json:"table_count"`
}

// newSchemaSubtaskMeta builds a SchemaSubtaskMeta from table indices, filling
// in the summary field.
func newSchemaSubtaskMeta(tableIdxs []int) *SchemaSubtaskMeta {
	return &SchemaSubtaskMeta{TableIdxs: tableIdxs, TableCount: len(tableIdxs)}
}
