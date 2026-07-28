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

import (
	"github.com/pingcap/tidb/pkg/meta/model"
)

// TableSpec identifies one table in the export set.
type TableSpec struct {
	DBName    string           `json:"db_name"`
	TableInfo *model.TableInfo `json:"table_info"`
}

// TaskMeta is the task meta of an export task. It is table-set-native: Tables
// holds one element for EXPORT TABLE and all base tables for EXPORT SCHEMA;
// everything downstream indexes into Tables and never special-cases one versus
// many.
type TaskMeta struct {
	Tables     []TableSpec `json:"tables"`
	SnapshotTS uint64      `json:"snapshot_ts"`
	// Dest is the destination URI, including credentials in the query part.
	Dest   string `json:"dest"`
	Format string `json:"format"`
	// FileSize is the target size in bytes at which the subtask executor cuts a
	// new data file.
	FileSize int64 `json:"file_size"`
	// SubtaskRegions is the number of regions per subtask span; 0 means auto.
	SubtaskRegions int `json:"subtask_regions"`
}

// Chunk is the atomic unit of export work: a contiguous key range of one
// physical table, sized to roughly one output file (~FileSize), that a worker
// reads, encodes and uploads. A chunk emits a group of files that share its
// name prefix — the encoder cuts a new file every FileSize, so an
// underestimated chunk simply rotates the file index rather than overflowing.
// The chunk's identity — the table-local Ordinal that fixes that name prefix —
// is stamped at split time, independent of which worker (or how many) run it,
// so any number of workers and any retry produce exactly the same files.
type Chunk struct {
	// TableIdx indexes into TaskMeta.Tables; the worker pulls the table name and
	// column types from there.
	TableIdx int `json:"table_idx"`
	// PhysicalID is the partition's physical id, equal to the table id for a
	// non-partitioned table.
	PhysicalID int64 `json:"physical_id"`
	// [Start, End) is the record-key range this chunk exports.
	Start []byte `json:"start"`
	End   []byte `json:"end"`
	// Size is the estimated byte size of the chunk's range, used at split time to
	// balance chunks across subtasks.
	Size int64 `json:"size"`
	// Ordinal is the table-local chunk index that fixes the name prefix of the
	// chunk's output files (see the file-name scheme) — the "how to name this
	// range" write meta. It is stamped at split time, not derived from the worker
	// or subtask, so the names are a pure function of the chunk.
	Ordinal int `json:"ordinal"`
}

// SubtaskMeta is the subtask meta of the Dump step: a batch of chunks. A
// subtask is the dispatch unit assigned to a node; its worker pool pulls these
// chunks from a queue and exports them concurrently. Big tables contribute many
// chunks; small tables contribute one chunk each and pack together.
type SubtaskMeta struct {
	Chunks []Chunk `json:"chunks"`
}
