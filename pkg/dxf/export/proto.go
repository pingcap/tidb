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

// Unit is one export unit within a subtask: a contiguous key range of one
// physical table, stamped with a table-local name ordinal at split time. A big
// table yields one unit per region span; several whole small tables may be
// packed as multiple units into a single subtask. The subtask executor loops
// its units and exports each into that table's own files.
type Unit struct {
	// TableIdx indexes into TaskMeta.Tables; the executor pulls the table name
	// and column types from there.
	TableIdx int `json:"table_idx"`
	// PhysicalID is the partition's physical id, equal to the table id for a
	// non-partitioned table.
	PhysicalID int64 `json:"physical_id"`
	// [Start, End) is the record-key range this unit exports.
	Start []byte `json:"start"`
	End   []byte `json:"end"`
	// NameOrdinal is the table-local span index used in the output file name;
	// it is 0 for a whole (unsplit) small table. It is stamped at split time,
	// not derived from the framework's global subtask ordinal, so file names
	// stay a pure function of the unit and an idempotent retry rewrites the same
	// files.
	NameOrdinal int `json:"name_ordinal"`
}

// SubtaskMeta is the subtask meta of the Dump step: the list of units a single
// subtask exports. One unit for a big-table span; several whole-table units
// when small tables are packed together.
type SubtaskMeta struct {
	Units []Unit `json:"units"`
}
