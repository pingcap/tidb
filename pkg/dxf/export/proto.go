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
	"os"
	"strconv"

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
	// Each encoder (one per task-concurrency slot) serves ReadersPerEncoder
	// readers and WritersPerEncoder writers; both 0 mean the default 1. Writers
	// must be a multiple of readers so the readers evenly split the writers'
	// sub-ranges. Total readers = thread×ReadersPerEncoder, total writers =
	// thread×WritersPerEncoder, decoupling cop read concurrency from S3 write
	// fan-out.
	ReadersPerEncoder int `json:"readers_per_encoder"`
	WritersPerEncoder int `json:"writers_per_encoder"`
}

const (
	defaultReadersPerEncoder = 1
	defaultWritersPerEncoder = 1
)

func (m *TaskMeta) effectiveReadersPerEncoder() int {
	if m.ReadersPerEncoder > 0 {
		return m.ReadersPerEncoder
	}
	return defaultReadersPerEncoder
}

// Benchmark env knobs (perf prototype). They let the worker-tidb operator tune
// the export pipeline without a statement-level option, mirroring the existing
// TIDB_EXPORT_NOOP_WRITER switch.
var (
	// exportReaderPool is the size of the decoupled reader pool. When > 0 the
	// executor uses the decoupled pipeline: a shared pool of this many readers
	// round-robins region-sized pages into m ordered per-file buffers (m =
	// file/writer count), so the number of concurrent cop reads is decoupled
	// from the writer/file count. 0 keeps the coupled 1:1 reader/writer path.
	exportReaderPool = envInt("TIDB_EXPORT_READERS", 0)
	// exportEncBufSize is the per-file encoded-buffer channel depth used by the
	// decoupled pipeline.
	exportEncBufSize = envInt("TIDB_EXPORT_ENCBUF", channelBufSize)
)

// envInt reads a positive integer from env var name, falling back to def when
// it is unset, malformed, or non-positive.
func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func (m *TaskMeta) effectiveWritersPerEncoder() int {
	if m.WritersPerEncoder > 0 {
		return m.WritersPerEncoder
	}
	return defaultWritersPerEncoder
}

// SubtaskMeta is the subtask meta of the Dump step. Each subtask owns a
// contiguous key range of one physical table.
type SubtaskMeta struct {
	PhysicalID int64 `json:"physical_id"`
	// WriterBounds are the boundaries of the per-writer sub-ranges: writer i
	// owns [WriterBounds[i], WriterBounds[i+1]). They are fixed at schedule
	// time from region boundaries so a subtask retry rewrites exactly the
	// same files. WriterBounds[0] and WriterBounds[len-1] are the subtask's
	// span.
	WriterBounds [][]byte `json:"writer_bounds"`
}
