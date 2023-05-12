// Copyright 2023 PingCAP, Inc.
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

package loaddata

import (
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
)

// TaskStep of LoadData.
const (
	Import int64 = 1
)

// TaskMeta is the task of LoadData.
// All the field should be serializable.
type TaskMeta struct {
	Plan  importer.Plan
	JobID int64
	Stmt  string

	// eligible instances to run this task, we run on all instances if it's empty.
	// we only need this when run LOAD DATA without distributed option now, i.e.
	// running on the instance that initiate the LOAD DATA.
	EligibleInstances []*infosync.ServerInfo
}

// SubtaskMeta is the subtask of LoadData.
// Dispatcher will split the task into subtasks(FileInfos -> Chunks)
// All the field should be serializable.
type SubtaskMeta struct {
	Plan   importer.Plan
	ID     int32
	Chunks []Chunk
}

// MinimalTaskMeta is the minimal task of LoadData.
// Scheduler will split the subtask into minimal tasks(Chunks -> Chunk)
type MinimalTaskMeta struct {
	Plan          importer.Plan
	Chunk         Chunk
	TableImporter *importer.TableImporter
	DataEngine    *backend.OpenedEngine
	IndexEngine   *backend.OpenedEngine
}

// IsMinimalTask implements the MinimalTask interface.
func (MinimalTaskMeta) IsMinimalTask() {}

// Chunk records the chunk information.
type Chunk struct {
	Path         string
	Offset       int64
	EndOffset    int64
	PrevRowIDMax int64
	RowIDMax     int64
	Type         mydump.SourceType
	Compression  mydump.Compression
	Timestamp    int64
}
