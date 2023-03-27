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
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
)

// TaskStep of LoadData.
const (
	Import int64 = 1
)

// TaskMeta is the task of LoadData.
type TaskMeta struct {
	Table     Table
	Format    Format
	Dir       string
	FileInfos []FileInfo
}

// SubtaskMeta is the subtask of LoadData.
// Dispatcher will split the task into subtasks(FileInfos -> Chunks)
type SubtaskMeta struct {
	Table  Table
	Format Format
	Dir    string
	Chunks []Chunk
}

// MinimalTaskMeta is the minimal task of LoadData.
// Scheduler will split the subtask into minimal tasks(Chunks -> Chunk)
type MinimalTaskMeta struct {
	Table  Table
	Format Format
	Dir    string
	Chunk  Chunk
	Writer *backend.LocalEngineWriter
}

// IsMinimalTask implements the MinimalTask interface.
func (MinimalTaskMeta) IsMinimalTask() {}

// Table records the table information.
type Table struct {
	DBName        string
	Info          *model.TableInfo
	TargetColumns []string
	IsRowOrdered  bool
}

// Format records the format information.
type Format struct {
	Type                   string
	Compression            mydump.Compression
	CSV                    CSV
	SQLDump                SQLDump
	Parquet                Parquet
	DataCharacterSet       string
	DataInvalidCharReplace string
}

// CSV records the CSV format information.
type CSV struct {
	Config config.CSVConfig
	Strict bool
}

// SQLDump records the SQL dump format information.
type SQLDump struct {
	SQLMode mysql.SQLMode
}

// Parquet records the Parquet format information.
type Parquet struct{}

// Chunk records the chunk information.
type Chunk struct {
	Path         string
	Offset       int64
	EndOffset    int64
	RealOffset   int64
	PrevRowIDMax int64
	RowIDMax     int64
}

// FileInfo records the file information.
type FileInfo struct {
	Path     string
	Size     int64
	RealSize int64
}
