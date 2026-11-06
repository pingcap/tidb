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

package importinto

import (
	"strconv"

	"github.com/pingcap/tidb/pkg/dumpformat/parquetfile"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
)

// Chunk records the chunk information.
type Chunk struct {
	Path         string
	FileSize     int64
	Offset       int64
	EndOffset    int64
	PrevRowIDMax int64
	RowIDMax     int64
	Type         mydump.SourceType
	Compression  mydump.Compression
	Timestamp    int64
	ParquetMeta  parquetfile.FileMeta
}

// GetKey returns the chunk key used in logs and encode errors.
func (c *Chunk) GetKey() string {
	return c.Path + ":" + strconv.FormatInt(c.Offset, 10)
}

// GetSize get the original file size of this chunk.
func (c *Chunk) GetSize() int64 {
	if c.Type == mydump.SourceTypeParquet {
		// for parquet, Offset/EndOffset means rows
		return c.FileSize
	}
	return c.EndOffset - c.Offset
}

// ToSourceFileMeta describes the input file for parser initialization.
func (c *Chunk) ToSourceFileMeta() mydump.SourceFileMeta {
	return mydump.SourceFileMeta{
		Path:        c.Path,
		Type:        c.Type,
		Compression: c.Compression,
		FileSize:    c.FileSize,
		ParquetMeta: c.ParquetMeta,
	}
}
