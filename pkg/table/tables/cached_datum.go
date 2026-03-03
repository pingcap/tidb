// Copyright 2025 PingCAP, Inc.
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

package tables

import (
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

const datumCacheChunkSize = 1024

// CachedDatumData holds pre-decoded cached table data as chunks.
// TIMESTAMP columns are stored in UTC; readers must convert to session timezone.
type CachedDatumData struct {
	Chunks       []*chunk.Chunk
	FieldTypes   []*types.FieldType
	TsColIndices []int // indices of TIMESTAMP columns in FieldTypes; empty means no conversion needed
	TotalRows    int
}

// BuildCachedDatumData iterates all rows for tableID in membuf and decodes them
// into chunks using a ChunkDecoder.
//
// loc is passed as nil so that TIMESTAMP values remain in UTC without timezone conversion.
// This is safe because initCompiledCols sets needTZConvert = tp == mysql.TypeTimestamp && decoder.loc != nil.
func BuildCachedDatumData(
	membuf kv.MemBuffer,
	tableID int64,
	columns []rowcodec.ColInfo,
	handleColIDs []int64,
	defDatum func(i int, chk *chunk.Chunk) error,
	fieldTypes []*types.FieldType,
) (*CachedDatumData, error) {
	cd := rowcodec.NewChunkDecoder(columns, handleColIDs, defDatum, nil)

	tsColIndices := findTimestampColumns(fieldTypes)

	var chunks []*chunk.Chunk
	curChk := chunk.New(fieldTypes, datumCacheChunkSize, datumCacheChunkSize)
	chunks = append(chunks, curChk)
	totalRows := 0

	prefix := tablecodec.GenTablePrefix(tableID)
	it, err := membuf.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		return nil, err
	}
	defer it.Close()

	for it.Valid() {
		key := it.Key()
		value := it.Value()

		if len(value) == 0 {
			if err := it.Next(); err != nil {
				return nil, err
			}
			continue
		}

		handle, err := tablecodec.DecodeRowKey(key)
		if err != nil {
			// Not a record key (e.g. index key), skip.
			if err := it.Next(); err != nil {
				return nil, err
			}
			continue
		}

		if curChk.NumRows() >= datumCacheChunkSize {
			curChk = chunk.New(fieldTypes, datumCacheChunkSize, datumCacheChunkSize)
			chunks = append(chunks, curChk)
		}

		if err := cd.DecodeToChunk(value, handle, curChk); err != nil {
			return nil, err
		}
		totalRows++

		if err := it.Next(); err != nil {
			return nil, err
		}
	}

	return &CachedDatumData{
		Chunks:       chunks,
		FieldTypes:   fieldTypes,
		TsColIndices: tsColIndices,
		TotalRows:    totalRows,
	}, nil
}

func findTimestampColumns(fieldTypes []*types.FieldType) []int {
	var indices []int
	for i, ft := range fieldTypes {
		if ft.GetType() == mysql.TypeTimestamp {
			indices = append(indices, i)
		}
	}
	return indices
}
