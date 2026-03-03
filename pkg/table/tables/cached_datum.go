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
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
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

// CachedIndexDatumData holds pre-decoded index entries for a single index.
// TIMESTAMP columns are stored in UTC; readers must convert to session timezone.
type CachedIndexDatumData struct {
	Entries      map[string][]types.Datum // raw KV key → decoded datums (all index cols + handle cols)
	TsColIndices []int                   // indices of TIMESTAMP columns in the datum slice
}

// BuildCachedIndexDatumData iterates all index entries for the given index in membuf
// and decodes them into a map keyed by raw KV key.
//
// TIMESTAMP values are decoded with time.UTC so they remain timezone-neutral in the cache.
// Readers must convert to session timezone on read.
func BuildCachedIndexDatumData(
	membuf kv.MemBuffer,
	tableID int64,
	indexInfo *model.IndexInfo,
	tblInfo *model.TableInfo,
) (*CachedIndexDatumData, error) {
	// Build field types for all decoded columns (index cols + handle cols).
	tps := make([]*types.FieldType, 0, len(indexInfo.Columns)+1)
	cols := tblInfo.Columns
	for _, col := range indexInfo.Columns {
		tps = append(tps, &cols[col.Offset].FieldType)
	}
	switch {
	case tblInfo.PKIsHandle:
		for _, col := range tblInfo.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				tps = append(tps, &(col.FieldType))
				break
			}
		}
	case tblInfo.IsCommonHandle:
		pkIdx := FindPrimaryIndex(tblInfo)
		for _, pkCol := range pkIdx.Columns {
			colInfo := tblInfo.Columns[pkCol.Offset]
			tps = append(tps, &colInfo.FieldType)
		}
	default: // ExtraHandle Column tp.
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}

	colInfos := BuildRowcodecColInfoForIndexColumns(indexInfo, tblInfo)
	colInfos = TryAppendCommonHandleRowcodecColInfos(colInfos, tblInfo)

	// Determine handle status.
	colsLen := len(indexInfo.Columns)
	hdStatus := tablecodec.HandleDefault
	if mysql.HasUnsignedFlag(tps[colsLen].GetFlag()) {
		hdStatus = tablecodec.HandleIsUnsigned
	}

	tsColIndices := findTimestampColumns(tps)
	loc := time.UTC

	prefix := tablecodec.EncodeTableIndexPrefix(tableID, indexInfo.ID)
	it, err := membuf.Iter(prefix, kv.Key(prefix).PrefixNext())
	if err != nil {
		return nil, err
	}
	defer it.Close()

	entries := make(map[string][]types.Datum)
	restoredDec := tablecodec.NewIndexRestoredDecoder(colInfos[:colsLen])

	for it.Valid() {
		key := it.Key()
		value := it.Value()

		if len(value) == 0 {
			if err := it.Next(); err != nil {
				return nil, err
			}
			continue
		}

		decodeBuff := make([][]byte, colsLen, colsLen+len(colInfos))
		var buf [16]byte
		values, err := tablecodec.DecodeIndexKVEx(key, value, colsLen, hdStatus, colInfos, buf[:0], decodeBuff, restoredDec)
		if err != nil {
			return nil, err
		}

		datums := make([]types.Datum, len(values))
		for i, val := range values {
			if err := tablecodec.DecodeColumnValueWithDatum(val, tps[i], loc, &datums[i]); err != nil {
				return nil, err
			}
		}

		entries[string(key)] = datums

		if err := it.Next(); err != nil {
			return nil, err
		}
	}

	return &CachedIndexDatumData{
		Entries:      entries,
		TsColIndices: tsColIndices,
	}, nil
}
