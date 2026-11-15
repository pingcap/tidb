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

package ddl

import (
	"time"

	"github.com/pingcap/errors"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

// spfreshBulkStoredEncoder reuses row scratch while returning an owned sidecar
// that follows its handle through sampling and shuffle.
type spfreshBulkStoredEncoder struct {
	dimension    uint32
	columns      []*model.ColumnInfo
	ids          []int64
	values       []types.Datum
	scratch      []types.Datum
	decoder      *rowcodec.DatumMapDecoder
	rowMap       map[int64]types.Datum
	defaultVals  []types.Datum
	tableColumns map[int64]*table.Column
	meta         *model.TableInfo
}

func newSPFreshBulkStoredEncoder(tbl table.Table, index *model.IndexInfo, vectorColumn *table.Column, loc *time.Location) (*spfreshBulkStoredEncoder, error) {
	meta := tbl.Meta()
	columns, err := model.ResolveSPFreshStoredColumns(meta, index)
	if err != nil {
		return nil, err
	}
	allColumns := tbl.Cols()
	tableColumns := make(map[int64]*table.Column, len(allColumns))
	for _, column := range allColumns {
		if column != nil {
			tableColumns[column.ID] = column
		}
	}
	encoder := &spfreshBulkStoredEncoder{
		dimension:    uint32(vectorColumn.GetFlen()),
		columns:      columns,
		ids:          make([]int64, len(columns)),
		values:       make([]types.Datum, len(columns)),
		scratch:      make([]types.Datum, 2*len(columns)),
		rowMap:       make(map[int64]types.Datum, len(columns)+1),
		defaultVals:  make([]types.Datum, len(allColumns)),
		tableColumns: tableColumns,
		meta:         meta,
	}
	decoderColumns := make([]rowcodec.ColInfo, 0, len(columns)+1)
	decoderColumns = append(decoderColumns, rowcodec.ColInfo{ID: vectorColumn.ID, Ft: &vectorColumn.FieldType})
	for i, column := range columns {
		encoder.ids[i] = column.ID
		decoderColumns = append(decoderColumns, rowcodec.ColInfo{ID: column.ID, Ft: &column.FieldType})
	}
	encoder.decoder = rowcodec.NewDatumMapDecoder(decoderColumns, loc)
	return encoder, nil
}

func (e *spfreshBulkStoredEncoder) encode(row []types.Datum, loc *time.Location) ([]byte, error) {
	payload, _, err := e.encodeInto(nil, row, loc)
	return payload, err
}

// decodeAndEncode reuses the row map and default scratch. It decodes only the
// vector plus selected stored columns instead of materializing the full table row.
func (e *spfreshBulkStoredEncoder) decodeAndEncode(
	buildCtx expression.BuildContext,
	handle kv.Handle,
	rawRecord []byte,
	vectorColumnID int64,
	arena []byte,
) ([]float32, []byte, []byte, error) {
	for id := range e.rowMap {
		delete(e.rowMap, id)
	}
	rowMap, err := e.decoder.DecodeToDatumMap(rawRecord, e.rowMap)
	if err != nil {
		return nil, nil, arena, err
	}
	vectorDatum, ok := rowMap[vectorColumnID]
	if !ok {
		return nil, nil, arena, errors.New("SPFresh MVP source row is missing the vector column")
	}
	vector, err := extractSPFreshBulkVector(vectorDatum, e.dimension)
	if err != nil {
		return nil, nil, arena, err
	}
	for i, column := range e.columns {
		value, ok := rowMap[column.ID]
		if !ok {
			tableColumn := e.tableColumns[column.ID]
			if tableColumn == nil {
				return nil, nil, arena, errors.Errorf("SPFresh stored column %d is missing", column.ID)
			}
			if tableColumn.IsPKHandleColumn(e.meta) {
				if handle == nil || !handle.IsInt() {
					return nil, nil, arena, errors.New("SPFresh stored handle must be an integer")
				}
				if mysql.HasUnsignedFlag(tableColumn.GetFlag()) {
					value.SetUint64(uint64(handle.IntValue()))
				} else {
					value.SetInt64(handle.IntValue())
				}
			} else {
				value, err = tables.GetColDefaultValue(buildCtx, tableColumn, e.defaultVals)
				if err != nil {
					return nil, nil, arena, err
				}
			}
		}
		e.values[i] = value
	}
	storedPayload, arena, err := e.encodeValuesInto(arena, buildCtx.GetEvalCtx().Location())
	return vector, storedPayload, arena, err
}

// encodeInto appends one sidecar to arena. The returned slice aliases arena;
// callers keep the arena alive until the fragment has been uploaded.
func (e *spfreshBulkStoredEncoder) encodeInto(arena []byte, row []types.Datum, loc *time.Location) ([]byte, []byte, error) {
	if len(e.columns) == 0 {
		return nil, arena, nil
	}
	for i, column := range e.columns {
		if column.Offset < 0 || column.Offset >= len(row) {
			return nil, arena, errors.New("SPFresh bulk stored column offset is invalid")
		}
		e.values[i] = row[column.Offset]
	}
	return e.encodeValuesInto(arena, loc)
}

func (e *spfreshBulkStoredEncoder) encodeValuesInto(arena []byte, loc *time.Location) ([]byte, []byte, error) {
	if len(e.columns) == 0 {
		return nil, arena, nil
	}
	start := len(arena)
	payload, err := tablecodec.EncodeOldRow(loc, e.values, e.ids, arena[start:], e.scratch)
	if err != nil {
		return nil, arena, err
	}
	if len(payload) > spfreshMaxStoredPayloadBytes {
		return nil, arena, errors.New("SPFresh bulk stored payload exceeds limit")
	}
	if cap(arena) > start && &payload[0] == &arena[:cap(arena)][start] {
		// The encoder reused the arena tail. Its size estimate may reallocate
		// even when the final payload fits, so check ownership, not length.
		arena = arena[:start+len(payload)]
	} else {
		// EncodeOldRow grew its tail slice. Keep the existing rows and append
		// the returned bytes instead of extending the obsolete backing array.
		arena = append(arena, payload...)
	}
	return arena[start:], arena, nil
}

// A single bounded row may exceed the target; otherwise fragments stop before
// the next record crosses the byte budget.
func spfreshBulkFragmentEnd(records []pkdbspfreshbulk.BaseFragmentRecord, start, targetBytes int, dimension uint32) int {
	end, size := start, 0
	for end < len(records) {
		next := records[end].EncodedBytes(dimension)
		if end > start && size+next > targetBytes {
			break
		}
		size += next
		end++
	}
	return end
}
