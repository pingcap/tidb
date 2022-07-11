// Copyright 2018 PingCAP, Inc.
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

package decoder

import (
	"time"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/rowcodec"
	"golang.org/x/exp/slices"
)

// Column contains the info and generated expr of column.
type Column struct {
	Col     *table.Column
	GenExpr expression.Expression
}

// RowDecoder decodes a byte slice into datums and eval the generated column value.
type RowDecoder struct {
	tbl table.Table
	// mutRow is used to evaluate the virtual generated column.
	mutRow          chunk.MutRow
	needMutRow      bool
	colMap          map[int64]Column
	nonGCColMap     map[int64]Column
	gcColMap        map[int64]Column
	orderedGCOffset []int
	offset2Id       map[int]int
	datumSlice      []*types.Datum
	colTypes        map[int64]*types.FieldType
	defaultVals     []types.Datum
	cols            []*table.Column
	pkCols          []int64
	datumMapDecoder *rowcodec.DatumMapDecoder
}

// NewRowDecoder returns a new RowDecoder.
func NewRowDecoder(tbl table.Table, cols []*table.Column, decodeColMap map[int64]Column, sctx sessionctx.Context) *RowDecoder {
	tblInfo := tbl.Meta()
	colFieldMap := make(map[int64]*types.FieldType, len(decodeColMap))
	for id, col := range decodeColMap {
		colFieldMap[id] = &col.Col.ColumnInfo.FieldType
	}

	tps := make([]*types.FieldType, len(cols))
	for _, col := range cols {
		// Even for changing column in column type change, we target field type uniformly.
		tps[col.Offset] = &col.FieldType
	}
	var pkCols []int64
	switch {
	case tblInfo.IsCommonHandle:
		pkCols = tables.TryGetCommonPkColumnIds(tbl.Meta())
	case tblInfo.PKIsHandle:
		pkCols = []int64{tblInfo.GetPkColInfo().ID}
	default: // support decoding _tidb_rowid.
		pkCols = []int64{model.ExtraHandleID}
	}
	nonGCColMap := make(map[int64]Column)
	gcColMap := make(map[int64]Column)
	for id, col := range decodeColMap {
		if col.GenExpr != nil {
			gcColMap[id] = col
		} else {
			nonGCColMap[id] = col
		}
	}

	orderedGCOffset := make([]int, 0, len(gcColMap))
	offset2Id := make(map[int]int, len(gcColMap))
	for id, col := range gcColMap {
		orderedGCOffset = append(orderedGCOffset, col.Col.Offset)
		offset2Id[col.Col.Offset] = int(id)
	}
	slices.Sort(orderedGCOffset)

	reqCols := make([]rowcodec.ColInfo, len(cols))
	var idx int
	for id, tp := range colFieldMap {
		reqCols[idx] = rowcodec.ColInfo{
			ID: id,
			Ft: tp,
		}
		idx++
	}
	rd := rowcodec.NewDatumMapDecoder(reqCols, sctx.GetSessionVars().StmtCtx.TimeZone)

	return &RowDecoder{
		tbl:             tbl,
		mutRow:          chunk.MutRowFromTypes(tps),
		needMutRow:      len(gcColMap) > 0,
		colMap:          decodeColMap,
		nonGCColMap:     nonGCColMap,
		gcColMap:        gcColMap,
		orderedGCOffset: orderedGCOffset,
		offset2Id:       offset2Id,
		colTypes:        colFieldMap,
		defaultVals:     make([]types.Datum, len(cols)),
		cols:            cols,
		pkCols:          pkCols,
		datumMapDecoder: rd,
	}
}

// DecodeAndEvalRowWithMap decodes a byte slice into datums and evaluates the generated column value.
func (rd *RowDecoder) DecodeAndEvalRowWithMap(ctx sessionctx.Context, handle kv.Handle, b []byte, decodeLoc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	var err error
	if rowcodec.IsNewFormat(b) {
		row, err = tablecodec.DecodeRowWithMapNew(b, rd.colTypes, decodeLoc, row, rd.datumMapDecoder)
	} else {
		row, err = tablecodec.DecodeRowWithMap(b, rd.colTypes, decodeLoc, row)
	}
	if err != nil {
		return nil, err
	}
	row, err = tablecodec.DecodeHandleToDatumMap(handle, rd.pkCols, rd.colTypes, decodeLoc, row)
	if err != nil {
		return nil, err
	}
	for _, dCol := range rd.nonGCColMap {
		colInfo := dCol.Col.ColumnInfo
		val, ok := row[colInfo.ID]
		if ok {
			if rd.needMutRow {
				rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
			}
			continue
		}
		if dCol.Col.ChangeStateInfo != nil {
			val, _, err = tables.GetChangingColVal(ctx, rd.cols, dCol.Col, row, rd.defaultVals)
		} else {
			// Get the default value of the column in the generated column expression.
			val, err = tables.GetColDefaultValue(ctx, dCol.Col, rd.defaultVals)
		}
		if err != nil {
			return nil, err
		}
		row[colInfo.ID] = val
		if rd.needMutRow {
			rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
		}
	}
	return rd.EvalRemainedExprColumnMap(ctx, row)
}

// BuildFullDecodeColMap builds a map that contains [columnID -> struct{*table.Column, expression.Expression}] from all columns.
func BuildFullDecodeColMap(cols []*table.Column, schema *expression.Schema) map[int64]Column {
	decodeColMap := make(map[int64]Column, len(cols))
	for _, col := range cols {
		decodeColMap[col.ID] = Column{
			Col:     col,
			GenExpr: schema.Columns[col.Offset].VirtualExpr,
		}
	}
	return decodeColMap
}

// DecodeTheExistedColumnMap is used by ddl column-type-change first column reorg stage.
// In the function, we only decode the existed column in the row and fill the default value.
// For changing column, we shouldn't cast it here, because we will do a unified cast operation latter.
// For generated column, we didn't cast it here too, because the eval process will depend on the changing column.
func (rd *RowDecoder) DecodeTheExistedColumnMap(ctx sessionctx.Context, handle kv.Handle, b []byte, decodeLoc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	var err error
	if rowcodec.IsNewFormat(b) {
		row, err = tablecodec.DecodeRowWithMapNew(b, rd.colTypes, decodeLoc, row, nil)
	} else {
		row, err = tablecodec.DecodeRowWithMap(b, rd.colTypes, decodeLoc, row)
	}
	if err != nil {
		return nil, err
	}
	row, err = tablecodec.DecodeHandleToDatumMap(handle, rd.pkCols, rd.colTypes, decodeLoc, row)
	if err != nil {
		return nil, err
	}
	for _, dCol := range rd.colMap {
		colInfo := dCol.Col.ColumnInfo
		val, ok := row[colInfo.ID]
		if ok || dCol.GenExpr != nil || dCol.Col.ChangeStateInfo != nil {
			if rd.needMutRow {
				rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
			}
			continue
		}
		// Get the default value of the column in the generated column expression.
		val, err = tables.GetColDefaultValue(ctx, dCol.Col, rd.defaultVals)
		if err != nil {
			return nil, err
		}
		// Fill the default value into map.
		row[colInfo.ID] = val
		if rd.needMutRow {
			rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
		}
	}
	// return the existed column map here.
	return row, nil
}

// EvalRemainedExprColumnMap is used by ddl column-type-change first column reorg stage.
// It is always called after DecodeTheExistedColumnMap to finish the generated column evaluation.
func (rd *RowDecoder) EvalRemainedExprColumnMap(ctx sessionctx.Context, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	for _, offset := range rd.orderedGCOffset {
		id := int64(rd.offset2Id[offset])
		col := rd.colMap[id]
		// Eval the column value
		val, err := col.GenExpr.Eval(rd.mutRow.ToRow())
		if err != nil {
			return nil, err
		}
		val, err = table.CastValue(ctx, *val.Clone(), col.Col.ColumnInfo, false, true)
		if err != nil {
			return nil, err
		}

		rd.mutRow.SetValue(col.Col.Offset, val.GetValue())
		row[id] = val
	}
	// return the existed and evaluated column map here.
	return row, nil
}
