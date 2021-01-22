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
// See the License for the specific language governing permissions and
// limitations under the License.

package decoder

import (
	"context"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
)

// Column contains the info and generated expr of column.
type Column struct {
	Col     *table.Column
	GenExpr expression.Expression
}

// RowDecoder decodes a byte slice into datums and eval the generated column value.
type RowDecoder struct {
	tbl           table.Table
	mutRow        chunk.MutRow
	colMap        map[int64]Column
	colTypes      map[int64]*types.FieldType
	haveGenColumn bool
	defaultVals   []types.Datum
	cols          []*table.Column
	pkCols        []int64
}

// NewRowDecoder returns a new RowDecoder.
func NewRowDecoder(tbl table.Table, cols []*table.Column, decodeColMap map[int64]Column) *RowDecoder {
	tblInfo := tbl.Meta()
	colFieldMap := make(map[int64]*types.FieldType, len(decodeColMap))
	for id, col := range decodeColMap {
		colFieldMap[id] = &col.Col.ColumnInfo.FieldType
	}

	tps := make([]*types.FieldType, len(cols))
	for _, col := range cols {
		if col.ChangeStateInfo == nil {
			tps[col.Offset] = &col.FieldType
		} else {
			// Since changing column in the mutRow will be set with relative column's old value in the process of column-type-change,
			// we should set fieldType as the relative column does. Otherwise it may get a panic, take change json to int as an example,
			// setting json value to a int type column in mutRow will panic because it lacks of offset array.
			tps[col.Offset] = &cols[col.ChangeStateInfo.DependencyColumnOffset].FieldType
		}
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
	return &RowDecoder{
		tbl:         tbl,
		mutRow:      chunk.MutRowFromTypes(tps),
		colMap:      decodeColMap,
		colTypes:    colFieldMap,
		defaultVals: make([]types.Datum, len(cols)),
		cols:        cols,
		pkCols:      pkCols,
	}
}

// DecodeAndEvalRowWithMap decodes a byte slice into datums and evaluates the generated column value.
func (rd *RowDecoder) DecodeAndEvalRowWithMap(ctx sessionctx.Context, handle kv.Handle, b []byte, decodeLoc, sysLoc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	var err error
	if rowcodec.IsNewFormat(b) {
		row, err = tablecodec.DecodeRowWithMapNew(b, rd.colTypes, decodeLoc, row)
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
		if ok || dCol.GenExpr != nil {
			rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
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
		rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
	}
	keys := make([]int, 0)
	ids := make(map[int]int)
	for k, col := range rd.colMap {
		keys = append(keys, col.Col.Offset)
		ids[col.Col.Offset] = int(k)
	}
	sort.Ints(keys)
	for _, id := range keys {
		col := rd.colMap[int64(ids[id])]
		if col.GenExpr == nil {
			continue
		}
		// Eval the column value
		val, err := col.GenExpr.Eval(rd.mutRow.ToRow())
		if err != nil {
			return nil, err
		}
		val, err = table.CastValue(ctx, val, col.Col.ColumnInfo, false, true)
		if err != nil {
			return nil, err
		}

		if val.Kind() == types.KindMysqlTime && sysLoc != time.UTC {
			t := val.GetMysqlTime()
			if t.Type() == mysql.TypeTimestamp {
				err := t.ConvertTimeZone(sysLoc, time.UTC)
				if err != nil {
					return nil, err
				}
				val.SetMysqlTime(t)
			}
		}
		rd.mutRow.SetValue(col.Col.Offset, val.GetValue())

		row[int64(ids[id])] = val
	}
	return row, nil
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

// CurrentRowWithDefaultVal returns current decoding row with default column values set properly.
// Please make sure calling DecodeAndEvalRowWithMap first.
func (rd *RowDecoder) CurrentRowWithDefaultVal() chunk.Row {
	return rd.mutRow.ToRow()
}

// RowWithCols returns the column datums of the row.
func RowWithCols(t table.Table, ctx sessionctx.Context, h kv.Handle, cols []*table.Column) ([]types.Datum, error) {
	// Get raw row data from kv.
	key := tablecodec.EncodeRecordKey(t.RecordPrefix(), h)
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	value, err := txn.Get(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	v, _, err := DecodeRawRowData(ctx, t.Meta(), h, cols, value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// DecodeRawRowData decodes raw row data into a datum slice and a (columnID:columnValue) map.
func DecodeRawRowData(ctx sessionctx.Context, meta *model.TableInfo, h kv.Handle, cols []*table.Column,
	value []byte) ([]types.Datum, map[int64]types.Datum, error) {
	v := make([]types.Datum, len(cols))
	colTps := make(map[int64]*types.FieldType, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) {
			if mysql.HasUnsignedFlag(col.Flag) {
				v[i].SetUint64(uint64(h.IntValue()))
			} else {
				v[i].SetInt64(h.IntValue())
			}
			continue
		}
		if col.IsCommonHandleColumn(meta) && !types.CommonHandleNeedRestoredData(&col.FieldType) {
			pkIdx := tables.FindPrimaryIndex(meta)
			var idxOfIdx int
			for i, idxCol := range pkIdx.Columns {
				if meta.Columns[idxCol.Offset].ID == col.ID {
					idxOfIdx = i
					break
				}
			}
			dtBytes := h.EncodedCol(idxOfIdx)
			_, dt, err := codec.DecodeOne(dtBytes)
			if err != nil {
				return nil, nil, err
			}
			dt, err = tablecodec.Unflatten(dt, &col.FieldType, ctx.GetSessionVars().Location())
			if err != nil {
				return nil, nil, err
			}
			v[i] = dt
			continue
		}
		colTps[col.ID] = &col.FieldType
	}
	rowMap, err := tablecodec.DecodeRowToDatumMap(value, colTps, ctx.GetSessionVars().Location())
	if err != nil {
		return nil, rowMap, err
	}
	defaultVals := make([]types.Datum, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) || (col.IsCommonHandleColumn(meta) && !types.CommonHandleNeedRestoredData(&col.FieldType)) {
			continue
		}
		ri, ok := rowMap[col.ID]
		if ok {
			v[i] = ri
			continue
		}
		if col.IsGenerated() && !col.GeneratedStored {
			continue
		}
		if col.ChangeStateInfo != nil {
			v[i], _, err = getChangingColVal(ctx, cols, col, rowMap, defaultVals)
		} else {
			v[i], err = tables.GetColDefaultValue(ctx, col, defaultVals)
		}
		if err != nil {
			return nil, rowMap, err
		}
	}
	return v, rowMap, nil
}

// getChangingColVal gets the changing column value when executing "modify/change column" statement.
func getChangingColVal(ctx sessionctx.Context, cols []*table.Column, col *table.Column, rowMap map[int64]types.Datum, defaultVals []types.Datum) (_ types.Datum, isDefaultVal bool, err error) {
	relativeCol := cols[col.ChangeStateInfo.DependencyColumnOffset]
	idxColumnVal, ok := rowMap[relativeCol.ID]
	if ok {
		// It needs cast values here when filling back column or index values in "modify/change column" statement.
		if ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue {
			return idxColumnVal, false, nil
		}
		idxColumnVal, err := table.CastValue(ctx, rowMap[relativeCol.ID], col.ColumnInfo, false, false)
		// TODO: Consider sql_mode and the error msg(encounter this error check whether to rollback).
		if err != nil {
			return idxColumnVal, false, errors.Trace(err)
		}
		return idxColumnVal, false, nil
	}

	idxColumnVal, err = tables.GetColDefaultValue(ctx, col, defaultVals)
	if err != nil {
		return idxColumnVal, false, errors.Trace(err)
	}

	return idxColumnVal, true, nil
}
