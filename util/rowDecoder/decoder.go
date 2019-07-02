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
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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
	columns       map[int64]Column
	colTypes      map[int64]*types.FieldType
	haveGenColumn bool
	defaultVals   []types.Datum
}

// NewRowDecoder returns a new RowDecoder.
func NewRowDecoder(tbl table.Table, decodeColMap map[int64]Column) *RowDecoder {
	colFieldMap := make(map[int64]*types.FieldType, len(decodeColMap))
	haveGenCol := false
	for id, col := range decodeColMap {
		colFieldMap[id] = &col.Col.ColumnInfo.FieldType
		if col.GenExpr != nil {
			haveGenCol = true
		}
	}
	if !haveGenCol {
		return &RowDecoder{
			colTypes: colFieldMap,
		}
	}

	cols := tbl.Cols()
	tps := make([]*types.FieldType, len(cols))
	for _, col := range cols {
		tps[col.Offset] = &col.FieldType
	}
	return &RowDecoder{
		tbl:           tbl,
		mutRow:        chunk.MutRowFromTypes(tps),
		columns:       decodeColMap,
		colTypes:      colFieldMap,
		haveGenColumn: haveGenCol,
		defaultVals:   make([]types.Datum, len(cols)),
	}
}

// DecodeAndEvalRowWithMap decodes a byte slice into datums and evaluates the generated column value.
func (rd *RowDecoder) DecodeAndEvalRowWithMap(ctx sessionctx.Context, handle int64, b []byte, decodeLoc, sysLoc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	row, err := tablecodec.DecodeRowWithMap(b, rd.colTypes, decodeLoc, row)
	if err != nil {
		return nil, err
	}
	if !rd.haveGenColumn {
		return row, nil
	}

	for _, dCol := range rd.columns {
		colInfo := dCol.Col.ColumnInfo
		val, ok := row[colInfo.ID]
		if ok || dCol.GenExpr != nil {
			rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
			continue
		}

		// Get the default value of the column in the generated column expression.
		if dCol.Col.IsPKHandleColumn(rd.tbl.Meta()) {
			if mysql.HasUnsignedFlag(colInfo.Flag) {
				val.SetUint64(uint64(handle))
			} else {
				val.SetInt64(handle)
			}
		} else {
			val, err = tables.GetColDefaultValue(ctx, dCol.Col, rd.defaultVals)
			if err != nil {
				return nil, err
			}
		}
		rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
	}
	for id, col := range rd.columns {
		if col.GenExpr == nil {
			continue
		}
		// Eval the column value
		val, err := col.GenExpr.Eval(rd.mutRow.ToRow())
		if err != nil {
			return nil, err
		}
		val, err = table.CastValue(ctx, val, col.Col.ColumnInfo)
		if err != nil {
			return nil, err
		}

		if val.Kind() == types.KindMysqlTime && sysLoc != time.UTC {
			t := val.GetMysqlTime()
			if t.Type == mysql.TypeTimestamp {
				err := t.ConvertTimeZone(sysLoc, time.UTC)
				if err != nil {
					return nil, err
				}
				val.SetMysqlTime(t)
			}
		}
		row[id] = val
	}
	return row, nil
}
