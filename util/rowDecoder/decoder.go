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
	"sort"
	"time"

	"github.com/pingcap/errors"
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
	columns       map[int64]Column
	colTypes      map[int64]*types.FieldType
	haveGenColumn bool
	defaultVals   []types.Datum
	pkCols        []int64
}

// NewRowDecoder returns a new RowDecoder.
func NewRowDecoder(tbl table.Table, decodeColMap map[int64]Column) *RowDecoder {
	colFieldMap := make(map[int64]*types.FieldType, len(decodeColMap))
	for id, col := range decodeColMap {
		colFieldMap[id] = &col.Col.ColumnInfo.FieldType
	}

	cols := tbl.Cols()
	tps := make([]*types.FieldType, len(cols))
	for _, col := range cols {
		tps[col.Offset] = &col.FieldType
	}
	return &RowDecoder{
		tbl:         tbl,
		mutRow:      chunk.MutRowFromTypes(tps),
		columns:     decodeColMap,
		colTypes:    colFieldMap,
		defaultVals: make([]types.Datum, len(cols)),
		pkCols:      tables.TryGetCommonPkColumnIds(tbl.Meta()),
	}
}

func (rd *RowDecoder) tryDecodeFromHandleAndSetRow(dCol Column, handle kv.Handle, row map[int64]types.Datum, decodeLoc *time.Location) (bool, error) {
	if handle == nil {
		return false, nil
	}
	colInfo := dCol.Col.ColumnInfo
	if dCol.Col.IsPKHandleColumn(rd.tbl.Meta()) {
		if mysql.HasUnsignedFlag(colInfo.Flag) {
			row[colInfo.ID] = types.NewUintDatum(uint64(handle.IntValue()))
			rd.mutRow.SetValue(colInfo.Offset, uint64(handle.IntValue()))
		} else {
			row[colInfo.ID] = types.NewIntDatum(handle.IntValue())
			rd.mutRow.SetValue(colInfo.Offset, handle.IntValue())
		}
		return true, nil
	}
	// Try to decode common handle.
	if mysql.HasPriKeyFlag(dCol.Col.Flag) {
		for i, hid := range rd.pkCols {
			if dCol.Col.ID == hid {
				_, d, err := codec.DecodeOne(handle.EncodedCol(i))
				if err != nil {
					return false, errors.Trace(err)
				}
				if d, err = tablecodec.Unflatten(d, &dCol.Col.FieldType, decodeLoc); err != nil {
					return false, err
				}
				row[colInfo.ID] = d
				rd.mutRow.SetValue(colInfo.Offset, d)
				return true, nil
			}
		}
	}
	return false, nil
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
	for _, dCol := range rd.columns {
		colInfo := dCol.Col.ColumnInfo
		val, ok := row[colInfo.ID]
		if ok || dCol.GenExpr != nil {
			rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
			continue
		}
		ok, err := rd.tryDecodeFromHandleAndSetRow(dCol, handle, row, decodeLoc)
		if err != nil {
			return nil, err
		}
		if ok {
			continue
		}
		// Get the default value of the column in the generated column expression.
		val, err = tables.GetColDefaultValue(ctx, dCol.Col, rd.defaultVals)
		if err != nil {
			return nil, err
		}
		rd.mutRow.SetValue(colInfo.Offset, val.GetValue())
	}
	keys := make([]int, 0)
	ids := make(map[int]int)
	for k, col := range rd.columns {
		keys = append(keys, col.Col.Offset)
		ids[col.Col.Offset] = int(k)
	}
	sort.Ints(keys)
	for _, id := range keys {
		col := rd.columns[int64(ids[id])]
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
func BuildFullDecodeColMap(t table.Table, schema *expression.Schema) map[int64]Column {
	decodeColMap := make(map[int64]Column, len(t.Cols()))
	for _, col := range t.Cols() {
		decodeColMap[col.ID] = Column{
			Col:     col,
			GenExpr: schema.Columns[col.Offset].VirtualExpr,
		}
	}
	return decodeColMap
}
