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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pkg/errors"
)

// Column contains the info and generated expr of column.
type Column struct {
	Info    *model.ColumnInfo
	GenExpr expression.Expression
}

// RowDecoder decodes a byte slice into datums and eval the generated column value.
type RowDecoder struct {
	mutRow        chunk.MutRow
	columns       map[int64]Column
	colTypes      map[int64]*types.FieldType
	haveGenColumn bool
}

// NewRowDecoder return a new RowDecoder.
func NewRowDecoder(cols []*table.Column, decodeColMap map[int64]Column) RowDecoder {
	colFieldMap := make(map[int64]*types.FieldType, len(decodeColMap))
	haveGenCol := false
	for id, col := range decodeColMap {
		colFieldMap[id] = &col.Info.FieldType
		if col.GenExpr != nil {
			haveGenCol = true
		}
	}
	if !haveGenCol {
		return RowDecoder{
			colTypes: colFieldMap,
		}
	}

	tps := make([]*types.FieldType, len(cols))
	for _, col := range cols {
		tps[col.Offset] = &col.FieldType
	}
	return RowDecoder{
		mutRow:        chunk.MutRowFromTypes(tps),
		columns:       decodeColMap,
		colTypes:      colFieldMap,
		haveGenColumn: haveGenCol,
	}
}

// DecodeAndEvalRowWithMap decodes a byte slice into datums and eval the generated column value.
func (rd RowDecoder) DecodeAndEvalRowWithMap(ctx sessionctx.Context, b []byte, loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	_, err := tablecodec.DecodeRowWithMap(b, rd.colTypes, loc, row)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !rd.haveGenColumn {
		return row, nil
	}

	for id, v := range row {
		rd.mutRow.SetValue(rd.columns[id].Info.Offset, v.GetValue())
	}
	for id, col := range rd.columns {
		if _, ok := row[id]; ok || col.GenExpr == nil {
			continue
		}
		// Eval the column value
		val, err := col.GenExpr.Eval(rd.mutRow.ToRow())
		if err != nil {
			return nil, errors.Trace(err)
		}
		val, err = table.CastValue(ctx, val, col.Info)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row[id] = val
	}
	return row, nil
}
