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
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/sessionctx"
	"github.com/pingcap/tidb/v4/table"
	"github.com/pingcap/tidb/v4/table/tables"
	"github.com/pingcap/tidb/v4/tablecodec"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/rowcodec"
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
	var err error
	if rowcodec.IsNewFormat(b) {
		row, err = tablecodec.DecodeRowWithMapNew(b, rd.colTypes, decodeLoc, row)
	} else {
		row, err = tablecodec.DecodeRowWithMap(b, rd.colTypes, decodeLoc, row)
	}
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
			if t.Type() == mysql.TypeTimestamp {
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

// BuildFullDecodeColMap build a map that contains [columnID -> struct{*table.Column, expression.Expression}] from
// indexed columns and all of its depending columns. `genExprProducer` is used to produce a generated expression based on a table.Column.
func BuildFullDecodeColMap(indexedCols []*table.Column, t table.Table, genExprProducer func(*table.Column) (expression.Expression, error)) (map[int64]Column, error) {
	pendingCols := make([]*table.Column, len(indexedCols))
	copy(pendingCols, indexedCols)
	decodeColMap := make(map[int64]Column, len(pendingCols))

	for i := 0; i < len(pendingCols); i++ {
		col := pendingCols[i]
		if _, ok := decodeColMap[col.ID]; ok {
			continue // already discovered
		}

		if col.IsGenerated() && !col.GeneratedStored {
			// Find depended columns and put them into pendingCols. For example, idx(c) with column definition `c int as (a + b)`,
			// depended columns of `c` is `a` and `b`, and both of them will be put into the pendingCols, waiting for next traversal.
			for _, c := range t.Cols() {
				if _, ok := col.Dependences[c.Name.L]; ok {
					pendingCols = append(pendingCols, c)
				}
			}

			e, err := genExprProducer(col)
			if err != nil {
				return nil, errors.Trace(err)
			}
			decodeColMap[col.ID] = Column{
				Col:     col,
				GenExpr: e,
			}
		} else {
			decodeColMap[col.ID] = Column{
				Col: col,
			}
		}
	}
	return decodeColMap, nil
}

// SubstituteGenColsInDecodeColMap substitutes generated columns in every expression
// with non-generated one by looking up decodeColMap.
func SubstituteGenColsInDecodeColMap(decodeColMap map[int64]Column) {
	// Sort columns by table.Column.Offset in ascending order.
	type Pair struct {
		colID     int64
		colOffset int
	}
	orderedCols := make([]Pair, 0, len(decodeColMap))
	for colID, col := range decodeColMap {
		orderedCols = append(orderedCols, Pair{colID, col.Col.Offset})
	}
	sort.Slice(orderedCols, func(i, j int) bool { return orderedCols[i].colOffset < orderedCols[j].colOffset })

	// Iterate over decodeColMap, the substitution only happens once for each virtual column because
	// columns with smaller offset can not refer to those with larger ones. https://dev.mysql.com/doc/refman/5.7/en/create-table-generated-columns.html.
	for _, pair := range orderedCols {
		colID := pair.colID
		decCol := decodeColMap[colID]
		if decCol.GenExpr != nil {
			decodeColMap[colID] = Column{
				Col:     decCol.Col,
				GenExpr: substituteGeneratedColumn(decCol.GenExpr, decodeColMap),
			}
		} else {
			decodeColMap[colID] = Column{
				Col: decCol.Col,
			}
		}
	}
}

// substituteGeneratedColumn substitutes generated columns in an expression with non-generated one by looking up decodeColMap.
func substituteGeneratedColumn(expr expression.Expression, decodeColMap map[int64]Column) expression.Expression {
	switch v := expr.(type) {
	case *expression.Column:
		if c, ok := decodeColMap[v.ID]; c.GenExpr != nil && ok {
			return c.GenExpr
		}
		return v
	case *expression.ScalarFunction:
		newArgs := make([]expression.Expression, 0, len(v.GetArgs()))
		for _, arg := range v.GetArgs() {
			newArgs = append(newArgs, substituteGeneratedColumn(arg, decodeColMap))
		}
		return expression.NewFunctionInternal(v.GetCtx(), v.FuncName.L, v.RetType, newArgs...)
	}
	return expr
}

// RemoveUnusedVirtualCols removes all virtual columns in decodeColMap that cannot found in indexedCols.
func RemoveUnusedVirtualCols(decodeColMap map[int64]Column, indexedCols []*table.Column) {
	for colID, decCol := range decodeColMap {
		col := decCol.Col
		if !col.IsGenerated() || col.GeneratedStored {
			continue
		}

		found := false
		for _, v := range indexedCols {
			if v.Offset == col.Offset {
				found = true
				break
			}
		}

		if !found {
			delete(decodeColMap, colID)
		}
	}
}
