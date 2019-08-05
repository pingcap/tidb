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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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

// NewRowDecoder returns a new RowDecoder.
func NewRowDecoder(cols []*table.Column, decodeColMap map[int64]Column) *RowDecoder {
	colFieldMap := make(map[int64]*types.FieldType, len(decodeColMap))
	haveGenCol := false
	for id, col := range decodeColMap {
		colFieldMap[id] = &col.Info.FieldType
		if col.GenExpr != nil {
			haveGenCol = true
		}
	}
	if !haveGenCol {
		return &RowDecoder{
			colTypes: colFieldMap,
		}
	}

	tps := make([]*types.FieldType, len(cols))
	for _, col := range cols {
		tps[col.Offset] = &col.FieldType
	}
	return &RowDecoder{
		mutRow:        chunk.MutRowFromTypes(tps),
		columns:       decodeColMap,
		colTypes:      colFieldMap,
		haveGenColumn: haveGenCol,
	}
}

// DecodeAndEvalRowWithMap decodes a byte slice into datums and evaluates the generated column value.
func (rd *RowDecoder) DecodeAndEvalRowWithMap(ctx sessionctx.Context, b []byte, decodeLoc, sysLoc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	row, err := tablecodec.DecodeRowWithMap(b, rd.colTypes, decodeLoc, row)
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
		if col.GenExpr == nil {
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

		if val.Kind() == types.KindMysqlTime && sysLoc != time.UTC {
			t := val.GetMysqlTime()
			if t.Type == mysql.TypeTimestamp {
				err := t.ConvertTimeZone(sysLoc, time.UTC)
				if err != nil {
					return nil, errors.Trace(err)
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
				Info:    col.ColumnInfo,
				GenExpr: e,
			}
		} else {
			decodeColMap[col.ID] = Column{
				Info: col.ColumnInfo,
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
	var orderedCols []Pair
	for colID, col := range decodeColMap {
		orderedCols = append(orderedCols, Pair{colID, col.Info.Offset})
	}
	sort.Slice(orderedCols, func(i, j int) bool { return orderedCols[i].colOffset < orderedCols[j].colOffset })

	// Iterate over decodeColMap, the substitution only happens once for each virtual column because
	// columns with smaller offset can not refer to those with larger ones. https://dev.mysql.com/doc/refman/5.7/en/create-table-generated-columns.html.
	for _, pair := range orderedCols {
		colID := pair.colID
		decCol := decodeColMap[colID]
		if decCol.GenExpr != nil {
			decodeColMap[colID] = Column{
				Info:    decCol.Info,
				GenExpr: substituteGeneratedColumn(decCol.GenExpr, decodeColMap),
			}
		} else {
			decodeColMap[colID] = Column{
				Info: decCol.Info,
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
		col := decCol.Info
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
