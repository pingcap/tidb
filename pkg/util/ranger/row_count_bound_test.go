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

package ranger_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

const (
	colA      = 1 // a int
	colHandle = 2 // id int primary key, the int handle
	colName   = 3 // name varchar(32)
)

func newBoundTestTable(indices ...*model.IndexInfo) *model.TableInfo {
	newCol := func(id int64, offset int, name string, tp byte) *model.ColumnInfo {
		col := &model.ColumnInfo{ID: id, Offset: offset, Name: ast.NewCIStr(name), State: model.StatePublic}
		col.FieldType = *types.NewFieldType(tp)
		return col
	}
	pk := newCol(colHandle, 1, "id", mysql.TypeLonglong)
	pk.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	return &model.TableInfo{
		Columns: []*model.ColumnInfo{
			newCol(colA, 0, "a", mysql.TypeLonglong),
			pk,
			newCol(colName, 2, "name", mysql.TypeVarchar),
		},
		PKIsHandle: true,
		Indices:    indices,
	}
}

type idxColDef struct {
	offset int
	length int
}

func newBoundTestIndex(unique bool, cols ...idxColDef) *model.IndexInfo {
	idx := &model.IndexInfo{Unique: unique, State: model.StatePublic}
	for _, c := range cols {
		idx.Columns = append(idx.Columns, &model.IndexColumn{Offset: c.offset, Length: c.length})
	}
	return idx
}

func pointRange(vals ...types.Datum) *ranger.Range {
	return &ranger.Range{
		LowVal:    vals,
		HighVal:   vals,
		Collators: collate.GetBinaryCollatorSlice(len(vals)),
	}
}

func intDatums(vals ...int64) []types.Datum {
	ds := make([]types.Datum, 0, len(vals))
	for _, v := range vals {
		ds = append(ds, types.NewIntDatum(v))
	}
	return ds
}

func TestRowCountUpperBoundByUniqueKey(t *testing.T) {
	full := types.UnspecifiedLength
	tc := types.DefaultStmtNoWarningContext
	colsOf := func(ids ...int64) []*expression.Column {
		cols := make([]*expression.Column, 0, len(ids))
		for _, id := range ids {
			cols = append(cols, &expression.Column{ID: id})
		}
		return cols
	}
	uniqA := newBoundTestIndex(true, idxColDef{0, full})
	// The scanned index is a non-unique index on (a, id); its ranges cover a and the handle.
	idxACols, idxALens := colsOf(colA, colHandle), []int{full, full}

	type testCase struct {
		name     string
		tbl      *model.TableInfo
		cols     []*expression.Column
		lens     []int
		ranges   []*ranger.Range
		expected uint64
		ok       bool
	}
	nonPublic := newBoundTestIndex(true, idxColDef{0, full})
	nonPublic.State = model.StateWriteReorganization
	partial := newBoundTestIndex(true, idxColDef{2, full})
	partial.ConditionExprString = "a > 0"
	mv := newBoundTestIndex(true, idxColDef{2, full})
	mv.MVIndex = true
	noHandleTbl := newBoundTestTable(newBoundTestIndex(false, idxColDef{0, full}))
	noHandleTbl.PKIsHandle = false
	exclusive := pointRange(types.NewIntDatum(1))
	exclusive.LowExclude = true

	for _, tt := range []testCase{
		{
			name: "points on a unique prefix", tbl: newBoundTestTable(uniqA), cols: idxACols, lens: idxALens,
			ranges:   []*ranger.Range{pointRange(intDatums(1)...), pointRange(intDatums(2)...), pointRange(intDatums(3)...)},
			expected: 3, ok: true,
		},
		{
			name: "unique prefix fixed, later column is a range", tbl: newBoundTestTable(uniqA), cols: idxACols, lens: idxALens,
			ranges: []*ranger.Range{{
				LowVal:     intDatums(1, 5),
				HighVal:    []types.Datum{types.NewIntDatum(1), types.MaxValueDatum()},
				LowExclude: true,
				Collators:  collate.GetBinaryCollatorSlice(2),
			}},
			expected: 1, ok: true,
		},
		{
			name: "points on all columns use the int handle", tbl: newBoundTestTable(), cols: idxACols, lens: idxALens,
			ranges:   []*ranger.Range{pointRange(intDatums(1, 10)...), pointRange(intDatums(1, 11)...)},
			expected: 2, ok: true,
		},
		{
			name: "NULL point", tbl: newBoundTestTable(uniqA), cols: idxACols, lens: idxALens,
			ranges: []*ranger.Range{pointRange(intDatums(1)...), pointRange(types.NewDatum(nil))},
		},
		{
			name: "range on the unique column", tbl: newBoundTestTable(uniqA), cols: idxACols, lens: idxALens,
			ranges: []*ranger.Range{{
				LowVal:     intDatums(5),
				HighVal:    []types.Datum{types.MaxValueDatum()},
				LowExclude: true,
				Collators:  collate.GetBinaryCollatorSlice(1),
			}},
		},
		{
			name: "exclusive point", tbl: newBoundTestTable(uniqA), cols: idxACols, lens: idxALens,
			ranges: []*ranger.Range{exclusive},
		},
		{
			name: "only the handle is fixed but a is not", tbl: newBoundTestTable(uniqA), cols: idxACols, lens: idxALens,
			ranges: []*ranger.Range{{
				LowVal:    []types.Datum{types.MinNotNullDatum(), types.NewIntDatum(10)},
				HighVal:   []types.Datum{types.MaxValueDatum(), types.NewIntDatum(10)},
				Collators: collate.GetBinaryCollatorSlice(2),
			}},
		},
		{
			name: "non-unique index without any unique key", tbl: noHandleTbl,
			cols: colsOf(colA), lens: []int{full}, ranges: []*ranger.Range{pointRange(intDatums(1)...)},
		},
		{
			name: "unique index is not public", tbl: newBoundTestTable(nonPublic),
			cols: colsOf(colA), lens: []int{full}, ranges: []*ranger.Range{pointRange(intDatums(1)...)},
		},
		{
			name: "point on a prefix column", tbl: newBoundTestTable(newBoundTestIndex(true, idxColDef{2, full})),
			cols: colsOf(colName), lens: []int{4}, ranges: []*ranger.Range{pointRange(types.NewStringDatum("abcd"))},
		},
		{
			name: "prefix unique index", tbl: newBoundTestTable(newBoundTestIndex(true, idxColDef{2, 4})),
			cols: colsOf(colName), lens: []int{full}, ranges: []*ranger.Range{pointRange(types.NewStringDatum("abcd"))},
		},
		{
			name: "partial unique index", tbl: newBoundTestTable(partial),
			cols: colsOf(colName), lens: []int{full}, ranges: []*ranger.Range{pointRange(types.NewStringDatum("abcd"))},
		},
		{
			name: "multi-valued unique index", tbl: newBoundTestTable(mv),
			cols: colsOf(colName), lens: []int{full}, ranges: []*ranger.Range{pointRange(types.NewStringDatum("abcd"))},
		},
		{
			name: "composite unique key partially fixed",
			tbl:  newBoundTestTable(newBoundTestIndex(true, idxColDef{0, full}, idxColDef{2, full})),
			cols: colsOf(colA, colName), lens: []int{full, full},
			ranges: []*ranger.Range{pointRange(intDatums(1)...)},
		},
		{
			name: "composite unique key fully fixed",
			tbl:  newBoundTestTable(newBoundTestIndex(true, idxColDef{0, full}, idxColDef{2, full})),
			cols: colsOf(colA, colName), lens: []int{full, full},
			ranges:   []*ranger.Range{pointRange(types.NewIntDatum(1), types.NewStringDatum("x"))},
			expected: 1, ok: true,
		},
		{
			name: "fixed prefix column before a fixed unique column",
			tbl:  newBoundTestTable(),
			cols: colsOf(colName, colHandle), lens: []int{4, full},
			ranges:   []*ranger.Range{pointRange(types.NewStringDatum("abcd"), types.NewIntDatum(7))},
			expected: 1, ok: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bound, ok := ranger.RowCountUpperBoundByUniqueKey(tc, tt.tbl, tt.cols, tt.lens, tt.ranges)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.expected, bound)
		})
	}
}
