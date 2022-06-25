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

package core

import (
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func newTypeWithFlen(typeByte byte, flen int) *types.FieldType {
	tp := types.NewFieldType(typeByte)
	tp.SetFlen(flen)
	return tp
}

func SubstituteCol2CorCol(expr expression.Expression, colIDs map[int64]struct{}) (expression.Expression, error) {
	switch x := expr.(type) {
	case *expression.ScalarFunction:
		newArgs := make([]expression.Expression, 0, len(x.GetArgs()))
		for _, arg := range x.GetArgs() {
			newArg, err := SubstituteCol2CorCol(arg, colIDs)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newArgs = append(newArgs, newArg)
		}
		newSf, err := expression.NewFunction(x.GetCtx(), x.FuncName.L, x.GetType(), newArgs...)
		return newSf, errors.Trace(err)
	case *expression.Column:
		if _, ok := colIDs[x.UniqueID]; ok {
			return &expression.CorrelatedColumn{Column: *x}, nil
		}
	}
	return expr, nil
}

func TestIndexPathSplitCorColCond(t *testing.T) {
	ctx := MockContext()
	totalSchema := expression.NewSchema()
	totalSchema.Append(&expression.Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	totalSchema.Append(&expression.Column{
		UniqueID: 2,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	totalSchema.Append(&expression.Column{
		UniqueID: 3,
		RetType:  newTypeWithFlen(mysql.TypeVarchar, 10),
	})
	totalSchema.Append(&expression.Column{
		UniqueID: 4,
		RetType:  newTypeWithFlen(mysql.TypeVarchar, 10),
	})
	totalSchema.Append(&expression.Column{
		UniqueID: 5,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	names := make(types.NameSlice, 0, 5)
	names = append(names, &types.FieldName{ColName: model.NewCIStr("col1")})
	names = append(names, &types.FieldName{ColName: model.NewCIStr("col2")})
	names = append(names, &types.FieldName{ColName: model.NewCIStr("col3")})
	names = append(names, &types.FieldName{ColName: model.NewCIStr("col4")})
	names = append(names, &types.FieldName{ColName: model.NewCIStr("col5")})
	testCases := []struct {
		expr       string
		corColIDs  []int64
		idxColIDs  []int64
		idxColLens []int
		access     string
		remained   string
	}{
		{
			expr:       "col1 = col2",
			corColIDs:  []int64{2},
			idxColIDs:  []int64{1},
			idxColLens: []int{types.UnspecifiedLength},
			access:     "[eq(Column#1, Column#2)]",
			remained:   "[]",
		},
		{
			expr:       "col1 = col5 and col2 = 1",
			corColIDs:  []int64{5},
			idxColIDs:  []int64{1, 2},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(Column#1, Column#5) eq(Column#2, 1)]",
			remained:   "[]",
		},
		{
			expr:       "col1 = col5 and col2 = 1",
			corColIDs:  []int64{5},
			idxColIDs:  []int64{2, 1},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(Column#2, 1) eq(Column#1, Column#5)]",
			remained:   "[]",
		},
		{
			expr:       "col1 = col5 and col2 = 1",
			corColIDs:  []int64{5},
			idxColIDs:  []int64{1},
			idxColLens: []int{types.UnspecifiedLength},
			access:     "[eq(Column#1, Column#5)]",
			remained:   "[eq(Column#2, 1)]",
		},
		{
			expr:       "col2 = 1 and col1 = col5",
			corColIDs:  []int64{5},
			idxColIDs:  []int64{1},
			idxColLens: []int{types.UnspecifiedLength},
			access:     "[eq(Column#1, Column#5)]",
			remained:   "[eq(Column#2, 1)]",
		},
		{
			expr:       "col1 = col2 and col3 = col4 and col5 = 1",
			corColIDs:  []int64{2, 4},
			idxColIDs:  []int64{1, 3},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(Column#1, Column#2) eq(Column#3, Column#4)]",
			remained:   "[eq(Column#5, 1)]",
		},
		{
			expr:       "col1 = col2 and col3 = col4 and col5 = 1",
			corColIDs:  []int64{2, 4},
			idxColIDs:  []int64{1, 3},
			idxColLens: []int{types.UnspecifiedLength, 2},
			access:     "[eq(Column#1, Column#2) eq(Column#3, Column#4)]",
			remained:   "[eq(Column#3, Column#4) eq(Column#5, 1)]",
		},
		{
			expr:       `col1 = col5 and col3 = "col1" and col2 = col5`,
			corColIDs:  []int64{5},
			idxColIDs:  []int64{1, 2, 3},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(Column#1, Column#5) eq(Column#2, Column#5) eq(Column#3, col1)]",
			remained:   "[]",
		},
		{
			expr:       "col3 = CHAR(1 COLLATE 'binary')",
			corColIDs:  []int64{},
			idxColIDs:  []int64{3},
			idxColLens: []int{types.UnspecifiedLength},
			access:     "[eq(Column#3, \x01)]",
			remained:   "[]",
		},
	}
	for _, tt := range testCases {
		comment := fmt.Sprintf("failed at case:\nexpr: %v\ncorColIDs: %v\nidxColIDs: %v\nidxColLens: %v\naccess: %v\nremained: %v\n", tt.expr, tt.corColIDs, tt.idxColIDs, tt.idxColLens, tt.access, tt.remained)
		filters, err := expression.ParseSimpleExprsWithNames(ctx, tt.expr, totalSchema, names)
		require.NoError(t, err, comment)
		if sf, ok := filters[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			filters = expression.FlattenCNFConditions(sf)
		}
		trueFilters := make([]expression.Expression, 0, len(filters))
		idMap := make(map[int64]struct{})
		for _, id := range tt.corColIDs {
			idMap[id] = struct{}{}
		}
		for _, filter := range filters {
			trueFilter, err := SubstituteCol2CorCol(filter, idMap)
			require.NoError(t, err, comment)
			trueFilters = append(trueFilters, trueFilter)
		}
		path := util.AccessPath{
			EqCondCount:  0,
			TableFilters: trueFilters,
			IdxCols:      expression.FindPrefixOfIndex(totalSchema.Columns, tt.idxColIDs),
			IdxColLens:   tt.idxColLens,
		}

		access, remained := path.SplitCorColAccessCondFromFilters(ctx, path.EqCondCount)
		require.Equal(t, tt.access, fmt.Sprintf("%s", access), comment)
		require.Equal(t, tt.remained, fmt.Sprintf("%s", remained), comment)
	}
}
