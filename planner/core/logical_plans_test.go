// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain col1 copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/sirupsen/logrus"
)

var _ = Suite(&testUnitTestSuit{})

type testUnitTestSuit struct {
	ctx sessionctx.Context
}

func (s *testUnitTestSuit) SetUpSuite(c *C) {
	s.ctx = MockContext()
}

func (s *testUnitTestSuit) newTypeWithFlen(typeByte byte, flen int) *types.FieldType {
	tp := types.NewFieldType(typeByte)
	tp.Flen = flen
	return tp
}

func (s *testUnitTestSuit) SubstituteCol2CorCol(expr expression.Expression, colIDs map[int64]struct{}) (expression.Expression, error) {
	switch x := expr.(type) {
	case *expression.ScalarFunction:
		newArgs := make([]expression.Expression, 0, len(x.GetArgs()))
		for _, arg := range x.GetArgs() {
			newArg, err := s.SubstituteCol2CorCol(arg, colIDs)
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

func (s *testUnitTestSuit) TestIndexPathSplitCorColCond(c *C) {
	defer testleak.AfterTest(c)()
	totalSchema := expression.NewSchema()
	totalSchema.Append(&expression.Column{
		ColName:  model.NewCIStr("col1"),
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	totalSchema.Append(&expression.Column{
		ColName:  model.NewCIStr("col2"),
		UniqueID: 2,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	totalSchema.Append(&expression.Column{
		ColName:  model.NewCIStr("col3"),
		UniqueID: 3,
		RetType:  s.newTypeWithFlen(mysql.TypeVarchar, 10),
	})
	totalSchema.Append(&expression.Column{
		ColName:  model.NewCIStr("col4"),
		UniqueID: 4,
		RetType:  s.newTypeWithFlen(mysql.TypeVarchar, 10),
	})
	totalSchema.Append(&expression.Column{
		ColName:  model.NewCIStr("col5"),
		UniqueID: 5,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
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
			access:     "[eq(col1, col2)]",
			remained:   "[]",
		},
		{
			expr:       "col1 = col5 and col2 = 1",
			corColIDs:  []int64{5},
			idxColIDs:  []int64{1, 2},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(col1, col5) eq(col2, 1)]",
			remained:   "[]",
		},
		{
			expr:       "col1 = col5 and col2 = 1",
			corColIDs:  []int64{5},
			idxColIDs:  []int64{2, 1},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(col2, 1) eq(col1, col5)]",
			remained:   "[]",
		},
		{
			expr:       "col1 = col5 and col2 = 1",
			corColIDs:  []int64{5},
			idxColIDs:  []int64{1},
			idxColLens: []int{types.UnspecifiedLength},
			access:     "[eq(col1, col5)]",
			remained:   "[eq(col2, 1)]",
		},
		{
			expr:       "col2 = 1 and col1 = col5",
			corColIDs:  []int64{5},
			idxColIDs:  []int64{1},
			idxColLens: []int{types.UnspecifiedLength},
			access:     "[eq(col1, col5)]",
			remained:   "[eq(col2, 1)]",
		},
		{
			expr:       "col1 = col2 and col3 = col4 and col5 = 1",
			corColIDs:  []int64{2, 4},
			idxColIDs:  []int64{1, 3},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(col1, col2) eq(col3, col4)]",
			remained:   "[eq(col5, 1)]",
		},
		{
			expr:       "col1 = col2 and col3 = col4 and col5 = 1",
			corColIDs:  []int64{2, 4},
			idxColIDs:  []int64{1, 3},
			idxColLens: []int{types.UnspecifiedLength, 2},
			access:     "[eq(col1, col2) eq(col3, col4)]",
			remained:   "[eq(col3, col4) eq(col5, 1)]",
		},
		{
			expr:       `col1 = col5 and col3 = "col1" and col2 = col5`,
			corColIDs:  []int64{5},
			idxColIDs:  []int64{1, 2, 3},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(col1, col5) eq(col2, col5) eq(col3, col1)]",
			remained:   "[]",
		},
	}
	for _, tt := range testCases {
		comment := Commentf("failed at case:\nexpr: %v\ncorColIDs: %v\nidxColIDs: %v\nidxColLens: %v\naccess: %v\nremained: %v\n", tt.expr, tt.corColIDs, tt.idxColIDs, tt.idxColLens, tt.access, tt.remained)
		filters, err := expression.ParseSimpleExprsWithSchema(s.ctx, tt.expr, totalSchema)
		if sf, ok := filters[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			filters = expression.FlattenCNFConditions(sf)
		}
		c.Assert(err, IsNil, comment)
		trueFilters := make([]expression.Expression, 0, len(filters))
		idMap := make(map[int64]struct{})
		for _, id := range tt.corColIDs {
			idMap[id] = struct{}{}
		}
		for _, filter := range filters {
			trueFilter, err := s.SubstituteCol2CorCol(filter, idMap)
			c.Assert(err, IsNil, comment)
			trueFilters = append(trueFilters, trueFilter)
		}
		path := accessPath{
			eqCondCount:  0,
			tableFilters: trueFilters,
			idxCols:      expression.FindPrefixOfIndex(totalSchema.Columns, tt.idxColIDs),
			idxColLens:   tt.idxColLens,
		}

		logrus.Warnf("idx cols: %v", path.idxCols)
		access, remained := path.splitCorColAccessCondFromFilters()
		c.Assert(fmt.Sprintf("%s", access), Equals, tt.access, comment)
		c.Assert(fmt.Sprintf("%s", remained), Equals, tt.remained, comment)
	}
}
