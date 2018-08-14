// Copyright 2016 PingCAP, Inc.
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

package plan

import (
	"fmt"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testUnitTestSuit{})

type testUnitTestSuit struct {
	ctx sessionctx.Context
}

func (s *testUnitTestSuit) SetUpSuite(c *C) {
	s.ctx = mockContext()
}

func (s *testUnitTestSuit) newTypeWithFlen(typeByte byte, flen int) *types.FieldType {
	tp := types.NewFieldType(typeByte)
	tp.Flen = flen
	return tp
}

func (s *testUnitTestSuit) SubstituteCol2CorCol(expr expression.Expression, colIDs map[int]struct{}) (expression.Expression, error) {
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
		newSf := expression.NewFunctionInternal(x.GetCtx(), x.FuncName.L, x.GetType(), newArgs...)
		return newSf, nil
	case *expression.Column:
		if _, ok := colIDs[x.UniqueID]; ok {
			return &expression.CorrelatedColumn{Column: *x}, nil
		}
	default:
		return x, nil
	}
	return expr, nil
}

func (s *testUnitTestSuit) TestIndexPathSplitCorColCond(c *C) {
	defer testleak.AfterTest(c)()
	totalSchema := expression.NewSchema()
	totalSchema.Append(&expression.Column{
		ColName:  model.NewCIStr("a"),
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	totalSchema.Append(&expression.Column{
		ColName:  model.NewCIStr("b"),
		UniqueID: 2,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	totalSchema.Append(&expression.Column{
		ColName:  model.NewCIStr("c"),
		UniqueID: 3,
		RetType:  s.newTypeWithFlen(mysql.TypeVarchar, 10),
	})
	totalSchema.Append(&expression.Column{
		ColName:  model.NewCIStr("d"),
		UniqueID: 4,
		RetType:  s.newTypeWithFlen(mysql.TypeVarchar, 10),
	})
	totalSchema.Append(&expression.Column{
		ColName:  model.NewCIStr("e"),
		UniqueID: 5,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	testCases := []struct {
		expr       string
		corColIDs  []int
		idxColIDs  []int
		idxColLens []int
		access     string
		remained   string
	}{
		{
			expr:       "a = b",
			corColIDs:  []int{2},
			idxColIDs:  []int{1},
			idxColLens: []int{types.UnspecifiedLength},
			access:     "[eq(a, b)]",
			remained:   "[]",
		},
		{
			expr:       "a = e and b = 1",
			corColIDs:  []int{5},
			idxColIDs:  []int{1, 2},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(a, e) eq(b, 1)]",
			remained:   "[]",
		},
		{
			expr:       "a = e and b = 1",
			corColIDs:  []int{5},
			idxColIDs:  []int{2, 1},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(b, 1) eq(a, e)]",
			remained:   "[]",
		},
		{
			expr:       "a = e and b = 1",
			corColIDs:  []int{5},
			idxColIDs:  []int{1},
			idxColLens: []int{types.UnspecifiedLength},
			access:     "[eq(a, e)]",
			remained:   "[eq(b, 1)]",
		},
		{
			expr:       "b = 1 and a = e",
			corColIDs:  []int{5},
			idxColIDs:  []int{1},
			idxColLens: []int{types.UnspecifiedLength},
			access:     "[eq(a, e)]",
			remained:   "[eq(b, 1)]",
		},
		{
			expr:       "a = b and c = d and e = 1",
			corColIDs:  []int{2, 4},
			idxColIDs:  []int{1, 3},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(a, b) eq(c, d)]",
			remained:   "[eq(e, 1)]",
		},
		{
			expr:       "a = b and c = d and e = 1",
			corColIDs:  []int{2, 4},
			idxColIDs:  []int{1, 3},
			idxColLens: []int{types.UnspecifiedLength, 2},
			access:     "[eq(a, b) eq(c, d)]",
			remained:   "[eq(c, d) eq(e, 1)]",
		},
		{
			expr:       `a = e and c = "a" and b = e`,
			corColIDs:  []int{5},
			idxColIDs:  []int{1, 2, 3},
			idxColLens: []int{types.UnspecifiedLength, types.UnspecifiedLength, types.UnspecifiedLength},
			access:     "[eq(a, e) eq(b, e) eq(c, a)]",
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
		idMap := make(map[int]struct{})
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
			idxCols:      expression.FindColumnsByUniqueIDs(totalSchema.Columns, tt.idxColIDs),
			idxColLens:   tt.idxColLens,
		}
		access, remained := path.splitCorColAccessCondFromFilters()
		c.Assert(fmt.Sprintf("%s", access), Equals, tt.access, comment)
		c.Assert(fmt.Sprintf("%s", remained), Equals, tt.remained, comment)
	}
}
