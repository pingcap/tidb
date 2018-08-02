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

package expression

import (
	"fmt"
	"sort"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testExpressionSuite{})

type testExpressionSuite struct{}

func newColumn(id int) *Column {
	return &Column{
		UniqueID: id,
		ColName:  model.NewCIStr(fmt.Sprint(id)),
		TblName:  model.NewCIStr("t"),
		DBName:   model.NewCIStr("test"),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
}

func newLonglong(value int64) *Constant {
	return &Constant{
		Value:   types.NewIntDatum(value),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
}

func newFunction(funcName string, args ...Expression) Expression {
	typeLong := types.NewFieldType(mysql.TypeLonglong)
	return NewFunctionInternal(mock.NewContext(), funcName, typeLong, args...)
}

func (*testExpressionSuite) TestConstantPropagation(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		conditions []Expression
		result     string
	}{
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(1), newColumn(2)),
				newFunction(ast.EQ, newColumn(2), newColumn(3)),
				newFunction(ast.EQ, newColumn(3), newLonglong(1)),
				newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
			},
			result: "1, eq(test.t.0, 1), eq(test.t.1, 1), eq(test.t.2, 1), eq(test.t.3, 1)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(1), newLonglong(1)),
				newFunction(ast.NE, newColumn(2), newLonglong(2)),
			},
			result: "eq(test.t.0, 1), eq(test.t.1, 1), ne(test.t.2, 2)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(1), newLonglong(1)),
				newFunction(ast.EQ, newColumn(2), newColumn(3)),
				newFunction(ast.GE, newColumn(2), newLonglong(2)),
				newFunction(ast.NE, newColumn(2), newLonglong(4)),
				newFunction(ast.NE, newColumn(3), newLonglong(5)),
			},
			result: "eq(test.t.0, 1), eq(test.t.1, 1), eq(test.t.2, test.t.3), ge(test.t.2, 2), ge(test.t.3, 2), ne(test.t.2, 4), ne(test.t.2, 5), ne(test.t.3, 4), ne(test.t.3, 5)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(0), newColumn(2)),
				newFunction(ast.GE, newColumn(1), newLonglong(0)),
			},
			result: "eq(test.t.0, test.t.1), eq(test.t.0, test.t.2), ge(test.t.0, 0), ge(test.t.1, 0), ge(test.t.2, 0)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.GT, newColumn(0), newLonglong(2)),
				newFunction(ast.GT, newColumn(1), newLonglong(3)),
				newFunction(ast.LT, newColumn(0), newLonglong(1)),
				newFunction(ast.GT, newLonglong(2), newColumn(1)),
			},
			result: "eq(test.t.0, test.t.1), gt(2, test.t.0), gt(2, test.t.1), gt(test.t.0, 2), gt(test.t.0, 3), gt(test.t.1, 2), gt(test.t.1, 3), lt(test.t.0, 1), lt(test.t.1, 1)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newLonglong(1), newColumn(0)),
				newLonglong(0),
			},
			result: "0",
		},
	}
	for _, tt := range tests {
		ctx := mock.NewContext()
		conds := make([]Expression, 0, len(tt.conditions))
		for _, cd := range tt.conditions {
			conds = append(conds, FoldConstant(cd))
		}
		newConds := PropagateConstant(ctx, conds)
		var result []string
		for _, v := range newConds {
			result = append(result, v.String())
		}
		sort.Strings(result)
		c.Assert(strings.Join(result, ", "), Equals, tt.result, Commentf("different for expr %s", tt.conditions))
	}
}

func (*testExpressionSuite) TestConstantFolding(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		condition Expression
		result    string
	}{
		{
			condition: newFunction(ast.LT, newColumn(0), newFunction(ast.Plus, newLonglong(1), newLonglong(2))),
			result:    "lt(test.t.0, 3)",
		},
		{
			condition: newFunction(ast.LT, newColumn(0), newFunction(ast.Greatest, newLonglong(1), newLonglong(2))),
			result:    "lt(test.t.0, 2)",
		},
		{
			condition: newFunction(ast.EQ, newColumn(0), newFunction(ast.Rand)),
			result:    "eq(cast(test.t.0), rand())",
		},
		{
			condition: newFunction(ast.IsNull, newLonglong(1)),
			result:    "0",
		},
		{
			condition: newFunction(ast.EQ, newColumn(0), newFunction(ast.UnaryNot, newFunction(ast.Plus, newLonglong(1), newLonglong(1)))),
			result:    "eq(test.t.0, 0)",
		},
		{
			condition: newFunction(ast.LT, newColumn(0), newFunction(ast.Plus, newColumn(1), newFunction(ast.Plus, newLonglong(2), newLonglong(1)))),
			result:    "lt(test.t.0, plus(test.t.1, 3))",
		},
	}
	for _, tt := range tests {
		newConds := FoldConstant(tt.condition)
		c.Assert(newConds.String(), Equals, tt.result, Commentf("different for expr %s", tt.condition))
	}
}
