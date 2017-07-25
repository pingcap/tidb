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
	"sort"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testExpressionSuite{})

type testExpressionSuite struct{}

func newColumn(name string) *Column {
	return &Column{
		FromID:  name,
		ColName: model.NewCIStr(name),
		TblName: model.NewCIStr("t"),
		DBName:  model.NewCIStr("test"),
		RetType: types.NewFieldType(mysql.TypeLonglong),
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
	newFunc, _ := NewFunction(mock.NewContext(), funcName, typeLong, args...)
	return newFunc
}

func (*testExpressionSuite) TestConstantPropagation(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		conditions []Expression
		result     string
	}{
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn("a"), newColumn("b")),
				newFunction(ast.EQ, newColumn("b"), newColumn("c")),
				newFunction(ast.EQ, newColumn("c"), newColumn("d")),
				newFunction(ast.EQ, newColumn("d"), newLonglong(1)),
				newFunction(ast.LogicOr, newLonglong(1), newColumn("a")),
			},
			result: "1, eq(test.t.a, 1), eq(test.t.b, 1), eq(test.t.c, 1), eq(test.t.d, 1)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn("a"), newColumn("b")),
				newFunction(ast.EQ, newColumn("b"), newLonglong(1)),
				newFunction(ast.NE, newColumn("c"), newLonglong(2)),
			},
			result: "eq(test.t.a, 1), eq(test.t.b, 1), ne(test.t.c, 2)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn("a"), newColumn("b")),
				newFunction(ast.EQ, newColumn("b"), newLonglong(1)),
				newFunction(ast.EQ, newColumn("c"), newColumn("d")),
				newFunction(ast.GE, newColumn("c"), newLonglong(2)),
				newFunction(ast.NE, newColumn("c"), newLonglong(4)),
				newFunction(ast.NE, newColumn("d"), newLonglong(5)),
			},
			result: "eq(test.t.a, 1), eq(test.t.b, 1), eq(test.t.c, test.t.d), ge(test.t.c, 2), ge(test.t.d, 2), ne(test.t.c, 4), ne(test.t.c, 5), ne(test.t.d, 4), ne(test.t.d, 5)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn("a"), newColumn("b")),
				newFunction(ast.EQ, newColumn("a"), newColumn("c")),
				newFunction(ast.GE, newColumn("b"), newLonglong(0)),
			},
			result: "eq(test.t.a, test.t.b), eq(test.t.a, test.t.c), ge(test.t.a, 0), ge(test.t.b, 0), ge(test.t.c, 0)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn("a"), newColumn("b")),
				newFunction(ast.GT, newColumn("a"), newLonglong(2)),
				newFunction(ast.GT, newColumn("b"), newLonglong(3)),
				newFunction(ast.LT, newColumn("a"), newLonglong(1)),
				newFunction(ast.GT, newLonglong(2), newColumn("b")),
			},
			result: "eq(test.t.a, test.t.b), gt(2, test.t.a), gt(2, test.t.b), gt(test.t.a, 2), gt(test.t.a, 3), gt(test.t.b, 2), gt(test.t.b, 3), lt(test.t.a, 1), lt(test.t.b, 1)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newLonglong(1), newColumn("a")),
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
			condition: newFunction(ast.LT, newColumn("a"), newFunction(ast.Plus, newLonglong(1), newLonglong(2))),
			result:    "lt(test.t.a, 3)",
		},
		{
			condition: newFunction(ast.LT, newColumn("a"), newFunction(ast.Greatest, newLonglong(1), newLonglong(2))),
			result:    "lt(test.t.a, 2)",
		},
		{
			condition: newFunction(ast.EQ, newColumn("a"), newFunction(ast.Rand)),
			result:    "eq(test.t.a, rand())",
		},
		{
			condition: newFunction(ast.In, newColumn("a"), newLonglong(1), newLonglong(2), newLonglong(3)),
			result:    "in(test.t.a, 1, 2, 3)",
		},
		{
			condition: newFunction(ast.IsNull, newLonglong(1)),
			result:    "0",
		},
		{
			condition: newFunction(ast.EQ, newColumn("a"), newFunction(ast.UnaryNot, newFunction(ast.Plus, newLonglong(1), newLonglong(1)))),
			result:    "eq(test.t.a, 0)",
		},
		{
			condition: newFunction(ast.LT, newColumn("a"), newFunction(ast.Plus, newColumn("b"), newFunction(ast.Plus, newLonglong(2), newLonglong(1)))),
			result:    "lt(test.t.a, plus(test.t.b, 3))",
		},
	}
	for _, tt := range tests {
		newConds := FoldConstant(tt.condition)
		c.Assert(newConds.String(), Equals, tt.result, Commentf("different for expr %s", tt.condition))
	}
}
