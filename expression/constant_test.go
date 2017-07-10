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
	nullValue := &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}
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
				newFunction(ast.OrOr, newLonglong(1), newColumn("a")),
			},
			result: "eq(a.a(0), 1), eq(b.b(0), 1), eq(c.c(0), 1), eq(d.d(0), 1), or(1, 1)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn("a"), newColumn("b")),
				newFunction(ast.EQ, newColumn("b"), newLonglong(1)),
				newFunction(ast.EQ, newColumn("a"), nullValue),
				newFunction(ast.NE, newColumn("c"), newLonglong(2)),
			},
			result: "0",
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
			result: "eq(a.a(0), 1), eq(b.b(0), 1), eq(c.c(0), d.d(0)), ge(c.c(0), 2), ge(d.d(0), 2), ne(c.c(0), 4), ne(c.c(0), 5), ne(d.d(0), 4), ne(d.d(0), 5)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn("a"), newColumn("b")),
				newFunction(ast.EQ, newColumn("a"), newColumn("c")),
				newFunction(ast.GE, newColumn("b"), newLonglong(0)),
			},
			result: "eq(a.a(0), b.b(0)), eq(a.a(0), c.c(0)), ge(a.a(0), 0), ge(b.b(0), 0), ge(c.c(0), 0)",
		},
		{
			conditions: []Expression{
				newFunction(ast.EQ, newColumn("a"), newColumn("b")),
				newFunction(ast.GT, newColumn("a"), newLonglong(2)),
				newFunction(ast.GT, newColumn("b"), newLonglong(3)),
				newFunction(ast.LT, newColumn("a"), newLonglong(1)),
				newFunction(ast.GT, newLonglong(2), newColumn("b")),
			},
			result: "eq(a.a(0), b.b(0)), gt(2, a.a(0)), gt(2, b.b(0)), gt(a.a(0), 2), gt(a.a(0), 3), gt(b.b(0), 2), gt(b.b(0), 3), lt(a.a(0), 1), lt(b.b(0), 1)",
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
		newConds := PropagateConstant(ctx, tt.conditions)
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
			result:    "lt(a.a(0), 3)",
		},
		{
			condition: newFunction(ast.LT, newColumn("a"), newFunction(ast.Greatest, newLonglong(1), newLonglong(2))),
			result:    "lt(a.a(0), 2)",
		},
		{
			condition: newFunction(ast.EQ, newColumn("a"), newFunction(ast.Rand)),
			result:    "eq(a.a(0), rand())",
		},
		{
			condition: newFunction(ast.In, newColumn("a"), newLonglong(1), newLonglong(2), newLonglong(3)),
			result:    "in(a.a(0), 1, 2, 3)",
		},
		{
			condition: newFunction(ast.IsNull, newLonglong(1)),
			result:    "0",
		},
		{
			condition: newFunction(ast.EQ, newColumn("a"), newFunction(ast.UnaryNot, newFunction(ast.Plus, newLonglong(1), newLonglong(1)))),
			result:    "eq(a.a(0), 0)",
		},
		{
			condition: newFunction(ast.LT, newColumn("a"), newFunction(ast.Plus, newColumn("b"), newFunction(ast.Plus, newLonglong(2), newLonglong(1)))),
			result:    "lt(a.a(0), plus(b.b(0), 3))",
		},
	}
	for _, tt := range tests {
		newConds := FoldConstant(tt.condition)
		c.Assert(newConds.String(), Equals, tt.result, Commentf("different for expr %s", tt.condition))
	}
}
