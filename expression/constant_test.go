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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testExpressionSuite{})

type testExpressionSuite struct{}

func newColumn(id int) *Column {
	return newColumnWithType(id, types.NewFieldType(mysql.TypeLonglong))
}

func newColumnWithType(id int, t *types.FieldType) *Column {
	return &Column{
		UniqueID: int64(id),
		ColName:  model.NewCIStr(fmt.Sprint(id)),
		TblName:  model.NewCIStr("t"),
		DBName:   model.NewCIStr("test"),
		RetType:  t,
	}
}

func newLonglong(value int64) *Constant {
	return &Constant{
		Value:   types.NewIntDatum(value),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
}

func newDate(year, month, day int) *Constant {
	return newTimeConst(year, month, day, 0, 0, 0, mysql.TypeDate)
}

func newTimestamp(yy, mm, dd, hh, min, ss int) *Constant {
	return newTimeConst(yy, mm, dd, hh, min, ss, mysql.TypeTimestamp)
}

func newTimeConst(yy, mm, dd, hh, min, ss int, tp uint8) *Constant {
	var tmp types.Datum
	tmp.SetMysqlTime(types.Time{
		Time: types.FromDate(yy, mm, dd, 0, 0, 0, 0),
		Type: tp,
	})
	return &Constant{
		Value:   tmp,
		RetType: types.NewFieldType(tp),
	}
}

func newFunction(funcName string, args ...Expression) Expression {
	typeLong := types.NewFieldType(mysql.TypeLonglong)
	return NewFunctionInternal(mock.NewContext(), funcName, typeLong, args...)
}

func (*testExpressionSuite) TestConstantPropagation(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		solver     []PropagateConstantSolver
		conditions []Expression
		result     string
	}{
		{
			solver: []PropagateConstantSolver{newPropConstSolver(), pgSolver2{}},
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
			solver: []PropagateConstantSolver{newPropConstSolver(), pgSolver2{}},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(1), newLonglong(1)),
				newFunction(ast.NE, newColumn(2), newLonglong(2)),
			},
			result: "eq(test.t.0, 1), eq(test.t.1, 1), ne(test.t.2, 2)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
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
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(0), newColumn(2)),
				newFunction(ast.GE, newColumn(1), newLonglong(0)),
			},
			result: "eq(test.t.0, test.t.1), eq(test.t.0, test.t.2), ge(test.t.0, 0), ge(test.t.1, 0), ge(test.t.2, 0)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
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
			solver: []PropagateConstantSolver{newPropConstSolver(), pgSolver2{}},
			conditions: []Expression{
				newFunction(ast.EQ, newLonglong(1), newColumn(0)),
				newLonglong(0),
			},
			result: "0",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.In, newColumn(0), newLonglong(1), newLonglong(2)),
				newFunction(ast.In, newColumn(1), newLonglong(3), newLonglong(4)),
			},
			result: "eq(test.t.0, test.t.1), in(test.t.0, 1, 2), in(test.t.0, 3, 4), in(test.t.1, 1, 2), in(test.t.1, 3, 4)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(0), newFunction(ast.BitLength, newColumn(2))),
			},
			result: "eq(test.t.0, bit_length(cast(test.t.2))), eq(test.t.0, test.t.1), eq(test.t.1, bit_length(cast(test.t.2)))",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.LE, newFunction(ast.Mul, newColumn(0), newColumn(0)), newLonglong(50)),
			},
			result: "eq(test.t.0, test.t.1), le(mul(test.t.0, test.t.0), 50), le(mul(test.t.1, test.t.1), 50)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.LE, newColumn(0), newFunction(ast.Plus, newColumn(1), newLonglong(1))),
			},
			result: "eq(test.t.0, test.t.1), le(test.t.0, plus(test.t.0, 1)), le(test.t.0, plus(test.t.1, 1)), le(test.t.1, plus(test.t.1, 1))",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.LE, newColumn(0), newFunction(ast.Rand)),
			},
			result: "eq(test.t.0, test.t.1), le(cast(test.t.0), rand())",
		},
	}
	for _, tt := range tests {
		for _, solver := range tt.solver {
			ctx := mock.NewContext()
			conds := make([]Expression, 0, len(tt.conditions))
			for _, cd := range tt.conditions {
				conds = append(conds, FoldConstant(cd))
			}
			newConds := solver.PropagateConstant(ctx, conds)
			var result []string
			for _, v := range newConds {
				result = append(result, v.String())
			}
			sort.Strings(result)
			c.Assert(strings.Join(result, ", "), Equals, tt.result, Commentf("different for expr %s", tt.conditions))
		}
	}
}

func (*testExpressionSuite) TestConstraintPropagation(c *C) {
	defer testleak.AfterTest(c)()
	col1 := newColumnWithType(1, types.NewFieldType(mysql.TypeDate))
	col2 := newColumnWithType(2, types.NewFieldType(mysql.TypeTimestamp))
	tests := []struct {
		solver     constraintSolver
		conditions []Expression
		result     string
	}{
		// Don't propagate this any more, because it makes the code more complex but not
		// useful for partition pruning.
		// {
		// 	solver: newConstraintSolver(ruleColumnGTConst),
		// 	conditions: []Expression{
		// 		newFunction(ast.GT, newColumn(0), newLonglong(5)),
		// 		newFunction(ast.GT, newColumn(0), newLonglong(7)),
		// 	},
		// 	result: "gt(test.t.0, 7)",
		// },
		{
			solver: newConstraintSolver(ruleColumnOPConst),
			conditions: []Expression{
				newFunction(ast.GT, newColumn(0), newLonglong(5)),
				newFunction(ast.LT, newColumn(0), newLonglong(5)),
			},
			result: "0",
		},
		{
			solver: newConstraintSolver(ruleColumnOPConst),
			conditions: []Expression{
				newFunction(ast.GT, newColumn(0), newLonglong(7)),
				newFunction(ast.LT, newColumn(0), newLonglong(5)),
			},
			result: "0",
		},
		{
			solver: newConstraintSolver(ruleColumnOPConst),
			// col1 > '2018-12-11' and to_days(col1) < 5 => false
			conditions: []Expression{
				newFunction(ast.GT, col1, newDate(2018, 12, 11)),
				newFunction(ast.LT, newFunction(ast.ToDays, col1), newLonglong(5)),
			},
			result: "0",
		},
		{
			solver: newConstraintSolver(ruleColumnOPConst),
			conditions: []Expression{
				newFunction(ast.LT, newColumn(0), newLonglong(5)),
				newFunction(ast.GT, newColumn(0), newLonglong(5)),
			},
			result: "0",
		},
		{
			solver: newConstraintSolver(ruleColumnOPConst),
			conditions: []Expression{
				newFunction(ast.LT, newColumn(0), newLonglong(5)),
				newFunction(ast.GT, newColumn(0), newLonglong(7)),
			},
			result: "0",
		},
		{
			solver: newConstraintSolver(ruleColumnOPConst),
			// col1 < '2018-12-11' and to_days(col1) > 737999 => false
			conditions: []Expression{
				newFunction(ast.LT, col1, newDate(2018, 12, 11)),
				newFunction(ast.GT, newFunction(ast.ToDays, col1), newLonglong(737999)),
			},
			result: "0",
		},
		{
			solver: newConstraintSolver(ruleColumnOPConst),
			// col2 > unixtimestamp('2008-05-01 00:00:00') and unixtimestamp(col2) < unixtimestamp('2008-04-01 00:00:00') => false
			conditions: []Expression{
				newFunction(ast.GT, col2, newTimestamp(2008, 5, 1, 0, 0, 0)),
				newFunction(ast.LT, newFunction(ast.UnixTimestamp, col2), newLonglong(1206979200)),
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
		newConds := tt.solver.Solve(ctx, conds)
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

func (*testExpressionSuite) TestDeferredExprNullConstantFold(c *C) {
	defer testleak.AfterTest(c)()
	nullConst := &Constant{
		Value:        types.NewDatum(nil),
		RetType:      types.NewFieldType(mysql.TypeTiny),
		DeferredExpr: Null,
	}
	tests := []struct {
		condition Expression
		deferred  string
	}{
		{
			condition: newFunction(ast.LT, newColumn(0), nullConst),
			deferred:  "lt(test.t.0, <nil>)",
		},
	}
	for _, tt := range tests {
		comment := Commentf("different for expr %s", tt.condition)
		sf, ok := tt.condition.(*ScalarFunction)
		c.Assert(ok, IsTrue, comment)
		sf.GetCtx().GetSessionVars().StmtCtx.InNullRejectCheck = true
		newConds := FoldConstant(tt.condition)
		newConst, ok := newConds.(*Constant)
		c.Assert(ok, IsTrue, comment)
		c.Assert(newConst.DeferredExpr.String(), Equals, tt.deferred, comment)
	}
}

func (*testExpressionSuite) TestDeferredExprNotNull(c *C) {
	defer testleak.AfterTest(c)()
	m := &MockExpr{}
	ctx := mock.NewContext()
	cst := &Constant{DeferredExpr: m, RetType: newIntFieldType()}
	m.i, m.err = nil, fmt.Errorf("ERROR")
	_, _, err := cst.EvalInt(ctx, chunk.Row{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalReal(ctx, chunk.Row{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalDecimal(ctx, chunk.Row{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalString(ctx, chunk.Row{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalTime(ctx, chunk.Row{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalDuration(ctx, chunk.Row{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalJSON(ctx, chunk.Row{})
	c.Assert(err, NotNil)

	m.i, m.err = nil, nil
	_, isNull, err := cst.EvalInt(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalReal(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalDecimal(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalString(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalTime(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalDuration(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalJSON(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)

	m.i = int64(2333)
	xInt, _, _ := cst.EvalInt(ctx, chunk.Row{})
	c.Assert(xInt, Equals, int64(2333))

	m.i = float64(123.45)
	xFlo, _, _ := cst.EvalReal(ctx, chunk.Row{})
	c.Assert(xFlo, Equals, float64(123.45))

	m.i = "abc"
	xStr, _, _ := cst.EvalString(ctx, chunk.Row{})
	c.Assert(xStr, Equals, "abc")

	m.i = &types.MyDecimal{}
	xDec, _, _ := cst.EvalDecimal(ctx, chunk.Row{})
	c.Assert(xDec.Compare(m.i.(*types.MyDecimal)), Equals, 0)

	m.i = types.Time{}
	xTim, _, _ := cst.EvalTime(ctx, chunk.Row{})
	c.Assert(xTim.Compare(m.i.(types.Time)), Equals, 0)

	m.i = types.Duration{}
	xDur, _, _ := cst.EvalDuration(ctx, chunk.Row{})
	c.Assert(xDur.Compare(m.i.(types.Duration)), Equals, 0)

	m.i = json.BinaryJSON{}
	xJsn, _, _ := cst.EvalJSON(ctx, chunk.Row{})
	c.Assert(m.i.(json.BinaryJSON).String(), Equals, xJsn.String())

	cln := cst.Clone().(*Constant)
	c.Assert(cln.DeferredExpr, Equals, cst.DeferredExpr)
}
