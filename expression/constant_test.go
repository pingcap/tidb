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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testExpressionSuite{})

type testExpressionSuite struct{}

func newColumn(id int) *Column {
	return newColumnWithType(id, types.NewFieldType(mysql.TypeLonglong))
}

func newColumnWithType(id int, t *types.FieldType) *Column {
	return &Column{
		UniqueID: int64(id),
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
	tmp.SetMysqlTime(types.NewTime(types.FromDate(yy, mm, dd, 0, 0, 0, 0), tp, types.DefaultFsp))
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
	tests := []struct {
		solver     []PropagateConstantSolver
		conditions []Expression
		result     string
	}{
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(1), newColumn(2)),
				newFunction(ast.EQ, newColumn(2), newColumn(3)),
				newFunction(ast.EQ, newColumn(3), newLonglong(1)),
				newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
			},
			result: "1, eq(Column#0, 1), eq(Column#1, 1), eq(Column#2, 1), eq(Column#3, 1)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(1), newLonglong(1)),
				newFunction(ast.NE, newColumn(2), newLonglong(2)),
			},
			result: "eq(Column#0, 1), eq(Column#1, 1), ne(Column#2, 2)",
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
			result: "eq(Column#0, 1), eq(Column#1, 1), eq(Column#2, Column#3), ge(Column#2, 2), ge(Column#3, 2), ne(Column#2, 4), ne(Column#2, 5), ne(Column#3, 4), ne(Column#3, 5)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(0), newColumn(2)),
				newFunction(ast.GE, newColumn(1), newLonglong(0)),
			},
			result: "eq(Column#0, Column#1), eq(Column#0, Column#2), ge(Column#0, 0), ge(Column#1, 0), ge(Column#2, 0)",
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
			result: "eq(Column#0, Column#1), gt(2, Column#0), gt(2, Column#1), gt(Column#0, 2), gt(Column#0, 3), gt(Column#1, 2), gt(Column#1, 3), lt(Column#0, 1), lt(Column#1, 1)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
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
			result: "eq(Column#0, Column#1), in(Column#0, 1, 2), in(Column#0, 3, 4), in(Column#1, 1, 2), in(Column#1, 3, 4)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(0), newFunction(ast.BitLength, newColumn(2))),
			},
			result: "eq(Column#0, Column#1), eq(Column#0, bit_length(cast(Column#2, var_string(20)))), eq(Column#1, bit_length(cast(Column#2, var_string(20))))",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.LE, newFunction(ast.Mul, newColumn(0), newColumn(0)), newLonglong(50)),
			},
			result: "eq(Column#0, Column#1), le(mul(Column#0, Column#0), 50), le(mul(Column#1, Column#1), 50)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.LE, newColumn(0), newFunction(ast.Plus, newColumn(1), newLonglong(1))),
			},
			result: "eq(Column#0, Column#1), le(Column#0, plus(Column#0, 1)), le(Column#0, plus(Column#1, 1)), le(Column#1, plus(Column#1, 1))",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.LE, newColumn(0), newFunction(ast.Rand)),
			},
			result: "eq(Column#0, Column#1), le(cast(Column#0, double BINARY), rand())",
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

func (*testExpressionSuite) TestConstantFolding(c *C) {
	tests := []struct {
		condition Expression
		result    string
	}{
		{
			condition: newFunction(ast.LT, newColumn(0), newFunction(ast.Plus, newLonglong(1), newLonglong(2))),
			result:    "lt(Column#0, 3)",
		},
		{
			condition: newFunction(ast.LT, newColumn(0), newFunction(ast.Greatest, newLonglong(1), newLonglong(2))),
			result:    "lt(Column#0, 2)",
		},
		{
			condition: newFunction(ast.EQ, newColumn(0), newFunction(ast.Rand)),
			result:    "eq(cast(Column#0, double BINARY), rand())",
		},
		{
			condition: newFunction(ast.IsNull, newLonglong(1)),
			result:    "0",
		},
		{
			condition: newFunction(ast.EQ, newColumn(0), newFunction(ast.UnaryNot, newFunction(ast.Plus, newLonglong(1), newLonglong(1)))),
			result:    "eq(Column#0, 0)",
		},
		{
			condition: newFunction(ast.LT, newColumn(0), newFunction(ast.Plus, newColumn(1), newFunction(ast.Plus, newLonglong(2), newLonglong(1)))),
			result:    "lt(Column#0, plus(Column#1, 3))",
		},
	}
	for _, tt := range tests {
		newConds := FoldConstant(tt.condition)
		c.Assert(newConds.String(), Equals, tt.result, Commentf("different for expr %s", tt.condition))
	}
}

func (*testExpressionSuite) TestDeferredExprNullConstantFold(c *C) {
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
			deferred:  "lt(Column#0, <nil>)",
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

func (*testExpressionSuite) TestDeferredParamNotNull(c *C) {
	ctx := mock.NewContext()
	testTime := time.Now()
	ctx.GetSessionVars().PreparedParams = []types.Datum{
		types.NewIntDatum(1),
		types.NewDecimalDatum(types.NewDecFromStringForTest("20170118123950.123")),
		types.NewTimeDatum(types.NewTime(types.FromGoTime(testTime), mysql.TypeTimestamp, 6)),
		types.NewDurationDatum(types.ZeroDuration),
		types.NewStringDatum("{}"),
		types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{1})),
		types.NewBytesDatum([]byte{'b'}),
		types.NewFloat32Datum(1.1),
		types.NewFloat64Datum(2.1),
		types.NewUintDatum(100),
		types.NewMysqlBitDatum(types.BinaryLiteral([]byte{1})),
		types.NewMysqlEnumDatum(types.Enum{Name: "n", Value: 2}),
	}
	cstInt := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 0}, RetType: newIntFieldType()}
	cstDec := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 1}, RetType: newDecimalFieldType()}
	cstTime := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 2}, RetType: newDateFieldType()}
	cstDuration := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 3}, RetType: newDurFieldType()}
	cstJSON := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 4}, RetType: newJSONFieldType()}
	cstBytes := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 6}, RetType: newBlobFieldType()}
	cstBinary := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 5}, RetType: newBinaryLiteralFieldType()}
	cstFloat32 := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 7}, RetType: newFloatFieldType()}
	cstFloat64 := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 8}, RetType: newFloatFieldType()}
	cstUint := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 9}, RetType: newIntFieldType()}
	cstBit := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 10}, RetType: newBinaryLiteralFieldType()}
	cstEnum := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 11}, RetType: newEnumFieldType()}

	c.Assert(mysql.TypeVarString, Equals, cstJSON.GetType().Tp)
	c.Assert(mysql.TypeNewDecimal, Equals, cstDec.GetType().Tp)
	c.Assert(mysql.TypeLonglong, Equals, cstInt.GetType().Tp)
	c.Assert(mysql.TypeLonglong, Equals, cstUint.GetType().Tp)
	c.Assert(mysql.TypeTimestamp, Equals, cstTime.GetType().Tp)
	c.Assert(mysql.TypeDuration, Equals, cstDuration.GetType().Tp)
	c.Assert(mysql.TypeBlob, Equals, cstBytes.GetType().Tp)
	c.Assert(mysql.TypeBit, Equals, cstBinary.GetType().Tp)
	c.Assert(mysql.TypeBit, Equals, cstBit.GetType().Tp)
	c.Assert(mysql.TypeFloat, Equals, cstFloat32.GetType().Tp)
	c.Assert(mysql.TypeDouble, Equals, cstFloat64.GetType().Tp)
	c.Assert(mysql.TypeEnum, Equals, cstEnum.GetType().Tp)

	d, _, err := cstInt.EvalInt(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d, Equals, int64(1))
	r, _, err := cstFloat64.EvalReal(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r, Equals, float64(2.1))
	de, _, err := cstDec.EvalDecimal(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(de.String(), Equals, "20170118123950.123")
	s, _, err := cstBytes.EvalString(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "b")
	t, _, err := cstTime.EvalTime(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(t.Compare(ctx.GetSessionVars().PreparedParams[2].GetMysqlTime()), Equals, 0)
	dur, _, err := cstDuration.EvalDuration(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(dur.Duration, Equals, types.ZeroDuration.Duration)
	json, _, err := cstJSON.EvalJSON(ctx, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(json, NotNil)
}

func (*testExpressionSuite) TestDeferredExprNotNull(c *C) {
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

	m.i = types.ZeroTime
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

func (*testExpressionSuite) TestVectorizedConstant(c *C) {
	// fixed-length type with/without Sel
	for _, cst := range []*Constant{
		{RetType: newIntFieldType(), Value: types.NewIntDatum(2333)},
		{RetType: newIntFieldType(), DeferredExpr: &Constant{RetType: newIntFieldType(), Value: types.NewIntDatum(2333)}}} {
		chk := chunk.New([]*types.FieldType{newIntFieldType()}, 1024, 1024)
		for i := 0; i < 1024; i++ {
			chk.AppendInt64(0, int64(i))
		}
		col := chunk.NewColumn(newIntFieldType(), 1024)
		ctx := mock.NewContext()
		c.Assert(cst.VecEvalInt(ctx, chk, col), IsNil)
		i64s := col.Int64s()
		c.Assert(len(i64s), Equals, 1024)
		for _, v := range i64s {
			c.Assert(v, Equals, int64(2333))
		}

		// fixed-length type with Sel
		sel := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
		chk.SetSel(sel)
		c.Assert(cst.VecEvalInt(ctx, chk, col), IsNil)
		i64s = col.Int64s()
		for i := range sel {
			c.Assert(i64s[i], Equals, int64(2333))
		}
	}

	// var-length type with/without Sel
	for _, cst := range []*Constant{
		{RetType: newStringFieldType(), Value: types.NewStringDatum("hello")},
		{RetType: newStringFieldType(), DeferredExpr: &Constant{RetType: newStringFieldType(), Value: types.NewStringDatum("hello")}}} {
		chk := chunk.New([]*types.FieldType{newIntFieldType()}, 1024, 1024)
		for i := 0; i < 1024; i++ {
			chk.AppendInt64(0, int64(i))
		}
		cst = &Constant{DeferredExpr: nil, RetType: newStringFieldType(), Value: types.NewStringDatum("hello")}
		chk.SetSel(nil)
		col := chunk.NewColumn(newStringFieldType(), 1024)
		ctx := mock.NewContext()
		c.Assert(cst.VecEvalString(ctx, chk, col), IsNil)
		for i := 0; i < 1024; i++ {
			c.Assert(col.GetString(i), Equals, "hello")
		}

		// var-length type with Sel
		sel := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
		chk.SetSel(sel)
		c.Assert(cst.VecEvalString(ctx, chk, col), IsNil)
		for i := range sel {
			c.Assert(col.GetString(i), Equals, "hello")
		}
	}
}

func (*testExpressionSuite) TestGetTypeThreadSafe(c *C) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().PreparedParams = []types.Datum{
		types.NewIntDatum(1),
	}
	con := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 0}, RetType: newStringFieldType()}
	ft1 := con.GetType()
	ft2 := con.GetType()
	c.Assert(ft1, Not(Equals), ft2)
}
