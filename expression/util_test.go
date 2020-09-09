// Copyright 2015 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = check.Suite(&testUtilSuite{})

type testUtilSuite struct {
}

func (s *testUtilSuite) TestSetExprColumnInOperand(c *check.C) {
	col := &Column{RetType: newIntFieldType()}
	c.Assert(setExprColumnInOperand(col).(*Column).InOperand, check.IsTrue)

	f, err := funcs[ast.Abs].getFunction(mock.NewContext(), []Expression{col})
	c.Assert(err, check.IsNil)
	fun := &ScalarFunction{Function: f}
	setExprColumnInOperand(fun)
	c.Assert(f.getArgs()[0].(*Column).InOperand, check.IsTrue)
}

func (s testUtilSuite) TestPopRowFirstArg(c *check.C) {
	c1, c2, c3 := &Column{RetType: newIntFieldType()}, &Column{RetType: newIntFieldType()}, &Column{RetType: newIntFieldType()}
	f, err := funcs[ast.RowFunc].getFunction(mock.NewContext(), []Expression{c1, c2, c3})
	c.Assert(err, check.IsNil)
	fun := &ScalarFunction{Function: f, FuncName: model.NewCIStr(ast.RowFunc), RetType: newIntFieldType()}
	fun2, err := PopRowFirstArg(mock.NewContext(), fun)
	c.Assert(err, check.IsNil)
	c.Assert(len(fun2.(*ScalarFunction).GetArgs()), check.Equals, 2)
}

func (s testUtilSuite) TestGetStrIntFromConstant(c *check.C) {
	col := &Column{}
	_, _, err := GetStringFromConstant(mock.NewContext(), col)
	c.Assert(err, check.NotNil)

	con := &Constant{RetType: &types.FieldType{Tp: mysql.TypeNull}}
	_, isNull, err := GetStringFromConstant(mock.NewContext(), con)
	c.Assert(err, check.IsNil)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{RetType: newIntFieldType(), Value: types.NewIntDatum(1)}
	ret, _, _ := GetStringFromConstant(mock.NewContext(), con)
	c.Assert(ret, check.Equals, "1")

	con = &Constant{RetType: &types.FieldType{Tp: mysql.TypeNull}}
	_, isNull, _ = GetIntFromConstant(mock.NewContext(), con)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{RetType: newStringFieldType(), Value: types.NewStringDatum("abc")}
	_, isNull, _ = GetIntFromConstant(mock.NewContext(), con)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{RetType: newStringFieldType(), Value: types.NewStringDatum("123")}
	num, _, _ := GetIntFromConstant(mock.NewContext(), con)
	c.Assert(num, check.Equals, 123)
}

func (s *testUtilSuite) TestSubstituteCorCol2Constant(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	corCol1 := &CorrelatedColumn{Data: &One.Value}
	corCol1.RetType = types.NewFieldType(mysql.TypeLonglong)
	corCol2 := &CorrelatedColumn{Data: &One.Value}
	corCol2.RetType = types.NewFieldType(mysql.TypeLonglong)
	cast := BuildCastFunction(ctx, corCol1, types.NewFieldType(mysql.TypeLonglong))
	plus := newFunction(ast.Plus, cast, corCol2)
	plus2 := newFunction(ast.Plus, plus, One)
	ans1 := &Constant{Value: types.NewIntDatum(3), RetType: types.NewFieldType(mysql.TypeLonglong)}
	ret, err := SubstituteCorCol2Constant(plus2)
	c.Assert(err, check.IsNil)
	c.Assert(ret.Equal(ctx, ans1), check.IsTrue)
	col1 := &Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	ret, err = SubstituteCorCol2Constant(col1)
	c.Assert(err, check.IsNil)
	ans2 := col1
	c.Assert(ret.Equal(ctx, ans2), check.IsTrue)
	plus3 := newFunction(ast.Plus, plus2, col1)
	ret, err = SubstituteCorCol2Constant(plus3)
	c.Assert(err, check.IsNil)
	ans3 := newFunction(ast.Plus, ans1, col1)
	c.Assert(ret.Equal(ctx, ans3), check.IsTrue)
}

func (s *testUtilSuite) TestPushDownNot(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	col := &Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	// !((a=1||a=1)&&a=1)
	eqFunc := newFunction(ast.EQ, col, One)
	orFunc := newFunction(ast.LogicOr, eqFunc, eqFunc)
	andFunc := newFunction(ast.LogicAnd, orFunc, eqFunc)
	notFunc := newFunction(ast.UnaryNot, andFunc)
	// (a!=1&&a!=1)||a=1
	neFunc := newFunction(ast.NE, col, One)
	andFunc2 := newFunction(ast.LogicAnd, neFunc, neFunc)
	orFunc2 := newFunction(ast.LogicOr, andFunc2, neFunc)
	notFuncCopy := notFunc.Clone()
	ret := PushDownNot(ctx, notFunc)
	c.Assert(ret.Equal(ctx, orFunc2), check.IsTrue)
	c.Assert(notFunc.Equal(ctx, notFuncCopy), check.IsTrue)

	// issue 15725
	// (not not a) should not be optimized to (a)
	notFunc = newFunction(ast.UnaryNot, col)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	ret = PushDownNot(ctx, notFunc)
<<<<<<< HEAD
	c.Assert(ret.Equal(ctx, col), check.IsFalse)
=======
	c.Assert(ret.Equal(ctx, newFunction(ast.IsTruthWithNull, col)), check.IsTrue)
>>>>>>> 0c36203... expression: add new scalar function IsTruthWithNull (#19621)

	// (not not (a+1)) should not be optimized to (a+1)
	plusFunc := newFunction(ast.Plus, col, One)
	notFunc = newFunction(ast.UnaryNot, plusFunc)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	ret = PushDownNot(ctx, notFunc)
<<<<<<< HEAD
	c.Assert(ret.Equal(ctx, col), check.IsFalse)
=======
	c.Assert(ret.Equal(ctx, newFunction(ast.IsTruthWithNull, plusFunc)), check.IsTrue)
>>>>>>> 0c36203... expression: add new scalar function IsTruthWithNull (#19621)

	// (not not not a) should be optimized to (not a)
	notFunc = newFunction(ast.UnaryNot, col)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	ret = PushDownNot(ctx, notFunc)
<<<<<<< HEAD
	c.Assert(ret.Equal(ctx, newFunction(ast.UnaryNot, col)), check.IsTrue)
=======
	c.Assert(ret.Equal(ctx, newFunction(ast.UnaryNot, newFunction(ast.IsTruthWithNull, col))), check.IsTrue)
>>>>>>> 0c36203... expression: add new scalar function IsTruthWithNull (#19621)

	// (not not not not a) should be optimized to (not not a)
	notFunc = newFunction(ast.UnaryNot, col)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	notFunc = newFunction(ast.UnaryNot, notFunc)
	ret = PushDownNot(ctx, notFunc)
<<<<<<< HEAD
	c.Assert(ret.Equal(ctx, newFunction(ast.UnaryNot, newFunction(ast.UnaryNot, col))), check.IsTrue)
=======
	c.Assert(ret.Equal(ctx, newFunction(ast.IsTruthWithNull, col)), check.IsTrue)
>>>>>>> 0c36203... expression: add new scalar function IsTruthWithNull (#19621)
}

func (s *testUtilSuite) TestFilter(c *check.C) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	result := make([]Expression, 0, 5)
	result = Filter(result, conditions, isLogicOrFunction)
	c.Assert(result, check.HasLen, 1)
}

func (s *testUtilSuite) TestFilterOutInPlace(c *check.C) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	remained, filtered := FilterOutInPlace(conditions, isLogicOrFunction)
	c.Assert(len(remained), check.Equals, 2)
	c.Assert(remained[0].(*ScalarFunction).FuncName.L, check.Equals, "eq")
	c.Assert(remained[1].(*ScalarFunction).FuncName.L, check.Equals, "eq")
	c.Assert(len(filtered), check.Equals, 1)
	c.Assert(filtered[0].(*ScalarFunction).FuncName.L, check.Equals, "or")
}

func isLogicOrFunction(e Expression) bool {
	if f, ok := e.(*ScalarFunction); ok {
		return f.FuncName.L == ast.LogicOr
	}
	return false
}

func (s *testUtilSuite) TestDisableParseJSONFlag4Expr(c *check.C) {
	var expr Expression
	expr = &Column{RetType: newIntFieldType()}
	ft := expr.GetType()
	ft.Flag |= mysql.ParseToJSONFlag
	DisableParseJSONFlag4Expr(expr)
	c.Assert(mysql.HasParseToJSONFlag(ft.Flag), check.IsTrue)

	expr = &CorrelatedColumn{Column: Column{RetType: newIntFieldType()}}
	ft = expr.GetType()
	ft.Flag |= mysql.ParseToJSONFlag
	DisableParseJSONFlag4Expr(expr)
	c.Assert(mysql.HasParseToJSONFlag(ft.Flag), check.IsTrue)

	expr = &ScalarFunction{RetType: newIntFieldType()}
	ft = expr.GetType()
	ft.Flag |= mysql.ParseToJSONFlag
	DisableParseJSONFlag4Expr(expr)
	c.Assert(mysql.HasParseToJSONFlag(ft.Flag), check.IsFalse)
}

func BenchmarkExtractColumns(b *testing.B) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.EQ, newColumn(2), newColumn(3)),
		newFunction(ast.EQ, newColumn(3), newLonglong(1)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	expr := ComposeCNFCondition(mock.NewContext(), conditions...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractColumns(expr)
	}
	b.ReportAllocs()
}

func BenchmarkExprFromSchema(b *testing.B) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.EQ, newColumn(2), newColumn(3)),
		newFunction(ast.EQ, newColumn(3), newLonglong(1)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	expr := ComposeCNFCondition(mock.NewContext(), conditions...)
	schema := &Schema{Columns: ExtractColumns(expr)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExprFromSchema(expr, schema)
	}
	b.ReportAllocs()
}

// MockExpr is mainly for test.
type MockExpr struct {
	err error
	t   *types.FieldType
	i   interface{}
}

func (m *MockExpr) String() string                          { return "" }
func (m *MockExpr) MarshalJSON() ([]byte, error)            { return nil, nil }
func (m *MockExpr) Eval(row chunk.Row) (types.Datum, error) { return types.NewDatum(m.i), m.err }
func (m *MockExpr) EvalInt(ctx sessionctx.Context, row chunk.Row) (val int64, isNull bool, err error) {
	if x, ok := m.i.(int64); ok {
		return int64(x), false, m.err
	}
	return 0, m.i == nil, m.err
}
func (m *MockExpr) EvalReal(ctx sessionctx.Context, row chunk.Row) (val float64, isNull bool, err error) {
	if x, ok := m.i.(float64); ok {
		return float64(x), false, m.err
	}
	return 0, m.i == nil, m.err
}
func (m *MockExpr) EvalString(ctx sessionctx.Context, row chunk.Row) (val string, isNull bool, err error) {
	if x, ok := m.i.(string); ok {
		return string(x), false, m.err
	}
	return "", m.i == nil, m.err
}
func (m *MockExpr) EvalDecimal(ctx sessionctx.Context, row chunk.Row) (val *types.MyDecimal, isNull bool, err error) {
	if x, ok := m.i.(*types.MyDecimal); ok {
		return x, false, m.err
	}
	return nil, m.i == nil, m.err
}
func (m *MockExpr) EvalTime(ctx sessionctx.Context, row chunk.Row) (val types.Time, isNull bool, err error) {
	if x, ok := m.i.(types.Time); ok {
		return x, false, m.err
	}
	return types.Time{}, m.i == nil, m.err
}
func (m *MockExpr) EvalDuration(ctx sessionctx.Context, row chunk.Row) (val types.Duration, isNull bool, err error) {
	if x, ok := m.i.(types.Duration); ok {
		return x, false, m.err
	}
	return types.Duration{}, m.i == nil, m.err
}
func (m *MockExpr) EvalJSON(ctx sessionctx.Context, row chunk.Row) (val json.BinaryJSON, isNull bool, err error) {
	if x, ok := m.i.(json.BinaryJSON); ok {
		return x, false, m.err
	}
	return json.BinaryJSON{}, m.i == nil, m.err
}
func (m *MockExpr) GetType() *types.FieldType                         { return m.t }
func (m *MockExpr) Clone() Expression                                 { return nil }
func (m *MockExpr) Equal(ctx sessionctx.Context, e Expression) bool   { return false }
func (m *MockExpr) IsCorrelated() bool                                { return false }
func (m *MockExpr) ConstItem() bool                                   { return false }
func (m *MockExpr) Decorrelate(schema *Schema) Expression             { return m }
func (m *MockExpr) ResolveIndices(schema *Schema) (Expression, error) { return m, nil }
func (m *MockExpr) resolveIndices(schema *Schema) error               { return nil }
func (m *MockExpr) ExplainInfo() string                               { return "" }
func (m *MockExpr) ExplainNormalizedInfo() string                     { return "" }
func (m *MockExpr) HashCode(sc *stmtctx.StatementContext) []byte      { return nil }
