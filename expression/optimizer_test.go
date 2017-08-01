// Copyright 2017 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestOptimizerIsCmpFuncName(c *C) {
	defer testleak.AfterTest(c)()

	tests := []struct {
		funcName string
		expected bool
	}{
		{ast.EQ, true},
		{ast.GT, true},
		{ast.LT, true},
		{ast.GE, true},
		{ast.LE, true},
		{ast.In, false},
		{ast.NE, false},
	}
	for _, t := range tests {
		ret := isCmpFuncName(t.funcName)
		c.Assert(ret, Equals, t.expected,
			Commentf("%s got %s expected %s",
				t.funcName, ret, t.expected,
			))
	}
}

func convertFloatToDecimal(val interface{}) interface{} {
	var cval interface{}
	switch val.(type) {
	case float32:
		cval = types.NewDecFromFloatForTest(float64(val.(float32)))
	case float64:
		cval = types.NewDecFromFloatForTest(val.(float64))
	default:
		cval = val
	}
	return cval
}

func newConstForTest(val interface{}) Expression {
	cval := convertFloatToDecimal(val)
	var retType types.FieldType
	types.DefaultTypeForValue(cval, &retType)
	return &Constant{
		Value:   types.NewDatum(cval),
		RetType: &retType,
	}
}

func newConstRealForTest(val float64) Expression {
	var retType types.FieldType
	types.DefaultTypeForValue(val, &retType)
	return &Constant{
		Value:   types.NewDatum(val),
		RetType: &retType,
	}
}

func newColForTest(name string, val interface{}) Expression {
	cval := convertFloatToDecimal(val)
	var retType types.FieldType
	types.DefaultTypeForValue(cval, &retType)
	return &Column{
		ColName: model.NewCIStr(name),
		RetType: &retType,
	}
}

func (s *testEvaluatorSuite) TestOptimizerCanConvertToDecimal(c *C) {
	defer testleak.AfterTest(c)()

	tests := []struct {
		expr     Expression
		expected bool
	}{
		{newConstForTest("1.1"), true},
		{newConstForTest("1"), true},
		{newConstForTest(1.1), false},
		{newConstForTest(1), false},
		{newConstForTest("string"), false},
	}

	for _, t := range tests {
		ret := canConvertToDecimal(t.expr)
		c.Assert(ret, Equals, t.expected,
			Commentf("%s canConvertToDecimal return %s expect %s",
				t.expr, ret, t.expected,
			))
	}
}

func (s *testEvaluatorSuite) TestOptimizerGetScalarFuncOptimizeType(c *C) {
	defer testleak.AfterTest(c)()

	tests := []struct {
		op       string
		args     []Expression
		expected exprOptimizeType
	}{
		{ast.GT, []Expression{newColForTest("a", 1), newConstForTest(1.1)}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newColForTest("a", 1), newConstForTest(1.0)}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newColForTest("a", 1), newConstForTest(1)}, noOpt},
		{ast.GT, []Expression{newColForTest("a", 1), newConstForTest("1.1")}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newColForTest("a", 1), newConstForTest("1.0")}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newColForTest("a", 1), newConstForTest("1")}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newConstForTest(1.1), newColForTest("a", 1)}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newConstForTest(1.0), newColForTest("a", 1)}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newConstForTest(1), newColForTest("a", 1)}, noOpt},
		{ast.GT, []Expression{newConstForTest("1.1"), newColForTest("a", 1)}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newConstForTest("1.0"), newColForTest("a", 1)}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newConstForTest("1"), newColForTest("a", 1)}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newConstForTest("string"), newColForTest("a", 1)}, noOpt},
		{ast.GT, []Expression{newConstForTest(1), newColForTest("a", 1.0)}, noOpt},
		{ast.GT, []Expression{newColForTest("a", "string"), newConstForTest(1.0)}, noOpt},
		{ast.GT, []Expression{newConstRealForTest(1.1), newColForTest("a", 1)}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newConstRealForTest(1.0), newColForTest("a", 1)}, intColCmpDecimalConstOpt},
		{ast.GT, []Expression{newConstRealForTest(float64(1)), newColForTest("a", 1)}, intColCmpDecimalConstOpt},
	}

	for _, t := range tests {
		ret := getScalarFuncOptimizeType(t.op, t.args...)
		c.Assert(ret, Equals, t.expected,
			Commentf("%v %s %v expect %v got %v",
				t.args[0], t.op, t.args[1], t.expected, ret,
			))
	}
}

func (s *testEvaluatorSuite) TestOptimizerOptimizeIntColumnCmpDecimalConstant(c *C) {
	defer testleak.AfterTest(c)()

	col := newColForTest("a", 1)
	tests := []struct {
		op           string
		args         []Expression
		expectedOp   string
		expectedArgs []Expression
	}{
		{
			op:           ast.EQ,
			args:         []Expression{col, newConstForTest(1.0)},
			expectedOp:   ast.EQ,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
		{
			op:           ast.EQ,
			args:         []Expression{newConstForTest(1.0), col},
			expectedOp:   ast.EQ,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
		{
			op:           ast.EQ,
			args:         []Expression{newConstRealForTest(1.0), col},
			expectedOp:   ast.EQ,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
		{
			op:           ast.EQ,
			args:         []Expression{newConstRealForTest(1.1), col},
			expectedOp:   ast.EQ,
			expectedArgs: []Expression{One, Zero},
		},
		{
			op:           ast.EQ,
			args:         []Expression{col, newConstForTest(1.1)},
			expectedOp:   ast.EQ,
			expectedArgs: []Expression{One, Zero},
		},
		{
			op:           ast.EQ,
			args:         []Expression{col, newConstForTest("1.1")},
			expectedOp:   ast.EQ,
			expectedArgs: []Expression{One, Zero},
		},
		{
			op:           ast.GT,
			args:         []Expression{col, newConstForTest(1.1)},
			expectedOp:   ast.GE,
			expectedArgs: []Expression{col, newConstForTest(2)},
		},
		{
			op:           ast.GT,
			args:         []Expression{col, newConstForTest("1.1")},
			expectedOp:   ast.GE,
			expectedArgs: []Expression{col, newConstForTest(2)},
		},
		{
			op:           ast.GT,
			args:         []Expression{col, newConstRealForTest(1.1)},
			expectedOp:   ast.GE,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
		{
			op:           ast.GE,
			args:         []Expression{col, newConstForTest(1.1)},
			expectedOp:   ast.GE,
			expectedArgs: []Expression{col, newConstForTest(2)},
		},
		{
			op:           ast.GE,
			args:         []Expression{col, newConstForTest("1.1")},
			expectedOp:   ast.GE,
			expectedArgs: []Expression{col, newConstForTest(2)},
		},
		{
			op:           ast.GE,
			args:         []Expression{col, newConstRealForTest(1.1)},
			expectedOp:   ast.GE,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
		{
			op:           ast.LT,
			args:         []Expression{col, newConstForTest(1.1)},
			expectedOp:   ast.LE,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
		{
			op:           ast.LT,
			args:         []Expression{col, newConstForTest("1.1")},
			expectedOp:   ast.LE,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
		{
			op:           ast.LT,
			args:         []Expression{col, newConstRealForTest(1.1)},
			expectedOp:   ast.LE,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
		{
			op:           ast.LE,
			args:         []Expression{col, newConstForTest(1.1)},
			expectedOp:   ast.LE,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
		{
			op:           ast.LE,
			args:         []Expression{col, newConstForTest("1.1")},
			expectedOp:   ast.LE,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
		{
			op:           ast.LE,
			args:         []Expression{col, newConstRealForTest(1.1)},
			expectedOp:   ast.LE,
			expectedArgs: []Expression{col, newConstForTest(1)},
		},
	}
	ctx := s.ctx
	for _, t := range tests {
		retOp, retArgs := optimizeIntColumnCmpDecimalConstant(t.op, t.args...)
		c.Assert(retOp, Equals, t.expectedOp)
		for i, arg := range retArgs {
			c.Assert(t.expectedArgs[i].Equal(arg, ctx), IsTrue,
				Commentf("%v %s %v => %v %s %v Fail (%v %s %v)",
					t.args[0], t.op, t.args[1],
					t.expectedArgs[0], t.op, t.expectedArgs[1],
					retArgs[0], retOp, retArgs[1],
				))
		}
	}
}
