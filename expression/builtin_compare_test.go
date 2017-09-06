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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
)

func (s *testEvaluatorSuite) TestCompareFunctionWithRefine(c *C) {
	defer testleak.AfterTest(c)()

	colInt := &Column{
		ColName: model.NewCIStr("a"),
		RetType: types.NewFieldType(mysql.TypeLong),
	}

	conOne := &Constant{
		Value:   types.NewFloat64Datum(1.0),
		RetType: types.NewFieldType(mysql.TypeDouble),
	}

	conOnePointOne := &Constant{
		Value:   types.NewFloat64Datum(1.1),
		RetType: types.NewFieldType(mysql.TypeDouble),
	}

	tests := []struct {
		arg0     Expression
		arg1     Expression
		funcName string
		result   string
	}{
		{colInt, conOne, ast.LT, "lt(a, 1)"},
		{colInt, conOne, ast.LE, "le(a, 1)"},
		{colInt, conOne, ast.GT, "gt(a, 1)"},
		{colInt, conOne, ast.GE, "ge(a, 1)"},
		{colInt, conOne, ast.EQ, "eq(a, 1)"},
		{colInt, conOne, ast.NullEQ, "nulleq(a, 1)"},
		{colInt, conOne, ast.NE, "ne(a, 1)"},
		{colInt, conOnePointOne, ast.LT, "lt(a, 2)"},
		{colInt, conOnePointOne, ast.LE, "le(a, 1)"},
		{colInt, conOnePointOne, ast.GT, "gt(a, 1)"},
		{colInt, conOnePointOne, ast.GE, "ge(a, 2)"},
		{colInt, conOnePointOne, ast.EQ, "eq(cast(a), 1.1)"},
		{colInt, conOnePointOne, ast.NullEQ, "nulleq(cast(a), 1.1)"},
		{colInt, conOnePointOne, ast.NE, "ne(cast(a), 1.1)"},
		{conOne, colInt, ast.LT, "lt(1, a)"},
		{conOne, colInt, ast.LE, "le(1, a)"},
		{conOne, colInt, ast.GT, "gt(1, a)"},
		{conOne, colInt, ast.GE, "ge(1, a)"},
		{conOne, colInt, ast.EQ, "eq(1, a)"},
		{conOne, colInt, ast.NullEQ, "nulleq(1, a)"},
		{conOne, colInt, ast.NE, "ne(1, a)"},
		{conOnePointOne, colInt, ast.LT, "lt(1, a)"},
		{conOnePointOne, colInt, ast.LE, "le(2, a)"},
		{conOnePointOne, colInt, ast.GT, "gt(2, a)"},
		{conOnePointOne, colInt, ast.GE, "ge(1, a)"},
		{conOnePointOne, colInt, ast.EQ, "eq(1.1, cast(a))"},
		{conOnePointOne, colInt, ast.NullEQ, "nulleq(1.1, cast(a))"},
		{conOnePointOne, colInt, ast.NE, "ne(1.1, cast(a))"},
	}

	for _, t := range tests {
		f, err := NewFunction(s.ctx, t.funcName, types.NewFieldType(mysql.TypeUnspecified), t.arg0, t.arg1)
		c.Assert(err, IsNil)
		c.Assert(f.String(), Equals, t.result)
	}
}

func (s *testEvaluatorSuite) TestCompare(c *C) {
	defer testleak.AfterTest(c)()

	intVal, uintVal, realVal, stringVal, decimalVal := 1, uint64(1), 1.1, "123", types.NewDecFromFloatForTest(123.123)
	timeVal := types.Time{Time: types.FromGoTime(time.Now()), Fsp: 6, Type: mysql.TypeDatetime}
	durationVal := types.Duration{Duration: time.Duration(12*time.Hour + 1*time.Minute + 1*time.Second)}
	jsonVal := json.CreateJSON("123")
	// test cases for generating function signatures.
	tests := []struct {
		arg0     interface{}
		arg1     interface{}
		funcName string
		tp       byte
		expected int64
	}{
		{intVal, intVal, ast.LT, mysql.TypeLonglong, 0},
		{stringVal, stringVal, ast.LT, mysql.TypeVarString, 0},
		{intVal, decimalVal, ast.LT, mysql.TypeNewDecimal, 1},
		{realVal, decimalVal, ast.LT, mysql.TypeDouble, 1},
		{durationVal, durationVal, ast.LT, mysql.TypeDuration, 0},
		{realVal, realVal, ast.LT, mysql.TypeDouble, 0},
		{intVal, intVal, ast.NullEQ, mysql.TypeLonglong, 1},
		{decimalVal, decimalVal, ast.LE, mysql.TypeNewDecimal, 1},
		{decimalVal, decimalVal, ast.GT, mysql.TypeNewDecimal, 0},
		{decimalVal, decimalVal, ast.GE, mysql.TypeNewDecimal, 1},
		{decimalVal, decimalVal, ast.NE, mysql.TypeNewDecimal, 0},
		{decimalVal, decimalVal, ast.EQ, mysql.TypeNewDecimal, 1},
		{decimalVal, decimalVal, ast.NullEQ, mysql.TypeNewDecimal, 1},
		{durationVal, durationVal, ast.LE, mysql.TypeDuration, 1},
		{durationVal, durationVal, ast.GT, mysql.TypeDuration, 0},
		{durationVal, durationVal, ast.GE, mysql.TypeDuration, 1},
		{durationVal, durationVal, ast.EQ, mysql.TypeDuration, 1},
		{durationVal, durationVal, ast.NE, mysql.TypeDuration, 0},
		{durationVal, durationVal, ast.NullEQ, mysql.TypeDuration, 1},
		{nil, nil, ast.NullEQ, mysql.TypeNull, 1},
		{nil, intVal, ast.NullEQ, mysql.TypeDouble, 0},
		{uintVal, intVal, ast.NullEQ, mysql.TypeLonglong, 1},
		{uintVal, intVal, ast.EQ, mysql.TypeLonglong, 1},
		{intVal, uintVal, ast.NullEQ, mysql.TypeLonglong, 1},
		{intVal, uintVal, ast.EQ, mysql.TypeLonglong, 1},
		{timeVal, timeVal, ast.LT, mysql.TypeDatetime, 0},
		{timeVal, timeVal, ast.LE, mysql.TypeDatetime, 1},
		{timeVal, timeVal, ast.GT, mysql.TypeDatetime, 0},
		{timeVal, timeVal, ast.GE, mysql.TypeDatetime, 1},
		{timeVal, timeVal, ast.EQ, mysql.TypeDatetime, 1},
		{timeVal, timeVal, ast.NE, mysql.TypeDatetime, 0},
		{timeVal, timeVal, ast.NullEQ, mysql.TypeDatetime, 1},
		{jsonVal, jsonVal, ast.LT, mysql.TypeJSON, 0},
		{jsonVal, jsonVal, ast.LE, mysql.TypeJSON, 1},
		{jsonVal, jsonVal, ast.GT, mysql.TypeJSON, 0},
		{jsonVal, jsonVal, ast.GE, mysql.TypeJSON, 1},
		{jsonVal, jsonVal, ast.NE, mysql.TypeJSON, 0},
		{jsonVal, jsonVal, ast.EQ, mysql.TypeJSON, 1},
		{jsonVal, jsonVal, ast.NullEQ, mysql.TypeJSON, 1},
	}

	for _, t := range tests {
		bf, err := funcs[t.funcName].getFunction(s.ctx, primitiveValsToConstants([]interface{}{t.arg0, t.arg1}))
		c.Assert(err, IsNil)
		args := bf.getArgs()
		c.Assert(args[0].GetType().Tp, Equals, t.tp)
		c.Assert(args[1].GetType().Tp, Equals, t.tp)
		res, isNil, err := bf.evalInt(nil)
		c.Assert(err, IsNil)
		c.Assert(isNil, IsFalse)
		c.Assert(res, Equals, t.expected)
	}

	// test <non-const decimal expression> <cmp> <const string expression>
	decimalCol, stringCon := &Column{RetType: types.NewFieldType(mysql.TypeNewDecimal)}, &Constant{RetType: types.NewFieldType(mysql.TypeVarchar)}
	bf, err := funcs[ast.LT].getFunction(s.ctx, []Expression{decimalCol, stringCon})
	c.Assert(err, IsNil)
	args := bf.getArgs()
	c.Assert(args[0].GetType().Tp, Equals, mysql.TypeNewDecimal)
	c.Assert(args[1].GetType().Tp, Equals, mysql.TypeNewDecimal)

	// test <time column> <cmp> <non-time const>
	timeCol := &Column{RetType: types.NewFieldType(mysql.TypeDatetime)}
	bf, err = funcs[ast.LT].getFunction(s.ctx, []Expression{timeCol, stringCon})
	c.Assert(err, IsNil)
	args = bf.getArgs()
	c.Assert(args[0].GetType().Tp, Equals, mysql.TypeDatetime)
	c.Assert(args[1].GetType().Tp, Equals, mysql.TypeDatetime)
}

func (s *testEvaluatorSuite) TestCoalesce(c *C) {
	defer testleak.AfterTest(c)()

	cases := []struct {
		args     []interface{}
		expected interface{}
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{nil}, nil, true, false},
		{[]interface{}{nil, nil}, nil, true, false},
		{[]interface{}{nil, nil, nil}, nil, true, false},
		{[]interface{}{nil, 1}, int64(1), false, false},
		{[]interface{}{nil, 1.1}, float64(1.1), false, false},
		{[]interface{}{1, 1.1}, float64(1), false, false},
		{[]interface{}{nil, types.NewDecFromFloatForTest(123.456)}, types.NewDecFromFloatForTest(123.456), false, false},
		{[]interface{}{1, types.NewDecFromFloatForTest(123.456)}, types.NewDecFromInt(1), false, false},
		{[]interface{}{nil, duration}, duration, false, false},
		{[]interface{}{nil, tm, nil}, tm, false, false},
		{[]interface{}{nil, dt, nil}, dt.String(), false, false},
		{[]interface{}{tm, dt}, tm, false, false},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Coalesce, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)

		d, err := f.Eval(nil)

		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetValue(), DeepEquals, t.expected)
			}
		}
	}

	f, err := funcs[ast.Length].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
	c.Assert(f.canBeFolded(), IsTrue)
}
