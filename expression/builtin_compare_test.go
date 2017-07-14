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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"time"
)

func (s *testEvaluatorSuite) TestCompare(c *C) {
	defer testleak.AfterTest(c)()

	intVal, realVal, stringVal, decimalVal := 1, 1.1, "123", types.NewDecFromFloatForTest(123.123)
	timeVal := types.Time{Time: types.FromGoTime(time.Now()), Fsp: 6, Type: mysql.TypeDatetime}
	durationVal := types.Duration{Duration: time.Duration(12*time.Hour + 1*time.Minute + 1*time.Second)}

	// test cases for generating function signatures.
	tests := []struct {
		arg0     interface{}
		arg1     interface{}
		funcName string
		tp       byte
	}{
		{intVal, intVal, ast.LT, mysql.TypeLonglong},
		{stringVal, stringVal, ast.LT, mysql.TypeVarString},
		{stringVal, timeVal, ast.LT, mysql.TypeDatetime},
		{intVal, decimalVal, ast.LT, mysql.TypeNewDecimal},
		{realVal, decimalVal, ast.LT, mysql.TypeDouble},
		{durationVal, durationVal, ast.LT, mysql.TypeDuration},
		{realVal, realVal, ast.LT, mysql.TypeDouble},
	}

	for _, t := range tests {
		bf, err := funcs[t.funcName].getFunction(primitiveValsToConstants([]interface{}{t.arg0, t.arg1}), s.ctx)
		c.Assert(err, IsNil)
		args := bf.getArgs()
		c.Assert(args[0].GetType().Tp, Equals, t.tp)
		c.Assert(args[1].GetType().Tp, Equals, t.tp)
	}

	// test <non-const decimal expression> <cmp> <const string expression>
	decimalCol, stringCon := &Column{RetType: types.NewFieldType(mysql.TypeNewDecimal)}, &Constant{RetType: types.NewFieldType(mysql.TypeVarchar)}
	bf, err := funcs[ast.LT].getFunction([]Expression{decimalCol, stringCon}, s.ctx)
	c.Assert(err, IsNil)
	args := bf.getArgs()
	c.Assert(args[0].GetType().Tp, Equals, mysql.TypeNewDecimal)
	c.Assert(args[1].GetType().Tp, Equals, mysql.TypeNewDecimal)

	// test <time column> <cmp> <non-time const>
	timeCol := &Column{RetType: types.NewFieldType(mysql.TypeDatetime)}
	bf, err = funcs[ast.LT].getFunction([]Expression{timeCol, stringCon}, s.ctx)
	c.Assert(err, IsNil)
	args = bf.getArgs()
	c.Assert(args[0].GetType().Tp, Equals, mysql.TypeDatetime)
	c.Assert(args[1].GetType().Tp, Equals, mysql.TypeDatetime)
}
