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
	"fmt"
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestBitCount(c *C) {
	defer testleak.AfterTest(c)()
	stmtCtx := s.ctx.GetSessionVars().StmtCtx
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()
	fc := funcs[ast.BitCount]
	var bitCountCases = []struct {
		origin interface{}
		count  interface{}
	}{
		{int64(8), int64(1)},
		{int64(29), int64(4)},
		{int64(0), int64(0)},
		{int64(-1), int64(64)},
		{int64(-11), int64(62)},
		{int64(-1000), int64(56)},
		{float64(1.1), int64(1)},
		{float64(3.1), int64(2)},
		{float64(-1.1), int64(64)},
		{float64(-3.1), int64(63)},
		{uint64(math.MaxUint64), int64(64)},
		{string("xxx"), int64(0)},
		{nil, nil},
	}
	for _, test := range bitCountCases {
		in := types.NewDatum(test.origin)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{in}))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		count, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		if count.IsNull() {
			c.Assert(test.count, IsNil)
			continue
		}
		sc := new(variable.StatementContext)
		sc.IgnoreTruncate = true
		res, err := count.ToInt64(sc)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.count)
	}
}

func (s *testEvaluatorSuite) TestRowFunc(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.RowFunc]
	_, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums([]interface{}{"1", 1.2, true, 120}...)))
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestSetVar(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.SetVar]
	testCases := []struct {
		args []interface{}
		res  interface{}
	}{
		{[]interface{}{"a", "12"}, "12"},
		{[]interface{}{"b", "34"}, "34"},
		{[]interface{}{"c", nil}, ""},
		{[]interface{}{"c", "ABC"}, "ABC"},
		{[]interface{}{"c", "dEf"}, "dEf"},
	}
	for _, tc := range testCases {
		fn, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(fn, types.DatumRow(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(d.GetString(), Equals, tc.res)
		if tc.args[1] != nil {
			key, ok := tc.args[0].(string)
			c.Assert(ok, Equals, true)
			val, ok := tc.res.(string)
			c.Assert(ok, Equals, true)
			c.Assert(s.ctx.GetSessionVars().Users[key], Equals, val)
		}
	}
}

func (s *testEvaluatorSuite) TestGetVar(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.GetVar]

	sessionVars := []struct {
		key string
		val string
	}{
		{"a", "中"},
		{"b", "文字符chuan"},
		{"c", ""},
	}
	for _, kv := range sessionVars {
		s.ctx.GetSessionVars().Users[kv.key] = kv.val
	}

	testCases := []struct {
		args []interface{}
		res  interface{}
	}{
		{[]interface{}{"a"}, "中"},
		{[]interface{}{"b"}, "文字符chuan"},
		{[]interface{}{"c"}, ""},
		{[]interface{}{"d"}, ""},
	}
	for _, tc := range testCases {
		fn, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(fn, types.DatumRow(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(d.GetString(), Equals, tc.res)
	}
}

func (s *testEvaluatorSuite) TestValues(c *C) {
	defer testleak.AfterTest(c)()
	fc := &valuesFunctionClass{baseFunctionClass{ast.Values, 0, 0}, 1, types.NewFieldType(mysql.TypeVarchar)}
	_, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums("")))
	c.Assert(err, ErrorMatches, "*Incorrect parameter count in the call to native function 'values'")
	sig, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums()))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(sig, nil)
	c.Assert(err.Error(), Equals, "Session current insert values is nil")
	s.ctx.GetSessionVars().CurrInsertValues = types.MakeDatums("1")
	_, err = evalBuiltinFunc(sig, nil)
	c.Assert(err.Error(), Equals, fmt.Sprintf("Session current insert values len %d and column's offset %v don't match", 1, 1))
	currInsertValues := types.MakeDatums("1", "2")
	s.ctx.GetSessionVars().CurrInsertValues = currInsertValues
	ret, err := evalBuiltinFunc(sig, nil)
	c.Assert(err, IsNil)
	cmp, err := ret.CompareDatum(nil, &currInsertValues[1])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
}
